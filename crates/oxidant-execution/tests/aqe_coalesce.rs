//! AQE shuffle-partition coalescing applied end-to-end (`OXIDANT_AQE=1`): a producer stage
//! whose sampled buckets are all small is coalesced at the stage barrier, and its consumers
//! dispatch `new_p` reader partitions that each pull a modulus class of producer buckets
//! (`p, p+new_p, …`) instead of the plain `0..new_p` range that would orphan the rest.
//! Every producer bucket must still be read exactly once — the distributed result must match
//! the single-node ground truth row-for-row — and the barrier must record the decision
//! (the `AqeCoalesced` event) for the coalesced read to have actually happened.

use std::sync::Arc;

use oxidant_execution::driver::{run_stages_obs, Cluster, DistributedPlan};
use oxidant_execution::flight::serve_worker;
use oxidant_execution::shuffle::hash_partition;
use oxidant_loom::arrow::array::Int64Array;
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;
use oxidant_observability::{AppStateStore, ExecutionEvent};

/// `OXIDANT_AQE` / `OXIDANT_SHUFFLE_PARTITIONS` are process-global; serialize these tests.
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const KEYS_PER_WORKER: i64 = 32;
const QUERY: &str = "SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k";

fn make_batch(start: i64, end: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from((start..end).collect::<Vec<_>>())),
            Arc::new(Int64Array::from((start..end).collect::<Vec<_>>())),
        ],
    )
    .unwrap()
}

/// Extract (k, c, s) rows and sort by k for order-insensitive comparison.
fn rows(batches: &[RecordBatch]) -> Vec<(i64, i64, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let c = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let s = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((k.value(i), c.value(i), s.value(i)));
        }
    }
    out.sort();
    out
}

async fn start_worker(start: i64, end: i64) -> String {
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let engine = Arc::new(Engine::new());
    engine
        .register_batches("t", vec![make_batch(start, end)])
        .unwrap();
    tokio::spawn(async move {
        let _ = serve_worker(port, engine).await;
    });
    format!("http://127.0.0.1:{port}")
}

/// Run the two-stage GROUP BY (`partial-agg → hash shuffle → final-agg`) over `workers`
/// workers with `np` shuffle partitions and AQE enabled. The barrier must coalesce the
/// producer to `workers` reader partitions (the `coalesced_partitions` target), and the
/// coalesced read must match the single-node result.
async fn coalesced_groupby_matches_single_node(workers: i64, np: u32) {
    let _guard = ENV_LOCK.lock().await;
    std::env::set_var("OXIDANT_AQE", "1");
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", np.to_string());

    let mut endpoints = Vec::new();
    for i in 0..workers {
        endpoints.push(start_worker(i * KEYS_PER_WORKER, (i + 1) * KEYS_PER_WORKER).await);
    }
    let cluster = Cluster::new(endpoints.clone());
    assert_eq!(
        cluster.num_partitions, np,
        "OXIDANT_SHUFFLE_PARTITIONS must pin np"
    );

    // The AQE skew guard keeps np when one bucket holds more than a third of the sampled
    // rows. The barrier samples bucket `p` from its owner endpoint only
    // (`owner_bucket_row_counts`), and each worker's partial agg emits one row per key, so
    // the sampled distribution is reproducible exactly: partition each worker's key range and
    // pick each bucket's count from its owner's distribution. FNV-1a bucketing is
    // deterministic, so this fails loudly here rather than silently skipping the coalesced
    // path if the test keys ever trip the guard.
    if np > 2 {
        let per_worker: Vec<Vec<usize>> = (0..workers)
            .map(|i| {
                hash_partition(
                    &[make_batch(i * KEYS_PER_WORKER, (i + 1) * KEYS_PER_WORKER)],
                    &[0],
                    np as usize,
                )
                .unwrap()
                .iter()
                .map(|bs| bs.iter().map(|b| b.num_rows()).sum())
                .collect()
            })
            .collect();
        let sampled: Vec<usize> = (0..np)
            .map(|p| {
                let owner = cluster.owner_endpoint(p).unwrap();
                let wi = (0..workers as usize)
                    .find(|&wi| endpoints[wi] == owner)
                    .expect("owner is a cluster worker");
                per_worker[wi][p as usize]
            })
            .collect();
        let total: usize = sampled.iter().sum();
        let max = *sampled.iter().max().unwrap();
        assert!(
            max * 3 <= total,
            "test keys must not trip the AQE skew guard: {sampled:?}"
        );
    }

    let plan = DistributedPlan {
        partial_sql: QUERY.into(),
        final_sql: "SELECT k, SUM(c) AS c, SUM(s) AS s FROM shuffle_input GROUP BY k".into(),
        hash_key_cols: vec![0],
    };
    let stages = plan.into_stages();

    // Single-node ground truth over the whole dataset.
    let single = Engine::new();
    single
        .register_batches("t", vec![make_batch(0, KEYS_PER_WORKER * workers)])
        .unwrap();
    let expected = rows(&single.sql(QUERY).await.unwrap());

    let store = Arc::new(AppStateStore::new());
    let mut rx = store.subscribe();
    // Retry until the workers are up and the distributed query returns.
    let mut actual = None;
    for _ in 0..50 {
        match run_stages_obs(
            &cluster,
            &stages,
            Some(store.clone()),
            Some("aqe-coalesce-test".into()),
            None,
        )
        .await
        {
            Ok(b) => {
                actual = Some(rows(&b));
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    }
    let actual = actual.expect("distributed query never succeeded");
    assert_eq!(
        actual, expected,
        "coalesced read must match the single-node result row-for-row"
    );

    // The barrier must have recorded the coalesce decision for the producer stage 0 —
    // without it the consumers ran the legacy per-bucket read and this test proves nothing.
    let mut decision = None;
    while let Ok(ev) = rx.try_recv() {
        if let ExecutionEvent::AqeCoalesced {
            stage_id: 0,
            old_partitions,
            new_partitions,
            ..
        } = ev
        {
            decision = Some((old_partitions, new_partitions));
        }
    }
    std::env::remove_var("OXIDANT_AQE");
    std::env::remove_var("OXIDANT_SHUFFLE_PARTITIONS");
    assert_eq!(
        decision,
        Some((np, workers as u32)),
        "barrier must coalesce the producer stage from {np} to {workers}"
    );
}

#[tokio::test]
async fn aqe_coalesces_four_partitions_to_two_readers() {
    coalesced_groupby_matches_single_node(2, 4).await;
}

#[tokio::test]
async fn aqe_coalesces_to_a_single_reader() {
    coalesced_groupby_matches_single_node(1, 2).await;
}

#[tokio::test]
async fn aqe_coalesced_modulus_need_not_divide_partitions() {
    // np=4 coalesced to m=3: reader classes are {0,3}, {1}, {2} — uneven, still exactly-once.
    coalesced_groupby_matches_single_node(3, 4).await;
}
