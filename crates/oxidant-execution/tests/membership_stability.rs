//! Membership stability: a mid-query worker-set change must not silently drop shuffle rows,
//! and the driver's fan-out must match the workers' file-shard modulus.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use oxidant_execution::driver::{
    run_distributed, run_distributed_with_membership, Cluster, DistributedPlan,
};
use oxidant_execution::flight::serve_worker;
use oxidant_execution::membership::ClusterMembership;
use oxidant_loom::arrow::array::Int64Array;
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;

fn make_batch(start: i64, end: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    // Many distinct keys so both shuffle buckets are populated under np=2.
    let ks: Vec<i64> = (start..end).collect();
    let vs: Vec<i64> = (start..end).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ks)),
            Arc::new(Int64Array::from(vs)),
        ],
    )
    .unwrap()
}

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

fn total_count(batches: &[RecordBatch]) -> i64 {
    batches
        .iter()
        .map(|b| {
            let c = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            (0..b.num_rows()).map(|i| c.value(i)).sum::<i64>()
        })
        .sum()
}

/// Membership that returns `full` for the first `keep_full_calls` snapshots, then `reduced`.
/// `Cluster::from_membership` + one producer-stage refresh + output-stage refresh → use
/// `keep_full_calls = 2` so the shrink lands between producer and consumer.
struct ShrinkingMembership {
    full: Vec<String>,
    reduced: Vec<String>,
    calls: AtomicUsize,
    keep_full_calls: usize,
}

impl ClusterMembership for ShrinkingMembership {
    fn endpoints(&self) -> Vec<String> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if n < self.keep_full_calls {
            self.full.clone()
        } else {
            self.reduced.clone()
        }
    }
}

async fn bind_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

#[tokio::test]
async fn membership_shrink_mid_query_must_not_silently_drop_rows() {
    const N: i64 = 200;
    let query = "SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k";

    let single = Engine::new();
    single
        .register_batches("t", vec![make_batch(0, N)])
        .unwrap();
    let expected = rows(&single.sql(query).await.unwrap());

    let p0 = bind_port().await;
    let p1 = bind_port().await;
    let e0 = Arc::new(Engine::new());
    e0.register_batches("t", vec![make_batch(0, N / 2)])
        .unwrap();
    let e1 = Arc::new(Engine::new());
    e1.register_batches("t", vec![make_batch(N / 2, N)])
        .unwrap();
    tokio::spawn(async move {
        let _ = serve_worker(p0, e0).await;
    });
    tokio::spawn(async move {
        let _ = serve_worker(p1, e1).await;
    });

    let ep0 = format!("http://127.0.0.1:{p0}");
    let ep1 = format!("http://127.0.0.1:{p1}");
    let membership = Arc::new(ShrinkingMembership {
        full: vec![ep0.clone(), ep1.clone()],
        reduced: vec![ep0.clone()],
        calls: AtomicUsize::new(0),
        keep_full_calls: 2, // construct + producer barrier
    });

    let plan = DistributedPlan {
        partial_sql: "SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k".into(),
        final_sql: "SELECT k, SUM(c) AS c, SUM(s) AS s FROM shuffle_input GROUP BY k".into(),
        hash_key_cols: vec![0],
    };

    // Wait for workers, then run once under shrinking membership.
    let stable = Cluster::new(vec![ep0, ep1]);
    for _ in 0..50 {
        if run_distributed(&stable, &plan).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    match run_distributed_with_membership(membership, &plan).await {
        Ok(batches) => {
            let actual = rows(&batches);
            let got = total_count(&batches);
            assert_eq!(
                actual, expected,
                "membership shrink mid-query returned Ok with wrong answer \
                 (row count sum={got}, expected={N}): silent shuffle row loss"
            );
        }
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("membership") || msg.contains("OXIDANT_WORKER_COUNT"),
                "unexpected error (want loud membership failure): {msg}"
            );
        }
    }
}

#[tokio::test]
async fn driver_rejects_shard_modulus_mismatch() {
    const N: i64 = 100;
    let p0 = bind_port().await;
    let e0 = Arc::new(Engine::new());
    // Only shard 0 of 2 is present — the surviving worker under a 1-of-2 driver view.
    e0.register_batches("t", vec![make_batch(0, N / 2)])
        .unwrap();
    tokio::spawn(async move {
        let _ = serve_worker(p0, e0).await;
    });

    let mut cluster = Cluster::new(vec![format!("http://127.0.0.1:{p0}")]);
    // Workers were configured as 2-way shards; driver only sees one Ready endpoint.
    cluster.expected_worker_count = Some(2);

    let plan = DistributedPlan {
        partial_sql: "SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k".into(),
        final_sql: "SELECT k, SUM(c) AS c, SUM(s) AS s FROM shuffle_input GROUP BY k".into(),
        hash_key_cols: vec![0],
    };

    let mut last_err = None;
    for _ in 0..50 {
        match run_distributed(&cluster, &plan).await {
            Ok(batches) => {
                let got = total_count(&batches);
                panic!(
                    "driver accepted 1-of-2 fan-out and returned Ok with partial data \
                     (count sum={got}, full N={N}); must hard-fail on shard modulus mismatch"
                );
            }
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("OXIDANT_WORKER_COUNT") || msg.contains("worker fan-out") {
                    return;
                }
                // Worker not up yet — retry.
                last_err = Some(msg);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
    panic!("never saw shard-modulus mismatch error; last={last_err:?}");
}

#[tokio::test]
async fn static_two_worker_cluster_still_succeeds() {
    const N: i64 = 40;
    let p0 = bind_port().await;
    let p1 = bind_port().await;
    let e0 = Arc::new(Engine::new());
    e0.register_batches("t", vec![make_batch(0, N / 2)])
        .unwrap();
    let e1 = Arc::new(Engine::new());
    e1.register_batches("t", vec![make_batch(N / 2, N)])
        .unwrap();
    tokio::spawn(async move {
        let _ = serve_worker(p0, e0).await;
    });
    tokio::spawn(async move {
        let _ = serve_worker(p1, e1).await;
    });

    let cluster = Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ]);
    let plan = DistributedPlan {
        partial_sql: "SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k".into(),
        final_sql: "SELECT k, SUM(c) AS c, SUM(s) AS s FROM shuffle_input GROUP BY k".into(),
        hash_key_cols: vec![0],
    };

    let mut ok = false;
    for _ in 0..50 {
        if run_distributed(&cluster, &plan).await.is_ok() {
            ok = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(ok, "fixed-membership 2-worker cluster must keep working");
}

/// Producers write 4 shuffle buckets on a 2-worker cluster. With AQE on,
/// `coalesced_partitions` suggests shrinking to 2 — previously the driver mutated
/// `num_partitions` mid-query and the consumer orphaned buckets 2..3 (silent row loss).
#[tokio::test]
async fn aqe_coalesce_suggestion_must_not_orphan_shuffle_buckets() {
    // Pin on explicitly (AQE defaults on; keep the test hermetic against env opt-out).
    std::env::set_var("OXIDANT_AQE", "1");
    // Prove the suggestion path would fire for this shape (partitions > workers, small buckets).
    let would_coalesce =
        oxidant_execution::aqe::coalesced_partitions(2, 4, &[10, 10, 10, 10]).expect("coalesce");
    assert_eq!(
        would_coalesce, 2,
        "test setup must engage AQE coalescing (suggested 2 of 4)"
    );
    assert!(
        oxidant_execution::aqe::aqe_enabled(),
        "OXIDANT_AQE=1 set above; coalescing suggestion path must run during the query"
    );

    const N: i64 = 200;
    let query = "SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k";
    let single = Engine::new();
    single
        .register_batches("t", vec![make_batch(0, N)])
        .unwrap();
    let expected = rows(&single.sql(query).await.unwrap());

    let p0 = bind_port().await;
    let p1 = bind_port().await;
    let e0 = Arc::new(Engine::new());
    e0.register_batches("t", vec![make_batch(0, N / 2)])
        .unwrap();
    let e1 = Arc::new(Engine::new());
    e1.register_batches("t", vec![make_batch(N / 2, N)])
        .unwrap();
    tokio::spawn(async move {
        let _ = serve_worker(p0, e0).await;
    });
    tokio::spawn(async move {
        let _ = serve_worker(p1, e1).await;
    });

    // Raise planned modulus above worker count without process-global env (avoids test races).
    let mut cluster = Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ]);
    cluster.num_partitions = 4;

    let plan = DistributedPlan {
        partial_sql: "SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k".into(),
        final_sql: "SELECT k, SUM(c) AS c, SUM(s) AS s FROM shuffle_input GROUP BY k".into(),
        hash_key_cols: vec![0],
    };

    let mut actual = None;
    let mut last_err = None;
    for _ in 0..50 {
        match run_distributed(&cluster, &plan).await {
            Ok(b) => {
                actual = Some(rows(&b));
                break;
            }
            Err(e) => {
                last_err = Some(e.to_string());
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }

    let actual = actual
        .unwrap_or_else(|| panic!("distributed query never succeeded; last_err={last_err:?}"));
    assert_eq!(
        actual,
        expected,
        "AQE coalesce suggestion must not orphan shuffle buckets (got count sum={}, expected {N})",
        actual.iter().map(|(_, c, _)| c).sum::<i64>()
    );
}
