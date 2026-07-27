//! Partial (threshold) spill through a real distributed shuffle: one worker stays in memory,
//! the other spills to disk — the SF100 skew shape — and results must match a no-spill run.
//!
//! Note: [`BucketCache`] is all-or-nothing *per worker stage* (not per bucket). The mixture we
//! can assert is cross-worker: Memory on the small shard + Spilled on the large shard. Append-
//! after-spill is covered by unit tests and `do_exchange_streams_large_partition_under_memory_budget`.

use std::sync::Arc;

use weft_execution::driver::{run_distributed, Cluster, DistributedPlan};
use weft_execution::flight::serve_worker_with_spill;
use weft_execution::shuffle::spill::{estimated_batch_bytes, SpillStore};
use weft_loom::arrow::array::Int64Array;
use weft_loom::arrow::datatypes::{DataType, Field, Schema};
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::Engine;

fn make_batch(start: i64, end: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
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

fn spill_file_count(dir: &std::path::Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .filter(|e| {
            e.path()
                .file_name()
                .and_then(|s| s.to_str())
                .is_some_and(|s| s.starts_with("stage_") && s.ends_with(".arrow"))
        })
        .count()
}

async fn bind_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

async fn run_groupby(cluster: &Cluster) -> Vec<(i64, i64, i64)> {
    let plan = DistributedPlan {
        partial_sql: "SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k".into(),
        final_sql: "SELECT k, SUM(c) AS c, SUM(s) AS s FROM shuffle_input GROUP BY k".into(),
        hash_key_cols: vec![0],
    };
    let mut last = None;
    for _ in 0..50 {
        match run_distributed(cluster, &plan).await {
            Ok(b) => return rows(&b),
            Err(e) => {
                last = Some(e.to_string());
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
    panic!("distributed query never succeeded; last={last:?}");
}

#[tokio::test]
async fn skewed_workers_partial_spill_matches_no_spill() {
    // Skew: small shard stays under threshold (Memory); large shard crosses it (Spilled).
    const SMALL_N: i64 = 200;
    const LARGE_N: i64 = 20_000;
    let small = make_batch(0, SMALL_N);
    let large = make_batch(SMALL_N, SMALL_N + LARGE_N);
    let small_bytes = estimated_batch_bytes(std::slice::from_ref(&small));
    let large_bytes = estimated_batch_bytes(std::slice::from_ref(&large));
    assert!(
        large_bytes > small_bytes.saturating_mul(8),
        "skew must be large enough to place a threshold between shards \
         (small={small_bytes}, large={large_bytes})"
    );
    // Threshold above the small producer's cache, below the large one.
    let threshold = small_bytes.saturating_mul(2).max(small_bytes + 1);
    assert!(
        threshold < large_bytes,
        "threshold {threshold} must be < large shard {large_bytes}"
    );

    let total_end = SMALL_N + LARGE_N;
    let query = "SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k";
    let single = Engine::new();
    single
        .register_batches("t", vec![make_batch(0, total_end)])
        .unwrap();
    let expected = rows(&single.sql(query).await.unwrap());

    // --- Baseline: both workers, no spill store ---
    let p0 = bind_port().await;
    let p1 = bind_port().await;
    let e0 = Arc::new(Engine::new());
    e0.register_batches("t", vec![small.clone()]).unwrap();
    let e1 = Arc::new(Engine::new());
    e1.register_batches("t", vec![large.clone()]).unwrap();
    // serve_worker uses SpillStore::from_env() — ensure clean.
    std::env::remove_var("WEFT_SHUFFLE_SPILL_DIR");
    std::env::remove_var("WEFT_SHUFFLE_SPILL_BYTES");
    std::env::remove_var("WEFT_MEMORY_LIMIT_BYTES");
    tokio::spawn({
        let e0 = e0.clone();
        async move {
            let _ = weft_execution::flight::serve_worker(p0, e0).await;
        }
    });
    tokio::spawn({
        let e1 = e1.clone();
        async move {
            let _ = weft_execution::flight::serve_worker(p1, e1).await;
        }
    });
    let baseline_cluster = Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ]);
    let baseline = run_groupby(&baseline_cluster).await;
    assert_eq!(
        baseline, expected,
        "no-spill distributed must match single-node"
    );

    // --- Mixed: explicit per-worker SpillStore budgets, keep files for assertion ---
    let base = std::env::temp_dir().join(format!(
        "weft-partial-spill-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let small_root = base.join("small");
    let large_root = base.join("large");
    // Small worker: astronomical limit → stays Memory (no spill files).
    let spill_small = SpillStore::with_memory_limit(&small_root, usize::MAX / 4).unwrap();
    // Large worker: threshold between shard sizes → Spilled.
    let spill_large = SpillStore::with_memory_limit(&large_root, threshold).unwrap();

    let q0 = bind_port().await;
    let q1 = bind_port().await;
    let f0 = Arc::new(Engine::new());
    f0.register_batches("t", vec![small]).unwrap();
    let f1 = Arc::new(Engine::new());
    f1.register_batches("t", vec![large]).unwrap();
    tokio::spawn(async move {
        let _ = serve_worker_with_spill(q0, f0, spill_small, true).await;
    });
    tokio::spawn(async move {
        let _ = serve_worker_with_spill(q1, f1, spill_large, true).await;
    });

    let mixed_cluster = Cluster::new(vec![
        format!("http://127.0.0.1:{q0}"),
        format!("http://127.0.0.1:{q1}"),
    ]);
    let mixed = run_groupby(&mixed_cluster).await;

    let small_files = spill_file_count(&small_root);
    let large_files = spill_file_count(&large_root);
    let _ = std::fs::remove_dir_all(&base);

    assert_eq!(
        mixed, baseline,
        "partial-spill distributed result must equal no-spill run"
    );
    assert_eq!(
        small_files, 0,
        "small worker must stay Memory (no spill files); got {small_files} — otherwise this \
         degrades into all-spill coverage we already have"
    );
    assert!(
        large_files > 0,
        "large worker must spill to disk (got {large_files} files) — otherwise this degrades \
         into no-spill coverage"
    );
}
