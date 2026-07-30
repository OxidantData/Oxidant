//! KAN-25: a worker running a join whose build side exceeds its bounded memory pool must
//! complete (via the engine's sort-merge fallback) instead of wedging or OOM-ing.
//!
//! Each in-process worker gets a 64 MiB `FairSpillPool` and runs a stage SQL whose hash-join
//! build side (~74 MB of wide strings, kept on the build by the aggregates over both string
//! columns) provably does not fit. DataFusion 54's `HashJoinExec` registers its build side
//! with the pool but cannot spill, so without the KAN-25 guard the stage fails with
//! `Resources Exhausted`; with it, the worker falls back to a spill-capable sort-merge join
//! and returns correct results. KAN-45: the fallback is opt-in (`WEFT_SORT_MERGE_FALLBACK`)
//! because the sort-merge path can itself deadlock under a bounded pool at scale — this test
//! enables it explicitly.

use std::sync::Arc;

use weft_execution::driver::{run_distributed, Cluster, DistributedPlan};
use weft_execution::flight::serve_worker;
use weft_loom::arrow::array::{Int64Array, StringArray};
use weft_loom::arrow::datatypes::{DataType, Field, Schema};
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::Engine;

const LEFT_ROWS_PER_WORKER: i64 = 170_000;
const RIGHT_ROWS: i64 = 400_000;
const STR_WIDTH: usize = 400;

/// `(k, s)` batches with unique keys and a `STR_WIDTH`-char string column, in 1024-row
/// batches so a tight memory pool's per-batch reservations stay small (mirrors real scan
/// batching; a single giant batch would force one oversized `try_grow`).
fn wide_batches(start: i64, end: i64) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("s", DataType::Utf8, false),
    ]));
    let filler = "x".repeat(STR_WIDTH);
    let per = 1024;
    (start..end)
        .step_by(per as usize)
        .map(|b0| {
            let b1 = (b0 + per).min(end);
            let ks: Vec<i64> = (b0..b1).collect();
            let ss: Vec<&str> = (b0..b1).map(|_| filler.as_str()).collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ks)),
                    Arc::new(StringArray::from(ss)),
                ],
            )
            .unwrap()
        })
        .collect()
}

fn engine_with_tables(left: Vec<RecordBatch>, right: Vec<RecordBatch>) -> Arc<Engine> {
    // Tight window: only the engine built here sees the small batch/partition knobs (keeps
    // per-batch pool reservations far below each sorter's fair share of the small pool;
    // production pools have this headroom at any setting).
    std::env::set_var("WEFT_TARGET_PARTITIONS", "2");
    std::env::set_var("WEFT_BATCH_SIZE", "1024");
    let engine = Arc::new(Engine::new_with_memory_limit(64 * 1024 * 1024));
    std::env::remove_var("WEFT_TARGET_PARTITIONS");
    std::env::remove_var("WEFT_BATCH_SIZE");
    engine.register_batches("left_wide", left).unwrap();
    engine.register_batches("right_wide", right).unwrap();
    engine
}

fn key_counts(batches: &[RecordBatch]) -> Vec<(i64, i64, i64, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let cols: Vec<&Int64Array> = (0..4)
            .map(|i| b.column(i).as_any().downcast_ref::<Int64Array>().unwrap())
            .collect();
        for i in 0..b.num_rows() {
            out.push((
                cols[0].value(i),
                cols[1].value(i),
                cols[2].value(i),
                cols[3].value(i),
            ));
        }
    }
    out.sort();
    out
}

#[tokio::test]
async fn worker_join_guard_bounds_oversized_build() {
    // KAN-45: the sort-merge fallback is opt-in; enable it for this test (it exercises the
    // fallback path itself).
    std::env::set_var("WEFT_SORT_MERGE_FALLBACK", "true");
    let partial_sql = "SELECT l.k AS k, COUNT(*) AS c, SUM(length(l.s)) AS sl, \
                       SUM(length(r.s)) AS sr \
                       FROM left_wide l JOIN right_wide r ON l.k = r.k GROUP BY l.k";
    let final_sql =
        "SELECT k, SUM(c) AS c, SUM(sl) AS sl, SUM(sr) AS sr FROM shuffle_input GROUP BY k";

    // Expected: a single unbounded engine holding both left halves + the replicated right.
    let single = Engine::new();
    single
        .register_batches("left_wide", wide_batches(0, 2 * LEFT_ROWS_PER_WORKER))
        .unwrap();
    single
        .register_batches("right_wide", wide_batches(0, RIGHT_ROWS))
        .unwrap();
    let expected = key_counts(&single.sql(partial_sql).await.unwrap());

    let p0 = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let p1 = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };

    // 64 MiB pool per worker; the join's build side (~74 MB of string data) cannot fit, so
    // the KAN-25 guard must engage on each worker for the stage to complete at all.
    let e0 = engine_with_tables(
        wide_batches(0, LEFT_ROWS_PER_WORKER),
        wide_batches(0, RIGHT_ROWS),
    );
    let e1 = engine_with_tables(
        wide_batches(LEFT_ROWS_PER_WORKER, 2 * LEFT_ROWS_PER_WORKER),
        wide_batches(0, RIGHT_ROWS),
    );

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
        partial_sql: partial_sql.into(),
        final_sql: final_sql.into(),
        hash_key_cols: vec![0],
    };

    let mut actual = None;
    for _ in 0..50 {
        match run_distributed(&cluster, &plan).await {
            Ok(b) => {
                actual = Some(key_counts(&b));
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    }
    let actual = actual.expect("distributed join over an oversized build side never succeeded");
    assert_eq!(
        actual, expected,
        "workers must fall back to sort-merge under pool pressure and still match \
         single-node output"
    );
    std::env::remove_var("WEFT_SORT_MERGE_FALLBACK");
}

/// KAN-53: same oversized-build stage, but with the DEFAULT `auto` join selection — no
/// `WEFT_SORT_MERGE_FALLBACK` opt-in, no `WEFT_PREFER_HASH_JOIN` force. The engine must
/// still route the pool-exhausted hash build to sort-merge (here via the runtime retry)
/// and complete.
#[tokio::test]
async fn worker_join_guard_auto_selects_sort_merge() {
    std::env::remove_var("WEFT_PREFER_HASH_JOIN");
    std::env::remove_var("WEFT_SORT_MERGE_FALLBACK");
    let partial_sql = "SELECT l.k AS k, COUNT(*) AS c, SUM(length(l.s)) AS sl, \
                       SUM(length(r.s)) AS sr \
                       FROM left_wide l JOIN right_wide r ON l.k = r.k GROUP BY l.k";
    let final_sql =
        "SELECT k, SUM(c) AS c, SUM(sl) AS sl, SUM(sr) AS sr FROM shuffle_input GROUP BY k";

    let single = Engine::new();
    single
        .register_batches("left_wide", wide_batches(0, 2 * LEFT_ROWS_PER_WORKER))
        .unwrap();
    single
        .register_batches("right_wide", wide_batches(0, RIGHT_ROWS))
        .unwrap();
    let expected = key_counts(&single.sql(partial_sql).await.unwrap());

    let p0 = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let p1 = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };

    let e0 = engine_with_tables(
        wide_batches(0, LEFT_ROWS_PER_WORKER),
        wide_batches(0, RIGHT_ROWS),
    );
    let e1 = engine_with_tables(
        wide_batches(LEFT_ROWS_PER_WORKER, 2 * LEFT_ROWS_PER_WORKER),
        wide_batches(0, RIGHT_ROWS),
    );

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
        partial_sql: partial_sql.into(),
        final_sql: final_sql.into(),
        hash_key_cols: vec![0],
    };

    let mut actual = None;
    for _ in 0..50 {
        match run_distributed(&cluster, &plan).await {
            Ok(b) => {
                actual = Some(key_counts(&b));
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    }
    let actual =
        actual.expect("auto-mode distributed join over an oversized build side never succeeded");
    assert_eq!(
        actual, expected,
        "auto join selection must route workers to sort-merge under pool pressure and \
         still match single-node output"
    );
}
