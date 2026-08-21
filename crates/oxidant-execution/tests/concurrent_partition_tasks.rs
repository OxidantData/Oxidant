//! F2: concurrent per-partition stage tasks on one worker.
//!
//! The driver dispatches all of a stage's partition tasks concurrently; the worker scopes
//! its shuffle-input MemTable registrations per task (`localize_shuffle_input_sql`), and
//! tasks beyond the slot count queue server-side (`acquire_task_slot`). Previously the
//! shared `shuffle_input` name forced the driver to serialize per-worker partition tasks
//! (a multi-second fixed tax on every multi-stage query at SF10), and a task arriving at
//! a full worker was rejected after a ~600ms retry window instead of queueing.

use std::sync::Arc;

use oxidant_execution::driver::{run_distributed, Cluster, DistributedPlan};
use oxidant_execution::flight::serve_worker;
use oxidant_loom::arrow::array::Int64Array;
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;

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

async fn bind_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// `OXIDANT_WORKER_TASK_SLOTS` is process-global; serialize the tests that mutate it.
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn start_single_worker(n: i64) -> (String, u16) {
    let port = bind_port().await;
    let engine = Arc::new(Engine::new());
    engine
        .register_batches("t", vec![make_batch(0, n)])
        .unwrap();
    tokio::spawn(async move {
        let _ = serve_worker(port, engine).await;
    });
    (format!("http://127.0.0.1:{port}"), port)
}

fn group_by_plan() -> DistributedPlan {
    DistributedPlan {
        partial_sql: "SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k".into(),
        final_sql: "SELECT k, SUM(c) AS c, SUM(s) AS s FROM shuffle_input GROUP BY k".into(),
        hash_key_cols: vec![0],
    }
}

async fn run_until_up(cluster: &Cluster, plan: &DistributedPlan) -> Vec<RecordBatch> {
    let mut last_err = None;
    for _ in 0..50 {
        match run_distributed(cluster, plan).await {
            Ok(batches) => return batches,
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
    panic!("cluster never came up: {last_err:?}");
}

/// Four output partitions on ONE worker with plenty of slots: all four combine tasks run
/// concurrently on that worker. With the old shared `shuffle_input` registration the
/// second task would overwrite the first's input mid-execution (and deregister it on
/// exit), surfacing as wrong rows or "table not found"; per-task localized names make
/// the result exactly the single-node answer.
#[tokio::test]
async fn concurrent_partition_tasks_on_one_worker_match_single_node() {
    let _env = ENV_LOCK.lock().await;
    std::env::set_var("OXIDANT_WORKER_TASK_SLOTS", "8");
    const N: i64 = 20_000;
    let plan = group_by_plan();

    let single = Engine::new();
    single
        .register_batches("t", vec![make_batch(0, N)])
        .unwrap();
    let expected = rows(
        &single
            .sql("SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k")
            .await
            .unwrap(),
    );

    let (ep, _port) = start_single_worker(N).await;
    let mut cluster = Cluster::new(vec![ep]);
    cluster.num_partitions = 4;
    let batches = run_until_up(&cluster, &plan).await;
    assert_eq!(
        rows(&batches),
        expected,
        "concurrent same-worker partition tasks must not corrupt each other's shuffle input"
    );
    std::env::remove_var("OXIDANT_WORKER_TASK_SLOTS");
}

/// One slot, three output partitions on ONE worker: the driver still dispatches all three
/// tasks at once (it no longer serializes per endpoint), so two must queue server-side
/// until the running task frees the slot. The run must complete with the exact answer —
/// a full worker rejecting (instead of queueing) would fail the task after the driver's
/// short retry window.
#[tokio::test]
async fn tasks_beyond_slot_count_queue_instead_of_rejecting() {
    let _env = ENV_LOCK.lock().await;
    std::env::set_var("OXIDANT_WORKER_TASK_SLOTS", "1");
    const N: i64 = 8_000;
    let plan = group_by_plan();

    let single = Engine::new();
    single
        .register_batches("t", vec![make_batch(0, N)])
        .unwrap();
    let expected = rows(
        &single
            .sql("SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k")
            .await
            .unwrap(),
    );

    let (ep, _port) = start_single_worker(N).await;
    let mut cluster = Cluster::new(vec![ep]);
    cluster.num_partitions = 3;
    let batches = run_until_up(&cluster, &plan).await;
    assert_eq!(
        rows(&batches),
        expected,
        "queued partition tasks must all complete with the full result"
    );
    std::env::remove_var("OXIDANT_WORKER_TASK_SLOTS");
}

/// F2 global-aggregate shape: no GROUP BY, so `hash_key_cols` is empty and every partial
/// row must land in bucket 0 — exactly one output partition combines to a single row.
/// Regression probe for the SF10 TPC-H Q6 anomaly (0/3 rows instead of 1).
#[tokio::test]
async fn concurrent_global_agg_returns_single_row() {
    let _env = ENV_LOCK.lock().await;
    std::env::set_var("OXIDANT_WORKER_TASK_SLOTS", "8");
    const N: i64 = 20_000;

    let single = Engine::new();
    single
        .register_batches("t", vec![make_batch(0, N)])
        .unwrap();
    let expected = single
        .sql("SELECT COUNT(*) AS c, SUM(v) AS s FROM t")
        .await
        .unwrap();

    let (ep, _port) = start_single_worker(N).await;
    let mut cluster = Cluster::new(vec![ep]);
    cluster.num_partitions = 4;
    let plan = DistributedPlan {
        partial_sql: "SELECT COUNT(*) AS c, SUM(v) AS s FROM t".into(),
        final_sql: "SELECT SUM(c) AS c, SUM(s) AS s FROM shuffle_input HAVING COUNT(*) > 0".into(),
        hash_key_cols: vec![],
    };
    let mut last = Vec::new();
    for attempt in 0..50 {
        match run_distributed(&cluster, &plan).await {
            Ok(batches) => {
                last = batches;
                break;
            }
            Err(e) => {
                if attempt == 49 {
                    panic!("cluster never came up: {e}");
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
    assert_eq!(
        last.iter().map(|b| b.num_rows()).sum::<usize>(),
        1,
        "global aggregate must return exactly one row, got {last:?}"
    );
    let got_c = last[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let got_s = last[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let exp_c = expected[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let exp_s = expected[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!((got_c, got_s), (exp_c, exp_s));
    std::env::remove_var("OXIDANT_WORKER_TASK_SLOTS");
}
