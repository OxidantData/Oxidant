//! Dependency-aware concurrent stage dispatch (`WEFT_CONCURRENT_STAGES`, default off;
//! the tests opt in via `WEFT_CONCURRENT_STAGES=1`):
//! independent branch arms of a stage DAG are dispatched together — as soon as ALL of
//! their upstreams completed — instead of serializing behind the previous stage's
//! barrier. End-to-end against real in-process workers:
//!
//! 1. `independent_arms_overlap_and_match_single_node` — a Q58/Q78-shaped three-arm branch
//!    DAG (one sharded fact arm + two replicated `Forward` arms) matches single-node
//!    row-for-row, and the observability stream proves the arms overlapped: every arm's
//!    `TaskStarted` precedes the first arm `TaskFinished`. The dispatcher emits a ready
//!    wave's `TaskStarted` events synchronously before any of the wave's cold task futures
//!    is polled, so this ordering is a deterministic dispatch contract, not a wall-clock
//!    race.
//! 2. `sequential_dispatch_when_disabled` — `WEFT_CONCURRENT_STAGES=0` restores the legacy
//!    strictly-sequential dispatch: the same stream then shows exactly one arm started
//!    before the first finish.
//! 3. `failed_arm_skips_dependents_and_surfaces_error` — one arm's stage fails: its
//!    dependents are never dispatched (no `TaskStarted`), the stage's own error surfaces,
//!    and the query returns promptly instead of hanging on the surviving arm.

// ENV_LOCK serializes process-global `WEFT_CONCURRENT_STAGES` across async tests.
#![allow(clippy::await_holding_lock)]

use std::collections::HashSet;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};

use weft_execution::driver::{run_stages, run_stages_obs, Cluster, StageDef};
use weft_execution::flight::serve_worker;
use weft_execution::plan::{plan_distributed_logical, DistributedQuery};
use weft_loom::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use weft_loom::arrow::datatypes::{DataType, Field, Schema};
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::arrow::util::display::{ArrayFormatter, FormatOptions};
use weft_loom::Engine;
use weft_observability::{AppStateStore, ExecutionEvent};

/// Serialize port allocation across tests in this binary (same rationale as
/// `tests/auto_distribute.rs`: bind/drop races steal ports under parallel tests).
static PORT: std::sync::OnceLock<AtomicU16> = std::sync::OnceLock::new();

fn unique_worker_port() -> u16 {
    // OnceLock-seeded allocator with the base BELOW the Linux ephemeral source range
    // (32768..=60999): the harness's own outbound connections can never steal a worker's
    // port (serve_worker swallows EADDRINUSE; the old in-range bases flaked "did not
    // bind" / "distributed run never succeeded" on loaded CI runners).
    PORT.get_or_init(|| AtomicU16::new(16000 + (std::process::id() as u16 % 512)))
        .fetch_add(1, Ordering::Relaxed)
}

/// `WEFT_CONCURRENT_STAGES` is process-global; serialize every test in this binary for
/// its whole duration (not only the ones that mutate it).
static ENV_LOCK: Mutex<()> = Mutex::new(());

fn i64f(name: &str) -> Field {
    Field::new(name, DataType::Int64, false)
}
fn f64f(name: &str) -> Field {
    Field::new(name, DataType::Float64, false)
}
fn utf8f(name: &str) -> Field {
    Field::new(name, DataType::Utf8, false)
}

fn batch(fields: Vec<Field>, cols: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
}

/// Replicated `item` dimension: sk 1..=4 → id 'a'..'d'.
fn item() -> RecordBatch {
    batch(
        vec![i64f("i_item_sk"), utf8f("i_item_id")],
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
        ],
    )
}

/// Sales rows `(item_sk, customer_sk, yr, price)`. Item 1 and customer 1 span both shards.
fn sales(rows: &[(i64, i64, i64, f64)], price_col: &str) -> RecordBatch {
    batch(
        vec![
            i64f("item_sk"),
            i64f("customer_sk"),
            i64f("yr"),
            f64f(price_col),
        ],
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.3).collect::<Vec<_>>(),
            )),
        ],
    )
}

fn store0() -> RecordBatch {
    sales(
        &[(1, 1, 2001, 100.0), (2, 2, 2001, 50.0), (3, 3, 2002, 10.0)],
        "ss_price",
    )
}
fn store1() -> RecordBatch {
    sales(
        &[(1, 1, 2002, 300.0), (2, 4, 2002, 70.0), (4, 5, 2001, 20.0)],
        "ss_price",
    )
}
fn catalog() -> RecordBatch {
    sales(
        &[(1, 1, 2001, 400.0), (2, 2, 2001, 500.0), (3, 9, 2001, 10.0)],
        "cs_price",
    )
}
fn web() -> RecordBatch {
    sales(
        &[(1, 1, 2001, 420.0), (2, 2, 2001, 490.0), (4, 9, 2001, 10.0)],
        "ws_price",
    )
}

/// Planner/ground-truth engine holding the full dataset.
fn full_engine() -> Engine {
    let e = Engine::new();
    e.register_batches("item", vec![item()]).unwrap();
    e.register_batches("store_sales", vec![store0(), store1()])
        .unwrap();
    e.register_batches("catalog_sales", vec![catalog()])
        .unwrap();
    e.register_batches("web_sales", vec![web()]).unwrap();
    e
}

/// `store_sales` sharded row-wise over two workers; everything else fully replicated (the
/// auto-broadcast layout at SF10: largest scanned table sharded, the rest replicated).
async fn two_workers() -> Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        e.register_batches("item", vec![item()]).unwrap();
        let shard = if i == 0 { store0() } else { store1() };
        e.register_batches("store_sales", vec![shard]).unwrap();
        e.register_batches("catalog_sales", vec![catalog()])
            .unwrap();
        e.register_batches("web_sales", vec![web()]).unwrap();
        tokio::spawn(async move {
            let _ = serve_worker(port, e).await;
        });
    }
    Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ])
}

const REPLICATED: [&str; 3] = ["item", "catalog_sales", "web_sales"];

/// Q58's shape, minimized: three per-item revenue aggregates — one over the sharded fact,
/// two over replicated facts — inner-joined on the item id. Same branch-DAG class as
/// TPC-DS Q4/Q61/Q78's independent fact arms.
const Q58_SHAPE: &str = "
WITH ss_items AS (
    SELECT i_item_id AS item_id, sum(ss_price) AS ss_rev
    FROM store_sales JOIN item ON store_sales.item_sk = item.i_item_sk GROUP BY i_item_id
),
cs_items AS (
    SELECT i_item_id AS item_id, sum(cs_price) AS cs_rev
    FROM catalog_sales JOIN item ON catalog_sales.item_sk = item.i_item_sk GROUP BY i_item_id
),
ws_items AS (
    SELECT i_item_id AS item_id, sum(ws_price) AS ws_rev
    FROM web_sales JOIN item ON web_sales.item_sk = item.i_item_sk GROUP BY i_item_id
)
SELECT ss_items.item_id, ss_rev, cs_rev, ws_rev
FROM ss_items, cs_items, ws_items
WHERE ss_items.item_id = cs_items.item_id
  AND ss_items.item_id = ws_items.item_id
  AND ss_rev BETWEEN 0.9 * cs_rev AND 1.1 * cs_rev
  AND ss_rev BETWEEN 0.9 * ws_rev AND 1.1 * ws_rev
ORDER BY ss_items.item_id";

/// Sorted value rows, mirroring the bench's `normalize_batches`.
fn rows_sorted(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let opts = FormatOptions::default().with_null("NULL");
    let mut rows = Vec::new();
    for b in batches {
        let fmts: Vec<_> = b
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c, &opts).unwrap())
            .collect();
        for r in 0..b.num_rows() {
            rows.push(
                fmts.iter()
                    .map(|f| f.value(r).to_string())
                    .collect::<Vec<_>>(),
            );
        }
    }
    rows.sort();
    rows
}

/// Plan `sql` against the full engine and return the distributed query plan.
async fn plan(planner: &Engine, sql: &str) -> DistributedQuery {
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    plan_distributed_logical(&lp, &REPLICATED).expect("plan_distributed_logical")
}

/// Run `dq`'s stages on `cluster` through `run_stages_obs` (so the store sees the query),
/// applying the driver's global finalize. Retries cover the worker-startup race only; the
/// query itself must succeed unchanged.
async fn run_distributed_observed(
    cluster: &Cluster,
    dq: &DistributedQuery,
    store: &Arc<AppStateStore>,
) -> Vec<RecordBatch> {
    let mut out = None;
    for _ in 0..150 {
        match run_stages_obs(
            cluster,
            &dq.stages,
            Some(store.clone()),
            Some("concurrent-stages".into()),
            None,
        )
        .await
        {
            Ok(b) => {
                out = Some(b);
                break;
            }
            Err(e) => {
                eprintln!("run_stages_obs err: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
    let gathered = out.expect("distributed run never succeeded");
    match &dq.finalize_sql {
        None => gathered,
        Some(fsql) => {
            let fin = Engine::new();
            fin.register_batches("result", gathered).unwrap();
            fin.sql(fsql).await.expect("finalize")
        }
    }
}

/// Subscribe a draining collector to `store`'s event stream; returns the collected events
/// (the collector keeps up with the broadcast channel, so event order is exactly the
/// driver's emission order).
fn collect_events(store: &Arc<AppStateStore>) -> Arc<Mutex<Vec<ExecutionEvent>>> {
    let mut rx = store.subscribe();
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_c = events.clone();
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(ev) => events_c.lock().expect("events poisoned").push(ev),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    panic!("event collector lagged by {n}: assertion would be unsound")
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });
    events
}

/// Distinct stage ids among `arms` that emitted a `TaskStarted` before the first arm
/// `TaskFinished` — the overlap witness: >1 means independent stages were in flight
/// together, ==1 means strict stage-at-a-time dispatch.
fn stages_started_before_first_arm_finish(
    events: &[ExecutionEvent],
    arms: &HashSet<i32>,
) -> HashSet<i32> {
    let first_finish = events
        .iter()
        .position(|e| {
            matches!(e, ExecutionEvent::TaskFinished { stage_id, .. } if arms.contains(stage_id))
        })
        .expect("an arm stage must have finished");
    events[..first_finish]
        .iter()
        .filter_map(|e| match e {
            ExecutionEvent::TaskStarted { stage_id, .. } if arms.contains(stage_id) => {
                Some(*stage_id)
            }
            _ => None,
        })
        .collect()
}

/// Zero-upstream (arm) stage ids of the plan, as i32 for event matching.
fn arm_stage_ids(dq: &DistributedQuery) -> HashSet<i32> {
    dq.stages
        .iter()
        .filter(|s| s.upstream_stage_ids.is_empty())
        .map(|s| s.stage_id as i32)
        .collect()
}

#[tokio::test]
async fn independent_arms_overlap_and_match_single_node() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("WEFT_CONCURRENT_STAGES", "1");
    let planner = full_engine();
    let expected = rows_sorted(&planner.sql(Q58_SHAPE).await.unwrap());
    let dq = plan(&planner, Q58_SHAPE).await;
    let arms = arm_stage_ids(&dq);
    assert!(
        arms.len() >= 2,
        "the branch plan must have at least two independent arm stages: {:?}",
        dq.stages
            .iter()
            .map(|s| (s.stage_id, s.upstream_stage_ids.clone()))
            .collect::<Vec<_>>()
    );

    let cluster = two_workers().await;
    let store = Arc::new(AppStateStore::new());
    let events = collect_events(&store);
    let actual = run_distributed_observed(&cluster, &dq, &store).await;
    assert_eq!(
        rows_sorted(&actual),
        expected,
        "concurrent dispatch must equal single-node row-for-row"
    );

    let events = events.lock().expect("events poisoned");
    let overlapped = stages_started_before_first_arm_finish(&events, &arms);
    assert!(
        overlapped.len() >= 2,
        "at least two independent arm stages must have started before any arm finished \
         (deterministic dispatch contract: a ready wave's TaskStarted events are all \
         emitted before the wave's tasks are polled); arms={arms:?}, events={events:?}"
    );
}

#[tokio::test]
async fn sequential_dispatch_when_disabled() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("WEFT_CONCURRENT_STAGES", "0");
    let planner = full_engine();
    let expected = rows_sorted(&planner.sql(Q58_SHAPE).await.unwrap());
    let dq = plan(&planner, Q58_SHAPE).await;
    let arms = arm_stage_ids(&dq);

    let cluster = two_workers().await;
    let store = Arc::new(AppStateStore::new());
    let events = collect_events(&store);
    let actual = run_distributed_observed(&cluster, &dq, &store).await;
    std::env::remove_var("WEFT_CONCURRENT_STAGES");
    assert_eq!(
        rows_sorted(&actual),
        expected,
        "the sequential fallback must still equal single-node"
    );

    let events = events.lock().expect("events poisoned");
    let first_wave = stages_started_before_first_arm_finish(&events, &arms);
    assert_eq!(
        first_wave.len(),
        1,
        "WEFT_CONCURRENT_STAGES=0 must dispatch one stage at a time; events={events:?}"
    );
}

#[tokio::test]
async fn failed_arm_skips_dependents_and_surfaces_error() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("WEFT_CONCURRENT_STAGES", "1");
    let cluster = two_workers().await;

    // Warm the workers up with a good query first (retry loop covers the startup race), so
    // the failure below is the bad stage's own error, never a connect error.
    let good = StageDef::new(
        0,
        "SELECT item_sk, ss_price FROM store_sales",
        vec![],
        vec![0],
    );
    let warmup = vec![
        good.clone(),
        StageDef::new(
            1,
            "SELECT item_sk, SUM(ss_price) AS s FROM shuffle_input GROUP BY item_sk",
            vec![0],
            vec![],
        ),
    ];
    let mut up = false;
    for _ in 0..150 {
        if run_stages(&cluster, &warmup).await.is_ok() {
            up = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(up, "workers never came up");

    // Branch DAG: good leaf 0 and bad leaf 1 (references a table no worker has) are
    // independent arms; intermediate 2 consumes both; output 3 consumes 2.
    let stages = vec![
        good,
        StageDef::new(1, "SELECT missing_col FROM no_such_table", vec![], vec![0]),
        StageDef::new(
            2,
            "SELECT item_sk, ss_price FROM shuffle_input_0",
            vec![0, 1],
            vec![0],
        ),
        StageDef::new(
            3,
            "SELECT item_sk, SUM(ss_price) AS s FROM shuffle_input GROUP BY item_sk",
            vec![2],
            vec![],
        ),
    ];
    let store = Arc::new(AppStateStore::new());
    let events = collect_events(&store);
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(120),
        run_stages_obs(
            &cluster,
            &stages,
            Some(store.clone()),
            Some("concurrent-stages-failure".into()),
            None,
        ),
    )
    .await;
    let err = result
        .expect("the query must return promptly, not hang")
        .expect_err("a query with a failing arm must fail");
    assert!(
        err.to_string().contains("no_such_table"),
        "the bad stage's own error must surface, got: {err}"
    );

    let events = events.lock().expect("events poisoned");
    let started: HashSet<i32> = events
        .iter()
        .filter_map(|e| match e {
            ExecutionEvent::TaskStarted { stage_id, .. } => Some(*stage_id),
            _ => None,
        })
        .collect();
    assert!(
        started.contains(&1),
        "the failing arm must have been dispatched: {events:?}"
    );
    assert!(
        !started.contains(&2) && !started.contains(&3),
        "dependents of the failed arm must be skipped (never dispatched): {events:?}"
    );
    let failed_arm_finished = events.iter().any(|e| {
        matches!(
            e,
            ExecutionEvent::TaskFinished {
                stage_id: 1,
                status: weft_observability::TaskStatus::Failed,
                ..
            }
        )
    });
    assert!(
        failed_arm_finished,
        "the failing arm's task must be attributed Failed in the stream: {events:?}"
    );
}
