//! R5-4 (KAN-2): the worker-side stage plan cache plans a stage **once per worker**, not
//! once per task (`oxidant_loom::stage_plan_cache`). A stage's tasks share one cached logical
//! plan template; a hit rebinds the template's `shuffle_input*` scans to the hitting task's
//! registered providers — carrying that task's measured row totals (KAN-2 A3) — so the
//! per-task optimize + physical planning sizes joins from per-task statistics exactly as an
//! uncached plan would. These tests pin, over a two-worker in-process shuffle join:
//!
//! - every task of a repeated run hits the cached template (zero additional builds) and the
//!   distributed result still matches the single-node ground truth row-for-row;
//! - a re-pinned lakehouse snapshot (KAN-48) and a base-table re-registration both miss —
//!   the key's staleness guards — with results still correct;
//! - `OXIDANT_STAGE_PLAN_CACHE_ENTRIES=0` restores the pre-cache path (plan every task).
//!
//! The cache is process-global; each test's workers are fresh engines, and the engine id is
//! a key component, so tests never see each other's entries. `OXIDANT_STAGE_PLAN_CACHE_ENTRIES`
//! is process-global too, so the tests serialize on `ENV_LOCK`.

use std::sync::Arc;

use oxidant_execution::driver::{run_stages, Cluster, StageDef};
use oxidant_execution::flight::serve_worker;
use oxidant_loom::arrow::array::Int64Array;
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::stage_plan_cache::{global as plan_cache, StagePlanCacheStats};
use oxidant_loom::Engine;

/// `OXIDANT_STAGE_PLAN_CACHE_ENTRIES` is process-global; serialize these tests.
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// orders(o_orderkey, o_custkey) for orderkeys in `[start, end)`, custkey = orderkey % `custs`.
fn orders(start: i64, end: i64, custs: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
    ]));
    let ok: Vec<i64> = (start..end).collect();
    let ck: Vec<i64> = (start..end).map(|i| i % custs).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ok)),
            Arc::new(Int64Array::from(ck)),
        ],
    )
    .unwrap()
}

/// customer(c_custkey, c_val) for custkeys in `[start, end)`, c_val = custkey * 10.
fn customer(start: i64, end: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_val", DataType::Int64, false),
    ]));
    let ck: Vec<i64> = (start..end).collect();
    let cv: Vec<i64> = (start..end).map(|i| i * 10).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ck)),
            Arc::new(Int64Array::from(cv)),
        ],
    )
    .unwrap()
}

/// (k, n, s) rows sorted by k, for order-insensitive comparison.
fn rows(batches: &[RecordBatch]) -> Vec<(i64, i64, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let n = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let s = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((k.value(i), n.value(i), s.value(i)));
        }
    }
    out.sort();
    out
}

// Single-node ground truth and distributed share this join+aggregate.
const SINGLE_SQL: &str = "SELECT o.o_custkey AS k, COUNT(*) AS n, SUM(c.c_val) AS s \
     FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey GROUP BY o.o_custkey";

/// The three-stage shuffle join: stages 0/1 hash-shuffle orders/customer onto the join key,
/// stage 2 joins the co-located buckets and aggregates.
fn join_stages() -> Vec<StageDef> {
    vec![
        StageDef {
            stage_id: 0,
            sql: "SELECT o_orderkey, o_custkey FROM orders".into(),
            upstream_stage_ids: vec![],
            hash_key_cols: vec![1],
            ..StageDef::default()
        },
        StageDef {
            stage_id: 1,
            sql: "SELECT c_custkey, c_val FROM customer".into(),
            upstream_stage_ids: vec![],
            hash_key_cols: vec![0],
            ..StageDef::default()
        },
        StageDef {
            stage_id: 2,
            sql: SINGLE_SQL
                .replace("FROM orders o", "FROM shuffle_input_0 o")
                .replace("JOIN customer c", "JOIN shuffle_input_1 c"),
            upstream_stage_ids: vec![0, 1],
            hash_key_cols: vec![],
            ..StageDef::default()
        },
    ]
}

/// One in-process worker holding its share of both tables on a bounded-pool engine (the
/// pool makes the KAN-25 budget branch of `sql_stream` run under the cache, the production
/// shape). Returns (endpoint, engine).
async fn start_worker(
    orders_batch: RecordBatch,
    customer_batch: RecordBatch,
) -> (String, Arc<Engine>) {
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    std::env::set_var("OXIDANT_TARGET_PARTITIONS", "2");
    std::env::set_var("OXIDANT_BATCH_SIZE", "1024");
    let engine = Arc::new(Engine::new_with_memory_limit(64 * 1024 * 1024));
    std::env::remove_var("OXIDANT_TARGET_PARTITIONS");
    std::env::remove_var("OXIDANT_BATCH_SIZE");
    engine
        .register_batches("orders", vec![orders_batch])
        .unwrap();
    engine
        .register_batches("customer", vec![customer_batch])
        .unwrap();
    let worker = engine.clone();
    tokio::spawn(async move {
        let _ = serve_worker(port, worker).await;
    });
    (format!("http://127.0.0.1:{port}"), engine)
}

/// Two workers, each holding half of both tables.
async fn two_worker_cluster(total_rows: i64, custs: i64) -> (Cluster, Arc<Engine>, Arc<Engine>) {
    let (ep0, e0) = start_worker(orders(0, total_rows / 2, custs), customer(0, custs / 2)).await;
    let (ep1, e1) = start_worker(
        orders(total_rows / 2, total_rows, custs),
        customer(custs / 2, custs),
    )
    .await;
    (Cluster::new(vec![ep0, ep1]), e0, e1)
}

/// Run the stages, retrying while the freshly spawned workers come up.
async fn run(cluster: &Cluster, stages: &[StageDef]) -> Vec<RecordBatch> {
    for _ in 0..50 {
        match run_stages(cluster, stages).await {
            Ok(batches) => return batches,
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    }
    panic!("distributed stages never succeeded")
}

/// Single-node ground truth over the whole dataset.
async fn single_node(total_rows: i64, custs: i64) -> Vec<RecordBatch> {
    let single = Engine::new();
    single
        .register_batches("orders", vec![orders(0, total_rows, custs)])
        .unwrap();
    single
        .register_batches("customer", vec![customer(0, custs)])
        .unwrap();
    single.sql(SINGLE_SQL).await.unwrap()
}

fn stats_delta(before: &StagePlanCacheStats, after: &StagePlanCacheStats) -> (u64, u64, u64) {
    (
        after.builds - before.builds,
        after.hits - before.hits,
        after.misses - before.misses,
    )
}

/// A repeated run of the same stages is planned entirely from the cache: zero additional
/// builds, one hit per task — and the cached plans produce the single-node result.
#[tokio::test]
async fn same_stage_tasks_share_one_cached_plan_and_match_single_node() {
    let _guard = ENV_LOCK.lock().await;
    std::env::remove_var("OXIDANT_STAGE_PLAN_CACHE_ENTRIES");
    const CUSTS: i64 = 200;
    const ORDERS: i64 = 2_000;
    let expected = rows(&single_node(ORDERS, CUSTS).await);
    let (cluster, e0, e1) = two_worker_cluster(ORDERS, CUSTS).await;
    let stages = join_stages();

    let before = plan_cache().stats();
    assert_eq!(rows(&run(&cluster, &stages).await), expected);
    let after_first = plan_cache().stats();
    let (builds1, hits1, _) = stats_delta(&before, &after_first);
    assert!(
        builds1 > 0,
        "the first run must plan each stage once per worker"
    );

    // Second run, identical stages: every task hits a template built by the first.
    assert_eq!(rows(&run(&cluster, &stages).await), expected);
    let after_second = plan_cache().stats();
    let (builds2, hits2, misses2) = stats_delta(&after_first, &after_second);
    assert_eq!(
        builds2,
        0,
        "a repeated run must build nothing: {stats:?}",
        stats = after_second
    );
    assert_eq!(misses2, 0, "a repeated run must miss nothing");
    assert_eq!(
        hits2,
        builds1 + hits1,
        "every task of the repeated run must hit (same task count as the first run)"
    );

    // Non-vacuity: the KAN-2 A3 measured-stats path ran alongside the cache — the
    // consumer tasks registered their shuffle inputs with per-task measured row totals,
    // which the hit-path rebind (not the cache key) carries into physical planning.
    assert!(
        e0.measured_stats_registration_count() + e1.measured_stats_registration_count() >= 4,
        "measured stage-input statistics must engage"
    );
}

/// A re-pinned lakehouse snapshot is a different cache key (KAN-48): the same stage SQL
/// under new pins plans fresh instead of serving the pinned snapshot's template.
#[tokio::test]
async fn repinned_snapshot_misses_and_stays_correct() {
    let _guard = ENV_LOCK.lock().await;
    std::env::remove_var("OXIDANT_STAGE_PLAN_CACHE_ENTRIES");
    const CUSTS: i64 = 200;
    const ORDERS: i64 = 2_000;
    let expected = rows(&single_node(ORDERS, CUSTS).await);
    let (cluster, _, _) = two_worker_cluster(ORDERS, CUSTS).await;
    let stages = join_stages();

    let before = plan_cache().stats();
    assert_eq!(rows(&run(&cluster, &stages).await), expected);
    let after_first = plan_cache().stats();
    let (builds1, _, _) = stats_delta(&before, &after_first);

    // The pins ride the ticket into the key; a changed pin (here a synthetic re-pin — no
    // lakehouse tables are referenced, so it is inert for planning) must miss everywhere.
    let repinned: Vec<StageDef> = join_stages()
        .into_iter()
        .map(|s| StageDef {
            lakehouse_snapshot_pins: r#"{"prod.db.t":{"format":"delta","version":8}}"#.into(),
            ..s
        })
        .collect();
    assert_eq!(rows(&run(&cluster, &repinned).await), expected);
    let after_second = plan_cache().stats();
    let (builds2, _, _) = stats_delta(&after_first, &after_second);
    assert_eq!(
        builds2, builds1,
        "a re-pinned snapshot must re-plan every stage, not hit the old template"
    );

    // A third run under the SAME pins hits again (the pin is a key, not a cache flush).
    assert_eq!(rows(&run(&cluster, &repinned).await), expected);
    let after_third = plan_cache().stats();
    let (builds3, _, _) = stats_delta(&after_second, &after_third);
    assert_eq!(builds3, 0, "same pins must hit the re-pinned templates");
}

/// A base-table re-registration bumps the engine's catalog version: templates built before
/// it are never served against the new provider.
#[tokio::test]
async fn base_table_reregistration_misses_and_stays_correct() {
    let _guard = ENV_LOCK.lock().await;
    std::env::remove_var("OXIDANT_STAGE_PLAN_CACHE_ENTRIES");
    const CUSTS: i64 = 200;
    const ORDERS: i64 = 2_000;
    let expected = rows(&single_node(ORDERS, CUSTS).await);
    let (cluster, e0, e1) = two_worker_cluster(ORDERS, CUSTS).await;
    let stages = join_stages();

    let before = plan_cache().stats();
    assert_eq!(rows(&run(&cluster, &stages).await), expected);
    let after_first = plan_cache().stats();
    let (builds1, _, _) = stats_delta(&before, &after_first);

    // Re-register a base table on both workers (same contents — the point is the
    // invalidation, and the result must stay identical).
    e0.register_batches("orders", vec![orders(0, ORDERS / 2, CUSTS)])
        .unwrap();
    e1.register_batches("orders", vec![orders(ORDERS / 2, ORDERS, CUSTS)])
        .unwrap();
    assert_eq!(rows(&run(&cluster, &stages).await), expected);
    let after_second = plan_cache().stats();
    let (builds2, _, _) = stats_delta(&after_first, &after_second);
    assert_eq!(
        builds2, builds1,
        "a base-table re-registration must re-plan every stage"
    );
}

/// `OXIDANT_STAGE_PLAN_CACHE_ENTRIES=0` disables the cache: every task plans fresh (no builds,
/// no hits) and results are unchanged.
#[tokio::test]
async fn disabled_cache_plans_every_task() {
    let _guard = ENV_LOCK.lock().await;
    std::env::set_var("OXIDANT_STAGE_PLAN_CACHE_ENTRIES", "0");
    const CUSTS: i64 = 200;
    const ORDERS: i64 = 2_000;
    let expected = rows(&single_node(ORDERS, CUSTS).await);
    let (cluster, _, _) = two_worker_cluster(ORDERS, CUSTS).await;
    let stages = join_stages();

    let before = plan_cache().stats();
    assert_eq!(rows(&run(&cluster, &stages).await), expected);
    assert_eq!(rows(&run(&cluster, &stages).await), expected);
    let after = plan_cache().stats();
    std::env::remove_var("OXIDANT_STAGE_PLAN_CACHE_ENTRIES");
    let (builds, hits, misses) = stats_delta(&before, &after);
    assert_eq!(
        (builds, hits, misses),
        (0, 0, 0),
        "a disabled cache must not even look up"
    );
}
