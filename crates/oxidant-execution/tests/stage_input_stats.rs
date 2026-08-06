//! KAN-2 A3: driver-measured stage-input statistics feed the worker's plan-time
//! join-strategy guard (Spark AQE's runtime SMJ→hash conversion). After each producer
//! stage the driver counts its output rows per bucket (cheap `bucket_row_counts` worker
//! action, default on via `OXIDANT_STAGE_INPUT_STATS`) and ships the per-bucket totals on the
//! consumer's `StageTicket`; the worker registers each `shuffle_input*` table with the
//! exact row count of the buckets its task pulls attached, so the KAN-53 `auto` join
//! selection sizes hash-join build sides from measured data:
//!
//! - a measured build that fits the pool budget keeps the hash join (no sort-merge
//!   reroute) — and the result still matches the single-node ground truth row-for-row;
//! - a measured build that genuinely does not fit still reroutes to sort-merge (the
//!   safety valve stays);
//! - `OXIDANT_STAGE_INPUT_STATS=0` restores the pre-A3 path: no measured counts on the
//!   ticket, plain MemTable registration on the worker.
//!
//! Workers run with bounded memory pools (`Engine::new_with_memory_limit`) because the
//! plan-time guard only engages with a budget to fit; the reroute and measured-stats
//! registration counters on `Engine` are the decision observability.

use std::sync::Arc;

use oxidant_execution::driver::{run_stages, Cluster, StageDef};
use oxidant_execution::flight::serve_worker;
use oxidant_loom::arrow::array::Int64Array;
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;

/// `OXIDANT_STAGE_INPUT_STATS` / `OXIDANT_TARGET_PARTITIONS` / `OXIDANT_BATCH_SIZE` are
/// process-global; serialize these tests.
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

/// (k, n, s, t) rows sorted by k — the [`LARGE_SQL`] shape.
fn rows4(batches: &[RecordBatch]) -> Vec<(i64, i64, i64, i64)> {
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

// Single-node ground truth and distributed share this join+aggregate (table names match the
// per-worker base tables single-node, and the registered shuffle inputs distributed).
const SINGLE_SQL: &str = "SELECT o.o_custkey AS k, COUNT(*) AS n, SUM(c.c_val) AS s \
     FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey GROUP BY o.o_custkey";

/// The large-build variant aggregates over BOTH value columns so DataFusion's join
/// projection cannot prune the build side to the join key — a keys-only build is the shape
/// DataFusion already handles well, and the guard's row-width estimate would (correctly)
/// come in at half the size.
const LARGE_SQL: &str = "SELECT o.o_custkey AS k, COUNT(*) AS n, SUM(c.c_val) AS s, \
     SUM(o.o_orderkey) AS t \
     FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey GROUP BY o.o_custkey";

/// The three-stage shuffle join: stages 0/1 hash-shuffle orders/customer onto the join key,
/// stage 2 joins the co-located buckets (its build side is an upstream stage output — the
/// KAN-2 A3 shape) and aggregates. `consumer_sql` is the stage-2 / single-node SQL.
fn join_stages(consumer_sql: &str) -> Vec<StageDef> {
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
            sql: consumer_sql.into(),
            upstream_stage_ids: vec![0, 1],
            hash_key_cols: vec![],
            ..StageDef::default()
        },
    ]
}

/// The stage-2 consumer SQL for a base-table query: same plan, reading the registered
/// shuffle inputs instead of `orders`/`customer`.
fn consumer_sql(base_sql: &str) -> String {
    base_sql
        .replace("FROM orders o", "FROM shuffle_input_0 o")
        .replace("JOIN customer c", "JOIN shuffle_input_1 c")
}

/// Start one in-process worker holding its half of both tables on a bounded-pool engine
/// (the plan-time join guard needs a budget to engage). Returns (endpoint, engine).
async fn start_worker(
    pool_bytes: usize,
    orders_batch: RecordBatch,
    customer_batch: RecordBatch,
) -> (String, Arc<Engine>) {
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    // Tight window: only the engine built here sees the small partition/batch knobs (keeps
    // per-batch pool reservations far below each operator's fair share of the small pool).
    std::env::set_var("OXIDANT_TARGET_PARTITIONS", "2");
    std::env::set_var("OXIDANT_BATCH_SIZE", "1024");
    let engine = Arc::new(Engine::new_with_memory_limit(pool_bytes));
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

/// Run the shuffle join over two workers, each holding half of both tables, and return the
/// raw result batches once the cluster is up.
async fn run_join(
    pool_bytes: usize,
    total_rows: i64,
    custs: i64,
    base_sql: &str,
) -> (Vec<RecordBatch>, Arc<Engine>, Arc<Engine>) {
    let (ep0, e0) = start_worker(
        pool_bytes,
        orders(0, total_rows / 2, custs),
        customer(0, custs / 2),
    )
    .await;
    let (ep1, e1) = start_worker(
        pool_bytes,
        orders(total_rows / 2, total_rows, custs),
        customer(custs / 2, custs),
    )
    .await;
    let cluster = Cluster::new(vec![ep0, ep1]);
    let stages = join_stages(&consumer_sql(base_sql));
    let mut actual = None;
    for _ in 0..50 {
        match run_stages(&cluster, &stages).await {
            Ok(b) => {
                actual = Some(b);
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    }
    (actual.expect("distributed join never succeeded"), e0, e1)
}

/// Single-node ground truth over the whole dataset.
async fn single_node(total_rows: i64, custs: i64, base_sql: &str) -> Vec<RecordBatch> {
    let single = Engine::new();
    single
        .register_batches("orders", vec![orders(0, total_rows, custs)])
        .unwrap();
    single
        .register_batches("customer", vec![customer(0, custs)])
        .unwrap();
    single.sql(base_sql).await.unwrap()
}

/// (a)+(b): with measured stage-input statistics (default on), the consumer stage's join
/// keeps its hash build when the measured build side fits the budget — no sort-merge
/// reroute — and the distributed result matches single-node.
#[tokio::test]
async fn measured_small_build_keeps_hash_join_and_matches_single_node() {
    let _guard = ENV_LOCK.lock().await;
    std::env::remove_var("OXIDANT_STAGE_INPUT_STATS");
    const CUSTS: i64 = 200;
    const ORDERS: i64 = 2_000;
    let expected = rows(&single_node(ORDERS, CUSTS, SINGLE_SQL).await);
    let (actual, e0, e1) = run_join(64 * 1024 * 1024, ORDERS, CUSTS, SINGLE_SQL).await;
    assert_eq!(
        rows(&actual),
        expected,
        "distributed result must equal single-node"
    );
    // Non-vacuity: the measured path actually engaged — two consumer tasks (np = 2
    // workers), each registering both shuffle inputs with measured row counts attached
    // (task retries would only add registrations).
    assert!(
        e0.measured_stats_registration_count() + e1.measured_stats_registration_count() >= 4,
        "workers must register shuffle inputs with measured statistics: {} + {}",
        e0.measured_stats_registration_count(),
        e1.measured_stats_registration_count()
    );
    assert_eq!(
        e0.plan_time_smj_reroute_count() + e1.plan_time_smj_reroute_count(),
        0,
        "a measured build side under the budget must keep the hash join on every task"
    );
}

/// (c): the safety valve stays — when the measured build side genuinely exceeds the pool
/// budget the consumer's join still reroutes to sort-merge, and the result still matches
/// single-node.
#[tokio::test]
async fn measured_large_build_still_reroutes_and_matches_single_node() {
    let _guard = ENV_LOCK.lock().await;
    std::env::remove_var("OXIDANT_STAGE_INPUT_STATS");
    // 64 MiB pool ⇒ 16 MiB build budget (0.25 fraction). np = 2 consumer tasks, so each
    // task's build side (the smaller of its two measured shuffle inputs) is ~1.2M rows
    // × 16 B ≈ 19 MB — over budget. 2.4M distinct custkeys keep the smaller side large,
    // and aggregating over both value columns keeps the build from being pruned to keys.
    const CUSTS: i64 = 2_400_000;
    const ORDERS: i64 = 2_400_000;
    let expected = rows4(&single_node(ORDERS, CUSTS, LARGE_SQL).await);
    let (actual, e0, e1) = run_join(64 * 1024 * 1024, ORDERS, CUSTS, LARGE_SQL).await;
    assert_eq!(
        rows4(&actual),
        expected,
        "rerouted sort-merge join must still equal single-node"
    );
    assert!(
        e0.measured_stats_registration_count() + e1.measured_stats_registration_count() >= 4,
        "workers must register shuffle inputs with measured statistics"
    );
    assert!(
        e0.plan_time_smj_reroute_count() + e1.plan_time_smj_reroute_count() > 0,
        "a measured over-budget build must reroute to sort-merge on at least one task"
    );
}

/// (d): `OXIDANT_STAGE_INPUT_STATS=0` restores the pre-A3 path — the driver ships no measured
/// counts, workers register plain MemTables — while results stay correct. (Under
/// DataFusion 54 the MemTable's own batch-derived statistics still reach the guard, so the
/// small-build join still avoids the reroute; the assertion that pins the old path is the
/// zero measured-stats registration count.)
#[tokio::test]
async fn stage_input_stats_disabled_restores_plain_registration() {
    let _guard = ENV_LOCK.lock().await;
    std::env::set_var("OXIDANT_STAGE_INPUT_STATS", "0");
    const CUSTS: i64 = 200;
    const ORDERS: i64 = 2_000;
    let expected = rows(&single_node(ORDERS, CUSTS, SINGLE_SQL).await);
    let (actual, e0, e1) = run_join(64 * 1024 * 1024, ORDERS, CUSTS, SINGLE_SQL).await;
    std::env::remove_var("OXIDANT_STAGE_INPUT_STATS");
    assert_eq!(
        rows(&actual),
        expected,
        "distributed result must equal single-node"
    );
    assert_eq!(
        e0.measured_stats_registration_count() + e1.measured_stats_registration_count(),
        0,
        "OXIDANT_STAGE_INPUT_STATS=0 must bypass measured-stats registration"
    );
    assert_eq!(
        e0.plan_time_smj_reroute_count() + e1.plan_time_smj_reroute_count(),
        0,
        "DF 54's MemTable statistics still cover the small build — no reroute"
    );
}
