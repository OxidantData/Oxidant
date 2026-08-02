//! KAN-55: distributed shapes for subqueries over sharded facts (TPC-DS Q9/Q10/Q16/Q35/Q69/Q94
//! at the SF10 strict configuration, where the whole-fact gather is refused).
//!
//! Shapes under test:
//!
//! - **Replicated-subquery conjuncts** (Q10/Q35/Q69): a subquery predicate whose every table is
//!   replicated is partition-independent, so it is emitted verbatim wherever the outer row is
//!   read — Q69's `NOT EXISTS` over replicated `web_sales`/`catalog_sales`, Q10/Q35's
//!   `EXISTS(web) OR EXISTS(catalog)` arm — while the genuinely sharded `EXISTS` becomes a
//!   co-located semi key stream. Before KAN-55 these conjuncts forced every subquery table
//!   through the key-stream checks, which declined (zero or ≠1 sharded scans) and fell into the
//!   strict-refused whole-fact gather.
//! - **Global aggregate above the semi/anti filter** (Q16/Q94): `count(DISTINCT …), sum(…)`
//!   with no GROUP BY gathers the co-located filtered rows to one partition and recomputes
//!   exactly there; a partition-0 gate preserves the synthetic zero-input row of a global
//!   aggregate when the true result is empty.
//! - **Scalar subqueries in the projection** (Q9): each uncorrelated global-aggregate scalar
//!   over the sharded fact decomposes into a per-worker partial + one-row combine; scalars
//!   sharing a body tail (Q9's 15 `store_sales` bands) merge into one FILTER-aggregate scan +
//!   one multi-column combine; the gated outer evaluation reads each scalar from its one-row
//!   `shuffle_input_{i}`.
//! - **Anti-only inline guard**: `NOT EXISTS` over a sharded key stream with a fully-replicated
//!   outer has no semi gate to co-locate emission — it must keep the gather (strict: refuse),
//!   not multiply rows once per partition.
//!
//! Every distributed plan must equal single-node end-to-end, in strict mode
//! (`WEFT_DISTRIBUTED_STRICT=1`) so the whole-fact gather cannot silently substitute.

// ENV_LOCK serializes process-global `WEFT_DISTRIBUTED_STRICT` across async tests.
#![allow(clippy::await_holding_lock)]

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};

use weft_execution::driver::{run_stages, Cluster};
use weft_execution::flight::serve_worker;
use weft_execution::plan::plan_distributed_logical;
use weft_loom::arrow::array::{ArrayRef, Date32Array, Float64Array, Int64Array, StringArray};
use weft_loom::arrow::datatypes::{DataType, Field, Schema};
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::arrow::util::display::{ArrayFormatter, FormatOptions};
use weft_loom::Engine;

const Q9: &str = include_str!("../../../bench/tpcds/queries/q9.sql");
const Q10: &str = include_str!("../../../bench/tpcds/queries/q10.sql");
const Q16: &str = include_str!("../../../bench/tpcds/queries/q16.sql");
const Q35: &str = include_str!("../../../bench/tpcds/queries/q35.sql");
const Q69: &str = include_str!("../../../bench/tpcds/queries/q69.sql");
const Q94: &str = include_str!("../../../bench/tpcds/queries/q94.sql");
const Q95: &str = include_str!("../../../bench/tpcds/queries/q95.sql");

/// The SF10 post-classification configuration per query: only the query's driving fact is
/// sharded; every other table (including the smaller sales channels and returns tables that
/// appear only inside subqueries) is replicated.
const REPL_STORE: [&str; 11] = [
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "web_sales",
    "catalog_sales",
    "catalog_returns",
    "web_returns",
    "call_center",
    "web_site",
    "reason",
];
const REPL_CATALOG: [&str; 11] = [
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "store_sales",
    "web_sales",
    "catalog_returns",
    "web_returns",
    "call_center",
    "web_site",
    "reason",
];
const REPL_WEB: [&str; 11] = [
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "store_sales",
    "catalog_sales",
    "catalog_returns",
    "web_returns",
    "call_center",
    "web_site",
    "reason",
];

static PORT: std::sync::OnceLock<AtomicU16> = std::sync::OnceLock::new();

fn unique_worker_port() -> u16 {
    // OnceLock-seeded allocator with the base BELOW the Linux ephemeral source range
    // (32768..=60999): the harness's own outbound connections can never steal a worker's
    // port (serve_worker swallows EADDRINUSE; the old in-range bases flaked "did not
    // bind" / "distributed run never succeeded" on loaded CI runners).
    PORT.get_or_init(|| AtomicU16::new(24000 + (std::process::id() as u16 % 512)))
        .fetch_add(1, Ordering::Relaxed)
}

/// `WEFT_DISTRIBUTED_STRICT` is process-global; serialize the tests that set it.
static ENV_LOCK: Mutex<()> = Mutex::new(());

fn i64f(name: &str) -> Field {
    Field::new(name, DataType::Int64, false)
}
fn f64f(name: &str) -> Field {
    Field::new(name, DataType::Float64, false)
}
fn strf(name: &str) -> Field {
    Field::new(name, DataType::Utf8, false)
}
fn datef(name: &str) -> Field {
    Field::new(name, DataType::Date32, false)
}

fn i64v(vals: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(vals.to_vec()))
}
fn f64v(vals: &[f64]) -> ArrayRef {
    Arc::new(Float64Array::from(vals.to_vec()))
}
fn strv(vals: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(vals.to_vec()))
}
fn datev(vals: &[i32]) -> ArrayRef {
    Arc::new(Date32Array::from(vals.to_vec()))
}

fn batch(fields: Vec<Field>, cols: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
}

// date32 days: 2002-02-15=11733, 2002-03-01=11747, 2001-04-10=11422, 2001-06-01=11474,
// 2002-01-05=11692, 1999-03-01=10651.
fn date_dim() -> RecordBatch {
    batch(
        vec![
            i64f("d_date_sk"),
            i64f("d_year"),
            i64f("d_moy"),
            i64f("d_qoy"),
            datef("d_date"),
        ],
        vec![
            i64v(&[1, 2, 3, 4, 5, 6]),
            i64v(&[2002, 2002, 2001, 2001, 2002, 1999]),
            i64v(&[2, 3, 4, 6, 1, 3]),
            i64v(&[1, 1, 2, 2, 1, 1]),
            datev(&[11733, 11747, 11422, 11474, 11692, 10651]),
        ],
    )
}

fn customer() -> RecordBatch {
    batch(
        vec![
            i64f("c_customer_sk"),
            i64f("c_current_addr_sk"),
            i64f("c_current_cdemo_sk"),
        ],
        vec![
            i64v(&[1, 2, 3, 4, 5]),
            i64v(&[1, 2, 3, 4, 5]),
            i64v(&[1, 2, 3, 4, 5]),
        ],
    )
}

fn customer_address() -> RecordBatch {
    batch(
        vec![i64f("ca_address_sk"), strf("ca_county"), strf("ca_state")],
        vec![
            i64v(&[1, 2, 3, 4, 5, 6]),
            strv(&[
                "Rush County",
                "Toole County",
                "Rush County",
                "Jefferson County",
                "Nowhere County",
                "Rush County",
            ]),
            strv(&["GA", "KY", "GA", "NM", "CA", "IL"]),
        ],
    )
}

fn customer_demographics() -> RecordBatch {
    batch(
        vec![
            i64f("cd_demo_sk"),
            strf("cd_gender"),
            strf("cd_marital_status"),
            strf("cd_education_status"),
            strf("cd_purchase_estimate"),
            strf("cd_credit_rating"),
            i64f("cd_dep_count"),
            i64f("cd_dep_employed_count"),
            i64f("cd_dep_college_count"),
        ],
        vec![
            i64v(&[1, 2, 3, 4, 5]),
            strv(&["M", "F", "M", "F", "M"]),
            strv(&["S", "M", "D", "S", "M"]),
            strv(&["HS", "BA", "MA", "HS", "BA"]),
            strv(&["500", "1000", "1500", "500", "1000"]),
            strv(&["A", "B", "C", "A", "B"]),
            i64v(&[0, 1, 2, 0, 1]),
            i64v(&[0, 1, 1, 0, 0]),
            i64v(&[1, 0, 1, 0, 1]),
        ],
    )
}

/// Sharded fact. Rows are ordered so the interesting keys span the two shards: the 2002 store
/// keys split {1,2} | {3}, and ticket 1001 has a low-quantity row on shard 0 plus a
/// quantity-55 row on shard 1 (the cross-shard EXISTS witness).
fn store_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ss_sold_date_sk"),
            i64f("ss_customer_sk"),
            i64f("ss_quantity"),
            f64f("ss_ext_discount_amt"),
            f64f("ss_net_paid"),
            i64f("ss_ticket_number"),
        ],
        vec![
            i64v(&[5, 2, 3, 5, 4, 3]),
            i64v(&[1, 2, 2, 3, 1, 3]),
            i64v(&[10, 25, 15, 45, 55, 65]),
            f64v(&[1.0, 3.0, 7.0, 5.0, 9.0, 11.0]),
            f64v(&[2.0, 4.0, 8.0, 6.0, 10.0, 12.0]),
            i64v(&[1001, 1002, 1003, 1004, 1001, 1006]),
        ],
    )
}

/// Second sharded fact (self-EXISTS for Q16). Order 100's two warehouses split across shards;
/// order 300 qualifies the EXISTS but is killed by the returns anti; order 200 (single row) and
/// order 400 (same warehouse twice) never qualify.
fn catalog_sales() -> RecordBatch {
    batch(
        vec![
            i64f("cs_sold_date_sk"),
            i64f("cs_ship_customer_sk"),
            i64f("cs_ship_date_sk"),
            i64f("cs_ship_addr_sk"),
            i64f("cs_call_center_sk"),
            i64f("cs_order_number"),
            i64f("cs_warehouse_sk"),
            f64f("cs_ext_ship_cost"),
            f64f("cs_net_profit"),
        ],
        vec![
            i64v(&[5, 1, 3, 1, 1, 1, 1]),
            i64v(&[3, 4, 1, 4, 4, 4, 4]),
            i64v(&[1, 1, 1, 1, 1, 1, 1]),
            i64v(&[1, 1, 1, 1, 1, 1, 1]),
            i64v(&[1, 1, 1, 1, 1, 1, 1]),
            i64v(&[100, 200, 300, 100, 300, 400, 400]),
            i64v(&[1, 1, 1, 2, 2, 1, 1]),
            f64v(&[100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0]),
            f64v(&[10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0]),
        ],
    )
}

/// Third sharded fact (Q94's driver; replicated in the store/catalog configurations). Order
/// 500's warehouses split across shards; order 700 qualifies the EXISTS but sits in web_returns;
/// order 600 has a single warehouse.
fn web_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ws_sold_date_sk"),
            i64f("ws_bill_customer_sk"),
            i64f("ws_ship_date_sk"),
            i64f("ws_ship_addr_sk"),
            i64f("ws_web_site_sk"),
            i64f("ws_order_number"),
            i64f("ws_warehouse_sk"),
            f64f("ws_ext_ship_cost"),
            f64f("ws_net_profit"),
        ],
        vec![
            i64v(&[5, 6, 3, 6, 6]),
            i64v(&[2, 4, 2, 5, 1]),
            i64v(&[6, 6, 6, 6, 6]),
            i64v(&[6, 6, 6, 6, 6]),
            i64v(&[1, 1, 1, 1, 1]),
            i64v(&[500, 600, 500, 700, 700]),
            i64v(&[1, 3, 2, 1, 4]),
            f64v(&[10.0, 30.0, 20.0, 40.0, 50.0]),
            f64v(&[1.0, 3.0, 2.0, 4.0, 5.0]),
        ],
    )
}

fn catalog_returns() -> RecordBatch {
    batch(vec![i64f("cr_order_number")], vec![i64v(&[300])])
}

fn web_returns() -> RecordBatch {
    batch(vec![i64f("wr_order_number")], vec![i64v(&[700])])
}

fn call_center() -> RecordBatch {
    batch(
        vec![i64f("cc_call_center_sk"), strf("cc_county")],
        vec![i64v(&[1, 2]), strv(&["Williamson County", "Other County"])],
    )
}

fn web_site() -> RecordBatch {
    batch(
        vec![i64f("web_site_sk"), strf("web_company_name")],
        vec![i64v(&[1, 2]), strv(&["pri", "other"])],
    )
}

fn reason() -> RecordBatch {
    batch(vec![i64f("r_reason_sk")], vec![i64v(&[1, 2])])
}

fn register(engine: &Engine, name: &str, batches: Vec<RecordBatch>) {
    engine.register_batches(name, batches).unwrap();
}

fn register_dims(engine: &Engine) {
    register(engine, "date_dim", vec![date_dim()]);
    register(engine, "customer", vec![customer()]);
    register(engine, "customer_address", vec![customer_address()]);
    register(
        engine,
        "customer_demographics",
        vec![customer_demographics()],
    );
    register(engine, "catalog_returns", vec![catalog_returns()]);
    register(engine, "web_returns", vec![web_returns()]);
    register(engine, "call_center", vec![call_center()]);
    register(engine, "web_site", vec![web_site()]);
    register(engine, "reason", vec![reason()]);
}

/// Planner/ground-truth engine holding the full dataset.
async fn tpcds_engine() -> Engine {
    let e = Engine::new();
    register_dims(&e);
    register(&e, "store_sales", vec![store_sales()]);
    register(&e, "catalog_sales", vec![catalog_sales()]);
    register(&e, "web_sales", vec![web_sales()]);
    e
}

/// Contiguous half of a table, so cross-shard keys/orders genuinely need both workers.
fn shard_rows(full: &RecordBatch, idx: usize) -> Vec<RecordBatch> {
    let half = full.num_rows() / 2;
    let (start, len) = if idx == 0 {
        (0, half)
    } else {
        (half, full.num_rows() - half)
    };
    vec![full.slice(start, len)]
}

/// The driving fact sharded row-wise across two in-process workers; every other table —
/// including the two smaller sales channels the planner is told are replicated — held in full
/// on each worker. (Production keeps the same invariant: `resolve_replicated_tables` marks a
/// table replicated only when every worker really has the whole table.)
async fn two_workers_sharded(fact: &str) -> Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        register_dims(&e);
        for (name, full) in [
            ("store_sales", store_sales()),
            ("catalog_sales", catalog_sales()),
            ("web_sales", web_sales()),
        ] {
            if name == fact {
                register(&e, name, shard_rows(&full, i));
            } else {
                register(&e, name, vec![full]);
            }
        }
        tokio::spawn(async move {
            let _ = serve_worker(port, e).await;
        });
    }
    Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ])
}

/// Plan in strict mode (the whole-fact gather must never substitute), then run the stages.
async fn run_distributed(
    cluster: &Cluster,
    planner: &Engine,
    sql: &str,
    replicated: &[&str],
) -> Vec<RecordBatch> {
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let dq = {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("WEFT_DISTRIBUTED_STRICT", "1");
        let planned = plan_distributed_logical(&lp, replicated);
        std::env::remove_var("WEFT_DISTRIBUTED_STRICT");
        planned.expect("strict-mode plan_distributed_logical")
    };
    let mut out = None;
    for _ in 0..150 {
        match run_stages(cluster, &dq.stages).await {
            Ok(b) => {
                out = Some(b);
                break;
            }
            Err(e) => {
                eprintln!("run_stages err: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await
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

/// Sorted value rows (headers are not compared: single-node and distributed plans name unaliased
/// aggregate outputs differently — pre-existing behavior of every distributed shape).
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

async fn assert_distributed_matches_single_node(sql: &str, replicated: &[&str]) {
    let planner = tpcds_engine().await;
    let expected = planner.sql(sql).await.expect("single-node");
    let fact = ["store_sales", "catalog_sales", "web_sales"]
        .into_iter()
        .find(|t| !replicated.contains(t))
        .expect("one sales fact stays sharded");
    let cluster = two_workers_sharded(fact).await;
    let actual = run_distributed(&cluster, &planner, sql, replicated).await;
    assert_eq!(
        rows_sorted(&actual),
        rows_sorted(&expected),
        "distributed must equal single-node"
    );
}

// --- Q10 / Q35 / Q69: replicated-subquery conjuncts + one sharded EXISTS key stream ---

#[tokio::test]
async fn q10_or_exists_over_replicated_channels_plans_semi() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let lp = planner.logical_plan(Q10).await.expect("logical plan");
    let dq = {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("WEFT_DISTRIBUTED_STRICT", "1");
        let planned = plan_distributed_logical(&lp, &REPL_STORE);
        std::env::remove_var("WEFT_DISTRIBUTED_STRICT");
        planned.expect("Q10 must plan in strict mode (pre-KAN-55 this was the refused gather)")
    };
    assert_eq!(
        dq.stages.len(),
        3,
        "store_sales key producer -> semi+partial -> combine: {dq:?}"
    );
    let producer = &dq.stages[0];
    assert_eq!(producer.hash_key_cols, vec![0], "hashed by ss_customer_sk");
    assert!(
        producer.sql.contains("AS k0") && producer.sql.contains("store_sales"),
        "{}",
        producer.sql
    );
    let semi = &dq.stages[1];
    assert!(
        semi.sql
            .contains("EXISTS (SELECT 1 FROM shuffle_input AS k WHERE k.k0 = c.c_customer_sk)"),
        "sharded store_sales EXISTS becomes the co-located semi: {}",
        semi.sql
    );
    // The OR arm reads only replicated tables: it is emitted verbatim, evaluated per partition.
    assert!(
        semi.sql.contains("web_sales") && semi.sql.contains("catalog_sales"),
        "the replicated OR-of-EXISTS rides along verbatim: {}",
        semi.sql
    );
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.sql.contains("__weft_materialize_gate")
                || s.sql.contains("__weft_subquery_gate")),
        "no whole-fact gather: {dq:?}"
    );
}

#[tokio::test]
async fn q10_distributed_matches_single_node() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    assert_distributed_matches_single_node(Q10, &REPL_STORE).await;
}

#[tokio::test]
async fn q35_distributed_matches_single_node() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    assert_distributed_matches_single_node(Q35, &REPL_STORE).await;
}

#[tokio::test]
async fn q69_not_exists_over_replicated_channels_plans_and_matches() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let lp = planner.logical_plan(Q69).await.expect("logical plan");
    let dq = {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("WEFT_DISTRIBUTED_STRICT", "1");
        let planned = plan_distributed_logical(&lp, &REPL_STORE);
        std::env::remove_var("WEFT_DISTRIBUTED_STRICT");
        planned.expect("Q69 must plan in strict mode")
    };
    let semi = &dq.stages[1];
    assert!(
        semi.sql.contains("NOT EXISTS") && semi.sql.contains("web_sales"),
        "the replicated NOT EXISTS arms evaluate verbatim per partition: {}",
        semi.sql
    );
    drop(dq);
    assert_distributed_matches_single_node(Q69, &REPL_STORE).await;
}

// --- Q16 / Q94: self-EXISTS with a residual + replicated anti + global count(DISTINCT) ---

#[tokio::test]
async fn q16_self_exists_global_count_distinct_plans_gathered_distinct() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let lp = planner.logical_plan(Q16).await.expect("logical plan");
    let dq = {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("WEFT_DISTRIBUTED_STRICT", "1");
        let planned = plan_distributed_logical(&lp, &REPL_CATALOG);
        std::env::remove_var("WEFT_DISTRIBUTED_STRICT");
        planned.expect("Q16 must plan in strict mode (pre-KAN-55 global aggs declined)")
    };
    let producer = dq
        .stages
        .iter()
        .find(|s| s.sql.contains("AS k0") && s.sql.contains("AS ic0"))
        .expect("self-EXISTS key producer: {dq:?}");
    assert_eq!(producer.hash_key_cols, vec![0], "hashed by cs_order_number");
    let semi = dq
        .stages
        .iter()
        .find(|s| s.sql.contains("FROM shuffle_input_0 AS o"))
        .expect("semi stage: {dq:?}");
    assert!(
        semi.sql.contains("k.k0 = o.ok0") && semi.sql.contains("o.oe0 <> k.ic0"),
        "the warehouse inequality stays as a co-located residual: {}",
        semi.sql
    );
    let scan = dq
        .stages
        .iter()
        .find(|s| s.sql.contains("AS ok0") && s.sql.contains("AS oe0"))
        .expect("outer export scan: {dq:?}");
    assert!(
        scan.sql.contains("NOT EXISTS") && scan.sql.contains("catalog_returns"),
        "the replicated returns anti evaluates verbatim in the outer scan: {}",
        scan.sql
    );
    let combine = dq.stages.last().expect("combine");
    assert!(
        combine.sql.contains("count(DISTINCT c0)"),
        "the exact distinct recompute over the gathered filtered rows: {}",
        combine.sql
    );
    assert_eq!(
        combine.upstream_stage_ids.len(),
        2,
        "semi rows + partition-0 gate feed the global distinct combine"
    );
    assert!(
        combine
            .sql
            .contains("EXISTS (SELECT 1 FROM shuffle_input_1)"),
        "the gate preserves the synthetic zero-input row exactly once: {}",
        combine.sql
    );
    assert!(
        dq.stages.iter().any(|s| s.sql.contains("__weft_semi_gate")),
        "gate stage present: {dq:?}"
    );
}

#[tokio::test]
async fn q16_distributed_matches_single_node() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    assert_distributed_matches_single_node(Q16, &REPL_CATALOG).await;
}

#[tokio::test]
async fn q16_empty_result_still_emits_the_global_row() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    // A state filter nothing matches: single-node emits one row (0, NULL, NULL); the gate must
    // make the distributed plan emit exactly one row too, not zero and not one per partition.
    let sql = Q16.replace("ca_state = 'GA'", "ca_state = 'ZZ'");
    let planner = tpcds_engine().await;
    let expected = planner.sql(&sql).await.expect("single-node");
    assert_eq!(
        expected.iter().map(RecordBatch::num_rows).sum::<usize>(),
        1,
        "single-node global aggregate emits the synthetic row"
    );
    let cluster = two_workers_sharded("catalog_sales").await;
    let actual = run_distributed(&cluster, &planner, &sql, &REPL_CATALOG).await;
    assert_eq!(rows_sorted(&actual), rows_sorted(&expected));
}

#[tokio::test]
async fn q94_distributed_matches_single_node() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    assert_distributed_matches_single_node(Q94, &REPL_WEB).await;
}

/// Global aggregate without DISTINCT: recombinable partials, no gate needed.
#[tokio::test]
async fn global_non_distinct_self_exists_matches_single_node() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let sql = "SELECT count(*) AS c, sum(ss_net_paid) AS s FROM store_sales \
               WHERE EXISTS (SELECT 1 FROM store_sales s2 \
                             WHERE s2.ss_ticket_number = store_sales.ss_ticket_number \
                               AND s2.ss_quantity > 50)";
    // Ticket 1001's qualifying row (quantity 55) lives on shard 1 while its quantity-10 row is
    // on shard 0: the EXISTS only holds through cross-shard co-location.
    assert_distributed_matches_single_node(sql, &REPL_STORE).await;
    // Empty true result: single-node emits (0, NULL); the partial-row convention must too.
    let empty = sql.replace("> 50", "> 1000000");
    assert_distributed_matches_single_node(&empty, &REPL_STORE).await;
}

// --- Q9: uncorrelated global-aggregate scalar subqueries in the projection ---

#[tokio::test]
async fn q9_scalar_projection_merges_same_tail_scalars_into_one_scan() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let lp = planner.logical_plan(Q9).await.expect("logical plan");
    let dq = {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("WEFT_DISTRIBUTED_STRICT", "1");
        let planned = plan_distributed_logical(&lp, &REPL_STORE);
        std::env::remove_var("WEFT_DISTRIBUTED_STRICT");
        planned.expect("Q9 must plan in strict mode (pre-KAN-55 this was the refused gather)")
    };
    // 15 same-tail scalars merge: one FILTER-aggregate partial + one combine + gate + gated
    // outer evaluation (was 15 × (partial + combine) + gate + outer = 32 stages, 15 scans).
    assert_eq!(dq.stages.len(), 4, "merged same-tail shape: {dq:?}");
    let partial = &dq.stages[0];
    assert_eq!(
        partial.sql.matches("FROM store_sales").count(),
        1,
        "one shared scan of the sharded fact: {}",
        partial.sql
    );
    for band in [
        "store_sales.ss_quantity BETWEEN 1 AND 20",
        "store_sales.ss_quantity BETWEEN 21 AND 40",
        "store_sales.ss_quantity BETWEEN 41 AND 60",
        "store_sales.ss_quantity BETWEEN 61 AND 80",
        "store_sales.ss_quantity BETWEEN 81 AND 100",
    ] {
        assert!(
            partial
                .sql
                .contains(&format!("count(1) FILTER (WHERE ({band}))")),
            "per-band count as a FILTER aggregate: {}",
            partial.sql
        );
        assert!(
            partial.sql.contains(&format!(
                "sum(store_sales.ss_ext_discount_amt) FILTER (WHERE ({band}))"
            )),
            "per-band avg sum-partial as a FILTER aggregate: {}",
            partial.sql
        );
        assert!(
            partial.sql.contains(&format!(
                "count(store_sales.ss_net_paid) FILTER (WHERE ({band}))"
            )),
            "per-band avg count-partial as a FILTER aggregate: {}",
            partial.sql
        );
    }
    let combine = &dq.stages[1];
    for j in 0..15 {
        assert!(
            combine.sql.contains(&format!("AS s{j}")),
            "every scalar value a column of the single combine row: {}",
            combine.sql
        );
    }
    assert!(
        combine.sql.contains("sum(a0)") && combine.sql.contains("NULLIF(sum(a1c), 0)"),
        "count/avg recombination over the shared partials: {}",
        combine.sql
    );
    let outer = dq.stages.last().expect("gated outer");
    assert!(outer.sql.contains("__weft_scalar_src"), "{}", outer.sql);
    assert!(
        outer.sql.contains("EXISTS (SELECT 1 FROM shuffle_input_1)"),
        "the partition-0 gate makes the replicated outer emit exactly once: {}",
        outer.sql
    );
    assert!(
        outer.sql.contains("(SELECT s0 FROM shuffle_input_0)")
            && outer.sql.contains("(SELECT s14 FROM shuffle_input_0)"),
        "each scalar reads its column of the merged one-row combine: {}",
        outer.sql
    );
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.sql == "SELECT * FROM store_sales"),
        "no whole-fact gather: {dq:?}"
    );
}

/// Mixed projection: two same-tail sharded scalars merge into one FILTER-aggregate scan while a
/// replicated-body scalar keeps its own per-scalar stage pair.
#[tokio::test]
async fn mixed_shared_and_unique_tail_scalars_compose() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let sql = "SELECT (SELECT count(*) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20) + \
                      (SELECT count(*) FROM store_sales WHERE ss_quantity BETWEEN 21 AND 40) \
                      AS bands, \
               (SELECT count(*) FROM reason) AS n_reasons \
               FROM reason WHERE r_reason_sk = 1";
    let planner = tpcds_engine().await;
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let dq = {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("WEFT_DISTRIBUTED_STRICT", "1");
        let planned = plan_distributed_logical(&lp, &REPL_STORE);
        std::env::remove_var("WEFT_DISTRIBUTED_STRICT");
        planned.expect("mixed scalar projection must plan in strict mode")
    };
    // Merged partial + combine (store_sales), per-scalar partial + combine (reason), gate,
    // gated outer.
    assert_eq!(dq.stages.len(), 6, "merged + per-scalar compose: {dq:?}");
    let partial = &dq.stages[0];
    assert_eq!(
        partial.sql.matches("FROM store_sales").count(),
        1,
        "one shared scan: {}",
        partial.sql
    );
    assert!(
        partial
            .sql
            .contains("count(1) FILTER (WHERE (store_sales.ss_quantity BETWEEN 1 AND 20)) AS a0")
            && partial.sql.contains(
                "count(1) FILTER (WHERE (store_sales.ss_quantity BETWEEN 21 AND 40)) AS a1"
            ),
        "one FILTER aggregate per band over the shared scan: {}",
        partial.sql
    );
    let combine = &dq.stages[1];
    assert!(
        combine.sql.contains("sum(a0) AS m0") && combine.sql.contains("sum(a1) AS m1"),
        "one combine row, one column per merged member: {}",
        combine.sql
    );
    // The unique-tail (replicated) reason scalar keeps its per-scalar pair, computed once.
    let reason_partial = &dq.stages[2];
    assert!(
        reason_partial.sql.contains("count(1) AS a0") && reason_partial.sql.contains("FROM reason"),
        "unique-tail scalar keeps its per-scalar partial: {}",
        reason_partial.sql
    );
    assert_eq!(
        reason_partial.exchange,
        weft_execution::driver::ExchangeMode::Forward,
        "replicated body computes its partial exactly once"
    );
    let outer = dq.stages.last().expect("gated outer");
    assert!(
        outer.sql.contains("(SELECT s0 FROM shuffle_input_0)")
            && outer.sql.contains("(SELECT s1 FROM shuffle_input_0)")
            && outer.sql.contains("(SELECT * FROM shuffle_input_1)"),
        "merged members read columns of shuffle_input_0, the unique scalar its own input: {}",
        outer.sql
    );
    assert!(
        outer.sql.contains("EXISTS (SELECT 1 FROM shuffle_input_2)"),
        "the partition-0 gate is the last upstream: {}",
        outer.sql
    );
    assert_distributed_matches_single_node(sql, &REPL_STORE).await;
}

#[tokio::test]
async fn q9_distributed_matches_single_node() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    assert_distributed_matches_single_node(Q9, &REPL_STORE).await;
}

/// Same shape with thresholds the synthetic data crosses: bucket 1 takes the THEN branch,
/// bucket 5 the ELSE over an empty input (NULL scalar).
#[tokio::test]
async fn q9_small_thresholds_exercise_both_case_arms_and_null_scalar() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let sql = "SELECT CASE WHEN (SELECT count(*) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20) > 1 \
               THEN (SELECT avg(ss_ext_discount_amt) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20) \
               ELSE (SELECT avg(ss_net_paid) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20) END bucket1, \
               CASE WHEN (SELECT count(*) FROM store_sales WHERE ss_quantity BETWEEN 81 AND 100) > 1 \
               THEN (SELECT avg(ss_ext_discount_amt) FROM store_sales WHERE ss_quantity BETWEEN 81 AND 100) \
               ELSE (SELECT avg(ss_net_paid) FROM store_sales WHERE ss_quantity BETWEEN 81 AND 100) END bucket5 \
               FROM reason WHERE r_reason_sk = 1";
    assert_distributed_matches_single_node(sql, &REPL_STORE).await;
}

// --- Anti-only inline guard + remaining refusals ---

/// A pure `NOT EXISTS` over a sharded key stream with a fully-replicated outer has no semi gate:
/// kept rows would emit once per partition. The shape must keep the (correct) gather path —
/// refused in strict mode — instead of taking the semi path.
#[tokio::test]
async fn anti_only_replicated_outer_keeps_the_gather() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let sql = "SELECT d.cd_gender AS g, count(*) AS c FROM customer_demographics d \
               WHERE NOT EXISTS (SELECT 1 FROM store_sales s WHERE s.ss_customer_sk = d.cd_demo_sk) \
               GROUP BY d.cd_gender";
    let planner = tpcds_engine().await;
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("WEFT_DISTRIBUTED_STRICT", "1");
        let planned = plan_distributed_logical(&lp, &REPL_STORE);
        std::env::remove_var("WEFT_DISTRIBUTED_STRICT");
        let err = planned.expect_err("strict mode must refuse the anti-only gather");
        assert!(
            err.to_string().contains("refusing whole-fact gather"),
            "got: {err}"
        );
    }
    // Non-strict: the gather plans and is correct (this is what the guard preserves).
    let dq = {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("WEFT_DISTRIBUTED_STRICT");
        plan_distributed_logical(&lp, &REPL_STORE).expect("non-strict gather")
    };
    assert!(
        dq.stages
            .iter()
            .any(|s| s.sql.contains("__weft_subquery_gate")),
        "the whole-fact gather, not a row-multiplying semi plan: {dq:?}"
    );
    let cluster = two_workers_sharded("store_sales").await;
    let mut out = None;
    for _ in 0..150 {
        match run_stages(&cluster, &dq.stages).await {
            Ok(b) => {
                out = Some(b);
                break;
            }
            Err(e) => {
                eprintln!("run_stages err: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await
            }
        }
    }
    let actual = out.expect("distributed run never succeeded");
    let expected = planner.sql(sql).await.expect("single-node");
    assert_eq!(rows_sorted(&actual), rows_sorted(&expected));
}

/// KAN-49 wave-3b: Q95's `IN` bodies scan the sharded fact twice (the `ws_wh` self-join CTE),
/// which the KAN-55 semi/anti machinery declined. The shuffle-first distinct-key producer now
/// exists (see `gather_shapes::try_self_join_in_keys` and the end-to-end coverage in
/// `auto_distribute_kan49c.rs`): the fact hash-shuffles by the order key, the self-join keys
/// compute per partition, and strict mode must plan.
#[tokio::test]
async fn q95_self_join_in_plans_in_strict() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let lp = planner.logical_plan(Q95).await.expect("logical plan");
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("WEFT_DISTRIBUTED_STRICT", "1");
    let planned = plan_distributed_logical(&lp, &REPL_WEB);
    std::env::remove_var("WEFT_DISTRIBUTED_STRICT");
    let dq = planned.expect("Q95 plans in strict mode (KAN-49 wave-3b)");
    let producer = dq
        .stages
        .iter()
        .find(|s| s.sql.contains("AS k0") && s.sql.contains("AS ic0"))
        .expect("the ws_wh key producer: {dq:?}");
    assert_eq!(producer.hash_key_cols, vec![0], "hashed by ws_order_number");
    let keys = dq
        .stages
        .iter()
        .find(|s| s.sql.contains("SELECT DISTINCT a.k0"))
        .expect("the per-partition self-join distinct keys: {dq:?}");
    assert!(
        keys.sql.contains("shuffle_input a JOIN shuffle_input b"),
        "the self-join evaluates over the co-located rows: {}",
        keys.sql
    );
    assert!(
        dq.stages
            .iter()
            .any(|s| s.sql.contains("count(DISTINCT o.ok0)")),
        "the per-partition exact distinct count: {dq:?}"
    );
    assert!(
        !dq.stages.iter().any(|s| s.sql == "SELECT * FROM web_sales"),
        "no whole-fact gather: {dq:?}"
    );
}

// --- Classification: subquery-only tables are sized too ---

/// KAN-55: a small table scanned only inside a subquery (SF10's `web_sales` in Q10/Q35/Q69)
/// must be size-classified like any other — before, subquery tables were invisible to
/// `resolve_replicated_tables` and defaulted to sharded, which blocked every shape in this file
/// at the real SF10 layout.
#[tokio::test]
async fn subquery_only_tables_are_size_classified() {
    use datafusion::parquet::arrow::ArrowWriter;

    std::env::remove_var("WEFT_REPLICATED_TABLES");
    let dir = tempfile::tempdir().unwrap();
    let write = |name: &str, batch: &RecordBatch| {
        let path = dir.path().join(name);
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        path
    };
    let fact = batch(
        vec![i64f("k"), i64f("v")],
        vec![
            Arc::new(Int64Array::from_iter_values(0..4_000)) as ArrayRef,
            Arc::new(Int64Array::from_iter_values(0..4_000)) as ArrayRef,
        ],
    );
    let subq = batch(
        vec![i64f("sk")],
        vec![Arc::new(Int64Array::from_iter_values(0..8)) as ArrayRef],
    );
    let fact_path = write("fact.parquet", &fact);
    let subq_path = write("subq.parquet", &subq);

    let engine = Engine::new();
    engine
        .register_parquet("fact", fact_path.to_str().unwrap())
        .await
        .unwrap();
    engine
        .register_parquet("subq", subq_path.to_str().unwrap())
        .await
        .unwrap();
    // `subq` is scanned only inside the EXISTS.
    let lp = engine
        .logical_plan(
            "SELECT f.k AS k, SUM(f.v) AS sv FROM fact f \
             WHERE EXISTS (SELECT 1 FROM subq s WHERE s.sk = f.k) GROUP BY f.k",
        )
        .await
        .unwrap();
    let replicated = weft_execution::plan::resolve_replicated_tables(&engine, &lp).await;
    assert!(
        replicated.iter().any(|t| t == "subq"),
        "the subquery-only small table must auto-replicate: {replicated:?}"
    );
    assert!(
        !replicated.iter().any(|t| t == "fact"),
        "the largest table stays sharded: {replicated:?}"
    );
}
