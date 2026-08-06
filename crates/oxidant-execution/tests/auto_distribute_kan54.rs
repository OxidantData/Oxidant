//! KAN-54: mixed sharded/replicated `UNION ALL` under an aggregate — the largest remaining
//! TPC-DS whole-fact-gather refusal family at the SF10 strict configuration
//! (`replicated = everything but the largest table`).
//!
//! Shapes under test:
//!
//! - **Pre-aggregated per-channel arms** (Q33/Q56/Q60): three channel CTEs each
//!   `GROUP BY` their own fact, `UNION ALL`, outer `sum(total_sales) GROUP BY key`. DataFusion
//!   keeps the 3-way set op nested (`Union(Union(ss, cs), ws)`), so the split-by-sharding pass
//!   now flattens the bag union before bucketing arms. The sharded arm recomputes its inner
//!   GROUP BY per worker (a key on w workers contributes w partial rows where single-node has
//!   one), which composes exactly only for an outer SUM over additively-decomposable inner
//!   aggregates — the guard in `try_split_broadcast_union` keeps outer COUNT/AVG/MIN/MAX and
//!   inner AVG/MIN/MAX/DISTINCT refusing.
//! - **Flat mixed arms** (Q76): one sharded arm + two replicated-only arms, no inner
//!   aggregates — replicated arms run once via `ExchangeMode::Forward`.
//! - **Hand-rolled ROLLUP** (Q27): `Union` of three aggregates over the *same* sharded table
//!   nested as `Union(Union(a, b), c)`; the arm peel in `plan_union` used to fail on the nested
//!   `Union` node.
//! - **Q4** (regression): non-aggregate 6-way self-join over the `year_total` union CTE — the
//!   branch-DAG path unparses per-branch unions whose nested arms now plan.
//!
//! Every distributed plan must equal single-node end-to-end, in strict mode
//! (`OXIDANT_DISTRIBUTED_STRICT=1`) so the whole-fact gather cannot silently substitute.

// ENV_LOCK serializes process-global `OXIDANT_DISTRIBUTED_STRICT` across async tests.
#![allow(clippy::await_holding_lock)]

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};

use oxidant_execution::driver::{run_stages, Cluster, ExchangeMode};
use oxidant_execution::flight::serve_worker;
use oxidant_execution::plan::plan_distributed_logical;
use oxidant_loom::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::arrow::util::display::{ArrayFormatter, FormatOptions};
use oxidant_loom::Engine;

const Q4: &str = include_str!("../../../bench/tpcds/queries/q4.sql");
const Q27: &str = include_str!("../../../bench/tpcds/queries/q27.sql");
const Q33: &str = include_str!("../../../bench/tpcds/queries/q33.sql");
const Q56: &str = include_str!("../../../bench/tpcds/queries/q56.sql");
const Q60: &str = include_str!("../../../bench/tpcds/queries/q60.sql");
const Q76: &str = include_str!("../../../bench/tpcds/queries/q76.sql");

/// The SF10 post-classification configuration for this family: only `store_sales` (the largest
/// table each query touches) is sharded; the smaller channels, returns, and every dimension are
/// replicated.
const REPL: [&str; 10] = [
    "catalog_sales",
    "web_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "item",
    "store",
    "warehouse",
    "promotion",
];

static PORT: std::sync::OnceLock<AtomicU16> = std::sync::OnceLock::new();

fn unique_worker_port() -> u16 {
    // OnceLock-seeded allocator with the base BELOW the Linux ephemeral source range
    // (32768..=60999): the harness's own outbound connections can never steal a worker's
    // port (serve_worker swallows EADDRINUSE; the old in-range bases flaked "did not
    // bind" / "distributed run never succeeded" on loaded CI runners).
    PORT.get_or_init(|| AtomicU16::new(23000 + (std::process::id() as u16 % 512)))
        .fetch_add(1, Ordering::Relaxed)
}

/// `OXIDANT_DISTRIBUTED_STRICT` is process-global; serialize the tests that set it.
static ENV_LOCK: Mutex<()> = Mutex::new(());

fn i64f(name: &str) -> Field {
    Field::new(name, DataType::Int64, false)
}
fn i64f_null(name: &str) -> Field {
    Field::new(name, DataType::Int64, true)
}
fn f64f(name: &str) -> Field {
    Field::new(name, DataType::Float64, false)
}
fn strf(name: &str) -> Field {
    Field::new(name, DataType::Utf8, false)
}
fn strf_null(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
}

fn i64v(vals: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(vals.to_vec()))
}
fn i64opt(vals: &[Option<i64>]) -> ArrayRef {
    Arc::new(Int64Array::from(vals.to_vec()))
}
fn f64v(vals: &[f64]) -> ArrayRef {
    Arc::new(Float64Array::from(vals.to_vec()))
}
fn strv(vals: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(vals.to_vec()))
}

fn batch(fields: Vec<Field>, cols: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
}

/// (d_date_sk 1..=6): 1998-05, 1998-09, 2001-02, 2002-01, 2002-02, 2001-05.
fn date_dim() -> RecordBatch {
    batch(
        vec![
            i64f("d_date_sk"),
            i64f("d_year"),
            i64f("d_moy"),
            i64f("d_qoy"),
        ],
        vec![
            i64v(&[1, 2, 3, 4, 5, 6]),
            i64v(&[1998, 1998, 2001, 2002, 2002, 2001]),
            i64v(&[5, 9, 2, 1, 2, 5]),
            i64v(&[2, 3, 1, 1, 1, 2]),
        ],
    )
}

/// manufact 10 spans both store_sales shards (items 1/3); item 2 is manufact 20. The nullable
/// string fields mirror real TPC-DS parquet nullability — Q27's hand-rolled ROLLUP arms emit
/// literal `NULL AS i_item_id` / `NULL AS s_state`, which a non-nullable union-arm field would
/// reject at batch validation.
fn item() -> RecordBatch {
    batch(
        vec![
            i64f("i_item_sk"),
            strf_null("i_item_id"),
            i64f("i_manufact_id"),
            strf("i_category"),
            strf("i_color"),
        ],
        vec![
            i64v(&[1, 2, 3, 4]),
            strv(&["AAAA", "BBBB", "CCCC", "DDDD"]),
            i64v(&[10, 20, 10, 30]),
            strv(&["Electronics", "Electronics", "Music", "Home"]),
            strv(&["slate", "blanched", "burnished", "other"]),
        ],
    )
}

fn customer_address() -> RecordBatch {
    batch(
        vec![i64f("ca_address_sk"), f64f("ca_gmt_offset")],
        vec![i64v(&[1, 2, 3]), f64v(&[-5.0, -5.0, 0.0])],
    )
}

fn store() -> RecordBatch {
    batch(
        vec![i64f("s_store_sk"), strf_null("s_state")],
        vec![i64v(&[1, 2]), strv(&["TN", "GA"])],
    )
}

fn customer_demographics() -> RecordBatch {
    batch(
        vec![
            i64f("cd_demo_sk"),
            strf("cd_gender"),
            strf("cd_marital_status"),
            strf("cd_education_status"),
        ],
        vec![
            i64v(&[1, 2]),
            strv(&["M", "F"]),
            strv(&["S", "M"]),
            strv(&["College", "HS"]),
        ],
    )
}

/// Q4's customer 1 buys in all three channels in 2001 and 2002; customers 2/3 are missing
/// channels and drop out of the 6-way equijoin.
fn customer() -> RecordBatch {
    batch(
        vec![
            i64f("c_customer_sk"),
            strf("c_customer_id"),
            strf("c_first_name"),
            strf("c_last_name"),
            strf("c_preferred_cust_flag"),
            strf("c_birth_country"),
            strf("c_login"),
            strf("c_email_address"),
        ],
        vec![
            i64v(&[1, 2, 3]),
            strv(&["C1", "C2", "C3"]),
            strv(&["Ann", "Bob", "Cy"]),
            strv(&["Alpha", "Beta", "Gamma"]),
            strv(&["Y", "N", "N"]),
            strv(&["US", "US", "CA"]),
            strv(&["", "", ""]),
            strv(&["a@x", "b@x", "c@x"]),
        ],
    )
}

/// Sharded fact, 10 rows: shard0 = rows 1-5, shard1 = rows 6-10. Manufact 10 (Q33) and the
/// 2002/TN/College rows (Q27) deliberately span the shards; two `NULL` ss_store_sk rows feed
/// Q76's store arm; customer 1's 2001/2002 rows feed Q4's store channel.
#[allow(clippy::too_many_lines)]
fn store_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ss_sold_date_sk"),
            i64f("ss_item_sk"),
            i64f_null("ss_store_sk"),
            i64f("ss_cdemo_sk"),
            i64f("ss_customer_sk"),
            i64f("ss_addr_sk"),
            i64f("ss_quantity"),
            f64f("ss_list_price"),
            f64f("ss_coupon_amt"),
            f64f("ss_sales_price"),
            f64f("ss_ext_sales_price"),
            f64f("ss_ext_list_price"),
            f64f("ss_ext_wholesale_cost"),
            f64f("ss_ext_discount_amt"),
        ],
        vec![
            i64v(&[1, 2, 3, 4, 1, 1, 3, 5, 2, 6]),
            i64v(&[1, 3, 1, 2, 2, 1, 3, 1, 2, 3]),
            i64opt(&[
                Some(1),
                Some(1),
                Some(2),
                Some(1),
                Some(1),
                None,
                Some(2),
                Some(1),
                None,
                Some(1),
            ]),
            i64v(&[1, 1, 2, 1, 1, 1, 2, 1, 1, 1]),
            i64v(&[1, 2, 3, 1, 1, 2, 3, 1, 2, 1]),
            i64v(&[1, 1, 2, 1, 2, 1, 2, 1, 2, 1]),
            i64v(&[10, 5, 20, 8, 12, 6, 9, 7, 4, 3]),
            f64v(&[
                100.0, 50.0, 200.0, 80.0, 120.0, 60.0, 90.0, 70.0, 40.0, 30.0,
            ]),
            f64v(&[2.0, 1.0, 4.0, 2.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0]),
            f64v(&[90.0, 40.0, 180.0, 70.0, 110.0, 55.0, 85.0, 65.0, 38.0, 28.0]),
            f64v(&[
                900.0, 200.0, 600.0, 560.0, 400.0, 330.0, 150.0, 455.0, 152.0, 84.0,
            ]),
            f64v(&[
                1000.0, 250.0, 800.0, 640.0, 480.0, 360.0, 200.0, 490.0, 160.0, 90.0,
            ]),
            f64v(&[100.0, 50.0, 100.0, 80.0, 60.0, 40.0, 30.0, 50.0, 20.0, 10.0]),
            f64v(&[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
        ],
    )
}

fn catalog_sales() -> RecordBatch {
    batch(
        vec![
            i64f("cs_sold_date_sk"),
            i64f("cs_item_sk"),
            i64f_null("cs_ship_addr_sk"),
            i64f("cs_bill_customer_sk"),
            i64f("cs_bill_addr_sk"),
            f64f("cs_ext_sales_price"),
            f64f("cs_ext_list_price"),
            f64f("cs_ext_wholesale_cost"),
            f64f("cs_ext_discount_amt"),
        ],
        vec![
            i64v(&[3, 4, 1, 2, 3, 1]),
            i64v(&[1, 1, 2, 3, 1, 1]),
            i64opt(&[Some(1), Some(1), Some(2), None, Some(1), Some(1)]),
            i64v(&[1, 1, 2, 1, 2, 1]),
            i64v(&[1, 1, 2, 1, 1, 1]),
            f64v(&[10.0, 1000.0, 700.0, 300.0, 400.0, 100.0]),
            f64v(&[10.0, 1000.0, 700.0, 300.0, 400.0, 100.0]),
            f64v(&[0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            f64v(&[0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
        ],
    )
}

fn web_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ws_sold_date_sk"),
            i64f("ws_item_sk"),
            i64f_null("ws_ship_customer_sk"),
            i64f("ws_bill_customer_sk"),
            i64f("ws_bill_addr_sk"),
            f64f("ws_ext_sales_price"),
            f64f("ws_ext_list_price"),
            f64f("ws_ext_wholesale_cost"),
            f64f("ws_ext_discount_amt"),
        ],
        vec![
            i64v(&[3, 4, 1, 2, 1]),
            i64v(&[1, 1, 2, 3, 1]),
            i64opt(&[Some(1), Some(1), Some(2), None, Some(1)]),
            i64v(&[1, 1, 2, 1, 2]),
            i64v(&[1, 1, 2, 1, 1]),
            f64v(&[100.0, 200.0, 250.0, 120.0, 60.0]),
            f64v(&[100.0, 200.0, 250.0, 120.0, 60.0]),
            f64v(&[0.0, 0.0, 0.0, 0.0, 0.0]),
            f64v(&[0.0, 0.0, 0.0, 0.0, 0.0]),
        ],
    )
}

fn register(engine: &Engine, name: &str, batches: Vec<RecordBatch>) {
    engine.register_batches(name, batches).unwrap();
}

fn register_all_but_fact(engine: &Engine, fact: &str) {
    for (name, full) in [
        ("store_sales", store_sales()),
        ("catalog_sales", catalog_sales()),
        ("web_sales", web_sales()),
    ] {
        if name != fact {
            register(engine, name, vec![full]);
        }
    }
    register(engine, "date_dim", vec![date_dim()]);
    register(engine, "item", vec![item()]);
    register(engine, "customer_address", vec![customer_address()]);
    register(engine, "store", vec![store()]);
    register(
        engine,
        "customer_demographics",
        vec![customer_demographics()],
    );
    register(engine, "customer", vec![customer()]);
}

/// Planner/ground-truth engine holding the full dataset.
async fn tpcds_engine() -> Engine {
    let e = Engine::new();
    register_all_but_fact(&e, "");
    register(&e, "store_sales", vec![store_sales()]);
    e
}

/// Contiguous half of a table, so cross-shard keys genuinely need both workers.
fn shard_rows(full: &RecordBatch, idx: usize) -> Vec<RecordBatch> {
    let half = full.num_rows() / 2;
    let (start, len) = if idx == 0 {
        (0, half)
    } else {
        (half, full.num_rows() - half)
    };
    vec![full.slice(start, len)]
}

/// `store_sales` sharded row-wise across two in-process workers; every other table held in full
/// on each worker (the production replicated-table invariant).
async fn two_workers() -> Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        register_all_but_fact(&e, "store_sales");
        register(&e, "store_sales", shard_rows(&store_sales(), i));
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
async fn plan_strict(planner: &Engine, sql: &str) -> oxidant_execution::plan::DistributedQuery {
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("OXIDANT_DISTRIBUTED_STRICT", "1");
    let planned = plan_distributed_logical(&lp, &REPL);
    std::env::remove_var("OXIDANT_DISTRIBUTED_STRICT");
    planned.expect("must plan in strict mode (pre-KAN-54 this was the refused whole-fact gather)")
}

async fn run_distributed(cluster: &Cluster, planner: &Engine, sql: &str) -> Vec<RecordBatch> {
    let dq = plan_strict(planner, sql).await;
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

async fn assert_distributed_matches_single_node(sql: &str) {
    let planner = tpcds_engine().await;
    let expected = planner.sql(sql).await.expect("single-node");
    let cluster = two_workers().await;
    let actual = run_distributed(&cluster, &planner, sql).await;
    assert_eq!(
        rows_sorted(&actual),
        rows_sorted(&expected),
        "distributed must equal single-node"
    );
}

/// The split-union plan shape: sharded partial (stage 0), one-shot replicated partial
/// (stage 1, `ExchangeMode::Forward`), combine over both (stage 2).
fn assert_split_union_shape(dq: &oxidant_execution::plan::DistributedQuery, n_group_cols: usize) {
    assert_eq!(dq.stages.len(), 3, "partial/partial/combine: {dq:?}");
    let sharded = &dq.stages[0];
    assert!(
        sharded.sql.contains("store_sales") && sharded.sql.contains("GROUP BY"),
        "stage 0 is the sharded-arm partial: {}",
        sharded.sql
    );
    assert_eq!(
        sharded.hash_key_cols.len(),
        n_group_cols,
        "hashed by every group key: {sharded:?}"
    );
    let replicated = &dq.stages[1];
    assert_eq!(
        replicated.exchange,
        ExchangeMode::Forward,
        "replicated arms compute exactly once: {replicated:?}"
    );
    assert!(
        !replicated.sql.contains("store_sales"),
        "replicated side must not scan the sharded fact: {}",
        replicated.sql
    );
    let combine = &dq.stages[2];
    assert!(
        combine.sql.contains("shuffle_input_0") && combine.sql.contains("shuffle_input_1"),
        "combine reads both producers: {}",
        combine.sql
    );
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.sql.contains("__oxidant_materialize_gate")
                || s.sql.contains("__oxidant_subquery_gate")),
        "no whole-fact gather: {dq:?}"
    );
}

// --- Q33 / Q56 / Q60: pre-aggregated per-channel arms, outer SUM (the guarded-provable case) ---

#[tokio::test]
async fn q33_plans_split_union_and_matches_single_node() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = plan_strict(&planner, Q33).await;
    assert_split_union_shape(&dq, 1);
    assert!(
        dq.stages[1].sql.contains("catalog_sales") && dq.stages[1].sql.contains("web_sales"),
        "replicated side unions the two smaller channels: {}",
        dq.stages[1].sql
    );
    assert_distributed_matches_single_node(Q33).await;
}

#[tokio::test]
async fn q56_distributed_matches_single_node() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    assert_distributed_matches_single_node(Q56).await;
}

#[tokio::test]
async fn q60_distributed_matches_single_node() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    assert_distributed_matches_single_node(Q60).await;
}

/// The guard: outer COUNT/AVG/MIN/MAX or a non-additive inner aggregate over a pre-aggregated
/// sharded arm must keep refusing (per-worker inner GROUP BY inflates row multiplicity / compares
/// partials), not emit a wrong plan.
#[tokio::test]
async fn pre_agg_mixed_union_with_non_sum_outer_still_refuses() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let inner_ss = "SELECT i_manufact_id, sum(ss_ext_sales_price) total_sales FROM store_sales, \
                    date_dim, item WHERE ss_item_sk = i_item_sk AND ss_sold_date_sk = d_date_sk \
                    AND d_year = 1998 GROUP BY i_manufact_id";
    let inner_cs = "SELECT i_manufact_id, sum(cs_ext_sales_price) total_sales FROM catalog_sales, \
                    date_dim, item WHERE cs_item_sk = i_item_sk AND cs_sold_date_sk = d_date_sk \
                    AND d_year = 1998 GROUP BY i_manufact_id";
    for outer in ["avg(total_sales)", "count(*)", "min(total_sales)"] {
        let sql = format!(
            "WITH ss AS ({inner_ss}), cs AS ({inner_cs}) \
             SELECT i_manufact_id, {outer} FROM (SELECT * FROM ss UNION ALL SELECT * FROM cs) t \
             GROUP BY i_manufact_id"
        );
        let lp = planner.logical_plan(&sql).await.expect("logical plan");
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("OXIDANT_DISTRIBUTED_STRICT", "1");
        let planned = plan_distributed_logical(&lp, &REPL);
        std::env::remove_var("OXIDANT_DISTRIBUTED_STRICT");
        let err = planned.expect_err("non-SUM outer over pre-aggregated arms must refuse");
        assert!(
            err.to_string().contains("whole-fact gather"),
            "refusal must stay the honest strict gather rejection, got: {err}"
        );
    }
    // Inner AVG does not decompose additively either — outer SUM over it must refuse too.
    let inner_avg_ss = inner_ss.replace("sum(ss_ext_sales_price)", "avg(ss_ext_sales_price)");
    let sql = format!(
        "WITH ss AS ({inner_avg_ss}), cs AS ({inner_cs}) \
         SELECT i_manufact_id, sum(total_sales) FROM (SELECT * FROM ss UNION ALL SELECT * FROM cs) t \
         GROUP BY i_manufact_id"
    );
    let lp = planner.logical_plan(&sql).await.expect("logical plan");
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("OXIDANT_DISTRIBUTED_STRICT", "1");
    let planned = plan_distributed_logical(&lp, &REPL);
    std::env::remove_var("OXIDANT_DISTRIBUTED_STRICT");
    let err = planned.expect_err("inner AVG must refuse even under outer SUM");
    assert!(
        err.to_string().contains("whole-fact gather"),
        "refusal must stay the honest strict gather rejection, got: {err}"
    );
}

// --- Q76: flat mixed arms (no inner aggregates) ---

#[tokio::test]
async fn q76_plans_split_union_and_matches_single_node() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = plan_strict(&planner, Q76).await;
    assert_split_union_shape(&dq, 5);
    assert!(
        dq.stages[0].sql.contains("IS NULL"),
        "Q76's store arm keeps its IS NULL filter: {}",
        dq.stages[0].sql
    );
    assert_distributed_matches_single_node(Q76).await;
}

// --- Q27: hand-rolled ROLLUP — three aggregates over one sharded table, nested union ---

#[tokio::test]
async fn q27_nested_union_of_aggregates_plans_and_matches() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = plan_strict(&planner, Q27).await;
    assert_eq!(
        dq.stages.len(),
        7,
        "three arms x (partial + combine) + union: {dq:?}"
    );
    let union = dq.stages.last().unwrap();
    assert!(
        union.sql.contains("shuffle_input_0")
            && union.sql.contains("shuffle_input_1")
            && union.sql.contains("shuffle_input_2")
            && union.sql.contains("UNION ALL"),
        "the three arm outputs concatenate in one union stage: {}",
        union.sql
    );
    assert_distributed_matches_single_node(Q27).await;
}

// --- Q4: branch-DAG over the year_total union CTE (regression: nested arms refused before) ---

#[tokio::test]
async fn q4_distributed_matches_single_node() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = plan_strict(&planner, Q4).await;
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.sql.contains("__oxidant_materialize_gate")
                || s.sql.contains("__oxidant_subquery_gate")),
        "no whole-fact gather: {dq:?}"
    );
    assert_distributed_matches_single_node(Q4).await;
}
