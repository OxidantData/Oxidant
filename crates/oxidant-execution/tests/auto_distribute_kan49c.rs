//! KAN-49 wave-3b ("gather" wave): distributed shapes for the six TPC-DS queries that still
//! fell into the strict-refused whole-fact gather (Q24/Q38/Q49/Q87/Q95/Q97).
//!
//! Shapes under test:
//!
//! - **Global aggregate over an INTERSECT / EXCEPT chain** (Q38/Q87): each channel's
//!   `SELECT DISTINCT key…` branch exports its rows hash-shuffled on the full row, so equal
//!   rows co-locate; the per-partition `INTERSECT` / `EXCEPT` is then exact and the global
//!   `count(*)` recombines per-partition counts.
//! - **Ranked channel UNION** (Q49): each arm's per-item aggregate is distributed as a
//!   partial/combine pair, the tiny combined relation gathers to one partition for the global
//!   `rank()` windows + top-10 filter, and the `UNION` (distinct) concatenates and dedups.
//! - **HAVING scalar threshold over a shared derived aggregate** (Q24): the `ssales` CTE is
//!   distributed once as a partial/combine pair; the uncorrelated `0.05 * avg(netpaid)`
//!   threshold decomposes off the combine output into a one-row scalar broadcast (KAN-27
//!   literal injection), and the outer aggregate rides the same combine as a second
//!   partial/combine pair with the threshold in its HAVING.
//! - **IN keys from a self-join of the sharded fact** (Q95): the `ws_wh` order-pair keys are
//!   produced by hash-shuffling the fact on the order key (every order co-locates) and
//!   self-joining locally; the outer scan exports the same key so the `IN` filters and the
//!   `count(DISTINCT order)` evaluate exactly per partition.
//! - **Global aggregate over a FULL OUTER JOIN of two distinct-key aggregates** (Q97): both
//!   channel key sets are reduced per worker, hash-shuffled by the join key and recombined, so
//!   the full outer join is exact per partition and the three `sum(CASE …)` buckets recombine.
//!
//! Every distributed plan must equal single-node end-to-end, in strict mode
//! (`OXIDANT_DISTRIBUTED_STRICT=1`) so the whole-fact gather cannot silently substitute.

// ENV_LOCK serializes process-global `OXIDANT_DISTRIBUTED_STRICT` across async tests.
#![allow(clippy::await_holding_lock)]

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};

use oxidant_execution::driver::{run_stages, Cluster};
use oxidant_execution::flight::serve_worker;
use oxidant_execution::plan::plan_distributed_logical;
use oxidant_loom::arrow::array::{ArrayRef, Date32Array, Float64Array, Int64Array, StringArray};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::arrow::util::display::{ArrayFormatter, FormatOptions};
use oxidant_loom::Engine;

const Q24: &str = include_str!("../../../bench/tpcds/queries/q24.sql");
const Q38: &str = include_str!("../../../bench/tpcds/queries/q38.sql");
const Q49: &str = include_str!("../../../bench/tpcds/queries/q49.sql");
const Q87: &str = include_str!("../../../bench/tpcds/queries/q87.sql");
const Q95: &str = include_str!("../../../bench/tpcds/queries/q95.sql");
const Q97: &str = include_str!("../../../bench/tpcds/queries/q97.sql");

/// The SF10 post-classification configuration per query: only the query's driving fact is
/// sharded; every other table is replicated.
const REPL_STORE_SALES: [&str; 11] = [
    "customer",
    "customer_address",
    "date_dim",
    "store",
    "item",
    "web_site",
    "store_returns",
    "catalog_sales",
    "catalog_returns",
    "web_sales",
    "web_returns",
];
const REPL_WEB_SALES: [&str; 11] = [
    "customer",
    "customer_address",
    "date_dim",
    "store",
    "item",
    "web_site",
    "store_sales",
    "store_returns",
    "catalog_sales",
    "catalog_returns",
    "web_returns",
];

static PORT: std::sync::OnceLock<AtomicU16> = std::sync::OnceLock::new();

fn unique_worker_port() -> u16 {
    // OnceLock-seeded allocator with the base BELOW the Linux ephemeral source range
    // (32768..=60999): the harness's own outbound connections can never steal a worker's
    // port (serve_worker swallows EADDRINUSE; the old in-range bases flaked "did not
    // bind" / "distributed run never succeeded" on loaded CI runners).
    PORT.get_or_init(|| AtomicU16::new(19000 + (std::process::id() as u16 % 512)))
        .fetch_add(1, Ordering::Relaxed)
}

/// `OXIDANT_DISTRIBUTED_STRICT` is process-global; serialize the tests that set it.
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

/// date_sk → (d_month_seq, d_date, d_year, d_moy): sk 1/2 sit in the Q38/Q87/Q97 month window
/// (1200..=1211) and the Q95 date window (1999-02-01..1999-04-02); sk 3/4 are the Q49 window
/// (2001-12); sk 5 is outside every window (negative control).
fn date_dim() -> RecordBatch {
    batch(
        vec![
            i64f("d_date_sk"),
            i64f("d_month_seq"),
            datef("d_date"),
            i64f("d_year"),
            i64f("d_moy"),
        ],
        vec![
            i64v(&[1, 2, 3, 4, 5]),
            i64v(&[1205, 1206, 1300, 1301, 900]),
            // 1999-03-01, 1999-03-15, 2001-12-01, 2001-12-15, 1998-05-01.
            datev(&[10651, 10665, 11657, 11671, 10347]),
            i64v(&[1999, 1999, 2001, 2001, 1998]),
            i64v(&[3, 3, 12, 12, 5]),
        ],
    )
}

fn customer() -> RecordBatch {
    batch(
        vec![
            i64f("c_customer_sk"),
            strf("c_last_name"),
            strf("c_first_name"),
            i64f("c_current_addr_sk"),
            strf("c_birth_country"),
        ],
        vec![
            i64v(&[1, 2, 3, 4]),
            strv(&["Aa", "Bb", "Cc", "Dd"]),
            strv(&["Ann", "Bob", "Cid", "Dee"]),
            i64v(&[1, 1, 2, 2]),
            // Q24's `c_birth_country <> upper(ca_country)` holds for every pair below.
            strv(&["US", "US", "CA", "CA"]),
        ],
    )
}

fn customer_address() -> RecordBatch {
    batch(
        vec![
            i64f("ca_address_sk"),
            strf("ca_state"),
            strf("ca_zip"),
            strf("ca_country"),
        ],
        vec![
            i64v(&[1, 2]),
            strv(&["IL", "GA"]),
            strv(&["60001", "30301"]),
            strv(&["usa", "canada"]),
        ],
    )
}

fn store() -> RecordBatch {
    batch(
        vec![
            i64f("s_store_sk"),
            strf("s_store_name"),
            strf("s_zip"),
            i64f("s_market_id"),
            strf("s_state"),
        ],
        vec![
            i64v(&[1, 2]),
            strv(&["store1", "store2"]),
            // Q24 keeps only store 1: market 8 and zip matching customer_address 1.
            strv(&["60001", "99999"]),
            i64v(&[8, 9]),
            strv(&["IL", "GA"]),
        ],
    )
}

fn item() -> RecordBatch {
    batch(
        vec![
            i64f("i_item_sk"),
            strf("i_color"),
            f64f("i_current_price"),
            i64f("i_manager_id"),
            i64f("i_units"),
            i64f("i_size"),
        ],
        vec![
            i64v(&[1, 2]),
            strv(&["peach", "plum"]),
            f64v(&[10.0, 20.0]),
            i64v(&[1, 2]),
            i64v(&[10, 20]),
            i64v(&[5, 6]),
        ],
    )
}

fn web_site() -> RecordBatch {
    batch(
        vec![i64f("web_site_sk"), strf("web_company_name")],
        vec![i64v(&[1, 2]), strv(&["pri", "sec"])],
    )
}

/// Sharded fact for Q24 / Q38 / Q87 / Q97 (and the Q49 store arm).
///
/// Q24 groups: Ann-peach 1000 (passes the 0.05·avg threshold), Bob-peach 60+1 (filtered;
/// the two partials sit on different shards), Ann-plum 40, Bob-plum 10000 (inflates the
/// threshold), plus a store-2 row nothing keeps. Q38 triples: (Aa,Ann,3/01) ×1, (Bb,Bob,3/01)
/// ×2, (Aa,Ann,3/15), (Bb,Bob,3/15), (Cc,Cid,3/15). Q97 keys: (1,1),(2,1),(1,2),(2,2),(3,1).
/// The last row is the Q49 store arm (d=4, customer 4), invisible to the other three queries.
fn store_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ss_sold_date_sk"),
            i64f("ss_item_sk"),
            i64f("ss_customer_sk"),
            i64f("ss_store_sk"),
            i64f("ss_ticket_number"),
            i64f("ss_quantity"),
            f64f("ss_net_paid"),
            f64f("ss_net_profit"),
        ],
        vec![
            i64v(&[1, 1, 2, 1, 2, 2, 4]),
            i64v(&[1, 1, 2, 1, 2, 1, 1]),
            i64v(&[1, 2, 1, 2, 2, 3, 4]),
            i64v(&[1, 1, 1, 1, 1, 2, 1]),
            i64v(&[100, 101, 102, 104, 106, 103, 800]),
            i64v(&[2, 3, 1, 1, 1, 1, 8]),
            f64v(&[1000.0, 60.0, 40.0, 1.0, 10000.0, 777.0, 80.0]),
            f64v(&[10.0, 5.0, 4.0, 1.0, 9.0, 7.0, 3.0]),
        ],
    )
}

fn store_returns() -> RecordBatch {
    batch(
        vec![
            i64f("sr_ticket_number"),
            i64f("sr_item_sk"),
            i64f("sr_return_quantity"),
            f64f("sr_return_amt"),
        ],
        vec![
            i64v(&[100, 101, 102, 104, 106, 103, 800, 900]),
            i64v(&[1, 1, 2, 1, 2, 1, 1, 1]),
            i64v(&[1, 1, 1, 1, 1, 1, 3, 1]),
            f64v(&[5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 25000.0, 5.0]),
        ],
    )
}

/// Q38 catalog triples: (Aa,Ann,3/15), (Bb,Bob,3/01), (Dd,Dee,3/01) — the (Bb,Bob,3/01) triple
/// is the one present in all three channels. Q97 keys: (1,2),(2,1),(4,1). Row 3 is the Q49
/// catalog arm (d=3), invisible elsewhere.
fn catalog_sales() -> RecordBatch {
    batch(
        vec![
            i64f("cs_sold_date_sk"),
            i64f("cs_item_sk"),
            i64f("cs_bill_customer_sk"),
            i64f("cs_order_number"),
            i64f("cs_quantity"),
            f64f("cs_net_paid"),
            f64f("cs_net_profit"),
        ],
        vec![
            i64v(&[2, 1, 3, 1]),
            i64v(&[2, 1, 1, 1]),
            i64v(&[1, 2, 1, 4]),
            i64v(&[900, 901, 700, 902]),
            i64v(&[1, 5, 5, 1]),
            f64v(&[50.0, 50.0, 50.0, 10.0]),
            f64v(&[2.0, 2.0, 2.0, 1.0]),
        ],
    )
}

fn catalog_returns() -> RecordBatch {
    batch(
        vec![
            i64f("cr_order_number"),
            i64f("cr_item_sk"),
            i64f("cr_return_quantity"),
            f64f("cr_return_amount"),
        ],
        vec![
            i64v(&[700, 998]),
            i64v(&[1, 1]),
            i64v(&[1, 1]),
            f64v(&[15000.0, 1.0]),
        ],
    )
}

/// Sharded fact for Q95 (and the Q49 web arm / Q38 web triples).
///
/// Order 500 has one warehouse-1 row (shard 0) and one warehouse-2 row (shard 1): the `ws_wh`
/// self-join only finds the pair once both shards hash-shuffle by order number. Order 501 has a
/// single warehouse (no `ws_wh` pair); order 502 has two warehouses but no `web_returns` row
/// (filtered by the second `IN`). Rows w0/w2 carry the Q38 web triples; w3/w5 are the Q49 web
/// arm (2001-12); the rest point at 1999 dates / address 1 / site 1 for Q95.
fn web_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ws_sold_date_sk"),
            i64f("ws_item_sk"),
            i64f("ws_bill_customer_sk"),
            i64f("ws_order_number"),
            i64f("ws_warehouse_sk"),
            i64f("ws_quantity"),
            f64f("ws_net_paid"),
            f64f("ws_net_profit"),
            f64f("ws_ext_ship_cost"),
            i64f("ws_ship_date_sk"),
            i64f("ws_ship_addr_sk"),
            i64f("ws_web_site_sk"),
        ],
        vec![
            i64v(&[1, 1, 1, 3, 1, 4, 1, 1, 1]),
            i64v(&[1, 1, 1, 1, 1, 2, 1, 1, 1]),
            i64v(&[1, 1, 2, 1, 1, 1, 1, 1, 1]),
            i64v(&[950, 500, 951, 600, 500, 601, 501, 502, 502]),
            i64v(&[1, 1, 1, 1, 2, 1, 1, 1, 2]),
            i64v(&[5, 1, 5, 10, 1, 20, 1, 1, 1]),
            f64v(&[25.0, 30.0, 25.0, 100.0, 40.0, 200.0, 99.0, 55.0, 65.0]),
            f64v(&[2.0, 7.0, 2.0, 5.0, 8.0, 6.0, 9.0, 11.0, 12.0]),
            f64v(&[3.0, 10.0, 3.0, 4.0, 20.0, 5.0, 30.0, 40.0, 50.0]),
            i64v(&[2, 1, 2, 2, 1, 2, 1, 1, 1]),
            i64v(&[2, 1, 2, 2, 1, 2, 1, 1, 1]),
            i64v(&[2, 1, 2, 2, 1, 2, 1, 1, 1]),
        ],
    )
}

fn web_returns() -> RecordBatch {
    batch(
        vec![
            i64f("wr_order_number"),
            i64f("wr_item_sk"),
            i64f("wr_return_quantity"),
            f64f("wr_return_amt"),
        ],
        vec![
            i64v(&[500, 600, 601, 999]),
            i64v(&[1, 1, 2, 1]),
            i64v(&[1, 2, 4, 1]),
            f64v(&[5.0, 20000.0, 30000.0, 1.0]),
        ],
    )
}

fn register(engine: &Engine, name: &str, batches: Vec<RecordBatch>) {
    engine.register_batches(name, batches).unwrap();
}

/// Every table the six queries touch, in full.
fn all_tables() -> Vec<(&'static str, RecordBatch)> {
    vec![
        ("date_dim", date_dim()),
        ("customer", customer()),
        ("customer_address", customer_address()),
        ("store", store()),
        ("item", item()),
        ("web_site", web_site()),
        ("store_sales", store_sales()),
        ("store_returns", store_returns()),
        ("catalog_sales", catalog_sales()),
        ("catalog_returns", catalog_returns()),
        ("web_sales", web_sales()),
        ("web_returns", web_returns()),
    ]
}

/// Planner/ground-truth engine holding the full dataset.
async fn tpcds_engine() -> Engine {
    let e = Engine::new();
    for (name, batch) in all_tables() {
        register(&e, name, vec![batch]);
    }
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

/// The driving fact sharded row-wise across two in-process workers; every other table held in
/// full on each worker.
async fn two_workers_sharded(fact: &str) -> Cluster {
    let fact_full = || match fact {
        "store_sales" => store_sales(),
        "web_sales" => web_sales(),
        other => panic!("unknown fact {other}"),
    };
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        for (name, batch) in all_tables() {
            if name == fact {
                register(&e, name, shard_rows(&fact_full(), i));
            } else {
                register(&e, name, vec![batch]);
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

/// Plan `sql` under `OXIDANT_DISTRIBUTED_STRICT=1` (the whole-fact gather must never substitute).
async fn strict_plan(
    planner: &Engine,
    sql: &str,
    replicated: &[&str],
) -> oxidant_execution::plan::DistributedQuery {
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("OXIDANT_DISTRIBUTED_STRICT", "1");
    let planned = plan_distributed_logical(&lp, replicated);
    std::env::remove_var("OXIDANT_DISTRIBUTED_STRICT");
    planned.expect("strict-mode plan_distributed_logical")
}

/// Plan in strict mode, then run the stages on the cluster.
async fn run_distributed(
    cluster: &Cluster,
    planner: &Engine,
    sql: &str,
    replicated: &[&str],
) -> Vec<RecordBatch> {
    let dq = strict_plan(planner, sql, replicated).await;
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

async fn assert_distributed_matches_single_node(sql: &str, replicated: &[&str], fact: &str) {
    let planner = tpcds_engine().await;
    let expected = planner.sql(sql).await.expect("single-node");
    assert!(
        expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
        "single-node result must be non-empty (otherwise the comparison is vacuous)"
    );
    let cluster = two_workers_sharded(fact).await;
    let actual = run_distributed(&cluster, &planner, sql, replicated).await;
    assert_eq!(
        rows_sorted(&actual),
        rows_sorted(&expected),
        "distributed must equal single-node"
    );
}

/// The whole-fact gather stages this wave replaces must not reappear.
fn assert_no_whole_fact_gather(dq: &oxidant_execution::plan::DistributedQuery, fact: &str) {
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.sql == format!("SELECT * FROM {fact}")),
        "no bare whole-fact scan stage: {dq:?}"
    );
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.sql.contains("__oxidant_materialize_gate")
                || s.sql.contains("__oxidant_subquery_gate")),
        "no whole-fact gather gate: {dq:?}"
    );
}

// --- Q38 / Q87: global count over an INTERSECT / EXCEPT of channel DISTINCT branches ---

#[tokio::test]
async fn q38_intersect_global_count_plans_and_matches() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = strict_plan(&planner, Q38, &REPL_STORE_SALES).await;
    assert_no_whole_fact_gather(&dq, "store_sales");
    assert!(
        dq.stages.iter().any(|s| s.sql.contains("INTERSECT")),
        "the co-located per-partition INTERSECT exists: {dq:?}"
    );
    drop(dq);
    assert_distributed_matches_single_node(Q38, &REPL_STORE_SALES, "store_sales").await;
}

#[tokio::test]
async fn q38_plans_with_any_channel_sharded() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    // The strict sweep counts the query supported when *any* candidate fact plans; the shape is
    // symmetric across channels, so web_sales-sharded must plan too (its refusal named
    // `web_sales` pre-fix).
    let dq = strict_plan(&planner, Q38, &REPL_WEB_SALES).await;
    assert_no_whole_fact_gather(&dq, "web_sales");
}

#[tokio::test]
async fn q87_except_global_count_plans_and_matches() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = strict_plan(&planner, Q87, &REPL_STORE_SALES).await;
    assert_no_whole_fact_gather(&dq, "store_sales");
    assert!(
        dq.stages.iter().any(|s| s.sql.contains("EXCEPT")),
        "the co-located per-partition EXCEPT exists: {dq:?}"
    );
    drop(dq);
    assert_distributed_matches_single_node(Q87, &REPL_STORE_SALES, "store_sales").await;
}

// --- Q97: global aggregate over a FULL OUTER JOIN of two distinct-key aggregates ---

#[tokio::test]
async fn q97_full_outer_join_global_agg_plans_and_matches() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = strict_plan(&planner, Q97, &REPL_STORE_SALES).await;
    assert_no_whole_fact_gather(&dq, "store_sales");
    assert!(
        dq.stages
            .iter()
            .any(|s| s.sql.contains("FULL OUTER JOIN") && s.sql.contains("shuffle_input")),
        "the co-located full outer join exists: {dq:?}"
    );
    drop(dq);
    assert_distributed_matches_single_node(Q97, &REPL_STORE_SALES, "store_sales").await;
}

// --- Q24: HAVING scalar threshold over a shared derived aggregate ---

#[tokio::test]
async fn q24_derived_having_scalar_threshold_plans_and_matches() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = strict_plan(&planner, Q24, &REPL_STORE_SALES).await;
    assert_no_whole_fact_gather(&dq, "store_sales");
    assert!(
        dq.stages
            .iter()
            .any(|s| s.sql.contains("__OXIDANT_SCALAR_STAGE__")),
        "the uncorrelated threshold rides the KAN-27 literal injection: {dq:?}"
    );
    drop(dq);
    assert_distributed_matches_single_node(Q24, &REPL_STORE_SALES, "store_sales").await;
}

// --- Q49: UNION of ranked per-channel window aggregates ---

#[tokio::test]
async fn q49_ranked_channel_union_plans_and_matches() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = strict_plan(&planner, Q49, &REPL_WEB_SALES).await;
    assert_no_whole_fact_gather(&dq, "web_sales");
    assert!(
        dq.stages
            .iter()
            .any(|s| s.sql.contains("rank() OVER") || s.sql.contains("RANK() OVER")),
        "the rank windows compute over the gathered tiny per-item relation: {dq:?}"
    );
    drop(dq);
    assert_distributed_matches_single_node(Q49, &REPL_WEB_SALES, "web_sales").await;
}

// --- Q95: IN keys produced by a shuffle-first self-join of the sharded fact ---

#[tokio::test]
async fn q95_self_join_in_keys_plans_and_matches() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = strict_plan(&planner, Q95, &REPL_WEB_SALES).await;
    assert_no_whole_fact_gather(&dq, "web_sales");
    assert!(
        dq.stages.iter().any(|s| s.sql.contains("count(DISTINCT")),
        "the per-partition exact distinct count over the co-located orders: {dq:?}"
    );
    drop(dq);
    assert_distributed_matches_single_node(Q95, &REPL_WEB_SALES, "web_sales").await;
}
