//! KAN-49a: distributed shapes for the TPC-DS queries whose unoptimized plan parks a
//! top-level `Cross Join` above independently-distributable branches (Q1/Q28/Q30/Q39/Q47/
//! Q57/Q75/Q81 at the SF10 strict configuration, where the whole-fact gather is refused).
//!
//! Shapes under test:
//!
//! - **Correlated scalar threshold over a derived per-key aggregate** (Q1/Q30/Q81): the
//!   `ctr_total_return > (SELECT avg(ctr_total_return)*1.2 … WHERE ctr1.k = ctr2.k)` predicate
//!   is decorrelated into a per-key aggregate branch (`GROUP BY k`) inner-joined into the
//!   gathered outer skeleton — the fact is never gathered whole.
//! - **Global aggregates with COUNT(DISTINCT)** (Q28): the cross product of six single-row
//!   global aggregates over the sharded fact. Each branch shuffles raw rows by the DISTINCT
//!   argument so equal values co-locate; per-partition exact distinct counts + recombinable
//!   partials then gather-combine into the single row.
//! - **CTE self-join with expression-aliased aggregate outputs** (Q39): the `inv` branch's
//!   HAVING / projection references `stdev`/`mean`, aliases of *expressions* over aggregate
//!   outputs (`stddev_samp(x)*1.0 AS stdev`); the remap inlines those expressions in terms of
//!   the recombined `r{i}` columns.
//! - **Stacked windows over an aggregate** (Q47/Q57): a ranking window (`rank() OVER
//!   (PARTITION BY … ORDER BY …)`) and a partition-wide aggregate window layered over the same
//!   GROUP BY plan as a chain of shuffle/local-compute stages.
//! - **Aggregate over a DISTINCT union with mixed sharding** (Q75): every leaf arm is exported
//!   raw (replicated arms via a single `Forward` producer), hash-shuffled on the full row so
//!   identical rows co-locate; per-partition `DISTINCT` + partial aggregate is then exact.
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
use weft_loom::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use weft_loom::arrow::datatypes::{DataType, Field, Schema};
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::arrow::util::display::{ArrayFormatter, FormatOptions};
use weft_loom::Engine;

const Q1: &str = include_str!("../../../bench/tpcds/queries/q1.sql");
const Q28: &str = include_str!("../../../bench/tpcds/queries/q28.sql");
const Q30: &str = include_str!("../../../bench/tpcds/queries/q30.sql");
const Q39: &str = include_str!("../../../bench/tpcds/queries/q39.sql");
const Q47: &str = include_str!("../../../bench/tpcds/queries/q47.sql");
const Q57: &str = include_str!("../../../bench/tpcds/queries/q57.sql");
const Q75: &str = include_str!("../../../bench/tpcds/queries/q75.sql");
const Q81: &str = include_str!("../../../bench/tpcds/queries/q81.sql");

/// The SF10 post-classification configuration per query: only the query's driving fact is
/// sharded; every other table is replicated.
const REPL_STORE_RETURNS: [&str; 13] = [
    "customer",
    "customer_address",
    "date_dim",
    "store",
    "item",
    "warehouse",
    "inventory",
    "store_sales",
    "catalog_sales",
    "web_sales",
    "catalog_returns",
    "web_returns",
    "call_center",
];
const REPL_STORE_SALES: [&str; 13] = [
    "customer",
    "customer_address",
    "date_dim",
    "store",
    "item",
    "warehouse",
    "inventory",
    "store_returns",
    "catalog_sales",
    "web_sales",
    "catalog_returns",
    "web_returns",
    "call_center",
];
const REPL_WEB_RETURNS: [&str; 13] = [
    "customer",
    "customer_address",
    "date_dim",
    "store",
    "item",
    "warehouse",
    "inventory",
    "store_returns",
    "store_sales",
    "catalog_sales",
    "catalog_returns",
    "web_sales",
    "call_center",
];
const REPL_CATALOG_RETURNS: [&str; 13] = [
    "customer",
    "customer_address",
    "date_dim",
    "store",
    "item",
    "warehouse",
    "inventory",
    "store_returns",
    "store_sales",
    "catalog_sales",
    "web_sales",
    "web_returns",
    "call_center",
];
const REPL_INVENTORY: [&str; 13] = [
    "customer",
    "customer_address",
    "date_dim",
    "store",
    "item",
    "warehouse",
    "store_returns",
    "store_sales",
    "catalog_sales",
    "web_sales",
    "catalog_returns",
    "web_returns",
    "call_center",
];
const REPL_CATALOG_SALES: [&str; 13] = [
    "customer",
    "customer_address",
    "date_dim",
    "store",
    "item",
    "warehouse",
    "inventory",
    "store_returns",
    "store_sales",
    "web_sales",
    "catalog_returns",
    "web_returns",
    "call_center",
];

static PORT: std::sync::OnceLock<AtomicU16> = std::sync::OnceLock::new();

fn unique_worker_port() -> u16 {
    // OnceLock-seeded allocator with the base BELOW the Linux ephemeral source range
    // (32768..=60999): the harness's own outbound connections can never steal a worker's
    // port (serve_worker swallows EADDRINUSE; the old in-range bases flaked "did not
    // bind" / "distributed run never succeeded" on loaded CI runners).
    PORT.get_or_init(|| AtomicU16::new(17000 + (std::process::id() as u16 % 512)))
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

fn i64v(vals: &[i64]) -> ArrayRef {
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

/// date_sk → (d_year, d_moy): 1=(2000,1), 2=(2001,1), 3=(2002,2), 4=(2001,2), 5=(1998,12),
/// 6=(1999,1), 7=(1999,2), 8=(1999,3), 9=(2000,1), 10=(2001,1), 11=(2002,1).
fn date_dim() -> RecordBatch {
    batch(
        vec![i64f("d_date_sk"), i64f("d_year"), i64f("d_moy")],
        vec![
            i64v(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
            i64v(&[
                2000, 2001, 2002, 2001, 1998, 1999, 1999, 1999, 2000, 2001, 2002,
            ]),
            i64v(&[1, 1, 2, 2, 12, 1, 2, 3, 1, 1, 1]),
        ],
    )
}

fn store() -> RecordBatch {
    batch(
        vec![
            i64f("s_store_sk"),
            strf("s_state"),
            strf("s_store_name"),
            strf("s_company_name"),
        ],
        vec![
            i64v(&[1, 2]),
            strv(&["TN", "GA"]),
            strv(&["store1", "store2"]),
            strv(&["company1", "company1"]),
        ],
    )
}

fn customer() -> RecordBatch {
    let ids: Vec<String> = (1..=6).map(|i| format!("cust{i}")).collect();
    let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
    batch(
        vec![
            i64f("c_customer_sk"),
            strf("c_customer_id"),
            i64f("c_current_addr_sk"),
            strf("c_salutation"),
            strf("c_first_name"),
            strf("c_last_name"),
            strf("c_preferred_cust_flag"),
            i64f("c_birth_day"),
            i64f("c_birth_month"),
            i64f("c_birth_year"),
            strf("c_birth_country"),
            strf("c_login"),
            strf("c_email_address"),
            i64f("c_last_review_date_sk"),
        ],
        vec![
            i64v(&[1, 2, 3, 4, 5, 6]),
            strv(&id_refs),
            i64v(&[1, 1, 1, 2, 2, 2]),
            strv(&["Mr.", "Ms.", "Mrs.", "Dr.", "Mr.", "Ms."]),
            strv(&["Ann", "Bob", "Cid", "Dee", "Eli", "Fay"]),
            strv(&["Aa", "Bb", "Cc", "Dd", "Ee", "Ff"]),
            strv(&["Y", "N", "Y", "N", "Y", "N"]),
            i64v(&[1, 2, 3, 4, 5, 6]),
            i64v(&[1, 2, 3, 4, 5, 6]),
            i64v(&[1970, 1971, 1972, 1973, 1974, 1975]),
            strv(&["US", "US", "CA", "CA", "US", "US"]),
            strv(&["l1", "l2", "l3", "l4", "l5", "l6"]),
            strv(&["a@x", "b@x", "c@x", "d@x", "e@x", "f@x"]),
            i64v(&[1, 2, 3, 4, 5, 6]),
        ],
    )
}

fn customer_address() -> RecordBatch {
    batch(
        vec![
            i64f("ca_address_sk"),
            strf("ca_state"),
            strf("ca_street_number"),
            strf("ca_street_name"),
            strf("ca_street_type"),
            strf("ca_suite_number"),
            strf("ca_city"),
            strf("ca_county"),
            strf("ca_zip"),
            strf("ca_country"),
            f64f("ca_gmt_offset"),
            strf("ca_location_type"),
        ],
        vec![
            i64v(&[1, 2]),
            strv(&["GA", "TN"]),
            strv(&["10", "20"]),
            strv(&["Main", "Oak"]),
            strv(&["St", "Ave"]),
            strv(&["1", "2"]),
            strv(&["Atl", "Nash"]),
            strv(&["Fulton", "Davidson"]),
            strv(&["30301", "37201"]),
            strv(&["US", "US"]),
            f64v(&[-5.0, -6.0]),
            strv(&["urban", "urban"]),
        ],
    )
}

fn item() -> RecordBatch {
    batch(
        vec![
            i64f("i_item_sk"),
            strf("i_category"),
            strf("i_brand"),
            strf("i_product_name"),
            i64f("i_brand_id"),
            i64f("i_class_id"),
            i64f("i_category_id"),
            i64f("i_manufact_id"),
        ],
        vec![
            i64v(&[1, 2]),
            strv(&["Books", "Books"]),
            strv(&["brand1", "brand2"]),
            strv(&["item1", "item2"]),
            i64v(&[1, 2]),
            i64v(&[1, 1]),
            i64v(&[1, 1]),
            i64v(&[1, 2]),
        ],
    )
}

fn warehouse() -> RecordBatch {
    batch(
        vec![i64f("w_warehouse_sk"), strf("w_warehouse_name")],
        vec![i64v(&[1, 2]), strv(&["wh1", "wh2"])],
    )
}

fn call_center() -> RecordBatch {
    batch(
        vec![i64f("cc_call_center_sk"), strf("cc_name")],
        vec![i64v(&[1, 2]), strv(&["cc1", "cc2"])],
    )
}

/// Sharded fact for Q1 / replicated arm for Q75. Rows are ordered so customer 1's
/// (store 1) group spans the two shards: 60 on shard 0, 40 on shard 1.
fn store_returns() -> RecordBatch {
    batch(
        vec![
            i64f("sr_returned_date_sk"),
            i64f("sr_customer_sk"),
            i64f("sr_store_sk"),
            f64f("sr_return_amt"),
            i64f("sr_ticket_number"),
            i64f("sr_item_sk"),
            i64f("sr_return_quantity"),
        ],
        vec![
            i64v(&[1, 1, 1, 1, 1, 1, 10]),
            i64v(&[1, 2, 1, 3, 4, 5, 9]),
            i64v(&[1, 1, 1, 1, 2, 1, 1]),
            f64v(&[60.0, 10.0, 40.0, 20.0, 50.0, 30.0, 50.0]),
            i64v(&[100, 101, 102, 103, 104, 105, 900]),
            i64v(&[1, 1, 1, 1, 1, 2, 1]),
            i64v(&[6, 1, 4, 2, 5, 3, 5]),
        ],
    )
}

/// Sharded fact for Q28 / Q47 / Q75. Bucket rows for Q28 (quantities 1..=28 in six buckets,
/// bucket 2 repeats price 95 so COUNT(DISTINCT) must co-locate), monthly rows for Q47, and
/// Books rows for Q75 — each family filtered by its own query, all sharing the one table.
fn store_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ss_sold_date_sk"),
            i64f("ss_item_sk"),
            i64f("ss_store_sk"),
            i64f("ss_quantity"),
            f64f("ss_list_price"),
            f64f("ss_coupon_amt"),
            f64f("ss_wholesale_cost"),
            f64f("ss_sales_price"),
            f64f("ss_ext_sales_price"),
            i64f("ss_ticket_number"),
            f64f("ss_net_profit"),
            i64f("ss_addr_sk"),
        ],
        vec![
            // Q28 bucket rows: 12 rows (sk 1 = d_year 2000, irrelevant to Q28).
            i64v(&[
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // Q28
                5, 5, 6, 6, 7, 7, 8, 8, 9, 9, // Q47 months
                10, 10, 10, 11, 11, // Q75 Books 2001/2002 + twin
            ]),
            i64v(&[
                2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, // Q28 (item 2, not Books)
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // Q47 item 1
                1, 1, 1, 1, 1, // Q75 item 1 (Books)
            ]),
            i64v(&[
                2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, // Q28 (store 2)
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // Q47 store 1
                2, 2, 2, 2, 2, // Q75
            ]),
            i64v(&[
                1, 2, 7, 8, 12, 13, 17, 18, 22, 23, 27, 28, // Q28 buckets
                3, 3, 3, 3, 3, 3, 3, 3, 3, 3, // Q47
                60, 40, 7, 50, 50, // Q75 (twin qty 7)
            ]),
            f64v(&[
                10.0, 12.0, 95.0, 95.0, 145.0, 147.0, 137.0, 139.0, 124.0, 126.0, 156.0, 158.0,
                3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 2.0, 2.0, 2.0, 2.0, 2.0,
            ]),
            f64v(&[0.0; 27]),
            f64v(&[0.0; 27]),
            f64v(&[
                1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, // Q28
                5.0, 5.0, 5.0, 5.0, 50.0, 50.0, 5.0, 5.0, 5.0, 5.0, // Q47 (10,10,100,10,10)
                600.0, 400.0, 70.0, 250.0, 250.0, // Q75
            ]),
            f64v(&[
                10.0, 12.0, 95.0, 95.0, 145.0, 147.0, 137.0, 139.0, 124.0, 126.0, 156.0, 158.0,
                50.0, 50.0, 50.0, 50.0, 500.0, 500.0, 50.0, 50.0, 50.0, 50.0, 600.0, 400.0, 70.0,
                250.0, 250.0,
            ]),
            i64v(&[
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                24, 25, 26, 900,
            ]),
            f64v(&[1.0; 27]),
            i64v(&[1; 27]),
        ],
    )
}

/// Sharded fact for Q57 / replicated arm for Q75.
fn catalog_sales() -> RecordBatch {
    batch(
        vec![
            i64f("cs_sold_date_sk"),
            i64f("cs_item_sk"),
            i64f("cs_call_center_sk"),
            f64f("cs_sales_price"),
            i64f("cs_order_number"),
            i64f("cs_quantity"),
            f64f("cs_ext_sales_price"),
        ],
        vec![
            i64v(&[5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11]),
            i64v(&[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]),
            i64v(&[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2]),
            f64v(&[5.0, 5.0, 5.0, 5.0, 50.0, 50.0, 5.0, 5.0, 5.0, 5.0, 1.0, 1.0]),
            i64v(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
            i64v(&[3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 200, 100]),
            f64v(&[
                50.0, 50.0, 50.0, 50.0, 500.0, 500.0, 50.0, 50.0, 50.0, 50.0, 2000.0, 1000.0,
            ]),
        ],
    )
}

/// Replicated for Q75.
fn web_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ws_sold_date_sk"),
            i64f("ws_item_sk"),
            i64f("ws_order_number"),
            i64f("ws_quantity"),
            f64f("ws_ext_sales_price"),
        ],
        vec![
            i64v(&[10, 10, 11]),
            i64v(&[1, 1, 1]),
            i64v(&[1, 2, 3]),
            i64v(&[10, 7, 5]), // the qty-7 row duplicates a store_sales row (dedup check)
            f64v(&[100.0, 70.0, 50.0]),
        ],
    )
}

/// Sharded fact for Q30 / replicated arm for Q75. Customer 1's GA group spans the shards.
fn web_returns() -> RecordBatch {
    batch(
        vec![
            i64f("wr_returned_date_sk"),
            i64f("wr_returning_customer_sk"),
            i64f("wr_returning_addr_sk"),
            f64f("wr_return_amt"),
            i64f("wr_order_number"),
            i64f("wr_item_sk"),
            i64f("wr_return_quantity"),
            f64f("wr_return_amount"),
        ],
        vec![
            i64v(&[3, 3, 3, 3, 3, 10]),
            i64v(&[1, 2, 1, 3, 4, 9]),
            i64v(&[1, 1, 1, 1, 2, 1]),
            f64v(&[60.0, 10.0, 40.0, 20.0, 50.0, 20.0]),
            i64v(&[100, 101, 102, 103, 104, 1]),
            i64v(&[1, 1, 1, 1, 1, 1]),
            i64v(&[6, 1, 4, 2, 5, 2]),
            f64v(&[60.0, 10.0, 40.0, 20.0, 50.0, 20.0]),
        ],
    )
}

/// Sharded fact for Q81 / replicated arm for Q75. Customer 1's GA group spans the shards.
fn catalog_returns() -> RecordBatch {
    batch(
        vec![
            i64f("cr_returned_date_sk"),
            i64f("cr_returning_customer_sk"),
            i64f("cr_returning_addr_sk"),
            f64f("cr_return_amt_inc_tax"),
            i64f("cr_order_number"),
            i64f("cr_item_sk"),
            i64f("cr_return_quantity"),
            f64f("cr_return_amount"),
        ],
        vec![
            i64v(&[1, 1, 1, 1, 1, 10]),
            i64v(&[1, 2, 1, 3, 4, 9]),
            i64v(&[1, 1, 1, 1, 2, 1]),
            f64v(&[60.0, 10.0, 40.0, 20.0, 50.0, 100.0]),
            i64v(&[100, 101, 102, 103, 104, 11]),
            i64v(&[1, 1, 1, 1, 1, 1]),
            i64v(&[6, 1, 4, 2, 5, 10]),
            f64v(&[60.0, 10.0, 40.0, 20.0, 50.0, 100.0]),
        ],
    )
}

/// Sharded fact for Q39. Item 1 / warehouse 1: month 1 rows (0, 8) and month 2 rows (0, 20)
/// split across shards; both groups have stdev/mean > 1. Item 2's group (100, 101) has
/// cov < 1 and must be filtered out.
fn inventory() -> RecordBatch {
    batch(
        vec![
            i64f("inv_item_sk"),
            i64f("inv_warehouse_sk"),
            i64f("inv_date_sk"),
            i64f("inv_quantity_on_hand"),
        ],
        vec![
            i64v(&[1, 1, 1, 1, 2, 2]),
            i64v(&[1, 1, 1, 1, 1, 1]),
            i64v(&[2, 2, 4, 4, 2, 2]), // sk 2 = (2001, moy 1), sk 4 = (2001, moy 2)
            i64v(&[0, 8, 0, 20, 100, 101]),
        ],
    )
}

fn register(engine: &Engine, name: &str, batches: Vec<RecordBatch>) {
    engine.register_batches(name, batches).unwrap();
}

/// Every table the nine queries touch, in full.
fn all_tables() -> Vec<(&'static str, RecordBatch)> {
    vec![
        ("date_dim", date_dim()),
        ("store", store()),
        ("customer", customer()),
        ("customer_address", customer_address()),
        ("item", item()),
        ("warehouse", warehouse()),
        ("call_center", call_center()),
        ("store_returns", store_returns()),
        ("store_sales", store_sales()),
        ("catalog_sales", catalog_sales()),
        ("web_sales", web_sales()),
        ("web_returns", web_returns()),
        ("catalog_returns", catalog_returns()),
        ("inventory", inventory()),
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
        "store_returns" => store_returns(),
        "store_sales" => store_sales(),
        "catalog_sales" => catalog_sales(),
        "web_returns" => web_returns(),
        "catalog_returns" => catalog_returns(),
        "inventory" => inventory(),
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

/// Plan `sql` under `WEFT_DISTRIBUTED_STRICT=1` (the whole-fact gather must never substitute).
async fn strict_plan(
    planner: &Engine,
    sql: &str,
    replicated: &[&str],
) -> weft_execution::plan::DistributedQuery {
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("WEFT_DISTRIBUTED_STRICT", "1");
    let planned = plan_distributed_logical(&lp, replicated);
    std::env::remove_var("WEFT_DISTRIBUTED_STRICT");
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

// --- Q1 / Q30 / Q81: correlated scalar threshold over a derived per-key aggregate ---

#[tokio::test]
async fn q1_correlated_avg_threshold_plans_and_matches() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = strict_plan(&planner, Q1, &REPL_STORE_RETURNS).await;
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.sql.contains("__weft_materialize_gate")
                || s.sql.contains("__weft_subquery_gate")),
        "no whole-fact gather: {dq:?}"
    );
    assert!(
        dq.stages
            .iter()
            .any(|s| s.sql.contains("shuffle_input") && s.sql.contains("AS ctr1")),
        "the ctr1 branch output feeds the gathered outer stage: {dq:?}"
    );
    drop(dq);
    assert_distributed_matches_single_node(Q1, &REPL_STORE_RETURNS, "store_returns").await;
}

#[tokio::test]
async fn q30_correlated_avg_threshold_plans_and_matches() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = strict_plan(&planner, Q30, &REPL_WEB_RETURNS).await;
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.sql.contains("__weft_materialize_gate")
                || s.sql.contains("__weft_subquery_gate")),
        "no whole-fact gather: {dq:?}"
    );
    drop(dq);
    assert_distributed_matches_single_node(Q30, &REPL_WEB_RETURNS, "web_returns").await;
}

#[tokio::test]
async fn q81_correlated_avg_threshold_plans_and_matches() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = strict_plan(&planner, Q81, &REPL_CATALOG_RETURNS).await;
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.sql.contains("__weft_materialize_gate")
                || s.sql.contains("__weft_subquery_gate")),
        "no whole-fact gather: {dq:?}"
    );
    drop(dq);
    assert_distributed_matches_single_node(Q81, &REPL_CATALOG_RETURNS, "catalog_returns").await;
}

// --- Q28: cross product of global aggregates with COUNT(DISTINCT) ---

#[tokio::test]
async fn q28_global_count_distinct_branches_plan_and_match() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = strict_plan(&planner, Q28, &REPL_STORE_SALES).await;
    assert!(
        dq.stages.iter().any(|s| s.sql.contains("count(DISTINCT c")),
        "per-partition exact distinct counts exist: {dq:?}"
    );
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.sql == "SELECT * FROM store_sales"),
        "no whole-fact gather: {dq:?}"
    );
    drop(dq);
    assert_distributed_matches_single_node(Q28, &REPL_STORE_SALES, "store_sales").await;
}

// --- Q39: CTE self-join with expression-aliased aggregate outputs (stddev/mean) ---

#[tokio::test]
async fn q39_expression_aliased_having_plans_and_matches() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = strict_plan(&planner, Q39, &REPL_INVENTORY).await;
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.sql.contains("__weft_materialize_gate")
                || s.sql.contains("__weft_subquery_gate")),
        "no whole-fact gather: {dq:?}"
    );
    drop(dq);
    assert_distributed_matches_single_node(Q39, &REPL_INVENTORY, "inventory").await;
}

// --- Q47 / Q57: stacked ranking + aggregate windows over a GROUP BY ---

#[tokio::test]
async fn q47_stacked_windows_plan_and_match() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = strict_plan(&planner, Q47, &REPL_STORE_SALES).await;
    assert!(
        dq.stages
            .iter()
            .any(|s| { s.sql.contains("rank() OVER (PARTITION BY") && s.sql.contains("ORDER BY") }),
        "the ranking window computes locally after the partition shuffle: {dq:?}"
    );
    drop(dq);
    assert_distributed_matches_single_node(Q47, &REPL_STORE_SALES, "store_sales").await;
}

#[tokio::test]
async fn q57_stacked_windows_plan_and_match() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = strict_plan(&planner, Q57, &REPL_CATALOG_SALES).await;
    assert!(
        dq.stages
            .iter()
            .any(|s| { s.sql.contains("rank() OVER (PARTITION BY") && s.sql.contains("ORDER BY") }),
        "the ranking window computes locally after the partition shuffle: {dq:?}"
    );
    drop(dq);
    assert_distributed_matches_single_node(Q57, &REPL_CATALOG_SALES, "catalog_sales").await;
}

// --- Q75: aggregate over a DISTINCT union with mixed sharding ---

#[tokio::test]
async fn q75_distinct_union_mixed_sharding_plans_and_matches() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = strict_plan(&planner, Q75, &REPL_STORE_SALES).await;
    assert!(
        dq.stages.iter().any(|s| s.sql.contains("SELECT DISTINCT")),
        "the co-located dedup stage exists: {dq:?}"
    );
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.sql.contains("__weft_materialize_gate")
                || s.sql.contains("__weft_subquery_gate")),
        "no whole-fact gather: {dq:?}"
    );
    drop(dq);
    assert_distributed_matches_single_node(Q75, &REPL_STORE_SALES, "store_sales").await;
}
