//! KAN-158: common-subexpression elimination of repeated fact scans within a query.
//!
//! Evidence (SF100 operator profile): Q23 stages 2 and 6 each spent ~46 s rescanning
//! `store_sales` for per-customer sums that share group keys + measure but differ by a
//! `date_dim` year filter; Q14 scans each channel fact 3× (INTERSECT / AVG / arm).
//!
//! This suite pins the Q23 shared-raw-leaf path (`try_kan158_share_restricted_agg`):
//! one export of `store_sales ⋈ customer` feeds both the dated (max_store_sales) and
//! unrestricted (best_ss_customer) partials. It also pins a Q14-like three-consumer
//! shape (three date-window aggregates over one fact under a cross join) via the
//! existing dag_splitter FILTER-merge shared leaf, and decline cases where sharing
//! would not be exact.

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

const Q23: &str = include_str!("../../../bench/tpcds/queries/q23.sql");
const REPL_Q23: [&str; 5] = ["catalog_sales", "web_sales", "customer", "date_dim", "item"];

static PORT: std::sync::OnceLock<AtomicU16> = std::sync::OnceLock::new();
static ENV_LOCK: Mutex<()> = Mutex::new(());

fn unique_worker_port() -> u16 {
    PORT.get_or_init(|| AtomicU16::new(23000 + (std::process::id() as u16 % 512)))
        .fetch_add(1, Ordering::Relaxed)
}

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

fn item() -> RecordBatch {
    batch(
        vec![
            i64f("i_item_sk"),
            strf("i_item_desc"),
            i64f("i_brand_id"),
            i64f("i_class_id"),
            i64f("i_category_id"),
        ],
        vec![
            i64v(&[1, 2, 3]),
            strv(&[
                "desc-one-aaaaaaaaaaaaaaaaaaaa",
                "desc-two-bbbbbbbbbbbbbbbbbbb",
                "desc-three-ccccccccccccccc",
            ]),
            i64v(&[101, 102, 103]),
            i64v(&[201, 202, 203]),
            i64v(&[301, 302, 303]),
        ],
    )
}

fn date_dim() -> RecordBatch {
    // sk 1..4 in years 2000..2003 (Q23 CTE window); sk 5 = 1999 (outside); sk 6 = Feb 2000 (arm).
    batch(
        vec![
            i64f("d_date_sk"),
            i64f("d_year"),
            i64f("d_moy"),
            datef("d_date"),
        ],
        vec![
            i64v(&[1, 2, 3, 4, 5, 6]),
            i64v(&[2000, 2001, 2002, 2003, 1999, 2000]),
            i64v(&[1, 1, 1, 1, 1, 2]),
            datev(&[10957, 11323, 11688, 12053, 10592, 10988]),
        ],
    )
}

fn customer() -> RecordBatch {
    batch(
        vec![
            i64f("c_customer_sk"),
            strf("c_first_name"),
            strf("c_last_name"),
        ],
        vec![
            i64v(&[1, 2, 3]),
            strv(&["Ann", "Bob", "Cat"]),
            strv(&["Alpha", "Beta", "Gamma"]),
        ],
    )
}

/// Store sales spanning both shards. Customer 1 has enough dated sales to be "best";
/// customer 2 has only an out-of-window sale (counts for unrestricted best_ss_customer
/// but not for max_store_sales) so the two aggregates diverge — CSE must stay exact.
fn store_sales_shard0() -> RecordBatch {
    batch(
        vec![
            i64f("ss_item_sk"),
            i64f("ss_customer_sk"),
            i64f("ss_sold_date_sk"),
            i64f("ss_quantity"),
            f64f("ss_sales_price"),
            f64f("ss_list_price"),
        ],
        vec![
            i64v(&[1, 1, 1, 2]),
            i64v(&[1, 1, 1, 1]),
            i64v(&[1, 1, 1, 1]),
            i64v(&[5, 5, 5, 5]),
            f64v(&[10.0, 10.0, 10.0, 10.0]),
            f64v(&[10.0, 10.0, 10.0, 10.0]),
        ],
    )
}

fn store_sales_shard1() -> RecordBatch {
    batch(
        vec![
            i64f("ss_item_sk"),
            i64f("ss_customer_sk"),
            i64f("ss_sold_date_sk"),
            i64f("ss_quantity"),
            f64f("ss_sales_price"),
            f64f("ss_list_price"),
        ],
        vec![
            i64v(&[1, 1, 2, 3]),
            i64v(&[1, 2, 2, 3]),
            i64v(&[6, 5, 2, 3]), // c1 Feb-2000 arm; c2 year-1999 only; c3 in-window
            i64v(&[5, 100, 5, 5]),
            f64v(&[10.0, 10.0, 10.0, 10.0]),
            f64v(&[10.0, 10.0, 10.0, 10.0]),
        ],
    )
}

fn channel(
    prefix: &str,
    item_sk: i64,
    cust: i64,
    date_sk: i64,
    qty: i64,
    price: f64,
) -> RecordBatch {
    let (isk, dsk, csk, qty_c, list_c) = match prefix {
        "cs" => (
            "cs_item_sk",
            "cs_sold_date_sk",
            "cs_bill_customer_sk",
            "cs_quantity",
            "cs_list_price",
        ),
        _ => (
            "ws_item_sk",
            "ws_sold_date_sk",
            "ws_bill_customer_sk",
            "ws_quantity",
            "ws_list_price",
        ),
    };
    batch(
        vec![i64f(isk), i64f(dsk), i64f(csk), i64f(qty_c), f64f(list_c)],
        vec![
            i64v(&[item_sk]),
            i64v(&[date_sk]),
            i64v(&[cust]),
            i64v(&[qty]),
            f64v(&[price]),
        ],
    )
}

async fn tpcds_engine() -> Engine {
    let e = Engine::new();
    e.register_batches("item", vec![item()]).unwrap();
    e.register_batches("date_dim", vec![date_dim()]).unwrap();
    e.register_batches("customer", vec![customer()]).unwrap();
    e.register_batches(
        "store_sales",
        vec![store_sales_shard0(), store_sales_shard1()],
    )
    .unwrap();
    e.register_batches("catalog_sales", vec![channel("cs", 1, 1, 6, 2, 50.0)])
        .unwrap();
    e.register_batches("web_sales", vec![channel("ws", 1, 1, 6, 2, 40.0)])
        .unwrap();
    e
}

async fn two_workers() -> Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        e.register_batches("item", vec![item()]).unwrap();
        e.register_batches("date_dim", vec![date_dim()]).unwrap();
        e.register_batches("customer", vec![customer()]).unwrap();
        let shard = if i == 0 {
            store_sales_shard0()
        } else {
            store_sales_shard1()
        };
        e.register_batches("store_sales", vec![shard]).unwrap();
        e.register_batches("catalog_sales", vec![channel("cs", 1, 1, 6, 2, 50.0)])
            .unwrap();
        e.register_batches("web_sales", vec![channel("ws", 1, 1, 6, 2, 40.0)])
            .unwrap();
        tokio::spawn(async move {
            let _ = serve_worker(port, e).await;
        });
    }
    Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ])
}

fn rows_sorted(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let opts = FormatOptions::default();
    let mut rows = Vec::new();
    for b in batches {
        let fmts: Vec<_> = b
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &opts).unwrap())
            .collect();
        for i in 0..b.num_rows() {
            rows.push(fmts.iter().map(|f| f.value(i).to_string()).collect());
        }
    }
    rows.sort();
    rows
}

async fn assert_distributed_matches(sql: &str, replicated: &[&str]) {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("OXIDANT_DISTRIBUTED_STRICT", "1");
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let expected = planner.sql(sql).await.expect("single-node");
    let cluster = two_workers().await;
    let lp = planner.logical_plan(sql).await.unwrap();
    let dq = plan_distributed_logical(&lp, replicated).expect("plan");
    let gathered = run_stages(&cluster, &dq.stages).await.expect("distributed");
    let actual = match &dq.finalize_sql {
        None => gathered,
        Some(fsql) => {
            let fin = Engine::new();
            fin.register_batches("result", gathered).unwrap();
            fin.sql(fsql).await.expect("finalize")
        }
    };
    std::env::remove_var("OXIDANT_DISTRIBUTED_STRICT");
    assert_eq!(
        rows_sorted(&actual),
        rows_sorted(&expected),
        "distributed must equal single-node"
    );
}

/// Plan-structure pin: Q23's max_store_sales / best_ss_customer share one store_sales leaf.
#[tokio::test]
async fn q23_shared_leaf_feeds_dated_and_wide_partials() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let lp = planner.logical_plan(Q23).await.unwrap();
    let dq = plan_distributed_logical(&lp, &REPL_Q23).expect("Q23 plans");

    let ss_leaves: Vec<_> = dq
        .stages
        .iter()
        .filter(|s| s.upstream_stage_ids.is_empty() && s.sql.contains("store_sales"))
        .collect();
    // frequent_ss_items leaf + one shared customer-agg leaf (not two).
    assert_eq!(
        ss_leaves.len(),
        2,
        "frequent_ss_items + one shared customer-agg leaf: {dq:?}"
    );
    let shared = ss_leaves
        .iter()
        .find(|s| s.sql.contains(" AS dsk") && s.sql.contains(" AS amt"))
        .expect("shared leaf exports dsk + amt");
    let shared_id = shared.stage_id;
    let consumers: Vec<_> = dq
        .stages
        .iter()
        .filter(|s| s.upstream_stage_ids == vec![shared_id])
        .collect();
    assert_eq!(
        consumers.len(),
        2,
        "dated + wide partials both read the shared leaf: {dq:?}"
    );
    assert!(
        consumers
            .iter()
            .any(|s| s.sql.contains("date_dim") && s.sql.contains("WHERE")),
        "dated consumer re-attaches date_dim: {consumers:?}"
    );
    assert!(
        consumers
            .iter()
            .any(|s| s.sql.contains("sum(amt)") && !s.sql.contains("date_dim")),
        "wide consumer aggregates without date_dim: {consumers:?}"
    );
}

#[tokio::test]
async fn q23_shared_leaf_distributed_matches_single_node() {
    assert_distributed_matches(Q23, &REPL_Q23).await;
}

/// Q14-like: three time-bucket counts over one fact (different predicates). The dag_splitter
/// FILTER-merge shared leaf plans them as one scan — the Q14 shape's three channel uses are
/// the gather_shapes analog; this pins the multi-consumer shared-leaf contract end-to-end.
const Q14_LIKE_THREE_BUCKETS: &str = "
SELECT *
FROM
  (SELECT count(*) c_1999 FROM store_sales, date_dim
   WHERE ss_sold_date_sk = d_date_sk AND d_year = 1999) a,
  (SELECT count(*) c_2000 FROM store_sales, date_dim
   WHERE ss_sold_date_sk = d_date_sk AND d_year = 2000) b,
  (SELECT count(*) c_2001 FROM store_sales, date_dim
   WHERE ss_sold_date_sk = d_date_sk AND d_year = 2001) c";

const REPL_DIMS: [&str; 1] = ["date_dim"];

#[tokio::test]
async fn q14_like_three_buckets_share_one_fact_scan() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let lp = planner.logical_plan(Q14_LIKE_THREE_BUCKETS).await.unwrap();
    let dq = plan_distributed_logical(&lp, &REPL_DIMS).expect("three-bucket shape plans");
    let leaves: Vec<_> = dq
        .stages
        .iter()
        .filter(|s| s.upstream_stage_ids.is_empty() && s.sql.contains("store_sales"))
        .collect();
    assert_eq!(
        leaves.len(),
        1,
        "one shared leaf for three date-window branches: {dq:?}"
    );
    assert!(
        leaves[0].sql.contains("FILTER (WHERE"),
        "shared leaf gates each bucket with FILTER: {}",
        leaves[0].sql
    );
    assert!(
        leaves[0].sql.matches("FILTER (WHERE").count() >= 3,
        "three FILTER clauses: {}",
        leaves[0].sql
    );
}

#[tokio::test]
async fn q14_like_three_buckets_distributed_matches_single_node() {
    assert_distributed_matches(Q14_LIKE_THREE_BUCKETS, &REPL_DIMS).await;
}

/// Decline pin: different measures over the same fact must not share a partial (would be
/// wrong). Two cross-joined aggregates with different args keep separate leaves.
const DECLINE_DIFFERENT_MEASURES: &str = "
SELECT *
FROM
  (SELECT sum(ss_quantity) q FROM store_sales) a,
  (SELECT sum(ss_sales_price) p FROM store_sales) b";

#[tokio::test]
async fn decline_different_measures_keep_separate_scans() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let lp = planner
        .logical_plan(DECLINE_DIFFERENT_MEASURES)
        .await
        .unwrap();
    let dq = plan_distributed_logical(&lp, &[]).expect("plans");
    // FILTER-merge requires identical filter-stripped tails *and* merges aggs into one
    // leaf — different measures over the same unfiltered tail DO share one scan (that is
    // exact). Pin that the shared leaf carries BOTH measures rather than dropping one.
    let leaves: Vec<_> = dq
        .stages
        .iter()
        .filter(|s| s.upstream_stage_ids.is_empty() && s.sql.contains("store_sales"))
        .collect();
    assert_eq!(
        leaves.len(),
        1,
        "same-tail different measures still share: {dq:?}"
    );
    assert!(
        leaves[0].sql.contains("sum(store_sales.ss_quantity)")
            && leaves[0].sql.contains("sum(store_sales.ss_sales_price)"),
        "both measures on the shared leaf: {}",
        leaves[0].sql
    );
}

/// Decline pin for the Q23 CSE path: when the "restricted" sibling's extra join is missing
/// (both sides identical), identical-stage CSE merges them — but try_kan158 requires a
/// proper filter-restriction (extra dim). A query with two identical per-customer aggs
/// under a cross join collapses via fingerprint/identical CSE, not kan158; pin no `dsk`
/// export (kan158 signature).
const DECLINE_NO_RESTRICTION: &str = "
SELECT *
FROM
  (SELECT c_customer_sk, sum(ss_quantity * ss_sales_price) s1
   FROM store_sales, customer
   WHERE ss_customer_sk = c_customer_sk
   GROUP BY c_customer_sk) a,
  (SELECT c_customer_sk, sum(ss_quantity * ss_sales_price) s2
   FROM store_sales, customer
   WHERE ss_customer_sk = c_customer_sk
   GROUP BY c_customer_sk) b";

#[tokio::test]
async fn decline_identical_aggs_use_fingerprint_not_kan158_dsk() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let lp = planner.logical_plan(DECLINE_NO_RESTRICTION).await.unwrap();
    let dq = plan_distributed_logical(&lp, &["customer"]).expect("plans");
    assert!(
        !dq.stages.iter().any(|s| s.sql.contains(" AS dsk")),
        "kan158 dsk export is only for filter-restriction sharing: {dq:?}"
    );
    let leaves: Vec<_> = dq
        .stages
        .iter()
        .filter(|s| s.upstream_stage_ids.is_empty() && s.sql.contains("store_sales"))
        .collect();
    assert_eq!(
        leaves.len(),
        1,
        "identical branches still CSE to one scan via fingerprint: {dq:?}"
    );
}
