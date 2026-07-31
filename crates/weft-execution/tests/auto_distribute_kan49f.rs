//! KAN-49 wave-3f: the two TPC-DS queries that plan under the local per-fact sweep but still
//! refuse at the real SF10 size-based classification, where the query's **largest** table is
//! the sharded one and every other table replicates.
//!
//! - **Q23** (`store_sales` sharded): three CTEs all over the sharded fact —
//!   `frequent_ss_items` (grouped, `HAVING count(*) > 4`), `max_store_sales` (scalar `max` of a
//!   per-customer aggregate), `best_ss_customer` (per-customer aggregate with
//!   `HAVING sum > 0.5 * max(tpcds_cmax)` against the single-row scalar CTE) — then a
//!   `UNION ALL` of a catalog and a web arm that each join those CTEs against replicated
//!   channel/dimension tables.
//! - **Q41** (`item` sharded): `SELECT DISTINCT i_product_name FROM item i1 WHERE … AND
//!   (SELECT count(*) FROM item WHERE i_manufact = i1.i_manufact AND <OR-of-conjuncts>) > 0` —
//!   an equality-correlated scalar count with a disjunctive residual and a DISTINCT outer. The
//!   local sweep never tries `item` as the sharded table (it is not in `FACT_TABLES`), so this
//!   refusal only appears under the SF10 classification.
//!
//! Both tests pin the SF10 configuration explicitly (everything replicated except the query's
//! driving fact) and require the distributed plan to equal single-node end-to-end in strict
//! mode (`WEFT_DISTRIBUTED_STRICT=1`), so the whole-fact gather cannot silently substitute.

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

const Q23: &str = include_str!("../../../bench/tpcds/queries/q23.sql");
const Q41: &str = include_str!("../../../bench/tpcds/queries/q41.sql");

/// The SF10 post-classification configuration per query: only the query's driving fact is
/// sharded; every other table it reads is replicated.
const REPL_Q23: [&str; 5] = ["catalog_sales", "web_sales", "customer", "date_dim", "item"];
/// Q41 reads only `item`, so the replicated set is empty (item itself stays sharded).
const REPL_Q41: [&str; 0] = [];

static PORT: std::sync::OnceLock<AtomicU16> = std::sync::OnceLock::new();

fn unique_worker_port() -> u16 {
    // OnceLock-seeded allocator with the base BELOW the Linux ephemeral source range
    // (32768..=60999): the harness's own outbound connections can never steal a worker's
    // port (serve_worker swallows EADDRINUSE; the old in-range bases flaked "did not
    // bind" / "distributed run never succeeded" on loaded CI runners).
    PORT.get_or_init(|| AtomicU16::new(22000 + (std::process::id() as u16 % 512)))
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

/// One `item` table serves both queries. Q41 reads the manufact/category/color/units/size
/// columns; Q23 joins `ss_item_sk = i_item_sk` and groups on `SUBSTRING(i_item_desc, 1, 30)`.
///
/// Q41 rows (in shard order): `alpha` (manufact 300, matches no OR branch itself — it passes
/// only because `beta`, which lands on the *other* shard, shares manufact 300 and matches),
/// `gamma` (manufact 400, nobody matches → excluded), `beta` (manufact 300, matches branch 1),
/// `delta` (matches a branch but `i_manufact_id` 900 is outside the BETWEEN window → excluded).
/// Expected Q41 output: alpha, beta — and alpha only survives when the correlated count sees
/// both shards.
fn item() -> RecordBatch {
    batch(
        vec![
            i64f("i_item_sk"),
            strf("i_item_desc"),
            strf("i_product_name"),
            i64f("i_manufact_id"),
            i64f("i_manufact"),
            strf("i_category"),
            strf("i_color"),
            strf("i_units"),
            strf("i_size"),
        ],
        vec![
            i64v(&[1, 3, 2, 4]),
            strv(&["desc-one", "desc-three", "desc-two", "desc-four"]),
            strv(&["alpha", "gamma", "beta", "delta"]),
            i64v(&[740, 760, 750, 900]),
            i64v(&[300, 400, 300, 500]),
            strv(&["Kids", "Kids", "Women", "Women"]),
            strv(&["powder", "plum", "khaki", "powder"]),
            strv(&["Ounce", "Ounce", "Oz", "Ounce"]),
            strv(&["medium", "small", "medium", "medium"]),
        ],
    )
}

/// d_date_sk 1 sits in Q23's outer window (d_year 2000, d_moy 2) and the CTE year range; sk 2
/// is 2000-03 (outside d_moy 2 for the outer arms, inside the CTE year range).
fn date_dim() -> RecordBatch {
    batch(
        vec![
            i64f("d_date_sk"),
            datef("d_date"),
            i64f("d_year"),
            i64f("d_moy"),
        ],
        vec![
            i64v(&[1, 2]),
            // 2000-02-10, 2000-03-01.
            datev(&[10997, 11017]),
            i64v(&[2000, 2000]),
            i64v(&[2, 3]),
        ],
    )
}

fn customer() -> RecordBatch {
    batch(
        vec![
            i64f("c_customer_sk"),
            strf("c_last_name"),
            strf("c_first_name"),
        ],
        vec![
            i64v(&[1, 2, 3]),
            strv(&["Smith", "Jones", "Zed"]),
            strv(&["Ann", "Bob", "Zoey"]),
        ],
    )
}

/// Sharded fact for Q23 (shard order chosen so every group straddles the two halves):
/// five (item 1, date 1) rows make the `frequent_ss_items` group (count 5 > 4 — 2 rows land on
/// shard 0, 3 on shard 1, so no per-shard count passes the HAVING), customer 1's five sales of
/// 10 sum to 50 while customer 2's single 1000 sale is the max, so the 0.5·max threshold keeps
/// only customer 2. The `desc-three` row (item 3, customer 3) is a negative control: its
/// (item, date) group has count 1 and customer 3's sum is below the threshold.
fn store_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ss_sold_date_sk"),
            i64f("ss_item_sk"),
            i64f("ss_customer_sk"),
            i64f("ss_quantity"),
            f64f("ss_sales_price"),
        ],
        vec![
            i64v(&[1, 1, 1, 1, 1, 1, 2]),
            i64v(&[1, 1, 2, 1, 1, 1, 3]),
            i64v(&[1, 1, 2, 1, 1, 1, 3]),
            i64v(&[1, 1, 100, 1, 1, 1, 5]),
            f64v(&[10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 2.0]),
        ],
    )
}

/// Replicated channel tables for Q23's outer arms: one row each joins the frequent item and
/// the best customer inside the d_year=2000/d_moy=2 window (sales 2·5=10 catalog, 3·7=21 web);
/// the second catalog row bills customer 1 (below the threshold) and the second web row sells
/// item 2 (not frequent) — both must be dropped.
fn catalog_sales() -> RecordBatch {
    batch(
        vec![
            i64f("cs_sold_date_sk"),
            i64f("cs_item_sk"),
            i64f("cs_bill_customer_sk"),
            i64f("cs_quantity"),
            f64f("cs_list_price"),
        ],
        vec![
            i64v(&[1, 1]),
            i64v(&[1, 1]),
            i64v(&[2, 1]),
            i64v(&[2, 9]),
            f64v(&[5.0, 5.0]),
        ],
    )
}

fn web_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ws_sold_date_sk"),
            i64f("ws_item_sk"),
            i64f("ws_bill_customer_sk"),
            i64f("ws_quantity"),
            f64f("ws_list_price"),
        ],
        vec![
            i64v(&[1, 1]),
            i64v(&[1, 2]),
            i64v(&[2, 2]),
            i64v(&[3, 4]),
            f64v(&[7.0, 7.0]),
        ],
    )
}

fn register(engine: &Engine, name: &str, batches: Vec<RecordBatch>) {
    engine.register_batches(name, batches).unwrap();
}

/// Every table the two queries touch, in full.
fn all_tables() -> Vec<(&'static str, RecordBatch)> {
    vec![
        ("item", item()),
        ("date_dim", date_dim()),
        ("customer", customer()),
        ("store_sales", store_sales()),
        ("catalog_sales", catalog_sales()),
        ("web_sales", web_sales()),
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
        "item" => item(),
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

/// The whole-fact gather stages this wave replaces must not reappear.
fn assert_no_whole_fact_gather(dq: &weft_execution::plan::DistributedQuery, fact: &str) {
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.sql == format!("SELECT * FROM {fact}")),
        "no bare whole-fact scan stage: {dq:?}"
    );
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.sql.contains("__weft_materialize_gate")
                || s.sql.contains("__weft_subquery_gate")),
        "no whole-fact gather gate: {dq:?}"
    );
}

// --- Q23: three sharded-fact CTEs feeding a replicated per-channel UNION ALL ---

#[tokio::test]
async fn q23_three_cte_union_plans_and_matches() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = strict_plan(&planner, Q23, &REPL_Q23).await;
    assert_no_whole_fact_gather(&dq, "store_sales");
    drop(dq);
    assert_distributed_matches_single_node(Q23, &REPL_Q23, "store_sales").await;
}

// --- Q41: DISTINCT outer over an equality-correlated scalar count on the sharded table ---

#[tokio::test]
async fn q41_distinct_correlated_count_plans_and_matches() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = strict_plan(&planner, Q41, &REPL_Q41).await;
    assert_no_whole_fact_gather(&dq, "item");
    drop(dq);
    assert_distributed_matches_single_node(Q41, &REPL_Q41, "item").await;
}
