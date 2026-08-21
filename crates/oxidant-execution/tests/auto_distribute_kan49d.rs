//! KAN-49 wave-4 (union/set-ops): ROLLUP over a per-channel `UNION ALL` and
//! ROLLUP + INTERSECT — the TPC-DS queries the KAN-54 union split deliberately kept
//! refusing because "per-arm `Forward` placement composed with the grouping-set gather
//! returned wrong answers" (Q5/Q77/Q80) and the subquery-over-sharded INTERSECT shape
//! (Q14).
//!
//! Shapes under test:
//!
//! - **ROLLUP over mixed sharded/replicated `UNION ALL` arms** (Q5/Q77/Q80): three channel
//!   arms under one `GROUP BY ROLLUP (channel, id)`. Exactly one arm scans the sharded fact;
//!   the others are fully replicated. `try_split_broadcast_union` buckets the arms by
//!   sharding: the sharded arm runs as a per-worker partial at the *finest* grouping level,
//!   the replicated arms compute once (`ExchangeMode::Forward`), and the combine rebuilds the
//!   ROLLUP levels over the merged, gathered partials — grouping-set levels co-locate because
//!   the finest-level partials all gather to partition 0 (empty hash key), so the recombine
//!   sees every group whole.
//! - **ROLLUP + INTERSECT over subqueries on the sharded fact** (Q14): distributed by
//!   `try_rollup_union_derived_subqueries` — the `cross_items` INTERSECT chain becomes full-row
//!   key-shuffles of its raw arms plus a broadcast join-back (a materialized `item_sk` key
//!   stream), the mixed-sharding global AVG becomes per-partition partials + a gathered one-row
//!   combine, each channel arm runs a scan-export / co-located semi / partial aggregate whose
//!   gathered recombine applies the HAVING against the co-located scalar row, and the outer
//!   ROLLUP recombine closes over the three exact arm streams.
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

const Q5: &str = include_str!("../../../bench/tpcds/queries/q5.sql");
const Q14: &str = include_str!("../../../bench/tpcds/queries/q14.sql");
const Q77: &str = include_str!("../../../bench/tpcds/queries/q77.sql");
const Q80: &str = include_str!("../../../bench/tpcds/queries/q80.sql");

/// The SF10 post-classification configuration for Q5/Q77/Q80: only `store_sales` (the
/// largest table each query touches) is sharded; the smaller channels, returns, and every
/// dimension are replicated.
const REPL_STORE_SALES: [&str; 13] = [
    "date_dim",
    "store",
    "item",
    "promotion",
    "catalog_page",
    "web_page",
    "web_site",
    "store_returns",
    "catalog_sales",
    "catalog_returns",
    "web_sales",
    "web_returns",
    "call_center",
];

/// Q14's alternate configuration: `web_sales` sharded, every other table replicated — the
/// sharded-arm role moves to the web channel in `cross_items`, `avg_sales`, and the UNION arm.
const REPL_WEB_SALES: [&str; 13] = [
    "date_dim",
    "store",
    "item",
    "promotion",
    "catalog_page",
    "web_page",
    "web_site",
    "store_sales",
    "store_returns",
    "catalog_sales",
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
    PORT.get_or_init(|| AtomicU16::new(20000 + (std::process::id() as u16 % 512)))
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

/// (d_date_sk 1..=9):
///   1 = 2000-08-23 (Q77/Q80 window open, Q5 window open),
///   2 = 2000-09-01, 3 = 2000-09-06 (Q5 window close),
///   4 = 2000-09-22 (Q77/Q80 window close, outside Q5),
///   5 = 2001-11-15 (Q14's main month: d_year 2001, d_moy 11),
///   6 = 1999-03-10, 7 = 2000-06-15, 8 = 2001-05-20 (Q14's 1999..2001 window, not Nov),
///   9 = 1998-01-01 (outside every window — control row that must always be filtered).
fn date_dim() -> RecordBatch {
    batch(
        vec![
            i64f("d_date_sk"),
            datef("d_date"),
            i64f("d_year"),
            i64f("d_moy"),
        ],
        vec![
            i64v(&[1, 2, 3, 4, 5, 6, 7, 8, 9]),
            datev(&[
                11192, 11201, 11206, 11222, 11641, 10660, 11123, 11462, 10227,
            ]),
            i64v(&[2000, 2000, 2000, 2000, 2001, 1999, 2000, 2001, 1998]),
            i64v(&[8, 9, 9, 9, 11, 3, 6, 5, 1]),
        ],
    )
}

fn store() -> RecordBatch {
    batch(
        vec![i64f("s_store_sk"), strf("s_store_id")],
        vec![i64v(&[1, 2]), strv(&["store1", "store2"])],
    )
}

/// Items 1/2 share Q14's surviving (brand, class, category) triple; item 3's triple is sold in
/// all three channels inside Q14's 1999..2001 window (so it joins `cross_items`) but its only
/// November row is a tiny web sale the `avg_sales` HAVING threshold cuts; item 4 fails Q80's
/// `i_current_price > 50` filter.
fn item() -> RecordBatch {
    batch(
        vec![
            i64f("i_item_sk"),
            f64f("i_current_price"),
            i64f("i_brand_id"),
            i64f("i_class_id"),
            i64f("i_category_id"),
        ],
        vec![
            i64v(&[1, 2, 3, 4]),
            f64v(&[60.0, 75.0, 80.0, 10.0]),
            i64v(&[100, 100, 101, 102]),
            i64v(&[200, 200, 201, 202]),
            i64v(&[300, 300, 301, 302]),
        ],
    )
}

fn promotion() -> RecordBatch {
    batch(
        vec![i64f("p_promo_sk"), strf("p_channel_tv")],
        vec![i64v(&[1, 2]), strv(&["N", "Y"])],
    )
}

fn catalog_page() -> RecordBatch {
    batch(
        vec![i64f("cp_catalog_page_sk"), strf("cp_catalog_page_id")],
        vec![i64v(&[1, 2]), strv(&["cpage1", "cpage2"])],
    )
}

fn web_page() -> RecordBatch {
    batch(vec![i64f("wp_web_page_sk")], vec![i64v(&[1, 2])])
}

fn web_site() -> RecordBatch {
    batch(
        vec![i64f("web_site_sk"), strf("web_site_id")],
        vec![i64v(&[1, 2]), strv(&["site1", "site2"])],
    )
}

fn call_center() -> RecordBatch {
    batch(
        vec![i64f("cc_call_center_sk"), strf("cc_name")],
        vec![i64v(&[1, 2]), strv(&["cc1", "cc2"])],
    )
}

/// Sharded fact for Q5/Q77/Q80 (replicated for Q14), 9 rows: shard0 = rows 0-3,
/// shard1 = rows 4-8. Store 1's in-window group deliberately spans the shards (rows 0/2 on
/// shard0, row 4 on shard1), so a per-worker partial that failed to recombine would show.
/// Row 3 (1998) is outside every window; row 5 carries promotion 2 (fails Q80's
/// `p_channel_tv = 'N'`) and date_sk 4 (outside Q5's window). Row 8 sells item 3 inside Q14's
/// 1999..2001 window (2000-06-15, not November), which puts item 3's triple into `cross_items`
/// without touching the store channel arm.
fn store_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ss_sold_date_sk"),
            i64f("ss_item_sk"),
            i64f("ss_store_sk"),
            i64f("ss_promo_sk"),
            i64f("ss_ticket_number"),
            i64f("ss_quantity"),
            f64f("ss_list_price"),
            f64f("ss_ext_sales_price"),
            f64f("ss_net_profit"),
        ],
        vec![
            i64v(&[1, 2, 3, 9, 2, 4, 5, 6, 7]),
            i64v(&[1, 2, 1, 1, 1, 2, 1, 1, 3]),
            i64v(&[1, 2, 1, 1, 1, 2, 1, 1, 1]),
            i64v(&[1, 1, 1, 1, 1, 2, 1, 1, 1]),
            i64v(&[100, 101, 102, 103, 104, 105, 106, 107, 108]),
            i64v(&[2, 3, 1, 100, 4, 5, 6, 1, 1]),
            f64v(&[10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 5.0, 10.0]),
            f64v(&[20.0, 30.0, 10.0, 1000.0, 40.0, 50.0, 60.0, 5.0, 10.0]),
            f64v(&[5.0, 6.0, 2.0, 100.0, 8.0, 10.0, 12.0, 1.0, 1.0]),
        ],
    )
}

/// Returns rows: tickets 100/104 match store_sales rows on *different* shards (the Q80 LEFT
/// JOIN must see both), ticket 105 matches the promotion-2 sale, ticket 999 matches nothing
/// (Q77's independent `sr` CTE keeps it; Q80's LEFT JOIN from sales never sees it), and the
/// 1998 row is out of window.
fn store_returns() -> RecordBatch {
    batch(
        vec![
            i64f("sr_returned_date_sk"),
            i64f("sr_store_sk"),
            i64f("sr_item_sk"),
            i64f("sr_ticket_number"),
            f64f("sr_return_amt"),
            f64f("sr_net_loss"),
        ],
        vec![
            i64v(&[2, 3, 4, 9, 3]),
            i64v(&[1, 1, 2, 1, 1]),
            i64v(&[1, 1, 2, 1, 9]),
            i64v(&[100, 104, 105, 100, 999]),
            f64v(&[3.0, 4.0, 6.0, 999.0, 7.0]),
            f64v(&[1.0, 2.0, 3.0, 999.0, 1.5]),
        ],
    )
}

fn catalog_sales() -> RecordBatch {
    batch(
        vec![
            i64f("cs_sold_date_sk"),
            i64f("cs_item_sk"),
            i64f("cs_call_center_sk"),
            i64f("cs_catalog_page_sk"),
            i64f("cs_promo_sk"),
            i64f("cs_order_number"),
            i64f("cs_quantity"),
            f64f("cs_list_price"),
            f64f("cs_ext_sales_price"),
            f64f("cs_net_profit"),
        ],
        vec![
            i64v(&[1, 2, 3, 5, 6]),
            i64v(&[1, 2, 1, 1, 3]),
            i64v(&[1, 1, 2, 1, 1]),
            i64v(&[1, 1, 2, 1, 1]),
            i64v(&[1, 1, 1, 1, 1]),
            i64v(&[200, 201, 202, 203, 204]),
            i64v(&[2, 3, 1, 5, 2]),
            f64v(&[10.0, 10.0, 10.0, 10.0, 5.0]),
            f64v(&[20.0, 30.0, 10.0, 50.0, 10.0]),
            f64v(&[4.0, 5.0, 1.0, 9.0, 2.0]),
        ],
    )
}

fn catalog_returns() -> RecordBatch {
    batch(
        vec![
            i64f("cr_returned_date_sk"),
            i64f("cr_call_center_sk"),
            i64f("cr_catalog_page_sk"),
            i64f("cr_item_sk"),
            i64f("cr_order_number"),
            f64f("cr_return_amount"),
            f64f("cr_net_loss"),
        ],
        vec![
            i64v(&[2, 3]),
            i64v(&[1, 2]),
            i64v(&[1, 2]),
            i64v(&[1, 1]),
            i64v(&[200, 202]),
            f64v(&[2.0, 3.0]),
            f64v(&[1.0, 1.0]),
        ],
    )
}

/// Replicated for Q5/Q77/Q80, sharded for Q14 (shard0 = rows 0-3, shard1 = rows 4-8).
/// Web page 1 / site 1's in-window group spans the shards (rows 0/1 vs row 4); the Q14 main
/// month row (row 5) sits on shard1 so Q14's semi-join must reach across workers. Row 3 is
/// the 1998 control row; row 2 (2000-06-15) is inside Q14's year window but outside the
/// Aug/Sep channel windows. Row 7 (2001-05-20) is item 3's in-window evidence for
/// `cross_items`; row 8 is item 3's only November row — a 5.00 sale, below the global
/// `avg_sales`, so Q14's HAVING cuts the web arm's (101, 201, 301) group while keeping it in
/// the other channels' lineage.
fn web_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ws_sold_date_sk"),
            i64f("ws_item_sk"),
            i64f("ws_web_page_sk"),
            i64f("ws_web_site_sk"),
            i64f("ws_promo_sk"),
            i64f("ws_order_number"),
            i64f("ws_quantity"),
            f64f("ws_list_price"),
            f64f("ws_ext_sales_price"),
            f64f("ws_net_profit"),
        ],
        vec![
            i64v(&[1, 2, 7, 9, 2, 5, 6, 8, 5]),
            i64v(&[1, 1, 2, 1, 1, 1, 2, 3, 3]),
            i64v(&[1, 1, 2, 1, 1, 2, 1, 2, 1]),
            i64v(&[1, 1, 2, 1, 1, 2, 1, 2, 1]),
            i64v(&[1, 1, 1, 1, 1, 1, 1, 1, 1]),
            i64v(&[300, 301, 302, 303, 304, 305, 306, 307, 308]),
            i64v(&[2, 1, 4, 100, 3, 6, 2, 1, 1]),
            f64v(&[10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 5.0, 20.0, 5.0]),
            f64v(&[20.0, 10.0, 40.0, 1000.0, 30.0, 60.0, 10.0, 20.0, 5.0]),
            f64v(&[3.0, 1.0, 7.0, 100.0, 5.0, 11.0, 2.0, 4.0, 1.0]),
        ],
    )
}

/// Orders 300/304 join back to web_sales rows on different shards (Q5's returns arm resolves
/// the web site through that LEFT JOIN); order 999 matches nothing, so Q5's arm-2 LEFT JOIN
/// emits a NULL site that the `web_site` equijoin then drops.
fn web_returns() -> RecordBatch {
    batch(
        vec![
            i64f("wr_returned_date_sk"),
            i64f("wr_web_page_sk"),
            i64f("wr_item_sk"),
            i64f("wr_order_number"),
            f64f("wr_return_amt"),
            f64f("wr_net_loss"),
        ],
        vec![
            i64v(&[2, 3, 3]),
            i64v(&[1, 1, 2]),
            i64v(&[1, 1, 9]),
            i64v(&[300, 304, 999]),
            f64v(&[2.0, 3.0, 5.0]),
            f64v(&[1.0, 1.0, 2.0]),
        ],
    )
}

fn register(engine: &Engine, name: &str, batches: Vec<RecordBatch>) {
    engine.register_batches(name, batches).unwrap();
}

/// Every table the four queries touch, in full.
fn all_tables() -> Vec<(&'static str, RecordBatch)> {
    vec![
        ("date_dim", date_dim()),
        ("store", store()),
        ("item", item()),
        ("promotion", promotion()),
        ("catalog_page", catalog_page()),
        ("web_page", web_page()),
        ("web_site", web_site()),
        ("call_center", call_center()),
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

// --- Q77 / Q80 / Q5: ROLLUP over a per-channel UNION ALL with one sharded arm ---

#[tokio::test]
async fn q77_rollup_over_channel_union_plans_and_matches() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = strict_plan(&planner, Q77, &REPL_STORE_SALES).await;
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.sql.contains("__oxidant_materialize_gate")
                || s.sql.contains("__oxidant_subquery_gate")),
        "no whole-fact gather: {dq:?}"
    );
    assert!(
        dq.stages.iter().any(|s| s.sql.contains("ROLLUP")),
        "the combine stage rebuilds the grouping sets: {dq:?}"
    );
    drop(dq);
    assert_distributed_matches_single_node(Q77, &REPL_STORE_SALES, "store_sales").await;
}

#[tokio::test]
async fn q80_rollup_over_channel_union_plans_and_matches() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = strict_plan(&planner, Q80, &REPL_STORE_SALES).await;
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.sql.contains("__oxidant_materialize_gate")
                || s.sql.contains("__oxidant_subquery_gate")),
        "no whole-fact gather: {dq:?}"
    );
    assert!(
        dq.stages.iter().any(|s| s.sql.contains("ROLLUP")),
        "the combine stage rebuilds the grouping sets: {dq:?}"
    );
    drop(dq);
    assert_distributed_matches_single_node(Q80, &REPL_STORE_SALES, "store_sales").await;
}

#[tokio::test]
async fn q5_rollup_over_channel_union_plans_and_matches() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = strict_plan(&planner, Q5, &REPL_STORE_SALES).await;
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.sql.contains("__oxidant_materialize_gate")
                || s.sql.contains("__oxidant_subquery_gate")),
        "no whole-fact gather: {dq:?}"
    );
    assert!(
        dq.stages.iter().any(|s| s.sql.contains("ROLLUP")),
        "the combine stage rebuilds the grouping sets: {dq:?}"
    );
    drop(dq);
    assert_distributed_matches_single_node(Q5, &REPL_STORE_SALES, "store_sales").await;
}

// --- Q14: ROLLUP + INTERSECT with subqueries over the sharded fact ---

/// Q14 at the SF10 classification (only `store_sales` sharded). Every channel arm reads the
/// sharded fact three ways: its own scan, the `cross_items` IN-subquery (a three-way INTERSECT
/// whose store arm scans the fact), and the `avg_sales` HAVING scalar (a global AVG over a
/// three-arm UNION ALL with one sharded arm). The distributed composition (see the module doc)
/// keeps every shuffle exact: INTERSECT arms hash-shuffle on the full triple so equal rows
/// co-locate before the per-partition set op; the arm exports hash-shuffle by `xx_item_sk` so
/// the `IN` semi joins co-locate against the key stream; the per-arm recombine and the scalar
/// combine both gather to partition 0, where the HAVING threshold comparison sees complete
/// groups and the complete scalar; the outer ROLLUP gathers the three tiny exact arm streams.
///
/// The fixture straddles the shards: the store arm's only in-month row (date_sk 5) sits on
/// worker 1 while worker 0 holds the cross_items/avg_sales evidence for the same items, so a
/// plan that failed to recombine (or read a partial copy of a subquery's fact) would diverge.
#[tokio::test]
async fn q14_rollup_intersect_plans_and_matches() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = strict_plan(&planner, Q14, &REPL_STORE_SALES).await;
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.sql.contains("__oxidant_materialize_gate")
                || s.sql.contains("__oxidant_subquery_gate")),
        "no whole-fact gather: {dq:?}"
    );
    assert!(
        dq.stages.iter().any(|s| s.sql.contains("INTERSECT")),
        "cross_items distributes as key-shuffled INTERSECT arms: {dq:?}"
    );
    assert!(
        dq.stages.iter().any(|s| s.sql.contains("ROLLUP")),
        "the combine stage rebuilds the grouping sets: {dq:?}"
    );
    drop(dq);
    assert_distributed_matches_single_node(Q14, &REPL_STORE_SALES, "store_sales").await;
}

/// The same composition under the coverage harness's local classification (`web_sales`
/// sharded, every other table replicated): the sharded-arm role moves to the web channel.
#[tokio::test]
async fn q14_rollup_intersect_plans_and_matches_web_sharded() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    let planner = tpcds_engine().await;
    let dq = strict_plan(&planner, Q14, &REPL_WEB_SALES).await;
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.sql.contains("__oxidant_materialize_gate")
                || s.sql.contains("__oxidant_subquery_gate")),
        "no whole-fact gather: {dq:?}"
    );
    drop(dq);
    assert_distributed_matches_single_node(Q14, &REPL_WEB_SALES, "web_sales").await;
}
