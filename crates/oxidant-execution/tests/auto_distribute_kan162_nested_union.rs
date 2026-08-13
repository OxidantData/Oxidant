//! KAN-162 (q5): aggregate over a `UNION ALL` whose arms are themselves a nested MIXED
//! union — TPC-DS Q5's per-channel `Aggregate over Join(Union(sales_scan [sharded],
//! returns_scan [replicated]) ⋈ date_dim ⋈ channel-dim)` under an outer ROLLUP.
//!
//! The top-level bucketing in `split_union_by_sharding_multi` assigns each channel arm to
//! its sales-fact bucket (each arm scans exactly one sharded table once), but
//! `reject_unsafe_broadcast_shapes` declines the nested union (the replicated returns branch
//! does not scan the sharded table) — correctly for the undistributed mechanism: the bucket
//! producer runs per worker with the sales scan sliced and the returns scan fully
//! replicated, so replicated union-branch rows would be counted once per worker.
//!
//! The fix distributes the arm's enclosing chain over the nested union
//! (`distribute_over_nested_union`): inner joins / filters / projections distribute over
//! `UNION ALL` exactly (bag semantics), and the arm's inner aggregate is exact under the
//! split's SUM recombine via the existing KAN-54 additive guard. The sales-side branches
//! bucket by their sharded table; the all-replicated returns-side branches form the
//! replicated bucket (`split_union_finish`'s slice-or-Forward placement partitions them —
//! no double-count).
//!
//! Shapes under test, all at the all-facts-sharded v0.1.11 classification:
//!
//! - **(a) q5-like nested mixed union, three channels**: outer aggregate over three arms,
//!   each a nested `Union(sales, returns)` joined to broadcast dims under a per-channel
//!   aggregate. Plan pin: 3 sales producers + ONE replicated returns bucket (Forward) +
//!   recombine over 4 streams. STRICT e2e row-for-row vs single-node on two workers with
//!   group keys spanning both shards of every sales fact.
//! - **(b) ROLLUP variant** (q5-authentic grouping set): producers gather to partition 0,
//!   `HAVING COUNT(*) > 0` keeps the grand-total row honest. Row-for-row.
//! - **(c) Declines**: a nested union whose enclosing chain inner-joins ANOTHER sharded
//!   table; a nested union sitting under a non-inner join (union side preserved).
//! - **(d) q5's real web leg** (the nested union's returns branch is
//!   `web_returns LEFT JOIN web_sales`, scanning the sharded fact a second time with the
//!   replicated side preserved): the bucket runs the co-located composition
//!   (`union_left_join_branch_stages`) — R1 key-shuffles the null-extended side, R2
//!   Forward-shuffles the preserved side by its join-key positions, R3 is the bucket's
//!   partial producer over the substituted FROM tail. Wiring pin + STRICT e2e.
//! - **(e) REAL `bench/tpcds/queries/q5.sql` STRICT e2e** (include_str!) at the
//!   all-facts-sharded classification with q5-authentic fixtures (Date32 `d_date` in the
//!   2000-08-23..09-06 window, Decimal128(7,2) measures, cross-shard match / unmatched /
//!   NULL-key / fanout-2 key split across the worker halves). Row-for-row vs single-node.
//! - **(f) No-dim-join variant** whose sums directly expose the LEFT-JOIN row classes: the
//!   web channel's `returns_` is exactly 42.00 (the broken per-worker shape would give
//!   60.00 by null-extending matched preserved rows on the non-matching worker).
//! - **(g) Web-leg decline pins**: residual `>` join filter; non-plain-column key; the
//!   `RIGHT JOIN` flipped spelling; preserved side scanning the fact twice / a DIFFERENT
//!   sharded table; null-extended side a UNION.

// ENV_LOCK serializes process-global `OXIDANT_DISTRIBUTED_STRICT` across async tests.
#![allow(clippy::await_holding_lock)]

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};

use oxidant_execution::driver::{run_stages, Cluster, ExchangeMode};
use oxidant_execution::flight::serve_worker;
use oxidant_execution::plan::{plan_distributed_logical, DistributedQuery};
use oxidant_loom::arrow::array::{
    ArrayRef, Date32Array, Decimal128Array, Float64Array, Int64Array, StringArray,
};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::arrow::util::display::{ArrayFormatter, FormatOptions};
use oxidant_loom::Engine;

/// The v0.1.11 SF100 classification for the q5 shape: every sales fact sharded, every dim
/// and every returns fact replicated.
const REPL: [&str; 7] = [
    "date_dim",
    "store",
    "catalog_page",
    "web_site",
    "store_returns",
    "catalog_returns",
    "web_returns",
];
const SHARDED: [&str; 3] = ["store_sales", "catalog_sales", "web_sales"];

static ENV_LOCK: Mutex<()> = Mutex::new(());

static PORT: std::sync::OnceLock<AtomicU16> = std::sync::OnceLock::new();

fn unique_worker_port() -> u16 {
    // Base BELOW the Linux ephemeral source range (32768..=60999) so the harness's own
    // outbound connections can never steal a worker's port (see auto_distribute_kan55).
    PORT.get_or_init(|| AtomicU16::new(25000 + (std::process::id() as u16 % 512)))
        .fetch_add(1, Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// RecordBatch helpers / fixtures
// ---------------------------------------------------------------------------

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

fn date_dim() -> RecordBatch {
    batch(
        vec![i64f("d_date_sk"), i64f("d_year")],
        vec![i64v(&[1, 2, 3, 4]), i64v(&[2001, 2001, 2002, 2001])],
    )
}

fn store() -> RecordBatch {
    batch(
        vec![i64f("s_store_sk"), strf("s_store_id")],
        vec![i64v(&[1, 2, 3]), strv(&["s1", "s2", "s3"])],
    )
}

fn catalog_page() -> RecordBatch {
    batch(
        vec![i64f("cp_catalog_page_sk"), strf("cp_catalog_page_id")],
        vec![i64v(&[1, 2, 3]), strv(&["c1", "c2", "c3"])],
    )
}

fn web_site() -> RecordBatch {
    batch(
        vec![i64f("web_site_sk"), strf("web_site_id")],
        vec![i64v(&[1, 2, 3]), strv(&["w1", "w2", "w3"])],
    )
}

/// Store 1's 2001 rows land in BOTH row halves, so its partial aggregates genuinely
/// recombine across workers; the date-3 row is 2002 (filtered out where d_year = 2001).
fn store_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ss_sold_date_sk"),
            i64f("ss_store_sk"),
            f64f("ss_ext_sales_price"),
            f64f("ss_net_profit"),
        ],
        vec![
            i64v(&[1, 2, 1, 3]),
            i64v(&[1, 2, 1, 3]),
            f64v(&[100.0, 200.0, 50.0, 70.0]),
            f64v(&[10.0, 20.0, 5.0, 7.0]),
        ],
    )
}

fn store_returns() -> RecordBatch {
    batch(
        vec![
            i64f("sr_returned_date_sk"),
            i64f("sr_store_sk"),
            f64f("sr_return_amt"),
            f64f("sr_net_loss"),
        ],
        vec![
            i64v(&[2, 4]),
            i64v(&[1, 3]),
            f64v(&[3.0, 4.0]),
            f64v(&[1.0, 2.0]),
        ],
    )
}

/// Catalog page 1 spans both row halves.
fn catalog_sales() -> RecordBatch {
    batch(
        vec![
            i64f("cs_sold_date_sk"),
            i64f("cs_catalog_page_sk"),
            f64f("cs_ext_sales_price"),
            f64f("cs_net_profit"),
        ],
        vec![
            i64v(&[1, 4, 2, 3]),
            i64v(&[1, 2, 1, 3]),
            f64v(&[5.0, 8.0, 7.0, 6.0]),
            f64v(&[0.5, 0.8, 0.7, 0.6]),
        ],
    )
}

fn catalog_returns() -> RecordBatch {
    batch(
        vec![
            i64f("cr_returned_date_sk"),
            i64f("cr_catalog_page_sk"),
            f64f("cr_return_amount"),
            f64f("cr_net_loss"),
        ],
        vec![
            i64v(&[1, 4]),
            i64v(&[2, 1]),
            f64v(&[1.0, 2.0]),
            f64v(&[0.1, 0.2]),
        ],
    )
}

/// Web site 1 spans both row halves. `ws_item_sk`/`ws_order_number` exist only for the
/// decline pin reproducing q5's web leg (`web_returns LEFT JOIN web_sales`).
fn web_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ws_sold_date_sk"),
            i64f("ws_web_site_sk"),
            i64f("ws_item_sk"),
            i64f("ws_order_number"),
            f64f("ws_ext_sales_price"),
            f64f("ws_net_profit"),
        ],
        vec![
            i64v(&[1, 2, 4, 3]),
            i64v(&[1, 2, 1, 3]),
            i64v(&[10, 11, 12, 13]),
            i64v(&[100, 101, 102, 103]),
            f64v(&[10.0, 20.0, 40.0, 30.0]),
            f64v(&[1.0, 2.0, 4.0, 3.0]),
        ],
    )
}

fn web_returns() -> RecordBatch {
    batch(
        vec![
            i64f("wr_returned_date_sk"),
            i64f("wr_web_site_sk"),
            i64f("wr_item_sk"),
            i64f("wr_order_number"),
            f64f("wr_return_amt"),
            f64f("wr_net_loss"),
        ],
        vec![
            i64v(&[2, 1]),
            i64v(&[2, 1]),
            i64v(&[11, 99]),
            i64v(&[101, 999]),
            f64v(&[5.0, 6.0]),
            f64v(&[0.5, 0.6]),
        ],
    )
}

fn register(engine: &Engine, name: &str, batches: Vec<RecordBatch>) {
    engine.register_batches(name, batches).unwrap();
}

/// Planner/ground-truth engine holding the full dataset.
fn planner_engine() -> Engine {
    let e = Engine::new();
    register(&e, "date_dim", vec![date_dim()]);
    register(&e, "store", vec![store()]);
    register(&e, "catalog_page", vec![catalog_page()]);
    register(&e, "web_site", vec![web_site()]);
    register(&e, "store_sales", vec![store_sales()]);
    register(&e, "store_returns", vec![store_returns()]);
    register(&e, "catalog_sales", vec![catalog_sales()]);
    register(&e, "catalog_returns", vec![catalog_returns()]);
    register(&e, "web_sales", vec![web_sales()]);
    register(&e, "web_returns", vec![web_returns()]);
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

/// Every sharded table split row-wise across two in-process workers; replicated tables held
/// in full on each worker (the production replicated-table invariant).
async fn two_workers() -> Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        for (name, full) in [
            ("date_dim", date_dim()),
            ("store", store()),
            ("catalog_page", catalog_page()),
            ("web_site", web_site()),
            ("store_sales", store_sales()),
            ("store_returns", store_returns()),
            ("catalog_sales", catalog_sales()),
            ("catalog_returns", catalog_returns()),
            ("web_sales", web_sales()),
            ("web_returns", web_returns()),
        ] {
            if SHARDED.contains(&name) {
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

/// Sorted value rows (headers are not compared: single-node and distributed plans name
/// unaliased outputs differently — pre-existing behavior of every distributed shape).
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

/// Plan under `OXIDANT_DISTRIBUTED_STRICT=1` (the whole-fact gather must never substitute).
fn plan_strict(
    lp: &datafusion::logical_expr::LogicalPlan,
    replicated: &[&str],
    tag: &str,
) -> DistributedQuery {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("OXIDANT_DISTRIBUTED_STRICT", "1");
    let planned = plan_distributed_logical(lp, replicated);
    std::env::remove_var("OXIDANT_DISTRIBUTED_STRICT");
    planned.unwrap_or_else(|e| panic!("{tag} must plan distributed in strict mode: {e}"))
}

/// Plan strict, run on the two-worker cluster, require row-for-row equality with
/// single-node. `check_plan` pins the stage shape.
async fn assert_matches(
    tag: &str,
    sql: &str,
    replicated: &[&str],
    planner: Engine,
    cluster: Cluster,
    check_plan: impl Fn(&DistributedQuery),
) {
    let expected = planner.sql(sql).await.expect("single-node");
    assert!(
        expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
        "{tag}: single-node result must be non-empty (otherwise the comparison is vacuous)"
    );

    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let dq = plan_strict(&lp, replicated, tag);
    check_plan(&dq);

    let mut out = None;
    for _ in 0..150 {
        match run_stages(&cluster, &dq.stages).await {
            Ok(b) => {
                out = Some(b);
                break;
            }
            Err(e) => {
                eprintln!("run_stages err: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
    let gathered = out.expect("distributed run never succeeded");
    let actual = match &dq.finalize_sql {
        None => gathered,
        Some(fsql) => {
            let fin = Engine::new();
            fin.register_batches("result", gathered).unwrap();
            fin.sql(fsql).await.expect("finalize")
        }
    };
    assert_eq!(
        rows_sorted(&expected),
        rows_sorted(&actual),
        "{tag}: distributed result must equal single-node"
    );
}

/// The reduced-shape classification (REPL) and fixtures.
async fn assert_matches_single_node(tag: &str, sql: &str, check_plan: impl Fn(&DistributedQuery)) {
    let cluster = two_workers().await;
    assert_matches(tag, sql, &REPL, planner_engine(), cluster, check_plan).await;
}

/// Leaf-stage count scanning `table`.
fn leaf_stages_scanning<'a>(
    dq: &'a DistributedQuery,
    table: &str,
) -> Vec<&'a oxidant_execution::driver::StageDef> {
    dq.stages
        .iter()
        .filter(|s| s.upstream_stage_ids.is_empty() && s.sql.contains(table))
        .collect()
}

// ---------------------------------------------------------------------------
// (a) q5-like nested mixed union: three channel arms, each
//     Aggregate over Join(Union(sales [sharded], returns [replicated]) ⋈ dims)
// ---------------------------------------------------------------------------

/// One arm per channel; the nested union's returns branch is fully replicated (unlike q5's
/// real web leg — see the decline pins below).
const NESTED_MIXED: &str = "
SELECT channel, id, SUM(sales) AS sales, SUM(returns_) AS returns_, SUM(profit) AS profit
FROM (
  SELECT 'store' AS channel, ssr.s_store_id AS id, ssr.sales AS sales,
         ssr.returns_ AS returns_, ssr.profit - ssr.profit_loss AS profit
  FROM (
    SELECT store.s_store_id,
           SUM(salesreturns.sales_price) AS sales, SUM(salesreturns.profit) AS profit,
           SUM(salesreturns.return_amt) AS returns_, SUM(salesreturns.net_loss) AS profit_loss
    FROM (
      SELECT ss_store_sk AS k, ss_sold_date_sk AS dsk,
             ss_ext_sales_price AS sales_price, CAST(0 AS DOUBLE) AS return_amt,
             ss_net_profit AS profit, CAST(0 AS DOUBLE) AS net_loss
      FROM store_sales
      UNION ALL
      SELECT sr_store_sk, sr_returned_date_sk, CAST(0 AS DOUBLE), sr_return_amt,
             CAST(0 AS DOUBLE), sr_net_loss
      FROM store_returns
    ) salesreturns, date_dim, store
    WHERE salesreturns.dsk = date_dim.d_date_sk AND date_dim.d_year = 2001
      AND salesreturns.k = store.s_store_sk
    GROUP BY store.s_store_id
  ) ssr
  UNION ALL
  SELECT 'catalog' AS channel, csr.cp_catalog_page_id AS id, csr.sales AS sales,
         csr.returns_ AS returns_, csr.profit - csr.profit_loss AS profit
  FROM (
    SELECT catalog_page.cp_catalog_page_id,
           SUM(salesreturns.sales_price) AS sales, SUM(salesreturns.profit) AS profit,
           SUM(salesreturns.return_amt) AS returns_, SUM(salesreturns.net_loss) AS profit_loss
    FROM (
      SELECT cs_catalog_page_sk AS k, cs_sold_date_sk AS dsk,
             cs_ext_sales_price AS sales_price, CAST(0 AS DOUBLE) AS return_amt,
             cs_net_profit AS profit, CAST(0 AS DOUBLE) AS net_loss
      FROM catalog_sales
      UNION ALL
      SELECT cr_catalog_page_sk, cr_returned_date_sk, CAST(0 AS DOUBLE), cr_return_amount,
             CAST(0 AS DOUBLE), cr_net_loss
      FROM catalog_returns
    ) salesreturns, date_dim, catalog_page
    WHERE salesreturns.dsk = date_dim.d_date_sk AND date_dim.d_year = 2001
      AND salesreturns.k = catalog_page.cp_catalog_page_sk
    GROUP BY catalog_page.cp_catalog_page_id
  ) csr
  UNION ALL
  SELECT 'web' AS channel, wsr.web_site_id AS id, wsr.sales AS sales,
         wsr.returns_ AS returns_, wsr.profit - wsr.profit_loss AS profit
  FROM (
    SELECT web_site.web_site_id,
           SUM(salesreturns.sales_price) AS sales, SUM(salesreturns.profit) AS profit,
           SUM(salesreturns.return_amt) AS returns_, SUM(salesreturns.net_loss) AS profit_loss
    FROM (
      SELECT ws_web_site_sk AS k, ws_sold_date_sk AS dsk,
             ws_ext_sales_price AS sales_price, CAST(0 AS DOUBLE) AS return_amt,
             ws_net_profit AS profit, CAST(0 AS DOUBLE) AS net_loss
      FROM web_sales
      UNION ALL
      SELECT wr_web_site_sk, wr_returned_date_sk, CAST(0 AS DOUBLE), wr_return_amt,
             CAST(0 AS DOUBLE), wr_net_loss
      FROM web_returns
    ) salesreturns, date_dim, web_site
    WHERE salesreturns.dsk = date_dim.d_date_sk AND date_dim.d_year = 2001
      AND salesreturns.k = web_site.web_site_sk
    GROUP BY web_site.web_site_id
  ) wsr
) x
GROUP BY channel, id
ORDER BY channel, id
";

#[tokio::test]
async fn nested_mixed_union_three_channels_matches_single_node() {
    assert_matches_single_node("nested-mixed", NESTED_MIXED, |dq| {
        // One partial producer per sales fact, hash-shuffled by (channel, id).
        for fact in SHARDED {
            let leaves = leaf_stages_scanning(dq, fact);
            assert_eq!(leaves.len(), 1, "one {fact} partial producer: {dq:?}");
            assert!(
                leaves[0].sql.contains("GROUP BY"),
                "{fact} producer partially aggregates: {:?}",
                leaves[0]
            );
            assert_eq!(
                leaves[0].hash_key_cols,
                vec![0, 1],
                "{fact} producer hash-shuffles by (channel, id)"
            );
        }
        // The three returns branches merge into ONE replicated bucket, computed once
        // (Forward: no OXIDANT_WORKER_COUNT is set, and the inner aggregates rule out the
        // sliced placement).
        let returns = leaf_stages_scanning(dq, "store_returns");
        assert_eq!(returns.len(), 1, "one replicated returns bucket: {dq:?}");
        assert!(
            returns[0].sql.contains("catalog_returns") && returns[0].sql.contains("web_returns"),
            "all three returns branches share the replicated bucket: {:?}",
            returns[0]
        );
        assert!(
            !returns[0].sql.contains("store_sales"),
            "the replicated bucket scans no sharded table: {:?}",
            returns[0]
        );
        assert_eq!(
            returns[0].exchange,
            ExchangeMode::Forward,
            "replicated-only bucket is computed once: {:?}",
            returns[0]
        );
        // One recombine over exactly the four producer streams.
        let combine = dq.stages.last().expect("recombine stage");
        assert_eq!(
            combine.upstream_stage_ids.len(),
            4,
            "three sales producers + the replicated returns bucket: {dq:?}"
        );
        assert!(
            combine.sql.contains("UNION ALL") && combine.sql.contains("shuffle_input_3"),
            "the recombine merges all four producer streams: {combine:?}"
        );
    })
    .await;
}

// ---------------------------------------------------------------------------
// (b) ROLLUP over the nested mixed union (q5-authentic grouping set)
// ---------------------------------------------------------------------------

const NESTED_MIXED_ROLLUP: &str = "
SELECT channel, id, SUM(sales) AS sales, SUM(returns_) AS returns_
FROM (
  SELECT 'store' AS channel, ssr.s_store_id AS id, ssr.sales AS sales, ssr.returns_ AS returns_
  FROM (
    SELECT store.s_store_id,
           SUM(salesreturns.sales_price) AS sales, SUM(salesreturns.return_amt) AS returns_
    FROM (
      SELECT ss_store_sk AS k, ss_sold_date_sk AS dsk,
             ss_ext_sales_price AS sales_price, CAST(0 AS DOUBLE) AS return_amt
      FROM store_sales
      UNION ALL
      SELECT sr_store_sk, sr_returned_date_sk, CAST(0 AS DOUBLE), sr_return_amt
      FROM store_returns
    ) salesreturns, date_dim, store
    WHERE salesreturns.dsk = date_dim.d_date_sk AND date_dim.d_year = 2001
      AND salesreturns.k = store.s_store_sk
    GROUP BY store.s_store_id
  ) ssr
  UNION ALL
  SELECT 'web' AS channel, wsr.web_site_id AS id, wsr.sales AS sales, wsr.returns_ AS returns_
  FROM (
    SELECT web_site.web_site_id,
           SUM(salesreturns.sales_price) AS sales, SUM(salesreturns.return_amt) AS returns_
    FROM (
      SELECT ws_web_site_sk AS k, ws_sold_date_sk AS dsk,
             ws_ext_sales_price AS sales_price, CAST(0 AS DOUBLE) AS return_amt
      FROM web_sales
      UNION ALL
      SELECT wr_web_site_sk, wr_returned_date_sk, CAST(0 AS DOUBLE), wr_return_amt
      FROM web_returns
    ) salesreturns, date_dim, web_site
    WHERE salesreturns.dsk = date_dim.d_date_sk AND date_dim.d_year = 2001
      AND salesreturns.k = web_site.web_site_sk
    GROUP BY web_site.web_site_id
  ) wsr
) x
GROUP BY ROLLUP(channel, id)
ORDER BY channel, id
";

#[tokio::test]
async fn rollup_over_nested_mixed_union_matches_single_node() {
    assert_matches_single_node("nested-mixed-rollup", NESTED_MIXED_ROLLUP, |dq| {
        let combine = dq.stages.last().expect("recombine stage");
        assert_eq!(
            combine.upstream_stage_ids.len(),
            3,
            "two sales producers + the replicated returns bucket: {dq:?}"
        );
        for s in &dq.stages[..dq.stages.len() - 1] {
            assert!(
                s.hash_key_cols.is_empty(),
                "grouping-set producers gather to partition 0: {s:?}"
            );
        }
        assert!(
            combine.sql.contains("HAVING COUNT(*) > 0"),
            "empty non-zero partitions must not emit a grand-total row: {combine:?}"
        );
    })
    .await;
}

// ---------------------------------------------------------------------------
// (c) Declines: distribution precondition failures keep the strict refusal
// ---------------------------------------------------------------------------

/// Plan-only strict decline assertion (the honest refusal IS the contract).
async fn assert_declines(tag: &str, sql: &str) {
    let planner = planner_engine();
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("OXIDANT_DISTRIBUTED_STRICT", "1");
    let planned = plan_distributed_logical(&lp, &REPL);
    std::env::remove_var("OXIDANT_DISTRIBUTED_STRICT");
    assert!(
        planned.is_err(),
        "{tag} must keep declining in strict mode, got {planned:?}"
    );
}

/// The store arm's nested union sits under an inner join whose OTHER child is the sharded
/// `web_sales` — distribution would clone a sharded table into every branch, so the shape
/// must keep declining.
const NESTED_UNION_BESIDE_SHARDED_JOIN_CHILD: &str = "
SELECT channel, id, SUM(sales) AS sales
FROM (
  SELECT 'store' AS channel, ssr.s_store_id AS id, ssr.sales AS sales
  FROM (
    SELECT store.s_store_id, SUM(salesreturns.sales_price) AS sales
    FROM (
      SELECT ss_store_sk AS k, ss_sold_date_sk AS dsk, ss_ext_sales_price AS sales_price
      FROM store_sales
      UNION ALL
      SELECT sr_store_sk, sr_returned_date_sk, sr_return_amt
      FROM store_returns
    ) salesreturns, date_dim, store, web_sales
    WHERE salesreturns.dsk = date_dim.d_date_sk AND date_dim.d_year = 2001
      AND salesreturns.k = store.s_store_sk
      AND store.s_store_sk = web_sales.ws_web_site_sk
    GROUP BY store.s_store_id
  ) ssr
  UNION ALL
  SELECT 'catalog' AS channel, csr.cp_catalog_page_id AS id, csr.sales AS sales
  FROM (
    SELECT catalog_page.cp_catalog_page_id, SUM(salesreturns.sales_price) AS sales
    FROM (
      SELECT cs_catalog_page_sk AS k, cs_sold_date_sk AS dsk, cs_ext_sales_price AS sales_price
      FROM catalog_sales
      UNION ALL
      SELECT cr_catalog_page_sk, cr_returned_date_sk, cr_return_amount
      FROM catalog_returns
    ) salesreturns, date_dim, catalog_page
    WHERE salesreturns.dsk = date_dim.d_date_sk AND date_dim.d_year = 2001
      AND salesreturns.k = catalog_page.cp_catalog_page_sk
    GROUP BY catalog_page.cp_catalog_page_id
  ) csr
) x
GROUP BY channel, id
";

#[tokio::test]
async fn nested_union_beside_sharded_join_child_declines_safely() {
    assert_declines(
        "nested-union-beside-sharded-join-child",
        NESTED_UNION_BESIDE_SHARDED_JOIN_CHILD,
    )
    .await;
}

/// The store arm's nested union sits under a LEFT join (the union side is preserved):
/// distribution only crosses INNER joins, so the shape must keep declining.
const NESTED_UNION_UNDER_LEFT_JOIN: &str = "
SELECT channel, id, SUM(sales) AS sales
FROM (
  SELECT 'store' AS channel, ssr.s_store_id AS id, ssr.sales AS sales
  FROM (
    SELECT store.s_store_id, SUM(salesreturns.sales_price) AS sales
    FROM (
      SELECT ss_store_sk AS k, ss_sold_date_sk AS dsk, ss_ext_sales_price AS sales_price
      FROM store_sales
      UNION ALL
      SELECT sr_store_sk, sr_returned_date_sk, sr_return_amt
      FROM store_returns
    ) salesreturns
    LEFT JOIN date_dim ON salesreturns.dsk = date_dim.d_date_sk
    JOIN store ON salesreturns.k = store.s_store_sk
    GROUP BY store.s_store_id
  ) ssr
  UNION ALL
  SELECT 'catalog' AS channel, csr.cp_catalog_page_id AS id, csr.sales AS sales
  FROM (
    SELECT catalog_page.cp_catalog_page_id, SUM(salesreturns.sales_price) AS sales
    FROM (
      SELECT cs_catalog_page_sk AS k, cs_sold_date_sk AS dsk, cs_ext_sales_price AS sales_price
      FROM catalog_sales
      UNION ALL
      SELECT cr_catalog_page_sk, cr_returned_date_sk, cr_return_amount
      FROM catalog_returns
    ) salesreturns, date_dim, catalog_page
    WHERE salesreturns.dsk = date_dim.d_date_sk AND date_dim.d_year = 2001
      AND salesreturns.k = catalog_page.cp_catalog_page_sk
    GROUP BY catalog_page.cp_catalog_page_id
  ) csr
) x
GROUP BY channel, id
";

#[tokio::test]
async fn nested_union_under_left_join_declines_safely() {
    assert_declines("nested-union-under-left-join", NESTED_UNION_UNDER_LEFT_JOIN).await;
}

/// Q5's real web leg: the nested union's returns branch is
/// `web_returns LEFT JOIN web_sales` — the arm scans the sharded `web_sales` TWICE, and the
/// distributed returns branch has the REPLICATED side preserved, so per-worker execution
/// would repeat unmatched web_returns rows on every worker. The bucket instead runs the
/// co-located composition (`union_left_join_branch_stages`): R1 key-shuffles the
/// null-extended `web_sales` side, R2 Forward-shuffles the preserved `web_returns` side by
/// its join-key positions, and R3 is the bucket's partial producer over the FROM tail with
/// both sides token-substituted to the positional shuffle inputs.
const Q5_WEB_LEG: &str = "
SELECT channel, id, SUM(sales) AS sales
FROM (
  SELECT 'web' AS channel, wsr.web_site_id AS id, wsr.sales AS sales
  FROM (
    SELECT web_site.web_site_id, SUM(salesreturns.sales_price) AS sales
    FROM (
      SELECT ws_web_site_sk AS k, ws_sold_date_sk AS dsk, ws_ext_sales_price AS sales_price
      FROM web_sales
      UNION ALL
      SELECT web_sales.ws_web_site_sk, web_returns.wr_returned_date_sk,
             CAST(0 AS DOUBLE)
      FROM web_returns
      LEFT JOIN web_sales ON web_returns.wr_item_sk = web_sales.ws_item_sk
                         AND web_returns.wr_order_number = web_sales.ws_order_number
    ) salesreturns, date_dim, web_site
    WHERE salesreturns.dsk = date_dim.d_date_sk AND date_dim.d_year = 2001
      AND salesreturns.k = web_site.web_site_sk
    GROUP BY web_site.web_site_id
  ) wsr
  UNION ALL
  SELECT 'catalog' AS channel, csr.cp_catalog_page_id AS id, csr.sales AS sales
  FROM (
    SELECT catalog_page.cp_catalog_page_id, SUM(salesreturns.sales_price) AS sales
    FROM (
      SELECT cs_catalog_page_sk AS k, cs_sold_date_sk AS dsk, cs_ext_sales_price AS sales_price
      FROM catalog_sales
      UNION ALL
      SELECT cr_catalog_page_sk, cr_returned_date_sk, cr_return_amount
      FROM catalog_returns
    ) salesreturns, date_dim, catalog_page
    WHERE salesreturns.dsk = date_dim.d_date_sk AND date_dim.d_year = 2001
      AND salesreturns.k = catalog_page.cp_catalog_page_sk
    GROUP BY catalog_page.cp_catalog_page_id
  ) csr
) x
GROUP BY channel, id
";

#[tokio::test]
async fn q5_web_leg_left_join_over_sharded_fact_matches_single_node() {
    assert_matches_single_node("q5-web-leg", Q5_WEB_LEG, |dq| {
        // Seven stages: the flat `web_sales` producer (the nested union's sales branch),
        // the R1–R3 co-located LEFT JOIN chain (the returns branch's singleton bucket),
        // the flat `catalog_sales` producer, the replicated `catalog_returns` bucket, and
        // the shared recombine.
        assert_eq!(
            dq.stages.len(),
            7,
            "web flat + R1/R2/R3 + catalog flat + replicated bucket + recombine: {dq:?}"
        );
        // R1: a narrow key-shuffle of the null-extended side — join keys first (so
        // hash_key_cols = 0..k), then the right-side columns referenced above the join
        // (`ws_web_site_sk`).
        let r1 = &dq.stages[1];
        assert!(
            r1.upstream_stage_ids.is_empty(),
            "R1 is an ordinary sliced sharded leaf: {r1:?}"
        );
        assert_eq!(
            r1.hash_key_cols,
            vec![0, 1],
            "R1 hash-shuffles web_sales by the join key: {r1:?}"
        );
        assert!(
            r1.sql.contains("FROM web_sales")
                && r1.sql.contains("ws_item_sk")
                && r1.sql.contains("ws_order_number")
                && r1.sql.contains("ws_web_site_sk"),
            "R1 carries the join keys plus the referenced payload: {r1:?}"
        );
        // R2: the preserved side in full, computed once (Forward), hash-co-located by the
        // left key positions in the verbatim scan's schema (wr_item_sk, wr_order_number).
        let r2 = &dq.stages[2];
        assert!(
            r2.upstream_stage_ids.is_empty() && r2.sql.contains("FROM web_returns"),
            "R2 is the preserved-side leaf: {r2:?}"
        );
        assert_eq!(
            r2.exchange,
            ExchangeMode::Forward,
            "R2 is computed once (replicated side): {r2:?}"
        );
        assert_eq!(
            r2.hash_key_cols,
            vec![2, 3],
            "R2 hash-shuffles web_returns by its join-key positions: {r2:?}"
        );
        // R3: the bucket's producer — the flat producer's construction over the
        // substituted tail. upstreams = [R1, R2], so shuffle_input_0 is the null-extended
        // side and shuffle_input_1 the preserved side; R3 takes the shared group-key hash.
        let r3 = &dq.stages[3];
        assert_eq!(
            r3.upstream_stage_ids,
            vec![1, 2],
            "R3 consumes R1 (null-extended) and R2 (preserved): {r3:?}"
        );
        assert!(
            r3.sql.contains(
                "(SELECT * FROM shuffle_input_1) AS web_returns LEFT OUTER JOIN (SELECT * FROM shuffle_input_0) AS web_sales"
            ),
            "R3's tail substitutes both shuffle inputs: {r3:?}"
        );
        assert_eq!(
            r3.hash_key_cols,
            vec![0, 1],
            "R3 is the bucket's producer into the shared (channel, id) recombine: {r3:?}"
        );
        // The recombine merges exactly the four producer streams: flat web_sales, the R3
        // terminal, flat catalog_sales, and the replicated catalog_returns bucket.
        let combine = dq.stages.last().expect("recombine stage");
        assert_eq!(
            combine.upstream_stage_ids,
            vec![0, 3, 4, 5],
            "flat web producer + R3 + flat catalog producer + replicated bucket: {combine:?}"
        );
    })
    .await;
}

// ---------------------------------------------------------------------------
// (e) REAL bench/tpcds/queries/q5.sql STRICT e2e — q5-authentic fixtures:
//     Date32 d_date inside the 2000-08-23..09-06 window, Decimal128(7,2) measures,
//     and LEFT-JOIN row classes split across the two contiguous worker halves.
// ---------------------------------------------------------------------------

const Q5_REAL: &str = include_str!("../../../bench/tpcds/queries/q5.sql");

fn decf(name: &str) -> Field {
    Field::new(name, DataType::Decimal128(7, 2), false)
}
fn datef(name: &str) -> Field {
    Field::new(name, DataType::Date32, false)
}
fn i64n(name: &str) -> Field {
    Field::new(name, DataType::Int64, true)
}

/// `cents` is the decimal(7,2) value x100 (API precedent: oxidant-bench tpcds.rs).
fn decv(cents: &[i64]) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(cents.iter().map(|c| Some(*c as i128)).collect::<Vec<_>>())
            .with_precision_and_scale(7, 2)
            .unwrap(),
    )
}
fn datev(vals: &[i32]) -> ArrayRef {
    Arc::new(Date32Array::from(vals.to_vec()))
}
fn i64ov(vals: &[Option<i64>]) -> ArrayRef {
    Arc::new(Int64Array::from(vals.to_vec()))
}

/// d_date_sk 1..=4 = 2000-08-24..2000-08-27 (Date32 days 11193-11196, inside q5's
/// 2000-08-23..09-06 window); d_date_sk 5 = 2000-09-07 (day 11207), the out-of-window
/// control row.
fn q5_date_dim() -> RecordBatch {
    batch(
        vec![i64f("d_date_sk"), datef("d_date")],
        vec![
            i64v(&[1, 2, 3, 4, 5]),
            datev(&[11193, 11194, 11195, 11196, 11207]),
        ],
    )
}

fn q5_store() -> RecordBatch {
    batch(
        vec![i64f("s_store_sk"), strf("s_store_id")],
        vec![i64v(&[1, 2]), strv(&["1", "2"])],
    )
}

fn q5_catalog_page() -> RecordBatch {
    batch(
        vec![i64f("cp_catalog_page_sk"), strf("cp_catalog_page_id")],
        vec![i64v(&[1, 2]), strv(&["1", "2"])],
    )
}

fn q5_web_site() -> RecordBatch {
    batch(
        vec![i64f("web_site_sk"), strf("web_site_id")],
        vec![i64v(&[1, 2]), strv(&["1", "2"])],
    )
}

/// Six rows: shard0 = rows 0-2, shard1 = rows 3-5. Store 1's in-window group (rows 0/2 vs
/// row 4) spans both shards; row 3 is the out-of-window control.
fn q5_store_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ss_sold_date_sk"),
            i64f("ss_store_sk"),
            decf("ss_ext_sales_price"),
            decf("ss_net_profit"),
        ],
        vec![
            i64v(&[1, 2, 3, 5, 4, 1]),
            i64v(&[1, 2, 1, 1, 1, 2]),
            decv(&[10000, 20000, 5000, 99900, 7000, 6000]),
            decv(&[1000, 2000, 500, 9900, 700, 600]),
        ],
    )
}

fn q5_store_returns() -> RecordBatch {
    batch(
        vec![
            i64f("sr_returned_date_sk"),
            i64f("sr_store_sk"),
            decf("sr_return_amt"),
            decf("sr_net_loss"),
        ],
        vec![
            i64v(&[2, 3]),
            i64v(&[1, 2]),
            decv(&[300, 400]),
            decv(&[100, 200]),
        ],
    )
}

/// Page 1's group spans both halves (rows 0/2 vs row 3).
fn q5_catalog_sales() -> RecordBatch {
    batch(
        vec![
            i64f("cs_sold_date_sk"),
            i64f("cs_catalog_page_sk"),
            decf("cs_ext_sales_price"),
            decf("cs_net_profit"),
        ],
        vec![
            i64v(&[1, 2, 3, 4]),
            i64v(&[1, 2, 1, 1]),
            decv(&[500, 800, 700, 600]),
            decv(&[50, 80, 70, 60]),
        ],
    )
}

fn q5_catalog_returns() -> RecordBatch {
    batch(
        vec![
            i64f("cr_returned_date_sk"),
            i64f("cr_catalog_page_sk"),
            decf("cr_return_amount"),
            decf("cr_net_loss"),
        ],
        vec![
            i64v(&[1, 4]),
            i64v(&[2, 1]),
            decv(&[100, 200]),
            decv(&[10, 20]),
        ],
    )
}

/// Six rows: shard0 = rows 0-2, shard1 = rows 3-5. The fanout-2 key (item 14, order 104)
/// has one row on EACH half (rows 2 and 3): the returns row joining on that key must see
/// both — only the co-located key shuffle co-locates them with the preserved row.
fn q5_web_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ws_sold_date_sk"),
            i64f("ws_web_site_sk"),
            i64f("ws_item_sk"),
            i64f("ws_order_number"),
            decf("ws_ext_sales_price"),
            decf("ws_net_profit"),
        ],
        vec![
            i64v(&[1, 2, 3, 4, 1, 2]),
            i64v(&[1, 2, 1, 2, 1, 2]),
            i64v(&[10, 11, 14, 14, 12, 13]),
            i64v(&[100, 101, 104, 104, 102, 103]),
            decv(&[1000, 2000, 4000, 3000, 2500, 3500]),
            decv(&[100, 200, 400, 300, 250, 350]),
        ],
    )
}

/// The four LEFT-JOIN row classes: (12, 102) matches only shard1's sale (the per-worker
/// duplication trap — the other worker's slice has no match); (99, 999) is unmatched;
/// (NULL, NULL) never matches; (14, 104) fans out to one sale on EACH half.
fn q5_web_returns() -> RecordBatch {
    batch(
        vec![
            i64f("wr_returned_date_sk"),
            i64n("wr_item_sk"),
            i64n("wr_order_number"),
            decf("wr_return_amt"),
            decf("wr_net_loss"),
        ],
        vec![
            i64v(&[2, 3, 1, 4]),
            i64ov(&[Some(12), Some(99), None, Some(14)]),
            i64ov(&[Some(102), Some(999), None, Some(104)]),
            decv(&[500, 600, 700, 1200]),
            decv(&[50, 60, 70, 120]),
        ],
    )
}

/// Planner/ground-truth engine holding the full q5-authentic dataset.
fn q5_planner_engine() -> Engine {
    let e = Engine::new();
    register(&e, "date_dim", vec![q5_date_dim()]);
    register(&e, "store", vec![q5_store()]);
    register(&e, "catalog_page", vec![q5_catalog_page()]);
    register(&e, "web_site", vec![q5_web_site()]);
    register(&e, "store_sales", vec![q5_store_sales()]);
    register(&e, "store_returns", vec![q5_store_returns()]);
    register(&e, "catalog_sales", vec![q5_catalog_sales()]);
    register(&e, "catalog_returns", vec![q5_catalog_returns()]);
    register(&e, "web_sales", vec![q5_web_sales()]);
    register(&e, "web_returns", vec![q5_web_returns()]);
    e
}

/// The q5-authentic dataset with every sales fact split row-wise across two workers.
async fn q5_two_workers() -> Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        for (name, full) in [
            ("date_dim", q5_date_dim()),
            ("store", q5_store()),
            ("catalog_page", q5_catalog_page()),
            ("web_site", q5_web_site()),
            ("store_sales", q5_store_sales()),
            ("store_returns", q5_store_returns()),
            ("catalog_sales", q5_catalog_sales()),
            ("catalog_returns", q5_catalog_returns()),
            ("web_sales", q5_web_sales()),
            ("web_returns", q5_web_returns()),
        ] {
            if SHARDED.contains(&name) {
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

#[tokio::test]
async fn q5_real_query_all_facts_sharded_matches_single_node() {
    let cluster = q5_two_workers().await;
    assert_matches(
        "q5-real",
        Q5_REAL,
        &REPL,
        q5_planner_engine(),
        cluster,
        |dq| {
            assert_eq!(
            dq.stages.len(),
            8,
            "flat web + R1/R2/R3 + flat store + flat catalog + replicated bucket + combine: {dq:?}"
        );
            assert!(
                dq.stages.iter().any(|s| s
                    .sql
                    .contains("LEFT OUTER JOIN (SELECT * FROM shuffle_input_0) AS web_sales")),
                "the web-leg bucket substitutes the co-located shuffle inputs: {dq:?}"
            );
        },
    )
    .await;
}

// ---------------------------------------------------------------------------
// (f) No-dim-join variant: without the web_site/date_dim inner joins the unmatched and
//     NULL-keyed preserved rows SURVIVE, so the sums directly expose the row classes.
// ---------------------------------------------------------------------------

/// Every channel arm aggregates its nested union verbatim (no dim joins, no date filter).
/// Web returns_ = 5.00 (cross-shard match) + 6.00 (unmatched) + 7.00 (NULL key) +
/// 12.00 x2 (fanout to both halves) = **42.00**; the broken per-worker shape (preserved
/// rows scanned in full on every worker) would emit 60.00 — every matched/unmatched/NULL
/// row counted twice, once null-extended on the worker lacking the match.
const Q5_WEB_LEG_NO_DIMS: &str = "
SELECT channel, SUM(sales) AS sales, SUM(returns_) AS returns_
FROM (
  SELECT 'web' AS channel, wsr.sales AS sales, wsr.returns_ AS returns_
  FROM (
    SELECT SUM(salesreturns.sales_price) AS sales, SUM(salesreturns.return_amt) AS returns_
    FROM (
      SELECT ws_web_site_sk AS k, ws_sold_date_sk AS dsk,
             ws_ext_sales_price AS sales_price, CAST(0 AS DECIMAL(7,2)) AS return_amt
      FROM web_sales
      UNION ALL
      SELECT web_sales.ws_web_site_sk, web_returns.wr_returned_date_sk,
             CAST(0 AS DECIMAL(7,2)), web_returns.wr_return_amt
      FROM web_returns
      LEFT JOIN web_sales ON web_returns.wr_item_sk = web_sales.ws_item_sk
                         AND web_returns.wr_order_number = web_sales.ws_order_number
    ) salesreturns
  ) wsr
  UNION ALL
  SELECT 'store' AS channel, ssr.sales AS sales, ssr.returns_ AS returns_
  FROM (
    SELECT SUM(salesreturns.sales_price) AS sales, SUM(salesreturns.return_amt) AS returns_
    FROM (
      SELECT ss_store_sk AS k, ss_sold_date_sk AS dsk,
             ss_ext_sales_price AS sales_price, CAST(0 AS DECIMAL(7,2)) AS return_amt
      FROM store_sales
      UNION ALL
      SELECT sr_store_sk, sr_returned_date_sk, CAST(0 AS DECIMAL(7,2)), sr_return_amt
      FROM store_returns
    ) salesreturns
  ) ssr
  UNION ALL
  SELECT 'catalog' AS channel, csr.sales AS sales, csr.returns_ AS returns_
  FROM (
    SELECT SUM(salesreturns.sales_price) AS sales, SUM(salesreturns.return_amt) AS returns_
    FROM (
      SELECT cs_catalog_page_sk AS k, cs_sold_date_sk AS dsk,
             cs_ext_sales_price AS sales_price, CAST(0 AS DECIMAL(7,2)) AS return_amt
      FROM catalog_sales
      UNION ALL
      SELECT cr_catalog_page_sk, cr_returned_date_sk, CAST(0 AS DECIMAL(7,2)), cr_return_amount
      FROM catalog_returns
    ) salesreturns
  ) csr
) x
GROUP BY channel
ORDER BY channel
";

#[tokio::test]
async fn q5_web_leg_no_dim_join_row_class_sums_match() {
    // Absolute pins on the single-node ground truth first: the fixture's row classes must
    // produce exactly these sums (the distributed comparison alone would not show that the
    // data exercises the unmatched/NULL/fanout classes).
    let planner = q5_planner_engine();
    let expected = planner.sql(Q5_WEB_LEG_NO_DIMS).await.expect("single-node");
    let want: Vec<Vec<String>> = vec![
        vec!["catalog", "26.00", "3.00"],
        vec!["store", "1479.00", "7.00"],
        vec!["web", "160.00", "42.00"],
    ]
    .into_iter()
    .map(|r| r.into_iter().map(String::from).collect())
    .collect();
    assert_eq!(
        rows_sorted(&expected),
        want,
        "single-node row-class sums (web returns_ = 42.00; a broken per-worker LEFT JOIN \
         shape would read 60.00)"
    );

    let cluster = q5_two_workers().await;
    assert_matches(
        "q5-web-leg-no-dims",
        Q5_WEB_LEG_NO_DIMS,
        &REPL,
        planner,
        cluster,
        |dq| {
            assert!(
                dq.stages.iter().any(|s| s
                    .sql
                    .contains("LEFT OUTER JOIN (SELECT * FROM shuffle_input_0) AS web_sales")),
                "the web-leg bucket substitutes the co-located shuffle inputs: {dq:?}"
            );
        },
    )
    .await;
}

// ---------------------------------------------------------------------------
// (g) Web-leg decline pins: the co-located composition's preconditions fail, so the arm
//     keeps the strict refusal. Each pin mutates ONLY the web arm's returns branch of the
//     two-arm template so the shape routes to the admission arm it pins.
// ---------------------------------------------------------------------------

/// The two-arm (web + catalog) nested-union template with a caller-chosen web returns
/// branch, routing the mutated branch through the co-located-LEFT-JOIN admission arm.
fn web_leg_branch_sql(returns_branch: &str) -> String {
    format!(
        "SELECT channel, id, SUM(sales) AS sales
FROM (
  SELECT 'web' AS channel, wsr.web_site_id AS id, wsr.sales AS sales
  FROM (
    SELECT web_site.web_site_id, SUM(salesreturns.sales_price) AS sales
    FROM (
      SELECT ws_web_site_sk AS k, ws_sold_date_sk AS dsk, ws_ext_sales_price AS sales_price
      FROM web_sales
      UNION ALL
      {returns_branch}
    ) salesreturns, date_dim, web_site
    WHERE salesreturns.dsk = date_dim.d_date_sk AND date_dim.d_year = 2001
      AND salesreturns.k = web_site.web_site_sk
    GROUP BY web_site.web_site_id
  ) wsr
  UNION ALL
  SELECT 'catalog' AS channel, csr.cp_catalog_page_id AS id, csr.sales AS sales
  FROM (
    SELECT catalog_page.cp_catalog_page_id, SUM(salesreturns.sales_price) AS sales
    FROM (
      SELECT cs_catalog_page_sk AS k, cs_sold_date_sk AS dsk, cs_ext_sales_price AS sales_price
      FROM catalog_sales
      UNION ALL
      SELECT cr_catalog_page_sk, cr_returned_date_sk, cr_return_amount
      FROM catalog_returns
    ) salesreturns, date_dim, catalog_page
    WHERE salesreturns.dsk = date_dim.d_date_sk AND date_dim.d_year = 2001
      AND salesreturns.k = catalog_page.cp_catalog_page_sk
    GROUP BY catalog_page.cp_catalog_page_id
  ) csr
) x
GROUP BY channel, id"
    )
}

/// q5's authentic returns branch (the admitted shape the pins mutate).
const WEB_LEG_BRANCH: &str = "\
SELECT web_sales.ws_web_site_sk, web_returns.wr_returned_date_sk, CAST(0 AS DOUBLE)
FROM web_returns
LEFT JOIN web_sales ON web_returns.wr_item_sk = web_sales.ws_item_sk
                   AND web_returns.wr_order_number = web_sales.ws_order_number";

/// A residual non-equality conjunct: the composition needs a pure equijoin.
const WEB_LEG_BRANCH_RESIDUAL: &str = "\
SELECT web_sales.ws_web_site_sk, web_returns.wr_returned_date_sk, CAST(0 AS DOUBLE)
FROM web_returns
LEFT JOIN web_sales ON web_returns.wr_item_sk = web_sales.ws_item_sk
                   AND web_returns.wr_order_number > web_sales.ws_order_number";

/// A non-plain-column key: the shuffle key positions must be plain columns on both sides.
const WEB_LEG_BRANCH_EXPR_KEY: &str = "\
SELECT web_sales.ws_web_site_sk, web_returns.wr_returned_date_sk, CAST(0 AS DOUBLE)
FROM web_returns
LEFT JOIN web_sales ON web_returns.wr_item_sk + 1 = web_sales.ws_item_sk
                   AND web_returns.wr_order_number = web_sales.ws_order_number";

/// The flipped `RIGHT JOIN` spelling of the same semantics: the finder matches
/// `JoinType::Left` only, so this declines. (The reverse — `web_returns RIGHT JOIN
/// web_sales`, sharded side preserved — is the q80-safe shape and takes the flat path.)
const WEB_LEG_BRANCH_RIGHT_SPELLING: &str = "\
SELECT web_sales.ws_web_site_sk, web_returns.wr_returned_date_sk, CAST(0 AS DOUBLE)
FROM web_sales
RIGHT JOIN web_returns ON web_returns.wr_item_sk = web_sales.ws_item_sk
                      AND web_returns.wr_order_number = web_sales.ws_order_number";

/// The branch scans the sharded fact TWICE (a self LEFT JOIN): count==2 admission fails.
const WEB_LEG_BRANCH_SELF_JOIN: &str = "\
SELECT b.ws_web_site_sk, a.ws_sold_date_sk, CAST(0 AS DOUBLE)
FROM web_sales a
LEFT JOIN web_sales b ON a.ws_item_sk = b.ws_item_sk AND a.ws_order_number = b.ws_order_number";

/// The preserved side is a DIFFERENT sharded table: only a fully-replicated preserved side
/// is co-locatable.
const WEB_LEG_BRANCH_OTHER_SHARDED_PRESERVED: &str = "\
SELECT web_sales.ws_web_site_sk, store_sales.ss_sold_date_sk, CAST(0 AS DOUBLE)
FROM store_sales
LEFT JOIN web_sales ON store_sales.ss_store_sk = web_sales.ws_item_sk
                   AND store_sales.ss_sold_date_sk = web_sales.ws_order_number";

/// The null-extended side peels to a UNION, not a single scan of the fact.
const WEB_LEG_BRANCH_UNION_RIGHT: &str = "\
SELECT web_sales.ws_web_site_sk, web_returns.wr_returned_date_sk, CAST(0 AS DOUBLE)
FROM web_returns
LEFT JOIN (
  SELECT ws_item_sk, ws_order_number, ws_web_site_sk FROM web_sales
  UNION ALL
  SELECT ws_item_sk, ws_order_number, ws_web_site_sk FROM web_sales
) web_sales ON web_returns.wr_item_sk = web_sales.ws_item_sk
           AND web_returns.wr_order_number = web_sales.ws_order_number";

/// Sanity: the template itself (authentic branch) still PLANS, so a pin failure is the
/// mutation's doing, not the harness's.
#[tokio::test]
async fn web_leg_branch_template_plans() {
    let planner = planner_engine();
    let sql = web_leg_branch_sql(WEB_LEG_BRANCH);
    let lp = planner.logical_plan(&sql).await.expect("logical plan");
    let dq = plan_strict(&lp, &REPL, "web-leg-template");
    assert!(
        dq.stages.iter().any(|s| s.sql.contains("LEFT OUTER JOIN")),
        "the template's web leg runs the co-located composition: {dq:?}"
    );
}

#[tokio::test]
async fn q5_web_leg_residual_join_filter_declines_safely() {
    assert_declines(
        "q5-web-leg-residual-join-filter",
        &web_leg_branch_sql(WEB_LEG_BRANCH_RESIDUAL),
    )
    .await;
}

#[tokio::test]
async fn q5_web_leg_non_column_key_declines_safely() {
    assert_declines(
        "q5-web-leg-non-column-key",
        &web_leg_branch_sql(WEB_LEG_BRANCH_EXPR_KEY),
    )
    .await;
}

#[tokio::test]
async fn q5_web_leg_right_join_spelling_declines_safely() {
    assert_declines(
        "q5-web-leg-right-join-spelling",
        &web_leg_branch_sql(WEB_LEG_BRANCH_RIGHT_SPELLING),
    )
    .await;
}

#[tokio::test]
async fn q5_web_leg_self_join_scan_twice_declines_safely() {
    assert_declines(
        "q5-web-leg-self-join-scan-twice",
        &web_leg_branch_sql(WEB_LEG_BRANCH_SELF_JOIN),
    )
    .await;
}

#[tokio::test]
async fn q5_web_leg_other_sharded_preserved_declines_safely() {
    assert_declines(
        "q5-web-leg-other-sharded-preserved",
        &web_leg_branch_sql(WEB_LEG_BRANCH_OTHER_SHARDED_PRESERVED),
    )
    .await;
}

#[tokio::test]
async fn q5_web_leg_union_right_side_declines_safely() {
    assert_declines(
        "q5-web-leg-union-right-side",
        &web_leg_branch_sql(WEB_LEG_BRANCH_UNION_RIGHT),
    )
    .await;
}
