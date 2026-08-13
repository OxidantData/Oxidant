//! KAN-162: aggregate over a `UNION ALL` whose arms collectively scan two or more sharded
//! tables. KAN-161's 4 GiB auto-broadcast threshold shards every sales fact at SF100, so the
//! per-channel union shapes (Q2/Q33/Q56/Q60/Q66/Q71/Q75/Q76/Q80) no longer fit
//! `try_split_broadcast_union`'s exactly-one-sharded-arm shape, and the shuffle join chain has
//! no UNION vocabulary — they regressed to a strict refusal at v0.1.11.
//!
//! The fix admits, in `aggregation_stages_for`, a union whose EVERY arm scans at most one
//! sharded table, exactly once, in a broadcast-safe tree (KAN-161's admission predicate per
//! arm), and plans one partial-aggregate producer per sharded table (plus one for the
//! replicated-only arms) hash-shuffled by the group key into a single associative recombine —
//! the two-producer union split generalized to N sharded arms.
//!
//! Shapes under test, all at the all-facts-sharded classification:
//!
//! - **(a) Q2** (real `q2.sql`): the branch-DAG's `wswscs` branch aggregates a
//!   `web_sales`+`catalog_sales` union joined to `date_dim`. Two sharded partial producers +
//!   recombine; must equal single-node row-for-row under `OXIDANT_DISTRIBUTED_STRICT=1`.
//! - **(b) Q5/Q77-like per-channel union**: three arms, each an aggregate over one sharded
//!   sales fact joined to broadcast dims, under an outer all-SUM aggregate. Three sharded
//!   producers + recombine; row-for-row.
//! - **Mixed union with a replicated arm**: two sharded arms + one `store_returns`
//!   replicated arm; the replicated producer runs once (`Forward`). Row-for-row.
//! - **ROLLUP over a two-sharded union**: grouping sets gather the producers to partition 0
//!   (`HAVING COUNT(*) > 0` keeps the grand-total row honest). Row-for-row.
//! - **(c) Declines**: a union arm scanning TWO sharded tables, and an arm scanning its
//!   sharded table twice (self-join), must keep declining safely in strict mode.

// ENV_LOCK serializes process-global `OXIDANT_DISTRIBUTED_STRICT` across async tests.
#![allow(clippy::await_holding_lock)]

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};

use oxidant_execution::driver::{run_stages, Cluster, ExchangeMode};
use oxidant_execution::flight::serve_worker;
use oxidant_execution::plan::{plan_distributed_logical, DistributedQuery};
use oxidant_loom::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::arrow::util::display::{ArrayFormatter, FormatOptions};
use oxidant_loom::Engine;

const Q2: &str = include_str!("../../../bench/tpcds/queries/q2.sql");

/// The v0.1.11 SF100 classification: every sales fact sharded, everything else replicated.
/// (`store_sales` stays replicated in the Q2 classification — that query never scans it; the
/// list only needs to cover each query's non-sharded tables.)
const REPL_Q2: [&str; 2] = ["date_dim", "store_sales"];
const REPL_ALL_BUT_SALES: [&str; 2] = ["date_dim", "store_returns"];

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

/// Two 2001 weeks (seq 100/101) and the 2002 week 53 ahead of seq 100, so Q2's
/// `d_week_seq1 = d_week_seq2 - 53` self-join produces rows.
fn date_dim() -> RecordBatch {
    batch(
        vec![
            i64f("d_date_sk"),
            i64f("d_year"),
            i64f("d_week_seq"),
            strf("d_day_name"),
        ],
        vec![
            i64v(&[1, 2, 3, 4, 5, 6]),
            i64v(&[2001, 2001, 2001, 2002, 2002, 2001]),
            i64v(&[100, 100, 100, 153, 153, 101]),
            strv(&[
                "Sunday",
                "Monday",
                "Wednesday",
                "Sunday",
                "Tuesday",
                "Friday",
            ]),
        ],
    )
}

/// Store-sk 1 spans both row halves, so its partial aggregate genuinely recombines across
/// workers.
fn store_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ss_sold_date_sk"),
            i64f("ss_store_sk"),
            i64f("ss_item_sk"),
            f64f("ss_ext_sales_price"),
            f64f("ss_net_profit"),
        ],
        vec![
            i64v(&[1, 1, 2, 2]),
            i64v(&[1, 2, 1, 3]),
            i64v(&[10, 11, 10, 12]),
            f64v(&[100.0, 200.0, 50.0, 70.0]),
            f64v(&[10.0, 20.0, 5.0, 7.0]),
        ],
    )
}

fn catalog_sales() -> RecordBatch {
    batch(
        vec![
            i64f("cs_sold_date_sk"),
            i64f("cs_call_center_sk"),
            i64f("cs_order_number"),
            f64f("cs_ext_sales_price"),
            f64f("cs_net_profit"),
        ],
        vec![
            i64v(&[1, 4, 5, 6]),
            i64v(&[1, 2, 1, 3]),
            i64v(&[100, 101, 102, 103]),
            f64v(&[5.0, 8.0, 7.0, 6.0]),
            f64v(&[0.5, 0.8, 0.7, 0.6]),
        ],
    )
}

/// Web-page 1 spans both row halves.
fn web_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ws_sold_date_sk"),
            i64f("ws_web_page_sk"),
            i64f("ws_order_number"),
            f64f("ws_ext_sales_price"),
            f64f("ws_net_profit"),
        ],
        vec![
            i64v(&[1, 2, 4, 3]),
            i64v(&[1, 2, 1, 1]),
            i64v(&[200, 201, 202, 203]),
            f64v(&[10.0, 20.0, 40.0, 30.0]),
            f64v(&[1.0, 2.0, 4.0, 3.0]),
        ],
    )
}

fn store_returns() -> RecordBatch {
    batch(
        vec![
            i64f("sr_returned_date_sk"),
            i64f("sr_store_sk"),
            f64f("sr_return_amt"),
        ],
        vec![i64v(&[1, 2]), i64v(&[1, 2]), f64v(&[3.0, 4.0])],
    )
}

fn register(engine: &Engine, name: &str, batches: Vec<RecordBatch>) {
    engine.register_batches(name, batches).unwrap();
}

/// Planner/ground-truth engine holding the full dataset.
fn planner_engine() -> Engine {
    let e = Engine::new();
    register(&e, "date_dim", vec![date_dim()]);
    register(&e, "store_sales", vec![store_sales()]);
    register(&e, "catalog_sales", vec![catalog_sales()]);
    register(&e, "web_sales", vec![web_sales()]);
    register(&e, "store_returns", vec![store_returns()]);
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

/// Every sharded table split row-wise across two in-process workers; replicated tables held in
/// full on each worker (the production replicated-table invariant).
async fn two_workers(sharded: &[&str]) -> Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        for (name, full) in [
            ("date_dim", date_dim()),
            ("store_sales", store_sales()),
            ("catalog_sales", catalog_sales()),
            ("web_sales", web_sales()),
            ("store_returns", store_returns()),
        ] {
            if sharded.contains(&name) {
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

/// Plan strict, run on the two-worker cluster, require row-for-row equality with single-node.
/// `sharded` names the row-sharded tables on the workers; `check_plan` pins the stage shape.
async fn assert_matches_single_node(
    tag: &str,
    sql: &str,
    replicated: &[&str],
    sharded: &[&str],
    check_plan: impl Fn(&DistributedQuery),
) {
    let planner = planner_engine();
    let expected = planner.sql(sql).await.expect("single-node");
    assert!(
        expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
        "{tag}: single-node result must be non-empty (otherwise the comparison is vacuous)"
    );

    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let dq = plan_strict(&lp, replicated, tag);
    check_plan(&dq);

    let cluster = two_workers(sharded).await;
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
// (a) Q2: branch aggregate over a web_sales+catalog_sales union (real q2.sql)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn q2_union_of_two_sharded_facts_matches_single_node() {
    assert_matches_single_node("q2", Q2, &REPL_Q2, &["web_sales", "catalog_sales"], |dq| {
        let ws = leaf_stages_scanning(dq, "web_sales");
        let cs = leaf_stages_scanning(dq, "catalog_sales");
        assert_eq!(ws.len(), 1, "one web_sales partial producer: {dq:?}");
        assert_eq!(cs.len(), 1, "one catalog_sales partial producer: {dq:?}");
        for s in ws.into_iter().chain(cs) {
            assert!(
                !s.hash_key_cols.is_empty(),
                "producer hash-shuffles by the group key: {s:?}"
            );
        }
        assert!(
            dq.stages.iter().any(|s| {
                s.sql.contains("UNION ALL")
                    && s.sql.contains("shuffle_input_0")
                    && s.sql.contains("shuffle_input_1")
            }),
            "a recombine merges both producer streams: {dq:?}"
        );
    })
    .await;
}

// ---------------------------------------------------------------------------
// (b) Q5/Q77-like per-channel union: three arms, each an aggregate over one sharded fact
// ---------------------------------------------------------------------------

const PER_CHANNEL: &str = "
SELECT channel, id, SUM(sales) AS total_sales, SUM(profit) AS total_profit
FROM (
  SELECT 'store' AS channel, ss_store_sk AS id,
         SUM(ss_ext_sales_price) AS sales, SUM(ss_net_profit) AS profit
  FROM store_sales JOIN date_dim ON ss_sold_date_sk = d_date_sk
  WHERE d_year = 2001
  GROUP BY ss_store_sk
  UNION ALL
  SELECT 'catalog' AS channel, cs_call_center_sk AS id,
         SUM(cs_ext_sales_price) AS sales, SUM(cs_net_profit) AS profit
  FROM catalog_sales JOIN date_dim ON cs_sold_date_sk = d_date_sk
  WHERE d_year = 2001
  GROUP BY cs_call_center_sk
  UNION ALL
  SELECT 'web' AS channel, ws_web_page_sk AS id,
         SUM(ws_ext_sales_price) AS sales, SUM(ws_net_profit) AS profit
  FROM web_sales JOIN date_dim ON ws_sold_date_sk = d_date_sk
  WHERE d_year = 2001
  GROUP BY ws_web_page_sk
) x
GROUP BY channel, id
ORDER BY channel, id
";

#[tokio::test]
async fn per_channel_union_of_three_sharded_facts_matches_single_node() {
    assert_matches_single_node(
        "per-channel",
        PER_CHANNEL,
        &REPL_ALL_BUT_SALES,
        &["store_sales", "catalog_sales", "web_sales"],
        |dq| {
            for fact in ["store_sales", "catalog_sales", "web_sales"] {
                let leaves = leaf_stages_scanning(dq, fact);
                assert_eq!(leaves.len(), 1, "one {fact} partial producer: {dq:?}");
                assert_eq!(
                    leaves[0].hash_key_cols,
                    vec![0, 1],
                    "{fact} producer hash-shuffles by (channel, id)"
                );
            }
            let combine = dq
                .stages
                .iter()
                .find(|s| s.sql.contains("shuffle_input_2"))
                .expect("recombine over all three producer streams");
            assert_eq!(
                combine.upstream_stage_ids.len(),
                3,
                "three producers: {dq:?}"
            );
        },
    )
    .await;
}

// ---------------------------------------------------------------------------
// Mixed union: two sharded arms + one replicated arm (replicated producer runs once)
// ---------------------------------------------------------------------------

const MIXED_REPLICATED_ARM: &str = "
SELECT d_year, SUM(amt) AS total
FROM (
  SELECT ss_sold_date_sk AS dsk, ss_ext_sales_price AS amt FROM store_sales
  UNION ALL
  SELECT ws_sold_date_sk AS dsk, ws_ext_sales_price AS amt FROM web_sales
  UNION ALL
  SELECT sr_returned_date_sk AS dsk, sr_return_amt AS amt FROM store_returns
) x JOIN date_dim ON dsk = d_date_sk
GROUP BY d_year
ORDER BY d_year
";

#[tokio::test]
async fn union_with_replicated_arm_matches_single_node() {
    assert_matches_single_node(
        "mixed-replicated-arm",
        MIXED_REPLICATED_ARM,
        &REPL_ALL_BUT_SALES,
        &["store_sales", "web_sales"],
        |dq| {
            assert_eq!(
                dq.stages.len(),
                4,
                "two sharded producers + one replicated producer + recombine: {dq:?}"
            );
            let returns = leaf_stages_scanning(dq, "store_returns");
            assert_eq!(returns.len(), 1, "one store_returns producer: {dq:?}");
            assert_eq!(
                returns[0].exchange,
                ExchangeMode::Forward,
                "replicated-only arm is computed once (no worker-count env set): {:?}",
                returns[0]
            );
            let combine = &dq.stages[3];
            assert_eq!(combine.upstream_stage_ids, vec![0, 1, 2]);
        },
    )
    .await;
}

// ---------------------------------------------------------------------------
// ROLLUP over a two-sharded union: producers gather to partition 0
// ---------------------------------------------------------------------------

const ROLLUP_UNION: &str = "
SELECT d_year, SUM(amt) AS total
FROM (
  SELECT ss_sold_date_sk AS dsk, ss_ext_sales_price AS amt FROM store_sales
  UNION ALL
  SELECT ws_sold_date_sk AS dsk, ws_ext_sales_price AS amt FROM web_sales
) x JOIN date_dim ON dsk = d_date_sk
GROUP BY ROLLUP(d_year)
ORDER BY d_year
";

#[tokio::test]
async fn rollup_over_two_sharded_union_matches_single_node() {
    assert_matches_single_node(
        "rollup-union",
        ROLLUP_UNION,
        &REPL_ALL_BUT_SALES,
        &["store_sales", "web_sales"],
        |dq| {
            assert_eq!(dq.stages.len(), 3, "two producers + recombine: {dq:?}");
            for s in &dq.stages[..2] {
                assert!(
                    s.hash_key_cols.is_empty(),
                    "grouping-set producers gather to partition 0: {s:?}"
                );
            }
            assert!(
                dq.stages[2].sql.contains("HAVING COUNT(*) > 0"),
                "empty non-zero partitions must not emit a grand-total row: {dq:?}"
            );
        },
    )
    .await;
}

// ---------------------------------------------------------------------------
// (c) Declines: arms outside the admission predicate keep the strict refusal
// ---------------------------------------------------------------------------

/// One arm scans TWO sharded tables (a fact⋈fact equijoin): not the N-producer union shape,
/// and the shuffle join chain still has no UNION vocabulary, so strict mode must refuse.
const ARM_WITH_TWO_SHARDED_TABLES: &str = "
SELECT d_year, SUM(amt) AS total
FROM (
  SELECT ws_sold_date_sk AS dsk, ws_ext_sales_price + cs_ext_sales_price AS amt
  FROM web_sales JOIN catalog_sales ON ws_order_number = cs_order_number
  UNION ALL
  SELECT ss_sold_date_sk AS dsk, ss_ext_sales_price AS amt FROM store_sales
) x JOIN date_dim ON dsk = d_date_sk
GROUP BY d_year
";

/// One arm scans its sharded table twice (self-join): not broadcast-safe per arm, so the
/// shape declines and strict mode must refuse.
const ARM_WITH_REPEATED_SHARDED_SCAN: &str = "
SELECT d_year, SUM(amt) AS total
FROM (
  SELECT s1.ss_sold_date_sk AS dsk, s1.ss_ext_sales_price + s2.ss_ext_sales_price AS amt
  FROM store_sales s1 JOIN store_sales s2 ON s1.ss_item_sk = s2.ss_item_sk
  UNION ALL
  SELECT cs_sold_date_sk AS dsk, cs_ext_sales_price AS amt FROM catalog_sales
) x JOIN date_dim ON dsk = d_date_sk
GROUP BY d_year
";

#[tokio::test]
async fn arm_with_two_sharded_tables_declines_safely() {
    let planner = planner_engine();
    let lp = planner
        .logical_plan(ARM_WITH_TWO_SHARDED_TABLES)
        .await
        .expect("logical plan");
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("OXIDANT_DISTRIBUTED_STRICT", "1");
    let planned = plan_distributed_logical(&lp, &REPL_ALL_BUT_SALES);
    std::env::remove_var("OXIDANT_DISTRIBUTED_STRICT");
    assert!(
        planned.is_err(),
        "arm scanning two sharded tables must keep declining in strict mode, got {planned:?}"
    );
}

#[tokio::test]
async fn arm_with_repeated_sharded_scan_declines_safely() {
    let planner = planner_engine();
    let lp = planner
        .logical_plan(ARM_WITH_REPEATED_SHARDED_SCAN)
        .await
        .expect("logical plan");
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("OXIDANT_DISTRIBUTED_STRICT", "1");
    let planned = plan_distributed_logical(&lp, &REPL_ALL_BUT_SALES);
    std::env::remove_var("OXIDANT_DISTRIBUTED_STRICT");
    assert!(
        planned.is_err(),
        "arm with a repeated sharded scan must keep declining in strict mode, got {planned:?}"
    );
}

// ---------------------------------------------------------------------------
// (d) Q77-like N-arm join-deferral: a sharded arm that joins a REPLICATED
//     per-key aggregate into its own per-key aggregate (KAN-49d's shape) gets
//     one stage-A–D deferral chain per bucket, recombined with the rest.
// ---------------------------------------------------------------------------

/// Two sharded arms, each `Projection over Join(Aggregate(sharded …), Aggregate(replicated
/// store_returns …))` — the KAN-49d two-producer shape once per sharded fact.
const AGG_JOIN_ARMS: &str = "
SELECT channel, id, SUM(sales) AS total_sales, SUM(returns_) AS total_returns
FROM (
  SELECT 'store' AS channel, s.id AS id, s.sales AS sales, r.returns_ AS returns_
  FROM (
    SELECT ss_store_sk AS id, SUM(ss_ext_sales_price) AS sales
    FROM store_sales JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE d_year = 2001
    GROUP BY ss_store_sk
  ) s
  LEFT JOIN (
    SELECT sr_store_sk AS id, SUM(sr_return_amt) AS returns_
    FROM store_returns
    GROUP BY sr_store_sk
  ) r ON s.id = r.id
  UNION ALL
  SELECT 'web' AS channel, s.id AS id, s.sales AS sales, r.returns_ AS returns_
  FROM (
    SELECT ws_web_page_sk AS id, SUM(ws_ext_sales_price) AS sales
    FROM web_sales
    GROUP BY ws_web_page_sk
  ) s
  LEFT JOIN (
    SELECT sr_store_sk AS id, SUM(sr_return_amt) AS returns_
    FROM store_returns
    GROUP BY sr_store_sk
  ) r ON s.id = r.id
) x
GROUP BY channel, id
ORDER BY channel, id
";

#[tokio::test]
async fn n_arm_agg_join_deferral_matches_single_node() {
    assert_matches_single_node(
        "agg-join-arms",
        AGG_JOIN_ARMS,
        &REPL_ALL_BUT_SALES,
        &["store_sales", "web_sales"],
        |dq| {
            // 2×4 deferral stages + recombine, minus one: the two buckets' stage-B replicated
            // aggregates are byte-identical (same store_returns subquery), so the stage-level
            // CSE merges them — the second chain's stage C reads stage 1 directly.
            assert_eq!(
                dq.stages.len(),
                8,
                "two stage-A–D deferral chains (stage B CSE-merged) + recombine: {dq:?}"
            );
            let deferred = dq
                .stages
                .iter()
                .filter(|s| s.sql.contains("LEFT JOIN (SELECT * FROM shuffle_input_1)"))
                .count();
            assert_eq!(deferred, 2, "one deferred join per sharded bucket: {dq:?}");
            let replicated_producers = dq
                .stages
                .iter()
                .filter(|s| s.exchange == ExchangeMode::Forward && s.sql.contains("store_returns"))
                .count();
            assert_eq!(
                replicated_producers, 1,
                "the identical replicated aggregates CSE to one once-computed stage: {dq:?}"
            );
            let combine = dq.stages.last().expect("combine stage");
            assert_eq!(
                combine.upstream_stage_ids,
                vec![3, 7],
                "the recombine reads exactly the two stage-D terminals: {dq:?}"
            );
            for id in [3u32, 7] {
                let d = dq
                    .stages
                    .iter()
                    .find(|s| s.stage_id == id)
                    .expect("stage-D terminal");
                assert_eq!(
                    d.hash_key_cols,
                    vec![0, 1],
                    "stage D hash-shuffles by the outer (channel, id) key: {d:?}"
                );
            }
        },
    )
    .await;
}

/// One deferred agg-join arm (store) plus one flat pre-aggregated arm (web): the recombine's
/// producer indexing must skip the deferral chain's interior stages.
const AGG_JOIN_MIXED: &str = "
SELECT channel, id, SUM(sales) AS total_sales, SUM(returns_) AS total_returns
FROM (
  SELECT 'store' AS channel, s.id AS id, s.sales AS sales, r.returns_ AS returns_
  FROM (
    SELECT ss_store_sk AS id, SUM(ss_ext_sales_price) AS sales
    FROM store_sales
    GROUP BY ss_store_sk
  ) s
  LEFT JOIN (
    SELECT sr_store_sk AS id, SUM(sr_return_amt) AS returns_
    FROM store_returns
    GROUP BY sr_store_sk
  ) r ON s.id = r.id
  UNION ALL
  SELECT 'web' AS channel, ws_web_page_sk AS id,
         SUM(ws_ext_sales_price) AS sales, SUM(ws_net_profit) AS returns_
  FROM web_sales
  GROUP BY ws_web_page_sk
) x
GROUP BY channel, id
ORDER BY channel, id
";

#[tokio::test]
async fn mixed_deferral_and_flat_arms_match_single_node() {
    assert_matches_single_node(
        "agg-join-mixed",
        AGG_JOIN_MIXED,
        &REPL_ALL_BUT_SALES,
        &["store_sales", "web_sales"],
        |dq| {
            assert_eq!(
                dq.stages.len(),
                6,
                "one deferral chain (0-3) + one flat producer (4) + recombine (5): {dq:?}"
            );
            let combine = dq.stages.last().expect("combine stage");
            assert_eq!(
                combine.upstream_stage_ids,
                vec![3, 4],
                "the recombine reads the stage-D terminal and the flat producer: {dq:?}"
            );
            assert!(
                combine.sql.contains("shuffle_input_1"),
                "both producer streams merge: {combine:?}"
            );
        },
    )
    .await;
}

/// Decline pin: the arm join carrying a non-equality residual (`s.sales > r.returns_`) is not
/// the deferral shape — the whole union must keep declining in strict mode.
const AGG_JOIN_RESIDUAL: &str = "
SELECT channel, id, SUM(sales) AS total_sales
FROM (
  SELECT 'store' AS channel, s.id AS id, s.sales AS sales
  FROM (
    SELECT ss_store_sk AS id, SUM(ss_ext_sales_price) AS sales
    FROM store_sales
    GROUP BY ss_store_sk
  ) s
  LEFT JOIN (
    SELECT sr_store_sk AS id, SUM(sr_return_amt) AS returns_
    FROM store_returns
    GROUP BY sr_store_sk
  ) r ON s.id = r.id AND s.sales > r.returns_
  UNION ALL
  SELECT 'web' AS channel, ws_web_page_sk AS id, SUM(ws_ext_sales_price) AS sales
  FROM web_sales
  GROUP BY ws_web_page_sk
) x
GROUP BY channel, id
";

#[tokio::test]
async fn agg_join_arm_with_residual_declines_safely() {
    let planner = planner_engine();
    let lp = planner
        .logical_plan(AGG_JOIN_RESIDUAL)
        .await
        .expect("logical plan");
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("OXIDANT_DISTRIBUTED_STRICT", "1");
    let planned = plan_distributed_logical(&lp, &REPL_ALL_BUT_SALES);
    std::env::remove_var("OXIDANT_DISTRIBUTED_STRICT");
    assert!(
        planned.is_err(),
        "an agg-join arm with a non-equality residual must keep declining in strict mode, \
         got {planned:?}"
    );
}

/// Q77's catalog arm spelling: `FROM cs, cr` — a genuine CROSS PRODUCT of two per-key
/// aggregates (no join predicate). DataFusion 54 plans it as `Join{Inner, on: [], filter:
/// None}` (Display prints "Cross Join:"). The deferral routes it with BOTH producers gathered
/// whole to partition 0, where stage C computes the full cross product against the recombined
/// left aggregate — exact because laq holds exactly one row per key before the cross. The web
/// arm is a flat pre-aggregated producer, mirroring the mixed test above.
const AGG_JOIN_CROSS: &str = "
SELECT channel, id, SUM(sales) AS total_sales, SUM(returns_) AS total_returns
FROM (
  SELECT 'catalog' AS channel, s.id AS id, s.sales AS sales, r.returns_ AS returns_
  FROM (
    SELECT cs_call_center_sk AS id, SUM(cs_ext_sales_price) AS sales
    FROM catalog_sales
    GROUP BY cs_call_center_sk
  ) s,
  (
    SELECT sr_store_sk AS id, SUM(sr_return_amt) AS returns_
    FROM store_returns
    GROUP BY sr_store_sk
  ) r
  UNION ALL
  SELECT 'store' AS channel, ss_store_sk AS id,
         SUM(ss_ext_sales_price) AS sales, SUM(ss_net_profit) AS returns_
  FROM store_sales
  GROUP BY ss_store_sk
) x
GROUP BY channel, id
ORDER BY channel, id
";

#[tokio::test]
async fn cross_join_agg_arm_deferral_matches_single_node() {
    assert_matches_single_node(
        "agg-join-cross",
        AGG_JOIN_CROSS,
        &REPL_ALL_BUT_SALES,
        &["catalog_sales", "store_sales"],
        |dq| {
            assert_eq!(
                dq.stages.len(),
                6,
                "one cross-join deferral chain (0-3) + one flat producer (4) + recombine (5): \
                 {dq:?}"
            );
            let stage_c = &dq.stages[2];
            assert!(
                stage_c
                    .sql
                    .contains("CROSS JOIN (SELECT * FROM shuffle_input_1) AS raq"),
                "stage C is the cross product against the recombined left aggregate: {stage_c:?}"
            );
            let stage_a = &dq.stages[0];
            assert!(
                stage_a.hash_key_cols.is_empty(),
                "no co-location key exists for a cross join: the left partials gather whole \
                 to partition 0: {stage_a:?}"
            );
            let stage_b = &dq.stages[1];
            assert!(
                stage_b.exchange == ExchangeMode::Forward && stage_b.hash_key_cols.is_empty(),
                "the replicated aggregate computes once and gathers whole to partition 0: \
                 {stage_b:?}"
            );
            let combine = dq.stages.last().expect("combine stage");
            assert_eq!(
                combine.upstream_stage_ids,
                vec![3, 4],
                "the recombine reads the stage-D terminal and the flat producer: {dq:?}"
            );
            assert_eq!(
                dq.stages[3].hash_key_cols,
                vec![0, 1],
                "stage D hash-shuffles by the outer (channel, id) key: {dq:?}"
            );
        },
    )
    .await;
}
