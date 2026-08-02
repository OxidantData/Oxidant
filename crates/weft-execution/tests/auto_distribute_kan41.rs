//! KAN-41: TPC-DS Q11/Q58/Q78 timed out at SF10 (>600s) on shapes whose *branch-aware* plans
//! wasted whole-cluster work:
//!
//! - **Q58/Q78 (replicated aggregate branches inlined in the gathered outer stage).** The
//!   branch DAG splitter kept replicated-only aggregate branches — Q58's `cs_items` over
//!   catalog_sales / `ws_items` over web_sales, Q78's `cs`/`ws` arms — inline in the outer
//!   join stage. That stage runs once per shuffle partition (16 at SF10), so the full
//!   replicated-fact scan + join + aggregate was recomputed 16× while only partition 0's copy
//!   fed the join. The splitter now materializes such a branch as a single `Forward` stage
//!   (computed exactly once on one worker; every worker already holds the replicated inputs).
//! - **Q11 (one CTE self-joined N times).** `year_total` inlined as 4 structurally identical
//!   branch sub-DAGs, so store_sales/web_sales were scanned and aggregated 4×. Identical
//!   deterministic branches are now planned once and every outer placeholder points at the
//!   same shuffle output (volatile branches are never deduplicated).
//!
//! These tests lock both shapes end-to-end: distributed must equal single-node, the outer
//! stage must not re-scan the replicated facts, and a self-joined CTE must produce exactly one
//! branch sub-DAG.

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use weft_execution::driver::{run_stages, Cluster, ExchangeMode};
use weft_execution::flight::serve_worker;
use weft_execution::plan::plan_distributed_logical;
use weft_loom::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use weft_loom::arrow::datatypes::{DataType, Field, Schema};
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::arrow::util::display::{ArrayFormatter, FormatOptions};
use weft_loom::Engine;

/// Serialize port allocation across tests in this binary (same rationale as
/// `tests/auto_distribute.rs`: bind/drop races steal ports under parallel tests).
static PORT: std::sync::OnceLock<AtomicU16> = std::sync::OnceLock::new();

fn unique_worker_port() -> u16 {
    // OnceLock-seeded allocator with the base BELOW the Linux ephemeral source range
    // (32768..=60999): the harness's own outbound connections can never steal a worker's
    // port (serve_worker swallows EADDRINUSE; the old in-range bases flaked "did not
    // bind" / "distributed run never succeeded" on loaded CI runners).
    PORT.get_or_init(|| AtomicU16::new(15000 + (std::process::id() as u16 % 512)))
        .fetch_add(1, Ordering::Relaxed)
}

fn i64f(name: &str) -> Field {
    Field::new(name, DataType::Int64, false)
}
fn f64f(name: &str) -> Field {
    Field::new(name, DataType::Float64, false)
}
fn utf8f(name: &str) -> Field {
    Field::new(name, DataType::Utf8, false)
}

fn batch(fields: Vec<Field>, cols: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
}

/// Replicated `item` dimension: sk 1..=4 → id 'a'..'d'.
fn item() -> RecordBatch {
    batch(
        vec![i64f("i_item_sk"), utf8f("i_item_id")],
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
        ],
    )
}

/// Sales rows `(item_sk, customer_sk, yr, price)`. Item 1 and customer 1 span both shards.
fn sales(rows: &[(i64, i64, i64, f64)], price_col: &str) -> RecordBatch {
    batch(
        vec![
            i64f("item_sk"),
            i64f("customer_sk"),
            i64f("yr"),
            f64f(price_col),
        ],
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.3).collect::<Vec<_>>(),
            )),
        ],
    )
}

fn store0() -> RecordBatch {
    sales(
        &[(1, 1, 2001, 100.0), (2, 2, 2001, 50.0), (3, 3, 2002, 10.0)],
        "ss_price",
    )
}
fn store1() -> RecordBatch {
    sales(
        &[(1, 1, 2002, 300.0), (2, 4, 2002, 70.0), (4, 5, 2001, 20.0)],
        "ss_price",
    )
}
fn catalog() -> RecordBatch {
    sales(
        &[(1, 1, 2001, 400.0), (2, 2, 2001, 500.0), (3, 9, 2001, 10.0)],
        "cs_price",
    )
}
fn web() -> RecordBatch {
    sales(
        &[(1, 1, 2001, 420.0), (2, 2, 2001, 490.0), (4, 9, 2001, 10.0)],
        "ws_price",
    )
}

/// Planner/ground-truth engine holding the full dataset.
fn full_engine() -> Engine {
    let e = Engine::new();
    e.register_batches("item", vec![item()]).unwrap();
    e.register_batches("store_sales", vec![store0(), store1()])
        .unwrap();
    e.register_batches("catalog_sales", vec![catalog()])
        .unwrap();
    e.register_batches("web_sales", vec![web()]).unwrap();
    e
}

/// `store_sales` sharded row-wise over two workers; everything else fully replicated (the
/// auto-broadcast layout at SF10: largest scanned table sharded, the rest replicated).
async fn two_workers() -> Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        e.register_batches("item", vec![item()]).unwrap();
        let shard = if i == 0 { store0() } else { store1() };
        e.register_batches("store_sales", vec![shard]).unwrap();
        e.register_batches("catalog_sales", vec![catalog()])
            .unwrap();
        e.register_batches("web_sales", vec![web()]).unwrap();
        tokio::spawn(async move {
            let _ = serve_worker(port, e).await;
        });
    }
    Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ])
}

const REPLICATED: [&str; 3] = ["item", "catalog_sales", "web_sales"];

/// Plan `sql` with only `store_sales` sharded and run the stages on `cluster`, applying the
/// driver's global finalize. Mirrors `tests/auto_distribute_kan44.rs::run_distributed`.
async fn run_distributed(
    cluster: &Cluster,
    planner: &Engine,
    sql: &str,
) -> (weft_execution::plan::DistributedQuery, Vec<RecordBatch>) {
    run_distributed_with(cluster, planner, sql, &REPLICATED).await
}

/// [`run_distributed`] with an explicit replicated-table list (per-shape engine layouts).
async fn run_distributed_with(
    cluster: &Cluster,
    planner: &Engine,
    sql: &str,
    replicated: &[&str],
) -> (weft_execution::plan::DistributedQuery, Vec<RecordBatch>) {
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, replicated).expect("plan_distributed_logical");
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
    let batches = match &dq.finalize_sql {
        None => gathered,
        Some(fsql) => {
            let fin = Engine::new();
            fin.register_batches("result", gathered).unwrap();
            fin.sql(fsql).await.expect("finalize")
        }
    };
    (dq, batches)
}

/// Sorted value rows, mirroring the bench's `normalize_batches`.
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

/// Q58's shape, minimized: three per-item revenue aggregates — one over the sharded fact, two
/// over replicated facts — inner-joined on the item id.
const Q58_SHAPE: &str = "
WITH ss_items AS (
    SELECT i_item_id AS item_id, sum(ss_price) AS ss_rev
    FROM store_sales JOIN item ON store_sales.item_sk = item.i_item_sk GROUP BY i_item_id
),
cs_items AS (
    SELECT i_item_id AS item_id, sum(cs_price) AS cs_rev
    FROM catalog_sales JOIN item ON catalog_sales.item_sk = item.i_item_sk GROUP BY i_item_id
),
ws_items AS (
    SELECT i_item_id AS item_id, sum(ws_price) AS ws_rev
    FROM web_sales JOIN item ON web_sales.item_sk = item.i_item_sk GROUP BY i_item_id
)
SELECT ss_items.item_id, ss_rev, cs_rev, ws_rev
FROM ss_items, cs_items, ws_items
WHERE ss_items.item_id = cs_items.item_id
  AND ss_items.item_id = ws_items.item_id
  AND ss_rev BETWEEN 0.9 * cs_rev AND 1.1 * cs_rev
  AND ss_rev BETWEEN 0.9 * ws_rev AND 1.1 * ws_rev
ORDER BY ss_items.item_id";

/// The replicated aggregate branches must each be a single `Forward` stage (computed once);
/// the gathered outer stage must join placeholders only — never re-scan `catalog_sales` /
/// `web_sales` once per shuffle partition.
#[tokio::test]
async fn q58_shape_materializes_replicated_aggregate_branches_once() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let planner = full_engine();
    let lp = planner.logical_plan(Q58_SHAPE).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &REPLICATED).expect("Q58 shape should plan");

    let forward = dq
        .stages
        .iter()
        .filter(|s| s.exchange == ExchangeMode::Forward)
        .count();
    assert_eq!(
        forward, 2,
        "cs_items/ws_items must each materialize as one Forward stage: {dq:?}"
    );
    let outer = &dq.stages.last().unwrap().sql;
    assert!(
        !outer.contains("catalog_sales") && !outer.contains("web_sales"),
        "outer stage must not re-scan the replicated facts per partition: {outer}"
    );
    assert!(
        outer.contains("shuffle_input_1") && outer.contains("shuffle_input_2"),
        "outer stage must join the materialized branch placeholders: {outer}"
    );
}

/// End-to-end: item 1's store rows span both shards; distributed must equal single-node.
#[tokio::test]
async fn q58_shape_distributed_matches_single_node() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let planner = full_engine();
    let expected = planner.sql(Q58_SHAPE).await.expect("single-node");
    assert!(
        expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
        "test data must produce a non-empty result"
    );
    let cluster = two_workers().await;
    let (_, actual) = run_distributed(&cluster, &planner, Q58_SHAPE).await;
    assert_eq!(
        rows_sorted(&actual),
        rows_sorted(&expected),
        "distributed must equal single-node (replicated branches computed once, exactly)"
    );
}

/// Q78's shape, minimized: a sharded-fact aggregate LEFT JOINed to two replicated-fact
/// aggregates — the replicated branches sit on the non-preserved side, so materializing them
/// keeps the outer skeleton gated.
const Q78_SHAPE: &str = "
WITH ss AS (
    SELECT customer_sk, sum(ss_price) AS ss_rev
    FROM store_sales GROUP BY customer_sk
),
cs AS (
    SELECT customer_sk, sum(cs_price) AS cs_rev
    FROM catalog_sales GROUP BY customer_sk
),
ws AS (
    SELECT customer_sk, sum(ws_price) AS ws_rev
    FROM web_sales GROUP BY customer_sk
)
SELECT ss.customer_sk, ss_rev, coalesce(cs_rev, 0) + coalesce(ws_rev, 0) AS other_rev
FROM ss
LEFT JOIN cs ON cs.customer_sk = ss.customer_sk
LEFT JOIN ws ON ws.customer_sk = ss.customer_sk
WHERE coalesce(cs_rev, 0) > 0 OR coalesce(ws_rev, 0) > 0
ORDER BY ss.customer_sk";

/// LEFT JOIN skeleton: the replicated branches are on the non-preserved side, so they may be
/// materialized as Forward stages (the preserved side stays a gated sharded branch).
#[tokio::test]
async fn q78_shape_materializes_replicated_branches_under_left_join() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let planner = full_engine();
    let lp = planner.logical_plan(Q78_SHAPE).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &REPLICATED).expect("Q78 shape should plan");

    let forward = dq
        .stages
        .iter()
        .filter(|s| s.exchange == ExchangeMode::Forward)
        .count();
    assert_eq!(
        forward, 2,
        "cs/ws arms must each materialize as one Forward stage: {dq:?}"
    );
    let outer = &dq.stages.last().unwrap().sql;
    assert!(
        outer.to_uppercase().contains("LEFT OUTER JOIN"),
        "outer stage keeps the LEFT JOIN skeleton: {outer}"
    );
    assert!(
        !outer.contains("catalog_sales") && !outer.contains("web_sales"),
        "outer stage must not re-scan the replicated facts per partition: {outer}"
    );

    let cluster = two_workers().await;
    let expected = planner.sql(Q78_SHAPE).await.expect("single-node");
    assert!(
        expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
        "test data must produce a non-empty result"
    );
    let (_, actual) = run_distributed(&cluster, &planner, Q78_SHAPE).await;
    assert_eq!(
        rows_sorted(&actual),
        rows_sorted(&expected),
        "distributed must equal single-node"
    );
}

/// Q11's shape, minimized: one aggregated CTE self-joined on its key with per-alias filters.
const Q11_SHAPE: &str = "
WITH year_total AS (
    SELECT customer_sk AS cid, yr, sum(ss_price) AS total
    FROM store_sales GROUP BY customer_sk, yr
)
SELECT a.cid, a.total AS first_total, b.total AS second_total
FROM year_total a, year_total b
WHERE a.cid = b.cid AND a.yr = 2001 AND b.yr = 2002 AND b.total > a.total
ORDER BY a.cid";

/// A self-joined CTE must be planned as ONE branch sub-DAG: the outer stage pulls the same
/// shuffle output at every placeholder position instead of re-scanning/re-aggregating the
/// fact per reference (Q11 had 4 identical 4-stage sub-DAGs — 17 stages — at SF10).
#[tokio::test]
async fn q11_shape_deduplicates_identical_cte_branches() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let planner = full_engine();
    let lp = planner.logical_plan(Q11_SHAPE).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &REPLICATED).expect("Q11 shape should plan");

    assert_eq!(
        dq.stages.len(),
        3,
        "one branch sub-DAG (partial + combine) plus the outer join stage: {dq:?}"
    );
    let outer = &dq.stages.last().unwrap();
    assert_eq!(
        outer.upstream_stage_ids.len(),
        2,
        "both aliases read a shuffle placeholder: {outer:?}"
    );
    assert_eq!(
        outer.upstream_stage_ids[0], outer.upstream_stage_ids[1],
        "identical branches share one shuffle output: {outer:?}"
    );
    assert!(
        outer.sql.contains("shuffle_input_0") && outer.sql.contains("shuffle_input_1"),
        "outer stage joins both aliased placeholders: {}",
        outer.sql
    );
}

/// End-to-end: customer 1's rows span both shards; the shared branch output must feed both
/// aliases correctly.
#[tokio::test]
async fn q11_shape_distributed_matches_single_node() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let planner = full_engine();
    let expected = planner.sql(Q11_SHAPE).await.expect("single-node");
    assert!(
        expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
        "test data must produce a non-empty result"
    );
    let cluster = two_workers().await;
    let (_, actual) = run_distributed(&cluster, &planner, Q11_SHAPE).await;
    assert_eq!(
        rows_sorted(&actual),
        rows_sorted(&expected),
        "distributed must equal single-node (deduplicated branch feeds every alias)"
    );
}

// --- Q88: time-bucket aggregate branches sharing one star scan ---

/// Replicated `time_dim`: sk 1=(8,45), 2=(9,10), 3=(9,45), 4=(12,15) — one sk per Q88
/// half-hour bucket, plus an hour no branch selects.
fn q88_time_dim() -> RecordBatch {
    batch(
        vec![i64f("t_time_sk"), i64f("t_hour"), i64f("t_minute")],
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int64Array::from(vec![8, 9, 9, 12])),
            Arc::new(Int64Array::from(vec![45, 10, 45, 15])),
        ],
    )
}

/// Replicated `household_demographics`: sk 1 qualifies (dep 2, vehicle 3 ≤ 2+2), sk 2 (dep 5)
/// matches no disjunct.
fn q88_household() -> RecordBatch {
    batch(
        vec![
            i64f("hd_demo_sk"),
            i64f("hd_dep_count"),
            i64f("hd_vehicle_count"),
        ],
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![2, 5])),
            Arc::new(Int64Array::from(vec![3, 1])),
        ],
    )
}

/// Replicated `store`: only store 1 is named 'ese'.
fn q88_store() -> RecordBatch {
    batch(
        vec![i64f("s_store_sk"), utf8f("s_store_name")],
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["ese", "other"])),
        ],
    )
}

/// Q88 fact rows `(time_sk, hdemo_sk, store_sk)`.
fn q88_sales(rows: &[(i64, i64, i64)]) -> RecordBatch {
    batch(
        vec![
            i64f("ss_sold_time_sk"),
            i64f("ss_hdemo_sk"),
            i64f("ss_store_sk"),
        ],
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
}

/// Bucket-1 rows span both shards (so single-shard counting is visibly wrong), one row lands
/// in no bucket (hour 12), one fails the household disjunct, one fails the store name.
fn q88_shard0() -> RecordBatch {
    q88_sales(&[(1, 1, 1), (2, 1, 1), (3, 1, 1), (4, 1, 1)])
}
fn q88_shard1() -> RecordBatch {
    q88_sales(&[(1, 1, 1), (2, 1, 1), (1, 2, 1), (2, 1, 2)])
}

/// Planner/ground-truth engine holding the full Q88 dataset.
fn q88_engine() -> Engine {
    let e = Engine::new();
    e.register_batches("time_dim", vec![q88_time_dim()])
        .unwrap();
    e.register_batches("household_demographics", vec![q88_household()])
        .unwrap();
    e.register_batches("store", vec![q88_store()]).unwrap();
    e.register_batches("store_sales", vec![q88_shard0(), q88_shard1()])
        .unwrap();
    e
}

/// `store_sales` sharded row-wise over two workers; the Q88 dimensions fully replicated.
async fn q88_two_workers() -> Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        e.register_batches("time_dim", vec![q88_time_dim()])
            .unwrap();
        e.register_batches("household_demographics", vec![q88_household()])
            .unwrap();
        e.register_batches("store", vec![q88_store()]).unwrap();
        let shard = if i == 0 { q88_shard0() } else { q88_shard1() };
        e.register_batches("store_sales", vec![shard]).unwrap();
        tokio::spawn(async move {
            let _ = serve_worker(port, e).await;
        });
    }
    Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ])
}

const Q88_REPLICATED: [&str; 3] = ["household_demographics", "time_dim", "store"];

/// Q88's shape, minimized to three half-hour buckets: three `count(*)` aggregates over the
/// same `store_sales ⋈ time_dim ⋈ household_demographics ⋈ store` star join, differing only in
/// their `time_dim` predicates. Expected row: (2, 2, 1).
const Q88_SHAPE: &str = "
SELECT *
FROM
  (SELECT count(*) h8_30_to_9
   FROM store_sales, household_demographics, time_dim, store
   WHERE ss_sold_time_sk = time_dim.t_time_sk
     AND ss_hdemo_sk = household_demographics.hd_demo_sk
     AND ss_store_sk = s_store_sk
     AND time_dim.t_hour = 8 AND time_dim.t_minute >= 30
     AND ((household_demographics.hd_dep_count = 2 AND household_demographics.hd_vehicle_count <= 2+2)
          OR (household_demographics.hd_dep_count = 0 AND household_demographics.hd_vehicle_count <= 0+2))
     AND store.s_store_name = 'ese') s1,
  (SELECT count(*) h9_to_9_30
   FROM store_sales, household_demographics, time_dim, store
   WHERE ss_sold_time_sk = time_dim.t_time_sk
     AND ss_hdemo_sk = household_demographics.hd_demo_sk
     AND ss_store_sk = s_store_sk
     AND time_dim.t_hour = 9 AND time_dim.t_minute < 30
     AND ((household_demographics.hd_dep_count = 2 AND household_demographics.hd_vehicle_count <= 2+2)
          OR (household_demographics.hd_dep_count = 0 AND household_demographics.hd_vehicle_count <= 0+2))
     AND store.s_store_name = 'ese') s2,
  (SELECT count(*) h9_30_to_10
   FROM store_sales, household_demographics, time_dim, store
   WHERE ss_sold_time_sk = time_dim.t_time_sk
     AND ss_hdemo_sk = household_demographics.hd_demo_sk
     AND ss_store_sk = s_store_sk
     AND time_dim.t_hour = 9 AND time_dim.t_minute >= 30
     AND ((household_demographics.hd_dep_count = 2 AND household_demographics.hd_vehicle_count <= 2+2)
          OR (household_demographics.hd_dep_count = 0 AND household_demographics.hd_vehicle_count <= 0+2))
     AND store.s_store_name = 'ese') s3";

/// Branches differing only in row predicates share ONE scan: a single leaf stage computes every
/// bucket's partial as `count(*) FILTER (WHERE <bucket predicate>)` over the shared star join
/// (Q88 had 8 branch sub-DAGs — 17 stages, 8 full fact scans — at SF10).
#[tokio::test]
async fn q88_shape_merges_time_bucket_branches_into_one_scan() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let planner = q88_engine();
    let lp = planner.logical_plan(Q88_SHAPE).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &Q88_REPLICATED).expect("Q88 shape should plan");

    assert_eq!(
        dq.stages.len(),
        3,
        "one shared leaf + one combine + the outer projection: {dq:?}"
    );
    let fact_leaves: Vec<_> = dq
        .stages
        .iter()
        .filter(|s| s.upstream_stage_ids.is_empty() && s.sql.contains("store_sales"))
        .collect();
    assert_eq!(
        fact_leaves.len(),
        1,
        "exactly one leaf stage scans the fact: {dq:?}"
    );
    let leaf = fact_leaves[0];
    assert!(
        leaf.sql.contains("FILTER (WHERE"),
        "the shared leaf gates each branch's partial to its own predicate: {}",
        leaf.sql
    );
    assert!(
        !leaf.sql.contains(" WHERE "),
        "branch predicates live in the FILTER clauses, not the shared tail: {}",
        leaf.sql
    );
    let outer = dq.stages.last().unwrap();
    assert_eq!(outer.upstream_stage_ids.len(), 3, "{outer:?}");
    assert!(
        outer
            .upstream_stage_ids
            .iter()
            .all(|id| *id == outer.upstream_stage_ids[0]),
        "every bucket placeholder pulls the shared combine output: {outer:?}"
    );
}

/// End-to-end: bucket-1 rows span both shards; the merged scan must equal single-node.
#[tokio::test]
async fn q88_shape_distributed_matches_single_node() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let planner = q88_engine();
    let expected = planner.sql(Q88_SHAPE).await.expect("single-node");
    assert!(
        expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
        "test data must produce a non-empty result"
    );
    let cluster = q88_two_workers().await;
    let (_, actual) = run_distributed_with(&cluster, &planner, Q88_SHAPE, &Q88_REPLICATED).await;
    assert_eq!(
        rows_sorted(&actual),
        rows_sorted(&expected),
        "distributed must equal single-node (one shared scan, FILTER-merged buckets)"
    );
}
