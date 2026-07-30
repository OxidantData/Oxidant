//! KAN-44: TPC-DS Q54's shape — a high-cardinality `GROUP BY` inside a CTE / derived table
//! (`my_revenue` groups revenue by customer) feeding an **outer** aggregate over the derived
//! output (`segments` → count per revenue band) — must combine the inner partial groups by the
//! inner group key before the outer aggregate ever sees them.
//!
//! At the auto-broadcast configuration (one sharded fact + replicated dims) the single-sharded
//! arm of `aggregation_stages_for` only diverted agg-over-agg inputs to the KAN-26 composition
//! for the KAN-36 null-extended outer-join shape; every other inner-aggregation input fell
//! through to the flat broadcast path, which splices the derived table into the partial stage's
//! FROM tail. The inner `GROUP BY c_customer_sk` then ran **per worker** over its local shard,
//! so a customer whose rows span two shards emitted two partial `(customer, revenue)` rows that
//! were never combined — the outer aggregate counted the customer twice, in two different
//! revenue bands (SF10 Q54: `(1308, 1, …)` + `(6493, 1, …)` instead of `(7801, 1, …)`).
//!
//! The fix diverts *every* single-sharded agg-over-agg input to the composition, which plans
//! the inner aggregation exactly (partial → shuffle by inner key → combine) and re-shuffles the
//! combined per-customer rows by the outer group key. These tests lock the shape end-to-end:
//! distributed must equal single-node, and shapes the composition cannot prove equivalent must
//! be declined instead of silently mis-planned.

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use weft_execution::driver::{run_stages, Cluster};
use weft_execution::flight::serve_worker;
use weft_execution::plan::plan_distributed_logical;
use weft_loom::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use weft_loom::arrow::datatypes::{DataType, Field, Schema};
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::arrow::util::display::{ArrayFormatter, FormatOptions};
use weft_loom::Engine;

/// Serialize port allocation across tests in this binary (same rationale as
/// `tests/auto_distribute.rs`: bind/drop races steal ports under parallel tests).
static PORT: AtomicU16 = AtomicU16::new(0);

fn unique_worker_port() -> u16 {
    let prev = PORT.fetch_add(1, Ordering::Relaxed);
    if prev == 0 {
        // Keep seed + port-count headroom under u16::MAX (47000 + 17999 + tests < 65535);
        // `% 20000` could overflow to a panic in debug builds for large PIDs.
        let seed = 47000 + (std::process::id() as u16 % 18000);
        PORT.store(seed.wrapping_add(1), Ordering::Relaxed);
        seed
    } else {
        prev
    }
}

fn i64f(name: &str) -> Field {
    Field::new(name, DataType::Int64, false)
}
fn f64f(name: &str) -> Field {
    Field::new(name, DataType::Float64, false)
}

fn batch(fields: Vec<Field>, cols: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
}

/// Replicated `customer` dimension: keys 1..=4.
fn customer() -> RecordBatch {
    batch(
        vec![
            i64f("c_custkey"),
            Field::new("c_name", DataType::Utf8, false),
        ],
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["c1", "c2", "c3", "c4"])),
        ],
    )
}

/// `sales` rows for one worker shard. Customer 1's and customer 4's rows deliberately span both
/// shards so their per-worker partial revenues land in *different* revenue bands:
/// - c1: shard0 400 (band 8) + shard1 600 (band 12) = 1000 → true band 20;
/// - c4: shard0 25 (band 1) + shard1 75 (band 2) = 100 → true band 2.
fn sales(rows: &[(i64, f64)]) -> RecordBatch {
    batch(
        vec![i64f("s_customer_sk"), f64f("s_price")],
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ],
    )
}

fn shard0() -> RecordBatch {
    sales(&[(1, 250.0), (1, 150.0), (2, 100.0), (4, 25.0)])
}

fn shard1() -> RecordBatch {
    sales(&[(1, 600.0), (3, 150.0), (4, 75.0)])
}

/// The Q54 shape, minimized: inner `GROUP BY` over sharded `sales` joined to replicated
/// `customer` inside a CTE, an expression projection over the derived output (`segments`), and
/// an outer aggregate grouping by that expression's column.
const Q54_SHAPE: &str = "
WITH my_revenue AS (
    SELECT c_custkey, sum(s_price) AS revenue
    FROM customer JOIN sales ON c_custkey = s_customer_sk
    GROUP BY c_custkey
),
segments AS (
    SELECT cast(round(revenue / 50) AS int) AS segment
    FROM my_revenue
)
SELECT segment, count(*) AS num_customers, segment * 50 AS segment_base
FROM segments
GROUP BY segment
ORDER BY segment";

/// Planner/ground-truth engine holding the full dataset.
fn full_engine() -> Engine {
    let e = Engine::new();
    e.register_batches("customer", vec![customer()]).unwrap();
    e.register_batches("sales", vec![shard0(), shard1()])
        .unwrap();
    e
}

/// `sales` sharded row-wise over two workers; `customer` fully replicated on both (the
/// auto-broadcast layout).
async fn two_workers() -> Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        e.register_batches("customer", vec![customer()]).unwrap();
        let shard = if i == 0 { shard0() } else { shard1() };
        e.register_batches("sales", vec![shard]).unwrap();
        tokio::spawn(async move {
            let _ = serve_worker(port, e).await;
        });
    }
    Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ])
}

/// Plan `sql` with only `sales` sharded and run the stages on `cluster`, applying the driver's
/// global finalize. Mirrors `tests/auto_distribute_kan36.rs::run_distributed`.
async fn run_distributed(
    cluster: &Cluster,
    planner: &Engine,
    sql: &str,
) -> (weft_execution::plan::DistributedQuery, Vec<RecordBatch>) {
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &["customer"]).expect("plan_distributed_logical");
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

/// The composition: inner partial (hashed by the customer key) → combine re-shuffled by the
/// outer band column → exact outer aggregate. No whole-fact gather gate.
#[tokio::test]
async fn q54_shape_plans_aggregate_over_aggregate() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let planner = full_engine();
    let lp = planner.logical_plan(Q54_SHAPE).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &["customer"]).expect("Q54 shape should plan");
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.sql.contains("__weft_materialize_gate")
                || s.sql.contains("__weft_subquery_gate")),
        "must not fall back to the whole-fact gather: {dq:?}"
    );
    assert_eq!(
        dq.stages.len(),
        3,
        "inner partial -> combine (re-shuffled by the outer band) -> exact outer aggregate: {dq:?}"
    );
    let partial = &dq.stages[0];
    assert_eq!(
        partial.hash_key_cols,
        vec![0],
        "the inner partial shuffles by the customer key so split customers combine: {dq:?}"
    );
    let outer = &dq.stages[2];
    assert!(
        outer.sql.contains("FROM shuffle_input GROUP BY segment"),
        "the outer aggregate runs exactly over co-located segment groups: {}",
        outer.sql
    );
}

/// End-to-end: a customer whose rows span both shards must be combined by the inner group key
/// before the outer band count — distributed must equal single-node exactly. Pre-fix the flat
/// path emitted one partial row per (customer, shard) and counted c1/c4 twice, in bands that do
/// not exist single-node.
#[tokio::test]
async fn q54_shape_distributed_matches_single_node() {
    std::env::set_var("WEFT_SHUFFLE_PARTITIONS", "16");
    let planner = full_engine();
    let expected = planner.sql(Q54_SHAPE).await.expect("single-node");
    assert!(
        expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
        "test data must produce a non-empty result"
    );
    let cluster = two_workers().await;
    let (_, actual) = run_distributed(&cluster, &planner, Q54_SHAPE).await;
    assert_eq!(
        rows_sorted(&actual),
        rows_sorted(&expected),
        "distributed must equal single-node (inner partial groups combined by customer key)"
    );
}

/// Only shapes the composition can prove equivalent may be accepted: an outer group key that is
/// an *expression* over the inner output (not a plain derived column) cannot serve as a shuffle
/// hash key, so the planner must decline rather than fall back to the per-worker partial shape
/// that silently double-counts.
#[tokio::test]
async fn q54_shape_expression_group_key_is_declined() {
    let sql = "
WITH rev AS (
    SELECT s_customer_sk, sum(s_price) AS revenue
    FROM sales
    GROUP BY s_customer_sk
)
SELECT revenue + 1 AS band, count(*) AS num_customers
FROM rev
GROUP BY revenue + 1";
    let planner = full_engine();
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let err = plan_distributed_logical(&lp, &["customer"])
        .expect_err("an expression outer group key must be declined, not mis-planned");
    assert!(
        err.to_string().contains("aggregate over aggregate"),
        "unexpected error: {err}"
    );
}
