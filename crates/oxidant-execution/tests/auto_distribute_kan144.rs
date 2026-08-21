//! KAN-144 (decorrelation half, narrow exact subset): uncorrelated scalar
//! `SELECT DISTINCT <expr>` over **replicated** tables in WHERE / BETWEEN —
//! TPC-DS Q54's `d_month_seq BETWEEN (SELECT DISTINCT d_month_seq+1 …) AND
//! (SELECT DISTINCT d_month_seq+3 …)`.
//!
//! Exactness: a SQL scalar admits 0 or 1 row; multi-row is an error. The planner
//! emits one Forward stage computing each bound as `GROUP BY <expr>` (exact
//! DISTINCT) and cross-joining the arms into a single multi-column row; the
//! driver injects the literals before dispatch. Empty → NULL bounds; multi-row
//! → hard error at pull (same as single-node scalar cardinality).
//!
//! Classical per-key decorrelation does **not** apply to Q54/Q82's SQL (Q82 has
//! no subqueries; Q54's remaining subqueries are uncorrelated Distinct scalars).
//! See `scratchpad/KAN-144-DECORRELATION.md`. Decline pins below lock shapes we
//! deliberately do not claim.

#![allow(clippy::await_holding_lock)]

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};

use oxidant_execution::driver::{run_stages, Cluster};
use oxidant_execution::flight::serve_worker;
use oxidant_execution::plan::plan_distributed_logical;
use oxidant_loom::arrow::array::{ArrayRef, Float64Array, Int64Array};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::arrow::util::display::{ArrayFormatter, FormatOptions};
use oxidant_loom::Engine;

static PORT: std::sync::OnceLock<AtomicU16> = std::sync::OnceLock::new();
static ENV_LOCK: Mutex<()> = Mutex::new(());

fn unique_worker_port() -> u16 {
    PORT.get_or_init(|| AtomicU16::new(27000 + (std::process::id() as u16 % 512)))
        .fetch_add(1, Ordering::Relaxed)
}

fn i64f(name: &str) -> Field {
    Field::new(name, DataType::Int64, false)
}
fn f64f(name: &str) -> Field {
    Field::new(name, DataType::Float64, false)
}
fn i64v(vals: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(vals.to_vec()))
}
fn f64v(vals: &[f64]) -> ArrayRef {
    Arc::new(Float64Array::from(vals.to_vec()))
}
fn batch(fields: Vec<Field>, cols: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
}

/// Replicated `date_dim`: Dec 1998 shares `d_month_seq = 100` across two days
/// (DISTINCT collapses to one row); Jan–Mar 1999 are 101/102/103.
fn date_dim() -> RecordBatch {
    batch(
        vec![
            i64f("d_date_sk"),
            i64f("d_month_seq"),
            i64f("d_year"),
            i64f("d_moy"),
        ],
        vec![
            i64v(&[1, 2, 3, 4, 5, 6]),
            i64v(&[100, 100, 101, 102, 103, 99]),
            i64v(&[1998, 1998, 1999, 1999, 1999, 1998]),
            i64v(&[12, 12, 1, 2, 3, 11]),
        ],
    )
}

/// Sharded `store_sales`: customer 1 has sales in month_seq 101 and 102 (inside
/// the BETWEEN window [101, 103]); customer 2 only in 99 (outside).
fn store_sales_shard(i: usize) -> RecordBatch {
    // w0: cust1 @ d_date_sk=3 (101); w1: cust1 @ d_date_sk=4 (102) + cust2 @ d_date_sk=6 (99)
    let rows = if i == 0 {
        (vec![3], vec![1], vec![10.0])
    } else {
        (vec![4, 6], vec![1, 2], vec![20.0, 99.0])
    };
    batch(
        vec![
            i64f("ss_sold_date_sk"),
            i64f("ss_customer_sk"),
            f64f("ss_ext_sales_price"),
        ],
        vec![i64v(&rows.0), i64v(&rows.1), f64v(&rows.2)],
    )
}

fn store_sales_full() -> RecordBatch {
    batch(
        vec![
            i64f("ss_sold_date_sk"),
            i64f("ss_customer_sk"),
            f64f("ss_ext_sales_price"),
        ],
        vec![
            i64v(&[3, 4, 6]),
            i64v(&[1, 1, 2]),
            f64v(&[10.0, 20.0, 99.0]),
        ],
    )
}

/// Q54's BETWEEN Distinct-scalar shape, minimized: one sharded fact ⋈ replicated
/// date_dim filtered by two uncorrelated `SELECT DISTINCT` bounds over date_dim.
const Q54_BETWEEN: &str = "
SELECT ss_customer_sk, sum(ss_ext_sales_price) AS revenue
FROM store_sales, date_dim
WHERE ss_sold_date_sk = d_date_sk
  AND d_month_seq BETWEEN
    (SELECT DISTINCT d_month_seq + 1 FROM date_dim WHERE d_year = 1998 AND d_moy = 12)
    AND
    (SELECT DISTINCT d_month_seq + 3 FROM date_dim WHERE d_year = 1998 AND d_moy = 12)
GROUP BY ss_customer_sk
ORDER BY ss_customer_sk
";

const REPL: [&str; 1] = ["date_dim"];

fn planner() -> Engine {
    let e = Engine::new();
    e.register_batches("date_dim", vec![date_dim()]).unwrap();
    e.register_batches("store_sales", vec![store_sales_full()])
        .unwrap();
    e
}

async fn two_workers() -> Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        e.register_batches("date_dim", vec![date_dim()]).unwrap();
        e.register_batches("store_sales", vec![store_sales_shard(i)])
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

async fn run_distributed(cluster: &Cluster, planner: &Engine, sql: &str) -> Vec<RecordBatch> {
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &REPL).expect("plan");
    let mut out = None;
    for _ in 0..150 {
        match run_stages(cluster, &dq.stages).await {
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
    match &dq.finalize_sql {
        None => gathered,
        Some(fsql) => {
            let fin = Engine::new();
            fin.register_batches("result", gathered).unwrap();
            fin.sql(fsql).await.expect("finalize")
        }
    }
}

#[tokio::test]
async fn q54_between_distinct_scalars_plan_with_forward_injection() {
    let _g = ENV_LOCK.lock().unwrap();
    std::env::set_var("OXIDANT_DISTRIBUTED_STRICT", "1");
    let planner = planner();
    let lp = planner
        .logical_plan(Q54_BETWEEN)
        .await
        .expect("logical plan");
    let dq =
        plan_distributed_logical(&lp, &REPL).expect("Q54 BETWEEN Distinct scalars should plan");
    assert_eq!(
        dq.stages[0].exchange,
        oxidant_execution::driver::ExchangeMode::Forward,
        "scalar DISTINCT stage must be Forward (replicated body)"
    );
    assert!(
        dq.stages[0].sql.contains("GROUP BY"),
        "stage 0 must be the Forward DISTINCT/GROUP BY scalar: {}",
        dq.stages[0].sql
    );
    let joined = dq
        .stages
        .iter()
        .map(|s| s.sql.as_str())
        .collect::<Vec<_>>()
        .join("\n---\n");
    assert!(
        joined.contains("__OXIDANT_SCALAR_STAGE_0__")
            && joined.contains("__OXIDANT_SCALAR_STAGE_1__"),
        "both BETWEEN bounds must become indexed scalar tokens:\n{joined}"
    );
    std::env::remove_var("OXIDANT_DISTRIBUTED_STRICT");
}

#[tokio::test]
async fn q54_between_distinct_scalars_distributed_matches_single_node() {
    let _g = ENV_LOCK.lock().unwrap();
    std::env::set_var("OXIDANT_DISTRIBUTED_STRICT", "1");
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    let planner = planner();
    let expected = planner.sql(Q54_BETWEEN).await.expect("single-node");
    let cluster = two_workers().await;
    let actual = run_distributed(&cluster, &planner, Q54_BETWEEN).await;
    assert_eq!(
        rows_sorted(&actual),
        rows_sorted(&expected),
        "distributed Q54 BETWEEN Distinct scalars must equal single-node\nactual: {:?}\nexpected: {:?}",
        rows_sorted(&actual),
        rows_sorted(&expected)
    );
    // Only customer 1's 10+20=30 falls in [101,103]; customer 2's 99 is outside.
    assert!(
        rows_sorted(&expected)
            .iter()
            .any(|r| r.first().map(String::as_str) == Some("1")),
        "sanity: expected includes customer 1: {:?}",
        rows_sorted(&expected)
    );
    std::env::remove_var("OXIDANT_DISTRIBUTED_STRICT");
    std::env::remove_var("OXIDANT_SHUFFLE_PARTITIONS");
}

/// Correlated Distinct scalar is not this path (and not classical per-key agg
/// decorrelation either) — must decline rather than silently mis-plan.
#[tokio::test]
async fn correlated_distinct_scalar_is_declined() {
    let _g = ENV_LOCK.lock().unwrap();
    std::env::set_var("OXIDANT_DISTRIBUTED_STRICT", "1");
    let e = Engine::new();
    e.register_batches(
        "t",
        vec![batch(
            vec![i64f("k"), i64f("v")],
            vec![i64v(&[1, 2]), i64v(&[10, 20])],
        )],
    )
    .unwrap();
    e.register_batches(
        "u",
        vec![batch(
            vec![i64f("k"), i64f("v")],
            vec![i64v(&[1, 1]), i64v(&[10, 11])],
        )],
    )
    .unwrap();
    let sql = "SELECT t.k, t.v FROM t WHERE t.v = (SELECT DISTINCT u.v FROM u WHERE u.k = t.k)";
    let lp = e.logical_plan(sql).await.expect("logical plan");
    // date_dim-style: u is "sharded" here (not in replicated), so the Distinct scalar path
    // must not claim it; correlated outer refs also force a decline from that path.
    let err = plan_distributed_logical(&lp, &[]).expect_err("correlated Distinct must not plan");
    let msg = err.to_string();
    assert!(
        msg.contains("unsupported") || msg.contains("Unsupported"),
        "expected explicit decline, got: {msg}"
    );
    std::env::remove_var("OXIDANT_DISTRIBUTED_STRICT");
}

/// Distinct scalar over a sharded fact is not broadcast-safe as a Forward one-shot —
/// leave it for other paths / gather rather than claiming exactness.
#[tokio::test]
async fn distinct_scalar_over_sharded_fact_not_claimed() {
    let e = Engine::new();
    e.register_batches(
        "fact",
        vec![batch(
            vec![i64f("k"), i64f("v")],
            vec![i64v(&[1, 1, 2]), i64v(&[10, 10, 20])],
        )],
    )
    .unwrap();
    e.register_batches("dim", vec![batch(vec![i64f("k")], vec![i64v(&[1, 2])])])
        .unwrap();
    let sql = "
SELECT dim.k FROM dim
WHERE dim.k = (SELECT DISTINCT fact.k FROM fact WHERE fact.v = 10)
";
    let lp = e.logical_plan(sql).await.expect("logical plan");
    // Only `dim` replicates; `fact` shards. The Distinct-scalar path must return None and
    // leave planning to other shapes (which may succeed or decline — either is fine so long
    // as we did not inject a Forward DISTINCT over the shard).
    if let Ok(dq) = plan_distributed_logical(&lp, &["dim"]) {
        let joined = dq
            .stages
            .iter()
            .map(|s| s.sql.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            !joined.contains("__OXIDANT_SCALAR_STAGE_0__")
                || !dq.stages[0].sql.contains("GROUP BY")
                || !dq.stages[0].sql.contains("fact"),
            "must not Forward-DISTINCT a sharded fact as a scalar stage:\n{joined}"
        );
    }
}
