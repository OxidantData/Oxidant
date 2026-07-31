//! KAN-36/KAN-38: TPC-H Q13 / Q22 at the **auto-broadcast** configuration (the SF10
//! connect-server layout): `resolve_replicated_tables` replicates every base table smaller than
//! the query's largest, so Q13 and Q22 run with `customer` replicated and only `orders` sharded.
//! The KAN-33 shapes required *both* sharded (Q13's co-located LEFT JOIN) or the outer table
//! sharded (Q22's export scan), so both queries fell to the strict whole-fact-gather refusal.
//!
//! - **Q13**: replicated-preserved-side `customer LEFT JOIN orders` feeding KAN-26's
//!   agg-over-agg count distribution — the inner aggregation's count partials recombine across
//!   workers, absorbing the preserved side's per-worker repetition.
//! - **Q22**: the outer `customer` export scan runs exactly once (`ExchangeMode::Forward`),
//!   hash-shuffled by `c_custkey` to co-locate with the `orders` anti key stream. (KAN-55
//!   simplified the scalar half: the uncorrelated threshold over replicated `customer` is
//!   partition-independent, so it now evaluates verbatim in the scan's WHERE — no Forward
//!   scalar partial or driver literal injection.)
//!
//! Every distributed plan must equal single-node end-to-end, and none may fall back to the
//! whole-fact gather (KAN-29 floor).

// ENV_LOCK serializes process-global `WEFT_DISTRIBUTED_STRICT` across async tests.
#![allow(clippy::await_holding_lock)]

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};

use weft_execution::driver::{run_stages, Cluster, ExchangeMode};
use weft_execution::flight::serve_worker;
use weft_execution::plan::plan_distributed_logical;
use weft_loom::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use weft_loom::arrow::datatypes::{DataType, Field, Schema};
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::arrow::util::display::{ArrayFormatter, FormatOptions};
use weft_loom::Engine;

const Q13: &str = include_str!("../../../bench/tpch/queries/q13.sql");
const Q22: &str = include_str!("../../../bench/tpch/queries/q22.sql");

/// The auto-broadcast configuration for these two queries: `customer` (the smaller base table)
/// replicates, `orders` (the largest) shards.
const AUTO_BROADCAST_REPLICATED: [&str; 3] = ["customer", "nation", "region"];

/// Serialize port allocation across tests in this binary (same rationale as
/// `tests/auto_distribute.rs`: bind/drop races steal ports under parallel tests).
static PORT: std::sync::OnceLock<AtomicU16> = std::sync::OnceLock::new();

fn unique_worker_port() -> u16 {
    // OnceLock-seeded allocator with the base BELOW the Linux ephemeral source range
    // (32768..=60999): the harness's own outbound connections can never steal a worker's
    // port (serve_worker swallows EADDRINUSE; the old in-range bases flaked "did not
    // bind" / "distributed run never succeeded" on loaded CI runners).
    PORT.get_or_init(|| AtomicU16::new(14000 + (std::process::id() as u16 % 512)))
        .fetch_add(1, Ordering::Relaxed)
}

/// `WEFT_DISTRIBUTED_STRICT` is process-global; serialize the tests that touch it.
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

/// Simplified `orders` covering the columns Q13/Q22 reference. The per-customer order counts
/// need both shards: customer 1 and 2's orders land in different halves (Q13), and the NOT
/// EXISTS probe for customer 2 must consult both shards' key streams (Q22).
fn orders() -> RecordBatch {
    batch(
        vec![i64f("o_orderkey"), i64f("o_custkey"), strf("o_comment")],
        vec![
            i64v(&[1, 2, 3, 4, 5]),
            i64v(&[1, 1, 2, 2, 3]),
            strv(&[
                "ordinary",
                "special requests pending",
                "ordinary too",
                "special requests again",
                "ordinary",
            ]),
        ],
    )
}

fn customer() -> RecordBatch {
    batch(
        vec![
            i64f("c_custkey"),
            strf("c_name"),
            strf("c_phone"),
            f64f("c_acctbal"),
        ],
        vec![
            i64v(&[1, 2, 3, 4, 5]),
            strv(&[
                "Customer#1",
                "Customer#2",
                "Customer#3",
                "Customer#4",
                "Customer#5",
            ]),
            strv(&["13-111", "13-222", "23-333", "17-444", "99-555"]),
            f64v(&[100.0, 500.0, 50.0, 1000.0, 9000.0]),
        ],
    )
}

/// Planner/ground-truth engine holding the full dataset.
async fn tpch_engine() -> Engine {
    let e = Engine::new();
    e.register_batches("customer", vec![customer()]).unwrap();
    e.register_batches("orders", vec![orders()]).unwrap();
    e
}

/// Contiguous half of a table, so per-key values need both shards.
fn shard_rows(full: &RecordBatch, idx: usize) -> Vec<RecordBatch> {
    let half = full.num_rows() / 2;
    let (start, len) = if idx == 0 {
        (0, half)
    } else {
        (half, full.num_rows() - half)
    };
    vec![full.slice(start, len)]
}

/// `orders` sharded row-wise over two workers; `customer` fully replicated on both (the
/// auto-broadcast layout).
async fn two_workers_auto_broadcast() -> Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        e.register_batches("customer", vec![customer()]).unwrap();
        e.register_batches("orders", shard_rows(&orders(), i))
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

/// Plan `sql` at the auto-broadcast configuration and run the stages on `cluster`, applying the
/// driver's global finalize. Mirrors `tests/auto_distribute_semi_anti.rs::run_distributed`.
async fn run_distributed(
    cluster: &Cluster,
    planner: &Engine,
    sql: &str,
) -> (weft_execution::plan::DistributedQuery, Vec<RecordBatch>) {
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &AUTO_BROADCAST_REPLICATED)
        .expect("plan_distributed_logical");
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

async fn assert_distributed_matches_single_node(sql: &str) {
    let planner = tpch_engine().await;
    let expected = planner.sql(sql).await.expect("single-node");
    assert!(
        expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
        "test data must produce a non-empty result"
    );
    let cluster = two_workers_auto_broadcast().await;
    let (_, actual) = run_distributed(&cluster, &planner, sql).await;
    assert_eq!(
        rows_sorted(&actual),
        rows_sorted(&expected),
        "distributed must equal single-node"
    );
}

/// Strict mode must plan both queries distributed at the auto-broadcast configuration (no
/// KAN-29 whole-fact gather floor).
#[tokio::test]
async fn strict_mode_plans_q13_q22_without_gather() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("WEFT_DISTRIBUTED_STRICT", "1");
    for (name, sql) in [("Q13", Q13), ("Q22", Q22)] {
        let planner = tpch_engine().await;
        let lp = planner.logical_plan(sql).await.expect("logical plan");
        let dq = plan_distributed_logical(&lp, &AUTO_BROADCAST_REPLICATED)
            .unwrap_or_else(|e| panic!("{name} must plan in strict mode: {e}"));
        assert!(
            !dq.stages
                .iter()
                .any(|s| s.sql.contains("__weft_materialize_gate")
                    || s.sql.contains("__weft_subquery_gate")),
            "{name} must not fall back to the whole-fact gather: {dq:?}"
        );
    }
    std::env::remove_var("WEFT_DISTRIBUTED_STRICT");
}

// --- Q13: replicated-preserved-side LEFT JOIN + agg-over-agg (KAN-26 composition) ---

#[tokio::test]
async fn q13_replicated_preserved_side_plans_broadcast_left_join() {
    let planner = tpch_engine().await;
    let lp = planner.logical_plan(Q13).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &AUTO_BROADCAST_REPLICATED).expect("Q13 should plan");
    // Broadcast LEFT JOIN partial-agg (hashed by c_custkey) -> combine (re-shuffled by c_count)
    // -> exact outer count-distribution.
    assert_eq!(dq.stages.len(), 3, "{dq:?}");
    let partial = &dq.stages[0];
    assert_eq!(partial.hash_key_cols, vec![0], "hashed by c_custkey");
    assert!(
        partial
            .sql
            .contains("FROM customer LEFT OUTER JOIN orders ON"),
        "the LEFT JOIN stays local (customer is replicated): {}",
        partial.sql
    );
    assert!(
        partial
            .sql
            .contains("orders.o_comment NOT LIKE '%special%requests%'"),
        "the residual stays ON-folded so unmatched customers null-extend: {}",
        partial.sql
    );
    assert!(
        partial.sql.contains("count(orders.o_orderkey)"),
        "{}",
        partial.sql
    );
    let combine = &dq.stages[1];
    assert_eq!(
        combine.hash_key_cols,
        vec![1],
        "the per-customer counts re-shuffle by c_count for the exact outer aggregate"
    );
    let outer = &dq.stages[2];
    assert!(
        outer
            .sql
            .contains("count(1) AS r0 FROM shuffle_input GROUP BY c_count"),
        "the outer count-distribution runs exactly over co-located c_count groups: {}",
        outer.sql
    );
}

#[tokio::test]
async fn q13_distributed_matches_single_node() {
    assert_distributed_matches_single_node(Q13).await;
}

// --- Q22: replicated scalar conjunct verbatim + run-once replicated outer scan, co-located
// NOT EXISTS (KAN-55 simplified this from the KAN-36 scalar-broadcast plan: the avg body reads
// only replicated `customer`, so it is partition-independent and no driver literal injection is
// needed — 4 stages instead of 6, same provable semantics) ---

#[tokio::test]
async fn q22_replicated_outer_plans_forward_scalar_and_scan() {
    let planner = tpch_engine().await;
    let lp = planner.logical_plan(Q22).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &AUTO_BROADCAST_REPLICATED).expect("Q22 should plan");
    assert_eq!(
        dq.stages.len(),
        4,
        "anti producer -> forward outer scan -> anti+partial -> combine: {dq:?}"
    );
    let producer = &dq.stages[0];
    assert_eq!(producer.hash_key_cols, vec![0], "hashed by o_custkey");
    let scan = &dq.stages[1];
    assert_eq!(
        scan.exchange,
        ExchangeMode::Forward,
        "the replicated outer export scan must run exactly once: {dq:?}"
    );
    assert_eq!(scan.hash_key_cols, vec![0], "hashed by c_custkey");
    assert!(
        scan.sql
            .contains("customer.c_acctbal > (SELECT avg(customer.c_acctbal) FROM customer"),
        "the replicated scalar body evaluates verbatim wherever the outer row is read: {}",
        scan.sql
    );
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.sql.contains("__WEFT_SCALAR_STAGE__")),
        "no scalar broadcast stage is needed for a replicated scalar body: {dq:?}"
    );
    let anti = &dq.stages[2];
    assert_eq!(anti.upstream_stage_ids, vec![1, 0]);
    assert!(
        anti.sql
            .contains("NOT EXISTS (SELECT 1 FROM shuffle_input_1 AS k WHERE k.k0 = o.ok0)"),
        "{}",
        anti.sql
    );
    assert!(
        anti.sql.contains("GROUP BY substr(oc0, 1, 2)"),
        "the derived cntrycode expression resolves through the body projection: {}",
        anti.sql
    );
}

#[tokio::test]
async fn q22_distributed_matches_single_node() {
    assert_distributed_matches_single_node(Q22).await;
}
