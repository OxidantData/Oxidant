//! KAN-29: the whole-fact single-partition gather must not be the answer for TPC-H
//! Q4/Q15/Q17/Q18/Q21 (~27 GB wedged on one worker at SF10+).
//!
//! - **Floor**: in strict mode (`OXIDANT_DISTRIBUTED_STRICT=1`) the whole-fact gather is a fast
//!   `Error::Unsupported` naming the shape instead of an unbounded single-partition grind.
//!   Non-strict mode keeps the gather as the correctness-first fallback.
//! - **Q4 / Q18 / Q21**: correlated `EXISTS` / `NOT EXISTS` and uncorrelated `IN` predicates
//!   over a sharded fact plan as co-located semi/anti key shuffles (per-key producers hash-
//!   shuffled by the correlation key, an outer scan co-located on the same key when the outer
//!   body is itself sharded) feeding the ordinary two-stage aggregation.
//! - **Q17**: the correlated `avg` scalar with a NON-equality compare decorrelates into the
//!   KAN-22 per-key aggregate (sum/count partials), joined back with the compare as a residual,
//!   followed by a partial/combine pair for the outer global aggregate.
//! - **Q15**: the uncorrelated scalar `max` over the `revenue` CTE plans as a distributed
//!   derived table (per-key partial/combine) plus the KAN-27 one-row scalar broadcast (driver
//!   literal injection into the outer stage).
//!
//! Every distributed plan must equal single-node end-to-end.

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

const Q4: &str = include_str!("../../../bench/tpch/queries/q4.sql");
const Q15: &str = include_str!("../../../bench/tpch/queries/q15.sql");
const Q17: &str = include_str!("../../../bench/tpch/queries/q17.sql");
const Q18: &str = include_str!("../../../bench/tpch/queries/q18.sql");
const Q21: &str = include_str!("../../../bench/tpch/queries/q21.sql");

/// The bench's single-fact configuration: everything but `lineitem` replicated.
const BENCH_REPLICATED: [&str; 5] = ["orders", "customer", "part", "supplier", "nation"];
/// KAN-26's connect-server configuration: only the tiny dims replicated.
const MULTI_REPLICATED: [&str; 2] = ["nation", "region"];

/// Serialize port allocation across tests in this binary (same rationale as
/// `tests/auto_distribute.rs`: bind/drop races steal ports under parallel tests).
static PORT: std::sync::OnceLock<AtomicU16> = std::sync::OnceLock::new();

fn unique_worker_port() -> u16 {
    // OnceLock-seeded allocator with the base BELOW the Linux ephemeral source range
    // (32768..=60999): the harness's own outbound connections can never steal a worker's
    // port (serve_worker swallows EADDRINUSE; the old in-range bases flaked "did not
    // bind" / "distributed run never succeeded" on loaded CI runners).
    PORT.get_or_init(|| AtomicU16::new(25000 + (std::process::id() as u16 % 512)))
        .fetch_add(1, Ordering::Relaxed)
}

/// `OXIDANT_DISTRIBUTED_STRICT` is process-global; serialize the tests that touch the gather path.
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

/// Simplified TPC-H: only the columns Q4/Q15/Q17/Q18/Q21 reference. The rows are chosen so the
/// interesting edges are exercised: per-key values that need both shards (Q17's avg, Q18's
/// 150+160>300 quantity sum), a semi match whose lineitem rows span shards (Q4), a Q21 order
/// counted exactly once (late l1 with an on-time other-supplier row) next to one killed by the
/// anti (both suppliers late), and an order with no qualifying lineitem at all.
///
/// date32 days: 1993-08-01=8613 .. 1993-08-20=8632, 1993-12-01=8735, 1996-02-01=9527,
/// 1997-01-01=9862.
fn lineitem() -> RecordBatch {
    batch(
        vec![
            i64f("l_orderkey"),
            i64f("l_partkey"),
            i64f("l_suppkey"),
            f64f("l_quantity"),
            f64f("l_extendedprice"),
            f64f("l_discount"),
            datef("l_shipdate"),
            datef("l_commitdate"),
            datef("l_receiptdate"),
        ],
        vec![
            i64v(&[1, 1, 2, 2, 3, 4]),
            i64v(&[1, 2, 1, 2, 1, 3]),
            i64v(&[1, 2, 1, 2, 1, 1]),
            f64v(&[10.0, 20.0, 150.0, 160.0, 5.0, 7.0]),
            f64v(&[100.0, 200.0, 1500.0, 1600.0, 50.0, 70.0]),
            f64v(&[0.1, 0.0, 0.05, 0.1, 0.0, 0.0]),
            datev(&[9527, 9527, 9527, 9527, 9862, 9862]),
            datev(&[8613, 8613, 8613, 8613, 8613, 8615]),
            datev(&[8617, 8613, 8618, 8619, 8614, 8615]),
        ],
    )
}

fn orders() -> RecordBatch {
    batch(
        vec![
            i64f("o_orderkey"),
            i64f("o_custkey"),
            strf("o_orderstatus"),
            f64f("o_totalprice"),
            datef("o_orderdate"),
            strf("o_orderpriority"),
        ],
        vec![
            i64v(&[1, 2, 3, 4]),
            i64v(&[1, 1, 2, 2]),
            strv(&["F", "F", "O", "F"]),
            f64v(&[100.0, 500.0, 700.0, 900.0]),
            datev(&[8622, 8627, 8735, 8632]),
            strv(&["1-URGENT", "2-HIGH", "1-URGENT", "2-HIGH"]),
        ],
    )
}

fn customer() -> RecordBatch {
    batch(
        vec![i64f("c_custkey"), strf("c_name")],
        vec![i64v(&[1, 2]), strv(&["Customer#1", "Customer#2"])],
    )
}

fn part() -> RecordBatch {
    batch(
        vec![i64f("p_partkey"), strf("p_brand"), strf("p_container")],
        vec![
            i64v(&[1, 2, 3]),
            strv(&["Brand#23", "Brand#44", "Brand#23"]),
            strv(&["MED BOX", "SM BOX", "LG BOX"]),
        ],
    )
}

fn supplier() -> RecordBatch {
    batch(
        vec![
            i64f("s_suppkey"),
            strf("s_name"),
            strf("s_address"),
            i64f("s_nationkey"),
            strf("s_phone"),
        ],
        vec![
            i64v(&[1, 2, 3]),
            strv(&["Supplier#1", "Supplier#2", "Supplier#3"]),
            strv(&["addr1", "addr2", "addr3"]),
            i64v(&[1, 1, 2]),
            strv(&["ph1", "ph2", "ph3"]),
        ],
    )
}

fn nation() -> RecordBatch {
    batch(
        vec![i64f("n_nationkey"), strf("n_name")],
        vec![i64v(&[1, 2]), strv(&["SAUDI ARABIA", "CANADA"])],
    )
}

fn region() -> RecordBatch {
    batch(
        vec![i64f("r_regionkey"), strf("r_name")],
        vec![i64v(&[1]), strv(&["MIDDLE EAST"])],
    )
}

fn register(engine: &Engine, name: &str, batches: Vec<RecordBatch>) {
    engine.register_batches(name, batches).unwrap();
}

fn register_dims(engine: &Engine) {
    register(engine, "customer", vec![customer()]);
    register(engine, "part", vec![part()]);
    register(engine, "supplier", vec![supplier()]);
    register(engine, "nation", vec![nation()]);
    register(engine, "region", vec![region()]);
}

/// Planner/ground-truth engine holding the full dataset.
async fn tpch_engine() -> Engine {
    let e = Engine::new();
    register_dims(&e);
    register(&e, "orders", vec![orders()]);
    register(&e, "lineitem", vec![lineitem()]);
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

/// The bench configuration: `lineitem` sharded row-wise, every other table replicated.
async fn two_workers_sharded_lineitem() -> Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        register_dims(&e);
        register(&e, "orders", vec![orders()]);
        register(&e, "lineitem", shard_rows(&lineitem(), i));
        tokio::spawn(async move {
            let _ = serve_worker(port, e).await;
        });
    }
    Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ])
}

/// The multi-sharded configuration: `orders` and `lineitem` sharded, only tiny dims replicated.
async fn two_workers_sharded_orders_lineitem() -> Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        register_dims(&e);
        register(&e, "orders", shard_rows(&orders(), i));
        register(&e, "lineitem", shard_rows(&lineitem(), i));
        tokio::spawn(async move {
            let _ = serve_worker(port, e).await;
        });
    }
    Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ])
}

/// Plan `sql` with `replicated` and run the stages on `cluster`, applying the driver's global
/// finalize. Mirrors `tests/auto_distribute_decorrelate.rs::run_distributed`.
async fn run_distributed(
    cluster: &Cluster,
    planner: &Engine,
    sql: &str,
    replicated: &[&str],
) -> Vec<RecordBatch> {
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
    match &dq.finalize_sql {
        None => gathered,
        Some(fsql) => {
            let fin = Engine::new();
            fin.register_batches("result", gathered).unwrap();
            fin.sql(fsql).await.expect("finalize")
        }
    }
}

/// Sorted value rows, mirroring the bench's `normalize_batches` (headers are not compared:
/// single-node and distributed plans name unaliased aggregate outputs differently, which is
/// pre-existing behavior of every distributed aggregation shape).
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

async fn assert_distributed_matches_single_node(sql: &str, replicated: &[&str], multi: bool) {
    let planner = tpch_engine().await;
    let expected = planner.sql(sql).await.expect("single-node");
    assert!(
        expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
        "test data must produce a non-empty result"
    );
    let cluster = if multi {
        two_workers_sharded_orders_lineitem().await
    } else {
        two_workers_sharded_lineitem().await
    };
    let actual = run_distributed(&cluster, &planner, sql, replicated).await;
    assert_eq!(
        rows_sorted(&actual),
        rows_sorted(&expected),
        "distributed must equal single-node"
    );
}

// --- Q4: EXISTS (semi) ---

#[tokio::test]
async fn q4_exists_plans_semi_shuffle_replicated_outer() {
    let planner = tpch_engine().await;
    let lp = planner.logical_plan(Q4).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &BENCH_REPLICATED).expect("Q4 should plan");

    assert_eq!(
        dq.stages.len(),
        3,
        "key producer -> semi+partial -> combine: {dq:?}"
    );
    let producer = &dq.stages[0];
    assert_eq!(producer.hash_key_cols, vec![0], "hashed by l_orderkey");
    assert!(
        producer
            .sql
            .contains("SELECT lineitem.l_orderkey AS k0 FROM lineitem WHERE"),
        "{}",
        producer.sql
    );
    // The outer body (`orders`) is replicated here: no outer scan stage, the semi stage reads
    // it directly and each partition emits only rows whose key co-locates.
    let semi = &dq.stages[1];
    assert_eq!(semi.upstream_stage_ids, vec![0]);
    assert!(
        semi.sql
            .contains("EXISTS (SELECT 1 FROM shuffle_input AS k WHERE k.k0 = orders.o_orderkey)"),
        "{}",
        semi.sql
    );
    assert!(
        semi.sql.contains("GROUP BY orders.o_orderpriority"),
        "{}",
        semi.sql
    );
    let combine = &dq.stages[2];
    assert_eq!(combine.upstream_stage_ids, vec![1]);
    assert!(combine.sql.contains("sum(a0) AS r0"), "{}", combine.sql);
    assert!(
        dq.finalize_sql
            .as_deref()
            .is_some_and(|f| f.contains("ORDER BY")),
        "{:?}",
        dq.finalize_sql
    );
}

#[tokio::test]
async fn q4_exists_plans_semi_shuffle_multi_sharded() {
    // orders (outer) and lineitem (inner) both sharded: the outer scan is hash-shuffled by the
    // correlation key to co-locate with the key producer.
    let planner = tpch_engine().await;
    let lp = planner.logical_plan(Q4).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &MULTI_REPLICATED).expect("Q4 should plan");

    assert_eq!(
        dq.stages.len(),
        4,
        "key producer -> outer scan -> semi+partial -> combine: {dq:?}"
    );
    let scan = &dq.stages[1];
    assert_eq!(
        scan.hash_key_cols,
        vec![0],
        "outer rows shuffle by o_orderkey"
    );
    assert!(
        scan.sql.contains(
            "SELECT orders.o_orderkey AS ok0, orders.o_orderpriority AS oc0 FROM orders WHERE"
        ),
        "{}",
        scan.sql
    );
    let semi = &dq.stages[2];
    assert_eq!(semi.upstream_stage_ids, vec![1, 0]);
    assert!(
        semi.sql
            .contains("EXISTS (SELECT 1 FROM shuffle_input_1 AS k WHERE k.k0 = o.ok0)"),
        "{}",
        semi.sql
    );
}

#[tokio::test]
async fn q4_distributed_matches_single_node() {
    assert_distributed_matches_single_node(Q4, &BENCH_REPLICATED, false).await;
}

#[tokio::test]
async fn q4_multi_sharded_distributed_matches_single_node() {
    assert_distributed_matches_single_node(Q4, &MULTI_REPLICATED, true).await;
}

// --- Q17: correlated avg scalar with a NON-equality compare ---

#[tokio::test]
async fn q17_correlated_avg_non_equality_plans_residual_join() {
    let planner = tpch_engine().await;
    let lp = planner.logical_plan(Q17).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &BENCH_REPLICATED).expect("Q17 should plan");

    assert_eq!(
        dq.stages.len(),
        5,
        "per-key avg partial -> combine -> outer scan -> residual join partial -> \
         global combine: {dq:?}"
    );
    let partial = &dq.stages[0];
    assert_eq!(partial.hash_key_cols, vec![0], "hashed by l_partkey");
    assert!(
        partial
            .sql
            .contains("sum(lineitem.l_quantity) AS a0s, count(lineitem.l_quantity) AS a0c"),
        "avg decomposes into sum/count partials: {}",
        partial.sql
    );
    let combine = &dq.stages[1];
    assert!(
        combine
            .sql
            .contains("(sum(a0s) / NULLIF(sum(a0c), 0)) AS m0"),
        "{}",
        combine.sql
    );
    let scan = &dq.stages[2];
    assert_eq!(
        scan.hash_key_cols,
        vec![0],
        "outer rows shuffle by p_partkey"
    );
    let join = &dq.stages[3];
    assert_eq!(join.upstream_stage_ids, vec![1, 2]);
    assert!(
        join.sql
            .contains("ON m.k0 = o.ok0 AND o.cmp0 < (0.2 * m.m0)"),
        "non-equality compare stays as a residual on the co-located join: {}",
        join.sql
    );
    let outer = &dq.stages[4];
    assert!(
        outer.sql.contains("HAVING COUNT(*) > 0"),
        "empty partitions must not emit a synthetic global-aggregate row: {}",
        outer.sql
    );
    assert!(outer.sql.contains("sum(b0) AS r0"), "{}", outer.sql);
}

#[tokio::test]
async fn q17_distributed_matches_single_node() {
    assert_distributed_matches_single_node(Q17, &BENCH_REPLICATED, false).await;
}

// --- Q18: IN with a GROUP BY + HAVING subquery ---

#[tokio::test]
async fn q18_grouped_in_fuses_outer_aggregate() {
    // KAN-37: the subquery's per-key sum(l_quantity) IS the outer aggregate, so the fact never
    // joins the dims — the tiny per-key stream does. At SF10 this avoids shuffling the full
    // ~60M-row 3-way join output by o_orderkey (the 600s stage-timeout blowout).
    let planner = tpch_engine().await;
    let lp = planner.logical_plan(Q18).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &BENCH_REPLICATED).expect("Q18 should plan");

    assert_eq!(
        dq.stages.len(),
        3,
        "per-key partial -> HAVING combine carrying r0 -> co-located dim join + final agg: {dq:?}"
    );
    let producer = &dq.stages[0];
    assert_eq!(producer.hash_key_cols, vec![0], "hashed by l_orderkey");
    assert!(
        producer.sql.contains(
            "SELECT lineitem.l_orderkey AS k0, sum(lineitem.l_quantity) AS a0 FROM lineitem"
        ),
        "{}",
        producer.sql
    );
    let combine = &dq.stages[1];
    assert_eq!(combine.hash_key_cols, vec![0]);
    assert!(
        combine.sql.contains("SELECT k0, r0 FROM"),
        "the recombined per-key sum rides along with the key: {}",
        combine.sql
    );
    assert!(
        combine.sql.contains("WHERE ((r0 > 300))"),
        "the IN subquery's HAVING is re-applied over the recombined per-key sums: {}",
        combine.sql
    );
    let join = &dq.stages[2];
    assert_eq!(join.upstream_stage_ids, vec![1]);
    assert_eq!(join.hash_key_cols, Vec::<u32>::new(), "terminal gather");
    assert!(
        !join.sql.contains("FROM lineitem") && !join.sql.contains("JOIN lineitem"),
        "the fact never joins the dims: {}",
        join.sql
    );
    assert!(
        join.sql
            .contains("FROM shuffle_input AS s CROSS JOIN customer CROSS JOIN orders"),
        "{}",
        join.sql
    );
    assert!(
        join.sql.contains("(orders.o_orderkey = s.k0)"),
        "one row per key in the combine ⇒ the join is an exact semi: {}",
        join.sql
    );
    assert!(
        join.sql.contains("sum(s.r0) AS r0"),
        "the outer aggregate recombines the per-key sums: {}",
        join.sql
    );
    assert!(
        join.sql.contains("GROUP BY customer.c_name"),
        "{}",
        join.sql
    );
    assert!(
        dq.finalize_sql
            .as_deref()
            .is_some_and(|f| f.contains("LIMIT 100")),
        "{:?}",
        dq.finalize_sql
    );
}

#[tokio::test]
async fn q18_distributed_matches_single_node() {
    assert_distributed_matches_single_node(Q18, &BENCH_REPLICATED, false).await;
}

#[tokio::test]
async fn q18_shape_declines_when_outer_aggregate_differs() {
    // The fusion is only valid when the subquery's per-key aggregate is exactly the outer
    // aggregate; a different outer aggregate (count(*) here) must fall through to the generic
    // semi/anti path, which still plans Q18's shape as a 5-stage semi shuffle.
    let sql = Q18.replace("sum(l_quantity)\nFROM", "count(*)\nFROM");
    let planner = tpch_engine().await;
    let lp = planner.logical_plan(&sql).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &BENCH_REPLICATED).expect("should plan");
    assert_eq!(
        dq.stages.len(),
        5,
        "mismatched outer aggregate keeps the generic semi/anti plan: {dq:?}"
    );
    assert!(
        dq.stages[3]
            .sql
            .contains("o.ok0 IN (SELECT k0 FROM shuffle_input_1)"),
        "{}",
        dq.stages[3].sql
    );
}

#[tokio::test]
async fn q18_shape_declines_when_group_key_lacks_in_outer_key() {
    // Without the IN outer key in the GROUP BY, groups would span key partitions, so the fusion
    // must decline (the generic path then shuffles by the real group key).
    let sql = Q18.replace("    o_orderkey,\n    o_orderdate,", "    o_orderdate,");
    let planner = tpch_engine().await;
    let lp = planner.logical_plan(&sql).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &BENCH_REPLICATED).expect("should plan");
    assert!(
        !dq.stages.iter().any(|s| s.sql.contains("sum(s.r0)")),
        "no fused terminal join without the IN key in the GROUP BY: {dq:?}"
    );
}

// --- Q21: EXISTS + NOT EXISTS with a residual (non-equality) correlation ---

#[tokio::test]
async fn q21_exists_not_exists_plans_semi_anti_shuffle() {
    let planner = tpch_engine().await;
    let lp = planner.logical_plan(Q21).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &BENCH_REPLICATED).expect("Q21 should plan");

    assert_eq!(
        dq.stages.len(),
        5,
        "two key producers -> outer scan -> semi/anti+partial -> combine: {dq:?}"
    );
    let exists_producer = &dq.stages[0];
    assert!(
        exists_producer
            .sql
            .contains("l2.l_orderkey AS k0, l2.l_suppkey AS ic0"),
        "the residual's inner column is exported alongside the key: {}",
        exists_producer.sql
    );
    let anti_producer = &dq.stages[1];
    assert!(
        anti_producer
            .sql
            .contains("l3.l_receiptdate > l3.l_commitdate"),
        "inner-only predicates stay in the producer: {}",
        anti_producer.sql
    );
    let scan = &dq.stages[2];
    assert!(
        scan.sql.contains("l1.l_suppkey AS oe0"),
        "the residual's outer column is exported: {}",
        scan.sql
    );
    let semi = &dq.stages[3];
    assert_eq!(semi.upstream_stage_ids, vec![2, 0, 1]);
    assert!(
        semi.sql.contains(
            "EXISTS (SELECT 1 FROM shuffle_input_1 AS k WHERE k.k0 = o.ok0 AND ((k.ic0 <> o.oe0)))"
        ),
        "{}",
        semi.sql
    );
    assert!(
        semi.sql.contains(
            "NOT EXISTS (SELECT 1 FROM shuffle_input_2 AS k WHERE k.k0 = o.ok0 AND ((k.ic0 <> o.oe0)))"
        ),
        "{}",
        semi.sql
    );
}

#[tokio::test]
async fn q21_distributed_matches_single_node() {
    assert_distributed_matches_single_node(Q21, &BENCH_REPLICATED, false).await;
}

// --- Q15: uncorrelated scalar max over a derived per-key aggregate (CTE) ---

#[tokio::test]
async fn q15_derived_scalar_plans_one_row_broadcast() {
    let planner = tpch_engine().await;
    let lp = planner.logical_plan(Q15).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &BENCH_REPLICATED).expect("Q15 should plan");

    assert_eq!(
        dq.stages.len(),
        5,
        "revenue partial -> revenue combine -> scalar partial -> scalar combine -> outer: {dq:?}"
    );
    let revenue_combine = &dq.stages[1];
    assert_eq!(
        revenue_combine.hash_key_cols,
        vec![0],
        "co-located by supplier key"
    );
    assert!(
        revenue_combine
            .sql
            .contains("k0 AS \"supplier_no\", sum(a0) AS \"total_revenue\""),
        "the combine re-emits the derived table under its own column names: {}",
        revenue_combine.sql
    );
    let scalar_combine = &dq.stages[3];
    assert!(
        scalar_combine.sql.contains("max(s0) AS m0"),
        "{}",
        scalar_combine.sql
    );
    assert!(
        scalar_combine.sql.contains("HAVING COUNT(s0) > 0"),
        "an empty revenue table must read as a NULL scalar: {}",
        scalar_combine.sql
    );
    let outer = &dq.stages[4];
    assert_eq!(
        outer.upstream_stage_ids,
        vec![1],
        "the outer stage joins the co-located revenue combine"
    );
    assert!(
        outer
            .sql
            .contains("revenue.total_revenue = '__OXIDANT_SCALAR_STAGE__'"),
        "the driver inlines the global max before dispatch: {}",
        outer.sql
    );
    assert!(
        outer
            .sql
            .contains("FROM shuffle_input AS revenue CROSS JOIN supplier"),
        "{}",
        outer.sql
    );
}

#[tokio::test]
async fn q15_distributed_matches_single_node() {
    assert_distributed_matches_single_node(Q15, &BENCH_REPLICATED, false).await;
}

// --- Floor + off-shapes ---

/// A shape none of the distributed handlers cover: a scalar subquery with its own GROUP BY.
/// Non-strict mode keeps the whole-fact gather; strict mode must reject it fast, naming the
/// shape, instead of running an unbounded single-partition grind (KAN-29 floor).
const GATHER_SHAPE: &str = "SELECT t1.k, sum(t1.v) AS total FROM t t1 GROUP BY t1.k \
     HAVING sum(t1.v) > (SELECT max(t2.v) FROM t t2 GROUP BY t2.k)";

async fn gather_engine() -> Engine {
    let planner = Engine::new();
    planner
        .register_batches(
            "t",
            vec![batch(
                vec![i64f("k"), i64f("v")],
                vec![i64v(&[0, 0, 1]), i64v(&[10, 20, 30])],
            )],
        )
        .unwrap();
    planner
}

#[tokio::test]
async fn strict_mode_rejects_whole_fact_gather() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("OXIDANT_DISTRIBUTED_STRICT", "1");
    let planner = gather_engine().await;
    let lp = planner
        .logical_plan(GATHER_SHAPE)
        .await
        .expect("logical plan");
    let result = plan_distributed_logical(&lp, &[]);
    std::env::remove_var("OXIDANT_DISTRIBUTED_STRICT");
    let err = result.expect_err("strict mode must refuse the whole-fact gather");
    let msg = err.to_string();
    assert!(
        msg.contains("refusing whole-fact gather of sharded table `t` in strict mode"),
        "rejection must name the shape and the fact, got: {msg}"
    );
}

#[tokio::test]
async fn non_strict_keeps_the_gather_for_unhandled_shapes() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::remove_var("OXIDANT_DISTRIBUTED_STRICT");
    let planner = gather_engine().await;
    let lp = planner
        .logical_plan(GATHER_SHAPE)
        .await
        .expect("logical plan");
    let dq = plan_distributed_logical(&lp, &[]).expect("gather path should plan");
    assert!(
        dq.stages
            .iter()
            .any(|s| s.sql.contains("__oxidant_materialize_gate")),
        "non-strict mode keeps the correctness-first gather: {dq:?}"
    );
}

#[tokio::test]
async fn correlated_in_stays_on_the_gather_path() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::remove_var("OXIDANT_DISTRIBUTED_STRICT");
    // Correlated IN (the subquery references the outer row) is deliberately out of scope for
    // the semi/anti path — it must keep the old behavior.
    let sql = "SELECT t1.k, sum(t1.v) AS total FROM t t1 \
               WHERE t1.k IN (SELECT t2.k FROM t t2 WHERE t2.v <> t1.v) GROUP BY t1.k";
    let planner = gather_engine().await;
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &[]).expect("gather path should plan");
    assert!(
        dq.stages
            .iter()
            .any(|s| s.sql.contains("__oxidant_materialize_gate")),
        "correlated IN must stay on the whole-fact gather: {dq:?}"
    );
}
