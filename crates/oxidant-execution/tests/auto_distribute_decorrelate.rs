//! KAN-22: correlated scalar subqueries over the sharded fact (TPC-H Q2's
//! `ps_supplycost = (SELECT min(ps_supplycost) … WHERE p_partkey = ps_partkey …)`).
//!
//! - With exactly one sharded fact the planner decorrelates the equality-correlated
//!   min/max/sum/count subquery into a distributed per-key aggregation hash-joined against the
//!   outer scan (4 stages), and the distributed result must equal single-node.
//! - With more than one sharded table (misconfigured replication, e.g. SF10 with unknown table
//!   sizes) the shape must stay a clean `Error::Unsupported` naming the unsupported shape — the
//!   strict-mode floor — never a silent driver-local collect.
//!
//! KAN-26: Q12/Q13-class multi-sharded join chains (2+ sharded fact-ish tables, only tiny dims
//! replicated — the connect-server replication config):
//!
//! - Q12's comma-join (`FROM orders, lineitem WHERE o_orderkey = l_orderkey AND …`) plans as a
//!   two-table shuffle equijoin, with the single-table predicates pushed into the scan stage.
//! - Q13's aggregation over a pre-aggregated derived table plans as a sharded LEFT JOIN chain
//!   (residual `o_comment NOT LIKE …` kept in the ON clause) whose combine output is
//!   re-shuffled by the outer group key into an exact single-stage outer aggregate.
//! - Both must return the single-node result end-to-end.
//!
//! KAN-27: Q11-class uncorrelated scalar thresholds (`HAVING sum(…) > (SELECT sum(…) * frac
//! FROM partsupp, …)`) plan as a one-row broadcast — scalar partial/combine stages whose single
//! row the driver inlines into the outer stages' HAVING before dispatch — and must also return
//! the single-node result end-to-end. Off-shape variants (GROUP BY in the scalar subquery,
//! correlation) stay on the whole-fact gather path.

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use oxidant_execution::driver::{run_stages, Cluster};
use oxidant_execution::flight::serve_worker;
use oxidant_execution::plan::plan_distributed_logical;
use oxidant_loom::arrow::array::{
    ArrayRef, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray,
};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::util::pretty::pretty_format_batches;
use oxidant_loom::Engine;

const Q2: &str = include_str!("../../../bench/tpch/queries/q2.sql");
const Q11: &str = include_str!("../../../bench/tpch/queries/q11.sql");
const Q12: &str = include_str!("../../../bench/tpch/queries/q12.sql");
const Q13: &str = include_str!("../../../bench/tpch/queries/q13.sql");

/// Q11 with the `__OXIDANT_SF__` placeholder (KAN-30) resolved — SF=1 for the general tests.
fn q11(sf: &str) -> String {
    Q11.replace("__OXIDANT_SF__", sf)
}

const REPLICATED_DIMS: [&str; 4] = ["part", "supplier", "nation", "region"];

/// Serialize port allocation across tests in this binary (same rationale as
/// `tests/auto_distribute.rs`: bind/drop races steal ports under parallel tests).
static PORT: std::sync::OnceLock<AtomicU16> = std::sync::OnceLock::new();

fn unique_worker_port() -> u16 {
    // OnceLock-seeded allocator with the base BELOW the Linux ephemeral source range
    // (32768..=60999): the harness's own outbound connections can never steal a worker's
    // port (serve_worker swallows EADDRINUSE; the old in-range bases flaked "did not
    // bind" / "distributed run never succeeded" on loaded CI runners).
    PORT.get_or_init(|| AtomicU16::new(12000 + (std::process::id() as u16 % 512)))
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

fn region() -> RecordBatch {
    batch(
        vec![i64f("r_regionkey"), strf("r_name")],
        vec![i64v(&[1, 2]), strv(&["EUROPE", "AMERICA"])],
    )
}

fn nation() -> RecordBatch {
    batch(
        vec![i64f("n_nationkey"), strf("n_name"), i64f("n_regionkey")],
        vec![
            i64v(&[1, 2, 3]),
            strv(&["GERMANY", "FRANCE", "USA"]),
            i64v(&[1, 1, 2]),
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
            f64f("s_acctbal"),
            strf("s_comment"),
        ],
        vec![
            i64v(&[1, 2, 3, 4]),
            strv(&["s1", "s2", "s3", "s4"]),
            strv(&["a1", "a2", "a3", "a4"]),
            i64v(&[1, 2, 3, 1]),
            strv(&["p1", "p2", "p3", "p4"]),
            f64v(&[100.0, 200.0, 300.0, 50.0]),
            strv(&["c1", "c2", "c3", "c4"]),
        ],
    )
}

fn part() -> RecordBatch {
    batch(
        vec![
            i64f("p_partkey"),
            strf("p_mfgr"),
            strf("p_type"),
            i64f("p_size"),
        ],
        vec![
            i64v(&[1, 2, 3, 4]),
            strv(&["m1", "m2", "m3", "m4"]),
            strv(&["BRASS", "COPPER", "POLISHED BRASS", "BRASS"]),
            i64v(&[15, 15, 15, 10]),
        ],
    )
}

/// All eight partsupp rows; `partsupp_shard` splits them row-wise across workers.
fn partsupp() -> RecordBatch {
    batch(
        vec![
            i64f("ps_partkey"),
            i64f("ps_suppkey"),
            i64f("ps_availqty"),
            f64f("ps_supplycost"),
        ],
        vec![
            i64v(&[1, 1, 1, 2, 3, 3, 4, 1]),
            i64v(&[1, 2, 3, 1, 2, 4, 1, 4]),
            i64v(&[10; 8]),
            f64v(&[30.0, 25.0, 1.0, 40.0, 60.0, 55.0, 70.0, 28.0]),
        ],
    )
}

/// Contiguous half of the fact, so the per-partkey EUROPE min for `ps_partkey = 1` needs rows
/// from both shards (partial min 25.0 on one worker, 28.0 on the other).
fn partsupp_shard(idx: usize) -> Vec<RecordBatch> {
    let full = partsupp();
    let half = full.num_rows() / 2;
    let (start, len) = if idx == 0 {
        (0, half)
    } else {
        (half, full.num_rows() - half)
    };
    vec![full.slice(start, len)]
}

fn register(engine: &Engine, name: &str, batches: Vec<RecordBatch>) {
    engine.register_batches(name, batches).unwrap();
}

fn register_dims(engine: &Engine) {
    register(engine, "region", vec![region()]);
    register(engine, "nation", vec![nation()]);
    register(engine, "supplier", vec![supplier()]);
    register(engine, "part", vec![part()]);
}

/// orders/customer/lineitem for the KAN-26 Q12/Q13 tests, sized to exercise the interesting
/// edges: a `special requests` comment (Q13's ON-clause residual must exclude the order but keep
/// the customer at count 0), a customer with no orders at all (LEFT JOIN null-extension), and
/// lineitem rows that each fail one Q12 predicate (date window, commit<receipt, shipmode) plus
/// one with no matching order (inner-join drop).
fn orders() -> RecordBatch {
    batch(
        vec![
            i64f("o_orderkey"),
            i64f("o_custkey"),
            strf("o_orderpriority"),
            strf("o_comment"),
        ],
        vec![
            i64v(&[1, 2, 3, 4]),
            i64v(&[10, 20, 10, 30]),
            strv(&["1-URGENT", "3-MEDIUM", "2-HIGH", "4-NOT SPECIFIED"]),
            strv(&["ok", "special requests pending", "fine", "ok"]),
        ],
    )
}

fn customer() -> RecordBatch {
    batch(
        vec![i64f("c_custkey"), strf("c_name")],
        vec![i64v(&[10, 20, 30, 40]), strv(&["c10", "c20", "c30", "c40"])],
    )
}

/// date32 days: 1994-01-01 = 8766, 1995-01-01 = 9131 (Q12's receiptdate window).
fn lineitem() -> RecordBatch {
    batch(
        vec![
            i64f("l_orderkey"),
            strf("l_shipmode"),
            Field::new("l_commitdate", DataType::Date32, false),
            Field::new("l_receiptdate", DataType::Date32, false),
            Field::new("l_shipdate", DataType::Date32, false),
        ],
        vec![
            i64v(&[1, 2, 3, 3, 4, 5]),
            strv(&["MAIL", "SHIP", "SHIP", "MAIL", "TRUCK", "MAIL"]),
            Arc::new(Date32Array::from(vec![9000, 9000, 9000, 9100, 9000, 9000])),
            Arc::new(Date32Array::from(vec![9100, 9050, 9200, 9000, 9100, 9100])),
            Arc::new(Date32Array::from(vec![8900, 8990, 8900, 8900, 8900, 8900])),
        ],
    )
}

/// Contiguous half of a table, so join partners for the same key can land on different workers.
fn shard_rows(full: &RecordBatch, idx: usize) -> Vec<RecordBatch> {
    let half = full.num_rows() / 2;
    let (start, len) = if idx == 0 {
        (0, half)
    } else {
        (half, full.num_rows() - half)
    };
    vec![full.slice(start, len)]
}

/// Planner/ground-truth engine holding the full dataset.
async fn tpch_engine() -> Engine {
    let e = Engine::new();
    register_dims(&e);
    register(&e, "partsupp", vec![partsupp()]);
    register(&e, "orders", vec![orders()]);
    register(&e, "customer", vec![customer()]);
    register(&e, "lineitem", vec![lineitem()]);
    e
}

async fn two_workers_sharded_partsupp() -> Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        register_dims(&e);
        register(&e, "partsupp", partsupp_shard(i));
        tokio::spawn(async move {
            let _ = serve_worker(port, e).await;
        });
    }
    Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ])
}

/// KAN-26's connect-server replication config: orders/customer/lineitem sharded row-wise across
/// both workers, only the tiny dims replicated.
async fn two_workers_sharded_orders_customer_lineitem() -> Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        register_dims(&e);
        register(&e, "orders", shard_rows(&orders(), i));
        register(&e, "customer", shard_rows(&customer(), i));
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
/// finalize. Mirrors `tests/auto_distribute.rs::run_auto` but goes through
/// [`plan_distributed_logical`] directly so an unsupported shape fails the test instead of
/// silently Forward-falling back.
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

fn show(batches: &[RecordBatch]) -> String {
    pretty_format_batches(batches).unwrap().to_string()
}

#[tokio::test]
async fn q2_scalar_subquery_decorrelates_into_shuffle_stages() {
    let planner = tpch_engine().await;
    let lp = planner.logical_plan(Q2).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &REPLICATED_DIMS).expect("Q2 should plan");

    assert_eq!(
        dq.stages.len(),
        4,
        "partial agg -> combine -> outer scan -> join"
    );
    let partial = &dq.stages[0];
    assert_eq!(partial.hash_key_cols, vec![0]);
    assert!(partial.sql.contains("FROM partsupp"), "{}", partial.sql);
    assert!(
        partial.sql.to_uppercase().contains("GROUP BY"),
        "{}",
        partial.sql
    );
    assert!(
        partial.sql.contains("min(partsupp.ps_supplycost)"),
        "{}",
        partial.sql
    );

    let combine = &dq.stages[1];
    assert_eq!(combine.upstream_stage_ids, vec![0]);
    assert_eq!(
        combine.hash_key_cols,
        vec![0],
        "combine output must stay co-located with the outer scan"
    );
    assert!(combine.sql.contains("min(a0)"), "{}", combine.sql);

    let scan = &dq.stages[2];
    assert_eq!(
        scan.hash_key_cols,
        vec![0],
        "outer rows shuffle by the correlation key"
    );
    assert!(scan.sql.contains("FROM \"part\""), "{}", scan.sql);
    assert!(
        !scan.sql.to_uppercase().contains("SELECT MIN"),
        "subquery must be gone from the scan: {}",
        scan.sql
    );

    let join = &dq.stages[3];
    assert_eq!(join.upstream_stage_ids, vec![1, 2]);
    assert!(
        join.sql
            .contains("FROM shuffle_input_0 AS m JOIN shuffle_input_1 AS o ON m.k0 = o.ok0"),
        "{}",
        join.sql
    );
    assert!(join.sql.contains("o.cmp0 = m.m0"), "{}", join.sql);

    let finalize = dq.finalize_sql.expect("ORDER BY / LIMIT finalize");
    assert!(finalize.contains("ORDER BY"), "{finalize}");
    assert!(finalize.contains("LIMIT 100"), "{finalize}");
}

#[tokio::test]
async fn q2_distributed_matches_single_node() {
    let planner = tpch_engine().await;
    let expected = planner.sql(Q2).await.expect("single-node Q2");
    assert!(
        expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
        "test data must produce a non-empty Q2 result"
    );

    let cluster = two_workers_sharded_partsupp().await;
    let actual = run_distributed(&cluster, &planner, Q2, &REPLICATED_DIMS).await;
    assert_eq!(
        show(&actual),
        show(&expected),
        "decorrelated distributed Q2 must equal single-node (ORDER BY makes the output ordered)"
    );
}

#[tokio::test]
async fn aliased_correlated_max_subquery_matches_single_node() {
    // Same decorrelation shape through table aliases on both sides.
    let sql = "SELECT t1.k, t1.v FROM t t1 \
               WHERE t1.v = (SELECT max(t2.v) FROM t t2 WHERE t2.k = t1.k) \
               ORDER BY t1.k";
    let planner = Engine::new();
    planner
        .register_batches(
            "t",
            vec![batch(
                vec![i64f("k"), i64f("v")],
                vec![i64v(&[0, 0, 1, 1, 2, 2]), i64v(&[10, 20, 30, 40, 50, 60])],
            )],
        )
        .unwrap();
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &[]).expect("aliased decorrelation should plan");
    assert_eq!(
        dq.stages.len(),
        4,
        "aliased shape should decorrelate: {dq:?}"
    );

    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    let full = planner.sql("SELECT * FROM t").await.unwrap().remove(0);
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        let half = full.num_rows() / 2;
        let (start, len) = if i == 0 {
            (0, half)
        } else {
            (half, full.num_rows() - half)
        };
        e.register_batches("t", vec![full.slice(start, len)])
            .unwrap();
        tokio::spawn(async move {
            let _ = serve_worker(port, e).await;
        });
    }
    let cluster = Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ]);
    let actual = run_distributed(&cluster, &planner, sql, &[]).await;
    let expected = planner.sql(sql).await.unwrap();
    assert_eq!(show(&actual), show(&expected));
}

#[tokio::test]
async fn q11_uncorrelated_threshold_plans_one_row_broadcast() {
    // KAN-27: Q11's scalar subquery is uncorrelated (a global threshold) — it plans as scalar
    // partial/combine stages whose single row the driver inlines into the outer stages' HAVING
    // (literal injection), replacing the old whole-fact gather.
    let planner = tpch_engine().await;
    let lp = planner.logical_plan(&q11("1")).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &["supplier", "nation"]).expect("Q11 should plan");

    assert_eq!(
        dq.stages.len(),
        4,
        "scalar partial -> scalar combine -> outer partial -> outer combine: {dq:?}"
    );

    let scalar_partial = &dq.stages[0];
    assert_eq!(scalar_partial.stage_id, 0);
    assert!(scalar_partial.upstream_stage_ids.is_empty());
    assert!(
        scalar_partial.hash_key_cols.is_empty(),
        "scalar partials gather: {scalar_partial:?}"
    );
    assert!(
        scalar_partial.sql.contains("FROM partsupp"),
        "{}",
        scalar_partial.sql
    );
    assert!(
        scalar_partial
            .sql
            .contains("sum((partsupp.ps_supplycost * partsupp.ps_availqty)) AS a0"),
        "{}",
        scalar_partial.sql
    );
    assert!(
        !scalar_partial.sql.to_uppercase().contains("GROUP BY"),
        "global scalar aggregate has no GROUP BY: {}",
        scalar_partial.sql
    );

    let scalar_combine = &dq.stages[1];
    assert_eq!(scalar_combine.upstream_stage_ids, vec![0]);
    assert!(
        scalar_combine.sql.contains("sum(a0) AS m0"),
        "{}",
        scalar_combine.sql
    );
    // The subquery's projection (`* (0.0001 / SF)`) is re-applied over the combined value.
    // Committed Q11 keeps parentheses around the fraction (postprocess rewrite).
    assert!(
        scalar_combine.sql.contains("m0 * (0.0001 / 1)")
            || scalar_combine.sql.contains("m0 * 0.0001"),
        "{}",
        scalar_combine.sql
    );
    assert!(
        scalar_combine.sql.contains("HAVING COUNT(a0) > 0"),
        "empty partitions must not emit a synthetic scalar row: {}",
        scalar_combine.sql
    );

    let outer_partial = &dq.stages[2];
    assert!(
        outer_partial.sql.to_uppercase().contains("GROUP BY"),
        "{}",
        outer_partial.sql
    );
    assert!(
        !outer_partial.sql.to_uppercase().contains("SELECT SUM"),
        "scalar subquery must be gone from the outer stages: {}",
        outer_partial.sql
    );

    let outer_combine = &dq.stages[3];
    assert_eq!(outer_combine.upstream_stage_ids, vec![2]);
    assert!(
        outer_combine
            .sql
            .contains("WHERE ((r0 > '__OXIDANT_SCALAR_STAGE__'))"),
        "HAVING threshold compares against the placeholder token: {}",
        outer_combine.sql
    );

    let finalize = dq.finalize_sql.expect("ORDER BY finalize");
    assert!(finalize.contains("`value` DESC"), "{finalize}");
}

#[tokio::test]
async fn q11_distributed_matches_single_node() {
    let planner = tpch_engine().await;
    let sql = q11("1");
    let expected = planner.sql(&sql).await.expect("single-node Q11");
    assert!(
        expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
        "test data must produce a non-empty Q11 result"
    );

    let cluster = two_workers_sharded_partsupp().await;
    let actual = run_distributed(&cluster, &planner, &sql, &REPLICATED_DIMS).await;
    assert_eq!(
        show(&actual),
        show(&expected),
        "distributed Q11 (one-row scalar broadcast) must equal single-node (ORDER BY orders the output)"
    );
}

#[tokio::test]
async fn q11_empty_result_returns_typed_zero_rows_distributed() {
    // KAN-28: when the HAVING passes nothing, the output stage collects zero rows. The
    // worker→driver→register path must still deliver a *typed* zero-row result — previously
    // the produce=false reply carried a zero-field placeholder schema that `unify_schema`
    // silently dropped, surfacing as "register `result`: no batches".
    let planner = tpch_engine().await;
    // A fraction larger than any group's share of the total empties the result.
    let sql = q11("0.00001");
    let expected = planner.sql(&sql).await.expect("single-node Q11");
    assert_eq!(
        expected.iter().map(RecordBatch::num_rows).sum::<usize>(),
        0,
        "test fraction must empty the Q11 result"
    );

    let cluster = two_workers_sharded_partsupp().await;
    let actual = run_distributed(&cluster, &planner, &sql, &REPLICATED_DIMS).await;
    assert_eq!(
        actual.iter().map(RecordBatch::num_rows).sum::<usize>(),
        0,
        "distributed result must also be zero rows"
    );
    assert!(
        actual.iter().all(|b| !b.schema().fields().is_empty()),
        "the zero-row result must still carry the output schema"
    );
    assert_eq!(show(&actual), show(&expected));
}

#[tokio::test]
async fn uncorrelated_threshold_with_min_and_subquery_on_left_matches_single_node() {
    // Same one-row-broadcast shape with `min` and the subquery on the left of the comparison.
    let sql = "SELECT t1.k, sum(t1.v) AS total FROM t t1 GROUP BY t1.k \
               HAVING (SELECT min(t2.v) FROM t t2 WHERE t2.v > 10) < sum(t1.v) ORDER BY t1.k";
    let planner = Engine::new();
    planner
        .register_batches(
            "t",
            vec![batch(
                vec![i64f("k"), i64f("v")],
                vec![i64v(&[0, 0, 1, 1, 2, 2]), i64v(&[10, 20, 30, 40, 50, 60])],
            )],
        )
        .unwrap();
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &[]).expect("min threshold should plan");
    assert_eq!(
        dq.stages.len(),
        4,
        "scalar partial -> scalar combine -> outer partial -> outer combine: {dq:?}"
    );
    assert!(
        dq.stages[0].sql.contains("min(t2.v) AS a0"),
        "{}",
        dq.stages[0].sql
    );
    assert!(
        dq.stages[3]
            .sql
            .contains("WHERE (('__OXIDANT_SCALAR_STAGE__' < r0))"),
        "comparison side order must be preserved: {}",
        dq.stages[3].sql
    );

    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    let full = planner.sql("SELECT * FROM t").await.unwrap().remove(0);
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        let half = full.num_rows() / 2;
        let (start, len) = if i == 0 {
            (0, half)
        } else {
            (half, full.num_rows() - half)
        };
        e.register_batches("t", vec![full.slice(start, len)])
            .unwrap();
        tokio::spawn(async move {
            let _ = serve_worker(port, e).await;
        });
    }
    let cluster = Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ]);
    let actual = run_distributed(&cluster, &planner, sql, &[]).await;
    let expected = planner.sql(sql).await.unwrap();
    assert_eq!(show(&actual), show(&expected));
}

#[tokio::test]
async fn grouped_scalar_subquery_stays_on_the_gather_path() {
    // Off-shape: the scalar subquery has a GROUP BY (not a single global value), so the
    // one-row broadcast must decline and the whole-fact gather must keep handling it.
    let sql = "SELECT t1.k, sum(t1.v) AS total FROM t t1 GROUP BY t1.k \
               HAVING sum(t1.v) > (SELECT max(t2.v) FROM t t2 GROUP BY t2.k)";
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
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &[]).expect("gather path should plan");
    assert!(
        dq.stages
            .iter()
            .any(|s| s.sql.contains("__oxidant_materialize_gate")),
        "GROUP BY scalar subquery must stay on the whole-fact gather: {dq:?}"
    );
}

#[tokio::test]
async fn correlated_having_scalar_stays_on_the_gather_path() {
    // Off-shape: the scalar subquery is correlated (references the outer group key) — the
    // one-row broadcast is uncorrelated-only, so this must stay on the gather path.
    let sql = "SELECT t1.k, sum(t1.v) AS total FROM t t1 GROUP BY t1.k \
               HAVING sum(t1.v) > (SELECT max(t2.v) FROM t t2 WHERE t2.k = t1.k)";
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
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &[]).expect("gather path should plan");
    assert!(
        dq.stages
            .iter()
            .any(|s| s.sql.contains("__oxidant_materialize_gate")),
        "correlated HAVING scalar must stay on the whole-fact gather: {dq:?}"
    );
}

#[tokio::test]
async fn non_equality_compare_decorrelates_with_residual() {
    // KAN-29: `v < (SELECT max(v) … WHERE t2.k = t1.k)` decorrelates like the equality case —
    // the per-key aggregate joins back on the correlation key and the non-equality compare
    // stays as a residual on the same co-located join (TPC-H Q17's shape).
    let sql = "SELECT t1.k, t1.v FROM t t1 \
               WHERE t1.v < (SELECT max(t2.v) FROM t t2 WHERE t2.k = t1.k)";
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
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &[]).expect("decorrelate path should plan");
    assert_eq!(
        dq.stages.len(),
        4,
        "partial -> combine -> scan -> join: {dq:?}"
    );
    let join = &dq.stages[3];
    assert!(
        join.sql.contains("ON m.k0 = o.ok0 AND o.cmp0 < m.m0"),
        "non-equality compare must be a residual on the co-located join: {}",
        join.sql
    );
}

// --- Floor: with more than one sharded table these shapes must reject cleanly (strict mode
// turns this Error::Unsupported into the query error; see oxidant-connect distributed.rs) ---

async fn plan_err(sql: &str, replicated: &[&str]) -> String {
    let planner = tpch_engine().await;
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    plan_distributed_logical(&lp, replicated)
        .expect_err("multi-sharded shape must be rejected")
        .to_string()
}

#[tokio::test]
async fn q2_multi_sharded_rejects_naming_the_shape() {
    let msg = plan_err(Q2, &["nation", "region"]).await;
    assert!(
        msg.contains("subquery over `partsupp` is only safe when that table is replicated"),
        "got: {msg}"
    );
}

#[tokio::test]
async fn q11_multi_sharded_rejects_naming_the_shape() {
    let msg = plan_err(&q11("1"), &["nation", "region"]).await;
    assert!(
        msg.contains("subquery over `partsupp` is only safe when that table is replicated"),
        "got: {msg}"
    );
}

// --- KAN-26: Q12/Q13-class shapes must *plan* under the multi-sharded config (2+ sharded
// fact-ish tables, only tiny dims replicated) and match single-node end-to-end ---

#[tokio::test]
async fn q12_comma_join_plans_as_shuffle_equijoin() {
    let planner = tpch_engine().await;
    let lp = planner.logical_plan(Q12).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &["nation", "region"]).expect("Q12 should plan");

    assert_eq!(
        dq.stages.len(),
        4,
        "two hash-shuffled scans + partial + combine: {dq:?}"
    );
    let (orders_scan, lineitem_scan) = (&dq.stages[0], &dq.stages[1]);
    assert_eq!(orders_scan.sql, "SELECT * FROM orders");
    assert_eq!(orders_scan.hash_key_cols, vec![0], "hashed by o_orderkey");
    assert_eq!(lineitem_scan.hash_key_cols, vec![0], "hashed by l_orderkey");
    // The comma-join's single-table predicates push into the sharded scan, pre-shuffle.
    assert!(
        lineitem_scan.sql.contains("FROM lineitem WHERE"),
        "{}",
        lineitem_scan.sql
    );
    assert!(
        lineitem_scan.sql.contains("l_shipmode IN"),
        "{}",
        lineitem_scan.sql
    );

    let partial = &dq.stages[2];
    assert!(
        partial.sql.contains(
            "JOIN shuffle_input_1 AS lineitem ON orders.o_orderkey = lineitem.l_orderkey"
        ),
        "comma-join equijoin must become the shuffle join key: {}",
        partial.sql
    );
    assert!(
        !partial.sql.to_uppercase().contains("CROSS JOIN"),
        "{}",
        partial.sql
    );
    let finalize = dq.finalize_sql.expect("ORDER BY finalize");
    assert!(finalize.contains("ORDER BY"), "{finalize}");
}

#[tokio::test]
async fn q12_distributed_matches_single_node() {
    let planner = tpch_engine().await;
    let expected = planner.sql(Q12).await.expect("single-node Q12");
    assert!(
        expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
        "test data must produce a non-empty Q12 result"
    );

    let cluster = two_workers_sharded_orders_customer_lineitem().await;
    let actual = run_distributed(&cluster, &planner, Q12, &["nation", "region"]).await;
    assert_eq!(
        show(&actual),
        show(&expected),
        "distributed Q12 (comma-join shuffle) must equal single-node (ORDER BY orders the output)"
    );
}

#[tokio::test]
async fn q13_aggregate_over_aggregate_plans_distributed() {
    let planner = tpch_engine().await;
    let lp = planner.logical_plan(Q13).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &["nation", "region"]).expect("Q13 should plan");

    assert_eq!(
        dq.stages.len(),
        5,
        "chain scans + LEFT JOIN partial + combine + outer aggregate: {dq:?}"
    );
    let partial = &dq.stages[2];
    assert!(partial.sql.contains("LEFT JOIN"), "{}", partial.sql);
    // The `o_comment NOT LIKE …` residual is part of the join condition: it must stay in the ON
    // clause so unmatched customers null-extend (count 0) instead of being filtered out.
    assert!(
        partial.sql.contains(
            "ON l.customer__c_custkey = r.orders__o_custkey AND (r.orders__o_comment NOT LIKE"
        ),
        "{}",
        partial.sql
    );
    assert!(
        !partial.sql.to_uppercase().contains("WHERE"),
        "residual must not become a post-join filter: {}",
        partial.sql
    );

    let combine = &dq.stages[3];
    assert_eq!(
        combine.hash_key_cols,
        vec![1],
        "combine output re-shuffled by the outer group key c_count: {combine:?}"
    );
    let outer = &dq.stages[4];
    assert!(outer.sql.contains("GROUP BY c_count"), "{}", outer.sql);
    let finalize = dq.finalize_sql.expect("ORDER BY finalize");
    assert!(finalize.contains("custdist DESC"), "{finalize}");
}

#[tokio::test]
async fn q13_distributed_matches_single_node() {
    let planner = tpch_engine().await;
    let expected = planner.sql(Q13).await.expect("single-node Q13");
    assert!(
        expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
        "test data must produce a non-empty Q13 result"
    );

    let cluster = two_workers_sharded_orders_customer_lineitem().await;
    let actual = run_distributed(&cluster, &planner, Q13, &["nation", "region"]).await;
    assert_eq!(
        show(&actual),
        show(&expected),
        "distributed Q13 (aggregate over aggregate) must equal single-node (ORDER BY orders the output)"
    );
}
