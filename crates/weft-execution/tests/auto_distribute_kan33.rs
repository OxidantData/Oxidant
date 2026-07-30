//! KAN-33: the last four TPC-H strict rejects at the connect-server configuration (only
//! `nation` / `region` replicated; every fact-ish table sharded).
//!
//! - **Q13**: `customer LEFT JOIN orders` with both sharded (null-extension co-located shuffle
//!   join) feeding KAN-26's agg-over-agg count distribution.
//! - **Q16**: `part ⋈ partsupp` co-located shuffle equijoin plus an uncorrelated `NOT IN`
//!   anti-join over sharded `supplier`.
//! - **Q20**: `supplier ⋈ nation` with a nested uncorrelated `IN` whose body carries an
//!   uncorrelated `IN` over sharded `part` and an equality-correlated scalar over `lineitem`.
//! - **Q22**: `customer` with an uncorrelated scalar-agg threshold over `customer` itself plus
//!   a correlated `NOT EXISTS` over sharded `orders`.
//!
//! Every distributed plan must equal single-node end-to-end, and none may fall back to the
//! whole-fact gather (KAN-29 floor).

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

const Q13: &str = include_str!("../../../bench/tpch/queries/q13.sql");
const Q16: &str = include_str!("../../../bench/tpch/queries/q16.sql");
const Q20: &str = include_str!("../../../bench/tpch/queries/q20.sql");
const Q22: &str = include_str!("../../../bench/tpch/queries/q22.sql");

/// The connect-server configuration: only the tiny dims replicated.
const MULTI_REPLICATED: [&str; 2] = ["nation", "region"];

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

/// Simplified TPC-H covering the columns Q13/Q16/Q20/Q22 reference. Per-key values are spread
/// across both halves so a shard-local evaluation would miscount: Q13's per-customer order
/// counts, Q16's part→supplier fan-out, Q20's per-(part,supplier) quantity sums, Q22's
/// cross-shard average.
///
/// date32 days: 1994-01-01=8766, 1994-06-01=8917, 1995-01-01=9131, 1996-02-01=9527.
fn lineitem() -> RecordBatch {
    batch(
        vec![
            i64f("l_orderkey"),
            i64f("l_partkey"),
            i64f("l_suppkey"),
            f64f("l_quantity"),
            datef("l_shipdate"),
        ],
        vec![
            i64v(&[1, 4, 3, 2, 5, 6]),
            i64v(&[1, 2, 1, 1, 2, 3]),
            i64v(&[1, 3, 2, 1, 3, 4]),
            f64v(&[10.0, 4.0, 8.0, 30.0, 100.0, 5.0]),
            datev(&[8766, 8917, 8766, 8917, 8766, 9527]),
        ],
    )
}

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

fn part() -> RecordBatch {
    batch(
        vec![
            i64f("p_partkey"),
            strf("p_name"),
            strf("p_brand"),
            strf("p_type"),
            i64f("p_size"),
        ],
        vec![
            i64v(&[1, 2, 3, 4]),
            strv(&["forest green", "sky blue", "forest red", "ocean"]),
            strv(&["Brand#12", "Brand#45", "Brand#34", "Brand#12"]),
            strv(&[
                "SMALL BURNISHED",
                "MEDIUM POLISHED",
                "LARGE PLATED",
                "SMALL PLATED",
            ]),
            i64v(&[14, 49, 23, 7]),
        ],
    )
}

fn partsupp() -> RecordBatch {
    batch(
        vec![i64f("ps_partkey"), i64f("ps_suppkey"), i64f("ps_availqty")],
        vec![
            i64v(&[1, 1, 1, 2, 3, 4, 4]),
            i64v(&[1, 2, 3, 3, 1, 2, 4]),
            i64v(&[25, 3, 9, 8, 40, 60, 70]),
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
            strf("s_comment"),
        ],
        vec![
            i64v(&[1, 2, 3, 4]),
            strv(&["Supplier#1", "Supplier#2", "Supplier#3", "Supplier#4"]),
            strv(&["addr1", "addr2", "addr3", "addr4"]),
            i64v(&[1, 1, 2, 2]),
            strv(&["ok", "Customer Complaints", "ok", "fine"]),
        ],
    )
}

fn nation() -> RecordBatch {
    batch(
        vec![i64f("n_nationkey"), strf("n_name")],
        vec![i64v(&[1, 2]), strv(&["CANADA", "BRAZIL"])],
    )
}

fn region() -> RecordBatch {
    batch(
        vec![i64f("r_regionkey"), strf("r_name")],
        vec![i64v(&[1]), strv(&["AMERICA"])],
    )
}

fn register(engine: &Engine, name: &str, batches: Vec<RecordBatch>) {
    engine.register_batches(name, batches).unwrap();
}

/// Planner/ground-truth engine holding the full dataset.
async fn tpch_engine() -> Engine {
    let e = Engine::new();
    register(&e, "customer", vec![customer()]);
    register(&e, "orders", vec![orders()]);
    register(&e, "lineitem", vec![lineitem()]);
    register(&e, "part", vec![part()]);
    register(&e, "partsupp", vec![partsupp()]);
    register(&e, "supplier", vec![supplier()]);
    register(&e, "nation", vec![nation()]);
    register(&e, "region", vec![region()]);
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

/// Every fact-ish table sharded row-wise over two workers; only `nation` / `region` replicated
/// (the connect-server configuration).
async fn two_workers_multi_sharded() -> Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        register(&e, "customer", shard_rows(&customer(), i));
        register(&e, "orders", shard_rows(&orders(), i));
        register(&e, "lineitem", shard_rows(&lineitem(), i));
        register(&e, "part", shard_rows(&part(), i));
        register(&e, "partsupp", shard_rows(&partsupp(), i));
        register(&e, "supplier", shard_rows(&supplier(), i));
        register(&e, "nation", vec![nation()]);
        register(&e, "region", vec![region()]);
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
/// finalize. Mirrors `tests/auto_distribute_semi_anti.rs::run_distributed`.
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
    let cluster = two_workers_multi_sharded().await;
    let actual = run_distributed(&cluster, &planner, sql, &MULTI_REPLICATED).await;
    assert_eq!(
        rows_sorted(&actual),
        rows_sorted(&expected),
        "distributed must equal single-node"
    );
}

/// Plan `sql` at the connect-server configuration and return the DAG.
async fn plan_multi(sql: &str) -> (weft_execution::plan::DistributedQuery, Engine) {
    let planner = tpch_engine().await;
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let dq = plan_distributed_logical(&lp, &MULTI_REPLICATED).expect("should plan");
    (dq, planner)
}

/// Strict mode must plan all four queries distributed (no KAN-29 whole-fact gather floor).
#[tokio::test]
async fn strict_mode_plans_all_four_without_gather() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("WEFT_DISTRIBUTED_STRICT", "1");
    for (name, sql) in [("Q13", Q13), ("Q16", Q16), ("Q20", Q20), ("Q22", Q22)] {
        let planner = tpch_engine().await;
        let lp = planner.logical_plan(sql).await.expect("logical plan");
        let dq = plan_distributed_logical(&lp, &MULTI_REPLICATED)
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

// --- Q13: sharded LEFT JOIN + agg-over-agg (KAN-26 shape at the connect-server config) ---

#[tokio::test]
async fn q13_left_outer_shuffle_join_null_extends() {
    let (dq, _) = plan_multi(Q13).await;
    // customer leaf, orders leaf, LEFT JOIN partial-agg, combine, outer count-distribution.
    assert_eq!(dq.stages.len(), 5, "{dq:?}");
    let join = &dq.stages[2];
    assert_eq!(join.upstream_stage_ids, vec![0, 1]);
    assert!(
        join.sql.contains(
            "LEFT JOIN shuffle_input_1 AS r ON l.customer__c_custkey = r.orders__o_custkey"
        ),
        "co-located LEFT JOIN keeps the key equality in the ON clause: {}",
        join.sql
    );
    assert!(
        join.sql
            .contains("r.orders__o_comment NOT LIKE '%special%requests%'"),
        "the residual stays ON-folded so unmatched customers null-extend: {}",
        join.sql
    );
    // The outer count-distribution is a single exact stage over the re-shuffled per-customer
    // counts.
    let outer = &dq.stages[4];
    assert_eq!(outer.upstream_stage_ids, vec![3]);
    assert!(
        outer
            .sql
            .contains("count(1) AS r0 FROM shuffle_input GROUP BY c_count"),
        "{}",
        outer.sql
    );
}

#[tokio::test]
async fn q13_distributed_matches_single_node() {
    assert_distributed_matches_single_node(Q13).await;
}

// --- Q16: sharded–sharded equijoin body + uncorrelated NOT IN anti over sharded supplier ---

#[tokio::test]
async fn q16_not_in_plans_shuffle_join_anti() {
    let (dq, _) = plan_multi(Q16).await;
    assert_eq!(
        dq.stages.len(),
        6,
        "anti producer -> part/partsupp leaves -> co-located join -> anti+project -> \
         distinct combine: {dq:?}"
    );
    let producer = &dq.stages[0];
    assert_eq!(producer.hash_key_cols, vec![0], "hashed by s_suppkey");
    assert!(
        producer
            .sql
            .contains("SELECT supplier.s_suppkey AS k0 FROM supplier WHERE"),
        "{}",
        producer.sql
    );
    // partsupp and part leaves hash by the equijoin key; the part side keeps its filters.
    let part_leaf = &dq.stages[2];
    assert_eq!(part_leaf.hash_key_cols, vec![0], "hashed by p_partkey");
    assert!(
        part_leaf.sql.contains("`part`.p_brand <> 'Brand#45'"),
        "{}",
        part_leaf.sql
    );
    let join = &dq.stages[3];
    assert_eq!(join.upstream_stage_ids, vec![1, 2]);
    assert_eq!(
        join.hash_key_cols,
        vec![0],
        "the join output re-shuffles by the anti outer key (ps_suppkey)"
    );
    assert!(
        join.sql
            .contains("ON l.partsupp__ps_partkey = r.part__p_partkey"),
        "{}",
        join.sql
    );
    let anti = &dq.stages[4];
    assert_eq!(anti.upstream_stage_ids, vec![3, 0]);
    assert!(
        anti.sql
            .contains("o.ok0 NOT IN (SELECT k0 FROM shuffle_input_1)"),
        "NOT IN keeps its spelling (three-valued semantics): {}",
        anti.sql
    );
    let combine = &dq.stages[5];
    assert!(
        combine.sql.contains("count(DISTINCT c0) AS r0"),
        "count(DISTINCT) runs exactly over co-located groups: {}",
        combine.sql
    );
}

#[tokio::test]
async fn q16_distributed_matches_single_node() {
    assert_distributed_matches_single_node(Q16).await;
}

// --- Q20: nested IN + equality-correlated scalar semi cascade ---

#[tokio::test]
async fn q20_nested_in_plans_semi_cascade() {
    let (dq, _) = plan_multi(Q20).await;
    assert_eq!(dq.stages.len(), 8, "{dq:?}");
    let scalar_partial = &dq.stages[0];
    assert_eq!(
        scalar_partial.hash_key_cols,
        vec![0, 1],
        "hashed by (l_partkey, l_suppkey)"
    );
    assert!(
        scalar_partial
            .sql
            .contains("sum(lineitem.l_quantity) AS a0"),
        "{}",
        scalar_partial.sql
    );
    let scalar_combine = &dq.stages[1];
    assert!(
        scalar_combine.sql.contains("(0.5 * m0) AS thr"),
        "the scalar's 0.5 * projection is re-applied over the recombined sum: {}",
        scalar_combine.sql
    );
    let part_keys = &dq.stages[2];
    assert_eq!(part_keys.hash_key_cols, vec![0], "hashed by p_partkey");
    let semi = &dq.stages[4];
    assert_eq!(semi.upstream_stage_ids, vec![3, 2]);
    assert!(
        semi.sql
            .contains("ps.nk0 IN (SELECT k0 FROM shuffle_input_1)"),
        "the nested IN keeps its spelling: {}",
        semi.sql
    );
    let threshold = &dq.stages[5];
    assert_eq!(threshold.upstream_stage_ids, vec![4, 1]);
    assert!(
        threshold
            .sql
            .contains("ON t.k0 = ps.k0 AND t.k1 = ps.k1 AND (ps.cmp0 > t.thr)"),
        "the correlated scalar compare is a residual on the co-located join: {}",
        threshold.sql
    );
    let outer = &dq.stages[6];
    assert_eq!(outer.hash_key_cols, vec![0], "hashed by s_suppkey");
    assert!(
        outer.sql.contains("FROM supplier CROSS JOIN nation WHERE"),
        "the replicated nation join stays local: {}",
        outer.sql
    );
    let final_semi = &dq.stages[7];
    assert_eq!(final_semi.upstream_stage_ids, vec![6, 5]);
    assert!(
        final_semi
            .sql
            .contains("o.ok0 IN (SELECT k0 FROM shuffle_input_1)"),
        "{}",
        final_semi.sql
    );
}

#[tokio::test]
async fn q20_distributed_matches_single_node() {
    assert_distributed_matches_single_node(Q20).await;
}

// --- Q22: uncorrelated scalar threshold + correlated NOT EXISTS over two sharded tables ---

#[tokio::test]
async fn q22_scalar_and_not_exists_plan_one_row_broadcast_semi() {
    let (dq, _) = plan_multi(Q22).await;
    assert_eq!(
        dq.stages.len(),
        6,
        "scalar partial/combine -> anti producer -> outer scan -> anti+partial -> combine: {dq:?}"
    );
    let scalar_partial = &dq.stages[0];
    assert!(
        scalar_partial
            .sql
            .contains("sum(customer.c_acctbal) AS a0s, count(customer.c_acctbal) AS a0c"),
        "avg decomposes into sum/count partials: {}",
        scalar_partial.sql
    );
    let scalar_combine = &dq.stages[1];
    assert!(
        scalar_combine
            .sql
            .contains("(sum(a0s) / NULLIF(sum(a0c), 0)) AS m0"),
        "{}",
        scalar_combine.sql
    );
    let producer = &dq.stages[2];
    assert_eq!(producer.hash_key_cols, vec![0], "hashed by o_custkey");
    let scan = &dq.stages[3];
    assert_eq!(scan.hash_key_cols, vec![0], "hashed by c_custkey");
    assert!(
        scan.sql
            .contains("customer.c_acctbal > '__WEFT_SCALAR_STAGE__'"),
        "the driver inlines the global avg before dispatch: {}",
        scan.sql
    );
    let anti = &dq.stages[4];
    assert_eq!(anti.upstream_stage_ids, vec![3, 2]);
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
