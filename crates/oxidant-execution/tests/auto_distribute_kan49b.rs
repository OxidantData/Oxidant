//! KAN-49b: distributable strict-mode plans for the remaining window-family TPC-DS queries
//! (Q36/Q44/Q51/Q67/Q70/Q86), each verified end-to-end against single-node execution at the
//! SF10-style configuration (driving fact sharded across two in-process workers, every other
//! table replicated, `OXIDANT_DISTRIBUTED_STRICT=1` so the whole-fact gather can never substitute).
//!
//! Shapes under test:
//!
//! - **Window over a ROLLUP aggregate** (Q67/Q70/Q86): the ROLLUP partial-aggregate hashes
//!   finest-level partials by the first grouping column, a per-partition ROLLUP reconstructs
//!   every level containing it (a tiny fixup stage combines the per-partition grand totals;
//!   Q70's IN-key path keeps the partition-0 gather), and the ranking window then re-shuffles
//!   the combined output (super-aggregate rows with NULL grouping columns included) by its
//!   `PARTITION BY` key — including keys that are
//!   *expressions* over `grouping()` outputs (Q70/Q86's
//!   `PARTITION BY grouping(a)+grouping(b), CASE WHEN grouping(b)=0 THEN a END`), materialized
//!   as computed columns on the combine stage so the shuffle can hash them.
//! - **Window through SubqueryAlias / Projection / HAVING layers** (Q44/Q67): the window no
//!   longer has to sit *directly* on the `Aggregate`; intervening renames fold into the remap
//!   and a HAVING-equivalent filter between the aggregate and the window applies on the combine
//!   stage's output before any window computes.
//! - **Uncorrelated scalar threshold under a window** (Q44): the branch HAVING
//!   `avg(x) > 0.9 * (SELECT avg(x) … GROUP BY key-pinned-to-a-literal)` plans the subquery as
//!   its own partial/combine pair whose one-row output gathers to partition 0 and rides the
//!   (gather-keyed) global-rank window stage as a co-located input — the HAVING reads it as
//!   `(SELECT m0 FROM shuffle_input_1)` — so the whole fact never gathers and no driver-side
//!   literal injection is needed.
//! - **Window over a UNION sharing one aggregate CTE** (Q36): the shared `results` aggregate is
//!   distributed once (partial → combine), gathered, and the whole `UNION` + ranking window then
//!   evaluates locally over the tiny combined CTE rows.
//! - **Window over a FULL OUTER JOIN of two windowed aggregates** (Q51): the sharded side runs
//!   the window-over-aggregate pipeline, the replicated side computes once on a single Forward
//!   worker; both shuffle by the join key for an exact co-located full join, then the outer
//!   framed (`ROWS BETWEEN`) windows compute over a partition keyed shuffle.
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

/// Run an e2e body on a runtime with large worker stacks: unoptimized builds plan the
/// deeply-nested stage SQL (e.g. Q39's HAVING-wrapped stddev combine, Q70's rollup arms) with
/// frames far bigger than tokio's 2 MiB default allows — the same guard
/// `branch_dag_keyed_outer.rs` / `auto_distribute_replicated_slice.rs` document.
fn run_e2e(fut: impl std::future::Future<Output = ()>) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_stack_size(32 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("e2e runtime");
    rt.block_on(fut);
}

const Q36: &str = include_str!("../../../bench/tpcds/queries/q36.sql");
const Q44: &str = include_str!("../../../bench/tpcds/queries/q44.sql");
const Q51: &str = include_str!("../../../bench/tpcds/queries/q51.sql");
const Q67: &str = include_str!("../../../bench/tpcds/queries/q67.sql");
const Q70: &str = include_str!("../../../bench/tpcds/queries/q70.sql");
const Q86: &str = include_str!("../../../bench/tpcds/queries/q86.sql");

/// The SF10 post-classification configuration per query: only the query's driving fact is
/// sharded; every other table the query touches is replicated.
const REPL_STORE_SALES: [&str; 4] = ["date_dim", "store", "item", "web_sales"];
const REPL_WEB_SALES: [&str; 4] = ["date_dim", "store", "item", "store_sales"];

static PORT: std::sync::OnceLock<AtomicU16> = std::sync::OnceLock::new();

fn unique_worker_port() -> u16 {
    // OnceLock-seeded allocator with the base BELOW the Linux ephemeral source range
    // (32768..=60999): the harness's own outbound connections can never steal a worker's
    // port (serve_worker swallows EADDRINUSE; the old in-range bases flaked "did not
    // bind" / "distributed run never succeeded" on loaded CI runners).
    PORT.get_or_init(|| AtomicU16::new(18000 + (std::process::id() as u16 % 512)))
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

/// sk 1-2 are year 2000 (month_seq 1198/1199, outside every query's window); sk 3-8 are 2001
/// with month_seq 1200..1205 (inside the `1200..1211` band Q51/Q67/Q70/Q86 filter on, and
/// `d_year = 2001` for Q36).
fn date_dim() -> RecordBatch {
    batch(
        vec![
            i64f("d_date_sk"),
            i64f("d_year"),
            i64f("d_month_seq"),
            i64f("d_qoy"),
            i64f("d_moy"),
            Field::new("d_date", DataType::Date32, false),
        ],
        vec![
            i64v(&[1, 2, 3, 4, 5, 6, 7, 8]),
            i64v(&[2000, 2000, 2001, 2001, 2001, 2001, 2001, 2001]),
            i64v(&[1198, 1199, 1200, 1201, 1202, 1203, 1204, 1205]),
            i64v(&[4, 4, 1, 1, 1, 2, 2, 2]),
            i64v(&[11, 12, 1, 2, 3, 4, 5, 6]),
            // 2000-11-01 .. 2001-06-01 as days since the epoch.
            Arc::new(Date32Array::from(vec![
                11262, 11292, 11323, 11354, 11382, 11413, 11443, 11474,
            ])),
        ],
    )
}

/// Two stores per state so Q70's ROLLUP(s_state, s_county) has subtotals to form; sk 4 is the
/// store Q44's branches pin (`ss_store_sk = 4`).
fn store() -> RecordBatch {
    batch(
        vec![
            i64f("s_store_sk"),
            strf("s_state"),
            strf("s_county"),
            strf("s_store_id"),
        ],
        vec![
            i64v(&[1, 2, 3, 4]),
            strv(&["TN", "TN", "GA", "GA"]),
            strv(&["Davidson", "Shelby", "Fulton", "DeKalb"]),
            strv(&["store_a", "store_b", "store_c", "store_d"]),
        ],
    )
}

/// 14 items: Books (1-6: fiction 1-3, nonfiction 4-6), Electronics (7-14: phones 7-10,
/// laptops 11-14). Q44 joins `item` twice for product names; Q36/Q67/Q86 roll up the
/// category/class hierarchy.
fn item() -> RecordBatch {
    let names: Vec<String> = (1..=14).map(|i| format!("prod{i}")).collect();
    let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
    batch(
        vec![
            i64f("i_item_sk"),
            strf("i_category"),
            strf("i_class"),
            strf("i_brand"),
            strf("i_product_name"),
        ],
        vec![
            i64v(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]),
            strv(&[
                "Books",
                "Books",
                "Books",
                "Books",
                "Books",
                "Books",
                "Electronics",
                "Electronics",
                "Electronics",
                "Electronics",
                "Electronics",
                "Electronics",
                "Electronics",
                "Electronics",
            ]),
            strv(&[
                "fiction",
                "fiction",
                "fiction",
                "nonfiction",
                "nonfiction",
                "nonfiction",
                "phones",
                "phones",
                "phones",
                "phones",
                "laptops",
                "laptops",
                "laptops",
                "laptops",
            ]),
            strv(&[
                "brandA", "brandA", "brandB", "brandB", "brandC", "brandC", "brandA", "brandA",
                "brandA", "brandB", "brandB", "brandB", "brandC", "brandC",
            ]),
            strv(&name_refs),
        ],
    )
}

/// Sharded fact for Q36/Q44/Q51/Q67/Q70. Row *order* is load-bearing for the test: the harness
/// shards contiguous halves, so the two Q44 row groups (items 1-12 at date 5, then the same
/// items at date 6) straddle the split — every one of those per-item groups needs both workers.
///
/// - Rows 0-11 (Q44): store 4, item 1-12, date 5, `ss_addr_sk IS NULL` (the scalar subquery's
///   rows), net_profit `i*10 - 5`.
/// - Rows 12-25 (Q36 + Q51): TN stores at 2001 dates (Q36 reads them) plus Q51's store-side
///   (item, date) cells.
/// - Rows 26-37 (Q44): store 4, item 1-12, date 6, addr set, net_profit `i*10 + 5` — so item
///   `i` averages `i*10` and the scalar over the NULL-addr rows averages 60 (threshold 54,
///   keeping items 6-12).
fn store_sales() -> RecordBatch {
    let mut date_sk = Vec::new();
    let mut item_sk = Vec::new();
    let mut store_sk = Vec::new();
    let mut addr_sk: Vec<Option<i64>> = Vec::new();
    let mut qty = Vec::new();
    let mut price = Vec::new();
    let mut ext = Vec::new();
    let mut profit = Vec::new();
    let mut row = |d: i64, i: i64, s: i64, a: Option<i64>, q: i64, p: f64, e: f64, n: f64| {
        date_sk.push(d);
        item_sk.push(i);
        store_sk.push(s);
        addr_sk.push(a);
        qty.push(q);
        price.push(p);
        ext.push(e);
        profit.push(n);
    };
    // Q44 date-5 rows (NULL addr): profits 5, 15, …, 115.
    for i in 1..=12i64 {
        row(5, i, 4, None, 2, 100.0, 200.0, (i * 10 - 5) as f64);
    }
    // Q36 rows (TN stores, 2001 dates).
    row(3, 1, 1, Some(1), 1, 50.0, 50.0, 10.0);
    row(3, 2, 1, Some(1), 1, 60.0, 60.0, 20.0);
    row(4, 1, 2, Some(1), 1, 70.0, 70.0, 30.0);
    row(4, 3, 1, Some(1), 1, 80.0, 80.0, 15.0);
    row(3, 7, 2, Some(1), 1, 90.0, 90.0, 25.0);
    row(4, 8, 1, Some(1), 1, 100.0, 100.0, 35.0);
    row(3, 4, 2, Some(1), 1, 110.0, 110.0, 5.0);
    row(4, 7, 2, Some(1), 1, 120.0, 120.0, 45.0);
    // Q51 store-side rows (item, date) cells.
    row(3, 1, 1, Some(1), 1, 10.0, 10.0, 1.0);
    row(4, 1, 1, Some(1), 1, 10.0, 10.0, 1.0);
    row(5, 1, 1, Some(1), 1, 10.0, 10.0, 1.0);
    row(3, 2, 1, Some(1), 1, 10.0, 10.0, 1.0);
    row(4, 2, 1, Some(1), 1, 10.0, 10.0, 1.0);
    row(4, 3, 1, Some(1), 1, 10.0, 10.0, 1.0);
    // Q44 date-6 rows (addr set): profits 15, 25, …, 125 → item i averages i*10.
    for i in 1..=12i64 {
        row(6, i, 4, Some(1), 2, 100.0, 200.0, (i * 10 + 5) as f64);
    }
    batch(
        vec![
            i64f("ss_sold_date_sk"),
            i64f("ss_item_sk"),
            i64f("ss_store_sk"),
            Field::new("ss_addr_sk", DataType::Int64, true),
            i64f("ss_quantity"),
            f64f("ss_sales_price"),
            f64f("ss_ext_sales_price"),
            f64f("ss_net_profit"),
        ],
        vec![
            i64v(&date_sk),
            i64v(&item_sk),
            i64v(&store_sk),
            Arc::new(Int64Array::from(addr_sk)),
            i64v(&qty),
            f64v(&price),
            f64v(&ext),
            f64v(&profit),
        ],
    )
}

/// Replicated for Q51 (the FULL OUTER JOIN's web side), sharded for Q86. The first four rows
/// are Q51's web-side (item, date) cells — two overlapping the store side, one web-only
/// (item 4, date 3); the rest give Q86 a two-category ROLLUP input whose Electronics groups
/// span the shard split. Row 0's price is boosted (2000) so item 1's web cumulative beats the
/// store side's (Q44's price-100 rows share the same (item, date) cells and inflate it),
/// leaving `web_cumulative > store_cumulative` non-empty.
fn web_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ws_sold_date_sk"),
            i64f("ws_item_sk"),
            f64f("ws_sales_price"),
            f64f("ws_net_paid"),
        ],
        vec![
            i64v(&[3, 4, 4, 3, 5, 4, 3, 5, 4, 3]),
            i64v(&[1, 1, 2, 4, 7, 8, 9, 11, 12, 13]),
            f64v(&[2000.0, 30.0, 30.0, 30.0, 40.0, 40.0, 40.0, 50.0, 50.0, 50.0]),
            f64v(&[25.0, 25.0, 25.0, 25.0, 30.0, 30.0, 30.0, 35.0, 35.0, 35.0]),
        ],
    )
}

fn register(engine: &Engine, name: &str, batches: Vec<RecordBatch>) {
    engine.register_batches(name, batches).unwrap();
}

/// Every table the six queries touch, in full.
fn all_tables() -> Vec<(&'static str, RecordBatch)> {
    vec![
        ("date_dim", date_dim()),
        ("store", store()),
        ("item", item()),
        ("store_sales", store_sales()),
        ("web_sales", web_sales()),
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

/// No whole-fact gather anywhere in the plan (strict mode must never substitute it).
fn assert_no_fact_gather(dq: &oxidant_execution::plan::DistributedQuery) {
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.sql.contains("__oxidant_materialize_gate")
                || s.sql.contains("__oxidant_subquery_gate")),
        "no whole-fact gather: {dq:?}"
    );
}

/// The identical-stage CSE's postcondition: no two stages share the full dispatch contract
/// (sql, exchange, hash key, upstreams) — every mergeable duplicate collapsed to one stage.
fn assert_no_identical_stages(dq: &oxidant_execution::plan::DistributedQuery) {
    let mut seen = std::collections::HashSet::new();
    for s in &dq.stages {
        let key = format!(
            "{:?}|{:?}|{:?}|{}",
            s.exchange, s.hash_key_cols, s.upstream_stage_ids, s.sql
        );
        assert!(seen.insert(key), "duplicate stage survived CSE: {dq:?}");
    }
}

// --- Q67 / Q70 / Q86: ranking window over a ROLLUP aggregate ---

#[test]
fn q67_rank_over_rollup_plans_and_matches() {
    run_e2e(async move {
        std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
        let planner = tpcds_engine().await;
        let dq = strict_plan(&planner, Q67, &REPL_STORE_SALES).await;
        assert_no_fact_gather(&dq);
        assert!(
            dq.stages.iter().any(|s| s.sql.contains("GROUP BY ROLLUP")),
            "the combine reconstructs the rollup levels: {dq:?}"
        );
        assert!(
            dq.stages
                .iter()
                .any(|s| s.sql.contains("rank() OVER (PARTITION BY")),
            "the ranking window computes after the partition shuffle: {dq:?}"
        );
        // Two-phase grouping sets (the Q67 SF10 win): the finest-level partial hash-shuffles by the
        // first rollup column (i_category = g0) instead of gathering 4.84M rows to one partition; a
        // per-partition ROLLUP computes every g0-bearing level exactly and tags each partition's
        // grand-total partial (`__gid = 1`); the fixup stage passes the exact rows through and
        // combines the ≤ #partitions grand totals on the NULL-g0 bucket. The
        // `HAVING COUNT(*) > 0` empty-partition guard rides both stages.
        let partial = &dq.stages[0];
        assert_eq!(
            partial.hash_key_cols,
            vec![0],
            "the partial keys on g0, not a partition-0 gather: {dq:?}"
        );
        let rollup = dq
            .stages
            .iter()
            .find(|s| s.sql.contains("GROUP BY ROLLUP"))
            .expect("per-partition rollup stage");
        assert_eq!(rollup.hash_key_cols, vec![0], "{dq:?}");
        assert!(
            rollup.sql.contains("grouping(g0) AS __gid")
                && rollup.sql.contains("HAVING COUNT(*) > 0"),
            "grand-total partials tagged; empty-partition guard kept: {dq:?}"
        );
        assert!(
            dq.stages
                .iter()
                .any(|s| s.sql.contains("__gid = 0")
                    && s.sql.contains("__gid = 1 HAVING COUNT(*) > 0")),
            "the grand-total fixup funnels ≤ #partitions rows: {dq:?}"
        );
        drop(dq);
        assert_distributed_matches_single_node(Q67, &REPL_STORE_SALES, "store_sales").await;
    });
}

#[test]
fn q70_rank_over_rollup_with_windowed_in_subquery_plans_and_matches() {
    run_e2e(async move {
        std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
        let planner = tpcds_engine().await;
        let dq = strict_plan(&planner, Q70, &REPL_STORE_SALES).await;
        assert_no_fact_gather(&dq);
        assert!(
            dq.stages
                .iter()
                .any(|s| s.sql.contains("GROUP BY ROLLUP") && s.sql.contains("grouping(")),
            "the combine recomputes grouping() at the rolled-up levels: {dq:?}"
        );
        assert!(
            dq.stages
                .iter()
                .any(|s| s.sql.contains("IN (SELECT") && s.sql.contains("shuffle_input_")),
            "the IN filter is evaluated against the co-located subquery stream: {dq:?}"
        );
        drop(dq);
        assert_distributed_matches_single_node(Q70, &REPL_STORE_SALES, "store_sales").await;
    });
}

#[test]
fn q86_rank_over_rollup_grouping_expr_key_plans_and_matches() {
    run_e2e(async move {
        std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
        let planner = tpcds_engine().await;
        let dq = strict_plan(&planner, Q86, &REPL_WEB_SALES).await;
        assert_no_fact_gather(&dq);
        assert!(
            dq.stages
                .iter()
                .any(|s| s.sql.contains("GROUP BY ROLLUP") && s.sql.contains("grouping(")),
            "the combine recomputes grouping() at the rolled-up levels: {dq:?}"
        );
        assert!(
            dq.stages
                .iter()
                .any(|s| s.sql.contains("rank() OVER (PARTITION BY")),
            "the ranking window computes after the partition shuffle: {dq:?}"
        );
        drop(dq);
        assert_distributed_matches_single_node(Q86, &REPL_WEB_SALES, "web_sales").await;
    });
}

// --- Q44: global rank() over a per-item aggregate with a scalar-subquery HAVING ---

#[test]
fn q44_global_rank_over_scalar_having_plans_and_matches() {
    run_e2e(async move {
        std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
        let planner = tpcds_engine().await;
        let dq = strict_plan(&planner, Q44, &REPL_STORE_SALES).await;
        assert_no_fact_gather(&dq);
        assert!(
            dq.stages
                .iter()
                .any(|s| s.sql.contains("rank() OVER (ORDER BY")),
            "the global ranking window computes on the gathered combine partition: {dq:?}"
        );
        // The two branches' identical scalar subqueries each plan a partial/combine pair whose
        // one-row output gathers to partition 0 and rides the window stage as a co-located input:
        // the branch HAVING reads it as `(SELECT m0 FROM shuffle_input_1)`.
        assert!(
            dq.stages
                .iter()
                .any(|s| s.sql.contains("(SELECT m0 FROM shuffle_input_1)")),
            "a co-located scalar stream feeds the branch HAVING stage: {dq:?}"
        );
        assert!(
            dq.stages
                .iter()
                .any(|s| s.sql.contains("shuffle_input_0") && s.sql.contains("shuffle_input_1")),
            "the outer skeleton joins both rank branches from shuffle inputs: {dq:?}"
        );
        // Identical-stage CSE: the two window branches re-planned byte-identical aggregate and
        // HAVING-scalar sub-DAGs (their branch fingerprints differ only on the window ORDER BY
        // direction, so plan-level dedup can't fire). The generic stage CSE merges them: 11 → 7
        // stages, 4 store_sales scans → 2; only the ASC/DESC window stages stay distinct.
        assert_eq!(dq.stages.len(), 7, "{dq:?}");
        assert_eq!(
            dq.stages
                .iter()
                .filter(|s| s.sql.contains("store_sales"))
                .count(),
            2,
            "each distinct store_sales input scans once: {dq:?}"
        );
        assert_no_identical_stages(&dq);
        drop(dq);
        assert_distributed_matches_single_node(Q44, &REPL_STORE_SALES, "store_sales").await;
    });
}

// --- Q36: ranking window over a UNION sharing one aggregate CTE ---

#[test]
fn q36_window_over_shared_cte_union_plans_and_matches() {
    run_e2e(async move {
        std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
        let planner = tpcds_engine().await;
        let dq = strict_plan(&planner, Q36, &REPL_STORE_SALES).await;
        assert_no_fact_gather(&dq);
        assert!(
            dq.stages.iter().any(|s| s.sql.contains("UNION")),
            "the union evaluates over the gathered CTE rows: {dq:?}"
        );
        assert!(
            dq.stages
                .iter()
                .any(|s| s.sql.contains("rank() OVER (PARTITION BY")),
            "the ranking window computes locally over the union: {dq:?}"
        );
        // The shared `results` CTE inlines per union arm, so the arms' partials are byte-identical
        // (stage 2 ≡ stage 0 before CSE): the stage-level CSE merges them — one store_sales scan
        // saved — while the arms' distinct combine projections stay separate.
        assert_eq!(dq.stages.len(), 8, "{dq:?}");
        assert_eq!(
            dq.stages
                .iter()
                .filter(|s| s.sql.contains("store_sales"))
                .count(),
            2,
            "the shared CTE scans store_sales once per distinct input: {dq:?}"
        );
        assert_no_identical_stages(&dq);
        drop(dq);
        assert_distributed_matches_single_node(Q36, &REPL_STORE_SALES, "store_sales").await;
    });
}

// --- Q51: framed windows over a FULL OUTER JOIN of two windowed aggregates ---

#[test]
fn q51_window_over_full_join_of_windowed_aggregates_plans_and_matches() {
    run_e2e(async move {
        std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
        let planner = tpcds_engine().await;
        let dq = strict_plan(&planner, Q51, &REPL_STORE_SALES).await;
        assert_no_fact_gather(&dq);
        assert!(
            dq.stages.iter().any(|s| s.sql.contains("FULL JOIN")),
            "the two windowed branches full-join on the co-located key: {dq:?}"
        );
        assert!(
            dq.stages.iter().any(|s| s
                .sql
                .contains("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW")),
            "the framed cumulative windows ride the partition shuffle: {dq:?}"
        );
        drop(dq);
        assert_distributed_matches_single_node(Q51, &REPL_STORE_SALES, "store_sales").await;
    });
}

// --- KAN-162: Q51 with BOTH join sides sharded (the all-facts-sharded SF100 classification) ---

/// Both facts sharded: only the shared dims replicate.
const REPL_NEITHER_FACT: [&str; 3] = ["date_dim", "store", "item"];

/// Every table held in full on each worker except the named facts, row-sharded into contiguous
/// halves — both windowed branches' partials genuinely recombine across workers.
async fn two_workers_two_sharded(facts: &[&str]) -> Cluster {
    let fact_full = |name: &str| match name {
        "store_sales" => store_sales(),
        "web_sales" => web_sales(),
        other => panic!("unknown fact {other}"),
    };
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        for (name, batch) in all_tables() {
            if facts.contains(&name) {
                register(&e, name, shard_rows(&fact_full(name), i));
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

#[test]
fn q51_both_sharded_window_join_plans_and_matches() {
    run_e2e(async move {
        std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
        let planner = tpcds_engine().await;
        let dq = strict_plan(&planner, Q51, &REPL_NEITHER_FACT).await;
        assert_no_fact_gather(&dq);
        assert!(
            dq.stages.iter().any(|s| s.sql.contains("FULL JOIN")),
            "the two windowed branches full-join on the co-located key: {dq:?}"
        );
        for fact in ["store_sales", "web_sales"] {
            assert!(
                dq.stages
                    .iter()
                    .any(|s| s.upstream_stage_ids.is_empty() && s.sql.contains(fact)),
                "each sharded windowed-aggregate branch runs its own sharded pipeline: {dq:?}"
            );
        }
        let expected = planner.sql(Q51).await.expect("single-node");
        assert!(
            expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
            "single-node result must be non-empty (otherwise the comparison is vacuous)"
        );
        let cluster = two_workers_two_sharded(&["store_sales", "web_sales"]).await;
        let actual = run_distributed(&cluster, &planner, Q51, &REPL_NEITHER_FACT).await;
        assert_eq!(
            rows_sorted(&actual),
            rows_sorted(&expected),
            "both-sharded window join must equal single-node"
        );
    });
}

/// Decline pin: the both-sharded relaxation keeps the per-side requirements — a sharded side
/// that is not a windowed aggregate branch must still refuse (strict mode).
const BOTH_SHARDED_PLAIN_SIDE: &str = "
SELECT a.d_year AS y, a.s_amt AS store_amt, b.s_amt AS web_amt,
       SUM(COALESCE(a.s_amt, 0)) OVER (PARTITION BY a.d_year) AS cum
FROM (
  SELECT d_year, i_item_sk, SUM(ss_ext_sales_price) AS s_amt,
         ROW_NUMBER() OVER (PARTITION BY d_year ORDER BY SUM(ss_ext_sales_price)) AS rn
  FROM store_sales JOIN date_dim ON ss_sold_date_sk = d_date_sk
  JOIN item ON ss_item_sk = i_item_sk
  GROUP BY d_year, i_item_sk
) a
FULL OUTER JOIN (
  SELECT d_year, ws_item_sk, SUM(ws_net_paid) AS s_amt
  FROM web_sales JOIN date_dim ON ws_sold_date_sk = d_date_sk
  GROUP BY d_year, ws_item_sk
) b ON a.d_year = b.d_year AND a.i_item_sk = b.ws_item_sk
";

#[test]
fn both_sharded_window_join_with_plain_side_still_refuses() {
    run_e2e(async move {
        let planner = tpcds_engine().await;
        let lp = planner
            .logical_plan(BOTH_SHARDED_PLAIN_SIDE)
            .await
            .expect("logical plan");
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("OXIDANT_DISTRIBUTED_STRICT", "1");
        let planned = plan_distributed_logical(&lp, &REPL_NEITHER_FACT);
        std::env::remove_var("OXIDANT_DISTRIBUTED_STRICT");
        assert!(
            planned.is_err(),
            "a both-sharded window join whose sharded side is not a windowed aggregate must \
             keep refusing in strict mode, got {planned:?}"
        );
    });
}
