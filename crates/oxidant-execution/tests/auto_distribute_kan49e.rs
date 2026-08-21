//! KAN-49 (Q6): the TPC-DS Q6 broadcast stage must never hand a worker a plain `CROSS JOIN`
//! between large tables. Q6's unoptimized plan parks a five-table comma join
//! (`customer_address, customer, store_sales, date_dim, item`) under one `Filter`, and the old
//! stage-0 SQL spliced it verbatim as `… CROSS JOIN …` with the four equijoin predicates in
//! `WHERE`. DataFusion has no cost-based join reordering, and `date_dim` / `item` equijoin only
//! against `store_sales`, so the emitted order left genuine cross joins under the fact join —
//! `CrossJoinExec` buffers its *entire* left input in worker memory (at SF10 that was
//! `date_dim` filtered to one `d_month_seq` × `customer ⋈ customer_address`, 16 GB) outside
//! the KAN-25 hash-join build guard, which only inspects `HashJoinExec` builds.
//!
//! The fix (`join_order::connect_comma_join_chain`) rewrites the comma chain into a connected
//! chain of keyed inner equijoins before stage SQL is emitted. These tests pin the structural
//! property (the SF10 OOM itself is not reproducible at fixture scale):
//!
//! - the generated stage SQL contains **no `CROSS JOIN`** — every join carries its equijoin
//!   keys in an `ON` clause;
//! - the fact-scan stage SQL (not always stage 0 after KAN-144's scalar-first layout) planned
//!   through the in-process engine (exactly what a worker runs after token substitution) yields
//!   a physical plan with **no `CrossJoinExec`** (and no `NestedLoopJoinExec`) — every join is
//!   a hash/sort-merge equijoin;
//! - the distributed plan still matches single-node end-to-end under
//!   `OXIDANT_DISTRIBUTED_STRICT=1` (no whole-fact gather substitution).

// ENV_LOCK serializes process-global `OXIDANT_DISTRIBUTED_STRICT` across async tests.
#![allow(clippy::await_holding_lock)]

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::physical_plan::ExecutionPlan;
use oxidant_execution::driver::{run_stages, Cluster};
use oxidant_execution::flight::serve_worker;
use oxidant_execution::plan::plan_distributed_logical;
use oxidant_loom::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::arrow::util::display::{ArrayFormatter, FormatOptions};
use oxidant_loom::Engine;

const Q6: &str = include_str!("../../../bench/tpcds/queries/q6.sql");

/// The SF10 post-classification configuration for Q6: `store_sales` is the sharded fact;
/// every other table it touches is replicated (present in full on every worker). The
/// correlated subquery scans `item`, the uncorrelated one `date_dim` — both replicated.
const REPL_Q6: [&str; 4] = ["customer_address", "customer", "date_dim", "item"];

static PORT: std::sync::OnceLock<AtomicU16> = std::sync::OnceLock::new();

fn unique_worker_port() -> u16 {
    // OnceLock-seeded allocator with the base BELOW the Linux ephemeral source range
    // (32768..=60999): the harness's own outbound connections can never steal a worker's
    // port (serve_worker swallows EADDRINUSE; the old in-range bases flaked "did not
    // bind" / "distributed run never succeeded" on loaded CI runners).
    PORT.get_or_init(|| AtomicU16::new(21000 + (std::process::id() as u16 % 512)))
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

/// sk 1..=12 are Jan-2001 (month_seq 120 — the subquery's `d_year = 2001 AND d_moy = 1`
/// filter), sk 13..=16 Feb-2001 (month_seq 121), sk 17..=20 Jan-2000 (month_seq 108).
fn date_dim() -> RecordBatch {
    let mut sks: Vec<i64> = (1..=20).collect();
    let mut month_seq = vec![120i64; 12];
    month_seq.extend([121, 121, 121, 121, 108, 108, 108, 108]);
    let mut year = vec![2001i64; 16];
    year.extend([2000, 2000, 2000, 2000]);
    let mut moy = vec![1i64; 12];
    moy.extend([2, 2, 2, 2, 1, 1, 1, 1]);
    sks.shrink_to_fit();
    batch(
        vec![
            i64f("d_date_sk"),
            i64f("d_month_seq"),
            i64f("d_year"),
            i64f("d_moy"),
        ],
        vec![i64v(&sks), i64v(&month_seq), i64v(&year), i64v(&moy)],
    )
}

/// Books avg price = 15.0 → the correlated `> 1.2 * avg` threshold is 18.0, so only item 2
/// (20.0) qualifies; Music avg = 5.0 → threshold 6.0, item 3 does not qualify.
fn item() -> RecordBatch {
    batch(
        vec![
            i64f("i_item_sk"),
            f64f("i_current_price"),
            strf("i_category"),
        ],
        vec![
            i64v(&[1, 2, 3]),
            f64v(&[10.0, 20.0, 5.0]),
            strv(&["Books", "Books", "Music"]),
        ],
    )
}

fn customer() -> RecordBatch {
    batch(
        vec![i64f("c_customer_sk"), i64f("c_current_addr_sk")],
        vec![i64v(&[1, 2]), i64v(&[1, 2])],
    )
}

fn customer_address() -> RecordBatch {
    batch(
        vec![i64f("ca_address_sk"), strf("ca_state")],
        vec![i64v(&[1, 2]), strv(&["GA", "TN"])],
    )
}

/// The sharded fact. Rows 0..=11 qualify (item 2, Jan-2001 dates, customer 1 → GA), split
/// 9/3 across the two shards so the GA count genuinely combines across workers. Distractors:
/// TN rows (group survives but stays under the HAVING ≥ 10 gate), Feb-2001 rows (wrong
/// month_seq), item-1 rows (under the correlated price threshold), item-3 rows (no threshold).
fn store_sales() -> RecordBatch {
    let mut sold_date: Vec<i64> = (1..=12).collect();
    sold_date.extend([1, 2, 3, 13, 14, 1, 2, 3, 4]);
    let mut items = vec![2i64; 12];
    items.extend([2, 2, 2, 2, 2, 1, 1, 3, 3]);
    let mut customers = vec![1i64; 12];
    customers.extend([2, 2, 2, 1, 1, 1, 1, 1, 1]);
    batch(
        vec![
            i64f("ss_sold_date_sk"),
            i64f("ss_item_sk"),
            i64f("ss_customer_sk"),
        ],
        vec![i64v(&sold_date), i64v(&items), i64v(&customers)],
    )
}

fn register_all(engine: &Engine) {
    for (name, batch) in [
        ("date_dim", date_dim()),
        ("item", item()),
        ("customer", customer()),
        ("customer_address", customer_address()),
        ("store_sales", store_sales()),
    ] {
        engine.register_batches(name, vec![batch]).unwrap();
    }
}

/// Planner/ground-truth engine holding the full dataset.
async fn tpcds_engine() -> Engine {
    let e = Engine::new();
    register_all(&e);
    e
}

/// Contiguous half of the fact, so cross-shard groups genuinely need both workers.
fn shard_rows(full: &RecordBatch, idx: usize) -> Vec<RecordBatch> {
    let half = full.num_rows() / 2;
    let (start, len) = if idx == 0 {
        (0, half)
    } else {
        (half, full.num_rows() - half)
    };
    vec![full.slice(start, len)]
}

/// `store_sales` sharded row-wise across two in-process workers; dims held in full on each.
async fn two_workers_sharded() -> Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        for (name, batch) in [
            ("date_dim", date_dim()),
            ("item", item()),
            ("customer", customer()),
            ("customer_address", customer_address()),
        ] {
            e.register_batches(name, vec![batch]).unwrap();
        }
        e.register_batches("store_sales", shard_rows(&store_sales(), i))
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

/// Plan `sql` under `OXIDANT_DISTRIBUTED_STRICT=1` (the whole-fact gather must never substitute).
async fn strict_plan(planner: &Engine, sql: &str) -> oxidant_execution::plan::DistributedQuery {
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("OXIDANT_DISTRIBUTED_STRICT", "1");
    let planned = plan_distributed_logical(&lp, &REPL_Q6);
    std::env::remove_var("OXIDANT_DISTRIBUTED_STRICT");
    planned.expect("strict-mode plan_distributed_logical")
}

/// The number of physical-plan nodes of type `T` anywhere in the plan tree.
fn count_exec<T: 'static>(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let here = usize::from(
        (plan.as_ref() as &dyn std::any::Any)
            .downcast_ref::<T>()
            .is_some(),
    );
    here + plan
        .children()
        .iter()
        .map(|c| count_exec::<T>(c))
        .sum::<usize>()
}

/// Sorted value rows (headers are not compared: single-node and distributed plans name
/// unaliased aggregate outputs differently — pre-existing behavior of every distributed shape).
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

/// The structural pin: Q6's generated stage SQL contains no `CROSS JOIN` (every join carries
/// its equijoin keys in `ON`), and the fact-scan stage SQL — planned through the in-process
/// engine exactly as a worker plans it — yields a physical plan with no `CrossJoinExec` and
/// no `NestedLoopJoinExec`. This is the property whose absence OOM'd the SF10 worker; it is
/// checkable at any scale because it is structural, not statistics-dependent.
///
/// KAN-144: the uncorrelated `SELECT DISTINCT d_month_seq` bound is a legitimate one-row
/// broadcast, so stage 0 is now the Forward scalar (`GROUP BY d_month_seq`) and the fact
/// scan (four equijoins + correlated price threshold + `'__OXIDANT_SCALAR_STAGE_0__'`) is a
/// later stage. Locate the fact-scan stage by content, not by index 0.
#[tokio::test]
async fn q6_stage_sql_has_no_cross_join_anywhere() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    std::env::set_var("OXIDANT_PREFER_HASH_JOIN", "true");
    let planner = tpcds_engine().await;
    let dq = strict_plan(&planner, Q6).await;

    for s in &dq.stages {
        assert!(
            !s.sql.to_ascii_uppercase().contains("CROSS JOIN"),
            "stage {} must not emit a cross join: {}",
            s.stage_id,
            s.sql
        );
    }
    // Scalar-stage-first (KAN-144): find the fact-scan stage that carries the four ON equijoins.
    let fact_sql = dq
        .stages
        .iter()
        .map(|s| s.sql.as_str())
        .find(|sql| sql.contains("JOIN store_sales AS s ON") && sql.contains("JOIN item AS i ON"))
        .expect("q6 plan must include a fact-scan stage with the four equijoins");
    // The four fact/dim equijoins are syntactic ON clauses; the correlated price threshold
    // stays spliced in WHERE; the uncorrelated month_seq bound is a scalar token (substituted
    // at dispatch from the Forward stage-0 DISTINCT/GROUP BY).
    assert!(fact_sql.contains("JOIN customer AS c ON"), "{fact_sql}");
    assert!(fact_sql.contains("JOIN store_sales AS s ON"), "{fact_sql}");
    assert!(fact_sql.contains("JOIN date_dim AS d ON"), "{fact_sql}");
    assert!(fact_sql.contains("JOIN item AS i ON"), "{fact_sql}");
    for key in [
        "a.ca_address_sk = c.c_current_addr_sk",
        "c.c_customer_sk = s.ss_customer_sk",
        "s.ss_sold_date_sk = d.d_date_sk",
        "s.ss_item_sk = i.i_item_sk",
    ] {
        assert!(
            fact_sql.contains(key),
            "equijoin key must be an ON clause: {fact_sql}"
        );
    }
    assert!(
        fact_sql.contains("SELECT avg(j.i_current_price)"),
        "{fact_sql}"
    );
    assert!(
        fact_sql.contains("__OXIDANT_SCALAR_STAGE_0__"),
        "month_seq bound must be the indexed scalar token: {fact_sql}"
    );
    assert!(
        dq.stages[0].sql.contains("GROUP BY") && dq.stages[0].sql.contains("d_month_seq"),
        "stage 0 must be the Forward month_seq scalar: {}",
        dq.stages[0].sql
    );

    // Plan the fact-scan stage through a worker-equivalent engine: no non-equijoin operator.
    // Substitute the scalar token with a fixture literal so the physical plan is parseable
    // (the real driver does this before dispatch).
    let fact_for_phys = fact_sql.replace("'__OXIDANT_SCALAR_STAGE_0__'", "120");
    let worker = Engine::new();
    register_all(&worker);
    let plan = worker
        .physical_plan(&fact_for_phys)
        .await
        .expect("worker physical plan of fact-scan SQL");
    let display = datafusion::physical_plan::displayable(plan.as_ref()).indent(false);
    assert_eq!(
        count_exec::<datafusion::physical_plan::joins::CrossJoinExec>(&plan),
        0,
        "fact-scan physical plan must contain no CrossJoinExec:\n{display}"
    );
    assert_eq!(
        count_exec::<datafusion::physical_plan::joins::NestedLoopJoinExec>(&plan),
        0,
        "fact-scan physical plan must contain no NestedLoopJoinExec:\n{display}"
    );
    assert!(
        count_exec::<datafusion::physical_plan::joins::HashJoinExec>(&plan) >= 4,
        "the four equijoins plan as hash joins under OXIDANT_PREFER_HASH_JOIN=true:\n{display}"
    );
}

/// The end-to-end pin: the rewritten stage SQL is not just better-shaped, it is correct —
/// distributed across two workers with `store_sales` genuinely sharded, strict mode, it must
/// equal single-node (GA count 12: 9 qualifying rows on shard 0 + 3 on shard 1).
#[tokio::test]
async fn q6_distributed_matches_single_node() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    std::env::set_var("OXIDANT_PREFER_HASH_JOIN", "true");
    let planner = tpcds_engine().await;
    let expected = planner.sql(Q6).await.expect("single-node");
    assert_eq!(
        rows_sorted(&expected),
        vec![vec!["GA".to_string(), "12".to_string()]],
        "fixture sanity: single-node Q6 must return GA/12"
    );
    let cluster = two_workers_sharded().await;
    let dq = strict_plan(&planner, Q6).await;
    let mut out = None;
    for _ in 0..150 {
        match run_stages(&cluster, &dq.stages).await {
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
    let actual = match &dq.finalize_sql {
        None => gathered,
        Some(fsql) => {
            let fin = Engine::new();
            fin.register_batches("result", gathered).unwrap();
            fin.sql(fsql).await.expect("finalize")
        }
    };
    assert_eq!(
        rows_sorted(&actual),
        rows_sorted(&expected),
        "distributed must equal single-node"
    );
}
