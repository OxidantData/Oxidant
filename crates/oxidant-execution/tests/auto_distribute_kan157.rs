//! KAN-157: semi/anti EXISTS subqueries at the all-facts-sharded classification (TPC-DS
//! Q10/Q35/Q69 at SF100, where KAN-161's 4 GiB threshold shards `store_sales`, `web_sales`
//! AND `catalog_sales` and only the dims stay replicated). KAN-55 covered one sharded fact
//! with the smaller channels replicated; here every subquery scans its own sharded fact.
//!
//! The mechanism (`try_semi_anti_subqueries`):
//!
//! - **Per-fact key producers**: each EXISTS subquery may scan its own single sharded fact
//!   exactly once; every other table is replicated. One `SELECT DISTINCT` key-stream producer
//!   per subquery, hash-shuffled by the shared correlation key.
//! - **OR-of-EXISTS disjuncts** (Q10/Q35): a top-level `OR` of non-negated EXISTS arms becomes
//!   one producer per disjunct, re-OR'd (parenthesized) against the co-located key streams in
//!   the semi stage. Negated or mixed ORs decline.
//! - **Mixed semi + anti over distinct facts** (Q69): `EXISTS(store)` gates emission onto the
//!   key's partition while the `NOT EXISTS(web)` / `NOT EXISTS(catalog)` streams co-locate on
//!   the same shared key.
//!
//! Every distributed plan must equal single-node end-to-end, in strict mode
//! (`OXIDANT_DISTRIBUTED_STRICT=1`) so the whole-fact gather cannot silently substitute.
//! Shapes that cannot be proven exact — NOT EXISTS inside an OR, two sharded facts in one
//! EXISTS, mismatched correlation keys across OR arms — must keep the strict refusal.

// ENV_LOCK serializes process-global `OXIDANT_DISTRIBUTED_STRICT` across async tests.
#![allow(clippy::await_holding_lock)]

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};

use oxidant_execution::driver::{run_stages, Cluster};
use oxidant_execution::flight::serve_worker;
use oxidant_execution::plan::{plan_distributed_logical, DistributedQuery};
use oxidant_loom::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::arrow::util::display::{ArrayFormatter, FormatOptions};
use oxidant_loom::Engine;

const Q10: &str = include_str!("../../../bench/tpcds/queries/q10.sql");
const Q35: &str = include_str!("../../../bench/tpcds/queries/q35.sql");
const Q69: &str = include_str!("../../../bench/tpcds/queries/q69.sql");

/// The dims-only replicated list: at SF100 only the dims fit the broadcast threshold; all
/// three sales facts are sharded.
const REPL_DIMS: [&str; 4] = [
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
];
const SHARDED_FACTS: [&str; 3] = ["store_sales", "web_sales", "catalog_sales"];

static ENV_LOCK: Mutex<()> = Mutex::new(());

static PORT: std::sync::OnceLock<AtomicU16> = std::sync::OnceLock::new();

fn unique_worker_port() -> u16 {
    // Base BELOW the Linux ephemeral source range (32768..=60999) so the harness's own
    // outbound connections can never steal a worker's port (see auto_distribute_kan55).
    PORT.get_or_init(|| AtomicU16::new(26000 + (std::process::id() as u16 % 512)))
        .fetch_add(1, Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// RecordBatch helpers / fixtures (same dataset as auto_distribute_kan55: the interesting
// customer keys span both shards of every fact)
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

fn date_dim() -> RecordBatch {
    batch(
        vec![
            i64f("d_date_sk"),
            i64f("d_year"),
            i64f("d_moy"),
            i64f("d_qoy"),
        ],
        vec![
            i64v(&[1, 2, 3, 4, 5, 6]),
            i64v(&[2002, 2002, 2001, 2001, 2002, 1999]),
            i64v(&[2, 3, 4, 6, 1, 3]),
            i64v(&[1, 1, 2, 2, 1, 1]),
        ],
    )
}

fn customer() -> RecordBatch {
    batch(
        vec![
            i64f("c_customer_sk"),
            i64f("c_current_addr_sk"),
            i64f("c_current_cdemo_sk"),
        ],
        vec![
            i64v(&[1, 2, 3, 4, 5]),
            i64v(&[1, 2, 3, 4, 5]),
            i64v(&[1, 2, 3, 4, 5]),
        ],
    )
}

fn customer_address() -> RecordBatch {
    batch(
        vec![i64f("ca_address_sk"), strf("ca_county"), strf("ca_state")],
        vec![
            i64v(&[1, 2, 3, 4, 5, 6]),
            strv(&[
                "Rush County",
                "Toole County",
                "Rush County",
                "Jefferson County",
                "Nowhere County",
                "Rush County",
            ]),
            strv(&["GA", "KY", "GA", "NM", "CA", "IL"]),
        ],
    )
}

fn customer_demographics() -> RecordBatch {
    batch(
        vec![
            i64f("cd_demo_sk"),
            strf("cd_gender"),
            strf("cd_marital_status"),
            strf("cd_education_status"),
            strf("cd_purchase_estimate"),
            strf("cd_credit_rating"),
            i64f("cd_dep_count"),
            i64f("cd_dep_employed_count"),
            i64f("cd_dep_college_count"),
        ],
        vec![
            i64v(&[1, 2, 3, 4, 5]),
            strv(&["M", "F", "M", "F", "M"]),
            strv(&["S", "M", "D", "S", "M"]),
            strv(&["HS", "BA", "MA", "HS", "BA"]),
            strv(&["500", "1000", "1500", "500", "1000"]),
            strv(&["A", "B", "C", "A", "B"]),
            i64v(&[0, 1, 2, 0, 1]),
            i64v(&[0, 1, 1, 0, 0]),
            i64v(&[1, 0, 1, 0, 1]),
        ],
    )
}

/// 2002 store customers {1,2,3} split {1,2} | {1,3} across the two shards.
fn store_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ss_sold_date_sk"),
            i64f("ss_customer_sk"),
            f64f("ss_net_paid"),
        ],
        vec![
            i64v(&[5, 2, 3, 5, 4, 3]),
            i64v(&[1, 2, 2, 3, 1, 3]),
            f64v(&[2.0, 4.0, 8.0, 6.0, 10.0, 12.0]),
        ],
    )
}

/// 2002 web customer 2 witnessed on shard 0; 2001 web customer 2 on shard 1 (Q69's anti).
fn web_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ws_sold_date_sk"),
            i64f("ws_bill_customer_sk"),
            f64f("ws_net_paid"),
        ],
        vec![
            i64v(&[5, 6, 3, 6, 6]),
            i64v(&[2, 4, 2, 5, 1]),
            f64v(&[1.0, 3.0, 2.0, 4.0, 5.0]),
        ],
    )
}

/// 2002 catalog customer 3 and 2001 catalog customer 1, both cross-shard.
fn catalog_sales() -> RecordBatch {
    batch(
        vec![
            i64f("cs_sold_date_sk"),
            i64f("cs_ship_customer_sk"),
            f64f("cs_net_profit"),
        ],
        vec![
            i64v(&[5, 1, 3, 1, 1, 1, 1]),
            i64v(&[3, 4, 1, 4, 4, 4, 4]),
            f64v(&[10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0]),
        ],
    )
}

fn register(engine: &Engine, name: &str, batches: Vec<RecordBatch>) {
    engine.register_batches(name, batches).unwrap();
}

/// Planner/ground-truth engine holding the full dataset.
fn planner_engine() -> Engine {
    let e = Engine::new();
    register(&e, "date_dim", vec![date_dim()]);
    register(&e, "customer", vec![customer()]);
    register(&e, "customer_address", vec![customer_address()]);
    register(&e, "customer_demographics", vec![customer_demographics()]);
    register(&e, "store_sales", vec![store_sales()]);
    register(&e, "web_sales", vec![web_sales()]);
    register(&e, "catalog_sales", vec![catalog_sales()]);
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

/// All three sales facts sharded row-wise across two in-process workers; dims held in full on
/// each worker (the production replicated-table invariant).
async fn two_workers() -> Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        register(&e, "date_dim", vec![date_dim()]);
        register(&e, "customer", vec![customer()]);
        register(&e, "customer_address", vec![customer_address()]);
        register(&e, "customer_demographics", vec![customer_demographics()]);
        for (name, full) in [
            ("store_sales", store_sales()),
            ("web_sales", web_sales()),
            ("catalog_sales", catalog_sales()),
        ] {
            register(&e, name, shard_rows(&full, i));
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

/// A declined shape keeps the strict refusal (never the silent whole-fact gather).
async fn assert_strict_decline(tag: &str, sql: &str) {
    let planner = planner_engine();
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("OXIDANT_DISTRIBUTED_STRICT", "1");
    let planned = plan_distributed_logical(&lp, &REPL_DIMS);
    std::env::remove_var("OXIDANT_DISTRIBUTED_STRICT");
    assert!(
        planned.is_err(),
        "{tag}: unprovable-exact shape must keep declining in strict mode, got {planned:?}"
    );
}

/// Plan strict at the dims-only classification, run on the two-worker cluster, require
/// row-for-row equality with single-node. `check_plan` pins the stage shape.
async fn assert_matches_single_node(tag: &str, sql: &str, check_plan: impl Fn(&DistributedQuery)) {
    let planner = planner_engine();
    let expected = planner.sql(sql).await.expect("single-node");
    assert!(
        expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
        "{tag}: single-node result must be non-empty (otherwise the comparison is vacuous)"
    );

    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let dq = plan_strict(&lp, &REPL_DIMS, tag);
    check_plan(&dq);

    let cluster = two_workers().await;
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
// Wiring pins: three per-fact producers + the parenthesized OR in the semi stage
// ---------------------------------------------------------------------------

#[tokio::test]
async fn q10_all_facts_sharded_plans_three_producers_with_parenthesized_or() {
    let planner = planner_engine();
    let lp = planner.logical_plan(Q10).await.expect("logical plan");
    let dq = plan_strict(&lp, &REPL_DIMS, "q10");
    for fact in SHARDED_FACTS {
        let leaves = leaf_stages_scanning(&dq, fact);
        assert_eq!(leaves.len(), 1, "one {fact} key producer: {dq:?}");
        assert_eq!(
            leaves[0].hash_key_cols,
            vec![0],
            "{fact} producer hash-shuffles by the correlation key"
        );
        assert!(
            leaves[0].sql.contains("SELECT DISTINCT"),
            "{fact} producer emits each distinct key once: {}",
            leaves[0].sql
        );
    }
    let semi = dq
        .stages
        .iter()
        .find(|s| s.sql.contains("EXISTS (SELECT 1 FROM shuffle_input"))
        .expect("semi stage: {dq:?}");
    assert!(
        semi.sql.contains(
            "(EXISTS (SELECT 1 FROM shuffle_input_1 AS k WHERE k.k0 = c.c_customer_sk) \
             OR EXISTS (SELECT 1 FROM shuffle_input_2 AS k WHERE k.k0 = c.c_customer_sk))"
        ),
        "the web/catalog disjuncts re-OR parenthesized against the co-located key streams: {}",
        semi.sql
    );
    assert!(
        !dq.stages
            .iter()
            .any(|s| s.sql.contains("__oxidant_materialize_gate")
                || s.sql.contains("__oxidant_subquery_gate")),
        "no whole-fact gather: {dq:?}"
    );
}

// ---------------------------------------------------------------------------
// STRICT e2e: Q10 / Q35 / Q69 row-for-row vs single-node at the all-facts-sharded
// classification (2 workers)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn q10_distributed_matches_single_node() {
    assert_matches_single_node("q10", Q10, |dq| {
        assert_eq!(
            dq.stages
                .iter()
                .filter(|s| s.upstream_stage_ids.is_empty())
                .count(),
            3,
            "three key producers: {dq:?}"
        );
    })
    .await;
}

#[tokio::test]
async fn q35_distributed_matches_single_node() {
    assert_matches_single_node("q35", Q35, |dq| {
        let semi = dq
            .stages
            .iter()
            .find(|s| s.sql.contains("EXISTS (SELECT 1 FROM shuffle_input"))
            .expect("semi stage: {dq:?}");
        assert!(
            semi.sql.contains(") OR EXISTS ("),
            "Q35's disjunct group stays a parenthesized OR: {}",
            semi.sql
        );
    })
    .await;
}

#[tokio::test]
async fn q69_distributed_matches_single_node() {
    assert_matches_single_node("q69", Q69, |dq| {
        for fact in SHARDED_FACTS {
            assert_eq!(
                leaf_stages_scanning(dq, fact).len(),
                1,
                "one {fact} producer: {dq:?}"
            );
        }
        let semi = dq
            .stages
            .iter()
            .find(|s| s.sql.contains("NOT EXISTS (SELECT 1 FROM shuffle_input"))
            .expect("semi stage: {dq:?}");
        assert!(
            semi.sql
                .contains("EXISTS (SELECT 1 FROM shuffle_input_0 AS k"),
            "the sharded store EXISTS gates emission: {}",
            semi.sql
        );
    })
    .await;
}

// ---------------------------------------------------------------------------
// Declines: shapes that cannot be proven exact keep the strict refusal
// ---------------------------------------------------------------------------

/// A negated arm inside the OR: `EXISTS(web) OR NOT EXISTS(catalog)` is not the non-negated
/// disjunct group (a partition-local NOT EXISTS over a sharded stream cannot be evaluated
/// exactly per partition), so the shape declines.
const NOT_EXISTS_INSIDE_OR: &str = "
SELECT cd_gender, COUNT(*) AS cnt
FROM customer c, customer_address ca, customer_demographics
WHERE c.c_current_addr_sk = ca.ca_address_sk
  AND cd_demo_sk = c.c_current_cdemo_sk
  AND (EXISTS (SELECT * FROM web_sales, date_dim
               WHERE c.c_customer_sk = ws_bill_customer_sk
                 AND ws_sold_date_sk = d_date_sk AND d_year = 2002)
       OR NOT EXISTS (SELECT * FROM catalog_sales, date_dim
                      WHERE c.c_customer_sk = cs_ship_customer_sk
                        AND cs_sold_date_sk = d_date_sk AND d_year = 2002))
GROUP BY cd_gender
";

/// One EXISTS scanning TWO sharded facts (a fact⋈fact subquery body): the per-subquery
/// single-sharded-fact rule admits at most one, so the shape declines.
const TWO_SHARDED_FACTS_IN_ONE_EXISTS: &str = "
SELECT cd_gender, COUNT(*) AS cnt
FROM customer c, customer_address ca, customer_demographics
WHERE c.c_current_addr_sk = ca.ca_address_sk
  AND cd_demo_sk = c.c_current_cdemo_sk
  AND EXISTS (SELECT * FROM store_sales, web_sales
              WHERE c.c_customer_sk = ss_customer_sk
                AND ws_bill_customer_sk = ss_customer_sk)
GROUP BY cd_gender
";

/// OR arms correlating on DIFFERENT outer keys: co-location requires one shared key stream, so
/// the mismatched disjunct group declines.
const MISMATCHED_CORRELATION_KEYS: &str = "
SELECT cd_gender, COUNT(*) AS cnt
FROM customer c, customer_address ca, customer_demographics
WHERE c.c_current_addr_sk = ca.ca_address_sk
  AND cd_demo_sk = c.c_current_cdemo_sk
  AND (EXISTS (SELECT * FROM web_sales
               WHERE c.c_customer_sk = ws_bill_customer_sk)
       OR EXISTS (SELECT * FROM catalog_sales
                  WHERE c.c_current_cdemo_sk = cs_ship_customer_sk))
GROUP BY cd_gender
";

#[tokio::test]
async fn not_exists_inside_or_declines_safely() {
    assert_strict_decline("not-exists-in-or", NOT_EXISTS_INSIDE_OR).await;
}

#[tokio::test]
async fn two_sharded_facts_in_one_exists_declines_safely() {
    assert_strict_decline("two-facts-one-exists", TWO_SHARDED_FACTS_IN_ONE_EXISTS).await;
}

#[tokio::test]
async fn mismatched_correlation_keys_decline_safely() {
    assert_strict_decline("mismatched-keys", MISMATCHED_CORRELATION_KEYS).await;
}
