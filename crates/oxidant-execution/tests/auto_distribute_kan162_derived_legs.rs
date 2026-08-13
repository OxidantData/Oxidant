//! KAN-162 (q54/q64 comma-derived legs): at the all-facts-sharded SF100 classification the
//! comma connector may place a `SubqueryAlias`-wrapped *derived* subplan that scans at least
//! one sharded table as an opaque chain leaf (q54's `my_customers` distinct-over-union,
//! q64's `cs_ui` derived aggregate). The chain planner materializes such a leg as its own
//! sub-DAG (planned recursively, `DISTINCT` rewritten to its group-by equivalent) plus one
//! export stage that re-flattens the leg's output columns to `<alias>__<col>` and re-hashes
//! by the boundary join key — the pairwise shuffle-join machinery downstream is unchanged.
//!
//! Every distributed plan must equal single-node end-to-end, in strict mode
//! (`OXIDANT_DISTRIBUTED_STRICT=1`) so no fallback can silently substitute. Decline pins —
//! never a wrong plan, always the explicit refusal: a leg whose output schema has duplicate
//! field names (the flat export could not name them apart), a leg output column that is not
//! a plain identifier (the chain's hand-built stage SQL does not quote), and an
//! all-replicated derived leaf (the prior rejection is kept byte-for-byte — the broadcast
//! paths own those). The remaining `materialize_derived_leg` guard — a leg whose recursive
//! plan outputs via a `Forward` exchange — is defensive: no SQL-reachable chain shape
//! produces it (a derived leg is admitted only when it scans a sharded table, and every
//! such subplan needs a shuffle), so it is pinned by a unit test in `plan::join_chain`
//! instead of an integration pin here.

// ENV_LOCK serializes process-global `OXIDANT_DISTRIBUTED_STRICT` across async tests.
#![allow(clippy::await_holding_lock)]

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};

use oxidant_execution::driver::{run_stages, Cluster};
use oxidant_execution::flight::serve_worker;
use oxidant_execution::plan::plan_distributed_logical;
use oxidant_loom::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::arrow::util::display::{ArrayFormatter, FormatOptions};
use oxidant_loom::Engine;

/// The q54-like classification: the three sales facts shard; dims replicate.
const REPL_Q54: [&str; 4] = ["item", "customer", "customer_address", "store"];
/// The q64-like classification: the sales + returns facts shard; dims replicate.
const REPL_Q64: [&str; 3] = ["item", "store", "customer"];

static PORT: std::sync::OnceLock<AtomicU16> = std::sync::OnceLock::new();

fn unique_worker_port() -> u16 {
    // OnceLock-seeded allocator with the base BELOW the Linux ephemeral source range
    // (32768..=60999): the harness's own outbound connections can never steal a worker's
    // port (serve_worker swallows EADDRINUSE; see auto_distribute_kan162_join_keys).
    PORT.get_or_init(|| AtomicU16::new(26000 + (std::process::id() as u16 % 512)))
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

/// Row order is load-bearing: contiguous halves split across the two workers, and every
/// chain-qualifying row sits on the OPPOSITE worker from its join partner, so the result is
/// wrong unless the substituted hash keys genuinely co-locate.
///
/// store_sales: w0 = [cust 10 / item 1 / ticket 500], w1 = [cust 30 / item 1 / ticket 501,
/// cust 20 / item 2 / ticket 502].
fn store_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ss_sold_date_sk"),
            i64f("ss_item_sk"),
            i64f("ss_customer_sk"),
            i64f("ss_store_sk"),
            i64f("ss_ticket_number"),
            f64f("ss_ext_sales_price"),
            f64f("ss_list_price"),
        ],
        vec![
            i64v(&[1, 1, 1]),
            i64v(&[1, 1, 2]),
            i64v(&[10, 30, 20]),
            i64v(&[1, 2, 1]),
            i64v(&[500, 501, 502]),
            f64v(&[50.0, 70.0, 60.0]),
            f64v(&[40.0, 60.0, 55.0]),
        ],
    )
}

/// catalog_sales: w0 = [item 2 / cust 20 / order 200], w1 = [item 1 / cust 10 / order 100].
fn catalog_sales() -> RecordBatch {
    batch(
        vec![
            i64f("cs_sold_date_sk"),
            i64f("cs_item_sk"),
            i64f("cs_bill_customer_sk"),
            i64f("cs_order_number"),
            f64f("cs_ext_list_price"),
        ],
        vec![
            i64v(&[1, 1]),
            i64v(&[2, 1]),
            i64v(&[20, 10]),
            i64v(&[200, 100]),
            f64v(&[10.0, 100.0]),
        ],
    )
}

/// web_sales: w0 = [item 1 / cust 30], w1 = [item 2 / cust 40].
fn web_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ws_sold_date_sk"),
            i64f("ws_item_sk"),
            i64f("ws_bill_customer_sk"),
        ],
        vec![i64v(&[1, 1]), i64v(&[1, 2]), i64v(&[30, 40])],
    )
}

/// store_returns: w0 = [item 2 / ticket 502], w1 = [item 1 / ticket 500] — the item-1 return
/// sits on the opposite worker from its store_sales partner.
fn store_returns() -> RecordBatch {
    batch(
        vec![i64f("sr_item_sk"), i64f("sr_ticket_number")],
        vec![i64v(&[2, 1]), i64v(&[502, 500])],
    )
}

/// catalog_returns: w0 = [item 2 / order 200 / refund 9], w1 = [item 1 / order 100 /
/// refund 10]. cs_ui keeps item 1 (sale 100 > 2*10) and drops item 2 (sale 10 < 2*9).
fn catalog_returns() -> RecordBatch {
    batch(
        vec![
            i64f("cr_item_sk"),
            i64f("cr_order_number"),
            f64f("cr_refunded_cash"),
        ],
        vec![i64v(&[2, 1]), i64v(&[200, 100]), f64v(&[9.0, 10.0])],
    )
}

fn item() -> RecordBatch {
    batch(
        vec![i64f("i_item_sk"), strf("i_category")],
        vec![i64v(&[1, 2]), strv(&["Women", "Men"])],
    )
}

fn customer() -> RecordBatch {
    batch(
        vec![i64f("c_customer_sk"), i64f("c_current_addr_sk")],
        vec![i64v(&[10, 20, 30, 40]), i64v(&[100, 200, 300, 400])],
    )
}

fn customer_address() -> RecordBatch {
    batch(
        vec![i64f("ca_address_sk"), strf("ca_county")],
        vec![
            i64v(&[100, 200, 300]),
            strv(&["CountyA", "CountyC", "CountyB"]),
        ],
    )
}

fn store() -> RecordBatch {
    batch(
        vec![i64f("s_store_sk"), strf("s_county"), strf("s_store_name")],
        vec![
            i64v(&[1, 2]),
            strv(&["CountyA", "CountyB"]),
            strv(&["StoreA", "StoreB"]),
        ],
    )
}

fn register(engine: &Engine, name: &str, batches: Vec<RecordBatch>) {
    engine.register_batches(name, batches).unwrap();
}

fn all_tables() -> [(&'static str, RecordBatch); 9] {
    [
        ("store_sales", store_sales()),
        ("catalog_sales", catalog_sales()),
        ("web_sales", web_sales()),
        ("store_returns", store_returns()),
        ("catalog_returns", catalog_returns()),
        ("item", item()),
        ("customer", customer()),
        ("customer_address", customer_address()),
        ("store", store()),
    ]
}

/// Planner/ground-truth engine holding the full dataset.
fn planner_engine() -> Engine {
    let e = Engine::new();
    for (name, full) in all_tables() {
        register(&e, name, vec![full]);
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

/// The tables named in `sharded` split row-wise across two in-process workers; every other
/// table is held in full on each worker (the production replicated-table invariant).
async fn two_workers(sharded: &[&str]) -> Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        for (name, full) in all_tables() {
            if sharded.contains(&name) {
                register(&e, name, shard_rows(&full, i));
            } else {
                register(&e, name, vec![full]);
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

/// Plan in strict mode (no fallback may substitute), then run the stages.
async fn run_distributed(
    cluster: &Cluster,
    planner: &Engine,
    sql: &str,
    replicated: &[&str],
) -> Vec<RecordBatch> {
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let dq = {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("OXIDANT_DISTRIBUTED_STRICT", "1");
        let planned = plan_distributed_logical(&lp, replicated);
        std::env::remove_var("OXIDANT_DISTRIBUTED_STRICT");
        planned.expect("strict-mode plan_distributed_logical")
    };
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

/// Sorted value rows (headers are not compared: single-node and distributed plans name
/// unaliased aggregate outputs differently — pre-existing behavior of every shape test).
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

/// Strict-mode plan as a stage dump, or `DECLINE: <err>` — the decline-pin shape.
async fn plan_strict(planner: &Engine, sql: &str, replicated: &[&str]) -> String {
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("OXIDANT_DISTRIBUTED_STRICT", "1");
    let planned = plan_distributed_logical(&lp, replicated);
    std::env::remove_var("OXIDANT_DISTRIBUTED_STRICT");
    planned
        .map(|dq| {
            dq.stages
                .iter()
                .map(|s| format!("stage {} keys={:?}: {}", s.stage_id, s.hash_key_cols, s.sql))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_else(|e| format!("DECLINE: {e}"))
}

async fn assert_distributed_matches_single_node(sql: &str, replicated: &[&str], sharded: &[&str]) {
    let planner = planner_engine();
    let expected = planner.sql(sql).await.expect("single-node");
    assert!(
        expected.iter().map(|b| b.num_rows()).sum::<usize>() > 0,
        "fixture must produce a non-empty result or the comparison is vacuous"
    );
    let cluster = two_workers(sharded).await;
    let actual = run_distributed(&cluster, &planner, sql, replicated).await;
    assert_eq!(
        rows_sorted(&actual),
        rows_sorted(&expected),
        "distributed must equal single-node"
    );
}

// --- q54-like: comma chain with a DISTINCT-over-union derived leg ------------------------

/// q54's shape reduced to the mechanism: `my_customers` is a DISTINCT over the union of two
/// sharded sales facts (catalog + web) comma-joined to replicated dims; the outer chain
/// comma-joins that derived leg to sharded store_sales and replicated dims.
const Q54_LIKE: &str = "
WITH my_customers AS (
  SELECT DISTINCT c_customer_sk, c_current_addr_sk
  FROM (
    SELECT cs_bill_customer_sk AS customer_sk, cs_item_sk AS item_sk FROM catalog_sales
    UNION ALL
    SELECT ws_bill_customer_sk AS customer_sk, ws_item_sk AS item_sk FROM web_sales
  ) cs_or_ws_sales,
  item,
  customer
  WHERE item_sk = i_item_sk
    AND i_category = 'Women'
    AND c_customer_sk = cs_or_ws_sales.customer_sk
)
SELECT c_customer_sk, sum(ss_ext_sales_price) AS revenue
FROM my_customers, store_sales, customer_address, store
WHERE c_current_addr_sk = ca_address_sk
  AND ca_county = s_county
  AND c_customer_sk = ss_customer_sk
GROUP BY c_customer_sk
";

const Q54_SHARDED: [&str; 3] = ["catalog_sales", "web_sales", "store_sales"];

#[tokio::test]
async fn q54_like_plans_with_materialized_derived_leg() {
    let planner = planner_engine();
    let plan = plan_strict(&planner, Q54_LIKE, &REPL_Q54).await;
    eprintln!("PLAN DUMP:\n{plan}");
    assert!(
        !plan.starts_with("DECLINE"),
        "q54-like must plan distributed at the all-facts-sharded classification: {plan}"
    );
    // The opaque leg materializes as its own sub-DAG + an export stage re-flattening the
    // leg's output columns to `<alias>__<col>`, hash-keyed on the boundary join key.
    assert!(
        plan.contains("my_customers__c_customer_sk AS my_customers__c_customer_sk")
            || plan.contains("c_customer_sk AS my_customers__c_customer_sk"),
        "the derived leg's export stage re-flattens its output columns: {plan}"
    );
}

#[tokio::test]
async fn q54_like_distributed_matches_single_node() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    assert_distributed_matches_single_node(Q54_LIKE, &REPL_Q54, &Q54_SHARDED).await;
}

// --- q64-like: comma chain with a derived-aggregate (HAVING) leg --------------------------

/// q64's shape reduced to the mechanism: `cs_ui` is a derived aggregate with a HAVING over a
/// sharded fact⋈fact comma join; the outer chain comma-joins it to sharded store_sales ⋈
/// store_returns and replicated dims, keyed on the leg's group-by column.
const Q64_LIKE: &str = "
WITH cs_ui AS (
  SELECT cs_item_sk,
         sum(cs_ext_list_price) AS sale,
         sum(cr_refunded_cash) AS refund
  FROM catalog_sales, catalog_returns
  WHERE cs_item_sk = cr_item_sk
    AND cs_order_number = cr_order_number
  GROUP BY cs_item_sk
  HAVING sum(cs_ext_list_price) > 2*sum(cr_refunded_cash)
)
SELECT i_item_sk, s_store_name, count(*) AS cnt, sum(ss_list_price) AS s2
FROM store_sales, store_returns, cs_ui, item, store, customer
WHERE ss_store_sk = s_store_sk
  AND ss_customer_sk = c_customer_sk
  AND ss_item_sk = i_item_sk
  AND ss_item_sk = sr_item_sk
  AND ss_ticket_number = sr_ticket_number
  AND ss_item_sk = cs_ui.cs_item_sk
GROUP BY i_item_sk, s_store_name
";

const Q64_SHARDED: [&str; 4] = [
    "store_sales",
    "store_returns",
    "catalog_sales",
    "catalog_returns",
];

#[tokio::test]
async fn q64_like_plans_with_materialized_derived_leg() {
    let planner = planner_engine();
    let plan = plan_strict(&planner, Q64_LIKE, &REPL_Q64).await;
    assert!(
        !plan.starts_with("DECLINE"),
        "q64-like must plan distributed at the all-facts-sharded classification: {plan}"
    );
    assert!(
        plan.contains("cs_item_sk AS cs_ui__cs_item_sk")
            || plan.contains("cs_ui__cs_item_sk AS cs_ui__cs_item_sk"),
        "the derived aggregate leg's export stage re-flattens its output columns: {plan}"
    );
}

#[tokio::test]
async fn q64_like_distributed_matches_single_node() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    assert_distributed_matches_single_node(Q64_LIKE, &REPL_Q64, &Q64_SHARDED).await;
}

// --- Decline pins ------------------------------------------------------------------------

/// A derived leg whose output schema carries two identically-named fields: the flat export
/// (`<alias>__<col>`) could not name them apart, so the planner must refuse, never guess.
///
/// Note: from SQL this declines at the non-identifier guard, not the duplicate-name guard —
/// DataFusion's SQL planner renames duplicate projection outputs (`cs_item_sk:1`) before the
/// chain planner ever sees the schema, and the `:` fails the identifier check first. Either
/// way the shape is refused with an explicit cause; the duplicate-name guard stays as
/// defense-in-depth for programmatically built plans.
#[tokio::test]
async fn derived_leg_with_duplicate_output_names_declines() {
    let planner = planner_engine();
    let sql = "SELECT ss_store_sk, sum(ss_ext_sales_price) FROM store_sales, \
               (SELECT c1.cs_item_sk, c2.cs_item_sk, c1.cs_order_number AS ord \
                FROM catalog_sales c1 \
                JOIN catalog_sales c2 ON c1.cs_order_number = c2.cs_order_number) dup \
               WHERE ss_ticket_number = dup.ord GROUP BY ss_store_sk";
    let plan = plan_strict(&planner, sql, &[]).await;
    assert!(
        plan.starts_with("DECLINE"),
        "duplicate leg field names must decline: {plan}"
    );
    assert!(
        plan.contains("non-identifier column") && plan.contains("cs_item_sk:1"),
        "the refusal names the DataFusion-renamed duplicate column: {plan}"
    );
}

/// A derived leg output column that is not a plain identifier (an un-aliased aggregate
/// expression): the chain's hand-built `l.<flat>` / `r.<flat>` stage SQL does not quote, so
/// the planner must refuse and ask for an alias. The leg must be genuinely opaque (an
/// aggregate — a bare filtered scan is absorbed by the chain's native leaf handling), and
/// three sharded tables force the chain path (the pairwise two-table path handles derived
/// sides natively and never builds the flat export).
#[tokio::test]
async fn derived_leg_with_non_identifier_column_declines() {
    let planner = planner_engine();
    let sql = "SELECT ss_store_sk, sum(ss_ext_sales_price) FROM store_sales, web_sales, \
               (SELECT cs_item_sk, sum(cs_ext_list_price) FROM catalog_sales \
                GROUP BY cs_item_sk) t \
               WHERE ss_item_sk = ws_item_sk AND ss_item_sk = t.cs_item_sk \
               GROUP BY ss_store_sk";
    let plan = plan_strict(&planner, sql, &[]).await;
    assert!(
        plan.starts_with("DECLINE"),
        "a non-identifier leg column must decline: {plan}"
    );
    assert!(
        plan.contains("non-identifier column"),
        "the refusal names the non-identifier cause: {plan}"
    );
}

/// A derived leaf scanning ONLY replicated tables is not admitted as an opaque leg (the
/// broadcast paths own that shape): the comma chain keeps its prior rejection. The derived
/// leaf must be an AGGREGATE — a filter-only derived leaf gets unnested by DataFusion's
/// decorrelation and planned elsewhere (here as a semi-join filter on the store_sales scan).
#[tokio::test]
async fn all_replicated_derived_leaf_keeps_prior_rejection() {
    let planner = planner_engine();
    let sql = "SELECT ss_store_sk, sum(ss_ext_sales_price) FROM store_sales, catalog_sales, \
               (SELECT i_item_sk, max(i_category) AS i_category FROM item \
                GROUP BY i_item_sk) t \
               WHERE ss_item_sk = cs_item_sk AND ss_item_sk = t.i_item_sk GROUP BY ss_store_sk";
    let plan = plan_strict(&planner, sql, &["item"]).await;
    assert!(
        plan.starts_with("DECLINE"),
        "an all-replicated derived leaf keeps the prior rejection: {plan}"
    );
}
