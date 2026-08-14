//! KAN-162 (q17/q25/q29 shuffle-projection join-key retention): a sharded shuffle boundary
//! whose left key references a *replicated* returns table folded into the stage
//! (`sr_customer_sk = cs_bill_customer_sk` with `store_returns` replicated) substitutes the
//! key through the chain's inner-join equality web (`ss_customer_sk = sr_customer_sk`), so
//! the shuffle hashes and the ON clause bind a column the co-located stream carries.
//!
//! This is the TPC-DS q17/q25/q29 shape at the all-facts-sharded SF100 classification
//! (store_sales / catalog_sales sharded, store_returns + dims replicated) — KAN-161's
//! original target queries, which regressed to STRICT refusals in v0.1.11 with
//! "join key `store_returns__sr_customer_sk` missing from shuffle projection".
//!
//! Every distributed plan must equal single-node end-to-end, in strict mode
//! (`OXIDANT_DISTRIBUTED_STRICT=1`) so no fallback can silently substitute. Decline pins:
//! a folded-dim key with NO carried equality-web peer, and the same key at a non-INNER
//! boundary, keep the historical rejection.

// ENV_LOCK serializes process-global `OXIDANT_DISTRIBUTED_STRICT` across async tests.
#![allow(clippy::await_holding_lock)]

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::catalog::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::datasource::MemTable;
use oxidant_execution::driver::{run_stages, Cluster};
use oxidant_execution::flight::serve_worker;
use oxidant_execution::plan::plan_distributed_logical;
use oxidant_loom::arrow::array::{ArrayRef, Int64Array, StringArray};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::arrow::util::display::{ArrayFormatter, FormatOptions};
use oxidant_loom::Engine;

const Q17: &str = include_str!("../../../bench/tpcds/queries/q17.sql");
const Q25: &str = include_str!("../../../bench/tpcds/queries/q25.sql");
const Q29: &str = include_str!("../../../bench/tpcds/queries/q29.sql");

/// The all-facts-sharded classification for the q17/q25/q29 chain: the two sales facts
/// shard; the returns table and every dim replicate (SF100: store_returns ~2.9 GB sits
/// below the 4 GiB auto-broadcast threshold).
const REPL: [&str; 4] = ["store_returns", "date_dim", "store", "item"];

static PORT: std::sync::OnceLock<AtomicU16> = std::sync::OnceLock::new();

fn unique_worker_port() -> u16 {
    // OnceLock-seeded allocator with the base BELOW the Linux ephemeral source range
    // (32768..=60999): the harness's own outbound connections can never steal a worker's
    // port (serve_worker swallows EADDRINUSE; the old in-range bases flaked "did not
    // bind" / "distributed run never succeeded" on loaded CI runners).
    PORT.get_or_init(|| AtomicU16::new(24000 + (std::process::id() as u16 % 512)))
        .fetch_add(1, Ordering::Relaxed)
}

/// `OXIDANT_DISTRIBUTED_STRICT` is process-global; serialize the tests that set it.
static ENV_LOCK: Mutex<()> = Mutex::new(());

fn i64f(name: &str) -> Field {
    Field::new(name, DataType::Int64, false)
}
fn strf(name: &str) -> Field {
    Field::new(name, DataType::Utf8, false)
}

fn i64v(vals: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(vals.to_vec()))
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
            strf("d_quarter_name"),
            i64f("d_moy"),
            i64f("d_year"),
        ],
        vec![
            i64v(&[1, 2, 3, 10, 11, 12, 9]),
            strv(&[
                "2001Q1", "2001Q2", "2001Q3", "1999Q3", "1999Q4", "1999Q4", "2000Q4",
            ]),
            i64v(&[4, 5, 6, 9, 10, 11, 12]),
            i64v(&[2001, 2001, 2001, 1999, 1999, 1999, 2000]),
        ],
    )
}

fn store() -> RecordBatch {
    batch(
        vec![
            i64f("s_store_sk"),
            strf("s_state"),
            strf("s_store_id"),
            strf("s_store_name"),
        ],
        vec![
            i64v(&[1, 2]),
            strv(&["TN", "CA"]),
            strv(&["S1", "S2"]),
            strv(&["Alpha", "Beta"]),
        ],
    )
}

fn item() -> RecordBatch {
    batch(
        vec![i64f("i_item_sk"), strf("i_item_id"), strf("i_item_desc")],
        vec![
            i64v(&[1, 2]),
            strv(&["I1", "I2"]),
            strv(&["Widget", "Gadget"]),
        ],
    )
}

/// Sharded fact one. Join-qualifying rows split across the contiguous halves so the
/// catalog side of each chain lives on the OTHER worker: (customer 10, item 1) and
/// (customer 60, item 2) here, (customer 20, item 2) and (customer 50, item 2) there.
/// Row D has no store_returns match; row C's date is excluded by every query's d1 filter.
fn store_sales() -> RecordBatch {
    batch(
        vec![
            i64f("ss_sold_date_sk"),
            i64f("ss_item_sk"),
            i64f("ss_customer_sk"),
            i64f("ss_store_sk"),
            i64f("ss_ticket_number"),
            i64f("ss_quantity"),
            i64f("ss_net_profit"),
        ],
        vec![
            i64v(&[1, 9, 10, 1, 1, 1, 10]),
            i64v(&[1, 1, 2, 2, 1, 1, 2]),
            i64v(&[10, 99, 50, 20, 30, 10, 60]),
            i64v(&[1, 1, 2, 1, 2, 1, 2]),
            i64v(&[100, 999, 500, 200, 300, 100, 600]),
            i64v(&[5, 1, 3, 7, 9, 6, 4]),
            i64v(&[50, 10, 30, 70, 90, 60, 40]),
        ],
    )
}

/// The replicated mid-chain table whose keys the catalog boundary references.
fn store_returns() -> RecordBatch {
    batch(
        vec![
            i64f("sr_customer_sk"),
            i64f("sr_item_sk"),
            i64f("sr_ticket_number"),
            i64f("sr_returned_date_sk"),
            i64f("sr_return_quantity"),
            i64f("sr_net_loss"),
        ],
        vec![
            i64v(&[10, 20, 50, 60, 70]),
            i64v(&[1, 2, 2, 2, 1]),
            i64v(&[100, 200, 500, 600, 700]),
            i64v(&[2, 3, 11, 12, 2]),
            i64v(&[2, 1, 2, 3, 9]),
            i64v(&[4, 5, 6, 7, 9]),
        ],
    )
}

/// Sharded fact two: each chain-qualifying row sits on the opposite worker from its
/// store_sales partner, so the result is wrong unless the substituted hash keys
/// genuinely co-locate.
fn catalog_sales() -> RecordBatch {
    batch(
        vec![
            i64f("cs_bill_customer_sk"),
            i64f("cs_item_sk"),
            i64f("cs_sold_date_sk"),
            i64f("cs_quantity"),
            i64f("cs_net_profit"),
        ],
        vec![
            i64v(&[20, 60, 10, 50, 80]),
            i64v(&[2, 2, 1, 2, 1]),
            i64v(&[3, 10, 3, 10, 3]),
            i64v(&[13, 6, 11, 5, 1]),
            i64v(&[130, 60, 110, 50, 10]),
        ],
    )
}

fn register(engine: &Engine, name: &str, batches: Vec<RecordBatch>) {
    engine.register_batches(name, batches).unwrap();
}

fn register_replicated(engine: &Engine) {
    register(engine, "date_dim", vec![date_dim()]);
    register(engine, "store", vec![store()]);
    register(engine, "item", vec![item()]);
    register(engine, "store_returns", vec![store_returns()]);
}

/// Planner/ground-truth engine holding the full dataset.
async fn tpcds_engine() -> Engine {
    let e = Engine::new();
    register_replicated(&e);
    register(&e, "store_sales", vec![store_sales()]);
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

/// Both sales facts sharded row-wise across two in-process workers; the returns table and
/// dims held in full on each worker (the production invariant for replicated tables).
async fn two_workers() -> Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        register_replicated(&e);
        register(&e, "store_sales", shard_rows(&store_sales(), i));
        register(&e, "catalog_sales", shard_rows(&catalog_sales(), i));
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
async fn run_distributed(cluster: &Cluster, planner: &Engine, sql: &str) -> Vec<RecordBatch> {
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let dq = {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("OXIDANT_DISTRIBUTED_STRICT", "1");
        let planned = plan_distributed_logical(&lp, &REPL);
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

async fn plan_strict(planner: &Engine, sql: &str) -> String {
    let lp = planner.logical_plan(sql).await.expect("logical plan");
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("OXIDANT_DISTRIBUTED_STRICT", "1");
    let planned = plan_distributed_logical(&lp, &REPL);
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

async fn assert_distributed_matches_single_node(sql: &str) {
    let planner = tpcds_engine().await;
    let expected = planner.sql(sql).await.expect("single-node");
    assert!(
        expected.iter().map(|b| b.num_rows()).sum::<usize>() > 0,
        "fixture must produce a non-empty result or the comparison is vacuous"
    );
    let cluster = two_workers().await;
    let actual = run_distributed(&cluster, &planner, sql).await;
    assert_eq!(
        rows_sorted(&actual),
        rows_sorted(&expected),
        "distributed must equal single-node"
    );
}

// --- q17 / q25 / q29: 3-fact chain with a replicated mid-chain returns table -----------

#[tokio::test]
async fn q17_plans_with_substituted_boundary_keys() {
    let planner = tpcds_engine().await;
    let plan = plan_strict(&planner, Q17).await;
    assert!(
        !plan.starts_with("DECLINE"),
        "q17 must plan distributed at the all-facts-sharded classification: {plan}"
    );
    // The catalog boundary hashes and joins on the carried store_sales keys substituted
    // through `ss_* = sr_*` — the folded store_returns columns never touch the stream.
    assert!(
        plan.contains("l.store_sales__ss_customer_sk = r.catalog_sales__cs_bill_customer_sk")
            && plan.contains("l.store_sales__ss_item_sk = r.catalog_sales__cs_item_sk"),
        "substituted shuffle ON clause: {plan}"
    );
    assert!(
        plan.contains(
            "JOIN store_returns AS store_returns ON l.store_sales__ss_customer_sk = store_returns.sr_customer_sk"
        ),
        "the replicated returns table folds into the boundary stage: {plan}"
    );
}

/// Register `batches` as `glue.tpcds_sf100.<name>` (MemoryCatalog) — models Glue view
/// expansion's fully-qualified TableScan without AWS.
fn register_glue_qualified(engine: &Engine, name: &str, batches: Vec<RecordBatch>) {
    let schema = batches[0].schema();
    let table = MemTable::try_new(schema, vec![batches]).expect("MemTable");
    if engine.ctx().catalog("glue").is_none() {
        engine
            .ctx()
            .register_catalog("glue", Arc::new(MemoryCatalogProvider::new()));
    }
    let catalog = engine.ctx().catalog("glue").expect("glue catalog");
    if catalog.schema("tpcds_sf100").is_none() {
        catalog
            .register_schema("tpcds_sf100", Arc::new(MemorySchemaProvider::new()))
            .unwrap();
    }
    catalog
        .schema("tpcds_sf100")
        .unwrap()
        .register_table(name.to_string(), Arc::new(table))
        .unwrap();
}

/// Glue SF100 shape: views expand to `TableScan: glue.tpcds_sf100.*`. Dim folds and leaf
/// stages must emit that qualification — bare `date_dim` resolves to
/// `spark_catalog.default.date_dim` on workers (SF100 Q17 runtime failure).
async fn tpcds_engine_catalog_qualified() -> Engine {
    let e = Engine::new();
    for (name, batch) in [
        ("date_dim", date_dim()),
        ("store", store()),
        ("item", item()),
        ("store_returns", store_returns()),
        ("store_sales", store_sales()),
        ("catalog_sales", catalog_sales()),
    ] {
        register_glue_qualified(&e, name, vec![batch]);
        e.sql(&format!(
            "CREATE OR REPLACE VIEW {name} AS SELECT * FROM glue.tpcds_sf100.{name}"
        ))
        .await
        .unwrap_or_else(|err| panic!("alias {name}: {err}"));
    }
    e
}

#[tokio::test]
async fn q17_catalog_qualified_scans_preserve_qualification_in_stage_sql() {
    let planner = tpcds_engine_catalog_qualified().await;
    let plan = plan_strict(&planner, Q17).await;
    assert!(
        !plan.starts_with("DECLINE"),
        "catalog-qualified q17 must plan: {plan}"
    );
    assert!(
        !plan.contains("spark_catalog.default"),
        "stage SQL must never emit spark_catalog.default: {plan}"
    );
    // Leaf scans + semi-filter subqueries + dim folds all carry the scan's TableReference.
    for relation in [
        "glue.tpcds_sf100.store_sales",
        "glue.tpcds_sf100.catalog_sales",
        "glue.tpcds_sf100.store_returns",
        "glue.tpcds_sf100.date_dim",
        "glue.tpcds_sf100.store",
        "glue.tpcds_sf100.item",
    ] {
        assert!(
            plan.contains(relation),
            "stage SQL must reference qualified `{relation}`: {plan}"
        );
    }
    // Dim-fold emission site (the SF100 failure): bare JOIN date_dim must not appear.
    assert!(
        plan.contains("JOIN glue.tpcds_sf100.date_dim AS")
            && plan.contains("JOIN glue.tpcds_sf100.store_returns AS")
            && plan.contains("JOIN glue.tpcds_sf100.store AS")
            && plan.contains("JOIN glue.tpcds_sf100.item AS"),
        "emit_dim_fold must use table_sql, not bare table: {plan}"
    );
    assert!(
        !plan.contains("JOIN date_dim AS")
            && !plan.contains("JOIN store_returns AS")
            && !plan.contains("JOIN store AS")
            && !plan.contains("JOIN item AS"),
        "bare dim-fold JOINs regress catalog qualification: {plan}"
    );
}

#[tokio::test]
async fn q17_distributed_matches_single_node() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    assert_distributed_matches_single_node(Q17).await;
}

#[tokio::test]
async fn q25_distributed_matches_single_node() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    assert_distributed_matches_single_node(Q25).await;
}

#[tokio::test]
async fn q29_distributed_matches_single_node() {
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "16");
    assert_distributed_matches_single_node(Q29).await;
}

// --- Decline pins -----------------------------------------------------------------------

/// A folded-dim boundary key with NO equality-web peer on a sharded leaf keeps the
/// historical rejection: `sr_return_quantity = cs_quantity` links catalog_sales to no
/// carried column, so there is nothing exact to substitute.
#[tokio::test]
async fn folded_dim_key_without_carried_peer_still_declines() {
    let planner = tpcds_engine().await;
    let sql =
        "SELECT ss_store_sk, sum(ss_quantity) FROM store_sales, store_returns, catalog_sales \
               WHERE ss_ticket_number = sr_ticket_number AND sr_return_quantity = cs_quantity \
               GROUP BY ss_store_sk";
    let plan = plan_strict(&planner, sql).await;
    assert!(
        plan.starts_with("DECLINE")
            && plan.contains("store_returns__sr_return_quantity")
            && plan.contains("missing from shuffle projection"),
        "no carried peer → historical rejection: {plan}"
    );
}

/// Substitution is exact only for INNER boundaries (transitive conjunctive equality); a
/// LEFT boundary whose key references the folded dim decides null-extension and must keep
/// the historical rejection.
#[tokio::test]
async fn outer_boundary_folded_dim_key_still_declines() {
    let planner = tpcds_engine().await;
    let sql = "SELECT ss_store_sk, sum(ss_quantity), sum(cs_quantity) FROM store_sales \
               JOIN store_returns ON ss_ticket_number = sr_ticket_number \
               LEFT JOIN catalog_sales ON sr_customer_sk = cs_bill_customer_sk \
               GROUP BY ss_store_sk";
    let plan = plan_strict(&planner, sql).await;
    assert!(
        plan.starts_with("DECLINE")
            && plan.contains("store_returns__sr_customer_sk")
            && plan.contains("missing from shuffle projection"),
        "outer boundary keys never substitute: {plan}"
    );
}
