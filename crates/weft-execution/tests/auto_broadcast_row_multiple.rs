//! B2: row-aware auto-broadcast classification (`WEFT_REPLICATE_MAX_ROW_MULTIPLE`, default
//! OFF). Byte-only classification replicates every table under the byte cap that is smaller
//! than the query's largest table — TPC-DS SF10's `inventory` (117M rows in ~0.5 GB parquet)
//! replicates while `catalog_sales` (14.4M rows, ~1+ GB) shards, so every worker full-scans
//! 8× the sharded table's row count. When the catalog carries row counts (`numRows` table
//! properties, read for free on the same `load_table` the sizing walk performs), the env-gated
//! rule keeps a byte-eligible candidate sharded once its rows exceed multiple × the largest
//! table's rows.
//!
//! The classification tests below drive `resolve_replicated_tables` end-to-end through a stub
//! catalog carrying `numRows` properties. The planner-shape tests show what the planner does
//! with the resulting 2-sharded classification: a fact-first same-key chain plans via the
//! shuffle-join chain, while TPC-DS Q37's dim-leftmost comma chain rejects 2-sharded and
//! falls back gracefully (Forward) — the reason the env defaults OFF.

// ENV_LOCK serializes process-global `WEFT_REPLICATE_MAX_ROW_MULTIPLE` /
// `WEFT_REPLICATED_TABLES` across async tests.
#![allow(clippy::await_holding_lock)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use weft_execution::driver::ExchangeMode;
use weft_execution::plan::{plan_distributed, plan_distributed_logical, resolve_replicated_tables};
use weft_loom::arrow::array::{ArrayRef, Date32Array, Int64Array, StringArray};
use weft_loom::arrow::datatypes::{DataType, Field, Schema};
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::Engine;

const Q37: &str = include_str!("../../../bench/tpcds/queries/q37.sql");

/// `WEFT_REPLICATE_MAX_ROW_MULTIPLE` / `WEFT_REPLICATED_TABLES` are process-global; serialize
/// the tests that mutate them.
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

fn batch(fields: Vec<Field>, cols: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
}

/// Simplified `catalog_sales`: the byte-anchor fact (large bytes, fewer rows).
fn catalog_sales() -> RecordBatch {
    batch(
        vec![
            i64f("cs_item_sk"),
            i64f("cs_sold_date_sk"),
            i64f("cs_quantity"),
        ],
        vec![
            i64v(&[1, 2, 3, 1]),
            i64v(&[100, 100, 101, 102]),
            i64v(&[10, 20, 30, 40]),
        ],
    )
}

/// Simplified `inventory`: many rows, small bytes (the row-rule candidate).
fn inventory() -> RecordBatch {
    batch(
        vec![
            i64f("inv_item_sk"),
            i64f("inv_date_sk"),
            i64f("inv_quantity_on_hand"),
        ],
        vec![
            i64v(&[1, 2, 3, 4]),
            i64v(&[100, 100, 101, 102]),
            i64v(&[150, 250, 350, 450]),
        ],
    )
}

/// Simplified `item` dim, covering the columns the real TPC-DS Q37 references.
fn item() -> RecordBatch {
    batch(
        vec![
            i64f("i_item_sk"),
            i64f("i_item_id"),
            strf("i_item_desc"),
            i64f("i_current_price"),
            i64f("i_manufact_id"),
        ],
        vec![
            i64v(&[1, 2, 3, 4]),
            i64v(&[11, 12, 13, 14]),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            i64v(&[70, 80, 90, 100]),
            i64v(&[677, 940, 694, 808]),
        ],
    )
}

/// Simplified `date_dim`; `d_date` is Date32 so Q37's `cast(... AS date)` bounds type-check.
fn date_dim() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            i64f("d_date_sk"),
            Field::new("d_date", DataType::Date32, false),
        ])),
        vec![
            i64v(&[100, 101, 102]),
            Arc::new(Date32Array::from(vec![11_000, 11_001, 11_002])),
        ],
    )
    .unwrap()
}

/// Planner/ground-truth engine holding the full dataset in memory.
async fn tpcds_engine() -> Engine {
    let e = Engine::new();
    e.register_batches("catalog_sales", vec![catalog_sales()])
        .unwrap();
    e.register_batches("inventory", vec![inventory()]).unwrap();
    e.register_batches("item", vec![item()]).unwrap();
    e.register_batches("date_dim", vec![date_dim()]).unwrap();
    e
}

// ---------------------------------------------------------------------------
// Classification through `resolve_replicated_tables` with catalog row counts
// ---------------------------------------------------------------------------

/// Stub catalog whose tables carry an optional `numRows` property (what Glue/Hive expose
/// after `ANALYZE TABLE`). Locations point at local parquet directories so the byte-sizing
/// walk lists real files.
struct StatsCatalog {
    tables: HashMap<String, (String, Option<u64>)>,
}

#[async_trait::async_trait]
impl weft_catalog::CatalogProvider for StatsCatalog {
    fn name(&self) -> &str {
        "testcat"
    }

    async fn list_namespaces(&self, parent: &[String]) -> weft_catalog::Result<Vec<Vec<String>>> {
        if parent.is_empty() {
            Ok(vec![vec!["default".to_string()]])
        } else {
            Ok(vec![])
        }
    }

    async fn list_tables(&self, _namespace: &[String]) -> weft_catalog::Result<Vec<String>> {
        Ok(self.tables.keys().cloned().collect())
    }

    async fn load_table(
        &self,
        _namespace: &[String],
        table: &str,
    ) -> weft_catalog::Result<weft_catalog::TableMetadata> {
        let (location, num_rows) = self
            .tables
            .get(table)
            .ok_or_else(|| weft_catalog::Error::Plan(format!("no such table `{table}`")))?;
        let mut properties = HashMap::new();
        if let Some(rows) = num_rows {
            properties.insert("numRows".to_string(), rows.to_string());
        }
        Ok(
            weft_catalog::TableMetadata::new(table, location, weft_catalog::TableFormat::Parquet)
                .with_properties(properties),
        )
    }
}

fn write_parquet(path: &std::path::Path, batch: &RecordBatch) {
    std::fs::create_dir_all(path).unwrap();
    let file = std::fs::File::create(path.join("part-0.parquet")).unwrap();
    let mut writer =
        datafusion::parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

/// `catalog_sales` must be the byte anchor while `inventory` has 10× its rows: the fact gets
/// a wide padded column (bytes ≫), inventory stays narrow with more rows.
fn catalog_fixtures(with_stats: bool) -> (tempfile::TempDir, StatsCatalog) {
    let dir = tempfile::tempdir().unwrap();

    let wide = catalog_sales();
    let pad = Arc::new(StringArray::from(
        wide.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .map(|_| "x".repeat(600))
            .collect::<Vec<_>>(),
    ));
    let mut fields = wide.schema().fields().to_vec();
    fields.push(Arc::new(strf("cs_comment")));
    let mut cols = wide.columns().to_vec();
    cols.push(pad);
    let wide =
        RecordBatch::try_new(Arc::new(Schema::new(fields)), [cols, vec![]].concat()).unwrap();
    let cs_rows = wide.num_rows() as u64;
    // Inventory carries 16× the fact's rows (the parquet stays smaller: narrow int columns).
    let inv_rows_target = 64_usize;
    let inv = RecordBatch::try_new(
        inventory().schema(),
        (0..3)
            .map(|c| {
                inventory()
                    .column(c)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .cycle()
                    .take(inv_rows_target)
                    .collect::<Int64Array>()
            })
            .map(|a| Arc::new(a) as ArrayRef)
            .collect(),
    )
    .unwrap();
    let inv_rows = inv.num_rows() as u64;
    let it = item();
    let it_rows = it.num_rows() as u64;

    let cs_dir = dir.path().join("catalog_sales");
    let inv_dir = dir.path().join("inventory");
    let item_dir = dir.path().join("item");
    write_parquet(&cs_dir, &wide);
    write_parquet(&inv_dir, &inv);
    write_parquet(&item_dir, &it);
    // Sanity for the byte/row split the scenario needs.
    let cs_bytes = std::fs::metadata(cs_dir.join("part-0.parquet"))
        .unwrap()
        .len();
    let inv_bytes = std::fs::metadata(inv_dir.join("part-0.parquet"))
        .unwrap()
        .len();
    assert!(
        cs_bytes > inv_bytes,
        "catalog_sales must be the byte anchor: {cs_bytes} vs {inv_bytes}"
    );
    assert!(inv_rows > 4 * cs_rows, "scenario needs >4× rows");

    let stat = |rows: u64| with_stats.then_some(rows);
    let mut tables = HashMap::new();
    tables.insert(
        "catalog_sales".to_string(),
        (cs_dir.to_str().unwrap().to_string(), stat(cs_rows)),
    );
    tables.insert(
        "inventory".to_string(),
        (inv_dir.to_str().unwrap().to_string(), stat(inv_rows)),
    );
    tables.insert(
        "item".to_string(),
        (item_dir.to_str().unwrap().to_string(), stat(it_rows)),
    );
    (dir, StatsCatalog { tables })
}

const STAR_SQL: &str = "SELECT i.i_item_desc AS d, SUM(cs.cs_quantity) AS q \
     FROM testcat.default.catalog_sales cs \
     JOIN testcat.default.inventory inv ON cs.cs_item_sk = inv.inv_item_sk \
     JOIN testcat.default.item i ON cs.cs_item_sk = i.i_item_sk \
     GROUP BY i.i_item_desc";

async fn resolve_with_catalog(catalog: StatsCatalog) -> Vec<String> {
    let engine = Engine::new();
    engine.register_catalog("testcat", Arc::new(catalog));
    let lp = engine.logical_plan(STAR_SQL).await.unwrap();
    resolve_replicated_tables(&engine, &lp).await
}

/// Default (env unset): byte-only legacy classification — byte-small `inventory` replicates
/// even though it carries 10× the anchor's rows.
#[tokio::test]
async fn legacy_classification_without_row_multiple_env() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::remove_var("WEFT_REPLICATE_MAX_ROW_MULTIPLE");
    std::env::remove_var("WEFT_REPLICATED_TABLES");

    let (_dir, catalog) = catalog_fixtures(true);
    let replicated = resolve_with_catalog(catalog).await;
    assert!(
        replicated.iter().any(|t| t == "inventory"),
        "{replicated:?}"
    );
    assert!(replicated.iter().any(|t| t == "item"), "{replicated:?}");
    assert!(
        !replicated.iter().any(|t| t == "catalog_sales"),
        "{replicated:?}"
    );
}

/// Env on (4.0): `inventory`'s 10× row count excludes it from the replicate set even though
/// its bytes are under the cap; the tiny `item` dim still replicates.
#[tokio::test]
async fn row_multiple_excludes_row_heavy_table_through_resolve() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::remove_var("WEFT_REPLICATED_TABLES");
    std::env::set_var("WEFT_REPLICATE_MAX_ROW_MULTIPLE", "4.0");

    let (_dir, catalog) = catalog_fixtures(true);
    let replicated = resolve_with_catalog(catalog).await;
    std::env::remove_var("WEFT_REPLICATE_MAX_ROW_MULTIPLE");
    assert!(replicated.iter().any(|t| t == "item"), "{replicated:?}");
    assert!(
        !replicated.iter().any(|t| t == "inventory"),
        "10× the anchor's rows must stay sharded: {replicated:?}"
    );
    assert!(
        !replicated.iter().any(|t| t == "catalog_sales"),
        "{replicated:?}"
    );
}

/// The `WEFT_REPLICATED_TABLES` force-include still wins over the row-multiple exclusion.
#[tokio::test]
async fn row_multiple_force_include_override_wins() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("WEFT_REPLICATE_MAX_ROW_MULTIPLE", "4.0");
    std::env::set_var("WEFT_REPLICATED_TABLES", "inventory");

    let (_dir, catalog) = catalog_fixtures(true);
    let replicated = resolve_with_catalog(catalog).await;
    std::env::remove_var("WEFT_REPLICATE_MAX_ROW_MULTIPLE");
    std::env::remove_var("WEFT_REPLICATED_TABLES");
    assert!(
        replicated.iter().any(|t| t == "inventory"),
        "{replicated:?}"
    );
    assert!(replicated.iter().any(|t| t == "item"), "{replicated:?}");
}

/// Row counts unavailable (metastore without statistics) ⇒ byte-for-byte legacy behavior
/// even with the env set.
#[tokio::test]
async fn row_multiple_without_catalog_stats_is_legacy() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::remove_var("WEFT_REPLICATED_TABLES");
    std::env::set_var("WEFT_REPLICATE_MAX_ROW_MULTIPLE", "4.0");

    let (_dir, catalog) = catalog_fixtures(false);
    let replicated = resolve_with_catalog(catalog).await;
    std::env::remove_var("WEFT_REPLICATE_MAX_ROW_MULTIPLE");
    assert!(
        replicated.iter().any(|t| t == "inventory"),
        "{replicated:?}"
    );
    assert!(replicated.iter().any(|t| t == "item"), "{replicated:?}");
    assert!(
        !replicated.iter().any(|t| t == "catalog_sales"),
        "{replicated:?}"
    );
}

// ---------------------------------------------------------------------------
// Planner shapes under the 2-sharded classification
// ---------------------------------------------------------------------------

fn assert_has_shuffle_stage(dq: &weft_execution::plan::DistributedQuery) {
    assert!(dq.stages.len() >= 2, "expected a multi-stage plan: {dq:?}");
    assert!(
        dq.stages.iter().any(|s| !s.hash_key_cols.is_empty()),
        "expected at least one hash-shuffled stage: {:?}",
        dq.stages
            .iter()
            .map(|s| (s.stage_id, &s.hash_key_cols))
            .collect::<Vec<_>>()
    );
}

/// Two sharded tables joined on the same key (`cs_item_sk = inv_item_sk`) plan as a
/// co-located shuffle join — the fast two-table path of the join-chain planner.
#[tokio::test]
async fn two_sharded_same_key_equijoin_plans_via_shuffle_chain() {
    let engine = tpcds_engine().await;
    let sql = "SELECT cs.cs_item_sk AS k, SUM(cs.cs_quantity) AS q \
               FROM catalog_sales cs \
               JOIN inventory inv ON cs.cs_item_sk = inv.inv_item_sk \
               GROUP BY cs.cs_item_sk";
    let lp = engine.logical_plan(sql).await.unwrap();
    // The row rule's output for this pair: nothing replicates — both stay sharded.
    let dq = plan_distributed_logical(&lp, &[]).expect("same-key 2-sharded join should plan");
    assert_has_shuffle_stage(&dq);
}

/// The general join-chain builder: replicated dims interleaved between the two sharded
/// tables fold into the shuffle-join stage as local broadcast joins (fact-first chain,
/// sharded table last — replicated dims may only sit in the middle).
#[tokio::test]
async fn two_sharded_dims_interleaved_plans_via_join_chain() {
    let engine = tpcds_engine().await;
    let sql = "SELECT i.i_item_id AS k, SUM(inv.inv_quantity_on_hand) AS q \
               FROM catalog_sales cs \
               JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk \
               JOIN item i ON cs.cs_item_sk = i.i_item_sk \
               JOIN inventory inv ON cs.cs_item_sk = inv.inv_item_sk \
               GROUP BY i.i_item_id";
    let lp = engine.logical_plan(sql).await.unwrap();
    let replicated = ["item", "date_dim"];
    let dq = plan_distributed_logical(&lp, &replicated)
        .expect("2-sharded chain with interleaved dims should plan");
    assert_has_shuffle_stage(&dq);
}

/// TPC-DS Q37 as written (`FROM item, inventory, date_dim, catalog_sales`): the comma-join
/// connector deliberately roots the chain at the ORIGINAL leftmost leaf — the replicated
/// `item` dim — and the shuffle-join-chain planner requires a sharded leftmost, so the
/// 2-sharded classification is REJECTED by `plan_distributed_logical` …
#[tokio::test]
async fn q37_shape_two_sharded_rejected_by_logical_planner() {
    let engine = tpcds_engine().await;
    let lp = engine.logical_plan(Q37).await.unwrap();
    let replicated = ["item", "date_dim"];
    let err = plan_distributed_logical(&lp, &replicated)
        .expect_err("dim-leftmost 2-sharded chain must be rejected");
    assert!(
        err.to_string().contains("sharded leftmost"),
        "unexpected rejection: {err}"
    );
}

/// … and the SQL entry point falls back gracefully: one Forward stage on a single worker
/// instead of an error. This fallback (or, under `WEFT_DISTRIBUTED_STRICT=1`, a hard error)
/// is why `WEFT_REPLICATE_MAX_ROW_MULTIPLE` defaults OFF: the same query plans distributed
/// under today's byte-only classification (next test).
#[tokio::test]
async fn q37_shape_two_sharded_falls_back_to_forward() {
    let engine = tpcds_engine().await;
    let replicated = ["item", "date_dim"];
    let dq = plan_distributed(&engine, Q37, &replicated)
        .await
        .expect("forward fallback must still produce a plan");
    assert_eq!(dq.stages.len(), 1, "{dq:?}");
    assert_eq!(dq.stages[0].exchange, ExchangeMode::Forward, "{dq:?}");
}

/// Same query, today's byte-only classification (inventory replicated): plans distributed
/// via the 1-sharded broadcast path — evidence that the fallback above is a consequence of
/// the new 2-sharded classification, not of the query itself.
#[tokio::test]
async fn q37_shape_legacy_classification_plans_distributed() {
    let engine = tpcds_engine().await;
    let lp = engine.logical_plan(Q37).await.unwrap();
    let replicated = ["item", "inventory", "date_dim"];
    let dq = plan_distributed_logical(&lp, &replicated)
        .expect("legacy 1-sharded classification must plan");
    assert_has_shuffle_stage(&dq);
}
