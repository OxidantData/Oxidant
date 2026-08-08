//! B2: row-aware auto-broadcast classification (`OXIDANT_REPLICATE_MAX_ROW_MULTIPLE`, default
//! **ON at 4.0**). Byte-only classification replicates every table under the byte cap that is
//! smaller than the query's largest table — TPC-DS SF10's `inventory` (117M rows in ~0.5 GB
//! parquet) replicates while `catalog_sales` (14.4M rows, ~1+ GB) shards, so every worker
//! full-scans 8× the sharded table's row count. When the catalog carries row counts
//! (`numRows` table properties, read for free on the same `load_table` the sizing walk
//! performs), the rule keeps a byte-eligible candidate sharded once its rows exceed
//! multiple × the largest table's rows. Setting the env to `0` restores byte-only
//! classification (the escape hatch).
//!
//! The classification tests below drive `resolve_replicated_tables` end-to-end through a stub
//! catalog carrying `numRows` properties. The planner-shape tests show what the planner does
//! with the resulting 2-sharded classification: a fact-first same-key chain plans via the
//! shuffle-join chain, and TPC-DS Q37's dim-leftmost comma chain (`FROM item, inventory,
//! date_dim, catalog_sales`) is **re-rooted** at the sharded `inventory` — the trailing
//! `catalog_sales` join, written against the folded `item` dim, keys on `inv_item_sk` through
//! the query's equality web — so the 2-sharded classification plans distributed instead of
//! falling back to single-node execution (the reason the rule originally shipped gated off).

// ENV_LOCK serializes process-global `OXIDANT_REPLICATE_MAX_ROW_MULTIPLE` /
// `OXIDANT_REPLICATED_TABLES` across async tests.
#![allow(clippy::await_holding_lock)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use oxidant_execution::driver::ExchangeMode;
use oxidant_execution::plan::{
    plan_distributed, plan_distributed_logical, resolve_replicated_tables,
};
use oxidant_loom::arrow::array::{ArrayRef, Date32Array, Int64Array, StringArray};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;

const Q37: &str = include_str!("../../../bench/tpcds/queries/q37.sql");
const Q82: &str = include_str!("../../../bench/tpcds/queries/q82.sql");

/// `OXIDANT_REPLICATE_MAX_ROW_MULTIPLE` / `OXIDANT_REPLICATED_TABLES` are process-global; serialize
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

/// Simplified `catalog_sales`: the byte-anchor fact (large bytes, fewer rows). The trailing
/// columns cover what TPC-DS Q72 references beyond Q37's needs.
fn catalog_sales() -> RecordBatch {
    batch(
        vec![
            i64f("cs_item_sk"),
            i64f("cs_sold_date_sk"),
            i64f("cs_quantity"),
            i64f("cs_ship_date_sk"),
            i64f("cs_bill_cdemo_sk"),
            i64f("cs_bill_hdemo_sk"),
            i64f("cs_promo_sk"),
            i64f("cs_order_number"),
        ],
        vec![
            i64v(&[1, 2, 3, 1]),
            i64v(&[100, 100, 101, 102]),
            i64v(&[10, 20, 30, 40]),
            i64v(&[101, 101, 101, 102]),
            i64v(&[10, 10, 20, 10]),
            i64v(&[30, 30, 30, 40]),
            i64v(&[60, 61, 60, 60]),
            i64v(&[1000, 1001, 1002, 1003]),
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
            i64f("inv_warehouse_sk"),
        ],
        vec![
            i64v(&[1, 2, 3, 4]),
            i64v(&[100, 100, 101, 102]),
            i64v(&[150, 250, 350, 450]),
            i64v(&[50, 50, 50, 50]),
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
/// `d_week_seq` / `d_year` cover Q72's dim filters.
fn date_dim() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            i64f("d_date_sk"),
            Field::new("d_date", DataType::Date32, false),
            i64f("d_week_seq"),
            i64f("d_year"),
        ])),
        vec![
            i64v(&[100, 101, 102]),
            Arc::new(Date32Array::from(vec![11_000, 11_001, 11_002])),
            i64v(&[7, 7, 8]),
            i64v(&[1999, 1999, 1999]),
        ],
    )
    .unwrap()
}

/// Simplified `warehouse` dim (Q72 groups by `w_warehouse_name`).
fn warehouse() -> RecordBatch {
    batch(
        vec![i64f("w_warehouse_sk"), strf("w_warehouse_name")],
        vec![
            i64v(&[50, 51]),
            Arc::new(StringArray::from(vec!["w1", "w2"])),
        ],
    )
}

/// Simplified `customer_demographics` dim (Q72 filters `cd_marital_status = 'D'`).
fn customer_demographics() -> RecordBatch {
    batch(
        vec![i64f("cd_demo_sk"), strf("cd_marital_status")],
        vec![i64v(&[10, 20]), Arc::new(StringArray::from(vec!["D", "S"]))],
    )
}

/// Simplified `household_demographics` dim (Q72 filters `hd_buy_potential = '>10000'`).
fn household_demographics() -> RecordBatch {
    batch(
        vec![i64f("hd_demo_sk"), strf("hd_buy_potential")],
        vec![
            i64v(&[30, 40]),
            Arc::new(StringArray::from(vec![">10000", "0-500"])),
        ],
    )
}

/// Simplified `promotion` dim (Q72 LEFT JOINs it: 60 matches, everything else null-extends).
fn promotion() -> RecordBatch {
    batch(vec![i64f("p_promo_sk")], vec![i64v(&[60, 62])])
}

/// Simplified `catalog_returns` fact-less RIGHT side (Q72 LEFT JOINs it on item+order).
fn catalog_returns() -> RecordBatch {
    batch(
        vec![i64f("cr_item_sk"), i64f("cr_order_number")],
        vec![i64v(&[1, 2]), i64v(&[1000, 2000])],
    )
}

/// Planner/ground-truth engine holding the full dataset in memory.
async fn tpcds_engine() -> Engine {
    let e = Engine::new();
    e.register_batches("catalog_sales", vec![catalog_sales()])
        .unwrap();
    e.register_batches("inventory", vec![inventory()]).unwrap();
    e.register_batches("item", vec![item()]).unwrap();
    e.register_batches("date_dim", vec![date_dim()]).unwrap();
    e.register_batches(
        "store_sales",
        vec![batch(vec![i64f("ss_item_sk")], vec![i64v(&[1, 2, 3, 1])])],
    )
    .unwrap();
    e.register_batches("warehouse", vec![warehouse()]).unwrap();
    e.register_batches("customer_demographics", vec![customer_demographics()])
        .unwrap();
    e.register_batches("household_demographics", vec![household_demographics()])
        .unwrap();
    e.register_batches("promotion", vec![promotion()]).unwrap();
    e.register_batches("catalog_returns", vec![catalog_returns()])
        .unwrap();
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
impl oxidant_catalog::CatalogProvider for StatsCatalog {
    fn name(&self) -> &str {
        "testcat"
    }

    async fn list_namespaces(
        &self,
        parent: &[String],
    ) -> oxidant_catalog::Result<Vec<Vec<String>>> {
        if parent.is_empty() {
            Ok(vec![vec!["default".to_string()]])
        } else {
            Ok(vec![])
        }
    }

    async fn list_tables(&self, _namespace: &[String]) -> oxidant_catalog::Result<Vec<String>> {
        Ok(self.tables.keys().cloned().collect())
    }

    async fn load_table(
        &self,
        _namespace: &[String],
        table: &str,
    ) -> oxidant_catalog::Result<oxidant_catalog::TableMetadata> {
        let (location, num_rows) = self
            .tables
            .get(table)
            .ok_or_else(|| oxidant_catalog::Error::Plan(format!("no such table `{table}`")))?;
        let mut properties = HashMap::new();
        if let Some(rows) = num_rows {
            properties.insert("numRows".to_string(), rows.to_string());
        }
        Ok(oxidant_catalog::TableMetadata::new(
            table,
            location,
            oxidant_catalog::TableFormat::Parquet,
        )
        .with_properties(properties))
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
        (0..inventory().num_columns())
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

/// Escape hatch (`OXIDANT_REPLICATE_MAX_ROW_MULTIPLE=0`): byte-only legacy classification —
/// byte-small `inventory` replicates even though it carries 16× the anchor's rows.
#[tokio::test]
async fn legacy_classification_when_row_multiple_disabled() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("OXIDANT_REPLICATE_MAX_ROW_MULTIPLE", "0");
    std::env::remove_var("OXIDANT_REPLICATED_TABLES");

    let (_dir, catalog) = catalog_fixtures(true);
    let replicated = resolve_with_catalog(catalog).await;
    std::env::remove_var("OXIDANT_REPLICATE_MAX_ROW_MULTIPLE");
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

/// Default (env unset): the row-aware rule is ON at 4.0 — `inventory`'s 16× row count keeps
/// it sharded even though its bytes are under the cap; the tiny `item` dim still replicates.
#[tokio::test]
async fn row_multiple_default_on_excludes_row_heavy_table() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::remove_var("OXIDANT_REPLICATE_MAX_ROW_MULTIPLE");
    std::env::remove_var("OXIDANT_REPLICATED_TABLES");

    let (_dir, catalog) = catalog_fixtures(true);
    let replicated = resolve_with_catalog(catalog).await;
    assert!(replicated.iter().any(|t| t == "item"), "{replicated:?}");
    assert!(
        !replicated.iter().any(|t| t == "inventory"),
        "16× the anchor's rows must stay sharded by default: {replicated:?}"
    );
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
    std::env::remove_var("OXIDANT_REPLICATED_TABLES");
    std::env::set_var("OXIDANT_REPLICATE_MAX_ROW_MULTIPLE", "4.0");

    let (_dir, catalog) = catalog_fixtures(true);
    let replicated = resolve_with_catalog(catalog).await;
    std::env::remove_var("OXIDANT_REPLICATE_MAX_ROW_MULTIPLE");
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

/// The `OXIDANT_REPLICATED_TABLES` force-include still wins over the row-multiple exclusion.
#[tokio::test]
async fn row_multiple_force_include_override_wins() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("OXIDANT_REPLICATE_MAX_ROW_MULTIPLE", "4.0");
    std::env::set_var("OXIDANT_REPLICATED_TABLES", "inventory");

    let (_dir, catalog) = catalog_fixtures(true);
    let replicated = resolve_with_catalog(catalog).await;
    std::env::remove_var("OXIDANT_REPLICATE_MAX_ROW_MULTIPLE");
    std::env::remove_var("OXIDANT_REPLICATED_TABLES");
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
    std::env::remove_var("OXIDANT_REPLICATED_TABLES");
    std::env::set_var("OXIDANT_REPLICATE_MAX_ROW_MULTIPLE", "4.0");

    let (_dir, catalog) = catalog_fixtures(false);
    let replicated = resolve_with_catalog(catalog).await;
    std::env::remove_var("OXIDANT_REPLICATE_MAX_ROW_MULTIPLE");
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
// KAN-81 walk-level regression: the sizing walk must reach the catalog with the
// scan's FULL TableReference. `resolve_replicated_tables` names tables by
// `TableReference::table()` (bare), so without re-reading the scan's reference the
// estimator only ever saw `orders` — and brute-force enumerated every namespace of
// every catalog (on Glue, O(databases) GetTable calls per table per query).
// ---------------------------------------------------------------------------

/// A `StatsCatalog`-style fixture that RECORDS every `list_namespaces`/`load_table` call, so
/// the sizing walk's call shape is asserted directly.
struct RecordingCatalog {
    tables: HashMap<String, String>, // "<db>.<table>" -> location
    list_calls: Mutex<Vec<Vec<String>>>,
    load_calls: Mutex<Vec<(Vec<String>, String)>>,
}

impl RecordingCatalog {
    fn clear_calls(&self) {
        self.list_calls.lock().unwrap().clear();
        self.load_calls.lock().unwrap().clear();
    }
    fn list_calls(&self) -> Vec<Vec<String>> {
        self.list_calls.lock().unwrap().clone()
    }
    fn load_calls(&self) -> Vec<(Vec<String>, String)> {
        self.load_calls.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl oxidant_catalog::CatalogProvider for RecordingCatalog {
    fn name(&self) -> &str {
        "testcat"
    }

    async fn list_namespaces(
        &self,
        parent: &[String],
    ) -> oxidant_catalog::Result<Vec<Vec<String>>> {
        self.list_calls.lock().unwrap().push(parent.to_vec());
        if parent.is_empty() {
            Ok(vec![vec!["db1".to_string()]])
        } else {
            Ok(vec![])
        }
    }

    async fn list_tables(&self, _namespace: &[String]) -> oxidant_catalog::Result<Vec<String>> {
        Ok(self.tables.keys().cloned().collect())
    }

    async fn load_table(
        &self,
        namespace: &[String],
        table: &str,
    ) -> oxidant_catalog::Result<oxidant_catalog::TableMetadata> {
        self.load_calls
            .lock()
            .unwrap()
            .push((namespace.to_vec(), table.to_string()));
        let key = format!("{}.{table}", namespace.join("."));
        let location = self
            .tables
            .get(&key)
            .ok_or_else(|| oxidant_catalog::Error::Plan(format!("no such table `{key}`")))?;
        Ok(oxidant_catalog::TableMetadata::new(
            key,
            location.clone(),
            oxidant_catalog::TableFormat::Parquet,
        ))
    }
}

/// Through `resolve_replicated_tables` (the production driver path), a query over two
/// three-part-named tables must cost exactly one `load_table` per table in its own namespace
/// and ZERO `list_namespaces` — while the classification output keeps its bare-name keys.
#[tokio::test]
async fn sizing_walk_uses_qualified_refs_without_namespace_enumeration() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::remove_var("OXIDANT_REPLICATE_MAX_ROW_MULTIPLE");
    std::env::remove_var("OXIDANT_REPLICATED_TABLES");

    let dir = tempfile::tempdir().unwrap();
    // `orders` is the byte anchor (padded wide column); `customers` is tiny and replicates.
    let orders = batch(
        vec![i64f("o_custkey"), i64f("o_total"), strf("o_comment")],
        vec![
            i64v(&[1, 2, 3, 4]),
            i64v(&[10, 20, 30, 40]),
            Arc::new(StringArray::from(
                (0..4).map(|_| "x".repeat(600)).collect::<Vec<_>>(),
            )),
        ],
    );
    let customers = batch(
        vec![i64f("c_custkey"), strf("c_name")],
        vec![i64v(&[1, 2]), Arc::new(StringArray::from(vec!["a", "b"]))],
    );
    let orders_dir = dir.path().join("orders");
    let customers_dir = dir.path().join("customers");
    write_parquet(&orders_dir, &orders);
    write_parquet(&customers_dir, &customers);
    assert!(
        std::fs::metadata(orders_dir.join("part-0.parquet"))
            .unwrap()
            .len()
            > std::fs::metadata(customers_dir.join("part-0.parquet"))
                .unwrap()
                .len(),
        "orders must be the byte anchor"
    );

    let mut tables = HashMap::new();
    tables.insert(
        "db1.orders".to_string(),
        orders_dir.to_str().unwrap().to_string(),
    );
    tables.insert(
        "db1.customers".to_string(),
        customers_dir.to_str().unwrap().to_string(),
    );
    let catalog = Arc::new(RecordingCatalog {
        tables,
        list_calls: Mutex::new(Vec::new()),
        load_calls: Mutex::new(Vec::new()),
    });

    let engine = Engine::new();
    engine.register_catalog("testcat", catalog.clone());
    let sql = "SELECT c.c_name AS n, SUM(o.o_total) AS t \
               FROM testcat.db1.orders o \
               JOIN testcat.db1.customers c ON o.o_custkey = c.c_custkey \
               GROUP BY c.c_name";
    let lp = engine.logical_plan(sql).await.unwrap();
    // Planning resolved each table through the bridge (one load_table per table); reset the
    // recordings so only the sizing walk's calls are asserted.
    catalog.clear_calls();

    let replicated = resolve_replicated_tables(&engine, &lp).await;

    let mut loads = catalog.load_calls();
    loads.sort();
    assert_eq!(
        loads,
        vec![
            (vec!["db1".to_string()], "customers".to_string()),
            (vec!["db1".to_string()], "orders".to_string()),
        ],
        "exactly one load_table per table, in its own namespace"
    );
    assert!(
        catalog.list_calls().is_empty(),
        "a qualified reference must never enumerate namespaces: {:?}",
        catalog.list_calls()
    );
    // Classification still keys off bare names: tiny customers replicates, the byte anchor
    // orders stays sharded.
    assert!(
        replicated.iter().any(|t| t == "customers"),
        "{replicated:?}"
    );
    assert!(!replicated.iter().any(|t| t == "orders"), "{replicated:?}");
}

// ---------------------------------------------------------------------------
// Planner shapes under the 2-sharded classification
// ---------------------------------------------------------------------------

fn assert_has_shuffle_stage(dq: &oxidant_execution::plan::DistributedQuery) {
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
/// connector roots the connected chain at the ORIGINAL leftmost leaf — the replicated `item`
/// dim — and the re-root then rotates it to the first sharded leaf, `inventory`. The trailing
/// `catalog_sales` join (written `cs_item_sk = i_item_sk` against the folded dim) keys on
/// `inv_item_sk` through the query's equality web, so the 2-sharded classification plans as a
/// co-located shuffle join …
#[tokio::test]
async fn q37_shape_two_sharded_plans_distributed_via_reroot() {
    let engine = tpcds_engine().await;
    let lp = engine.logical_plan(Q37).await.unwrap();
    let replicated = ["item", "date_dim"];
    let dq = plan_distributed_logical(&lp, &replicated)
        .expect("dim-leftmost 2-sharded chain must plan via re-rooting");
    assert_has_shuffle_stage(&dq);
    // `inventory` is sharded, not replicated: exactly one stage scans it, as a hash-keyed
    // leaf, and no stage carries it in the replicated-tables stamp (the pre-reroot plan
    // re-scanned the full replicated `inventory` on every worker — the 2× tax).
    let inv_scans: Vec<_> = dq
        .stages
        .iter()
        .filter(|s| s.sql.contains("FROM inventory"))
        .collect();
    assert_eq!(inv_scans.len(), 1, "inventory scanned exactly once: {dq:?}");
    assert!(
        !inv_scans[0].hash_key_cols.is_empty(),
        "inventory leaf must be hash-shuffled: {dq:?}"
    );
    for s in &dq.stages {
        assert!(
            !s.replicated_tables.split(',').any(|t| t == "inventory"),
            "inventory must not be replicated: {s:?}"
        );
    }
    // The shuffle boundary keys the two sharded tables directly (transitively, via
    // `inv_item_sk = i_item_sk = cs_item_sk`); the folded dims join inside the stage.
    let boundary = &dq.stages[2].sql;
    assert!(
        boundary.contains("l.inventory__inv_item_sk = r.catalog_sales__cs_item_sk"),
        "sharded–sharded shuffle key: {boundary}"
    );
    assert!(
        boundary.contains("JOIN item AS item ON")
            && boundary.contains("JOIN date_dim AS date_dim ON"),
        "replicated dims fold into the boundary stage: {boundary}"
    );
}

/// … and the SQL entry point produces the real multi-stage plan — no single-worker Forward
/// fallback (previously the reason the rule was gated off).
#[tokio::test]
async fn q37_shape_two_sharded_no_forward_fallback() {
    let engine = tpcds_engine().await;
    let replicated = ["item", "date_dim"];
    let dq = plan_distributed(&engine, Q37, &replicated)
        .await
        .expect("must produce a plan");
    assert!(
        dq.stages.len() > 1,
        "a single Forward stage would be the local fallback: {dq:?}"
    );
    assert!(
        dq.stages
            .iter()
            .any(|s| s.exchange != ExchangeMode::Forward),
        "at least one shuffled stage: {dq:?}"
    );
}

/// Q82 is Q37's twin (`FROM item, inventory, date_dim, store_sales`): the same dim-leftmost
/// comma chain re-roots to `inventory` and plans distributed at the 2-sharded classification.
#[tokio::test]
async fn q82_shape_two_sharded_plans_distributed_via_reroot() {
    let engine = tpcds_engine().await;
    let lp = engine.logical_plan(Q82).await.unwrap();
    let replicated = ["item", "date_dim"];
    let dq = plan_distributed_logical(&lp, &replicated)
        .expect("dim-leftmost 2-sharded chain must plan via re-rooting");
    assert_has_shuffle_stage(&dq);
    let boundary = dq
        .stages
        .iter()
        .map(|s| s.sql.as_str())
        .find(|sql| sql.contains("shuffle_input_0"))
        .expect("a shuffle-join boundary stage");
    assert!(
        boundary.contains("l.inventory__inv_item_sk = r.store_sales__ss_item_sk"),
        "sharded–sharded shuffle key via the equality web: {boundary}"
    );
}

/// Same query, today's byte-only classification (inventory replicated): plans distributed
/// via the 1-sharded broadcast path — the pre-default behavior, still valid when an operator
/// pins the classification.
#[tokio::test]
async fn q37_shape_legacy_classification_plans_distributed() {
    let engine = tpcds_engine().await;
    let lp = engine.logical_plan(Q37).await.unwrap();
    let replicated = ["item", "inventory", "date_dim"];
    let dq = plan_distributed_logical(&lp, &replicated)
        .expect("legacy 1-sharded classification must plan");
    assert_has_shuffle_stage(&dq);
}

// ---------------------------------------------------------------------------
// Q37 end-to-end at the 2-sharded classification: two in-process workers, both
// sharded tables row-split across them, dims held in full on each.
// ---------------------------------------------------------------------------

static PORT: std::sync::OnceLock<std::sync::atomic::AtomicU16> = std::sync::OnceLock::new();

fn unique_worker_port() -> u16 {
    // Base BELOW the Linux ephemeral source range (32768..=60999) so the harness's own
    // outbound connections can never steal a worker's port; offset from the kan49* files'
    // base to avoid cross-binary collisions on shared CI runners.
    PORT.get_or_init(|| {
        std::sync::atomic::AtomicU16::new(24000 + (std::process::id() as u16 % 512))
    })
    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
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

/// `inventory` and `catalog_sales` sharded row-wise across two in-process workers; the
/// replicated dims (`item`, `date_dim`) held in full on each worker — exactly the shape the
/// row-aware classification produces for Q37.
async fn two_workers_q37() -> oxidant_execution::driver::Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        e.register_batches("inventory", shard_rows(&inventory(), i))
            .unwrap();
        e.register_batches("catalog_sales", shard_rows(&catalog_sales(), i))
            .unwrap();
        e.register_batches("item", vec![item()]).unwrap();
        e.register_batches("date_dim", vec![date_dim()]).unwrap();
        tokio::spawn(async move {
            let _ = oxidant_execution::flight::serve_worker(port, e).await;
        });
    }
    oxidant_execution::driver::Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ])
}

/// Sorted value rows (headers are not compared: single-node and distributed plans name
/// unaliased outputs differently — pre-existing behavior of every distributed shape).
fn rows_sorted(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let opts = oxidant_loom::arrow::util::display::FormatOptions::default().with_null("NULL");
    let mut rows = Vec::new();
    for b in batches {
        let fmts: Vec<_> = b
            .columns()
            .iter()
            .map(|c| oxidant_loom::arrow::util::display::ArrayFormatter::try_new(c, &opts).unwrap())
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

/// The re-rooted Q37 stage SQL actually executes: distributed over two workers (each holding
/// half of both sharded tables) it must equal single-node, proving the folded-dim stage SQL
/// the chain builder emits is valid — plan-shape assertions alone cannot catch a bad
/// flattened reference.
#[tokio::test]
async fn q37_two_sharded_distributed_matches_single_node() {
    let planner = tpcds_engine().await;
    let expected = planner.sql(Q37).await.expect("single-node");
    assert!(
        expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
        "single-node result must be non-empty (otherwise the comparison is vacuous)"
    );
    let replicated = ["item", "date_dim"];
    let lp = planner.logical_plan(Q37).await.unwrap();
    let dq = plan_distributed_logical(&lp, &replicated).expect("must plan distributed");
    let cluster = two_workers_q37().await;
    let mut out = None;
    for _ in 0..150 {
        match oxidant_execution::driver::run_stages(&cluster, &dq.stages).await {
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

// ---------------------------------------------------------------------------
// TPC-DS Q72 at the 2-sharded classification: `catalog_sales ⋈ inventory` is the
// only sharded–sharded boundary; the seven replicated dims and the trailing
// `LEFT JOIN promotion` / `LEFT JOIN catalog_returns` fold into the final chain
// stage, and the six WHERE conjuncts distribute onto scans / step residuals
// (nothing rides a dropped `Filter` above the chain).
// ---------------------------------------------------------------------------

const Q72: &str = include_str!("../../../bench/tpcds/queries/q72.sql");
const Q72_REPLICATED: [&str; 7] = [
    "warehouse",
    "item",
    "customer_demographics",
    "household_demographics",
    "date_dim",
    "promotion",
    "catalog_returns",
];

/// Q72-tailored `catalog_sales`: rows A/B survive every conjunct (A matches `promotion` and
/// `catalog_returns`, B null-extends both); C–G each fail exactly one conjunct (cd status,
/// hd potential, quantity comparison, d1 year, ship-date comparison).
fn q72_catalog_sales() -> RecordBatch {
    batch(
        vec![
            i64f("cs_item_sk"),
            i64f("cs_sold_date_sk"),
            i64f("cs_quantity"),
            i64f("cs_ship_date_sk"),
            i64f("cs_bill_cdemo_sk"),
            i64f("cs_bill_hdemo_sk"),
            i64f("cs_promo_sk"),
            i64f("cs_order_number"),
        ],
        vec![
            i64v(&[1, 1, 1, 1, 2, 2, 1]),
            i64v(&[100, 100, 100, 100, 100, 101, 100]),
            i64v(&[100, 90, 100, 100, 40, 100, 100]),
            i64v(&[300, 300, 300, 300, 300, 300, 301]),
            i64v(&[10, 10, 20, 10, 10, 10, 10]),
            i64v(&[30, 30, 30, 40, 30, 30, 30]),
            i64v(&[60, 61, 60, 60, 60, 60, 60]),
            i64v(&[1000, 1001, 1002, 1003, 1004, 1005, 1006]),
        ],
    )
}

/// Q72-tailored `inventory`: item 1 passes the quantity comparison (50 < 90/100), item 2
/// fails it (70 ≥ 40); `inv_date_sk = 201` carries a mismatching `d_week_seq`.
fn q72_inventory() -> RecordBatch {
    batch(
        vec![
            i64f("inv_item_sk"),
            i64f("inv_date_sk"),
            i64f("inv_quantity_on_hand"),
            i64f("inv_warehouse_sk"),
        ],
        vec![
            i64v(&[1, 1, 2, 2]),
            i64v(&[200, 201, 200, 201]),
            i64v(&[50, 50, 70, 70]),
            i64v(&[50, 50, 50, 50]),
        ],
    )
}

/// Q72-tailored `date_dim`: sk 100 is the passing sold date (year 1999, week 7), 101 the
/// wrong-year one; 200/201 the matching / week-mismatched inventory dates; 300/301 the
/// passing / failing ship dates (`d3.d_date > d1.d_date + 5` ⇔ `> 11005`).
fn q72_date_dim() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            i64f("d_date_sk"),
            Field::new("d_date", DataType::Date32, false),
            i64f("d_week_seq"),
            i64f("d_year"),
        ])),
        vec![
            i64v(&[100, 101, 200, 201, 300, 301]),
            Arc::new(Date32Array::from(vec![
                11_000, 11_001, 10_999, 10_998, 11_010, 11_003,
            ])),
            i64v(&[7, 7, 7, 8, 10, 10]),
            i64v(&[1999, 1998, 1999, 1999, 1999, 1999]),
        ],
    )
    .unwrap()
}

fn register_q72_dims(e: &Engine) {
    e.register_batches("item", vec![item()]).unwrap();
    e.register_batches("warehouse", vec![warehouse()]).unwrap();
    e.register_batches("customer_demographics", vec![customer_demographics()])
        .unwrap();
    e.register_batches("household_demographics", vec![household_demographics()])
        .unwrap();
    e.register_batches("promotion", vec![promotion()]).unwrap();
    e.register_batches("catalog_returns", vec![catalog_returns()])
        .unwrap();
    e.register_batches("date_dim", vec![q72_date_dim()])
        .unwrap();
}

/// Single-node ground truth over the Q72-tailored dataset.
async fn q72_engine() -> Engine {
    let e = Engine::new();
    e.register_batches("catalog_sales", vec![q72_catalog_sales()])
        .unwrap();
    e.register_batches("inventory", vec![q72_inventory()])
        .unwrap();
    register_q72_dims(&e);
    e
}

/// Q72's two sharded tables row-split across two in-process workers; every dim (including
/// the LEFT JOINed `promotion` / `catalog_returns`) held in full on each — exactly the
/// shape the row-aware classification produces for Q72.
async fn two_workers_q72() -> oxidant_execution::driver::Cluster {
    let (p0, p1) = (unique_worker_port(), unique_worker_port());
    for (i, port) in [p0, p1].into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        e.register_batches("catalog_sales", shard_rows(&q72_catalog_sales(), i))
            .unwrap();
        e.register_batches("inventory", shard_rows(&q72_inventory(), i))
            .unwrap();
        register_q72_dims(&e);
        tokio::spawn(async move {
            let _ = oxidant_execution::flight::serve_worker(port, e).await;
        });
    }
    oxidant_execution::driver::Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ])
}

/// The Q72 plan shape at the 2-sharded classification: two hash-shuffled leaf scans
/// (`catalog_sales` by `cs_item_sk`, `inventory` by `inv_item_sk`), one co-located boundary
/// stage carrying every folded dim — the trailing `LEFT JOIN promotion` /
/// `LEFT JOIN catalog_returns` included — plus every WHERE conjunct, and a combine stage.
#[tokio::test]
async fn q72_shape_two_sharded_plans_distributed() {
    let engine = tpcds_engine().await;
    let lp = engine.logical_plan(Q72).await.unwrap();
    let dq = plan_distributed_logical(&lp, &Q72_REPLICATED)
        .expect("Q72 must plan as a 2-sharded shuffle-join chain");

    assert_eq!(
        dq.stages.len(),
        4,
        "leaf, leaf, boundary+partial, combine: {dq:?}"
    );
    let leaf0 = &dq.stages[0];
    let leaf1 = &dq.stages[1];
    assert!(leaf0.sql.contains("FROM catalog_sales"), "{}", leaf0.sql);
    assert!(leaf1.sql.contains("FROM inventory"), "{}", leaf1.sql);
    assert!(!leaf0.hash_key_cols.is_empty(), "cs leaf shuffles");
    assert!(!leaf1.hash_key_cols.is_empty(), "inventory leaf shuffles");
    // `inventory` is sharded, not replicated: scanned exactly once, never carried whole.
    assert_eq!(
        dq.stages
            .iter()
            .filter(|s| s.sql.contains("FROM inventory"))
            .count(),
        1,
        "inventory scanned exactly once: {dq:?}"
    );

    let boundary = &dq.stages[2];
    assert_eq!(boundary.upstream_stage_ids, vec![0, 1]);
    let sql = &boundary.sql;
    // The sharded–sharded boundary keys the two shuffle inputs directly.
    assert!(
        sql.contains("l.catalog_sales__cs_item_sk = r.inventory__inv_item_sk"),
        "boundary key: {sql}"
    );
    // Every trailing replicated join folds into the final stage; the LEFT JOINs keep their
    // keyword (null-extension is key-local against a complete replicated right side).
    for frag in [
        "JOIN warehouse AS warehouse ON r.inventory__inv_warehouse_sk",
        "JOIN item AS item ON",
        "JOIN customer_demographics AS customer_demographics ON",
        "JOIN household_demographics AS household_demographics ON",
        "JOIN date_dim AS d1 ON",
        "JOIN date_dim AS d2 ON",
        "JOIN date_dim AS d3 ON",
        "LEFT JOIN promotion AS promotion ON",
        "LEFT JOIN catalog_returns AS catalog_returns ON",
    ] {
        assert!(
            sql.contains(frag),
            "missing `{frag}` in boundary stage: {sql}"
        );
    }
    // Every WHERE conjunct is accounted for — scan filter or step residual, none dropped:
    // cd_marital_status = 'D', hd_buy_potential = '>10000', d1.d_year = 1999 (folded scan
    // filters), the d_week_seq equality (d2's fold key), the quantity comparison and the
    // ship-date comparison (stage WHERE residuals).
    for frag in [
        "cd_marital_status = 'D'",
        "hd_buy_potential = '>10000'",
        "d1.d_year = 1999",
        "d1.d_week_seq = d2.d_week_seq",
        "r.inventory__inv_quantity_on_hand < l.catalog_sales__cs_quantity",
        "d3.d_date",
        "d1.d_date",
        "WHERE",
        "GROUP BY",
    ] {
        assert!(
            sql.contains(frag),
            "conjunct `{frag}` dropped from plan: {sql}"
        );
    }
    assert_eq!(dq.stages[3].upstream_stage_ids, vec![2]);
    let finalize = dq.finalize_sql.expect("ORDER BY / LIMIT finalize");
    assert!(finalize.contains("ORDER BY"), "{finalize}");
    assert!(finalize.contains("LIMIT 100"), "{finalize}");
}

/// A trailing replicated RIGHT join cannot fold (its preserved side would repeat on every
/// worker): the chain declines with the historical rejection instead of planning wrong.
#[tokio::test]
async fn trailing_right_join_declines_to_old_rejection() {
    let engine = tpcds_engine().await;
    let sql = "SELECT i.i_item_desc AS d, SUM(cs.cs_quantity) AS q \
               FROM catalog_sales cs \
               JOIN inventory inv ON cs.cs_item_sk = inv.inv_item_sk \
               RIGHT JOIN item i ON cs.cs_item_sk = i.i_item_sk \
               GROUP BY i.i_item_desc";
    let lp = engine.logical_plan(sql).await.unwrap();
    let err = plan_distributed_logical(&lp, &["item"])
        .expect_err("a trailing RIGHT join must keep the old rejection");
    assert!(
        err.to_string().contains("trailing replicated-only joins"),
        "{err}"
    );
}

/// A WHERE conjunct referencing the null-extended side of a trailing LEFT JOIN cannot move
/// below the outer join, so the filter stays parked and the fold declines — the query keeps
/// the rejection/fallback path rather than returning unfiltered rows.
#[tokio::test]
async fn filter_on_outer_join_side_declines_to_old_rejection() {
    let engine = tpcds_engine().await;
    let sql = "SELECT i.i_item_desc AS d, SUM(cs.cs_quantity) AS q \
               FROM catalog_sales cs \
               JOIN inventory inv ON cs.cs_item_sk = inv.inv_item_sk \
               JOIN item i ON cs.cs_item_sk = i.i_item_sk \
               LEFT JOIN promotion p ON cs.cs_promo_sk = p.p_promo_sk \
               WHERE p.p_promo_sk IS NOT NULL \
               GROUP BY i.i_item_desc";
    let lp = engine.logical_plan(sql).await.unwrap();
    let err = plan_distributed_logical(&lp, &["item", "promotion"])
        .expect_err("an undistributable filter must keep the old rejection");
    assert!(
        err.to_string().contains("trailing replicated-only joins"),
        "{err}"
    );
}

/// The folded Q72 stage SQL actually executes: distributed over two workers (each holding
/// half of both sharded tables) it must equal single-node — proving every conjunct landed
/// and the folded LEFT JOINs null-extend exactly as the single-node plan does.
#[tokio::test]
async fn q72_two_sharded_distributed_matches_single_node() {
    let planner = q72_engine().await;
    let expected = planner.sql(Q72).await.expect("single-node");
    assert!(
        expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
        "single-node result must be non-empty (otherwise the comparison is vacuous)"
    );
    let lp = planner.logical_plan(Q72).await.unwrap();
    let dq = plan_distributed_logical(&lp, &Q72_REPLICATED).expect("must plan distributed");
    let cluster = two_workers_q72().await;
    let mut out = None;
    for _ in 0..150 {
        match oxidant_execution::driver::run_stages(&cluster, &dq.stages).await {
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
