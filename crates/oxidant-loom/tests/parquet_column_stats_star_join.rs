//! KAN-143 end-to-end: footer column statistics on catalog-backed parquet scans fix the
//! join cardinality estimate for foreign-key star joins.
//!
//! Before KAN-143 the scan attached only row counts, so DataFusion 54.1 estimated an inner
//! join's output as `Inexact(min(left, right))` — for a fact⋈dimension FK join that
//! UNDERESTIMATES the real (fact-sized) output by orders of magnitude (see
//! `provable_row_bound` in `oxidant_loom` and `estimate_inner_join_cardinality` in
//! datafusion-physical-plan-54.1.0 `joins/utils.rs`). With min/max from the footers the
//! NDV estimate becomes the key RANGE (~the dimension's key domain), the selectivity
//! `max(ndv_l, ndv_r)` stops collapsing to the fact row count, and the estimate lands on
//! the fact size.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::stats::Precision;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_plan::ExecutionPlan;
use oxidant_catalog::{
    CatalogProvider as OxidantCatalog, Result as CatResult, TableFormat, TableMetadata,
};
use oxidant_loom::Engine;

/// A fake catalog whose namespace `ns` serves every table registered in
/// [`StarSchema::tables`] from a per-table directory of parquet files (schema inferred —
/// names match the files exactly, so footer column stats are trusted).
struct StarCatalog {
    tables: Vec<(String, String)>,
}

#[async_trait]
impl OxidantCatalog for StarCatalog {
    fn name(&self) -> &str {
        "star"
    }
    async fn list_namespaces(&self, _parent: &[String]) -> CatResult<Vec<Vec<String>>> {
        Ok(vec![vec!["ns".to_string()]])
    }
    async fn list_tables(&self, _ns: &[String]) -> CatResult<Vec<String>> {
        Ok(self.tables.iter().map(|(name, _)| name.clone()).collect())
    }
    async fn load_table(&self, ns: &[String], table: &str) -> CatResult<TableMetadata> {
        if ns == ["ns"] {
            if let Some((_, location)) = self.tables.iter().find(|(name, _)| name == table) {
                return Ok(TableMetadata::new(
                    format!("star.ns.{table}"),
                    location.clone(),
                    TableFormat::Parquet,
                ));
            }
        }
        Err(oxidant_common::Error::Plan(format!(
            "no such table: {}.{table}",
            ns.join(".")
        )))
    }
}

/// Serialize the two estimate tests: `OXIDANT_PARQUET_COLUMN_STATS` is process-global.
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct StarSchema {
    dirs: Vec<std::path::PathBuf>,
    tables: Vec<(String, String)>,
    fact_rows: usize,
    dim_rows: usize,
}

/// Write `fact` (rows cycling through the full key domain for both FK columns) and `dims`
/// (one row per key) as single-file parquet tables in fresh temp dirs, and return the
/// catalog table list.
fn write_star_schema(fact_rows: usize, dim_rows: usize) -> StarSchema {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let seq = NEXT.fetch_add(1, Ordering::Relaxed);

    let mut dirs = Vec::new();
    let mut tables = Vec::new();
    let mut write_table = |name: &str, schema: SchemaRef, batch: &RecordBatch| {
        let dir =
            std::env::temp_dir().join(format!("oxidant-star-{}-{seq}-{name}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let f = std::fs::File::create(dir.join("part-0.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(batch).unwrap();
        w.close().unwrap();
        tables.push((name.to_string(), format!("file://{}", dir.display())));
        dirs.push(dir);
    };

    let fact_schema = Arc::new(Schema::new(vec![
        Field::new("k1", DataType::Int64, false),
        Field::new("k2", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let keys = |offset: usize| -> Arc<Int64Array> {
        Arc::new(Int64Array::from(
            (0..fact_rows)
                .map(|i| ((i + offset) % dim_rows) as i64)
                .collect::<Vec<_>>(),
        ))
    };
    let fact = RecordBatch::try_new(
        fact_schema.clone(),
        vec![
            keys(0),
            keys(1),
            Arc::new(Int64Array::from(
                (0..fact_rows).map(|i| i as i64).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    write_table("fact", fact_schema, &fact);

    let dim_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("d", DataType::Int64, false),
    ]));
    let dim = RecordBatch::try_new(
        dim_schema.clone(),
        vec![
            Arc::new(Int64Array::from(
                (0..dim_rows).map(|i| i as i64).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                (0..dim_rows).map(|i| (i * 2) as i64).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    write_table("dim1", dim_schema.clone(), &dim);
    write_table("dim2", dim_schema, &dim);

    StarSchema {
        dirs,
        tables,
        fact_rows,
        dim_rows,
    }
}

impl Drop for StarSchema {
    fn drop(&mut self) {
        for dir in &self.dirs {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

fn star_engine(star: &StarSchema) -> Engine {
    let engine = Engine::new();
    engine.register_catalog(
        "star",
        Arc::new(StarCatalog {
            tables: star.tables.clone(),
        }),
    );
    engine
}

const STAR_SQL: &str = "SELECT COUNT(*) AS c FROM star.ns.fact f \
     JOIN star.ns.dim1 d1 ON f.k1 = d1.k JOIN star.ns.dim2 d2 ON f.k2 = d2.k";

/// The topmost hash join of the plan (the last join of the chain).
fn top_hash_join(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.name() == "HashJoinExec" {
        return Some(Arc::clone(plan));
    }
    for child in plan.children() {
        if let Some(found) = top_hash_join(child) {
            return Some(found);
        }
    }
    None
}

/// The chain output estimate keys off footer min/max ranges: every FK join key spans the
/// dimension's key domain on both sides, so `max(ndv_l, ndv_r)` is the domain size — NOT
/// the fact row count — and the estimate lands on the FACT size instead of
/// `Inexact(min(l, r))` (the dimension size).
#[tokio::test(flavor = "multi_thread")]
async fn star_join_estimate_is_fact_sized_with_footer_column_stats() {
    let _env = ENV_LOCK.lock().await;
    std::env::remove_var("OXIDANT_PARQUET_COLUMN_STATS");
    let star = write_star_schema(50_000, 1_000);
    let engine = star_engine(&star);
    let plan = engine.physical_plan(STAR_SQL).await.unwrap();
    eprintln!(
        "star-join physical plan (column stats ON):\n{}",
        datafusion::physical_plan::displayable(plan.as_ref()).indent(false)
    );
    let top = top_hash_join(&plan).expect("a hash join chain");
    let stats = top.partition_statistics(None).unwrap();
    assert_eq!(
        stats.num_rows,
        Precision::Inexact(star.fact_rows),
        "FK star-join output must estimate to the fact size, not Inexact(min(l, r)) = {}",
        star.dim_rows
    );
    // And the query itself is unaffected (row counts were always exact): every fact row
    // matches one row per dimension.
    let batches = engine.sql(STAR_SQL).await.unwrap();
    let c = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(c as usize, star.fact_rows);
}

/// The escape hatch: with column stats disabled the estimate falls back to the pre-KAN-143
/// `Inexact(min(l, r))` shape.
#[tokio::test(flavor = "multi_thread")]
async fn star_join_estimate_kill_switch_restores_min_estimate() {
    let _env = ENV_LOCK.lock().await;
    std::env::set_var("OXIDANT_PARQUET_COLUMN_STATS", "0");
    let star = write_star_schema(50_000, 1_000);
    let engine = star_engine(&star);
    let plan = engine.physical_plan(STAR_SQL).await.unwrap();
    std::env::remove_var("OXIDANT_PARQUET_COLUMN_STATS");
    let top = top_hash_join(&plan).expect("a hash join chain");
    let stats = top.partition_statistics(None).unwrap();
    assert_eq!(
        stats.num_rows,
        Precision::Inexact(star.dim_rows),
        "column stats off: the estimate must fall back to Inexact(min(l, r))"
    );
}
