//! KAN-35 regression: the catalog bridge caches resolved table providers per worker, and a
//! cached provider *embeds* the shard/replicate decision that was active at first resolution
//! (a sharded table lists only this worker's file shard; a replicated one lists everything).
//! The driver's per-query auto-broadcast classification flips a table's role between queries
//! via the stage ticket's task-local replicate overlay, so the cache must key on that
//! decision — otherwise a stale full provider is scanned on every worker where the plan
//! assumes shards (rows × worker count; SF10 Q4's exact 2×), or a stale shard is served where
//! the plan assumes a full copy (rows dropped; SF10 Q5/Q7's ~0.5×, compounding in Q9).
//!
//! Lives in an integration-test binary (not the lib unit tests) because the shard assignment
//! is process-global env (`OXIDANT_WORKER_COUNT` / `OXIDANT_SHARD_INDEX`): process isolation keeps
//! the env from leaking into parallel unit tests that resolve catalog tables.

use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use oxidant_catalog::{CatalogProvider as OxidantCatalog, Result as CatResult, TableMetadata};
use oxidant_loom::Engine;

/// Serializes the two tests: they mutate the same process-global shard env. Held across
/// awaits deliberately (like the other env-mutating suites) so the env stays stable for the
/// whole test — the lock only ever serializes these two tests.
static SHARD_ENV_LOCK: Mutex<()> = Mutex::new(());

/// A fake catalog whose single namespace `ns` has one table `orders` at a fixed location.
struct FakeCatalog {
    location: String,
}

#[async_trait]
impl OxidantCatalog for FakeCatalog {
    fn name(&self) -> &str {
        "fake"
    }
    async fn list_namespaces(&self, _parent: &[String]) -> CatResult<Vec<Vec<String>>> {
        Ok(vec![vec!["ns".to_string()]])
    }
    async fn list_tables(&self, _ns: &[String]) -> CatResult<Vec<String>> {
        Ok(vec!["orders".to_string()])
    }
    async fn load_table(&self, ns: &[String], table: &str) -> CatResult<TableMetadata> {
        if ns == ["ns"] && table == "orders" {
            Ok(TableMetadata::new(
                "fake.ns.orders",
                self.location.clone(),
                oxidant_catalog::TableFormat::Parquet,
            ))
        } else {
            Err(oxidant_catalog::Error::Plan(format!(
                "no such table {table}"
            )))
        }
    }
}

/// Two parquet files with deterministic size ordering: the LPT sharder assigns the larger
/// file (`part-0`, values [1, 2, 3]) to worker 0 and `part-1` ([10]) to worker 1, so shard 0
/// sees count=3/sum=6 while the unsharded (replicated) table has count=4/sum=16.
fn write_two_file_parquet_dir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "oxidant-cat-2f-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    for (file, values) in [("part-0", vec![1_i64, 2, 3]), ("part-1", vec![10])] {
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))]).unwrap();
        let f = std::fs::File::create(dir.join(format!("{file}.parquet"))).unwrap();
        let mut w = ArrowWriter::try_new(f, schema.clone(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
    dir
}

/// COUNT/SUM of `fake.ns.orders` on this (shard-0) worker under the given stage-ticket
/// replicate overlay ("" = sharded).
async fn count_sum_under_overlay(engine: &Engine, overlay: &str) -> (i64, i64) {
    let batches = oxidant_loom::shard::with_replicated_tables(overlay, async {
        engine
            .sql("SELECT COUNT(*) AS c, SUM(x) AS s FROM fake.ns.orders")
            .await
            .unwrap()
    })
    .await;
    let c = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let s = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    (c, s)
}

fn shard0_env() {
    std::env::set_var("OXIDANT_WORKER_COUNT", "2");
    std::env::set_var("OXIDANT_SHARD_INDEX", "0");
}

fn clear_shard_env() {
    std::env::remove_var("OXIDANT_WORKER_COUNT");
    std::env::remove_var("OXIDANT_SHARD_INDEX");
}

/// A table first resolved while *sharded* must be re-resolved when a later query's
/// classification marks it replicated — the plan then assumes every worker scans the full
/// table, and serving the stale shard silently drops rows (the Q5/Q7 losses).
#[tokio::test]
#[allow(clippy::await_holding_lock)] // SHARD_ENV_LOCK serializes process-global env
async fn provider_cache_rebuilds_when_table_flips_to_replicated() {
    let _guard = SHARD_ENV_LOCK.lock().unwrap();
    shard0_env();
    let dir = write_two_file_parquet_dir();
    let location = format!("file://{}", dir.to_string_lossy());
    let engine = Engine::new();
    engine.register_catalog("fake", Arc::new(FakeCatalog { location }));

    // First resolution is sharded: shard 0 reads only the larger file.
    assert_eq!(count_sum_under_overlay(&engine, "").await, (3, 6));
    // A later stage ticket replicates the table: the full contents must be scanned even
    // though a sharded provider is already cached.
    assert_eq!(count_sum_under_overlay(&engine, "orders").await, (4, 16));
    // …and flipping back still serves the sharded variant.
    assert_eq!(count_sum_under_overlay(&engine, "").await, (3, 6));

    clear_shard_env();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Mirror direction: a table first resolved while *replicated* must be re-resolved when a
/// later query shards it — otherwise every worker scans the full table and downstream stages
/// combine each row once per worker (the Q4 duplication).
#[tokio::test]
#[allow(clippy::await_holding_lock)] // SHARD_ENV_LOCK serializes process-global env
async fn provider_cache_rebuilds_when_table_flips_to_sharded() {
    let _guard = SHARD_ENV_LOCK.lock().unwrap();
    shard0_env();
    let dir = write_two_file_parquet_dir();
    let location = format!("file://{}", dir.to_string_lossy());
    let engine = Engine::new();
    engine.register_catalog("fake", Arc::new(FakeCatalog { location }));

    // First resolution is replicated: the full table is scanned on this worker.
    assert_eq!(count_sum_under_overlay(&engine, "orders").await, (4, 16));
    // A later stage ticket shards it: only this worker's shard may be scanned.
    assert_eq!(count_sum_under_overlay(&engine, "").await, (3, 6));

    clear_shard_env();
    let _ = std::fs::remove_dir_all(&dir);
}
