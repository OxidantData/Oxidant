//! Bridge a [`weft_catalog::CatalogProvider`] into DataFusion's catalog API.
//!
//! DataFusion already resolves three-part names (`catalog.schema.table`) and loads tables
//! **lazily and asynchronously** through [`SchemaProvider::table`]. This module adapts a weft
//! catalog onto that model so an external metastore plugs straight into query resolution: the
//! catalog is hit only when a query first references one of its tables, and the resolved
//! [`TableMetadata`] is turned into a `TableProvider` via the engine's shared listing-table
//! builder (so Parquet/Delta/Iceberg all read through the same version-safe path).
//!
//! Mapping to DataFusion's fixed three-level model: a weft *namespace* is the middle level
//! (DataFusion's "schema"), so it is single-part here — covering Hive (`database`) and Unity /
//! Iceberg-REST (`schema`). The sync `schema_names`/`table_names`/`table_exist` methods are
//! best-effort (a cached snapshot); authoritative listing for the `spark.catalog.*` RPC goes
//! straight to the weft provider in `weft-connect`, not through these.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::prelude::SessionContext;
use weft_catalog::{CatalogProvider as WeftCatalog, TableFormat, TableMetadata};
use weft_common::Error;

/// DataFusion `CatalogProvider` backed by a weft [`WeftCatalog`].
pub struct WeftCatalogProvider {
    catalog: Arc<dyn WeftCatalog>,
    ctx: Arc<SessionContext>,
    /// Lazily-created per-namespace schema providers (cheap wrappers; cached so repeated
    /// references to the same namespace share a table cache).
    schemas: Mutex<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl WeftCatalogProvider {
    /// Wrap a weft catalog. `ctx` supplies the session state used to infer schemas / read files.
    pub fn new(catalog: Arc<dyn WeftCatalog>, ctx: Arc<SessionContext>) -> Self {
        Self {
            catalog,
            ctx,
            schemas: Mutex::new(HashMap::new()),
        }
    }
}

impl fmt::Debug for WeftCatalogProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WeftCatalogProvider")
            .field("catalog", &self.catalog.name())
            .finish()
    }
}

impl CatalogProvider for WeftCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        // Best-effort: the namespaces we've already materialized a provider for. Authoritative
        // listing is the `spark.catalog.listDatabases` RPC, which queries the weft provider.
        self.schemas
            .lock()
            .expect("schemas poisoned")
            .keys()
            .cloned()
            .collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        // Always hand back a provider (without a sync existence check); a non-existent table
        // surfaces as `Ok(None)` from the async `table()` below — DataFusion's normal "table not
        // found" path.
        let mut schemas = self.schemas.lock().expect("schemas poisoned");
        let provider = schemas.entry(name.to_string()).or_insert_with(|| {
            Arc::new(WeftSchemaProvider::new(
                self.catalog.clone(),
                vec![name.to_string()],
                self.ctx.clone(),
            ))
        });
        Some(provider.clone())
    }
}

/// DataFusion `SchemaProvider` for one namespace of a weft catalog.
struct WeftSchemaProvider {
    catalog: Arc<dyn WeftCatalog>,
    namespace: Vec<String>,
    ctx: Arc<SessionContext>,
    /// Resolved tables, cached so a table referenced repeatedly in a query is loaded once.
    tables: Mutex<HashMap<String, Arc<dyn TableProvider>>>,
}

impl WeftSchemaProvider {
    fn new(
        catalog: Arc<dyn WeftCatalog>,
        namespace: Vec<String>,
        ctx: Arc<SessionContext>,
    ) -> Self {
        Self {
            catalog,
            namespace,
            ctx,
            tables: Mutex::new(HashMap::new()),
        }
    }
}

impl fmt::Debug for WeftSchemaProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WeftSchemaProvider")
            .field("catalog", &self.catalog.name())
            .field("namespace", &self.namespace)
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for WeftSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        // Best-effort: already-resolved tables. `spark.catalog.listTables` uses the weft provider.
        self.tables
            .lock()
            .expect("tables poisoned")
            .keys()
            .cloned()
            .collect()
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        if let Some(t) = self.tables.lock().expect("tables poisoned").get(name) {
            return Ok(Some(t.clone()));
        }
        let metadata = match self.catalog.load_table(&self.namespace, name).await {
            Ok(md) => md,
            // A "no such table" (analysis) error → DataFusion's standard not-found path.
            Err(Error::Plan(_)) => return Ok(None),
            // A storage / connection / unsupported failure is a real error — surface it.
            Err(e) => return Err(weft_to_df(e)),
        };
        let provider = metadata_to_provider(&self.ctx.state(), &metadata, name).await?;
        self.tables
            .lock()
            .expect("tables poisoned")
            .insert(name.to_string(), provider.clone());
        Ok(Some(provider))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables
            .lock()
            .expect("tables poisoned")
            .contains_key(name)
    }
}

/// Turn resolved table metadata into a readable DataFusion `TableProvider`.
async fn metadata_to_provider(
    state: &SessionState,
    md: &TableMetadata,
    table_name: &str,
) -> DfResult<Arc<dyn TableProvider>> {
    use datafusion::datasource::file_format::csv::CsvFormat;
    use datafusion::datasource::file_format::json::JsonFormat;
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};

    match md.format {
        TableFormat::Parquet => {
            let loc = crate::shard::ensure_collection_url(&md.location);
            let url = ListingTableUrl::parse(&loc).map_err(loc_err(md))?;
            ensure_remote_store(state, &url);
            let opts = ListingOptions::new(Arc::new(ParquetFormat::default()))
                .with_file_extension(".parquet");
            sharded_listing_table(state, vec![url], opts, md.schema.clone(), table_name, ".parquet")
                .await
        }
        TableFormat::Csv => {
            let loc = crate::shard::ensure_collection_url(&md.location);
            let url = ListingTableUrl::parse(&loc).map_err(loc_err(md))?;
            ensure_remote_store(state, &url);
            let opts =
                ListingOptions::new(Arc::new(CsvFormat::default())).with_file_extension(".csv");
            sharded_listing_table(state, vec![url], opts, md.schema.clone(), table_name, ".csv").await
        }
        TableFormat::Json => {
            let loc = crate::shard::ensure_collection_url(&md.location);
            let url = ListingTableUrl::parse(&loc).map_err(loc_err(md))?;
            ensure_remote_store(state, &url);
            let opts =
                ListingOptions::new(Arc::new(JsonFormat::default())).with_file_extension(".json");
            sharded_listing_table(state, vec![url], opts, md.schema.clone(), table_name, ".json")
                .await
        }
        // Lakehouse formats resolve to their active Parquet files (version-safe), then the
        // Parquet reader. v1 reads from the local filesystem — remote object stores for Delta /
        // Iceberg follow once the resolver registers an `object_store` for the location's scheme.
        TableFormat::Delta => {
            let path = local_path(&md.location)?;
            let files = weft_datasource::delta_active_files(&path).map_err(weft_to_df)?;
            parquet_files_provider(state, &md.location, files, md.schema.clone(), table_name).await
        }
        TableFormat::Iceberg => {
            let path = local_path(&md.location)?;
            let files = weft_datasource::iceberg_active_files(&path).map_err(weft_to_df)?;
            parquet_files_provider(state, &md.location, files, md.schema.clone(), table_name).await
        }
    }
}


/// List+shard files then build a [`ListingTable`], or an empty MemTable when this shard is vacant.
async fn sharded_listing_table(
    state: &SessionState,
    urls: Vec<datafusion::datasource::listing::ListingTableUrl>,
    opts: datafusion::datasource::listing::ListingOptions,
    schema: Option<datafusion::arrow::datatypes::SchemaRef>,
    table_name: &str,
    file_extension: &str,
) -> DfResult<Arc<dyn TableProvider>> {
    let sharded = crate::shard::apply_file_shard(state, urls, file_extension, Some(table_name))
        .await
        .map_err(weft_to_df)?;
    if sharded.is_empty() {
        let schema = match schema {
            Some(s) => s,
            None => {
                return Err(DataFusionError::Plan(format!(
                    "sharded table `{table_name}` has no files on this worker and no declared schema"
                )))
            }
        };
        return crate::shard::empty_table(schema).map_err(weft_to_df);
    }
    crate::build_listing_table(state, sharded, opts, schema)
        .await
        .map_err(weft_to_df)
}

/// Ensure an object store is registered on the session's runtime for a remote table location so
/// DataFusion can read it. Currently handles `s3://` — credentials come from the environment or the
/// EC2 instance role (IMDS) via object_store's default provider; no static keys. Registering on the
/// shared runtime is idempotent and persists for the session, so query-time resolution finds it.
/// `file://` and bare paths need nothing and are skipped.
fn ensure_remote_store(
    state: &SessionState,
    url: &datafusion::datasource::listing::ListingTableUrl,
) {
    if url.scheme() != "s3" {
        return;
    }
    let os_url = url.object_store(); // canonical `s3://bucket` key
    if state.runtime_env().object_store(&os_url).is_ok() {
        return; // already registered for this bucket
    }
    // `os_url` is the canonical `s3://bucket/` — pull the bucket from the authority.
    let bucket = os_url
        .as_str()
        .strip_prefix("s3://")
        .and_then(|r| r.split('/').next())
        .unwrap_or("")
        .to_string();
    if bucket.is_empty() {
        return;
    }
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-west-2".to_string());
    match object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(&bucket)
        .with_region(region)
        .build()
    {
        Ok(store) => {
            state
                .runtime_env()
                .register_object_store(os_url.as_ref(), Arc::new(store));
        }
        Err(e) => eprintln!("warn: could not register S3 object store for `{bucket}`: {e}"),
    }
}

/// Build a Parquet listing table over an explicit set of files (the Delta/Iceberg seam).
async fn parquet_files_provider(
    state: &SessionState,
    location: &str,
    files: Vec<std::path::PathBuf>,
    schema: Option<datafusion::arrow::datatypes::SchemaRef>,
    table_name: &str,
) -> DfResult<Arc<dyn TableProvider>> {
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};

    if files.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "table `{location}` has no active data files"
        )));
    }
    let urls = files
        .iter()
        .map(|p| {
            ListingTableUrl::parse(p.to_string_lossy())
                .map_err(|e| DataFusionError::Plan(format!("bad file path {}: {e}", p.display())))
        })
        .collect::<DfResult<Vec<_>>>()?;
    let opts = ListingOptions::new(Arc::new(ParquetFormat::default()));
    sharded_listing_table(state, urls, opts, schema, table_name, ".parquet").await
}

/// Convert a storage URI to a local filesystem path, or error for a scheme v1 can't read locally.
///
/// Handles both `file:///abs` (RFC form) and Hive's `file:/abs` (single-slash, as the Metastore
/// returns it), as well as bare paths. Non-`file` schemes (`s3://`, `hdfs://`, …) are not local.
fn local_path(location: &str) -> DfResult<String> {
    if let Some(rest) = location.strip_prefix("file://") {
        return Ok(rest.to_string());
    }
    if let Some(rest) = location.strip_prefix("file:") {
        return Ok(rest.to_string());
    }
    if location.contains("://") {
        let scheme = location.split("://").next().unwrap_or("");
        return Err(DataFusionError::NotImplemented(format!(
            "reading Delta/Iceberg from `{scheme}://` is not supported yet (local filesystem only)"
        )));
    }
    Ok(location.to_string())
}

fn loc_err(md: &TableMetadata) -> impl Fn(DataFusionError) -> DataFusionError + '_ {
    move |e| DataFusionError::Plan(format!("bad table location `{}`: {e}", md.location))
}

/// Map a weft error onto DataFusion's error type, preserving the failure class.
fn weft_to_df(e: Error) -> DataFusionError {
    match e {
        Error::Plan(m) => DataFusionError::Plan(m),
        Error::Unsupported(m) => DataFusionError::NotImplemented(m),
        Error::Execution(m) | Error::Io(m) => DataFusionError::Execution(m),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;
    use weft_catalog::{Result as CatResult, TableMetadata};

    /// A fake catalog whose single namespace `ns` has one table `orders` at a fixed location.
    struct FakeCatalog {
        location: String,
    }

    #[async_trait]
    impl WeftCatalog for FakeCatalog {
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
                    TableFormat::Parquet,
                ))
            } else {
                Err(Error::Plan(format!(
                    "no such table: {}.{table}",
                    ns.join(".")
                )))
            }
        }
    }

    fn write_parquet_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("weft-cat-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("part-0.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        dir
    }

    #[tokio::test]
    async fn lazy_resolution_through_registered_catalog() {
        let dir = write_parquet_dir();
        let location = format!("file://{}", dir.to_string_lossy());

        let engine = crate::Engine::new();
        engine.register_catalog("fake", Arc::new(FakeCatalog { location }));

        // Never pre-registered the table — it resolves lazily via the bridge's async `table()`.
        let batches = engine
            .sql("SELECT COUNT(*) AS c, SUM(x) AS s FROM fake.ns.orders")
            .await
            .unwrap();
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
        assert_eq!((c, s), (4, 10));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn missing_table_is_a_clean_not_found() {
        let engine = crate::Engine::new();
        engine.register_catalog(
            "fake",
            Arc::new(FakeCatalog {
                location: "file:///nonexistent".to_string(),
            }),
        );
        let err = engine
            .sql("SELECT * FROM fake.ns.missing")
            .await
            .unwrap_err();
        // DataFusion's table-not-found analysis error, not a panic / internal error.
        assert!(format!("{err}").to_lowercase().contains("not"));
    }

    #[tokio::test]
    async fn show_databases_in_catalog_lists_namespaces() {
        use datafusion::arrow::array::{Array, StringArray};
        let engine = crate::Engine::new();
        engine.register_catalog(
            "fake",
            Arc::new(FakeCatalog {
                location: "file:///nonexistent".to_string(),
            }),
        );
        let batches = engine.sql("SHOW DATABASES IN fake").await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "namespace");
        let ns = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let got: Vec<&str> = (0..ns.len()).map(|i| ns.value(i)).collect();
        assert_eq!(got, vec!["ns"]);
    }

    #[tokio::test]
    async fn show_tables_in_namespace_lists_tables() {
        use datafusion::arrow::array::{Array, StringArray};
        use datafusion::arrow::datatypes::DataType;
        let engine = crate::Engine::new();
        engine.register_catalog(
            "fake",
            Arc::new(FakeCatalog {
                location: "file:///nonexistent".to_string(),
            }),
        );
        let batches = engine.sql("SHOW TABLES IN fake.ns").await.unwrap();
        assert_eq!(batches.len(), 1);
        // Exact 3-column Spark schema, names + types.
        let schema = batches[0].schema();
        assert_eq!(schema.field(0).name(), "namespace");
        assert_eq!(schema.field(1).name(), "tableName");
        assert_eq!(schema.field(2).name(), "isTemporary");
        assert_eq!(schema.field(2).data_type(), &DataType::Boolean);
        let names = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let got: Vec<&str> = (0..names.len()).map(|i| names.value(i)).collect();
        assert_eq!(got, vec!["orders"]);
    }

    #[tokio::test]
    async fn show_databases_includes_registered_catalog() {
        use datafusion::arrow::array::{Array, StringArray};
        let engine = crate::Engine::new();
        engine.register_catalog(
            "fake",
            Arc::new(FakeCatalog {
                location: "file:///nonexistent".to_string(),
            }),
        );
        let batches = engine.sql("SHOW DATABASES").await.unwrap();
        assert_eq!(batches[0].schema().field(0).name(), "namespace");
        let ns = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let got: Vec<&str> = (0..ns.len()).map(|i| ns.value(i)).collect();
        assert!(got.contains(&"ns"), "expected `ns` in {got:?}");
    }

    /// A fake catalog whose single table `mixed` lives at a fixed location with an *optionally*
    /// declared schema — the lever the coercion test flips.
    struct SchemaCatalog {
        location: String,
        schema: Option<SchemaRef>,
    }

    #[async_trait]
    impl WeftCatalog for SchemaCatalog {
        fn name(&self) -> &str {
            "fake"
        }
        async fn list_namespaces(&self, _parent: &[String]) -> CatResult<Vec<Vec<String>>> {
            Ok(vec![vec!["ns".to_string()]])
        }
        async fn list_tables(&self, _ns: &[String]) -> CatResult<Vec<String>> {
            Ok(vec!["mixed".to_string()])
        }
        async fn load_table(&self, ns: &[String], table: &str) -> CatResult<TableMetadata> {
            if ns == ["ns"] && table == "mixed" {
                let md = TableMetadata::new(
                    "fake.ns.mixed",
                    self.location.clone(),
                    TableFormat::Parquet,
                );
                Ok(match &self.schema {
                    Some(s) => md.with_schema(s.clone()),
                    None => md,
                })
            } else {
                Err(Error::Plan(format!(
                    "no such table: {}.{table}",
                    ns.join(".")
                )))
            }
        }
    }

    /// Write two Parquet files into a fresh dir where column `v` is Int32 in one file and Int64 in
    /// the other — the cross-file type mismatch that breaks schema inference. Returns the dir.
    fn write_mixed_int_parquet_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "weft-mixed-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();

        // File A: v as Int32 (values 1,2,3).
        let schema32 = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        let batch32 = RecordBatch::try_new(
            schema32.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("part-a.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema32, None).unwrap();
        w.write(&batch32).unwrap();
        w.close().unwrap();

        // File B: v as Int64 (values 10,20).
        let schema64 = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let batch64 = RecordBatch::try_new(
            schema64.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20]))],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("part-b.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema64, None).unwrap();
        w.write(&batch64).unwrap();
        w.close().unwrap();

        dir
    }

    /// With a catalog-declared schema (`v: Int64`), the mixed-int-type Parquet files read fine: the
    /// Int32 file is *cast* to Int64 at scan time by DataFusion's default expression adapter, so the
    /// query succeeds. This is the catalog-schema-honoring behavior the change adds.
    #[tokio::test]
    async fn declared_schema_coerces_mixed_file_types() {
        let dir = write_mixed_int_parquet_dir();
        let location = format!("file://{}", dir.to_string_lossy());
        let declared = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));

        let engine = crate::Engine::new();
        engine.register_catalog(
            "fake",
            Arc::new(SchemaCatalog {
                location,
                schema: Some(declared),
            }),
        );

        let batches = engine
            .sql("SELECT COUNT(*) AS c, SUM(v) AS s FROM fake.ns.mixed")
            .await
            .expect("query with declared schema should succeed");
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
        assert_eq!((c, s), (5, 36)); // 1+2+3+10+20
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Control: *without* the declared schema, the same mixed-int-type files reproduce DataFusion's
    /// schema-merge failure — proving the declared schema is what makes the read work.
    #[tokio::test]
    async fn without_declared_schema_merge_fails() {
        let dir = write_mixed_int_parquet_dir();
        let location = format!("file://{}", dir.to_string_lossy());

        let engine = crate::Engine::new();
        engine.register_catalog(
            "fake",
            Arc::new(SchemaCatalog {
                location,
                schema: None,
            }),
        );

        let err = engine
            .sql("SELECT SUM(v) AS s FROM fake.ns.mixed")
            .await
            .expect_err("inference should fail to merge Int32 vs Int64");
        let msg = format!("{err}").to_lowercase();
        assert!(
            msg.contains("merge") || msg.contains("does not equal") || msg.contains("data type"),
            "expected a schema-merge error, got: {err}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Write two Parquet files whose column is named `VendorID` (mixed case) — Int32 in one, Int64
    /// in the other — mimicking real NYC-taxi monthly dumps. Glue would declare this column as the
    /// lowercase `vendorid`, so the file→table name match must be case-insensitive.
    fn write_mixedcase_int_parquet_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "weft-mixedcase-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();

        let schema32 = Arc::new(Schema::new(vec![Field::new(
            "VendorID",
            DataType::Int32,
            true,
        )]));
        let batch32 = RecordBatch::try_new(
            schema32.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("part-a.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema32, None).unwrap();
        w.write(&batch32).unwrap();
        w.close().unwrap();

        let schema64 = Arc::new(Schema::new(vec![Field::new(
            "VendorID",
            DataType::Int64,
            true,
        )]));
        let batch64 = RecordBatch::try_new(
            schema64.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20]))],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("part-b.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema64, None).unwrap();
        w.write(&batch64).unwrap();
        w.close().unwrap();

        dir
    }

    /// Databricks/Athena parity: a lowercase catalog column (`vendorid`) binds to the mixed-case
    /// file column (`VendorID`) case-insensitively, *and* the Int32 file is cast to the declared
    /// Int64 — so `SUM(vendorid)` returns the correct non-null total instead of NULL.
    #[tokio::test]
    async fn declared_schema_matches_columns_case_insensitively() {
        let dir = write_mixedcase_int_parquet_dir();
        let location = format!("file://{}", dir.to_string_lossy());
        // Glue-style lowercase declared name, widened to Int64.
        let declared = Arc::new(Schema::new(vec![Field::new(
            "vendorid",
            DataType::Int64,
            true,
        )]));

        let engine = crate::Engine::new();
        engine.register_catalog(
            "fake",
            Arc::new(SchemaCatalog {
                location,
                schema: Some(declared),
            }),
        );

        let batches = engine
            .sql("SELECT COUNT(vendorid) AS c, SUM(vendorid) AS s FROM fake.ns.mixed")
            .await
            .expect("case-insensitive declared-schema query should succeed");
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
        // All 5 rows resolved (not NULL) and summed across both physical types.
        assert_eq!((c, s), (5, 36)); // 1+2+3+10+20
        let _ = std::fs::remove_dir_all(&dir);
    }
}
