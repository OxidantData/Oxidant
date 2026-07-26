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
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::datasource::{MemTable, TableProvider};
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

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DfResult<Option<Arc<dyn TableProvider>>> {
        let catalog = self.catalog.clone();
        let namespace = self.namespace.clone();
        let ctx = self.ctx.clone();
        let name_for_worker = name.clone();

        // `register_table` is a sync fn (DataFusion's trait), but the write path is all async
        // (Glue CLI / Hive Thrift / object-store puts). `Handle::current().block_on(...)` would
        // panic under a single-thread runtime (e.g. plain `#[tokio::test]`, used throughout this
        // file's own tests) — so this dispatches to a single persistent background worker thread
        // (see `ctas_writer`) instead of spawning a fresh OS thread + runtime per call, which is
        // safe under any caller runtime flavor but also bounds CTAS write concurrency to one at a
        // time process-wide (a deliberately rare, non-hot-path DDL operation).
        let provider = ctas_writer().run(move |rt| {
            rt.block_on(register_table_async(
                catalog,
                ctx,
                namespace,
                name_for_worker,
                table,
            ))
        })??;

        self.tables
            .lock()
            .expect("tables poisoned")
            .insert(name, provider.clone());
        Ok(Some(provider))
    }
}

/// A single persistent background thread (created lazily, once, for the process lifetime) with
/// its own `current_thread` Tokio runtime, used to run CTAS write futures from `register_table`'s
/// sync entry point without spawning a new OS thread + runtime on every call.
type CtasJob = Box<dyn FnOnce(&tokio::runtime::Runtime) + Send>;

struct CtasWriter {
    jobs: std::sync::mpsc::Sender<CtasJob>,
}

impl CtasWriter {
    fn new() -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<CtasJob>();
        std::thread::Builder::new()
            .name("weft-ctas-writer".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build CTAS writer runtime");
                for job in rx {
                    // A panicking job must not take this thread down with it — every other
                    // catalog/session shares this single process-wide writer, so one bad CTAS
                    // (e.g. an internal panic in a dependency) would otherwise permanently break
                    // CTAS writes for everyone until the process restarts. `run`'s caller already
                    // gets a clean "CTAS writer thread died" error for THIS call (the boxed job's
                    // `result_tx` is dropped mid-unwind, closing its channel), so the only extra
                    // work needed here is keeping the loop itself alive for the NEXT job.
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job(&rt)));
                }
            })
            .expect("spawn CTAS writer thread");
        Self { jobs: tx }
    }

    /// Run `f` (which calls `rt.block_on(...)` itself) on the writer thread and block the caller
    /// until it completes, returning its result.
    fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&tokio::runtime::Runtime) -> T + Send + 'static,
    ) -> DfResult<T> {
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        self.jobs
            .send(Box::new(move |rt| {
                let _ = result_tx.send(f(rt));
            }))
            .map_err(|_| {
                DataFusionError::Execution("CTAS writer thread unavailable".to_string())
            })?;
        result_rx
            .recv()
            .map_err(|_| DataFusionError::Execution("CTAS writer thread died".to_string()))
    }
}

fn ctas_writer() -> &'static CtasWriter {
    static WRITER: std::sync::OnceLock<CtasWriter> = std::sync::OnceLock::new();
    WRITER.get_or_init(CtasWriter::new)
}

/// The async body of `WeftSchemaProvider::register_table`: extract the CTAS result's schema and
/// data from `table` (always a `MemTable` — what DataFusion's native `CREATE TABLE ... AS SELECT`
/// produces), ask the catalog to declare the table (`CatalogProvider::create_table`), physically
/// write the data to the resolved location, then build a REAL `TableProvider` over those durable
/// files (not the transient `MemTable`) so a subsequent `SELECT` — same session or a new one —
/// reads genuine external-catalog data.
async fn register_table_async(
    catalog: Arc<dyn WeftCatalog>,
    ctx: Arc<SessionContext>,
    namespace: Vec<String>,
    name: String,
    table: Arc<dyn TableProvider>,
) -> DfResult<Arc<dyn TableProvider>> {
    let (schema, batches) = extract_mem_table_data(&table).await?;

    let metadata = catalog
        .create_table(
            &namespace,
            &name,
            schema.clone(),
            TableFormat::Parquet,
            None,
            &[],
        )
        .await
        .map_err(weft_to_df)?;

    let state = ctx.state();
    write_batches_to_location(
        &state,
        &metadata.location,
        metadata.format,
        &schema,
        batches,
        &metadata.storage_options,
    )
    .await?;

    metadata_to_provider(&state, &metadata, &name).await
}

/// Extract `(schema, batches)` from a `TableProvider` that's always a `MemTable` on this path
/// (DataFusion's `CreateMemoryTable` DDL handling always wraps the CTAS `SELECT`'s output that
/// way before calling `register_table`). Falls back to a full `scan` + `collect` if that ever
/// changes, so this doesn't silently break on a DataFusion upgrade.
async fn extract_mem_table_data(
    table: &Arc<dyn TableProvider>,
) -> DfResult<(SchemaRef, Vec<RecordBatch>)> {
    // `TableProvider: Any` (a supertrait), so a `&dyn TableProvider` upcasts to `&dyn Any` for
    // downcasting — this DataFusion version doesn't expose a dedicated `as_any()` method.
    let any: &dyn std::any::Any = table.as_ref();
    if let Some(mem) = any.downcast_ref::<MemTable>() {
        let schema = mem.schema();
        let mut batches = Vec::new();
        for partition in &mem.batches {
            batches.extend(partition.read().await.iter().cloned());
        }
        return Ok((schema, batches));
    }
    // Defensive fallback: scan the provider directly.
    let ctx = SessionContext::new();
    let state = ctx.state();
    let plan = table.scan(&state, None, &[], None).await?;
    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok((table.schema(), batches))
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
            ensure_remote_store(state, &url, Some(&md.storage_options))?;
            let opts = ListingOptions::new(Arc::new(ParquetFormat::default()))
                .with_file_extension(".parquet");
            let (opts, file_schema) = apply_partition_columns(opts, md);
            sharded_listing_table(state, vec![url], opts, file_schema, table_name, ".parquet").await
        }
        TableFormat::Csv => {
            let loc = crate::shard::ensure_collection_url(&md.location);
            let url = ListingTableUrl::parse(&loc).map_err(loc_err(md))?;
            ensure_remote_store(state, &url, Some(&md.storage_options))?;
            let opts =
                ListingOptions::new(Arc::new(CsvFormat::default())).with_file_extension(".csv");
            let (opts, file_schema) = apply_partition_columns(opts, md);
            sharded_listing_table(state, vec![url], opts, file_schema, table_name, ".csv").await
        }
        TableFormat::Json => {
            let loc = crate::shard::ensure_collection_url(&md.location);
            let url = ListingTableUrl::parse(&loc).map_err(loc_err(md))?;
            ensure_remote_store(state, &url, Some(&md.storage_options))?;
            let opts =
                ListingOptions::new(Arc::new(JsonFormat::default())).with_file_extension(".json");
            let (opts, file_schema) = apply_partition_columns(opts, md);
            sharded_listing_table(state, vec![url], opts, file_schema, table_name, ".json").await
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

/// Configure Hive-style partition columns on a listing table. Glue (and other Hive metastores)
/// append partition columns to the declared schema, but their values live in the object *path*
/// (e.g. `.../year=2015/month=01/part.parquet`), not inside the data files. So we (1) declare them
/// as table partition columns on the `ListingOptions` — DataFusion derives their values from the
/// path — and (2) hand `build_listing_table` the *file* schema with those columns removed, so the
/// reader doesn't look for them in the files. Without a declared schema (Parquet inference) or with
/// no partition columns, this is a no-op passing the metadata schema through unchanged.
fn apply_partition_columns(
    opts: datafusion::datasource::listing::ListingOptions,
    md: &TableMetadata,
) -> (
    datafusion::datasource::listing::ListingOptions,
    Option<SchemaRef>,
) {
    match &md.schema {
        Some(schema) if !md.partition_columns.is_empty() => {
            let (file_schema, part_cols) = split_partition_schema(schema, &md.partition_columns);
            (opts.with_table_partition_cols(part_cols), Some(file_schema))
        }
        _ => (opts, md.schema.clone()),
    }
}

/// Split a Hive-partitioned table's declared schema into `(file_schema, partition_cols)`: the file
/// schema is every field that is *not* a partition column, and `partition_cols` is the
/// `(name, type)` pairs for the partition columns, emitted in the declared partition order. Types
/// come from the declared schema (Glue records them on `PartitionKeys`).
fn split_partition_schema(
    schema: &SchemaRef,
    partition_columns: &[String],
) -> (
    SchemaRef,
    Vec<(String, datafusion::arrow::datatypes::DataType)>,
) {
    use datafusion::arrow::datatypes::Schema;
    let part_set: std::collections::HashSet<&str> =
        partition_columns.iter().map(String::as_str).collect();
    let mut file_fields = Vec::new();
    let mut part_types = HashMap::new();
    for f in schema.fields() {
        if part_set.contains(f.name().as_str()) {
            part_types.insert(f.name().clone(), f.data_type().clone());
        } else {
            file_fields.push(f.clone());
        }
    }
    let part_cols = partition_columns
        .iter()
        .filter_map(|n| part_types.get(n).map(|dt| (n.clone(), dt.clone())))
        .collect();
    (Arc::new(Schema::new(file_fields)), part_cols)
}

/// Tracks which assumed-role identity (if any) each S3 bucket was registered with, for the
/// lifetime of this process. DataFusion's object-store registry (`RuntimeEnv::register_object_store`)
/// is keyed purely by `scheme://authority` (i.e. just the bucket) — it has no concept of two
/// different credential identities coexisting for the same bucket within one session. If table A
/// and table B live in the same bucket but declare different `fs.s3a.assumed.role.arn` values (or
/// one declares one and the other doesn't), whichever is resolved first silently decides the
/// identity for BOTH for the rest of the session unless something checks for the mismatch — this
/// map is that check (see `ensure_remote_store`). One weft engine process backs one cluster
/// (`weft-cl-<id>`), so process-wide scope here matches the session it's actually protecting.
static REGISTERED_BUCKET_ROLES: std::sync::Mutex<Option<HashMap<String, Option<String>>>> =
    std::sync::Mutex::new(None);

/// Ensure an object store is registered on the session's runtime for a remote table location so
/// DataFusion can read it. Currently handles `s3://` — credentials come from the environment or the
/// EC2 instance role (IMDS) via object_store's default provider; no static keys, UNLESS
/// `storage_options` names `fs.s3a.assumed.role.arn` (Hadoop-AWS's assume-role config, resolved to
/// a temporary session via `crate::assume_role_credentials::AssumeRoleCredentialProvider` — see
/// its module docs). Registering on the shared runtime is idempotent and persists for the session,
/// so query-time resolution finds it. `file://` and bare paths need nothing and are skipped.
///
/// Errors (rather than silently proceeding) if `bucket` was already registered under a DIFFERENT
/// assumed-role identity than this call requests — seeing `Ok(())` from this function is a
/// guarantee that the session's registered store for this bucket matches `storage_options`, not
/// just "some store exists for this bucket." See `REGISTERED_BUCKET_ROLES`'s doc comment for why
/// that guarantee needs an explicit check instead of being automatic.
fn ensure_remote_store(
    state: &SessionState,
    url: &datafusion::datasource::listing::ListingTableUrl,
    storage_options: Option<&HashMap<String, String>>,
) -> DfResult<()> {
    if url.scheme() != "s3" {
        return Ok(());
    }
    let os_url = url.object_store(); // canonical `s3://bucket` key
                                     // `os_url` is the canonical `s3://bucket/` — pull the bucket from the authority.
    let bucket = os_url
        .as_str()
        .strip_prefix("s3://")
        .and_then(|r| r.split('/').next())
        .unwrap_or("")
        .to_string();
    if bucket.is_empty() {
        return Ok(());
    }
    let requested_role = storage_options
        .and_then(|opts| opts.get(crate::assume_role_credentials::ASSUMED_ROLE_ARN_KEY))
        .cloned();

    if state.runtime_env().object_store(&os_url).is_ok() {
        // Already registered for this bucket — confirm it was registered with the SAME identity
        // this call is asking for. A mismatch means two tables in this bucket disagree on which
        // role to assume, which DataFusion's registry has no way to honor simultaneously — that's
        // a real misconfiguration to surface, not something to paper over by silently keeping
        // whichever table happened to resolve first.
        let mut registry = REGISTERED_BUCKET_ROLES
            .lock()
            .expect("bucket-role registry poisoned");
        let map = registry.get_or_insert_with(HashMap::new);
        return match map.get(&bucket) {
            Some(registered) if *registered == requested_role => Ok(()),
            // Registered before this tracking map existed in this process's lifetime (shouldn't
            // happen in practice — both paths go through this same function — but fails open
            // rather than blocking reads that were working fine before this check existed).
            None => Ok(()),
            Some(registered) => Err(DataFusionError::Plan(format!(
                "bucket `{bucket}` is already registered in this session using a different S3 \
                 identity (assumed role {registered:?}) than this table requests \
                 ({requested_role:?}) — DataFusion can only have one active identity per bucket \
                 per session; two tables in the same bucket must agree on `fs.s3a.assumed.role.arn`"
            ))),
        };
    }

    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-west-2".to_string());
    let mut builder = object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(&bucket)
        .with_region(region.clone());
    if let Some(role_arn) = &requested_role {
        let session_name = storage_options
            .and_then(|opts| {
                opts.get(crate::assume_role_credentials::ASSUMED_ROLE_SESSION_NAME_KEY)
            })
            .cloned();
        let provider = crate::assume_role_credentials::AssumeRoleCredentialProvider::new(
            role_arn.clone(),
            session_name,
            region,
        );
        builder = builder.with_credentials(std::sync::Arc::new(provider));
    }
    match builder.build() {
        Ok(store) => {
            state
                .runtime_env()
                .register_object_store(os_url.as_ref(), Arc::new(store));
            REGISTERED_BUCKET_ROLES
                .lock()
                .expect("bucket-role registry poisoned")
                .get_or_insert_with(HashMap::new)
                .insert(bucket, requested_role);
            Ok(())
        }
        Err(e) => {
            eprintln!("warn: could not register S3 object store for `{bucket}`: {e}");
            Ok(())
        }
    }
}

/// Write `batches` as a single file at `location` in `format` (Parquet/Csv/Json — the only CTAS
/// write targets; any other format is a bug upstream since `hive_types::format_serde` already
/// rejects Delta/Iceberg before a catalog's `create_table` is ever called). Serializes in memory
/// then `put`s through the session's `object_store` for `location`'s scheme, so this works for
/// `s3://` (registered via [`ensure_remote_store`]) exactly like `file://`/bare local paths
/// (DataFusion's default object-store registry resolves those to `LocalFileSystem` with no
/// explicit registration needed) — unlike the local-only `ArrowWriter`-to-`std::fs::File` CTAS
/// writer used by the (unrelated) local-warehouse `CREATE TABLE ... USING <fmt>` path.
async fn write_batches_to_location(
    state: &SessionState,
    location: &str,
    format: TableFormat,
    schema: &SchemaRef,
    batches: Vec<RecordBatch>,
    storage_options: &HashMap<String, String>,
) -> DfResult<()> {
    use datafusion::datasource::listing::ListingTableUrl;
    use object_store::ObjectStoreExt;

    let url = ListingTableUrl::parse(location)
        .map_err(|e| DataFusionError::Plan(format!("bad table location `{location}`: {e}")))?;
    ensure_remote_store(state, &url, Some(storage_options))?;
    let store = state.runtime_env().object_store(&url)?;

    let ext = match format {
        TableFormat::Parquet => "parquet",
        TableFormat::Csv => "csv",
        TableFormat::Json => "json",
        TableFormat::Delta | TableFormat::Iceberg => {
            return Err(DataFusionError::NotImplemented(format!(
                "{format:?} is not a supported CTAS write target"
            )));
        }
    };
    let bytes = encode_batches(format, schema, &batches)?;
    let path = url.prefix().clone().join(format!("part-00000.{ext}"));
    store
        .put(&path, bytes.into())
        .await
        .map_err(|e| DataFusionError::Execution(format!("write `{location}`: {e}")))?;
    Ok(())
}

/// Serialize `batches` into an in-memory buffer in `format` (Parquet/Csv/Json).
fn encode_batches(
    format: TableFormat,
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> DfResult<Vec<u8>> {
    let mut buf = Vec::new();
    match format {
        TableFormat::Parquet => {
            let mut writer =
                datafusion::parquet::arrow::ArrowWriter::try_new(&mut buf, schema.clone(), None)
                    .map_err(|e| {
                        DataFusionError::Execution(format!("build parquet writer: {e}"))
                    })?;
            for b in batches {
                writer
                    .write(b)
                    .map_err(|e| DataFusionError::Execution(format!("write parquet batch: {e}")))?;
            }
            writer
                .close()
                .map_err(|e| DataFusionError::Execution(format!("close parquet writer: {e}")))?;
        }
        TableFormat::Csv => {
            let mut writer = datafusion::arrow::csv::Writer::new(&mut buf);
            for b in batches {
                writer
                    .write(b)
                    .map_err(|e| DataFusionError::Execution(format!("write csv batch: {e}")))?;
            }
        }
        TableFormat::Json => {
            let mut writer = datafusion::arrow::json::LineDelimitedWriter::new(&mut buf);
            for b in batches {
                writer
                    .write(b)
                    .map_err(|e| DataFusionError::Execution(format!("write json batch: {e}")))?;
            }
            writer
                .finish()
                .map_err(|e| DataFusionError::Execution(format!("finish json writer: {e}")))?;
        }
        TableFormat::Delta | TableFormat::Iceberg => {
            return Err(DataFusionError::NotImplemented(format!(
                "{format:?} is not a supported CTAS write target"
            )));
        }
    }
    Ok(buf)
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

    /// Write a Hive-partitioned parquet layout: `<dir>/region=<r>/part-0.parquet`, each file
    /// holding only the DATA column `x` (the partition column `region` lives in the path).
    fn write_partitioned_parquet_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("weft-cat-part-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let file_schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        for (region, vals) in [("west", vec![1_i64, 2]), ("east", vec![10, 20, 30])] {
            let pdir = dir.join(format!("region={region}"));
            std::fs::create_dir_all(&pdir).unwrap();
            let batch =
                RecordBatch::try_new(file_schema.clone(), vec![Arc::new(Int64Array::from(vals))])
                    .unwrap();
            let f = std::fs::File::create(pdir.join("part-0.parquet")).unwrap();
            let mut w = ArrowWriter::try_new(f, file_schema.clone(), None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
        dir
    }

    /// A fake catalog exposing one Hive-partitioned table whose declared schema is `x` + the
    /// partition column `region` (Glue's convention: partition columns appended to the schema).
    struct PartitionedFakeCatalog {
        location: String,
    }

    #[async_trait]
    impl WeftCatalog for PartitionedFakeCatalog {
        fn name(&self) -> &str {
            "fakepart"
        }
        async fn list_namespaces(&self, _parent: &[String]) -> CatResult<Vec<Vec<String>>> {
            Ok(vec![vec!["ns".to_string()]])
        }
        async fn list_tables(&self, _ns: &[String]) -> CatResult<Vec<String>> {
            Ok(vec!["events".to_string()])
        }
        async fn load_table(&self, ns: &[String], table: &str) -> CatResult<TableMetadata> {
            if ns == ["ns"] && table == "events" {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("x", DataType::Int64, false),
                    Field::new("region", DataType::Utf8, false),
                ]));
                Ok(TableMetadata::new(
                    "fakepart.ns.events",
                    self.location.clone(),
                    TableFormat::Parquet,
                )
                .with_schema(schema)
                .with_partition_columns(vec!["region".to_string()]))
            } else {
                Err(Error::Plan(format!(
                    "no such table: {}.{table}",
                    ns.join(".")
                )))
            }
        }
    }

    #[tokio::test]
    async fn hive_partitioned_read_derives_partition_column_from_path() {
        // The partition column `region` is in the *path*, not the data files. Before A4 it was in
        // the declared schema but never registered as a table partition column, so it scanned as
        // NULL (or failed). Now a filter on it must prune to the matching partition and sum only
        // that partition's rows.
        let dir = write_partitioned_parquet_dir();
        let location = format!("file://{}", dir.to_string_lossy());
        let engine = crate::Engine::new();
        engine.register_catalog("fakepart", Arc::new(PartitionedFakeCatalog { location }));

        let west = engine
            .sql("SELECT SUM(x) AS s FROM fakepart.ns.events WHERE region = 'west'")
            .await
            .unwrap();
        let s = west[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // If `region` scanned as NULL (the pre-A4 bug), the filter matches nothing and SUM is NULL
        // (value 0); a correct partition-from-path read sums only the `region=west` rows.
        assert_eq!(
            s.value(0),
            3,
            "west partition sums 1 + 2 (region derived from the path)"
        );

        let total = engine
            .sql("SELECT SUM(x) AS s FROM fakepart.ns.events")
            .await
            .unwrap();
        let t = total[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(t, 63, "all partitions: 1+2 + 10+20+30");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A fake EXTERNAL catalog whose `create_table` writes to a local temp dir (no real Glue/Hive
    /// — exercises the `register_table` write path end to end: downcast the `MemTable` → declare
    /// the table → write real Parquet files → build a durable `ListingTable` provider).
    struct WritableFakeCatalog {
        dir: std::path::PathBuf,
    }

    #[async_trait]
    impl WeftCatalog for WritableFakeCatalog {
        fn name(&self) -> &str {
            "fakewrite"
        }
        async fn list_namespaces(&self, _parent: &[String]) -> CatResult<Vec<Vec<String>>> {
            Ok(vec![vec!["ns".to_string()]])
        }
        async fn list_tables(&self, _ns: &[String]) -> CatResult<Vec<String>> {
            Ok(vec![])
        }
        async fn load_table(&self, ns: &[String], table: &str) -> CatResult<TableMetadata> {
            // Real Glue/Hive would already know about a table `create_table` just declared; this
            // fake mimics that by checking whether `create_table`'s write path actually landed
            // files under the same location convention it used.
            let db = ns.first().cloned().unwrap_or_default();
            let dir = self.dir.join(&db).join(table);
            if dir.is_dir() {
                Ok(TableMetadata::new(
                    format!("fakewrite.{db}.{table}"),
                    format!("file://{}/", dir.to_string_lossy()),
                    TableFormat::Parquet,
                ))
            } else {
                Err(Error::Plan(format!(
                    "no such table: {}.{table}",
                    ns.join(".")
                )))
            }
        }
        async fn create_table(
            &self,
            namespace: &[String],
            table: &str,
            schema: SchemaRef,
            format: TableFormat,
            location: Option<String>,
            partition_columns: &[String],
        ) -> CatResult<TableMetadata> {
            let db = namespace.first().cloned().unwrap_or_default();
            let location = location
                .unwrap_or_else(|| format!("file://{}/{db}/{table}/", self.dir.to_string_lossy()));
            Ok(
                TableMetadata::new(format!("fakewrite.{db}.{table}"), location, format)
                    .with_schema(schema)
                    .with_partition_columns(partition_columns.to_vec()),
            )
        }
    }

    #[tokio::test]
    async fn ctas_against_external_catalog_writes_durable_data() {
        let base = std::env::temp_dir().join(format!("weft-cat-write-{}", std::process::id()));

        {
            let engine = crate::Engine::new();
            engine.register_catalog(
                "fakewrite",
                Arc::new(WritableFakeCatalog { dir: base.clone() }),
            );
            // No `USING <fmt>` clause — falls straight through to DataFusion's native
            // `CreateMemoryTable` DDL handling, which is exactly the path that used to fail with
            // "schema provider does not support registering tables" for an external catalog.
            engine
                .sql("CREATE TABLE fakewrite.ns.newtable AS SELECT 1 AS x UNION ALL SELECT 2 AS x")
                .await
                .unwrap();
        } // `engine` (and its in-memory MemTable) dropped here.

        // A brand-new Engine/session proves the data is durable on disk, not just cached in the
        // first Engine's transient MemTable.
        let engine2 = crate::Engine::new();
        engine2.register_catalog(
            "fakewrite",
            Arc::new(WritableFakeCatalog { dir: base.clone() }),
        );
        let batches = engine2
            .sql("SELECT SUM(x) AS s FROM fakewrite.ns.newtable")
            .await
            .unwrap();
        let s = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(s, 3);
        let _ = std::fs::remove_dir_all(&base);
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

    /// Regression test for the bug a code review caught: DataFusion's object-store registry is
    /// keyed purely by bucket, so registering a second, differently-configured (or unconfigured)
    /// identity for a bucket that already has one registered must be rejected, not silently
    /// accepted with the FIRST table's identity — otherwise two tables in one bucket with
    /// different `fs.s3a.assumed.role.arn` values would silently share whichever one resolved
    /// first, a real cross-permission-boundary bug.
    #[tokio::test]
    async fn ensure_remote_store_rejects_mismatched_role_for_already_registered_bucket() {
        use datafusion::datasource::listing::ListingTableUrl;

        let ctx = SessionContext::new();
        let state = ctx.state();
        // Bucket name unique to this test — REGISTERED_BUCKET_ROLES is a process-wide static
        // shared across every test in this binary, so reusing a name any other test touches would
        // make this test's outcome depend on test execution order.
        let url = ListingTableUrl::parse("s3://weft-loom-test-mismatch-bucket/table-a/").unwrap();

        let mut opts_role_a = HashMap::new();
        opts_role_a.insert(
            crate::assume_role_credentials::ASSUMED_ROLE_ARN_KEY.to_string(),
            "arn:aws:iam::123456789012:role/weft-poolctl/role-a".to_string(),
        );
        ensure_remote_store(&state, &url, Some(&opts_role_a))
            .expect("first registration for a fresh bucket must succeed");

        // Same bucket, different assumed role — DataFusion has no way to honor both
        // simultaneously, so this must be a loud error, not a silent reuse of role-a's identity.
        let mut opts_role_b = HashMap::new();
        opts_role_b.insert(
            crate::assume_role_credentials::ASSUMED_ROLE_ARN_KEY.to_string(),
            "arn:aws:iam::123456789012:role/weft-poolctl/role-b".to_string(),
        );
        let err = ensure_remote_store(&state, &url, Some(&opts_role_b))
            .expect_err("a second, conflicting role for the same bucket must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("role-a"),
            "error should name the already-registered role: {msg}"
        );
        assert!(
            msg.contains("role-b"),
            "error should name the conflicting requested role: {msg}"
        );

        // Same bucket, same role again — must succeed (idempotent, not just "first wins").
        ensure_remote_store(&state, &url, Some(&opts_role_a))
            .expect("re-requesting the SAME role for an already-registered bucket must succeed");

        // Same bucket, no role requested this time (e.g. a second table with no assume-role
        // config) — also a mismatch against the registered role-a identity, must be rejected.
        let err = ensure_remote_store(&state, &url, None).expect_err(
            "no-role-requested must be rejected when the bucket is already role-scoped",
        );
        assert!(
            format!("{err}").contains("role-a"),
            "error should name the already-registered role"
        );
    }

    #[test]
    fn local_path_accepts_file_uri_forms_and_bare_paths() {
        // RFC `file:///abs` and Hive Metastore's single-slash `file:/abs`.
        assert_eq!(
            local_path("file:///tmp/delta/table").unwrap(),
            "/tmp/delta/table"
        );
        assert_eq!(
            local_path("file:/tmp/delta/table").unwrap(),
            "/tmp/delta/table"
        );
        assert_eq!(local_path("/tmp/delta/table").unwrap(), "/tmp/delta/table");
    }

    #[test]
    fn local_path_rejects_non_file_schemes() {
        let err = local_path("s3://bucket/path").expect_err("s3 must not look local");
        let msg = format!("{err}");
        assert!(
            msg.contains("s3://") || msg.contains("`s3://`"),
            "error should name the scheme: {msg}"
        );
        assert!(local_path("hdfs://nn/path").is_err());
    }

    #[test]
    fn split_partition_schema_preserves_partition_order_and_drops_file_fields() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("amount", DataType::Int32, true),
            Field::new("year", DataType::Int32, false),
        ]));
        let (file_schema, part_cols) =
            split_partition_schema(&schema, &["year".into(), "region".into()]);

        assert_eq!(
            file_schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            vec!["id", "amount"]
        );
        // Partition columns must follow the *declared partition order*, not schema field order.
        assert_eq!(
            part_cols
                .iter()
                .map(|(n, dt)| (n.as_str(), dt.clone()))
                .collect::<Vec<_>>(),
            vec![("year", DataType::Int32), ("region", DataType::Utf8),]
        );
    }

    #[test]
    fn split_partition_schema_skips_partition_names_missing_from_schema() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
        ]));
        let (file_schema, part_cols) =
            split_partition_schema(&schema, &["region".into(), "missing".into()]);

        assert_eq!(file_schema.fields().len(), 1);
        assert_eq!(file_schema.field(0).name(), "id");
        assert_eq!(part_cols.len(), 1);
        assert_eq!(part_cols[0].0, "region");
    }
}
