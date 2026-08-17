//! The datalake sink: micro-batches become durable table data, committed to a catalog.
//!
//! This is the half of Structured Streaming that makes a stream useful to a dashboard — a
//! *live table*. Each micro-batch writes one Parquet data file and then commits it, so a reader
//! that queries the table between batches sees a consistent, whole number of batches and never a
//! half-written file:
//!
//! - **Delta** (`format("delta")`) commits the file into `_delta_log/`. The transaction log is
//!   the authority on which files are live, so the commit is atomic and a dashboard polling the
//!   table gets read-your-writes freshness with no coordination. This is the default and the
//!   recommended target.
//! - **Parquet** (`format("parquet")`) just drops the file in the table directory — Hive-style.
//!   Cheaper, but a reader listing mid-write can see a partial file, so it is only appropriate
//!   for a sink nothing queries concurrently.
//!
//! The catalog side is the Glue path: the target database (the "schema") is created if missing,
//! then the table, then every batch appends. Keeping streaming tables in their **own database**
//! is the intended usage — a live table's write rate and freshness expectations are nothing like
//! a batch-loaded one's, and a separate schema keeps the two from sharing a blast radius.

use std::collections::HashMap;
use std::sync::Arc;

use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use oxidant_catalog::{CatalogProvider, TableChange, TableFormat};
use oxidant_common::{Error, Result};
use oxidant_datasource::delta_write::{DeltaTableWriter, DeltaWriterConfig};
use oxidant_datasource::uniform::UniformTable;
use oxidant_loom::arrow::datatypes::SchemaRef;
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;

use crate::sink::Sink;

/// Everything about a sink that is not the table's identity.
#[derive(Debug, Clone, Default)]
pub struct LakeSinkOptions {
    /// Delta `txn.appId` for idempotent commits — the streaming query id, which survives a
    /// restart. `None` writes no `txn` action and gives up replay deduplication.
    pub app_id: Option<String>,
    /// `writeStream.partitionBy(...)`: columns written as directories rather than into the file.
    pub partition_columns: Vec<String>,
    /// Publish Iceberg metadata over the Delta table so Iceberg engines can read it too.
    pub publish_iceberg: bool,
    /// Name of the sibling Iceberg catalog entry, when one is registered.
    pub iceberg_table_suffix: String,
    /// Commits between Delta checkpoints (and Iceberg publishes). `0` disables both.
    pub checkpoint_interval: u64,
}

/// Where a streaming query materializes its output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LakeTarget {
    /// Registered external catalog (e.g. `glue`). `None` writes to `location` without declaring
    /// the table anywhere — the `writeStream.start(path)` form.
    pub catalog: Option<String>,
    /// Database / schema the table lives in. Empty for a location-only sink.
    pub namespace: Vec<String>,
    /// Table name. Empty for a location-only sink.
    pub table: String,
    /// Physical table format. Only `Delta` and `Parquet` are writable.
    pub format: TableFormat,
    /// Explicit table root. Required without a catalog; otherwise the catalog's warehouse
    /// convention decides.
    pub location: Option<String>,
}

impl LakeTarget {
    /// Parse a `writeStream.toTable("catalog.db.table")` identifier against the session's current
    /// catalog and namespace, which is how Spark resolves a partially-qualified name.
    pub fn from_table_identifier(
        identifier: &str,
        current_catalog: &str,
        current_namespace: &[String],
        format: TableFormat,
        location: Option<String>,
    ) -> Result<Self> {
        let parts = oxidant_catalog::split_ident(identifier);
        let (catalog, namespace, table) = match parts.len() {
            3 => (parts[0].clone(), vec![parts[1].clone()], parts[2].clone()),
            2 => (
                current_catalog.to_string(),
                vec![parts[0].clone()],
                parts[1].clone(),
            ),
            1 => {
                let ns = current_namespace.to_vec();
                if ns.is_empty() {
                    return Err(Error::Plan(format!(
                        "writeStream.toTable(`{identifier}`): no current database — qualify the \
                         name as `database.table` or run `USE <database>` first"
                    )));
                }
                (current_catalog.to_string(), ns, parts[0].clone())
            }
            _ => {
                return Err(Error::Plan(format!(
                    "writeStream.toTable(`{identifier}`): expected `table`, `database.table`, or \
                     `catalog.database.table`"
                )))
            }
        };
        Ok(Self {
            catalog: Some(catalog),
            namespace,
            table,
            format,
            location,
        })
    }

    /// A sink that writes to a path with no catalog entry.
    pub fn location_only(location: impl Into<String>, format: TableFormat) -> Self {
        Self {
            catalog: None,
            namespace: vec![],
            table: String::new(),
            format,
            location: Some(location.into()),
        }
    }

    fn display_name(&self) -> String {
        match &self.catalog {
            Some(c) => format!("{c}.{}.{}", self.namespace.join("."), self.table),
            None => self.location.clone().unwrap_or_default(),
        }
    }
}

/// Parse a `writeStream.format(...)` string into a writable table format.
pub fn writable_format(format: &str) -> Result<TableFormat> {
    match TableFormat::from_provider(format) {
        Some(f @ (TableFormat::Delta | TableFormat::Parquet)) => Ok(f),
        // Not a gap so much as a redirect: a Delta sink already publishes Iceberg metadata over
        // the same data files, so `format("delta")` gives Iceberg readers the table anyway —
        // without a second copy, and with a transaction log that commits atomically per batch.
        Some(TableFormat::Iceberg) => Err(Error::Unsupported(
            "writeStream.format(\"iceberg\") is not a sink format — use format(\"delta\"), which \
             publishes Iceberg metadata over the same Parquet files. The table is then readable \
             by Spark and Athena as Delta and by Trino, Athena, and DuckDB as Iceberg. Turn the \
             Iceberg side off with .option(\"icebergCompat\", \"false\")."
                .into(),
        )),
        Some(other) => Err(Error::Unsupported(format!(
            "writeStream.format(\"{}\") is not a table sink — use \"delta\" or \"parquet\"",
            format_name(other)
        ))),
        None => Err(Error::Unsupported(format!(
            "unknown writeStream format `{format}` — use \"delta\" or \"parquet\""
        ))),
    }
}

fn format_name(f: TableFormat) -> &'static str {
    match f {
        TableFormat::Parquet => "parquet",
        TableFormat::Delta => "delta",
        TableFormat::Iceberg => "iceberg",
        TableFormat::Csv => "csv",
        TableFormat::Json => "json",
    }
}

/// The format-specific half of a sink.
enum Writer {
    /// Delta: one transaction per batch, with checkpoints, stats, and `txn` idempotency.
    Delta(Box<DeltaTableWriter>),
    /// Bare Parquet: one file per batch, no commit protocol.
    Parquet,
}

/// A sink resolved against its catalog and object store, ready to accept batches.
pub struct LakeSink {
    target: LakeTarget,
    schema: SchemaRef,
    store: Arc<dyn ObjectStore>,
    /// Table root as an object-store prefix (bucket-relative for `s3://`).
    root: ObjectPath,
    writer: Writer,
    options: LakeSinkOptions,
    /// The Iceberg view of this table, when interoperability is on.
    uniform: Option<UniformTable>,
    /// Catalog handle kept so each Iceberg publish can refresh `metadata_location`.
    catalog: Option<Arc<dyn CatalogProvider>>,
    /// Namespace and name of the sibling Iceberg catalog entry, when one was registered.
    iceberg_entry: Option<(Vec<String>, String)>,
    batch_counter: u64,
    location: String,
}

impl LakeSink {
    /// Resolve the target: create the database and table if they do not exist, then bind an
    /// object store to the table's location.
    ///
    /// Done once at query start rather than on the first batch so a misconfigured sink (bad
    /// catalog, no permission to create the database, unwritable bucket) fails the
    /// `writeStream.start()` call — not silently, minutes later, on whichever batch first
    /// carried data.
    pub async fn open(
        engine: &Engine,
        target: LakeTarget,
        schema: SchemaRef,
        options: LakeSinkOptions,
    ) -> Result<Self> {
        if !matches!(target.format, TableFormat::Delta | TableFormat::Parquet) {
            return Err(Error::Unsupported(format!(
                "streaming sink format {:?} is not writable",
                target.format
            )));
        }
        for column in &options.partition_columns {
            if schema.field_with_name(column).is_err() {
                return Err(Error::Plan(format!(
                    "writeStream.partitionBy(`{column}`): the query does not produce that column"
                )));
            }
        }

        let mut catalog_handle = None;
        let (location, storage_options) = match &target.catalog {
            Some(catalog_name) => {
                let catalog = engine.external_catalog(catalog_name).ok_or_else(|| {
                    Error::Plan(format!(
                        "writeStream target catalog `{catalog_name}` is not registered"
                    ))
                })?;
                let db = target.namespace.join(".");

                // The streaming schema is created on demand: a live-table pipeline is usually the
                // first thing to touch its own database, and failing on a missing one would make
                // every deployment a two-step.
                catalog
                    .create_database(&db, true, Some("Oxidant streaming tables".into()), None)
                    .await?;

                let md = if catalog
                    .table_exists(&target.namespace, &target.table)
                    .await?
                {
                    let existing = catalog.load_table(&target.namespace, &target.table).await?;
                    if existing.format != target.format {
                        return Err(Error::Plan(format!(
                            "`{}` already exists as {:?}, but the stream writes {:?} — drop the \
                             table or change writeStream.format(...)",
                            target.display_name(),
                            existing.format,
                            target.format
                        )));
                    }
                    // Appending rows shaped differently from the table would write files a
                    // reader resolves against the declared schema and then misreads by
                    // position. Schema evolution is not automatic — say so at start time.
                    if let Some(declared) = &existing.schema {
                        if let Some(mismatch) = column_mismatch(declared, &schema) {
                            return Err(Error::Plan(format!(
                                "`{}` already exists with a different schema: {mismatch}. \
                                 Streaming appends do not evolve a table's schema — align the \
                                 query's projection, or drop the table.",
                                target.display_name()
                            )));
                        }
                    }
                    existing
                } else {
                    catalog
                        .create_table(
                            &target.namespace,
                            &target.table,
                            schema.clone(),
                            target.format,
                            target.location.clone(),
                            &options.partition_columns,
                        )
                        .await?
                };
                catalog_handle = Some(catalog);
                (md.location, md.storage_options)
            }
            None => {
                let location = target.location.clone().ok_or_else(|| {
                    Error::Plan(
                        "writeStream needs either a table name (`toTable`) or a path (`start`)"
                            .into(),
                    )
                })?;
                (location, HashMap::new())
            }
        };

        let store = engine.object_store_for(&location, &storage_options)?;
        let root = table_root_prefix(&location)?;
        // Deterministic from the table identity, so a restarted query that re-declares version 0
        // of a wiped table does not invent a second table uuid.
        let table_id = stable_table_id(&location);

        // Iceberg interoperability is only meaningful for Delta: a bare Parquet directory has no
        // authoritative file list to mirror.
        let publish_iceberg = options.publish_iceberg && target.format == TableFormat::Delta;
        let uniform = publish_iceberg
            .then(|| UniformTable::new(&location, &schema, &options.partition_columns, &table_id))
            .transpose()?;

        let writer = match target.format {
            TableFormat::Delta => Writer::Delta(Box::new(
                DeltaTableWriter::open(
                    store.clone(),
                    root.clone(),
                    schema.clone(),
                    DeltaWriterConfig {
                        table_id,
                        partition_columns: options.partition_columns.clone(),
                        app_id: options.app_id.clone(),
                        checkpoint_interval: options.checkpoint_interval,
                        ..Default::default()
                    },
                )
                .await?,
            )),
            _ => Writer::Parquet,
        };

        Ok(Self {
            target,
            schema,
            store,
            root,
            writer,
            options,
            uniform,
            catalog: catalog_handle,
            iceberg_entry: None,
            batch_counter: 0,
            location,
        })
    }

    /// Register the sibling Iceberg catalog entry that Iceberg-only engines query.
    ///
    /// One metastore entry cannot be both formats — Athena decides how to read a table from its
    /// `table_type`, and Delta and Iceberg readers want different answers. So the Delta table
    /// stays the primary entry and a second one, pointing at the *same* location and the same
    /// data files, is registered for Iceberg readers.
    ///
    /// Registered on the first publish rather than at query start, because an Iceberg entry with
    /// no `metadata_location` yet is not merely empty — it is an error to every reader that opens
    /// it. Better no table than a broken one.
    async fn register_iceberg_entry(&mut self) -> Result<()> {
        if self.iceberg_entry.is_some() || self.uniform.is_none() {
            return Ok(());
        }
        let (Some(catalog), true) = (&self.catalog, !self.target.table.is_empty()) else {
            return Ok(());
        };
        let name = format!("{}{}", self.target.table, self.options.iceberg_table_suffix);
        if !catalog.table_exists(&self.target.namespace, &name).await? {
            catalog
                .create_table(
                    &self.target.namespace,
                    &name,
                    self.schema.clone(),
                    TableFormat::Iceberg,
                    Some(self.location.clone()),
                    &self.options.partition_columns,
                )
                .await?;
        }
        self.iceberg_entry = Some((self.target.namespace.clone(), name));
        Ok(())
    }

    pub fn location(&self) -> &str {
        &self.location
    }

    /// The Iceberg catalog entry mirroring this table, if one was registered.
    pub fn iceberg_table_name(&self) -> Option<String> {
        self.iceberg_entry
            .as_ref()
            .map(|(ns, table)| format!("{}.{table}", ns.join(".")))
    }

    /// A per-batch file-name fragment. The uuid keeps files from colliding across query restarts
    /// that reset the counter.
    fn next_file_prefix(&mut self) -> String {
        self.batch_counter += 1;
        uuid::Uuid::new_v4().to_string()
    }

    /// Publish Iceberg metadata for the table as of `version`, and point the catalog at it.
    ///
    /// Best-effort by design: the Delta commit already succeeded and is the source of truth, so a
    /// failure here leaves Iceberg readers on the previous snapshot rather than failing the
    /// stream.
    async fn publish_iceberg(&mut self, version: u64) {
        let (Some(uniform), Writer::Delta(writer)) = (&self.uniform, &self.writer) else {
            return;
        };
        let Some(files) = writer.live_files() else {
            return;
        };
        let metadata_location = match uniform
            .publish(self.store.as_ref(), &self.root, files, version)
            .await
        {
            Ok(location) => location,
            Err(e) => {
                eprintln!(
                    "[oxidant] iceberg publish for `{}` at version {version} failed: {e}",
                    self.target.display_name()
                );
                return;
            }
        };

        if let Err(e) = self.register_iceberg_entry().await {
            eprintln!(
                "[oxidant] registering the Iceberg entry for `{}`: {e}",
                self.target.display_name()
            );
            return;
        }

        // The pointer every Iceberg catalog reader resolves first.
        let Some(catalog) = &self.catalog else {
            return;
        };
        let change = vec![TableChange::SetProperties(
            [("metadata_location".to_string(), metadata_location)]
                .into_iter()
                .collect(),
        )];
        let targets = self
            .iceberg_entry
            .iter()
            .map(|(ns, table)| (ns.clone(), table.clone()))
            .chain(std::iter::once((
                self.target.namespace.clone(),
                self.target.table.clone(),
            )));
        for (namespace, table) in targets {
            if table.is_empty() {
                continue;
            }
            if let Err(e) = catalog
                .alter_table(&namespace, &table, change.clone())
                .await
            {
                eprintln!("[oxidant] iceberg metadata_location for `{table}`: {e}");
            }
        }
    }
}

#[async_trait::async_trait]
impl Sink for LakeSink {
    /// Where this sink writes, for progress reporting.
    fn description(&self) -> String {
        format!(
            "{}Sink[{}]",
            format_name(self.target.format),
            self.target.display_name()
        )
    }

    async fn write_batch(&mut self, batches: &[RecordBatch], batch_id: u64) -> Result<u64> {
        let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        // An empty micro-batch must not produce an empty data file or an empty commit: Delta
        // readers would replay a growing log of no-ops, and a dashboard would see the table's
        // version tick with nothing behind it.
        if rows == 0 {
            return Ok(0);
        }
        for b in batches {
            if b.schema() != self.schema {
                return Err(Error::Execution(format!(
                    "streaming sink `{}`: batch schema {:?} does not match the table schema {:?}",
                    self.target.display_name(),
                    b.schema(),
                    self.schema
                )));
            }
        }

        let prefix = self.next_file_prefix();
        let published_at = match &mut self.writer {
            Writer::Delta(writer) => {
                let commit = writer
                    .append(batches, &prefix, Some(batch_id as i64))
                    .await?;
                if commit.deduplicated {
                    // A replay of a batch the log already carries. Reporting zero rows is the
                    // truth: nothing was added to the table.
                    return Ok(0);
                }
                let interval = self.options.checkpoint_interval;
                // Publish on the very first commit, then every `interval` after it. Without the
                // first-commit case a table is not Iceberg-readable *at all* until its tenth
                // micro-batch — the sibling catalog entry does not even exist — so a low-volume
                // table would quietly never gain the interoperability this sink advertises.
                // Later publishes stay on the interval: rewriting the manifest set costs more
                // than writing the data, so it is deliberately amortized.
                let due = interval > 0 && (commit.version + 1) % interval == 0;
                (commit.version == 0 || due).then_some(commit.version)
            }
            Writer::Parquet => {
                let bytes = encode_parquet(&self.schema, batches)?;
                let path = self
                    .root
                    .clone()
                    .join(format!("part-{prefix}-c000.snappy.parquet").as_str());
                self.store
                    .put(&path, bytes.into())
                    .await
                    .map_err(|e| Error::Io(format!("write `{path}`: {e}")))?;
                None
            }
        };
        if let Some(version) = published_at {
            self.publish_iceberg(version).await;
        }
        Ok(rows)
    }
}

fn encode_parquet(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Vec<u8>> {
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::basic::Compression;
    use datafusion::parquet::file::properties::WriterProperties;

    // parquet-rs defaults to UNCOMPRESSED. Spark writes snappy, and so should a sink whose files
    // are uploaded to object storage and then scanned by every dashboard query.
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props))
        .map_err(|e| Error::Io(format!("parquet writer: {e}")))?;
    for b in batches {
        writer
            .write(b)
            .map_err(|e| Error::Io(format!("parquet write: {e}")))?;
    }
    writer
        .close()
        .map_err(|e| Error::Io(format!("parquet close: {e}")))?;
    Ok(buf)
}

/// The object-store prefix for a table location.
///
/// For `s3://bucket/a/b` the store is already bucket-scoped, so the prefix is `a/b`; for a local
/// path or `file://` URI the store is filesystem-rooted and the prefix is the absolute path. This
/// mirrors how DataFusion's `ListingTableUrl::prefix()` splits the two, which is what the read
/// path uses — so writes and reads agree on where the table is.
fn table_root_prefix(location: &str) -> Result<ObjectPath> {
    use datafusion::datasource::listing::ListingTableUrl;

    let url = ListingTableUrl::parse(location)
        .map_err(|e| Error::Plan(format!("bad table location `{location}`: {e}")))?;
    Ok(url.prefix().clone())
}

/// Describe the first column-level difference between a catalog's declared schema and the one a
/// stream produces, or `None` when they line up.
///
/// Nullability is deliberately ignored: Hive and Glue declare every column nullable regardless of
/// what was written, so comparing it would reject every append to a table the catalog itself
/// created. Names and types are what a reader resolves data files against.
fn column_mismatch(declared: &SchemaRef, produced: &SchemaRef) -> Option<String> {
    if declared.fields().len() != produced.fields().len() {
        return Some(format!(
            "the table has {} columns, the query produces {}",
            declared.fields().len(),
            produced.fields().len()
        ));
    }
    for (d, p) in declared.fields().iter().zip(produced.fields()) {
        if d.name() != p.name() {
            return Some(format!(
                "column `{}` in the table is `{}` in the query",
                d.name(),
                p.name()
            ));
        }
        if d.data_type() != p.data_type() {
            return Some(format!(
                "column `{}` is {:?} in the table but {:?} in the query",
                d.name(),
                d.data_type(),
                p.data_type()
            ));
        }
    }
    None
}

/// A UUID derived from the table location, so re-declaring a table produces the same `metaData`
/// id rather than a fresh random one on every restart.
fn stable_table_id(location: &str) -> String {
    uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, location.as_bytes()).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidant_loom::arrow::array::Int64Array;
    use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]))
    }

    fn batch(vals: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vals))]).unwrap()
    }

    /// Sink options with the catalog-dependent features off, for path-only tests.
    fn options() -> LakeSinkOptions {
        LakeSinkOptions {
            app_id: Some("test-query".into()),
            checkpoint_interval: 10,
            ..Default::default()
        }
    }

    #[test]
    fn table_identifiers_resolve_against_the_current_catalog_and_database() {
        let three = LakeTarget::from_table_identifier(
            "glue.live.events",
            "spark_catalog",
            &[],
            TableFormat::Delta,
            None,
        )
        .unwrap();
        assert_eq!(three.catalog.as_deref(), Some("glue"));
        assert_eq!(three.namespace, vec!["live"]);
        assert_eq!(three.table, "events");

        let two = LakeTarget::from_table_identifier(
            "live.events",
            "glue",
            &["other".into()],
            TableFormat::Delta,
            None,
        )
        .unwrap();
        assert_eq!(two.catalog.as_deref(), Some("glue"));
        assert_eq!(two.namespace, vec!["live"]);

        let one = LakeTarget::from_table_identifier(
            "events",
            "glue",
            &["live".into()],
            TableFormat::Delta,
            None,
        )
        .unwrap();
        assert_eq!(one.namespace, vec!["live"]);
        assert_eq!(one.table, "events");
    }

    #[test]
    fn a_bare_table_name_with_no_current_database_is_an_error() {
        let err =
            LakeTarget::from_table_identifier("events", "glue", &[], TableFormat::Delta, None)
                .unwrap_err();
        assert!(err.to_string().contains("no current database"), "{err}");
    }

    #[test]
    fn iceberg_is_refused_with_a_pointer_to_delta() {
        let err = writable_format("iceberg").unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
        assert!(err.to_string().contains("delta"), "{err}");
    }

    #[test]
    fn only_table_formats_are_accepted_as_sinks() {
        assert_eq!(writable_format("delta").unwrap(), TableFormat::Delta);
        assert_eq!(writable_format("parquet").unwrap(), TableFormat::Parquet);
        assert!(writable_format("csv").is_err());
        assert!(writable_format("console").is_err());
    }

    #[test]
    fn the_s3_prefix_drops_the_bucket_but_a_local_path_keeps_its_root() {
        let s3 = table_root_prefix("s3://my-bucket/live/events").unwrap();
        assert_eq!(s3.as_ref(), "live/events");
        let local = table_root_prefix("/data/live/events").unwrap();
        assert_eq!(local.as_ref(), "data/live/events");
    }

    #[test]
    fn schema_comparison_ignores_nullability_but_not_names_or_types() {
        let declared: SchemaRef = Arc::new(Schema::new(vec![
            // Glue declares every column nullable regardless of what was written; rejecting on
            // that alone would refuse every append to a table the catalog itself created.
            Field::new("n", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let produced: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, false),
            Field::new("s", DataType::Utf8, false),
        ]));
        assert_eq!(column_mismatch(&declared, &produced), None);

        let renamed: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, true),
            Field::new("other", DataType::Utf8, true),
        ]));
        assert!(column_mismatch(&declared, &renamed).is_some_and(|m| m.contains("other")));

        let retyped: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Utf8, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        assert!(column_mismatch(&declared, &retyped).is_some_and(|m| m.contains("Utf8")));

        let widened: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        assert!(column_mismatch(&declared, &widened).is_some_and(|m| m.contains("columns")));
    }

    #[test]
    fn the_table_id_is_stable_for_a_location() {
        assert_eq!(
            stable_table_id("s3://b/live/events"),
            stable_table_id("s3://b/live/events")
        );
        assert_ne!(
            stable_table_id("s3://b/live/events"),
            stable_table_id("s3://b/live/audit")
        );
    }

    #[tokio::test]
    async fn a_delta_sink_commits_each_batch_and_reads_back() {
        let dir = tempfile::TempDir::new().unwrap();
        let table_dir = dir.path().join("events");
        std::fs::create_dir_all(&table_dir).unwrap();
        let engine = Engine::new();

        let mut sink = LakeSink::open(
            &engine,
            LakeTarget::location_only(table_dir.to_str().unwrap(), TableFormat::Delta),
            schema(),
            options(),
        )
        .await
        .unwrap();

        assert_eq!(
            sink.write_batch(&[batch(vec![1, 2, 3])], 1).await.unwrap(),
            3
        );
        assert_eq!(sink.write_batch(&[batch(vec![4, 5])], 2).await.unwrap(), 2);
        // An empty batch is a no-op, not an empty commit.
        assert_eq!(sink.write_batch(&[], 3).await.unwrap(), 0);

        let log = table_dir.join("_delta_log");
        let commits: Vec<_> = std::fs::read_dir(&log)
            .unwrap()
            .flatten()
            .filter(|e| e.path().extension().is_some_and(|x| x == "json"))
            .collect();
        assert_eq!(commits.len(), 2, "one commit per non-empty batch");

        // The point of the whole exercise: the engine can read the table back.
        engine
            .register_delta("events", table_dir.to_str().unwrap())
            .await
            .unwrap();
        let rows = engine.sql("SELECT sum(n) AS s FROM events").await.unwrap();
        let total = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(total, 15);
    }

    #[tokio::test]
    async fn one_table_written_as_delta_is_readable_as_iceberg() {
        // The whole point of publishing Iceberg metadata: *the same Parquet files*, resolved
        // through two different metadata trees, give two different engines the same rows. If
        // this passes, a Trino or Athena user querying the Iceberg side sees what a Spark user
        // querying the Delta side sees, with no second copy of the data.
        let dir = tempfile::TempDir::new().unwrap();
        let table_dir = dir.path().join("events");
        std::fs::create_dir_all(&table_dir).unwrap();
        let engine = Engine::new();

        let mut sink = LakeSink::open(
            &engine,
            LakeTarget::location_only(table_dir.to_str().unwrap(), TableFormat::Delta),
            schema(),
            LakeSinkOptions {
                app_id: Some("q".into()),
                publish_iceberg: true,
                // Publish on every second commit so the test does not need ten batches.
                checkpoint_interval: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        sink.write_batch(&[batch(vec![1, 2, 3])], 1).await.unwrap();
        sink.write_batch(&[batch(vec![4, 5])], 2).await.unwrap();

        // Delta's view.
        engine
            .register_delta("as_delta", table_dir.to_str().unwrap())
            .await
            .unwrap();
        let delta = engine
            .sql("SELECT sum(n) AS s, count(*) AS c FROM as_delta")
            .await
            .unwrap();

        // Iceberg's view of the very same directory.
        engine
            .register_iceberg("as_iceberg", table_dir.to_str().unwrap())
            .await
            .unwrap();
        let iceberg = engine
            .sql("SELECT sum(n) AS s, count(*) AS c FROM as_iceberg")
            .await
            .unwrap();

        let value = |rows: &[RecordBatch], col: usize| {
            rows[0]
                .column(col)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        assert_eq!(value(&delta, 1), 5, "Delta sees five rows");
        assert_eq!(
            (value(&iceberg, 0), value(&iceberg, 1)),
            (value(&delta, 0), value(&delta, 1)),
            "the Iceberg and Delta views of one table must agree"
        );
    }

    #[tokio::test]
    async fn a_replayed_batch_id_does_not_write_the_rows_twice() {
        // A crash between the sink write and the offset checkpoint replays the batch. Without
        // the `txn` stamp a dashboard's `count(*)` would climb every time the query restarted.
        let dir = tempfile::TempDir::new().unwrap();
        let engine = Engine::new();
        let open = || {
            LakeSink::open(
                &engine,
                LakeTarget::location_only(dir.path().to_str().unwrap(), TableFormat::Delta),
                schema(),
                options(),
            )
        };

        let mut sink = open().await.unwrap();
        assert_eq!(sink.write_batch(&[batch(vec![1, 2])], 1).await.unwrap(), 2);

        let mut restarted = open().await.unwrap();
        assert_eq!(
            restarted
                .write_batch(&[batch(vec![1, 2])], 1)
                .await
                .unwrap(),
            0,
            "batch 1 was already committed"
        );
        assert_eq!(
            restarted.write_batch(&[batch(vec![3])], 2).await.unwrap(),
            1,
            "but the next batch still lands"
        );

        engine
            .register_delta("replayed", dir.path().to_str().unwrap())
            .await
            .unwrap();
        let rows = engine
            .sql("SELECT count(*) AS c FROM replayed")
            .await
            .unwrap();
        assert_eq!(
            rows[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            3,
            "three rows, not five"
        );
    }

    #[tokio::test]
    async fn partition_by_writes_hive_directories_a_dashboard_can_prune() {
        let dir = tempfile::TempDir::new().unwrap();
        let engine = Engine::new();
        let partitioned: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, false),
            Field::new("day", DataType::Utf8, false),
        ]));
        let rows = RecordBatch::try_new(
            partitioned.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3])),
                Arc::new(oxidant_loom::arrow::array::StringArray::from(vec![
                    "2026-08-16",
                    "2026-08-17",
                    "2026-08-17",
                ])),
            ],
        )
        .unwrap();

        let mut sink = LakeSink::open(
            &engine,
            LakeTarget::location_only(dir.path().to_str().unwrap(), TableFormat::Delta),
            partitioned,
            LakeSinkOptions {
                partition_columns: vec!["day".into()],
                ..options()
            },
        )
        .await
        .unwrap();
        assert_eq!(sink.write_batch(&[rows], 1).await.unwrap(), 3);

        assert!(dir.path().join("day=2026-08-16").is_dir());
        assert!(dir.path().join("day=2026-08-17").is_dir());

        // The partition column is reconstructed from the path, so readers still see it.
        engine
            .register_delta("by_day", dir.path().to_str().unwrap())
            .await
            .unwrap();
        let counted = engine
            .sql("SELECT count(*) AS c FROM by_day WHERE day = '2026-08-17'")
            .await
            .unwrap();
        assert_eq!(
            counted[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2
        );
    }

    #[tokio::test]
    async fn partitioning_on_a_column_the_query_does_not_produce_fails_at_start() {
        let dir = tempfile::TempDir::new().unwrap();
        let engine = Engine::new();
        let Err(err) = LakeSink::open(
            &engine,
            LakeTarget::location_only(dir.path().to_str().unwrap(), TableFormat::Delta),
            schema(),
            LakeSinkOptions {
                partition_columns: vec!["nope".into()],
                ..options()
            },
        )
        .await
        else {
            panic!("partitioning on a missing column must fail at start");
        };
        assert!(err.to_string().contains("nope"), "{err}");
    }

    #[tokio::test]
    async fn a_batch_whose_schema_drifted_is_rejected_before_it_is_written() {
        let dir = tempfile::TempDir::new().unwrap();
        let engine = Engine::new();
        let mut sink = LakeSink::open(
            &engine,
            LakeTarget::location_only(dir.path().to_str().unwrap(), TableFormat::Delta),
            schema(),
            options(),
        )
        .await
        .unwrap();

        let other: SchemaRef = Arc::new(Schema::new(vec![Field::new("m", DataType::Int64, false)]));
        let wrong =
            RecordBatch::try_new(other, vec![Arc::new(Int64Array::from(vec![1i64]))]).unwrap();
        let err = sink.write_batch(&[wrong], 1).await.unwrap_err();
        assert!(err.to_string().contains("does not match"), "{err}");
        assert!(
            !dir.path().join("_delta_log").exists(),
            "nothing may be committed for a rejected batch"
        );
    }
}
