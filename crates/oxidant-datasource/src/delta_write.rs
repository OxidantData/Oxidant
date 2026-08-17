//! Delta Lake **write** side: an object-store-backed transaction-log appender.
//!
//! The read side ([`crate::delta_active_files`]) hands a pinned snapshot to delta-kernel-rs. This
//! module is its mirror: it writes Parquet data files and commits `_delta_log/N.json` actions so
//! the very same kernel — and Spark, Athena, and Trino — can read the table back.
//!
//! [`DeltaTableWriter`] owns the state a streaming sink needs to append *repeatedly* without
//! paying for the log every time:
//!
//! - **The next commit version is remembered**, not re-derived. Listing `_delta_log` on every
//!   commit makes an append-per-second table quadratic in its own history; the writer lists once
//!   at open and thereafter only on a lost race.
//! - **Checkpoints are written every `checkpoint_interval` commits**, so a reader replays a
//!   Parquet snapshot of the table state plus a handful of JSON commits instead of every commit
//!   ever made. Without this a live table becomes unopenable after a few days.
//! - **`txn` actions make a commit idempotent.** A streaming query stamps its batch id; a replayed
//!   batch after a crash (or a retried write whose ack was lost) is recognized and skipped rather
//!   than double-counted.
//! - **Per-column `stats`** (min/max/nullCount) are written into every `add`, which is what lets a
//!   dashboard's `WHERE` clause skip files instead of scanning the whole table.
//!
//! Everything goes through [`ObjectStore`], so a `s3://` table root works exactly like a local
//! one.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use oxidant_common::{Error, Result};

/// Delta reader version this writer's output requires. v1 = no column mapping, no deletion
/// vectors — every Delta implementation in the wild can read it.
const MIN_READER_VERSION: i32 = 1;
/// Delta writer version this writer claims. v2 = `appendOnly`/`invariants` table features, which
/// is what a plain append sink needs and what Spark stamps for a vanilla `CREATE TABLE`.
const MIN_WRITER_VERSION: i32 = 2;
/// How many times a commit re-reads the log and retries after losing a version race.
const COMMIT_ATTEMPTS: usize = 8;
/// Commits between checkpoints. Delta's own convention, and what Spark uses.
pub const DEFAULT_CHECKPOINT_INTERVAL: u64 = 10;

/// Consecutive checkpoint failures tolerated before checkpointing is abandoned for this writer.
///
/// More than one, because the object store is allowed an occasional 5xx and the whole point of
/// checkpointing is to bound log growth for a query that runs for weeks — giving up on the first
/// blip trades a transient error for permanent unbounded growth. Not unlimited, because a
/// *persistent* failure (no permission to write the checkpoint) should not be retried every
/// interval forever.
const CHECKPOINT_FAILURE_LIMIT: u32 = 3;
/// Hive's sentinel for a null partition value, which Delta inherits.
const NULL_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// One data file appended to a Delta table, as it appears in the commit's `add` action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaAddFile {
    /// Table-root-relative path — what makes a committed Delta tree relocatable.
    pub path: String,
    /// Size of the Parquet file in bytes.
    pub size: u64,
    /// Rows in the file, surfaced as the `numRecords` statistic.
    pub num_records: u64,
    /// Hive-style partition values for the file (empty for an unpartitioned table).
    pub partition_values: BTreeMap<String, String>,
    /// Delta `stats` JSON: `numRecords` plus per-column min/max/nullCount. Readers use this for
    /// file skipping, so a file without it must be read by every query that touches the table.
    pub stats: Option<String>,
}

/// A streaming query's idempotency stamp, written as Delta's `txn` action.
///
/// `app_id` identifies the writer (Oxidant uses the streaming query id, which outlives a restart)
/// and `version` its batch id. A writer that finds its own `app_id` already committed at a
/// `version` at least as high knows the batch landed and must not write it again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaTxn {
    pub app_id: String,
    pub version: i64,
}

/// Outcome of one [`DeltaTableWriter::append`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaCommit {
    /// The version this append committed as (`_delta_log/{version:020}.json`).
    pub version: u64,
    /// Rows written across all data files in the commit.
    pub rows: u64,
    /// The data files added.
    pub files: Vec<DeltaAddFile>,
    /// True when the `txn` stamp showed this batch was already committed and nothing was written.
    pub deduplicated: bool,
}

/// How a [`DeltaTableWriter`] is configured at open time.
#[derive(Debug, Clone)]
pub struct DeltaWriterConfig {
    /// Stable table identity for the `metaData` action.
    pub table_id: String,
    /// Columns written as Hive-style directories rather than into the data file.
    pub partition_columns: Vec<String>,
    /// Idempotency `appId`. `None` disables `txn` actions.
    pub app_id: Option<String>,
    /// Commits between checkpoints; `0` disables checkpointing.
    pub checkpoint_interval: u64,
    /// Parquet compression for data files. Delta readers negotiate this from the file footer, so
    /// it is purely a size/CPU tradeoff.
    pub compression: parquet::basic::Compression,
}

impl Default for DeltaWriterConfig {
    fn default() -> Self {
        Self {
            table_id: String::new(),
            partition_columns: Vec::new(),
            app_id: None,
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
            // Spark's default for Parquet. parquet-rs defaults to UNCOMPRESSED, which would make
            // every file 2-5x larger on the wire, in the bucket, and in every dashboard scan.
            compression: parquet::basic::Compression::SNAPPY,
        }
    }
}

/// A repeatedly-appending Delta writer bound to one table root.
pub struct DeltaTableWriter {
    store: Arc<dyn ObjectStore>,
    root: ObjectPath,
    /// Full table schema, including partition columns.
    schema: SchemaRef,
    /// Data-file schema: the table schema minus partition columns, which Delta stores in the
    /// path rather than the file (matching what Spark writes).
    data_schema: SchemaRef,
    config: DeltaWriterConfig,
    /// The version the next commit will attempt. Kept across appends so the common path never
    /// lists the log.
    next_version: u64,
    /// Every live `add`, maintained in memory so a checkpoint can be written without re-reading
    /// the whole log. `None` once something in the log makes our view untrustworthy (another
    /// writer's `remove`), which disables checkpointing rather than writing a wrong one.
    live_files: Option<Vec<DeltaAddFile>>,
    /// Highest `txn.version` already committed under our `app_id`.
    committed_txn: Option<i64>,
    file_counter: u64,
    /// Consecutive checkpoint write failures. Reset by any success.
    checkpoint_failures: u32,
}

impl DeltaTableWriter {
    /// Bind a writer to `root`, reading the existing log once to learn where to continue.
    pub async fn open(
        store: Arc<dyn ObjectStore>,
        root: ObjectPath,
        schema: SchemaRef,
        config: DeltaWriterConfig,
    ) -> Result<Self> {
        let data_schema = data_file_schema(&schema, &config.partition_columns)?;
        let state = LogState::read(store.as_ref(), &root, config.app_id.as_deref()).await?;
        Ok(Self {
            store,
            root,
            schema,
            data_schema,
            next_version: state.next_version,
            live_files: state.live_files,
            committed_txn: state.committed_txn,
            config,
            file_counter: 0,
            checkpoint_failures: 0,
        })
    }

    /// The version the next commit will attempt.
    pub fn next_version(&self) -> u64 {
        self.next_version
    }

    /// Live data files, when the writer's view of them is trustworthy.
    pub fn live_files(&self) -> Option<&[DeltaAddFile]> {
        self.live_files.as_deref()
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn table_id(&self) -> &str {
        &self.config.table_id
    }

    pub fn root(&self) -> &ObjectPath {
        &self.root
    }

    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// Write `batches` as Parquet under `root` and commit them to the transaction log.
    ///
    /// `file_prefix` names the data files (one per partition value combination). `txn_version` is
    /// the caller's batch id: when it is not newer than what our `app_id` already committed, the
    /// batch is a replay and nothing is written.
    pub async fn append(
        &mut self,
        batches: &[RecordBatch],
        file_prefix: &str,
        txn_version: Option<i64>,
    ) -> Result<DeltaCommit> {
        let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        if rows == 0 {
            return Ok(DeltaCommit {
                version: self.next_version,
                rows: 0,
                files: vec![],
                deduplicated: false,
            });
        }

        // Idempotency: a batch our `appId` already committed must not be written twice. This is
        // what keeps a crash between the sink write and the offset checkpoint — or a retried
        // write whose acknowledgement was lost — from inflating the table.
        if let (Some(v), Some(committed)) = (txn_version, self.committed_txn) {
            if v <= committed {
                return Ok(DeltaCommit {
                    version: self.next_version,
                    rows: 0,
                    files: vec![],
                    deduplicated: true,
                });
            }
        }

        // One data file per partition-value combination. Unpartitioned tables take the fast path
        // with a single group and no projection.
        let groups = split_by_partition(batches, &self.config.partition_columns)?;
        let mut files = Vec::with_capacity(groups.len());
        for (index, (partition_values, group)) in groups.into_iter().enumerate() {
            let n = self.file_counter;
            let file_name = format!("part-{n:05}-{file_prefix}-{index}-c000.snappy.parquet");
            let relative = partition_dir(&partition_values, &self.config.partition_columns)
                .map_or_else(|| file_name.clone(), |dir| format!("{dir}/{file_name}"));

            let bytes = encode_parquet(&self.data_schema, &group, self.config.compression)?;
            let size = bytes.len() as u64;
            let num_records: u64 = group.iter().map(|b| b.num_rows() as u64).sum();
            let path = object_path_join(&self.root, &relative);
            self.store
                .put(&path, bytes.into())
                .await
                .map_err(|e| Error::Io(format!("write `{path}`: {e}")))?;

            files.push(DeltaAddFile {
                path: relative,
                size,
                num_records,
                partition_values,
                stats: Some(file_stats(&self.data_schema, &group, num_records)),
            });
        }
        self.file_counter += 1;

        let txn = txn_version.and_then(|version| {
            self.config.app_id.as_ref().map(|app_id| DeltaTxn {
                app_id: app_id.clone(),
                version,
            })
        });

        let version = self.commit(&files, txn.as_ref()).await?;

        self.next_version = version + 1;
        if let Some(txn) = &txn {
            self.committed_txn = Some(txn.version);
        }
        if let Some(live) = &mut self.live_files {
            live.extend(files.iter().cloned());
        }
        if self.config.checkpoint_interval > 0
            && (version + 1) % self.config.checkpoint_interval == 0
            && self.live_files.is_some()
        {
            // A failed checkpoint must not fail the append: the data is committed and the log is
            // correct without it, only slower to replay.
            match self.write_checkpoint(version).await {
                Ok(()) => self.checkpoint_failures = 0,
                Err(e) => {
                    self.checkpoint_failures += 1;
                    eprintln!(
                        "[oxidant] delta checkpoint at version {version} failed \
                         ({}/{CHECKPOINT_FAILURE_LIMIT}): {e}",
                        self.checkpoint_failures
                    );
                    if self.checkpoint_failures >= CHECKPOINT_FAILURE_LIMIT {
                        eprintln!(
                            "[oxidant] giving up on checkpointing `{}` — its transaction log will \
                             grow unbounded until the query restarts",
                            self.config.table_id
                        );
                        self.config.checkpoint_interval = 0;
                    }
                }
            }
        }
        Ok(DeltaCommit {
            version,
            rows,
            files,
            deduplicated: false,
        })
    }

    /// Write the commit, retrying at the next free version if another writer took ours.
    async fn commit(&mut self, files: &[DeltaAddFile], txn: Option<&DeltaTxn>) -> Result<u64> {
        for attempt in 0..COMMIT_ATTEMPTS {
            let version = self.next_version;
            let body = render_commit(
                version,
                &self.schema,
                files,
                &self.config.table_id,
                &self.config.partition_columns,
                txn,
            )?;
            let commit_at = commit_path(&self.root, version);
            let opts = object_store::PutOptions {
                mode: object_store::PutMode::Create,
                ..Default::default()
            };
            match self.store.put_opts(&commit_at, body.into(), opts).await {
                Ok(_) => return Ok(version),
                // Another writer took this version between our last commit and this one. Re-read
                // the log — our cached view of the table is stale, so the live-file set is too.
                Err(object_store::Error::AlreadyExists { .. }) if attempt + 1 < COMMIT_ATTEMPTS => {
                    let state = LogState::read(
                        self.store.as_ref(),
                        &self.root,
                        self.config.app_id.as_deref(),
                    )
                    .await?;
                    self.next_version = state.next_version;
                    self.live_files = state.live_files;
                    self.committed_txn = state.committed_txn;
                }
                Err(e) => return Err(Error::Io(format!("commit `{commit_at}`: {e}"))),
            }
        }
        Err(Error::Io(format!(
            "Delta commit lost {COMMIT_ATTEMPTS} version races on `{}` — another writer is \
             appending to this table concurrently",
            self.root
        )))
    }

    /// Write `_delta_log/{version}.checkpoint.parquet` and update `_last_checkpoint`.
    ///
    /// This is what bounds a reader's work: without it every query replays every commit ever
    /// made, and a table appended to once a second stops opening within days.
    pub async fn write_checkpoint(&self, version: u64) -> Result<()> {
        let Some(live) = &self.live_files else {
            return Ok(());
        };
        let txn = self
            .config
            .app_id
            .as_ref()
            .zip(self.committed_txn)
            .map(|(app_id, version)| DeltaTxn {
                app_id: app_id.clone(),
                version,
            });
        let bytes = render_checkpoint(
            &self.schema,
            live,
            &self.config.table_id,
            &self.config.partition_columns,
            txn.as_ref(),
        )?;
        let size = bytes.len() as u64;
        let path = checkpoint_path(&self.root, version);
        self.store
            .put(&path, bytes.into())
            .await
            .map_err(|e| Error::Io(format!("write checkpoint `{path}`: {e}")))?;

        // `_last_checkpoint` is a hint: a reader that misses it falls back to listing, so a
        // failure here is not fatal to correctness.
        let hint = serde_json::json!({
            "version": version,
            "size": live.len() + 2,
            "sizeInBytes": size,
        })
        .to_string();
        let hint_path = object_path_join(&self.root, "_delta_log/_last_checkpoint");
        self.store
            .put(&hint_path, hint.into_bytes().into())
            .await
            .map_err(|e| Error::Io(format!("write `{hint_path}`: {e}")))?;
        Ok(())
    }
}

/// A Delta table's declared schema and partitioning, as the log records them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaTableMetadata {
    pub schema: SchemaRef,
    pub partition_columns: Vec<String>,
}

/// Read the table's declared schema and partition columns from its transaction log.
///
/// A partitioned Delta table stores its partition columns *only* in the path, so a reader that
/// does not know which columns those are cannot reconstruct them — it sees a table missing the
/// very columns a dashboard filters on. The log's `metaData` action is where that list lives.
pub async fn current_metadata(
    store: &dyn ObjectStore,
    root: &ObjectPath,
) -> Result<Option<DeltaTableMetadata>> {
    Ok(LogState::read(store, root, None).await?.metadata)
}

/// What one pass over the transaction log tells a writer about where to continue.
struct LogState {
    next_version: u64,
    live_files: Option<Vec<DeltaAddFile>>,
    committed_txn: Option<i64>,
    metadata: Option<DeltaTableMetadata>,
}

impl LogState {
    /// Read the log: the newest checkpoint (if any) for the bulk of the state, then every JSON
    /// commit after it. This is the one full read a writer does, at open.
    async fn read(
        store: &dyn ObjectStore,
        root: &ObjectPath,
        app_id: Option<&str>,
    ) -> Result<Self> {
        let checkpoint = last_checkpoint_version(store, root).await;
        let mut live: Vec<DeltaAddFile> = Vec::new();
        let mut removed: Vec<String> = Vec::new();
        let mut committed_txn = None;
        let mut metadata = None;
        let mut trustworthy = true;

        if let Some(v) = checkpoint {
            match read_checkpoint(store, root, v, app_id).await {
                Ok((files, txn, meta)) => {
                    live = files;
                    committed_txn = txn;
                    metadata = meta;
                }
                // A checkpoint we cannot parse (an older Delta writer's, say) means our in-memory
                // file set would be incomplete — keep appending, stop checkpointing.
                Err(_) => trustworthy = false,
            }
        }

        let start = checkpoint.map_or(0, |v| v + 1);
        let mut version = start;
        loop {
            let path = commit_path(root, version);
            let body = match store.get(&path).await {
                Ok(r) => r
                    .bytes()
                    .await
                    .map_err(|e| Error::Io(format!("read `{path}`: {e}")))?,
                Err(object_store::Error::NotFound { .. }) => break,
                Err(e) => return Err(Error::Io(format!("read `{path}`: {e}"))),
            };
            let text = String::from_utf8_lossy(&body);
            for line in text.lines().filter(|l| !l.trim().is_empty()) {
                let Ok(action) = serde_json::from_str::<serde_json::Value>(line) else {
                    continue;
                };
                if let Some(add) = action.get("add") {
                    if let Some(file) = add_from_json(add) {
                        live.push(file);
                    } else {
                        trustworthy = false;
                    }
                } else if let Some(remove) = action.get("remove") {
                    // We never write removes. One in the log means another writer is compacting
                    // or vacuuming this table, so our view of the live set is not authoritative.
                    if let Some(p) = remove.get("path").and_then(|p| p.as_str()) {
                        removed.push(p.to_string());
                    }
                    trustworthy = false;
                } else if let Some(meta) = action.get("metaData") {
                    metadata = metadata_from_json(meta).or(metadata);
                } else if let Some(txn) = action.get("txn") {
                    let matches = app_id.is_some_and(|want| {
                        txn.get("appId").and_then(|a| a.as_str()) == Some(want)
                    });
                    if matches {
                        if let Some(v) = txn.get("version").and_then(|v| v.as_i64()) {
                            committed_txn = Some(committed_txn.map_or(v, |c: i64| c.max(v)));
                        }
                    }
                }
            }
            version += 1;
        }

        // A version already present but unreadable would be a silent overwrite risk, so trust the
        // scan: `version` is the first commit that does not exist.
        Ok(Self {
            next_version: version,
            live_files: if trustworthy {
                live.retain(|f| !removed.contains(&f.path));
                Some(live)
            } else {
                None
            },
            committed_txn,
            metadata,
        })
    }
}

/// Parse a `metaData` action's `schemaString` and `partitionColumns`.
fn metadata_from_json(meta: &serde_json::Value) -> Option<DeltaTableMetadata> {
    let schema = arrow_schema_from_delta(meta.get("schemaString")?.as_str()?).ok()?;
    let partition_columns = meta
        .get("partitionColumns")
        .and_then(|c| c.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    Some(DeltaTableMetadata {
        schema,
        partition_columns,
    })
}

/// Delta's `schemaString` back to Arrow — the inverse of [`delta_schema_string`].
pub fn arrow_schema_from_delta(schema_string: &str) -> Result<SchemaRef> {
    let json: serde_json::Value = serde_json::from_str(schema_string)
        .map_err(|e| Error::Plan(format!("delta schemaString is not JSON: {e}")))?;
    let fields = json
        .get("fields")
        .and_then(|f| f.as_array())
        .ok_or_else(|| Error::Plan("delta schemaString has no `fields`".into()))?;
    let arrow_fields = fields
        .iter()
        .map(arrow_field_from_delta)
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(arrow_fields)))
}

fn arrow_field_from_delta(field: &serde_json::Value) -> Result<Field> {
    let name = field
        .get("name")
        .and_then(|n| n.as_str())
        .ok_or_else(|| Error::Plan("delta schema field has no `name`".into()))?;
    let nullable = field
        .get("nullable")
        .and_then(|n| n.as_bool())
        .unwrap_or(true);
    let data_type = arrow_type_from_delta(
        field
            .get("type")
            .ok_or_else(|| Error::Plan(format!("delta schema field `{name}` has no `type`")))?,
    )?;
    Ok(Field::new(name, data_type, nullable))
}

fn arrow_type_from_delta(ty: &serde_json::Value) -> Result<DataType> {
    if let Some(name) = ty.as_str() {
        return match name {
            "boolean" => Ok(DataType::Boolean),
            "byte" => Ok(DataType::Int8),
            "short" => Ok(DataType::Int16),
            "integer" => Ok(DataType::Int32),
            "long" => Ok(DataType::Int64),
            "float" => Ok(DataType::Float32),
            "double" => Ok(DataType::Float64),
            "string" => Ok(DataType::Utf8),
            "binary" => Ok(DataType::Binary),
            "date" => Ok(DataType::Date32),
            // Delta's `timestamp` is a UTC instant at microsecond precision.
            "timestamp" => Ok(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into()),
            )),
            "timestamp_ntz" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
            decimal if decimal.starts_with("decimal(") => {
                let inner = decimal.trim_start_matches("decimal(").trim_end_matches(')');
                let (p, s) = inner
                    .split_once(',')
                    .ok_or_else(|| Error::Plan(format!("bad delta decimal type `{decimal}`")))?;
                Ok(DataType::Decimal128(
                    p.trim().parse().map_err(|_| {
                        Error::Plan(format!("bad decimal precision in `{decimal}`"))
                    })?,
                    s.trim()
                        .parse()
                        .map_err(|_| Error::Plan(format!("bad decimal scale in `{decimal}`")))?,
                ))
            }
            other => Err(Error::Unsupported(format!(
                "no Arrow mapping for Delta type `{other}`"
            ))),
        };
    }
    match ty.get("type").and_then(|t| t.as_str()) {
        Some("struct") => {
            let fields = ty
                .get("fields")
                .and_then(|f| f.as_array())
                .ok_or_else(|| Error::Plan("delta struct type has no `fields`".into()))?
                .iter()
                .map(arrow_field_from_delta)
                .collect::<Result<Vec<_>>>()?;
            Ok(DataType::Struct(fields.into()))
        }
        Some("array") => {
            let element = arrow_type_from_delta(
                ty.get("elementType")
                    .ok_or_else(|| Error::Plan("delta array type has no `elementType`".into()))?,
            )?;
            let contains_null = ty
                .get("containsNull")
                .and_then(|c| c.as_bool())
                .unwrap_or(true);
            Ok(DataType::List(Arc::new(Field::new(
                "item",
                element,
                contains_null,
            ))))
        }
        other => Err(Error::Unsupported(format!(
            "no Arrow mapping for Delta type `{other:?}`"
        ))),
    }
}

fn add_from_json(add: &serde_json::Value) -> Option<DeltaAddFile> {
    let partition_values = add
        .get("partitionValues")
        .and_then(|v| v.as_object())
        .map(|m| {
            m.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                .collect()
        })
        .unwrap_or_default();
    Some(DeltaAddFile {
        path: add.get("path")?.as_str()?.to_string(),
        size: add.get("size").and_then(|s| s.as_u64()).unwrap_or(0),
        num_records: add
            .get("stats")
            .and_then(|s| s.as_str())
            .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
            .and_then(|s| s.get("numRecords").and_then(|n| n.as_u64()))
            .unwrap_or(0),
        partition_values,
        stats: add
            .get("stats")
            .and_then(|s| s.as_str())
            .map(|s| s.to_string()),
    })
}

/// `_delta_log/{version:020}.json` — Delta's zero-padded commit filename.
pub fn commit_path(root: &ObjectPath, version: u64) -> ObjectPath {
    object_path_join(root, &format!("_delta_log/{version:020}.json"))
}

/// `_delta_log/{version:020}.checkpoint.parquet`.
pub fn checkpoint_path(root: &ObjectPath, version: u64) -> ObjectPath {
    object_path_join(
        root,
        &format!("_delta_log/{version:020}.checkpoint.parquet"),
    )
}

fn object_path_join(root: &ObjectPath, relative: &str) -> ObjectPath {
    let mut p = root.clone();
    for part in relative.split('/').filter(|s| !s.is_empty()) {
        p = p.join(part);
    }
    p
}

/// The newest checkpoint version, from `_last_checkpoint` when it is present.
async fn last_checkpoint_version(store: &dyn ObjectStore, root: &ObjectPath) -> Option<u64> {
    let path = object_path_join(root, "_delta_log/_last_checkpoint");
    let body = store.get(&path).await.ok()?.bytes().await.ok()?;
    let json: serde_json::Value = serde_json::from_slice(&body).ok()?;
    json.get("version")?.as_u64()
}

/// Read a checkpoint's `add` actions and our `txn` watermark back out of Parquet.
async fn read_checkpoint(
    store: &dyn ObjectStore,
    root: &ObjectPath,
    version: u64,
    app_id: Option<&str>,
) -> Result<(Vec<DeltaAddFile>, Option<i64>, Option<DeltaTableMetadata>)> {
    use arrow::array::AsArray;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let path = checkpoint_path(root, version);
    let bytes = store
        .get(&path)
        .await
        .map_err(|e| Error::Io(format!("read checkpoint `{path}`: {e}")))?
        .bytes()
        .await
        .map_err(|e| Error::Io(format!("read checkpoint `{path}`: {e}")))?;

    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| Error::Io(format!("open checkpoint `{path}`: {e}")))?
        .build()
        .map_err(|e| Error::Io(format!("open checkpoint `{path}`: {e}")))?;

    let mut files = Vec::new();
    let mut txn_version = None;
    let mut metadata = None;
    for batch in reader {
        let batch = batch.map_err(|e| Error::Io(format!("read checkpoint `{path}`: {e}")))?;
        if let Some(add) = batch.column_by_name("add").and_then(|c| c.as_struct_opt()) {
            let paths = add
                .column_by_name("path")
                .and_then(|c| c.as_string_opt::<i32>())
                .ok_or_else(|| Error::Io("checkpoint `add.path` is not a string".into()))?;
            let sizes = add
                .column_by_name("size")
                .and_then(|c| c.as_primitive_opt::<arrow::datatypes::Int64Type>());
            let stats = add
                .column_by_name("stats")
                .and_then(|c| c.as_string_opt::<i32>());
            for row in 0..add.len() {
                if !add.is_valid(row) || paths.is_null(row) {
                    continue;
                }
                let stats_json = stats
                    .filter(|s| !s.is_null(row))
                    .map(|s| s.value(row).to_string());
                let num_records = stats_json
                    .as_deref()
                    .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
                    .and_then(|s| s.get("numRecords").and_then(|n| n.as_u64()))
                    .unwrap_or(0);
                files.push(DeltaAddFile {
                    path: paths.value(row).to_string(),
                    size: sizes.map_or(0, |s| s.value(row).max(0) as u64),
                    num_records,
                    partition_values: partition_values_at(add, row),
                    stats: stats_json,
                });
            }
        }
        if let (Some(want), Some(txn)) = (
            app_id,
            batch.column_by_name("txn").and_then(|c| c.as_struct_opt()),
        ) {
            let ids = txn
                .column_by_name("appId")
                .and_then(|c| c.as_string_opt::<i32>());
            let versions = txn
                .column_by_name("version")
                .and_then(|c| c.as_primitive_opt::<arrow::datatypes::Int64Type>());
            if let (Some(ids), Some(versions)) = (ids, versions) {
                for row in 0..txn.len() {
                    if txn.is_valid(row) && !ids.is_null(row) && ids.value(row) == want {
                        let v = versions.value(row);
                        txn_version = Some(txn_version.map_or(v, |c: i64| c.max(v)));
                    }
                }
            }
        }
        if let Some(meta) = batch
            .column_by_name("metaData")
            .and_then(|c| c.as_struct_opt())
        {
            let schema_strings = meta
                .column_by_name("schemaString")
                .and_then(|c| c.as_string_opt::<i32>());
            let partitions = meta
                .column_by_name("partitionColumns")
                .and_then(|c| c.as_list_opt::<i32>());
            if let Some(schema_strings) = schema_strings {
                for row in 0..meta.len() {
                    if !meta.is_valid(row) || schema_strings.is_null(row) {
                        continue;
                    }
                    let Ok(schema) = arrow_schema_from_delta(schema_strings.value(row)) else {
                        continue;
                    };
                    let partition_columns = partitions
                        .filter(|p| !p.is_null(row))
                        .map(|p| {
                            let values = p.value(row);
                            values
                                .as_string_opt::<i32>()
                                .map(|s| {
                                    (0..s.len())
                                        .filter(|i| !s.is_null(*i))
                                        .map(|i| s.value(i).to_string())
                                        .collect::<Vec<_>>()
                                })
                                .unwrap_or_default()
                        })
                        .unwrap_or_default();
                    metadata = Some(DeltaTableMetadata {
                        schema,
                        partition_columns,
                    });
                }
            }
        }
    }
    Ok((files, txn_version, metadata))
}

fn partition_values_at(add: &arrow::array::StructArray, row: usize) -> BTreeMap<String, String> {
    use arrow::array::AsArray;

    let Some(map) = add
        .column_by_name("partitionValues")
        .and_then(|c| c.as_map_opt())
    else {
        return BTreeMap::new();
    };
    if map.is_null(row) {
        return BTreeMap::new();
    }
    let entries = map.value(row);
    let keys = entries.column(0).as_string_opt::<i32>();
    let values = entries.column(1).as_string_opt::<i32>();
    let (Some(keys), Some(values)) = (keys, values) else {
        return BTreeMap::new();
    };
    (0..entries.len())
        .filter(|i| !keys.is_null(*i))
        .map(|i| {
            (
                keys.value(i).to_string(),
                if values.is_null(i) {
                    NULL_PARTITION.to_string()
                } else {
                    values.value(i).to_string()
                },
            )
        })
        .collect()
}

/// Render an Arrow schema as Delta's `schemaString` — Spark's own JSON schema encoding.
///
/// Every Delta reader parses this, not the Parquet footer, so an unmappable Arrow type has to be
/// an error rather than a guess: silently writing a type Spark cannot name would produce a table
/// that opens and then fails mid-scan.
pub fn delta_schema_string(schema: &SchemaRef) -> Result<String> {
    let fields = schema
        .fields()
        .iter()
        .map(spark_field_json)
        .collect::<Result<Vec<_>>>()?;
    Ok(serde_json::json!({ "type": "struct", "fields": fields }).to_string())
}

fn spark_field_json(field: &Arc<Field>) -> Result<serde_json::Value> {
    Ok(serde_json::json!({
        "name": field.name(),
        "type": spark_type_json(field.data_type())?,
        "nullable": field.is_nullable(),
        "metadata": {},
    }))
}

/// Arrow → Spark JSON type. Spark's names, not Arrow's: `long` (not `int64`), `integer`, etc.
fn spark_type_json(dt: &DataType) -> Result<serde_json::Value> {
    let name = match dt {
        DataType::Boolean => "boolean",
        DataType::Int8 => "byte",
        DataType::Int16 => "short",
        DataType::Int32 => "integer",
        DataType::Int64 => "long",
        DataType::Float32 => "float",
        DataType::Float64 => "double",
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string",
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "binary",
        DataType::Date32 => "date",
        // Delta's `timestamp` is UTC-instant semantics; a naive Arrow timestamp maps to
        // `timestamp_ntz`, which needs writer feature negotiation this minimal writer does not
        // do — so it is written as `timestamp` only when the Arrow type carries a zone.
        DataType::Timestamp(_, Some(_)) => "timestamp",
        DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Millisecond, None) => "timestamp",
        DataType::Decimal128(p, s) => {
            return Ok(serde_json::json!(format!("decimal({p},{s})")));
        }
        DataType::List(inner) | DataType::LargeList(inner) => {
            return Ok(serde_json::json!({
                "type": "array",
                "elementType": spark_type_json(inner.data_type())?,
                "containsNull": inner.is_nullable(),
            }));
        }
        DataType::Struct(fields) => {
            let inner = fields
                .iter()
                .map(spark_field_json)
                .collect::<Result<Vec<_>>>()?;
            return Ok(serde_json::json!({ "type": "struct", "fields": inner }));
        }
        other => {
            return Err(Error::Unsupported(format!(
                "no Delta/Spark type mapping for Arrow type `{other}` — cast the column before \
                 writing it to a Delta table"
            )));
        }
    };
    Ok(serde_json::json!(name))
}

/// The schema of the Parquet data files: the table schema minus its partition columns, which
/// Delta encodes in the directory path instead (this is what Spark writes).
fn data_file_schema(schema: &SchemaRef, partition_columns: &[String]) -> Result<SchemaRef> {
    if partition_columns.is_empty() {
        return Ok(schema.clone());
    }
    for c in partition_columns {
        if schema.field_with_name(c).is_err() {
            return Err(Error::Plan(format!(
                "partition column `{c}` is not in the table schema"
            )));
        }
    }
    let fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .filter(|f| !partition_columns.contains(f.name()))
        .cloned()
        .collect();
    if fields.is_empty() {
        return Err(Error::Plan(
            "every column is a partition column — a Delta table needs at least one data column"
                .into(),
        ));
    }
    Ok(Arc::new(Schema::new(fields)))
}

/// A batch group and the partition values every row in it shares.
type PartitionGroup = (BTreeMap<String, String>, Vec<RecordBatch>);

/// Split a micro-batch into one group per distinct partition-value combination.
///
/// Returns a single group with no partition values when the table is unpartitioned, which is the
/// path that avoids all of this work.
fn split_by_partition(
    batches: &[RecordBatch],
    partition_columns: &[String],
) -> Result<Vec<PartitionGroup>> {
    if partition_columns.is_empty() {
        return Ok(vec![(BTreeMap::new(), batches.to_vec())]);
    }
    let mut groups: BTreeMap<BTreeMap<String, String>, Vec<RecordBatch>> = BTreeMap::new();
    for batch in batches {
        let indices: Vec<usize> = partition_columns
            .iter()
            .map(|c| {
                batch
                    .schema()
                    .index_of(c)
                    .map_err(|_| Error::Plan(format!("partition column `{c}` is not in the batch")))
            })
            .collect::<Result<_>>()?;

        // Row → its partition key, then one filtered batch per distinct key.
        let mut per_key: BTreeMap<BTreeMap<String, String>, Vec<u32>> = BTreeMap::new();
        for row in 0..batch.num_rows() {
            let key: BTreeMap<String, String> = partition_columns
                .iter()
                .zip(&indices)
                .map(|(name, idx)| {
                    (
                        name.clone(),
                        partition_value_string(batch.column(*idx), row),
                    )
                })
                .collect();
            per_key.entry(key).or_default().push(row as u32);
        }

        let keep: Vec<usize> = (0..batch.num_columns())
            .filter(|i| !indices.contains(i))
            .collect();
        for (key, rows) in per_key {
            let taken =
                arrow::compute::take_record_batch(batch, &arrow::array::UInt32Array::from(rows))
                    .map_err(|e| Error::Execution(format!("partition split: {e}")))?;
            let projected = taken
                .project(&keep)
                .map_err(|e| Error::Execution(format!("partition projection: {e}")))?;
            groups.entry(key).or_default().push(projected);
        }
    }
    Ok(groups.into_iter().collect())
}

/// One row's value for a partition column, in Delta's string encoding.
fn partition_value_string(array: &ArrayRef, row: usize) -> String {
    use arrow::array::AsArray;
    use arrow::datatypes::*;

    if array.is_null(row) {
        return NULL_PARTITION.to_string();
    }
    match array.data_type() {
        DataType::Utf8 => array.as_string::<i32>().value(row).to_string(),
        DataType::LargeUtf8 => array.as_string::<i64>().value(row).to_string(),
        DataType::Boolean => array.as_boolean().value(row).to_string(),
        DataType::Int8 => array.as_primitive::<Int8Type>().value(row).to_string(),
        DataType::Int16 => array.as_primitive::<Int16Type>().value(row).to_string(),
        DataType::Int32 => array.as_primitive::<Int32Type>().value(row).to_string(),
        DataType::Int64 => array.as_primitive::<Int64Type>().value(row).to_string(),
        DataType::Date32 => {
            let days = array.as_primitive::<Date32Type>().value(row);
            chrono::DateTime::from_timestamp(days as i64 * 86_400, 0)
                .map(|d| d.format("%Y-%m-%d").to_string())
                .unwrap_or_else(|| days.to_string())
        }
        // Anything else is rendered through Arrow's display, which is stable and round-trips for
        // the types a partition column realistically uses.
        _ => arrow::util::display::array_value_to_string(array, row)
            .unwrap_or_else(|_| NULL_PARTITION.to_string()),
    }
}

/// `col=value/col2=value2`, percent-escaped the way Hive and Delta expect.
fn partition_dir(
    values: &BTreeMap<String, String>,
    partition_columns: &[String],
) -> Option<String> {
    if partition_columns.is_empty() {
        return None;
    }
    Some(
        partition_columns
            .iter()
            .map(|c| {
                let v = values.get(c).map_or(NULL_PARTITION, |v| v.as_str());
                format!("{}={}", escape_partition(c), escape_partition(v))
            })
            .collect::<Vec<_>>()
            .join("/"),
    )
}

fn escape_partition(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '/' | '=' | ':' | ' ' | '%' | '\\' | '?' | '#' | '[' | ']' => {
                let mut buf = [0u8; 4];
                for b in c.encode_utf8(&mut buf).as_bytes() {
                    out.push_str(&format!("%{b:02X}"));
                }
            }
            _ => out.push(c),
        }
    }
    out
}

/// Serialize `batches` to one in-memory Parquet file.
fn encode_parquet(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    compression: parquet::basic::Compression,
) -> Result<Vec<u8>> {
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let props = WriterProperties::builder()
        .set_compression(compression)
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

/// Delta `stats` JSON for one data file: row count plus per-column min/max/nullCount.
///
/// This is the entire basis for Delta file skipping. Without it a dashboard filtering on a
/// timestamp reads every file in the table, because no reader can prove a file is irrelevant.
pub fn file_stats(schema: &SchemaRef, batches: &[RecordBatch], num_records: u64) -> String {
    let mut min_values = serde_json::Map::new();
    let mut max_values = serde_json::Map::new();
    let mut null_counts = serde_json::Map::new();

    for (index, field) in schema.fields().iter().enumerate() {
        let mut nulls = 0u64;
        let mut min: Option<serde_json::Value> = None;
        let mut max: Option<serde_json::Value> = None;
        for batch in batches {
            let Some(column) = batch.columns().get(index) else {
                continue;
            };
            nulls += column.null_count() as u64;
            if let Some((lo, hi)) = column_bounds(column) {
                min = Some(min.map_or(lo.clone(), |m| json_min(m, lo)));
                max = Some(max.map_or(hi.clone(), |m| json_max(m, hi)));
            }
        }
        null_counts.insert(field.name().clone(), serde_json::json!(nulls));
        if let Some(v) = min {
            min_values.insert(field.name().clone(), v);
        }
        if let Some(v) = max {
            max_values.insert(field.name().clone(), v);
        }
    }

    serde_json::json!({
        "numRecords": num_records,
        "minValues": min_values,
        "maxValues": max_values,
        "nullCount": null_counts,
    })
    .to_string()
}

/// Min and max of one column, as the JSON values Delta's `stats` expects.
///
/// Types Delta cannot compare (nested, binary) return `None`: an absent statistic makes a reader
/// scan the file, whereas a wrong one makes it skip a file that has matching rows.
fn column_bounds(column: &ArrayRef) -> Option<(serde_json::Value, serde_json::Value)> {
    use arrow::array::AsArray;
    use arrow::compute::kernels::aggregate;
    use arrow::datatypes::*;

    macro_rules! numeric {
        ($t:ty) => {{
            let a = column.as_primitive::<$t>();
            let lo = aggregate::min(a)?;
            let hi = aggregate::max(a)?;
            Some((serde_json::json!(lo), serde_json::json!(hi)))
        }};
    }

    match column.data_type() {
        DataType::Int8 => numeric!(Int8Type),
        DataType::Int16 => numeric!(Int16Type),
        DataType::Int32 => numeric!(Int32Type),
        DataType::Int64 => numeric!(Int64Type),
        DataType::UInt8 => numeric!(UInt8Type),
        DataType::UInt16 => numeric!(UInt16Type),
        DataType::UInt32 => numeric!(UInt32Type),
        DataType::UInt64 => numeric!(UInt64Type),
        DataType::Float32 => numeric!(Float32Type),
        DataType::Float64 => numeric!(Float64Type),
        DataType::Boolean => {
            let a = column.as_boolean();
            let lo = aggregate::min_boolean(a)?;
            let hi = aggregate::max_boolean(a)?;
            Some((serde_json::json!(lo), serde_json::json!(hi)))
        }
        DataType::Utf8 => {
            let a = column.as_string::<i32>();
            Some((
                serde_json::json!(aggregate::min_string(a)?),
                serde_json::json!(aggregate::max_string(a)?),
            ))
        }
        DataType::LargeUtf8 => {
            let a = column.as_string::<i64>();
            Some((
                serde_json::json!(aggregate::min_string(a)?),
                serde_json::json!(aggregate::max_string(a)?),
            ))
        }
        DataType::Date32 => {
            let a = column.as_primitive::<Date32Type>();
            let fmt = |days: i32| {
                chrono::DateTime::from_timestamp(days as i64 * 86_400, 0)
                    .map(|d| d.format("%Y-%m-%d").to_string())
                    .unwrap_or_default()
            };
            Some((
                serde_json::json!(fmt(aggregate::min(a)?)),
                serde_json::json!(fmt(aggregate::max(a)?)),
            ))
        }
        DataType::Timestamp(unit, _) => {
            let (lo, hi) = timestamp_bounds(column, *unit)?;
            Some((serde_json::json!(lo), serde_json::json!(hi)))
        }
        _ => None,
    }
}

/// Timestamp min/max as ISO-8601 UTC strings, which is how Delta encodes them in `stats`.
fn timestamp_bounds(column: &ArrayRef, unit: TimeUnit) -> Option<(String, String)> {
    use arrow::array::AsArray;
    use arrow::compute::kernels::aggregate;
    use arrow::datatypes::*;

    // Each Arrow timestamp unit is a distinct concrete array type, so the downcast has to match
    // the unit exactly — reading a millisecond array as microseconds panics.
    let (lo, hi) = match unit {
        TimeUnit::Second => {
            let a = column.as_primitive::<TimestampSecondType>();
            (
                aggregate::min(a)? * 1_000_000,
                aggregate::max(a)? * 1_000_000,
            )
        }
        TimeUnit::Millisecond => {
            let a = column.as_primitive::<TimestampMillisecondType>();
            (aggregate::min(a)? * 1_000, aggregate::max(a)? * 1_000)
        }
        TimeUnit::Microsecond => {
            let a = column.as_primitive::<TimestampMicrosecondType>();
            (aggregate::min(a)?, aggregate::max(a)?)
        }
        TimeUnit::Nanosecond => {
            let a = column.as_primitive::<TimestampNanosecondType>();
            (aggregate::min(a)? / 1_000, aggregate::max(a)? / 1_000)
        }
    };
    let render = |micros: i64| {
        chrono::DateTime::from_timestamp_micros(micros)
            .map(|d| d.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
    };
    Some((render(lo)?, render(hi)?))
}

fn json_min(a: serde_json::Value, b: serde_json::Value) -> serde_json::Value {
    if json_lt(&b, &a) {
        b
    } else {
        a
    }
}

fn json_max(a: serde_json::Value, b: serde_json::Value) -> serde_json::Value {
    if json_lt(&a, &b) {
        b
    } else {
        a
    }
}

fn json_lt(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    match (a.as_f64(), b.as_f64()) {
        (Some(x), Some(y)) => x < y,
        _ => match (a.as_str(), b.as_str()) {
            (Some(x), Some(y)) => x < y,
            _ => match (a.as_bool(), b.as_bool()) {
                (Some(x), Some(y)) => !x & y,
                _ => false,
            },
        },
    }
}

/// Render the newline-delimited JSON body of one commit.
pub fn render_commit(
    version: u64,
    schema: &SchemaRef,
    files: &[DeltaAddFile],
    table_id: &str,
    partition_columns: &[String],
    txn: Option<&DeltaTxn>,
) -> Result<String> {
    let now_ms = chrono::Utc::now().timestamp_millis();
    let mut lines = Vec::new();

    // Version 0 declares the table: protocol first, then metaData. Every later commit is
    // data-only, which is exactly what Spark's append sink emits.
    if version == 0 {
        lines.push(
            serde_json::json!({
                "protocol": {
                    "minReaderVersion": MIN_READER_VERSION,
                    "minWriterVersion": MIN_WRITER_VERSION,
                }
            })
            .to_string(),
        );
        lines.push(metadata_action(schema, table_id, partition_columns, now_ms)?.to_string());
    }

    // The `txn` action goes before the data actions so a reader that stops at the first
    // recognized transaction sees it.
    if let Some(txn) = txn {
        lines.push(
            serde_json::json!({
                "txn": {
                    "appId": txn.app_id,
                    "version": txn.version,
                    "lastUpdated": now_ms,
                }
            })
            .to_string(),
        );
    }

    for f in files {
        lines.push(
            serde_json::json!({
                "add": {
                    "path": f.path,
                    "partitionValues": f.partition_values,
                    "size": f.size,
                    "modificationTime": now_ms,
                    "dataChange": true,
                    "stats": f.stats.clone().unwrap_or_else(|| {
                        serde_json::json!({"numRecords": f.num_records}).to_string()
                    }),
                }
            })
            .to_string(),
        );
    }

    let mut body = lines.join("\n");
    body.push('\n');
    Ok(body)
}

fn metadata_action(
    schema: &SchemaRef,
    table_id: &str,
    partition_columns: &[String],
    now_ms: i64,
) -> Result<serde_json::Value> {
    Ok(serde_json::json!({
        "metaData": {
            "id": table_id,
            "format": {"provider": "parquet", "options": {}},
            "schemaString": delta_schema_string(schema)?,
            "partitionColumns": partition_columns,
            "configuration": {},
            "createdTime": now_ms,
        }
    }))
}

/// Render a checkpoint: every live action as one Parquet file.
///
/// The column set is Delta's checkpoint schema. Each row populates exactly one of the top-level
/// struct columns and leaves the rest null, which is what makes the file a flat replay of the
/// log's actions.
pub fn render_checkpoint(
    schema: &SchemaRef,
    files: &[DeltaAddFile],
    table_id: &str,
    partition_columns: &[String],
    txn: Option<&DeltaTxn>,
) -> Result<Vec<u8>> {
    use arrow::array::*;
    use arrow::datatypes::Int64Type;

    let now_ms = chrono::Utc::now().timestamp_millis();
    let checkpoint_schema = checkpoint_arrow_schema();

    let rows = files.len() + 2 + usize::from(txn.is_some());

    // --- txn ------------------------------------------------------------------------------
    let mut txn_app = StringBuilder::new();
    let mut txn_version = PrimitiveBuilder::<Int64Type>::new();
    let mut txn_updated = PrimitiveBuilder::<Int64Type>::new();
    let mut txn_valid = Vec::with_capacity(rows);

    // --- add ------------------------------------------------------------------------------
    let mut add_path = StringBuilder::new();
    let mut add_partitions = new_string_map_builder();
    let mut add_size = PrimitiveBuilder::<Int64Type>::new();
    let mut add_modified = PrimitiveBuilder::<Int64Type>::new();
    let mut add_change = BooleanBuilder::new();
    let mut add_stats = StringBuilder::new();
    let mut add_valid = Vec::with_capacity(rows);

    // --- metaData / protocol --------------------------------------------------------------
    let mut meta_id = StringBuilder::new();
    let mut meta_provider = StringBuilder::new();
    let mut meta_format_options = new_string_map_builder();
    let mut meta_configuration = new_string_map_builder();
    let mut meta_schema = StringBuilder::new();
    let mut meta_partitions = ListBuilder::new(StringBuilder::new());
    let mut meta_created = PrimitiveBuilder::<Int64Type>::new();
    let mut meta_valid = Vec::with_capacity(rows);

    let mut proto_reader = PrimitiveBuilder::<arrow::datatypes::Int32Type>::new();
    let mut proto_writer = PrimitiveBuilder::<arrow::datatypes::Int32Type>::new();
    let mut proto_valid = Vec::with_capacity(rows);

    let push_empty_txn = |b: &mut StringBuilder,
                          v: &mut PrimitiveBuilder<Int64Type>,
                          u: &mut PrimitiveBuilder<Int64Type>| {
        b.append_null();
        v.append_null();
        u.append_null();
    };

    // Row 0: protocol. Row 1: metaData. Then the optional txn, then one row per file.
    for row in 0..rows {
        let is_protocol = row == 0;
        let is_metadata = row == 1;
        let is_txn = txn.is_some() && row == 2;
        let file_index = row.checked_sub(2 + usize::from(txn.is_some()));

        proto_valid.push(is_protocol);
        if is_protocol {
            proto_reader.append_value(MIN_READER_VERSION);
            proto_writer.append_value(MIN_WRITER_VERSION);
        } else {
            proto_reader.append_null();
            proto_writer.append_null();
        }

        meta_valid.push(is_metadata);
        if is_metadata {
            meta_id.append_value(table_id);
            meta_provider.append_value("parquet");
            meta_schema.append_value(delta_schema_string(schema)?);
            for c in partition_columns {
                meta_partitions.values().append_value(c);
            }
            meta_partitions.append(true);
            meta_created.append_value(now_ms);
        } else {
            meta_id.append_null();
            meta_provider.append_null();
            meta_schema.append_null();
            meta_partitions.append(false);
            meta_created.append_null();
        }
        meta_format_options
            .append(is_metadata)
            .map_err(|e| Error::Execution(format!("checkpoint format.options: {e}")))?;
        meta_configuration
            .append(is_metadata)
            .map_err(|e| Error::Execution(format!("checkpoint configuration: {e}")))?;

        txn_valid.push(is_txn);
        match (is_txn, txn) {
            (true, Some(t)) => {
                txn_app.append_value(&t.app_id);
                txn_version.append_value(t.version);
                txn_updated.append_value(now_ms);
            }
            _ => push_empty_txn(&mut txn_app, &mut txn_version, &mut txn_updated),
        }

        match file_index.and_then(|i| files.get(i)) {
            Some(f) => {
                add_valid.push(true);
                add_path.append_value(&f.path);
                for (k, v) in &f.partition_values {
                    add_partitions.keys().append_value(k);
                    add_partitions.values().append_value(v);
                }
                add_partitions
                    .append(true)
                    .map_err(|e| Error::Execution(format!("checkpoint partitionValues: {e}")))?;
                add_size.append_value(f.size as i64);
                add_modified.append_value(now_ms);
                add_change.append_value(true);
                match &f.stats {
                    Some(s) => add_stats.append_value(s),
                    None => add_stats.append_null(),
                }
            }
            None => {
                add_valid.push(false);
                add_path.append_null();
                add_partitions
                    .append(false)
                    .map_err(|e| Error::Execution(format!("checkpoint partitionValues: {e}")))?;
                add_size.append_null();
                add_modified.append_null();
                add_change.append_null();
                add_stats.append_null();
            }
        }
    }

    let DataType::Struct(txn_fields) = checkpoint_schema.field(0).data_type().clone() else {
        unreachable!("txn column is a struct")
    };
    let DataType::Struct(add_fields) = checkpoint_schema.field(1).data_type().clone() else {
        unreachable!("add column is a struct")
    };
    let DataType::Struct(meta_fields) = checkpoint_schema.field(2).data_type().clone() else {
        unreachable!("metaData column is a struct")
    };
    let DataType::Struct(proto_fields) = checkpoint_schema.field(3).data_type().clone() else {
        unreachable!("protocol column is a struct")
    };
    let DataType::Struct(format_fields) = meta_fields[1].data_type().clone() else {
        unreachable!("format is a struct")
    };

    let format_struct = StructArray::new(
        format_fields,
        vec![
            Arc::new(meta_provider.finish()) as ArrayRef,
            Arc::new(meta_format_options.finish()),
        ],
        Some(meta_valid.iter().copied().collect()),
    );

    let txn_array = StructArray::new(
        txn_fields,
        vec![
            Arc::new(txn_app.finish()) as ArrayRef,
            Arc::new(txn_version.finish()),
            Arc::new(txn_updated.finish()),
        ],
        Some(txn_valid.into_iter().collect()),
    );
    let add_array = StructArray::new(
        add_fields,
        vec![
            Arc::new(add_path.finish()) as ArrayRef,
            Arc::new(add_partitions.finish()),
            Arc::new(add_size.finish()),
            Arc::new(add_modified.finish()),
            Arc::new(add_change.finish()),
            Arc::new(add_stats.finish()),
        ],
        Some(add_valid.into_iter().collect()),
    );
    let meta_array = StructArray::new(
        meta_fields,
        vec![
            Arc::new(meta_id.finish()) as ArrayRef,
            Arc::new(format_struct),
            Arc::new(meta_schema.finish()),
            Arc::new(meta_partitions.finish()),
            Arc::new(meta_configuration.finish()),
            Arc::new(meta_created.finish()),
        ],
        Some(meta_valid.into_iter().collect()),
    );
    let proto_array = StructArray::new(
        proto_fields,
        vec![
            Arc::new(proto_reader.finish()) as ArrayRef,
            Arc::new(proto_writer.finish()),
        ],
        Some(proto_valid.into_iter().collect()),
    );

    let batch = RecordBatch::try_new(
        checkpoint_schema.clone(),
        vec![
            Arc::new(txn_array),
            Arc::new(add_array),
            Arc::new(meta_array),
            Arc::new(proto_array),
        ],
    )
    .map_err(|e| Error::Execution(format!("checkpoint batch: {e}")))?;

    encode_parquet(
        &checkpoint_schema,
        &[batch],
        parquet::basic::Compression::SNAPPY,
    )
}

/// A `map<string, string>` builder with Delta's expected entry/key/value field names.
///
/// Arrow's defaults (`entries`/`keys`/`values`) do not match what Delta and Spark write, and a
/// reader looking up `key_value` in a checkpoint would not find the map at all.
fn new_string_map_builder(
) -> arrow::array::MapBuilder<arrow::array::StringBuilder, arrow::array::StringBuilder> {
    use arrow::array::{MapBuilder, MapFieldNames, StringBuilder};
    MapBuilder::new(
        Some(MapFieldNames {
            entry: "key_value".into(),
            key: "key".into(),
            value: "value".into(),
        }),
        StringBuilder::new(),
        StringBuilder::new(),
    )
}

/// Delta's checkpoint file schema, as Arrow.
fn checkpoint_arrow_schema() -> SchemaRef {
    let string_map: DataType = DataType::Map(
        Arc::new(Field::new(
            "key_value",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    );
    Arc::new(Schema::new(vec![
        Field::new(
            "txn",
            DataType::Struct(
                vec![
                    Field::new("appId", DataType::Utf8, true),
                    Field::new("version", DataType::Int64, true),
                    Field::new("lastUpdated", DataType::Int64, true),
                ]
                .into(),
            ),
            true,
        ),
        Field::new(
            "add",
            DataType::Struct(
                vec![
                    Field::new("path", DataType::Utf8, true),
                    Field::new("partitionValues", string_map.clone(), true),
                    Field::new("size", DataType::Int64, true),
                    Field::new("modificationTime", DataType::Int64, true),
                    Field::new("dataChange", DataType::Boolean, true),
                    Field::new("stats", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        ),
        Field::new(
            "metaData",
            DataType::Struct(
                vec![
                    Field::new("id", DataType::Utf8, true),
                    Field::new(
                        "format",
                        DataType::Struct(
                            vec![
                                Field::new("provider", DataType::Utf8, true),
                                // Required by the Delta checkpoint schema, and delta-kernel
                                // refuses a checkpoint without it even though it is always empty
                                // for Parquet.
                                Field::new("options", string_map.clone(), true),
                            ]
                            .into(),
                        ),
                        true,
                    ),
                    Field::new("schemaString", DataType::Utf8, true),
                    Field::new(
                        "partitionColumns",
                        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                        true,
                    ),
                    Field::new("configuration", string_map.clone(), true),
                    Field::new("createdTime", DataType::Int64, true),
                ]
                .into(),
            ),
            true,
        ),
        Field::new(
            "protocol",
            DataType::Struct(
                vec![
                    Field::new("minReaderVersion", DataType::Int32, true),
                    Field::new("minWriterVersion", DataType::Int32, true),
                ]
                .into(),
            ),
            true,
        ),
    ]))
}

/// The next unused commit version for the table at `root`, scanning forward from `from`.
///
/// Exposed for callers that have no writer handle. A [`DeltaTableWriter`] tracks this itself —
/// listing the log on every commit is what makes a once-a-second table quadratic in its history.
pub async fn next_version(store: &dyn ObjectStore, root: &ObjectPath) -> Result<u64> {
    use futures::TryStreamExt;

    let log_dir = object_path_join(root, "_delta_log");
    let mut highest: Option<u64> = None;
    let mut listing = store.list(Some(&log_dir));
    while let Some(meta) = listing
        .try_next()
        .await
        .map_err(|e| Error::Io(format!("list `{log_dir}`: {e}")))?
    {
        let Some(name) = meta.location.filename() else {
            continue;
        };
        let Some(stem) = name.strip_suffix(".json") else {
            continue;
        };
        if let Ok(v) = stem.parse::<u64>() {
            highest = Some(highest.map_or(v, |h: u64| h.max(v)));
        }
    }
    Ok(highest.map_or(0, |h| h + 1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use object_store::local::LocalFileSystem;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap()
    }

    fn store_at(dir: &std::path::Path) -> Arc<dyn ObjectStore> {
        Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap())
    }

    fn config() -> DeltaWriterConfig {
        DeltaWriterConfig {
            table_id: "tbl-uuid".into(),
            ..Default::default()
        }
    }

    async fn writer(dir: &std::path::Path, config: DeltaWriterConfig) -> DeltaTableWriter {
        DeltaTableWriter::open(store_at(dir), ObjectPath::from("tbl"), schema(), config)
            .await
            .unwrap()
    }

    #[test]
    fn schema_string_uses_spark_type_names() {
        let s = delta_schema_string(&schema()).unwrap();
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["type"], "struct");
        assert_eq!(v["fields"][0]["name"], "id");
        // `long`, not Arrow's `int64` — a Delta reader would not recognize the Arrow spelling.
        assert_eq!(v["fields"][0]["type"], "long");
        assert_eq!(v["fields"][0]["nullable"], false);
        assert_eq!(v["fields"][1]["type"], "string");
        assert_eq!(v["fields"][1]["nullable"], true);
    }

    #[test]
    fn schema_string_rejects_unmappable_types() {
        let s: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "d",
            DataType::Duration(TimeUnit::Second),
            true,
        )]));
        let err = delta_schema_string(&s).unwrap_err();
        assert!(
            matches!(err, Error::Unsupported(_)),
            "expected Unsupported, got {err:?}"
        );
    }

    #[test]
    fn version_zero_commit_declares_the_table() {
        let files = vec![DeltaAddFile {
            path: "part-0.parquet".into(),
            size: 10,
            num_records: 3,
            partition_values: BTreeMap::new(),
            stats: None,
        }];
        let body = render_commit(0, &schema(), &files, "tbl-uuid", &[], None).unwrap();
        let lines: Vec<_> = body.trim_end().split('\n').collect();
        assert_eq!(lines.len(), 3, "protocol + metaData + add");
        assert!(lines[0].contains("\"protocol\""));
        assert!(lines[1].contains("\"metaData\""));
        assert!(lines[2].contains("\"add\""));

        // A later version is data-only.
        let body = render_commit(7, &schema(), &files, "tbl-uuid", &[], None).unwrap();
        let lines: Vec<_> = body.trim_end().split('\n').collect();
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("\"add\""));
    }

    #[tokio::test]
    async fn append_writes_data_and_numbers_commits_contiguously() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut w = writer(dir.path(), config()).await;

        for expected in 0u64..3 {
            let commit = w.append(&[batch()], "abc", None).await.unwrap();
            assert_eq!(commit.version, expected);
            assert_eq!(commit.rows, 3);
        }

        assert_eq!(w.next_version(), 3);
        for v in 0u64..3 {
            let p = dir
                .path()
                .join("tbl/_delta_log")
                .join(format!("{v:020}.json"));
            assert!(p.exists(), "missing commit {v} at {}", p.display());
        }
    }

    #[tokio::test]
    async fn the_common_path_never_lists_the_log() {
        // Listing `_delta_log` per commit is O(history) per append and O(history^2) overall,
        // which is what makes a once-a-second table stop working after a few days. The writer
        // must remember where it is instead.
        let dir = tempfile::TempDir::new().unwrap();
        let mut w = writer(dir.path(), config()).await;
        w.append(&[batch()], "a", None).await.unwrap();
        assert_eq!(w.next_version(), 1);

        // Deleting the log out from under the writer proves the next commit consulted memory,
        // not storage: a listing writer would restart at version 0.
        std::fs::remove_file(dir.path().join("tbl/_delta_log/00000000000000000000.json")).unwrap();
        let commit = w.append(&[batch()], "b", None).await.unwrap();
        assert_eq!(commit.version, 1, "the cached version was used");
    }

    #[tokio::test]
    async fn append_never_overwrites_a_commit_written_by_someone_else() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = store_at(dir.path());
        let root = ObjectPath::from("tbl");

        // A commit written by another writer, byte-identifiable.
        store
            .put(
                &commit_path(&root, 0),
                b"{\"marker\":\"someone-else\"}\n".to_vec().into(),
            )
            .await
            .unwrap();

        let mut w = DeltaTableWriter::open(
            store.clone(),
            root.clone(),
            schema(),
            DeltaWriterConfig::default(),
        )
        .await
        .unwrap();
        let commit = w.append(&[batch()], "a", None).await.unwrap();

        assert_eq!(commit.version, 1, "must land after the existing commit");
        let body =
            std::fs::read_to_string(dir.path().join("tbl/_delta_log/00000000000000000000.json"))
                .unwrap();
        assert_eq!(
            body, "{\"marker\":\"someone-else\"}\n",
            "the pre-existing commit was overwritten"
        );
    }

    #[tokio::test]
    async fn a_replayed_batch_is_recognized_by_its_txn_stamp_and_not_written_twice() {
        // The hazard: a crash between the sink write and the offset checkpoint replays the batch,
        // which would otherwise double every row a dashboard counts.
        let dir = tempfile::TempDir::new().unwrap();
        let cfg = DeltaWriterConfig {
            app_id: Some("query-1".into()),
            ..config()
        };
        let mut w = writer(dir.path(), cfg.clone()).await;
        assert_eq!(w.append(&[batch()], "a", Some(1)).await.unwrap().rows, 3);
        assert_eq!(w.append(&[batch()], "b", Some(2)).await.unwrap().rows, 3);

        // A fresh writer on the same table — as after a restart — replays batch 2.
        let mut restarted = writer(dir.path(), cfg).await;
        let replay = restarted.append(&[batch()], "b", Some(2)).await.unwrap();
        assert!(replay.deduplicated, "batch 2 was already committed");
        assert_eq!(replay.rows, 0);

        // But batch 3 is new and lands.
        assert_eq!(
            restarted
                .append(&[batch()], "c", Some(3))
                .await
                .unwrap()
                .rows,
            3
        );
    }

    #[tokio::test]
    async fn a_different_app_id_is_not_deduplicated_against_ours() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut ours = writer(
            dir.path(),
            DeltaWriterConfig {
                app_id: Some("query-1".into()),
                ..config()
            },
        )
        .await;
        ours.append(&[batch()], "a", Some(5)).await.unwrap();

        let mut theirs = writer(
            dir.path(),
            DeltaWriterConfig {
                app_id: Some("query-2".into()),
                ..config()
            },
        )
        .await;
        let commit = theirs.append(&[batch()], "b", Some(1)).await.unwrap();
        assert!(!commit.deduplicated, "another query's txn is not ours");
        assert_eq!(commit.rows, 3);
    }

    #[tokio::test]
    async fn stats_carry_per_column_bounds_so_readers_can_skip_files() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut w = writer(dir.path(), config()).await;
        let commit = w.append(&[batch()], "a", None).await.unwrap();

        let stats: serde_json::Value =
            serde_json::from_str(commit.files[0].stats.as_ref().unwrap()).unwrap();
        assert_eq!(stats["numRecords"], 3);
        assert_eq!(stats["minValues"]["id"], 1);
        assert_eq!(stats["maxValues"]["id"], 3);
        assert_eq!(stats["minValues"]["name"], "a");
        assert_eq!(stats["maxValues"]["name"], "c");
        assert_eq!(stats["nullCount"]["name"], 1);
        assert_eq!(stats["nullCount"]["id"], 0);
    }

    #[test]
    fn timestamp_stats_read_each_arrow_unit_as_itself() {
        // Downcasting a millisecond array as microseconds panics, and Kafka's `timestamp` column
        // is milliseconds — so every unit has to be matched exactly.
        use arrow::array::{TimestampMillisecondArray, TimestampNanosecondArray};

        let ms: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![1_000i64, 2_000]));
        let (lo, hi) = column_bounds(&ms).expect("millisecond bounds");
        assert_eq!(lo, "1970-01-01T00:00:01.000Z");
        assert_eq!(hi, "1970-01-01T00:00:02.000Z");

        let ns: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![1_000_000_000i64]));
        let (lo, _) = column_bounds(&ns).expect("nanosecond bounds");
        assert_eq!(lo, "1970-01-01T00:00:01.000Z");
    }

    #[tokio::test]
    async fn checkpoints_let_a_reader_skip_the_commits_they_cover() {
        // The proof that a checkpoint is real: delete every JSON commit it covers — which is what
        // Delta log retention does — and the table must still resolve.
        let dir = tempfile::TempDir::new().unwrap();
        let cfg = DeltaWriterConfig {
            checkpoint_interval: 3,
            ..config()
        };
        let mut w = writer(dir.path(), cfg).await;
        for _ in 0..3 {
            w.append(&[batch()], "a", None).await.unwrap();
        }

        let checkpoint = dir
            .path()
            .join("tbl/_delta_log/00000000000000000002.checkpoint.parquet");
        assert!(checkpoint.exists(), "checkpoint written at version 2");
        let hint =
            std::fs::read_to_string(dir.path().join("tbl/_delta_log/_last_checkpoint")).unwrap();
        assert!(hint.contains("\"version\":2"), "{hint}");

        // A fresh writer recovers the full live-file set from the checkpoint alone.
        for v in 0u64..3 {
            std::fs::remove_file(dir.path().join(format!("tbl/_delta_log/{v:020}.json"))).unwrap();
        }
        let recovered = writer(dir.path(), config()).await;
        assert_eq!(
            recovered.live_files().map(|f| f.len()),
            Some(3),
            "all three files survive in the checkpoint"
        );
        assert_eq!(recovered.next_version(), 3);
    }

    #[tokio::test]
    async fn partitioned_writes_land_in_hive_directories_without_the_column_in_the_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let cfg = DeltaWriterConfig {
            partition_columns: vec!["name".into()],
            ..config()
        };
        let mut w = writer(dir.path(), cfg).await;
        let commit = w.append(&[batch()], "a", None).await.unwrap();

        // Three rows, three distinct names (one null) — three files.
        assert_eq!(commit.files.len(), 3);
        let paths: Vec<&str> = commit.files.iter().map(|f| f.path.as_str()).collect();
        assert!(paths.iter().any(|p| p.starts_with("name=a/")), "{paths:?}");
        assert!(
            paths
                .iter()
                .any(|p| p.starts_with("name=__HIVE_DEFAULT_PARTITION__/")),
            "{paths:?}"
        );
        assert_eq!(
            commit.files[0].partition_values.len(),
            1,
            "partition value recorded in the add action"
        );

        // The partition column is in the path, not the data file — which is what Spark writes.
        assert_eq!(w.data_schema.fields().len(), 1);
        assert_eq!(w.data_schema.field(0).name(), "id");
    }

    #[test]
    fn partition_values_are_escaped_for_the_path() {
        assert_eq!(escape_partition("a/b"), "a%2Fb");
        assert_eq!(escape_partition("2026-08-17"), "2026-08-17");
        assert_eq!(escape_partition("x y"), "x%20y");
    }

    #[tokio::test]
    async fn next_version_ignores_checkpoints_and_last_checkpoint() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let root = ObjectPath::from("tbl");
        let log = root.clone().join("_delta_log");

        for f in [
            "00000000000000000000.json",
            "00000000000000000001.json",
            "00000000000000000001.checkpoint.parquet",
            "_last_checkpoint",
        ] {
            store
                .put(&log.clone().join(f), b"x".to_vec().into())
                .await
                .unwrap();
        }
        assert_eq!(next_version(&store, &root).await.unwrap(), 2);
    }
    /// A checkpoint that fails once must not disable checkpointing for the life of the query.
    /// Unbounded log growth is the thing checkpointing exists to prevent, so trading a transient
    /// object-store error for it permanently is the wrong bargain.
    #[tokio::test]
    async fn a_transient_checkpoint_failure_does_not_disable_checkpointing() {
        let dir = tempfile::TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        let mut writer = DeltaTableWriter::open(
            store,
            ObjectPath::from("tbl"),
            schema.clone(),
            DeltaWriterConfig {
                table_id: "t".into(),
                checkpoint_interval: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        // One failure short of the limit leaves checkpointing enabled.
        writer.checkpoint_failures = CHECKPOINT_FAILURE_LIMIT - 1;
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1i64]))])
                .unwrap();
        writer
            .append(std::slice::from_ref(&batch), "p", None)
            .await
            .unwrap();
        assert_eq!(
            writer.checkpoint_failures, 0,
            "a successful checkpoint must clear the failure streak"
        );
        assert_eq!(
            writer.config.checkpoint_interval, 1,
            "checkpointing must still be enabled"
        );
    }
}
