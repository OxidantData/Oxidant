//! `weft-datasource` — turn storage into Arrow record batches.
//!
//! Lakehouse reads use engine-agnostic kernels to resolve a pinned snapshot to Parquet files,
//! sizes, schema mappings, and row-delete metadata. DataFusion remains the data reader, so the
//! resolver does not couple the engine to another DataFusion version.

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use url::Url;
use weft_common::{Error, Result};

/// A stable identity for one resolved lakehouse snapshot.
///
/// Persist this value with a distributed query and pass it back through
/// [`active_files_for_scan`] so every worker resolves exactly the same table state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "format", rename_all = "snake_case")]
pub enum SnapshotIdentity {
    /// A Delta transaction-log version.
    Delta {
        /// The pinned `_delta_log` version.
        version: u64,
    },
    /// An Iceberg snapshot selected from one authoritative metadata JSON file.
    Iceberg {
        /// The pinned Iceberg snapshot ID.
        snapshot_id: i64,
        /// The snapshot sequence number (zero for v1 tables).
        sequence_number: i64,
        /// The metadata JSON that was used to select the snapshot.
        metadata_location: String,
    },
}

/// Logical-to-physical field identity supplied by the table format.
///
/// Delta column mapping uses `physical_path`; Iceberg normally leaves it equal to
/// `logical_path` and relies on `field_id` across schema renames.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnMapping {
    /// Dotted logical field path exposed to SQL.
    pub logical_path: String,
    /// Dotted physical path stored in Parquet.
    pub physical_path: String,
    /// Stable Delta or Iceberg field ID, when the format supplies one.
    pub field_id: Option<i32>,
}

/// Physical encoding of an Iceberg data or delete file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IcebergFileFormat {
    /// Apache Avro.
    Avro,
    /// Apache ORC.
    Orc,
    /// Apache Parquet.
    Parquet,
    /// Iceberg Puffin container (used by v3 deletion vectors).
    Puffin,
}

/// A delete file that must be applied while scanning an Iceberg data file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergDeleteFile {
    /// Fully qualified delete-file URI.
    pub location: String,
    /// Physical encoding the reader must use for this delete file.
    pub file_format: IcebergFileFormat,
    /// Size recorded in the delete manifest.
    pub size: u64,
    /// Number of delete records recorded in the manifest.
    pub record_count: u64,
    /// Equality field IDs; non-empty only for equality deletes.
    pub equality_field_ids: Vec<i32>,
    /// Referenced data file for targeted position deletes / v3 deletion vectors.
    pub referenced_data_file: Option<String>,
    /// Byte offset of a v3 deletion-vector blob.
    pub content_offset: Option<i64>,
    /// Byte length of a v3 deletion-vector blob.
    pub content_size_in_bytes: Option<i64>,
}

/// Row deletes that the native Parquet reader must apply to one data file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RowDeletion {
    /// Delta deletion vector, decoded by delta-kernel-rs.
    DeltaDeletionVector {
        /// Zero-based row indexes to exclude from the data file.
        deleted_row_indexes: Vec<u64>,
    },
    /// Iceberg position-delete file applicable to this data file.
    IcebergPositionDelete {
        /// Delete-file metadata needed by the reader.
        delete_file: IcebergDeleteFile,
    },
    /// Iceberg equality-delete file applicable to this data file.
    IcebergEqualityDelete {
        /// Delete-file metadata, including equality field IDs.
        delete_file: IcebergDeleteFile,
    },
}

/// One data file in a resolved snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedFile {
    /// Fully qualified data-file URI.
    pub location: String,
    /// File size from the Delta add action or Iceberg manifest.
    pub size: u64,
    /// Format-provided partition values (currently populated by Delta).
    pub partition_values: BTreeMap<String, String>,
    /// Deletes that must be applied while reading this file.
    pub deletions: Vec<RowDeletion>,
}

/// Complete, serializable metadata for a pinned lakehouse scan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedTable {
    /// Snapshot/version that produced this file set.
    pub snapshot: SnapshotIdentity,
    /// Active data files, including metadata sizes and applicable deletes.
    pub files: Vec<ResolvedFile>,
    /// Stable field mappings needed to read evolved/column-mapped Parquet schemas.
    pub column_mappings: Vec<ColumnMapping>,
}

/// A read request against a source: which columns, what filter, optional row limit.
#[derive(Debug, Clone, Default)]
pub struct ScanRequest {
    /// Projected column names; empty = all.
    pub projection: Vec<String>,
    /// Pushed-down filter as a SQL fragment (placeholder; becomes a typed predicate).
    pub filter: Option<String>,
    /// Optional `LIMIT` for top-N / sample pushdown.
    pub limit: Option<usize>,
}

/// Open a source and produce Arrow batches. Implemented in Phase 0/1.
pub fn scan(_uri: &str, _req: &ScanRequest) -> Result<()> {
    Ok(())
}

/// Write Arrow record batches to a Parquet file (create or overwrite).
pub fn write_parquet(path: &str, batches: &[arrow::record_batch::RecordBatch]) -> Result<()> {
    use arrow::datatypes::Schema;
    use parquet::arrow::ArrowWriter;
    use std::fs::File;
    use std::sync::Arc;

    if batches.is_empty() {
        let schema = Arc::new(Schema::empty());
        let file = File::create(path).map_err(|e| Error::Io(format!("create {path}: {e}")))?;
        let writer = ArrowWriter::try_new(file, schema, None)
            .map_err(|e| Error::Io(format!("parquet writer: {e}")))?;
        writer
            .close()
            .map_err(|e| Error::Io(format!("parquet close: {e}")))?;
        return Ok(());
    }
    let file = File::create(path).map_err(|e| Error::Io(format!("create {path}: {e}")))?;
    let mut writer = ArrowWriter::try_new(file, batches[0].schema(), None)
        .map_err(|e| Error::Io(format!("parquet writer: {e}")))?;
    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| Error::Io(format!("parquet write: {e}")))?;
    }
    writer
        .close()
        .map_err(|e| Error::Io(format!("parquet close: {e}")))?;
    Ok(())
}

/// Append a new Parquet data file to a Delta table by writing a JSON add action to `_delta_log`.
pub fn delta_append(
    table_path: &str,
    relative_path: &str,
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<()> {
    use std::path::Path;

    let base = Path::new(table_path);
    let data_path = base.join(relative_path);
    if let Some(parent) = data_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| Error::Io(format!("mkdir {}: {e}", parent.display())))?;
    }
    write_parquet(data_path.to_str().unwrap(), batches)?;

    let log_dir = base.join("_delta_log");
    std::fs::create_dir_all(&log_dir)
        .map_err(|e| Error::Io(format!("mkdir {}: {e}", log_dir.display())))?;
    let version = std::fs::read_dir(&log_dir)
        .map(|rd| rd.filter_map(|e| e.ok()).count())
        .unwrap_or(0);
    let commit = log_dir.join(format!("{version:020}.json"));
    let action = serde_json::json!({
        "add": {
            "path": relative_path.replace('\\', "/"),
            "size": std::fs::metadata(base.join(relative_path)).map(|m| m.len()).unwrap_or(0),
            "modificationTime": chrono::Utc::now().timestamp_millis(),
            "dataChange": true
        }
    });
    std::fs::write(&commit, format!("{action}\n"))
        .map_err(|e| Error::Io(format!("write {}: {e}", commit.display())))?;
    Ok(())
}

/// Resolve a Delta table through delta-kernel-rs.
///
/// `store` is normally bucket/root scoped. `table_location` may be a `file://` or `s3://` URI
/// (a plain local path is also accepted). Passing `version` pins time travel; `None` resolves the
/// latest version once. The kernel validates protocol/reader features, replays checkpoint Parquet
/// plus later commits, validates column mapping, and decodes deletion vectors.
pub async fn delta_active_files(
    store: Arc<dyn ObjectStore>,
    table_location: &str,
    version: Option<u64>,
) -> Result<ResolvedTable> {
    let table_root = table_url(table_location)?;
    tokio::task::spawn_blocking(move || resolve_delta(store, table_root, version))
        .await
        .map_err(|e| Error::Execution(format!("Delta resolver task failed: {e}")))?
}

fn resolve_delta(
    store: Arc<dyn ObjectStore>,
    table_root: Url,
    version: Option<u64>,
) -> Result<ResolvedTable> {
    use delta_kernel::scan::state::ScanFile;
    use delta_kernel::Snapshot;
    use delta_kernel_default_engine::DefaultEngineBuilder;

    fn collect_scan_file(files: &mut Vec<ScanFile>, file: ScanFile) {
        files.push(file);
    }

    let engine = DefaultEngineBuilder::new(store).build();
    let mut builder = Snapshot::builder_for(table_root.as_str());
    if let Some(version) = version {
        builder = builder.at_version(version);
    }
    let snapshot = builder
        .build(&engine)
        .map_err(|e| Error::Io(format!("Delta snapshot resolution failed: {e}")))?;
    let scan = snapshot
        .clone()
        .scan_builder()
        .build()
        .map_err(|e| Error::Unsupported(format!("Delta protocol is not readable: {e}")))?;

    let mut scan_files = Vec::new();
    for metadata in scan
        .scan_metadata(&engine)
        .map_err(|e| Error::Io(format!("Delta log replay failed: {e}")))?
    {
        scan_files = metadata
            .map_err(|e| Error::Io(format!("Delta scan metadata failed: {e}")))?
            .visit_scan_files(scan_files, collect_scan_file)
            .map_err(|e| Error::Io(format!("Delta scan-file decoding failed: {e}")))?;
    }

    let files = scan_files
        .into_iter()
        .map(|file| {
            let location = table_root
                .join(&file.path)
                .map_err(|e| Error::Io(format!("invalid Delta data path `{}`: {e}", file.path)))?;
            let deleted_row_indexes =
                file.dv_info
                    .get_row_indexes(&engine, &table_root)
                    .map_err(|e| {
                        Error::Io(format!(
                            "reading deletion vector for `{}` failed: {e}",
                            file.path
                        ))
                    })?;
            let deletions = deleted_row_indexes
                .map(|deleted_row_indexes| {
                    vec![RowDeletion::DeltaDeletionVector {
                        deleted_row_indexes,
                    }]
                })
                .unwrap_or_default();
            Ok(ResolvedFile {
                location: location.to_string(),
                size: u64::try_from(file.size).map_err(|_| {
                    Error::Io(format!(
                        "Delta add action has negative size {} for `{}`",
                        file.size, file.path
                    ))
                })?,
                partition_values: file.partition_values.into_iter().collect(),
                deletions,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ResolvedTable {
        snapshot: SnapshotIdentity::Delta {
            version: snapshot.version(),
        },
        files,
        column_mappings: delta_column_mappings(snapshot.schema().as_ref()),
    })
}

fn delta_column_mappings(schema: &delta_kernel::schema::Schema) -> Vec<ColumnMapping> {
    use delta_kernel::schema::{ColumnMetadataKey, DataType, MetadataValue, StructType};

    fn walk(
        schema: &StructType,
        logical_parent: &[String],
        physical_parent: &[String],
        output: &mut Vec<ColumnMapping>,
    ) {
        for field in schema.fields() {
            let mut logical = logical_parent.to_vec();
            logical.push(field.name.clone());
            let physical_name = match field
                .metadata
                .get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
            {
                Some(MetadataValue::String(name)) => name.clone(),
                _ => field.name.clone(),
            };
            let mut physical = physical_parent.to_vec();
            physical.push(physical_name);
            let field_id = field
                .metadata
                .get(ColumnMetadataKey::ColumnMappingId.as_ref())
                .and_then(|value| match value {
                    MetadataValue::Number(id) => i32::try_from(*id).ok(),
                    _ => None,
                });
            output.push(ColumnMapping {
                logical_path: logical.join("."),
                physical_path: physical.join("."),
                field_id,
            });
            if let DataType::Struct(child) = &field.data_type {
                walk(child, &logical, &physical, output);
            }
        }
    }

    let mut output = Vec::new();
    walk(schema, &[], &[], &mut output);
    output
}

/// Hive-style partition pruning: keep only paths whose `key=value` segments match `filter`.
///
/// `filter` is a simple SQL fragment like `year = 2024 AND month = 3` or `region='us'`.
pub fn prune_partition_paths(
    files: &[std::path::PathBuf],
    filter: Option<&str>,
) -> Vec<std::path::PathBuf> {
    let Some(filter) = filter else {
        return files.to_vec();
    };
    let predicates = parse_partition_predicates(filter);
    if predicates.is_empty() {
        return files.to_vec();
    }
    files
        .iter()
        .filter(|p| path_matches_predicates(p, &predicates))
        .cloned()
        .collect()
}

/// Apply partition pruning from a [`ScanRequest`] before scanning lakehouse files.
///
/// `pinned_snapshot` is the identity returned by a prior driver-side resolution. Passing it on a
/// worker prevents a concurrent table commit from changing that worker's file set.
pub async fn active_files_for_scan(
    store: Arc<dyn ObjectStore>,
    table_location: &str,
    format: &str,
    metadata_location: Option<&str>,
    pinned_snapshot: Option<&SnapshotIdentity>,
    req: &ScanRequest,
) -> Result<ResolvedTable> {
    let mut resolved = match format.to_ascii_lowercase().as_str() {
        "delta" => {
            let version = match pinned_snapshot {
                Some(SnapshotIdentity::Delta { version }) => Some(*version),
                Some(other) => {
                    return Err(Error::Plan(format!(
                        "Delta scan received incompatible snapshot identity {other:?}"
                    )))
                }
                None => None,
            };
            delta_active_files(store, table_location, version).await?
        }
        "iceberg" => {
            let (metadata_location, snapshot_id) = match pinned_snapshot {
                Some(SnapshotIdentity::Iceberg {
                    snapshot_id,
                    metadata_location,
                    ..
                }) => (Some(metadata_location.as_str()), Some(*snapshot_id)),
                Some(other) => {
                    return Err(Error::Plan(format!(
                        "Iceberg scan received incompatible snapshot identity {other:?}"
                    )))
                }
                None => (metadata_location, None),
            };
            iceberg_active_files(store, table_location, metadata_location, snapshot_id).await?
        }
        other => {
            return Err(Error::Unsupported(format!(
                "active_files_for_scan: unsupported format `{other}`"
            )))
        }
    };
    resolved.files = prune_resolved_files(&resolved.files, req.filter.as_deref());
    Ok(resolved)
}

/// Hive-path pruning for resolved lakehouse files.
pub fn prune_resolved_files(files: &[ResolvedFile], filter: Option<&str>) -> Vec<ResolvedFile> {
    let Some(filter) = filter else {
        return files.to_vec();
    };
    let predicates = parse_partition_predicates(filter);
    if predicates.is_empty() {
        return files.to_vec();
    }
    files
        .iter()
        .filter(|file| {
            predicates.iter().all(|(key, expected)| {
                let actual = file
                    .partition_values
                    .get(key)
                    .cloned()
                    .or_else(|| extract_partition_value(&file.location, key));
                match actual {
                    Some(actual) => partition_values_equal(&actual, expected),
                    None => true,
                }
            })
        })
        .cloned()
        .collect()
}

fn parse_partition_predicates(filter: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for part in filter.split("AND").flat_map(|s| s.split("and")) {
        let part = part.trim();
        if let Some((k, v)) = part.split_once('=') {
            let key = k.trim().trim_matches('`').trim_matches('"');
            let val = v
                .trim()
                .trim_matches('\'')
                .trim_matches('"')
                .trim_matches('`');
            if !key.is_empty() {
                out.push((key.to_string(), val.to_string()));
            }
        }
    }
    out
}

fn path_matches_predicates(path: &std::path::Path, preds: &[(String, String)]) -> bool {
    path_string_matches_predicates(&path.to_string_lossy(), preds)
}

fn path_string_matches_predicates(path: &str, preds: &[(String, String)]) -> bool {
    preds.iter().all(|(k, v)| {
        if path.contains(&format!("{k}={v}")) || path.contains(&format!("{k}={v}/")) {
            return true;
        }
        // Hive paths often zero-pad numeric partition values (month=01 vs month=1).
        extract_partition_value(path, k).is_some_and(|actual| partition_values_equal(&actual, v))
    })
}

fn partition_values_equal(actual: &str, expected: &str) -> bool {
    actual == expected
        || matches!(
            (actual.parse::<i64>(), expected.parse::<i64>()),
            (Ok(actual), Ok(expected)) if actual == expected
        )
}

fn extract_partition_value(path: &str, key: &str) -> Option<String> {
    let needle = format!("{key}=");
    let start = path.find(&needle)? + needle.len();
    let rest = &path[start..];
    let end = rest.find('/').unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

/// Append Parquet data files to an Iceberg table by writing a new manifest + snapshot metadata.
pub fn iceberg_append(
    table_path: &str,
    relative_path: &str,
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<()> {
    use std::path::Path;

    let base = Path::new(table_path);
    let data_path = base.join(relative_path);
    if let Some(parent) = data_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| Error::Io(format!("mkdir {}: {e}", parent.display())))?;
    }
    write_parquet(data_path.to_str().unwrap(), batches)?;
    let meta_dir = base.join("metadata");
    std::fs::create_dir_all(&meta_dir)
        .map_err(|e| Error::Io(format!("mkdir {}: {e}", meta_dir.display())))?;

    let version = std::fs::read_dir(&meta_dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter(|e| e.path().extension().is_some_and(|x| x == "json"))
                .count()
        })
        .unwrap_or(0) as i64
        + 1;
    let manifest = base.join(format!("metadata/snap-{version}.avro"));
    let data_file = data_path.to_string_lossy();
    write_avro_manifest(&manifest, &data_file)?;
    let metadata = serde_json::json!({
        "format-version": 2,
        "table-uuid": uuid_simple(),
        "location": base.to_string_lossy(),
        "current-snapshot-id": version,
        "snapshots": [{
            "snapshot-id": version,
            "manifest-list": manifest.to_string_lossy(),
        }]
    });
    std::fs::write(
        meta_dir.join(format!("v{version}.metadata.json")),
        serde_json::to_string_pretty(&metadata).unwrap(),
    )
    .map_err(|e| Error::Io(format!("write metadata: {e}")))?;
    std::fs::write(meta_dir.join("version-hint.text"), version.to_string())
        .map_err(|e| Error::Io(format!("write version hint: {e}")))?;
    Ok(())
}

fn uuid_simple() -> String {
    format!("weft-{}", std::process::id())
}

fn write_avro_manifest(path: &std::path::Path, data_file: &str) -> Result<()> {
    use apache_avro::{types::Value, Schema, Writer};
    let schema = Schema::parse_str(
        r#"{"type":"record","name":"manifest_entry","fields":[
            {"name":"status","type":"int"},
            {"name":"data_file","type":{"type":"record","name":"data_file","fields":[
                {"name":"content","type":"int"},
                {"name":"file_path","type":"string"}]}}]}"#,
    )
    .map_err(|e| Error::Io(format!("avro schema: {e}")))?;
    let mut w = Writer::new(&schema, Vec::new());
    w.append(Value::Record(vec![
        ("status".into(), Value::Int(1)),
        (
            "data_file".into(),
            Value::Record(vec![
                ("content".into(), Value::Int(0)),
                ("file_path".into(), Value::String(data_file.into())),
            ]),
        ),
    ]))
    .map_err(|e| Error::Io(format!("avro append: {e}")))?;
    std::fs::write(path, w.into_inner().unwrap())
        .map_err(|e| Error::Io(format!("write {}: {e}", path.display())))
}

// ---- Iceberg -------------------------------------------------------------------------------

#[derive(Clone)]
struct IcebergManifestEntry {
    data_file: iceberg::spec::DataFile,
    sequence_number: Option<i64>,
    partition_spec_id: i32,
}

/// Resolve an Iceberg table through the Iceberg core crate.
///
/// `metadata_location` is the authoritative catalog pointer when supplied. Without it, the
/// resolver falls back to `version-hint.text` and then numeric metadata-file discovery.
/// `snapshot_id` pins a snapshot from that metadata JSON; `None` selects its current snapshot.
/// Position/equality delete files are associated with every data file to which Iceberg sequence
/// and partition rules say they apply.
pub async fn iceberg_active_files(
    store: Arc<dyn ObjectStore>,
    table_location: &str,
    metadata_location: Option<&str>,
    snapshot_id: Option<i64>,
) -> Result<ResolvedTable> {
    use iceberg::spec::{DataContentType, Manifest, ManifestList, TableMetadata};

    let table_root = table_url(table_location)?;
    let metadata_url = match metadata_location {
        Some(location) => resolve_location(&table_root, location)?,
        None => discover_iceberg_metadata(store.as_ref(), &table_root).await?,
    };
    let metadata_bytes = get_bytes(store.as_ref(), &metadata_url).await?;
    let metadata: TableMetadata = serde_json::from_slice(&metadata_bytes)
        .map_err(|e| Error::Io(format!("invalid Iceberg metadata `{metadata_url}`: {e}")))?;
    let snapshot = match snapshot_id {
        Some(id) => metadata.snapshot_by_id(id).ok_or_else(|| {
            Error::Plan(format!(
                "Iceberg snapshot {id} is not present in `{metadata_url}`"
            ))
        })?,
        None => metadata
            .current_snapshot()
            .ok_or_else(|| Error::Io(format!("Iceberg table `{table_root}` has no snapshot")))?,
    };
    let manifest_list_url = resolve_location(&table_root, snapshot.manifest_list())?;
    let manifest_list = ManifestList::parse_with_version(
        &get_bytes(store.as_ref(), &manifest_list_url).await?,
        metadata.format_version(),
    )
    .map_err(|e| Error::Io(format!("invalid manifest list `{manifest_list_url}`: {e}")))?;

    let mut data_entries = Vec::new();
    let mut delete_entries = Vec::new();
    for manifest_file in manifest_list.entries() {
        let manifest_url = resolve_location(&table_root, &manifest_file.manifest_path)?;
        let manifest = Manifest::parse_avro(&get_bytes(store.as_ref(), &manifest_url).await?)
            .map_err(|e| Error::Io(format!("invalid Iceberg manifest `{manifest_url}`: {e}")))?;
        for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
            let effective_sequence = entry.sequence_number().or_else(|| {
                (entry.status() == iceberg::spec::ManifestStatus::Added
                    || manifest_file.sequence_number == 0)
                    .then_some(manifest_file.sequence_number)
            });
            let resolved_entry = IcebergManifestEntry {
                data_file: entry.data_file.clone(),
                sequence_number: effective_sequence,
                partition_spec_id: manifest_file.partition_spec_id,
            };
            match entry.content_type() {
                DataContentType::Data => data_entries.push(resolved_entry),
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    delete_entries.push(resolved_entry)
                }
            }
        }
    }

    let files = data_entries
        .iter()
        .map(|data| {
            let location = resolve_location(&table_root, data.data_file.file_path())?;
            let deletions = delete_entries
                .iter()
                .filter(|delete| iceberg_delete_applies(delete, data, &metadata))
                .map(|delete| {
                    let delete_file = iceberg_delete_file(delete, &table_root)?;
                    Ok(match delete.data_file.content_type() {
                        DataContentType::PositionDeletes => {
                            RowDeletion::IcebergPositionDelete { delete_file }
                        }
                        DataContentType::EqualityDeletes => {
                            RowDeletion::IcebergEqualityDelete { delete_file }
                        }
                        DataContentType::Data => {
                            return Err(Error::Execution(
                                "internal Iceberg delete index contained a data file".into(),
                            ))
                        }
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(ResolvedFile {
                location: location.to_string(),
                size: data.data_file.file_size_in_bytes(),
                partition_values: BTreeMap::new(),
                deletions,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ResolvedTable {
        snapshot: SnapshotIdentity::Iceberg {
            snapshot_id: snapshot.snapshot_id(),
            sequence_number: snapshot.sequence_number(),
            metadata_location: metadata_url.to_string(),
        },
        files,
        column_mappings: iceberg_column_mappings(
            snapshot
                .schema(&metadata)
                .map_err(|e| Error::Io(format!("invalid Iceberg snapshot schema: {e}")))?
                .as_ref(),
        ),
    })
}

fn iceberg_delete_applies(
    delete: &IcebergManifestEntry,
    data: &IcebergManifestEntry,
    metadata: &iceberg::spec::TableMetadata,
) -> bool {
    use iceberg::spec::DataContentType;

    let sequence_applies = match delete.data_file.content_type() {
        DataContentType::EqualityDeletes => match data.sequence_number {
            Some(data_sequence) => delete
                .sequence_number
                .is_some_and(|seq| seq > data_sequence),
            None => true,
        },
        DataContentType::PositionDeletes => match data.sequence_number {
            Some(data_sequence) => delete
                .sequence_number
                .is_some_and(|seq| seq >= data_sequence),
            None => true,
        },
        DataContentType::Data => false,
    };
    if !sequence_applies {
        return false;
    }

    if let Some(referenced) = delete.data_file.referenced_data_file() {
        return referenced == data.data_file.file_path();
    }

    let delete_spec_unpartitioned = metadata
        .partition_spec_by_id(delete.partition_spec_id)
        .is_some_and(|spec| spec.is_unpartitioned());
    if delete.data_file.content_type() == DataContentType::EqualityDeletes
        && delete_spec_unpartitioned
    {
        return true;
    }

    delete.partition_spec_id == data.partition_spec_id
        && delete.data_file.partition() == data.data_file.partition()
}

fn iceberg_delete_file(
    entry: &IcebergManifestEntry,
    table_root: &Url,
) -> Result<IcebergDeleteFile> {
    use iceberg::spec::DataFileFormat;

    let file_format = match entry.data_file.file_format() {
        DataFileFormat::Avro => IcebergFileFormat::Avro,
        DataFileFormat::Orc => IcebergFileFormat::Orc,
        DataFileFormat::Parquet => IcebergFileFormat::Parquet,
        DataFileFormat::Puffin => IcebergFileFormat::Puffin,
    };
    Ok(IcebergDeleteFile {
        location: resolve_location(table_root, entry.data_file.file_path())?.to_string(),
        file_format,
        size: entry.data_file.file_size_in_bytes(),
        record_count: entry.data_file.record_count(),
        equality_field_ids: entry.data_file.equality_ids().unwrap_or_default(),
        referenced_data_file: entry.data_file.referenced_data_file(),
        content_offset: entry.data_file.content_offset(),
        content_size_in_bytes: entry.data_file.content_size_in_bytes(),
    })
}

fn iceberg_column_mappings(schema: &iceberg::spec::Schema) -> Vec<ColumnMapping> {
    use iceberg::spec::{StructType, Type};

    fn walk(schema: &StructType, parent: &[String], output: &mut Vec<ColumnMapping>) {
        for field in schema.fields() {
            let mut path = parent.to_vec();
            path.push(field.name.clone());
            output.push(ColumnMapping {
                logical_path: path.join("."),
                physical_path: path.join("."),
                field_id: Some(field.id),
            });
            if let Type::Struct(child) = field.field_type.as_ref() {
                walk(child, &path, output);
            }
        }
    }

    let mut output = Vec::new();
    walk(schema.as_struct(), &[], &mut output);
    output
}

async fn discover_iceberg_metadata(store: &dyn ObjectStore, table_root: &Url) -> Result<Url> {
    let metadata_dir = table_root
        .join("metadata/")
        .map_err(|e| Error::Io(format!("invalid Iceberg metadata directory: {e}")))?;
    let hint_url = metadata_dir
        .join("version-hint.text")
        .map_err(|e| Error::Io(format!("invalid Iceberg version-hint path: {e}")))?;
    if let Some(bytes) = get_bytes_if_present(store, &hint_url).await? {
        let hint = std::str::from_utf8(&bytes)
            .map_err(|e| Error::Io(format!("invalid UTF-8 in `{hint_url}`: {e}")))?
            .trim();
        if !hint.is_empty() {
            let hinted = metadata_dir
                .join(&format!("v{hint}.metadata.json"))
                .map_err(|e| Error::Io(format!("invalid hinted metadata path: {e}")))?;
            if get_bytes_if_present(store, &hinted).await?.is_some() {
                return Ok(hinted);
            }
        }
    }

    let prefix = object_path(&metadata_dir)?;
    let objects = store
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| Error::Io(format!("listing Iceberg metadata `{metadata_dir}`: {e}")))?;
    let best = objects
        .into_iter()
        .filter_map(|object| {
            let name = object.location.filename()?;
            if !name.ends_with(".metadata.json") {
                return None;
            }
            let version = iceberg_metadata_version(name);
            Some((version, object.last_modified, object))
        })
        .max_by_key(|(version, modified, _)| (*version, *modified))
        .map(|(_, _, object)| object)
        .ok_or_else(|| Error::Io(format!("no `*.metadata.json` under `{metadata_dir}`")))?;
    url_for_object(table_root, &best.location)
}

fn iceberg_metadata_version(filename: &str) -> u64 {
    filename
        .strip_prefix('v')
        .unwrap_or(filename)
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .unwrap_or(0)
}

async fn get_bytes(store: &dyn ObjectStore, location: &Url) -> Result<bytes::Bytes> {
    store
        .get(&object_path(location)?)
        .await
        .map_err(|e| Error::Io(format!("reading `{location}`: {e}")))?
        .bytes()
        .await
        .map_err(|e| Error::Io(format!("reading `{location}`: {e}")))
}

async fn get_bytes_if_present(
    store: &dyn ObjectStore,
    location: &Url,
) -> Result<Option<bytes::Bytes>> {
    match store.get(&object_path(location)?).await {
        Ok(result) => result
            .bytes()
            .await
            .map(Some)
            .map_err(|e| Error::Io(format!("reading `{location}`: {e}"))),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(Error::Io(format!("reading `{location}`: {e}"))),
    }
}

fn table_url(location: &str) -> Result<Url> {
    if let Ok(url) = Url::parse(location) {
        return match url.scheme() {
            "file" | "s3" => ensure_directory_url(url),
            scheme => Err(Error::Unsupported(format!(
                "lakehouse object-store scheme `{scheme}` is not supported"
            ))),
        };
    }
    let path = std::path::Path::new(location);
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| Error::Io(format!("resolving current directory: {e}")))?
            .join(path)
    };
    Url::from_directory_path(&absolute)
        .map_err(|_| Error::Io(format!("invalid local table path `{}`", absolute.display())))
}

fn ensure_directory_url(mut url: Url) -> Result<Url> {
    if url.scheme() == "s3" && url.host_str().is_none() {
        return Err(Error::Io(format!("S3 table URI has no bucket: `{url}`")));
    }
    if !url.path().ends_with('/') {
        let path = format!("{}/", url.path());
        url.set_path(&path);
    }
    Ok(url)
}

fn resolve_location(table_root: &Url, location: &str) -> Result<Url> {
    match Url::parse(location) {
        Ok(url) => Ok(url),
        Err(_) => table_root
            .join(location)
            .map_err(|e| Error::Io(format!("invalid object location `{location}`: {e}"))),
    }
}

fn object_path(location: &Url) -> Result<ObjectPath> {
    ObjectPath::from_url_path(location.path())
        .map_err(|e| Error::Io(format!("invalid object-store path `{location}`: {e}")))
}

fn url_for_object(table_root: &Url, object: &ObjectPath) -> Result<Url> {
    let mut url = table_root.clone();
    url.set_path(&format!("/{}", object.as_ref()));
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::io::FileIOBuilder;
    use iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, ManifestListWriter,
        ManifestWriterBuilder, NestedField, PartitionSpec, PrimitiveType, Schema, Struct, Type,
    };
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    fn local_store() -> Arc<dyn ObjectStore> {
        Arc::new(LocalFileSystem::new())
    }

    fn local_url(path: &std::path::Path) -> String {
        Url::from_file_path(path).unwrap().to_string()
    }

    fn delta_metadata_lines(protocol: serde_json::Value) -> String {
        delta_metadata_lines_with_field(protocol, serde_json::json!({}), serde_json::json!({}))
    }

    fn delta_metadata_lines_with_field(
        protocol: serde_json::Value,
        field_metadata: serde_json::Value,
        configuration: serde_json::Value,
    ) -> String {
        let schema = serde_json::json!({
            "type": "struct",
            "fields": [{
                "name": "id",
                "type": "long",
                "nullable": true,
                "metadata": field_metadata
            }]
        });
        [
            serde_json::json!({"protocol": protocol}).to_string(),
            serde_json::json!({
                "metaData": {
                    "id": "00000000-0000-0000-0000-000000000001",
                    "name": null,
                    "description": null,
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": schema.to_string(),
                    "partitionColumns": [],
                    "configuration": configuration,
                    "createdTime": 1
                }
            })
            .to_string(),
        ]
        .join("\n")
    }

    fn write_delta_commit(table: &std::path::Path, version: u64, lines: &[String]) {
        let log = table.join("_delta_log");
        std::fs::create_dir_all(&log).unwrap();
        std::fs::write(
            log.join(format!("{version:020}.json")),
            format!("{}\n", lines.join("\n")),
        )
        .unwrap();
    }

    fn delta_add(path: &str, size: u64, deletion_vector: Option<serde_json::Value>) -> String {
        let mut add = serde_json::json!({
            "path": path,
            "partitionValues": {},
            "size": size,
            "modificationTime": 1,
            "dataChange": true,
            "stats": "{\"numRecords\":30}"
        });
        if let Some(deletion_vector) = deletion_vector {
            add["deletionVector"] = deletion_vector;
        }
        serde_json::json!({"add": add}).to_string()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delta_resolves_sizes_versions_and_checkpoint_only_tables() {
        use delta_kernel::Snapshot;
        use delta_kernel_default_engine::{
            executor::tokio::TokioMultiThreadExecutor, DefaultEngineBuilder,
        };

        let dir = TempDir::new().unwrap();
        let table = dir.path();
        std::fs::write(table.join("a.parquet"), b"aaa").unwrap();
        std::fs::write(table.join("b.parquet"), b"bbbb").unwrap();
        let initial = delta_metadata_lines(serde_json::json!({
            "minReaderVersion": 1,
            "minWriterVersion": 2
        }));
        write_delta_commit(table, 0, &[initial, delta_add("a.parquet", 3, None)]);
        write_delta_commit(
            table,
            1,
            &[
                serde_json::json!({
                    "remove": {
                        "path": "a.parquet",
                        "deletionTimestamp": 2,
                        "dataChange": true
                    }
                })
                .to_string(),
                delta_add("b.parquet", 4, None),
            ],
        );

        let latest = delta_active_files(local_store(), table.to_str().unwrap(), None)
            .await
            .unwrap();
        assert_eq!(latest.snapshot, SnapshotIdentity::Delta { version: 1 });
        assert_eq!(latest.files.len(), 1);
        assert!(latest.files[0].location.ends_with("/b.parquet"));
        assert_eq!(latest.files[0].size, 4);

        let pinned = delta_active_files(local_store(), table.to_str().unwrap(), Some(0))
            .await
            .unwrap();
        assert_eq!(pinned.files.len(), 1);
        assert!(pinned.files[0].location.ends_with("/a.parquet"));

        let table_root = table_url(table.to_str().unwrap()).unwrap();
        let executor = Arc::new(TokioMultiThreadExecutor::new(
            tokio::runtime::Handle::current(),
        ));
        tokio::task::spawn_blocking(move || {
            let engine = DefaultEngineBuilder::new(local_store())
                .with_task_executor(executor)
                .build();
            let snapshot = Snapshot::builder_for(table_root.as_str())
                .at_version(1)
                .build(&engine)
                .unwrap();
            snapshot.checkpoint(&engine, None).unwrap();
        })
        .await
        .unwrap();
        std::fs::remove_file(table.join("_delta_log/00000000000000000000.json")).unwrap();
        std::fs::remove_file(table.join("_delta_log/00000000000000000001.json")).unwrap();

        let checkpoint_only = delta_active_files(local_store(), table.to_str().unwrap(), None)
            .await
            .unwrap();
        assert_eq!(
            checkpoint_only.snapshot,
            SnapshotIdentity::Delta { version: 1 }
        );
        assert_eq!(checkpoint_only.files, latest.files);
    }

    #[tokio::test]
    async fn delta_decodes_inline_deletion_vectors() {
        let dir = TempDir::new().unwrap();
        let initial = delta_metadata_lines(serde_json::json!({
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["deletionVectors"],
            "writerFeatures": ["deletionVectors"]
        }));
        let deletion_vector = serde_json::json!({
            "storageType": "i",
            "pathOrInlineDv": "^Bg9^0rr910000000000iXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L",
            "sizeInBytes": 44,
            "cardinality": 6
        });
        write_delta_commit(
            dir.path(),
            0,
            &[
                initial,
                delta_add("data.parquet", 123, Some(deletion_vector)),
            ],
        );

        let resolved = delta_active_files(local_store(), dir.path().to_str().unwrap(), None)
            .await
            .unwrap();
        assert_eq!(
            resolved.files[0].deletions,
            vec![RowDeletion::DeltaDeletionVector {
                deleted_row_indexes: vec![3, 4, 7, 11, 18, 29]
            }]
        );
    }

    #[tokio::test]
    async fn delta_exposes_column_mapping_identity() {
        let dir = TempDir::new().unwrap();
        let initial = delta_metadata_lines_with_field(
            serde_json::json!({
                "minReaderVersion": 2,
                "minWriterVersion": 5
            }),
            serde_json::json!({
                "delta.columnMapping.id": 1,
                "delta.columnMapping.physicalName": "col-0001"
            }),
            serde_json::json!({
                "delta.columnMapping.mode": "name",
                "delta.columnMapping.maxColumnId": "1"
            }),
        );
        write_delta_commit(
            dir.path(),
            0,
            &[initial, delta_add("data.parquet", 123, None)],
        );

        let resolved = delta_active_files(local_store(), dir.path().to_str().unwrap(), None)
            .await
            .unwrap();
        assert_eq!(
            resolved.column_mappings,
            vec![ColumnMapping {
                logical_path: "id".into(),
                physical_path: "col-0001".into(),
                field_id: Some(1),
            }]
        );
    }

    async fn write_iceberg_fixture(dir: &std::path::Path) -> std::path::PathBuf {
        let metadata_dir = dir.join("metadata");
        std::fs::create_dir_all(&metadata_dir).unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "value",
                        Type::Primitive(PrimitiveType::String),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .build()
            .unwrap();
        let data_path = local_url(&dir.join("data.parquet"));
        let position_delete_path = local_url(&dir.join("position-deletes.parquet"));
        let equality_delete_path = local_url(&dir.join("equality-deletes.parquet"));

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(data_path.clone())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(10)
            .file_size_in_bytes(1_001)
            .build()
            .unwrap();
        let position_delete = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(position_delete_path)
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(2)
            .file_size_in_bytes(201)
            .referenced_data_file(Some(data_path))
            .build()
            .unwrap();
        let equality_delete = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path(equality_delete_path)
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(101)
            .equality_ids(Some(vec![1]))
            .build()
            .unwrap();

        let data_manifest_path = metadata_dir.join("data-manifest.avro");
        let mut data_writer = ManifestWriterBuilder::new(
            file_io
                .new_output(data_manifest_path.to_str().unwrap())
                .unwrap(),
            Some(42),
            None,
            schema.clone(),
            spec.clone(),
        )
        .build_v2_data();
        data_writer.add_file(data_file, 1).unwrap();
        let data_manifest = data_writer.write_manifest_file().await.unwrap();

        let delete_manifest_path = metadata_dir.join("delete-manifest.avro");
        let mut delete_writer = ManifestWriterBuilder::new(
            file_io
                .new_output(delete_manifest_path.to_str().unwrap())
                .unwrap(),
            Some(42),
            None,
            schema,
            spec,
        )
        .build_v2_deletes();
        delete_writer.add_file(position_delete, 2).unwrap();
        delete_writer.add_file(equality_delete, 2).unwrap();
        let delete_manifest = delete_writer.write_manifest_file().await.unwrap();

        let manifest_list_path = metadata_dir.join("snap-42.avro");
        let mut manifest_list = ManifestListWriter::v2(
            file_io
                .new_output(manifest_list_path.to_str().unwrap())
                .unwrap(),
            42,
            None,
            2,
        );
        manifest_list
            .add_manifests(vec![data_manifest, delete_manifest].into_iter())
            .unwrap();
        manifest_list.close().await.unwrap();

        let metadata_path = metadata_dir.join("00010-fixture.metadata.json");
        let metadata = serde_json::json!({
            "format-version": 2,
            "table-uuid": "00000000-0000-0000-0000-000000000042",
            "location": local_url(dir),
            "last-sequence-number": 2,
            "last-updated-ms": 2,
            "last-column-id": 2,
            "current-schema-id": 0,
            "schemas": [{
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"},
                    {"id": 2, "name": "value", "required": false, "type": "string"}
                ]
            }],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 999,
            "properties": {},
            "current-snapshot-id": 42,
            "snapshots": [{
                "snapshot-id": 42,
                "sequence-number": 2,
                "timestamp-ms": 2,
                "summary": {"operation": "delete"},
                "manifest-list": local_url(&manifest_list_path),
                "schema-id": 0
            }],
            "snapshot-log": [{"snapshot-id": 42, "timestamp-ms": 2}],
            "metadata-log": [],
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
            "refs": {"main": {"snapshot-id": 42, "type": "branch"}}
        });
        std::fs::write(&metadata_path, serde_json::to_vec(&metadata).unwrap()).unwrap();
        metadata_path
    }

    #[tokio::test]
    async fn iceberg_resolves_sizes_explicit_pointer_and_deletes() {
        let dir = TempDir::new().unwrap();
        let metadata_path = write_iceberg_fixture(dir.path()).await;
        let metadata_location = local_url(&metadata_path);
        let resolved = iceberg_active_files(
            local_store(),
            dir.path().to_str().unwrap(),
            Some(&metadata_location),
            None,
        )
        .await
        .unwrap();

        assert_eq!(resolved.files.len(), 1);
        assert_eq!(resolved.files[0].size, 1_001);
        assert_eq!(resolved.files[0].deletions.len(), 2);
        assert!(resolved.files[0].deletions.iter().any(|delete| matches!(
            delete,
            RowDeletion::IcebergPositionDelete { delete_file }
                if delete_file.size == 201
                    && delete_file.file_format == IcebergFileFormat::Parquet
        )));
        assert!(resolved.files[0].deletions.iter().any(|delete| matches!(
            delete,
            RowDeletion::IcebergEqualityDelete { delete_file }
                if delete_file.equality_field_ids == [1]
        )));
        assert_eq!(
            resolved.snapshot,
            SnapshotIdentity::Iceberg {
                snapshot_id: 42,
                sequence_number: 2,
                metadata_location,
            }
        );
    }

    #[tokio::test]
    async fn iceberg_discovery_orders_numeric_uuid_metadata_names() {
        let dir = TempDir::new().unwrap();
        let metadata_path = write_iceberg_fixture(dir.path()).await;
        let metadata_dir = dir.path().join("metadata");
        std::fs::copy(&metadata_path, metadata_dir.join("00009-old.metadata.json")).unwrap();

        let resolved =
            iceberg_active_files(local_store(), dir.path().to_str().unwrap(), None, None)
                .await
                .unwrap();
        let SnapshotIdentity::Iceberg {
            metadata_location, ..
        } = resolved.snapshot
        else {
            panic!("expected Iceberg identity")
        };
        assert!(metadata_location.ends_with("/00010-fixture.metadata.json"));
    }

    #[test]
    fn prune_partition_paths_filters_hive_layout() {
        let files = vec![
            std::path::PathBuf::from("/data/year=2024/month=01/part.parquet"),
            std::path::PathBuf::from("/data/year=2024/month=02/part.parquet"),
            std::path::PathBuf::from("/data/year=2023/month=12/part.parquet"),
        ];
        let pruned = prune_partition_paths(&files, Some("year = 2024 AND month = 1"));
        assert_eq!(pruned.len(), 1);
        assert!(pruned[0].to_string_lossy().contains("month=01"));
    }

    #[test]
    fn prune_resolved_files_keeps_unknown_partition_values() {
        let file = |location: &str, partition_values: BTreeMap<String, String>| ResolvedFile {
            location: location.into(),
            size: 1,
            partition_values,
            deletions: vec![],
        };
        let files = vec![
            file(
                "s3://bucket/table/data/unpartitioned.parquet",
                BTreeMap::new(),
            ),
            file(
                "s3://bucket/table/data/year=2023/part.parquet",
                BTreeMap::new(),
            ),
            file(
                "s3://bucket/table/data/opaque.parquet",
                BTreeMap::from([("year".into(), "2024".into())]),
            ),
        ];

        let pruned = prune_resolved_files(&files, Some("year = 2024"));
        assert_eq!(pruned, vec![files[0].clone(), files[2].clone()]);
    }

    #[test]
    fn write_parquet_roundtrip() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("out.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))])
                .unwrap();
        write_parquet(path.to_str().unwrap(), &[batch]).unwrap();
        assert!(path.exists());
    }
}
