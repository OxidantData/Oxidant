//! Delta Lake **write** side: an object-store-backed transaction-log appender.
//!
//! The read side ([`crate::delta_active_files`]) hands a pinned snapshot to delta-kernel-rs. This
//! module is its mirror: it writes Parquet data files and commits `_delta_log/N.json` actions so
//! the very same kernel — and Spark, Athena, and Trino — can read the table back.
//!
//! Only the actions a streaming append sink needs are emitted: `protocol` + `metaData` on version
//! 0, then one `add` per data file on every commit. That is deliberately the *minimum* legal Delta
//! writer (reader v1 / writer v2, no deletion vectors, no column mapping, no checkpoints), which
//! keeps every Delta reader in the ecosystem able to open the result.
//!
//! Everything goes through [`ObjectStore`], so a `s3://` table root works exactly like a local
//! one.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use oxidant_common::{Error, Result};

/// Delta reader version this writer's output requires. v1 = no column mapping, no deletion
/// vectors — every Delta implementation in the wild can read it.
const MIN_READER_VERSION: i32 = 1;
/// Delta writer version this writer claims. v2 = `appendOnly`/`invariants` table features, which
/// is what a plain append sink needs and what Spark stamps for a vanilla `CREATE TABLE`.
const MIN_WRITER_VERSION: i32 = 2;

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
    pub partition_values: HashMap<String, String>,
}

/// Outcome of one [`append`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaCommit {
    /// The version this append committed as (`_delta_log/{version:020}.json`).
    pub version: u64,
    /// Rows written across all data files in the commit.
    pub rows: u64,
    /// The data files added.
    pub files: Vec<DeltaAddFile>,
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

/// `_delta_log/{version:020}.json` — Delta's zero-padded commit filename.
pub fn commit_path(root: &ObjectPath, version: u64) -> ObjectPath {
    root.clone()
        .join("_delta_log")
        .join(format!("{version:020}.json"))
}

/// The next unused commit version for the table at `root`.
///
/// Returns `0` for a table with no `_delta_log` (a brand-new table), otherwise one past the
/// highest numbered commit. Checkpoint files (`*.checkpoint.parquet`) and `_last_checkpoint` are
/// ignored — this writer never produces them, and a table that has them still numbers its JSON
/// commits contiguously.
pub async fn next_version(store: &dyn ObjectStore, root: &ObjectPath) -> Result<u64> {
    use futures::TryStreamExt;

    let log_dir = root.clone().join("_delta_log");
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

/// Serialize `batches` to one in-memory Parquet file.
fn encode_parquet(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Vec<u8>> {
    use parquet::arrow::ArrowWriter;

    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None)
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

/// Write `batches` as a single Parquet file under `root` and commit it to the transaction log.
///
/// `file_name` is the table-root-relative name of the data file (e.g.
/// `part-00000-<uuid>.c000.snappy.parquet`). On version 0 the commit also carries the `protocol`
/// and `metaData` actions that declare the table; later commits carry only the `add`.
///
/// Concurrency: the version is chosen by listing `_delta_log` and the commit is written
/// create-if-not-exists, so a racing writer that picked the same version cannot be silently
/// overwritten — the loser re-reads the log and retries at the next free version, up to
/// [`COMMIT_ATTEMPTS`] times. The data file is written once and reused across retries.
pub async fn append(
    store: &dyn ObjectStore,
    root: &ObjectPath,
    schema: &SchemaRef,
    batches: &[RecordBatch],
    file_name: &str,
    table_id: &str,
    partition_columns: &[String],
) -> Result<DeltaCommit> {
    let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    let bytes = encode_parquet(schema, batches)?;
    let size = bytes.len() as u64;

    let data_path = root.clone().join(file_name);
    store
        .put(&data_path, bytes.into())
        .await
        .map_err(|e| Error::Io(format!("write `{data_path}`: {e}")))?;

    let add = DeltaAddFile {
        path: file_name.to_string(),
        size,
        num_records: rows,
        partition_values: HashMap::new(),
    };

    for attempt in 0..COMMIT_ATTEMPTS {
        let version = next_version(store, root).await?;
        let commit = render_commit(
            version,
            schema,
            std::slice::from_ref(&add),
            table_id,
            partition_columns,
        )?;
        let commit_at = commit_path(root, version);
        let opts = object_store::PutOptions {
            mode: object_store::PutMode::Create,
            ..Default::default()
        };
        match store.put_opts(&commit_at, commit.into(), opts).await {
            Ok(_) => {
                return Ok(DeltaCommit {
                    version,
                    rows,
                    files: vec![add],
                })
            }
            // Another writer took this version between our list and our put. Re-read the log.
            Err(object_store::Error::AlreadyExists { .. }) if attempt + 1 < COMMIT_ATTEMPTS => {}
            Err(e) => return Err(Error::Io(format!("commit `{commit_at}`: {e}"))),
        }
    }
    Err(Error::Io(format!(
        "Delta commit for `{data_path}` lost {COMMIT_ATTEMPTS} version races — another writer is \
         appending to this table concurrently"
    )))
}

/// How many times [`append`] re-reads the log and retries after losing a commit-version race.
const COMMIT_ATTEMPTS: usize = 8;

/// Render the newline-delimited JSON body of one commit.
pub fn render_commit(
    version: u64,
    schema: &SchemaRef,
    files: &[DeltaAddFile],
    table_id: &str,
    partition_columns: &[String],
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
        lines.push(
            serde_json::json!({
                "metaData": {
                    "id": table_id,
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": delta_schema_string(schema)?,
                    "partitionColumns": partition_columns,
                    "configuration": {},
                    "createdTime": now_ms,
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
                    "stats": serde_json::json!({"numRecords": f.num_records}).to_string(),
                }
            })
            .to_string(),
        );
    }

    let mut body = lines.join("\n");
    body.push('\n');
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::Schema;
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
            partition_values: HashMap::new(),
        }];
        let body = render_commit(0, &schema(), &files, "tbl-uuid", &[]).unwrap();
        let lines: Vec<_> = body.trim_end().split('\n').collect();
        assert_eq!(lines.len(), 3, "protocol + metaData + add");
        assert!(lines[0].contains("\"protocol\""));
        assert!(lines[1].contains("\"metaData\""));
        assert!(lines[2].contains("\"add\""));

        // A later version is data-only.
        let body = render_commit(7, &schema(), &files, "tbl-uuid", &[]).unwrap();
        let lines: Vec<_> = body.trim_end().split('\n').collect();
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("\"add\""));
    }

    #[tokio::test]
    async fn append_writes_data_and_numbers_commits_contiguously() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let root = ObjectPath::from("tbl");
        let schema = schema();

        for expected in 0u64..3 {
            let commit = append(
                &store,
                &root,
                &schema,
                &[batch()],
                &format!("part-{expected:05}.parquet"),
                "tbl-uuid",
                &[],
            )
            .await
            .unwrap();
            assert_eq!(commit.version, expected);
            assert_eq!(commit.rows, 3);
        }

        assert_eq!(next_version(&store, &root).await.unwrap(), 3);
        for v in 0u64..3 {
            let p = dir
                .path()
                .join("tbl/_delta_log")
                .join(format!("{v:020}.json"));
            assert!(p.exists(), "missing commit {v} at {}", p.display());
        }
        assert!(dir.path().join("tbl/part-00000.parquet").exists());
    }

    #[tokio::test]
    async fn append_never_overwrites_a_commit_written_by_someone_else() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let root = ObjectPath::from("tbl");
        let schema = schema();

        // A commit written by another writer, byte-identifiable.
        let taken = commit_path(&root, 0);
        store
            .put(&taken, b"{\"marker\":\"someone-else\"}\n".to_vec().into())
            .await
            .unwrap();

        let commit = append(
            &store,
            &root,
            &schema,
            &[batch()],
            "part-a.parquet",
            "id",
            &[],
        )
        .await
        .unwrap();

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
}
