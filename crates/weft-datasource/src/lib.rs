//! `weft-datasource` — turn storage into Arrow record batches.
//!
//! Phase 0: Parquet/CSV/JSON. Phase 1 lakehouse reads use a **version-safe resolver** pattern:
//! resolve the table format to its active Parquet file list, then let the engine's native
//! (DataFusion 54) reader scan them — avoiding the DataFusion-version coupling that the
//! `deltalake`/`iceberg` crates impose (they pin older DataFusion). **Delta**:
//! [`delta_active_files`] replays the `_delta_log` JSON (add/remove). **Iceberg**:
//! [`iceberg_active_files`] walks metadata.json → manifest list (Avro) → manifests (Avro).
//! v1 limits: no deletion vectors / merge-on-read deletes / partition pruning yet (those are a
//! delta-kernel / iceberg-rust integration once their DataFusion versions catch up to ours).

use weft_common::{Error, Result};

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

/// Resolve a Delta Lake table to its active data-file paths by replaying the JSON transaction
/// log (`_delta_log/*.json`): `add` actions introduce files, `remove` actions retire them.
///
/// This is the version-safe Phase-1 read path — it yields plain Parquet paths that the
/// engine's native reader consumes, so it does not couple Weft to a Delta crate's DataFusion
/// version. Limitations (v1): JSON commits only (no checkpoint Parquet), and no deletion
/// vectors / column mapping — those arrive with a `delta-kernel` integration later.
pub fn delta_active_files(table_path: &str) -> Result<Vec<std::path::PathBuf>> {
    use std::collections::HashSet;
    use std::path::Path;

    let base = Path::new(table_path);
    let log_dir = base.join("_delta_log");
    let mut commits: Vec<std::path::PathBuf> = std::fs::read_dir(&log_dir)
        .map_err(|e| Error::Io(format!("reading {}: {e}", log_dir.display())))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "json").unwrap_or(false))
        .collect();
    commits.sort(); // 000…0.json, 000…1.json, … apply in version order

    if commits.is_empty() {
        return Err(Error::Io(format!(
            "no _delta_log/*.json under {} (checkpoint-only tables not supported yet)",
            table_path
        )));
    }

    let mut order: Vec<String> = Vec::new();
    let mut present: HashSet<String> = HashSet::new();
    for commit in &commits {
        let content = std::fs::read_to_string(commit)
            .map_err(|e| Error::Io(format!("reading {}: {e}", commit.display())))?;
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let v: serde_json::Value = serde_json::from_str(line)
                .map_err(|e| Error::Io(format!("delta log json: {e}")))?;
            if let Some(p) = v
                .get("add")
                .and_then(|a| a.get("path"))
                .and_then(|p| p.as_str())
            {
                if present.insert(p.to_string()) {
                    order.push(p.to_string());
                }
            } else if let Some(p) = v
                .get("remove")
                .and_then(|r| r.get("path"))
                .and_then(|p| p.as_str())
            {
                if present.remove(p) {
                    order.retain(|x| x != p);
                }
            }
        }
    }
    Ok(order.into_iter().map(|p| base.join(p)).collect())
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
pub fn active_files_for_scan(
    table_path: &str,
    format: &str,
    req: &ScanRequest,
) -> Result<Vec<std::path::PathBuf>> {
    let files = match format.to_ascii_lowercase().as_str() {
        "delta" => delta_active_files(table_path)?,
        "iceberg" => iceberg_active_files(table_path)?,
        other => {
            return Err(Error::Unsupported(format!(
                "active_files_for_scan: unsupported format `{other}`"
            )))
        }
    };
    Ok(prune_partition_paths(&files, req.filter.as_deref()))
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
    let s = path.to_string_lossy();
    preds.iter().all(|(k, v)| {
        if s.contains(&format!("{k}={v}")) || s.contains(&format!("{k}={v}/")) {
            return true;
        }
        // Hive paths often zero-pad numeric partition values (month=01 vs month=1).
        if let (Ok(want), Some(Ok(have))) = (
            v.parse::<i64>(),
            extract_partition_value(&s, k).map(|h| h.parse::<i64>()),
        ) {
            return want == have;
        }
        false
    })
}

fn extract_partition_value(path: &str, key: &str) -> Option<String> {
    let needle = format!("{key}=");
    let start = path.find(&needle)? + needle.len();
    let rest = &path[start..];
    let end = rest.find('/').unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

/// Read Delta deletion-vector metadata paths from the transaction log (v1: skip DV files on read).
pub fn delta_deletion_vector_paths(table_path: &str) -> Result<Vec<std::path::PathBuf>> {
    use std::collections::HashSet;
    use std::path::Path;

    let base = Path::new(table_path);
    let log_dir = base.join("_delta_log");
    let mut commits: Vec<std::path::PathBuf> = std::fs::read_dir(&log_dir)
        .map_err(|e| Error::Io(format!("reading {}: {e}", log_dir.display())))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "json").unwrap_or(false))
        .collect();
    commits.sort();
    let mut dvs = HashSet::new();
    for commit in &commits {
        let content = std::fs::read_to_string(commit)
            .map_err(|e| Error::Io(format!("reading {}: {e}", commit.display())))?;
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let v: serde_json::Value = serde_json::from_str(line)
                .map_err(|e| Error::Io(format!("delta log json: {e}")))?;
            if let Some(dv) = v
                .get("add")
                .and_then(|a| a.get("deletionVector"))
                .and_then(|dv| dv.get("storageType"))
                .and_then(|t| t.as_str())
            {
                if dv == "u" || dv == "i" {
                    if let Some(path) = v
                        .get("add")
                        .and_then(|a| a.get("path"))
                        .and_then(|p| p.as_str())
                    {
                        dvs.insert(base.join(path));
                    }
                }
            }
        }
    }
    Ok(dvs.into_iter().collect())
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

use apache_avro::types::Value as AvroValue;

fn unwrap_union(v: &AvroValue) -> &AvroValue {
    match v {
        AvroValue::Union(_, b) => b,
        other => other,
    }
}
fn avro_field<'a>(v: &'a AvroValue, name: &str) -> Option<&'a AvroValue> {
    match v {
        AvroValue::Record(fields) => fields.iter().find(|(k, _)| k == name).map(|(_, x)| x),
        _ => None,
    }
}
fn avro_str(v: &AvroValue) -> Option<String> {
    match unwrap_union(v) {
        AvroValue::String(s) => Some(s.clone()),
        _ => None,
    }
}
fn avro_int(v: &AvroValue) -> Option<i64> {
    match unwrap_union(v) {
        AvroValue::Int(n) => Some(*n as i64),
        AvroValue::Long(n) => Some(*n),
        _ => None,
    }
}

fn strip_scheme(p: &str) -> String {
    p.strip_prefix("file://")
        .or_else(|| p.strip_prefix("file:"))
        .unwrap_or(p)
        .to_string()
}

fn avro_records(bytes: &[u8]) -> Result<Vec<AvroValue>> {
    let reader = apache_avro::Reader::new(bytes).map_err(|e| Error::Io(format!("avro: {e}")))?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Io(format!("avro record: {e}")))
}

/// Resolve an Iceberg table to its active data-file paths: read the current `metadata.json`
/// (JSON), follow the current snapshot's manifest list (Avro) to the manifests (Avro), and
/// collect non-deleted data files. Version-safe (avro + json only; no Iceberg-crate DataFusion
/// coupling). v1 limits: no positional/equality delete application, no partition pruning yet.
pub fn iceberg_active_files(table_path: &str) -> Result<Vec<std::path::PathBuf>> {
    use std::path::Path;
    let base = Path::new(table_path);
    let meta_dir = base.join("metadata");

    // Locate the current metadata.json: prefer version-hint.text, else the latest v*.metadata.json.
    let meta_path = if let Ok(hint) = std::fs::read_to_string(meta_dir.join("version-hint.text")) {
        meta_dir.join(format!("v{}.metadata.json", hint.trim()))
    } else {
        let mut metas: Vec<std::path::PathBuf> = std::fs::read_dir(&meta_dir)
            .map_err(|e| Error::Io(format!("reading {}: {e}", meta_dir.display())))?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.to_string_lossy().ends_with(".metadata.json"))
            .collect();
        metas.sort();
        metas
            .pop()
            .ok_or_else(|| Error::Io(format!("no metadata.json under {}", meta_dir.display())))?
    };

    let meta: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(&meta_path).map_err(|e| Error::Io(format!("{e}")))?,
    )
    .map_err(|e| Error::Io(format!("iceberg metadata json: {e}")))?;

    let current = meta.get("current-snapshot-id").and_then(|v| v.as_i64());
    let snapshots = meta
        .get("snapshots")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let snap = snapshots
        .iter()
        .find(|s| current.is_some() && s.get("snapshot-id").and_then(|v| v.as_i64()) == current)
        .or_else(|| snapshots.last())
        .ok_or_else(|| Error::Io("iceberg: no current snapshot".into()))?;
    let manifest_list = snap
        .get("manifest-list")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::Io("iceberg: snapshot has no manifest-list".into()))?;

    // manifest list (avro) -> manifest file paths
    let ml_bytes = std::fs::read(strip_scheme(manifest_list))
        .map_err(|e| Error::Io(format!("manifest list: {e}")))?;
    let mut data_files: Vec<std::path::PathBuf> = Vec::new();
    for entry in avro_records(&ml_bytes)? {
        let Some(mp) = avro_field(&entry, "manifest_path").and_then(avro_str) else {
            continue;
        };
        let m_bytes = std::fs::read(strip_scheme(&mp))
            .map_err(|e| Error::Io(format!("manifest {mp}: {e}")))?;
        for me in avro_records(&m_bytes)? {
            // status: 0=EXISTING, 1=ADDED, 2=DELETED — skip deleted.
            if avro_field(&me, "status").and_then(avro_int) == Some(2) {
                continue;
            }
            let Some(df) = avro_field(&me, "data_file").map(unwrap_union) else {
                continue;
            };
            // content: 0=DATA, 1=POSITION_DELETES, 2=EQUALITY_DELETES (v1 manifests omit it → data)
            if avro_field(df, "content").and_then(avro_int).unwrap_or(0) != 0 {
                continue;
            }
            if let Some(fp) = avro_field(df, "file_path").and_then(avro_str) {
                data_files.push(std::path::PathBuf::from(strip_scheme(&fp)));
            }
        }
    }
    Ok(data_files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::{types::Value, Schema, Writer};

    fn write_avro(path: &std::path::Path, schema_json: &str, rec: Value) {
        let schema = Schema::parse_str(schema_json).unwrap();
        let mut w = Writer::new(&schema, Vec::new());
        w.append(rec).unwrap();
        std::fs::write(path, w.into_inner().unwrap()).unwrap();
    }

    #[test]
    fn iceberg_resolves_data_files() {
        let dir = std::env::temp_dir().join(format!("weft-ice-{}", std::process::id()));
        let meta = dir.join("metadata");
        std::fs::create_dir_all(&meta).unwrap();
        let data = dir.join("data-0.parquet");
        std::fs::write(&data, b"x").unwrap();

        // manifest (one ADDED data file)
        let manifest = dir.join("manifest-0.avro");
        write_avro(
            &manifest,
            r#"{"type":"record","name":"manifest_entry","fields":[
                {"name":"status","type":"int"},
                {"name":"data_file","type":{"type":"record","name":"data_file","fields":[
                    {"name":"content","type":"int"},
                    {"name":"file_path","type":"string"}]}}]}"#,
            Value::Record(vec![
                ("status".into(), Value::Int(1)),
                (
                    "data_file".into(),
                    Value::Record(vec![
                        ("content".into(), Value::Int(0)),
                        (
                            "file_path".into(),
                            Value::String(data.to_string_lossy().into()),
                        ),
                    ]),
                ),
            ]),
        );

        // manifest list -> the manifest above
        let ml = dir.join("snap-0.avro");
        write_avro(
            &ml,
            r#"{"type":"record","name":"manifest_file","fields":[{"name":"manifest_path","type":"string"}]}"#,
            Value::Record(vec![(
                "manifest_path".into(),
                Value::String(manifest.to_string_lossy().into()),
            )]),
        );

        // metadata.json + version hint
        let metadata = serde_json::json!({
            "current-snapshot-id": 1,
            "snapshots": [ {"snapshot-id": 1, "manifest-list": ml.to_string_lossy()} ]
        });
        std::fs::write(
            meta.join("v1.metadata.json"),
            serde_json::to_string(&metadata).unwrap(),
        )
        .unwrap();
        std::fs::write(meta.join("version-hint.text"), "1").unwrap();

        let files = iceberg_active_files(dir.to_str().unwrap()).unwrap();
        assert_eq!(files, vec![data]);
        let _ = std::fs::remove_dir_all(&dir);
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
    fn write_parquet_roundtrip() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;
        use tempfile::TempDir;

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
