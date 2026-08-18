//! Streaming data sources.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

use oxidant_loom::arrow::datatypes::SchemaRef;
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;
use serde::{Deserialize, Serialize};

/// A source's replay position, persisted in the query checkpoint after the sink commits.
///
/// The shape is deliberately source-agnostic — `source` names the implementation so a checkpoint
/// written by one source is never silently interpreted by another (a Kafka offset restored into a
/// file source would read as a file name), and `entries` is an ordered map so the serialized JSON
/// is byte-stable across runs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceOffsets {
    pub source: String,
    pub entries: BTreeMap<String, i64>,
}

/// A micro-batch data source.
#[async_trait::async_trait]
pub trait Source: Send + Sync {
    /// Read the next micro-batch. Returns empty when no new data is available.
    async fn poll_batch(&mut self, engine: &Engine) -> oxidant_common::Result<Vec<RecordBatch>>;

    /// The schema this source emits, known before the first batch arrives.
    ///
    /// The streaming DataFrame is planned against this, so a source whose schema depends on the
    /// data (a file source over an unknown directory) returns an empty schema and only supports
    /// pass-through queries.
    fn schema(&self) -> SchemaRef;

    /// Spark-style source description for progress reporting (`KafkaV2[Subscribe[topic]]`).
    fn description(&self) -> String;

    /// Position to persist once the current batch is durably in the sink. `None` means the source
    /// has no replayable position (a rate/memory source).
    fn committed_offsets(&self) -> Option<SourceOffsets> {
        None
    }

    /// Resume from a checkpointed position. Called once, before the first poll.
    fn restore_offsets(&mut self, _offsets: &SourceOffsets) {}
}

/// File-directory source: reads new Parquet/JSON/CSV files not yet in the offset set.
pub struct FileSource {
    path: PathBuf,
    format: String,
    seen: HashSet<String>,
    schema: SchemaRef,
}

impl FileSource {
    pub fn new(path: impl AsRef<Path>, format: &str) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            format: format.to_ascii_lowercase(),
            seen: HashSet::new(),
            // A directory of not-yet-existing files has no schema until the first file lands.
            schema: std::sync::Arc::new(oxidant_loom::arrow::datatypes::Schema::empty()),
        }
    }

    pub fn new_files(&self) -> Vec<PathBuf> {
        let Ok(entries) = std::fs::read_dir(&self.path) else {
            return vec![];
        };
        let mut files: Vec<PathBuf> = entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.is_file() && !self.seen.contains(&p.to_string_lossy().to_string()))
            .collect();
        // Deterministic order so a restart replays files the same way it first read them.
        files.sort();
        files
    }
}

#[async_trait::async_trait]
impl Source for FileSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn description(&self) -> String {
        format!("FileStreamSource[{}]", self.path.display())
    }

    async fn poll_batch(&mut self, engine: &Engine) -> oxidant_common::Result<Vec<RecordBatch>> {
        use datafusion::prelude::{CsvReadOptions, JsonReadOptions, ParquetReadOptions};

        let new_files = self.new_files();
        if new_files.is_empty() {
            return Ok(vec![]);
        }
        // Read each file through DataFusion's readers rather than a SQL string. `parquet_scan()`
        // and friends are DuckDB table functions that this engine has never registered, so the
        // SQL form this used to build could not resolve for any format.
        let ctx = engine.ctx();
        let mut all = Vec::new();
        for f in &new_files {
            let path = f.to_string_lossy().into_owned();
            let df = match self.format.as_str() {
                "parquet" => ctx.read_parquet(&path, ParquetReadOptions::default()).await,
                "json" => ctx.read_json(&path, JsonReadOptions::default()).await,
                "csv" => ctx.read_csv(&path, CsvReadOptions::default()).await,
                other => {
                    return Err(oxidant_common::Error::Unsupported(format!(
                        "readStream.format(`{other}`) is not a file source"
                    )))
                }
            };
            let batches = df
                .map_err(|e| oxidant_common::Error::Execution(format!("read `{path}`: {e}")))?
                .collect()
                .await
                .map_err(|e| oxidant_common::Error::Execution(format!("read `{path}`: {e}")))?;
            if let Some(b) = batches.first() {
                self.schema = b.schema();
            }
            all.extend(batches);
            self.seen.insert(path);
        }
        Ok(all)
    }

    fn committed_offsets(&self) -> Option<SourceOffsets> {
        // A file source's position is the set of consumed paths; the value is unused.
        Some(SourceOffsets {
            source: "file".into(),
            entries: self.seen.iter().map(|p| (p.clone(), 1i64)).collect(),
        })
    }

    fn restore_offsets(&mut self, offsets: &SourceOffsets) {
        if offsets.source != "file" {
            return;
        }
        self.seen.extend(offsets.entries.keys().cloned());
    }
}

/// Rate source for tests: emits N rows per batch.
pub struct MemoryRateSource {
    rows_per_batch: u64,
    batch_count: u64,
    max_batches: u64,
}

impl MemoryRateSource {
    pub fn new(rows_per_batch: u64, max_batches: u64) -> Self {
        Self {
            rows_per_batch,
            batch_count: 0,
            max_batches,
        }
    }
}

#[async_trait::async_trait]
impl Source for MemoryRateSource {
    fn schema(&self) -> SchemaRef {
        use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
        std::sync::Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn description(&self) -> String {
        format!("RateStreamSource[rowsPerBatch={}]", self.rows_per_batch)
    }

    async fn poll_batch(&mut self, engine: &Engine) -> oxidant_common::Result<Vec<RecordBatch>> {
        if self.batch_count >= self.max_batches {
            return Ok(vec![]);
        }
        self.batch_count += 1;
        let sql = format!(
            "SELECT id FROM range(0, {}, 1) AS t(id)",
            self.rows_per_batch
        );
        engine.sql(&sql).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_file_source_reads_new_files_and_skips_ones_it_has_seen() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("a.json"), "{\"n\":1}\n{\"n\":2}\n").unwrap();
        let engine = Engine::new();
        let mut src = FileSource::new(dir.path(), "json");

        let first = src.poll_batch(&engine).await.unwrap();
        let rows: usize = first.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2);
        assert_eq!(src.schema().fields().len(), 1);

        // Already consumed.
        assert!(src.poll_batch(&engine).await.unwrap().is_empty());

        std::fs::write(dir.path().join("b.json"), "{\"n\":3}\n").unwrap();
        let second = src.poll_batch(&engine).await.unwrap();
        assert_eq!(second.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }

    #[test]
    fn file_offsets_do_not_cross_source_types() {
        let mut src = FileSource::new("/tmp/does-not-matter", "json");
        src.restore_offsets(&SourceOffsets {
            source: "kafka".into(),
            entries: [("events-0".to_string(), 7i64)].into_iter().collect(),
        });
        assert!(src.seen.is_empty(), "a kafka checkpoint is not a file list");

        src.restore_offsets(&SourceOffsets {
            source: "file".into(),
            entries: [("/data/a.json".to_string(), 1i64)].into_iter().collect(),
        });
        assert!(src.seen.contains("/data/a.json"));
    }
}
