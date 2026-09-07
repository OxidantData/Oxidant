//! Streaming data sinks.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use oxidant_loom::arrow::record_batch::RecordBatch;

/// A streaming sink that accepts micro-batches.
///
/// Async because the sinks that matter commit to object storage and a catalog; the in-memory and
/// local-file sinks below just don't await anything.
///
/// `batch_id` is the query's monotonic micro-batch number. A transactional sink stamps it into
/// the commit so a batch replayed after a crash is recognized and dropped instead of counted
/// twice; sinks with no transaction log ignore it.
#[async_trait::async_trait]
pub trait Sink: Send + Sync {
    async fn write_batch(
        &mut self,
        batches: &[RecordBatch],
        batch_id: u64,
    ) -> oxidant_common::Result<u64>;

    /// Spark-shaped sink description for `lastProgress`, e.g. `DeltaSink[glue.live.orders]`.
    fn description(&self) -> String {
        "Sink".to_string()
    }
}

/// Append micro-batches to a file directory as Parquet.
pub struct FileSink {
    path: PathBuf,
    format: String,
    batch_counter: u64,
}

impl FileSink {
    pub fn new(path: impl AsRef<Path>, format: &str) -> Self {
        std::fs::create_dir_all(path.as_ref()).ok();
        Self {
            path: path.as_ref().to_path_buf(),
            format: format.to_ascii_lowercase(),
            batch_counter: 0,
        }
    }
}

#[async_trait::async_trait]
impl Sink for FileSink {
    async fn write_batch(
        &mut self,
        batches: &[RecordBatch],
        _batch_id: u64,
    ) -> oxidant_common::Result<u64> {
        let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        if rows == 0 {
            return Ok(0);
        }
        // JSON is encoded in memory first so a writer error cannot leave an unreadable
        // part file that the scheduler would treat as a successful commit
        // (OxidantData/Oxidant#173).
        let json_bytes = if self.format == "json" {
            Some(encode_json_objects(batches)?)
        } else {
            None
        };
        self.batch_counter += 1;
        let out = self.path.join(format!(
            "part-{:05}.{}",
            self.batch_counter,
            if json_bytes.is_some() {
                "json"
            } else {
                "parquet"
            }
        ));
        if let Some(buf) = json_bytes {
            std::fs::write(&out, buf)
                .map_err(|e| oxidant_common::Error::Execution(e.to_string()))?;
        } else {
            use datafusion::parquet::arrow::ArrowWriter;
            let file = std::fs::File::create(&out)
                .map_err(|e| oxidant_common::Error::Execution(e.to_string()))?;
            let mut writer = ArrowWriter::try_new(file, batches[0].schema(), None)
                .map_err(|e| oxidant_common::Error::Execution(e.to_string()))?;
            for batch in batches {
                writer
                    .write(batch)
                    .map_err(|e| oxidant_common::Error::Execution(e.to_string()))?;
            }
            writer
                .close()
                .map_err(|e| oxidant_common::Error::Execution(e.to_string()))?;
        }
        Ok(rows)
    }

    fn description(&self) -> String {
        format!("FileSink[{}]", self.path.display())
    }
}

fn encode_json_objects(batches: &[RecordBatch]) -> oxidant_common::Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut writer = datafusion::arrow::json::LineDelimitedWriter::new(&mut buf);
        for batch in batches {
            writer.write(batch).map_err(|e| {
                oxidant_common::Error::Execution(format!(
                    "json sink cannot encode batch as JSON objects: {e}"
                ))
            })?;
        }
        writer.finish().map_err(|e| {
            oxidant_common::Error::Execution(format!("json sink cannot finish JSON objects: {e}"))
        })?;
    }
    Ok(buf)
}

/// In-memory sink for tests.
pub struct MemorySink {
    pub batches: Mutex<Vec<RecordBatch>>,
}

impl MemorySink {
    pub fn new() -> Self {
        Self {
            batches: Mutex::new(vec![]),
        }
    }
}

impl Default for MemorySink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Sink for MemorySink {
    async fn write_batch(
        &mut self,
        batches: &[RecordBatch],
        _batch_id: u64,
    ) -> oxidant_common::Result<u64> {
        let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let mut guard = self
            .batches
            .lock()
            .map_err(|e| oxidant_common::Error::Execution(e.to_string()))?;
        guard.extend_from_slice(batches);
        Ok(rows)
    }

    fn description(&self) -> String {
        "MemorySink".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidant_loom::arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
    use oxidant_loom::arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    async fn file_sink_writes_parquet() {
        let dir = TempDir::new().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))])
                .unwrap();
        let mut sink = FileSink::new(dir.path(), "parquet");
        let rows = sink.write_batch(&[batch], 1).await.unwrap();
        assert_eq!(rows, 3);
        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert_eq!(files.len(), 1);
        assert!(files[0].extension().is_some_and(|e| e == "parquet"));
    }

    #[tokio::test]
    async fn json_sink_writes_objects_an_independent_parser_round_trips() {
        // The issue query: id plus a string that contains a comma and quotes. The old
        // FileSink joined display cells with commas, so json.loads failed
        // (OxidantData/Oxidant#173).
        let dir = TempDir::new().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("flag", DataType::Boolean, false),
            Field::new("n", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0i64, 1])),
                Arc::new(StringArray::from(vec![Some(r#"quoted, "value""#), None])),
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(Float64Array::from(vec![Some(12.5), None])),
            ],
        )
        .unwrap();
        let mut sink = FileSink::new(dir.path(), "json");
        assert_eq!(sink.write_batch(&[batch], 1).await.unwrap(), 2);
        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert_eq!(files.len(), 1);
        assert!(files[0].extension().is_some_and(|e| e == "json"));
        let text = std::fs::read_to_string(&files[0]).unwrap();
        let lines: Vec<&str> = text.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 2, "got: {text}");
        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["v"], 0);
        assert_eq!(first["name"], r#"quoted, "value""#);
        assert_eq!(first["flag"], true);
        assert!((first["n"].as_f64().unwrap() - 12.5).abs() < 1e-9);
        let second: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(second["v"], 1);
        assert!(second["name"].is_null());
        assert_eq!(second["flag"], false);
        assert!(second["n"].is_null());
    }

    #[tokio::test]
    async fn json_sink_does_not_create_a_part_file_when_encoding_fails() {
        // Union is not a JSON object leaf the Arrow writer will encode. A failed
        // encode must not create part-*.json (no success-looking output).
        use oxidant_loom::arrow::array::UnionArray;
        use oxidant_loom::arrow::buffer::ScalarBuffer;
        use oxidant_loom::arrow::datatypes::{UnionFields, UnionMode};

        let dir = TempDir::new().unwrap();
        let fields: UnionFields = [(0i8, Arc::new(Field::new("i", DataType::Int64, false)))]
            .into_iter()
            .collect();
        let type_ids = ScalarBuffer::<i8>::from(vec![0i8]);
        let children = vec![Arc::new(Int64Array::from(vec![1i64])) as _];
        let union = UnionArray::try_new(fields.clone(), type_ids, None, children).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "u",
            DataType::Union(fields, UnionMode::Sparse),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(union)]).unwrap();
        let mut sink = FileSink::new(dir.path(), "json");
        let err = sink.write_batch(&[batch], 1).await.unwrap_err();
        assert!(
            err.to_string().contains("json sink cannot encode"),
            "named encode error, got: {err}"
        );
        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert!(
            files.is_empty(),
            "failed encode must not leave a part file: {files:?}"
        );
    }
}
