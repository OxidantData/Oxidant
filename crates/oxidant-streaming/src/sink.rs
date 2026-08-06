//! Streaming data sinks.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use oxidant_loom::arrow::record_batch::RecordBatch;

/// A streaming sink that accepts micro-batches.
pub trait Sink: Send + Sync {
    fn write_batch(&mut self, batches: &[RecordBatch]) -> oxidant_common::Result<u64>;
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

impl Sink for FileSink {
    fn write_batch(&mut self, batches: &[RecordBatch]) -> oxidant_common::Result<u64> {
        let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        if rows == 0 {
            return Ok(0);
        }
        self.batch_counter += 1;
        let out = self.path.join(format!(
            "part-{:05}.{}",
            self.batch_counter,
            if self.format == "json" {
                "json"
            } else {
                "parquet"
            }
        ));
        if self.format == "json" {
            let mut lines = String::new();
            for batch in batches {
                for row in 0..batch.num_rows() {
                    let mut cells = Vec::new();
                    for col in 0..batch.num_columns() {
                        cells.push(format_cell(batch.column(col), row));
                    }
                    lines.push_str(&cells.join(","));
                    lines.push('\n');
                }
            }
            std::fs::write(&out, lines)
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
}

fn format_cell(arr: &oxidant_loom::arrow::array::ArrayRef, row: usize) -> String {
    use oxidant_loom::arrow::array::{Array, AsArray};
    use oxidant_loom::arrow::datatypes::DataType;
    if arr.is_null(row) {
        return String::new();
    }
    match arr.data_type() {
        DataType::Utf8 => arr.as_string::<i32>().value(row).to_string(),
        DataType::Int64 => arr
            .as_primitive::<oxidant_loom::arrow::datatypes::Int64Type>()
            .value(row)
            .to_string(),
        _ => String::new(),
    }
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

impl Sink for MemorySink {
    fn write_batch(&mut self, batches: &[RecordBatch]) -> oxidant_common::Result<u64> {
        let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let mut guard = self
            .batches
            .lock()
            .map_err(|e| oxidant_common::Error::Execution(e.to_string()))?;
        guard.extend_from_slice(batches);
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidant_loom::arrow::array::Int64Array;
    use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
    use oxidant_loom::arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn file_sink_writes_parquet() {
        let dir = TempDir::new().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))])
                .unwrap();
        let mut sink = FileSink::new(dir.path(), "parquet");
        let rows = sink.write_batch(&[batch]).unwrap();
        assert_eq!(rows, 3);
        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert_eq!(files.len(), 1);
        assert!(files[0].extension().is_some_and(|e| e == "parquet"));
    }
}
