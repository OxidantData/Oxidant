//! Stateful operators for streaming (deduplication within watermark bounds).

use std::collections::HashSet;

use weft_loom::arrow::record_batch::RecordBatch;

/// In-memory dedup state: tracks composite keys seen within the watermark window.
#[derive(Debug, Default)]
pub struct DedupState {
    seen: HashSet<Vec<u8>>,
    max_keys: usize,
}

impl DedupState {
    pub fn new(max_keys: usize) -> Self {
        Self {
            seen: HashSet::new(),
            max_keys,
        }
    }

    /// Filter batches to rows whose key hash was not seen before (dropDuplicates).
    pub fn dedup_batches(
        &mut self,
        batches: &[RecordBatch],
        key_cols: &[usize],
    ) -> Vec<RecordBatch> {
        use weft_loom::arrow::array::BooleanArray;
        use weft_loom::arrow::compute::filter_record_batch;

        let mut out = Vec::new();
        for batch in batches {
            if key_cols.is_empty() {
                out.push(batch.clone());
                continue;
            }
            let mut keep = vec![false; batch.num_rows()];
            for (row, slot) in keep.iter_mut().enumerate() {
                let key = row_key(batch, key_cols, row);
                if self.seen.insert(key) {
                    *slot = true;
                }
            }
            if self.seen.len() > self.max_keys {
                self.seen.clear();
            }
            let mask = BooleanArray::from(keep);
            if let Ok(filtered) = filter_record_batch(batch, &mask) {
                if filtered.num_rows() > 0 {
                    out.push(filtered);
                }
            }
        }
        out
    }
}

fn row_key(batch: &RecordBatch, cols: &[usize], row: usize) -> Vec<u8> {
    use weft_loom::arrow::array::Array;
    let mut key = Vec::new();
    for &c in cols {
        let arr = batch.column(c);
        if arr.is_null(row) {
            key.push(0xff);
        } else {
            key.extend_from_slice(format!("{:?}", arr.data_type()).as_bytes());
            key.push(b':');
            key.extend_from_slice(&format_cell(arr, row).into_bytes());
        }
        key.push(b'|');
    }
    key
}

fn format_cell(arr: &weft_loom::arrow::array::ArrayRef, row: usize) -> String {
    use weft_loom::arrow::array::{Array, AsArray};
    use weft_loom::arrow::datatypes::DataType;
    match arr.data_type() {
        DataType::Utf8 => arr.as_string::<i32>().value(row).to_string(),
        DataType::Int64 => arr
            .as_primitive::<weft_loom::arrow::datatypes::Int64Type>()
            .value(row)
            .to_string(),
        _ => row.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use weft_loom::arrow::array::{Int64Array, StringArray};
    use weft_loom::arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn dedup_drops_duplicate_keys() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "a", "b"])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
            ],
        )
        .unwrap();
        let mut state = DedupState::new(10_000);
        let out = state.dedup_batches(&[batch], &[0]);
        let rows: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2);
    }
}
