//! Hash partitioning of a stage's output into per-downstream buckets.
//!
//! Only the *producer* hashes (the consumer just asks for a bucket id), so any deterministic
//! hash works as long as it is stable across processes. We hash the Arrow row-format bytes of
//! the key columns with FNV-1a — no external dependency, identical on every worker.

use weft_loom::arrow::array::{RecordBatch, UInt32Array};
use weft_loom::arrow::compute::take;
use weft_loom::arrow::row::{RowConverter, SortField};

use weft_common::{Error, Result};

const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h = FNV_OFFSET;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

/// Split `batches` into `n` buckets by `hash(key_cols) % n`. Returns `n` vectors of batches;
/// concatenating all of them reproduces the input rows (in a possibly different order).
pub fn hash_partition(
    batches: &[RecordBatch],
    key_cols: &[usize],
    n: usize,
) -> Result<Vec<Vec<RecordBatch>>> {
    assert!(n > 0, "partition count must be positive");
    let mut out: Vec<Vec<RecordBatch>> = (0..n).map(|_| Vec::new()).collect();
    if batches.is_empty() {
        return Ok(out);
    }
    // Empty key list = global gather: every row lands on partition 0 (ungrouped aggregates).
    if key_cols.is_empty() {
        out[0].extend(batches.iter().cloned());
        return Ok(out);
    }

    // One converter for the key columns; the row bytes are an order/value-faithful encoding.
    let key_fields: Vec<SortField> = key_cols
        .iter()
        .map(|&c| SortField::new(batches[0].schema().field(c).data_type().clone()))
        .collect();
    let converter = RowConverter::new(key_fields)
        .map_err(|e| Error::Execution(format!("row converter: {e}")))?;

    for batch in batches {
        let key_arrays: Vec<_> = key_cols.iter().map(|&c| batch.column(c).clone()).collect();
        let rows = converter
            .convert_columns(&key_arrays)
            .map_err(|e| Error::Execution(format!("convert columns: {e}")))?;

        // Bucket each row, collecting the row indices that land in each bucket.
        let mut idx: Vec<Vec<u32>> = (0..n).map(|_| Vec::new()).collect();
        for (i, row) in rows.iter().enumerate() {
            let bucket = (fnv1a(row.as_ref()) % n as u64) as usize;
            idx[bucket].push(i as u32);
        }

        for (bucket, indices) in idx.into_iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let take_idx = UInt32Array::from(indices);
            let cols = batch
                .columns()
                .iter()
                .map(|col| take(col, &take_idx, None))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| Error::Execution(format!("take: {e}")))?;
            let part = RecordBatch::try_new(batch.schema(), cols)
                .map_err(|e| Error::Execution(format!("build partition batch: {e}")))?;
            out[bucket].push(part);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use weft_loom::arrow::array::Int64Array;
    use weft_loom::arrow::datatypes::{DataType, Field, Schema};

    fn sample() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 1, 2, 3, 4, 5])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 11, 21, 31, 40, 50])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn partitions_are_complete_and_disjoint() {
        let batch = sample();
        let total: usize = batch.num_rows();
        let parts = hash_partition(&[batch], &[0], 3).unwrap();
        let got: usize = parts
            .iter()
            .flat_map(|p| p.iter())
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(got, total, "every row must land in exactly one bucket");
        assert_eq!(parts.len(), 3);
    }

    #[test]
    fn same_key_lands_in_same_bucket() {
        // All rows with key=1 must end up in one bucket regardless of how many batches.
        let parts = hash_partition(&[sample()], &[0], 4).unwrap();
        // Count buckets that contain key==1; must be exactly one.
        let mut buckets_with_k1 = 0;
        for p in &parts {
            let has = p.iter().any(|b| {
                let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                (0..k.len()).any(|i| k.value(i) == 1)
            });
            if has {
                buckets_with_k1 += 1;
            }
        }
        assert_eq!(buckets_with_k1, 1);
    }
}
