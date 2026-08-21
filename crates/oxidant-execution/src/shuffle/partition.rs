//! Hash partitioning of a stage's output into per-downstream buckets.
//!
//! Only the *producer* hashes (the consumer just asks for a bucket id), so any deterministic
//! hash works as long as it is stable across processes. We hash the Arrow row-format bytes of
//! the key columns with FNV-1a — no external dependency, identical on every worker.

use oxidant_loom::arrow::array::{Array, RecordBatch, StringViewArray, UInt32Array};
use oxidant_loom::arrow::compute::take;
use oxidant_loom::arrow::datatypes::DataType;
use oxidant_loom::arrow::row::{RowConverter, SortField};

use oxidant_common::{Error, Result};

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

/// Compact `Utf8View` columns so a partitioned batch no longer retains the producer's whole
/// string buffers. `take` on a view array reuses the source buffers, so a bucket holding a
/// few thousand filtered/joined rows can pin hundreds of MB of scan-side string data (SF10
/// TPC-H Q16: 33 MB spill segments for 4,765 rows — the inflated shuffle footprint tripped
/// the spill threshold early, and the read-back of those bloated buckets OOM-killed workers
/// in the downstream output stage). `gc` copies only the live strings into fresh buffers;
/// every other array type already copies on `take`.
fn compact_views(batch: RecordBatch) -> Result<RecordBatch> {
    let needs_gc = batch
        .schema()
        .fields()
        .iter()
        .any(|f| matches!(f.data_type(), DataType::Utf8View));
    if !needs_gc {
        return Ok(batch);
    }
    let cols = batch
        .columns()
        .iter()
        .map(|col| -> Result<oxidant_loom::arrow::array::ArrayRef> {
            if col.data_type() == &DataType::Utf8View {
                let a = col
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .ok_or_else(|| Error::Execution("Utf8View downcast".into()))?;
                Ok(std::sync::Arc::new(a.gc()))
            } else {
                Ok(col.clone())
            }
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(batch.schema(), cols)
        .map_err(|e| Error::Execution(format!("compact partition batch: {e}")))
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
        for b in batches {
            out[0].push(compact_views(b.clone())?);
        }
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
            out[bucket].push(compact_views(part)?);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidant_loom::arrow::array::Int64Array;
    use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

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

    /// Partitioned output must not pin the producer's whole string buffers: a `take` over a
    /// `StringViewArray` reuses the source buffer, so a small filtered bucket would otherwise
    /// retain megabytes of dead string data (SF10 TPC-H Q16 shuffle bloat).
    #[test]
    fn string_view_buckets_do_not_retain_source_buffers() {
        use oxidant_loom::arrow::array::StringViewArray;

        let wide = "x".repeat(64);
        // 100k rows × ~64 B of live string data; partitioning by key keeps ~1/16 of them.
        let keys: Vec<i64> = (0..100_000).collect();
        let vals: Vec<String> = (0..100_000).map(|i| format!("{wide}{i}")).collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Utf8View, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(StringViewArray::from(vals)),
            ],
        )
        .unwrap();
        let source_bytes = batch.get_array_memory_size();

        let parts = hash_partition(&[batch], &[0], 16).unwrap();
        let (mut rows, mut bytes) = (0usize, 0usize);
        let mut seen = Vec::new();
        for p in &parts {
            for b in p {
                rows += b.num_rows();
                bytes += b.get_array_memory_size();
                let v = b
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .unwrap();
                seen.push(v.value(0).to_string());
            }
        }
        assert_eq!(rows, 100_000, "compaction must not drop rows");
        assert!(
            seen.iter().all(|s| s.starts_with(&wide)),
            "compaction must preserve values"
        );
        // Live data is ~7 MB; un-compacted buckets would pin the full source buffer in
        // every bucket (~16× retention). Assert the total stays near the live size.
        assert!(
            bytes < source_bytes * 2,
            "compacted buckets must not retain source buffers: {bytes} vs source {source_bytes}"
        );
    }
}
