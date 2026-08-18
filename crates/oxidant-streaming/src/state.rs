//! Cross-batch streaming state: deduplication within a watermark bound.
//!
//! State is what makes streaming hard, and the two ways to get it wrong are both silent. This
//! used to be an in-memory `HashSet` that was never persisted and, on reaching 100,000 keys,
//! called `clear()` — so a restart re-admitted every duplicate, and a high-cardinality stream
//! re-admitted them mid-run at an unpredictable moment. Neither produced an error; both produced
//! wrong answers.
//!
//! Two rules fix that, and they are the reason [`DedupState`] cannot be built without a
//! watermark:
//!
//! - **Forgetting is driven by the watermark, never by a count.** A key is dropped once no
//!   record old enough to match it is expected any more, which is precisely what a watermark
//!   asserts. Dropping the *oldest* keys because there are too many of them asserts nothing.
//! - **The state is part of the checkpoint.** It is written with the batch that produced it, so a
//!   restart resumes with the keys it had rather than an empty set.

use std::collections::BTreeMap;

use oxidant_loom::arrow::array::BooleanArray;
use oxidant_loom::arrow::compute::filter_record_batch;
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::arrow::util::display::{ArrayFormatter, FormatOptions};
use serde::{Deserialize, Serialize};

/// Keys seen within the watermark window, each with the event time it was seen at.
///
/// Serialized into the checkpoint, so the representation is deliberately plain: a sorted map of
/// key to event time. Sorted so the serialized form is byte-stable across runs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DedupState {
    /// Key → the event time of the record that claimed it.
    seen: BTreeMap<String, i64>,
}

impl DedupState {
    /// Filter `batches` to rows whose key has not been seen (Spark's `dropDuplicates`).
    ///
    /// `event_times` supplies the event time per row, which is what later makes the key
    /// forgettable. A row with no usable event time is deduplicated but never expires, so it is
    /// the caller's job — via config validation — to ensure the watermark column exists.
    pub fn dedup_batches(
        &mut self,
        batches: &[RecordBatch],
        key_cols: &[usize],
        event_time_col: Option<usize>,
    ) -> oxidant_common::Result<Vec<RecordBatch>> {
        if key_cols.is_empty() {
            return Ok(batches.to_vec());
        }
        let mut out = Vec::new();
        for batch in batches {
            let formatters = key_formatters(batch, key_cols)?;
            let event_times = event_time_col.map(|index| batch.column(index).clone());

            let mut keep = vec![false; batch.num_rows()];
            for (row, slot) in keep.iter_mut().enumerate() {
                let key = row_key(&formatters, row)?;
                if self.seen.contains_key(&key) {
                    continue;
                }
                let at = event_times
                    .as_ref()
                    .and_then(|array| crate::watermark::row_event_time_micros(array, row))
                    .unwrap_or(i64::MAX);
                self.seen.insert(key, at);
                *slot = true;
            }
            let filtered = filter_record_batch(batch, &BooleanArray::from(keep))
                .map_err(|e| oxidant_common::Error::Execution(format!("dropDuplicates: {e}")))?;
            if filtered.num_rows() > 0 {
                out.push(filtered);
            }
        }
        Ok(out)
    }

    /// Forget every key whose record is older than the watermark.
    ///
    /// This is the only way keys leave the map. `i64::MAX` — a row whose event time could not be
    /// read — is never below any watermark, so it is retained rather than silently expired.
    pub fn expire(&mut self, watermark_micros: i64) {
        self.seen.retain(|_, at| *at >= watermark_micros);
    }

    pub fn len(&self) -> usize {
        self.seen.len()
    }

    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }
}

/// One formatter per key column, built once per batch rather than per row.
///
/// Arrow's own display formatting is what makes the key correct for *every* type. The previous
/// hand-rolled version handled `Utf8` and `Int64` and fell back to the **row index** for
/// everything else — so deduplicating on an `Int32`, a timestamp, or a decimal keyed on the row's
/// position in the batch, which is to say on nothing at all.
fn key_formatters<'a>(
    batch: &'a RecordBatch,
    key_cols: &[usize],
) -> oxidant_common::Result<Vec<ArrayFormatter<'a>>> {
    let options = FormatOptions::default().with_null("\u{0}null");
    key_cols
        .iter()
        .map(|index| {
            ArrayFormatter::try_new(batch.column(*index).as_ref(), &options).map_err(|e| {
                oxidant_common::Error::Execution(format!(
                    "dropDuplicates cannot key on column {index}: {e}"
                ))
            })
        })
        .collect()
}

/// The composite key for one row.
///
/// Fields are separated by a unit separator that cannot occur in Arrow's rendering of a value, so
/// `("ab", "c")` and `("a", "bc")` are different keys.
fn row_key(formatters: &[ArrayFormatter<'_>], row: usize) -> oxidant_common::Result<String> {
    let mut key = String::new();
    for formatter in formatters {
        if !key.is_empty() {
            key.push('\u{1f}');
        }
        use std::fmt::Write;
        write!(key, "{}", formatter.value(row))
            .map_err(|e| oxidant_common::Error::Execution(format!("dropDuplicates key: {e}")))?;
    }
    Ok(key)
}

/// Resolve the configured column names to indices in the batch, rejecting names that are absent.
///
/// Silently skipping a missing column would quietly widen the key — deduplicating on
/// `(order_id, region)` when `region` was misspelled keeps rows that should have collapsed.
pub fn resolve_key_columns(
    batch: &RecordBatch,
    names: &[String],
) -> oxidant_common::Result<Vec<usize>> {
    names
        .iter()
        .map(|name| {
            batch.schema().index_of(name).map_err(|_| {
                oxidant_common::Error::Plan(format!(
                    "`dedup_columns` names `{name}`, which the stream does not produce; \
                     available: {}",
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidant_loom::arrow::array::{
        Int32Array, Int64Array, StringArray, TimestampMillisecondArray,
    };
    use oxidant_loom::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    fn keyed_batch(keys: Vec<&str>, times: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(TimestampMillisecondArray::from(times)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn dedup_drops_duplicate_keys() {
        let batch = keyed_batch(vec!["a", "a", "b"], vec![1, 2, 3]);
        let mut state = DedupState::default();
        let out = state.dedup_batches(&[batch], &[0], Some(1)).unwrap();
        let rows: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2);
    }

    #[test]
    fn keys_are_forgotten_by_the_watermark_and_not_by_a_count() {
        // The failure this rules out: a bounded set calling `clear()` at some arbitrary size, so
        // a high-cardinality stream starts re-admitting duplicates part-way through a run with
        // no error and no way to notice.
        let mut state = DedupState::default();
        state
            .dedup_batches(
                &[keyed_batch(vec!["old", "new"], vec![1_000, 9_000])],
                &[0],
                Some(1),
            )
            .unwrap();
        assert_eq!(state.len(), 2);

        state.expire(5_000_000);
        assert_eq!(state.len(), 1, "only the key behind the watermark goes");

        // The expired key is deduplicated no longer; the retained one still is.
        let out = state
            .dedup_batches(
                &[keyed_batch(vec!["old", "new"], vec![6_000, 9_000])],
                &[0],
                Some(1),
            )
            .unwrap();
        assert_eq!(out.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }

    #[test]
    fn the_key_covers_types_the_hand_rolled_encoding_used_to_ignore() {
        // `Int32` fell through to the row *index* before, so two different values in the same
        // positions looked distinct and two equal values in different positions looked distinct
        // too — dedup on an Int32 column was effectively a no-op.
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let first =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![7, 8]))])
                .unwrap();
        let second =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![8, 9]))]).unwrap();

        let mut state = DedupState::default();
        assert_eq!(
            state
                .dedup_batches(&[first], &[0], None)
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>(),
            2
        );
        assert_eq!(
            state
                .dedup_batches(&[second], &[0], None)
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>(),
            1,
            "the 8 was already seen, in a different row position"
        );
    }

    #[test]
    fn a_composite_key_cannot_be_confused_by_concatenation() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["ab", "a"])),
                Arc::new(StringArray::from(vec!["c", "bc"])),
            ],
        )
        .unwrap();
        let mut state = DedupState::default();
        let out = state.dedup_batches(&[batch], &[0, 1], None).unwrap();
        assert_eq!(
            out.iter().map(|b| b.num_rows()).sum::<usize>(),
            2,
            "(ab,c) and (a,bc) are different keys"
        );
    }

    #[test]
    fn the_state_round_trips_through_the_checkpoint() {
        let mut state = DedupState::default();
        state
            .dedup_batches(&[keyed_batch(vec!["a"], vec![1_000])], &[0], Some(1))
            .unwrap();
        let json = serde_json::to_vec(&state).unwrap();
        let mut restored: DedupState = serde_json::from_slice(&json).unwrap();

        // The whole point: a restart recognizes the duplicate it saw before it went down.
        let out = restored
            .dedup_batches(&[keyed_batch(vec!["a"], vec![1_000])], &[0], Some(1))
            .unwrap();
        assert!(
            out.is_empty(),
            "a restored state still recognizes what it had seen"
        );
    }

    #[test]
    fn a_misspelled_dedup_column_is_an_error_not_a_wider_key() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        let err = resolve_key_columns(&batch, &["id".into(), "reigon".into()])
            .expect_err("the typo must not be skipped");
        assert!(err.to_string().contains("reigon"), "{err}");
    }
}
