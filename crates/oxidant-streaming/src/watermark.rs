//! Event-time watermarks.
//!
//! A watermark is a claim about **event time**: "no record older than this is expected any
//! more". It is derived from the data — the greatest event time seen so far, less the lateness
//! you are willing to tolerate — and it only moves forward.
//!
//! This used to be computed from the *processing* clock (`now - delay`) and used to delete rows
//! from the output. Both halves were wrong, and together they were destructive: replaying a
//! topic from `earliest` meant every historical record sat far below `now - delay`, so the whole
//! backfill was silently discarded. A watermark says nothing about wall-clock time, and in an
//! append-only query it does not filter anything.
//!
//! What it is *for* here is bounding state. `dropDuplicates` has to remember keys, and the
//! watermark is what makes forgetting them safe: once no record older than the watermark is
//! expected, the keys below it cannot be needed to recognize a duplicate. That is the whole job,
//! and it is why [`crate::state::DedupState`] requires one.

use std::time::Duration;

use oxidant_loom::arrow::array::{Array, ArrayRef, AsArray};
use oxidant_loom::arrow::datatypes::{
    Date32Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use oxidant_loom::arrow::record_batch::RecordBatch;

/// Watermark configuration (Spark `withWatermark` equivalent).
#[derive(Debug, Clone)]
pub struct WatermarkConfig {
    /// Column holding event time (must be timestamp or date).
    pub event_time_column: String,
    /// Allowed lateness before a record is considered too old to matter.
    pub delay: Duration,
}

impl WatermarkConfig {
    pub fn from_options(options: &std::collections::HashMap<String, String>) -> Option<Self> {
        let col = options
            .get("eventTimeColumn")
            .or_else(|| options.get("watermarkColumn"))?;
        let delay_ms = options
            .get("delayMs")
            .or_else(|| options.get("watermarkDelayMs"))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        Some(Self {
            event_time_column: col.clone(),
            delay: Duration::from_millis(delay_ms),
        })
    }

    /// The watermark implied by the greatest event time seen so far.
    pub fn watermark_for(&self, max_event_time_micros: i64) -> i64 {
        max_event_time_micros.saturating_sub(self.delay.as_micros() as i64)
    }
}

/// The moving watermark, restored from the checkpoint and advanced by each batch.
///
/// Monotonic by construction: a batch of older records does not drag it backwards, because a
/// watermark that could retreat would make already-evicted state necessary again.
#[derive(Debug, Clone, Default)]
pub struct WatermarkTracker {
    max_event_time_micros: Option<i64>,
}

impl WatermarkTracker {
    /// Resume from a checkpointed high-water mark.
    pub fn restore(max_event_time_micros: Option<i64>) -> Self {
        Self {
            max_event_time_micros,
        }
    }

    /// The greatest event time observed, for persisting.
    pub fn max_event_time_micros(&self) -> Option<i64> {
        self.max_event_time_micros
    }

    /// Take the greatest event time in `batches` into account, and return the watermark *before*
    /// this batch was observed.
    ///
    /// The pre-batch value is what a record in this batch is judged late against. Advancing
    /// first and then comparing would let a single record from the future mark its own batch
    /// late — and, worse, evict state the rest of the batch still needed.
    pub fn observe(&mut self, config: &WatermarkConfig, batches: &[RecordBatch]) -> Option<i64> {
        let before = self.max_event_time_micros.map(|m| config.watermark_for(m));
        for batch in batches {
            let Ok(index) = batch.schema().index_of(&config.event_time_column) else {
                continue;
            };
            if let Some(max) = max_event_time_micros(batch.column(index)) {
                self.max_event_time_micros =
                    Some(self.max_event_time_micros.map_or(max, |seen| seen.max(max)));
            }
        }
        before
    }

    /// How many rows in `batches` fall below `watermark`.
    ///
    /// Counted and reported rather than removed. In an append-only query a late record is still
    /// a record: Spark drops late data only where it would feed a stateful operator, and this
    /// engine has no stateful aggregation across batches at all, so dropping here would delete
    /// data for no benefit — which is exactly what the processing-time version did.
    pub fn count_late(config: &WatermarkConfig, batches: &[RecordBatch], watermark: i64) -> u64 {
        let mut late = 0;
        for batch in batches {
            let Ok(index) = batch.schema().index_of(&config.event_time_column) else {
                continue;
            };
            let array = batch.column(index);
            for row in 0..batch.num_rows() {
                if let Some(micros) = event_time_micros(array, row) {
                    if micros < watermark {
                        late += 1;
                    }
                }
            }
        }
        late
    }
}

/// One row's event time as microseconds since the epoch, or `None` when it is null or the column
/// is not a time type.
///
/// Each Arrow timestamp unit is a distinct concrete array type. Reading them all as microseconds
/// panics on anything else — and Kafka's own `timestamp` column is milliseconds, so the obvious
/// `withWatermark("timestamp", …)` was a crash.
pub(crate) fn row_event_time_micros(array: &ArrayRef, row: usize) -> Option<i64> {
    event_time_micros(array, row)
}

fn event_time_micros(array: &ArrayRef, row: usize) -> Option<i64> {
    use oxidant_loom::arrow::datatypes::DataType;

    if array.is_null(row) {
        return None;
    }
    Some(match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => array
            .as_primitive::<TimestampSecondType>()
            .value(row)
            .saturating_mul(1_000_000),
        DataType::Timestamp(TimeUnit::Millisecond, _) => array
            .as_primitive::<TimestampMillisecondType>()
            .value(row)
            .saturating_mul(1_000),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            array.as_primitive::<TimestampMicrosecondType>().value(row)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            array.as_primitive::<TimestampNanosecondType>().value(row) / 1_000
        }
        // `mul`, not a shift: a date is a day count, and the epoch-relative microsecond value is
        // what every other unit is normalized to.
        DataType::Date32 => {
            i64::from(array.as_primitive::<Date32Type>().value(row)).saturating_mul(86_400_000_000)
        }
        _ => return None,
    })
}

/// The greatest event time in one column.
fn max_event_time_micros(array: &ArrayRef) -> Option<i64> {
    (0..array.len())
        .filter_map(|row| event_time_micros(array, row))
        .max()
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidant_loom::arrow::array::TimestampMillisecondArray;
    use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn config(delay_secs: u64) -> WatermarkConfig {
        WatermarkConfig {
            event_time_column: "ts".into(),
            delay: Duration::from_secs(delay_secs),
        }
    }

    fn batch(millis: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampMillisecondArray::from(millis))],
        )
        .unwrap()
    }

    #[test]
    fn the_watermark_comes_from_the_data_not_the_clock() {
        // The failure this rules out: replaying a topic from `earliest` in 2026, where every
        // historical record sits far below `now - delay`. Under the processing-time version the
        // entire backfill counted as late; under this one nothing does, because the watermark
        // tracks the records themselves.
        let config = config(10);
        let mut tracker = WatermarkTracker::default();
        // Events from 2021 — ancient by the wall clock.
        let historical = batch(vec![1_600_000_000_000, 1_600_000_001_000]);

        assert_eq!(
            tracker.observe(&config, std::slice::from_ref(&historical)),
            None
        );
        let watermark = config.watermark_for(tracker.max_event_time_micros().unwrap());
        assert_eq!(
            WatermarkTracker::count_late(&config, &[historical], watermark),
            0,
            "a backfill is not late against its own event times"
        );
    }

    #[test]
    fn the_watermark_only_moves_forward() {
        let config = config(0);
        let mut tracker = WatermarkTracker::default();
        tracker.observe(&config, &[batch(vec![5_000])]);
        tracker.observe(&config, &[batch(vec![1_000])]);
        assert_eq!(
            tracker.max_event_time_micros(),
            Some(5_000_000),
            "an older batch must not drag the watermark backwards"
        );
    }

    #[test]
    fn a_batch_is_judged_against_the_watermark_that_preceded_it() {
        // One record from the future must not retroactively make its own batch late.
        let config = config(0);
        let mut tracker = WatermarkTracker::default();
        tracker.observe(&config, &[batch(vec![1_000])]);

        let mixed = batch(vec![900, 100_000]);
        let before = tracker
            .observe(&config, std::slice::from_ref(&mixed))
            .unwrap();
        assert_eq!(
            WatermarkTracker::count_late(&config, &[mixed], before),
            1,
            "only the record behind the previous watermark is late"
        );
    }

    #[test]
    fn late_records_are_counted_and_kept() {
        let config = config(0);
        let mut tracker = WatermarkTracker::default();
        tracker.observe(&config, &[batch(vec![10_000])]);
        let late = batch(vec![1_000, 2_000]);
        let before = tracker
            .observe(&config, std::slice::from_ref(&late))
            .unwrap();
        assert_eq!(
            WatermarkTracker::count_late(&config, std::slice::from_ref(&late), before),
            2
        );
        assert_eq!(late.num_rows(), 2, "counting does not remove them");
    }

    #[test]
    fn restoring_resumes_the_high_water_mark() {
        let tracker = WatermarkTracker::restore(Some(42_000_000));
        assert_eq!(tracker.max_event_time_micros(), Some(42_000_000));
        assert_eq!(config(1).watermark_for(42_000_000), 41_000_000);
    }
}
