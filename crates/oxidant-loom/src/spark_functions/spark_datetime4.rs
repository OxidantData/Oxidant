//! Spark's *date-part extraction* family: the one-argument accessors that pull a calendar field
//! out of a date or timestamp.
//!
//! DataFusion 54 ships `date_part`/`datepart` and nothing else — it registers no `year`, `month`,
//! `hour`, … under their own names (verified against `datafusion-functions-54.1.0`, whose
//! `DatePartFunc` carries only the `datepart` alias). Every one of these was therefore an
//! `Invalid function` in oxidant, which is what made *Date, timestamp, and interval functions* the
//! weakest category in `docs/databricks-functions.md`.
//!
//! All of these are pure calendar arithmetic over a UTC-naive instant, so they reuse the
//! proleptic-Gregorian helpers in [`super::spark_datetime3`] rather than pulling in a date library.
//!
//! Functions (all return `int` except `dayname`, which returns `string`):
//! - `year` / `month` / `day` / `dayofmonth` — `day` and `dayofmonth` are the same function under
//!   two Spark spellings.
//! - `quarter` — 1–4.
//! - `dayofyear` — 1–366.
//! - `hour` / `minute` / `second` — whole units; `second` truncates the fraction (Spark returns
//!   `int`, not the fractional seconds `date_part('second', …)` would give).
//! - `dayofweek` — **Sunday = 1** … Saturday = 7 (Spark's `DayOfWeek`).
//! - `weekday` — **Monday = 0** … Sunday = 6 (Spark's `WeekDay`). The two disagree deliberately;
//!   both spellings exist in Spark and the goldens pin both
//!   (`datetime-legacy.sql.out`: `dayofweek('2007-02-03')` → `7`, `weekday('2007-02-03')` → `5`).
//! - `weekofyear` — ISO-8601 week number: weeks start Monday and week 1 is the one containing the
//!   first Thursday, so 2016-01-01 (a Friday) is week 53 of 2015.
//! - `dayname` — the three-letter English abbreviation, `Sun`…`Sat`.
//!
//! Input coercion follows Spark: `DATE`, `TIMESTAMP`, and strings are all accepted (a date is read
//! as midnight, so `hour(DATE'2024-01-01')` is `0`), and an unparseable string is a *cast error*,
//! not a silent NULL — Spark raises `CAST_INVALID_INPUT` for `year('xx')` under ANSI. NULL in,
//! NULL out.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, Int64Array, StringBuilder};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

use super::spark_datetime3::{civil_from_days, days_from_civil, MICROS_PER_DAY};

/// Register the date-part extraction family into `ctx`.
pub fn register(ctx: &SessionContext) {
    for part in [
        Part::Year,
        Part::Month,
        Part::Day,
        Part::DayOfMonth,
        Part::Quarter,
        Part::DayOfYear,
        Part::Hour,
        Part::Minute,
        Part::Second,
        Part::DayOfWeek,
        Part::WeekDay,
        Part::WeekOfYear,
        Part::DayName,
    ] {
        ctx.register_udf(ScalarUDF::from(DatePart::new(part)));
    }
}

fn arrow_err(e: datafusion::arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(e), None)
}

/// Which calendar field to extract. One variant per registered Spark function name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Part {
    Year,
    Month,
    /// Spark spells the same function `day` and `dayofmonth`; both are registered.
    Day,
    DayOfMonth,
    Quarter,
    DayOfYear,
    Hour,
    Minute,
    Second,
    /// Sunday = 1 … Saturday = 7.
    DayOfWeek,
    /// Monday = 0 … Sunday = 6.
    WeekDay,
    /// ISO-8601 week number.
    WeekOfYear,
    /// `Sun`…`Sat`.
    DayName,
}

impl Part {
    fn name(self) -> &'static str {
        match self {
            Part::Year => "year",
            Part::Month => "month",
            Part::Day => "day",
            Part::DayOfMonth => "dayofmonth",
            Part::Quarter => "quarter",
            Part::DayOfYear => "dayofyear",
            Part::Hour => "hour",
            Part::Minute => "minute",
            Part::Second => "second",
            Part::DayOfWeek => "dayofweek",
            Part::WeekDay => "weekday",
            Part::WeekOfYear => "weekofyear",
            Part::DayName => "dayname",
        }
    }

    fn return_type(self) -> DataType {
        match self {
            Part::DayName => DataType::Utf8,
            _ => DataType::Int32,
        }
    }
}

/// Days since the epoch → weekday index with **Monday = 0**.
///
/// 1970-01-01 was a Thursday, so day 0 must land on index 3. `rem_euclid` keeps pre-epoch days
/// (negative) on the same wheel instead of producing a negative index.
fn weekday_mon0(days: i64) -> i64 {
    (days.rem_euclid(7) + 3) % 7
}

/// ISO-8601 week number for a day: the week containing this day's Thursday, numbered from the
/// first Thursday of its own ISO year.
fn iso_week(days: i64) -> i32 {
    let thursday = days - weekday_mon0(days) + 3;
    let (iso_year, _, _) = civil_from_days(thursday);
    let jan1 = days_from_civil(iso_year, 1, 1);
    // `thursday` is by construction in the same ISO year as `jan1`'s week-1 Thursday, so this is
    // always ≥ 0 and never needs the "week 52/53 of the previous year" fixup that a naive
    // day-of-year formula would.
    ((thursday - jan1) / 7 + 1) as i32
}

const DAY_NAMES: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

/// One Spark date-part accessor.
#[derive(Debug, PartialEq, Eq, Hash)]
struct DatePart {
    part: Part,
    signature: Signature,
}

impl DatePart {
    fn new(part: Part) -> Self {
        Self {
            part,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for DatePart {
    fn name(&self) -> &str {
        self.part.name()
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.part.return_type())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let arr = args.args[0].clone().into_array(n)?;

        // Strict cast: Spark raises CAST_INVALID_INPUT for `year('xx')` under ANSI rather than
        // returning NULL, so an unparseable string must surface as an error, not a null row.
        // Everything (DATE, TIMESTAMP of any unit/zone, string) funnels into UTC-naive micros; a
        // DATE lands at midnight, which is exactly why `hour(DATE'…')` is 0 in Spark.
        let cast_opts = datafusion::arrow::compute::CastOptions {
            safe: false,
            format_options: Default::default(),
        };
        let ts = datafusion::arrow::compute::cast_with_options(
            &arr,
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            &cast_opts,
        )
        .map_err(arrow_err)?;
        let micros = datafusion::arrow::compute::cast(&ts, &DataType::Int64).map_err(arrow_err)?;
        let micros = micros
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("cast to Int64 yields Int64Array");

        if self.part == Part::DayName {
            let mut out = StringBuilder::new();
            for i in 0..n {
                if micros.is_null(i) {
                    out.append_null();
                } else {
                    let days = micros.value(i).div_euclid(MICROS_PER_DAY);
                    out.append_value(DAY_NAMES[weekday_mon0(days) as usize]);
                }
            }
            return Ok(ColumnarValue::Array(Arc::new(out.finish())));
        }

        let mut out = Int32Array::builder(n);
        for i in 0..n {
            if micros.is_null(i) {
                out.append_null();
                continue;
            }
            let v = micros.value(i);
            // `div_euclid`/`rem_euclid` (not `/` and `%`) so pre-epoch instants floor toward -inf
            // and keep a non-negative time-of-day, matching Spark's calendar.
            let days = v.div_euclid(MICROS_PER_DAY);
            let tod = v.rem_euclid(MICROS_PER_DAY);
            let (y, m, d) = civil_from_days(days);
            let value = match self.part {
                Part::Year => y as i32,
                Part::Month => m as i32,
                Part::Day | Part::DayOfMonth => d as i32,
                Part::Quarter => ((m - 1) / 3 + 1) as i32,
                Part::DayOfYear => (days - days_from_civil(y, 1, 1) + 1) as i32,
                Part::Hour => (tod / 3_600_000_000) as i32,
                Part::Minute => ((tod / 60_000_000) % 60) as i32,
                Part::Second => ((tod / 1_000_000) % 60) as i32,
                // Spark's `dayofweek` is 1-based from Sunday; `weekday_mon0` is 0-based from
                // Monday, so rotate by one and shift.
                Part::DayOfWeek => ((weekday_mon0(days) + 1) % 7 + 1) as i32,
                Part::WeekDay => weekday_mon0(days) as i32,
                Part::WeekOfYear => iso_week(days),
                Part::DayName => unreachable!("handled above"),
            };
            out.append_value(value);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn row(q: &str) -> String {
        let engine = crate::Engine::new();
        let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        crate::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string()
    }

    /// Pinned against `spark-tests/results/datetime-legacy.sql.out`, which fixes both
    /// `dayofweek('2007-02-03')` → 7 and `weekday('2007-02-03')` → 5 for the same Saturday.
    #[tokio::test]
    async fn day_of_week_uses_both_spark_origins() {
        for (q, want) in [
            ("SELECT dayofweek('2007-02-03') AS x", "7"),
            ("SELECT dayofweek('2009-07-30') AS x", "5"),
            ("SELECT dayofweek('2017-05-27') AS x", "7"),
            ("SELECT weekday('2007-02-03') AS x", "5"),
            ("SELECT weekday('2009-07-30') AS x", "3"),
            ("SELECT weekday('2017-05-27') AS x", "5"),
            // The goldens also pin the pre-Gregorian-reform instant on the proleptic calendar.
            ("SELECT dayofweek('1582-10-15 13:10:15') AS x", "6"),
            ("SELECT weekday('1582-10-15 13:10:15') AS x", "4"),
            ("SELECT dayname('2009-07-30') AS x", "Thu"),
        ] {
            let got = row(q).await;
            assert!(got.contains(want), "{q} -> want {want}, got:\n{got}");
        }
    }

    #[tokio::test]
    async fn calendar_fields_match_spark() {
        for (q, want) in [
            ("SELECT year('1500-01-01') AS x", "1500"),
            ("SELECT year(DATE'2024-03-05') AS x", "2024"),
            ("SELECT month(DATE'2024-03-05') AS x", "3"),
            ("SELECT day(DATE'2024-03-05') AS x", "5"),
            ("SELECT dayofmonth(DATE'2024-03-05') AS x", "5"),
            ("SELECT quarter(DATE'2024-03-05') AS x", "1"),
            ("SELECT quarter(DATE'2024-12-31') AS x", "4"),
            // 2024 is a leap year: 31 + 29 + 5.
            ("SELECT dayofyear(DATE'2024-03-05') AS x", "65"),
            ("SELECT dayofyear(DATE'2023-03-05') AS x", "64"),
            ("SELECT hour(TIMESTAMP'2024-03-05 13:45:59') AS x", "13"),
            ("SELECT minute(TIMESTAMP'2024-03-05 13:45:59') AS x", "45"),
            ("SELECT second(TIMESTAMP'2024-03-05 13:45:59') AS x", "59"),
            // A DATE is midnight, so the time fields are zero rather than an error.
            ("SELECT hour(DATE'2024-03-05') AS x", "0"),
        ] {
            let got = row(q).await;
            assert!(got.contains(want), "{q} -> want {want}, got:\n{got}");
        }
    }

    /// ISO weeks, including the year-boundary cases a naive day-of-year formula gets wrong.
    #[tokio::test]
    async fn week_of_year_is_iso8601() {
        for (q, want) in [
            // 2016-01-01 is a Friday, so it belongs to the last ISO week of 2015.
            ("SELECT weekofyear(DATE'2016-01-01') AS x", "53"),
            // 2018-01-01 is a Monday: week 1 outright.
            ("SELECT weekofyear(DATE'2018-01-01') AS x", "1"),
            ("SELECT weekofyear(DATE'2024-03-05') AS x", "10"),
            ("SELECT weekofyear(DATE'2024-12-30') AS x", "1"),
        ] {
            let got = row(q).await;
            assert!(got.contains(want), "{q} -> want {want}, got:\n{got}");
        }
    }

    #[tokio::test]
    async fn null_in_null_out_and_pre_epoch() {
        // `pretty_format_batches` renders a null as a blank cell, so assert on the array.
        let engine = crate::Engine::new();
        let batches = engine
            .sql("SELECT year(CAST(NULL AS DATE)) AS x")
            .await
            .expect("year(NULL)");
        assert_eq!(batches[0].column(0).null_count(), 1);
        // Pre-epoch: floor toward -inf, never a negative time-of-day.
        for (q, want) in [
            ("SELECT year(TIMESTAMP'1969-12-31 23:00:00') AS x", "1969"),
            ("SELECT hour(TIMESTAMP'1969-12-31 23:00:00') AS x", "23"),
            ("SELECT day(TIMESTAMP'1969-12-31 23:00:00') AS x", "31"),
        ] {
            let got = row(q).await;
            assert!(got.contains(want), "{q} -> want {want}, got:\n{got}");
        }
    }
}
