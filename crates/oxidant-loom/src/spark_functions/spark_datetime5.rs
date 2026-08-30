//! Spark's *calendar arithmetic* datetime functions: differences and month-aware offsets.
//!
//! Companion to [`super::spark_datetime4`] (field extraction); this file covers the functions that
//! move between two dates. Like its sibling it reuses [`super::spark_datetime3`]'s
//! proleptic-Gregorian helpers and works over UTC-naive instants.
//!
//! Functions:
//! - `datediff(endDate, startDate)` / `date_diff(endDate, startDate)` — whole days between two
//!   dates as `int`. Both operands are cast to `DATE` first, so the time of day never contributes
//!   (`datediff('2024-01-02 23:00', '2024-01-01 01:00')` is `1`, not `0`).
//! - `add_months(startDate, numMonths)` — `date` shifted by whole months, clamping the day to the
//!   last day of the target month (`add_months('2024-01-31', 1)` → `2024-02-29`).
//! - `last_day(date)` — the last day of the argument's month, as `date`.
//! - `months_between(ts1, ts2[, roundOff])` — fractional months as `double`, following Spark's
//!   (and Hive's) exact formula: whole months when the day-of-month matches or both operands are
//!   their month's last day, otherwise a 31-day-month fraction computed in *seconds*, rounded to
//!   8 decimal places unless `roundOff` is `false`.
//!
//! ## Not implemented here, deliberately
//!
//! The three-argument unit forms — `datediff(unit, start, end)`, `dateadd(unit, value, expr)`,
//! `timestampadd`, `timestampdiff` — are unreachable as UDFs for the same reason
//! [`super::spark_datetime3`] records: Spark's grammar special-cases the bare unit keyword, but
//! sqlparser's Databricks dialect parses `datediff(MONTH, a, b)` with `MONTH` as a *column
//! reference*, so planning fails with "No field named month" before any UDF is invoked. Closing
//! those needs a parser/`ExprPlanner` change, not another `register_udf`. The two-argument
//! `datediff`/`date_diff` implemented here are a different, unambiguous signature.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Date32Array, Float64Array, Int32Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;

use super::spark_datetime3::{civil_from_days, days_from_civil, MICROS_PER_DAY};

/// Register the calendar-arithmetic Spark functions into `ctx`.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(DateDiff::new("datediff")));
    ctx.register_udf(ScalarUDF::from(DateDiff::new("date_diff")));
    ctx.register_udf(ScalarUDF::from(AddMonths::new()));
    ctx.register_udf(ScalarUDF::from(LastDay::new()));
    ctx.register_udf(ScalarUDF::from(MonthsBetween::new()));
}

fn arrow_err(e: datafusion::arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(e), None)
}

/// Number of days in `(y, m)` on the proleptic Gregorian calendar.
fn days_in_month(y: i64, m: u32) -> u32 {
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        _ => 28,
    }
}

/// Cast any date/timestamp/string column to `Date32` days, erroring (never nulling) on an
/// unparseable string — Spark raises `CAST_INVALID_INPUT` under ANSI.
fn to_days(v: &ColumnarValue, n: usize) -> Result<Date32Array> {
    let arr = v.clone().into_array(n)?;
    let cast_opts = datafusion::arrow::compute::CastOptions {
        safe: false,
        format_options: Default::default(),
    };
    Ok(
        datafusion::arrow::compute::cast_with_options(&arr, &DataType::Date32, &cast_opts)
            .map_err(arrow_err)?
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("cast to Date32 yields Date32Array")
            .clone(),
    )
}

/// Cast any date/timestamp/string column to UTC-naive microseconds, erroring on a bad string.
fn to_micros(v: &ColumnarValue, n: usize) -> Result<Int64Array> {
    let arr = v.clone().into_array(n)?;
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
    Ok(datafusion::arrow::compute::cast(&ts, &DataType::Int64)
        .map_err(arrow_err)?
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("cast to Int64 yields Int64Array")
        .clone())
}

// ---------------------------------------------------------------------------
// datediff / date_diff
// ---------------------------------------------------------------------------

/// `datediff(endDate, startDate)` — whole days from `startDate` to `endDate`, as `int`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct DateDiff {
    name: &'static str,
    signature: Signature,
}

impl DateDiff {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for DateDiff {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let end = to_days(&args.args[0], n)?;
        let start = to_days(&args.args[1], n)?;
        let mut out = Int32Array::builder(n);
        for i in 0..n {
            if end.is_null(i) || start.is_null(i) {
                out.append_null();
            } else {
                out.append_value(end.value(i) - start.value(i));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// add_months
// ---------------------------------------------------------------------------

/// `add_months(startDate, numMonths)` — shift by whole months, clamping to the target month's
/// last day. Spark returns `date` even for a timestamp input.
#[derive(Debug, PartialEq, Eq, Hash)]
struct AddMonths {
    signature: Signature,
}

impl AddMonths {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for AddMonths {
    fn name(&self) -> &str {
        "add_months"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let start = to_days(&args.args[0], n)?;
        let months_in = args.args[1].clone().into_array(n)?;
        let cast_opts = datafusion::arrow::compute::CastOptions {
            safe: false,
            format_options: Default::default(),
        };
        let months =
            datafusion::arrow::compute::cast_with_options(&months_in, &DataType::Int32, &cast_opts)
                .map_err(arrow_err)?;
        let months = months.as_any().downcast_ref::<Int32Array>().unwrap();

        let mut out = Date32Array::builder(n);
        for i in 0..n {
            if start.is_null(i) || months.is_null(i) {
                out.append_null();
                continue;
            }
            let (y, m, d) = civil_from_days(start.value(i) as i64);
            // Work in absolute months so `div_euclid`/`rem_euclid` handle negative offsets and
            // pre-year-0 dates without a sign special case.
            let total = y * 12 + (m as i64 - 1) + months.value(i) as i64;
            let ny = total.div_euclid(12);
            let nm = total.rem_euclid(12) as u32 + 1;
            let nd = d.min(days_in_month(ny, nm));
            out.append_value(days_from_civil(ny, nm, nd) as i32);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// last_day
// ---------------------------------------------------------------------------

/// `last_day(date)` — the last day of the argument's month.
#[derive(Debug, PartialEq, Eq, Hash)]
struct LastDay {
    signature: Signature,
}

impl LastDay {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for LastDay {
    fn name(&self) -> &str {
        "last_day"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let d = to_days(&args.args[0], n)?;
        let mut out = Date32Array::builder(n);
        for i in 0..n {
            if d.is_null(i) {
                out.append_null();
            } else {
                let (y, m, _) = civil_from_days(d.value(i) as i64);
                out.append_value(days_from_civil(y, m, days_in_month(y, m)) as i32);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// months_between
// ---------------------------------------------------------------------------

const SECONDS_PER_DAY: i64 = 86_400;
/// Spark (following Hive) divides the sub-month remainder by a fixed 31-day month.
const SECONDS_PER_MONTH: f64 = (31 * SECONDS_PER_DAY) as f64;

/// `months_between(ts1, ts2[, roundOff])` — fractional months between two instants, as `double`.
///
/// Reproduces `DateTimeUtils.monthsBetween` exactly, including its two quirks: the result is a
/// whole number when the days-of-month agree *or* both operands are the last day of their month,
/// and otherwise the remainder is computed in whole seconds over a nominal 31-day month (Hive
/// compatibility — using millis loses precision past 8 digits, which is also why `roundOff`
/// rounds to 1e-8).
#[derive(Debug, PartialEq, Eq, Hash)]
struct MonthsBetween {
    signature: Signature,
}

impl MonthsBetween {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(2), TypeSignature::Any(3)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for MonthsBetween {
    fn name(&self) -> &str {
        "months_between"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let a = to_micros(&args.args[0], n)?;
        let b = to_micros(&args.args[1], n)?;

        // `roundOff` defaults to true; a NULL third argument makes the whole result NULL, which is
        // what Spark's null-intolerant expression does.
        let round_off = match args.args.get(2) {
            None => None,
            Some(v) => {
                let arr = v.clone().into_array(n)?;
                Some(
                    datafusion::arrow::compute::cast(&arr, &DataType::Boolean)
                        .map_err(arrow_err)?
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::BooleanArray>()
                        .expect("cast to Boolean yields BooleanArray")
                        .clone(),
                )
            }
        };

        let mut out = Float64Array::builder(n);
        for i in 0..n {
            let round = match &round_off {
                None => true,
                Some(r) if r.is_null(i) => {
                    out.append_null();
                    continue;
                }
                Some(r) => r.value(i),
            };
            if a.is_null(i) || b.is_null(i) {
                out.append_null();
                continue;
            }
            out.append_value(months_between(a.value(i), b.value(i), round));
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// The scalar kernel, split out so it is unit-testable without an engine.
fn months_between(micros1: i64, micros2: i64, round_off: bool) -> f64 {
    let days1 = micros1.div_euclid(MICROS_PER_DAY);
    let days2 = micros2.div_euclid(MICROS_PER_DAY);
    let (y1, m1, d1) = civil_from_days(days1);
    let (y2, m2, d2) = civil_from_days(days2);

    let month_diff = ((y1 * 12 + m1 as i64) - (y2 * 12 + m2 as i64)) as f64;
    let to_month_end1 = days_in_month(y1, m1) - d1;
    let to_month_end2 = days_in_month(y2, m2) - d2;
    if d1 == d2 || (to_month_end1 == 0 && to_month_end2 == 0) {
        return month_diff;
    }

    let secs_in_day1 = micros1.rem_euclid(MICROS_PER_DAY) / 1_000_000;
    let secs_in_day2 = micros2.rem_euclid(MICROS_PER_DAY) / 1_000_000;
    let secs_diff = (d1 as i64 - d2 as i64) * SECONDS_PER_DAY + secs_in_day1 - secs_in_day2;
    let diff = month_diff + secs_diff as f64 / SECONDS_PER_MONTH;
    if round_off {
        (diff * 1e8).round() / 1e8
    } else {
        diff
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

    #[tokio::test]
    async fn datediff_counts_whole_days_and_ignores_time() {
        for (q, want) in [
            ("SELECT datediff(DATE'2024-03-05', DATE'2024-03-01') AS x", "4"),
            ("SELECT date_diff(DATE'2024-03-01', DATE'2024-03-05') AS x", "-4"),
            // Time of day never contributes: both operands are cast to DATE first.
            (
                "SELECT datediff(TIMESTAMP'2024-01-02 23:00:00', TIMESTAMP'2024-01-01 01:00:00') AS x",
                "1",
            ),
            // Leap day is a real day.
            ("SELECT datediff(DATE'2024-03-01', DATE'2024-02-28') AS x", "2"),
            ("SELECT datediff(DATE'2023-03-01', DATE'2023-02-28') AS x", "1"),
        ] {
            let got = row(q).await;
            assert!(got.contains(want), "{q} -> want {want}, got:\n{got}");
        }
    }

    #[tokio::test]
    async fn add_months_clamps_to_the_month_end() {
        for (q, want) in [
            ("SELECT add_months(DATE'2024-01-31', 1) AS x", "2024-02-29"),
            ("SELECT add_months(DATE'2023-01-31', 1) AS x", "2023-02-28"),
            ("SELECT add_months(DATE'2024-01-15', 1) AS x", "2024-02-15"),
            ("SELECT add_months(DATE'2024-03-31', -1) AS x", "2024-02-29"),
            // Whole-year offsets cross the year boundary in both directions.
            ("SELECT add_months(DATE'2024-03-15', 12) AS x", "2025-03-15"),
            (
                "SELECT add_months(DATE'2024-03-15', -15) AS x",
                "2022-12-15",
            ),
        ] {
            let got = row(q).await;
            assert!(got.contains(want), "{q} -> want {want}, got:\n{got}");
        }
    }

    #[tokio::test]
    async fn last_day_handles_leap_years() {
        for (q, want) in [
            ("SELECT last_day(DATE'2024-02-05') AS x", "2024-02-29"),
            ("SELECT last_day(DATE'2023-02-05') AS x", "2023-02-28"),
            ("SELECT last_day(DATE'2024-12-01') AS x", "2024-12-31"),
            (
                "SELECT last_day(TIMESTAMP'2024-04-10 12:00:00') AS x",
                "2024-04-30",
            ),
        ] {
            let got = row(q).await;
            assert!(got.contains(want), "{q} -> want {want}, got:\n{got}");
        }
    }

    /// The three documented Spark cases: matching day-of-month, both-month-end, and the seconds
    /// fraction over a nominal 31-day month.
    #[test]
    fn months_between_kernel_matches_spark() {
        let d = |y: i64, m: u32, day: u32| days_from_civil(y, m, day) * MICROS_PER_DAY;
        // Same day-of-month -> whole months.
        assert_eq!(months_between(d(2024, 3, 15), d(2024, 1, 15), true), 2.0);
        // Both operands are their month's last day -> whole months, even though 31 != 29.
        assert_eq!(months_between(d(2024, 3, 31), d(2024, 2, 29), true), 1.0);
        // Otherwise: (d1-d2) days over a 31-day month.
        assert_eq!(
            months_between(d(2024, 3, 20), d(2024, 2, 15), true),
            1.16129032
        );
        // roundOff=false keeps the unrounded quotient.
        let raw = months_between(d(2024, 3, 20), d(2024, 2, 15), false);
        assert!((raw - (1.0 + 5.0 / 31.0)).abs() < 1e-12, "{raw}");
        // Negative direction is the mirror image.
        assert_eq!(months_between(d(2024, 1, 15), d(2024, 3, 15), true), -2.0);
    }

    #[tokio::test]
    async fn months_between_end_to_end() {
        for (q, want) in [
            (
                "SELECT months_between(TIMESTAMP'2024-03-31 00:00:00', TIMESTAMP'2024-02-29 00:00:00') AS x",
                "1.0",
            ),
            (
                "SELECT months_between(DATE'2024-03-20', DATE'2024-02-15') AS x",
                "1.16129032",
            ),
            (
                "SELECT months_between(DATE'2024-03-20', DATE'2024-02-15', false) AS x",
                "1.1612903225806452",
            ),
        ] {
            let got = row(q).await;
            assert!(got.contains(want), "{q} -> want {want}, got:\n{got}");
        }
    }
}
