//! Spark datetime constructor / converter scalar functions that DataFusion lacks.
//!
//! Implemented faithfully against Apache Spark v4.0.0 (see
//! `oxidant-spark-compat/spark-tests/{inputs,results}/{date,timestamp}.sql*`):
//!
//! - `timestamp_seconds(x)` / `timestamp_millis(x)` / `timestamp_micros(x)` — interpret a numeric
//!   epoch offset (seconds / millis / micros) as a `timestamp` (timezone-aware). The result is a
//!   `Timestamp(Microsecond, Some(tz))`; we tag it with the session timezone (UTC by default).
//!   Integer overflow when widening to microseconds is a runtime error (Spark raises
//!   `java.lang.ArithmeticException: long overflow`). For `timestamp_seconds`, a fractional
//!   floating-point input is scaled to micros; a value with sub-microsecond precision that cannot
//!   be represented exactly is an error in Spark (`Rounding necessary`) — we mirror that by
//!   rejecting any double whose micros value is not integral.
//!
//! - `next_day(date, dayOfWeek)` — the first date *strictly after* `date` that falls on the named
//!   weekday. `dayOfWeek` accepts Spark's abbreviations (`Mon`/`Monday`/`Mo`, case-insensitive);
//!   an unrecognized name raises `ILLEGAL_DAY_OF_WEEK`, *unless* the date operand is NULL (then the
//!   result is NULL regardless). The date operand is cast to `date` first (string/timestamp inputs
//!   are accepted in non-ANSI mode, matching Spark). Returns `date`.
//!
//! Deferred (exact Spark semantics not verifiable here without the session/JVM machinery):
//! - `make_date` — DataFusion already implements it with matching values and error behavior; only
//!   the synthesized column *name* differs (a `schema-only`, already-semantic-pass divergence), so
//!   overriding it would gain nothing.
//! - `make_timestamp` / `make_timestamp_ntz` / `make_timestamp_ltz` — Spark renders these in the
//!   session timezone with second-fraction and 60->rollover rules; the golden output is in
//!   `America/Los_Angeles` while oxidant's session is UTC, so a faithful value can never match the
//!   golden render. Deferred until the harness pins a session timezone.
//! - 3-arg `dateadd`/`date_add` and `datediff` (unit form), `convert_timezone` — involved
//!   unit/timezone semantics; deferred.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Date32Array, Float64Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{exec_err, DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

/// Register all datetime constructor/converter Spark functions into `ctx`.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(EpochToTimestamp::new(EpochUnit::Second)));
    ctx.register_udf(ScalarUDF::from(EpochToTimestamp::new(EpochUnit::Milli)));
    ctx.register_udf(ScalarUDF::from(EpochToTimestamp::new(EpochUnit::Micro)));
    ctx.register_udf(ScalarUDF::from(NextDay::new()));
}

fn arrow_err(e: datafusion::arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(e), None)
}

// ---------------------------------------------------------------------------
// timestamp_seconds / timestamp_millis / timestamp_micros
// ---------------------------------------------------------------------------

/// The epoch unit of an [`EpochToTimestamp`] input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum EpochUnit {
    Second,
    Milli,
    Micro,
}

impl EpochUnit {
    fn name(self) -> &'static str {
        match self {
            EpochUnit::Second => "timestamp_seconds",
            EpochUnit::Milli => "timestamp_millis",
            EpochUnit::Micro => "timestamp_micros",
        }
    }

    /// Multiplier to convert one unit into microseconds.
    fn micros_per_unit(self) -> i64 {
        match self {
            EpochUnit::Second => 1_000_000,
            EpochUnit::Milli => 1_000,
            EpochUnit::Micro => 1,
        }
    }
}

/// `timestamp_seconds` / `timestamp_millis` / `timestamp_micros` — epoch offset -> timestamp.
///
/// The result is a timezone-aware `Timestamp(Microsecond, Some(tz))` (Spark's `timestamp`/LTZ
/// type). Overflow while widening to microseconds is a runtime error, matching Spark's
/// `java.lang.ArithmeticException: long overflow`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct EpochToTimestamp {
    unit: EpochUnit,
    signature: Signature,
}

impl EpochToTimestamp {
    fn new(unit: EpochUnit) -> Self {
        Self {
            unit,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }

    /// The session-timezone tag for the produced timestamp. oxidant's default session is UTC; we
    /// emit `UTC` so the value is rendered as the documented epoch instant in UTC. (Spark tags it
    /// with the session timezone; the two agree whenever the session timezone is UTC.)
    fn tz() -> Arc<str> {
        Arc::from("UTC")
    }
}

impl ScalarUDFImpl for EpochToTimestamp {
    fn name(&self) -> &str {
        self.unit.name()
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Microsecond, Some(Self::tz())))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let dt = args.arg_fields[0].data_type().clone();
        let arr = args.args[0].clone().into_array(n)?;

        // Build the micros column. Floating-point inputs (meaningful for `timestamp_seconds`, but
        // accepted for any unit) are scaled with rounding-rejection; integral inputs use checked
        // multiplication so overflow is an error rather than a silent wraparound.
        let micros: Int64Array = if is_floatish(&dt) {
            let f = cast_f64(&arr)?;
            let scale = self.unit.micros_per_unit() as f64;
            let mut out = Int64Array::builder(n);
            for i in 0..n {
                if f.is_null(i) {
                    out.append_null();
                    continue;
                }
                let scaled = f.value(i) * scale;
                // Spark errors ("Rounding necessary") when the scaled micros is not integral.
                if scaled.fract() != 0.0 {
                    return exec_err!("{}: rounding necessary", self.unit.name());
                }
                if !scaled.is_finite() || scaled.abs() >= 9.223_372_036_854_776e18 {
                    return exec_err!("{}: long overflow", self.unit.name());
                }
                out.append_value(scaled as i64);
            }
            out.finish()
        } else {
            let raw = cast_i64(&arr)?;
            let mul = self.unit.micros_per_unit();
            let mut out = Int64Array::builder(n);
            for i in 0..n {
                if raw.is_null(i) {
                    out.append_null();
                    continue;
                }
                match raw.value(i).checked_mul(mul) {
                    Some(v) => out.append_value(v),
                    None => return exec_err!("{}: long overflow", self.unit.name()),
                }
            }
            out.finish()
        };

        // Re-interpret the i64 micros (same native type) as a timezone-aware timestamp.
        let ts = micros
            .reinterpret_cast::<datafusion::arrow::datatypes::TimestampMicrosecondType>()
            .with_timezone(Self::tz());
        Ok(ColumnarValue::Array(Arc::new(ts)))
    }
}

/// Whether a type is floating-point (so we scale with rounding-rejection rather than checked
/// integer multiply).
fn is_floatish(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

fn cast_i64(arr: &ArrayRef) -> Result<Int64Array> {
    Ok(datafusion::arrow::compute::cast(arr, &DataType::Int64)
        .map_err(arrow_err)?
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("cast to Int64 yields Int64Array")
        .clone())
}

fn cast_f64(arr: &ArrayRef) -> Result<Float64Array> {
    Ok(datafusion::arrow::compute::cast(arr, &DataType::Float64)
        .map_err(arrow_err)?
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("cast to Float64 yields Float64Array")
        .clone())
}

// ---------------------------------------------------------------------------
// next_day
// ---------------------------------------------------------------------------

/// `next_day(startDate, dayOfWeek)` — the first date strictly after `startDate` whose weekday is
/// `dayOfWeek`. Returns `date`. The date operand is cast to `date` (string/timestamp accepted);
/// an unrecognized `dayOfWeek` raises `ILLEGAL_DAY_OF_WEEK`, but only when the date is non-NULL
/// (a NULL date yields NULL regardless of the weekday string).
#[derive(Debug, PartialEq, Eq, Hash)]
struct NextDay {
    signature: Signature,
}

impl NextDay {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

/// Map a Spark day-of-week name to `0..=6` where `0 = Sunday` … `6 = Saturday`, matching Spark's
/// `DateTimeUtils.getDayOfWeekFromString` (accepts 2-letter, 3-letter and full names,
/// case-insensitive). Returns `None` for an unrecognized name.
fn day_of_week_index(s: &str) -> Option<i32> {
    match s.trim().to_uppercase().as_str() {
        "SU" | "SUN" | "SUNDAY" => Some(0),
        "MO" | "MON" | "MONDAY" => Some(1),
        "TU" | "TUE" | "TUESDAY" => Some(2),
        "WE" | "WED" | "WEDNESDAY" => Some(3),
        "TH" | "THU" | "THURSDAY" => Some(4),
        "FR" | "FRI" | "FRIDAY" => Some(5),
        "SA" | "SAT" | "SATURDAY" => Some(6),
        _ => None,
    }
}

/// Weekday of a Date32 value (days since the Unix epoch), `0 = Sunday` … `6 = Saturday`.
/// 1970-01-01 (day 0) is a Thursday (index 4), so `weekday = (days + 4) mod 7`.
fn weekday_of_date32(days: i32) -> i32 {
    (days as i64 + 4).rem_euclid(7) as i32
}

impl ScalarUDFImpl for NextDay {
    fn name(&self) -> &str {
        "next_day"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        if args.args.len() != 2 {
            return exec_err!("next_day: expected 2 arguments (startDate, dayOfWeek)");
        }
        // Cast the date operand to Date32. Spark casts string/timestamp to date here; an invalid
        // date string (e.g. `'aa'`) raises `CAST_INVALID_INPUT` rather than yielding NULL, so we
        // cast *strictly* (`safe = false`) to surface that as an error.
        let date_in = args.args[0].clone().into_array(n)?;
        let cast_opts = datafusion::arrow::compute::CastOptions {
            safe: false,
            format_options: Default::default(),
        };
        let dates =
            datafusion::arrow::compute::cast_with_options(&date_in, &DataType::Date32, &cast_opts)
                .map_err(arrow_err)?;
        let dates = dates
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("cast to Date32 yields Date32Array");

        let dow_in = args.args[1].clone().into_array(n)?;
        let dow = datafusion::arrow::compute::cast(&dow_in, &DataType::Utf8).map_err(arrow_err)?;
        let dow = dow
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .expect("cast to Utf8 yields StringArray");

        let mut out = Date32Array::builder(n);
        for i in 0..n {
            // A NULL date yields NULL regardless of the weekday string (even an invalid one).
            if dates.is_null(i) || dow.is_null(i) {
                out.append_null();
                continue;
            }
            let target = match day_of_week_index(dow.value(i)) {
                Some(t) => t,
                None => {
                    return exec_err!("next_day: illegal day-of-week string `{}`", dow.value(i))
                }
            };
            let d = dates.value(i);
            let cur = weekday_of_date32(d);
            // Days to the *next* occurrence (strictly after `d`): in 1..=7.
            let delta = (target - cur).rem_euclid(7);
            let delta = if delta == 0 { 7 } else { delta };
            out.append_value(d + delta);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

#[cfg(test)]
mod tests {
    use crate::Engine;

    /// Render `q` as a pretty table string (NULL cells render as an empty cell).
    async fn run(q: &str) -> String {
        let engine = Engine::new();
        let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        crate::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn timestamp_seconds_basic_utc() {
        // 1230219000s = 2008-12-25 15:30:00 UTC (the golden is LA: 07:30:00; oxidant's session is UTC).
        let g = run("SELECT timestamp_seconds(1230219000) AS a").await;
        assert!(g.contains("2008-12-25T15:30:00"), "{g}");
        // Negative epoch.
        let g2 = run("SELECT timestamp_seconds(CAST(-1230219000 AS BIGINT)) AS a").await;
        assert!(g2.contains("1931-01-07T08:30:00"), "{g2}");
        // NULL passthrough.
        let n = run("SELECT timestamp_seconds(CAST(NULL AS BIGINT)) AS a").await;
        assert!(n.contains("|   |"), "{n}");
    }

    #[tokio::test]
    async fn timestamp_millis_and_micros() {
        let g = run("SELECT timestamp_millis(1230219000123) AS a").await;
        assert!(g.contains("2008-12-25T15:30:00.123"), "{g}");
        let g2 = run("SELECT timestamp_micros(1230219000123123) AS a").await;
        assert!(g2.contains("2008-12-25T15:30:00.123123"), "{g2}");
    }

    #[tokio::test]
    async fn timestamp_seconds_overflow_errors() {
        let engine = Engine::new();
        let r = engine
            .sql("SELECT timestamp_seconds(CAST(1230219000123123 AS BIGINT))")
            .await;
        assert!(r.is_err(), "expected long-overflow error, got {r:?}");
    }

    #[tokio::test]
    async fn timestamp_seconds_fractional() {
        // 0.123456 s = 123456 micros, exactly representable.
        let g = run("SELECT timestamp_seconds(CAST(0.123456 AS DOUBLE)) AS a").await;
        assert!(g.contains("1970-01-01T00:00:00.123456"), "{g}");
    }

    #[tokio::test]
    async fn next_day_basic() {
        // 2015-07-23 is a Thursday; next Monday is 2015-07-27.
        let g = run("SELECT next_day(DATE '2015-07-23', 'Mon') AS a").await;
        assert!(g.contains("2015-07-27"), "{g}");
        // String date operand.
        let g2 = run("SELECT next_day('2015-07-23', 'Mon') AS a").await;
        assert!(g2.contains("2015-07-27"), "{g2}");
        // Full name.
        let g3 = run("SELECT next_day(DATE '2015-07-23', 'Monday') AS a").await;
        assert!(g3.contains("2015-07-27"), "{g3}");
    }

    #[tokio::test]
    async fn next_day_null_date_is_null_even_with_bad_dow() {
        let g = run("SELECT next_day(CAST(NULL AS DATE), 'xx') AS a").await;
        assert!(g.contains("|   |"), "{g}");
    }

    #[tokio::test]
    async fn next_day_illegal_dow_errors() {
        let engine = Engine::new();
        let r = engine.sql("SELECT next_day(DATE '2015-07-23', 'xx')").await;
        assert!(r.is_err(), "expected ILLEGAL_DAY_OF_WEEK error");
    }

    #[tokio::test]
    async fn next_day_invalid_date_string_errors() {
        // An unparseable date string raises CAST_INVALID_INPUT in Spark (strict cast), not NULL.
        let engine = Engine::new();
        let r = engine.sql("SELECT next_day('aa', 'MO')").await;
        assert!(r.is_err(), "expected CAST_INVALID_INPUT error, got {r:?}");
    }

    #[tokio::test]
    async fn next_day_timestamp_string_truncates_to_date() {
        // A date string with a trailing time part casts to its date and resolves the next weekday.
        let g = run("SELECT next_day('2015-07-23 12:12:12', 'Mon') AS a").await;
        assert!(g.contains("2015-07-27"), "{g}");
    }
}
