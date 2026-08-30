//! Spark's timezone-shifting datetime functions.
//!
//! [`super::spark_datetime3`] deliberately declined the *local-zone* functions
//! (`make_timestamp_ltz`, `to_timestamp_ltz`, `localtimestamp`) because oxidant's session timezone
//! is UTC and a bare `TIMESTAMP` is timezone-naive, so those cannot reproduce Spark's local-zone
//! rendering. The functions in this file are a different shape and *are* well defined under that
//! model: each takes a naive instant plus one or two explicit zone names and applies a pure offset
//! shift, producing another naive instant. No session state is involved, so the answers are
//! session-independent and byte-comparable with Spark.
//!
//! - `current_timezone()` — the session timezone. Oxidant's is **UTC**; Spark returns whatever
//!   `spark.sql.session.timeZone` holds, so a golden generated on a machine set to
//!   `America/Los_Angeles` will disagree. That is a difference in session configuration, not a
//!   defect, and it is the honest answer for this engine.
//! - `from_utc_timestamp(ts, tz)` — read `ts` as UTC, render it in `tz`.
//! - `to_utc_timestamp(ts, tz)` — read `ts` as local time in `tz`, render it in UTC.
//! - `convert_timezone([sourceTz, ] targetTz, sourceTs)` — `to_utc_timestamp` then
//!   `from_utc_timestamp`. The two-argument form takes the source zone from the session, which for
//!   oxidant is UTC (`spark-tests/results/timestamp-ntz.sql.out` renders it as
//!   `convert_timezone(current_timezone(), …)`, confirming Spark resolves it the same way).
//!
//! Zone names resolve through the IANA database (`chrono-tz`); an unknown name is an error, not a
//! silent NULL, matching Spark's `INVALID_TIMEZONE`. **Only region-based IDs** are accepted —
//! Spark additionally takes fixed offsets (`+08:00`, `GMT+8`, `UTC+05:30`), which `chrono_tz`
//! cannot parse; the error message says so rather than implying broader support.
//!
//! ## Ambiguous and non-existent local times
//!
//! `to_utc_timestamp` has to map a local wall-clock time back onto the UTC line, and DST makes that
//! ambiguous twice a year. We follow `java.time.LocalDateTime.atZone`, which is what Spark calls:
//! - **Ambiguous** (the hour repeats when clocks go back): take the *earlier* offset.
//! - **Non-existent** (the hour is skipped when clocks go forward): use the offset in force
//!   *before* the transition, which is equivalent to Java shifting the local time forward by the
//!   gap. `02:30` on a US spring-forward day therefore resolves as if it were `03:30`.

use std::str::FromStr;
use std::sync::Arc;

use chrono::{Duration, LocalResult, NaiveDateTime, Offset, TimeZone};
use chrono_tz::Tz;
use datafusion::arrow::array::{Array, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;

/// Oxidant's session timezone. Fixed, and documented as such in `docs/runtime-contract.md`'s
/// UTC-naive timestamp model.
pub(super) const SESSION_TIMEZONE: &str = "UTC";

/// Register the timezone-shifting Spark functions into `ctx`.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(CurrentTimezone::new()));
    ctx.register_udf(ScalarUDF::from(UtcShift::new("from_utc_timestamp", true)));
    ctx.register_udf(ScalarUDF::from(UtcShift::new("to_utc_timestamp", false)));
    ctx.register_udf(ScalarUDF::from(ConvertTimezone::new()));
}

fn arrow_err(e: datafusion::arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(e), None)
}

/// Resolve an IANA region name (`Europe/Paris`, `UTC`).
///
/// Spark also accepts fixed offsets — `+08:00`, `GMT+8`, `UTC+05:30` — which `chrono_tz` does not
/// parse. Rather than advertise support that does not exist, the error names exactly what this
/// implementation takes; supporting offsets is tracked as follow-on work.
fn parse_zone(name: &str) -> Result<Tz> {
    Tz::from_str(name).map_err(|_| {
        DataFusionError::Execution(format!(
            "[INVALID_TIMEZONE] The timezone: {name} is invalid. Oxidant resolves region-based \
             IANA zone IDs (for example 'Europe/Paris' or 'UTC'); fixed-offset forms such as \
             '+08:00' or 'GMT+8' are not supported yet."
        ))
    })
}

/// Cast any date/timestamp/string column to UTC-naive microseconds, erroring on a bad string.
fn to_micros(v: &ColumnarValue, n: usize) -> Result<datafusion::arrow::array::Int64Array> {
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
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("cast to Int64 yields Int64Array")
        .clone())
}

fn to_strings(v: &ColumnarValue, n: usize) -> Result<StringArray> {
    let arr = v.clone().into_array(n)?;
    Ok(datafusion::arrow::compute::cast(&arr, &DataType::Utf8)
        .map_err(arrow_err)?
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("cast to Utf8 yields StringArray")
        .clone())
}

/// Micros of a naive instant read as UTC → micros of the same instant rendered in `tz`.
fn utc_to_zone(micros: i64, tz: Tz) -> Option<i64> {
    let naive = NaiveDateTime::from_timestamp_micros_compat(micros)?;
    let offset = tz.offset_from_utc_datetime(&naive).fix().local_minus_utc() as i64;
    Some(micros + offset * 1_000_000)
}

/// Micros of a naive instant read as local time in `tz` → micros of that instant in UTC.
fn zone_to_utc(micros: i64, tz: Tz) -> Option<i64> {
    let naive = NaiveDateTime::from_timestamp_micros_compat(micros)?;
    let offset_secs = match tz.from_local_datetime(&naive) {
        // Unambiguous.
        LocalResult::Single(dt) => dt.offset().fix().local_minus_utc(),
        // Clocks went back: Java's `atZone` keeps the earlier (pre-transition) offset.
        LocalResult::Ambiguous(earlier, _) => earlier.offset().fix().local_minus_utc(),
        // Clocks went forward and this local time never happened. Java shifts it forward by the
        // gap, which is the same instant you get by applying the pre-transition offset. Resolve
        // that offset a day earlier, where the local time is unambiguous.
        LocalResult::None => {
            let before = naive - Duration::days(1);
            match tz.from_local_datetime(&before) {
                LocalResult::Single(dt) => dt.offset().fix().local_minus_utc(),
                LocalResult::Ambiguous(earlier, _) => earlier.offset().fix().local_minus_utc(),
                // Two gaps within 24 hours does not occur in the IANA database; fall back to the
                // standard offset rather than erroring on data.
                LocalResult::None => tz.offset_from_utc_datetime(&naive).fix().local_minus_utc(),
            }
        }
    } as i64;
    Some(micros - offset_secs * 1_000_000)
}

/// `NaiveDateTime::from_timestamp_micros` is deprecated in newer chrono and absent in older ones;
/// build it from seconds + nanos, which is stable across the 0.4 line.
trait FromMicros: Sized {
    fn from_timestamp_micros_compat(micros: i64) -> Option<Self>;
}

impl FromMicros for NaiveDateTime {
    fn from_timestamp_micros_compat(micros: i64) -> Option<Self> {
        let secs = micros.div_euclid(1_000_000);
        let nanos = (micros.rem_euclid(1_000_000) * 1_000) as u32;
        chrono::DateTime::from_timestamp(secs, nanos).map(|dt| dt.naive_utc())
    }
}

// ---------------------------------------------------------------------------
// current_timezone
// ---------------------------------------------------------------------------

/// `current_timezone()` — oxidant's session timezone, always `UTC`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct CurrentTimezone {
    signature: Signature,
}

impl CurrentTimezone {
    fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for CurrentTimezone {
    fn name(&self) -> &str {
        "current_timezone"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            SESSION_TIMEZONE.to_string(),
        ))))
    }
}

// ---------------------------------------------------------------------------
// from_utc_timestamp / to_utc_timestamp
// ---------------------------------------------------------------------------

/// `from_utc_timestamp(ts, tz)` (`from_utc = true`) and `to_utc_timestamp(ts, tz)`
/// (`from_utc = false`) — the two directions of a single explicit-zone shift.
#[derive(Debug, PartialEq, Eq, Hash)]
struct UtcShift {
    name: &'static str,
    from_utc: bool,
    signature: Signature,
}

impl UtcShift {
    fn new(name: &'static str, from_utc: bool) -> Self {
        Self {
            name,
            from_utc,
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for UtcShift {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let ts = to_micros(&args.args[0], n)?;
        let zones = to_strings(&args.args[1], n)?;
        let mut out = TimestampMicrosecondArray::builder(n);
        for i in 0..n {
            if ts.is_null(i) || zones.is_null(i) {
                out.append_null();
                continue;
            }
            let tz = parse_zone(zones.value(i))?;
            let shifted = if self.from_utc {
                utc_to_zone(ts.value(i), tz)
            } else {
                zone_to_utc(ts.value(i), tz)
            };
            match shifted {
                Some(v) => out.append_value(v),
                None => {
                    return exec_err!("{}: timestamp out of the representable range", self.name)
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// convert_timezone
// ---------------------------------------------------------------------------

/// `convert_timezone([sourceTz, ] targetTz, sourceTs)` — shift between two explicit zones. The
/// two-argument form resolves the source zone from the session (UTC for oxidant).
#[derive(Debug, PartialEq, Eq, Hash)]
struct ConvertTimezone {
    signature: Signature,
}

impl ConvertTimezone {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(2), TypeSignature::Any(3)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ConvertTimezone {
    fn name(&self) -> &str {
        "convert_timezone"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        // Two-arg: (targetTz, sourceTs). Three-arg: (sourceTz, targetTz, sourceTs).
        let (source, target, ts) = if args.args.len() == 2 {
            (None, &args.args[0], &args.args[1])
        } else {
            (Some(&args.args[0]), &args.args[1], &args.args[2])
        };
        let ts = to_micros(ts, n)?;
        let target = to_strings(target, n)?;
        let source = match source {
            Some(v) => Some(to_strings(v, n)?),
            None => None,
        };

        let mut out = TimestampMicrosecondArray::builder(n);
        for i in 0..n {
            let source_null = source.as_ref().is_some_and(|s| s.is_null(i));
            if ts.is_null(i) || target.is_null(i) || source_null {
                out.append_null();
                continue;
            }
            let source_tz = match &source {
                Some(s) => parse_zone(s.value(i))?,
                None => parse_zone(SESSION_TIMEZONE)?,
            };
            let target_tz = parse_zone(target.value(i))?;
            let utc = zone_to_utc(ts.value(i), source_tz);
            match utc.and_then(|u| utc_to_zone(u, target_tz)) {
                Some(v) => out.append_value(v),
                None => {
                    return exec_err!("convert_timezone: timestamp out of the representable range")
                }
            }
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

    #[tokio::test]
    async fn current_timezone_is_utc() {
        let got = row("SELECT current_timezone() AS x").await;
        assert!(got.contains("UTC"), "{got}");
    }

    /// Pinned to `spark-tests/results/timestamp-ntz.sql.out`, which fixes
    /// `convert_timezone('Europe/Moscow', 'America/Los_Angeles', TIMESTAMP_NTZ'2022-01-01 00:00:00')`
    /// → `2021-12-31 13:00:00`. Moscow is UTC+3 year-round, Los Angeles is UTC-8 in January.
    #[tokio::test]
    async fn convert_timezone_matches_the_spark_golden() {
        let got = row(
            "SELECT convert_timezone('Europe/Moscow', 'America/Los_Angeles', \
             TIMESTAMP'2022-01-01 00:00:00') AS x",
        )
        .await;
        assert!(got.contains("2021-12-31T13:00:00"), "{got}");
    }

    /// The two-argument form takes the source zone from the session. Oxidant's is UTC, so this is
    /// `from_utc_timestamp`; Spark's golden for this row was generated under
    /// `America/Los_Angeles` and therefore reads `08:00:00` rather than `01:00:00`.
    #[tokio::test]
    async fn convert_timezone_two_arg_uses_the_session_zone() {
        let got =
            row("SELECT convert_timezone('Europe/Brussels', TIMESTAMP'2022-03-23 00:00:00') AS x")
                .await;
        assert!(got.contains("2022-03-23T01:00:00"), "{got}");
    }

    #[tokio::test]
    async fn utc_shifts_round_trip() {
        for (q, want) in [
            (
                "SELECT from_utc_timestamp(TIMESTAMP'2024-01-15 12:00:00', 'America/New_York') AS x",
                "2024-01-15T07:00:00",
            ),
            // July: New York is on daylight time, so the offset is -4, not -5.
            (
                "SELECT from_utc_timestamp(TIMESTAMP'2024-07-15 12:00:00', 'America/New_York') AS x",
                "2024-07-15T08:00:00",
            ),
            (
                "SELECT to_utc_timestamp(TIMESTAMP'2024-01-15 07:00:00', 'America/New_York') AS x",
                "2024-01-15T12:00:00",
            ),
            // A zone with a half-hour offset, to catch a seconds/hours mix-up.
            (
                "SELECT from_utc_timestamp(TIMESTAMP'2024-01-15 12:00:00', 'Asia/Kolkata') AS x",
                "2024-01-15T17:30:00",
            ),
        ] {
            let got = row(q).await;
            assert!(got.contains(want), "{q} -> want {want}, got:\n{got}");
        }
    }

    /// DST edges, resolved the way `java.time.LocalDateTime.atZone` does.
    #[test]
    fn dst_edges_follow_java_atzone() {
        let la: Tz = "America/Los_Angeles".parse().unwrap();
        let micros = |s: &str| {
            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .unwrap()
                .and_utc()
                .timestamp_micros()
        };
        let hours = |m: i64| m / 3_600_000_000;

        // Spring forward: 2024-03-10 02:30 never happened. Java shifts it to 03:30 -07:00 = 10:30Z,
        // i.e. the pre-transition (-8) offset applied to the literal local time.
        let got = zone_to_utc(micros("2024-03-10 02:30:00"), la).unwrap();
        assert_eq!(hours(got - micros("2024-03-10 00:00:00")), 10);

        // Fall back: 2024-11-03 01:30 happens twice. Java keeps the earlier offset (-7) = 08:30Z.
        let got = zone_to_utc(micros("2024-11-03 01:30:00"), la).unwrap();
        assert_eq!(hours(got - micros("2024-11-03 00:00:00")), 8);
    }

    #[tokio::test]
    async fn unknown_zone_is_an_error_not_a_null() {
        let engine = crate::Engine::new();
        let err = engine
            .sql("SELECT from_utc_timestamp(TIMESTAMP'2024-01-15 12:00:00', 'Mars/Olympus') AS x")
            .await
            .expect_err("unknown zone must error");
        assert!(format!("{err}").contains("INVALID_TIMEZONE"), "{err}");
    }
}
