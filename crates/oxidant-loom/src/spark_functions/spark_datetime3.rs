//! A third wave of Spark datetime *scalar* functions that DataFusion does not provide (or provides
//! under different/timezone-aware semantics). Implemented faithfully against Apache Spark v4.0.0,
//! cross-checked byte-for-byte against the `oxidant-spark-compat` goldens
//! (`{timestamp,timestamp-ntz,date,datetime-special,try_datetime_functions}.sql.out`).
//!
//! oxidant's session timezone is UTC and oxidant/DataFusion-54 treats a bare `TIMESTAMP` as
//! timezone-NAIVE. We therefore build *timezone-naive* (`Timestamp(Microsecond, None)`) values and
//! perform all calendar math in UTC. The timezone-aware (LTZ) variants
//! (`make_timestamp_ltz`, `to_timestamp_ltz`, `convert_timezone`, `localtimestamp`) are intentionally
//! NOT implemented here — under a UTC-naive timestamp they cannot reproduce Spark's local-zone
//! rendering and would emit wrong answers.
//!
//! Functions:
//! - `make_timestamp(year, month, day, hour, min, sec)` — build a naive timestamp. `sec` is a
//!   fractional double; the whole value `60` rolls over to the next minute (`:00`), but `60.xxx`
//!   with a nonzero fraction is `INVALID_FRACTION_OF_SECOND` and any other out-of-range component is
//!   `DATETIME_FIELD_OUT_OF_BOUNDS` — both surfaced as errors (Spark 4.0 ANSI default), matching the
//!   golden. A NULL in any argument yields NULL.
//! - `make_timestamp_ntz(...)` — identical to `make_timestamp` (both are naive here); exactly 6 args.
//! - `to_timestamp_ntz(str [, fmt])` — parse a string/date/timestamp to a naive timestamp; NULL on a
//!   parse mismatch (non-ANSI).
//! - `try_to_timestamp(str [, fmt])` — like `to_timestamp` but never errors: NULL on failure.
//! - `unix_seconds(ts)` / `unix_millis(ts)` / `unix_micros(ts)` — epoch seconds / millis / micros
//!   as `bigint` (floor toward -inf).
//! - `unix_date(date)` — days since 1970-01-01 as `int`.
//! - `date_from_unix_date(int)` — `date` from days-since-epoch.
//! - `date_add(date, numDays)` / `dateadd(date, numDays)` — `date` plus N days (DataFusion-54 has
//!   no builtin; verified absent). `dateadd` is Databricks' spelling of the same function.
//!
//! Dropped: `timestampdiff(unit, ...)` is NOT reachable as a UDF. Spark's grammar special-cases the
//! bare `unit` keyword, but DataFusion/sqlparser (Databricks dialect) parses `timestampdiff(MONTH,
//! ...)` with `MONTH` as a *column reference*, so planning fails with "No field named month" before
//! any UDF runs; the quoted form `timestampdiff('MONTH', ...)` is itself a Spark ParseException
//! (`INVALID_PARAMETER_VALUE.DATETIME_UNIT`). A faithful implementation needs a parser/AST change,
//! which is out of scope for a UDF-only file, so it is dropped here.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Date32Array, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{exec_err, DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;

/// Register all third-wave datetime Spark functions into `ctx`.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(MakeTimestamp::new("make_timestamp")));
    ctx.register_udf(ScalarUDF::from(MakeTimestamp::new("make_timestamp_ntz")));
    ctx.register_udf(ScalarUDF::from(ToTimestampNtz::new(
        "to_timestamp_ntz",
        false,
    )));
    ctx.register_udf(ScalarUDF::from(ToTimestampNtz::new(
        "try_to_timestamp",
        true,
    )));
    ctx.register_udf(ScalarUDF::from(UnixEpoch::new(EpochOut::Seconds)));
    ctx.register_udf(ScalarUDF::from(UnixEpoch::new(EpochOut::Millis)));
    ctx.register_udf(ScalarUDF::from(UnixEpoch::new(EpochOut::Micros)));
    ctx.register_udf(ScalarUDF::from(UnixDate::new()));
    ctx.register_udf(ScalarUDF::from(DateFromUnixDate::new()));
    ctx.register_udf(ScalarUDF::from(DateAdd::new("date_add", 1)));
    // Databricks spells the same two-argument function `dateadd`.
    ctx.register_udf(ScalarUDF::from(DateAdd::new("dateadd", 1)));
    ctx.register_udf(ScalarUDF::from(DateAdd::new("date_sub", -1)));
}

fn arrow_err(e: datafusion::arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(e), None)
}

// ---------------------------------------------------------------------------
// Civil <-> epoch arithmetic (UTC, proleptic Gregorian). Self-contained: Howard
// Hinnant's algorithms, valid across the full proleptic Gregorian range.
// ---------------------------------------------------------------------------

pub(super) const MICROS_PER_DAY: i64 = 86_400_000_000;
const MICROS_PER_SEC: i64 = 1_000_000;

/// (year, month, day) -> days since 1970-01-01.
pub(super) fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) as i64 + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// days since 1970-01-01 -> (year, month, day).
pub(super) fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Whether `(y, m, d)` is a valid proleptic-Gregorian calendar date.
pub(super) fn is_valid_ymd(y: i64, m: u32, d: u32) -> bool {
    if !(1..=12).contains(&m) || d < 1 {
        return false;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let mdays = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    d <= mdays[(m - 1) as usize]
}

// ---------------------------------------------------------------------------
// make_timestamp / make_timestamp_ntz
// ---------------------------------------------------------------------------

/// `make_timestamp(year, month, day, hour, min, sec)` (and the explicit-NTZ alias). Builds a
/// timezone-naive `Timestamp(Microsecond, None)`. Both names take exactly 6 arguments here (the
/// 7th timezone argument that LTS `make_timestamp` accepts is out of scope — oxidant is UTC-naive).
#[derive(Debug, PartialEq, Eq, Hash)]
struct MakeTimestamp {
    name: &'static str,
    signature: Signature,
}

impl MakeTimestamp {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            signature: Signature::any(6, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MakeTimestamp {
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
        // Integer components.
        let year = cast_i64(&args.args[0].clone().into_array(n)?)?;
        let month = cast_i64(&args.args[1].clone().into_array(n)?)?;
        let day = cast_i64(&args.args[2].clone().into_array(n)?)?;
        let hour = cast_i64(&args.args[3].clone().into_array(n)?)?;
        let min = cast_i64(&args.args[4].clone().into_array(n)?)?;
        // `sec` is a fractional double.
        let sec = cast_f64(&args.args[5].clone().into_array(n)?)?;

        let mut out = TimestampMicrosecondArray::builder(n);
        for i in 0..n {
            if year.is_null(i)
                || month.is_null(i)
                || day.is_null(i)
                || hour.is_null(i)
                || min.is_null(i)
                || sec.is_null(i)
            {
                out.append_null();
                continue;
            }
            let micros = self.build_micros(
                year.value(i),
                month.value(i),
                day.value(i),
                hour.value(i),
                min.value(i),
                sec.value(i),
            )?;
            out.append_value(micros);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

impl MakeTimestamp {
    /// Assemble epoch-micros from the components, or surface Spark's `SparkDateTimeException`
    /// (`DATETIME_FIELD_OUT_OF_BOUNDS` / `INVALID_FRACTION_OF_SECOND`) as a oxidant execution error.
    #[allow(clippy::too_many_arguments)]
    fn build_micros(
        &self,
        year: i64,
        month: i64,
        day: i64,
        hour: i64,
        min: i64,
        sec: f64,
    ) -> Result<i64> {
        // Field-range checks (Spark validates each field independently before assembly).
        if !(1..=12).contains(&month) {
            return self.field_oob("MonthOfYear", "1 - 12", month);
        }
        if !(1..=31).contains(&day) {
            return self.field_oob("DayOfMonth", "1 - 31", day);
        }
        if !(0..=23).contains(&hour) {
            return self.field_oob("HourOfDay", "0 - 23", hour);
        }
        if !(0..=59).contains(&min) {
            return self.field_oob("MinuteOfHour", "0 - 59", min);
        }
        if !is_valid_ymd(year, month as u32, day as u32) {
            return self.field_oob("DayOfMonth", "1 - 31", day);
        }

        // Seconds: a fractional double scaled to whole micros.
        if !sec.is_finite() {
            return exec_err!("{}: invalid value for SecondOfMinute: {sec}", self.name);
        }
        let total_micros_f = (sec * MICROS_PER_SEC as f64).round();
        let total_micros = total_micros_f as i64;
        let whole_sec = total_micros.div_euclid(MICROS_PER_SEC);
        let frac_micros = total_micros.rem_euclid(MICROS_PER_SEC);

        // Spark allows the exact value 60 (and 0..=59), rolling 60 over to the next minute's :00.
        // A value in [60, 61) with a nonzero fraction is INVALID_FRACTION_OF_SECOND; >= 61 is OOB.
        let (sec_field, minute_rollover) = if whole_sec == 60 {
            if frac_micros != 0 {
                return exec_err!(
                    "{}: invalid fraction of second (valid 0 - 59.999999): {sec}",
                    self.name
                );
            }
            (0i64, 1i64)
        } else if (0..=59).contains(&whole_sec) {
            (whole_sec, 0i64)
        } else {
            return self.field_oob("SecondOfMinute", "0 - 59", whole_sec);
        };

        let days = days_from_civil(year, month as u32, day as u32);
        let minute_total = min + minute_rollover;
        let secs_in_day = hour * 3600 + minute_total * 60 + sec_field;
        let micros = days * MICROS_PER_DAY + secs_in_day * MICROS_PER_SEC + frac_micros;
        Ok(micros)
    }

    fn field_oob(&self, field: &str, range: &str, value: i64) -> Result<i64> {
        exec_err!(
            "{}: DATETIME_FIELD_OUT_OF_BOUNDS: invalid value for {field} (valid values {range}): {value}",
            self.name
        )
    }
}

// ---------------------------------------------------------------------------
// to_timestamp_ntz / try_to_timestamp
// ---------------------------------------------------------------------------

/// `to_timestamp_ntz(str [, fmt])` and `try_to_timestamp(str [, fmt])`. Parse a string (or pass a
/// date/timestamp through) into a timezone-naive timestamp. A parse mismatch yields NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ToTimestampNtz {
    name: &'static str,
    /// `true` for `try_*` — never error (NULL on any failure). (Both behave identically here since
    /// the parse path already returns NULL on mismatch.)
    _try: bool,
    signature: Signature,
}

impl ToTimestampNtz {
    fn new(name: &'static str, is_try: bool) -> Self {
        Self {
            name,
            _try: is_try,
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ToTimestampNtz {
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
        let in_type = args.arg_fields[0].data_type().clone();
        let first = args.args[0].clone().into_array(n)?;

        // A temporal first argument (date / timestamp) converts directly to a naive timestamp.
        if matches!(
            in_type,
            DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64
        ) {
            let ts = datafusion::arrow::compute::cast(
                &first,
                &DataType::Timestamp(TimeUnit::Microsecond, None),
            )
            .map_err(arrow_err)?;
            return Ok(ColumnarValue::Array(ts));
        }

        // A bare numeric argument is treated as epoch seconds (Spark's `to_timestamp(1)` path).
        if matches!(
            in_type,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64
        ) {
            let secs = cast_i64(&first)?;
            let mut out = TimestampMicrosecondArray::builder(n);
            for i in 0..n {
                if secs.is_null(i) {
                    out.append_null();
                } else {
                    match secs.value(i).checked_mul(MICROS_PER_SEC) {
                        Some(m) => out.append_value(m),
                        None => out.append_null(),
                    }
                }
            }
            return Ok(ColumnarValue::Array(Arc::new(out.finish())));
        }

        // String path.
        let strs = datafusion::arrow::compute::cast(&first, &DataType::Utf8).map_err(arrow_err)?;
        let strs = strs.as_any().downcast_ref::<StringArray>().unwrap();

        let fmt_arr: Option<ArrayRef> = if args.args.len() >= 2 {
            let f = args.args[1].clone().into_array(n)?;
            Some(datafusion::arrow::compute::cast(&f, &DataType::Utf8).map_err(arrow_err)?)
        } else {
            None
        };
        let fmt_str = fmt_arr
            .as_ref()
            .map(|a| a.as_any().downcast_ref::<StringArray>().unwrap());

        let mut out = TimestampMicrosecondArray::builder(n);
        for i in 0..n {
            if strs.is_null(i) {
                out.append_null();
                continue;
            }
            let micros = match fmt_str {
                Some(a) => {
                    if a.is_null(i) {
                        out.append_null();
                        continue;
                    }
                    parse_with_pattern(strs.value(i), a.value(i))
                }
                None => parse_default(strs.value(i)),
            };
            match micros {
                Some(m) => out.append_value(m),
                None => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Parse a default-format Spark timestamp string (`yyyy-MM-dd[ HH:mm:ss[.ffffff]]`, also accepting a
/// bare date or a `T` separator) into epoch micros (UTC). NULL (`None`) on any mismatch.
fn parse_default(s: &str) -> Option<i64> {
    let s = s.trim();
    // Split date and optional time on a space or 'T'.
    let (date_part, time_part) = match s.find([' ', 'T']) {
        Some(idx) => (&s[..idx], Some(s[idx + 1..].trim())),
        None => (s, None),
    };
    let mut dp = date_part.splitn(3, '-');
    let y: i64 = dp.next()?.parse().ok()?;
    let mo: u32 = dp.next()?.parse().ok()?;
    let d: u32 = dp.next()?.parse().ok()?;
    if dp.next().is_some() {
        return None;
    }
    if !is_valid_ymd(y, mo, d) {
        return None;
    }
    let (mut h, mut mi, mut se, mut micros) = (0u32, 0u32, 0u32, 0i64);
    if let Some(tp) = time_part {
        if !tp.is_empty() {
            // Strip a trailing 'Z' (UTC marker); reject any other zone designator under UTC-naive.
            let tp = tp.strip_suffix('Z').unwrap_or(tp);
            let (hms, frac) = match tp.split_once('.') {
                Some((a, b)) => (a, Some(b)),
                None => (tp, None),
            };
            let mut t = hms.splitn(3, ':');
            h = t.next()?.parse().ok()?;
            mi = t.next()?.parse().ok()?;
            se = match t.next() {
                Some(x) => x.parse().ok()?,
                None => 0,
            };
            if t.next().is_some() {
                return None;
            }
            if let Some(f) = frac {
                if !f.chars().all(|c| c.is_ascii_digit()) || f.is_empty() {
                    return None;
                }
                let mut frac6 = f.to_string();
                frac6.truncate(6);
                while frac6.len() < 6 {
                    frac6.push('0');
                }
                micros = frac6.parse().ok()?;
            }
        }
    }
    if h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let days = days_from_civil(y, mo, d);
    Some(
        days * MICROS_PER_DAY
            + (h as i64 * 3600 + mi as i64 * 60 + se as i64) * MICROS_PER_SEC
            + micros,
    )
}

/// The java.time pattern letters [`parse_with_pattern`] understands (numeric fields only). Any other
/// alphabetic letter (text day-of-week `E`, zone `z`, era `G`, week-of-year `w`, …) is unsupported
/// and forces a NULL result, matching the corpus (text-field patterns return NULL here, not a wrong
/// value).
fn parse_field_supported(letter: char) -> bool {
    matches!(
        letter,
        'y' | 'u' | 'M' | 'd' | 'D' | 'H' | 'h' | 'k' | 'K' | 'm' | 's' | 'S' | 'a'
    )
}

/// Parse `input` against the java.time pattern `fmt` into epoch micros (UTC). Returns `None` on any
/// mismatch *or* on an unsupported pattern field. Supports an optional-section `[...]` (java.time
/// optional block) by trying with each bracketed run both present and absent.
fn parse_with_pattern(input: &str, fmt: &str) -> Option<i64> {
    // Expand a single optional `[...]` section into two candidate formats (present / absent). Spark
    // patterns in the corpus use at most one trailing optional block (`[zzz]`).
    if let Some(open) = fmt.find('[') {
        if let Some(close) = fmt[open..].find(']') {
            let close = open + close;
            let with = format!("{}{}", &fmt[..open], &fmt[open + 1..close]) + &fmt[close + 1..];
            let without = format!("{}{}", &fmt[..open], &fmt[close + 1..]);
            return parse_with_pattern(input, &with)
                .or_else(|| parse_with_pattern(input, &without));
        }
    }
    parse_fixed_pattern(input, fmt)
}

/// Parse against a pattern with no optional sections.
fn parse_fixed_pattern(input: &str, fmt: &str) -> Option<i64> {
    let fchars: Vec<char> = fmt.chars().collect();
    let ichars: Vec<char> = input.chars().collect();
    let mut fi = 0;
    let mut ii = 0;

    let mut year: i64 = 1970;
    let mut month: u32 = 1;
    let mut day: u32 = 1;
    let mut hour: u32 = 0;
    let mut minute: u32 = 0;
    let mut second: u32 = 0;
    let mut micros: i64 = 0;
    let mut pm_flag: Option<bool> = None;
    let mut h12 = false;

    while fi < fchars.len() {
        let ch = fchars[fi];
        if ch == '\'' {
            fi += 1;
            if fi < fchars.len() && fchars[fi] == '\'' {
                if ii >= ichars.len() || ichars[ii] != '\'' {
                    return None;
                }
                ii += 1;
                fi += 1;
                continue;
            }
            while fi < fchars.len() && fchars[fi] != '\'' {
                if ii >= ichars.len() || ichars[ii] != fchars[fi] {
                    return None;
                }
                ii += 1;
                fi += 1;
            }
            fi += 1;
            continue;
        }
        if !ch.is_ascii_alphabetic() {
            if ii >= ichars.len() || ichars[ii] != ch {
                return None;
            }
            ii += 1;
            fi += 1;
            continue;
        }
        let mut count = 1;
        while fi + count < fchars.len() && fchars[fi + count] == ch {
            count += 1;
        }
        fi += count;

        if !parse_field_supported(ch) {
            return None;
        }

        if ch == 'a' {
            let rest: String = ichars[ii..].iter().collect();
            let up = rest.to_ascii_uppercase();
            if up.starts_with("AM") {
                pm_flag = Some(false);
                ii += 2;
            } else if up.starts_with("PM") {
                pm_flag = Some(true);
                ii += 2;
            } else {
                return None;
            }
            continue;
        }

        let max_digits = match ch {
            'y' | 'u' => {
                if count == 2 {
                    2
                } else {
                    7
                }
            }
            'S' => 9,
            _ => count.max(2),
        };
        let start = ii;
        while ii < ichars.len() && ichars[ii].is_ascii_digit() && (ii - start) < max_digits {
            ii += 1;
        }
        if ii == start {
            return None;
        }
        let digits: String = ichars[start..ii].iter().collect();
        let val: i64 = digits.parse().ok()?;
        match ch {
            'y' | 'u' => year = if count == 2 { 2000 + val } else { val },
            'M' => month = val as u32,
            'd' => day = val as u32,
            'D' => {
                let base = days_from_civil(year, 1, 1) + (val - 1);
                let (y, m, dd) = civil_from_days(base);
                year = y;
                month = m;
                day = dd;
            }
            'H' => hour = val as u32,
            'k' => hour = if val == 24 { 0 } else { val as u32 },
            'h' | 'K' => {
                h12 = true;
                hour = (val % 12) as u32;
            }
            'm' => minute = val as u32,
            's' => second = val as u32,
            'S' => {
                let mut frac = digits.clone();
                while frac.len() < 6 {
                    frac.push('0');
                }
                micros = frac[..6].parse().ok()?;
            }
            _ => return None,
        }
    }
    if ii != ichars.len() {
        return None;
    }
    if h12 {
        if let Some(true) = pm_flag {
            hour += 12;
        }
    }
    if !is_valid_ymd(year, month, day) || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let days = days_from_civil(year, month, day);
    Some(
        days * MICROS_PER_DAY
            + (hour as i64 * 3600 + minute as i64 * 60 + second as i64) * MICROS_PER_SEC
            + micros,
    )
}

// ---------------------------------------------------------------------------
// unix_seconds / unix_millis
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum EpochOut {
    Seconds,
    Millis,
    Micros,
}

/// `unix_seconds(ts)` / `unix_millis(ts)` / `unix_micros(ts)` — epoch seconds / millis / micros
/// from a timestamp, as `bigint`. Division floors toward -inf so negative (pre-epoch) instants are
/// handled like Spark (`unix_micros` divides by 1, so it is the raw micros).
#[derive(Debug, PartialEq, Eq, Hash)]
struct UnixEpoch {
    out: EpochOut,
    signature: Signature,
}

impl UnixEpoch {
    fn new(out: EpochOut) -> Self {
        Self {
            out,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for UnixEpoch {
    fn name(&self) -> &str {
        match self.out {
            EpochOut::Seconds => "unix_seconds",
            EpochOut::Millis => "unix_millis",
            EpochOut::Micros => "unix_micros",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let arr = args.args[0].clone().into_array(n)?;
        // Cast (any unit / tz) timestamp to micros, then reinterpret as i64 micros.
        let ts = datafusion::arrow::compute::cast(
            &arr,
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .map_err(arrow_err)?;
        let micros = datafusion::arrow::compute::cast(&ts, &DataType::Int64).map_err(arrow_err)?;
        let micros = micros.as_any().downcast_ref::<Int64Array>().unwrap();
        let divisor = match self.out {
            EpochOut::Seconds => 1_000_000,
            EpochOut::Millis => 1_000,
            EpochOut::Micros => 1,
        };
        let mut out = Int64Array::builder(n);
        for i in 0..n {
            if micros.is_null(i) {
                out.append_null();
            } else {
                out.append_value(micros.value(i).div_euclid(divisor));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// unix_date / date_from_unix_date
// ---------------------------------------------------------------------------

/// `unix_date(date)` — days since 1970-01-01 as `int`. A `Date32` is already stored as exactly this,
/// so the value is the underlying days count.
#[derive(Debug, PartialEq, Eq, Hash)]
struct UnixDate {
    signature: Signature,
}

impl UnixDate {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for UnixDate {
    fn name(&self) -> &str {
        "unix_date"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let arr = args.args[0].clone().into_array(n)?;
        let dates = datafusion::arrow::compute::cast(&arr, &DataType::Date32).map_err(arrow_err)?;
        let dates = dates.as_any().downcast_ref::<Date32Array>().unwrap();
        // Date32 native days == the integer value we want.
        let out: Int32Array = dates
            .reinterpret_cast::<datafusion::arrow::datatypes::Int32Type>()
            .clone();
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

/// `date_from_unix_date(int)` — a `date` from days-since-epoch.
#[derive(Debug, PartialEq, Eq, Hash)]
struct DateFromUnixDate {
    signature: Signature,
}

impl DateFromUnixDate {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for DateFromUnixDate {
    fn name(&self) -> &str {
        "date_from_unix_date"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let arr = args.args[0].clone().into_array(n)?;
        let days = datafusion::arrow::compute::cast(&arr, &DataType::Int32).map_err(arrow_err)?;
        let days = days.as_any().downcast_ref::<Int32Array>().unwrap();
        // Reinterpret the i32 days as Date32 (same native representation).
        let out: Date32Array = days
            .reinterpret_cast::<datafusion::arrow::datatypes::Date32Type>()
            .clone();
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

// ---------------------------------------------------------------------------
// date_add (2-arg: date + numDays)
// ---------------------------------------------------------------------------

/// `date_add(startDate, numDays)` / `date_sub(startDate, numDays)` — `startDate` plus/minus
/// `numDays` days, returned as `date`. DataFusion-54 ships no `date_add`/`date_sub`/`dateadd`
/// builtin, so we provide them (`sign = +1` for add, `-1` for sub — `date_sub(d, n) = d - n`, and a
/// negative `n` adds). The first argument is cast to `date` (string/timestamp accepted in non-ANSI
/// mode); the day count is cast to `int`. A NULL in either argument yields NULL. (Only the
/// 2-argument form is implemented — the 3-argument `date_add(unit, num, ts)` form has distinct unit
/// semantics and is left unregistered rather than answered wrongly.)
#[derive(Debug, PartialEq, Eq, Hash)]
struct DateAdd {
    signature: Signature,
    name: &'static str,
    sign: i32,
}

impl DateAdd {
    fn new(name: &'static str, sign: i32) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            name,
            sign,
        }
    }
}

impl ScalarUDFImpl for DateAdd {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        // Spark's `date_add`/`date_sub` accept an INT day count and implicitly coerce narrower ints
        // and strings to it, but a `bigint`/`double`/`decimal` is a type error, NOT an implicit cast
        // — Spark *rejects* `date_sub('2011-11-11', 1L)` and `'… 1.0'` (verified vs date.sql.out:
        // AnalysisException), while accepting `'1'` (→ 1) and `null` (→ NULL). Reject exactly those
        // wider-numeric types so oxidant doesn't silently accept a query Spark refuses. (A bare `1`
        // reaches here as `int` — the Spark integer-literal default — so valid calls are unaffected.)
        let day_ty = args.args[1].data_type();
        if matches!(
            day_ty,
            DataType::Int64
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
        ) {
            return exec_err!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] the second argument of `{}` requires \
                 the int type, however '{day_ty}' has the type that does not match",
                self.name
            );
        }
        // Strict cast of the date operand: an invalid date string raises CAST_INVALID_INPUT in Spark
        // (not NULL), so surface a cast failure as an error.
        let date_in = args.args[0].clone().into_array(n)?;
        let cast_opts = datafusion::arrow::compute::CastOptions {
            safe: false,
            format_options: Default::default(),
        };
        let dates =
            datafusion::arrow::compute::cast_with_options(&date_in, &DataType::Date32, &cast_opts)
                .map_err(arrow_err)?;
        let dates = dates.as_any().downcast_ref::<Date32Array>().unwrap();

        // numDays: cast strictly to Int32 (string '1.2' must error, matching Spark CAST_INVALID_INPUT).
        let days_in = args.args[1].clone().into_array(n)?;
        let days =
            datafusion::arrow::compute::cast_with_options(&days_in, &DataType::Int32, &cast_opts)
                .map_err(arrow_err)?;
        let days = days.as_any().downcast_ref::<Int32Array>().unwrap();

        let mut out = Date32Array::builder(n);
        for i in 0..n {
            if dates.is_null(i) || days.is_null(i) {
                out.append_null();
            } else {
                out.append_value(dates.value(i) + self.sign * days.value(i));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// shared numeric casts
// ---------------------------------------------------------------------------

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

#[cfg(test)]
mod tests {
    use crate::Engine;

    /// Run `q` and return the single scalar cell as a string, mapping NULL to "NULL".
    async fn cell(q: &str) -> String {
        use datafusion::arrow::array::Array;
        let engine = Engine::new();
        let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        let col = batches[0].column(0);
        if col.is_null(0) {
            return "NULL".to_string();
        }
        let txt = crate::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string();
        txt.lines()
            .nth(3)
            .unwrap()
            .trim_matches(|c| c == '|' || c == ' ')
            .to_string()
    }

    #[tokio::test]
    async fn make_timestamp_basic() {
        assert_eq!(
            cell("SELECT make_timestamp(2021, 7, 11, 6, 30, CAST(45.678 AS DOUBLE)) AS x").await,
            "2021-07-11T06:30:45.678"
        );
        assert_eq!(
            cell("SELECT make_timestamp(1, 1, 1, 1, 1, CAST(1 AS DOUBLE)) AS x").await,
            "0001-01-01T01:01:01"
        );
        // Whole 60 rolls over to the next minute's :00.
        assert_eq!(
            cell("SELECT make_timestamp(1, 1, 1, 1, 1, CAST(60 AS DOUBLE)) AS x").await,
            "0001-01-01T01:02:00"
        );
        // 59.999999 keeps its fraction.
        assert_eq!(
            cell("SELECT make_timestamp(1, 1, 1, 1, 1, CAST(59.999999 AS DOUBLE)) AS x").await,
            "0001-01-01T01:01:59.999999"
        );
        // NULL component -> NULL.
        assert_eq!(
            cell("SELECT make_timestamp(1, 1, 1, 1, 1, CAST(NULL AS DOUBLE)) AS x").await,
            "NULL"
        );
    }

    #[tokio::test]
    async fn make_timestamp_ntz_alias() {
        assert_eq!(
            cell("SELECT make_timestamp_ntz(2021, 7, 11, 6, 30, CAST(45.678 AS DOUBLE)) AS x")
                .await,
            "2021-07-11T06:30:45.678"
        );
    }

    #[tokio::test]
    async fn make_timestamp_invalid_components_error() {
        let engine = Engine::new();
        // 60.007 -> INVALID_FRACTION_OF_SECOND.
        assert!(engine
            .sql("SELECT make_timestamp(2021, 7, 11, 6, 30, CAST(60.007 AS DOUBLE))")
            .await
            .is_err());
        // 61 -> out of bounds.
        assert!(engine
            .sql("SELECT make_timestamp(1, 1, 1, 1, 1, CAST(61 AS DOUBLE))")
            .await
            .is_err());
        // Month 13 -> out of bounds.
        assert!(engine
            .sql("SELECT make_timestamp(2021, 13, 1, 0, 0, CAST(0 AS DOUBLE))")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn to_timestamp_ntz_parses() {
        assert_eq!(
            cell("SELECT to_timestamp_ntz('2016-12-31 00:12:00') AS x").await,
            "2016-12-31T00:12:00"
        );
        assert_eq!(
            cell("SELECT to_timestamp_ntz('2016-12-31', 'yyyy-MM-dd') AS x").await,
            "2016-12-31T00:00:00"
        );
        assert_eq!(
            cell("SELECT to_timestamp_ntz(CAST(NULL AS STRING)) AS x").await,
            "NULL"
        );
        // Bare date input.
        assert_eq!(
            cell("SELECT to_timestamp_ntz(DATE '2016-12-31') AS x").await,
            "2016-12-31T00:00:00"
        );
    }

    #[tokio::test]
    async fn try_to_timestamp_null_on_failure() {
        assert_eq!(
            cell("SELECT try_to_timestamp('2016-12-31 00:12:00') AS x").await,
            "2016-12-31T00:12:00"
        );
        // Garbage -> NULL.
        assert_eq!(
            cell("SELECT try_to_timestamp('2016-12-31 abc') AS x").await,
            "NULL"
        );
        // 02-29 with no year is invalid (year defaults 1970, not a leap year) -> NULL.
        assert_eq!(
            cell("SELECT try_to_timestamp('02-29', 'MM-dd') AS x").await,
            "NULL"
        );
        // Optional-section pattern: the input lacks the optional zone, and the trailing '.' has no
        // fraction digits -> NULL.
        assert_eq!(
            cell("SELECT try_to_timestamp('2019-10-06 10:11:12.', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]') AS x")
                .await,
            "NULL"
        );
    }

    #[tokio::test]
    async fn unix_seconds_and_millis() {
        // 2020-12-01 14:30:08 UTC = 1606833008.
        assert_eq!(
            cell("SELECT unix_seconds(timestamp '2020-12-01 14:30:08') AS x").await,
            "1606833008"
        );
        // Sub-second floors toward -inf (truncates here since positive).
        assert_eq!(
            cell("SELECT unix_seconds(timestamp '2020-12-01 14:30:08.999999') AS x").await,
            "1606833008"
        );
        assert_eq!(
            cell("SELECT unix_millis(timestamp '2020-12-01 14:30:08.999999') AS x").await,
            "1606833008999"
        );
        assert_eq!(
            cell("SELECT unix_seconds(CAST(NULL AS TIMESTAMP)) AS x").await,
            "NULL"
        );
    }

    #[tokio::test]
    async fn unix_date_and_back() {
        assert_eq!(cell("SELECT unix_date(DATE '1970-01-01') AS x").await, "0");
        assert_eq!(
            cell("SELECT unix_date(DATE '2020-12-04') AS x").await,
            "18600"
        );
        assert_eq!(
            cell("SELECT unix_date(CAST(NULL AS DATE)) AS x").await,
            "NULL"
        );
        assert_eq!(
            cell("SELECT date_from_unix_date(0) AS x").await,
            "1970-01-01"
        );
        assert_eq!(
            cell("SELECT date_from_unix_date(1000) AS x").await,
            "1972-09-27"
        );
        assert_eq!(
            cell("SELECT date_from_unix_date(CAST(NULL AS INT)) AS x").await,
            "NULL"
        );
    }

    #[tokio::test]
    async fn date_add_basic() {
        assert_eq!(
            cell("SELECT date_add(DATE '2011-11-11', 1) AS x").await,
            "2011-11-12"
        );
        // String date operand.
        assert_eq!(
            cell("SELECT date_add('2011-11-11', 1) AS x").await,
            "2011-11-12"
        );
        // String day count.
        assert_eq!(
            cell("SELECT date_add('2011-11-11', '1') AS x").await,
            "2011-11-12"
        );
        // NULL passthrough.
        assert_eq!(
            cell("SELECT date_add(CAST(NULL AS DATE), 1) AS x").await,
            "NULL"
        );
        assert_eq!(
            cell("SELECT date_add(DATE '2011-11-11', CAST(NULL AS INT)) AS x").await,
            "NULL"
        );
    }

    #[tokio::test]
    async fn date_add_invalid_inputs_error() {
        let engine = Engine::new();
        // Non-numeric day-count string -> CAST_INVALID_INPUT (error, not NULL).
        assert!(engine
            .sql("SELECT date_add('2011-11-11', '1.2')")
            .await
            .is_err());
    }
}
