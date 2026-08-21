//! More Spark datetime *scalar converters* between epoch numbers, formatted strings and temporal
//! values. These complement `spark_datetime.rs` (which owns `timestamp_seconds/millis/micros` and
//! `next_day`) and intentionally do **not** duplicate any function it registers.
//!
//! Implemented faithfully against Apache Spark v4.0.0 semantics. oxidant's session timezone is UTC, so
//! every conversion here is performed in UTC (Spark performs them in the session timezone; the two
//! agree whenever that timezone is UTC, which is oxidant's default).
//!
//! - `from_unixtime(sec [, fmt])` — render an epoch-seconds `bigint` as a formatted **string**.
//!   Spark's default format is `yyyy-MM-dd HH:mm:ss`. NULL in either argument yields NULL. (Note:
//!   DataFusion ships a `from_unixtime` that returns a *timestamp*; Spark returns a string, so we
//!   override it.)
//!
//! - `unix_timestamp([str [, fmt]])` / `to_unix_timestamp(str [, fmt])` — parse a date/time string
//!   into epoch **seconds** (`bigint`). The default format is `yyyy-MM-dd HH:mm:ss`. A string that
//!   does not match the format yields NULL (Spark's non-ANSI `unix_timestamp` returns NULL on
//!   unparseable input; `to_unix_timestamp` shares this). A timestamp/date argument is converted
//!   directly. `unix_timestamp()` with no argument is the current epoch second.
//!
//! - `date_format(ts/date/str, fmt)` — format a temporal value with Spark's java.time-style pattern
//!   (`yyyy MM dd HH mm ss`, etc.). DataFusion's built-in `date_format` interprets *chrono*
//!   `strftime` patterns (`%Y`), which silently mis-renders Spark patterns, so we override it.
//!
//! Pattern support is the faithful subset that does not depend on a locale or a non-UTC session
//! timezone: era-independent numeric/­text fields `y M d H h m s S a E D` (plus `K k` hour variants
//! and `G` era). Fields whose Spark rendering is locale- or zone-sensitive in a way oxidant cannot
//! reproduce under a UTC session (`z Z X x V O`, week-of-year `w W`, quarter localized text) are
//! **not** translated and are deferred — a pattern containing one is rejected with an error rather
//! than emitting a wrong value.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int64Array, StringArray, StringBuilder};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;

/// Register the epoch/format datetime converters into `ctx`.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(FromUnixtime::new()));
    ctx.register_udf(ScalarUDF::from(UnixTimestamp::new("unix_timestamp")));
    ctx.register_udf(ScalarUDF::from(UnixTimestamp::new("to_unix_timestamp")));
    ctx.register_udf(ScalarUDF::from(DateFormat::new()));
}

fn arrow_err(e: datafusion::arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(e), None)
}

const DEFAULT_FORMAT: &str = "yyyy-MM-dd HH:mm:ss";

// ---------------------------------------------------------------------------
// Civil <-> epoch arithmetic (UTC, proleptic Gregorian), no external crate.
// ---------------------------------------------------------------------------

/// A broken-down UTC date/time. `micros` is the sub-second part in microseconds (0..1_000_000).
#[derive(Debug, Clone, Copy)]
struct Civil {
    year: i64,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    micros: u32,
    /// Day-of-week, 0 = Monday … 6 = Sunday (ISO).
    weekday: u32,
    /// 1-based day of the year.
    day_of_year: u32,
}

/// Convert a count of days since 1970-01-01 to (year, month, day) using Howard Hinnant's
/// civil-from-days algorithm (valid across the full proleptic Gregorian range).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Inverse of [`civil_from_days`]: (year, month, day) -> days since 1970-01-01.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) as i64 + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Break an epoch-microseconds instant down into a UTC [`Civil`].
fn civil_from_micros(micros: i64) -> Civil {
    let micros_per_day: i64 = 86_400_000_000;
    // Floor-divide so negative instants map to the correct civil day.
    let days = micros.div_euclid(micros_per_day);
    let mut tod = micros.rem_euclid(micros_per_day); // micros into the day, [0, 86_400_000_000)
    let (year, month, day) = civil_from_days(days);

    let sub = (tod % 1_000_000) as u32;
    tod /= 1_000_000; // seconds into day
    let second = (tod % 60) as u32;
    tod /= 60;
    let minute = (tod % 60) as u32;
    tod /= 60;
    let hour = tod as u32;

    // ISO weekday: 1970-01-01 (day 0) is a Thursday => Monday-based index 3.
    let weekday = (days.rem_euclid(7) + 3).rem_euclid(7) as u32;
    // Day of year.
    let jan1 = days_from_civil(year, 1, 1);
    let day_of_year = (days - jan1 + 1) as u32;

    Civil {
        year,
        month,
        day,
        hour,
        minute,
        second,
        micros: sub,
        weekday,
        day_of_year,
    }
}

const MONTHS_SHORT: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];
const MONTHS_FULL: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];
/// Indexed by ISO weekday (0 = Monday … 6 = Sunday).
const DAYS_SHORT: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
const DAYS_FULL: [&str; 7] = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
];

// ---------------------------------------------------------------------------
// java.time pattern formatting (the faithful, zone-independent subset).
// ---------------------------------------------------------------------------

/// Render `c` per the java.time pattern `fmt`. Returns `Err` if `fmt` uses a field we deliberately
/// do not support under a UTC session (to avoid emitting a value that disagrees with Spark).
fn format_pattern(c: &Civil, fmt: &str) -> std::result::Result<String, String> {
    let chars: Vec<char> = fmt.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        let ch = chars[i];
        // A single-quoted run is a literal; '' is an escaped quote.
        if ch == '\'' {
            i += 1;
            if i < chars.len() && chars[i] == '\'' {
                out.push('\'');
                i += 1;
                continue;
            }
            while i < chars.len() && chars[i] != '\'' {
                out.push(chars[i]);
                i += 1;
            }
            i += 1; // consume the closing quote
            continue;
        }
        if !ch.is_ascii_alphabetic() {
            out.push(ch);
            i += 1;
            continue;
        }
        // Count the run length of this letter.
        let mut count = 1;
        while i + count < chars.len() && chars[i + count] == ch {
            count += 1;
        }
        emit_field(c, ch, count, &mut out)?;
        i += count;
    }
    Ok(out)
}

/// Emit one pattern field of `count` repetitions of `letter`.
fn emit_field(
    c: &Civil,
    letter: char,
    count: usize,
    out: &mut String,
) -> std::result::Result<(), String> {
    // Spark v4.0.0 (default timeParserPolicy) rejects over-length pattern fields for the
    // supported formatting letters, raising
    // INCONSISTENT_BEHAVIOR_CROSS_VERSION.DATETIME_PATTERN_RECOGNITION. Each cap below is the
    // largest run-length Spark accepts for that letter (taken byte-for-byte from the committed
    // VALID datetime-formatting golden); anything longer is a pattern Spark also rejects, so
    // erroring here cannot regress a currently-correct row. The bare 'u' letter (and any other
    // letter not listed) falls through to the existing error branches.
    let max_count = match letter {
        'G' => 4,
        'y' => 6,
        'M' => 4,
        'L' => 4,
        'E' => 4,
        'd' => 2,
        'D' => 3,
        'H' => 2,
        'h' => 2,
        'k' => 2,
        'K' => 2,
        'm' => 2,
        's' => 2,
        'S' => 9,
        'a' => 1,
        _ => usize::MAX,
    };
    if count > max_count {
        return Err(format!(
            "date_format: pattern field '{letter}' repeated {count} times exceeds Spark's \
             accepted maximum of {max_count} (DATETIME_PATTERN_RECOGNITION)"
        ));
    }
    match letter {
        // Era.
        'G' => out.push_str(if c.year > 0 { "AD" } else { "BC" }),
        // Year. `yy` => last two digits zero-padded; otherwise zero-pad to `count`.
        'y' => {
            let y = c.year.abs();
            if count == 2 {
                out.push_str(&format!("{:02}", (y.unsigned_abs() % 100)));
            } else {
                out.push_str(&format!("{:0width$}", y, width = count));
            }
        }
        // Month: 1-2 => numeric; 3 => short name; >=4 => full name.
        'M' | 'L' => {
            let idx = (c.month - 1) as usize;
            match count {
                1 => out.push_str(&c.month.to_string()),
                2 => out.push_str(&format!("{:02}", c.month)),
                3 => out.push_str(MONTHS_SHORT[idx]),
                _ => out.push_str(MONTHS_FULL[idx]),
            }
        }
        // Day of month.
        'd' => out.push_str(&format!("{:0width$}", c.day, width = count)),
        // Day of year.
        'D' => out.push_str(&format!("{:0width$}", c.day_of_year, width = count)),
        // Day-of-week name (text). 1-3 => short, >=4 => full.
        'E' => {
            let idx = c.weekday as usize;
            if count <= 3 {
                out.push_str(DAYS_SHORT[idx]);
            } else {
                out.push_str(DAYS_FULL[idx]);
            }
        }
        // Hour fields.
        'H' => out.push_str(&format!("{:0width$}", c.hour, width = count)), // 0-23
        'k' => {
            let h = if c.hour == 0 { 24 } else { c.hour }; // 1-24
            out.push_str(&format!("{:0width$}", h, width = count));
        }
        'h' => {
            let h = match c.hour % 12 {
                0 => 12,
                v => v,
            }; // 1-12
            out.push_str(&format!("{:0width$}", h, width = count));
        }
        'K' => out.push_str(&format!("{:0width$}", c.hour % 12, width = count)), // 0-11
        'm' => out.push_str(&format!("{:0width$}", c.minute, width = count)),
        's' => out.push_str(&format!("{:0width$}", c.second, width = count)),
        // Fraction of second: `count` leading digits of the micros (Spark caps at 9; we have 6).
        'S' => {
            // micros has 6 digits of resolution; pad to 9 then take `count`.
            let nanos = c.micros as u64 * 1000; // to nanoseconds (9 digits)
            let s = format!("{nanos:09}");
            let take = count.min(9);
            out.push_str(&s[..take]);
        }
        // AM/PM.
        'a' => out.push_str(if c.hour < 12 { "AM" } else { "PM" }),
        // Zone / offset / week-of-year / quarter-text: locale- or zone-sensitive. Defer.
        'z' | 'Z' | 'X' | 'x' | 'V' | 'O' | 'w' | 'W' | 'q' | 'Q' | 'F' | 'c' | 'e' => {
            return Err(format!(
                "date_format: pattern field '{letter}' is not supported under oxidant's UTC session"
            ));
        }
        other => {
            return Err(format!("date_format: unsupported pattern field '{other}'"));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// java.time pattern parsing (for unix_timestamp / to_unix_timestamp).
// ---------------------------------------------------------------------------

/// The java.time pattern letters [`parse_pattern`] can consume. Any *other* alphabetic letter in a
/// parse format (text month/day-of-week, zone, era, week-of-year, …) is unsupported; Spark itself
/// rejects several of these (e.g. narrow `EEEEE`) under the default time-parser policy, so we treat
/// an unsupported field as an error rather than silently returning NULL.
fn parse_field_supported(letter: char) -> bool {
    matches!(
        letter,
        'y' | 'u' | 'M' | 'd' | 'D' | 'H' | 'h' | 'k' | 'K' | 'm' | 's' | 'S' | 'a'
    )
}

/// Whether every pattern field in `fmt` is one [`parse_pattern`] can handle (literals are always
/// fine). Used to raise an error on an unsupported parse pattern instead of returning NULL.
fn parse_format_supported(fmt: &str) -> bool {
    let chars: Vec<char> = fmt.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let ch = chars[i];
        if ch == '\'' {
            // Skip a quoted literal run.
            i += 1;
            if i < chars.len() && chars[i] == '\'' {
                i += 1;
                continue;
            }
            while i < chars.len() && chars[i] != '\'' {
                i += 1;
            }
            i += 1;
            continue;
        }
        if ch.is_ascii_alphabetic() && !parse_field_supported(ch) {
            return false;
        }
        i += 1;
    }
    true
}

/// Parse `input` against the java.time pattern `fmt` into epoch microseconds (UTC). Returns `None`
/// on any input/format mismatch. Supports the numeric fields needed by the default format and the
/// corpus (`y u M d D H h k K m s S a`). The caller is expected to have already rejected formats
/// with unsupported fields via [`parse_format_supported`].
fn parse_pattern(input: &str, fmt: &str) -> Option<i64> {
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
    let mut micros: u32 = 0;
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
            // Literal separator: must match exactly.
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

        // 'a' (AM/PM) is alphabetic input.
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

        // All remaining supported fields are numeric. Read a digit run, bounded so the next literal
        // can still match.
        let max_digits = match ch {
            'y' | 'u' => {
                if count == 2 {
                    2
                } else {
                    7
                }
            }
            'S' => 9,
            'M' | 'd' | 'H' | 'h' | 'k' | 'K' | 'm' | 's' | 'D' => count.max(2),
            // Unsupported-for-parsing field (text month, zone, etc.).
            _ => return None,
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
            'y' | 'u' => {
                year = if count == 2 { 2000 + val } else { val };
            }
            'M' => month = val as u32,
            'd' => day = val as u32,
            'D' => {
                // day-of-year -> resolve to (year, month, day).
                let base = days_from_civil(year, 1, 1) + (val - 1);
                let (y, m, d) = civil_from_days(base);
                year = y;
                month = m;
                day = d;
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
                // Left-aligned fraction: scale the digits to microseconds.
                let mut frac = digits.clone();
                while frac.len() < 6 {
                    frac.push('0');
                }
                micros = frac[..6].parse().ok()?;
            }
            _ => return None,
        }
    }
    // Any unconsumed input is a mismatch.
    if ii != ichars.len() {
        return None;
    }
    if h12 {
        if let Some(true) = pm_flag {
            hour += 12;
        }
    }
    // Validate ranges (Spark's resolver is strict; reject obviously-invalid components).
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hour > 23 || minute > 59 {
        return None;
    }
    let days = days_from_civil(year, month, day);
    let secs = days * 86_400 + hour as i64 * 3600 + minute as i64 * 60 + second as i64;
    Some(secs * 1_000_000 + micros as i64)
}

// ---------------------------------------------------------------------------
// Shared: pull an epoch-micros column out of a temporal/string argument.
// ---------------------------------------------------------------------------

/// Cast `arr` to epoch microseconds (UTC). Accepts timestamps (any unit / tz), dates, and strings
/// (parsed via DataFusion's timestamp cast). The returned array is `Int64` micros with NULLs
/// preserved.
fn to_epoch_micros(arr: &ArrayRef) -> Result<Int64Array> {
    let ts =
        datafusion::arrow::compute::cast(arr, &DataType::Timestamp(TimeUnit::Microsecond, None))
            .map_err(arrow_err)?;
    // Reinterpret the timestamp's i64 micros as a plain Int64 column.
    let i64arr = datafusion::arrow::compute::cast(&ts, &DataType::Int64).map_err(arrow_err)?;
    Ok(i64arr
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("cast to Int64 yields Int64Array")
        .clone())
}

// ---------------------------------------------------------------------------
// from_unixtime
// ---------------------------------------------------------------------------

/// `from_unixtime(unixSeconds [, fmt])` — epoch-seconds bigint -> formatted UTC string.
#[derive(Debug, PartialEq, Eq, Hash)]
struct FromUnixtime {
    signature: Signature,
}

impl FromUnixtime {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for FromUnixtime {
    fn name(&self) -> &str {
        "from_unixtime"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let secs = args.args[0].clone().into_array(n)?;
        // Spark ANSI-casts the seconds argument to BIGINT; a non-numeric string ('aa',
        // '2018-01-01') raises CAST_INVALID_INPUT rather than yielding NULL. A safe
        // (NULL-on-failure) cast would silently mask that, so a string input uses a strict cast
        // while numeric inputs keep the infallible safe cast (their behaviour is unchanged).
        let secs = if matches!(
            args.arg_fields[0].data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            datafusion::arrow::compute::cast_with_options(
                &secs,
                &DataType::Int64,
                &datafusion::arrow::compute::CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )
            .map_err(arrow_err)?
        } else {
            datafusion::arrow::compute::cast(&secs, &DataType::Int64).map_err(arrow_err)?
        };
        let secs = secs.as_any().downcast_ref::<Int64Array>().unwrap();

        // Optional format column.
        let fmt_arr: Option<ArrayRef> = if args.args.len() >= 2 {
            let f = args.args[1].clone().into_array(n)?;
            Some(datafusion::arrow::compute::cast(&f, &DataType::Utf8).map_err(arrow_err)?)
        } else {
            None
        };
        let fmt_str = fmt_arr
            .as_ref()
            .map(|a| a.as_any().downcast_ref::<StringArray>().unwrap());

        let mut out = StringBuilder::new();
        for i in 0..n {
            if secs.is_null(i) {
                out.append_null();
                continue;
            }
            let fmt = match fmt_str {
                Some(a) => {
                    if a.is_null(i) {
                        out.append_null();
                        continue;
                    }
                    a.value(i)
                }
                None => DEFAULT_FORMAT,
            };
            let micros = match secs.value(i).checked_mul(1_000_000) {
                Some(m) => m,
                None => {
                    out.append_null();
                    continue;
                }
            };
            let c = civil_from_micros(micros);
            match format_pattern(&c, fmt) {
                Ok(s) => out.append_value(s),
                Err(e) => return exec_err!("{e}"),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// unix_timestamp / to_unix_timestamp
// ---------------------------------------------------------------------------

/// `unix_timestamp([str [, fmt]])` / `to_unix_timestamp(str [, fmt])` — parse to epoch seconds.
#[derive(Debug, PartialEq, Eq, Hash)]
struct UnixTimestamp {
    name: &'static str,
    signature: Signature,
}

impl UnixTimestamp {
    fn new(name: &'static str) -> Self {
        // Accept 0/1/2 args. `unix_timestamp()` (0 args) is "now"; Spark rejects 0-arg
        // `to_unix_timestamp` at analysis, but accepting it is harmless and keeps one impl.
        Self {
            name,
            signature: Signature::one_of(
                vec![
                    TypeSignature::Any(0),
                    TypeSignature::Any(1),
                    TypeSignature::Any(2),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for UnixTimestamp {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;

        // No argument: the current epoch second (constant for the call).
        if args.args.is_empty() {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            return Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(now))));
        }

        let in_type = args.arg_fields[0].data_type().clone();
        let first = args.args[0].clone().into_array(n)?;

        // A temporal first argument is converted directly to epoch seconds; a string is parsed with
        // the (optional) format.
        let is_temporal = matches!(
            in_type,
            DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64
        );

        if is_temporal {
            let micros = to_epoch_micros(&first)?;
            let mut out = Int64Array::builder(n);
            for i in 0..n {
                if micros.is_null(i) {
                    out.append_null();
                } else {
                    out.append_value(micros.value(i).div_euclid(1_000_000));
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

        let mut out = Int64Array::builder(n);
        for i in 0..n {
            if strs.is_null(i) {
                out.append_null();
                continue;
            }
            let fmt = match fmt_str {
                Some(a) => {
                    if a.is_null(i) {
                        out.append_null();
                        continue;
                    }
                    a.value(i)
                }
                None => DEFAULT_FORMAT,
            };
            // An unsupported pattern field (text day-of-week, zone, era, …) is an error in Spark
            // under the default time-parser policy — match that rather than emitting a wrong NULL.
            if !parse_format_supported(fmt) {
                return exec_err!("{}: unsupported datetime pattern `{fmt}`", self.name);
            }
            match parse_pattern(strs.value(i), fmt) {
                Some(micros) => out.append_value(micros.div_euclid(1_000_000)),
                // Spark 4.0 ANSI raises on an unparseable non-null input (1-arg:
                // CANNOT_PARSE_TIMESTAMP; 2-arg fmt mismatch: DATETIME_PATTERN_RECOGNITION)
                // rather than yielding NULL.
                None => {
                    return exec_err!(
                        "[CANNOT_PARSE_TIMESTAMP] could not parse '{}' for {}",
                        strs.value(i),
                        self.name
                    )
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// date_format
// ---------------------------------------------------------------------------

/// `date_format(temporal, fmt)` — render a date/timestamp/string with a Spark java.time pattern.
#[derive(Debug, PartialEq, Eq, Hash)]
struct DateFormat {
    signature: Signature,
}

impl DateFormat {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for DateFormat {
    fn name(&self) -> &str {
        "date_format"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let micros = to_epoch_micros(&args.args[0].clone().into_array(n)?)?;
        let fmt = args.args[1].clone().into_array(n)?;
        let fmt = datafusion::arrow::compute::cast(&fmt, &DataType::Utf8).map_err(arrow_err)?;
        let fmt = fmt.as_any().downcast_ref::<StringArray>().unwrap();

        let mut out = StringBuilder::new();
        for i in 0..n {
            if micros.is_null(i) || fmt.is_null(i) {
                out.append_null();
                continue;
            }
            let c = civil_from_micros(micros.value(i));
            match format_pattern(&c, fmt.value(i)) {
                Ok(s) => out.append_value(s),
                Err(e) => return exec_err!("{e}"),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
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
    async fn from_unixtime_default_and_custom() {
        assert_eq!(
            cell("SELECT from_unixtime(0) AS x").await,
            "1970-01-01 00:00:00"
        );
        // 1230219000 = 2008-12-25 15:30:00 UTC.
        assert_eq!(
            cell("SELECT from_unixtime(1230219000) AS x").await,
            "2008-12-25 15:30:00"
        );
        assert_eq!(
            cell("SELECT from_unixtime(1230219000, 'yyyy/MM/dd') AS x").await,
            "2008/12/25"
        );
        assert_eq!(
            cell("SELECT from_unixtime(CAST(NULL AS BIGINT)) AS x").await,
            "NULL"
        );
    }

    #[tokio::test]
    async fn from_unixtime_negative_epoch() {
        assert_eq!(
            cell("SELECT from_unixtime(CAST(-1 AS BIGINT)) AS x").await,
            "1969-12-31 23:59:59"
        );
    }

    #[tokio::test]
    async fn unix_timestamp_roundtrip() {
        assert_eq!(
            cell("SELECT unix_timestamp('2008-12-25 15:30:00') AS x").await,
            "1230219000"
        );
        assert_eq!(
            cell("SELECT unix_timestamp('1970-01-01 00:00:00') AS x").await,
            "0"
        );
        assert_eq!(
            cell("SELECT unix_timestamp('2008/12/25', 'yyyy/MM/dd') AS x").await,
            "1230163200"
        );
        assert_eq!(
            cell("SELECT to_unix_timestamp('2008-12-25 15:30:00') AS x").await,
            "1230219000"
        );
        // Spark 4.0 ANSI raises on an unparseable input rather than yielding NULL.
        let engine = Engine::new();
        assert!(engine
            .sql("SELECT unix_timestamp('not a date') AS x")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn unix_timestamp_on_timestamp_value() {
        assert_eq!(
            cell("SELECT unix_timestamp(timestamp '2008-12-25 15:30:00') AS x").await,
            "1230219000"
        );
        // A bare DATE -> midnight UTC epoch.
        assert_eq!(
            cell("SELECT unix_timestamp(DATE '1970-01-02') AS x").await,
            "86400"
        );
    }

    #[tokio::test]
    async fn date_format_patterns() {
        assert_eq!(
            cell("SELECT date_format(timestamp '2018-11-17 13:33:33', 'yyyy MM dd HH mm ss') AS x")
                .await,
            "2018 11 17 13 33 33"
        );
        // 12-hour + AM/PM.
        assert_eq!(
            cell("SELECT date_format(timestamp '2018-11-17 13:33:33', 'hh:mm a') AS x").await,
            "01:33 PM"
        );
        // Month/day names.
        assert_eq!(
            cell("SELECT date_format(timestamp '2018-11-17 13:33:33', 'MMM EEE') AS x").await,
            "Nov Sat"
        );
        assert_eq!(
            cell("SELECT date_format(timestamp '2018-11-17 13:33:33', 'MMMM EEEE') AS x").await,
            "November Saturday"
        );
        // A DATE input.
        assert_eq!(
            cell("SELECT date_format(DATE '2018-11-17', 'yyyy-MM-dd') AS x").await,
            "2018-11-17"
        );
        // Fraction-of-second.
        assert_eq!(
            cell("SELECT date_format(timestamp '2018-11-17 13:33:33.123', 'SSS') AS x").await,
            "123"
        );
    }

    #[tokio::test]
    async fn date_format_literal_quoting() {
        // A single-quoted run inside the java.time pattern is a literal. In SQL the surrounding
        // single quotes are escaped by doubling, so the format string is `yyyy'T'HH`.
        assert_eq!(
            cell("SELECT date_format(timestamp '2018-11-17 13:33:33', 'yyyy''T''HH') AS x").await,
            "2018T13"
        );
    }

    #[tokio::test]
    async fn date_format_zone_field_errors() {
        // A zone field is deferred -> error (rather than a wrong value).
        let engine = Engine::new();
        assert!(engine
            .sql("SELECT date_format(timestamp '2018-11-17 13:33:33', 'yyyy z')")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn day_of_year_format() {
        // 2018-11-17 is day 321 of the year.
        assert_eq!(
            cell("SELECT date_format(timestamp '2018-11-17 00:00:00', 'D') AS x").await,
            "321"
        );
    }
}
