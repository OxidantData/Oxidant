//! Spark number / base conversion scalar functions.
//!
//! Implemented faithfully against Apache Spark v4.0.0 semantics (golden files:
//! `oxidant-spark-compat/spark-tests/{inputs,results}/{string-functions,charvarchar,math}.sql*` and
//! the PostgreSQL `int8`/`numeric` `to_char` corpus):
//!
//! - `bin(expr)` — the two's-complement binary string of a `bigint` (no leading zeros; `0` => `"0"`,
//!   negatives use the full 64-bit pattern). Returns `string`.
//! - `conv(num, fromBase, toBase)` — reinterpret `num` (a string/integer in base `fromBase`) into
//!   base `toBase`. Bases in `[2, 36]`; a negative `toBase` yields a signed result, otherwise the
//!   value is treated as an unsigned 64-bit integer. Overflow of the unsigned 64-bit accumulator is
//!   an error under ANSI (Spark's `ARITHMETIC_OVERFLOW`). Returns `string`.
//! - `to_number(str, fmt)` / `try_to_number(str, fmt)` — parse a formatted numeric string into a
//!   `decimal` whose precision/scale are derived from the format string. `try_to_number` returns
//!   `NULL` instead of erroring on a mismatched input.
//! - `to_char(num, fmt)` / `to_varchar(num, fmt)` — the inverse: render a numeric value as a string
//!   using the same format model. When the integer part does not fit the format, Spark emits a
//!   string of `#` of the format's width.
//!
//! The shared format model (Spark's `ToNumberParser`) supports the characters the corpus actually
//! exercises: `0` `9` (digits), `.` `D` (decimal point), `,` `G` (grouping), `$` (currency), `S`
//! (anchored sign), `MI` (trailing minus / leading-space-for-positive) and `PR` (angle-bracket
//! negative). Exotic Postgres-only directives (`EEEE`, `TH`, `RN`, `FM`, quoted literals, `L`, `V`)
//! are **not** implemented and are deferred — they do not appear in the Spark-faithful corpus.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Decimal128Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::SessionContext;

/// Register all conversion Spark functions into `ctx`.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(Bin::new()));
    ctx.register_udf(ScalarUDF::from(Conv::new()));
    ctx.register_udf(ScalarUDF::from(ToNumber::new(false)));
    ctx.register_udf(ScalarUDF::from(ToNumber::new(true)));
    ctx.register_udf(ScalarUDF::from(ToChar::new("to_char")));
    ctx.register_udf(ScalarUDF::from(ToChar::new("to_varchar")));
}

fn arrow_err(e: datafusion::arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(e), None)
}

// ---------------------------------------------------------------------------
// bin
// ---------------------------------------------------------------------------

/// `bin(expr)` — binary string of the `bigint` two's-complement value. `0` => `"0"`; positives have
/// no leading zeros; negatives render the full 64-bit two's-complement pattern (as Java's
/// `Long.toBinaryString`).
#[derive(Debug, PartialEq, Eq, Hash)]
struct Bin {
    signature: Signature,
}

impl Bin {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Bin {
    fn name(&self) -> &str {
        "bin"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let arr = args.args[0].clone().into_array(n)?;
        // Spark casts the argument to bigint (truncating toward zero for fractional inputs).
        let casted = datafusion::arrow::compute::cast(&arr, &DataType::Int64).map_err(arrow_err)?;
        let a = casted.as_any().downcast_ref::<Int64Array>().unwrap();
        let out: StringArray = (0..n)
            .map(|i| {
                (!a.is_null(i)).then(|| {
                    let v = a.value(i) as u64;
                    // u64 binary, no leading zeros; 0 prints "0".
                    format!("{v:b}")
                })
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

// ---------------------------------------------------------------------------
// conv
// ---------------------------------------------------------------------------

/// `conv(num, fromBase, toBase)`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Conv {
    signature: Signature,
}

impl Conv {
    fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

/// Parse `s` in base `from_base` into an unsigned-64-bit accumulator. A leading `-` is honored and
/// the magnitude negated (mod 2^64). Parsing stops at the first character that is not a valid digit
/// in `from_base` (Spark mirrors Java's behavior of consuming the valid prefix). Returns the
/// 64-bit pattern, or `Err` on unsigned-64 overflow (Spark `ARITHMETIC_OVERFLOW`).
fn conv_parse(s: &str, from_base: u32) -> std::result::Result<u64, ()> {
    let s = s.trim();
    let (neg, digits) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let mut acc: u64 = 0;
    let mut any = false;
    for c in digits.chars() {
        let d = c.to_digit(from_base);
        match d {
            Some(d) => {
                any = true;
                // acc = acc * from_base + d, detecting unsigned-64 overflow.
                acc = acc
                    .checked_mul(from_base as u64)
                    .and_then(|x| x.checked_add(d as u64))
                    .ok_or(())?;
            }
            // Stop at the first invalid digit (consume valid prefix only).
            None => break,
        }
    }
    if !any {
        // No valid digits -> 0 (Spark returns "0").
        return Ok(0);
    }
    Ok(if neg {
        (acc as i64).wrapping_neg() as u64
    } else {
        acc
    })
}

/// Render the 64-bit pattern `val` in base `to_base`. If `to_base` is negative, interpret `val` as
/// a signed `i64`; otherwise as unsigned `u64`.
fn conv_render(val: u64, to_base: i32) -> String {
    let unsigned_base = to_base.unsigned_abs();
    if to_base < 0 {
        let signed = val as i64;
        if signed < 0 {
            let mag = (signed as i128).unsigned_abs() as u64;
            format!("-{}", to_base_digits(mag, unsigned_base))
        } else {
            to_base_digits(signed as u64, unsigned_base)
        }
    } else {
        to_base_digits(val, unsigned_base)
    }
}

/// Unsigned base conversion to an upper-case digit string (no sign). `0` => `"0"`.
fn to_base_digits(mut v: u64, base: u32) -> String {
    if v == 0 {
        return "0".to_string();
    }
    let base64 = base as u64;
    let mut buf = Vec::new();
    while v > 0 {
        let d = (v % base64) as u32;
        let c = std::char::from_digit(d, base).unwrap().to_ascii_uppercase();
        buf.push(c as u8);
        v /= base64;
    }
    buf.reverse();
    String::from_utf8(buf).unwrap()
}

impl ScalarUDFImpl for Conv {
    fn name(&self) -> &str {
        "conv"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let num = args.args[0].clone().into_array(n)?;
        let from = args.args[1].clone().into_array(n)?;
        let to = args.args[2].clone().into_array(n)?;
        let num = datafusion::arrow::compute::cast(&num, &DataType::Utf8).map_err(arrow_err)?;
        let from = datafusion::arrow::compute::cast(&from, &DataType::Int64).map_err(arrow_err)?;
        let to = datafusion::arrow::compute::cast(&to, &DataType::Int64).map_err(arrow_err)?;
        let num = num.as_any().downcast_ref::<StringArray>().unwrap();
        let from = from.as_any().downcast_ref::<Int64Array>().unwrap();
        let to = to.as_any().downcast_ref::<Int64Array>().unwrap();

        let mut out = datafusion::arrow::array::StringBuilder::new();
        for i in 0..n {
            if num.is_null(i) || from.is_null(i) || to.is_null(i) {
                out.append_null();
                continue;
            }
            let from_base = from.value(i);
            let to_base = to.value(i);
            if !(2..=36).contains(&from_base) || !(2..=36).contains(&to_base.abs()) {
                // Spark returns NULL when a base is outside [2, 36].
                out.append_null();
                continue;
            }
            match conv_parse(num.value(i), from_base as u32) {
                Ok(v) => out.append_value(conv_render(v, to_base as i32)),
                Err(()) => return exec_err!("Overflow in function conv()"),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// shared numeric format model
// ---------------------------------------------------------------------------

/// How the sign is represented in a format string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SignKind {
    /// No explicit sign directive: negatives still print a leading `-`, positives nothing.
    None,
    /// `S` anchored at the front of the format (`+`/`-`).
    SLeading,
    /// `S` anchored at the end of the format (`+`/`-`).
    STrailing,
    /// `MI` (always trailing): `-` for negatives, a space for non-negatives.
    Mi,
    /// `PR`: negatives wrapped in `<...>`, non-negatives wrapped in spaces.
    Pr,
}

/// A parsed numeric format string in the shared `to_number` / `to_char` model.
#[derive(Debug, Clone)]
struct NumFormat {
    /// Number of digit slots before the decimal point.
    int_digits: usize,
    /// Number of digit slots after the decimal point.
    frac_digits: usize,
    /// `true` if the leading digit slot is `0` (zero-padded rather than space/blank-padded).
    zero_pad: bool,
    /// Whether the format contains a decimal point (`.` or `D`).
    has_decimal: bool,
    /// Whether the format contains grouping separators (`,` or `G`).
    has_grouping: bool,
    /// Whether the format contains a currency `$`.
    has_dollar: bool,
    sign: SignKind,
}

/// Parse a format string into a [`NumFormat`]. Returns `Err` for unsupported directives.
fn parse_format(fmt: &str) -> std::result::Result<NumFormat, String> {
    let upper = fmt.to_ascii_uppercase();
    let bytes = upper.as_bytes();
    let mut int_digits = 0usize;
    let mut frac_digits = 0usize;
    let mut has_decimal = false;
    let mut has_grouping = false;
    let mut has_dollar = false;
    let mut zero_pad = false;
    let mut seen_digit = false;
    let mut sign = SignKind::None;
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b'0' | b'9' => {
                if !seen_digit && c == b'0' {
                    zero_pad = true;
                }
                seen_digit = true;
                if has_decimal {
                    frac_digits += 1;
                } else {
                    int_digits += 1;
                }
                i += 1;
            }
            b'.' | b'D' => {
                if has_decimal {
                    return Err("multiple decimal points".into());
                }
                has_decimal = true;
                i += 1;
            }
            b',' | b'G' => {
                has_grouping = true;
                i += 1;
            }
            b'$' => {
                has_dollar = true;
                i += 1;
            }
            b'S' => {
                sign = if seen_digit {
                    SignKind::STrailing
                } else {
                    SignKind::SLeading
                };
                i += 1;
            }
            b'M' if i + 1 < bytes.len() && bytes[i + 1] == b'I' => {
                sign = SignKind::Mi;
                i += 2;
            }
            b'P' if i + 1 < bytes.len() && bytes[i + 1] == b'R' => {
                sign = SignKind::Pr;
                i += 2;
            }
            other => {
                return Err(format!("unsupported format character '{}'", other as char));
            }
        }
    }
    Ok(NumFormat {
        int_digits,
        frac_digits,
        zero_pad,
        has_decimal,
        has_grouping,
        has_dollar,
        sign,
    })
}

// ---------------------------------------------------------------------------
// to_number / try_to_number
// ---------------------------------------------------------------------------

/// `to_number(str, fmt)` (and `try_to_number`). Returns a `decimal(p, s)` where `p = int_digits +
/// frac_digits` and `s = frac_digits`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ToNumber {
    signature: Signature,
    try_mode: bool,
}

impl ToNumber {
    fn new(try_mode: bool) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            try_mode,
        }
    }
}

/// Parse one formatted string into an unscaled `i128` value at `fmt`'s scale. Returns `None` on any
/// mismatch with the format (Spark's `INVALID_FORMAT.MISMATCH_INPUT`).
fn to_number_parse(input: &str, fmt: &NumFormat) -> Option<i128> {
    let mut s = input.trim();
    let mut negative = false;

    // Leading/trailing sign handling.
    match fmt.sign {
        SignKind::Pr => {
            if let Some(inner) = s.strip_prefix('<').and_then(|x| x.strip_suffix('>')) {
                negative = true;
                s = inner;
            }
        }
        SignKind::Mi => {
            if let Some(rest) = s.strip_suffix('-') {
                negative = true;
                s = rest;
            } else if let Some(rest) = s.strip_suffix(' ') {
                s = rest;
            }
        }
        SignKind::SLeading => {
            if let Some(rest) = s.strip_prefix('-') {
                negative = true;
                s = rest;
            } else if let Some(rest) = s.strip_prefix('+') {
                s = rest;
            }
        }
        SignKind::STrailing => {
            if let Some(rest) = s.strip_suffix('-') {
                negative = true;
                s = rest;
            } else if let Some(rest) = s.strip_suffix('+') {
                s = rest;
            }
        }
        SignKind::None => {
            // A bare leading sign is allowed by Spark only via S/MI/PR; a leading '+'/'-' here is
            // a mismatch. But the corpus's `S000` cases route through SLeading, so keep strict.
            if s.starts_with('-') || s.starts_with('+') {
                return None;
            }
        }
    }

    // Currency.
    if fmt.has_dollar {
        s = s.strip_prefix('$')?;
    }

    // Split fractional part.
    let (int_part, frac_part) = match s.split_once('.') {
        Some((a, b)) => {
            if !fmt.has_decimal {
                return None;
            }
            (a, b)
        }
        None => (s, ""),
    };

    // Strip grouping separators from the integer part; reject them if the format lacks grouping.
    let int_digits: String = if fmt.has_grouping {
        int_part.chars().filter(|&c| c != ',').collect()
    } else {
        if int_part.contains(',') {
            return None;
        }
        int_part.to_string()
    };

    if int_digits.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_digits.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    if !frac_part.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    // Integer part must fit the format's integer-digit budget.
    if int_digits.len() > fmt.int_digits {
        return None;
    }
    // Fractional part must not exceed the format's fractional-digit budget.
    if frac_part.len() > fmt.frac_digits {
        return None;
    }

    // Build the unscaled value at scale = frac_digits.
    let mut combined = String::new();
    combined.push_str(&int_digits);
    combined.push_str(frac_part);
    // Right-pad fractional digits to the format scale.
    for _ in 0..(fmt.frac_digits - frac_part.len()) {
        combined.push('0');
    }
    let magnitude: i128 = if combined.is_empty() {
        0
    } else {
        combined.parse().ok()?
    };
    Some(if negative { -magnitude } else { magnitude })
}

impl ScalarUDFImpl for ToNumber {
    fn name(&self) -> &str {
        if self.try_mode {
            "try_to_number"
        } else {
            "to_number"
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Without the literal format we cannot know the exact precision/scale; the planner uses
        // `return_field_from_args` (below) when the format is a constant, which it always is in the
        // corpus. This fallback keeps a sane default for the rare non-constant case.
        Ok(DataType::Decimal128(38, 0))
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // Derive decimal(precision, scale) from the literal format string when present.
        let (p, s) = match args.scalar_arguments.get(1).and_then(|o| *o) {
            Some(ScalarValue::Utf8(Some(f)))
            | Some(ScalarValue::LargeUtf8(Some(f)))
            | Some(ScalarValue::Utf8View(Some(f))) => {
                let parsed = parse_format(f).map_err(|e| {
                    DataFusionError::Plan(format!("{}: invalid format `{f}`: {e}", self.name()))
                })?;
                (
                    (parsed.int_digits + parsed.frac_digits).max(1) as u8,
                    parsed.frac_digits as i8,
                )
            }
            _ => (38, 0),
        };
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Decimal128(p, s),
            true,
        )))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let input = args.args[0].clone().into_array(n)?;
        let fmt = args.args[1].clone().into_array(n)?;
        let input = datafusion::arrow::compute::cast(&input, &DataType::Utf8).map_err(arrow_err)?;
        let fmt = datafusion::arrow::compute::cast(&fmt, &DataType::Utf8).map_err(arrow_err)?;
        let input = input.as_any().downcast_ref::<StringArray>().unwrap();
        let fmt = fmt.as_any().downcast_ref::<StringArray>().unwrap();

        // The produced decimal type must match what `return_field_from_args` promised: derive it
        // from the (constant) format. Fall back to the first non-null row's format.
        let mut precision = 38u8;
        let mut scale = 0i8;
        let mut parsed_fmt: Option<NumFormat> = None;
        for i in 0..n {
            if !fmt.is_null(i) {
                let parsed = parse_format(fmt.value(i)).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "to_number: invalid format `{}`: {e}",
                        fmt.value(i)
                    ))
                })?;
                let p = (parsed.int_digits + parsed.frac_digits).max(1) as u8;
                precision = p;
                scale = parsed.frac_digits as i8;
                parsed_fmt = Some(parsed);
                break;
            }
        }

        let mut builder = Decimal128Array::builder(n)
            .with_precision_and_scale(precision, scale)
            .map_err(arrow_err)?;

        for i in 0..n {
            if input.is_null(i) || fmt.is_null(i) {
                builder.append_null();
                continue;
            }
            // Re-parse per row only if the format varies; reuse the constant parse otherwise.
            let f = match &parsed_fmt {
                Some(f) => f.clone(),
                None => parse_format(fmt.value(i)).map_err(|e| {
                    DataFusionError::Execution(format!("to_number: invalid format: {e}"))
                })?,
            };
            match to_number_parse(input.value(i), &f) {
                Some(v) => builder.append_value(v),
                None => {
                    if self.try_mode {
                        builder.append_null();
                    } else {
                        return exec_err!(
                            "to_number: the input string `{}` does not match the format `{}`",
                            input.value(i),
                            fmt.value(i)
                        );
                    }
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// ---------------------------------------------------------------------------
// to_char / to_varchar
// ---------------------------------------------------------------------------

/// `to_char(num, fmt)` / `to_varchar(num, fmt)` — the numeric-formatting inverse of `to_number`.
///
/// This UDF dispatches on the first argument's type: numeric (decimal/integer/float) inputs use the
/// shared numeric format model; non-numeric (e.g. temporal) inputs are rejected here so we never
/// silently mis-format a date — those belong to the datetime `to_char`, which we do not subsume.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ToChar {
    name: &'static str,
    signature: Signature,
}

impl ToChar {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

/// Render `unscaled` (a value at `scale` decimal places) per `fmt`. Returns the `#`-overflow string
/// when the integer part does not fit.
fn to_char_render(unscaled: i128, scale: u32, fmt: &NumFormat) -> String {
    let negative = unscaled < 0;
    let mut magnitude = unscaled.unsigned_abs();

    // Re-scale the magnitude to the format's fractional digit count, rounding half-up.
    let frac = fmt.frac_digits as u32;
    if frac < scale {
        // Drop (scale - frac) digits with half-up rounding.
        let drop = scale - frac;
        let divisor = 10u128.pow(drop);
        let rem = magnitude % divisor;
        magnitude /= divisor;
        if rem * 2 >= divisor {
            magnitude += 1;
        }
    } else if frac > scale {
        magnitude = magnitude.saturating_mul(10u128.pow(frac - scale));
    }

    // Split into integer and fractional digit strings at the format scale.
    let frac_divisor = 10u128.pow(frac);
    let int_value = magnitude / frac_divisor;
    let frac_value = magnitude % frac_divisor;

    let int_str = int_value.to_string();
    // Overflow: integer part has more digits than the format allows -> Spark emits '#' for each
    // digit position, preserving the decimal point separator (e.g. '99.9' -> '##.#').
    if int_str.len() > fmt.int_digits {
        return overflow_string(fmt);
    }

    // Build the integer field (zero- or space-padded to int_digits), with grouping.
    let pad_char = if fmt.zero_pad { '0' } else { ' ' };
    let mut int_field = String::new();
    let pad = fmt.int_digits - int_str.len();
    for _ in 0..pad {
        int_field.push(pad_char);
    }
    int_field.push_str(&int_str);

    if fmt.has_grouping {
        int_field = apply_grouping(&int_field);
    }

    // Assemble the numeric body.
    let mut body = String::new();
    if fmt.has_dollar {
        body.push('$');
    }
    body.push_str(&int_field);
    if fmt.has_decimal {
        body.push('.');
        let fs = format!("{frac_value:0width$}", width = frac as usize);
        body.push_str(&fs);
    }

    apply_sign(body, negative, fmt.sign)
}

/// Apply grouping (comma every three digits) to an already-padded integer field, grouping across
/// the full field width.
fn apply_grouping(field: &str) -> String {
    let chars: Vec<char> = field.chars().collect();
    let mut out = String::new();
    let len = chars.len();
    for (idx, c) in chars.iter().enumerate() {
        // Insert a comma before a group boundary (every 3 from the right), except at the start.
        let from_right = len - idx;
        if idx > 0 && from_right % 3 == 0 {
            out.push(',');
        }
        out.push(*c);
    }
    out
}

/// Wrap/prefix/suffix `body` with the configured sign representation.
fn apply_sign(body: String, negative: bool, sign: SignKind) -> String {
    match sign {
        SignKind::None => {
            if negative {
                format!("-{body}")
            } else {
                body
            }
        }
        SignKind::SLeading => {
            if negative {
                format!("-{body}")
            } else {
                format!("+{body}")
            }
        }
        SignKind::STrailing => {
            if negative {
                format!("{body}-")
            } else {
                format!("{body}+")
            }
        }
        SignKind::Mi => {
            if negative {
                format!("{body}-")
            } else {
                format!("{body} ")
            }
        }
        SignKind::Pr => {
            if negative {
                format!("<{body}>")
            } else {
                format!(" {body} ")
            }
        }
    }
}

/// The `#`-overflow string Spark emits when the integer part does not fit the format: each digit
/// slot becomes `#`, with the decimal-point separator preserved (e.g. `99.9` -> `##.#`).
fn overflow_string(fmt: &NumFormat) -> String {
    let mut s = String::new();
    s.push_str(&"#".repeat(fmt.int_digits.max(1)));
    if fmt.has_decimal {
        s.push('.');
        s.push_str(&"#".repeat(fmt.frac_digits));
    }
    s
}

impl ScalarUDFImpl for ToChar {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let in_type = args.arg_fields[0].data_type().clone();

        // Only numeric inputs are handled here; defer temporal inputs to the datetime path.
        let is_numeric = matches!(
            in_type,
            DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::Int8
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
                | DataType::Null
        );
        if !is_numeric {
            return exec_err!(
                "{}: numeric formatting expects a numeric first argument, got {in_type:?}",
                self.name
            );
        }

        let fmt = args.args[1].clone().into_array(n)?;
        let fmt = datafusion::arrow::compute::cast(&fmt, &DataType::Utf8).map_err(arrow_err)?;
        let fmt = fmt.as_any().downcast_ref::<StringArray>().unwrap();

        // Cast the numeric input to a Decimal128, then read its (unscaled, scale) representation.
        let target = match &in_type {
            DataType::Decimal128(p, s) => DataType::Decimal128(*p, *s),
            _ => DataType::Decimal128(38, 18),
        };
        let casted =
            datafusion::arrow::compute::cast(&args.args[0].clone().into_array(n)?, &target)
                .map_err(arrow_err)?;
        let scale = match casted.data_type() {
            DataType::Decimal128(_, s) => (*s).max(0) as u32,
            _ => 0,
        };
        let unscaled_arr = casted.as_any().downcast_ref::<Decimal128Array>().unwrap();

        let mut out = datafusion::arrow::array::StringBuilder::new();
        for i in 0..n {
            if unscaled_arr.is_null(i) || fmt.is_null(i) {
                out.append_null();
                continue;
            }
            let f = parse_format(fmt.value(i)).map_err(|e| {
                DataFusionError::Execution(format!(
                    "{}: invalid format `{}`: {e}",
                    self.name,
                    fmt.value(i)
                ))
            })?;
            out.append_value(to_char_render(unscaled_arr.value(i), scale, &f));
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
    async fn bin_basic() {
        assert_eq!(cell("SELECT bin(0) AS x").await, "0");
        assert_eq!(cell("SELECT bin(25) AS x").await, "11001");
        assert_eq!(cell("SELECT bin(CAST(25 AS BIGINT)) AS x").await, "11001");
        // 25.5 casts (truncates) to 25.
        assert_eq!(cell("SELECT bin(CAST(25.5 AS DOUBLE)) AS x").await, "11001");
    }

    #[tokio::test]
    async fn conv_basic_and_signed_and_overflow() {
        assert_eq!(cell("SELECT conv('100', 2, 10) AS x").await, "4");
        assert_eq!(cell("SELECT conv(-10, 16, -10) AS x").await, "-16");
        assert_eq!(
            cell("SELECT conv('9223372036854775808', 10, 16) AS x").await,
            "8000000000000000"
        );
        // Unsigned-64 overflow -> error.
        let engine = Engine::new();
        assert!(engine
            .sql("SELECT conv('92233720368547758070', 10, 16)")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn to_number_corpus() {
        assert_eq!(cell("SELECT to_number('454', '000') AS x").await, "454");
        assert_eq!(
            cell("SELECT to_number('454.2', '000.0') AS x").await,
            "454.2"
        );
        assert_eq!(
            cell("SELECT to_number('12,454', '00,000') AS x").await,
            "12454"
        );
        assert_eq!(
            cell("SELECT to_number('$78.12', '$00.00') AS x").await,
            "78.12"
        );
        assert_eq!(cell("SELECT to_number('+454', 'S000') AS x").await, "454");
        assert_eq!(cell("SELECT to_number('-454', 'S000') AS x").await, "-454");
        assert_eq!(
            cell("SELECT to_number('12,454.8-', '00,000.9MI') AS x").await,
            "-12454.8"
        );
        assert_eq!(
            cell("SELECT to_number('00,454.8-', '00,000.9MI') AS x").await,
            "-454.8"
        );
        assert_eq!(
            cell("SELECT to_number('<00,454.8>', '00,000.9PR') AS x").await,
            "-454.8"
        );
    }

    #[tokio::test]
    async fn try_to_number_returns_null_on_mismatch() {
        assert_eq!(
            cell("SELECT try_to_number('abc', '000') AS x").await,
            "NULL"
        );
        // Overflow of integer budget -> NULL under try.
        assert_eq!(
            cell("SELECT try_to_number('1234', '00') AS x").await,
            "NULL"
        );
        let engine = Engine::new();
        assert!(engine.sql("SELECT to_number('abc', '000')").await.is_err());
    }

    #[tokio::test]
    async fn to_char_corpus() {
        // to_varchar is an alias for to_char.
        assert_eq!(
            cell("SELECT to_varchar(78.12, '$99.99') AS x").await,
            "$78.12"
        );
        // 111.11 with '99.9' -> integer part 111 doesn't fit 2 digit slots -> '##.#'.
        assert_eq!(cell("SELECT to_varchar(111.11, '99.9') AS x").await, "##.#");
        // grouping + trailing sign.
        assert_eq!(
            cell("SELECT to_varchar(12454.8, '99,999.9S') AS x").await,
            "12,454.8+"
        );
        assert_eq!(cell("SELECT to_char(454, '000') AS x").await, "454");
        // zero-padded.
        assert_eq!(cell("SELECT to_char(123, '00000') AS x").await, "00123");
    }
}
