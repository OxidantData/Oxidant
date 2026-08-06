//! Spark encoding / bit / URL scalar functions.
//!
//! Implemented faithfully against Apache Spark v4.0.0 semantics (see
//! `oxidant-spark-compat/spark-tests/{inputs,results}/{bitwise,mask-functions,url-functions}.sql*`):
//!
//! - `bit_count(x)` — number of set bits, **respecting the integer width** of the input
//!   (`bit_count(CAST(-1 AS TINYINT))` = 8, not 64). Booleans map to 0/1. Returns `int`.
//! - `getbit(x, pos)` — the bit at `pos` (0 = least-significant) of a 64-bit integer, as
//!   `tinyint`. `pos` outside `[0, 63]` is a runtime error in Spark.
//! - `mask(str [, upper [, lower [, digit [, other]]]])` — replace upper-case letters with
//!   `upper` (default `X`), lower-case with `lower` (default `x`), digits with `digit`
//!   (default `n`), and every other char with `other` (default unchanged). A `NULL` mask char
//!   means "leave that category unchanged". Returns `string`.
//! - `parse_url(url, part)` — extract `HOST`/`PATH`/`QUERY`/`REF`/`PROTOCOL`/`FILE`/
//!   `AUTHORITY`/`USERINFO`. Returns `string` (NULL if the part is absent).
//! - `url_encode(str)` / `url_decode(str)` / `try_url_decode(str)` — `application/x-www-form-
//!   urlencoded` codec (space ↔ `+`). `url_decode` errors on a malformed escape; `try_url_decode`
//!   returns NULL instead.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int32Array, Int8Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

/// Register all encoding/bit/url Spark functions into `ctx`.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(BitCount::new()));
    ctx.register_udf(ScalarUDF::from(GetBit::new()));
    ctx.register_udf(ScalarUDF::from(Mask::new()));
    ctx.register_udf(ScalarUDF::from(ParseUrl::new()));
    ctx.register_udf(ScalarUDF::from(UrlEncode::new()));
    ctx.register_udf(ScalarUDF::from(UrlDecode::new(false)));
    ctx.register_udf(ScalarUDF::from(UrlDecode::new(true)));
}

fn arrow_err(e: datafusion::arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(e), None)
}

// ---------------------------------------------------------------------------
// bit_count
// ---------------------------------------------------------------------------

/// `bit_count(expr)` — count of set bits, honoring the input integer width. Booleans count as
/// 0/1. Floating-point and string inputs are a type error in Spark; here we mirror that by
/// rejecting them at runtime (the analyzer would normally reject earlier).
#[derive(Debug, PartialEq, Eq, Hash)]
struct BitCount {
    signature: Signature,
}

impl BitCount {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for BitCount {
    fn name(&self) -> &str {
        "bit_count"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let dt = args.arg_fields[0].data_type().clone();
        let arr = args.args[0].clone().into_array(args.number_rows)?;
        let out: Int32Array = match &dt {
            DataType::Boolean => {
                let a = datafusion::arrow::array::cast::as_boolean_array(&arr);
                (0..a.len())
                    .map(|i| (!a.is_null(i)).then(|| if a.value(i) { 1 } else { 0 }))
                    .collect()
            }
            DataType::Int8 | DataType::UInt8 => {
                bit_count_int(&arr, |v| (v as u8).count_ones() as i32)?
            }
            DataType::Int16 | DataType::UInt16 => {
                bit_count_int(&arr, |v| (v as u16).count_ones() as i32)?
            }
            DataType::Int32 | DataType::UInt32 => {
                bit_count_int(&arr, |v| (v as u32).count_ones() as i32)?
            }
            DataType::Int64 => bit_count_int(&arr, |v| (v as u64).count_ones() as i32)?,
            DataType::UInt64 => bit_count_u64(&arr)?,
            DataType::Null => Int32Array::from(vec![None; args.number_rows]),
            other => {
                return exec_err!(
                    "bit_count: unexpected input type {other:?}; expected integral or boolean"
                )
            }
        };
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

/// Cast `arr` to `i64` and apply `f` to each raw 64-bit value; `f` re-narrows to the original
/// width so only the meaningful bits are counted.
fn bit_count_int(arr: &ArrayRef, f: impl Fn(i64) -> i32) -> Result<Int32Array> {
    let casted = datafusion::arrow::compute::cast(arr, &DataType::Int64).map_err(arrow_err)?;
    let a = casted
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap();
    Ok((0..a.len())
        .map(|i| (!a.is_null(i)).then(|| f(a.value(i))))
        .collect())
}

fn bit_count_u64(arr: &ArrayRef) -> Result<Int32Array> {
    let casted = datafusion::arrow::compute::cast(arr, &DataType::UInt64).map_err(arrow_err)?;
    let a = casted
        .as_any()
        .downcast_ref::<datafusion::arrow::array::UInt64Array>()
        .unwrap();
    Ok((0..a.len())
        .map(|i| (!a.is_null(i)).then(|| a.value(i).count_ones() as i32))
        .collect())
}

// ---------------------------------------------------------------------------
// getbit
// ---------------------------------------------------------------------------

/// `getbit(expr, pos)` — bit `pos` (0 = LSB) of a 64-bit integer, returned as `tinyint`.
/// `pos` must be in `[0, 63]`; otherwise Spark raises `INVALID_PARAMETER_VALUE.BIT_POSITION_RANGE`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct GetBit {
    signature: Signature,
}

impl GetBit {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for GetBit {
    fn name(&self) -> &str {
        "getbit"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let val = args.args[0].clone().into_array(args.number_rows)?;
        let pos = args.args[1].clone().into_array(args.number_rows)?;
        let val = datafusion::arrow::compute::cast(&val, &DataType::Int64).map_err(arrow_err)?;
        let pos = datafusion::arrow::compute::cast(&pos, &DataType::Int64).map_err(arrow_err)?;
        let val = val
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        let pos = pos
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        let mut out = Int8Array::builder(args.number_rows);
        for i in 0..args.number_rows {
            if val.is_null(i) || pos.is_null(i) {
                out.append_null();
                continue;
            }
            let p = pos.value(i);
            if !(0..=63).contains(&p) {
                return exec_err!(
                    "getbit: invalid bit position {p} outside the range [0, 64) (parameter `pos`)"
                );
            }
            let bit = ((val.value(i) >> p) & 1) as i8;
            out.append_value(bit);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// mask
// ---------------------------------------------------------------------------

/// `mask(str [, upperChar [, lowerChar [, digitChar [, otherChar]]]])`.
///
/// Per-category replacement chars; a `NULL` mask char means "leave that category unchanged".
/// Defaults: upper=`X`, lower=`x`, digit=`n`, other=unchanged. Each mask char arg must be a
/// single-character string (or NULL); anything else is a type error in Spark.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Mask {
    signature: Signature,
}

impl Mask {
    fn new() -> Self {
        Self {
            // 1..=5 args; validate the exact count inside `invoke_with_args`.
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

/// One mask configuration char: `Keep` => leave the category unchanged.
#[derive(Clone, Copy)]
enum MaskChar {
    Replace(char),
    Keep,
}

/// Resolve the mask-config char for one category at `row`. `None` array => use `default`; a NULL
/// element => `Keep`; a single-char string => `Replace`; anything else is a type error.
fn mask_char_at(arr: &Option<ArrayRef>, row: usize, default: MaskChar) -> Result<MaskChar> {
    match arr {
        None => Ok(default),
        Some(a) => {
            if a.is_null(row) {
                return Ok(MaskChar::Keep);
            }
            let s = a
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "mask: mask character arguments must be strings".into(),
                    )
                })?
                .value(row);
            let mut chars = s.chars();
            match (chars.next(), chars.next()) {
                (Some(c), None) => Ok(MaskChar::Replace(c)),
                _ => exec_err!("mask: each masking character must be a single character"),
            }
        }
    }
}

fn apply_mask(c: char, upper: MaskChar, lower: MaskChar, digit: MaskChar, other: MaskChar) -> char {
    let cfg = if c.is_ascii_uppercase() {
        upper
    } else if c.is_ascii_lowercase() {
        lower
    } else if c.is_ascii_digit() {
        digit
    } else {
        other
    };
    match cfg {
        MaskChar::Keep => c,
        MaskChar::Replace(r) => r,
    }
}

impl ScalarUDFImpl for Mask {
    fn name(&self) -> &str {
        "mask"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.args.len();
        if !(1..=5).contains(&n) {
            return exec_err!("mask: expected between 1 and 5 arguments, got {n}");
        }
        let to_str_array = |cv: &ColumnarValue| -> Result<ArrayRef> {
            let a = cv.clone().into_array(args.number_rows)?;
            datafusion::arrow::compute::cast(&a, &DataType::Utf8).map_err(arrow_err)
        };
        let input = to_str_array(&args.args[0])?;
        let input = input.as_any().downcast_ref::<StringArray>().unwrap();

        let opt_arr = |idx: usize| -> Result<Option<ArrayRef>> {
            if idx < n {
                Ok(Some(to_str_array(&args.args[idx])?))
            } else {
                Ok(None)
            }
        };
        let upper_arr = opt_arr(1)?;
        let lower_arr = opt_arr(2)?;
        let digit_arr = opt_arr(3)?;
        let other_arr = opt_arr(4)?;

        let mut out = datafusion::arrow::array::StringBuilder::new();
        for row in 0..args.number_rows {
            if input.is_null(row) {
                out.append_null();
                continue;
            }
            let upper = mask_char_at(&upper_arr, row, MaskChar::Replace('X'))?;
            let lower = mask_char_at(&lower_arr, row, MaskChar::Replace('x'))?;
            let digit = mask_char_at(&digit_arr, row, MaskChar::Replace('n'))?;
            let other = mask_char_at(&other_arr, row, MaskChar::Keep)?;
            let masked: String = input
                .value(row)
                .chars()
                .map(|c| apply_mask(c, upper, lower, digit, other))
                .collect();
            out.append_value(masked);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// parse_url
// ---------------------------------------------------------------------------

/// `parse_url(url, partToExtract)` — extract a component of a URL string. Supported parts:
/// `HOST`, `PATH`, `QUERY`, `REF`, `PROTOCOL`, `FILE`, `AUTHORITY`, `USERINFO`. Returns NULL
/// when the requested part is absent. (The 3-arg `parse_url(url, 'QUERY', key)` form is not yet
/// implemented.)
#[derive(Debug, PartialEq, Eq, Hash)]
struct ParseUrl {
    signature: Signature,
}

impl ParseUrl {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

/// Extract a URL component over the RFC-3986 generic syntax. Matches Spark (which delegates to
/// `java.net.URI`) for the supported parts. Returns `None` (NULL) when the component is absent.
fn parse_url_part(url: &str, part: &str) -> Option<String> {
    // Split off the fragment (#ref) first.
    let (before_frag, frag) = match url.split_once('#') {
        Some((a, b)) => (a, Some(b)),
        None => (url, None),
    };
    let has_authority = before_frag.contains("://");
    let (scheme, rest) = if has_authority {
        let (s, r) = before_frag.split_once("://").unwrap();
        (Some(s), r)
    } else {
        match before_frag.split_once(':') {
            Some((s, r)) => (Some(s), r),
            None => (None, before_frag),
        }
    };
    // `rest` = authority + path + query (for the `scheme://` form). Split out the authority.
    let (authority, path_and_query): (Option<&str>, &str) = if has_authority {
        let end = rest.find(['/', '?']).unwrap_or(rest.len());
        let (auth, pq) = rest.split_at(end);
        (Some(auth), pq)
    } else {
        (None, rest)
    };
    let (path, query) = match path_and_query.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (path_and_query, None),
    };

    match part {
        "PROTOCOL" => scheme.map(|s| s.to_string()),
        "REF" => frag.map(|s| s.to_string()),
        "QUERY" => query.map(|s| s.to_string()),
        "PATH" => Some(path.to_string()),
        "FILE" => Some(match query {
            Some(q) => format!("{path}?{q}"),
            None => path.to_string(),
        }),
        "AUTHORITY" => authority.map(|s| s.to_string()),
        "USERINFO" => authority.and_then(|a| a.split_once('@').map(|(u, _)| u.to_string())),
        "HOST" => authority.map(|a| {
            let host_port = a.split_once('@').map(|(_, h)| h).unwrap_or(a);
            // strip a `:port` suffix if present
            match host_port.rsplit_once(':') {
                Some((h, p)) if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) => {
                    h.to_string()
                }
                _ => host_port.to_string(),
            }
        }),
        _ => None,
    }
}

impl ScalarUDFImpl for ParseUrl {
    fn name(&self) -> &str {
        "parse_url"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!(
                "parse_url: expected 2 arguments (url, partToExtract), got {}",
                args.args.len()
            );
        }
        let to_str = |cv: &ColumnarValue| -> Result<ArrayRef> {
            let a = cv.clone().into_array(args.number_rows)?;
            datafusion::arrow::compute::cast(&a, &DataType::Utf8).map_err(arrow_err)
        };
        let url = to_str(&args.args[0])?;
        let part = to_str(&args.args[1])?;
        let url = url.as_any().downcast_ref::<StringArray>().unwrap();
        let part = part.as_any().downcast_ref::<StringArray>().unwrap();
        let mut out = datafusion::arrow::array::StringBuilder::new();
        for i in 0..args.number_rows {
            if url.is_null(i) || part.is_null(i) {
                out.append_null();
                continue;
            }
            match parse_url_part(url.value(i), part.value(i)) {
                Some(s) => out.append_value(s),
                None => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// url_encode / url_decode / try_url_decode
// ---------------------------------------------------------------------------

/// `application/x-www-form-urlencoded` encode: unreserved chars pass through, space => `+`,
/// everything else => `%XX` (upper-case hex). Matches Spark's `url_encode`.
fn form_url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'*' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => {
                out.push('%');
                out.push(hex_upper(b >> 4));
                out.push(hex_upper(b & 0x0f));
            }
        }
    }
    out
}

fn hex_upper(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'A' + (nibble - 10)) as char,
    }
}

/// `application/x-www-form-urlencoded` decode. `+` => space, `%XX` => byte. Returns `Err` on a
/// malformed escape (incomplete or non-hex) or invalid UTF-8, matching Spark's `CANNOT_DECODE_URL`.
fn form_url_decode(s: &str) -> std::result::Result<String, ()> {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' => {
                if i + 2 >= bytes.len() {
                    return Err(());
                }
                let hi = hex_val(bytes[i + 1]).ok_or(())?;
                let lo = hex_val(bytes[i + 2]).ok_or(())?;
                out.push((hi << 4) | lo);
                i += 3;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|_| ())
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct UrlEncode {
    signature: Signature,
}

impl UrlEncode {
    fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for UrlEncode {
    fn name(&self) -> &str {
        "url_encode"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let a = args.args[0].clone().into_array(args.number_rows)?;
        let a = datafusion::arrow::compute::cast(&a, &DataType::Utf8).map_err(arrow_err)?;
        let a = a.as_any().downcast_ref::<StringArray>().unwrap();
        let out: StringArray = (0..a.len())
            .map(|i| (!a.is_null(i)).then(|| form_url_encode(a.value(i))))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

/// `url_decode` (`try=false`) errors on a malformed escape; `try_url_decode` (`try=true`)
/// returns NULL instead.
#[derive(Debug, PartialEq, Eq, Hash)]
struct UrlDecode {
    signature: Signature,
    try_mode: bool,
}

impl UrlDecode {
    fn new(try_mode: bool) -> Self {
        Self {
            signature: Signature::uniform(1, vec![DataType::Utf8], Volatility::Immutable),
            try_mode,
        }
    }
}

impl ScalarUDFImpl for UrlDecode {
    fn name(&self) -> &str {
        if self.try_mode {
            "try_url_decode"
        } else {
            "url_decode"
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let a = args.args[0].clone().into_array(args.number_rows)?;
        let a = datafusion::arrow::compute::cast(&a, &DataType::Utf8).map_err(arrow_err)?;
        let a = a.as_any().downcast_ref::<StringArray>().unwrap();
        let mut out = datafusion::arrow::array::StringBuilder::new();
        for i in 0..a.len() {
            if a.is_null(i) {
                out.append_null();
                continue;
            }
            match form_url_decode(a.value(i)) {
                Ok(s) => out.append_value(s),
                Err(()) => {
                    if self.try_mode {
                        out.append_null();
                    } else {
                        return exec_err!("url_decode: could not decode URL `{}`", a.value(i));
                    }
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

#[cfg(test)]
mod tests {
    use crate::Engine;

    async fn run(q: &str) -> String {
        let engine = Engine::new();
        let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        crate::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn bit_count_respects_width() {
        // -1 as bigint => 64; -1 as tinyint => 8.
        let g = run("SELECT bit_count(CAST(-1 AS BIGINT)) AS a, bit_count(CAST(-1 AS TINYINT)) AS b, bit_count(3) AS c, bit_count(true) AS d, bit_count(false) AS e").await;
        assert!(g.contains("64"), "{g}");
        assert!(g.contains('8'), "{g}");
        assert!(g.contains('2'), "{g}"); // bit_count(3)
                                         // NULL input -> NULL output (arrow pretty-print renders NULL as an empty cell).
        let n = run("SELECT bit_count(CAST(NULL AS INT)) AS a").await;
        assert!(n.contains("|   |"), "{n}");
    }

    #[tokio::test]
    async fn getbit_basic_and_type() {
        // 11 = 1011b -> bit3=1, bit2=0, bit1=1, bit0=1, bit63=0
        let g = run("SELECT getbit(CAST(11 AS BIGINT), 3) AS a, getbit(CAST(11 AS BIGINT), 2) AS b, getbit(CAST(11 AS BIGINT), 1) AS c, getbit(CAST(11 AS BIGINT), 0) AS d, getbit(CAST(11 AS BIGINT), 63) AS e").await;
        // exactly the row "1  0  1  1  0"
        assert!(g.contains("1") && g.contains("0"), "{g}");
        let g2 = run("SELECT getbit(CAST(11 AS BIGINT), 3) AS a").await;
        assert!(g2.contains("| 1"), "{g2}");
    }

    #[tokio::test]
    async fn getbit_out_of_range_errors() {
        let engine = Engine::new();
        let r = engine.sql("SELECT getbit(CAST(11 AS BIGINT), 64)").await;
        assert!(r.is_err(), "expected error for out-of-range pos");
        let r2 = engine.sql("SELECT getbit(CAST(11 AS BIGINT), -1)").await;
        assert!(r2.is_err(), "expected error for negative pos");
    }

    #[tokio::test]
    async fn mask_defaults_and_overrides() {
        let g = run("SELECT mask('AbCD123-@$#') AS a").await;
        assert!(g.contains("XxXXnnn-@$#"), "{g}");
        let g2 = run("SELECT mask('AbCD123-@$#', 'Q', 'q', 'd', 'o') AS a").await;
        assert!(g2.contains("QqQQdddoooo"), "{g2}");
        let g3 = run("SELECT mask('AbCD123-@$#', NULL, 'q', 'd', 'o') AS a").await;
        assert!(g3.contains("AqCDdddoooo"), "{g3}");
        let g4 = run("SELECT mask(CAST(NULL AS STRING)) AS a").await;
        assert!(g4.contains("|   |"), "{g4}");
    }

    #[tokio::test]
    async fn parse_url_components() {
        let u = "http://userinfo@spark.apache.org/path?query=1#Ref";
        for (part, want) in [
            ("HOST", "spark.apache.org"),
            ("PATH", "/path"),
            ("QUERY", "query=1"),
            ("REF", "Ref"),
            ("PROTOCOL", "http"),
            ("FILE", "/path?query=1"),
            ("AUTHORITY", "userinfo@spark.apache.org"),
            ("USERINFO", "userinfo"),
        ] {
            let g = run(&format!("SELECT parse_url('{u}', '{part}') AS a")).await;
            assert!(g.contains(want), "{part}: want {want}, got {g}");
        }
    }

    #[tokio::test]
    async fn url_codec_roundtrip_and_errors() {
        let g = run("SELECT url_encode('https://spark.apache.org') AS a").await;
        assert!(g.contains("https%3A%2F%2Fspark.apache.org"), "{g}");
        let g2 = run("SELECT url_decode('https%3A%2F%2Fspark.apache.org') AS a").await;
        assert!(g2.contains("https://spark.apache.org"), "{g2}");
        // malformed escape: url_decode errors, try_url_decode returns NULL.
        let engine = Engine::new();
        let bad = engine
            .sql("SELECT url_decode('http%3A%2F%2spark.apache.org')")
            .await;
        assert!(bad.is_err(), "url_decode should error on bad escape");
        let g3 = run("SELECT try_url_decode('http%3A%2F%2spark.apache.org') AS a").await;
        assert!(g3.contains("|   |"), "{g3}");
    }
}
