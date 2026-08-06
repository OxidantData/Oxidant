//! Spark-only encoding / misc scalar functions.
//!
//! Implemented faithfully against Apache Spark v4.0.0 semantics (golden files:
//! `oxidant-spark-compat/spark-tests/{inputs,results}/{string-functions,misc-functions,
//! current_database_catalog}.sql*`):
//!
//! - `hex(expr)` — UPPERCASE hex string. For integral inputs, the hex of the two's-complement
//!   `bigint` with no leading zeros (`hex(0)` => `"0"`). For string/binary inputs, the hex of the
//!   raw bytes (two hex chars per byte). Returns `string`.
//! - `unhex(str)` — inverse of `hex` for the string/binary form: parse a hex string to `binary`.
//!   An odd-length input is left-padded with a leading `0` nibble (Spark/Hive behavior, e.g.
//!   `unhex('123')` => bytes `01 23`). Returns `NULL` on any non-hex character.
//! - `to_binary(str [, fmt])` / `try_to_binary(str [, fmt])` — convert a string to `binary` using
//!   `fmt` (`hex` default, also `utf-8`/`utf8` and `base64`). `to_binary` raises a runtime error on
//!   malformed input; `try_to_binary` returns `NULL`. The format string is matched
//!   case-insensitively; an unknown format is a (planning-ish) error. NULL in any argument => NULL.
//! - `current_database()` / `current_schema()` — the current schema name; Spark's default is
//!   `default`. `current_catalog()` — the current catalog; Spark's default is `spark_catalog`.
//!   Returns `string`.
//! - `assert_true(expr [, msg])` — returns `NULL` when `expr` is `true`; raises a runtime error
//!   (Spark `USER_RAISED_EXCEPTION`) when `expr` is `false` or `NULL`. With one argument the default
//!   message is `'<expr>' is not true!`; here we cannot recover the source text per row, so we use a
//!   fixed default message for the no-message form.
//! - `replace(str, search)` — Spark's 2-argument `replace`, equivalent to `replace(str, search,
//!   '')` (remove all occurrences). DataFusion's builtin `replace` requires exactly 3 args, so we
//!   register a variadic `replace` that handles both the 2-arg (remove) and 3-arg (substitute)
//!   forms.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

/// Register all encoding/misc Spark functions into `ctx`.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(Hex::new()));
    ctx.register_udf(ScalarUDF::from(Unhex::new()));
    ctx.register_udf(ScalarUDF::from(ToBinary::new(false)));
    ctx.register_udf(ScalarUDF::from(ToBinary::new(true)));
    ctx.register_udf(ScalarUDF::from(CurrentName::new(
        "current_database",
        "default",
    )));
    ctx.register_udf(ScalarUDF::from(CurrentName::new(
        "current_schema",
        "default",
    )));
    ctx.register_udf(ScalarUDF::from(CurrentName::new(
        "current_catalog",
        "spark_catalog",
    )));
    ctx.register_udf(ScalarUDF::from(AssertTrue::new()));
    ctx.register_udf(ScalarUDF::from(Replace::new()));
}

fn arrow_err(e: datafusion::arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(e), None)
}

const HEX_UPPER: &[u8; 16] = b"0123456789ABCDEF";

/// Hex-encode raw bytes as an UPPERCASE string (two chars per byte).
fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX_UPPER[(b >> 4) as usize] as char);
        out.push(HEX_UPPER[(b & 0x0f) as usize] as char);
    }
    out
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Decode a hex string to bytes, left-padding an odd-length input with a `0` nibble (Hive/Spark
/// `unhex`/`to_binary('..','hex')` behavior). Returns `None` on any non-hex character.
fn hex_to_bytes(s: &str) -> Option<Vec<u8>> {
    let raw = s.as_bytes();
    // Left-pad odd length: '123' -> '0123'.
    let (mut out, start_high) = if raw.len() % 2 == 1 {
        (Vec::with_capacity(raw.len() / 2 + 1), false)
    } else {
        (Vec::with_capacity(raw.len() / 2), true)
    };
    let mut high: Option<u8> = if start_high { None } else { Some(0) };
    for &b in raw {
        let nibble = hex_val(b)?;
        match high {
            None => high = Some(nibble),
            Some(h) => {
                out.push((h << 4) | nibble);
                high = None;
            }
        }
    }
    Some(out)
}

// ---------------------------------------------------------------------------
// hex
// ---------------------------------------------------------------------------

/// `hex(expr)` — UPPERCASE hex string. Integral inputs hex the two's-complement bigint (no leading
/// zeros); string/binary inputs hex the raw bytes.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Hex {
    signature: Signature,
}

impl Hex {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Hex {
    fn name(&self) -> &str {
        "hex"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let dt = args.arg_fields[0].data_type().clone();
        let arr = args.args[0].clone().into_array(n)?;
        let mut out = StringBuilder::new();
        match &dt {
            // Integral types: hex of the two's-complement bigint, no leading zeros, "0" for zero.
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => {
                let casted =
                    datafusion::arrow::compute::cast(&arr, &DataType::Int64).map_err(arrow_err)?;
                let a = casted
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .unwrap();
                for i in 0..n {
                    if a.is_null(i) {
                        out.append_null();
                    } else {
                        let v = a.value(i) as u64;
                        out.append_value(format!("{v:X}"));
                    }
                }
            }
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                let casted =
                    datafusion::arrow::compute::cast(&arr, &DataType::Binary).map_err(arrow_err)?;
                let a = casted.as_any().downcast_ref::<BinaryArray>().unwrap();
                for i in 0..n {
                    if a.is_null(i) {
                        out.append_null();
                    } else {
                        out.append_value(bytes_to_hex(a.value(i)));
                    }
                }
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                let casted =
                    datafusion::arrow::compute::cast(&arr, &DataType::Utf8).map_err(arrow_err)?;
                let a = casted.as_any().downcast_ref::<StringArray>().unwrap();
                for i in 0..n {
                    if a.is_null(i) {
                        out.append_null();
                    } else {
                        out.append_value(bytes_to_hex(a.value(i).as_bytes()));
                    }
                }
            }
            DataType::Null => {
                for _ in 0..n {
                    out.append_null();
                }
            }
            other => {
                return exec_err!(
                    "hex: unsupported input type {other:?}; expected integral, string or binary"
                )
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// unhex
// ---------------------------------------------------------------------------

/// `unhex(str)` — parse a hex string to `binary`; `NULL` on invalid (non-hex) input.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Unhex {
    signature: Signature,
}

impl Unhex {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Unhex {
    fn name(&self) -> &str {
        "unhex"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let arr = args.args[0].clone().into_array(n)?;
        let casted = datafusion::arrow::compute::cast(&arr, &DataType::Utf8).map_err(arrow_err)?;
        let a = casted.as_any().downcast_ref::<StringArray>().unwrap();
        let mut out = BinaryBuilder::new();
        for i in 0..n {
            if a.is_null(i) {
                out.append_null();
                continue;
            }
            match hex_to_bytes(a.value(i)) {
                Some(bytes) => out.append_value(bytes),
                None => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// to_binary / try_to_binary
// ---------------------------------------------------------------------------

/// `to_binary(str [, fmt])` and `try_to_binary(str [, fmt])`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ToBinary {
    signature: Signature,
    try_mode: bool,
}

impl ToBinary {
    fn new(try_mode: bool) -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            try_mode,
        }
    }
}

/// Decode standard (RFC-4648) base64, ignoring ASCII whitespace, requiring correct padding. Returns
/// `None` on any invalid character or malformed padding (Spark's `CONVERSION_INVALID_INPUT`).
fn base64_decode(s: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    // Strip ASCII whitespace (Spark trims spaces inside the token).
    let cleaned: Vec<u8> = s.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    if cleaned.is_empty() {
        return Some(Vec::new());
    }
    // Split off padding.
    let mut data = cleaned.as_slice();
    let mut pad = 0usize;
    while data.last() == Some(&b'=') {
        pad += 1;
        data = &data[..data.len() - 1];
    }
    if pad > 2 {
        return None;
    }
    // After stripping '=', no '=' may remain and length+pad must be a multiple of 4.
    if data.contains(&b'=') {
        return None;
    }
    if (data.len() + pad) % 4 != 0 {
        return None;
    }
    // The number of remaining data chars in the final group must be consistent with the padding.
    let rem = data.len() % 4;
    match (rem, pad) {
        (0, 0) | (3, 1) | (2, 2) => {}
        _ => return None,
    }
    let mut out = Vec::with_capacity(data.len() / 4 * 3 + 3);
    let mut chunk = data.chunks_exact(4);
    for c in chunk.by_ref() {
        let a = val(c[0])?;
        let b = val(c[1])?;
        let cc = val(c[2])?;
        let d = val(c[3])?;
        let triple = (a as u32) << 18 | (b as u32) << 12 | (cc as u32) << 6 | d as u32;
        out.push((triple >> 16) as u8);
        out.push((triple >> 8) as u8);
        out.push(triple as u8);
    }
    let tail = chunk.remainder();
    match tail.len() {
        0 => {}
        2 => {
            let a = val(tail[0])?;
            let b = val(tail[1])?;
            let triple = (a as u32) << 18 | (b as u32) << 12;
            out.push((triple >> 16) as u8);
        }
        3 => {
            let a = val(tail[0])?;
            let b = val(tail[1])?;
            let cc = val(tail[2])?;
            let triple = (a as u32) << 18 | (b as u32) << 12 | (cc as u32) << 6;
            out.push((triple >> 16) as u8);
            out.push((triple >> 8) as u8);
        }
        _ => return None,
    }
    Some(out)
}

/// Convert one string to bytes using a (case-insensitive) format. `Ok(None)` is unreachable; an
/// `Err(())` indicates malformed input for the chosen format.
fn to_binary_one(s: &str, fmt: &str) -> std::result::Result<Vec<u8>, ()> {
    match fmt.to_ascii_lowercase().as_str() {
        "hex" => hex_to_bytes(s).ok_or(()),
        "utf-8" | "utf8" => Ok(s.as_bytes().to_vec()),
        "base64" => base64_decode(s).ok_or(()),
        _ => Err(()),
    }
}

impl ScalarUDFImpl for ToBinary {
    fn name(&self) -> &str {
        if self.try_mode {
            "try_to_binary"
        } else {
            "to_binary"
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let nargs = args.args.len();
        if !(1..=2).contains(&nargs) {
            return exec_err!("{}: expected 1 or 2 arguments, got {nargs}", self.name());
        }
        let input = args.args[0].clone().into_array(n)?;
        let input = datafusion::arrow::compute::cast(&input, &DataType::Utf8).map_err(arrow_err)?;
        let input = input.as_any().downcast_ref::<StringArray>().unwrap();

        let fmt_arr: Option<ArrayRef> = if nargs == 2 {
            let f = args.args[1].clone().into_array(n)?;
            Some(datafusion::arrow::compute::cast(&f, &DataType::Utf8).map_err(arrow_err)?)
        } else {
            None
        };
        let fmt_arr = fmt_arr
            .as_ref()
            .map(|a| a.as_any().downcast_ref::<StringArray>().unwrap());

        let mut out = BinaryBuilder::new();
        for i in 0..n {
            let fmt = match fmt_arr {
                None => "hex",
                Some(a) => {
                    if a.is_null(i) {
                        // NULL format => NULL result.
                        out.append_null();
                        continue;
                    }
                    a.value(i)
                }
            };
            if input.is_null(i) {
                out.append_null();
                continue;
            }
            match to_binary_one(input.value(i), fmt) {
                Ok(bytes) => out.append_value(bytes),
                Err(()) => {
                    if self.try_mode {
                        out.append_null();
                    } else {
                        return exec_err!(
                            "to_binary: cannot convert `{}` to binary with format `{}`",
                            input.value(i),
                            fmt
                        );
                    }
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// current_database / current_schema / current_catalog
// ---------------------------------------------------------------------------

/// `current_database()` / `current_schema()` / `current_catalog()` — constant scalars matching
/// Spark's defaults (`default` / `default` / `spark_catalog`).
#[derive(Debug, PartialEq, Eq, Hash)]
struct CurrentName {
    name: &'static str,
    value: &'static str,
    signature: Signature,
}

impl CurrentName {
    fn new(name: &'static str, value: &'static str) -> Self {
        Self {
            name,
            value,
            signature: Signature::nullary(Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for CurrentName {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            self.value.to_string(),
        ))))
    }
}

// ---------------------------------------------------------------------------
// assert_true
// ---------------------------------------------------------------------------

/// `assert_true(expr [, msg])` — `NULL` when `expr` is `true`; raise a runtime error otherwise
/// (including when `expr` is `NULL`).
#[derive(Debug, PartialEq, Eq, Hash)]
struct AssertTrue {
    signature: Signature,
}

impl AssertTrue {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for AssertTrue {
    fn name(&self) -> &str {
        "assert_true"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Null)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let nargs = args.args.len();
        if !(1..=2).contains(&nargs) {
            return exec_err!("assert_true: expected 1 or 2 arguments, got {nargs}");
        }
        let cond = args.args[0].clone().into_array(n)?;
        let cond =
            datafusion::arrow::compute::cast(&cond, &DataType::Boolean).map_err(arrow_err)?;
        let cond = datafusion::arrow::array::cast::as_boolean_array(&cond);

        // Optional message column.
        let msg_arr = if nargs == 2 {
            let m = args.args[1].clone().into_array(n)?;
            Some(datafusion::arrow::compute::cast(&m, &DataType::Utf8).map_err(arrow_err)?)
        } else {
            None
        };
        let msg_arr = msg_arr
            .as_ref()
            .map(|a| a.as_any().downcast_ref::<StringArray>().unwrap());

        for i in 0..n {
            let ok = !cond.is_null(i) && cond.value(i);
            if !ok {
                let msg = match msg_arr {
                    Some(a) if !a.is_null(i) => a.value(i).to_string(),
                    _ => "'false' is not true!".to_string(),
                };
                return Err(DataFusionError::Execution(msg));
            }
        }
        // All rows satisfied the predicate: Spark returns NULL.
        Ok(ColumnarValue::Scalar(ScalarValue::Null))
    }
}

// ---------------------------------------------------------------------------
// replace (2-arg Spark variant + 3-arg passthrough)
// ---------------------------------------------------------------------------

/// `replace(str, search [, replace])` — Spark allows the 2-arg form (remove all occurrences of
/// `search`), unlike DataFusion's builtin which requires 3 args. We register a variadic `replace`
/// covering both arities.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Replace {
    signature: Signature,
}

impl Replace {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Replace {
    fn name(&self) -> &str {
        "replace"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let nargs = args.args.len();
        if !(2..=3).contains(&nargs) {
            return exec_err!("replace: expected 2 or 3 arguments, got {nargs}");
        }
        let to_str = |cv: &ColumnarValue| -> Result<ArrayRef> {
            let a = cv.clone().into_array(n)?;
            datafusion::arrow::compute::cast(&a, &DataType::Utf8).map_err(arrow_err)
        };
        let src = to_str(&args.args[0])?;
        let search = to_str(&args.args[1])?;
        let src = src.as_any().downcast_ref::<StringArray>().unwrap();
        let search = search.as_any().downcast_ref::<StringArray>().unwrap();
        let repl = if nargs == 3 {
            Some(to_str(&args.args[2])?)
        } else {
            None
        };
        let repl = repl
            .as_ref()
            .map(|a| a.as_any().downcast_ref::<StringArray>().unwrap());

        let mut out = StringBuilder::new();
        for i in 0..n {
            if src.is_null(i) || search.is_null(i) || repl.map(|r| r.is_null(i)).unwrap_or(false) {
                out.append_null();
                continue;
            }
            let r = repl.map(|r| r.value(i)).unwrap_or("");
            // Spark: replacing an empty search string returns the source unchanged.
            if search.value(i).is_empty() {
                out.append_value(src.value(i));
            } else {
                out.append_value(src.value(i).replace(search.value(i), r));
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
    async fn hex_integral_string_binary() {
        // integral: two's-complement bigint, uppercase, no leading zeros.
        assert!(run("SELECT hex(CAST(0 AS BIGINT)) AS x")
            .await
            .contains("| 0"));
        assert!(run("SELECT hex(CAST(255 AS BIGINT)) AS x")
            .await
            .contains("FF"));
        assert!(run("SELECT hex(CAST(-1 AS BIGINT)) AS x")
            .await
            .contains("FFFFFFFFFFFFFFFF"));
        // string bytes.
        assert!(run("SELECT hex('ABC') AS x").await.contains("414243"));
        // round-trips through unhex.
        assert!(run("SELECT hex(unhex('aabb')) AS x").await.contains("AABB"));
    }

    #[tokio::test]
    async fn unhex_basic_and_invalid() {
        // 'string' is the ascii for 0x737472696E67.
        assert!(run("SELECT CAST(unhex('737472696E67') AS STRING) AS x")
            .await
            .contains("string"));
        // odd length left-pads: '123' -> 0x01 0x23 -> hex '0123'.
        assert!(run("SELECT hex(unhex('123')) AS x").await.contains("0123"));
        // invalid hex -> NULL.
        assert!(run("SELECT unhex('GG') AS x").await.contains("|   |"));
    }

    #[tokio::test]
    async fn to_binary_formats() {
        // hex default and explicit.
        assert!(run("SELECT CAST(to_binary('737472696E67') AS STRING) AS x")
            .await
            .contains("string"));
        assert!(
            run("SELECT CAST(to_binary('737472696E67', 'hex') AS STRING) AS x")
                .await
                .contains("string")
        );
        // utf-8 round-trips the text.
        assert!(run("SELECT CAST(to_binary('abc', 'utf-8') AS STRING) AS x")
            .await
            .contains("abc"));
        // base64: ' ab cd ' (whitespace ignored) decodes to bytes 0x69 0xB7 -> hex '69B7'.
        assert!(run("SELECT hex(to_binary(' ab cd ', 'base64')) AS x")
            .await
            .contains("69B7"));
        // case-insensitive format.
        assert!(!run("SELECT to_binary('abc', 'Hex') AS x").await.is_empty());
        // NULL inputs.
        assert!(run("SELECT to_binary('abc', NULL) AS x")
            .await
            .contains("|   |"));
        assert!(run("SELECT to_binary(CAST(NULL AS STRING), 'utf-8') AS x")
            .await
            .contains("|   |"));
    }

    #[tokio::test]
    async fn to_binary_invalid_errors_try_nulls() {
        let engine = Engine::new();
        assert!(engine.sql("SELECT to_binary('GG')").await.is_err());
        assert!(engine.sql("SELECT to_binary('a', 'base64')").await.is_err());
        // try_to_binary returns NULL instead.
        assert!(run("SELECT try_to_binary('GG') AS x")
            .await
            .contains("|   |"));
        assert!(run("SELECT try_to_binary('a', 'base64') AS x")
            .await
            .contains("|   |"));
    }

    #[tokio::test]
    async fn current_names() {
        assert!(run("SELECT current_database() AS x")
            .await
            .contains("default"));
        assert!(run("SELECT current_schema() AS x")
            .await
            .contains("default"));
        assert!(run("SELECT current_catalog() AS x")
            .await
            .contains("spark_catalog"));
    }

    #[tokio::test]
    async fn assert_true_semantics() {
        // true -> NULL (no error).
        assert!(run("SELECT assert_true(true) AS x").await.contains("|   |"));
        let engine = Engine::new();
        assert!(engine.sql("SELECT assert_true(false)").await.is_err());
        assert!(engine
            .sql("SELECT assert_true(CAST(NULL AS BOOLEAN))")
            .await
            .is_err());
        // custom message.
        let e = engine
            .sql("SELECT assert_true(false, 'custom error message')")
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("custom error message"), "{e}");
    }

    #[tokio::test]
    async fn replace_two_and_three_args() {
        // 2-arg: removes all occurrences.
        assert!(run("SELECT replace('abc', 'b') AS x").await.contains("ac"));
        // 3-arg: substitutes.
        assert!(run("SELECT replace('abc', 'b', '123') AS x")
            .await
            .contains("a123c"));
    }
}
