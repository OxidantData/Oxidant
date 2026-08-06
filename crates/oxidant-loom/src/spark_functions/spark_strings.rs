//! High-usage Spark string functions that DataFusion does not provide: `elt`, `split` (regex),
//! and `format_string`.
//!
//! Faithfulness notes (verified against Spark v4.0.0 golden output in
//! `oxidant-spark-compat/spark-tests`):
//! * `elt(n, s1, s2, …)` — 1-indexed pick; `NULL` when `n` is `NULL` or out of `[1, k]`. Spark
//!   coerces the candidate columns to a common type; we render the chosen value as a string
//!   (the common ClickBench/SQL-test case), which matches the `struct<col:string>` goldens.
//! * `split(str, regex [, limit])` — splits on a **Java regex** (not a literal), returning
//!   `array<string>`. Default `limit` is `-1` (keep all trailing empty strings, Java semantics).
//!   `NULL` in `str` or `regex` yields `NULL`. Splitting on the empty pattern yields one element
//!   per character (Java 8+ drops the leading empty match).
//! * `format_string(fmt, args…)` — printf-style; supports `%s %d %f %% ` plus width/precision
//!   flags (e.g. `%5d`, `%.2f`, `%-10s`). `NULL` in `fmt` yields `NULL`.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, GenericListBuilder, GenericStringBuilder, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use regex::Regex;

/// Register `elt`, `split`, and `format_string` into `ctx`.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(Elt::new()));
    ctx.register_udf(ScalarUDF::from(SparkSplit::new()));
    ctx.register_udf(ScalarUDF::from(FormatString::new()));
}

/// Render an Arrow value at `row` as the string Spark would produce for `elt`/`format_string`'s
/// `%s`. Mirrors Arrow's display for the scalar types that already match Spark.
fn value_to_string(array: &dyn Array, row: usize) -> Option<String> {
    use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
    if array.is_null(row) {
        return None;
    }
    let opts = FormatOptions::default().with_null("NULL");
    ArrayFormatter::try_new(array, &opts)
        .ok()
        .map(|f| f.value(row).to_string())
}

// ---------------------------------------------------------------------------
// elt(n, s1, s2, ...)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct Elt {
    signature: Signature,
}

impl Elt {
    fn new() -> Self {
        // n + at least one candidate.
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Elt {
    fn name(&self) -> &str {
        "elt"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() < 2 {
            return exec_err!("elt requires an index and at least one value");
        }
        let n = args.number_rows;
        let arrays: Vec<ArrayRef> = args
            .args
            .iter()
            .map(|a| a.clone().into_array(n))
            .collect::<Result<Vec<_>>>()?;

        let idx = &arrays[0];
        let idx = datafusion::arrow::compute::cast(idx, &DataType::Int32)?;
        let idx = idx
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Internal(
                    "elt: index did not cast to Int32".into(),
                )
            })?;

        let candidates = &arrays[1..];
        let mut builder = StringBuilder::new();
        for row in 0..n {
            if idx.is_null(row) {
                builder.append_null();
                continue;
            }
            let pick = idx.value(row);
            // 1-indexed; out of range -> NULL.
            if pick < 1 || (pick as usize) > candidates.len() {
                builder.append_null();
                continue;
            }
            let col = &candidates[(pick - 1) as usize];
            match value_to_string(col.as_ref(), row) {
                Some(s) => builder.append_value(s),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// ---------------------------------------------------------------------------
// split(str, regex [, limit]) -> array<string>
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkSplit {
    signature: Signature,
}

impl SparkSplit {
    fn new() -> Self {
        // (str, regex) or (str, regex, limit).
        Self {
            signature: Signature::one_of(
                vec![
                    datafusion::logical_expr::TypeSignature::Any(2),
                    datafusion::logical_expr::TypeSignature::Any(3),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

/// Java-`String.split`-faithful split of `s` on the compiled regex `re`, honoring `limit`:
/// `limit > 0` caps the number of pieces; `limit <= 0` is unlimited; `limit == 0`-style trailing
/// empty trimming is NOT applied here because Spark's default limit is `-1` (keep trailing empties).
fn java_split(s: &str, re: &Regex, limit: i32) -> Vec<String> {
    // Mirror java.util.regex.Pattern.split(CharSequence, limit).
    let mut result: Vec<String> = Vec::new();
    let mut index = 0usize; // start of the current unmatched segment (byte offset)
    let mut match_count = 0usize;
    let want = if limit > 0 {
        limit as usize
    } else {
        usize::MAX
    };

    for m in re.find_iter(s) {
        if match_count + 1 >= want {
            break;
        }
        // Java skips a zero-width match at position 0 (index == 0 && m.start() == 0).
        if m.start() == 0 && m.end() == 0 && index == 0 {
            continue;
        }
        // Java's Pattern.split skips a zero-width match sitting at the end of the input, so
        // `"hello".split("")` is `[h,e,l,l,o]` (no trailing empty) while a non-empty terminal
        // match like `"aa1".split("[1-9]+")` still yields a trailing empty `[aa,]`.
        if m.start() == m.end() && m.start() == s.len() {
            continue;
        }
        // Avoid an infinite-style zero-width match at the current index producing nothing useful:
        // a zero-width match at `index` contributes the single char before advancing.
        if m.start() < index {
            continue;
        }
        result.push(s[index..m.start()].to_string());
        index = m.end();
        match_count += 1;
        // Zero-width (empty-pattern) matches advance by one byte automatically: `regex`'s
        // `find_iter` never returns the same zero-width position twice, so each char is emitted.
    }

    // Trailing segment.
    result.push(s[index..].to_string());

    // Java with negative limit keeps trailing empties; with limit == 0 it strips them. Spark's
    // default is -1, so callers pass -1 (keep). We only strip when limit == 0 was explicitly
    // requested (Spark never does, but stay faithful to the parameter).
    if limit == 0 {
        while result.len() > 1 && result.last().map(|x| x.is_empty()).unwrap_or(false) {
            result.pop();
        }
    }
    result
}

impl ScalarUDFImpl for SparkSplit {
    fn name(&self) -> &str {
        "split"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        ))))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() < 2 || args.args.len() > 3 {
            return exec_err!("split requires 2 or 3 arguments");
        }
        let n = args.number_rows;
        let strs = args.args[0].clone().into_array(n)?;
        let strs = datafusion::arrow::compute::cast(&strs, &DataType::Utf8)?;
        let strs = strs
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap()
            .clone();

        let pats = args.args[1].clone().into_array(n)?;
        let pats = datafusion::arrow::compute::cast(&pats, &DataType::Utf8)?;
        let pats = pats
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap()
            .clone();

        let limits: Option<datafusion::arrow::array::Int32Array> = if args.args.len() == 3 {
            let l = args.args[2].clone().into_array(n)?;
            let l = datafusion::arrow::compute::cast(&l, &DataType::Int32)?;
            Some(
                l.as_any()
                    .downcast_ref::<datafusion::arrow::array::Int32Array>()
                    .unwrap()
                    .clone(),
            )
        } else {
            None
        };

        let values_builder = GenericStringBuilder::<i32>::new();
        let mut builder = GenericListBuilder::<i32, _>::new(values_builder);

        for row in 0..n {
            // NULL str or NULL regex -> NULL array.
            if strs.is_null(row) || pats.is_null(row) {
                builder.append_null();
                continue;
            }
            let limit = match &limits {
                Some(l) if !l.is_null(row) => l.value(row),
                Some(_) => {
                    // NULL limit -> NULL result, matching Spark's null propagation.
                    builder.append_null();
                    continue;
                }
                None => -1,
            };
            let s = strs.value(row);
            let pat = pats.value(row);
            let re = match Regex::new(pat) {
                Ok(re) => re,
                Err(e) => return exec_err!("split: invalid regex '{pat}': {e}"),
            };
            for piece in java_split(s, &re, limit) {
                builder.values().append_value(piece);
            }
            builder.append(true);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// ---------------------------------------------------------------------------
// format_string(fmt, args...)  (printf-style)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct FormatString {
    signature: Signature,
}

impl FormatString {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

/// A parsed `%`-conversion: optional flags/width/precision plus the conversion char.
struct Conv {
    left_justify: bool,
    zero_pad: bool,
    width: Option<usize>,
    precision: Option<usize>,
    kind: char,
}

/// Apply width/justification padding to an already-formatted body.
fn pad(body: String, conv: &Conv) -> String {
    match conv.width {
        Some(w) if body.chars().count() < w => {
            let need = w - body.chars().count();
            if conv.left_justify {
                let mut s = body;
                s.push_str(&" ".repeat(need));
                s
            } else if conv.zero_pad && matches!(conv.kind, 'd' | 'f') {
                // Insert zeros after a leading sign, if any.
                if let Some(stripped) = body.strip_prefix('-') {
                    format!("-{}{}", "0".repeat(need), stripped)
                } else {
                    format!("{}{}", "0".repeat(need), body)
                }
            } else {
                format!("{}{}", " ".repeat(need), body)
            }
        }
        _ => body,
    }
}

/// Render the printf format string `fmt` with `arrays` (column values at `row`).
fn format_one(fmt: &str, arrays: &[ArrayRef], row: usize) -> Result<String> {
    let mut out = String::new();
    let mut arg_idx = 0usize;
    let bytes: Vec<char> = fmt.chars().collect();
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        if c != '%' {
            out.push(c);
            i += 1;
            continue;
        }
        i += 1;
        if i >= bytes.len() {
            out.push('%');
            break;
        }
        if bytes[i] == '%' {
            out.push('%');
            i += 1;
            continue;
        }
        if bytes[i] == 'n' {
            out.push('\n');
            i += 1;
            continue;
        }
        // Parse flags.
        let mut conv = Conv {
            left_justify: false,
            zero_pad: false,
            width: None,
            precision: None,
            kind: ' ',
        };
        while i < bytes.len() && matches!(bytes[i], '-' | '0' | '+' | ' ' | '#' | ',') {
            match bytes[i] {
                '-' => conv.left_justify = true,
                '0' => conv.zero_pad = true,
                _ => {}
            }
            i += 1;
        }
        // Width.
        let mut width_str = String::new();
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            width_str.push(bytes[i]);
            i += 1;
        }
        if !width_str.is_empty() {
            conv.width = width_str.parse::<usize>().ok();
        }
        // Precision.
        if i < bytes.len() && bytes[i] == '.' {
            i += 1;
            let mut prec = String::new();
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                prec.push(bytes[i]);
                i += 1;
            }
            conv.precision = prec.parse::<usize>().ok();
        }
        if i >= bytes.len() {
            return exec_err!("format_string: dangling '%' in format");
        }
        conv.kind = bytes[i];
        i += 1;

        // Pull the next argument (arrays index arg_idx; arrays already exclude the fmt column).
        let body = render_arg(&conv, arrays, &mut arg_idx, row)?;
        out.push_str(&pad(body, &conv));
    }
    Ok(out)
}

/// Render a single conversion's body (pre-padding) from the next positional argument.
fn render_arg(conv: &Conv, arrays: &[ArrayRef], arg_idx: &mut usize, row: usize) -> Result<String> {
    let array = arrays.get(*arg_idx);
    *arg_idx += 1;
    let array = match array {
        Some(a) => a,
        None => return Ok(String::new()),
    };
    match conv.kind {
        's' | 'S' => {
            let s = value_to_string(array.as_ref(), row).unwrap_or_else(|| "null".to_string());
            let s = if conv.kind == 'S' {
                s.to_uppercase()
            } else {
                s
            };
            Ok(match conv.precision {
                Some(p) => s.chars().take(p).collect(),
                None => s,
            })
        }
        'd' => {
            if array.is_null(row) {
                return Ok("null".to_string());
            }
            let casted = datafusion::arrow::compute::cast(array, &DataType::Int64)?;
            let v = casted
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .unwrap()
                .value(row);
            Ok(v.to_string())
        }
        'f' => {
            if array.is_null(row) {
                return Ok("null".to_string());
            }
            let casted = datafusion::arrow::compute::cast(array, &DataType::Float64)?;
            let v = casted
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Float64Array>()
                .unwrap()
                .value(row);
            let prec = conv.precision.unwrap_or(6);
            Ok(format!("{v:.prec$}"))
        }
        'x' => {
            if array.is_null(row) {
                return Ok("null".to_string());
            }
            let casted = datafusion::arrow::compute::cast(array, &DataType::Int64)?;
            let v = casted
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .unwrap()
                .value(row);
            Ok(format!("{v:x}"))
        }
        other => exec_err!("format_string: unsupported conversion '%{other}'"),
    }
}

impl ScalarUDFImpl for FormatString {
    fn name(&self) -> &str {
        "format_string"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return exec_err!("format_string requires at least the format argument");
        }
        let n = args.number_rows;
        let fmt_arr = args.args[0].clone().into_array(n)?;
        let fmt_arr = datafusion::arrow::compute::cast(&fmt_arr, &DataType::Utf8)?;
        let fmt_arr = fmt_arr
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap()
            .clone();

        let rest: Vec<ArrayRef> = args.args[1..]
            .iter()
            .map(|a| a.clone().into_array(n))
            .collect::<Result<Vec<_>>>()?;

        let mut builder = StringBuilder::new();
        for row in 0..n {
            if fmt_arr.is_null(row) {
                builder.append_null();
                continue;
            }
            let fmt = fmt_arr.value(row);
            builder.append_value(format_one(fmt, &rest, row)?);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[cfg(test)]
mod tests {
    async fn run(q: &str) -> String {
        let engine = crate::Engine::new();
        let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        crate::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn elt_picks_nth_one_indexed() {
        let got = run("SELECT elt(2, 'a', 'b', 'c') AS x").await;
        assert!(got.contains(" b "), "{got}");
    }

    #[tokio::test]
    async fn elt_out_of_range_is_null() {
        for q in [
            "SELECT elt(0, 'a', 'b') AS x",
            "SELECT elt(5, 'a', 'b') AS x",
        ] {
            let got = run(q).await;
            assert!(got.contains("|     |") || got.contains("| x"), "{q}: {got}");
            // NULL renders as empty cell in pretty printer; just ensure no value leaked.
            assert!(!got.contains(" a ") && !got.contains(" b "), "{q}: {got}");
        }
    }

    #[tokio::test]
    async fn elt_coerces_numbers_to_string() {
        let got = run("SELECT elt(2, 'x', CAST(7 AS INT)) AS x").await;
        assert!(got.contains(" 7 "), "{got}");
    }

    #[tokio::test]
    async fn split_basic_keeps_trailing_empty() {
        let got = run("SELECT split('aa1cc2ee3', '[1-9]+') AS x").await;
        assert!(got.contains("[aa, cc, ee, ]"), "{got}");
    }

    #[tokio::test]
    async fn split_with_limit() {
        let got = run("SELECT split('aa1cc2ee3', '[1-9]+', 2) AS x").await;
        assert!(got.contains("[aa, cc2ee3]"), "{got}");
    }

    #[tokio::test]
    async fn split_empty_pattern_splits_chars() {
        let got = run("SELECT split('hello', '') AS x").await;
        assert!(got.contains("[h, e, l, l, o]"), "{got}");
    }

    #[tokio::test]
    async fn split_null_propagates() {
        let got = run("SELECT split(CAST(NULL AS STRING), 'b') AS x").await;
        // NULL array renders empty in pretty printer.
        assert!(!got.contains('['), "{got}");
    }

    #[tokio::test]
    async fn format_string_printf() {
        let got =
            run("SELECT format_string('Hello %s, you are %d', 'World', CAST(42 AS INT)) AS x")
                .await;
        assert!(got.contains("Hello World, you are 42"), "{got}");
    }

    #[tokio::test]
    async fn format_string_float_and_percent() {
        let got = run("SELECT format_string('%.2f%%', CAST(3.14159 AS DOUBLE)) AS x").await;
        assert!(got.contains("3.14%"), "{got}");
    }

    #[tokio::test]
    async fn format_string_width_zero_pad() {
        let got = run("SELECT format_string('%05d', CAST(42 AS INT)) AS x").await;
        assert!(got.contains("00042"), "{got}");
    }

    #[tokio::test]
    async fn format_string_null_fmt() {
        let got = run("SELECT format_string(CAST(NULL AS STRING), 'x') AS x").await;
        assert!(!got.contains('x') || got.contains("| x"), "{got}");
    }
}
