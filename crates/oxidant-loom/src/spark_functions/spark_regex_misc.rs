//! Spark regex + miscellaneous string scalar functions that DataFusion does not provide.
//!
//! Implemented faithfully against Apache Spark v4.0.0 semantics (see
//! `oxidant-spark-compat/spark-tests/{inputs,results}/regexp-functions.sql*` and the
//! `substring_index` / `find_in_set` cases in `collations.sql`):
//!
//! - `regexp_extract(str, regex [, idx])` — return the `idx`-th capture group of the **first**
//!   match (`idx = 0` is the whole match, default `idx = 1`). When the regex matches but the
//!   requested group did not participate, Spark returns the empty string `''`; when the regex does
//!   not match at all, Spark also returns `''`. `idx` must be in `[0, groupCount]`, otherwise Spark
//!   raises `INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX`; an invalid pattern raises
//!   `INVALID_PARAMETER_VALUE.PATTERN`. Returns `string`.
//! - `regexp_extract_all(str, regex [, idx])` — the same, but over **all** non-overlapping matches,
//!   returning `array<string>` (one element per match, `''` for a non-participating group).
//! - `substring_index(str, delim, count)` — the substring before the `count`-th occurrence of
//!   `delim`. `count > 0` counts from the left and keeps everything before it; `count < 0` counts
//!   from the right and keeps everything after it; `count = 0` yields `''`. If `delim` is empty or
//!   `|count|` exceeds the number of occurrences, the whole string is returned. Matching on `delim`
//!   is by literal (byte) substring. Returns `string`.
//! - `find_in_set(str, strList)` — 1-based index of `str` in the comma-separated `strList`, or `0`
//!   if absent. Spark returns `0` when `str` itself contains a comma. Returns `int`.
//!
//! `str_to_map` is intentionally deferred: its only goldens are collation-typed (`collations.sql`)
//! and it returns a `map<string,string>`, which has no other consumer in the harness yet; getting
//! the map-type plumbing exactly right is not worth the risk here.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, GenericListBuilder, GenericStringBuilder, Int32Array, StringArray,
    StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use regex::Regex;

/// Register the regex + misc string Spark functions into `ctx`.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(RegexpExtract::new()));
    ctx.register_udf(ScalarUDF::from(RegexpExtractAll::new()));
    ctx.register_udf(ScalarUDF::from(RegexpSubstr::new()));
    ctx.register_udf(ScalarUDF::from(RegexpCount::new()));
    ctx.register_udf(ScalarUDF::from(RegexpInstr::new()));
    // `regexp_like` and its aliases `regexp` / `rlike` (when invoked as a function) are the same
    // boolean "does the regex match anywhere" predicate.
    ctx.register_udf(ScalarUDF::from(RegexpLike::new("regexp_like")));
    ctx.register_udf(ScalarUDF::from(RegexpLike::new("regexp")));
    ctx.register_udf(ScalarUDF::from(RegexpLike::new("rlike")));
    // Shadows DataFusion's builtin `regexp_replace`, whose semantics diverge from Spark on three
    // counts (no `\\`→`\` unescape, first-match-only, and a `flags` 4th arg where Spark has a
    // 1-based `position`). `RegexpReplace` reimplements Spark's documented contract faithfully.
    ctx.register_udf(ScalarUDF::from(RegexpReplace::new()));
    ctx.register_udf(ScalarUDF::from(SubstringIndex::new()));
    ctx.register_udf(ScalarUDF::from(FindInSet::new()));
}

/// Cast a `ColumnarValue` to a materialized `Utf8` array of `n` rows.
fn to_str_array(cv: &ColumnarValue, n: usize) -> Result<ArrayRef> {
    let a = cv.clone().into_array(n)?;
    Ok(datafusion::arrow::compute::cast(&a, &DataType::Utf8)?)
}

/// Cast a `ColumnarValue` to a materialized `Int32` array of `n` rows.
fn to_i32_array(cv: &ColumnarValue, n: usize) -> Result<ArrayRef> {
    let a = cv.clone().into_array(n)?;
    Ok(datafusion::arrow::compute::cast(&a, &DataType::Int32)?)
}

/// Compile a Spark regex, mapping a compile failure onto Spark's `PATTERN` error wording.
///
/// The pattern arrives already decoded: Spark unescapes string literals at parse time, which oxidant
/// reproduces in `normalize_spark_sql`'s `unescape_spark_string_literals` (so a SQL literal `'\\d+'`
/// reaches here as `\d+`, exactly what Spark's regex engine sees). A pattern sourced from a column is
/// runtime data that Spark does *not* unescape, so it is likewise used verbatim. We therefore do
/// **no** backslash collapsing here: doing it a second time would corrupt a pattern that
/// legitimately contains an escaped backslash (`\\`, i.e. a literal backslash).
fn compile_pattern(func: &str, pat: &str) -> Result<Regex> {
    Regex::new(pat).map_err(|_| {
        datafusion::common::DataFusionError::Execution(format!(
            "[INVALID_PARAMETER_VALUE.PATTERN] The value of parameter(s) `regexp` in `{func}` is \
             invalid: '{pat}'"
        ))
    })
}

/// Validate `idx` against the capture-group count, raising Spark's `REGEX_GROUP_INDEX` error when
/// out of `[0, group_count]`. Spark treats a *missing* `idx` as `1`, so even a zero-group regex
/// errors when no explicit `idx` is given — but here `idx` is always resolved before the call.
fn check_group_index(func: &str, idx: i32, group_count: usize) -> Result<()> {
    if idx < 0 || idx as usize > group_count {
        return exec_err!(
            "[INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX] The value of parameter(s) `idx` in \
             `{func}` is invalid: expected a group index between 0 and {group_count}, got {idx}"
        );
    }
    Ok(())
}

/// Enumerate the `group`-th capture across every match the way Java's `Matcher.find()` does —
/// which is what Spark's `regexp_extract_all` is built on. Unlike Rust's `captures_iter`, Java
/// re-anchors the search at every position when the previous match was empty, so a nullable
/// pattern like `(\d+)?` over `"1a 2b 14m"` yields one entry per gap (`["1","","","2", …]`). A
/// non-participating group contributes the empty string.
fn java_find_all(re: &Regex, text: &str, group: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let len = text.len();
    while let Some(caps) = re.captures_at(text, start) {
        let whole = caps.get(0).unwrap();
        let empty = whole.end() == whole.start();
        let g = caps
            .get(group)
            .map(|m| m.as_str().to_string())
            .unwrap_or_default();
        out.push(g);
        // Advance past the match, or one char forward for a zero-width match. A zero-width match
        // anchored at end-of-input (start == len) is still emitted (matching Java's
        // `Matcher.find()`), after which `next_char_boundary` pushes us past `len` to stop.
        start = if empty {
            next_char_boundary(text, whole.end())
        } else {
            whole.end()
        };
        if start > len {
            break;
        }
    }
    out
}

/// Byte index of the char boundary immediately after `i` (or `i + 1` past the end).
fn next_char_boundary(text: &str, i: usize) -> usize {
    if i >= text.len() {
        return i + 1;
    }
    let mut j = i + 1;
    while j < text.len() && !text.is_char_boundary(j) {
        j += 1;
    }
    j
}

// ---------------------------------------------------------------------------
// regexp_extract(str, regex [, idx])
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct RegexpExtract {
    signature: Signature,
}

impl RegexpExtract {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(2), TypeSignature::Any(3)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpExtract {
    fn name(&self) -> &str {
        "regexp_extract"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        if !(2..=3).contains(&args.args.len()) {
            return exec_err!("regexp_extract: expected 2 or 3 arguments");
        }
        let strs = to_str_array(&args.args[0], n)?;
        let pats = to_str_array(&args.args[1], n)?;
        let strs = strs.as_any().downcast_ref::<StringArray>().unwrap();
        let pats = pats.as_any().downcast_ref::<StringArray>().unwrap();
        let idxs = match args.args.get(2) {
            Some(a) => Some(to_i32_array(a, n)?),
            None => None,
        };
        let idxs = idxs
            .as_ref()
            .map(|a| a.as_any().downcast_ref::<Int32Array>().unwrap());

        let mut out = StringBuilder::new();
        for row in 0..n {
            // NULL str or NULL regex propagates to NULL.
            if strs.is_null(row) || pats.is_null(row) {
                out.append_null();
                continue;
            }
            // A NULL explicit idx also propagates to NULL (matches Spark).
            let idx = match idxs {
                Some(a) if a.is_null(row) => {
                    out.append_null();
                    continue;
                }
                Some(a) => a.value(row),
                None => 1,
            };
            let re = compile_pattern("regexp_extract", pats.value(row))?;
            check_group_index("regexp_extract", idx, re.captures_len() - 1)?;
            let extracted = match re.captures(strs.value(row)) {
                Some(caps) => caps
                    .get(idx as usize)
                    .map(|m| m.as_str().to_string())
                    .unwrap_or_default(),
                None => String::new(),
            };
            out.append_value(extracted);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// regexp_substr(str, regex) -> string
// ---------------------------------------------------------------------------

/// Spark `regexp_substr(str, regexp)` — the **first** substring of `str` matching `regexp` (the
/// whole match, i.e. group 0), or `NULL` when there is no match. This is the one behavioral
/// difference from `regexp_extract(str, regexp, 0)`, which returns `''` on no match; `regexp_substr`
/// returns `NULL`. A NULL `str` or `regexp` propagates to NULL; an invalid pattern raises Spark's
/// `INVALID_PARAMETER_VALUE.PATTERN`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct RegexpSubstr {
    signature: Signature,
}

impl RegexpSubstr {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for RegexpSubstr {
    fn name(&self) -> &str {
        "regexp_substr"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        if args.args.len() != 2 {
            return exec_err!("regexp_substr: expected 2 arguments");
        }
        let strs = to_str_array(&args.args[0], n)?;
        let pats = to_str_array(&args.args[1], n)?;
        let strs = strs.as_any().downcast_ref::<StringArray>().unwrap();
        let pats = pats.as_any().downcast_ref::<StringArray>().unwrap();

        let mut out = StringBuilder::new();
        for row in 0..n {
            if strs.is_null(row) || pats.is_null(row) {
                out.append_null();
                continue;
            }
            let pat = pats.value(row);
            // Spark returns NULL for an empty pattern (rather than the zero-width match an empty
            // regex would otherwise yield) — see `regexp_substr('1a 2b 14m', '')` → NULL.
            if pat.is_empty() {
                out.append_null();
                continue;
            }
            let re = compile_pattern("regexp_substr", pat)?;
            // The leftmost whole match (group 0); no match → NULL (not `''`).
            match re.find(strs.value(row)) {
                Some(m) => out.append_value(m.as_str()),
                None => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// regexp_count(str, regex) -> int   ·   regexp_instr(str, regex) -> int
// regexp_like / regexp / rlike (str, regex) -> boolean
// ---------------------------------------------------------------------------

/// Materialize the (str, regexp) operands of a 2-arg regex predicate as `Utf8` arrays.
fn regex_pair(args: &ScalarFunctionArgs, func: &str) -> Result<(ArrayRef, ArrayRef)> {
    if args.args.len() != 2 {
        return exec_err!("{func}: expected 2 arguments");
    }
    let n = args.number_rows;
    Ok((
        to_str_array(&args.args[0], n)?,
        to_str_array(&args.args[1], n)?,
    ))
}

/// Spark `regexp_count(str, regexp)` — the number of non-overlapping matches of `regexp` in `str`
/// (`0` when none). NULL `str`/`regexp` → NULL. Returns `int`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct RegexpCount {
    signature: Signature,
}

impl RegexpCount {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for RegexpCount {
    fn name(&self) -> &str {
        "regexp_count"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let (strs, pats) = regex_pair(&args, "regexp_count")?;
        let strs = strs.as_any().downcast_ref::<StringArray>().unwrap();
        let pats = pats.as_any().downcast_ref::<StringArray>().unwrap();
        let mut out = Int32Array::builder(n);
        for row in 0..n {
            if strs.is_null(row) || pats.is_null(row) {
                out.append_null();
                continue;
            }
            let re = compile_pattern("regexp_count", pats.value(row))?;
            out.append_value(re.find_iter(strs.value(row)).count() as i32);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Spark `regexp_instr(str, regexp)` — the **1-based character** position at which the first match
/// of `regexp` begins in `str`, or `0` when there is no match. NULL `str`/`regexp` → NULL.
/// Returns `int`. (Spark's auto-name carries a synthetic `, 0` idx argument — see `spark_names`.)
#[derive(Debug, PartialEq, Eq, Hash)]
struct RegexpInstr {
    signature: Signature,
}

impl RegexpInstr {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for RegexpInstr {
    fn name(&self) -> &str {
        "regexp_instr"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let (strs, pats) = regex_pair(&args, "regexp_instr")?;
        let strs = strs.as_any().downcast_ref::<StringArray>().unwrap();
        let pats = pats.as_any().downcast_ref::<StringArray>().unwrap();
        let mut out = Int32Array::builder(n);
        for row in 0..n {
            if strs.is_null(row) || pats.is_null(row) {
                out.append_null();
                continue;
            }
            let text = strs.value(row);
            let re = compile_pattern("regexp_instr", pats.value(row))?;
            let pos = match re.find(text) {
                // 1-based *character* index of the match start (not byte offset).
                Some(m) => text[..m.start()].chars().count() as i32 + 1,
                None => 0,
            };
            out.append_value(pos);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Spark `regexp_like(str, regexp)` (and its function-call aliases `regexp` / `rlike`) — whether
/// `regexp` matches anywhere in `str`. NULL `str`/`regexp` → NULL. Returns `boolean`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct RegexpLike {
    signature: Signature,
    name: &'static str,
}

impl RegexpLike {
    fn new(name: &'static str) -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
            name,
        }
    }
}

impl ScalarUDFImpl for RegexpLike {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let (strs, pats) = regex_pair(&args, self.name)?;
        let strs = strs.as_any().downcast_ref::<StringArray>().unwrap();
        let pats = pats.as_any().downcast_ref::<StringArray>().unwrap();
        let mut out = datafusion::arrow::array::BooleanArray::builder(n);
        for row in 0..n {
            if strs.is_null(row) || pats.is_null(row) {
                out.append_null();
                continue;
            }
            let re = compile_pattern(self.name, pats.value(row))?;
            out.append_value(re.is_match(strs.value(row)));
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// regexp_extract_all(str, regex [, idx]) -> array<string>
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct RegexpExtractAll {
    signature: Signature,
}

impl RegexpExtractAll {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(2), TypeSignature::Any(3)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpExtractAll {
    fn name(&self) -> &str {
        "regexp_extract_all"
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
        let n = args.number_rows;
        if !(2..=3).contains(&args.args.len()) {
            return exec_err!("regexp_extract_all: expected 2 or 3 arguments");
        }
        let strs = to_str_array(&args.args[0], n)?;
        let pats = to_str_array(&args.args[1], n)?;
        let strs = strs.as_any().downcast_ref::<StringArray>().unwrap();
        let pats = pats.as_any().downcast_ref::<StringArray>().unwrap();
        let idxs = match args.args.get(2) {
            Some(a) => Some(to_i32_array(a, n)?),
            None => None,
        };
        let idxs = idxs
            .as_ref()
            .map(|a| a.as_any().downcast_ref::<Int32Array>().unwrap());

        let values_builder = GenericStringBuilder::<i32>::new();
        let mut builder = GenericListBuilder::<i32, _>::new(values_builder);
        for row in 0..n {
            if strs.is_null(row) || pats.is_null(row) {
                builder.append_null();
                continue;
            }
            let idx = match idxs {
                Some(a) if a.is_null(row) => {
                    builder.append_null();
                    continue;
                }
                Some(a) => a.value(row),
                None => 1,
            };
            let re = compile_pattern("regexp_extract_all", pats.value(row))?;
            check_group_index("regexp_extract_all", idx, re.captures_len() - 1)?;
            for group in java_find_all(&re, strs.value(row), idx as usize) {
                builder.values().append_value(group);
            }
            builder.append(true);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// ---------------------------------------------------------------------------
// regexp_replace(str, regex, rep [, position])
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct RegexpReplace {
    signature: Signature,
}

impl RegexpReplace {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(3), TypeSignature::Any(4)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpReplace {
    fn name(&self) -> &str {
        "regexp_replace"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    /// Spark's `regexp_replace(str, regex, rep [, position])`:
    /// - **global** replacement (every non-overlapping match, not just the first — DataFusion's
    ///   builtin replaces only the first unless a `g` flag is passed);
    /// - the regex is Spark-unescaped (`\\d` → `\d`) before compiling, mirroring how Spark's SQL
    ///   literal parser would have unescaped it (oxidant's `sqlparser` does not);
    /// - the optional 1-based `position` starts matching at the `position`-th character, leaving the
    ///   prefix before it untouched (Spark `Matcher.region(position-1, len)` + `appendReplacement`,
    ///   which preserves text before the region). `position <= 0` is rejected
    ///   (`DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE`); `position > len` returns the string unchanged;
    ///   a NULL `str` / `regex` / `rep` / `position` yields NULL.
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        if !(3..=4).contains(&args.args.len()) {
            return exec_err!("regexp_replace: expected 3 or 4 arguments");
        }
        let strs = to_str_array(&args.args[0], n)?;
        let pats = to_str_array(&args.args[1], n)?;
        let reps = to_str_array(&args.args[2], n)?;
        let strs = strs.as_any().downcast_ref::<StringArray>().unwrap();
        let pats = pats.as_any().downcast_ref::<StringArray>().unwrap();
        let reps = reps.as_any().downcast_ref::<StringArray>().unwrap();
        let poss = match args.args.get(3) {
            Some(a) => Some(to_i32_array(a, n)?),
            None => None,
        };
        let poss = poss
            .as_ref()
            .map(|a| a.as_any().downcast_ref::<Int32Array>().unwrap());

        let mut out = StringBuilder::new();
        for row in 0..n {
            // Any NULL operand propagates to NULL.
            if strs.is_null(row) || pats.is_null(row) || reps.is_null(row) {
                out.append_null();
                continue;
            }
            let pos = match poss {
                Some(a) if a.is_null(row) => {
                    out.append_null();
                    continue;
                }
                Some(a) => a.value(row),
                None => 1,
            };
            if pos < 1 {
                return exec_err!(
                    "[DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE] The value of parameter(s) `position` in \
                     `regexp_replace` is out of range: expected a value in (0, 2147483647], got {pos}"
                );
            }
            let re = compile_pattern("regexp_replace", pats.value(row))?;
            let source = strs.value(row);
            let rep = reps.value(row);
            let pos0 = (pos - 1) as usize;
            let chars_len = source.chars().count();
            let replaced = if pos0 == 0 || pos0 < chars_len {
                // Keep the prefix before `position` verbatim; replace globally in the suffix.
                let byte_off = source
                    .char_indices()
                    .nth(pos0)
                    .map(|(b, _)| b)
                    .unwrap_or_else(|| source.len());
                let (prefix, suffix) = source.split_at(byte_off);
                format!("{}{}", prefix, re.replace_all(suffix, rep))
            } else {
                source.to_string()
            };
            out.append_value(replaced);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// substring_index(str, delim, count)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct SubstringIndex {
    signature: Signature,
}

impl SubstringIndex {
    fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

/// Spark's `substring_index`: keep the part of `s` before the `count`-th (1-based) occurrence of
/// the literal `delim`, counting from the left for `count > 0` and from the right for `count < 0`.
fn substring_index(s: &str, delim: &str, count: i32) -> String {
    if count == 0 {
        return String::new();
    }
    if delim.is_empty() {
        return s.to_string();
    }
    // Collect the byte offsets at which (non-overlapping) `delim` occurrences start.
    let mut starts: Vec<usize> = Vec::new();
    let mut from = 0usize;
    while let Some(rel) = s[from..].find(delim) {
        let at = from + rel;
        starts.push(at);
        from = at + delim.len();
    }
    let total = starts.len();
    if count > 0 {
        let c = count as usize;
        if c > total {
            return s.to_string();
        }
        // Everything before the c-th occurrence.
        s[..starts[c - 1]].to_string()
    } else {
        let c = (-(count as i64)) as usize;
        if c > total {
            return s.to_string();
        }
        // Everything after the c-th occurrence counted from the right.
        let occ = starts[total - c];
        s[occ + delim.len()..].to_string()
    }
}

impl ScalarUDFImpl for SubstringIndex {
    fn name(&self) -> &str {
        "substring_index"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let strs = to_str_array(&args.args[0], n)?;
        let delims = to_str_array(&args.args[1], n)?;
        let counts = to_i32_array(&args.args[2], n)?;
        let strs = strs.as_any().downcast_ref::<StringArray>().unwrap();
        let delims = delims.as_any().downcast_ref::<StringArray>().unwrap();
        let counts = counts.as_any().downcast_ref::<Int32Array>().unwrap();

        let mut out = StringBuilder::new();
        for row in 0..n {
            if strs.is_null(row) || delims.is_null(row) || counts.is_null(row) {
                out.append_null();
                continue;
            }
            out.append_value(substring_index(
                strs.value(row),
                delims.value(row),
                counts.value(row),
            ));
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// find_in_set(str, strList)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct FindInSet {
    signature: Signature,
}

impl FindInSet {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

/// Spark's `find_in_set`: 1-based index of `s` among the comma-separated entries of `list`, or `0`
/// if not found. Returns `0` when `s` itself contains a comma (it can never be a single entry).
fn find_in_set(s: &str, list: &str) -> i32 {
    if s.contains(',') {
        return 0;
    }
    for (i, entry) in list.split(',').enumerate() {
        if entry == s {
            return (i + 1) as i32;
        }
    }
    0
}

impl ScalarUDFImpl for FindInSet {
    fn name(&self) -> &str {
        "find_in_set"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let strs = to_str_array(&args.args[0], n)?;
        let lists = to_str_array(&args.args[1], n)?;
        let strs = strs.as_any().downcast_ref::<StringArray>().unwrap();
        let lists = lists.as_any().downcast_ref::<StringArray>().unwrap();

        let mut out = Int32Array::builder(n);
        for row in 0..n {
            if strs.is_null(row) || lists.is_null(row) {
                out.append_null();
                continue;
            }
            out.append_value(find_in_set(strs.value(row), lists.value(row)));
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
    async fn regexp_substr_first_match_or_null() {
        // First whole match.
        assert!(run("SELECT regexp_substr('1a 2b 14m', '\\\\d+') AS x")
            .await
            .contains("| 1 "));
        assert!(
            run("SELECT regexp_substr('1a 2b 14m', '\\\\d+(a|b|m)') AS x")
                .await
                .contains("1a")
        );
        // No match → NULL (not empty string, which is what regexp_extract would give). Arrow's
        // pretty-format prints a null as blank, so assert nullness via `IS NULL`.
        assert!(
            run("SELECT regexp_substr('1a 2b 14m', '\\\\d+ x') IS NULL AS x")
                .await
                .contains("true")
        );
    }

    #[tokio::test]
    async fn regexp_extract_groups_and_whole_match() {
        // idx 0 = whole match, idx 1/2 = capture groups.
        assert!(
            run("SELECT regexp_extract('1a 2b 14m', '(\\\\d+)([a-z]+)', 0) AS x")
                .await
                .contains("1a"),
        );
        assert!(
            run("SELECT regexp_extract('1a 2b 14m', '(\\\\d+)([a-z]+)', 1) AS x")
                .await
                .contains("| 1 "),
        );
        assert!(
            run("SELECT regexp_extract('1a 2b 14m', '(\\\\d+)([a-z]+)', 2) AS x")
                .await
                .contains("| a "),
        );
        // Default idx is 1.
        assert!(
            run("SELECT regexp_extract('1a 2b 14m', '(\\\\d+)([a-z]+)') AS x")
                .await
                .contains("| 1 "),
        );
    }

    #[tokio::test]
    async fn regexp_extract_no_match_and_nonparticipating_group_is_empty() {
        // Regex matches (optional group), but the group did not participate -> ''.
        let g = run("SELECT regexp_extract('a b m', '(\\\\d+)?([a-z]+)', 1) AS x").await;
        // Empty string renders as an empty cell.
        assert!(g.contains("|    |") || g.contains("| x"), "{g}");
        assert!(!g.contains("| a "), "{g}");
    }

    #[tokio::test]
    async fn regexp_extract_group_index_out_of_range_errors() {
        let engine = Engine::new();
        // 2 groups, idx 3 -> error.
        assert!(engine
            .sql("SELECT regexp_extract('1a 2b 14m', '(\\\\d+)([a-z]+)', 3)")
            .await
            .is_err());
        // negative idx -> error.
        assert!(engine
            .sql("SELECT regexp_extract('1a 2b 14m', '(\\\\d+)([a-z]+)', -1)")
            .await
            .is_err());
        // 0 groups, default idx 1 -> error.
        assert!(engine
            .sql("SELECT regexp_extract('1a 2b 14m', '\\\\d+')")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn regexp_extract_all_basic() {
        let g = run("SELECT regexp_extract_all('1a 2b 14m', '\\\\d+', 0) AS x").await;
        assert!(g.contains("[1, 2, 14]"), "{g}");
        let g2 = run("SELECT regexp_extract_all('1a 2b 14m', '(\\\\d+)([a-z]+)', 2) AS x").await;
        assert!(g2.contains("[a, b, m]"), "{g2}");
    }

    #[tokio::test]
    async fn regexp_extract_all_nonparticipating_group_is_empty() {
        // Optional group, every other match empty.
        let g = run("SELECT regexp_extract_all('1a 2b 14m', '(\\\\d+)?', 1) AS x").await;
        assert!(g.contains("[1, , , 2, , , 14, , ]"), "{g}");
    }

    #[tokio::test]
    async fn regexp_replace_is_global_and_unescapes_pattern() {
        // Global (both `*thy` matches replaced), and `\\w+thy` is unescaped to `\w+thy`.
        let g = run(
            "SELECT regexp_replace('healthy, wealthy, and wise', '\\\\w+thy', 'something') AS x",
        )
        .await;
        assert!(g.contains("something, something, and wise"), "{g}");
    }

    #[tokio::test]
    async fn regexp_replace_position_keeps_prefix() {
        // position 2 keeps the leading "h"; position 8 keeps "healthy".
        assert!(run(
            "SELECT regexp_replace('healthy, wealthy, and wise', '\\\\w+thy', 'something', 2) AS x"
        )
        .await
        .contains("hsomething, something, and wise"));
        assert!(run(
            "SELECT regexp_replace('healthy, wealthy, and wise', '\\\\w+thy', 'something', 8) AS x"
        )
        .await
        .contains("healthy, something, and wise"));
        // position 26 replaces only the final char; position past the end leaves it unchanged.
        assert!(run(
            "SELECT regexp_replace('healthy, wealthy, and wise', '\\\\w', 'something', 26) AS x"
        )
        .await
        .contains("healthy, wealthy, and wissomething"));
        assert!(run(
            "SELECT regexp_replace('healthy, wealthy, and wise', '\\\\w', 'something', 27) AS x"
        )
        .await
        .contains("healthy, wealthy, and wise"));
    }

    #[tokio::test]
    async fn regexp_replace_position_out_of_range_errors_and_null_propagates() {
        let engine = Engine::new();
        assert!(engine
            .sql("SELECT regexp_replace('healthy, wealthy, and wise', '\\\\w+thy', 'something', 0)")
            .await
            .is_err());
        assert!(engine
            .sql(
                "SELECT regexp_replace('healthy, wealthy, and wise', '\\\\w+thy', 'something', -2)"
            )
            .await
            .is_err());
        // NULL position -> NULL output (the pretty-printer renders a null cell as blank, so the
        // tell is simply that no replacement happened).
        let g = run(
            "SELECT regexp_replace('healthy, wealthy, and wise', '\\\\w', 'something', null) AS x",
        )
        .await;
        assert!(!g.contains("something"), "{g}");
    }

    #[tokio::test]
    async fn substring_index_positive_negative_zero() {
        assert!(run("SELECT substring_index('www.apache.org', '.', 2) AS x")
            .await
            .contains("www.apache"),);
        assert!(
            run("SELECT substring_index('www.apache.org', '.', -2) AS x")
                .await
                .contains("apache.org"),
        );
        // count 0 -> empty.
        let z = run("SELECT substring_index('www.apache.org', '.', 0) AS x").await;
        assert!(z.contains("|    |") || z.contains("| x"), "{z}");
        // |count| beyond occurrences -> whole string.
        assert!(
            run("SELECT substring_index('www.apache.org', '.', 10) AS x")
                .await
                .contains("www.apache.org"),
        );
    }

    #[tokio::test]
    async fn find_in_set_index_and_absent() {
        assert!(run("SELECT find_in_set('ab', 'abc,b,ab,c,def') AS x")
            .await
            .contains("| 3 "),);
        assert!(run("SELECT find_in_set('x', 'abc,b,ab,c,def') AS x")
            .await
            .contains("| 0 "),);
        // str containing a comma -> 0.
        assert!(run("SELECT find_in_set('a,b', 'a,b,c') AS x")
            .await
            .contains("| 0 "),);
    }
}
