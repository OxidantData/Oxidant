//! `weft-loom` — the vectorized CPU engine and Weft's workhorse.
//!
//! **This is what beats Sail on ClickBench.** Phase 0 embeds DataFusion behind the warp
//! IR to reach correctness + a credible benchmark entry fast. Phase 1 carves out native
//! operators for the handful of queries that dominate the total runtime:
//!
//! - high-cardinality `GROUP BY` (Q31–Q35): adaptive, radix-partitioned, open-addressing
//!   hash table with an inline hash salt; spill partitions independently;
//! - sort / top-N (Q23–Q26 and every `… ORDER BY c DESC LIMIT 10`): late-materialized
//!   top-N heap that decodes only the surviving rows;
//! - string `LIKE`/regex (Q20–Q23, Q28): SIMD substring + vectorized regex;
//! - `COUNT(DISTINCT)` (Q4/Q5 + per-group): HyperLogLog sketches.
//!
//! The strategy: tie Sail on the ~33 cheap queries (DataFusion parity), beat it 1.5–2× on
//! the ~10 expensive ones. Winning those *is* winning the total.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use datafusion::prelude::SessionContext;
use weft_common::{Error, Result};

pub mod catalog_bridge;
pub mod shard;

/// Case-insensitive file→table column matching for catalog-declared schemas (Glue/Hive parity).
mod schema_adapt;

/// Spark-only scalar functions (DataFusion `ScalarUDF`s) registered into every [`Engine`].
pub mod spark_functions;

/// Spark-compatible output column naming for the top result projection (drop-in `df.columns`
/// parity). See [`spark_names::project_spark_names`].
mod spark_names;

/// Spark-compatible integer-literal typing (`INT` vs `BIGINT` default). See
/// [`spark_int_literals::downcast_int_literals`].
mod spark_int_literals;

/// Faithful lowering of Spark's `CREATE TABLE … USING <fmt>` DDL to a real, format-backed
/// `CREATE EXTERNAL TABLE`. See [`spark_create_table::lower_create_table_using`].
mod spark_create_table;
mod spark_decimal;

/// Re-export of the exact `arrow` DataFusion uses, so every crate in the workspace encodes
/// Arrow IPC against one version (no cross-crate `arrow` mismatch).
pub use datafusion::arrow;

use arrow::record_batch::RecordBatch;

/// Native operators (Phase-1 carve-outs) that replace DataFusion's generic physical operators
/// on the heavy ClickBench queries. See [`ops`] for status and scope.
pub mod ops;

/// Backend identifier reported in `EXPLAIN`.
pub const NAME: &str = "loom";

/// Parse a `usize` tuning knob from the environment (absent / unparseable → `None`).
fn env_usize(key: &str) -> Option<usize> {
    std::env::var(key).ok().and_then(|s| s.parse().ok())
}

/// Parse a boolean tuning knob from the environment. Accepts `1/0`, `true/false`, `on/off`
/// (case-insensitive); absent / unrecognized → `None`.
fn env_bool(key: &str) -> Option<bool> {
    match std::env::var(key)
        .ok()?
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "1" | "true" | "on" | "yes" => Some(true),
        "0" | "false" | "off" | "no" => Some(false),
        _ => None,
    }
}

/// Adapt Spark-dialect SQL that DataFusion's planner rejects verbatim but supports once a
/// dialect-only keyword is dropped. The rewrite only touches the leading DDL keywords and leaves
/// the statement body byte-for-byte intact.
///
/// Today it handles `CREATE [OR REPLACE] [GLOBAL] TEMPORARY VIEW … ` → `CREATE [OR REPLACE]
/// VIEW … `. Spark temporary views are *session*-scoped; a DataFusion session-catalog view is
/// too, so dropping `TEMPORARY`/`GLOBAL` preserves the semantics within a session while letting
/// DataFusion register the view (its `create_view` rejects `temporary` and nothing else). This is
/// the single biggest Spark-parity unlock — almost every Spark SQL test opens with
/// `CREATE OR REPLACE TEMPORARY VIEW testData AS …`.
///
/// This is a stopgap living in the engine; it will migrate into the `weft-sql` Spark-dialect
/// front end when that lands.
/// Detect `COUNT(DISTINCT col1, col2, …)` — Spark rejects this; DataFusion panics.
fn is_multi_arg_count_distinct(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    let Some(pos) = lower.find("count") else {
        return false;
    };
    let rest = &lower[pos..];
    if !rest.contains("distinct") {
        return false;
    }
    let Some(lp) = rest.find('(') else {
        return false;
    };
    let Some(rp) = rest[lp..].find(')') else {
        return false;
    };
    let inside = &rest[lp + 1..lp + rp];
    if !inside.contains("distinct") {
        return false;
    }
    let after_distinct = inside.split("distinct").nth(1).unwrap_or("");
    after_distinct.contains(',')
}

pub fn normalize_spark_sql(query: &str) -> std::borrow::Cow<'_, str> {
    // Passes run in order: (1) the leading-keyword DDL rewrite, (2) Spark single-quoted
    // string-literal unescaping, (3) the typed-literal rewrite over the result. Unescaping runs
    // BEFORE the typed-literal pass for two reasons: the re-emitted literals use `''` quote-doubling
    // (which the typed-literal scanner understands) instead of Spark's `\'`, and a numeric token
    // freed by a mis-delimited `\'` can therefore never be mistaken for code and wrapped in a CAST.
    let stripped = strip_temporary_view(query);
    let base = stripped.as_deref().unwrap_or(query);
    let unescaped = unescape_spark_string_literals(base);
    let base2 = unescaped.as_deref().unwrap_or(base);
    let typed = rewrite_spark_typed_literals(base2);
    match typed {
        Some(t) => std::borrow::Cow::Owned(t),
        None => match unescaped {
            Some(u) => std::borrow::Cow::Owned(u),
            None => match stripped {
                Some(s) => std::borrow::Cow::Owned(s),
                None => std::borrow::Cow::Borrowed(query),
            },
        },
    }
}

/// Reproduce Spark's parse-time `unescapeSQLString` over every single-quoted string literal, then
/// re-emit a DataFusion-equivalent literal. Returns `None` when nothing changed (so the caller keeps
/// the borrowed fast path).
///
/// Spark's default parser (`spark.sql.parser.escapedStringLiterals=false`) runs `unescapeSQLString`
/// on every `'…'` literal: `\\`→`\`, `\n`→newline, `\t`→tab, `\uXXXX`→code point, octal `\ooo`→char,
/// `\'`→`'`, and (Spark's LIKE-pattern carve-out) `\%`/`\_` kept verbatim. DataFusion parses on the
/// Databricks dialect, which (like ANSI SQL) treats backslash as an ordinary character inside `'…'`
/// and only recognizes `''` quote-doubling — so without this pass weft would feed the raw
/// backslashes to the planner and compute the wrong value (e.g. `'a\nb'` would stay a 4-char string
/// instead of Spark's 3-char `a⏎b`). Reproducing Spark's documented default-parser decode here and
/// re-encoding the *value* as a Databricks-dialect literal is a faithful syntax→equivalent-plan
/// lowering, not a lossy rewrite.
///
/// The re-encoding emits the decoded value back as `'…'`, doubling any `'` to `''` and embedding
/// real backslashes / control chars / unicode directly, because the Databricks dialect keeps
/// backslashes literal and decodes only `''`. The scan is comment-/identifier-/double-quote-aware so
/// only single-quoted literals are touched; a literal containing no backslash is copied byte-for-byte
/// (the common case — zero risk to `''`-only literals), and an unterminated literal is left intact so
/// its original parse error is preserved.
fn unescape_spark_string_literals(sql: &str) -> Option<String> {
    let b = sql.as_bytes();
    let n = b.len();
    let mut out = String::with_capacity(n);
    let mut i = 0;
    let mut changed = false;

    while i < n {
        let c = b[i];
        match c {
            // Single-quoted string literal — the only kind Spark `unescapeSQLString` rewrites.
            b'\'' => {
                let start = i;
                i += 1;
                let content_start = i;
                let mut has_backslash = false;
                // Find the closing quote using Spark's lexer rule: a backslash escapes the next
                // char (so `\'`/`\\` do not terminate) and `''` is a doubled (escaped) quote.
                loop {
                    if i >= n {
                        break; // unterminated
                    }
                    match b[i] {
                        b'\\' => {
                            has_backslash = true;
                            i += 1;
                            if i < n {
                                i += utf8_len(b[i]).min(n - i);
                            }
                        }
                        b'\'' => {
                            if i + 1 < n && b[i + 1] == b'\'' {
                                i += 2; // doubled quote — stays inside the literal
                            } else {
                                break; // closing quote
                            }
                        }
                        other => i += utf8_len(other).min(n - i),
                    }
                }
                let content_end = i;
                let terminated = i < n;
                let after = if terminated { i + 1 } else { i };
                // Copy verbatim unless the literal both terminates and carries a backslash: a
                // backslash-free literal already means the same to Spark and DataFusion, and an
                // unterminated literal must keep its original parse error.
                if has_backslash && terminated {
                    let value = spark_unescape_sql_string(&sql[content_start..content_end]);
                    out.push('\'');
                    for vch in value.chars() {
                        if vch == '\'' {
                            out.push_str("''");
                        } else {
                            out.push(vch);
                        }
                    }
                    out.push('\'');
                    changed = true;
                } else {
                    out.push_str(&sql[start..after]);
                }
                i = after;
            }
            // Double-quoted string literal (Databricks dialect) — copy verbatim (`""` doubling).
            // Left to the existing scanner/parser rules per Spark's literal handling.
            b'"' => {
                let start = i;
                i += 1;
                while i < n {
                    if b[i] == b'"' {
                        if i + 1 < n && b[i + 1] == b'"' {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += utf8_len(b[i]).min(n - i);
                }
                out.push_str(&sql[start..i]);
            }
            // Backtick-quoted identifier — copy verbatim (`` `` `` doubling).
            b'`' => {
                let start = i;
                i += 1;
                while i < n {
                    if b[i] == b'`' {
                        if i + 1 < n && b[i + 1] == b'`' {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                out.push_str(&sql[start..i]);
            }
            // Line comment.
            b'-' if i + 1 < n && b[i + 1] == b'-' => {
                let start = i;
                while i < n && b[i] != b'\n' {
                    i += 1;
                }
                out.push_str(&sql[start..i]);
            }
            // Block comment.
            b'/' if i + 1 < n && b[i + 1] == b'*' => {
                let start = i;
                i += 2;
                while i < n && !(b[i] == b'*' && i + 1 < n && b[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(n);
                out.push_str(&sql[start..i]);
            }
            _ => {
                let len = utf8_len(c).min(n - i);
                out.push_str(&sql[i..i + len]);
                i += len;
            }
        }
    }

    changed.then_some(out)
}

/// Decode the *contents* of a single-quoted literal (the chars between the quotes) per Spark's
/// `unescapeSQLString`. Mirrors Spark's branch structure and bounds (translated from the
/// quote-inclusive form to operate on content): `\uXXXX` (exactly 4 hex) → code point; `\ooo`
/// (3 octal digits, first ∈ {0,1}) → char; otherwise a single-char escape via [`append_escaped_char`].
/// `''` is collapsed to one `'` (the dialect's quote-doubling, which the scanner preserved inside
/// the content). A lone trailing backslash with no following char is dropped, exactly as Spark does.
fn spark_unescape_sql_string(content: &str) -> String {
    let c: Vec<char> = content.chars().collect();
    let m = c.len();
    let mut out = String::with_capacity(content.len());
    let mut k = 0;
    while k < m {
        let ch = c[k];
        if ch == '\\' {
            // `\uXXXX` — exactly 4 hex digits. (Spark guard `i+6 < len` → `k+5 < m` on content.)
            if k + 5 < m && c[k + 1] == 'u' {
                if let Some(cp) = hex4(&c, k + 2) {
                    if let Some(uc) = char::from_u32(cp) {
                        out.push(uc);
                    }
                    k += 6;
                    continue;
                }
                // Not valid hex — fall through to the single-char escape of `u`.
            }
            // Octal `\ooo` (first digit 0/1). (Spark guard `i+4 < len` → `k+3 < m` on content.)
            if k + 3 < m {
                let (o1, o2, o3) = (c[k + 1], c[k + 2], c[k + 3]);
                if ('0'..='1').contains(&o1)
                    && ('0'..='7').contains(&o2)
                    && ('0'..='7').contains(&o3)
                {
                    let v = ((o1 as u32 - '0' as u32) << 6)
                        + ((o2 as u32 - '0' as u32) << 3)
                        + (o3 as u32 - '0' as u32);
                    if let Some(uc) = char::from_u32(v) {
                        out.push(uc);
                    }
                    k += 4;
                    continue;
                }
                append_escaped_char(o1, &mut out);
                k += 2;
                continue;
            }
            // Single-char escape. (Spark guard `i+2 < len` → `k+1 < m` on content.)
            if k + 1 < m {
                append_escaped_char(c[k + 1], &mut out);
                k += 2;
                continue;
            }
            // Lone trailing backslash — Spark appends nothing.
            k += 1;
            continue;
        }
        // `''` → one `'` (quote-doubling the scanner left inside the content).
        if ch == '\'' && k + 1 < m && c[k + 1] == '\'' {
            out.push('\'');
            k += 2;
            continue;
        }
        out.push(ch);
        k += 1;
    }
    out
}

/// Spark's `appendEscapedChar`: the single-char escape table. Unknown escapes drop the backslash and
/// keep the char (`\d`→`d`); the LIKE-pattern carve-outs `\%`/`\_` keep the backslash so downstream
/// `LIKE`/`RLIKE` escaping still works.
fn append_escaped_char(n: char, out: &mut String) {
    match n {
        '0' => out.push('\u{0}'),
        '\'' => out.push('\''),
        '"' => out.push('"'),
        'b' => out.push('\u{8}'),
        'n' => out.push('\n'),
        'r' => out.push('\r'),
        't' => out.push('\t'),
        'Z' => out.push('\u{1A}'),
        '\\' => out.push('\\'),
        '%' => out.push_str("\\%"),
        '_' => out.push_str("\\_"),
        other => out.push(other),
    }
}

/// Parse exactly four hex digits starting at `start` into a code point; `None` if any isn't hex.
fn hex4(c: &[char], start: usize) -> Option<u32> {
    let mut v = 0u32;
    for j in 0..4 {
        v = v * 16 + c.get(start + j)?.to_digit(16)?;
    }
    Some(v)
}

/// Byte length of the UTF-8 char starting with leading byte `lead`.
fn utf8_len(lead: u8) -> usize {
    if lead < 0x80 {
        1
    } else if lead < 0xE0 {
        2
    } else if lead < 0xF0 {
        3
    } else {
        4
    }
}

/// Derive Spark's `DECIMAL(precision, scale)` for a `…BD` literal from its digit text (no sign, no
/// exponent), matching `java.math.BigDecimal`: scale = fractional digits; precision = significant
/// digits (leading zeros stripped, min 1), widened so `precision >= scale`. Returns `None` if it
/// would exceed Spark's 38-digit decimal range (leave the literal untouched).
fn decimal_ps(num: &str) -> Option<(u8, u8)> {
    let (int_part, frac_part) = num.split_once('.').unwrap_or((num, ""));
    let scale = frac_part.len();
    let sig_digits: String = format!("{int_part}{frac_part}");
    let trimmed = sig_digits.trim_start_matches('0');
    let sig = if trimmed.is_empty() { 1 } else { trimmed.len() };
    let precision = sig.max(scale).max(1);
    if precision > 38 {
        return None;
    }
    Some((precision as u8, scale as u8))
}

/// Rewrite Spark's typed numeric literals — `1L`, `2Y`, `3S`, `1.0F`, `1.0D`, `1.0BD` — into the
/// equivalent `CAST(<num> AS <type>)`. DataFusion's lexer reads the suffixed forms as identifiers
/// (failing with `No field named "1l"`), so Spark SQL that uses typed literals — pervasive in the
/// corpus — won't plan. The cast is exactly Spark's semantics (`1L` *is* a bigint `1`), so the
/// rewrite is faithful, not lossy.
///
/// The scan is string-/identifier-/comment-aware: single- and double-quoted strings (`"…"` is a
/// string literal under the Databricks dialect), backtick-quoted identifiers, and `--`/`/* */`
/// comments are copied through verbatim, so a literal like `'1L'` or a column `` `2Y` `` is never
/// touched. A numeric token is only rewritten when it sits in code position (the preceding char is
/// not an identifier char or `.`) and the suffix is followed by a non-identifier boundary, so
/// `col1`, `0x1F`, `1e5`, and `3.14desc` are all left intact. Returns `None` when nothing changed
/// (so the caller keeps the borrowed fast-path).
fn rewrite_spark_typed_literals(sql: &str) -> Option<String> {
    let b = sql.as_bytes();
    let n = b.len();
    let mut out = String::with_capacity(n + 16);
    let mut i = 0;
    let mut changed = false;

    while i < n {
        let c = b[i];

        // Quoted string ('…', "…") or identifier (`…`) — copy verbatim, honoring doubled quotes.
        if c == b'\'' || c == b'"' || c == b'`' {
            let start = i;
            i += 1;
            while i < n {
                if b[i] == c {
                    if i + 1 < n && b[i + 1] == c {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            out.push_str(&sql[start..i]);
            continue;
        }
        // Line comment.
        if c == b'-' && i + 1 < n && b[i + 1] == b'-' {
            let start = i;
            while i < n && b[i] != b'\n' {
                i += 1;
            }
            out.push_str(&sql[start..i]);
            continue;
        }
        // Block comment.
        if c == b'/' && i + 1 < n && b[i + 1] == b'*' {
            let start = i;
            i += 2;
            while i < n && !(b[i] == b'*' && i + 1 < n && b[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(n);
            out.push_str(&sql[start..i]);
            continue;
        }

        // Numeric literal candidate: a digit in code position (not part of an identifier or a
        // fractional tail).
        let prev = if i == 0 { 0 } else { b[i - 1] };
        let prev_blocks = prev.is_ascii_alphanumeric() || prev == b'_' || prev == b'.';
        if c.is_ascii_digit() && !prev_blocks {
            let num_start = i;
            while i < n && b[i].is_ascii_digit() {
                i += 1;
            }
            // Fraction (only when a digit follows the dot — otherwise the dot isn't ours).
            if i + 1 < n && b[i] == b'.' && b[i + 1].is_ascii_digit() {
                i += 1;
                while i < n && b[i].is_ascii_digit() {
                    i += 1;
                }
            }
            // Exponent.
            let mut has_exp = false;
            if i < n && (b[i] == b'e' || b[i] == b'E') {
                let mut j = i + 1;
                if j < n && (b[j] == b'+' || b[j] == b'-') {
                    j += 1;
                }
                if j < n && b[j].is_ascii_digit() {
                    i = j;
                    while i < n && b[i].is_ascii_digit() {
                        i += 1;
                    }
                    has_exp = true;
                }
            }
            let num = &sql[num_start..i];
            let after_ok = |k: usize| k >= n || !(b[k].is_ascii_alphanumeric() || b[k] == b'_');

            // `BD` → DECIMAL (only without an exponent, where precision/scale are well-defined).
            if i + 1 < n
                && (b[i] == b'b' || b[i] == b'B')
                && (b[i + 1] == b'd' || b[i + 1] == b'D')
                && after_ok(i + 2)
            {
                if !has_exp {
                    if let Some((p, s)) = decimal_ps(num) {
                        out.push_str(&format!("CAST({num} AS DECIMAL({p},{s}))"));
                        i += 2;
                        changed = true;
                        continue;
                    }
                }
                out.push_str(num);
                continue;
            }
            // Single-letter type suffix.
            if i < n && after_ok(i + 1) {
                let ty = match b[i] {
                    b'y' | b'Y' => Some("TINYINT"),
                    b's' | b'S' => Some("SMALLINT"),
                    b'l' | b'L' => Some("BIGINT"),
                    b'f' | b'F' => Some("FLOAT"),
                    b'd' | b'D' => Some("DOUBLE"),
                    _ => None,
                };
                if let Some(ty) = ty {
                    out.push_str(&format!("CAST({num} AS {ty})"));
                    i += 1;
                    changed = true;
                    continue;
                }
            }
            // A plain number with no type suffix — copy as-is.
            out.push_str(num);
            continue;
        }

        // Any other char — copy one UTF-8 char.
        let len = utf8_len(c).min(n - i);
        out.push_str(&sql[i..i + len]);
        i += len;
    }

    changed.then_some(out)
}

/// Read the next whitespace-delimited token from `s` starting at `*cur`, returning its byte span
/// and advancing `*cur` past it. `None` at end of input.
fn next_token(s: &str, cur: &mut usize) -> Option<(usize, usize)> {
    let b = s.as_bytes();
    while *cur < b.len() && b[*cur].is_ascii_whitespace() {
        *cur += 1;
    }
    let start = *cur;
    while *cur < b.len() && !b[*cur].is_ascii_whitespace() {
        *cur += 1;
    }
    (start < *cur).then_some((start, *cur))
}

/// If `query` begins with `CREATE [OR REPLACE] [GLOBAL] TEMPORARY VIEW`, return the same
/// statement with `GLOBAL TEMPORARY` removed; otherwise `None` (leave the query untouched).
fn strip_temporary_view(query: &str) -> Option<String> {
    let lead = query.len() - query.trim_start().len();
    let (ws, rest) = query.split_at(lead);
    let eq = |span: (usize, usize), kw: &str| rest[span.0..span.1].eq_ignore_ascii_case(kw);

    let mut cur = 0;
    if !eq(next_token(rest, &mut cur)?, "create") {
        return None;
    }
    let mut or_replace = false;
    let mut tok = next_token(rest, &mut cur)?;
    if eq(tok, "or") {
        if !eq(next_token(rest, &mut cur)?, "replace") {
            return None;
        }
        or_replace = true;
        tok = next_token(rest, &mut cur)?;
    }
    if eq(tok, "global") {
        tok = next_token(rest, &mut cur)?;
    }
    // Only rewrite when the temp keyword is present (otherwise DataFusion already copes). Spark
    // accepts both `TEMPORARY` and the `TEMP` abbreviation.
    if !eq(tok, "temporary") && !eq(tok, "temp") {
        return None;
    }
    if !eq(next_token(rest, &mut cur)?, "view") {
        return None;
    }
    // The statement body (view name onward) is preserved verbatim from just after `VIEW`.
    let head = if or_replace {
        "CREATE OR REPLACE VIEW"
    } else {
        "CREATE VIEW"
    };
    Some(format!("{ws}{head}{}", &rest[cur..]))
}

/// Parsed shape of a `CREATE [OR REPLACE] [GLOBAL] [TEMP[ORARY]] VIEW` statement, used to enforce
/// Spark's SPARK-29628 rule that a persistent view may not reference a session-temporary view.
struct CreateViewInfo {
    /// Lowercased, unqualified view name (last identifier component).
    name: String,
    /// True for `TEMPORARY` / `TEMP` (incl. `GLOBAL TEMPORARY`) views.
    temporary: bool,
    /// Lowercased, unqualified names of every relation referenced in the view body.
    relations: Vec<String>,
}

/// Recognize a `CREATE VIEW` statement and extract its name, temporary-ness, and the relations its
/// body references. Returns `None` for any non-`CREATE VIEW` statement (and for anything sqlparser
/// cannot parse), in which case the caller leaves engine behavior completely unchanged. Parsing
/// uses the same Databricks dialect the engine plans with, so the AST matches what DataFusion sees.
fn analyze_create_view(query: &str) -> Option<CreateViewInfo> {
    use datafusion::sql::sqlparser::ast::{visit_relations, ObjectName, Statement};
    use datafusion::sql::sqlparser::dialect::DatabricksDialect;
    use datafusion::sql::sqlparser::parser::Parser;
    use std::ops::ControlFlow;

    let stmts = Parser::parse_sql(&DatabricksDialect {}, query).ok()?;
    let [stmt] = stmts.as_slice() else {
        return None;
    };
    let Statement::CreateView(cv) = stmt else {
        return None;
    };
    let last_part = |on: &ObjectName| -> Option<String> {
        on.0.last()?
            .as_ident()
            .map(|i| i.value.to_ascii_lowercase())
    };
    let name = last_part(&cv.name)?;
    // Collect every relation referenced in the view body. `visit_relations` only visits
    // table-position object names (FROM / JOIN / subquery relations), never the view's own name,
    // so the new view name can never spuriously match itself.
    let mut relations = Vec::new();
    let _ = visit_relations(cv.query.as_ref(), |on| {
        if let Some(part) = last_part(on) {
            relations.push(part);
        }
        ControlFlow::<()>::Continue(())
    });
    Some(CreateViewInfo {
        name,
        temporary: cv.temporary,
        relations,
    })
}

/// Register Spark function names that DataFusion already implements under a *different* name, as
/// faithful aliases — same implementation, extra invocation name. Purely additive and zero-risk:
/// it can only make more Spark SQL resolve, never change an existing result (DataFusion's
/// `with_aliases` merges, so no built-in alias is dropped). This is "Wave A" of the Spark function
/// backlog (aliases for functions with identical semantics under another name); real UDF
/// implementations for Spark-only functions are a separate, larger effort.
fn register_spark_function_aliases(ctx: &SessionContext) {
    use datafusion::execution::FunctionRegistry;

    // (Spark name, DataFusion builtin with identical semantics).
    const SCALAR_ALIASES: &[(&str, &str)] = &[
        ("startswith", "starts_with"),
        ("endswith", "ends_with"),
        ("len", "length"),
        ("ucase", "upper"),
        ("lcase", "lower"),
        ("sign", "signum"),
        ("char", "chr"),
        // Spark `array(e1, …)` constructs an array — identical to DataFusion's `make_array`.
        ("array", "make_array"),
    ];
    const AGG_ALIASES: &[(&str, &str)] = &[
        ("variance", "var_samp"),
        ("approx_count_distinct", "approx_distinct"),
        ("any", "bool_or"),
        ("some", "bool_or"),
        ("every", "bool_and"),
    ];

    let state = ctx.state();
    for (alias, target) in SCALAR_ALIASES {
        // If the target isn't registered (name drift across DataFusion versions), skip silently —
        // never panic the engine over an alias.
        if let Ok(udf) = state.udf(target) {
            // `(*udf).clone()` (not `Arc::unwrap_or_clone`, which needs Rust 1.76 > our 1.72 MSRV).
            ctx.register_udf((*udf).clone().with_aliases([*alias]));
        }
    }
    for (alias, target) in AGG_ALIASES {
        if let Ok(udaf) = state.udaf(target) {
            ctx.register_udaf((*udaf).clone().with_aliases([*alias]));
        }
    }
}

/// A custom [`ExprPlanner`] that lowers Spark's `/` operator to true (double-precision) division
/// whenever both operands are integral, matching Spark's documented `Divide` contract.
///
/// Spark's `/` always evaluates in `DOUBLE` for non-decimal operands — `cast(1 as int) / cast(1 as
/// int)` is the double `1.0`, `7 / 2` is `3.5`. DataFusion's default [`Operator::Divide`], by
/// contrast, performs *truncating integer* division and yields an integer type when both operands
/// are integral (`7 / 2` → `3`, `5 / 2` → `2`). Relative to Spark that is genuine data corruption
/// of both the value and the result type, not a formatting nit.
///
/// This is a faithful, EQUIVALENT-plan lowering (explicitly allowed by the parity contract:
/// "lowering Spark syntax to an equivalent DataFusion plan" matching Spark's documented `/`
/// contract), never a lossy rewrite: when both operand types are integral we rebuild the binary op
/// as `CAST(left AS DOUBLE) / CAST(right AS DOUBLE)`, so DataFusion evaluates it in double
/// precision and returns the Spark value/type. The output column name is unaffected — Spark (and
/// `spark_names::render`) omit coercion casts from a column's name, so the operands still render as
/// before.
///
/// Scope is deliberately narrow so no sibling row (in `division.sql` or elsewhere) regresses:
/// - only `Operator::Divide` (`/`); Spark integer division (`DIV`) is `Operator::IntegerDivide`,
///   a different operator, and is never matched;
/// - only when *both* operands are integral (signed/unsigned `Int*`). `DECIMAL` operands keep
///   Spark's decimal-division precision rules; `FLOAT`/`DOUBLE` operands are already double;
///   string/binary/boolean/date/timestamp/interval/null operands aren't integral, so the existing
///   error / exec parity for those rows is untouched;
/// - a *literal-zero* divisor is left to DataFusion's integer divide, which raises `DIVIDE_BY_ZERO`
///   exactly as Spark's ANSI `/` does. Lowering it to IEEE double division would instead yield a
///   non-erroring `Infinity` and silently drop a Spark error (`SELECT 5 / 0`), so we don't.
#[derive(Debug)]
struct SparkDividePlanner;

impl datafusion::logical_expr::planner::ExprPlanner for SparkDividePlanner {
    fn plan_binary_op(
        &self,
        expr: datafusion::logical_expr::planner::RawBinaryExpr,
        schema: &datafusion::common::DFSchema,
    ) -> datafusion::common::Result<
        datafusion::logical_expr::planner::PlannerResult<
            datafusion::logical_expr::planner::RawBinaryExpr,
        >,
    > {
        use datafusion::arrow::datatypes::DataType;
        use datafusion::logical_expr::expr::ScalarFunction;
        use datafusion::logical_expr::planner::PlannerResult;
        use datafusion::logical_expr::{cast, BinaryExpr, Expr, ExprSchemable, Operator};
        use datafusion::sql::sqlparser::ast::BinaryOperator;

        // We rewrite Spark `/` (true division) and, for a decimal divisor, `%` (modulo). (Spark
        // integer division `DIV` is `Operator::IntegerDivide`, never `/`.)
        let is_divide = matches!(expr.op, BinaryOperator::Divide);
        let is_modulo = matches!(expr.op, BinaryOperator::Modulo);
        if !is_divide && !is_modulo {
            return Ok(PlannerResult::Original(expr));
        }
        // Resolve operand types against the input schema; if either is unresolvable (e.g. a bare
        // placeholder), defer to the default planner unchanged.
        let (Ok(left_ty), Ok(right_ty)) = (expr.left.get_type(schema), expr.right.get_type(schema))
        else {
            return Ok(PlannerResult::Original(expr));
        };
        // Decimal/float divisor: Spark ANSI `/` and `%` raise DIVIDE_BY_ZERO on *any* non-null zero
        // divisor — including decimal (`a / b`, `a % b` over `SELECT 1.0 a, 0.0 b`, where weft types
        // the decimal literals as `Float64`) and floating-point (Spark's `Divide`/`Remainder` share
        // one `failOnError` zero-check across every numeric type; non-ANSI it returns NULL, ANSI it
        // throws — Spark never yields Infinity/NaN from a zero divisor). DataFusion's native decimal/
        // float divide/modulo instead produce a value (Infinity/NaN/null) and silently drop that
        // error — a forbidden missing-error gap. Wrap the divisor in the identity guard
        // `spark_nonzero_divisor`: every non-zero/null row passes through byte-identical (so the
        // divide/modulo keeps DataFusion's exact result type and value, and the Spark column name is
        // unchanged — see `spark_names`), and only a non-null zero divisor raises, converting
        // missing-error→error-parity, never pass→fail. The integral `/` path below covers integral
        // zero divisors via `spark_divide`.
        if matches!(
            right_ty,
            DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64
        ) {
            let guarded_right = Expr::ScalarFunction(ScalarFunction::new_udf(
                crate::spark_functions::spark_nonzero_divisor::udf(),
                vec![expr.right],
            ));
            let op = if is_divide {
                Operator::Divide
            } else {
                Operator::Modulo
            };
            let planned = Expr::BinaryExpr(BinaryExpr::new(
                Box::new(expr.left),
                op,
                Box::new(guarded_right),
            ));
            return Ok(PlannerResult::Planned(planned));
        }
        // Beyond the decimal-divisor guard, only the integral `/` true-division lowering applies.
        if !is_divide {
            return Ok(PlannerResult::Original(expr));
        }
        // Both operands must be integral. Anything else is left exactly as DataFusion/Spark handle
        // it (decimal precision, already-double float, string/binary/bool/date/timestamp errors).
        if !is_integral(&left_ty) || !is_integral(&right_ty) {
            return Ok(PlannerResult::Original(expr));
        }
        // Route EVERY integral `/` through the internal `spark_divide(double, double)` UDF. It has a
        // static `Float64` return type — identical to a plain `CAST(l AS DOUBLE) / CAST(r AS DOUBLE)`
        // double divide for every non-zero (and null) divisor, so those rows are byte-identical — but
        // it ALSO raises Spark's ANSI `DIVIDE_BY_ZERO` whenever a divisor *actually evaluates to zero*
        // (eager `SELECT 5 / 0`, or a cast-zero divisor like `bigint('0')` that a literal-zero check
        // can't see). A plain double divide yields `Infinity` there, silently dropping a Spark error
        // (a forbidden missing-error regression); the UDF closes that gap for all integral `/` while
        // changing only the runtime-zero-divisor rows — which Spark ANSI always rejects. The static
        // DOUBLE type also lets a dead `1/0` branch in `if`/`coalesce`/`CASE` promote the column to
        // `double` and print `1.0`; those dead branches never hit the error (the constant-guard
        // `CASE`/`coalesce` is pruned by the simplifier before the UDF runs, and a dynamic branch is
        // evaluated only on matching rows). See `spark_functions::spark_divide`.
        let planned = Expr::ScalarFunction(ScalarFunction::new_udf(
            crate::spark_functions::spark_divide::udf(),
            vec![
                cast(expr.left, DataType::Float64),
                cast(expr.right, DataType::Float64),
            ],
        ));
        Ok(PlannerResult::Planned(planned))
    }
}

/// Lower every integral `*` whose Spark result type is `bigint` to the ANSI-checked
/// `spark_checked_mul` UDF, matching Spark's overflow contract.
///
/// Spark's `*` is ANSI-checked: an `Int64` product that overflows two's-complement raises
/// `ARITHMETIC_OVERFLOW` (`bigint(min) * bigint(-1)`, the unfiltered `q1 * q2` over `INT8_TBL`).
/// DataFusion's native `Int64` multiply *wraps* silently, yielding a corrupt value where Spark
/// errors — a forbidden missing-error gap.
///
/// This runs as a logical-plan rewrite, deliberately **after** [`spark_int_literals::
/// downcast_int_literals`], so every operand type it sees is already Spark-final: an in-range
/// integer literal is `Int32` (Spark `int`), so `int_col * 2` is an `int` product and is left alone,
/// while a genuine `bigint` operand (a `bigint` column, a `CAST(... AS BIGINT)`, or an out-of-range
/// literal) makes the product `bigint`. (Doing this at expression-planning time instead would see
/// DataFusion's transient pre-retyping `Int64` literal types and wrongly promote `int * 2` to
/// `bigint`.) For each integral `*` with at least one `Int64` operand we cast both operands to
/// `Int64` and route to `spark_checked_mul` (return type `Int64`, identical to the native multiply's
/// result type). The checked product equals the wrapping product whenever no overflow occurs, so
/// every non-overflowing row is byte-identical; only overflow rows change, and Spark ANSI rejects
/// those too — so this can only convert missing-error→error-parity, never pass→fail. A `NULL`
/// operand yields `NULL` (no error), exactly like Spark `*`. Output column names are preserved (the
/// [`NamePreserver`], like the literal-retype pass) so no by-name reference breaks, and the
/// per-node schema is recomputed; any node that cannot be re-validated aborts the rewrite back to
/// the original plan (never an error, never a partial plan).
/// Faithful TIGHTEN-to-REJECT for `IN`-lists that mix a constant string with a temporal operand.
///
/// Spark's `InTypeCoercion` casts every `IN` operand to the list's common type. When the operands
/// mix a `STRING` with a `DATE`/`TIMESTAMP`, the common type is the *temporal* one, so the string
/// side is ANSI-cast to it. For a constant string that can't parse as that temporal (e.g. `'1'`,
/// `'2'` — the values of `cast(1 as string)` / `cast(2 as string)`) that ANSI cast **fails at
/// runtime** with `CAST_INVALID_INPUT`, so the whole query errors. DataFusion's
/// `string_temporal_coercion` instead unifies the pair and silently produces a value, so weft
/// accepts a query Spark rejects (`missing-error`).
///
/// This pass walks the **raw** (pre-analysis) plan — where the `Expr::InList` is still intact and
/// each operand still carries its explicit `CAST(… AS <type>)` — and returns an error exactly when
/// a *constant* string operand provably cannot ANSI-cast to the list's temporal common type. It is
/// conservative on purpose:
/// - only fires when at least one operand is temporal AND at least one string operand is a constant;
/// - only rejects on a string constant whose cast to the temporal type yields NULL (parse failure) —
///   a *valid* temporal string (which Spark would accept) casts successfully and is left alone;
/// - non-constant string operands (columns) are never used to reject (Spark's per-row runtime error
///   can't be decided statically), so no currently-correct query is turned into an error.
///
/// Whenever it rejects, Spark also rejects the same query, so this can only move
/// `missing-error → error-parity`.
mod spark_in_temporal {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::logical_expr::expr::InList;
    use datafusion::logical_expr::{Expr, LogicalPlan};
    use weft_common::{Error, Result};

    fn is_temporal(dt: &DataType) -> bool {
        matches!(
            dt,
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
        )
    }

    /// A numeric (integral / floating / decimal) type — the set Spark deems type-incompatible with a
    /// temporal in an `IN` predicate (`DATATYPE_MISMATCH.DATA_DIFF_TYPES`).
    fn is_numeric(dt: &DataType) -> bool {
        matches!(
            dt,
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
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
        )
    }

    /// The top-level result type of an operand we can classify statically (an explicit `CAST`, or a
    /// bare literal). Anything else (a column ref, an arbitrary expression) returns `None` and is
    /// ignored — we never reject based on it, so a non-constant operand can never drive a rejection.
    fn operand_result_type(expr: &Expr) -> Option<DataType> {
        match expr {
            Expr::Cast(c) => Some(c.field.data_type().clone()),
            Expr::Literal(sv, _) => Some(sv.data_type()),
            _ => None,
        }
    }

    /// Spark rejects an `IN` predicate whose operands mix a *numeric* type with a *temporal*
    /// (DATE/TIMESTAMP) type as `DATATYPE_MISMATCH.DATA_DIFF_TYPES` — the two type families are not
    /// comparable. DataFusion, however, will happily coerce e.g. `INT IN (DATE)` (Date32 shares
    /// Int32's physical layout) and silently produce a value, so weft is too lenient (missing-error).
    /// When we can prove statically (every relevant operand is an explicit `CAST`/literal) that the
    /// list mixes the two families, return the rejection message. Whenever this fires, Spark also
    /// rejects the same query, so it can only move `missing-error → error-parity`.
    fn check_inlist(inlist: &InList) -> Option<String> {
        let operands = std::iter::once(inlist.expr.as_ref()).chain(inlist.list.iter());

        let mut temporal: Option<DataType> = None;
        let mut numeric: Option<DataType> = None;
        for op in operands {
            if let Some(dt) = operand_result_type(op) {
                if is_temporal(&dt) {
                    temporal.get_or_insert(dt);
                } else if is_numeric(&dt) {
                    numeric.get_or_insert(dt);
                }
            }
        }
        match (temporal, numeric) {
            (Some(t), Some(n)) => Some(format!(
                "[DATATYPE_MISMATCH.DATA_DIFF_TYPES] IN predicate mixes incompatible types {n} and \
                 {t} (Apache Spark rejects this query)"
            )),
            _ => None,
        }
    }

    /// Walk every expression in the plan and reject the first numeric/temporal `IN`-list.
    pub fn reject_invalid_in_temporal(plan: &LogicalPlan) -> Result<()> {
        let mut rejection: Option<String> = None;
        // `apply` over plan nodes; for each node scan its expressions for an offending `InList`.
        let _ = plan.apply(|node| {
            for expr in node.expressions() {
                let _ = expr.apply(|e| {
                    if let Expr::InList(inlist) = e {
                        if let Some(msg) = check_inlist(inlist) {
                            rejection = Some(msg);
                            return Ok(TreeNodeRecursion::Stop);
                        }
                    }
                    Ok(TreeNodeRecursion::Continue)
                });
                if rejection.is_some() {
                    break;
                }
            }
            if rejection.is_some() {
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        });
        match rejection {
            Some(msg) => Err(Error::Plan(msg)),
            None => Ok(()),
        }
    }
}

fn lower_checked_multiply(
    plan: datafusion::logical_expr::LogicalPlan,
) -> datafusion::logical_expr::LogicalPlan {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::expr_rewriter::NamePreserver;
    use std::cell::Cell;

    let changed = Cell::new(false);
    let rewritten = plan.clone().transform_up(|node| {
        // Operand types in this node's expressions resolve against its children's combined output
        // schema (Projection/Filter/Aggregate read their input; a Join's `ON` reads both sides).
        let mut input_schema = DFSchema::empty();
        for input in node.inputs() {
            input_schema.merge(input.schema());
        }
        let preserver = NamePreserver::new(&node);
        let mut node_changed = false;
        let t = node.map_expressions(|expr| {
            let saved = preserver.save(&expr);
            let r = rewrite_mul_expr(expr, &input_schema)?;
            node_changed |= r.transformed;
            Ok(r.update_data(|e| saved.restore(e)))
        })?;
        if node_changed {
            changed.set(true);
            // Recompute the node's schema so the `bigint` product type flows through consistently.
            let node = t.data.recompute_schema()?;
            Ok(Transformed::yes(node))
        } else {
            Ok(Transformed::no(t.data))
        }
    });
    match rewritten {
        Ok(t) if changed.get() => t.data,
        _ => plan,
    }
}

/// Rewrite every integral `*` (with at least one `Int64` operand) nested anywhere in one expression
/// into `spark_checked_mul(CAST(l AS BIGINT), CAST(r AS BIGINT))`. Operand types are resolved
/// against `schema`; an operand whose type can't be resolved leaves that `*` untouched.
fn rewrite_mul_expr(
    expr: datafusion::logical_expr::Expr,
    schema: &datafusion::common::DFSchema,
) -> datafusion::common::Result<
    datafusion::common::tree_node::Transformed<datafusion::logical_expr::Expr>,
> {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::logical_expr::expr::ScalarFunction;
    use datafusion::logical_expr::{cast, BinaryExpr, Expr, ExprSchemable, Operator};

    expr.transform_up(|e| {
        let Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Multiply,
            right,
        }) = &e
        else {
            return Ok(Transformed::no(e));
        };
        let (Ok(lt), Ok(rt)) = (left.get_type(schema), right.get_type(schema)) else {
            return Ok(Transformed::no(e));
        };
        // Both integral and at least one `Int64` (Spark `bigint` result). `Int32 * Int32` (and
        // narrower) keeps Spark's `int` result type — its overflow boundary is different and is left
        // on DataFusion. Decimal/float/double operands aren't integral, so they're untouched.
        if !is_integral(&lt) || !is_integral(&rt) {
            return Ok(Transformed::no(e));
        }
        if !matches!(lt, DataType::Int64) && !matches!(rt, DataType::Int64) {
            return Ok(Transformed::no(e));
        }
        let (l, r) = match e {
            Expr::BinaryExpr(BinaryExpr { left, right, .. }) => (*left, *right),
            _ => unreachable!("matched BinaryExpr above"),
        };
        let new = Expr::ScalarFunction(ScalarFunction::new_udf(
            crate::spark_functions::spark_checked_mul::udf(),
            vec![cast(l, DataType::Int64), cast(r, DataType::Int64)],
        ));
        Ok(Transformed::yes(new))
    })
}

/// Whether `t` is one of Spark's integral types (the signed/unsigned fixed-width integers). Decimal,
/// float, and double are intentionally excluded — only these need Spark's true-division lowering.
fn is_integral(t: &datafusion::arrow::datatypes::DataType) -> bool {
    use datafusion::arrow::datatypes::DataType::{
        Int16, Int32, Int64, Int8, UInt16, UInt32, UInt64, UInt8,
    };
    matches!(
        t,
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64
    )
}

/// AND for the `ALL` quantifier, OR for `ANY`/`SOME`.
#[derive(Clone, Copy)]
enum LikeQuantifier {
    All,
    Any,
}

/// Cheap pre-check: does `sql` contain a `[I]LIKE {ALL|ANY|SOME}` token sequence? Gates the
/// statement-rewrite path in [`Engine::create_logical_plan_spark`] so the overwhelmingly common
/// case keeps planning through DataFusion's `create_logical_plan` untouched. A false positive is
/// harmless — the rewrite is a no-op and the AST path is otherwise identical to
/// `create_logical_plan` (which *is* `sql_to_statement` + `statement_to_plan`).
fn contains_like_quantifier(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    // Every `[I]LIKE` ends in the substring "like"; find each and look at the following token.
    for (i, _) in lower.match_indices("like") {
        let mut j = i + 4;
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        let rest = &lower[j..];
        if rest.starts_with("all") || rest.starts_with("any") || rest.starts_with("some") {
            return true;
        }
    }
    false
}

/// Lower every Spark `e [NOT] [I]LIKE {ALL|ANY|SOME} (p1, …, pn)` quantified predicate in `stmt`
/// into the equivalent boolean fold of ordinary `[I]LIKE` predicates.
///
/// DataFusion cannot plan these forms: sqlparser mis-parses `ALL`/`SOME` as a call to a missing
/// scalar function (`all`/`some`) and the planner rejects the `ANY` form outright ("ANY in LIKE
/// expression"). The lowering reproduces Spark's `LikeAll`/`NotLikeAll`/`LikeAny`/`NotLikeAny`
/// semantics exactly, including SQL three-valued NULL handling:
///
/// - `e [I]LIKE ALL (p1,…,pn)`        → `(e [I]LIKE p1) AND … AND (e [I]LIKE pn)`
/// - `e NOT [I]LIKE ALL (p1,…,pn)`    → `(e NOT [I]LIKE p1) AND … AND (e NOT [I]LIKE pn)`
/// - `e [I]LIKE ANY|SOME (p1,…,pn)`   → `(e [I]LIKE p1) OR … OR (e [I]LIKE pn)`
/// - `e NOT [I]LIKE ANY|SOME (…)`     → `(e NOT [I]LIKE p1) OR … OR (e NOT [I]LIKE pn)`
///
/// (Spark's `NotLikeAll`/`NotLikeAny` distribute the `NOT` onto each pattern but keep the AND/OR
/// connective — matched here.) An empty pattern list is left untouched, so Spark's parse-error
/// parity for `LIKE ALL ()` is preserved. This is a faithful, EQUIVALENT-plan lowering at the AST
/// level: each rewritten node is structurally an AND/OR tree of `[I]LIKE` nodes, so the enclosing
/// plan, operator grouping (tree shape), and `WHERE`/`CASE`/outer-`NOT` context are all preserved.
fn lower_like_quantifiers(stmt: &mut datafusion::sql::sqlparser::ast::Statement) {
    use datafusion::sql::sqlparser::ast::{visit_expressions_mut, Expr};
    use std::ops::ControlFlow;
    // Post-order visit: children are rewritten before their parent, so a quantifier nested inside
    // another expression is handled correctly and the replacement we install is final.
    let _ = visit_expressions_mut(stmt, |expr: &mut Expr| {
        if let Some(lowered) = lower_like_quantifier_expr(expr) {
            *expr = lowered;
        }
        ControlFlow::<()>::Continue(())
    });
}

/// If `expr` is a Spark `[I]LIKE {ALL|ANY|SOME} (...)` node, return its equivalent AND/OR fold of
/// plain `[I]LIKE` predicates; otherwise `None` (an ordinary `[I]LIKE`, or any other expression, is
/// left untouched).
fn lower_like_quantifier_expr(
    expr: &datafusion::sql::sqlparser::ast::Expr,
) -> Option<datafusion::sql::sqlparser::ast::Expr> {
    use datafusion::sql::sqlparser::ast::{BinaryOperator, Expr};

    // `any` is sqlparser's flag for the `ANY` keyword; `case_insensitive` distinguishes ILIKE.
    let (negated, any_flag, left, pattern, escape_char, case_insensitive) = match expr {
        Expr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => (
            *negated,
            *any,
            expr.as_ref(),
            pattern.as_ref(),
            escape_char,
            false,
        ),
        Expr::ILike {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => (
            *negated,
            *any,
            expr.as_ref(),
            pattern.as_ref(),
            escape_char,
            true,
        ),
        _ => return None,
    };

    let (patterns, quant) = if any_flag {
        // `[I]LIKE ANY (...)`: sqlparser consumed the ANY keyword and parsed the pattern list as a
        // parenthesized expression (`Tuple` for ≥2 patterns, `Nested` for a single one).
        (parenthesized_pattern_list(pattern)?, LikeQuantifier::Any)
    } else {
        // `[I]LIKE ALL (...)` / `... SOME (...)`: ALL/SOME are not the ANY keyword, so sqlparser
        // parsed the list as a call to a (missing) function named `all`/`any`/`some`.
        function_pattern_list(pattern)?
    };
    if patterns.is_empty() {
        // Empty list — Spark raises a parse error; leave the node untouched to keep that parity.
        return None;
    }

    let op = match quant {
        LikeQuantifier::All => BinaryOperator::And,
        LikeQuantifier::Any => BinaryOperator::Or,
    };
    let mut folded: Option<Expr> = None;
    for p in patterns {
        let one = make_like(
            case_insensitive,
            negated,
            left.clone(),
            p,
            escape_char.clone(),
        );
        folded = Some(match folded {
            None => one,
            Some(acc) => Expr::BinaryOp {
                left: Box::new(acc),
                op: op.clone(),
                right: Box::new(one),
            },
        });
    }
    folded
}

/// Extract the pattern list of a parenthesized `(p1, …, pn)` (the parsed form of `[I]LIKE ANY`'s
/// argument). `None` for any other shape (e.g. a subquery), which keeps DataFusion's existing
/// behavior for that node.
fn parenthesized_pattern_list(
    pattern: &datafusion::sql::sqlparser::ast::Expr,
) -> Option<Vec<datafusion::sql::sqlparser::ast::Expr>> {
    use datafusion::sql::sqlparser::ast::Expr;
    match pattern {
        Expr::Tuple(items) => Some(items.clone()),
        Expr::Nested(inner) => Some(vec![inner.as_ref().clone()]),
        _ => None,
    }
}

/// Extract the pattern list (and AND/OR quantifier) from the function form `all(...)`/`some(...)`/
/// `any(...)` that sqlparser produces for `[I]LIKE ALL|SOME (...)`. Returns `None` for anything
/// that isn't a bare single-identifier positional call (so a real function call wearing one of
/// those names, or one decorated with DISTINCT/ORDER BY/FILTER/OVER, is never misinterpreted).
fn function_pattern_list(
    pattern: &datafusion::sql::sqlparser::ast::Expr,
) -> Option<(Vec<datafusion::sql::sqlparser::ast::Expr>, LikeQuantifier)> {
    use datafusion::sql::sqlparser::ast::{
        Expr, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectNamePart,
    };
    let Expr::Function(func) = pattern else {
        return None;
    };
    let [ObjectNamePart::Identifier(ident)] = func.name.0.as_slice() else {
        return None;
    };
    let quant = if ident.value.eq_ignore_ascii_case("all") {
        LikeQuantifier::All
    } else if ident.value.eq_ignore_ascii_case("any") || ident.value.eq_ignore_ascii_case("some") {
        LikeQuantifier::Any
    } else {
        return None;
    };
    // Reject any call decoration — only the plain `name(p1, …, pn)` sugar is the quantifier form.
    if func.uses_odbc_syntax
        || func.over.is_some()
        || func.filter.is_some()
        || func.null_treatment.is_some()
        || !func.within_group.is_empty()
        || !matches!(func.parameters, FunctionArguments::None)
    {
        return None;
    }
    let FunctionArguments::List(list) = &func.args else {
        return None;
    };
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
        return None;
    }
    let mut patterns = Vec::with_capacity(list.args.len());
    for arg in &list.args {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => patterns.push(e.clone()),
            _ => return None,
        }
    }
    Some((patterns, quant))
}

/// Build a single ordinary `[I]LIKE` predicate node (`any: false`).
fn make_like(
    case_insensitive: bool,
    negated: bool,
    left: datafusion::sql::sqlparser::ast::Expr,
    pattern: datafusion::sql::sqlparser::ast::Expr,
    escape_char: Option<datafusion::sql::sqlparser::ast::ValueWithSpan>,
) -> datafusion::sql::sqlparser::ast::Expr {
    use datafusion::sql::sqlparser::ast::Expr;
    if case_insensitive {
        Expr::ILike {
            negated,
            any: false,
            expr: Box::new(left),
            pattern: Box::new(pattern),
            escape_char,
        }
    } else {
        Expr::Like {
            negated,
            any: false,
            expr: Box::new(left),
            pattern: Box::new(pattern),
            escape_char,
        }
    }
}

/// Cheap text pre-check: could `sql` contain an ordered-set / window percentile shape that Spark
/// rejects but DataFusion would happily plan? Mirrors [`contains_like_quantifier`] — a false
/// positive only costs one extra parse + AST walk, and a false negative is impossible for the
/// shapes [`unsupported_percentile_shape`] rejects, because every one of them lexes either
/// `within group` or an `over`-decorated `median`/`percentile_cont`/`percentile_disc` call.
fn contains_percentile_reject_precheck(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    if lower.contains("within group") {
        return true;
    }
    lower.contains("over")
        && (lower.contains("median")
            || lower.contains("percentile_cont")
            || lower.contains("percentile_disc"))
}

/// Spark rejects several ordered-set / window percentile shapes that DataFusion accepts and plans.
/// If `stmt` contains one, return the matching Spark error text so [`Engine::create_logical_plan_spark`]
/// can surface an `Err` (turning a silent missing-error / engine-panic into error-parity). Every
/// shape below is a faithful rejection — Apache Spark v4.0.0 also errors on it, so no currently
/// correct result can change:
///
/// - `WITHIN GROUP (ORDER BY ...)` on any function other than `percentile_cont` / `percentile_disc`
///   / `mode` / `listagg` (`string_agg`) — Spark: `INVALID_SQL_SYNTAX.FUNCTION_WITH_UNSUPPORTED_SYNTAX`.
/// - `DISTINCT` inside a `WITHIN GROUP` aggregate — Spark: `INVALID_WITHIN_GROUP_EXPRESSION.DISTINCT_UNSUPPORTED`.
/// - `median` / `percentile_cont` / `percentile_disc` used as a *window* function whose resolved
///   frame is not the whole partition — i.e. it carries an `ORDER BY` (a running frame) or an
///   explicit frame other than `UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`. Spark:
///   `INVALID_WINDOW_SPEC_FOR_AGGREGATION_FUNC`.
fn unsupported_percentile_shape(
    stmt: &datafusion::sql::sqlparser::ast::Statement,
) -> Option<String> {
    use datafusion::sql::sqlparser::ast::{
        Expr, NamedWindowExpr, Select, Visit, Visitor, WindowSpec,
    };
    use std::collections::HashMap;
    use std::ops::ControlFlow;

    struct PercentileRejectVisitor {
        // Maps a named window (lowercased) to its spec, so `OVER w` can be resolved against the
        // enclosing `SELECT`'s `WINDOW w AS (...)` clause. `pre_visit_select` runs before the
        // select's projection expressions, so the map is populated before any `OVER w` is checked.
        named_windows: HashMap<String, WindowSpec>,
    }
    impl Visitor for PercentileRejectVisitor {
        type Break = String;
        fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<String> {
            for def in &select.named_window {
                if let NamedWindowExpr::WindowSpec(spec) = &def.1 {
                    self.named_windows
                        .insert(def.0.value.to_ascii_lowercase(), spec.clone());
                }
            }
            ControlFlow::Continue(())
        }
        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<String> {
            if let Expr::Function(func) = expr {
                if let Some(msg) = check_percentile_function(func, &self.named_windows) {
                    return ControlFlow::Break(msg);
                }
            }
            ControlFlow::Continue(())
        }
    }

    let mut visitor = PercentileRejectVisitor {
        named_windows: HashMap::new(),
    };
    match stmt.visit(&mut visitor) {
        ControlFlow::Break(msg) => Some(msg),
        ControlFlow::Continue(()) => None,
    }
}

/// Inspect a single function-call node for a Spark-rejected percentile/ordered-set shape (see
/// [`unsupported_percentile_shape`] for the catalogue). `named_windows` resolves an `OVER w`
/// reference to its `WINDOW w AS (...)` spec.
fn check_percentile_function(
    func: &datafusion::sql::sqlparser::ast::Function,
    named_windows: &std::collections::HashMap<String, datafusion::sql::sqlparser::ast::WindowSpec>,
) -> Option<String> {
    use datafusion::sql::sqlparser::ast::{
        DuplicateTreatment, FunctionArguments, ObjectNamePart, WindowType,
    };
    let name = match func.name.0.last() {
        Some(ObjectNamePart::Identifier(ident)) => ident.value.to_ascii_lowercase(),
        _ => return None,
    };

    // Shapes 1 + 2: `WITHIN GROUP (ORDER BY ...)` decorations.
    if !func.within_group.is_empty() {
        const WITHIN_GROUP_ALLOWED: [&str; 5] = [
            "percentile_cont",
            "percentile_disc",
            "mode",
            "listagg",
            "string_agg",
        ];
        if !WITHIN_GROUP_ALLOWED.contains(&name.as_str()) {
            return Some(format!(
                "[INVALID_SQL_SYNTAX.FUNCTION_WITH_UNSUPPORTED_SYNTAX] The function `{name}` does not support the WITHIN GROUP (ORDER BY ...) clause."
            ));
        }
        // `DISTINCT` inside a WITHIN GROUP aggregate is unconditionally rejected by Spark only for
        // the percentile/mode ordered-set aggregates. `listagg`/`string_agg` *do* accept DISTINCT
        // (Spark only errors when the ordering key disagrees with the distinct input — a different,
        // value-dependent check we deliberately don't reproduce), so they are excluded here.
        const DISTINCT_FORBIDDEN: [&str; 3] = ["percentile_cont", "percentile_disc", "mode"];
        if DISTINCT_FORBIDDEN.contains(&name.as_str()) {
            if let FunctionArguments::List(list) = &func.args {
                if matches!(list.duplicate_treatment, Some(DuplicateTreatment::Distinct)) {
                    return Some(format!(
                        "[INVALID_WITHIN_GROUP_EXPRESSION.DISTINCT_UNSUPPORTED] DISTINCT is not supported inside the WITHIN GROUP aggregate `{name}`."
                    ));
                }
            }
        }
    }

    // Shape 3: `median` / `percentile_cont` / `percentile_disc` as a window function whose resolved
    // frame is not the whole partition.
    const WINDOW_FAMILY: [&str; 3] = ["median", "percentile_cont", "percentile_disc"];
    if WINDOW_FAMILY.contains(&name.as_str()) {
        let spec = match &func.over {
            Some(WindowType::WindowSpec(spec)) => Some(spec.clone()),
            Some(WindowType::NamedWindow(ident)) => named_windows
                .get(&ident.value.to_ascii_lowercase())
                .cloned(),
            None => None,
        };
        if let Some(spec) = spec {
            if !window_frame_is_full_partition(&spec) {
                return Some(format!(
                    "[INVALID_WINDOW_SPEC_FOR_AGGREGATION_FUNC] The window function `{name}` requires the window to span the whole partition (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)."
                ));
            }
        }
    }
    None
}

/// Whether `spec`'s *resolved* frame spans the entire partition — Spark's only valid frame for
/// ordered-set / median window aggregates. With no explicit frame, the frame is the whole partition
/// only when there is also no `ORDER BY` (an `ORDER BY` without an explicit frame resolves to the
/// running `RANGE UNBOUNDED PRECEDING .. CURRENT ROW`, which is *not* full).
fn window_frame_is_full_partition(spec: &datafusion::sql::sqlparser::ast::WindowSpec) -> bool {
    use datafusion::sql::sqlparser::ast::WindowFrameBound;
    match &spec.window_frame {
        None => spec.order_by.is_empty(),
        Some(frame) => {
            matches!(frame.start_bound, WindowFrameBound::Preceding(None))
                && matches!(frame.end_bound, Some(WindowFrameBound::Following(None)))
        }
    }
}

/// Monotonic counter giving each [`Engine`] a unique managed-warehouse subdirectory (combined
/// with the process id) so concurrent engines never share table storage.
static WAREHOUSE_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The CPU execution engine: a DataFusion [`SessionContext`] today, growing native
/// operators behind the same surface in Phase 1.
pub struct Engine {
    ctx: Arc<SessionContext>,
    /// Per-engine managed warehouse directory. Spark's `CREATE TABLE … USING <fmt>` is lowered to
    /// a real `CREATE EXTERNAL TABLE … LOCATION '<warehouse>/<name>/'` whose data lives in actual
    /// `<fmt>` files under here (see [`spark_create_table`]). One directory per `Engine` isolates
    /// otherwise-colliding table names across files and is removed on `Drop`.
    warehouse: PathBuf,
    /// Lowercased names of the session-temporary views created so far in this engine's lifetime
    /// (`CREATE [GLOBAL] TEMP[ORARY] VIEW <name>`). Spark forbids a *persistent* `CREATE VIEW` from
    /// referencing any of these (SPARK-29628 / `INVALID_TEMP_OBJ_REFERENCE`); DataFusion has no
    /// temp/permanent distinction and would silently accept it, so we track the temp set ourselves
    /// and reject the offending persistent view to keep error-parity with Spark. A name is removed
    /// when a later persistent view re-uses it (DataFusion's single namespace would shadow it).
    temp_views: Mutex<HashSet<String>>,
    /// The external [`weft_catalog::CatalogProvider`]s registered via [`Engine::register_catalog`],
    /// keyed by their registered name. Held alongside the DataFusion bridge so the engine can answer
    /// `SHOW DATABASES`/`SHOW TABLES IN …` authoritatively (the bridge only exposes a best-effort,
    /// already-materialized listing). See the SHOW interception in [`Engine::sql`].
    weft_catalogs: Mutex<HashMap<String, Arc<dyn weft_catalog::CatalogProvider>>>,
}

impl Engine {
    /// Create a fresh engine with default session state.
    ///
    /// If `WEFT_MEMORY_LIMIT_BYTES` is set, the engine runs with a bounded spill pool of
    /// that size (DataFusion spills aggregations/sorts to disk instead of OOM-killing the
    /// process) — important when running ClickBench on a memory-constrained box. Unset
    /// (the default) keeps the unbounded pool, so local/test behavior is unchanged.
    ///
    /// Phase 1.4 margin-push knobs, each applied only when its env var is set (so the default
    /// behavior is unchanged and the values can be swept on a benchmark box without a rebuild):
    /// - `WEFT_TARGET_PARTITIONS` (usize) — scan/aggregation parallelism (default = vCPUs).
    /// - `WEFT_BATCH_SIZE` (usize) — vectorized batch size (default 8192).
    /// - `WEFT_COALESCE_BATCHES` (bool) — coalesce small batches after filtering.
    /// - `WEFT_REPARTITION_AGGREGATIONS` (bool) — repartition before aggregation for parallelism
    ///   (the lever most likely to move the high-card `GROUP BY` queries Q32–Q34).
    pub fn new() -> Self {
        use datafusion::prelude::SessionConfig;

        let mut config = SessionConfig::new();
        if let Some(p) = env_usize("WEFT_TARGET_PARTITIONS") {
            config = config.with_target_partitions(p);
        }
        if let Some(n) = env_usize("WEFT_BATCH_SIZE") {
            config = config.with_batch_size(n);
        }
        // ClickBench-winning scan settings (mirrors DataFusion's published entry + what Sail
        // tunes): push filters into the Parquet decoder, reorder them by selectivity, read
        // binary columns as strings, and use Arrow StringView for big string columns (URL,
        // Title, Referer) — decisive for the string/scan-heavy queries (Q20–Q28, Q34/Q35).
        {
            let opts = config.options_mut();
            // Parse SQL the Spark way: the Databricks dialect (Databricks SQL *is* Spark SQL) uses
            // backticks for identifiers and treats `"..."` as a STRING LITERAL — Spark's default
            // (`spark.sql.ansi.double_quoted_identifiers=false`). DataFusion's Generic dialect treats
            // `"..."` as an identifier, which mis-parses Spark string literals like
            // `next_day("2015-07-23", "Mon")`.
            opts.sql_parser.dialect = datafusion::common::config::Dialect::Databricks;
            // Spark's default NULL ordering treats NULL as the smallest value (ASC → NULLS FIRST,
            // DESC → NULLS LAST), whereas DataFusion defaults to Postgres's `nulls_max` (ASC →
            // NULLS LAST). Matching Spark makes weft's implicit ORDER BY (including window-function
            // ORDER BY, where it changes computed running aggregates, not just row order) produce
            // Spark's committed output.
            opts.sql_parser.default_null_ordering = "nulls_min".to_string();
            // Spark's default null ordering is ASC NULLS FIRST / DESC NULLS LAST, expressed by
            // DataFusion's `nulls_min`. DataFusion's own default (`nulls_max`, Postgres-style ASC
            // NULLS LAST) silently flips both the outer ORDER BY *and* the within-window ORDER BY,
            // which changes window-frame contents (e.g. a NULL row's RANGE/ROWS neighbours) and the
            // final row order. Aligning the default is a faithful match to Spark.
            opts.sql_parser.default_null_ordering = "nulls_min".to_string();
            opts.execution.parquet.pushdown_filters = true;
            opts.execution.parquet.reorder_filters = true;
            opts.execution.parquet.binary_as_string = true;
            opts.execution.parquet.schema_force_view_types = true;
            if let Some(b) = env_bool("WEFT_COALESCE_BATCHES") {
                opts.execution.coalesce_batches = b;
            }
            if let Some(b) = env_bool("WEFT_REPARTITION_AGGREGATIONS") {
                opts.optimizer.repartition_aggregations = b;
            }
        }

        let mut ctx = match std::env::var("WEFT_MEMORY_LIMIT_BYTES")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            Some(bytes) => {
                use datafusion::execution::memory_pool::FairSpillPool;
                use datafusion::execution::runtime_env::RuntimeEnvBuilder;
                use std::sync::Arc;
                let env = RuntimeEnvBuilder::new()
                    .with_memory_pool(Arc::new(FairSpillPool::new(bytes)))
                    .build_arc()
                    .expect("runtime env");
                SessionContext::new_with_config_rt(config, env)
            }
            None => SessionContext::new_with_config(config),
        };
        register_spark_function_aliases(&ctx);
        spark_functions::register(&ctx);
        // Spark's `/` is true (double) division for non-decimal operands; lower integral `/` to a
        // double divide so it returns Spark's value/type instead of DataFusion's truncating integer
        // division. Additive: only Divide between two integral operands is rewritten (see
        // `SparkDividePlanner`); registration only appends a planner and cannot fail.
        {
            use datafusion::execution::FunctionRegistry;
            let _ = ctx.register_expr_planner(Arc::new(SparkDividePlanner));
        }
        // A process+atomic-unique managed warehouse dir for `CREATE TABLE … USING <fmt>` tables.
        // Created lazily (per-table `create_dir_all` in `Engine::sql`) and torn down on `Drop`.
        let id = WAREHOUSE_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let warehouse = std::env::temp_dir().join("weft-warehouse").join(format!(
            "{}-{}",
            std::process::id(),
            id
        ));
        Self {
            ctx: Arc::new(ctx),
            warehouse,
            temp_views: Mutex::new(HashSet::new()),
            weft_catalogs: Mutex::new(HashMap::new()),
        }
    }

    /// Run a SQL string and collect the result as Arrow record batches.
    ///
    /// Errors are mapped onto the Weft error model: a planning/analysis failure becomes
    /// [`Error::Plan`] (→ Spark `AnalysisException`), an execution failure [`Error::Execution`].
    pub async fn sql(&self, query: &str) -> Result<Vec<RecordBatch>> {
        // Spark rejects multi-column `COUNT(DISTINCT a, b)` at analysis time; DataFusion panics.
        // Reject early so the parity harness records `exec-error` instead of `engine-panic`.
        if is_multi_arg_count_distinct(query) {
            return Err(Error::Plan(
                "COUNT(DISTINCT) does not support multiple columns".into(),
            ));
        }
        // Catalog-listing statements (`SHOW DATABASES`/`SHOW SCHEMAS`[ IN <cat>],
        // `SHOW TABLES IN <cat>[.<db>]`) are served straight from the registered weft catalogs —
        // DataFusion's parser rejects most of these shapes and its bridge can only see
        // already-materialized listings, so we answer them here before any planning. `parse_show`
        // returns `None` for anything that isn't one of these forms, leaving every other statement
        // (including bare `SHOW TABLES`) to flow through unchanged.
        if let Some(show) = parse_show(query) {
            return self.run_show(&show).await;
        }
        // SPARK-29628 (`INVALID_TEMP_OBJ_REFERENCE`): a *persistent* `CREATE VIEW` may not reference
        // a session-temporary view. DataFusion has no temp/permanent distinction (we strip the
        // `TEMPORARY` keyword in `normalize_spark_sql` so it plans), so it would silently accept the
        // body and weft would drop Spark's analysis error. Detect the offending shape up front and
        // reject it so both engines reject (error-parity). `analyze_create_view` returns `None` for
        // anything that isn't a parseable `CREATE VIEW`, leaving every other statement untouched.
        let create_view = analyze_create_view(query);
        if let Some(cv) = &create_view {
            if !cv.temporary {
                let temp = self.temp_views.lock().unwrap();
                if let Some(referenced) = cv.relations.iter().find(|r| temp.contains(*r)) {
                    return Err(Error::Plan(format!(
                        "[INVALID_TEMP_OBJ_REFERENCE] Cannot create the persistent object \
                         `{}` of the type VIEW because it references the temporary object \
                         `{referenced}` of the type VIEW. SQLSTATE: 42K0F",
                        cv.name
                    )));
                }
            }
        }
        // Faithful lowering of Spark's `CREATE TABLE … USING <fmt>` to a real, format-backed
        // `CREATE EXTERNAL TABLE` (genuine durable storage — NOT the forbidden MemTable shim). On
        // success the statement produces no result set, matching Spark's `struct<>`. If the lowered
        // DDL fails to plan/execute (exotic column types, etc.) we fall through to the normal path,
        // which reproduces the original parse error — so an unsupported CREATE stays in exactly the
        // bucket it failed in before (never a regression).
        if let Some(low) = spark_create_table::lower_create_table_using(query, &self.warehouse) {
            if self.run_create_external(&low).await.is_ok() {
                return Ok(vec![]);
            }
        } else if let Some(ctas) =
            spark_create_table::lower_create_table_ctas(query, &self.warehouse)
        {
            if self.run_create_table_ctas(&ctas).await.is_ok() {
                return Ok(vec![]);
            }
        } else if spark_create_table::is_insert(query) {
            // Spark's `spark.sql("INSERT …")` returns an empty DataFrame; DataFusion returns a
            // one-row `count`. Execute the write for its side effects, then drop the count row so
            // the result renders as Spark's `struct<>` + empty.
            let df = self.plan_spark(query).await?;
            df.collect()
                .await
                .map_err(|e| Error::Execution(e.to_string()))?;
            return Ok(vec![]);
        }
        let df = self.plan_spark(query).await?;
        let batches = df
            .collect()
            .await
            .map_err(|e| Error::Execution(e.to_string()))?;
        // The view planned/created successfully — update the temp-view registry. A new temporary
        // view is recorded; a persistent view with the same name removes any prior temp entry
        // (DataFusion keeps a single namespace, so the persistent definition now shadows it).
        if let Some(cv) = create_view {
            let mut temp = self.temp_views.lock().unwrap();
            if cv.temporary {
                temp.insert(cv.name);
            } else {
                temp.remove(&cv.name);
            }
        }
        Ok(batches)
    }

    /// Create the managed directory and run a lowered `CREATE EXTERNAL TABLE` DDL, materializing a
    /// real format-backed [`datafusion`] `ListingTable`. The directory must exist before any
    /// empty-table SELECT (which lists it), so we `create_dir_all` first.
    async fn run_create_external(&self, low: &spark_create_table::Lowered) -> Result<()> {
        std::fs::create_dir_all(&low.table_dir).map_err(|e| Error::Execution(e.to_string()))?;
        let ddl = normalize_spark_sql(&low.ddl);
        self.ctx
            .sql(ddl.as_ref())
            .await
            .map_err(|e| Error::Plan(e.to_string()))?
            .collect()
            .await
            .map_err(|e| Error::Execution(e.to_string()))?;
        Ok(())
    }

    /// CTAS: execute SELECT, write result as format files, then CREATE EXTERNAL TABLE.
    async fn run_create_table_ctas(&self, ctas: &spark_create_table::LoweredCtas) -> Result<()> {
        std::fs::create_dir_all(&ctas.table_dir).map_err(|e| Error::Execution(e.to_string()))?;
        let batches = self
            .plan_spark(&ctas.select_sql)
            .await?
            .collect()
            .await
            .map_err(|e| Error::Execution(e.to_string()))?;
        if !batches.is_empty() {
            let ext = match ctas.fmt.as_str() {
                "json" => "json",
                "csv" => "csv",
                _ => "parquet",
            };
            let file = ctas.table_dir.join(format!("part-00000.{ext}"));
            use datafusion::parquet::arrow::ArrowWriter;
            let schema = batches[0].schema();
            let f = std::fs::File::create(&file).map_err(|e| Error::Execution(e.to_string()))?;
            let mut writer = ArrowWriter::try_new(f, schema, None)
                .map_err(|e| Error::Execution(e.to_string()))?;
            for b in &batches {
                writer
                    .write(b)
                    .map_err(|e| Error::Execution(e.to_string()))?;
            }
            writer
                .close()
                .map_err(|e| Error::Execution(e.to_string()))?;
        }
        let ddl = normalize_spark_sql(&ctas.ddl);
        self.ctx
            .sql(ddl.as_ref())
            .await
            .map_err(|e| Error::Plan(e.to_string()))?
            .collect()
            .await
            .map_err(|e| Error::Execution(e.to_string()))?;
        Ok(())
    }

    /// Resolve the result schema of `query` without executing it — the logical-plan schema.
    /// Used by Spark Connect `AnalyzePlan(Schema)` (PySpark `df.schema` / `printSchema`).
    pub async fn schema(&self, query: &str) -> Result<arrow::datatypes::SchemaRef> {
        let df = self.plan_spark(query).await?;
        Ok(std::sync::Arc::new(df.schema().as_arrow().clone()))
    }

    /// Plan `query` and rewrite its top output projection to use Spark-compatible column names, so
    /// the executed result and `df.schema` both expose the same column names Spark would. Shared by
    /// [`Engine::sql`] and [`Engine::schema`] so the two never disagree.
    async fn plan_spark(&self, query: &str) -> Result<datafusion::dataframe::DataFrame> {
        let query = match spark_decimal::rewrite_decimal_string_compare(query) {
            Some(q) => std::borrow::Cow::Owned(q),
            None => normalize_spark_sql(query),
        };
        // Plan WITHOUT executing. `ctx.sql()` eagerly runs DDL (e.g. `CREATE VIEW`) inside its
        // call, registering the view *before* we could retype its body — so we go one level down:
        // `create_logical_plan` returns the raw, un-analyzed plan, we (1) retype in-range integer
        // literals to Int32 (Spark's `INT` default vs DataFusion's `BIGINT`) and (2) apply Spark
        // output column names, then hand the rewritten plan to `execute_logical_plan` (which runs
        // any DDL / builds the lazy DataFrame). Under the default `SQLOptions` `ctx.sql()` uses,
        // all statement kinds are allowed, so this is behavior-equivalent plus the two rewrites.
        let plan = self.create_logical_plan_spark(query.as_ref()).await?;
        // Faithful TIGHTEN-to-REJECT: Spark rejects an `IN`-list whose operands mix a numeric type
        // with a temporal (DATE/TIMESTAMP) type as `DATATYPE_MISMATCH.DATA_DIFF_TYPES` (the two type
        // families are incomparable, e.g. `cast(1 as int) IN (cast('…' as date))`). DataFusion
        // instead coerces them (Date32 shares Int32's layout) and silently yields a value, so weft is
        // too lenient (missing-error). Detect the mix on the raw plan and reject so both engines do.
        spark_in_temporal::reject_invalid_in_temporal(&plan)?;
        // Order is load-bearing. `project_spark_names` runs FIRST, on the raw plan, so it sees the
        // bare (un-aliased) anonymous literal columns and renames them to their Spark names — its
        // outer projection then references the inner columns by their original DataFusion names.
        // `downcast_int_literals` runs SECOND and *preserves* exactly those names while retyping
        // Int64→Int32, so the Spark-name projection (and every other by-name reference) keeps
        // resolving. Reversing the order would hide the literals behind name-preserving aliases and
        // defeat the Spark-name pass.
        let plan = spark_names::project_spark_names(plan);
        let plan = spark_int_literals::downcast_int_literals(plan);
        // Lower integral `*` with a `bigint` result to the ANSI-checked-overflow UDF. Runs AFTER the
        // literal retype so operand types are Spark-final (an in-range literal is `int`, so `int * 2`
        // stays `int` and is not promoted to `bigint`). See `lower_checked_multiply`.
        let plan = lower_checked_multiply(plan);
        self.ctx
            .execute_logical_plan(plan)
            .await
            .map_err(|e| Error::Plan(e.to_string()))
    }

    /// Build the raw (un-analyzed) logical plan for `query`, first lowering any Spark
    /// `[I]LIKE {ALL|ANY|SOME} (...)` quantified predicate that DataFusion's planner cannot handle
    /// (see [`lower_like_quantifiers`]). For every other query this is exactly
    /// [`SessionState::create_logical_plan`], which itself is `sql_to_statement` followed by
    /// `statement_to_plan` — so the gated fast path and the rewrite path produce identical plans
    /// for any query without an `[I]LIKE` quantifier.
    async fn create_logical_plan_spark(
        &self,
        query: &str,
    ) -> Result<datafusion::logical_expr::LogicalPlan> {
        use datafusion::sql::parser::Statement as DFStatement;
        let state = self.ctx.state();
        // Spark rejects several ordered-set / window percentile shapes (WITHIN GROUP on an
        // unsupported function, DISTINCT inside WITHIN GROUP, a percentile/median window with a
        // non-full-partition frame) that DataFusion would silently plan. Detect them up front and
        // return an error so weft matches Spark's rejection (error-parity). The pre-check keeps the
        // overwhelmingly common case on the untouched fast path below.
        if contains_percentile_reject_precheck(query) {
            let dialect = state.config().options().sql_parser.dialect;
            if let Ok(DFStatement::Statement(inner)) = state.sql_to_statement(query, &dialect) {
                if let Some(msg) = unsupported_percentile_shape(inner.as_ref()) {
                    return Err(Error::Plan(msg));
                }
            }
        }
        if !contains_like_quantifier(query) {
            return state
                .create_logical_plan(query)
                .await
                .map_err(|e| Error::Plan(e.to_string()));
        }
        let dialect = state.config().options().sql_parser.dialect;
        let mut statement = state
            .sql_to_statement(query, &dialect)
            .map_err(|e| Error::Plan(e.to_string()))?;
        if let DFStatement::Statement(inner) = &mut statement {
            lower_like_quantifiers(inner.as_mut());
        }
        state
            .statement_to_plan(statement)
            .await
            .map_err(|e| Error::Plan(e.to_string()))
    }

    /// Build the optimized DataFusion physical plan for `query`. The driver side of
    /// distributed execution uses this to obtain a serializable plan to split into stages.
    pub async fn physical_plan(
        &self,
        query: &str,
    ) -> Result<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let df = self
            .ctx
            .sql(query)
            .await
            .map_err(|e| Error::Plan(e.to_string()))?;
        df.create_physical_plan()
            .await
            .map_err(|e| Error::Execution(e.to_string()))
    }

    /// Build the (unoptimized) logical plan for a SQL query, without executing it.
    /// Used by Spark Connect `AnalyzePlan(Explain)` for a `spark.sql(...)` command.
    pub async fn logical_plan(&self, query: &str) -> Result<datafusion::logical_expr::LogicalPlan> {
        self.ctx
            .state()
            .create_logical_plan(query)
            .await
            .map_err(|e| Error::Plan(e.to_string()))
    }

    /// Render a Spark-style `EXPLAIN` string for a logical plan, for Spark Connect
    /// `AnalyzePlan(Explain)` (PySpark `df.explain()`). `extended` mirrors Spark's EXTENDED mode:
    /// it prepends the parsed + optimized logical plans; otherwise only the physical plan is shown
    /// (Spark's SIMPLE mode). Running the optimizer here also exercises the same passes (predicate
    /// / projection pushdown) the execution path applies, so the output reflects what will run.
    pub async fn explain(
        &self,
        plan: &datafusion::logical_expr::LogicalPlan,
        extended: bool,
    ) -> Result<String> {
        use std::fmt::Write as _;
        let mut out = String::new();
        if extended {
            let _ = write!(
                out,
                "== Parsed Logical Plan ==\n{}\n",
                plan.display_indent()
            );
        }
        let optimized = self
            .ctx
            .state()
            .optimize(plan)
            .map_err(|e| Error::Plan(e.to_string()))?;
        if extended {
            let _ = write!(
                out,
                "== Optimized Logical Plan ==\n{}\n",
                optimized.display_indent()
            );
        }
        let physical = self
            .ctx
            .state()
            .create_physical_plan(&optimized)
            .await
            .map_err(|e| Error::Execution(e.to_string()))?;
        let _ = write!(
            out,
            "== Physical Plan ==\n{}",
            datafusion::physical_plan::displayable(physical.as_ref()).indent(false)
        );
        Ok(out)
    }

    /// Execute a DataFusion logical plan to record batches — the seam the Spark Connect relation
    /// translator uses to run lowered `DataFrame` plans.
    pub async fn execute_logical_plan(
        &self,
        plan: datafusion::logical_expr::LogicalPlan,
    ) -> Result<Vec<RecordBatch>> {
        self.ctx
            .execute_logical_plan(plan)
            .await
            .map_err(|e| Error::Plan(e.to_string()))?
            .collect()
            .await
            .map_err(|e| Error::Execution(e.to_string()))
    }

    /// Execute an already-built physical plan to record batches (the worker side of a stage).
    pub async fn execute_plan(
        &self,
        plan: std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    ) -> Result<Vec<RecordBatch>> {
        datafusion::physical_plan::collect(plan, self.ctx.task_ctx())
            .await
            .map_err(|e| Error::Execution(e.to_string()))
    }

    /// Register an in-memory table of `batches` under `name` — the worker-side landing zone
    /// for shuffle input, so a downstream stage can read it as an ordinary table. Idempotent: any
    /// existing table of the same name is replaced (a worker reuses its engine across queries, so
    /// `shuffle_input` is re-registered each time).
    pub fn register_batches(&self, name: &str, batches: Vec<RecordBatch>) -> Result<()> {
        use datafusion::datasource::MemTable;
        use std::sync::Arc;

        let schema = match batches.first() {
            Some(b) => b.schema(),
            None => return Err(Error::Plan(format!("register `{name}`: no batches"))),
        };
        let table = MemTable::try_new(schema, vec![batches])
            .map_err(|e| Error::Execution(format!("mem table `{name}`: {e}")))?;
        // Drop any prior registration so re-registering the same name doesn't error.
        let _ = self.ctx.deregister_table(name);
        self.ctx
            .register_table(name, Arc::new(table))
            .map_err(|e| Error::Execution(format!("register `{name}`: {e}")))?;
        Ok(())
    }

    /// Snapshot of the session state, for building a `FunctionRegistry`/codec when
    /// deserializing physical-plan fragments shipped from the driver.
    pub fn session_state(&self) -> datafusion::execution::context::SessionState {
        self.ctx.state()
    }

    /// Register a Parquet file or directory under `name` (a thin wrapper over DataFusion's
    /// reader, so callers needn't depend on DataFusion's option types).
    pub async fn register_parquet(&self, name: &str, path: &str) -> Result<()> {
        use datafusion::prelude::ParquetReadOptions;
        self.ctx
            .register_parquet(name, path, ParquetReadOptions::default())
            .await
            .map_err(|e| Error::Execution(format!("register parquet `{name}`: {e}")))
    }

    /// Register a Delta Lake table directory under `name` — resolves active files from the
    /// `_delta_log` (via [`weft_datasource::delta_active_files`]), then the native reader.
    pub async fn register_delta(&self, name: &str, table_path: &str) -> Result<()> {
        let files = weft_datasource::delta_active_files(table_path)?;
        self.register_parquet_files(name, table_path, files).await
    }

    /// Register an Iceberg table directory under `name` — resolves data files from the current
    /// snapshot's manifests (via [`weft_datasource::iceberg_active_files`]), then the reader.
    pub async fn register_iceberg(&self, name: &str, table_path: &str) -> Result<()> {
        let files = weft_datasource::iceberg_active_files(table_path)?;
        self.register_parquet_files(name, table_path, files).await
    }

    /// Expose a set of Parquet files as a DataFusion listing table — the version-safe seam both
    /// lakehouse readers share (resolve the format to files, then use DataFusion 54's reader).
    async fn register_parquet_files(
        &self,
        name: &str,
        table_path: &str,
        files: Vec<std::path::PathBuf>,
    ) -> Result<()> {
        use datafusion::datasource::file_format::parquet::ParquetFormat;
        use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};

        if files.is_empty() {
            return Err(Error::Plan(format!(
                "table `{table_path}` has no active data files"
            )));
        }
        let urls = files
            .iter()
            .map(|p| {
                ListingTableUrl::parse(p.to_string_lossy())
                    .map_err(|e| Error::Io(format!("bad file path {}: {e}", p.display())))
            })
            .collect::<Result<Vec<_>>>()?;
        let opts = ListingOptions::new(Arc::new(ParquetFormat::default()));
        let table = build_listing_table(&self.ctx.state(), urls, opts, None).await?;
        self.ctx
            .register_table(name, table)
            .map_err(|e| Error::Execution(format!("register `{name}`: {e}")))?;
        Ok(())
    }

    /// Register an external catalog under `name`, bridging it into DataFusion's catalog API so
    /// `SELECT … FROM {name}.namespace.table` (and `spark.read.table("{name}.ns.t")`) resolve
    /// **lazily** — the catalog is hit only when a query first references one of its tables.
    pub fn register_catalog(&self, name: &str, provider: Arc<dyn weft_catalog::CatalogProvider>) {
        // Keep the raw weft provider so the engine can answer catalog-listing SQL (`SHOW DATABASES`,
        // `SHOW TABLES IN …`) authoritatively — the DataFusion bridge below only surfaces a
        // best-effort, already-materialized snapshot.
        self.weft_catalogs
            .lock()
            .expect("weft_catalogs poisoned")
            .insert(name.to_string(), provider.clone());
        let bridge = Arc::new(catalog_bridge::WeftCatalogProvider::new(
            provider,
            self.ctx.clone(),
        ));
        self.ctx.register_catalog(name, bridge);
    }

    /// Serve a parsed catalog-listing statement directly from the registered weft catalogs.
    ///
    /// The output column names are load-bearing — a downstream gateway parser keys off them:
    /// - `SHOW DATABASES`/`SHOW SCHEMAS`[ `IN <cat>`] → one `namespace` (Utf8) column;
    /// - `SHOW TABLES IN <cat>[.<db>]` → three columns `namespace` (Utf8), `tableName` (Utf8),
    ///   `isTemporary` (Boolean, always false), matching Spark.
    ///
    /// An unknown catalog yields an empty (0-row) result of the right shape rather than an error.
    async fn run_show(&self, show: &ShowStmt) -> Result<Vec<RecordBatch>> {
        match show {
            ShowStmt::Databases { catalog: None } => {
                // Union of every registered catalog's top-level namespaces.
                let cats: Vec<Arc<dyn weft_catalog::CatalogProvider>> = self
                    .weft_catalogs
                    .lock()
                    .expect("weft_catalogs poisoned")
                    .values()
                    .cloned()
                    .collect();
                let mut namespaces = Vec::new();
                for cat in cats {
                    let nss = cat
                        .list_namespaces(&[])
                        .await
                        .map_err(|e| Error::Execution(e.to_string()))?;
                    for ns in nss {
                        namespaces.push(ns.join("."));
                    }
                }
                Ok(vec![namespace_batch(namespaces)?])
            }
            ShowStmt::Databases { catalog: Some(cat) } => {
                let provider = self.weft_catalog(cat);
                let namespaces = match provider {
                    Some(p) => p
                        .list_namespaces(&[])
                        .await
                        .map_err(|e| Error::Execution(e.to_string()))?
                        .into_iter()
                        .map(|ns| ns.join("."))
                        .collect(),
                    // Unknown catalog → empty result, not an error.
                    None => Vec::new(),
                };
                Ok(vec![namespace_batch(namespaces)?])
            }
            ShowStmt::Tables { catalog, database } => {
                let provider = self.weft_catalog(catalog);
                let mut rows: Vec<(String, String)> = Vec::new();
                if let Some(p) = provider {
                    match database {
                        // `SHOW TABLES IN <cat>.<db>` — tables directly in that namespace.
                        Some(db) => {
                            let tables = p
                                .list_tables(std::slice::from_ref(db))
                                .await
                                .map_err(|e| Error::Execution(e.to_string()))?;
                            for t in tables {
                                rows.push((db.clone(), t));
                            }
                        }
                        // `SHOW TABLES IN <cat>` — union across the catalog's top-level namespaces.
                        None => {
                            let nss = p
                                .list_namespaces(&[])
                                .await
                                .map_err(|e| Error::Execution(e.to_string()))?;
                            for ns in nss {
                                let tables = p
                                    .list_tables(&ns)
                                    .await
                                    .map_err(|e| Error::Execution(e.to_string()))?;
                                let ns_label = ns.join(".");
                                for t in tables {
                                    rows.push((ns_label.clone(), t));
                                }
                            }
                        }
                    }
                }
                Ok(vec![tables_batch(rows)?])
            }
        }
    }

    /// Look up a registered weft catalog by name (case-sensitive, as registered).
    fn weft_catalog(&self, name: &str) -> Option<Arc<dyn weft_catalog::CatalogProvider>> {
        self.weft_catalogs
            .lock()
            .expect("weft_catalogs poisoned")
            .get(name)
            .cloned()
    }

    /// Access the underlying DataFusion context (e.g. to register tables/Parquet).
    pub fn ctx(&self) -> &SessionContext {
        self.ctx.as_ref()
    }

    /// Schema (database) names in the built-in in-process catalog — backs `listDatabases` for the
    /// default `spark_catalog` (the catalog holding temp views and ad-hoc registered tables).
    pub fn builtin_namespaces(&self) -> Vec<String> {
        let default = self.default_catalog_name();
        match self.ctx.catalog(&default) {
            Some(cat) => cat.schema_names(),
            None => Vec::new(),
        }
    }

    /// Table names in `schema` of the built-in catalog — backs `listTables` for `spark_catalog`.
    pub fn builtin_table_names(&self, schema: &str) -> Vec<String> {
        let default = self.default_catalog_name();
        self.ctx
            .catalog(&default)
            .and_then(|c| c.schema(schema))
            .map(|s| s.table_names())
            .unwrap_or_default()
    }

    fn default_catalog_name(&self) -> String {
        self.ctx
            .state()
            .config()
            .options()
            .catalog
            .default_catalog
            .clone()
    }
}

/// A parsed catalog-listing statement (see [`parse_show`]).
#[derive(Debug, PartialEq, Eq)]
enum ShowStmt {
    /// `SHOW DATABASES`/`SHOW SCHEMAS`, optionally `IN <catalog>`.
    Databases { catalog: Option<String> },
    /// `SHOW TABLES IN <catalog>[.<database>]`.
    Tables {
        catalog: String,
        database: Option<String>,
    },
}

/// Recognize the catalog-listing `SHOW` statements weft answers itself, returning `None` for
/// anything else (so it flows through normal planning untouched). Tolerant by design: keywords are
/// case-insensitive, identifiers may be backtick-quoted or bare, a trailing `;` and extra
/// whitespace are ignored. Deliberately does NOT match bare `SHOW TABLES` (no `IN`) — that has no
/// unambiguous cross-catalog meaning and is left to the normal path.
fn parse_show(query: &str) -> Option<ShowStmt> {
    let trimmed = query.trim().trim_end_matches(';').trim();
    let mut words = trimmed.split_whitespace();
    if !words.next()?.eq_ignore_ascii_case("show") {
        return None;
    }
    let kind = words.next()?;
    let rest: Vec<&str> = words.collect();
    if kind.eq_ignore_ascii_case("databases") || kind.eq_ignore_ascii_case("schemas") {
        match rest.as_slice() {
            [] => Some(ShowStmt::Databases { catalog: None }),
            [in_kw, name] if in_kw.eq_ignore_ascii_case("in") => {
                // `SHOW DATABASES IN <cat>` — take the first segment as the catalog name.
                let segs = parse_qualified_name(name);
                segs.into_iter().next().map(|catalog| ShowStmt::Databases {
                    catalog: Some(catalog),
                })
            }
            _ => None,
        }
    } else if kind.eq_ignore_ascii_case("tables") {
        match rest.as_slice() {
            [in_kw, name] if in_kw.eq_ignore_ascii_case("in") => {
                let mut segs = parse_qualified_name(name).into_iter();
                let catalog = segs.next()?;
                let database = segs.next();
                Some(ShowStmt::Tables { catalog, database })
            }
            _ => None,
        }
    } else {
        None
    }
}

/// Split a (possibly backtick-quoted) dotted identifier like `glue.clickbench` or
/// `` `glue`.`my db` `` into its segments, stripping the backtick quoting.
fn parse_qualified_name(name: &str) -> Vec<String> {
    let mut segments = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    for ch in name.chars() {
        match ch {
            '`' => in_quote = !in_quote,
            '.' if !in_quote => {
                segments.push(std::mem::take(&mut current));
            }
            _ => current.push(ch),
        }
    }
    segments.push(current);
    segments.into_iter().filter(|s| !s.is_empty()).collect()
}

/// Single-column `namespace` (Utf8) batch for the `SHOW DATABASES`/`SHOW SCHEMAS` forms.
fn namespace_batch(namespaces: Vec<String>) -> Result<RecordBatch> {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![Field::new(
        "namespace",
        DataType::Utf8,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(namespaces))])
        .map_err(|e| Error::Execution(e.to_string()))
}

/// Three-column `namespace`/`tableName`/`isTemporary` batch matching Spark's `SHOW TABLES`.
fn tables_batch(rows: Vec<(String, String)>) -> Result<RecordBatch> {
    use arrow::array::{BooleanArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![
        Field::new("namespace", DataType::Utf8, false),
        Field::new("tableName", DataType::Utf8, false),
        Field::new("isTemporary", DataType::Boolean, false),
    ]));
    let namespaces: Vec<String> = rows.iter().map(|(ns, _)| ns.clone()).collect();
    let names: Vec<String> = rows.iter().map(|(_, t)| t.clone()).collect();
    let temp: Vec<bool> = vec![false; rows.len()];
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(namespaces)),
            Arc::new(StringArray::from(names)),
            Arc::new(BooleanArray::from(temp)),
        ],
    )
    .map_err(|e| Error::Execution(e.to_string()))
}

/// Build a DataFusion [`ListingTable`] over `urls` — the one place the Parquet/Delta/Iceberg
/// readers and the catalog bridge converge. Infers the schema from the data files unless `schema`
/// is supplied (a catalog that already knows the schema passes it, avoiding a metadata read and
/// handling empty tables). Returned as a `TableProvider` so callers can register it or hand it to
/// the bridge.
pub(crate) async fn build_listing_table(
    state: &datafusion::execution::context::SessionState,
    urls: Vec<datafusion::datasource::listing::ListingTableUrl>,
    options: datafusion::datasource::listing::ListingOptions,
    schema: Option<arrow::datatypes::SchemaRef>,
) -> Result<Arc<dyn datafusion::datasource::TableProvider>> {
    use datafusion::datasource::listing::{ListingTable, ListingTableConfig};

    let config = ListingTableConfig::new_with_multi_paths(urls).with_listing_options(options);
    let config = match schema {
        // Declared-schema path: read files *against* the catalog schema. Install a
        // case-insensitive physical-expression adapter so a lowercase catalog column (Glue's
        // `vendorid`) binds to a mixed-case file column (`VendorID`) — then DataFusion's default
        // adapter casts types as usual. Inference path (below) is left untouched.
        Some(s) => config
            .with_schema(s)
            .with_expr_adapter_factory(Arc::new(schema_adapt::CaseInsensitiveExprAdapterFactory)),
        None => config
            .infer_schema(state)
            .await
            .map_err(|e| Error::Execution(format!("infer schema: {e}")))?,
    };
    let table = ListingTable::try_new(config)
        .map_err(|e| Error::Execution(format!("listing table: {e}")))?;
    Ok(Arc::new(table))
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Engine {
    /// Tear down this engine's managed warehouse directory (the `CREATE TABLE … USING <fmt>`
    /// format-backed storage). Best-effort: a leftover temp dir is harmless, so failures are
    /// ignored.
    fn drop(&mut self) {
        if self.warehouse.exists() {
            let _ = std::fs::remove_dir_all(&self.warehouse);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn select_one() {
        let engine = Engine::new();
        let batches = engine.sql("SELECT 1 AS x").await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        assert_eq!(batches[0].num_columns(), 1);
    }

    /// `CREATE TABLE … USING <fmt>` must lower to real, format-backed storage that round-trips
    /// data (incl. NULLs) byte-faithfully, and INSERT must render as Spark's empty `struct<>`.
    async fn roundtrip_fmt(fmt: &str) {
        use arrow::array::Array;
        let engine = Engine::new();
        // CREATE returns an empty result set (Spark `struct<>`).
        let c = engine
            .sql(&format!("create table rt(a int, b string) using {fmt}"))
            .await
            .unwrap();
        assert!(c.is_empty(), "CREATE should yield no batches ({fmt})");
        // INSERT returns empty (Spark drops DataFusion's count row).
        let i = engine
            .sql("insert into rt values (1, 'x'), (2, null), (3, 'z')")
            .await
            .unwrap();
        assert!(i.is_empty(), "INSERT should yield no batches ({fmt})");
        // SELECT reads the data back, NULLs preserved.
        let out = engine.sql("select a, b from rt order by a").await.unwrap();
        let rows: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 3, "round-trip row count ({fmt})");
        let batch = out.first().expect("a batch");
        let b_col = batch.column(1);
        // Row order is guaranteed by ORDER BY a; row 2 (b) must read back as NULL, not "" — the
        // CSV NULL-vs-empty-string faithfulness trap. (Type-agnostic: Utf8 vs Utf8View vary.)
        assert!(!b_col.is_null(0), "row 0 b must be non-null ({fmt})");
        assert!(
            b_col.is_null(1),
            "NULL string must survive {fmt} round-trip (not become \"\")"
        );
        assert!(!b_col.is_null(2), "row 2 b must be non-null ({fmt})");
    }

    #[tokio::test]
    async fn create_table_using_parquet_roundtrips_with_nulls() {
        roundtrip_fmt("parquet").await;
    }

    #[tokio::test]
    async fn create_table_using_json_roundtrips_with_nulls() {
        roundtrip_fmt("json").await;
    }

    #[tokio::test]
    async fn create_table_using_csv_roundtrips_with_nulls() {
        roundtrip_fmt("csv").await;
    }

    #[tokio::test]
    async fn select_arithmetic() {
        let engine = Engine::new();
        let batches = engine.sql("SELECT 40 + 2 AS answer").await.unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn normalize_strips_temporary_view() {
        // The four Spark spellings collapse to plain CREATE [OR REPLACE] VIEW, body untouched.
        assert_eq!(
            normalize_spark_sql("CREATE TEMPORARY VIEW t AS SELECT 1 a"),
            "CREATE VIEW t AS SELECT 1 a"
        );
        assert_eq!(
            normalize_spark_sql("CREATE OR REPLACE TEMPORARY VIEW t AS SELECT 1 a"),
            "CREATE OR REPLACE VIEW t AS SELECT 1 a"
        );
        assert_eq!(
            normalize_spark_sql("create global temporary view t as select 1"),
            "CREATE VIEW t as select 1"
        );
        // `TEMP` is Spark's accepted abbreviation for `TEMPORARY`.
        assert_eq!(
            normalize_spark_sql("CREATE TEMP VIEW df AS SELECT 1"),
            "CREATE VIEW df AS SELECT 1"
        );
        assert_eq!(
            normalize_spark_sql("CREATE GLOBAL TEMP VIEW v(a,b) AS VALUES (1,2)"),
            "CREATE VIEW v(a,b) AS VALUES (1,2)"
        );
        // Case-insensitive keywords, leading whitespace preserved.
        assert_eq!(
            normalize_spark_sql("  Create Temporary View v As Select 2"),
            "  CREATE VIEW v As Select 2"
        );
    }

    #[test]
    fn normalize_leaves_other_statements_untouched() {
        for q in [
            "SELECT * FROM t",
            "CREATE VIEW v AS SELECT 1",
            "CREATE TABLE t(a INT)",
            "CREATE TEMPORARY FUNCTION f AS 'x'",
            "INSERT INTO t VALUES (1)",
        ] {
            assert_eq!(normalize_spark_sql(q), q, "should not rewrite: {q}");
        }
    }

    #[test]
    fn normalize_rewrites_typed_literals() {
        // Each Spark suffix maps to the matching CAST.
        assert_eq!(
            normalize_spark_sql("SELECT 1Y, 2S, 3L, 4F, 5D"),
            "SELECT CAST(1 AS TINYINT), CAST(2 AS SMALLINT), CAST(3 AS BIGINT), \
             CAST(4 AS FLOAT), CAST(5 AS DOUBLE)"
        );
        // Fractions and exponents are part of the number; case-insensitive suffix.
        assert_eq!(
            normalize_spark_sql("VALUES (1.0d), (2.5e3D)"),
            "VALUES (CAST(1.0 AS DOUBLE)), (CAST(2.5e3 AS DOUBLE))"
        );
        // BD → DECIMAL with BigDecimal precision/scale.
        assert_eq!(
            normalize_spark_sql("SELECT 1.0BD, 0.1BD, 123BD, 0.001BD"),
            "SELECT CAST(1.0 AS DECIMAL(2,1)), CAST(0.1 AS DECIMAL(1,1)), \
             CAST(123 AS DECIMAL(3,0)), CAST(0.001 AS DECIMAL(3,3))"
        );
        // Protected contexts: string literals ('…' and Databricks "…"), backtick identifiers,
        // comments, ordinary identifiers, hex, and plain numbers are all left untouched.
        for q in [
            "SELECT '1L' AS s",
            "SELECT \"2Y\" AS s",
            "SELECT `3S` FROM t",
            "SELECT 1 -- a 4L comment\n",
            "SELECT /* 5D */ 1",
            "SELECT col1, a2d, x1L FROM t",
            "SELECT 0x1F, 1e5, 3.14, 42",
        ] {
            assert_eq!(normalize_spark_sql(q), q, "should not rewrite: {q}");
        }
    }

    #[test]
    fn normalize_unescapes_spark_string_literals() {
        // `\\` -> `\` (ilike block 9): the LIKE escape survives, so `\_` still means literal `_`.
        assert_eq!(normalize_spark_sql(r"select 'a\\__b'"), r"select 'a\__b'");
        // `\n` -> a real newline (ilike block 12): the 4-char literal becomes Spark's 3-char value.
        assert_eq!(normalize_spark_sql(r"select 'a\nb'"), "select 'a\nb'");
        // Octal `\ooo` -> char (literals.sql Hello!). (`\uXXXX` is covered by the golden harness.)
        assert_eq!(
            normalize_spark_sql(r"select '\110\145\154\154\157\041'"),
            "select 'Hello!'"
        );
        // `\%` / `\_` keep the backslash so downstream LIKE escaping still works (literals.sql).
        assert_eq!(
            normalize_spark_sql(r"select 'no-pattern\%'"),
            r"select 'no-pattern\%'"
        );
        assert_eq!(
            normalize_spark_sql(r"select 'pattern\\\%'"),
            r"select 'pattern\\%'"
        );
        // `\'` (Spark's escaped quote) is re-emitted as `''` so the value survives the dialect switch.
        assert_eq!(normalize_spark_sql(r"select 'a\'b'"), "select 'a''b'");
        // Regex literal: `'\\d+'` reaches the planner as `\d+`, exactly what Spark hands its engine.
        assert_eq!(normalize_spark_sql(r"select '\\d+'"), r"select '\d+'");
    }

    #[test]
    fn normalize_leaves_backslash_free_and_protected_literals_untouched() {
        for q in [
            "SELECT 'a' ILIKE 'b'",     // no backslash anywhere → byte-identical, borrowed
            "SELECT 'it''s fine'",      // `''` quote-doubling preserved verbatim
            "SELECT \"a\\nb\" AS s",    // Databricks `"…"` literal left to the parser
            "SELECT 1 -- a\\nb keep\n", // backslash inside a comment is not a literal
            "SELECT `c\\d` FROM t",     // backtick identifier untouched
        ] {
            assert_eq!(normalize_spark_sql(q), q, "should not rewrite: {q}");
        }
    }

    #[tokio::test]
    async fn typed_literals_plan_and_eval() {
        let engine = Engine::new();
        // bigint literal resolves and computes (would otherwise be `No field named "3l"`).
        let b = engine.sql("SELECT 3L + 4L AS x").await.unwrap();
        let got = crate::arrow::util::pretty::pretty_format_batches(&b)
            .unwrap()
            .to_string();
        assert!(got.contains("7"), "got: {got}");
        // decimal literal keeps scale.
        let b = engine.sql("SELECT 1.0BD AS x").await.unwrap();
        let got = crate::arrow::util::pretty::pretty_format_batches(&b)
            .unwrap()
            .to_string();
        assert!(got.contains("1.0"), "got: {got}");
    }

    #[tokio::test]
    async fn spark_function_aliases_resolve() {
        let engine = Engine::new();
        // Scalar aliases delegate to the DataFusion builtin with identical semantics.
        for (q, want) in [
            ("SELECT startswith('hello', 'he') AS x", "true"),
            ("SELECT endswith('hello', 'lo') AS x", "true"),
            ("SELECT len('hello') AS x", "5"),
            ("SELECT ucase('abc') AS x", "ABC"),
            ("SELECT lcase('ABC') AS x", "abc"),
            ("SELECT sign(-3) AS x", "-1"),
        ] {
            let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
            let got = crate::arrow::util::pretty::pretty_format_batches(&batches)
                .unwrap()
                .to_string();
            assert!(got.contains(want), "{q} -> expected {want}, got:\n{got}");
        }
        // Aggregate aliases too.
        for q in [
            "SELECT variance(c) FROM (VALUES (1.0),(2.0),(3.0)) AS t(c)",
            "SELECT any(c) FROM (VALUES (true),(false)) AS t(c)",
            "SELECT every(c) FROM (VALUES (true),(false)) AS t(c)",
            "SELECT approx_count_distinct(c) FROM (VALUES (1),(2),(2)) AS t(c)",
        ] {
            engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        }
    }

    /// Collect column 0 of `batches` as a sorted `Vec<String>` (NULLs dropped). Used by the
    /// LIKE-quantifier test below, whose queries all return a single `company` string column.
    fn col0_strings(batches: &[RecordBatch]) -> Vec<String> {
        use arrow::array::{Array, StringArray, StringViewArray};
        let mut out = Vec::new();
        for b in batches {
            let c = b.column(0);
            if let Some(a) = c.as_any().downcast_ref::<StringArray>() {
                for i in 0..a.len() {
                    if a.is_valid(i) {
                        out.push(a.value(i).to_string());
                    }
                }
            } else if let Some(a) = c.as_any().downcast_ref::<StringViewArray>() {
                for i in 0..a.len() {
                    if a.is_valid(i) {
                        out.push(a.value(i).to_string());
                    }
                }
            } else {
                panic!("col0 is not a string array: {:?}", c.data_type());
            }
        }
        out.sort();
        out
    }

    #[test]
    fn like_quantifier_gate_matches_only_the_quantified_forms() {
        assert!(contains_like_quantifier("a LIKE ALL ('x')"));
        assert!(contains_like_quantifier("a ILIKE ANY ('x')"));
        assert!(contains_like_quantifier("a like\n  some ('x')"));
        // Ordinary LIKE / unrelated SQL must NOT take the rewrite path.
        assert!(!contains_like_quantifier("a LIKE '%oo%'"));
        assert!(!contains_like_quantifier("SELECT * FROM small_table"));
    }

    #[tokio::test]
    async fn like_all_any_quantifiers_lower_faithfully() {
        // Mirrors Spark's like-all.sql / like-any.sql corpus, including the three-valued-logic
        // NULL rows — the lowering must reproduce `LikeAll`/`LikeAny` semantics exactly.
        let engine = Engine::new();
        engine
            .sql(
                "CREATE OR REPLACE TEMPORARY VIEW lt AS SELECT * FROM (VALUES \
                 ('google','%oo%'),('facebook','%oo%'),('linkedin','%in')) AS t1(company, pat)",
            )
            .await
            .expect("view");

        async fn companies(engine: &Engine, q: &str) -> Vec<String> {
            let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
            col0_strings(&batches)
        }

        // LIKE ALL = AND fold; LIKE ANY = OR fold.
        assert_eq!(
            companies(
                &engine,
                "SELECT company FROM lt WHERE company LIKE ALL ('%oo%', '%go%')"
            )
            .await,
            vec!["google"]
        );
        assert_eq!(
            companies(
                &engine,
                "SELECT company FROM lt WHERE company LIKE ANY ('%oo%', '%in', 'fa%')"
            )
            .await,
            vec!["facebook", "google", "linkedin"]
        );
        // A column-valued pattern in the list evaluates per row.
        assert_eq!(
            companies(
                &engine,
                "SELECT company FROM lt WHERE company LIKE ALL ('%oo%', pat)"
            )
            .await,
            vec!["facebook", "google"]
        );
        // 3VL: a NULL pattern makes ALL never-true → empty.
        assert!(companies(
            &engine,
            "SELECT company FROM lt WHERE company LIKE ALL ('%oo%', NULL)"
        )
        .await
        .is_empty());
        // 3VL: ANY is satisfied by a matching pattern; non-matchers become NULL (not false).
        assert_eq!(
            companies(
                &engine,
                "SELECT company FROM lt WHERE company LIKE ANY ('%oo%', NULL)"
            )
            .await,
            vec!["facebook", "google"]
        );
        // NOT LIKE ANY distributes NOT onto each pattern, keeps the OR connective.
        assert_eq!(
            companies(
                &engine,
                "SELECT company FROM lt WHERE company NOT LIKE ANY ('%oo%', NULL)"
            )
            .await,
            vec!["linkedin"]
        );
        // An outer NOT over a LIKE ALL is the boolean negation of the whole AND fold.
        assert_eq!(
            companies(
                &engine,
                "SELECT company FROM lt WHERE NOT company LIKE ALL ('%oo%', 'fa%')"
            )
            .await,
            vec!["google", "linkedin"]
        );
        // ILIKE ALL is case-insensitive.
        assert_eq!(
            companies(
                &engine,
                "SELECT company FROM lt WHERE company ILIKE ALL ('%OO%', '%GO%')"
            )
            .await,
            vec!["google"]
        );
        // An ordinary LIKE is left untouched by the rewrite.
        assert_eq!(
            companies(&engine, "SELECT company FROM lt WHERE company LIKE '%oo%'").await,
            vec!["facebook", "google"]
        );
    }

    #[tokio::test]
    async fn temporary_view_then_query_roundtrips() {
        // The whole point: a Spark-style temp view registers and is queryable afterwards.
        let engine = Engine::new();
        engine
            .sql("CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES (1,2),(3,4) AS t(a,b)")
            .await
            .expect("temp view should register");
        let batches = engine
            .sql("SELECT COUNT(*) AS n FROM testData")
            .await
            .expect("query against temp view");
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }

    #[tokio::test]
    async fn physical_plan_round_trips_through_execute() {
        let engine = Engine::new();
        let plan = engine.physical_plan("SELECT 1 AS x").await.unwrap();
        let batches = engine.execute_plan(plan).await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }

    #[tokio::test]
    async fn register_batches_is_queryable() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![10, 20, 30]))])
                .unwrap();
        let engine = Engine::new();
        engine.register_batches("t", vec![batch]).unwrap();
        let out = engine.sql("SELECT SUM(v) AS s FROM t").await.unwrap();
        let s = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(s, 60);
    }

    #[tokio::test]
    async fn reads_a_delta_table() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        // Build a minimal Delta table: one Parquet data file + a single JSON commit that
        // `add`s it.
        let dir = std::env::temp_dir().join(format!("weft-delta-{}", std::process::id()));
        let log = dir.join("_delta_log");
        std::fs::create_dir_all(&log).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .unwrap();
        {
            let f = std::fs::File::create(dir.join("part-0.parquet")).unwrap();
            let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
        let commit = concat!(
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            "\n",
            r#"{"metaData":{"id":"t","format":{"provider":"parquet"},"schemaString":"{}","partitionColumns":[]}}"#,
            "\n",
            r#"{"add":{"path":"part-0.parquet","partitionValues":{},"size":1,"modificationTime":0,"dataChange":true}}"#,
            "\n",
        );
        std::fs::write(log.join("00000000000000000000.json"), commit).unwrap();

        let engine = Engine::new();
        engine
            .register_delta("t", dir.to_str().unwrap())
            .await
            .unwrap();
        let batches = engine
            .sql("SELECT COUNT(*) AS c, SUM(x) AS s FROM t")
            .await
            .unwrap();
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let s = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!((c, s), (4, 10));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
