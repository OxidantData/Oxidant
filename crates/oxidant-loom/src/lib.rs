//! `oxidant-loom` — the vectorized CPU engine and Oxidant's workhorse.
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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::prelude::SessionContext;
use oxidant_common::{Error, Result};

pub mod catalog_bridge;
/// Applies a catalog authorizer's column/row access decision to a table's scans.
pub mod lakeformation_provider;
/// Reads governed tables with Lake Formation's vended, per-table credentials.
pub mod lakeformation_store;
pub mod schema_conform;
pub mod shard;

/// Shuffle-input scans carrying driver-measured row counts (runtime join-strategy
/// conversion input). See [`measured_scan::MeasuredStatsTable`].
pub mod measured_scan;

/// Process-global cache of decoded replicated (dimension) table scans — a replicated table is
/// read + decoded once per worker per data version, then served from memory. See [`dim_cache`].
pub mod dim_cache;

/// SPIKE (issue #118): `s3://` ONNX model URIs resolved through the engine's object store.
pub mod ml_blob_source;

/// Disk-caching object_store wrapper for remote analytical reads (`OXIDANT_S3_CACHE_DIR`).
pub mod s3_cache;

/// S3 / object-store scan I/O counters + range-read concurrency (KAN-153).
pub mod s3_io;
/// Worker-side stage plan cache (R5-4 / KAN-2): a distributed stage is planned once per
/// worker, not once per task. See [`stage_plan_cache`].
pub mod stage_plan_cache;

/// `sts:AssumeRole` credential provider for S3 access (Hadoop-AWS `fs.s3a.assumed.role.arn`
/// equivalent) — see [`assume_role_credentials::AssumeRoleCredentialProvider`].
mod assume_role_credentials;
mod default_credentials;

/// Case-insensitive file→table column matching for catalog-declared schemas (Glue/Hive parity).
mod schema_adapt;

/// Spark-only scalar functions (DataFusion `ScalarUDF`s) registered into every [`Engine`].
pub mod spark_functions;

/// Session UDF registry (`CREATE FUNCTION`, worker sync).
pub mod udf_registry;

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

/// Memory-pool progress timestamps for the worker no-progress stage watchdog (KAN-47).
pub mod progress_pool;

/// Re-export of the exact `arrow` DataFusion uses, so every crate in the workspace encodes
/// Arrow IPC against one version (no cross-crate `arrow` mismatch).
pub use datafusion::arrow;
// Re-exported so downstream crates (the CLI's pipeline planner) can use DataFusion's own SQL
// parser without taking a direct `datafusion` dependency and risking a second, incompatible
// copy of it in the tree.
pub use datafusion;

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

/// Total size in bytes of every regular file under `dir`, recursively. Best-effort —
/// unreadable entries count as 0 (spill files churn while the watchdog walks).
pub fn dir_bytes(dir: &std::path::Path) -> u64 {
    let mut total = 0;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            total += dir_bytes(&path);
        } else if let Ok(meta) = entry.metadata() {
            total += meta.len();
        }
    }
    total
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

/// Split a (possibly qualified, possibly backtick-quoted) object name on `.`, treating a
/// backtick-quoted span as a single segment (its contents, including a literal `.`, are never
/// treated as a separator). Used by [`Engine::name_targets_external_catalog`] to check a name's
/// arity (only a 3+ segment name — `catalog.db.table` or deeper — can be catalog-qualified).
fn split_name_segments(name: &str) -> Vec<&str> {
    let bytes = name.as_bytes();
    let mut segments = Vec::new();
    let mut i = 0;
    let mut seg_start = 0;
    while i < bytes.len() {
        if bytes[i] == b'`' {
            i += 1;
            while i < bytes.len() && bytes[i] != b'`' {
                i += 1;
            }
            if i < bytes.len() {
                i += 1; // closing backtick
            }
            continue;
        }
        if bytes[i] == b'.' {
            segments.push(&name[seg_start..i]);
            i += 1;
            seg_start = i;
            continue;
        }
        i += 1;
    }
    segments.push(&name[seg_start..]);
    segments
}

/// Adapt Spark-dialect SQL that DataFusion's planner rejects verbatim but supports once a
/// dialect-only keyword is dropped or a literal is re-encoded.
///
/// The first pass is the `oxidant-sql` Stage-1 string prefilter registry
/// ([`oxidant_sql::dialect::spark_pipeline`]) — home of the `CREATE [OR REPLACE] [GLOBAL]
/// TEMP[ORARY] VIEW …` → `CREATE [OR REPLACE] VIEW …` rewrite. New dialect fixes belong in that
/// staged pipeline, not in the passes below: this function is the shrinking remainder of the
/// engine-local prefilter, kept only for the rewrites that have not been migrated yet.
pub fn normalize_spark_sql(query: &str) -> std::borrow::Cow<'_, str> {
    // Passes run in order: (1) the oxidant-sql Stage-1 prefilter registry, (2) Spark single-quoted
    // string-literal unescaping, (3) the typed-literal rewrite over the result, (4) strip ANSI
    // INTERVAL leading-precision qualifiers (`day (3)`) that DataFusion rejects, (5) qualify
    // INTERVAL literals that carry their unit inside the string (`interval '30 days'`),
    // (6) rewrite Postgres/TPC-DS bare forms (`date + 30 days`) to qualified INTERVAL SQL.
    // Unescaping runs BEFORE the typed-literal pass for two reasons: the re-emitted literals use
    // `''` quote-doubling (which the typed-literal scanner understands) instead of Spark's `\'`,
    // and a numeric token freed by a mis-delimited `\'` can therefore never be mistaken for code
    // and wrapped in a CAST.
    // Production runs Stage 1 only via `rewrite_str`. When Stage 2 intercepts register, replace
    // this with `spark_pipeline().lower(...)` (or wire `lower()` into `Engine::sql` / `plan_spark`).
    let stripped = match oxidant_sql::dialect::spark_pipeline().rewrite_str(query) {
        std::borrow::Cow::Owned(rewritten) => Some(rewritten),
        std::borrow::Cow::Borrowed(_) => None,
    };
    let base = stripped.as_deref().unwrap_or(query);
    let unescaped = unescape_spark_string_literals(base);
    let base2 = unescaped.as_deref().unwrap_or(base);
    let typed = rewrite_spark_typed_literals(base2);
    let base3 = typed.as_deref().unwrap_or(base2);
    let interval = strip_interval_leading_precision(base3);
    let base4 = interval.as_deref().unwrap_or(base3);
    let qualified = qualify_interval_units(base4);
    let base5 = qualified.as_deref().unwrap_or(base4);
    let bare = rewrite_bare_pg_interval_literals(base5);
    match bare {
        Some(b) => std::borrow::Cow::Owned(b),
        None => match qualified {
            Some(q) => std::borrow::Cow::Owned(q),
            None => match interval {
                Some(i) => std::borrow::Cow::Owned(i),
                None => match typed {
                    Some(t) => std::borrow::Cow::Owned(t),
                    None => match unescaped {
                        Some(u) => std::borrow::Cow::Owned(u),
                        None => match stripped {
                            Some(s) => std::borrow::Cow::Owned(s),
                            None => std::borrow::Cow::Borrowed(query),
                        },
                    },
                },
            },
        },
    }
}

/// Rewrite Postgres/TPC-DS bare interval arithmetic (`date + 30 days`, `date - 14 days`)
/// into qualified `INTERVAL 'N' DAY` that DataFusion's Databricks dialect accepts.
///
/// Official `dsqgen` (oxidant dialect) emits this form for date windows (Q5/Q12/Q16/…).
/// Spark/Postgres accept it; sqlparser-under-Databricks rejects with
/// `Expected: ), found: days`. Already-qualified `INTERVAL …` forms and string literals
/// are left untouched. Returns `None` when nothing changed.
fn rewrite_bare_pg_interval_literals(sql: &str) -> Option<String> {
    let b = sql.as_bytes();
    let n = b.len();
    let mut out = String::with_capacity(n + 16);
    let mut i = 0;
    let mut changed = false;

    while i < n {
        if b[i] == b'\'' || b[i] == b'"' {
            let quote = b[i];
            let start = i;
            i += 1;
            while i < n {
                if b[i] == quote {
                    if quote == b'\'' && i + 1 < n && b[i + 1] == b'\'' {
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

        if (b[i] == b'+' || b[i] == b'-') && !is_ident_byte(b, i.wrapping_sub(1)) {
            if let Some(m) = match_bare_pg_interval(b, i) {
                // Skip when this is already `interval N days` / `INTERVAL 'N' days`.
                if !preceded_by_interval_keyword(b, i) {
                    let sign = if b[i] == b'-' { "-" } else { "+" };
                    let amount = &sql[m.amount_start..m.amount_end];
                    let unit = interval_unit_keyword(&sql[m.unit_start..m.unit_end])
                        .expect("match_bare_pg_interval only returns known units");
                    out.push_str(sign);
                    out.push_str(" INTERVAL '");
                    out.push_str(amount);
                    out.push_str("' ");
                    out.push_str(unit);
                    i = m.end;
                    changed = true;
                    continue;
                }
            }
        }

        out.push(b[i] as char);
        i += 1;
    }

    changed.then_some(out)
}

struct BarePgInterval {
    amount_start: usize,
    amount_end: usize,
    unit_start: usize,
    unit_end: usize,
    end: usize,
}

/// At `+/-`, match `\s*<digits>\s*<unit>\b` where unit is a known interval spelling.
fn match_bare_pg_interval(b: &[u8], sign_idx: usize) -> Option<BarePgInterval> {
    let n = b.len();
    let mut j = sign_idx + 1;
    while j < n && b[j].is_ascii_whitespace() {
        j += 1;
    }
    let amount_start = j;
    if j >= n || !b[j].is_ascii_digit() {
        return None;
    }
    while j < n && b[j].is_ascii_digit() {
        j += 1;
    }
    let amount_end = j;
    if amount_start == amount_end {
        return None;
    }
    // Require whitespace between amount and unit (`30days` is not a TPC-DS form).
    if j >= n || !b[j].is_ascii_whitespace() {
        return None;
    }
    while j < n && b[j].is_ascii_whitespace() {
        j += 1;
    }
    let unit_start = j;
    let unit_len = interval_unit_len(&b[j..])?;
    let unit_end = unit_start + unit_len;
    // Trailing identifier char would make this part of a larger token.
    if is_ident_byte(b, unit_end) {
        return None;
    }
    Some(BarePgInterval {
        amount_start,
        amount_end,
        unit_start,
        unit_end,
        end: unit_end,
    })
}

fn is_ident_byte(b: &[u8], i: usize) -> bool {
    b.get(i)
        .copied()
        .is_some_and(|c| c.is_ascii_alphanumeric() || c == b'_')
}

/// True when `interval` (case-insensitive) is the token immediately before `sign_idx`
/// (allowing only whitespace between).
fn preceded_by_interval_keyword(b: &[u8], sign_idx: usize) -> bool {
    let mut k = sign_idx;
    while k > 0 && b[k - 1].is_ascii_whitespace() {
        k -= 1;
    }
    const KW: &[u8] = b"interval";
    if k < KW.len() {
        return false;
    }
    let start = k - KW.len();
    if !b[start..k].eq_ignore_ascii_case(KW) {
        return false;
    }
    // Must be a token boundary before `interval`.
    start == 0 || !is_ident_byte(b, start - 1)
}

/// Strip ANSI SQL-92 interval *leading precision* qualifiers that TPC-H emits
/// (`interval '90' day (3)`) but DataFusion rejects (`Unsupported Interval Expression with
/// leading_precision`). Only touches `INTERVAL '<literal>' <unit> (N)` — leaves function calls
/// like `day(col)` alone. Returns `None` when nothing changed.
fn strip_interval_leading_precision(sql: &str) -> Option<String> {
    let b = sql.as_bytes();
    let n = b.len();
    let mut out = String::with_capacity(n);
    let mut i = 0;
    let mut changed = false;

    while i < n {
        // Copy quoted regions verbatim so string content is never rewritten.
        if b[i] == b'\'' || b[i] == b'"' {
            let quote = b[i];
            let start = i;
            i += 1;
            while i < n {
                if b[i] == quote {
                    if i + 1 < n && b[i + 1] == quote {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += utf8_len(b[i]).min(n - i);
            }
            out.push_str(&sql[start..i]);
            continue;
        }

        if let Some(end) = match_interval_with_precision(b, i) {
            // Emit INTERVAL … <unit> and skip the `(N)` precision.
            out.push_str(&sql[i..end.unit_end]);
            i = end.after_precision;
            changed = true;
            continue;
        }

        let len = utf8_len(b[i]).min(n - i);
        out.push_str(&sql[i..i + len]);
        i += len;
    }

    changed.then_some(out)
}

/// If `sql[i..]` starts with `INTERVAL '<lit>' <unit> (N)`, return the end of the unit token and
/// the index just past the closing `)`.
fn match_interval_with_precision(b: &[u8], i: usize) -> Option<IntervalPrecisionMatch> {
    if !interval_keyword_at(b, i) {
        return None;
    }
    let n = b.len();
    let mut j = i + 8; // len("interval")

    while j < n && b[j].is_ascii_whitespace() {
        j += 1;
    }
    if j >= n || b[j] != b'\'' {
        return None;
    }
    j += 1;
    while j < n {
        if b[j] == b'\'' {
            if j + 1 < n && b[j + 1] == b'\'' {
                j += 2;
                continue;
            }
            j += 1;
            break;
        }
        j += utf8_len(b[j]).min(n - j);
    }

    while j < n && b[j].is_ascii_whitespace() {
        j += 1;
    }
    let unit_len = interval_unit_len(&b[j..])?;
    let unit_end = j + unit_len;
    j = unit_end;

    while j < n && b[j].is_ascii_whitespace() {
        j += 1;
    }
    if j >= n || b[j] != b'(' {
        return None;
    }
    j += 1;
    while j < n && b[j].is_ascii_whitespace() {
        j += 1;
    }
    let dig_start = j;
    while j < n && b[j].is_ascii_digit() {
        j += 1;
    }
    if j == dig_start {
        return None;
    }
    while j < n && b[j].is_ascii_whitespace() {
        j += 1;
    }
    if j >= n || b[j] != b')' {
        return None;
    }
    Some(IntervalPrecisionMatch {
        unit_end,
        after_precision: j + 1,
    })
}

struct IntervalPrecisionMatch {
    unit_end: usize,
    after_precision: usize,
}

fn interval_keyword_at(b: &[u8], i: usize) -> bool {
    const KW: &[u8] = b"interval";
    if i + KW.len() > b.len() {
        return false;
    }
    if i > 0 {
        let prev = b[i - 1];
        if prev.is_ascii_alphanumeric() || prev == b'_' {
            return false;
        }
    }
    if !b[i..i + KW.len()].eq_ignore_ascii_case(KW) {
        return false;
    }
    let after = i + KW.len();
    if after < b.len() {
        let next = b[after];
        if next.is_ascii_alphanumeric() || next == b'_' {
            return false;
        }
    }
    true
}

fn interval_unit_len(b: &[u8]) -> Option<usize> {
    // Longer units first so `years` wins over `year`.
    const UNITS: &[&[u8]] = &[
        b"years", b"year", b"months", b"month", b"days", b"day", b"hours", b"hour", b"minutes",
        b"minute", b"seconds", b"second",
    ];
    for u in UNITS {
        if b.len() >= u.len() && b[..u.len()].eq_ignore_ascii_case(u) {
            let after = u.len();
            if after < b.len() {
                let next = b[after];
                if next.is_ascii_alphanumeric() || next == b'_' {
                    continue;
                }
            }
            return Some(u.len());
        }
    }
    None
}

/// Move the unit of a Spark `INTERVAL` literal out of the string and into the qualifier position
/// DataFusion's parser demands: `interval '30 days'` → `interval '30' DAY`.
///
/// Spark accepts the unit either inside the literal or as a following token; the Databricks
/// dialect oxidant plans on (`Dialect::require_interval_qualifier`) accepts only the latter and
/// otherwise fails with `INTERVAL requires a unit after the literal value` before execution
/// starts — TPC-DS Q12/Q20/Q98 (`+ interval '30 days'`) never reached the engine at SF100.
///
/// The same spelling also arrives from oxidant's OWN distributed stage SQL: DataFusion's
/// `Unparser` renders interval scalars in Postgres-verbose style
/// (`INTERVAL '0 YEARS 0 MONS 30 DAYS 0 HOURS 0 MINS 0.00 SECS'`), which each worker re-parses
/// under the same dialect. Zero terms are dropped and multi-unit content becomes a parenthesized
/// sum, so that form collapses to `(INTERVAL '30' DAY)`.
///
/// Faithful, not lossy: DataFusion re-joins value and qualifier into a single string and hands it
/// to the same Arrow interval parser that reads Spark's spelling, so both forms yield the same
/// `IntervalMonthDayNano`. Anything that is not a clean `<amount> <unit>` sequence is left
/// untouched so its original parse error survives. Returns `None` when nothing changed.
fn qualify_interval_units(sql: &str) -> Option<String> {
    let b = sql.as_bytes();
    let n = b.len();
    let mut out = String::with_capacity(n);
    let mut i = 0;
    let mut changed = false;

    while i < n {
        // Copy quoted regions verbatim so string content is never rewritten. An INTERVAL
        // keyword is only recognized at a code position, so `'interval ''30 days'''` is safe.
        if b[i] == b'\'' || b[i] == b'"' {
            let quote = b[i];
            let start = i;
            i += 1;
            while i < n {
                if b[i] == quote {
                    if i + 1 < n && b[i + 1] == quote {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += utf8_len(b[i]).min(n - i);
            }
            out.push_str(&sql[start..i]);
            continue;
        }

        if let Some(m) = match_unqualified_interval(b, i) {
            if let Some(terms) = interval_terms_to_sql(&sql[m.content_start..m.content_end]) {
                out.push_str(&terms);
                i = m.end;
                changed = true;
                continue;
            }
        }

        let len = utf8_len(b[i]).min(n - i);
        out.push_str(&sql[i..i + len]);
        i += len;
    }

    changed.then_some(out)
}

/// If `sql[i..]` starts with `INTERVAL '<content>'` **not** followed by a unit token, return the
/// content span and the index just past the closing quote. An already-qualified interval
/// (`interval '30' day`) returns `None` so it is copied through byte-for-byte.
fn match_unqualified_interval(b: &[u8], i: usize) -> Option<UnqualifiedInterval> {
    if !interval_keyword_at(b, i) {
        return None;
    }
    let n = b.len();
    let mut j = i + 8; // len("interval")

    while j < n && b[j].is_ascii_whitespace() {
        j += 1;
    }
    if j >= n || b[j] != b'\'' {
        return None;
    }
    j += 1;
    let content_start = j;
    let content_end;
    loop {
        if j >= n {
            // Unterminated literal — leave it alone so the original parse error is preserved.
            return None;
        }
        if b[j] == b'\'' {
            // A `''` escape never appears in an interval amount; bail rather than guess.
            if j + 1 < n && b[j + 1] == b'\'' {
                return None;
            }
            content_end = j;
            j += 1;
            break;
        }
        j += utf8_len(b[j]).min(n - j);
    }
    let end = j;

    while j < n && b[j].is_ascii_whitespace() {
        j += 1;
    }
    if temporal_unit_token_len(&b[j..]).is_some() {
        return None;
    }

    Some(UnqualifiedInterval {
        content_start,
        content_end,
        end,
    })
}

struct UnqualifiedInterval {
    content_start: usize,
    content_end: usize,
    end: usize,
}

/// Render the contents of an unqualified interval literal as qualified `INTERVAL` SQL, or `None`
/// when `content` is not a whitespace-separated sequence of `<amount> <unit>` pairs.
///
/// Zero-valued terms are dropped (Postgres-verbose output is mostly zeros); an all-zero interval
/// becomes `INTERVAL '0' SECOND`. Two or more surviving terms are summed inside parentheses so the
/// rewrite is safe in any expression position.
fn interval_terms_to_sql(content: &str) -> Option<String> {
    let tokens: Vec<&str> = content.split_whitespace().collect();
    if tokens.is_empty() || tokens.len() % 2 != 0 {
        return None;
    }
    let mut terms = Vec::with_capacity(tokens.len() / 2);
    for pair in tokens.chunks(2) {
        let amount = pair[0];
        if !is_interval_amount(amount) {
            return None;
        }
        let unit = interval_unit_keyword(pair[1])?;
        // A zero term contributes nothing; keeping them all would bloat every stage SQL string.
        if amount.parse::<f64>().is_ok_and(|v| v == 0.0) {
            continue;
        }
        terms.push(format!("INTERVAL '{amount}' {unit}"));
    }
    match terms.len() {
        0 => Some("INTERVAL '0' SECOND".to_string()),
        1 => Some(terms.pop().expect("one term")),
        _ => Some(format!("({})", terms.join(" + "))),
    }
}

/// Whether `s` is an interval amount: an optionally signed decimal number.
fn is_interval_amount(s: &str) -> bool {
    let digits = s.strip_prefix(['-', '+']).unwrap_or(s);
    if digits.is_empty() {
        return false;
    }
    let mut parts = digits.splitn(2, '.');
    let int_part = parts.next().unwrap_or("");
    let frac_part = parts.next();
    if !int_part.bytes().all(|c| c.is_ascii_digit()) {
        return false;
    }
    match frac_part {
        // `5.` and `.5` are both accepted, but at least one digit must be present overall.
        Some(frac) => {
            !(int_part.is_empty() && frac.is_empty()) && frac.bytes().all(|c| c.is_ascii_digit())
        }
        None => !int_part.is_empty(),
    }
}

/// Map an in-literal unit spelling (Spark's and Arrow's, including the abbreviations DataFusion's
/// Postgres-verbose unparser emits) onto the qualifier keyword sqlparser accepts.
fn interval_unit_keyword(unit: &str) -> Option<&'static str> {
    let u = unit.to_ascii_lowercase();
    Some(match u.as_str() {
        "y" | "yr" | "yrs" | "year" | "years" => "YEAR",
        "mon" | "mons" | "month" | "months" => "MONTH",
        "w" | "week" | "weeks" => "WEEK",
        "d" | "day" | "days" => "DAY",
        "h" | "hr" | "hrs" | "hour" | "hours" => "HOUR",
        "m" | "min" | "mins" | "minute" | "minutes" => "MINUTE",
        "s" | "sec" | "secs" | "second" | "seconds" => "SECOND",
        "ms" | "msec" | "msecs" | "millisecond" | "milliseconds" => "MILLISECOND",
        "us" | "usec" | "usecs" | "microsecond" | "microseconds" => "MICROSECOND",
        "nanosecond" | "nanoseconds" => "NANOSECOND",
        _ => return None,
    })
}

/// Length of a temporal-unit *token* at the start of `b`, matching sqlparser's
/// `next_token_is_temporal_unit` set. Used only to tell an already-qualified interval from one
/// whose unit is inside the literal — deliberately wider than [`interval_unit_len`] (which drives
/// the leading-precision pass) so no qualified spelling is ever rewritten.
fn temporal_unit_token_len(b: &[u8]) -> Option<usize> {
    // Longer spellings first so `years` wins over `year`.
    const UNITS: &[&[u8]] = &[
        b"centuries",
        b"century",
        b"decade",
        b"dow",
        b"doy",
        b"epoch",
        b"isodow",
        b"isoyear",
        b"julian",
        b"microseconds",
        b"microsecond",
        b"millenium",
        b"millennium",
        b"milliseconds",
        b"millisecond",
        b"nanoseconds",
        b"nanosecond",
        b"quarter",
        b"timezone_hour",
        b"timezone_minute",
        b"timezone",
        b"years",
        b"year",
        b"months",
        b"month",
        b"weeks",
        b"week",
        b"days",
        b"day",
        b"hours",
        b"hour",
        b"minutes",
        b"minute",
        b"seconds",
        b"second",
    ];
    for u in UNITS {
        if b.len() >= u.len() && b[..u.len()].eq_ignore_ascii_case(u) {
            let after = u.len();
            if after < b.len() {
                let next = b[after];
                if next.is_ascii_alphanumeric() || next == b'_' {
                    continue;
                }
            }
            return Some(u.len());
        }
    }
    None
}

/// Reproduce Spark's parse-time `unescapeSQLString` over every single-quoted string literal, then
/// re-emit a DataFusion-equivalent literal. Returns `None` when nothing changed (so the caller keeps
/// the borrowed fast path).
///
/// Spark's default parser (`spark.sql.parser.escapedStringLiterals=false`) runs `unescapeSQLString`
/// on every `'…'` literal: `\\`→`\`, `\n`→newline, `\t`→tab, `\uXXXX`→code point, octal `\ooo`→char,
/// `\'`→`'`, and (Spark's LIKE-pattern carve-out) `\%`/`\_` kept verbatim. DataFusion parses on the
/// Databricks dialect, which (like ANSI SQL) treats backslash as an ordinary character inside `'…'`
/// and only recognizes `''` quote-doubling — so without this pass oxidant would feed the raw
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
        // divisor — including decimal (`a / b`, `a % b` over `SELECT 1.0 a, 0.0 b`, where oxidant types
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
/// `string_temporal_coercion` instead unifies the pair and silently produces a value, so oxidant
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
    use oxidant_common::{Error, Result};

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
    /// Int32's physical layout) and silently produce a value, so oxidant is too lenient (missing-error).
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

/// Metadata for a table created locally via `CREATE TABLE ... USING <fmt>` (including CTAS),
/// captured at CREATE time since `spark_create_table`'s lowering rewrites the statement to a plain
/// `CREATE EXTERNAL TABLE` that DataFusion's own catalog has no way to recover `COMMENT`/
/// `TBLPROPERTIES`/partitioning from. Consulted by later `SHOW CREATE TABLE`/`SHOW TBLPROPERTIES`/
/// `DESCRIBE EXTENDED` work via [`Engine::created_table_meta`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CreatedTableMeta {
    pub format: String,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
    pub partition_columns: Vec<String>,
}

/// Execution statistics for a completed query — the substrate for Databricks-style observability
/// (query time, rows returned, bytes scanned). Populated from DataFusion's `ExecutionPlan` metrics
/// by [`Engine::sql_with_stats`].
#[derive(Debug, Clone, Copy, Default)]
pub struct QueryStats {
    /// Wall-clock execution time in milliseconds.
    pub duration_ms: u64,
    /// Total rows produced by the query.
    pub output_rows: u64,
    /// Bytes read from storage by the plan's scan nodes.
    pub bytes_scanned: u64,
}

/// Sum a named DataFusion metric (e.g. `bytes_scanned`) across every node of an executed physical
/// plan tree. Metrics are per-operator — `bytes_scanned` lives on scan leaves — so we walk the whole
/// tree and total the matching counters. Returns 0 when the metric is absent (e.g. an in-memory
/// scan that reports no bytes).
fn aggregate_plan_metric(plan: &dyn datafusion::physical_plan::ExecutionPlan, name: &str) -> u64 {
    let mut total = plan
        .metrics()
        .and_then(|set| set.sum_by_name(name))
        .map(|v| v.as_usize() as u64)
        .unwrap_or(0);
    for child in plan.children() {
        total += aggregate_plan_metric(child.as_ref(), name);
    }
    total
}

/// Session join-strategy preference from `OXIDANT_PREFER_HASH_JOIN` (KAN-53).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinPreference {
    /// `auto` — the default (also the fallback for unset/empty/unrecognized values): the
    /// engine chooses per query instead of per deployment. KAN-142: a query needing ANY
    /// strategy decision is re-planned once with the per-join rule
    /// ([`Engine::per_join_strategy_physical_plan`]) — each partitioned hash join keeps
    /// hash only when its build side is positively estimated to fit the KAN-25 budget
    /// ([`Engine::hash_join_build_budget`]), converts to broadcast (`CollectLeft`) when the
    /// build is positively measured/estimated under
    /// [`Engine::broadcast_admission_cap`], and converts to spill-capable sort-merge when
    /// over budget OR with no usable build-side statistics (unknown ⇒ safe: an
    /// unaccounted hash build can OOM-kill the worker before the runtime pool-exhaustion
    /// retry fires — SF10 TPC-H Q16/Q21, TPC-DS Q11; KAN-57). A hash join the per-join
    /// rule cannot convert still falls back to the whole-plan sort-merge re-plan.
    /// Without a bounded pool there is no budget to guard, so plans keep their hash
    /// joins. The runtime pool-exhaustion retry (and the KAN-53 stall-retry) remains the
    /// backstop for estimates that undershoot in the other direction.
    Auto,
    /// `true`/`1`/`on`/`yes`: force DataFusion's in-memory hash join session-wide (the
    /// pre-KAN-53 behavior when the variable was set).
    ForceHash,
    /// `false`/`0`/`off`/`no`: force spill-capable sort-merge joins for partitioned
    /// equijoins session-wide.
    ForceSortMerge,
}

/// Parse `OXIDANT_PREFER_HASH_JOIN` as the KAN-53 tri-state (`auto`|`true`|`false`). Before
/// KAN-53 this was a plain boolean force; `auto` is now the default so the engine picks a
/// strategy per join from build-side statistics rather than coin-flipping one per
/// deployment (TPC-DS SF10: Q93 wants hash, Q58 wants sort-merge).
fn join_preference() -> JoinPreference {
    let Ok(raw) = std::env::var("OXIDANT_PREFER_HASH_JOIN") else {
        return JoinPreference::Auto;
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "on" | "yes" => JoinPreference::ForceHash,
        "0" | "false" | "off" | "no" => JoinPreference::ForceSortMerge,
        _ => JoinPreference::Auto,
    }
}

tokio::task_local! {
    /// KAN-53 stall-retry marker: installed by the Flight worker when it re-runs a stage
    /// task that the KAN-47 no-progress watchdog aborted, so the engine plans the retry
    /// with the OPPOSITE join strategy (hash ⇄ sort-merge) from the first attempt.
    /// Task-local — concurrent stage tasks on one worker keep independent selections
    /// (same idiom as `shard::with_replicated_tables`).
    static JOIN_STRATEGY_FLIP: ();
}

/// Whether the current task is a KAN-53 stall-retry attempt (see [`JOIN_STRATEGY_FLIP`]).
fn join_strategy_flipped() -> bool {
    JOIN_STRATEGY_FLIP.try_with(|_| ()).is_ok()
}

/// Run `future` as a KAN-53 stall-retry attempt: [`Engine::collect_join_guarded`] and
/// [`Engine::sql_stream`] plan it with the join strategy opposite to the first attempt's
/// physical plan, bypassing the `auto`/forced selection — the flip IS the retry.
pub async fn with_join_strategy_flipped<F, T>(future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    JOIN_STRATEGY_FLIP.scope((), future).await
}

/// Default ceiling for a single hash-join build side, as a fraction of the bounded memory
/// pool (`OXIDANT_MEMORY_LIMIT_BYTES`); overridable via `OXIDANT_HASH_JOIN_MAX_BUILD_FRACTION`.
/// KAN-25: DataFusion 54's `HashJoinExec` registers its build side with the runtime memory
/// pool (`HashJoinInput`) but is NOT spillable — once the bounded pool fills, the build
/// fails with `Resources Exhausted` instead of spilling, and its untracked overhead (the
/// merged build batch, hash-table growth, probe-side buffering) can push worker RSS far
/// past the pool and wedge the query against the cgroup limit. The guard built on this
/// budget re-plans such queries with spill-capable sort-merge joins.
///
/// Spark EMR parity: the effective budget is also capped by Spark's
/// `canBuildLocalHashMap` rule — see [`DEFAULT_HASH_JOIN_PER_PARTITION_THRESHOLD_BYTES`].
const DEFAULT_HASH_JOIN_MAX_BUILD_FRACTION: f64 = 0.25;

/// Spark `spark.sql.autoBroadcastJoinThreshold` default (10 MiB). Used with shuffle
/// partition count as the SHJ admission gate: build bytes must be `< threshold ×
/// partitions` (and further divided by [`HASH_JOIN_BUILD_OVERHEAD_FACTOR`] for hash-table
/// / Arrow overhead that sits outside FairSpillPool). Override via
/// `OXIDANT_HASH_JOIN_PER_PARTITION_THRESHOLD_BYTES`.
const DEFAULT_HASH_JOIN_PER_PARTITION_THRESHOLD_BYTES: usize = 10 * 1024 * 1024;

/// Multiplier for untracked HashJoin RSS (hash table, merged build batch, probe buffers)
/// relative to the row-width estimate. Without this, SF100 fact⋈fact builds can look
/// "under budget" then cgroup-OOM before the pool-exhaustion retry (KAN-57).
const HASH_JOIN_BUILD_OVERHEAD_FACTOR: usize = 2;

/// KAN-142 broadcast (`CollectLeft`) admission threshold, mirroring Spark's
/// `spark.sql.autoBroadcastJoinThreshold` default (10 MiB). A partitioned hash join whose
/// build side is POSITIVELY estimated at or below this size (and within the KAN-25 budget)
/// is converted to a broadcast hash join — the build side is coalesced once and shared by
/// every probe partition, eliding both sides' shuffle repartitions (Spark AQE's
/// runtime broadcast conversion; DataFusion 54's own `CollectLeft` admission stops at
/// `hash_join_single_partition_threshold` = 1 MiB / 128K rows, far below barrier-measured
/// shuffle inputs). Override via `OXIDANT_BROADCAST_JOIN_THRESHOLD_BYTES`; `0` disables.
const DEFAULT_BROADCAST_JOIN_THRESHOLD_BYTES: usize = 10 * 1024 * 1024;

/// The KAN-142 broadcast admission threshold from `OXIDANT_BROADCAST_JOIN_THRESHOLD_BYTES`;
/// `None` disables broadcast conversion (`=0`). Unparseable values keep the default.
fn broadcast_join_threshold_bytes() -> Option<usize> {
    match std::env::var("OXIDANT_BROADCAST_JOIN_THRESHOLD_BYTES") {
        Ok(s) => match s.parse::<usize>() {
            Ok(0) => None,
            Ok(n) => Some(n),
            Err(_) => Some(DEFAULT_BROADCAST_JOIN_THRESHOLD_BYTES),
        },
        Err(_) => Some(DEFAULT_BROADCAST_JOIN_THRESHOLD_BYTES),
    }
}

/// Spark-like default shuffle partition floor when sizing the hash-join build budget
/// (`spark.sql.shuffle.partitions` default is 200).
const DEFAULT_SHUFFLE_PARTITIONS_FOR_JOIN_BUDGET: u32 = 200;

/// Shuffle partition count used for the Spark-aligned HashJoin build cap. Prefers
/// `OXIDANT_SHUFFLE_PARTITIONS`, then `OXIDANT_DEFAULT_PARALLELISM`, else 200.
fn shuffle_partitions_for_join_budget() -> u32 {
    std::env::var("OXIDANT_SHUFFLE_PARTITIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n: &u32| n > 0)
        .or_else(|| {
            std::env::var("OXIDANT_DEFAULT_PARALLELISM")
                .ok()
                .and_then(|s| s.parse().ok())
                .filter(|&n: &u32| n > 0)
        })
        .unwrap_or(DEFAULT_SHUFFLE_PARTITIONS_FOR_JOIN_BUDGET)
}

/// Spark `canBuildLocalHashMap` budget: `threshold × partitions / overhead`.
fn spark_aligned_hash_join_build_cap() -> usize {
    let threshold = std::env::var("OXIDANT_HASH_JOIN_PER_PARTITION_THRESHOLD_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n: &usize| n > 0)
        .unwrap_or(DEFAULT_HASH_JOIN_PER_PARTITION_THRESHOLD_BYTES);
    let partitions = shuffle_partitions_for_join_budget() as usize;
    threshold
        .saturating_mul(partitions)
        .saturating_div(HASH_JOIN_BUILD_OVERHEAD_FACTOR)
        .max(threshold) // never below one partition's threshold
}

/// Estimated in-memory width of one row of `schema`, in bytes. Fixed-width types use their
/// exact size; variable-width values use a flat 48 bytes (offset + typical content). Only a
/// coarse build-side size estimate: an underestimate merely defers a query to the runtime
/// retry in [`Engine::collect_join_guarded`], an overestimate only picks the (correct,
/// spill-capable) sort-merge join earlier.
fn estimated_row_width(schema: &arrow::datatypes::Schema) -> usize {
    use arrow::datatypes::DataType;
    schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Boolean | DataType::Int8 | DataType::UInt8 => 1,
            DataType::Int16 | DataType::UInt16 => 2,
            DataType::Int32
            | DataType::UInt32
            | DataType::Float32
            | DataType::Date32
            | DataType::Time32(_) => 4,
            DataType::Int64
            | DataType::UInt64
            | DataType::Float64
            | DataType::Date64
            | DataType::Timestamp(..)
            | DataType::Time64(_)
            | DataType::Duration(_) => 8,
            DataType::Decimal128(..) => 16,
            DataType::Decimal256(..) => 32,
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView => 48,
            _ => 64,
        })
        .sum()
}

/// Downcast a plan node to a hash join, if it is one. (DataFusion 54's `ExecutionPlan`
/// supertrait is `Any` directly — there is no `as_any()` adapter.)
fn as_hash_join(
    plan: &dyn datafusion::physical_plan::ExecutionPlan,
) -> Option<&datafusion::physical_plan::joins::HashJoinExec> {
    (plan as &dyn std::any::Any).downcast_ref()
}

/// Whether `plan`'s tree contains a hash join at all.
fn contains_hash_join(plan: &dyn datafusion::physical_plan::ExecutionPlan) -> bool {
    as_hash_join(plan).is_some()
        || plan
            .children()
            .iter()
            .any(|c| contains_hash_join(c.as_ref()))
}

/// Estimated in-memory bytes of a hash join's build (left) side, from the child's plan
/// statistics. Prefers row count × schema row width (`total_byte_size` under-reports for
/// compressed scans — e.g. it is the Parquet file size); `None` when the plan carries no
/// usable statistics (a join- or aggregate-output build side, a CSV/JSON scan, or footer
/// statistics disabled via `OXIDANT_PARQUET_SCAN_STATS` — catalog Parquet/Delta/Iceberg scans
/// otherwise carry exact footer row counts, see
/// [`catalog_bridge::parquet_footer_file_groups`]). `None` is NOT "fits the budget": with a
/// bounded pool the caller treats it as a sort-merge reroute (see
/// [`Engine::plan_time_smj_reroute`]) — an unaccounted hash build (KAN-57) can OOM-kill the
/// worker before the runtime pool-exhaustion retry ever fires (SF10: TPC-H Q16/Q21, TPC-DS
/// Q11).
fn hash_join_build_estimated_bytes(
    hj: &datafusion::physical_plan::joins::HashJoinExec,
) -> Option<usize> {
    let build = hj.left();
    let stats = build.partition_statistics(None).ok()?;
    if let Some(rows) = stats.num_rows.get_value() {
        return Some(rows.saturating_mul(estimated_row_width(&build.schema())));
    }
    stats
        .total_byte_size
        .get_value()
        .copied()
        .filter(|b| *b > 0)
}

/// Whether any hash join in `plan` has a build side estimated above `budget` bytes.
fn hash_join_build_exceeds(
    plan: &dyn datafusion::physical_plan::ExecutionPlan,
    budget: usize,
) -> bool {
    if let Some(hj) = as_hash_join(plan) {
        if hash_join_build_estimated_bytes(hj).is_some_and(|b| b > budget) {
            return true;
        }
    }
    plan.children()
        .iter()
        .any(|c| hash_join_build_exceeds(c.as_ref(), budget))
}

/// Whether any hash join in `plan` has NO usable build-side estimate
/// ([`hash_join_build_estimated_bytes`] is `None`). Distinct from
/// [`hash_join_build_exceeds`]: unknown is not "fits".
fn hash_join_build_estimate_unknown(plan: &dyn datafusion::physical_plan::ExecutionPlan) -> bool {
    if let Some(hj) = as_hash_join(plan) {
        if hash_join_build_estimated_bytes(hj).is_none() {
            return true;
        }
    }
    plan.children()
        .iter()
        .any(|c| hash_join_build_estimate_unknown(c.as_ref()))
}

// ---- KAN-142: per-join runtime strategy conversion (Spark AQE analogue) --------------

/// Whether `hj` still runs partitioned while its build side is PROVABLY at or below `cap`
/// bytes — a KAN-142 broadcast (`CollectLeft`) candidate. Restricted to INNER joins:
/// outer/semi/anti joins constrain which side may be broadcast, and null-aware anti joins
/// REQUIRE `CollectLeft` already (DataFusion seats them so at physical planning).
///
/// KAN-146: the sizing standard is a provable row UPPER BOUND ([`provable_row_bound`] —
/// `Exact` statistics seen through row-preserving/non-increasing wrappers), NOT the
/// KAN-25 budget guard's estimate ([`hash_join_build_estimated_bytes`], which accepts
/// `Inexact`). An `Inexact` estimate is a guess, not a bound: DataFusion estimates a
/// column-stats-less join output as `Inexact(min(left, right))` — orders of magnitude
/// under the real fact-sized result for an FK star join (see [`PreferBoundedJoinBuildSide`])
/// — and a filtered scan's post-selectivity estimate undershoots the same way when the
/// filter's column statistics are absent or stale. A broadcast conversion on such a
/// phantom-small "estimate" coalesces the real build to ONE partition and single-thread
/// hash-builds it on every consuming task — the exact wedge the KAN-2 rule exists to
/// prevent — while eliding the probe-side repartition that kept the join parallel. The
/// guard can afford `Inexact` (its error direction is a safe sort-merge reroute);
/// broadcast admission cannot (its error direction is a serialized coalesce), so unknown
/// OR inexact builds are NOT broadcast candidates. Honest admissions keep working:
/// footer-exact parquet scans and barrier-measured (`MeasuredStatsTable`) shuffle inputs
/// report `Exact` rows.
fn hash_join_broadcast_eligible(
    hj: &datafusion::physical_plan::joins::HashJoinExec,
    cap: usize,
) -> bool {
    use datafusion::physical_plan::joins::PartitionMode;
    !matches!(hj.partition_mode(), PartitionMode::CollectLeft)
        && !hj.null_aware
        && *hj.join_type() == datafusion::logical_expr::JoinType::Inner
        && provable_row_bound(hj.left().as_ref()).is_some_and(|rows| {
            rows.saturating_mul(estimated_row_width(&hj.left().schema())) <= cap
        })
}

/// Whether any hash join in `plan` is a KAN-142 broadcast candidate at `cap` bytes (see
/// [`hash_join_broadcast_eligible`]).
fn hash_join_broadcast_candidate(
    plan: &dyn datafusion::physical_plan::ExecutionPlan,
    cap: usize,
) -> bool {
    if let Some(hj) = as_hash_join(plan) {
        if hash_join_broadcast_eligible(hj, cap) {
            return true;
        }
    }
    plan.children()
        .iter()
        .any(|c| hash_join_broadcast_candidate(c.as_ref(), cap))
}

/// Convert a partitioned hash join to broadcast (`CollectLeft`): the build side is
/// coalesced to one partition and collected ONCE, shared by every probe-side partition —
/// both sides' shuffle repartitions drop out under `EnforceDistribution`. `None` when the
/// rebuild fails (the caller then keeps the partitioned join). The builder preserves the
/// projection, join filter and null equality (the KAN-2 R2 dynamic filter is always
/// `None` at this point — it attaches in the later `FilterPushdown` phase); only the
/// partition mode changes (properties are recomputed).
fn hash_join_as_broadcast(
    hj: &datafusion::physical_plan::joins::HashJoinExec,
) -> Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
    use datafusion::physical_plan::joins::PartitionMode;
    hj.builder()
        .with_partition_mode(PartitionMode::CollectLeft)
        .build()
        .ok()
        .map(|h| Arc::new(h) as Arc<dyn datafusion::physical_plan::ExecutionPlan>)
}

/// Convert a hash join to a spill-capable sort-merge join exactly as the DataFusion
/// physical planner builds one under `prefer_hash_join=false` (same `on`, filter, join
/// type, default sort options, null equality), so a per-join conversion is
/// byte-equivalent to what the whole-plan KAN-25 re-plan would produce for that join.
/// `None` when the join carries a pushed-down column projection (a `SortMergeJoinExec`
/// cannot re-seat one — the caller falls back to the whole-plan re-plan), when the join
/// is NULL-AWARE (see below), or when construction fails.
///
/// Null-aware anti joins (`NOT IN` with nullable keys) must NEVER be converted here:
/// `SortMergeJoinExec` has no null-aware support — DataFusion's own planner comment is
/// "Null-aware joins must use CollectLeft" (datafusion-54.1.0 physical_planner.rs) — so
/// converting one would silently drop NOT-IN NULL semantics (a NULL in the subquery must
/// empty the result; a plain anti join treats it as never matching). Note the pre-existing
/// hazard this does NOT fix: the whole-plan KAN-25 re-plan (`prefer_hash_join=false`)
/// still seats null-aware joins as sort-merge because DataFusion's planner checks
/// `prefer_hash_join` before `null_aware` — out of scope for KAN-142, but this rule must
/// never make it worse by newly converting one itself.
fn hash_join_as_sort_merge(
    hj: &datafusion::physical_plan::joins::HashJoinExec,
) -> Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
    if hj.contains_projection() || hj.null_aware {
        return None;
    }
    let on = hj.on().to_vec();
    datafusion::physical_plan::joins::SortMergeJoinExec::try_new(
        Arc::clone(hj.left()),
        Arc::clone(hj.right()),
        on.clone(),
        hj.filter().cloned(),
        *hj.join_type(),
        vec![arrow::compute::SortOptions::default(); on.len()],
        hj.null_equality(),
    )
    .ok()
    .map(|smj| Arc::new(smj) as Arc<dyn datafusion::physical_plan::ExecutionPlan>)
}

/// The KAN-142 physical optimizer rule: decide the join strategy PER JOIN from the
/// build-side size statistics available at plan time (barrier-measured on shuffle inputs,
/// footer-exact on catalog scans) instead of the all-or-nothing session re-plan:
///
/// - build side PROVABLY at or below `broadcast_cap` (an INNER join, KAN-146: a
///   `Exact`-statistic row upper bound through row-preserving wrappers, never an
///   `Inexact` estimate — see [`hash_join_broadcast_eligible`]) ⇒ broadcast
///   (`CollectLeft`) hash join — Spark AQE's runtime broadcast conversion;
/// - build side over `budget`, or with NO usable estimate, with `smj_allowed` ⇒
///   sort-merge join — the KAN-25/KAN-53 "unknown ⇒ safe" policy, per join;
/// - otherwise the partitioned hash join stands (build positively fits the budget).
///
/// NULL-AWARE anti joins are excluded from BOTH conversions: they already require
/// `CollectLeft`, and `SortMergeJoinExec` has no null-aware support (see
/// [`hash_join_as_sort_merge`]).
///
/// Inserted after [`PreferBoundedJoinBuildSide`] and BEFORE `EnforceDistribution`, so a
/// converted join's inputs are (re-)partitioned and sorted for the NEW strategy. The
/// KAN-25 memory guarantee is preserved: a broadcast admission cap never exceeds the
/// hash-join budget ([`Engine::broadcast_admission_cap`]), so no hash build is admitted
/// by this rule that the old guard would have rerouted; over-budget or unknown builds are
/// converted to spill-capable sort-merge, and a join the rule cannot convert (a
/// projection-carrying hash join) still trips [`Engine::needs_smj_reroute`] on the
/// converted plan and falls back to the whole-plan sort-merge re-plan.
#[derive(Debug)]
struct PerJoinJoinStrategy {
    /// KAN-25 build-side budget in bytes; `None` disables sort-merge conversion.
    budget: Option<usize>,
    /// Broadcast admission cap in bytes (≤ `budget`); `None` disables broadcast conversion.
    broadcast_cap: Option<usize>,
    /// Whether hash → sort-merge conversion is allowed ([`Engine::smj_replan_allowed`]).
    smj_allowed: bool,
}

impl datafusion::physical_optimizer::PhysicalOptimizerRule for PerJoinJoinStrategy {
    fn optimize(
        &self,
        plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        _config: &datafusion::common::config::ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
        plan.transform_up(|p| {
            let Some(hj) = as_hash_join(p.as_ref()) else {
                return Ok(Transformed::no(p));
            };
            if let Some(cap) = self.broadcast_cap {
                if hash_join_broadcast_eligible(hj, cap) {
                    if let Some(broadcast) = hash_join_as_broadcast(hj) {
                        return Ok(Transformed::yes(broadcast));
                    }
                }
            }
            if self.smj_allowed
                && !hj.null_aware
                && self.budget.is_some_and(|budget| {
                    hash_join_build_estimated_bytes(hj).map_or(true, |est| est > budget)
                })
            {
                if let Some(smj) = hash_join_as_sort_merge(hj) {
                    return Ok(Transformed::yes(smj));
                }
            }
            Ok(Transformed::no(p))
        })
        .data()
    }

    fn name(&self) -> &str {
        "per_join_join_strategy"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

// ---- KAN-2 (TPC-DS Q62 wedge): build hash joins on row-BOUNDED sides only -------------

/// A provable UPPER BOUND on `plan`'s output rows: its own `Exact` row statistic, or such
/// a statistic seen through row-preserving / row-non-increasing single-child wrappers
/// (projection, filter, repartition, coalesce, sort — a filter only drops rows, the rest
/// preserve them). `None` when any operator on the path can multiply rows relative to its
/// known input (a join above all) — an `Inexact` statistic is an estimate, NOT a bound:
/// DataFusion 54.1.0 estimates a column-stats-less inner join output as
/// `Inexact(min(left, right))` rows (the NDV falls back to the row counts, see
/// [`join_chain_output_statistics_report_usable_inexact_num_rows`]), which for a
/// foreign-key star join UNDERESTIMATES the real (fact-sized) output by orders of
/// magnitude.
fn provable_row_bound(plan: &dyn datafusion::physical_plan::ExecutionPlan) -> Option<usize> {
    use datafusion::common::stats::Precision;
    if let Ok(stats) = plan.partition_statistics(None) {
        if let Precision::Exact(rows) = stats.num_rows {
            return Some(rows);
        }
    }
    let is_wrapper = (plan as &dyn std::any::Any)
        .is::<datafusion::physical_plan::projection::ProjectionExec>()
        || (plan as &dyn std::any::Any).is::<datafusion::physical_plan::filter::FilterExec>()
        || (plan as &dyn std::any::Any)
            .is::<datafusion::physical_plan::repartition::RepartitionExec>()
        || (plan as &dyn std::any::Any)
            .is::<datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec>()
        || (plan as &dyn std::any::Any).is::<datafusion::physical_plan::sorts::sort::SortExec>();
    if is_wrapper && plan.children().len() == 1 {
        return provable_row_bound(plan.children()[0].as_ref());
    }
    None
}

/// The KAN-2 physical optimizer rule: an INNER hash join whose build (left) side has NO
/// provable row bound while its probe (right) side has one is re-seated to build on the
/// bounded side (via the same [`HashJoinExec::swap_inputs`] DataFusion's own
/// `JoinSelection` uses, preserving the partition mode).
///
/// Why this exists (the TPC-DS Q62 SF10 wedge): the Q62 stage-0 arm is a comma-join chain
/// `web_sales × warehouse × ship_mode × web_site × date_dim` with the fact FIRST. With
/// footer row counts but no column statistics, every chain-intermediate reports
/// `Inexact(min(l, r))` ≈ 20 rows for what is really the full 7.2M-row fact-wide output.
/// `JoinSelection` compares those raw values precision-blind
/// (`should_swap_join_order`: `Inexact(20) > Exact(20)` is false), so the swap never fires;
/// worse, the phantom-tiny estimate passes the `CollectLeft` thresholds, so each of the
/// three upper joins COALESCED the whole chain intermediate to one partition and
/// single-thread hash-built it (three ever-wider 7.2M-row tables) while the KAN-25 guard
/// read `Inexact(20) × row width` ≈ 3 KB and saw no risk: ~100x slowdown under the shared
/// bounded pool, slow progress rather than an error, so neither the budget guard nor the
/// runtime pool retry could engage (local repro: 29 s auto vs 1.2 s sort-merge at 3M
/// rows). After the re-seat, every build side is a positively-sized dimension scan
/// (exact/bounded rows), which is also what the KAN-25 budget guard needs to make sound
/// reroute decisions — a genuinely large bounded build now reroutes to sort-merge instead
/// of hiding behind an inexact chain estimate.
///
/// Restricted to INNER joins: outer/semi/anti joins swap semantics along with sides, and
/// their DataFusion estimates already clamp to the preserved side's row count. Firing only
/// on (unbounded build, bounded probe) keeps every positively-sized plan byte-for-byte
/// unchanged — the star shapes whose builds are already exact dims (TPC-DS Q37/Q82) and
/// plans with no provable side at all are left alone.
#[derive(Debug)]
struct PreferBoundedJoinBuildSide;

impl datafusion::physical_optimizer::PhysicalOptimizerRule for PreferBoundedJoinBuildSide {
    fn optimize(
        &self,
        plan: std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        _config: &datafusion::common::config::ConfigOptions,
    ) -> datafusion::common::Result<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>>
    {
        use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
        plan.transform_up(|p| {
            let Some(hj) = as_hash_join(p.as_ref()) else {
                return Ok(Transformed::no(p));
            };
            if *hj.join_type() != datafusion::logical_expr::JoinType::Inner || hj.null_aware {
                return Ok(Transformed::no(p));
            }
            // Never re-seat a join that carries a non-equi filter. The filter is evaluated
            // against its own intermediate schema via side-tagged column indices; re-seating
            // here (after `JoinSelection`, before `EnforceDistribution`) leaves that mapping
            // resolving from the opposite inputs, which silently evaluates the predicate with
            // its operands exchanged rather than failing. TPC-DS Q72 at SF10 hit exactly this:
            // `inv_quantity_on_hand < cs_quantity` ran as `cs_quantity < inv_quantity_on_hand`
            // and inflated `count(*)` ~19x (786,559 groups vs the correct 42,226) with no error.
            // The rule is a build-side performance heuristic, so declining these joins costs at
            // most the re-seat; returning wrong answers is not an acceptable trade.
            if hj.filter().is_some() {
                return Ok(Transformed::no(p));
            }
            if provable_row_bound(hj.left().as_ref()).is_some()
                || provable_row_bound(hj.right().as_ref()).is_none()
            {
                return Ok(Transformed::no(p));
            }
            Ok(Transformed::yes(hj.swap_inputs(*hj.partition_mode())?))
        })
        .data()
    }

    fn name(&self) -> &str {
        "prefer_bounded_join_build_side"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// The engine's physical optimizer pipeline: DataFusion 54.1.0's stock rules plus
/// [`PreferBoundedJoinBuildSide`] inserted immediately after `JoinSelection` — crucially
/// BEFORE `EnforceDistribution`, so a re-seated join's inputs are (re-)partitioned for the
/// NEW build/probe sides (`HashJoinExec::swap_inputs` is only valid before distribution
/// enforcement has repartitioned the children). If the stock pipeline ever loses the
/// `join_selection` anchor (an upstream reshuffle), the rule is inserted before
/// `EnforceDistribution` instead; with neither anchor present the stock pipeline is kept
/// whole — a mis-positioned swap could silently break partitioning, which is worse than
/// no rule at all.
fn physical_optimizer_rules(
) -> Vec<std::sync::Arc<dyn datafusion::physical_optimizer::PhysicalOptimizerRule + Send + Sync>> {
    let mut rules = datafusion::physical_optimizer::optimizer::PhysicalOptimizer::new().rules;
    let position = rules
        .iter()
        .position(|r| r.name() == "join_selection")
        .map(|i| i + 1)
        .or_else(|| rules.iter().position(|r| r.name() == "EnforceDistribution"));
    if let Some(i) = position {
        rules.insert(i, std::sync::Arc::new(PreferBoundedJoinBuildSide));
    }
    // KAN-70 GPU offload spike: appends the conservative scan+filter+aggregate
    // offload rule immediately before `EnforceDistribution` — but only when
    // `OXIDANT_GPU_OFFLOAD=1` is set; otherwise the pipeline is untouched.
    oxidant_gpu::register_if_enabled(&mut rules);
    rules
}

/// [`physical_optimizer_rules`] plus the KAN-142 per-join strategy rule, inserted
/// immediately after [`PreferBoundedJoinBuildSide`] (build-side seating must be final
/// before per-join strategy sizing) and before `EnforceDistribution` (converted joins
/// need their inputs re-partitioned / re-sorted for the new strategy). Used by the
/// query-scoped KAN-142 re-plan ([`Engine::per_join_strategy_physical_plan`]); the engine's
/// own session pipeline stays stock so plans that need no conversion never pay for one.
/// With neither anchor present the pipeline is kept whole — a mis-positioned conversion
/// could silently break partitioning, which is worse than no rule at all (the re-plan
/// then degrades to the unchanged plan, and the whole-plan sort-merge fallback still
/// guards over-budget builds).
fn physical_optimizer_rules_with_join_strategy(
    strategy: std::sync::Arc<PerJoinJoinStrategy>,
) -> Vec<std::sync::Arc<dyn datafusion::physical_optimizer::PhysicalOptimizerRule + Send + Sync>> {
    let mut rules = physical_optimizer_rules();
    let position = rules
        .iter()
        .position(|r| r.name() == "prefer_bounded_join_build_side")
        .map(|i| i + 1)
        .or_else(|| rules.iter().position(|r| r.name() == "EnforceDistribution"));
    if let Some(i) = position {
        rules.insert(i, strategy);
    }
    rules
}

/// Whether an execution error is the bounded memory pool denying a reservation.
/// DataFusion renders `DataFusionError::ResourcesExhausted` as "Resources Exhausted: …";
/// match textually so wrapped / contextual errors still count.
fn is_pool_exhausted(err: &datafusion::error::DataFusionError) -> bool {
    err.to_string()
        .to_ascii_lowercase()
        .contains("resources exhausted")
}

/// `(bytes, catalog row-count statistic)` pair behind [`Engine::estimate_table_stats`];
/// aliased so the per-engine TTL cache field stays readable.
type TableStats = (Option<u64>, Option<u64>);

/// The CPU execution engine: a DataFusion [`SessionContext`] today, growing native
/// operators behind the same surface in Phase 1.
///
/// Cloning a handle shares ALL engine state — including the `current` catalog/namespace cell
/// (KAN-85: use [`Engine::for_session`] for a DIFFERENT session's state). The managed
/// directories are removed only when the last handle drops.
#[derive(Clone)]
pub struct Engine {
    ctx: Arc<SessionContext>,
    /// Per-engine managed directories: the warehouse (Spark's `CREATE TABLE … USING <fmt>` is
    /// lowered to a real `CREATE EXTERNAL TABLE … LOCATION '<warehouse>/<name>/'` whose data
    /// lives in actual `<fmt>` files under here — see [`spark_create_table`]) and the
    /// DataFusion spill dir (sort/aggregate spill files, kept out of the shared OS temp root so
    /// the watchdog can size it cheaply). One directory per `Engine` isolates
    /// otherwise-colliding table names across files; both are removed when the LAST handle
    /// drops — `for_session` clones share them (KAN-85).
    dirs: Arc<ManagedDirs>,
    /// Lowercased names of the session-temporary views created so far in this engine's lifetime
    /// (`CREATE [GLOBAL] TEMP[ORARY] VIEW <name>`). Spark forbids a *persistent* `CREATE VIEW` from
    /// referencing any of these (SPARK-29628 / `INVALID_TEMP_OBJ_REFERENCE`); DataFusion has no
    /// temp/permanent distinction and would silently accept it, so we track the temp set ourselves
    /// and reject the offending persistent view to keep error-parity with Spark. A name is removed
    /// when a later persistent view re-uses it (DataFusion's single namespace would shadow it).
    temp_views: Arc<Mutex<HashSet<String>>>,
    /// The external [`oxidant_catalog::CatalogProvider`]s registered via [`Engine::register_catalog`],
    /// keyed by their registered name. Held alongside the DataFusion bridge so the engine can answer
    /// `SHOW DATABASES`/`SHOW TABLES IN …` authoritatively (the bridge only exposes a best-effort,
    /// already-materialized listing). See the SHOW interception in [`Engine::sql`].
    oxidant_catalogs: Arc<Mutex<HashMap<String, Arc<dyn oxidant_catalog::CatalogProvider>>>>,
    /// User-defined functions registered in this session (SQL `CREATE FUNCTION`, Connect sync).
    udf_registry: udf_registry::SharedUdfRegistry,
    /// This handle's current catalog + current namespace ("current database"), set by `USE` and
    /// consulted for defaulting unqualified names in SHOW/DESCRIBE (see [`Engine::sql`]'s `USE`
    /// interception and [`Engine::current_catalog_and_namespace`]). The ONLY per-handle state:
    /// a plain `Engine::new()` handle owns a private cell (CLI/tests/direct use unchanged),
    /// while [`Engine::for_session`] handles share everything else but point `current` at the
    /// session's registry cell (KAN-85) — one Connect session's `USE` no longer leaks into
    /// every other session.
    current: SessionCurrent,
    /// KAN-85: per-session (catalog, namespace) cells keyed by Connect session id, plus the
    /// catalog new sessions seed from (`spark.sql.defaultCatalog`; default `spark_catalog`).
    sessions: Arc<Mutex<SessionState>>,
    /// Metadata for tables created locally via `CREATE TABLE ... USING <fmt>` (see
    /// [`CreatedTableMeta`]), keyed by the table name as written in the `CREATE TABLE` statement.
    /// `spark_create_table`'s lowering rewrites the statement into a plain `CREATE EXTERNAL TABLE`
    /// that DataFusion's catalog cannot answer `COMMENT`/`TBLPROPERTIES` from, so this is captured
    /// separately at CREATE time. Consulted by later SHOW/DESCRIBE work via
    /// [`Engine::created_table_meta`].
    created_tables: Arc<Mutex<HashMap<String, CreatedTableMeta>>>,
    /// Set permanently on Flight workers so losing a distributed stage's task-local snapshot
    /// scope fails rather than silently resolving a newer lakehouse snapshot.
    require_lakehouse_snapshot_pins: Arc<AtomicBool>,
    /// The bounded spill-pool size in bytes (`OXIDANT_MEMORY_LIMIT_BYTES`), when configured.
    /// Drives the KAN-25 hash-join memory guard ([`Engine::collect_join_guarded`]); `None`
    /// (unbounded pool) disables the guard entirely.
    memory_pool_bytes: Option<usize>,
    /// Test/diagnostic observability for the plan-time join-strategy guard: how often the
    /// KAN-53 auto selection's sort-merge predicate fired on this engine
    /// ([`Engine::plan_time_smj_reroute`] returning true). Since KAN-142 the firing query
    /// is usually handled by the per-join conversion re-plan, with the whole-plan
    /// sort-merge re-plan as the fallback, so the count reads "guard engagements", not
    /// strictly "whole-plan reroutes". Read via [`Engine::plan_time_smj_reroute_count`].
    plan_time_smj_reroutes: Arc<AtomicU64>,
    /// How many tables were registered with driver-measured statistics on this engine
    /// ([`Engine::register_batches_with_stats`]) — the worker-side observable that the
    /// stage-input statistics path (KAN-2 A3) actually engaged. Read via
    /// [`Engine::measured_stats_registration_count`].
    measured_stats_registrations: Arc<AtomicU64>,
    /// This engine's identity in the process-global stage plan cache
    /// ([`stage_plan_cache`], R5-4): two engines in one process never share plan templates
    /// (a template embeds its engine's base-table providers). Unique per engine, from the
    /// same sequence as the managed-warehouse id.
    plan_cache_id: u64,
    /// Bumped on every non-shuffle catalog mutation (register/deregister/DDL/UDF sync) —
    /// the stage plan cache's staleness guard for a template's embedded base-table
    /// providers. Per-task localized `shuffle_input__s*_p*` registrations are exempt
    /// (their schemas ride in the cache key instead; bumping on every task would
    /// invalidate the cache constantly). Read into [`stage_plan_cache::StagePlanKey`].
    catalog_version: Arc<AtomicU64>,
    /// Last-activity timestamp of the engine's memory pool (grow/shrink/try_grow) — a
    /// worker-wide operator-progress signal for the stage no-progress watchdog (KAN-47).
    pool_activity: progress_pool::PoolActivity,
    /// Cached `estimate_table_stats` results (bytes + catalog row-count statistic), keyed by
    /// the lowercased RESOLVED (catalog, namespace, table) — bare-name estimates are scoped to
    /// the session state that produced them (KAN-85), qualified names share one entry.
    /// Auto-broadcast sizing runs per query, and the uncached path issues Glue/Catalog API
    /// calls and S3 LISTs for every table — a multi-second fixed tax per query (the SF10
    /// per-query floor). Sizes/row counts only steer the replicate/shard heuristic (never
    /// correctness), so a bounded TTL (`OXIDANT_TABLE_BYTES_CACHE_TTL_MS`, default 1h, 0
    /// disables) is safe against data growth.
    table_bytes_cache: Arc<Mutex<HashMap<String, (TableStats, std::time::Instant)>>>,
}

/// The engine's managed directories (warehouse + spill). Removed when the last `Arc` handle
/// drops — `Engine` handles produced by [`Engine::for_session`] share one `ManagedDirs`, so a
/// session handle dropping mid-flight never deletes files another session is reading (KAN-85).
struct ManagedDirs {
    warehouse: PathBuf,
    spill_dir: PathBuf,
}

impl Drop for ManagedDirs {
    /// Tear down the managed directories: the warehouse (the `CREATE TABLE …
    /// USING <fmt>` format-backed storage) and the DataFusion spill dir. Best-effort: a
    /// leftover temp dir is harmless, so failures are ignored.
    fn drop(&mut self) {
        if self.warehouse.exists() {
            let _ = std::fs::remove_dir_all(&self.warehouse);
        }
        if self.spill_dir.exists() {
            let _ = std::fs::remove_dir_all(&self.spill_dir);
        }
    }
}

/// A session's current `(catalog, namespace)` cell (KAN-85) — shared by every engine handle
/// derived for that session via [`Engine::for_session`].
type SessionCurrent = Arc<Mutex<(String, Vec<String>)>>;

/// KAN-85 per-session state: each Connect session's (catalog, namespace) cell, plus the
/// catalog NEW sessions seed from (`spark.sql.defaultCatalog`, default `spark_catalog`).
/// Existing sessions keep their (possibly `USE`-adjusted) state when the default changes.
struct SessionState {
    default_catalog: String,
    cells: HashMap<String, SessionCurrent>,
}

/// How much rewriting a plan tolerates before the distributed stage split, gating
/// [`Engine::optimize_logical_plan`]. The splitter's handlers pattern-match specific
/// plan shapes, so the pre-split optimizer must stay inside the vocabulary they can
/// re-render.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreSplitRewrite {
    /// `extract_equijoin_predicate` + `push_down_filter` only — the proven default
    /// (TPC-DS Q78/Q39 group-key predicates reach the scans; every node type in the
    /// plan keeps its identity).
    Standard,
    /// The plan contains a `Union`: additionally fold constant predicates
    /// ([`FoldConstantFilters`] — filter-scoped, unlike `simplify_expressions`), then run
    /// `eliminate_filter`, `propagate_empty_relation`, and `optimize_unions` so
    /// predicates pushed into union arms *prune the arms they contradict*. TPC-DS
    /// Q4's six `year_total` occurrences each carry a different `sale_type = 's'/'c'/'w'`
    /// predicate; pushed through the arm projections those become literal comparisons
    /// (`'c' = 's'`) that fold to `false`, the arm collapses to an `EmptyRelation`, and
    /// the union drops it — each occurrence degenerates to the single fact slice it
    /// filters (and the `dyear` group-key predicate reaches the `date_dim` scan).
    /// Pushdown *without* the pruning rules is the v12 failure mode: per-branch
    /// predicates only made the shared union arms textually distinct, defeating
    /// stage-level CSE (Q4 went from ~15 stages to 66 and workers failed under the
    /// tiny-stage orchestration load — do_get transport error).
    UnionExtended,
    /// Shape classes the splitter pattern-matches in their unoptimized form — return
    /// the plan byte-for-byte untouched:
    /// - SQL table aliases (`JOIN date_dim d1`): a `SubqueryAlias` wrapping exactly one
    ///   `TableScan` plus passthrough `Filter`/`Projection` nodes. `PushDownFilter`
    ///   re-qualifies pushed predicates to the *base* table name (`date_dim.d_year`),
    ///   but the splitter's broadcast/replicated path re-renders the scan with the
    ///   alias (`FROM date_dim AS d1`) — the worker then fails with "No field named
    ///   date_dim.d_year. Did you mean 'd1.d_year'?" (TPC-DS Q72). CTE `SubqueryAlias`
    ///   nodes (over aggregates/joins — TPC-DS Q78/Q39) are NOT this class and still
    ///   qualify.
    /// - `EXISTS` / `IN` / scalar subquery expressions: pushing predicates inside them
    ///   moves their filters into the inner `TableScan`, which the splitter's subquery
    ///   handlers cannot re-render
    ///   (`auto_distribute::exists_subquery_over_replicated_dim`).
    /// - `Window`: the splitter's window handlers match the unoptimized frame shape;
    ///   pushing outer predicates through the frame (TPC-DS Q47/Q57's `avg() OVER` /
    ///   `rank() OVER` CTE families) left no distributed shape the splitter recognizes
    ///   ("no distributed semi/anti or decorrelated shape matched").
    Skip,
}

/// Filter-scoped constant folding for the union-extended pre-split rule set
/// ([`PreSplitRewrite::UnionExtended`]). The stock `SimplifyExpressions` rule is *not*
/// used: it also simplifies projection/aggregate expressions, and folding
/// `CAST(0 AS DECIMAL(7,2))` to a bare decimal literal makes the stage-SQL unparser emit
/// `0.00`, which re-parses as `DECIMAL(3,2)` — the union's downstream decimal coercion
/// then shifts result scales and distributed results no longer match single-node
/// byte-for-byte (TPC-DS Q5: `1141124.71` vs `1141124.710000000000000`).
///
/// Union arm pruning only needs *predicates* folded (`'c' = 's'` → `false` so
/// `EliminateFilter` can drop the arm; `d_year = 2001 + 1` → `d_year = 2002`), so this
/// rule simplifies only `Filter` predicates and `TableScan` filter lists — emptying the
/// scan outright when a folded `false` lands in it — and never touches projection or
/// aggregate expressions, whose casts and literals round-trip through the unparser
/// byte-for-byte.
#[derive(Debug, Default)]
struct FoldConstantFilters;

impl FoldConstantFilters {
    fn new() -> Self {
        Self
    }
}

impl datafusion::optimizer::OptimizerRule for FoldConstantFilters {
    fn name(&self) -> &str {
        "fold_constant_filters"
    }

    fn apply_order(&self) -> Option<datafusion::optimizer::ApplyOrder> {
        Some(datafusion::optimizer::ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: datafusion::logical_expr::LogicalPlan,
        config: &dyn datafusion::optimizer::OptimizerConfig,
    ) -> datafusion::common::Result<
        datafusion::common::tree_node::Transformed<datafusion::logical_expr::LogicalPlan>,
    > {
        use datafusion::common::tree_node::Transformed;
        use datafusion::common::{DFSchema, DFSchemaRef, ScalarValue};
        use datafusion::logical_expr::simplify::SimplifyContext;
        use datafusion::logical_expr::utils::merge_schema;
        use datafusion::logical_expr::{EmptyRelation, Expr, Filter, LogicalPlan};
        use datafusion::optimizer::simplify_expressions::ExprSimplifier;
        use std::sync::Arc;

        // Same simplification context as `SimplifyExpressions`, scoped to predicates:
        // filter-pushdownable providers keep the full inner schema visible (a pushed
        // predicate may reference columns outside the scan's output projection).
        let schema = if !plan.inputs().is_empty() {
            DFSchemaRef::new(merge_schema(&plan.inputs()))
        } else if let LogicalPlan::TableScan(scan) = &plan {
            Arc::new(DFSchema::try_from_qualified_schema(
                scan.table_name.clone(),
                &scan.source.schema(),
            )?)
        } else {
            Arc::new(DFSchema::empty())
        };
        let info = SimplifyContext::builder()
            .with_schema(schema)
            .with_config_options(config.options())
            .with_query_execution_start_time(config.query_execution_start_time())
            .build();
        let simplifier = ExprSimplifier::new(info);

        match plan {
            LogicalPlan::Filter(filter) => {
                let simplified = simplifier.simplify(filter.predicate.clone())?;
                if simplified == filter.predicate {
                    Ok(Transformed::no(LogicalPlan::Filter(filter)))
                } else {
                    Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
                        simplified,
                        filter.input,
                    )?)))
                }
            }
            LogicalPlan::TableScan(mut scan) => {
                let output_schema = scan.projected_schema.clone();
                let mut simplified_filters = Vec::with_capacity(scan.filters.len());
                for expr in scan.filters.iter().cloned() {
                    let simplified = simplifier.simplify(expr)?;
                    if matches!(
                        &simplified,
                        Expr::Literal(ScalarValue::Boolean(Some(false)), _)
                    ) {
                        // A constant-false scan filter yields zero rows — empty the scan
                        // so `PropagateEmptyRelation` can prune the union arm.
                        return Ok(Transformed::yes(LogicalPlan::EmptyRelation(
                            EmptyRelation {
                                produce_one_row: false,
                                schema: output_schema,
                            },
                        )));
                    }
                    simplified_filters.push(simplified);
                }
                if simplified_filters == scan.filters {
                    return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
                }
                scan.filters = simplified_filters;
                Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

/// Default fraction of detected host/cgroup RAM used for the DataFusion spill pool when
/// `OXIDANT_MEMORY_LIMIT_BYTES` is unset. Leaves headroom for OS page cache, Arrow IPC
/// buffers outside the pool, and the shuffle bucket cache.
const DEFAULT_MEMORY_POOL_FRACTION: f64 = 0.7;

/// Resolve the engine's memory-pool budget from the environment and the host.
///
/// | `OXIDANT_MEMORY_LIMIT_BYTES` | Result |
/// |---|---|
/// | positive integer | `Some(n)` — that many bytes |
/// | `0` | `None` — unbounded (explicit opt-out) |
/// | unset / empty / unparseable | auto-size from cgroup v2 → cgroup v1 → host RAM, times
///   `OXIDANT_MEMORY_POOL_FRACTION` (default [`DEFAULT_MEMORY_POOL_FRACTION`]); `None` only
///   if every detection path fails |
///
/// Shared with the shuffle spill threshold ([`SpillStore::from_env`] in oxidant-execution)
/// so an unset memory limit still engages both the FairSpillPool and the bucket cache.
pub fn resolve_memory_pool_bytes() -> Option<usize> {
    match std::env::var("OXIDANT_MEMORY_LIMIT_BYTES") {
        Ok(s) => {
            let s = s.trim();
            if s.is_empty() {
                return auto_size_memory_pool_bytes();
            }
            match s.parse::<usize>() {
                Ok(0) => None,
                Ok(n) => Some(n),
                Err(_) => auto_size_memory_pool_bytes(),
            }
        }
        Err(_) => auto_size_memory_pool_bytes(),
    }
}

/// Detect available RAM and apply `OXIDANT_MEMORY_POOL_FRACTION`. Returns `None` when no
/// usable figure can be read (tests on exotic hosts stay unbounded rather than guessing).
fn auto_size_memory_pool_bytes() -> Option<usize> {
    let total = cgroup_memory_bytes().or_else(host_memory_bytes)?;
    if total == 0 {
        return None;
    }
    let fraction = std::env::var("OXIDANT_MEMORY_POOL_FRACTION")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|f| *f > 0.0 && *f <= 1.0)
        .unwrap_or(DEFAULT_MEMORY_POOL_FRACTION);
    let mut bytes = (total as f64 * fraction) as usize;
    // In-process multi-worker modes (`local-cluster`, benches) create one Engine per
    // worker that would each claim ~0.7× RAM. Divide by the colocated count when set.
    if let Some(n) = colocated_engine_count() {
        bytes /= n;
    }
    // Floor at 64 MiB so a tiny fraction or a mis-reported cgroup cannot create a
    // unusable FairSpillPool that starves every operator on first grow.
    Some(bytes.max(64 * 1024 * 1024))
}

/// Number of Engines expected to share this process's RAM. Set by
/// `oxidant spark server --mode local-cluster` (and optionally by benches).
fn colocated_engine_count() -> Option<usize> {
    std::env::var("OXIDANT_COLOCATED_ENGINES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 1)
}

/// Shuffle-cache threshold sibling of [`resolve_memory_pool_bytes`].
///
/// - Explicit `OXIDANT_MEMORY_LIMIT_BYTES=<n>` → `Some(n)` (legacy: same number as the pool).
/// - `OXIDANT_MEMORY_LIMIT_BYTES=0` → `None` (opt out).
/// - unset (auto-size) → **¼ of the auto-sized pool**, so the FairSpillPool and the in-memory
///   shuffle cache do not both claim ~70% of RAM. Floor 64 MiB.
///
/// Callers that set `OXIDANT_SHUFFLE_SPILL_BYTES` should prefer that and never call this.
pub fn resolve_shuffle_spill_bytes() -> Option<usize> {
    match std::env::var("OXIDANT_MEMORY_LIMIT_BYTES") {
        Ok(s) => {
            let s = s.trim();
            if s.is_empty() {
                return auto_size_shuffle_spill_bytes();
            }
            match s.parse::<usize>() {
                Ok(0) => None,
                Ok(n) => Some(n),
                Err(_) => auto_size_shuffle_spill_bytes(),
            }
        }
        Err(_) => auto_size_shuffle_spill_bytes(),
    }
}

fn auto_size_shuffle_spill_bytes() -> Option<usize> {
    resolve_memory_pool_bytes().map(|n| (n / 4).max(64 * 1024 * 1024))
}

/// Process cgroup memory limit (v2 then v1), preferring **this process's** cgroup over the
/// root. Root `/sys/fs/cgroup/memory.max` is often `max` even when the unit has
/// `MemoryMax=` (systemd / EC2 workers), which would incorrectly fall through to host RAM.
fn cgroup_memory_bytes() -> Option<usize> {
    for path in cgroup_memory_limit_paths() {
        if let Some(n) = read_cgroup_limit(&path) {
            return Some(n);
        }
    }
    None
}

fn cgroup_memory_limit_paths() -> Vec<std::path::PathBuf> {
    let mut paths = Vec::new();
    if let Ok(content) = std::fs::read_to_string("/proc/self/cgroup") {
        for line in content.lines() {
            // cgroup v2: `0::/system.slice/oxidant-worker.service`
            if let Some(rel) = line.strip_prefix("0::") {
                let mut base = std::path::PathBuf::from("/sys/fs/cgroup");
                let rel = rel.trim().trim_start_matches('/');
                if !rel.is_empty() {
                    base.push(rel);
                }
                paths.push(base.join("memory.max"));
                break;
            }
        }
        for line in content.lines() {
            // cgroup v1: `…:memory:/system.slice/oxidant-worker.service`
            if let Some((_, path)) = line.split_once(":memory:") {
                let mut base = std::path::PathBuf::from("/sys/fs/cgroup/memory");
                let rel = path.trim().trim_start_matches('/');
                if !rel.is_empty() {
                    base.push(rel);
                }
                paths.push(base.join("memory.limit_in_bytes"));
                break;
            }
        }
    }
    // Root fallbacks (bare containers / non-systemd).
    paths.push(std::path::PathBuf::from("/sys/fs/cgroup/memory.max"));
    paths.push(std::path::PathBuf::from(
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ));
    paths
}

fn read_cgroup_limit(path: &std::path::Path) -> Option<usize> {
    let raw = std::fs::read_to_string(path).ok()?;
    let s = raw.trim();
    if s == "max" {
        return None;
    }
    let n = s.parse::<u64>().ok()?;
    usable_cgroup_limit(n)
}

/// Reject cgroup "unlimited" sentinels (page-aligned `2^63`-ish values) and limits that
/// exceed physical RAM by more than a small factor — those are not real budgets.
fn usable_cgroup_limit(n: u64) -> Option<usize> {
    // Linux reports "no limit" as a huge page-aligned number near 2^63.
    if n == 0 || n >= (1u64 << 50) {
        return None;
    }
    if let Some(host) = host_memory_bytes() {
        // A cgroup larger than 4× host RAM is almost certainly the unlimited sentinel on a
        // host whose RAM we can see; prefer the host figure.
        if n as usize > host.saturating_mul(4) {
            return None;
        }
    }
    usize::try_from(n).ok()
}

/// Host physical RAM via sysinfo. Returns `None` when the probe reports zero.
fn host_memory_bytes() -> Option<usize> {
    let mut sys = sysinfo::System::new();
    sys.refresh_memory();
    let total = sys.total_memory();
    if total == 0 {
        None
    } else {
        usize::try_from(total).ok()
    }
}

impl Engine {
    /// Create a fresh engine with default session state.
    ///
    /// Memory pool sizing (see [`resolve_memory_pool_bytes`]):
    /// - `OXIDANT_MEMORY_LIMIT_BYTES=<n>` — bounded `FairSpillPool` of `n` bytes.
    /// - `OXIDANT_MEMORY_LIMIT_BYTES=0` — explicit unbounded pool (legacy local/test mode).
    /// - unset — auto-size from cgroup / host RAM × `OXIDANT_MEMORY_POOL_FRACTION` (default
    ///   0.7), so workers cannot OOM by omitting the CloudFormation `MemoryLimitBytes`
    ///   parameter (SF100 TPC-DS Q10: unbounded pool + no shuffle spill → `do_get:
    ///   transport error` after the worker died).
    ///
    /// Phase 1.4 margin-push knobs, each applied only when its env var is set (so the default
    /// behavior is unchanged and the values can be swept on a benchmark box without a rebuild):
    /// - `OXIDANT_TARGET_PARTITIONS` (usize) — scan/aggregation parallelism (default = vCPUs).
    /// - `OXIDANT_BATCH_SIZE` (usize) — vectorized batch size (default 8192).
    /// - `OXIDANT_COALESCE_BATCHES` (bool) — coalesce small batches after filtering.
    /// - `OXIDANT_REPARTITION_AGGREGATIONS` (bool) — repartition before aggregation for parallelism
    ///   (the lever most likely to move the high-card `GROUP BY` queries Q32–Q34).
    /// - `OXIDANT_PREFER_HASH_JOIN` (`auto`|`true`|`false`, default `auto`, KAN-53) — `true`
    ///   forces DataFusion's in-memory hash join session-wide, `false` forces spill-capable
    ///   sort-merge joins for partitioned equijoins; `auto` chooses per join (KAN-142):
    ///   hash joins only when each build side is positively estimated to fit the KAN-25
    ///   budget below, broadcast (`CollectLeft`) when the build fits the
    ///   `OXIDANT_BROADCAST_JOIN_THRESHOLD_BYTES` cap, sort-merge when over budget OR when
    ///   no usable build-side statistics exist (unknown ⇒ safe; without a bounded pool,
    ///   plans keep their hash joins).
    /// - `OXIDANT_HASH_JOIN_MAX_BUILD_FRACTION` (f64 in (0, 1], default 0.25) — with a bounded
    ///   pool, the per-join build-side budget (as a pool fraction) above which `auto` mode
    ///   converts a join to sort-merge (KAN-25/KAN-53/KAN-142; see
    ///   [`Engine::collect_join_guarded`]).
    /// - `OXIDANT_HASH_JOIN_PER_PARTITION_THRESHOLD_BYTES` (usize, default **10 MiB** = Spark
    ///   `autoBroadcastJoinThreshold`) — Spark `canBuildLocalHashMap` gate: effective build
    ///   budget is also capped at `threshold × shuffle_partitions / 2` so SF100 fact⋈fact
    ///   joins prefer spillable sort-merge on EMR-class (`m8g.4xlarge`) workers.
    /// - `OXIDANT_BROADCAST_JOIN_THRESHOLD_BYTES` (usize, default **10 MiB** = Spark
    ///   `autoBroadcastJoinThreshold`; `0` disables, KAN-142) — `auto` mode converts a
    ///   partitioned INNER hash join to broadcast (`CollectLeft`) when its build side is
    ///   PROVABLY at or below this threshold (KAN-146: an `Exact` row upper bound through
    ///   row-preserving wrappers — barrier-measured or footer-exact statistics, never an
    ///   `Inexact` estimate), clamped to the build budget, eliding both sides' shuffle
    ///   repartitions.
    ///
    /// KAN-2 R2 dynamic-filter knobs (hash-join build-side → probe-side scan filters, the
    /// star-shape fast path; the pushdown itself is pinned on session-wide):
    /// - `OXIDANT_DYN_FILTER_INLIST_MAX_DISTINCT` (usize, default **150 = stock DataFusion**) —
    ///   max distinct build-side join keys per build partition pushed into the probe-side
    ///   scan as a transparent `IN (SET)` membership filter. We shipped a raised default
    ///   (100k) briefly: at SF10 the hash-set construction cost (distinct × build
    ///   partitions × joins) made TPC-DS Q4/Q11/Q18/Q21 3–6× SLOWER — stock caps win.
    /// - `OXIDANT_DYN_FILTER_INLIST_MAX_BYTES` (usize, default **128 KiB = stock DataFusion**)
    ///   — max build-side join-key bytes per build partition for the `IN (SET)` strategy.
    ///   Above either cap the membership degrades to an opaque hash-table lookup (batch
    ///   filtering only; min/max bounds still prune — and bounds carry the row-group
    ///   pruning for clustered keys like `ss_sold_date_sk`).
    pub fn new() -> Self {
        Self::new_inner(resolve_memory_pool_bytes())
    }

    /// Engine with an explicit bounded spill pool of `bytes`, independent of the process
    /// environment — identical to [`Engine::new`] with `OXIDANT_MEMORY_LIMIT_BYTES=<bytes>`.
    /// Used by tests and by callers that size the pool programmatically.
    pub fn new_with_memory_limit(bytes: usize) -> Self {
        Self::new_inner(Some(bytes))
    }

    fn new_inner(memory_limit: Option<usize>) -> Self {
        use datafusion::prelude::SessionConfig;

        let mut config = SessionConfig::new();
        if let Some(p) = env_usize("OXIDANT_TARGET_PARTITIONS") {
            config = config.with_target_partitions(p);
        }
        if let Some(n) = env_usize("OXIDANT_BATCH_SIZE") {
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
            // Name DataFusion's own auto-created default catalog/schema `spark_catalog`/`default`
            // (DataFusion's own defaults are `datafusion`/`public`) so they're the *same* catalog
            // and namespace oxidant's own bookkeeping (`Engine::current`, `oxidant_catalog::DEFAULT_
            // CATALOG`/`DEFAULT_NAMESPACE`) already names them — not just cosmetically matching
            // Spark's own naming, but load-bearing: `Engine::run_show`'s SHOW COLUMNS/TABLES/
            // CREATE TABLE handlers build literal `spark_catalog.default.<table>`-qualified SQL
            // and `<default>.<schema>` lookups from that bookkeeping, which would silently resolve
            // to nothing (or the wrong schema) against DataFusion's differently-named defaults.
            opts.catalog.default_catalog = oxidant_catalog::DEFAULT_CATALOG.to_string();
            opts.catalog.default_schema = oxidant_catalog::DEFAULT_NAMESPACE.to_string();
            // Spark's default NULL ordering treats NULL as the smallest value (ASC → NULLS FIRST,
            // DESC → NULLS LAST — Spark's ORDER BY reference:
            // https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-orderby.html), whereas
            // DataFusion defaults to Postgres's `nulls_max` (ASC → NULLS LAST, DESC → NULLS FIRST).
            // Matching Spark via `nulls_min` makes oxidant's implicit ORDER BY (including
            // window-function ORDER BY, where it changes window-frame contents — e.g. a NULL row's
            // RANGE/ROWS neighbours — and computed running aggregates, not just row order) produce
            // Spark's committed output. KAN-52 regression-pins this with the `kan52_*` tests.
            opts.sql_parser.default_null_ordering = "nulls_min".to_string();
            opts.execution.parquet.pushdown_filters = true;
            opts.execution.parquet.reorder_filters = true;
            opts.execution.parquet.binary_as_string = true;
            opts.execution.parquet.schema_force_view_types = true;
            // KAN-153: bounded parquet readahead — how many decoded batches the scan
            // keeps in flight while waiting on downstream. Higher values issue more
            // concurrent row-group ranged GETs (pairs with OXIDANT_S3_RANGE_CONCURRENCY).
            // Default 4 (DataFusion stock is 2); cap via env; `1` disables readahead.
            opts.execution
                .parquet
                .maximum_buffered_record_batches_per_stream =
                env_usize("OXIDANT_PARQUET_PREFETCH_BATCHES").unwrap_or(4);
            // KAN-2 R2: hash-join dynamic filters — the build side publishes a runtime
            // bounds+membership filter over the probe-side join keys, and the
            // probe-side parquet scan absorbs it for row-group/page-index/bloom
            // pruning (proven on the worker star shape by the `dynamic_filter_*`
            // tests). Pin the pushdown ON explicitly: the DataFusion default is
            // already `true`, but an upstream default flip must not silently disable
            // the fact-scan pruning this engine's star joins rely on. The IN-list
            // membership caps stay at stock (150 distinct values / 128 KiB of keys
            // per build partition): raising them (tried 100k / 32 MiB at SF10) made
            // TPC-DS Q4/Q11/Q18/Q21 3–6x slower — hash-set construction cost is
            // distinct × build partitions × joins, and the opaque hash-table lookup
            // + min/max bounds (collected either way) already give the pruning that
            // matters for clustered fact keys. Env overrides remain for tuning;
            // worst-case extra build-side memory is ≈
            // `OXIDANT_DYN_FILTER_INLIST_MAX_BYTES` × target partitions per join.
            opts.optimizer.enable_join_dynamic_filter_pushdown = true;
            opts.optimizer.hash_join_inlist_pushdown_max_distinct_values =
                env_usize("OXIDANT_DYN_FILTER_INLIST_MAX_DISTINCT").unwrap_or(150);
            opts.optimizer.hash_join_inlist_pushdown_max_size =
                env_usize("OXIDANT_DYN_FILTER_INLIST_MAX_BYTES").unwrap_or(128 * 1024);
            if let Some(b) = env_bool("OXIDANT_COALESCE_BATCHES") {
                opts.execution.coalesce_batches = b;
            }
            if let Some(b) = env_bool("OXIDANT_REPARTITION_AGGREGATIONS") {
                opts.optimizer.repartition_aggregations = b;
            }
            match join_preference() {
                JoinPreference::ForceHash => opts.optimizer.prefer_hash_join = true,
                JoinPreference::ForceSortMerge => opts.optimizer.prefer_hash_join = false,
                // Auto (KAN-53 default): leave the planner default (hash) as the
                // under-budget fast path; `collect_join_guarded` / `sql_stream` re-plan
                // per query from build-side statistics.
                JoinPreference::Auto => {}
            }
        }

        // A process+atomic-unique id scopes this engine's managed dirs (warehouse + DF spill).
        let id = WAREHOUSE_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // A dedicated per-engine DataFusion spill dir (sort/aggregate spill files), nested
        // under the OS temp root exactly like DataFusion's default `TempOs` placement but
        // owned by this engine, so the worker watchdog can size it cheaply as a progress
        // signal (KAN-47) and `Drop` can reclaim it like the warehouse dir.
        let spill_dir = std::env::temp_dir().join("oxidant-df-spill").join(format!(
            "{}-{}",
            std::process::id(),
            id
        ));
        // DataFusion's disk manager `create_dir`s the configured dir itself (parents must
        // exist already), so create the full path up front.
        std::fs::create_dir_all(&spill_dir).expect("create DataFusion spill dir");
        let (pool_activity, mut ctx) = {
            use datafusion::execution::memory_pool::{
                FairSpillPool, MemoryPool, UnboundedMemoryPool,
            };
            use datafusion::execution::runtime_env::RuntimeEnvBuilder;
            use std::sync::Arc;
            // Bounded `FairSpillPool` when [`resolve_memory_pool_bytes`] returns `Some`
            // (explicit limit or auto-sized from host/cgroup); `None` (`=0` opt-out) keeps
            // DataFusion's unbounded pool. Either way the pool is wrapped so operator
            // memory activity is timestamped for the no-progress watchdog.
            let inner: Arc<dyn MemoryPool> = match memory_limit {
                Some(bytes) => Arc::new(FairSpillPool::new(bytes)),
                None => Arc::new(UnboundedMemoryPool::default()),
            };
            let (pool, activity) = progress_pool::ProgressMemoryPool::new(inner);
            let env = RuntimeEnvBuilder::new()
                .with_memory_pool(pool)
                .with_temp_file_path(&spill_dir)
                .build_arc()
                .expect("runtime env");
            // `SessionContext::new_with_config_rt` + the engine's physical optimizer rules
            // (stock DataFusion pipeline + the KAN-2 bounded-build-side rule, see
            // [`physical_optimizer_rules`]). KAN-53's query-scoped re-plans inherit them
            // via `SessionStateBuilder::new_from_existing`.
            let state = datafusion::execution::session_state::SessionStateBuilder::new()
                .with_config(config)
                .with_runtime_env(env)
                // `SparkSubstrPlanner` MUST precede the default expr planners:
                // `with_default_features` *extends* this list with the defaults, and the SQL
                // planner consults expr planners in order — the default planner always claims
                // `plan_substring`, so a substring planner appended later (e.g. via
                // `register_expr_planner`) would never run. See `spark_functions::spark_substr`.
                .with_expr_planners(vec![Arc::new(
                    spark_functions::spark_substr::SparkSubstrPlanner,
                )])
                .with_default_features()
                .with_physical_optimizer_rules(physical_optimizer_rules())
                .build();
            (activity, SessionContext::new_with_state(state))
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
        let warehouse = std::env::temp_dir().join("oxidant-warehouse").join(format!(
            "{}-{}",
            std::process::id(),
            id
        ));
        Self {
            ctx: Arc::new(ctx),
            dirs: Arc::new(ManagedDirs {
                warehouse,
                spill_dir,
            }),
            temp_views: Arc::new(Mutex::new(HashSet::new())),
            oxidant_catalogs: Arc::new(Mutex::new(HashMap::new())),
            udf_registry: Arc::new(Mutex::new(udf_registry::UdfRegistry::new())),
            current: Arc::new(Mutex::new((
                oxidant_catalog::DEFAULT_CATALOG.to_string(),
                vec![oxidant_catalog::DEFAULT_NAMESPACE.to_string()],
            ))),
            sessions: Arc::new(Mutex::new(SessionState {
                default_catalog: oxidant_catalog::DEFAULT_CATALOG.to_string(),
                cells: HashMap::new(),
            })),
            created_tables: Arc::new(Mutex::new(HashMap::new())),
            require_lakehouse_snapshot_pins: Arc::new(AtomicBool::new(false)),
            memory_pool_bytes: memory_limit,
            plan_time_smj_reroutes: Arc::new(AtomicU64::new(0)),
            measured_stats_registrations: Arc::new(AtomicU64::new(0)),
            plan_cache_id: id,
            catalog_version: Arc::new(AtomicU64::new(0)),
            pool_activity,
            table_bytes_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Milliseconds since engine construction of the last memory-pool `grow`/`shrink`/
    /// `try_grow` — a worker-wide operator-activity signal for the stage no-progress
    /// watchdog (KAN-47). Any live operator work touches the pool; a parked query goes silent.
    pub fn pool_activity_ms(&self) -> u64 {
        self.pool_activity.last_activity_ms()
    }

    /// Total bytes currently on disk in this engine's DataFusion spill directory —
    /// a second worker-wide progress signal (frozen under the spill-pool deadlock class).
    /// Best-effort: a missing/unreadable dir counts as 0.
    pub fn spill_dir_bytes(&self) -> u64 {
        dir_bytes(&self.dirs.spill_dir)
    }

    /// Reserve `bytes` against this engine's DataFusion memory pool for Arrow data the caller
    /// holds OUTSIDE any DataFusion operator, returning the reservation to hold for its
    /// lifetime (dropping it releases the bytes).
    ///
    /// The worker's shuffle-input `MemTable`s are the motivating case: a consumer task pulls
    /// every bucket it was assigned into RSS and registers it, and until this existed those
    /// bytes were invisible to the pool. The consequence was not a slow query — it was that
    /// the pool believed it had headroom it did not have, so downstream operators never
    /// spilled and the kernel killed the worker outright (no `ResourcesExhausted`, nothing to
    /// act on). Two sharded facts joined together reached 6x the configured pool in ~5 s.
    ///
    /// Registered with `can_spill = false` deliberately: the caller cannot hand these bytes
    /// back on demand, so under a `FairSpillPool` they must count against the non-spillable
    /// share and squeeze the spillable operators rather than the reverse.
    ///
    /// With no bounded pool (`OXIDANT_MEMORY_LIMIT_BYTES=0`) this is a no-op reservation that
    /// always succeeds, matching the unbounded pool's contract.
    pub fn reserve_external_bytes(
        &self,
        label: &str,
        bytes: usize,
    ) -> Result<datafusion::execution::memory_pool::MemoryReservation> {
        use datafusion::execution::memory_pool::MemoryConsumer;

        let pool = self.ctx.task_ctx().runtime_env().memory_pool.clone();
        let reservation = MemoryConsumer::new(label)
            .with_can_spill(false)
            .register(&pool);
        reservation.try_grow(bytes).map_err(|e| {
            Error::Execution(format!(
                "reserve {bytes} bytes for `{label}`: {e} (pool {}; this is shuffle-input data \
                 that cannot be spilled — lower OXIDANT_WORKER_TASK_SLOTS, raise the worker \
                 memory limit, or replicate one side of the join)",
                self.memory_pool_bytes
                    .map_or_else(|| "unbounded".to_string(), |b| format!("{b} bytes")),
            ))
        })?;
        Ok(reservation)
    }

    /// Import UDF definitions from JSON (distributed worker sync).
    pub fn register_udfs_json(&self, json: &str) -> Result<()> {
        let mut reg = self.udf_registry.lock().unwrap();
        reg.import_json(json)?;
        reg.apply_to_context(&self.ctx)?;
        // UDF definitions are resolved into plan templates; a re-sync invalidates them.
        self.note_catalog_change("<udfs>");
        Ok(())
    }

    /// Export registered UDFs for broadcast to workers.
    pub fn export_udfs_json(&self) -> String {
        self.udf_registry.lock().unwrap().export_json()
    }

    /// Shared UDF registry handle (Connect registration, worker sync).
    pub fn udf_registry(&self) -> udf_registry::SharedUdfRegistry {
        Arc::clone(&self.udf_registry)
    }

    /// Run a SQL string and collect the result as Arrow record batches.
    ///
    /// Errors are mapped onto the Oxidant error model: a planning/analysis failure becomes
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
        // `SHOW TABLES IN <cat>[.<db>]`) are served straight from the registered oxidant catalogs —
        // DataFusion's parser rejects most of these shapes and its bridge can only see
        // already-materialized listings, so we answer them here before any planning. `parse_show`
        // returns `None` for anything that isn't one of these forms, leaving every other statement
        // (including bare `SHOW TABLES`) to flow through unchanged.
        if let Some(show) = parse_show(query) {
            return self.run_show(&show).await;
        }
        // `DESCRIBE`/`DESC` (table columns, `QUERY`, `DATABASE`/`SCHEMA`, `CATALOG`, `FUNCTION`)
        // — same interception style as `SHOW` above, and for the same reason: DataFusion's native
        // `DESCRIBE` only understands a bare table/query and returns its own column shape
        // (`column_name, data_type, is_nullable`) instead of Spark's (`col_name, data_type,
        // comment`), with none of `EXTENDED`/`DATABASE`/`CATALOG`/`FUNCTION` support. `parse_describe`
        // returns `None` for anything that isn't one of these forms, leaving every other statement
        // untouched.
        if let Some(describe) = parse_describe(query) {
            return self.run_describe(&describe).await;
        }
        // `USE [CATALOG] <name>` / `USE <catalog>.<namespace>` sets the session's current
        // catalog/namespace, consulted by later SHOW/DESCRIBE work for defaulting unqualified
        // names. Handled here (rather than DataFusion's planner) since oxidant's current-catalog
        // state lives on `Engine`, not in DataFusion's session config. `parse_use` returns `None`
        // for anything that isn't one of these forms.
        if let Some(use_stmt) = parse_use(query) {
            return self.run_use(&use_stmt).await;
        }
        // SQL user-defined functions: `CREATE [OR REPLACE] FUNCTION … RETURN …`
        if let Some(def) = udf_registry::try_create_function(query) {
            let mut reg = self.udf_registry.lock().unwrap();
            reg.register_sql_fn(def.clone());
            reg.apply_to_context(&self.ctx)?;
            return Ok(vec![]);
        }
        // SPARK-29628 (`INVALID_TEMP_OBJ_REFERENCE`): a *persistent* `CREATE VIEW` may not reference
        // a session-temporary view. DataFusion has no temp/permanent distinction (we strip the
        // `TEMPORARY` keyword in `normalize_spark_sql` so it plans), so it would silently accept the
        // body and oxidant would drop Spark's analysis error. Detect the offending shape up front and
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
        // Spark's `CREATE TABLE <catalog>.<db>.<t> USING <fmt> [LOCATION …] [PARTITIONED BY …]
        // AS SELECT …`. DataFusion's parser rejects `USING` outright, and its CTAS planning has no
        // seam for a storage format, an explicit location, or partition columns — so the storage
        // clauses are parsed off here, staged for `catalog_bridge`'s `register_table`, and the
        // stripped statement is planned normally. Only for names that actually target a registered
        // external catalog; everything else falls through to the local-warehouse lowerings below.
        if let Some(ext) = spark_create_table::lower_external_ctas(query)
            .filter(|e| self.name_targets_external_catalog(&e.name))
        {
            self.run_external_ctas(&ext).await?;
            return Ok(vec![]);
        }
        // Faithful lowering of Spark's `CREATE TABLE … USING <fmt>` to a real, format-backed
        // `CREATE EXTERNAL TABLE` (genuine durable storage — NOT the forbidden MemTable shim). On
        // success the statement produces no result set, matching Spark's `struct<>`. If the lowered
        // DDL fails to plan/execute (exotic column types, etc.) we fall through to the normal path,
        // which reproduces the original parse error — so an unsupported CREATE stays in exactly the
        // bucket it failed in before (never a regression).
        if let Some(low) = spark_create_table::lower_create_table_using(query, &self.dirs.warehouse)
            .filter(|l| !self.name_targets_external_catalog(&l.name))
        {
            if self.run_create_external(&low).await.is_ok() {
                self.created_tables.lock().unwrap().insert(
                    created_table_key(&low.name),
                    CreatedTableMeta {
                        format: low.format.clone(),
                        comment: low.comment.clone(),
                        properties: low.properties.clone(),
                        partition_columns: low.partition_columns.clone(),
                    },
                );
                return Ok(vec![]);
            }
        } else if let Some(ctas) =
            spark_create_table::lower_create_table_ctas(query, &self.dirs.warehouse)
                .filter(|c| !self.name_targets_external_catalog(&c.name))
        {
            if self.run_create_table_ctas(&ctas).await.is_ok() {
                self.created_tables.lock().unwrap().insert(
                    created_table_key(&ctas.name),
                    CreatedTableMeta {
                        format: ctas.fmt.clone(),
                        comment: ctas.comment.clone(),
                        properties: ctas.properties.clone(),
                        partition_columns: Vec::new(),
                    },
                );
                return Ok(vec![]);
            }
        } else if spark_create_table::is_insert(query) {
            // Spark's `spark.sql("INSERT …")` returns an empty DataFrame; DataFusion returns a
            // one-row `count`. Execute the write for its side effects, then drop the count row so
            // the result renders as Spark's `struct<>` + empty.
            let statement = self.quote_insert_catalog_segment(query);
            let df = self.plan_spark(statement.as_ref()).await?;
            df.collect()
                .await
                .map_err(|e| Error::Execution(e.to_string()))?;
            // A catalog-backed table's provider embeds the file list it was resolved with, so an
            // INSERT that just added files would otherwise be invisible to the next SELECT in this
            // session — it would be served from the pre-insert snapshot. Local-warehouse
            // `ListingTable`s re-list on every scan and are unaffected; the eviction is a no-op
            // for them.
            if let Some(target) = spark_create_table::insert_target(query) {
                self.refresh_table(&target).await?;
            }
            return Ok(vec![]);
        }
        let df = self.plan_spark(query).await?;
        let batches = self.collect_join_guarded(df).await?;
        // The view planned/created successfully — update the temp-view registry. A new temporary
        // view is recorded; a persistent view with the same name removes any prior temp entry
        // (DataFusion keeps a single namespace, so the persistent definition now shadows it).
        if let Some(cv) = create_view {
            let mut temp = self.temp_views.lock().unwrap();
            if cv.temporary {
                temp.insert(cv.name.clone());
            } else {
                temp.remove(&cv.name);
            }
            self.note_catalog_change(&cv.name);
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
        self.note_catalog_change(&low.name);
        Ok(())
    }

    /// Backtick the leading segment of an `INSERT` target that names a registered external
    /// catalog, leaving the statement otherwise byte-identical.
    ///
    /// sqlparser consumes `LOCAL` as Hive's `INSERT OVERWRITE LOCAL DIRECTORY` keyword, so
    /// `INSERT INTO local.live.t` fails at parse for a catalog named `local` — which is the name
    /// [`docs/config.md`]'s example uses. Quoting makes it an identifier again, and the
    /// *registered* spelling is substituted so quoting (which makes the identifier
    /// case-sensitive) cannot change which catalog the statement names. Only the leading segment
    /// is quoted: quoting the namespace or table would make those case-sensitive too, and a name
    /// stored lowercase but written uppercase would stop resolving.
    fn quote_insert_catalog_segment<'a>(&self, query: &'a str) -> std::borrow::Cow<'a, str> {
        let Some((offset, segment)) = spark_create_table::insert_target_catalog(query) else {
            return std::borrow::Cow::Borrowed(query);
        };
        let registered = self
            .oxidant_catalogs
            .lock()
            .expect("oxidant_catalogs poisoned")
            .keys()
            .find(|k| k.eq_ignore_ascii_case(&segment))
            .cloned();
        match registered {
            Some(name) => std::borrow::Cow::Owned(format!(
                "{}`{name}`{}",
                &query[..offset],
                &query[offset + segment.len()..]
            )),
            None => std::borrow::Cow::Borrowed(query),
        }
    }

    /// Run a `CREATE TABLE <catalog>.<db>.<t> USING <fmt> … AS SELECT …` against an external
    /// catalog: stage the storage attributes the DDL carries, then let DataFusion plan the
    /// stripped statement, whose `register_table` call lands in
    /// [`catalog_bridge::register_table_async`] and reads them back.
    ///
    /// Clauses that would be silently lost are refused rather than dropped. `OPTIONS(…)` decides
    /// how a CSV table is even read (`header`, `delimiter`); `COMMENT` / `TBLPROPERTIES` have
    /// nowhere to go in `CatalogProvider::create_table`. Accepting and discarding any of them
    /// would produce a table that does not match its own DDL.
    async fn run_external_ctas(&self, ext: &spark_create_table::ExternalCtas) -> Result<()> {
        use oxidant_catalog::TableFormat;

        for (clause, present) in [
            ("OPTIONS", !ext.clauses.options.is_empty()),
            ("COMMENT", ext.clauses.comment.is_some()),
            ("TBLPROPERTIES", !ext.clauses.properties.is_empty()),
        ] {
            if present {
                return Err(Error::Unsupported(format!(
                    "`{clause}` is not yet carried through to an external catalog's \
                     `CREATE TABLE … AS SELECT`; remove it or create the table in the catalog first"
                )));
            }
        }
        let format = TableFormat::from_provider(&ext.format).ok_or_else(|| {
            Error::Unsupported(format!("unsupported table format `{}`", ext.format))
        })?;

        // `catalog.namespace….table`: the leading segment is DataFusion's own catalog routing, and
        // the rest is exactly what the schema provider will see when it calls `register_table`.
        let segments = split_name_segments(&ext.name);
        let unquote = |s: &&str| s.trim_matches('`').to_string();
        let table = segments
            .last()
            .map(|s| s.trim_matches('`').to_string())
            .ok_or_else(|| Error::Plan(format!("cannot parse table name `{}`", ext.name)))?;
        let namespace: Vec<String> = segments[1..segments.len() - 1]
            .iter()
            .map(unquote)
            .collect();

        // Staged on the target catalog's own provider (per session), so two catalogs — or two
        // sessions — running a CTAS for the same `namespace.table` cannot take each other's
        // attributes. This is the same downcast `refresh_table` uses to reach the bridge.
        let catalog_name = segments[0].trim_matches('`');
        let provider = self
            .ctx
            .catalog(catalog_name)
            .ok_or_else(|| Error::Plan(format!("catalog `{catalog_name}` is not registered")))?;
        let any: &dyn std::any::Any = provider.as_ref();
        let bridge = any
            .downcast_ref::<catalog_bridge::OxidantCatalogProvider>()
            .ok_or_else(|| {
                Error::Unsupported(format!(
                    "`CREATE TABLE … USING <fmt> AS SELECT` is not supported for catalog \
                     `{catalog_name}`"
                ))
            })?;

        bridge.set_pending_ctas_attributes(
            &namespace,
            &table,
            catalog_bridge::CtasAttributes {
                format: Some(format),
                location: ext.clauses.location.clone(),
                partition_columns: ext.clauses.partition_columns.clone(),
            },
        );
        let executed = match self.plan_spark(&ext.ddl).await {
            Ok(df) => df
                .collect()
                .await
                .map(|_| ())
                .map_err(|e| Error::Execution(e.to_string())),
            Err(e) => Err(e),
        };
        // Whether or not the statement ever reached `register_table`, nothing may be left staged —
        // a failed `USING delta` must not hand its format to the next plain CTAS of this name.
        bridge.clear_pending_ctas_attributes(&namespace, &table);
        executed?;
        self.note_catalog_change(&ext.name);
        Ok(())
    }

    /// CTAS: execute SELECT, write result as format files, then CREATE EXTERNAL TABLE.
    async fn run_create_table_ctas(&self, ctas: &spark_create_table::LoweredCtas) -> Result<()> {
        use datafusion::parquet::arrow::ArrowWriter;
        use futures::StreamExt;

        std::fs::create_dir_all(&ctas.table_dir).map_err(|e| Error::Execution(e.to_string()))?;
        // Stream the SELECT straight to the output file instead of collecting the whole result into
        // driver memory first. A large `CREATE TABLE AS SELECT * FROM bigtable` otherwise buffers
        // the entire source table in RAM (via `df.collect()`) and OOMs the driver; streaming holds
        // at most one record batch at a time. The stream's own schema drives the writer so each
        // batch matches exactly (as `batches[0].schema()` did before).
        let df = self.plan_spark(&ctas.select_sql).await?;
        let mut stream = df
            .execute_stream()
            .await
            .map_err(|e| Error::Execution(e.to_string()))?;

        // The writer has to match the format the DDL is about to declare. It used to be an
        // `ArrowWriter` unconditionally while only the *extension* followed `ctas.fmt`, so
        // `CREATE TABLE t USING csv AS SELECT ...` wrote Parquet bytes into `part-00000.csv`
        // and then registered the table as `STORED AS CSV`. Nothing failed at write time; the
        // table simply could not be read back.
        let ext = match ctas.fmt.as_str() {
            "json" => "json",
            "csv" => "csv",
            "parquet" => "parquet",
            // `orc` parses as a Spark format but Oxidant can neither write nor read it, and
            // silently substituting Parquet under an `.orc` name is how this bug started.
            other => {
                return Err(Error::Unsupported(format!(
                    "CREATE TABLE ... USING {other} AS SELECT is not supported \
                     (writable formats: parquet, csv, json)"
                )))
            }
        };
        let file = ctas.table_dir.join(format!("part-00000.{ext}"));
        let f = std::fs::File::create(&file).map_err(|e| Error::Execution(e.to_string()))?;
        // Each arm streams: at most one record batch is held at a time, so a
        // `CREATE TABLE ... AS SELECT * FROM bigtable` does not buffer the source in driver RAM.
        match ctas.fmt.as_str() {
            "csv" => {
                let mut writer = arrow::csv::Writer::new(f);
                while let Some(batch) = stream.next().await {
                    let batch = batch.map_err(|e| Error::Execution(e.to_string()))?;
                    writer
                        .write(&batch)
                        .map_err(|e| Error::Execution(e.to_string()))?;
                }
                // The JSON arm has `finish()` and the Parquet arm has `close()`; the CSV writer
                // has neither, and its inner `csv::Writer` flushes on drop with the result
                // *discarded*. A failure on that last flush — disk full, closed pipe — would
                // otherwise leave a truncated file while this goes on to register the table and
                // report success. Take the file back and flush it where the error can be seen.
                let mut f = writer.into_inner();
                std::io::Write::flush(&mut f)
                    .map_err(|e| Error::Execution(format!("flush `{}`: {e}", file.display())))?;
            }
            "json" => {
                // Newline-delimited, which is what DataFusion's JSON reader expects — a single
                // JSON array would write cleanly and then fail to scan.
                let mut writer = arrow::json::LineDelimitedWriter::new(f);
                while let Some(batch) = stream.next().await {
                    let batch = batch.map_err(|e| Error::Execution(e.to_string()))?;
                    writer
                        .write(&batch)
                        .map_err(|e| Error::Execution(e.to_string()))?;
                }
                writer
                    .finish()
                    .map_err(|e| Error::Execution(e.to_string()))?;
            }
            _ => {
                let mut writer = ArrowWriter::try_new(f, stream.schema(), None)
                    .map_err(|e| Error::Execution(e.to_string()))?;
                while let Some(batch) = stream.next().await {
                    let batch = batch.map_err(|e| Error::Execution(e.to_string()))?;
                    writer
                        .write(&batch)
                        .map_err(|e| Error::Execution(e.to_string()))?;
                }
                writer
                    .close()
                    .map_err(|e| Error::Execution(e.to_string()))?;
            }
        }

        let ddl = normalize_spark_sql(&ctas.ddl);
        self.ctx
            .sql(ddl.as_ref())
            .await
            .map_err(|e| Error::Plan(e.to_string()))?
            .collect()
            .await
            .map_err(|e| Error::Execution(e.to_string()))?;
        self.note_catalog_change(&ctas.name);
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
        // instead coerces them (Date32 shares Int32's layout) and silently yields a value, so oxidant is
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
        // return an error so oxidant matches Spark's rejection (error-parity). The pre-check keeps the
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
    /// Used by Spark Connect `AnalyzePlan(Explain)` for a `spark.sql(...)` command, and by the
    /// distributed stage planner. Applies the same [`normalize_spark_sql`] front-end as
    /// [`Engine::sql`] so ANSI interval leading precision (`day (3)`) and other Spark spellings
    /// plan consistently on both paths.
    pub async fn logical_plan(&self, query: &str) -> Result<datafusion::logical_expr::LogicalPlan> {
        let query = normalize_spark_sql(query);
        self.create_logical_plan_spark(query.as_ref()).await
    }

    /// Plan one driver query while capturing the exact Delta/Iceberg identities it resolved.
    pub async fn logical_plan_with_lakehouse_snapshots(
        &self,
        query: &str,
    ) -> Result<(datafusion::logical_expr::LogicalPlan, String)> {
        catalog_bridge::capture_lakehouse_snapshots(self.logical_plan(query)).await
    }

    /// Classify a driver-side plan for [`Engine::optimize_logical_plan`] (see
    /// [`PreSplitRewrite`] for the classes). `Skip` wins over `UnionExtended` when a plan
    /// contains both a union and a skipped class (e.g. a window over a union).
    fn pre_split_rewrite_class(lp: &datafusion::logical_expr::LogicalPlan) -> PreSplitRewrite {
        use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
        use datafusion::logical_expr::{Expr, LogicalPlan};

        let mut class = PreSplitRewrite::Standard;
        let _ = lp.apply(|node| {
            match node {
                LogicalPlan::Window(_) => {
                    class = PreSplitRewrite::Skip;
                    return Ok(TreeNodeRecursion::Stop);
                }
                LogicalPlan::Union(_) => {
                    class = PreSplitRewrite::UnionExtended;
                    // Keep walking: a Window or subquery expression elsewhere in the plan
                    // still forces Skip.
                }
                LogicalPlan::SubqueryAlias(alias) => {
                    // Walk past passthrough nodes: a bare TableScan at the end means this
                    // SubqueryAlias is an SQL table alias, not a relation-valued subquery.
                    let mut inner = alias.input.as_ref();
                    loop {
                        match inner {
                            LogicalPlan::Filter(f) => inner = f.input.as_ref(),
                            LogicalPlan::Projection(p) => inner = p.input.as_ref(),
                            LogicalPlan::TableScan(_) => {
                                class = PreSplitRewrite::Skip;
                                return Ok(TreeNodeRecursion::Stop);
                            }
                            _ => break,
                        }
                    }
                }
                _ => {
                    for e in node.expressions() {
                        let has_subquery = e
                            .exists(|sub| {
                                Ok(matches!(
                                    sub,
                                    Expr::Exists(_) | Expr::InSubquery(_) | Expr::ScalarSubquery(_)
                                ))
                            })
                            .unwrap_or(false);
                        if has_subquery {
                            class = PreSplitRewrite::Skip;
                            return Ok(TreeNodeRecursion::Stop);
                        }
                    }
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });
        class
    }

    /// Apply optimizer rules to a driver-side plan so the distributed stage splitter sees
    /// predicates where they belong (below aggregates and join sides) rather than where the
    /// SQL text put them. Without this the splitter unparses the *unoptimized* plan and no
    /// pushdown can cross a stage boundary — e.g. TPC-DS Q78's `ss_sold_year=2000` landed in
    /// the final stage while leaf stages scanned and grouped every year of all three fact
    /// tables (6.3s → 1.5s at SF10 once pushed).
    ///
    /// The rule set is class-dependent ([`PreSplitRewrite`]): the standard pair
    /// (`extract_equijoin_predicate` + `push_down_filter`) for ordinary plans, plus
    /// filter-scoped constant folding ([`FoldConstantFilters`]) and `eliminate_filter` /
    /// `propagate_empty_relation` / `optimize_unions` for union plans so pushed
    /// predicates prune contradictory arms (TPC-DS Q4: six shared `year_total` union
    /// occurrences collapse to single-fact slices instead of defeating stage CSE — the
    /// v12 66-stage do_get failure).
    ///
    /// Two deliberate restrictions versus running `Optimizer::optimize`:
    /// - Rule subset: rules like `eliminate_distinct` / `replace_distinct_aggregate`
    ///   rewrite node *types* (TPC-DS Q37's aggregate-free `GROUP BY` becomes a `Distinct`)
    ///   into shapes the splitter does not yet recognize, which would de-distribute those
    ///   queries to a single-node Forward stage. The chosen rules only normalize equijoin
    ///   keys, move/merge/fold `Filter` nodes, and prune empty union arms, leaving every
    ///   surviving plan's node-type vocabulary intact.
    /// - Plan tree only (no expr-subquery descent): the stock driver applies rules via
    ///   `rewrite_with_subqueries`, which also rewrites plans inside `EXISTS` / scalar
    ///   subquery *expressions* (pushing their predicates into the inner `TableScan`).
    ///   The splitter's subquery handlers pattern-match the unoptimized subquery shape —
    ///   mutating it produces stage SQL with dangling table qualifiers (`dim.d_key`
    ///   instead of `d.d_key`). `TreeNode::rewrite` traverses only the plan tree.
    pub fn optimize_logical_plan(
        &self,
        lp: datafusion::logical_expr::LogicalPlan,
    ) -> Result<datafusion::logical_expr::LogicalPlan> {
        use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
        use datafusion::logical_expr::LogicalPlan;
        use datafusion::optimizer::{
            eliminate_filter::EliminateFilter,
            extract_equijoin_predicate::ExtractEquijoinPredicate, optimize_unions::OptimizeUnions,
            propagate_empty_relation::PropagateEmptyRelation, push_down_filter::PushDownFilter,
            ApplyOrder, OptimizerConfig, OptimizerRule,
        };
        use std::sync::Arc;

        // Shape gate: leave plan classes the splitter cannot re-render after rewriting
        // exactly as they are (today's behavior), rather than emitting broken stage SQL.
        let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> =
            match Self::pre_split_rewrite_class(&lp) {
                PreSplitRewrite::Skip => return Ok(lp),
                PreSplitRewrite::Standard => vec![
                    Arc::new(ExtractEquijoinPredicate::new()),
                    Arc::new(PushDownFilter::new()),
                    // NOT here: `OptimizeProjections`. Column pruning is badly wanted — without
                    // it `TableScan.projection` is never set, `projected_schema` stays the FULL
                    // table schema, and every leaf shuffle stage ships every column of a sharded
                    // fact (TPC-DS q37 references ONE column of `catalog_sales` and shuffles all
                    // ~34, which is what makes the producer buffer whole fact shards and get the
                    // worker OOM-killed). But adding the rule here makes the splitter DECLINE
                    // q37's two-sharded shape entirely and fall back to a single Forward stage —
                    // `tests/auto_broadcast_row_multiple::q37_shape_two_sharded_no_forward_fallback`
                    // catches it, and a non-distributed fallback is strictly worse than a wide
                    // shuffle. Pruning has to be taught to the splitter (or applied at the leaf
                    // in `join_chain::leaf_stage_sql` from the chain's own column usage) before
                    // the rule can go in. Measured, not assumed: that was the only failure in the
                    // whole oxidant-execution suite.
                ],
                PreSplitRewrite::UnionExtended => vec![
                    // Same pushdown pair first — outer predicates reach the union arms…
                    Arc::new(ExtractEquijoinPredicate::new()),
                    Arc::new(PushDownFilter::new()),
                    // …then fold constant *predicates* only (`'c' = 's'` → `false`;
                    // `d_year = 2001 + 1` → `d_year = 2002`) — never projection
                    // expressions, whose decimal casts must survive the unparser
                    // byte-for-byte (TPC-DS Q5) — collapse the false arms to
                    // `EmptyRelation`, drop them from the union, and flatten
                    // single-input / nested unions.
                    Arc::new(FoldConstantFilters::new()),
                    Arc::new(EliminateFilter::new()),
                    Arc::new(PropagateEmptyRelation::new()),
                    Arc::new(OptimizeUnions::new()),
                ],
            };

        /// Mirror of datafusion-optimizer's private driver `Rewriter`, dispatched through
        /// `TreeNode::rewrite` (plan tree only) rather than `rewrite_with_subqueries`.
        struct PlanOnlyRewriter<'a> {
            apply_order: ApplyOrder,
            rule: &'a dyn OptimizerRule,
            config: &'a dyn OptimizerConfig,
        }
        impl TreeNodeRewriter for PlanOnlyRewriter<'_> {
            type Node = LogicalPlan;
            fn f_down(
                &mut self,
                node: LogicalPlan,
            ) -> datafusion::common::Result<Transformed<LogicalPlan>> {
                if self.apply_order == ApplyOrder::TopDown {
                    self.rule.rewrite(node, self.config)
                } else {
                    Ok(Transformed::no(node))
                }
            }
            fn f_up(
                &mut self,
                node: LogicalPlan,
            ) -> datafusion::common::Result<Transformed<LogicalPlan>> {
                if self.apply_order == ApplyOrder::BottomUp {
                    self.rule.rewrite(node, self.config)
                } else {
                    Ok(Transformed::no(node))
                }
            }
        }

        let state = self.ctx.state();
        let config: &dyn OptimizerConfig = &state;
        let mut plan = lp;
        for rule in &rules {
            plan = match rule.apply_order() {
                Some(apply_order) => {
                    let mut rewriter = PlanOnlyRewriter {
                        apply_order,
                        rule: rule.as_ref(),
                        config,
                    };
                    plan.rewrite(&mut rewriter)
                        .map_err(|e| Error::Plan(e.to_string()))?
                        .data
                }
                None => {
                    rule.rewrite(plan, config)
                        .map_err(|e| Error::Plan(e.to_string()))?
                        .data
                }
            };
        }
        Ok(plan)
    }

    /// Run an arbitrary planning future while capturing the exact Delta/Iceberg identities
    /// it resolves — the generic counterpart of [`Engine::logical_plan_with_lakehouse_snapshots`]
    /// for callers that build a logical plan without SQL (the Spark Connect DataFrame path).
    pub async fn capture_lakehouse_snapshots<F, T>(&self, future: F) -> Result<(T, String)>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        catalog_bridge::capture_lakehouse_snapshots(future).await
    }

    /// Execute worker SQL under snapshot pins serialized by the driver.
    pub async fn sql_with_lakehouse_snapshots(
        &self,
        query: &str,
        pins_json: &str,
    ) -> Result<Vec<RecordBatch>> {
        catalog_bridge::with_lakehouse_snapshots(pins_json, self.sql(query)).await
    }

    /// Resolve worker output schema under the same snapshot pins as execution.
    pub async fn schema_with_lakehouse_snapshots(
        &self,
        query: &str,
        pins_json: &str,
    ) -> Result<arrow::datatypes::SchemaRef> {
        catalog_bridge::with_lakehouse_snapshots(pins_json, self.schema(query)).await
    }

    /// Mark this engine as a distributed worker where an unpinned lakehouse read is invalid.
    pub fn require_lakehouse_snapshot_pins(&self) {
        self.require_lakehouse_snapshot_pins
            .store(true, Ordering::Relaxed);
    }

    /// Run `future` recording whether it resolved any Lake Formation-governed table, so the driver
    /// can stamp that requirement onto the stage tickets it dispatches.
    pub async fn capture_lakeformation_enforcement<F, T>(
        &self,
        future: F,
    ) -> Result<(T, bool, String)>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        catalog_bridge::capture_lakeformation_enforcement(future).await
    }

    /// The Lake Formation principal this engine enforces as, if any catalog is configured with an
    /// authorizer. Used to scope paths that carry no driver context of their own.
    pub fn lakeformation_principal(&self) -> Option<String> {
        self.oxidant_catalogs
            .lock()
            .expect("oxidant_catalogs poisoned")
            .values()
            .find_map(|c| c.authorizer().map(|a| a.principal().to_string()))
    }

    /// Execute `future` under the driver's Lake Formation requirement (worker side). When
    /// `required` is set, resolving a table through a catalog with no authorizer is an error
    /// rather than an unfiltered read.
    pub async fn with_lakeformation_required<F, T>(
        &self,
        required: bool,
        principal: String,
        future: F,
    ) -> T
    where
        F: std::future::Future<Output = T>,
    {
        catalog_bridge::with_lakeformation_required(required, principal, future).await
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

    /// The build-side budget in bytes for the KAN-25 hash-join memory guard.
    ///
    /// Effective budget = `min(pool × fraction, spark_shj_cap)` where:
    /// - `fraction` is `OXIDANT_HASH_JOIN_MAX_BUILD_FRACTION` (default 0.25)
    /// - `spark_shj_cap` is Spark's `canBuildLocalHashMap` rule:
    ///   `(OXIDANT_HASH_JOIN_PER_PARTITION_THRESHOLD_BYTES × shuffle_partitions)
    ///    / HASH_JOIN_BUILD_OVERHEAD_FACTOR`
    ///
    /// Matching Spark's `preferSortMergeJoin=true` default: large equi-joins take
    /// spill-capable sort-merge; HashJoin is only admitted when the build fits a
    /// *per-partition* working set, not when it merely fits a quarter of the whole pool
    /// (the old rule that still OOM'd SF100 on `m8g.4xlarge`).
    ///
    /// `None` when the engine runs unbounded — the guard is then fully off.
    fn hash_join_build_budget(&self) -> Option<usize> {
        let pool = self.memory_pool_bytes?;
        let fraction = std::env::var("OXIDANT_HASH_JOIN_MAX_BUILD_FRACTION")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|f| *f > 0.0 && *f <= 1.0)
            .unwrap_or(DEFAULT_HASH_JOIN_MAX_BUILD_FRACTION);
        let pool_cap = (pool as f64 * fraction) as usize;
        let spark_cap = spark_aligned_hash_join_build_cap();
        Some(pool_cap.min(spark_cap))
    }

    /// Whether the KAN-25 sort-merge fallback is explicitly allowed (env
    /// `OXIDANT_SORT_MERGE_FALLBACK`, default **false**, KAN-45). DataFusion 54.0's
    /// sort-merge/external-sort pipeline could deadlock under a bounded `FairSpillPool` at
    /// scale: a spilling operator stalls waiting for pool memory while its consumer stops
    /// draining a `RepartitionExec` channel, and the whole query parks with zero CPU/IO
    /// until the stage timeout kills it (observed live at TPC-DS SF10 on Q11/Q72/Q93;
    /// upstream: delta-io/delta-rs#4614). The 54.1.0 upgrade fixed that deadlock, so the
    /// KAN-53 `auto` join selection re-plans with sort-merge by default
    /// ([`Engine::smj_replan_allowed`]); this knob remains as an opt-in that also allows
    /// the re-plan when `OXIDANT_PREFER_HASH_JOIN` forces a strategy.
    fn smj_fallback_enabled() -> bool {
        std::env::var("OXIDANT_SORT_MERGE_FALLBACK")
            .ok()
            .as_deref()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    /// Whether a query may be re-planned with sort-merge joins (KAN-25/KAN-53): explicitly
    /// via the KAN-45 [`Engine::smj_fallback_enabled`] opt-in, or implicitly under the
    /// KAN-53 default `auto` join selection — the DataFusion 54.1.0 upgrade fixed the
    /// bounded-pool sort-merge deadlock the KAN-45 default guarded against. Forced
    /// `OXIDANT_PREFER_HASH_JOIN=true|false` without the opt-in keeps the KAN-45 fail-fast
    /// behavior.
    fn smj_replan_allowed() -> bool {
        Self::smj_fallback_enabled() || join_preference() == JoinPreference::Auto
    }

    /// Whether `plan` still needs the whole-plan sort-merge reroute: a bounded-pool
    /// build-side estimate over the KAN-25 budget ([`Engine::hash_join_build_budget`]) —
    /// or NO usable estimate at all — with the re-plan allowed
    /// ([`Engine::smj_replan_allowed`]). Shared by [`Engine::collect_join_guarded`] and
    /// [`Engine::sql_stream`] so the auto selection is one predicate.
    ///
    /// Unknown ⇒ sort-merge, not hash: a hash build whose size the planner cannot see is
    /// exactly the shape that OOM-killed SF10 workers (TPC-H Q16/Q21, TPC-DS Q11) — the
    /// build is not fully pool-accounted (KAN-57), so the runtime pool-exhaustion retry
    /// never fires before the cgroup killer. Hash is chosen only when statistics
    /// positively say the build fits the budget; the runtime retry remains the backstop
    /// for estimates that undershoot in the other direction. Without a bounded pool there
    /// is no budget to fit, so the plan always keeps its hash joins (unchanged).
    ///
    /// KAN-142: this is ALSO the post-conversion safety check — a per-join converted plan
    /// ([`Engine::per_join_strategy_physical_plan`]) that still trips it contains a hash
    /// join the rule could not convert and falls back to the whole-plan sort-merge
    /// re-plan. The counter-free core lets that check run without double-counting
    /// reroutes (see [`Engine::plan_time_smj_reroute`]).
    ///
    /// Known pre-existing hazard (unchanged by KAN-142): when the only violation is a
    /// NULL-AWARE anti join (`NOT IN` with nullable keys), this predicate still fires
    /// and the whole-plan `prefer_hash_join=false` re-plan seats the join as sort-merge
    /// even though `SortMergeJoinExec` has no null-aware support — DataFusion's planner
    /// checks `prefer_hash_join` before `null_aware` (datafusion-54.1.0
    /// physical_planner.rs). The KAN-142 rule itself never converts one
    /// ([`hash_join_as_sort_merge`] refuses), so the per-join path never makes it worse;
    /// fixing the whole-plan fallback for that shape is follow-up work.
    fn needs_smj_reroute(&self, plan: &dyn datafusion::physical_plan::ExecutionPlan) -> bool {
        let Some(budget) = self.hash_join_build_budget() else {
            return false;
        };
        if !Self::smj_replan_allowed() {
            return false;
        }
        hash_join_build_exceeds(plan, budget) || hash_join_build_estimate_unknown(plan)
    }

    /// [`Engine::needs_smj_reroute`] plus the KAN-53 observability counter (a reroute that
    /// FIRES is counted once per call — call sites that also re-check a converted plan use
    /// [`Engine::needs_smj_reroute`] so one query never counts twice).
    fn plan_time_smj_reroute(&self, plan: &dyn datafusion::physical_plan::ExecutionPlan) -> bool {
        let reroute = self.needs_smj_reroute(plan);
        if reroute {
            self.plan_time_smj_reroutes.fetch_add(1, Ordering::Relaxed);
        }
        reroute
    }

    /// The KAN-142 broadcast admission cap in bytes: the
    /// `OXIDANT_BROADCAST_JOIN_THRESHOLD_BYTES` threshold (default
    /// [`DEFAULT_BROADCAST_JOIN_THRESHOLD_BYTES`]) clamped to the KAN-25 build budget, so a
    /// broadcast conversion never admits a hash build the budget guard would have rerouted
    /// (the `CollectLeft` build is coalesced to ONE partition and collected whole — the
    /// same bytes a partitioned build would spread, so the budget must cover all of them).
    /// `None` — broadcast conversion disabled — when the threshold is set to `0`, when
    /// there is no bounded pool (no budget to clamp to; the unbounded path stays
    /// byte-for-byte unchanged), or under a forced `OXIDANT_PREFER_HASH_JOIN` (KAN-45
    /// semantics: forced sessions are never re-planned).
    fn broadcast_admission_cap(&self) -> Option<usize> {
        if join_preference() != JoinPreference::Auto {
            return None;
        }
        let budget = self.hash_join_build_budget()?;
        let threshold = broadcast_join_threshold_bytes()?;
        Some(threshold.min(budget))
    }

    /// Whether any hash join in `plan` is a KAN-142 broadcast candidate: an INNER
    /// partitioned hash join whose build side is positively estimated at or below
    /// [`Engine::broadcast_admission_cap`] — the runtime analog of Spark AQE's
    /// sort-merge → broadcast conversion, driven by barrier-measured (`MeasuredStatsTable`)
    /// or footer-exact statistics instead of a config threshold alone.
    fn plan_time_broadcast_upgrade(
        &self,
        plan: &dyn datafusion::physical_plan::ExecutionPlan,
    ) -> bool {
        let Some(cap) = self.broadcast_admission_cap() else {
            return false;
        };
        hash_join_broadcast_candidate(plan, cap)
    }

    /// Test/diagnostic observability: how many times the plan-time join-strategy guard's
    /// sort-merge predicate fired on this engine (KAN-53 auto selection; since KAN-142 the
    /// firing query is usually handled by the per-join conversion re-plan — see
    /// [`Engine::per_join_strategy_physical_plan`]).
    #[doc(hidden)]
    pub fn plan_time_smj_reroute_count(&self) -> u64 {
        self.plan_time_smj_reroutes.load(Ordering::Relaxed)
    }

    /// Test/diagnostic observability: how many tables were registered with driver-measured
    /// statistics on this engine (the KAN-2 A3 stage-input statistics path engaging).
    #[doc(hidden)]
    pub fn measured_stats_registration_count(&self) -> u64 {
        self.measured_stats_registrations.load(Ordering::Relaxed)
    }

    /// The join strategy a KAN-53 stall-retry flip re-plans with (`true` = hash): the
    /// opposite of what the first attempt actually RAN — its plan-time sort-merge reroute
    /// ([`Engine::plan_time_smj_reroute`]) when that engaged, else the session plan's own
    /// joins. (Re-deriving from the fresh session plan alone would misread an over-budget
    /// `auto` first attempt, whose executed plan was the rerouted sort-merge one.)
    fn flip_prefer_hash(&self, plan: &dyn datafusion::physical_plan::ExecutionPlan) -> bool {
        if self.plan_time_smj_reroute(plan) {
            return true; // the first attempt ran the sort-merge reroute
        }
        !contains_hash_join(plan)
    }

    /// Re-plan `logical` with `prefer_hash_join` set to `prefer_hash` on a query-scoped
    /// session state that shares this engine's catalogs, function registries and —
    /// load-bearing — its runtime environment (the same bounded `FairSpillPool`). With
    /// `prefer_hash=false` the result is a physical plan whose partitioned equijoins are
    /// spill-capable sort-merge joins (KAN-25). Session-global config is never mutated, so
    /// concurrent queries on this engine keep their own join selection.
    ///
    /// `sort_spill_reservation_bytes` is lowered to 1 MiB for a sort-merge re-plan: with
    /// DataFusion's 10 MiB default, `partitions × 2` sorters on a tight pool can each be
    /// denied their minimum reservation ("Not enough memory to continue external sort"),
    /// which would make the retry fail where the hash join only errored — the fallback must
    /// be able to spill its way through pools far smaller than the join inputs.
    async fn physical_plan_with_join_preference(
        &self,
        logical: datafusion::logical_expr::LogicalPlan,
        prefer_hash: bool,
    ) -> Result<(
        SessionContext,
        Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    )> {
        use datafusion::execution::session_state::SessionStateBuilder;
        let mut config = self.ctx.state().config().clone();
        // The query-scoped session SHARES this engine's catalog list; DataFusion's default
        // `create_default_catalog_and_schema=true` would make `SessionStateBuilder::build()`
        // register a fresh EMPTY default catalog into that shared list, wiping every table
        // the engine has registered (latent KAN-25 bug — a worker taking the sort-merge
        // fallback lost its registered tables for all later queries — exposed by KAN-53's
        // flipped-retry path, which queries the engine again after a re-plan).
        config = config.with_create_default_catalog_and_schema(false);
        {
            let opts = config.options_mut();
            opts.optimizer.prefer_hash_join = prefer_hash;
            if !prefer_hash {
                opts.execution.sort_spill_reservation_bytes =
                    opts.execution.sort_spill_reservation_bytes.min(1024 * 1024);
            }
        }
        let state = SessionStateBuilder::new_from_existing(self.ctx.state())
            .with_config(config)
            .build();
        // The returned context is load-bearing: the plan must be COLLECTED under its
        // `task_ctx` (same shared catalog/runtime/pool, fallback config), or execution
        // reads the original session's options — e.g. the 10 MiB sort spill reservation
        // this session deliberately lowers.
        let ctx = SessionContext::new_with_state(state);
        let df = ctx
            .execute_logical_plan(logical)
            .await
            .map_err(|e| Error::Plan(e.to_string()))?;
        let plan = df
            .create_physical_plan()
            .await
            .map_err(|e| Error::Execution(e.to_string()))?;
        Ok((ctx, plan))
    }

    /// Re-plan `logical` with sort-merge joins — the KAN-25 fallback shape; see
    /// [`Engine::physical_plan_with_join_preference`].
    async fn sort_merge_physical_plan(
        &self,
        logical: datafusion::logical_expr::LogicalPlan,
    ) -> Result<(
        SessionContext,
        Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    )> {
        self.physical_plan_with_join_preference(logical, false)
            .await
    }

    /// Re-plan `logical` with the KAN-142 per-join strategy rule added to the physical
    /// optimizer pipeline ([`PerJoinJoinStrategy`]): ONE physical plan in which each
    /// partitioned hash join independently becomes a broadcast (`CollectLeft`) hash join
    /// when its build side is positively measured/estimated at or below
    /// [`Engine::broadcast_admission_cap`], a sort-merge join when its build side is over
    /// the KAN-25 budget or un-estimable ([`Engine::smj_replan_allowed`]), or stays a
    /// partitioned hash join when the build positively fits — the distributed engine's
    /// per-join runtime strategy conversion, replacing the all-or-nothing session re-plan
    /// for multi-join stage SQL.
    ///
    /// The query-scoped session shares this engine's catalogs/runtime/pool and keeps the
    /// session's own `prefer_hash_join` (the rule, not the planner config, picks per
    /// join); `sort_spill_reservation_bytes` is lowered exactly as for the whole-plan
    /// sort-merge re-plan so converted sorters can spill through a tight pool. Callers
    /// MUST re-check the returned plan with [`Engine::needs_smj_reroute`]: a hash join
    /// the rule cannot convert (a projection-carrying one) keeps its over-budget build,
    /// and only the whole-plan sort-merge re-plan covers it.
    async fn per_join_strategy_physical_plan(
        &self,
        logical: datafusion::logical_expr::LogicalPlan,
    ) -> Result<(
        SessionContext,
        Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    )> {
        use datafusion::execution::session_state::SessionStateBuilder;
        let mut config = self.ctx.state().config().clone();
        // Same shared-catalog guard as `physical_plan_with_join_preference`.
        config = config.with_create_default_catalog_and_schema(false);
        {
            let opts = config.options_mut();
            opts.execution.sort_spill_reservation_bytes =
                opts.execution.sort_spill_reservation_bytes.min(1024 * 1024);
        }
        let strategy = std::sync::Arc::new(PerJoinJoinStrategy {
            budget: self.hash_join_build_budget(),
            broadcast_cap: self.broadcast_admission_cap(),
            smj_allowed: Self::smj_replan_allowed(),
        });
        let state = SessionStateBuilder::new_from_existing(self.ctx.state())
            .with_config(config)
            .with_physical_optimizer_rules(physical_optimizer_rules_with_join_strategy(strategy))
            .build();
        // The returned context is load-bearing: the plan must be COLLECTED under its
        // `task_ctx` (same shared catalog/runtime/pool, fallback config), or execution
        // reads the original session's options.
        let ctx = SessionContext::new_with_state(state);
        let df = ctx
            .execute_logical_plan(logical)
            .await
            .map_err(|e| Error::Plan(e.to_string()))?;
        let plan = df
            .create_physical_plan()
            .await
            .map_err(|e| Error::Execution(e.to_string()))?;
        Ok((ctx, plan))
    }

    /// Give the doomed first attempt's surviving partition streams a moment to release
    /// their pool reservations before the sort-merge retry claims its own. When a hash-join
    /// build errors out of `collect`, the other partitions' streams are still finishing
    /// asynchronously and hold most of the pool; they drain within a few hundred ms, and
    /// retrying against a still-full pool starves the fallback's external sorters
    /// ("Not enough memory to continue external sort"). Bounded at 10 s — if the pool never
    /// drains (a concurrent third query holding it, e.g.) the retry proceeds anyway and may
    /// fail with the actionable error rather than wedging.
    async fn wait_for_pool_drain(&self) {
        let Some(pool_bytes) = self.memory_pool_bytes else {
            return;
        };
        let pool = &self.ctx.task_ctx().runtime_env().memory_pool;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while pool.reserved() > pool_bytes / 4 && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    /// Collect a row-returning DataFrame under the KAN-25/KAN-53 join guard. With no
    /// bounded pool this is exactly `df.collect()`. With a pool:
    /// 1. stall-retry (KAN-53) — a watchdog-aborted stage's retry attempt runs the OPPOSITE
    ///    join strategy from the first attempt's plan ([`with_join_strategy_flipped`]);
    /// 2. plan time — if any hash join's build side is estimated above
    ///    [`Engine::hash_join_build_budget`], re-plan and run with sort-merge joins when
    ///    [`Engine::smj_replan_allowed`] (KAN-53 `auto` default or the KAN-45 opt-in);
    /// 3. runtime — if execution exhausts the pool under a hash join (statistics can
    ///    underestimate, e.g. wide strings), retry once with sort-merge joins (same gate);
    /// 4. if the retry also fails, or the re-plan is not allowed, return an actionable
    ///    error instead of wedging the worker.
    ///
    /// Queries that neither trip the estimate nor exhaust the pool are byte-for-byte on the
    /// old path, so hash-join-friendly queries keep their current performance.
    async fn collect_join_guarded(
        &self,
        df: datafusion::dataframe::DataFrame,
    ) -> Result<Vec<RecordBatch>> {
        let plan = df
            .create_physical_plan()
            .await
            .map_err(|e| Error::Execution(e.to_string()))?;
        // KAN-53 stall-retry: the worker re-runs a watchdog-aborted stage under
        // `with_join_strategy_flipped`; plan the retry with the strategy opposite to the
        // first attempt's (hash ⇄ sort-merge), bypassing the selection below — the flip
        // IS the retry, so it also bypasses `smj_replan_allowed`.
        if join_strategy_flipped() {
            let prefer_hash = self.flip_prefer_hash(plan.as_ref());
            let (flip_ctx, flip_plan) = self
                .physical_plan_with_join_preference(df.logical_plan().clone(), prefer_hash)
                .await?;
            self.wait_for_pool_drain().await;
            return datafusion::physical_plan::collect(flip_plan, flip_ctx.task_ctx())
                .await
                .map_err(|e| Error::Execution(e.to_string()));
        }
        let smj_reroute = self.plan_time_smj_reroute(plan.as_ref());
        if smj_reroute || self.plan_time_broadcast_upgrade(plan.as_ref()) {
            // KAN-142 first: ONE re-plan that decides the strategy PER JOIN from the
            // (barrier-measured or footer-exact) build-side sizes — broadcast for
            // measured-small builds, hash for builds that fit, sort-merge only for the
            // joins whose build is over budget or un-estimable — instead of the
            // all-or-nothing session re-plan below.
            if let Ok((pj_ctx, pj)) = self
                .per_join_strategy_physical_plan(df.logical_plan().clone())
                .await
            {
                if !self.needs_smj_reroute(pj.as_ref()) {
                    // Runtime pool exhaustion under a per-join plan's remaining hash
                    // joins (estimates can undershoot, e.g. wide strings) keeps the same
                    // backstop as the unconverted path below: one whole-plan sort-merge
                    // retry.
                    match datafusion::physical_plan::collect(pj.clone(), pj_ctx.task_ctx()).await {
                        Ok(batches) => return Ok(batches),
                        Err(e) => {
                            let had_hash_join = contains_hash_join(pj.as_ref());
                            drop(pj);
                            if is_pool_exhausted(&e) && had_hash_join && Self::smj_replan_allowed()
                            {
                                self.wait_for_pool_drain().await;
                                let (smj_ctx, smj) = self
                                    .sort_merge_physical_plan(df.logical_plan().clone())
                                    .await?;
                                return datafusion::physical_plan::collect(smj, smj_ctx.task_ctx())
                                    .await
                                    .map_err(|retry| {
                                        Error::Execution(format!(
                                            "query exhausted the OXIDANT_MEMORY_LIMIT_BYTES pool \
                                             under a non-spillable hash join and the sort-merge \
                                             retry failed: {retry}"
                                        ))
                                    });
                            }
                            if smj_reroute {
                                return Err(Error::Execution(e.to_string()));
                            }
                            // Broadcast-only trigger: the original hash plan was already
                            // validated safe by the guard — a non-pool error from the
                            // per-join plan falls back to it (below) rather than failing
                            // a query that pre-KAN-142 simply executed.
                        }
                    }
                }
                // An over-budget / un-estimable hash build the per-join rule could not
                // convert (a projection-carrying join, e.g.): the whole-plan sort-merge
                // re-plan remains the fallback.
            }
            if smj_reroute {
                if let Ok((smj_ctx, smj)) = self
                    .sort_merge_physical_plan(df.logical_plan().clone())
                    .await
                {
                    return datafusion::physical_plan::collect(smj, smj_ctx.task_ctx())
                        .await
                        .map_err(|e| Error::Execution(e.to_string()));
                }
            }
            // Re-planning failed (a join shape sort-merge cannot take, e.g.): fall through
            // to the hash plan — the runtime guard below still bounds the blast radius.
        }
        // KAN-45: with the sort-merge re-plan not allowed (a forced
        // `OXIDANT_PREFER_HASH_JOIN=true|false` without OXIDANT_SORT_MERGE_FALLBACK) an
        // over-budget build runs the hash plan anyway; a pool overflow then fails fast
        // (below) rather than rerouting silently.
        match datafusion::physical_plan::collect(plan.clone(), self.ctx.task_ctx()).await {
            Ok(batches) => Ok(batches),
            Err(e) => {
                let had_hash_join = contains_hash_join(plan.as_ref());
                drop(plan);
                if is_pool_exhausted(&e) && had_hash_join && Self::smj_replan_allowed() {
                    self.wait_for_pool_drain().await;
                    let (smj_ctx, smj) = self
                        .sort_merge_physical_plan(df.logical_plan().clone())
                        .await?;
                    return datafusion::physical_plan::collect(smj, smj_ctx.task_ctx())
                        .await
                        .map_err(|retry| {
                            Error::Execution(format!(
                                "query exhausted the OXIDANT_MEMORY_LIMIT_BYTES pool under a \
                                 non-spillable hash join and the sort-merge retry failed: \
                                 {retry}"
                            ))
                        });
                }
                if is_pool_exhausted(&e) && had_hash_join {
                    return Err(Error::Execution(format!(
                        "query exhausted the OXIDANT_MEMORY_LIMIT_BYTES pool under a \
                         non-spillable hash join: {e}. The KAN-25 sort-merge fallback is \
                         not allowed for this session (a forced OXIDANT_PREFER_HASH_JOIN \
                         without OXIDANT_SORT_MERGE_FALLBACK); use the default \
                         OXIDANT_PREFER_HASH_JOIN=auto, set OXIDANT_SORT_MERGE_FALLBACK=true, or \
                         raise OXIDANT_MEMORY_LIMIT_BYTES / OXIDANT_HASH_JOIN_MAX_BUILD_FRACTION."
                    )));
                }
                Err(Error::Execution(e.to_string()))
            }
        }
    }

    /// Execute a row-returning `query` as a **stream** of record batches (KAN-32) instead of
    /// collecting it whole. Distributed producer stages use this to pipeline hash
    /// partitioning and shuffle spill with execution, so a large stage output never sits
    /// fully materialized in worker memory outside the bounded pool.
    ///
    /// Planning is identical to [`Engine::sql`] (same Spark rewrites via `plan_spark`), and
    /// the KAN-25 plan-time fallback applies when [`Engine::smj_replan_allowed`] (KAN-53
    /// `auto` default or the KAN-45 opt-in): when a hash join's build side is estimated
    /// above the pool budget, the stream runs the sort-merge plan instead. A *runtime* pool
    /// exhaustion surfaces as a stream error — callers should discard any partial output
    /// and retry through [`Engine::sql`]; a failed stream must never be retried in place,
    /// or already-emitted rows would be duplicated.
    ///
    /// A KAN-53 stall-retry attempt ([`with_join_strategy_flipped`]) runs the OPPOSITE join
    /// strategy from the first attempt's plan, mirroring [`Engine::collect_join_guarded`].
    pub async fn sql_stream(
        &self,
        query: &str,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        self.sql_stream_stage(query, None).await
    }

    /// [`Engine::sql_stream`] for one distributed stage task (R5-4 / KAN-2): when
    /// `plan_request` is set, the parse + Spark-rewrite + name-resolution front-end is served
    /// from the worker's [`stage_plan_cache`] — the first task of the stage on this engine
    /// plans (publishing the template), the rest hit it. A hit rebinds the template's
    /// `shuffle_input*` scans to THIS task's registered providers (carrying this task's
    /// measured row totals, KAN-2 A3), so the per-task optimize + physical planning below —
    /// including the KAN-25/KAN-53 join guards — sees exactly the same inputs as an uncached
    /// plan. `query` is the task's localized stage SQL, used only on a miss (the template is
    /// built from it); the request key carries the canonical pre-localization SQL.
    pub async fn sql_stream_stage(
        &self,
        query: &str,
        plan_request: Option<&stage_plan_cache::StagePlanRequest>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let df = self.plan_spark_stage(query, plan_request).await?;
        // KAN-53 stall-retry (mirrors `collect_join_guarded`): plan the retried stage with
        // the join strategy opposite to the first attempt's — the flip bypasses the
        // auto/forced selection below.
        if join_strategy_flipped() {
            let plan = df
                .create_physical_plan()
                .await
                .map_err(|e| Error::Execution(e.to_string()))?;
            let prefer_hash = self.flip_prefer_hash(plan.as_ref());
            let (flip_ctx, flip_plan) = self
                .physical_plan_with_join_preference(df.logical_plan().clone(), prefer_hash)
                .await?;
            self.wait_for_pool_drain().await;
            // Merge the re-planned partitions into one stream (stage output is unordered
            // by contract — ORDER BY/LIMIT live in the driver finalize).
            let merged: Arc<dyn datafusion::physical_plan::ExecutionPlan> = Arc::new(
                datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(
                    flip_plan,
                ),
            );
            return merged
                .execute(0, flip_ctx.task_ctx())
                .map_err(|e| Error::Execution(e.to_string()));
        }
        if self.hash_join_build_budget().is_some() {
            let plan = df
                .create_physical_plan()
                .await
                .map_err(|e| Error::Execution(e.to_string()))?;
            let smj_reroute = self.plan_time_smj_reroute(plan.as_ref());
            if smj_reroute || self.plan_time_broadcast_upgrade(plan.as_ref()) {
                // KAN-142 first: ONE re-plan deciding the strategy PER JOIN from this
                // task's barrier-measured shuffle-input sizes (broadcast for measured-
                // small builds, hash for builds that fit, sort-merge only for over-budget
                // / un-estimable builds) instead of the all-or-nothing session re-plan.
                if let Ok((pj_ctx, pj)) = self
                    .per_join_strategy_physical_plan(df.logical_plan().clone())
                    .await
                {
                    if !self.needs_smj_reroute(pj.as_ref()) {
                        // Merge partitions into one stream (stage output is unordered by
                        // contract — ORDER BY/LIMIT live in the driver finalize).
                        let merged: Arc<dyn datafusion::physical_plan::ExecutionPlan> = Arc::new(
                            datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(
                                pj,
                            ),
                        );
                        match merged.execute(0, pj_ctx.task_ctx()) {
                            Ok(stream) => return Ok(stream),
                            Err(e) => {
                                if smj_reroute {
                                    return Err(Error::Execution(e.to_string()));
                                }
                                // Broadcast-only trigger: the original hash plan was
                                // already validated safe by the guard — fall back to it
                                // (below) rather than failing a task that pre-KAN-142
                                // simply executed. Stream-time errors keep the existing
                                // contract (caller discards partial output and retries
                                // through `Engine::sql`).
                            }
                        }
                    }
                    // An over-budget / un-estimable hash build the per-join rule could not
                    // convert: the whole-plan sort-merge re-plan remains the fallback.
                }
                if smj_reroute {
                    if let Ok((smj_ctx, smj)) = self
                        .sort_merge_physical_plan(df.logical_plan().clone())
                        .await
                    {
                        // Merge the sort-merge plan's partitions into one stream (stage output
                        // is unordered by contract — ORDER BY/LIMIT live in the driver finalize).
                        let merged: Arc<dyn datafusion::physical_plan::ExecutionPlan> = Arc::new(
                            datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(
                                smj,
                            ),
                        );
                        return merged
                            .execute(0, smj_ctx.task_ctx())
                            .map_err(|e| Error::Execution(e.to_string()));
                    }
                }
                // Re-planning failed (a join shape sort-merge cannot take, e.g.): fall
                // through to the hash plan — a runtime pool exhaustion then surfaces as a
                // stream error and the caller's collect fallback bounds the blast radius.
            }
            // No reroute: execute the physical plan just built for the guard check instead
            // of letting `execute_stream` plan a second time (the budget guard runs on
            // every worker stage task, so this was a full duplicate physical-plan pass per
            // task). Merge partitions into one stream, mirroring the branches above and
            // `DataFrame::execute_stream`'s own contract (stage output is unordered —
            // ORDER BY/LIMIT live in the driver finalize).
            let merged: Arc<dyn datafusion::physical_plan::ExecutionPlan> = Arc::new(
                datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(plan),
            );
            return merged
                .execute(0, self.ctx.task_ctx())
                .map_err(|e| Error::Execution(e.to_string()));
        }
        df.execute_stream()
            .await
            .map_err(|e| Error::Execution(e.to_string()))
    }

    /// Stream worker SQL under snapshot pins serialized by the driver — the streaming
    /// counterpart of [`Engine::sql_with_lakehouse_snapshots`]. The pins scope covers
    /// planning, which is where lakehouse tables resolve; the returned stream reads the
    /// pinned providers captured in the physical plan.
    pub async fn sql_stream_with_lakehouse_snapshots(
        &self,
        query: &str,
        pins_json: &str,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        catalog_bridge::with_lakehouse_snapshots(pins_json, self.sql_stream(query)).await
    }

    /// [`Engine::sql_stream_stage`] under driver-serialized snapshot pins, mirroring
    /// [`Engine::sql_stream_with_lakehouse_snapshots`]. The pins scope covers both the
    /// miss-path planning and the hit-path provider rebind; the pins JSON itself is a stage
    /// plan cache key component, so a re-pinned snapshot (KAN-48) never hits a stale
    /// template.
    pub async fn sql_stream_stage_with_lakehouse_snapshots(
        &self,
        query: &str,
        pins_json: &str,
        plan_request: Option<&stage_plan_cache::StagePlanRequest>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        catalog_bridge::with_lakehouse_snapshots(
            pins_json,
            self.sql_stream_stage(query, plan_request),
        )
        .await
    }

    /// Build one stage task's [`stage_plan_cache::StagePlanRequest`]: fills the cache key's
    /// engine identity and current catalog version from this engine; the caller (the Flight
    /// worker) supplies the canonical pre-localization stage SQL, the driver's snapshot pins
    /// and replicated classification verbatim from the ticket, and this task's registered
    /// shuffle-input providers in upstream order (each carrying its measured row totals when
    /// the ticket shipped them — those stay OUT of the key and re-enter per task via the
    /// hit-path rebind; see the [`stage_plan_cache`] module docs).
    pub fn stage_plan_request(
        &self,
        canonical_sql: &str,
        stage_id: u32,
        pins_json: &str,
        replicated_csv: &str,
        shuffle_inputs: Vec<Arc<dyn datafusion::catalog::TableProvider>>,
    ) -> stage_plan_cache::StagePlanRequest {
        stage_plan_cache::StagePlanRequest::new(
            self.plan_cache_id,
            self.catalog_version.load(Ordering::Relaxed),
            stage_id,
            canonical_sql,
            pins_json,
            replicated_csv,
            shuffle_inputs,
        )
    }

    /// `plan_spark` front-end with the stage plan cache in front of it (R5-4). With no
    /// `plan_request` (or a disabled cache) this is exactly [`Engine::plan_spark`].
    /// Otherwise: hit → rebind the cached template to this task's shuffle-input providers
    /// and wrap it as the query DataFrame (optimize + physical planning still run per task,
    /// downstream of here); miss → plan fresh and publish the template; concurrent
    /// same-stage tasks single-flight on one build.
    async fn plan_spark_stage(
        &self,
        query: &str,
        plan_request: Option<&stage_plan_cache::StagePlanRequest>,
    ) -> Result<datafusion::dataframe::DataFrame> {
        let Some(request) = plan_request else {
            return self.plan_spark(query).await;
        };
        loop {
            let Some(lookup) = stage_plan_cache::global().lookup(request.key()) else {
                return self.plan_spark(query).await;
            };
            match lookup {
                stage_plan_cache::PlanLookup::Hit(template) => {
                    let plan = stage_plan_cache::rebind_shuffle_inputs(
                        &template,
                        request.stage_id(),
                        request.shuffle_inputs(),
                    )
                    .map_err(|e| Error::Execution(format!("stage plan cache rebind: {e}")))?;
                    return self
                        .ctx
                        .execute_logical_plan(plan)
                        .await
                        .map_err(|e| Error::Plan(e.to_string()));
                }
                stage_plan_cache::PlanLookup::Build(ticket) => {
                    return match self.plan_spark(query).await {
                        Ok(df) => {
                            stage_plan_cache::global()
                                .complete_build(ticket, Some(df.logical_plan().clone()));
                            Ok(df)
                        }
                        // Planning failures are never cached, but waiters must be released.
                        Err(e) => {
                            stage_plan_cache::global().complete_build(ticket, None);
                            Err(e)
                        }
                    };
                }
                // Another task of this stage is planning right now; re-lookup once it
                // publishes (a `Some(None)` slot — its build failed — loops into a fresh
                // build attempt here, which fails the same way, matching uncached behavior).
                // A dropped sender (builder cancelled) is treated the same as a failure.
                stage_plan_cache::PlanLookup::Wait(mut rx) => {
                    let _ = rx.wait_for(|slot| slot.is_some()).await;
                }
            }
        }
    }

    /// Run a row-returning `query` and return its result batches **plus** execution statistics —
    /// the substrate for Databricks-style observability (duration, rows, bytes scanned).
    ///
    /// Unlike [`Engine::sql`], this builds the physical plan explicitly and *retains* it, so
    /// DataFusion's per-operator metrics can be read after execution (`plan.metrics()`); `df.collect()`
    /// drops the plan, so `bytes_scanned` and friends are otherwise lost. Intended for the
    /// display/result path — it does not run `sql`'s SHOW/DDL/CTAS/INSERT interception, so callers
    /// use it only for queries they have already classified as row-returning.
    pub async fn sql_with_stats(&self, query: &str) -> Result<(Vec<RecordBatch>, QueryStats)> {
        // Same guard `Engine::sql` applies: Spark rejects multi-column `COUNT(DISTINCT a, b)` at
        // analysis time, but DataFusion *panics* while planning it. Reject up front so this path
        // (reached for scan queries via the Spark Connect metrics route) returns a clean
        // `Error::Plan` instead of panicking the driver task — matching `Engine::sql`.
        if is_multi_arg_count_distinct(query) {
            return Err(Error::Plan(
                "COUNT(DISTINCT) does not support multiple columns".into(),
            ));
        }
        let start = std::time::Instant::now();
        let df = self.plan_spark(query).await?;
        let plan = df
            .create_physical_plan()
            .await
            .map_err(|e| Error::Execution(e.to_string()))?;
        let batches = datafusion::physical_plan::collect(plan.clone(), self.ctx.task_ctx())
            .await
            .map_err(|e| Error::Execution(e.to_string()))?;
        let output_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let stats = QueryStats {
            duration_ms: start.elapsed().as_millis() as u64,
            output_rows,
            // Scan nodes carry `bytes_scanned`; sum it across the executed plan tree.
            bytes_scanned: aggregate_plan_metric(plan.as_ref(), "bytes_scanned"),
        };
        Ok((batches, stats))
    }

    /// Register an in-memory table of `batches` under `name` — the worker-side landing zone
    /// for shuffle input, so a downstream stage can read it as an ordinary table. Idempotent: any
    /// existing table of the same name is replaced (a worker reuses its engine across queries, so
    /// `shuffle_input` is re-registered each time). Returns the registered provider (the stage
    /// plan cache rebinds templates to it, R5-4).
    pub fn register_batches(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        use datafusion::datasource::MemTable;

        let schema = match batches.first() {
            Some(b) => b.schema(),
            None => return Err(Error::Plan(format!("register `{name}`: no batches"))),
        };
        let table: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(
            MemTable::try_new(schema, vec![batches])
                .map_err(|e| Error::Execution(format!("mem table `{name}`: {e}")))?,
        );
        // Drop any prior registration so re-registering the same name doesn't error.
        let _ = self.ctx.deregister_table(name);
        self.ctx
            .register_table(name, table.clone())
            .map_err(|e| Error::Execution(format!("register `{name}`: {e}")))?;
        self.note_catalog_change(name);
        Ok(table)
    }

    /// Deregister a table previously registered via [`Self::register_batches`] (e.g. a finished
    /// stage's `shuffle_input`). A missing name is a no-op.
    pub fn deregister_table(&self, name: &str) {
        let _ = self.ctx.deregister_table(name);
        self.note_catalog_change(name);
    }

    /// Bump the stage plan cache's catalog-version guard ([`Engine::catalog_version`]) for a
    /// catalog mutation — EXCEPT a per-task localized shuffle-input registration
    /// (`shuffle_input__s*_p*`), whose per-task churn is covered by the cache key's input
    /// schema fingerprints instead (bumping on it would invalidate every stage's template
    /// on every task).
    fn note_catalog_change(&self, table_name: &str) {
        if stage_plan_cache::is_localized_shuffle_input_name(table_name) {
            return;
        }
        self.catalog_version.fetch_add(1, Ordering::Relaxed);
    }

    /// Register an in-memory table of `batches` under `name` carrying a driver-measured
    /// exact row count — the worker-side landing zone for shuffle input when the consumer's
    /// `StageTicket` carries the producer stage's barrier-measured bucket totals
    /// (`OXIDANT_STAGE_INPUT_STATS`). The physical scan reports `measured_rows` as an exact
    /// `num_rows` statistic (plus the batches' real in-memory byte size; column statistics
    /// stay unknown), so the plan-time join-strategy guard sizes hash-join build sides from
    /// measured data instead of recomputing — or failing to find — statistics. Like
    /// [`Self::register_batches`], any existing table of the same name is replaced, and the
    /// registered provider is returned.
    pub fn register_batches_with_stats(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
        measured_rows: u64,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        let table: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(
            measured_scan::MeasuredStatsTable::try_new(batches, measured_rows as usize)
                .map_err(|e| Error::Execution(format!("mem table `{name}`: {e}")))?,
        );
        let _ = self.ctx.deregister_table(name);
        self.ctx
            .register_table(name, table.clone())
            .map_err(|e| Error::Execution(format!("register `{name}`: {e}")))?;
        self.measured_stats_registrations
            .fetch_add(1, Ordering::Relaxed);
        self.note_catalog_change(name);
        Ok(table)
    }

    /// Directory this worker uses for one consumer task's spilled shuffle input.
    ///
    /// Nested under the engine's own DataFusion spill root so `Drop` reclaims it with
    /// everything else, and keyed by (stage, partition, upstream) so sibling tasks on the same
    /// worker never scan each other's files.
    pub fn shuffle_pull_spill_dir(
        &self,
        stage_id: u32,
        partition_id: u32,
        upstream_idx: u32,
    ) -> std::path::PathBuf {
        self.dirs
            .spill_dir
            .join(format!("pull_{stage_id}_{partition_id}_{upstream_idx}"))
    }

    /// Register a shuffle input that lives on DISK as Arrow IPC files, rather than in a
    /// `MemTable`, so scanning it streams instead of requiring the whole input to fit in RAM.
    ///
    /// This is the spill-backed twin of [`Engine::register_batches_with_stats`]. A consumer
    /// task materializing its whole modulus class in memory for the life of the stage is the
    /// one allocation that neither the DataFusion pool nor the shuffle budget bounds — the
    /// worker simply grows until the cgroup kills it. Streaming from IPC removes the
    /// requirement instead of trying to budget for it.
    ///
    /// `measured_rows` is the driver's barrier count and is attached exactly as the in-memory
    /// path attaches it, so the join-strategy guard sees the same statistics either way.
    pub fn register_arrow_ipc_shuffle_input(
        &self,
        name: &str,
        dir: &std::path::Path,
        schema: datafusion::arrow::datatypes::SchemaRef,
        measured_rows: u64,
        measured_bytes: usize,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        use datafusion::datasource::file_format::arrow::ArrowFormat;
        use datafusion::datasource::listing::{
            ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
        };

        let url = ListingTableUrl::parse(format!("file://{}/", dir.display()))
            .map_err(|e| Error::Execution(format!("spill dir url for `{name}`: {e}")))?;
        let options = ListingOptions::new(Arc::new(ArrowFormat)).with_file_extension(".arrow");
        let config = ListingTableConfig::new(url)
            .with_listing_options(options)
            .with_schema(schema);
        let listing = Arc::new(
            ListingTable::try_new(config)
                .map_err(|e| Error::Execution(format!("spill listing `{name}`: {e}")))?,
        );
        let table: Arc<dyn datafusion::catalog::TableProvider> =
            Arc::new(measured_scan::MeasuredStatsTable::from_provider(
                listing,
                measured_rows as usize,
                measured_bytes,
            ));
        let _ = self.ctx.deregister_table(name);
        self.ctx
            .register_table(name, table.clone())
            .map_err(|e| Error::Execution(format!("register `{name}`: {e}")))?;
        // This IS a measured-stats registration — count it like the in-memory path, or the
        // spill route silently reads as "no measured statistics" to the observability that
        // `tests/stage_input_stats.rs` asserts on.
        self.measured_stats_registrations
            .fetch_add(1, Ordering::Relaxed);
        self.note_catalog_change(name);
        Ok(table)
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
            .map_err(|e| Error::Execution(format!("register parquet `{name}`: {e}")))?;
        self.note_catalog_change(name);
        Ok(())
    }

    /// Register a Delta Lake table directory under `name`.
    pub async fn register_delta(&self, name: &str, table_path: &str) -> Result<()> {
        self.register_lakehouse(name, table_path, oxidant_catalog::TableFormat::Delta)
            .await
    }

    /// A Delta table's declared schema and partition columns, when its log can be read.
    ///
    /// Best effort: anything unreadable (no log yet, an unmappable type, a permissions error)
    /// falls back to the inference path rather than failing the registration.
    async fn delta_table_metadata(
        &self,
        table_path: &str,
    ) -> Option<oxidant_datasource::delta_write::DeltaTableMetadata> {
        use datafusion::datasource::listing::ListingTableUrl;

        let store = self
            .object_store_for(table_path, &std::collections::HashMap::new())
            .ok()?;
        let root = ListingTableUrl::parse(table_path).ok()?.prefix().clone();
        oxidant_datasource::delta_write::current_metadata(store.as_ref(), &root)
            .await
            .ok()
            .flatten()
    }

    /// Register an Iceberg table directory under `name`.
    pub async fn register_iceberg(&self, name: &str, table_path: &str) -> Result<()> {
        self.register_lakehouse(name, table_path, oxidant_catalog::TableFormat::Iceberg)
            .await
    }

    /// Build a provider for the lakehouse table at `table_path` — resolved *now*, and NOT
    /// registered under any name.
    ///
    /// `name` is not a registration: it only feeds the shard/replicate classification
    /// ([`shard::is_replicated_table`]) and the error messages, so a read through this path makes
    /// the same file-listing decision a read of the registered name would.
    async fn lakehouse_provider(
        &self,
        name: &str,
        table_path: &str,
        format: oxidant_catalog::TableFormat,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        let mut metadata = oxidant_catalog::TableMetadata::new(name, table_path, format);
        // The log's `metaData` action is this form's catalog: given only a directory there is
        // nothing else to declare the table with, and a catalog like Glue supplies the same two
        // things from its own entry — which is why this only matters for the bare-path form.
        //
        // The schema is supplied whether or not the table is partitioned, because the resolver's
        // fallback is to infer from a data file and an **empty** table has none: a table whose
        // live files have all been removed — a CDC target drained by deletes or a truncate, an
        // `INSERT OVERWRITE ... WHERE false` — is empty, not broken, but without a declared
        // schema it cannot be read at all, and for a read-modify-write sink that is a table that
        // can never be written to again. A committed Delta log always carries a schema, so this
        // asks it rather than guessing. (Tables with no readable log at all — no commits yet, an
        // unmappable type — still fall through to inference, which is what serves them today.)
        //
        // Partition columns matter for the same reason but only when there are any: a partitioned
        // table keeps them in the path, not in the data files, so a reader that does not know
        // their names sees a table missing exactly the columns a dashboard filters on.
        if format == oxidant_catalog::TableFormat::Delta {
            if let Some(declared) = self.delta_table_metadata(table_path).await {
                metadata.schema = Some(declared.schema);
                if !declared.partition_columns.is_empty() {
                    metadata.partition_columns = declared.partition_columns;
                }
            }
        }
        catalog_bridge::metadata_to_provider(&self.ctx.state(), &metadata, name, false)
            .await
            .map(|resolved| resolved.provider)
            .map_err(|e| Error::Execution(e.to_string()))
    }

    /// Read a Delta table at `table_path` in full, through a provider built *for this call*.
    ///
    /// A registered name is a **snapshot**: the provider behind it embeds the file list it was
    /// resolved from, and neither `register_delta`'s session registration nor the catalog
    /// bridge's lakehouse cache revalidates it (see [`catalog_bridge`] — only non-lakehouse
    /// entries have a TTL). Anything that reads a table *this process has just written* must not
    /// go through a name, or it reads its own pre-commit snapshot back. For a read-modify-write
    /// — the AUTO CDC sink's per-batch merge — that is not a stale read but silent data loss:
    /// the rows it cannot see are absent from the merge result it then commits.
    ///
    /// [`Engine::refresh_table`] closes the same gap for readers that go by name, and the write
    /// path calls it after every commit; this is for the readers that should not have to depend
    /// on that being true.
    pub async fn read_delta(&self, name: &str, table_path: &str) -> Result<Vec<RecordBatch>> {
        let provider = self
            .lakehouse_provider(name, table_path, oxidant_catalog::TableFormat::Delta)
            .await?;
        self.ctx
            .read_table(provider)
            .map_err(|e| Error::Execution(format!("read `{name}` at `{table_path}`: {e}")))?
            .collect()
            .await
            .map_err(|e| Error::Execution(format!("read `{name}` at `{table_path}`: {e}")))
    }

    async fn register_lakehouse(
        &self,
        name: &str,
        table_path: &str,
        format: oxidant_catalog::TableFormat,
    ) -> Result<()> {
        let table = self.lakehouse_provider(name, table_path, format).await?;
        self.ctx
            .register_table(name, table)
            .map_err(|e| Error::Execution(format!("register `{name}`: {e}")))?;
        self.note_catalog_change(name);
        Ok(())
    }

    /// Register a sample-data directory (`oxidant spark server --sample-data <DIR>` /
    /// `OXIDANT_SAMPLE_DATA_DIR`) under the `samples` schema of the built-in catalog, so a
    /// first-time user can immediately `SELECT count(*) FROM samples.tpch_nation` with zero
    /// setup. Recognized layout (every subdir optional):
    ///
    /// - `parquet/<name>.parquet` → `samples.<name>` (the primary tables)
    /// - `csv/<name>.csv`         → `samples.<name>_csv`
    /// - `delta/<name>/`          → `samples.<name>_delta`
    /// - `iceberg/<name>/`        → `samples.<name>_iceberg`
    ///
    /// Best-effort: a missing directory or a table that fails to register is logged and
    /// skipped — sample data must never block server boot. Returns the number of tables
    /// registered.
    pub async fn register_sample_tables(&self, dir: impl AsRef<std::path::Path>) -> usize {
        let dir = dir.as_ref();
        if !dir.is_dir() {
            eprintln!(
                "oxidant: sample-data directory {} not found; no sample tables registered",
                dir.display()
            );
            return 0;
        }
        let dir = match dir.canonicalize() {
            Ok(d) => d,
            Err(e) => {
                eprintln!(
                    "oxidant: cannot resolve sample-data directory {}: {e}",
                    dir.display()
                );
                return 0;
            }
        };
        let catalog_name = self.default_catalog_name();
        let Some(catalog) = self.ctx.catalog(&catalog_name) else {
            return 0;
        };
        // Get-or-create the `samples` schema in the built-in catalog.
        let schema = match catalog.schema(SAMPLES_SCHEMA) {
            Some(s) => s,
            None => {
                let provider = Arc::new(datafusion::catalog::MemorySchemaProvider::new());
                if let Err(e) = catalog.register_schema(SAMPLES_SCHEMA, provider.clone()) {
                    eprintln!("oxidant: cannot create `{SAMPLES_SCHEMA}` schema: {e}");
                    return 0;
                }
                provider
            }
        };
        let mut registered = 0;
        for (sub, format, suffix) in [
            ("parquet", oxidant_catalog::TableFormat::Parquet, ""),
            ("csv", oxidant_catalog::TableFormat::Csv, "_csv"),
            ("delta", oxidant_catalog::TableFormat::Delta, "_delta"),
            ("iceberg", oxidant_catalog::TableFormat::Iceberg, "_iceberg"),
        ] {
            let Ok(entries) = std::fs::read_dir(dir.join(sub)) else {
                continue;
            };
            // Sort for deterministic registration order (and deterministic logs).
            let mut tables: Vec<(String, std::path::PathBuf)> = entries
                .filter_map(|e| e.ok())
                .filter_map(|e| {
                    let path = e.path();
                    sample_table_stem(&path, sub).map(|stem| (stem, path))
                })
                .collect();
            tables.sort();
            for (stem, path) in tables {
                let table_name = format!("{stem}{suffix}");
                let qualified = format!("{SAMPLES_SCHEMA}.{table_name}");
                let md = oxidant_catalog::TableMetadata::new(
                    &qualified,
                    path.to_string_lossy().as_ref(),
                    format,
                );
                let provider =
                    catalog_bridge::metadata_to_provider(&self.ctx.state(), &md, &qualified, false)
                        .await;
                match provider {
                    Ok(res) => match schema.register_table(table_name.clone(), res.provider) {
                        Ok(_) => {
                            registered += 1;
                            eprintln!("oxidant: registered sample table `{qualified}`");
                            self.note_catalog_change(&qualified);
                        }
                        Err(e) => {
                            eprintln!("oxidant: sample table `{qualified}`: {e} (skipped)")
                        }
                    },
                    Err(e) => eprintln!("oxidant: sample table `{qualified}`: {e} (skipped)"),
                }
            }
        }
        registered
    }

    /// Register an external catalog under `name`, bridging it into DataFusion's catalog API so
    /// `SELECT … FROM {name}.namespace.table` (and `spark.read.table("{name}.ns.t")`) resolve
    /// **lazily** — the catalog is hit only when a query first references one of its tables.
    pub fn register_catalog(
        &self,
        name: &str,
        provider: Arc<dyn oxidant_catalog::CatalogProvider>,
    ) {
        // Keep the raw oxidant provider so the engine can answer catalog-listing SQL (`SHOW DATABASES`,
        // `SHOW TABLES IN …`) authoritatively — the DataFusion bridge below only surfaces a
        // best-effort, already-materialized snapshot.
        self.oxidant_catalogs
            .lock()
            .expect("oxidant_catalogs poisoned")
            .insert(name.to_string(), provider.clone());
        let bridge = Arc::new(catalog_bridge::OxidantCatalogProvider::new(
            provider,
            self.ctx.clone(),
            self.require_lakehouse_snapshot_pins.clone(),
            self.catalog_version.clone(),
        ));
        self.ctx.register_catalog(name, bridge);
        self.note_catalog_change(name);
    }

    /// `spark.catalog.refreshTable`: drop the table's cached provider from the external catalog
    /// bridge, so the next reference re-resolves it from the metastore (a re-typed schema / new
    /// location is picked up without a restart). When an entry was actually evicted, the stage
    /// plan cache's catalog version is bumped so cached distributed plans rebuild; a refresh
    /// that evicted nothing (never-resolved table, local non-external-catalog table) is a
    /// no-op — like Spark, which skips cache invalidation when nothing was cached. Accepts
    /// `catalog.db.table`, `db.table`, or a bare `table` — resolved against this handle's
    /// current catalog + namespace (the same session state SQL `USE` and the Connect Catalog
    /// RPCs drive).
    ///
    /// Edge case: external-catalog sessions seed an EMPTY namespace (KAN-84, see
    /// [`Engine::for_session`]/`run_use`), so a bare-name refresh has no namespace to key the
    /// bridge's schema providers by. Rather than no-op, the eviction then falls back to a
    /// bare-name sweep of the resolved catalog's schema providers (or of every registered
    /// external catalog when the current catalog isn't external).
    ///
    /// NOTE: this RPC only reaches the process it lands on (the driver). Worker processes are
    /// not reachable from here — they converge on metastore changes via the non-lakehouse
    /// catalog cache TTL (`OXIDANT_CATALOG_CACHE_TTL_MS`, see `catalog_bridge`).
    pub async fn refresh_table(&self, table_name: &str) -> Result<()> {
        let parts = oxidant_catalog::split_ident(table_name);
        let Some((table, qualifier)) = parts.split_last() else {
            return Err(Error::Plan(format!(
                "refreshTable: empty table name `{table_name}`"
            )));
        };
        let (current_catalog, current_namespace) = self.current_catalog_and_namespace();
        let (catalog, namespace, external, external_names) = {
            let catalogs = self
                .oxidant_catalogs
                .lock()
                .expect("oxidant_catalogs poisoned");
            // A leading segment that names a registered external catalog is the catalog (the
            // rest is the namespace); otherwise the whole qualifier is a namespace in the
            // current catalog.
            let (catalog, namespace) = match qualifier.split_first() {
                Some((first, rest)) if catalogs.contains_key(first.as_str()) => {
                    (first.clone(), rest.to_vec())
                }
                _ if !qualifier.is_empty() => (current_catalog.clone(), qualifier.to_vec()),
                _ => (current_catalog.clone(), current_namespace),
            };
            let external = catalogs.contains_key(&catalog);
            let external_names: Vec<String> = catalogs.keys().cloned().collect();
            (catalog, namespace, external, external_names)
        };

        // The bridge is registered under the same name in DataFusion's catalog list; downcast
        // via the `Any` supertrait (`CatalogProvider` has no dedicated `as_any`). The
        // `provider` Arc is bound in each scope so the downcast reference stays valid.
        let mut evicted = false;
        if external {
            if let Some(provider) = self.ctx.catalog(&catalog) {
                let any: &dyn std::any::Any = provider.as_ref();
                if let Some(bridge) = any.downcast_ref::<catalog_bridge::OxidantCatalogProvider>() {
                    evicted = match namespace.last() {
                        // The bridge's schema providers are single-part namespaces (see
                        // `catalog_bridge` module docs) — the last segment is the key.
                        Some(ns) => bridge.evict_table(ns, table),
                        // Empty session namespace (KAN-84): no schema key — evict by bare name
                        // across every schema provider the bridge materialized.
                        None => bridge.evict_table_anywhere(table),
                    };
                }
            }
        } else if namespace.is_empty() {
            // Bare name, no current namespace, and the current catalog isn't external: sweep
            // every registered external catalog — evict by bare name wherever it is cached.
            for name in &external_names {
                if let Some(provider) = self.ctx.catalog(name) {
                    let any: &dyn std::any::Any = provider.as_ref();
                    if let Some(bridge) =
                        any.downcast_ref::<catalog_bridge::OxidantCatalogProvider>()
                    {
                        evicted = bridge.evict_table_anywhere(table) || evicted;
                    }
                }
            }
        }
        if evicted {
            self.catalog_version.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Whether `name` is qualified with a registered external catalog (`catalog.db.table`, or
    /// deeper). Used to bail out of the local-warehouse `CREATE TABLE ... USING <fmt>` lowerings
    /// (`spark_create_table::lower_create_table_using`/`lower_create_table_ctas`) when the target
    /// actually targets an external catalog (e.g. `CREATE TABLE glue.db.t USING parquet AS
    /// SELECT ...`) — otherwise that lowering would silently write to the local warehouse under
    /// the default catalog instead of routing to the external catalog's real
    /// `CatalogProvider::create_table` (via `catalog_bridge`'s `register_table`, which the
    /// un-qualified/no-`USING` CTAS path already reaches).
    ///
    /// Two things this deliberately gets right (each was a real bug in an earlier version):
    /// - **Arity**: only a name with 3+ dotted segments can be catalog-qualified at all — a bare
    ///   1-part name (`t`) or 2-part `schema.table` (the existing, tested local-warehouse shape,
    ///   e.g. `s.tab`) is always local, even if its first segment happens to spell a registered
    ///   catalog's name (e.g. a local schema named the same as some catalog `glue`).
    /// - **Case**: SQL unquoted identifiers are conventionally case-insensitive, but catalog names
    ///   are registered verbatim (`register_catalog`); comparing case-sensitively would silently
    ///   misroute e.g. `CREATE TABLE Glue.db.t ...` when the catalog was registered as `glue`.
    ///   This deliberately diverges from Spark's catalog-name matching (KAN-87 — exact keys for
    ///   v2 plugins, case-folding only for the session catalog): the cost of a false positive
    ///   here is a clean catalog not-found downstream, while a false negative silently writes a
    ///   catalog-targeted table into the local warehouse — the conservative direction wins for
    ///   DDL routing, unlike name RESOLUTION (which follows Spark exactly).
    fn name_targets_external_catalog(&self, name: &str) -> bool {
        let segments = split_name_segments(name);
        if segments.len() < 3 {
            return false;
        }
        let first = segments[0].trim_matches('`');
        self.oxidant_catalogs
            .lock()
            .expect("oxidant_catalogs poisoned")
            .keys()
            .any(|k| k.eq_ignore_ascii_case(first))
    }

    /// Serve a parsed catalog-listing/`SHOW` statement directly from the registered oxidant catalogs
    /// (and, for the built-in `spark_catalog`, the DataFusion bridge + [`Engine::created_tables`]).
    ///
    /// The output column names are load-bearing — a downstream gateway parser keys off them, and
    /// each shape matches Spark's own `SHOW …` schema:
    /// - `SHOW CATALOGS` → one `catalog` (Utf8) column;
    /// - `SHOW DATABASES`/`SHOW SCHEMAS`[ `IN <cat>`] → one `namespace` (Utf8) column;
    /// - `SHOW TABLES`[ `IN|FROM <cat>[.<db>]`][ `LIKE '<pattern>'`] → `namespace`/`tableName`/
    ///   `isTemporary` (Boolean, always false — oxidant's catalog-backed listings never distinguish);
    /// - `SHOW COLUMNS IN|FROM <table>[ IN|FROM <db>]` → one `col_name` (Utf8) column;
    /// - `SHOW VIEWS`[ `IN|FROM <db>`][ `LIKE '<pattern>'`] → `namespace`/`viewName`/`isTemporary`;
    /// - `SHOW TBLPROPERTIES <table>[('key')]` → `key`/`value` (Utf8) columns;
    /// - `SHOW TABLE EXTENDED [IN|FROM <db>] LIKE '<pattern>'` → `namespace`/`tableName`/
    ///   `isTemporary`/`information`;
    /// - `SHOW CREATE TABLE <table>[ AS SERDE]` → one `createtab_stmt` (Utf8) column — see
    ///   [`reconstruct_create_table_ddl`];
    /// - `SHOW PARTITIONS <table>[ PARTITION (…)]` → one `partition` (Utf8) column;
    /// - `SHOW FUNCTIONS[ LIKE '<pattern>']` → one `function` (Utf8) column.
    ///
    /// An unknown catalog/namespace/pattern yields an empty (0-row) result of the right shape
    /// rather than an error for the listing forms (`Catalogs`/`Databases`/`Tables`/`Views`/
    /// `Partitions`); a single-table lookup that can't resolve (`Columns`/`TblProperties`/
    /// `CreateTable`) returns a `TABLE_OR_VIEW_NOT_FOUND`-style [`Error::Plan`] instead, matching
    /// Spark's own analysis error for those forms.
    async fn run_show(&self, show: &ShowStmt) -> Result<Vec<RecordBatch>> {
        match show {
            ShowStmt::Catalogs => {
                let mut names: Vec<String> = self
                    .oxidant_catalogs
                    .lock()
                    .expect("oxidant_catalogs poisoned")
                    .keys()
                    .cloned()
                    .collect();
                names.push(oxidant_catalog::DEFAULT_CATALOG.to_string());
                names.sort();
                names.dedup();
                Ok(vec![single_col_batch("catalog", names)?])
            }
            ShowStmt::Databases { catalog: None } => {
                // The built-in catalog's own namespaces, plus the union of every registered
                // external catalog's top-level namespaces.
                let mut namespaces = self.builtin_namespaces();
                let cats: Vec<Arc<dyn oxidant_catalog::CatalogProvider>> = self
                    .oxidant_catalogs
                    .lock()
                    .expect("oxidant_catalogs poisoned")
                    .values()
                    .cloned()
                    .collect();
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
                let namespaces = if cat == oxidant_catalog::DEFAULT_CATALOG {
                    self.builtin_namespaces()
                } else {
                    match self.oxidant_catalog(cat) {
                        Some(p) => p
                            .list_namespaces(&[])
                            .await
                            .map_err(|e| Error::Execution(e.to_string()))?
                            .into_iter()
                            .map(|ns| ns.join("."))
                            .collect(),
                        // Unknown catalog → empty result, not an error.
                        None => Vec::new(),
                    }
                };
                Ok(vec![namespace_batch(namespaces)?])
            }
            ShowStmt::Tables {
                catalog,
                database,
                like,
            } => {
                let (cat, db) = match catalog {
                    Some(c) => (c.clone(), database.clone()),
                    // Bare `SHOW TABLES`/`SHOW TABLES LIKE '…'` — default to the session's current
                    // catalog + (last segment of the) current namespace.
                    None => {
                        let (cur_cat, cur_ns) = self.current_catalog_and_namespace();
                        let ns = database.clone().or_else(|| cur_ns.into_iter().next_back());
                        (cur_cat, ns)
                    }
                };
                let mut rows: Vec<(String, String)> = Vec::new();
                if let Some(p) = self.oxidant_catalog(&cat) {
                    match &db {
                        // `SHOW TABLES IN <cat>.<db>` — tables directly in that namespace.
                        Some(d) => {
                            let tables = p
                                .list_tables(std::slice::from_ref(d))
                                .await
                                .map_err(|e| Error::Execution(e.to_string()))?;
                            for t in tables {
                                rows.push((d.clone(), t));
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
                } else if cat == oxidant_catalog::DEFAULT_CATALOG {
                    // The built-in catalog isn't a `oxidant_catalog::CatalogProvider` — its tables
                    // (temp views + `CREATE TABLE … USING` tables) live on the DataFusion bridge.
                    let namespaces: Vec<String> = match &db {
                        Some(d) => vec![d.clone()],
                        None => self.builtin_namespaces(),
                    };
                    for ns in namespaces {
                        for t in self.builtin_table_names(&ns) {
                            rows.push((ns.clone(), t));
                        }
                    }
                }
                if let Some(pat) = like {
                    rows.retain(|(_, t)| sql_like_match(pat, t));
                }
                Ok(vec![tables_batch(rows)?])
            }
            ShowStmt::Columns { table, namespace } => {
                let mut segments = parse_qualified_name(table);
                if let Some(ns) = namespace {
                    // An explicit `FROM <db>` clause names the namespace directly — keep only the
                    // table's own bare (unqualified) name and requalify it under `ns`.
                    let bare_table = segments.pop().unwrap_or_default();
                    segments = parse_qualified_name(ns);
                    segments.push(bare_table);
                }
                let (cat, ns, tbl) = self.resolve_table_ref(&segments);
                // `USE CATALOG <external>` leaves no current database (KAN-84): the schema
                // probe below can't be qualified into SQL (`{cat}..{tbl}` is a syntax error).
                // Probe the provider with the empty namespace instead: its not-found Plan
                // (Glue/Hive: "needs a database …") surfaces Spark-shaped via
                // `load_catalog_table` as `[TABLE_OR_VIEW_NOT_FOUND] The table or view
                // `cat.tbl` cannot be found`; backend failures (Io/…) pass through unchanged.
                // In-tree providers all require a namespace, so this errors; a flat provider
                // accepting the empty namespace falls through with the parts joined without
                // the empty segment.
                if cat != oxidant_catalog::DEFAULT_CATALOG && ns.is_empty() {
                    self.load_catalog_table(&cat, &ns, &tbl).await?;
                }
                let qualified = {
                    let mut parts: Vec<&str> = if cat == oxidant_catalog::DEFAULT_CATALOG {
                        Vec::new()
                    } else {
                        vec![cat.as_str()]
                    };
                    parts.extend(ns.iter().map(String::as_str));
                    parts.push(&tbl);
                    join_table_name_parts(parts)
                };
                let schema = self.schema(&format!("SELECT * FROM {qualified}")).await?;
                let names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
                Ok(vec![single_col_batch("col_name", names)?])
            }
            ShowStmt::Views { database, like } => {
                let (_, cur_ns) = self.current_catalog_and_namespace();
                let ns = database.clone().unwrap_or_else(|| {
                    cur_ns
                        .into_iter()
                        .next_back()
                        .unwrap_or_else(|| oxidant_catalog::DEFAULT_NAMESPACE.to_string())
                });
                let temp_set = self.temp_views.lock().expect("temp_views poisoned").clone();
                let mut names: HashSet<String> = HashSet::new();
                // Session temp views only apply to a bare `SHOW VIEWS`/`LIKE …` — an explicit
                // `IN|FROM <db>` clause names a persistent-view namespace, which temp views (a
                // session-global namespace of their own) never belong to.
                if database.is_none() {
                    names.extend(temp_set.iter().cloned());
                }
                let default = self.default_catalog_name();
                if let Some(cat) = self.ctx.catalog(&default) {
                    if let Some(schema) = cat.schema(&ns) {
                        for t in schema.table_names() {
                            if let Ok(Some(datafusion::datasource::TableType::View)) =
                                schema.table_type(&t).await
                            {
                                names.insert(t);
                            }
                        }
                    }
                }
                let mut rows: Vec<(String, String, bool)> = names
                    .into_iter()
                    .map(|n| {
                        let is_temp = temp_set.contains(&n);
                        (ns.clone(), n, is_temp)
                    })
                    .collect();
                if let Some(pat) = like {
                    rows.retain(|(_, n, _)| sql_like_match(pat, n));
                }
                rows.sort_by(|a, b| a.1.cmp(&b.1));
                Ok(vec![views_batch(rows)?])
            }
            ShowStmt::TblProperties { table, key } => {
                let segments = parse_qualified_name(table);
                let (cat, ns, tbl) = self.resolve_table_ref(&segments);
                let qualified = join_table_name_parts(
                    [cat.as_str()]
                        .into_iter()
                        .chain(ns.iter().map(String::as_str))
                        .chain([tbl.as_str()]),
                );
                let props: HashMap<String, String> = if cat == oxidant_catalog::DEFAULT_CATALOG {
                    self.created_table_meta(&tbl)
                        .map(|m| m.properties)
                        .unwrap_or_default()
                } else {
                    self.load_catalog_table(&cat, &ns, &tbl).await?.properties
                };
                let rows: Vec<(String, String)> = match key {
                    Some(k) => match props.get(k) {
                        Some(v) => vec![(k.clone(), redact_property_value(k, v))],
                        None => vec![(
                            k.clone(),
                            format!("Table {qualified} does not have property: {k}"),
                        )],
                    },
                    None => {
                        let mut kv: Vec<(String, String)> = props
                            .into_iter()
                            .map(|(k, v)| {
                                let redacted = redact_property_value(&k, &v);
                                (k, redacted)
                            })
                            .collect();
                        kv.sort_by(|a, b| a.0.cmp(&b.0));
                        kv
                    }
                };
                Ok(vec![key_value_batch(rows)?])
            }
            ShowStmt::TableExtended { database, like } => {
                let (cur_cat, cur_ns) = self.current_catalog_and_namespace();
                let ns = database.clone().unwrap_or_else(|| {
                    cur_ns
                        .into_iter()
                        .next_back()
                        .unwrap_or_else(|| oxidant_catalog::DEFAULT_NAMESPACE.to_string())
                });
                let mut names: Vec<String> = self.builtin_table_names(&ns);
                names.retain(|t| sql_like_match(like, t));
                let mut rows: Vec<(String, String, bool, String)> = Vec::new();
                for name in names {
                    let info = match self.created_table_meta(&name) {
                        Some(meta) => format!(
                            "Catalog: {cur_cat}\nDatabase: {ns}\nTable: {name}\nProvider: {}\nComment: {}\nTable Properties: [{}]\n",
                            meta.format,
                            meta.comment.clone().unwrap_or_default(),
                            format_properties(&meta.properties)
                        ),
                        None => format!("Catalog: {cur_cat}\nDatabase: {ns}\nTable: {name}\n"),
                    };
                    rows.push((ns.clone(), name, false, info));
                }
                Ok(vec![table_extended_batch(rows)?])
            }
            ShowStmt::CreateTable { table } => {
                let segments = parse_qualified_name(table);
                let (cat, ns, tbl) = self.resolve_table_ref(&segments);
                let qualified = join_table_name_parts(
                    [cat.as_str()]
                        .into_iter()
                        .chain(ns.iter().map(String::as_str))
                        .chain([tbl.as_str()]),
                );
                if cat == oxidant_catalog::DEFAULT_CATALOG {
                    let meta = self.created_table_meta(&tbl).ok_or_else(|| {
                        Error::Plan(format!(
                            "[TABLE_OR_VIEW_NOT_FOUND] The table or view `{qualified}` cannot be \
                             found"
                        ))
                    })?;
                    let schema = self.schema(&format!("SELECT * FROM {tbl}")).await?;
                    let ddl = reconstruct_create_table_ddl(
                        &qualified,
                        &schema,
                        &meta.format,
                        &meta.partition_columns,
                        None,
                        meta.comment.as_deref(),
                        &meta.properties,
                    );
                    Ok(vec![single_col_batch("createtab_stmt", vec![ddl])?])
                } else {
                    let md = self.load_catalog_table(&cat, &ns, &tbl).await?;
                    let schema = md
                        .schema
                        .clone()
                        .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));
                    let ddl = reconstruct_create_table_ddl(
                        &qualified,
                        &schema,
                        table_format_str(md.format),
                        &md.partition_columns,
                        Some(&md.location),
                        md.comment.as_deref(),
                        &md.properties,
                    );
                    Ok(vec![single_col_batch("createtab_stmt", vec![ddl])?])
                }
            }
            ShowStmt::Partitions { table, spec } => {
                let segments = parse_qualified_name(table);
                let (cat, ns, tbl) = self.resolve_table_ref(&segments);
                if cat == oxidant_catalog::DEFAULT_CATALOG {
                    // Local `CREATE TABLE … USING` tables never carry partition info (v1 doesn't
                    // lower `PARTITIONED BY`) — empty, not an error.
                    return Ok(vec![single_col_batch("partition", Vec::new())?]);
                }
                let md = self.load_catalog_table(&cat, &ns, &tbl).await?;
                if md.partition_columns.is_empty() {
                    return Ok(vec![single_col_batch("partition", Vec::new())?]);
                }
                let parts = list_hive_partitions(&md.location, &md.partition_columns, spec);
                Ok(vec![single_col_batch("partition", parts)?])
            }
            ShowStmt::Functions { like } => {
                let mut names: HashSet<String> = self
                    .udf_registry
                    .lock()
                    .expect("udf_registry poisoned")
                    .names()
                    .into_iter()
                    .collect();
                let state = self.ctx.state();
                names.extend(state.scalar_functions().keys().cloned());
                names.extend(state.aggregate_functions().keys().cloned());
                names.extend(state.window_functions().keys().cloned());
                let mut list: Vec<String> = names.into_iter().collect();
                if let Some(pat) = like {
                    list.retain(|n| sql_like_match(pat, n));
                }
                list.sort();
                Ok(vec![single_col_batch("function", list)?])
            }
        }
    }

    /// Serve a parsed `DESCRIBE`/`DESC` statement directly, mirroring [`Engine::run_show`]'s
    /// interception style and data sources (`created_tables` for locally-created tables,
    /// `oxidant_catalog::TableMetadata` for catalog-backed ones, [`Engine::schema`] for column
    /// resolution). Output shapes:
    /// - `Table`/`Query` → `struct<col_name:string,data_type:string,comment:string>`, matching
    ///   Spark's own `DESCRIBE` shape (`spark-tests/results/describe.sql.out`,
    ///   `describe-query.sql.out`). `EXTENDED`/`FORMATTED` append a blank row plus a
    ///   `# Detailed Table Information` block with whatever fields oxidant can answer; unavailable
    ///   fields (`Owner`, `Created Time`, `Serde Library`, …) are omitted rather than fabricated.
    ///   `AS JSON` (only legal combined with `EXTENDED`/`FORMATTED`, matching Spark's
    ///   `DESCRIBE_JSON_NOT_EXTENDED` rule) instead returns a single `json_metadata` column with a
    ///   best-effort JSON object of the same known fields.
    /// - `Database`/`Catalog` → two-column `info_name`/`info_value`.
    /// - `Function` → one `function_desc` column, one line per fact known about the function.
    async fn run_describe(&self, describe: &DescribeStmt) -> Result<Vec<RecordBatch>> {
        match describe {
            DescribeStmt::Table {
                name,
                extended,
                partition: _partition,
                as_json,
            } => {
                if *as_json && !*extended {
                    return Err(Error::Plan(
                        "[DESCRIBE_JSON_NOT_EXTENDED] DESC TABLE ... AS JSON is only supported \
                         with EXTENDED/FORMATTED"
                            .into(),
                    ));
                }
                let segments = parse_qualified_name(name);
                let (cat, ns, tbl) = self.resolve_table_ref(&segments);
                // Same KAN-84 empty-namespace handling as `SHOW COLUMNS` (see [`Engine::run_show`]):
                // probe the provider with the empty namespace — its not-found Plan surfaces as
                // `TABLE_OR_VIEW_NOT_FOUND` via `load_catalog_table`, backend failures pass
                // through — rather than building `{cat}..{tbl}` SQL.
                if cat != oxidant_catalog::DEFAULT_CATALOG && ns.is_empty() {
                    self.load_catalog_table(&cat, &ns, &tbl).await?;
                }
                let qualified = {
                    let mut parts: Vec<&str> = if cat == oxidant_catalog::DEFAULT_CATALOG {
                        Vec::new()
                    } else {
                        vec![cat.as_str()]
                    };
                    parts.extend(ns.iter().map(String::as_str));
                    parts.push(&tbl);
                    join_table_name_parts(parts)
                };
                let schema = self.schema(&format!("SELECT * FROM {qualified}")).await?;
                // Metadata for the detailed/JSON forms: local `CREATE TABLE ... USING` tables read
                // from `created_tables` (format known → reported as `MANAGED`, oxidant never lowers an
                // explicit `LOCATION`); catalog-backed tables read from `TableMetadata` (always
                // reported as `EXTERNAL`, since they live outside oxidant's own managed warehouse).
                let (fmt_opt, comment, properties, partition_columns, location, is_local) =
                    if cat == oxidant_catalog::DEFAULT_CATALOG {
                        match self.created_table_meta(&tbl) {
                            Some(meta) => (
                                Some(meta.format),
                                meta.comment,
                                meta.properties,
                                meta.partition_columns,
                                None,
                                true,
                            ),
                            None => (None, None, HashMap::new(), Vec::new(), None, true),
                        }
                    } else {
                        let md = self.load_catalog_table(&cat, &ns, &tbl).await?;
                        (
                            Some(table_format_str(md.format).to_string()),
                            md.comment,
                            md.properties,
                            md.partition_columns,
                            Some(md.location),
                            false,
                        )
                    };
                if *as_json {
                    let json = serde_json::json!({
                        "table_name": tbl,
                        "catalog_name": cat,
                        "namespace": ns,
                        "columns": schema
                            .fields()
                            .iter()
                            .map(|f| serde_json::json!({
                                "name": f.name(),
                                "type": spark_ddl_type(f.data_type()).to_lowercase(),
                                "nullable": f.is_nullable(),
                            }))
                            .collect::<Vec<_>>(),
                        "location": location,
                        "type": if is_local { "MANAGED" } else { "EXTERNAL" },
                        "provider": fmt_opt,
                        "comment": comment,
                        "table_properties": properties,
                        "partition_columns": partition_columns,
                    });
                    return Ok(vec![single_col_batch(
                        "json_metadata",
                        vec![json.to_string()],
                    )?]);
                }
                let mut rows: Vec<(String, String, String)> = schema
                    .fields()
                    .iter()
                    .map(|f| {
                        (
                            f.name().clone(),
                            spark_ddl_type(f.data_type()).to_lowercase(),
                            String::new(),
                        )
                    })
                    .collect();
                if !partition_columns.is_empty() {
                    rows.push((
                        "# Partition Information".to_string(),
                        String::new(),
                        String::new(),
                    ));
                    rows.push((
                        "# col_name".to_string(),
                        "data_type".to_string(),
                        "comment".to_string(),
                    ));
                    for pc in &partition_columns {
                        let dtype = schema
                            .field_with_name(pc)
                            .map(|f| spark_ddl_type(f.data_type()).to_lowercase())
                            .unwrap_or_default();
                        rows.push((pc.clone(), dtype, String::new()));
                    }
                }
                if *extended {
                    rows.push((String::new(), String::new(), String::new()));
                    rows.push((
                        "# Detailed Table Information".to_string(),
                        String::new(),
                        String::new(),
                    ));
                    rows.push(("Catalog".to_string(), cat.clone(), String::new()));
                    rows.push(("Database".to_string(), ns.join("."), String::new()));
                    rows.push(("Table".to_string(), tbl.clone(), String::new()));
                    if let Some(fmt) = &fmt_opt {
                        rows.push((
                            "Type".to_string(),
                            if is_local { "MANAGED" } else { "EXTERNAL" }.to_string(),
                            String::new(),
                        ));
                        rows.push(("Provider".to_string(), fmt.clone(), String::new()));
                    }
                    if let Some(c) = &comment {
                        rows.push(("Comment".to_string(), c.clone(), String::new()));
                    }
                    if !properties.is_empty() {
                        rows.push((
                            "Table Properties".to_string(),
                            format!("[{}]", format_properties(&properties)),
                            String::new(),
                        ));
                    }
                    if let Some(loc) = &location {
                        rows.push(("Location".to_string(), loc.clone(), String::new()));
                    }
                    if !partition_columns.is_empty() {
                        rows.push((
                            "Partition Columns".to_string(),
                            format!("[{}]", partition_columns.join(", ")),
                            String::new(),
                        ));
                    }
                }
                Ok(vec![describe_batch(rows)?])
            }
            DescribeStmt::Query { stmt } => {
                let schema = self.schema(stmt).await?;
                let rows: Vec<(String, String, String)> = schema
                    .fields()
                    .iter()
                    .map(|f| {
                        (
                            f.name().clone(),
                            spark_ddl_type(f.data_type()).to_lowercase(),
                            String::new(),
                        )
                    })
                    .collect();
                Ok(vec![describe_batch(rows)?])
            }
            DescribeStmt::Database { catalog, name } => {
                let cat = catalog
                    .clone()
                    .unwrap_or_else(|| self.current_catalog_and_namespace().0);
                let exists = if cat == oxidant_catalog::DEFAULT_CATALOG {
                    self.builtin_namespaces().iter().any(|n| n == name)
                } else {
                    match self.oxidant_catalog(&cat) {
                        Some(p) => p
                            .namespace_exists(std::slice::from_ref(name))
                            .await
                            .unwrap_or(false),
                        None => false,
                    }
                };
                if !exists {
                    return Err(Error::Plan(format!(
                        "[SCHEMA_NOT_FOUND] The schema `{name}` cannot be found"
                    )));
                }
                // oxidant's `CatalogProvider` trait has no namespace-level comment/location/owner
                // concept, so those fields are left blank rather than fabricated.
                let rows = vec![
                    ("Namespace Name".to_string(), name.clone()),
                    ("Comment".to_string(), String::new()),
                    ("Location".to_string(), String::new()),
                    ("Owner".to_string(), String::new()),
                ];
                Ok(vec![two_col_batch("info_name", "info_value", rows)?])
            }
            DescribeStmt::Catalog { name } => {
                if !self.catalog_registered(name) {
                    return Err(Error::Plan(format!(
                        "[CATALOG_NOT_FOUND] The catalog `{name}` not found"
                    )));
                }
                Ok(vec![two_col_batch(
                    "info_name",
                    "info_value",
                    vec![("Catalog Name".to_string(), name.clone())],
                )?])
            }
            DescribeStmt::Function { name, extended } => {
                let bare = parse_qualified_name(name)
                    .into_iter()
                    .next_back()
                    .unwrap_or_else(|| name.clone());
                let mut rows: Vec<String> = Vec::new();
                if let Some(def) = self
                    .udf_registry
                    .lock()
                    .expect("udf_registry poisoned")
                    .get(&bare)
                {
                    rows.push(format!("Function: {}", def.name));
                    rows.push("Class: SQL UDF".to_string());
                    rows.push(format!(
                        "Usage: {}({}) RETURNS {}",
                        def.name,
                        def.param_names.join(", "),
                        def.return_type
                    ));
                    if *extended {
                        rows.push(format!(
                            "Extended Usage: {}",
                            def.sql_body.clone().unwrap_or_default()
                        ));
                    }
                } else {
                    let state = self.ctx.state();
                    let lower = bare.to_lowercase();
                    let is_builtin = state.scalar_functions().contains_key(lower.as_str())
                        || state.aggregate_functions().contains_key(lower.as_str())
                        || state.window_functions().contains_key(lower.as_str());
                    if !is_builtin {
                        return Err(Error::Plan(format!(
                            "[UNRESOLVED_ROUTINE] Cannot resolve function `{bare}`"
                        )));
                    }
                    rows.push(format!("Function: {bare}"));
                    rows.push("Class: N/A".to_string());
                    rows.push("Usage: N/A".to_string());
                    if *extended {
                        rows.push("Extended Usage: N/A".to_string());
                    }
                }
                Ok(vec![single_col_batch("function_desc", rows)?])
            }
        }
    }

    /// Look up a registered oxidant catalog by name (case-sensitive, as registered).
    fn oxidant_catalog(&self, name: &str) -> Option<Arc<dyn oxidant_catalog::CatalogProvider>> {
        self.oxidant_catalogs
            .lock()
            .expect("oxidant_catalogs poisoned")
            .get(name)
            .cloned()
    }

    /// Whether `name` is a registered catalog — either an external [`oxidant_catalog::CatalogProvider`]
    /// (`register_catalog`) or the built-in `spark_catalog`.
    ///
    /// KAN-87 — matching follows Spark's `CatalogManager.catalog` exactly: ONLY the session
    /// catalog name matches case-insensitively (`name.equalsIgnoreCase(SESSION_CATALOG_NAME)`);
    /// v2 plugin catalog names are exact map keys. So `USE CATALOG SPARK_CATALOG` resolves, but
    /// a case-mismatched external catalog (`Glue` for registered `glue`) is `CATALOG_NOT_FOUND`.
    fn catalog_registered(&self, name: &str) -> bool {
        self.canonical_catalog_name(name).is_some()
    }

    /// The registered casing for a catalog name: the canonical `spark_catalog` for a
    /// case-insensitive builtin match (KAN-87 — backend callers compare the current catalog
    /// against `DEFAULT_CATALOG` case-sensitively, so the user's casing is never stored), else
    /// the name as given (external names are exact map keys, so it IS the registered casing).
    /// `None` when unregistered.
    fn canonical_catalog_name(&self, name: &str) -> Option<String> {
        if name.eq_ignore_ascii_case(oxidant_catalog::DEFAULT_CATALOG) {
            return Some(oxidant_catalog::DEFAULT_CATALOG.to_string());
        }
        self.oxidant_catalog(name).map(|_| name.to_string())
    }

    /// Apply a parsed `USE` statement, updating the session's current catalog/namespace.
    /// `USE` produces no result rows (Spark's `struct<>`).
    ///
    /// KAN-84 — `USE CATALOG <catalog>` follows Spark's `CatalogManager.setCurrentCatalog`:
    /// switching catalogs CLEARS the current-namespace override, and the switch is a no-op
    /// when the catalog is already current (a `USE glue.db1` … `USE CATALOG glue` sequence
    /// keeps `db1`, matching Spark). Spark then resolves bare names against the new catalog's
    /// `defaultNamespace()` — `["default"]` for the builtin session catalog. oxidant's
    /// external providers carry no default-namespace metadata, so switching to an external
    /// catalog leaves the namespace EMPTY ("no database selected"): bare-name SHOW/DESCRIBE
    /// forms then probe the provider with the empty namespace, whose not-found Plan surfaces
    /// Spark-shaped as `[TABLE_OR_VIEW_NOT_FOUND] The table or view `cat.tbl` cannot be found`
    /// (via `load_catalog_table`; backend failures pass through unchanged), and the KAN-81
    /// sizing walk restricts its namespace search to that one catalog.
    async fn run_use(&self, stmt: &UseStmt) -> Result<Vec<RecordBatch>> {
        match stmt {
            UseStmt::Catalog { catalog } => {
                let Some(canonical) = self.canonical_catalog_name(catalog) else {
                    return Err(Error::Plan(format!(
                        "[CATALOG_NOT_FOUND] The catalog `{catalog}` not found"
                    )));
                };
                let mut current = self.current.lock().expect("current poisoned");
                // The same-catalog no-op compares CANONICAL names (KAN-87): re-`USE CATALOG`
                // of the builtin under different casing is a no-op, not a namespace reset —
                // Spark's raw case-sensitive comparison there reads as an implementation
                // accident, since its `catalog()` folds the session-catalog case anyway.
                if current.0 != canonical {
                    current.0 = canonical.clone();
                    current.1 = if canonical == oxidant_catalog::DEFAULT_CATALOG {
                        vec![oxidant_catalog::DEFAULT_NAMESPACE.to_string()]
                    } else {
                        Vec::new()
                    };
                }
            }
            UseStmt::Namespace { catalog, namespace } => {
                let canonical = match catalog {
                    Some(cat) => Some(self.canonical_catalog_name(cat).ok_or_else(|| {
                        Error::Plan(format!("[CATALOG_NOT_FOUND] The catalog `{cat}` not found"))
                    })?),
                    None => None,
                };
                let target_catalog = canonical
                    .clone()
                    .unwrap_or_else(|| self.current_catalog_and_namespace().0);
                // KAN-86: the namespace must exist (Spark's `setCurrentNamespace` →
                // `SCHEMA_NOT_FOUND`) — validated BEFORE any state changes, so a failed USE
                // leaves the session untouched.
                let namespace = self.validate_namespace(&target_catalog, namespace).await?;
                let mut current = self.current.lock().expect("current poisoned");
                if let Some(cat) = canonical {
                    current.0 = cat;
                }
                current.1 = namespace;
            }
        }
        Ok(vec![])
    }

    /// Validate a `USE` target namespace exists — Spark's `setCurrentNamespace` runs
    /// `assertNamespaceExist` and raises `[SCHEMA_NOT_FOUND]` (KAN-86) — and return the
    /// namespace to STORE: the builtin catalog matches case-insensitively and stores the
    /// registered casing (Spark's v1 `formatDatabaseName` fold, `caseSensitive=false`
    /// default), so `USE DEFAULT` works and lands on `["default"]`; external catalogs match
    /// exactly (Spark's v2 `namespaceExists` is exact) and store as given.
    ///
    /// External validation walks level-by-level via `list_namespaces` (cheap in-process since
    /// KAN-82), so a multi-part namespace on a single-level provider (Glue) fails
    /// `SCHEMA_NOT_FOUND` at the second level. A backend failure (`Error::Io`) propagates — a
    /// namespace is never silently accepted when validation itself fails.
    async fn validate_namespace(&self, catalog: &str, namespace: &[String]) -> Result<Vec<String>> {
        if namespace.is_empty() {
            return Ok(namespace.to_vec());
        }
        let not_found = || {
            let qualified = std::iter::once(catalog)
                .chain(namespace.iter().map(String::as_str))
                .map(|part| format!("`{part}`"))
                .collect::<Vec<_>>()
                .join(".");
            Error::Plan(format!(
                "[SCHEMA_NOT_FOUND] The schema {qualified} cannot be found"
            ))
        };
        if catalog == oxidant_catalog::DEFAULT_CATALOG {
            let want = namespace.join(".");
            if let Some(registered) = self
                .builtin_namespaces()
                .into_iter()
                .find(|n| n.eq_ignore_ascii_case(&want))
            {
                return Ok(vec![registered]);
            }
            return Err(not_found());
        }
        let Some(provider) = self.oxidant_catalog(catalog) else {
            // Unreachable: the caller validates the catalog first.
            return Err(not_found());
        };
        let mut prefix: Vec<String> = Vec::new();
        for segment in namespace {
            let children = provider.list_namespaces(&prefix).await?;
            prefix.push(segment.clone());
            if !children.iter().any(|child| child == &prefix) {
                return Err(not_found());
            }
        }
        Ok(namespace.to_vec())
    }

    /// The session's current catalog + current namespace, set by `USE` (default:
    /// `spark_catalog`/`default`). Consulted by [`Engine::run_show`] (and later `DESCRIBE` work) to
    /// default unqualified catalog/namespace-relative names.
    pub fn current_catalog_and_namespace(&self) -> (String, Vec<String>) {
        self.current.lock().expect("current poisoned").clone()
    }

    /// A handle to this engine whose current catalog/namespace is the per-session cell for
    /// `session_id` — created on first use, seeded from the engine's session default catalog
    /// (`spark.sql.defaultCatalog`; `["default"]` namespace for the builtin catalog, empty for
    /// an external one — the KAN-84 `USE CATALOG` semantics). Everything else is shared with
    /// this handle: catalogs, registered tables, UDFs, the estimate cache, managed dirs.
    ///
    /// KAN-85: the Connect server holds one shared `Arc<Engine>`; per-request handles from this
    /// method keep one session's `USE` from leaking into every other session.
    pub fn for_session(&self, session_id: &str) -> Engine {
        let cell = {
            let mut state = self.sessions.lock().expect("sessions poisoned");
            let default_catalog = state.default_catalog.clone();
            state
                .cells
                .entry(session_id.to_string())
                .or_insert_with(|| {
                    let namespace = if default_catalog == oxidant_catalog::DEFAULT_CATALOG {
                        vec![oxidant_catalog::DEFAULT_NAMESPACE.to_string()]
                    } else {
                        Vec::new()
                    };
                    Arc::new(Mutex::new((default_catalog, namespace)))
                })
                .clone()
        };
        Engine {
            ctx: self.ctx.clone(),
            dirs: self.dirs.clone(),
            temp_views: self.temp_views.clone(),
            oxidant_catalogs: self.oxidant_catalogs.clone(),
            udf_registry: self.udf_registry.clone(),
            current: cell,
            sessions: self.sessions.clone(),
            created_tables: self.created_tables.clone(),
            require_lakehouse_snapshot_pins: self.require_lakehouse_snapshot_pins.clone(),
            memory_pool_bytes: self.memory_pool_bytes,
            plan_time_smj_reroutes: self.plan_time_smj_reroutes.clone(),
            measured_stats_registrations: self.measured_stats_registrations.clone(),
            plan_cache_id: self.plan_cache_id,
            catalog_version: self.catalog_version.clone(),
            pool_activity: self.pool_activity.clone(),
            table_bytes_cache: self.table_bytes_cache.clone(),
        }
    }

    /// Drop the per-session state cell for `session_id` (Connect `ReleaseSession`). In-flight
    /// requests hold their own `Arc` of the cell, so evicting mid-flight is safe — they finish
    /// on their handle, and the next `for_session` for the id re-seeds from the default.
    pub fn drop_session(&self, session_id: &str) {
        self.sessions
            .lock()
            .expect("sessions poisoned")
            .cells
            .remove(session_id);
    }

    /// Set the catalog NEW sessions seed from (`spark.sql.defaultCatalog`); existing sessions
    /// keep their (possibly `USE`-adjusted) state. Errors if the catalog isn't registered.
    pub fn set_default_catalog(&self, catalog: &str) -> Result<()> {
        let canonical = self.canonical_catalog_name(catalog).ok_or_else(|| {
            Error::Plan(format!(
                "[CATALOG_NOT_FOUND] The catalog `{catalog}` not found"
            ))
        })?;
        self.sessions
            .lock()
            .expect("sessions poisoned")
            .default_catalog = canonical;
        Ok(())
    }

    /// Spark Catalog RPC `setCurrentCatalog`: identical semantics to SQL `USE CATALOG <name>`
    /// (KAN-84 namespace reset + KAN-87 matching) over the same per-session state (KAN-85).
    pub async fn set_current_catalog(&self, catalog: &str) -> Result<()> {
        self.run_use(&UseStmt::Catalog {
            catalog: catalog.to_string(),
        })
        .await
        .map(|_| ())
    }

    /// Spark Catalog RPC `setCurrentDatabase`: identical semantics to SQL `USE <namespace>`
    /// (KAN-86 existence validation included) over the same per-session state (KAN-85). An
    /// empty/blank database name is rejected (Spark errors; an empty namespace would otherwise
    /// slip past validation and read as "no database selected").
    pub async fn set_current_namespace(&self, namespace: &str) -> Result<()> {
        let segments = parse_qualified_name(namespace);
        if segments.is_empty() {
            return Err(Error::Plan(format!(
                "[SCHEMA_NOT_FOUND] The schema `{namespace}` cannot be found"
            )));
        }
        self.run_use(&UseStmt::Namespace {
            catalog: None,
            namespace: segments,
        })
        .await
        .map(|_| ())
    }

    /// Resolve a (possibly qualified) dotted name — as returned by [`parse_qualified_name`] — to an
    /// explicit `(catalog, namespace, table)` triple, defaulting unspecified parts from
    /// [`Engine::current_catalog_and_namespace`]. Mirrors Spark's own multi-part name resolution:
    /// - `[table]` (unqualified) → current catalog, current (possibly multi-part) namespace;
    /// - `[ns, table]` → current catalog, namespace `[ns]` (overrides only the last namespace
    ///   segment, matching Spark's `USE`d-database convention);
    /// - `[cat, ns.., table]` (3+ segments) → every segment explicit.
    ///
    /// Used by every single-table `SHOW`/`DESCRIBE` form (`Columns`, `TblProperties`, `CreateTable`,
    /// `Partitions`, …) so they all default unqualified names the same way.
    fn resolve_table_ref(&self, segments: &[String]) -> (String, Vec<String>, String) {
        let (cur_cat, cur_ns) = self.current_catalog_and_namespace();
        match segments.len() {
            0 => (cur_cat, cur_ns, String::new()),
            1 => (cur_cat, cur_ns, segments[0].clone()),
            2 => (cur_cat, vec![segments[0].clone()], segments[1].clone()),
            _ => {
                let last = segments.len() - 1;
                (
                    segments[0].clone(),
                    segments[1..last].to_vec(),
                    segments[last].clone(),
                )
            }
        }
    }

    /// Resolve one table's [`oxidant_catalog::TableMetadata`] from a registered external catalog,
    /// mapping an unregistered catalog name onto a `TABLE_OR_VIEW_NOT_FOUND`-style [`Error::Plan`]
    /// — the shape every caller (`SHOW COLUMNS`/`TBLPROPERTIES`/`CREATE TABLE`/`PARTITIONS`) wants
    /// for a table it can't resolve.
    ///
    /// A `load_table` failure is *not* blanket-mapped to "not found": providers (e.g. Glue's
    /// `classify_glue_failure`) already distinguish a genuine "doesn't exist" ([`Error::Plan`])
    /// from a real backend failure — auth, throttling, network — surfaced as [`Error::Io`]/
    /// [`Error::Execution`]/[`Error::Unsupported`]. Collapsing the latter into "not found" would
    /// hide the real cause from the user; only an already-`Plan` error is rewritten to the
    /// qualified `TABLE_OR_VIEW_NOT_FOUND` message, everything else passes through unchanged.
    async fn load_catalog_table(
        &self,
        catalog: &str,
        namespace: &[String],
        table: &str,
    ) -> Result<oxidant_catalog::TableMetadata> {
        let qualified = join_table_name_parts(
            [catalog]
                .into_iter()
                .chain(namespace.iter().map(String::as_str))
                .chain([table]),
        );
        let provider = self.oxidant_catalog(catalog).ok_or_else(|| {
            Error::Plan(format!(
                "[TABLE_OR_VIEW_NOT_FOUND] The table or view `{qualified}` cannot be found"
            ))
        })?;
        provider
            .load_table(namespace, table)
            .await
            .map_err(|e| match e {
                Error::Plan(_) => Error::Plan(format!(
                    "[TABLE_OR_VIEW_NOT_FOUND] The table or view `{qualified}` cannot be found"
                )),
                other => other,
            })
    }

    /// Look up the [`CreatedTableMeta`] captured for a table created locally via
    /// `CREATE TABLE ... USING <fmt>` (including CTAS), keyed by the table name as written in the
    /// `CREATE TABLE` statement. Returns `None` for catalog-backed (Hive/Glue) tables and for any
    /// name never seen by a successful local `CREATE TABLE`. Consumed by [`Engine::run_show`]'s
    /// `SHOW CREATE TABLE`/`SHOW TBLPROPERTIES`/`SHOW TABLE EXTENDED` handling (and later
    /// `DESCRIBE EXTENDED`).
    pub fn created_table_meta(&self, name: &str) -> Option<CreatedTableMeta> {
        self.created_tables
            .lock()
            .expect("created_tables poisoned")
            .get(name)
            .cloned()
    }

    /// Access the underlying DataFusion context (e.g. to register tables/Parquet).
    pub fn ctx(&self) -> &SessionContext {
        self.ctx.as_ref()
    }

    /// Look up a registered external catalog by name (case-sensitive, as registered).
    ///
    /// The streaming lake sink needs the raw [`oxidant_catalog::CatalogProvider`], not the
    /// DataFusion bridge over it: it creates the target database and table through the SPI
    /// (`create_database` / `create_table`) before any reader has ever resolved them.
    pub fn external_catalog(
        &self,
        name: &str,
    ) -> Option<Arc<dyn oxidant_catalog::CatalogProvider>> {
        self.oxidant_catalog(name)
    }

    /// Resolve an [`ObjectStore`](object_store::ObjectStore) for a table location, registering an
    /// S3 client for the bucket first when the location is `s3://` (local and `file://` paths
    /// resolve through DataFusion's default registry with no registration).
    ///
    /// This is the write-side counterpart of the read path's store resolution, and it deliberately
    /// shares `ensure_remote_store` so a streaming write to a Glue table uses exactly the same
    /// credentials, endpoint, and assumed role that a `SELECT` from it would.
    pub fn object_store_for(
        &self,
        location: &str,
        storage_options: &std::collections::HashMap<String, String>,
    ) -> Result<Arc<dyn object_store::ObjectStore>> {
        use datafusion::datasource::listing::ListingTableUrl;

        let url = ListingTableUrl::parse(location)
            .map_err(|e| Error::Plan(format!("bad table location `{location}`: {e}")))?;
        let state = self.ctx.state();
        catalog_bridge::ensure_remote_store(&state, &url, Some(storage_options))
            .map_err(|e| Error::Io(format!("object store for `{location}`: {e}")))?;
        state
            .runtime_env()
            .object_store(&url)
            .map_err(|e| Error::Io(format!("object store for `{location}`: {e}")))
    }

    /// Estimate total file bytes for a scanned table (Glue/Parquet/Delta/Iceberg listing).
    /// Returns `None` for MemTables, missing tables, or listing failures — callers treat that as
    /// "unknown" for auto-broadcast (not auto-replicated unless overridden).
    ///
    /// Results are cached per engine for `OXIDANT_TABLE_BYTES_CACHE_TTL_MS` (default 1 hour;
    /// `0` disables). The uncached path lists every file (and, for external catalogs, probes the
    /// catalog for table metadata) on **every query** — seconds per table per query on
    /// Glue+S3. Only the replicate/shard heuristic consumes these sizes, so bounded staleness
    /// is safe: a stale size changes performance, never results.
    pub async fn estimate_table_bytes(&self, table_name: &str) -> Option<u64> {
        self.estimate_table_stats(table_name).await.0
    }

    /// Estimate total file bytes + catalog row-count statistic for a scanned table. The row
    /// count is read from the catalog table's properties (`numRows` / Spark statistics keys)
    /// on the same `load_table` the byte-sizing walk already performs — no extra I/O — and is
    /// `None` for session-registered tables and metastores without statistics. Callers treat
    /// a `None` row count as "unknown" and keep byte-only replicate/shard classification for
    /// that table (`OXIDANT_REPLICATE_MAX_ROW_MULTIPLE`).
    ///
    /// Shares the [`Engine::estimate_table_bytes`] cache and TTL
    /// (`OXIDANT_TABLE_BYTES_CACHE_TTL_MS`): both estimates steer only the auto-broadcast
    /// heuristic, so bounded staleness is safe.
    ///
    /// The cache key is the RESOLVED (catalog, namespace, table), lowercased — so a bare-name
    /// estimate is scoped to the session catalog/namespace state that produced it (KAN-85: one
    /// session's estimate never leaks into another's via the shared cache), while a
    /// fully-qualified name resolves identically in every session and shares one entry. Keys
    /// are lowercased while catalog lookup (`oxidant_catalog`) is case-sensitive; production
    /// names arrive planner-normalized, so the mismatch is latent, not live.
    pub async fn estimate_table_stats(&self, table_name: &str) -> TableStats {
        let segments = parse_qualified_name(table_name);
        let key = self.stats_cache_key(&segments);
        self.estimate_table_stats_cached(key, self.estimate_table_stats_uncached(table_name))
            .await
    }

    /// [`Engine::estimate_table_stats`] over a structured logical [`TableReference`] — the
    /// shape the distributed planner actually holds. Building the segments from
    /// `catalog()`/`schema()`/`table()` (instead of a Display/string round-trip) keeps the
    /// qualifier exact, so a `glue.db.t` scan probes Glue's `db` exactly once (KAN-81; the
    /// string entry point is only reached with bare names from `resolve_replicated_tables`).
    pub async fn estimate_table_stats_ref(
        &self,
        reference: &datafusion::common::TableReference,
    ) -> TableStats {
        let segments = table_ref_segments(reference);
        let key = self.stats_cache_key(&segments);
        self.estimate_table_stats_cached(key, self.estimate_table_stats_ref_uncached(reference))
            .await
    }

    /// The `table_bytes_cache` key for a name: its resolved `(catalog, namespace, table)`
    /// triple (session-state defaults applied), joined and lowercased.
    fn stats_cache_key(&self, segments: &[String]) -> String {
        let (catalog, namespace, table) = self.resolve_table_ref(segments);
        join_table_name_parts(
            [catalog.as_str()]
                .into_iter()
                .chain(namespace.iter().map(String::as_str))
                .chain([table.as_str()]),
        )
        .to_ascii_lowercase()
    }

    /// The shared per-engine TTL cache behind [`Engine::estimate_table_stats`] and
    /// [`Engine::estimate_table_stats_ref`]; `compute` is the uncached walk for the entry.
    async fn estimate_table_stats_cached(
        &self,
        key: String,
        compute: impl std::future::Future<Output = TableStats>,
    ) -> TableStats {
        let Some(ttl) = table_bytes_cache_ttl() else {
            return compute.await;
        };
        let fresh = self
            .table_bytes_cache
            .lock()
            .expect("table_bytes_cache poisoned")
            .get(&key)
            .filter(|(_, at)| at.elapsed() < ttl)
            .map(|(stats, _)| *stats);
        if let Some(stats) = fresh {
            return stats;
        }
        let stats = compute.await;
        self.table_bytes_cache
            .lock()
            .expect("table_bytes_cache poisoned")
            .insert(key, (stats, std::time::Instant::now()));
        stats
    }

    /// The uncached sizing walk behind [`Engine::estimate_table_stats`].
    async fn estimate_table_stats_uncached(&self, table_name: &str) -> TableStats {
        let bare = table_name.rsplit('.').next().unwrap_or(table_name);

        // Session-registered ListingTable (e.g. `register_parquet`): sized from the listing;
        // there is no catalog metadata, so no row-count statistic.
        if let Ok(provider) = self.ctx.table_provider(bare).await {
            if let Some(bytes) =
                estimate_listing_provider_bytes(&self.ctx.state(), provider.as_ref()).await
            {
                return (Some(bytes), None);
            }
        }

        let segments = parse_qualified_name(table_name);
        self.estimate_stats_resolved(&segments).await
    }

    /// The uncached sizing walk behind [`Engine::estimate_table_stats_ref`]: same as the
    /// string path, but the segments come straight from the planner's structured
    /// [`datafusion::common::TableReference`].
    async fn estimate_table_stats_ref_uncached(
        &self,
        reference: &datafusion::common::TableReference,
    ) -> TableStats {
        let bare = reference.table();

        // Session-registered ListingTable (e.g. `register_parquet`): sized from the listing;
        // there is no catalog metadata, so no row-count statistic.
        if let Ok(provider) = self.ctx.table_provider(bare).await {
            if let Some(bytes) =
                estimate_listing_provider_bytes(&self.ctx.state(), provider.as_ref()).await
            {
                return (Some(bytes), None);
            }
        }

        let segments = table_ref_segments(reference);
        self.estimate_stats_resolved(&segments).await
    }

    /// The resolution core shared by both uncached sizing walks: `segments` is the
    /// (possibly partial) dotted name, from either `parse_qualified_name` (string path) or a
    /// structured [`datafusion::common::TableReference`] (planner path).
    ///
    /// Resolving exactly the way SHOW/DESCRIBE do means a catalog/namespace qualifier pins
    /// *where* the table lives. Before KAN-81 the walk discarded the qualifier and brute-force
    /// searched every namespace of every external catalog — on Glue each probe is a `GetTable`
    /// call, so one `count(*)` against a fully-qualified table fanned out into O(databases)
    /// API calls per query.
    async fn estimate_stats_resolved(&self, segments: &[String]) -> TableStats {
        let (catalog, namespace, table) = self.resolve_table_ref(segments);

        if let Some(provider) = self.oxidant_catalog(&catalog) {
            if !namespace.is_empty() {
                // Qualified reference: the user told us where the table lives — probe exactly
                // once and take the answer at face value, miss included (never search other
                // namespaces/catalogs for a qualified name). Sizing stays best-effort: a
                // not-found (`Error::Plan`) or any backend failure (`Error::Io`/…) estimates
                // "unknown" rather than failing the query from a stats path.
                return match provider.load_table(&namespace, &table).await {
                    Ok(md) => {
                        catalog_bridge::estimate_stats_for_metadata(&self.ctx.state(), &md).await
                    }
                    Err(_) => (None, None),
                };
            }
            // External current catalog with no current namespace (e.g. `USE glue` + a bare
            // table): restrict the namespace search to *that* catalog only.
            if let Some(stats) =
                estimate_stats_in_catalog(&self.ctx.state(), provider.as_ref(), &table).await
            {
                return stats;
            }
            return (None, None);
        }

        // A 3+-segment name whose catalog isn't registered can never resolve — sizing it via
        // the all-catalog fan-out would burn O(databases) catalog probes on a name guaranteed
        // to miss. (The 2-segment `db.t` form keeps the legacy fallback: its first segment is
        // a namespace in the *current* catalog, not a catalog name.)
        if segments.len() >= 3 {
            return (None, None);
        }

        // Bare name under the builtin catalog: legacy convenience fallback — search every
        // registered external catalog's namespaces for the table.
        let catalogs: Vec<Arc<dyn oxidant_catalog::CatalogProvider>> = self
            .oxidant_catalogs
            .lock()
            .expect("oxidant_catalogs poisoned")
            .values()
            .cloned()
            .collect();
        for cat in catalogs {
            if let Some(stats) =
                estimate_stats_in_catalog(&self.ctx.state(), cat.as_ref(), &table).await
            {
                return stats;
            }
        }
        (None, None)
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

async fn estimate_listing_provider_bytes(
    state: &datafusion::execution::context::SessionState,
    provider: &dyn datafusion::catalog::TableProvider,
) -> Option<u64> {
    use datafusion::datasource::listing::ListingTable;
    let listing = provider.downcast_ref::<ListingTable>()?;
    let urls = listing.table_paths().clone();
    let ext = listing.options().file_extension.as_str();
    shard::sum_listing_bytes(state, urls, ext).await.ok()
}

/// The schema in the built-in catalog that holds the bundled sample tables
/// ([`Engine::register_sample_tables`]; `samples.tpch_nation`, …).
pub const SAMPLES_SCHEMA: &str = "samples";

/// Table-name stem for one entry of a sample-data subdir: `parquet`/`csv` tables are files
/// with a matching extension; `delta`/`iceberg` tables are directories. Anything else (wrong
/// extension, stray file) returns `None`.
fn sample_table_stem(path: &std::path::Path, sub: &str) -> Option<String> {
    match sub {
        "parquet" | "csv" => {
            if path.is_file() && path.extension().and_then(|e| e.to_str()) == Some(sub) {
                path.file_stem()?.to_str().map(str::to_string)
            } else {
                None
            }
        }
        _ => {
            if path.is_dir() {
                path.file_name()?.to_str().map(str::to_string)
            } else {
                None
            }
        }
    }
}

/// Freshness window for the per-engine [`Engine::estimate_table_bytes`] cache
/// (`OXIDANT_TABLE_BYTES_CACHE_TTL_MS`). Default 1 hour; `0` disables caching (every call
/// re-lists). Sizes feed only the auto-broadcast heuristic, never correctness.
fn table_bytes_cache_ttl() -> Option<std::time::Duration> {
    let ms = std::env::var("OXIDANT_TABLE_BYTES_CACHE_TTL_MS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(3_600_000);
    (ms > 0).then_some(std::time::Duration::from_millis(ms))
}

async fn estimate_stats_in_catalog(
    state: &datafusion::execution::context::SessionState,
    catalog: &dyn oxidant_catalog::CatalogProvider,
    table: &str,
) -> Option<TableStats> {
    let top = catalog.list_namespaces(&[]).await.ok()?;
    let mut namespaces = top;
    // One level of nesting is enough for Hive/Glue (`db`) and covers Unity-style parents.
    let mut i = 0;
    while i < namespaces.len() {
        if let Ok(children) = catalog.list_namespaces(&namespaces[i]).await {
            for child in children {
                if !namespaces.iter().any(|n| n == &child) {
                    namespaces.push(child);
                }
            }
        }
        i += 1;
        if i > 64 {
            break;
        }
    }
    for ns in namespaces {
        if let Ok(md) = catalog.load_table(&ns, table).await {
            let (bytes, rows) = catalog_bridge::estimate_stats_for_metadata(state, &md).await;
            if bytes.is_some() {
                return Some((bytes, rows));
            }
        }
    }
    // Also try empty namespace (flat catalogs).
    if let Ok(md) = catalog.load_table(&[], table).await {
        let (bytes, rows) = catalog_bridge::estimate_stats_for_metadata(state, &md).await;
        if bytes.is_some() {
            return Some((bytes, rows));
        }
    }
    None
}

/// A parsed catalog-listing/`SHOW` statement (see [`parse_show`]).
#[derive(Debug, PartialEq, Eq)]
enum ShowStmt {
    /// `SHOW CATALOGS`.
    Catalogs,
    /// `SHOW DATABASES`/`SHOW SCHEMAS`, optionally `IN <catalog>`.
    Databases { catalog: Option<String> },
    /// `SHOW TABLES`, optionally `IN|FROM <catalog>[.<database>]` and/or `LIKE '<pattern>'` (or
    /// Spark's bare-pattern shorthand with no `LIKE` keyword). `catalog`/`database` both absent
    /// defaults to the session's current catalog/namespace (see [`Engine::resolve_table_ref`]-style
    /// defaulting, applied directly in [`Engine::run_show`]).
    Tables {
        catalog: Option<String>,
        database: Option<String>,
        like: Option<String>,
    },
    /// `SHOW COLUMNS IN|FROM <table>[ IN|FROM <namespace>]`.
    Columns {
        table: String,
        namespace: Option<String>,
    },
    /// `SHOW VIEWS`, optionally `IN|FROM <database>` and/or `LIKE '<pattern>'`. Always answered
    /// from the built-in default catalog (session temp views + persistent views) — Spark's
    /// `SHOW VIEWS` grammar has no cross-catalog form.
    Views {
        database: Option<String>,
        like: Option<String>,
    },
    /// `SHOW TBLPROPERTIES <table>[('key')]`.
    TblProperties { table: String, key: Option<String> },
    /// `SHOW TABLE EXTENDED [IN|FROM <database>] LIKE '<pattern>'[ PARTITION (…)]` (the trailing
    /// `PARTITION` clause is accepted but not yet reflected in the result — see
    /// [`parse_show_table_extended`]).
    TableExtended {
        database: Option<String>,
        like: String,
    },
    /// `SHOW CREATE TABLE <table>[ AS SERDE]` — the core bug fix (see
    /// [`reconstruct_create_table_ddl`]).
    CreateTable { table: String },
    /// `SHOW PARTITIONS <table>[ PARTITION (k=v, …)]`.
    Partitions {
        table: String,
        spec: Vec<(String, String)>,
    },
    /// `SHOW FUNCTIONS[ LIKE '<pattern>']`.
    Functions { like: Option<String> },
}

/// Recognize the `SHOW` statements oxidant answers itself, returning `None` for anything else (so it
/// flows through normal planning untouched — never a regression for a form this doesn't cover
/// yet). Tolerant by design, matching [`parse_use`]'s conventions: keywords are case-insensitive,
/// identifiers may be backtick-quoted or bare, a trailing `;` and extra whitespace are ignored.
///
/// Parens are space-padded before tokenizing (`tbl("p1")` → `tbl ( "p1" )`) so every sub-parser
/// below can work on plain whitespace-split tokens even when Spark's grammar allows a clause to
/// butt directly against an adjacent paren — safe here because none of SHOW's patterns/keys/specs
/// legitimately contain a literal `(`/`)`.
/// Pad every `(`/`)` with surrounding whitespace so a simple `split_whitespace()` tokenizer sees
/// them as standalone tokens (used to parse `SHOW PARTITIONS ... PARTITION (k=v, ...)`-style
/// parenthesized tails) — except inside a single-quoted string literal, where a literal paren
/// (e.g. `SHOW TABLES LIKE 'foo(bar)'`) must stay part of the quoted token, not get split apart.
fn pad_parens_outside_quotes(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut in_quote = false;
    for ch in s.chars() {
        match ch {
            '\'' => {
                in_quote = !in_quote;
                out.push(ch);
            }
            '(' | ')' if !in_quote => {
                out.push(' ');
                out.push(ch);
                out.push(' ');
            }
            _ => out.push(ch),
        }
    }
    out
}

fn parse_show(query: &str) -> Option<ShowStmt> {
    let trimmed = query.trim().trim_end_matches(';').trim();
    let spaced = pad_parens_outside_quotes(trimmed);
    let mut words = spaced.split_whitespace();
    if !words.next()?.eq_ignore_ascii_case("show") {
        return None;
    }
    let kind = words.next()?;
    let rest: Vec<&str> = words.collect();
    if kind.eq_ignore_ascii_case("catalogs") {
        rest.is_empty().then_some(ShowStmt::Catalogs)
    } else if kind.eq_ignore_ascii_case("databases") || kind.eq_ignore_ascii_case("schemas") {
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
        parse_show_tables(&rest)
    } else if kind.eq_ignore_ascii_case("table") {
        parse_show_table_extended(&rest)
    } else if kind.eq_ignore_ascii_case("columns") {
        parse_show_columns(&rest)
    } else if kind.eq_ignore_ascii_case("views") {
        parse_show_views(&rest)
    } else if kind.eq_ignore_ascii_case("tblproperties") {
        parse_show_tblproperties(&rest)
    } else if kind.eq_ignore_ascii_case("create") {
        parse_show_create_table(&rest)
    } else if kind.eq_ignore_ascii_case("partitions") {
        parse_show_partitions(&rest)
    } else if kind.eq_ignore_ascii_case("functions") {
        parse_show_functions(&rest)
    } else {
        None
    }
}

/// `SHOW TABLES`[ `IN|FROM <cat>[.<db>]`][ `LIKE '<pattern>'` | bare `'<pattern>'`].
fn parse_show_tables(rest: &[&str]) -> Option<ShowStmt> {
    let (like, head) = take_trailing_like(rest);
    match head {
        [] => Some(ShowStmt::Tables {
            catalog: None,
            database: None,
            like,
        }),
        [in_kw, name] if in_kw.eq_ignore_ascii_case("in") || in_kw.eq_ignore_ascii_case("from") => {
            let mut segs = parse_qualified_name(name).into_iter();
            let catalog = segs.next()?;
            let database = segs.next();
            Some(ShowStmt::Tables {
                catalog: Some(catalog),
                database,
                like,
            })
        }
        _ => None,
    }
}

/// `SHOW COLUMNS IN|FROM <table>[ IN|FROM <namespace>]`.
fn parse_show_columns(rest: &[&str]) -> Option<ShowStmt> {
    match rest {
        [in_kw, table]
            if in_kw.eq_ignore_ascii_case("in") || in_kw.eq_ignore_ascii_case("from") =>
        {
            Some(ShowStmt::Columns {
                table: (*table).to_string(),
                namespace: None,
            })
        }
        [in_kw1, table, in_kw2, ns]
            if (in_kw1.eq_ignore_ascii_case("in") || in_kw1.eq_ignore_ascii_case("from"))
                && (in_kw2.eq_ignore_ascii_case("in") || in_kw2.eq_ignore_ascii_case("from")) =>
        {
            Some(ShowStmt::Columns {
                table: (*table).to_string(),
                namespace: Some((*ns).to_string()),
            })
        }
        _ => None,
    }
}

/// `SHOW VIEWS`[ `IN|FROM <database>`][ `LIKE '<pattern>'` | bare `'<pattern>'`].
fn parse_show_views(rest: &[&str]) -> Option<ShowStmt> {
    let (like, head) = take_trailing_like(rest);
    match head {
        [] => Some(ShowStmt::Views {
            database: None,
            like,
        }),
        [in_kw, name] if in_kw.eq_ignore_ascii_case("in") || in_kw.eq_ignore_ascii_case("from") => {
            Some(ShowStmt::Views {
                database: Some((*name).to_string()),
                like,
            })
        }
        _ => None,
    }
}

/// `SHOW TBLPROPERTIES <table>[('key')]` (with or without whitespace before the paren — see
/// [`parse_show`]'s paren-spacing normalization).
fn parse_show_tblproperties(rest: &[&str]) -> Option<ShowStmt> {
    match rest {
        [table] => Some(ShowStmt::TblProperties {
            table: (*table).to_string(),
            key: None,
        }),
        [table, "(", key, ")"] => Some(ShowStmt::TblProperties {
            table: (*table).to_string(),
            key: Some(strip_quotes(key)),
        }),
        _ => None,
    }
}

/// `SHOW TABLE EXTENDED [IN|FROM <database>] LIKE '<pattern>'[ PARTITION (…)]`. `LIKE` is
/// mandatory in Spark's own grammar for this form; a bare `SHOW TABLE EXTENDED` (no `LIKE`) isn't
/// matched here and falls through to the normal path. A trailing `PARTITION (…)` clause is parsed
/// (so it doesn't break tokenization) but currently discarded — [`Engine::run_show`] answers with
/// the unfiltered per-table listing rather than erroring.
fn parse_show_table_extended(rest: &[&str]) -> Option<ShowStmt> {
    let [ext_kw, tail @ ..] = rest else {
        return None;
    };
    if !ext_kw.eq_ignore_ascii_case("extended") {
        return None;
    }
    let mut i = 0;
    let mut database = None;
    if tail.len() > i + 1
        && (tail[i].eq_ignore_ascii_case("in") || tail[i].eq_ignore_ascii_case("from"))
    {
        database = Some(tail[i + 1].to_string());
        i += 2;
    }
    if tail.get(i)?.eq_ignore_ascii_case("like") {
        i += 1;
    } else {
        return None;
    }
    let like = strip_quotes(tail.get(i)?);
    // No trailing tokens after the LIKE pattern — matches `parse_describe`'s convention of
    // rejecting (falling through, not silently ignoring) unrecognized trailing input.
    if tail.len() != i + 1 {
        return None;
    }
    Some(ShowStmt::TableExtended { database, like })
}

/// `SHOW CREATE TABLE <table>[ AS SERDE]`.
fn parse_show_create_table(rest: &[&str]) -> Option<ShowStmt> {
    let [tbl_kw, table, extra @ ..] = rest else {
        return None;
    };
    if !tbl_kw.eq_ignore_ascii_case("table") {
        return None;
    }
    match extra {
        [] => Some(ShowStmt::CreateTable {
            table: (*table).to_string(),
        }),
        // `AS SERDE` (Hive-serde output format) isn't distinguished from the plain form — oxidant
        // has no serde-specific rendering, so both produce the same DDL reconstruction.
        [as_kw, serde_kw]
            if as_kw.eq_ignore_ascii_case("as") && serde_kw.eq_ignore_ascii_case("serde") =>
        {
            Some(ShowStmt::CreateTable {
                table: (*table).to_string(),
            })
        }
        _ => None,
    }
}

/// `SHOW PARTITIONS <table>[ PARTITION (k=v, …)]`.
fn parse_show_partitions(rest: &[&str]) -> Option<ShowStmt> {
    match rest {
        [] => None,
        [table] => Some(ShowStmt::Partitions {
            table: (*table).to_string(),
            spec: Vec::new(),
        }),
        [table, part_kw, "(", tail @ .., ")"] if part_kw.eq_ignore_ascii_case("partition") => {
            Some(ShowStmt::Partitions {
                table: (*table).to_string(),
                spec: parse_partition_spec_tokens(tail),
            })
        }
        _ => None,
    }
}

/// Parse `k = 'v', k2 = v2, …` tokens (as split by [`parse_show`]'s paren-spaced tokenizer) into
/// `(key, value)` pairs. Best-effort: an entry that doesn't match `key = value` is simply dropped
/// rather than failing the whole parse (mirrors `spark_create_table::parse_properties`'s leniency).
fn parse_partition_spec_tokens(tokens: &[&str]) -> Vec<(String, String)> {
    tokens
        .join(" ")
        .split(',')
        .filter_map(|entry| {
            let (k, v) = entry.split_once('=')?;
            Some((k.trim().to_string(), strip_quotes(v.trim())))
        })
        .collect()
}

/// `SHOW FUNCTIONS[ LIKE '<pattern>']` (a `db.func`-qualified filter isn't supported — only the
/// unqualified `LIKE` form).
fn parse_show_functions(rest: &[&str]) -> Option<ShowStmt> {
    let (like, head) = take_trailing_like(rest);
    head.is_empty().then_some(ShowStmt::Functions { like })
}

/// True if `s` is wrapped in one matching pair of `'…'`/`"…"` quotes.
fn is_quoted(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() >= 2 && (b[0] == b'\'' || b[0] == b'"') && b[b.len() - 1] == b[0]
}

/// Strip one layer of surrounding `'…'`/`"…"` quoting, if present; otherwise return `s` unchanged.
fn strip_quotes(s: &str) -> String {
    if is_quoted(s) {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

/// Pull a trailing `LIKE '<pattern>'` — or Spark's shorthand bare `'<pattern>'` with no `LIKE`
/// keyword (e.g. `SHOW TABLES 'show_t*'`) — off the end of a SHOW statement's remaining tokens.
/// Returns the unquoted pattern (if present) and the tokens that remain before it.
fn take_trailing_like<'a>(rest: &'a [&'a str]) -> (Option<String>, &'a [&'a str]) {
    match rest {
        [head @ .., like_kw, pat] if like_kw.eq_ignore_ascii_case("like") => {
            (Some(strip_quotes(pat)), head)
        }
        [head @ .., pat] if is_quoted(pat) => (Some(strip_quotes(pat)), head),
        _ => (None, rest),
    }
}

/// SQL `LIKE` glob match (`%` = any run of chars, `_` = exactly one char), case-sensitive — the
/// filter every SHOW `LIKE '<pattern>'` clause applies to table/view/function names (see
/// [`ShowStmt::Tables`]/[`ShowStmt::Views`]/[`ShowStmt::Functions`]). Classic two-pointer wildcard
/// matching with backtracking on `%`, operating on `char`s so multi-byte names aren't corrupted.
fn sql_like_match(pattern: &str, s: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = s.chars().collect();
    let (mut pi, mut ti) = (0usize, 0usize);
    let mut backtrack: Option<(usize, usize)> = None; // (pattern pos after '%', text pos '%' started matching at)
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '_' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '%' {
            backtrack = Some((pi + 1, ti));
            pi += 1;
        } else if let Some((bp, bt)) = backtrack {
            pi = bp;
            ti = bt + 1;
            backtrack = Some((bp, ti));
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '%' {
        pi += 1;
    }
    pi == p.len()
}

/// A parsed `DESCRIBE`/`DESC` statement (see [`parse_describe`]). Mirrors [`ShowStmt`]'s shape and
/// interception pattern.
#[derive(Debug, PartialEq, Eq)]
enum DescribeStmt {
    /// `DESCRIBE|DESC [TABLE] [EXTENDED|FORMATTED] <table>[ PARTITION (k=v, …)][ AS JSON]` — the
    /// common case. `partition` is parsed (so it doesn't break tokenization) but not yet reflected
    /// in the result, matching [`parse_show_table_extended`]'s precedent for an accepted-but-not-
    /// filtered trailing clause.
    Table {
        name: String,
        extended: bool,
        partition: Option<Vec<(String, String)>>,
        as_json: bool,
    },
    /// `DESCRIBE|DESC QUERY <select>`, or a bare `DESCRIBE|DESC <select>` recognized because the
    /// statement starts with a query keyword (`SELECT`/`WITH`/`VALUES`) rather than a table name.
    Query { stmt: String },
    /// `DESCRIBE|DESC DATABASE|SCHEMA [EXTENDED] [<catalog>.]<name>`.
    Database {
        catalog: Option<String>,
        name: String,
    },
    /// `DESCRIBE|DESC CATALOG <name>`.
    Catalog { name: String },
    /// `DESCRIBE|DESC FUNCTION [EXTENDED] <name>`.
    Function { name: String, extended: bool },
}

/// Recognize the `DESCRIBE`/`DESC` statements oxidant answers itself, returning `None` for anything
/// else (so it flows through normal planning untouched — never a regression for a form this
/// doesn't cover). Tolerant by design, matching [`parse_show`]'s conventions: keywords are
/// case-insensitive, a trailing `;` and extra whitespace are ignored. Only intercepts a form once
/// every trailing token is understood — any leftover, unrecognized tokens fall through rather than
/// risk silently mis-parsing an exotic shape (e.g. Spark's unsupported
/// `DESC FORMATTED t col AS JSON` per-column form).
fn parse_describe(query: &str) -> Option<DescribeStmt> {
    let trimmed = query.trim().trim_end_matches(';').trim();
    let mut words = trimmed.split_whitespace();
    let kw = words.next()?;
    if !(kw.eq_ignore_ascii_case("describe") || kw.eq_ignore_ascii_case("desc")) {
        return None;
    }
    let rest: Vec<&str> = words.collect();
    let first = *rest.first()?;

    if first.eq_ignore_ascii_case("database") || first.eq_ignore_ascii_case("schema") {
        let mut i = 1;
        if rest
            .get(i)
            .is_some_and(|t| t.eq_ignore_ascii_case("extended"))
        {
            i += 1;
        }
        let name_tok = *rest.get(i)?;
        if rest.get(i + 1).is_some() {
            return None;
        }
        let mut segs = parse_qualified_name(name_tok).into_iter();
        let seg0 = segs.next()?;
        return match segs.next() {
            Some(seg1) if segs.next().is_none() => Some(DescribeStmt::Database {
                catalog: Some(seg0),
                name: seg1,
            }),
            None => Some(DescribeStmt::Database {
                catalog: None,
                name: seg0,
            }),
            _ => None,
        };
    }
    if first.eq_ignore_ascii_case("catalog") {
        return match rest[1..] {
            [name] => Some(DescribeStmt::Catalog {
                name: name.to_string(),
            }),
            _ => None,
        };
    }
    if first.eq_ignore_ascii_case("function") {
        let mut i = 1;
        let mut extended = false;
        if rest
            .get(i)
            .is_some_and(|t| t.eq_ignore_ascii_case("extended"))
        {
            extended = true;
            i += 1;
        }
        return match rest[i..] {
            [name] => Some(DescribeStmt::Function {
                name: name.to_string(),
                extended,
            }),
            _ => None,
        };
    }
    if first.eq_ignore_ascii_case("query") {
        let stmt = rest[1..].join(" ");
        return (!stmt.is_empty()).then_some(DescribeStmt::Query { stmt });
    }
    if first.eq_ignore_ascii_case("select")
        || first.eq_ignore_ascii_case("with")
        || first.eq_ignore_ascii_case("values")
    {
        return Some(DescribeStmt::Query {
            stmt: rest.join(" "),
        });
    }

    // `[TABLE] [EXTENDED|FORMATTED] <table>[ PARTITION (…)][ AS JSON]`.
    let mut i = 0;
    if rest.get(i).is_some_and(|t| t.eq_ignore_ascii_case("table")) {
        i += 1;
    }
    let mut extended = false;
    if rest
        .get(i)
        .is_some_and(|t| t.eq_ignore_ascii_case("extended") || t.eq_ignore_ascii_case("formatted"))
    {
        extended = true;
        i += 1;
    }
    let name = (*rest.get(i)?).to_string();
    i += 1;
    let spaced = rest[i..].join(" ").replace('(', " ( ").replace(')', " ) ");
    let ptoks: Vec<&str> = spaced.split_whitespace().collect();
    let mut j = 0;
    let mut partition = None;
    if ptoks
        .first()
        .is_some_and(|t| t.eq_ignore_ascii_case("partition"))
        && ptoks.get(1) == Some(&"(")
    {
        let close = ptoks.iter().position(|t| *t == ")")?;
        partition = Some(parse_partition_spec_tokens(&ptoks[2..close]));
        j = close + 1;
    }
    let mut as_json = false;
    if ptoks.get(j).is_some_and(|t| t.eq_ignore_ascii_case("as"))
        && ptoks
            .get(j + 1)
            .is_some_and(|t| t.eq_ignore_ascii_case("json"))
    {
        as_json = true;
        j += 2;
    }
    if j != ptoks.len() {
        // Leftover tokens oxidant doesn't understand (e.g. a per-column `DESC ... col AS JSON`) —
        // don't guess, fall through untouched.
        return None;
    }
    Some(DescribeStmt::Table {
        name,
        extended,
        partition,
        as_json,
    })
}

/// A parsed `USE` statement (see [`parse_use`]).
#[derive(Debug, PartialEq, Eq)]
enum UseStmt {
    /// `USE CATALOG <catalog>` — switch the current catalog, resetting the current namespace
    /// (KAN-84: empty for external catalogs, `["default"]` for the builtin) unless the catalog
    /// is already current (Spark's no-op switch).
    Catalog { catalog: String },
    /// `USE <namespace>` (current catalog unchanged) or `USE <catalog>.<namespace>` (switches
    /// both). Spark's default `USE <db>` behavior: a single unqualified segment changes only the
    /// current database within the current catalog.
    Namespace {
        catalog: Option<String>,
        namespace: Vec<String>,
    },
}

/// Recognize `USE` statements, returning `None` for anything else (so it flows through normal
/// planning untouched). Tolerant by design, following [`parse_show`]'s conventions: keywords are
/// case-insensitive, identifiers may be backtick-quoted or bare, a trailing `;` and extra
/// whitespace are ignored.
///
/// Recognized forms:
/// - `USE CATALOG <catalog>` — catalog switch (resets the current namespace, KAN-84).
/// - `USE <catalog>.<namespace>` — a dotted name switches both catalog and namespace.
/// - `USE <namespace>` — a single unqualified segment switches only the current namespace,
///   matching Spark's `USE <db>`.
fn parse_use(query: &str) -> Option<UseStmt> {
    let trimmed = query.trim().trim_end_matches(';').trim();
    let mut words = trimmed.split_whitespace();
    if !words.next()?.eq_ignore_ascii_case("use") {
        return None;
    }
    let rest: Vec<&str> = words.collect();
    match rest.as_slice() {
        [kw, name] if kw.eq_ignore_ascii_case("catalog") => Some(UseStmt::Catalog {
            catalog: parse_qualified_name(name).into_iter().next()?,
        }),
        [name] => {
            let mut segs = parse_qualified_name(name).into_iter();
            let first = segs.next()?;
            match segs.next() {
                // `USE <catalog>.<namespace...>` — everything after the first segment is the
                // (possibly multi-part) namespace.
                Some(second) => {
                    let mut namespace = vec![second];
                    namespace.extend(segs);
                    Some(UseStmt::Namespace {
                        catalog: Some(first),
                        namespace,
                    })
                }
                // `USE <namespace>` — current catalog unchanged.
                None => Some(UseStmt::Namespace {
                    catalog: None,
                    namespace: vec![first],
                }),
            }
        }
        _ => None,
    }
}

/// Join a resolved table name's parts (catalog?, namespace…, table) with `.`, dropping empty
/// segments: an empty namespace (the `USE CATALOG <external>` "no database selected" state,
/// KAN-84) must never produce a `catalog..table` double dot in display names or probe SQL.
fn join_table_name_parts<'a>(parts: impl IntoIterator<Item = &'a str>) -> String {
    parts
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(".")
}

/// The segments of a structured [`datafusion::common::TableReference`] — `[catalog?, schema?,
/// table]` — the input shape `parse_qualified_name` produces for the string path, so both
/// sizing-walk entries share one resolution core (KAN-81).
fn table_ref_segments(reference: &datafusion::common::TableReference) -> Vec<String> {
    reference
        .catalog()
        .into_iter()
        .chain(reference.schema())
        .chain([reference.table()])
        .map(str::to_string)
        .collect()
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

/// Normalize a `CREATE TABLE` statement's (possibly qualified, possibly backtick-quoted) name into
/// the bare-table-name key `Engine::created_tables` is keyed by. Every lookup
/// (`created_table_meta`) resolves its input through [`parse_qualified_name`] +
/// [`Engine::resolve_table_ref`], which keeps only the final (unquoted) segment — so the insert
/// side must strip backticks/qualification the same way, or `SHOW CREATE TABLE`/`SHOW
/// TBLPROPERTIES`/`DESCRIBE EXTENDED` on a backtick-quoted or qualified `CREATE TABLE` name would
/// silently miss the entry keyed by the raw, unnormalized source span.
fn created_table_key(name: &str) -> String {
    parse_qualified_name(name).pop().unwrap_or_default()
}

/// Single-column `Utf8` (non-null) batch — the shape shared by every SHOW form whose result is
/// one bare name per row (`SHOW DATABASES`'s `namespace`, `SHOW CATALOGS`'s `catalog`,
/// `SHOW COLUMNS`'s `col_name`, `SHOW PARTITIONS`'s `partition`, `SHOW FUNCTIONS`'s `function`,
/// and `SHOW CREATE TABLE`'s single-row `createtab_stmt`).
fn single_col_batch(field_name: &str, values: Vec<String>) -> Result<RecordBatch> {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![Field::new(
        field_name,
        DataType::Utf8,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))])
        .map_err(|e| Error::Execution(e.to_string()))
}

/// Single-column `namespace` (Utf8) batch for the `SHOW DATABASES`/`SHOW SCHEMAS` forms.
fn namespace_batch(namespaces: Vec<String>) -> Result<RecordBatch> {
    single_col_batch("namespace", namespaces)
}

/// Generic two-column `Utf8` (non-null) batch, column names given by the caller — shared by
/// `SHOW TBLPROPERTIES` ([`key_value_batch`]'s `key`/`value`) and `DESCRIBE DATABASE`/
/// `DESCRIBE CATALOG` ([`Engine::run_describe`]'s `info_name`/`info_value`).
fn two_col_batch(col1: &str, col2: &str, rows: Vec<(String, String)>) -> Result<RecordBatch> {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![
        Field::new(col1, DataType::Utf8, false),
        Field::new(col2, DataType::Utf8, false),
    ]));
    let firsts: Vec<String> = rows.iter().map(|(a, _)| a.clone()).collect();
    let seconds: Vec<String> = rows.iter().map(|(_, b)| b.clone()).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(firsts)),
            Arc::new(StringArray::from(seconds)),
        ],
    )
    .map_err(|e| Error::Execution(e.to_string()))
}

/// Two-column `key`/`value` (Utf8) batch — `SHOW TBLPROPERTIES`.
fn key_value_batch(rows: Vec<(String, String)>) -> Result<RecordBatch> {
    two_col_batch("key", "value", rows)
}

/// Three-column `col_name`/`data_type`/`comment` (Utf8) batch — Spark's `DESCRIBE`/`DESC` shape,
/// shared by [`DescribeStmt::Table`]'s plain/`EXTENDED` column listing and [`DescribeStmt::Query`].
fn describe_batch(rows: Vec<(String, String, String)>) -> Result<RecordBatch> {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![
        Field::new("col_name", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("comment", DataType::Utf8, false),
    ]));
    let names: Vec<String> = rows.iter().map(|(n, _, _)| n.clone()).collect();
    let types: Vec<String> = rows.iter().map(|(_, t, _)| t.clone()).collect();
    let comments: Vec<String> = rows.iter().map(|(_, _, c)| c.clone()).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(types)),
            Arc::new(StringArray::from(comments)),
        ],
    )
    .map_err(|e| Error::Execution(e.to_string()))
}

/// Render a table-properties map as Spark's `[k=v, k2=v2]` bracketed, sorted-key display string —
/// shared by `SHOW TABLE EXTENDED`'s `information` blob and `DESCRIBE EXTENDED`'s
/// `Table Properties` row.
fn format_properties(properties: &HashMap<String, String>) -> String {
    let mut kv: Vec<String> = properties
        .iter()
        .map(|(k, v)| format!("{k}={}", redact_property_value(k, v)))
        .collect();
    kv.sort();
    kv.join(", ")
}

/// Spark redacts any table-property (or `OPTIONS`) value whose *key* matches its default
/// sensitive-config regex (`spark.sql.redaction.string.regex`, which defaults to
/// `(?i)secret|password`) before it can appear in `SHOW CREATE TABLE`/`SHOW TBLPROPERTIES`/
/// `DESCRIBE EXTENDED` output — otherwise a `TBLPROPERTIES ('password' = '...')` on a table
/// would leak the literal credential back out through any of those statements. Golden:
/// `spark-tests/results/show-tblproperties.sql.out` (`password\t*********(redacted)`).
fn redact_property_value(key: &str, value: &str) -> String {
    let k = key.to_ascii_lowercase();
    if k.contains("secret") || k.contains("password") {
        "*********(redacted)".to_string()
    } else {
        value.to_string()
    }
}

/// Three-column `namespace`/`<name_col>`/`isTemporary` batch shared by `SHOW TABLES`
/// (`tableName`) and `SHOW VIEWS` (`viewName`).
fn namespace_name_temp_batch(
    name_col: &str,
    rows: Vec<(String, String, bool)>,
) -> Result<RecordBatch> {
    use arrow::array::{BooleanArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![
        Field::new("namespace", DataType::Utf8, false),
        Field::new(name_col, DataType::Utf8, false),
        Field::new("isTemporary", DataType::Boolean, false),
    ]));
    let namespaces: Vec<String> = rows.iter().map(|(ns, _, _)| ns.clone()).collect();
    let names: Vec<String> = rows.iter().map(|(_, n, _)| n.clone()).collect();
    let temp: Vec<bool> = rows.iter().map(|(_, _, t)| *t).collect();
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

/// Three-column `namespace`/`tableName`/`isTemporary` batch matching Spark's `SHOW TABLES`.
fn tables_batch(rows: Vec<(String, String)>) -> Result<RecordBatch> {
    namespace_name_temp_batch(
        "tableName",
        rows.into_iter().map(|(ns, t)| (ns, t, false)).collect(),
    )
}

/// Three-column `namespace`/`viewName`/`isTemporary` batch matching Spark's `SHOW VIEWS`.
fn views_batch(rows: Vec<(String, String, bool)>) -> Result<RecordBatch> {
    namespace_name_temp_batch("viewName", rows)
}

/// Four-column `namespace`/`tableName`/`isTemporary`/`information` batch — `SHOW TABLE EXTENDED`.
fn table_extended_batch(rows: Vec<(String, String, bool, String)>) -> Result<RecordBatch> {
    use arrow::array::{BooleanArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![
        Field::new("namespace", DataType::Utf8, false),
        Field::new("tableName", DataType::Utf8, false),
        Field::new("isTemporary", DataType::Boolean, false),
        Field::new("information", DataType::Utf8, false),
    ]));
    let namespaces: Vec<String> = rows.iter().map(|(ns, _, _, _)| ns.clone()).collect();
    let names: Vec<String> = rows.iter().map(|(_, n, _, _)| n.clone()).collect();
    let temp: Vec<bool> = rows.iter().map(|(_, _, t, _)| *t).collect();
    let info: Vec<String> = rows.iter().map(|(_, _, _, i)| i.clone()).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(namespaces)),
            Arc::new(StringArray::from(names)),
            Arc::new(BooleanArray::from(temp)),
            Arc::new(StringArray::from(info)),
        ],
    )
    .map_err(|e| Error::Execution(e.to_string()))
}

/// Lowercase provider name for a [`oxidant_catalog::TableFormat`], as `USING <fmt>` renders it.
fn table_format_str(fmt: oxidant_catalog::TableFormat) -> &'static str {
    use oxidant_catalog::TableFormat;
    match fmt {
        TableFormat::Parquet => "parquet",
        TableFormat::Delta => "delta",
        TableFormat::Iceberg => "iceberg",
        TableFormat::Csv => "csv",
        TableFormat::Json => "json",
    }
}

/// Spark DDL type-name spelling for an Arrow [`DataType`](arrow::datatypes::DataType) — the column
/// type syntax Spark's own `CREATE TABLE`/`SHOW CREATE TABLE` use (`INT`, `STRING`, `DECIMAL(p,s)`,
/// `ARRAY<…>`, …). Used only by [`reconstruct_create_table_ddl`]; nested container types are
/// rendered with the same recursive shape Spark uses, though exact nested-type formatting isn't
/// pursued byte-for-byte (structural correctness is what `SHOW CREATE TABLE` needs — see
/// `spark-tests/results/show-create-table.sql.out`).
fn spark_ddl_type(dt: &arrow::datatypes::DataType) -> String {
    use arrow::datatypes::DataType;
    match dt {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 | DataType::UInt8 => "TINYINT".to_string(),
        DataType::Int16 | DataType::UInt16 => "SMALLINT".to_string(),
        DataType::Int32 | DataType::UInt32 => "INT".to_string(),
        DataType::Int64 | DataType::UInt64 => "BIGINT".to_string(),
        DataType::Float16 | DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "STRING".to_string(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "BINARY".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMP".to_string(),
        DataType::Timestamp(_, None) => "TIMESTAMP_NTZ".to_string(),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => format!("DECIMAL({p},{s})"),
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::FixedSizeList(f, _) => format!("ARRAY<{}>", spark_ddl_type(f.data_type())),
        DataType::Struct(fields) => {
            let inner: Vec<String> = fields
                .iter()
                .map(|f| format!("{}:{}", f.name(), spark_ddl_type(f.data_type())))
                .collect();
            format!("STRUCT<{}>", inner.join(","))
        }
        DataType::Map(entry, _) => match entry.data_type() {
            DataType::Struct(kv) if kv.len() == 2 => format!(
                "MAP<{},{}>",
                spark_ddl_type(kv[0].data_type()),
                spark_ddl_type(kv[1].data_type())
            ),
            _ => "MAP<STRING,STRING>".to_string(),
        },
        other => format!("{other:?}").to_uppercase(),
    }
}

/// Reconstruct a Spark-shaped `CREATE TABLE …` DDL string for `SHOW CREATE TABLE`
/// (`ShowStmt::CreateTable`) and — later — `DESCRIBE TABLE EXTENDED`. Pure formatting: every input
/// is already resolved (qualified name, Arrow schema, format string, partition columns, an
/// optional explicit location, an optional comment, and properties), so this has no I/O and can be
/// shared by both call sites without either owning catalog access.
///
/// Matches the general shape of Spark's `SHOW CREATE TABLE` output (see
/// `spark-tests/results/show-create-table.sql.out`): one column per line, `USING <fmt>`, then
/// `PARTITIONED BY`/`LOCATION`/`COMMENT`/`TBLPROPERTIES` each on their own line when present.
/// Exact byte-for-byte Spark formatting isn't attempted — properties are rendered in sorted-key
/// order for determinism (Spark preserves declaration order, which oxidant doesn't track).
fn reconstruct_create_table_ddl(
    qualified_name: &str,
    schema: &arrow::datatypes::Schema,
    format: &str,
    partition_columns: &[String],
    location: Option<&str>,
    comment: Option<&str>,
    properties: &HashMap<String, String>,
) -> String {
    let mut out = format!("CREATE TABLE {qualified_name} (\n");
    let cols: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| format!("  {} {}", f.name(), spark_ddl_type(f.data_type())))
        .collect();
    out.push_str(&cols.join(",\n"));
    out.push_str(")\n");
    out.push_str(&format!("USING {}\n", format.to_lowercase()));
    if !partition_columns.is_empty() {
        out.push_str(&format!(
            "PARTITIONED BY ({})\n",
            partition_columns.join(", ")
        ));
    }
    if let Some(loc) = location {
        out.push_str(&format!("LOCATION '{}'\n", loc.replace('\'', "\\'")));
    }
    if let Some(c) = comment {
        out.push_str(&format!("COMMENT '{}'\n", c.replace('\'', "\\'")));
    }
    if !properties.is_empty() {
        let mut keys: Vec<&String> = properties.keys().collect();
        keys.sort();
        let body: Vec<String> = keys
            .iter()
            .map(|k| {
                format!(
                    "  '{k}' = '{}'",
                    redact_property_value(k, &properties[*k]).replace('\'', "\\'")
                )
            })
            .collect();
        out.push_str("TBLPROPERTIES (\n");
        out.push_str(&body.join(",\n"));
        out.push_str(")\n");
    }
    out.trim_end().to_string()
}

/// Best-effort hive-style partition directory listing under `location`, filtered by `spec` (a
/// `PARTITION (k=v, …)` clause, possibly a subset of `partition_columns`) — backs
/// `ShowStmt::Partitions` for catalog-backed (Hive/Glue) partitioned tables. Local filesystem only
/// (`file://`/bare paths); any other scheme (`s3://`, `hdfs://`, …) returns empty rather than
/// erroring, matching `SHOW PARTITIONS`'s "empty, not an error" contract for anything v1 can't
/// introspect yet.
fn list_hive_partitions(
    location: &str,
    partition_columns: &[String],
    spec: &[(String, String)],
) -> Vec<String> {
    let Some(root) = local_fs_path(location) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    walk_hive_partitions(&root, partition_columns, spec, &mut Vec::new(), &mut out);
    out.sort();
    out
}

/// Convert a storage URI to a local filesystem path, or `None` for a scheme that isn't locally
/// listable (`s3://`, `hdfs://`, …). Handles both `file:///abs` (RFC form) and Hive's `file:/abs`
/// (single-slash, as the Metastore returns it), as well as bare paths.
fn local_fs_path(location: &str) -> Option<PathBuf> {
    if let Some(rest) = location.strip_prefix("file://") {
        return Some(PathBuf::from(rest));
    }
    if let Some(rest) = location.strip_prefix("file:") {
        return Some(PathBuf::from(rest));
    }
    if location.contains("://") {
        return None;
    }
    Some(PathBuf::from(location))
}

/// Recursively descend `dir` exactly `remaining_cols.len()` levels, expecting each level to be a
/// `key=value` directory name; pushes one `/`-joined `col1=v1/col2=v2/…` string per matching leaf
/// onto `out`. A `spec` entry restricts that column's level to the matching value; directories that
/// don't parse as `key=value` (or whose key doesn't match the expected column) are skipped.
fn walk_hive_partitions(
    dir: &std::path::Path,
    remaining_cols: &[String],
    spec: &[(String, String)],
    acc: &mut Vec<String>,
    out: &mut Vec<String>,
) {
    let Some((col, rest_cols)) = remaining_cols.split_first() else {
        out.push(acc.join("/"));
        return;
    };
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some((k, v)) = name.split_once('=') else {
            continue;
        };
        if !k.eq_ignore_ascii_case(col) {
            continue;
        }
        if let Some((_, want)) = spec.iter().find(|(sk, _)| sk.eq_ignore_ascii_case(col)) {
            if want != v {
                continue;
            }
        }
        acc.push(format!("{k}={v}"));
        walk_hive_partitions(&entry.path(), rest_cols, spec, acc, out);
        acc.pop();
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// KAN-2 regression: [`PreferBoundedJoinBuildSide`] must not re-seat a hash join that carries
    /// a non-equi filter.
    ///
    /// A join filter is evaluated against its own intermediate schema through *side-tagged* column
    /// indices. Re-seating the build side moves the inputs but not that mapping, so the predicate
    /// silently runs with its operands exchanged — no error, just a wrong answer. TPC-DS Q72 at
    /// SF10 hit exactly this: `inv_quantity_on_hand < cs_quantity` executed reversed and inflated
    /// `count(*)` ~19x (786,559 groups vs the correct 42,226).
    ///
    /// The join is built directly rather than via SQL because the firing conditions (unbounded
    /// build, bounded probe, Partitioned mode) are what the planner's own join selection keeps
    /// arranging *away*; a SQL fixture ends up with the bounded side already on the left and never
    /// exercises the rule. The no-filter case is asserted too, so this fails if the fixture ever
    /// stops meeting the conditions rather than passing vacuously.
    #[tokio::test]
    async fn filtered_join_is_not_re_seated() {
        use datafusion::common::config::ConfigOptions;
        use datafusion::common::{JoinSide, NullEquality};
        use datafusion::logical_expr::{JoinType, Operator};
        use datafusion::physical_expr::expressions::{BinaryExpr, Column};
        use datafusion::physical_optimizer::PhysicalOptimizerRule;
        use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
        use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
        use datafusion::physical_plan::ExecutionPlan;

        fn kv(rows: impl Iterator<Item = (i64, i64)>, kn: &str, vn: &str) -> Vec<RecordBatch> {
            let (ks, vs): (Vec<i64>, Vec<i64>) = rows.unzip();
            let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
                datafusion::arrow::datatypes::Field::new(
                    kn,
                    datafusion::arrow::datatypes::DataType::Int64,
                    false,
                ),
                datafusion::arrow::datatypes::Field::new(
                    vn,
                    datafusion::arrow::datatypes::DataType::Int64,
                    false,
                ),
            ]));
            vec![RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(datafusion::arrow::array::Int64Array::from(ks)),
                    Arc::new(datafusion::arrow::array::Int64Array::from(vs)),
                ],
            )
            .unwrap()]
        }

        let engine = Engine::new();
        engine
            .register_batches_with_stats(
                "f1",
                kv((0..3_000).map(|i| (i % 10, i)), "fk", "fv"),
                3_000,
            )
            .unwrap();
        engine
            .register_batches_with_stats("d1", kv((0..10).map(|k| (k, 0)), "dk", "dv"), 10)
            .unwrap();
        engine
            .register_batches_with_stats("dsmall", kv((0..10).map(|k| (k, 50)), "sk", "sv"), 10)
            .unwrap();

        // Build side: a join intermediate, whose row count is Inexact -> no provable bound.
        let left = engine
            .physical_plan("SELECT f1.fk AS ak, f1.fv AS av FROM f1 JOIN d1 ON f1.fk = d1.dk")
            .await
            .unwrap();
        // Probe side: a plain scan carrying an Exact row count -> bounded.
        let right = engine
            .physical_plan("SELECT sk, sv FROM dsmall")
            .await
            .unwrap();
        assert!(
            provable_row_bound(left.as_ref()).is_none(),
            "fixture: the build side must have no provable row bound"
        );
        assert!(
            provable_row_bound(right.as_ref()).is_some(),
            "fixture: the probe side must be bounded"
        );

        let on = vec![(
            Arc::new(Column::new("ak", 0)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
            Arc::new(Column::new("sk", 0)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        )];
        // `sv < av`, i.e. right.v < left.v, over the intermediate schema [av, sv].
        let filter_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "av",
                datafusion::arrow::datatypes::DataType::Int64,
                false,
            ),
            datafusion::arrow::datatypes::Field::new(
                "sv",
                datafusion::arrow::datatypes::DataType::Int64,
                false,
            ),
        ]));
        let filter = JoinFilter::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("sv", 1)),
                Operator::Lt,
                Arc::new(Column::new("av", 0)),
            )),
            vec![
                ColumnIndex {
                    index: 1,
                    side: JoinSide::Left,
                },
                ColumnIndex {
                    index: 1,
                    side: JoinSide::Right,
                },
            ],
            filter_schema,
        );

        let build = |f: Option<JoinFilter>| {
            Arc::new(
                HashJoinExec::try_new(
                    Arc::clone(&left),
                    Arc::clone(&right),
                    on.clone(),
                    f,
                    &JoinType::Inner,
                    None,
                    PartitionMode::Partitioned,
                    NullEquality::NullEqualsNothing,
                    false,
                )
                .unwrap(),
            ) as Arc<dyn ExecutionPlan>
        };
        // A re-seat wraps the join in a ProjectionExec to restore column order, so find the join.
        fn first_join_build_col(plan: &dyn ExecutionPlan) -> Option<String> {
            if let Some(hj) = as_hash_join(plan) {
                return Some(hj.left().schema().field(0).name().clone());
            }
            plan.children()
                .iter()
                .find_map(|c| first_join_build_col(c.as_ref()))
        }
        let build_side_name = |p: &Arc<dyn ExecutionPlan>| {
            first_join_build_col(p.as_ref()).expect("a hash join in the result")
        };

        let cfg = ConfigOptions::default();
        // Without a filter the rule fires: the bounded `dsmall` becomes the build side. This is
        // the fixture's proof that the (unbounded build, bounded probe) conditions really hold.
        let swapped = PreferBoundedJoinBuildSide
            .optimize(build(None), &cfg)
            .unwrap();
        assert_eq!(
            build_side_name(&swapped),
            "sk",
            "fixture: an unfiltered join in this shape must be re-seated onto the bounded side"
        );

        // With a filter the rule must decline, leaving the original build side in place.
        let kept = PreferBoundedJoinBuildSide
            .optimize(build(Some(filter)), &cfg)
            .unwrap();
        assert_eq!(
            build_side_name(&kept),
            "ak",
            "a join carrying a non-equi filter was re-seated: the filter's side-tagged column \
             indices now resolve from the opposite inputs, silently reversing the predicate"
        );
    }

    #[tokio::test]
    async fn select_one() {
        let engine = Engine::new();
        let batches = engine.sql("SELECT 1 AS x").await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        assert_eq!(batches[0].num_columns(), 1);
    }

    /// `OXIDANT_TABLE_BYTES_CACHE_TTL_MS` is process-global; serialize tests that mutate it.
    static TABLE_BYTES_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// F1: auto-broadcast sizing runs on every query, and the uncached path re-lists every
    /// file (on Glue+S3 it also probes the catalog per probed namespace) — the
    /// multi-second fixed tax on every query at SF10. The estimate must be cached per
    /// engine: after the first estimate, deregistering the table must NOT change the
    /// answer within the TTL (the estimator is not re-invoked), and
    /// `OXIDANT_TABLE_BYTES_CACHE_TTL_MS=0` must disable the cache (the now-missing table
    /// estimates to `None`).
    #[tokio::test]
    async fn estimate_table_bytes_caches_listing_within_ttl() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::parquet::arrow::ArrowWriter;

        let _guard = TABLE_BYTES_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_TABLE_BYTES_CACHE_TTL_MS");

        let dir = std::env::temp_dir().join(format!(
            "oxidant-est-bytes-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4]))],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("part-0.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema.clone(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let part0_len = std::fs::metadata(dir.join("part-0.parquet")).unwrap().len();

        let engine = Engine::new();
        // Trailing slash: DataFusion treats the path as a collection (lists at query time).
        engine
            .register_parquet("t", &format!("{}/", dir.display()))
            .await
            .unwrap();
        let first = engine
            .estimate_table_bytes("t")
            .await
            .expect("listing must size the table");
        assert_eq!(first, part0_len);

        // Within the TTL the cached estimate must be served without re-invoking the
        // estimator — observable because the table no longer exists to re-estimate.
        engine.deregister_table("t");
        let cached = engine.estimate_table_bytes("t").await;
        assert_eq!(
            cached,
            Some(first),
            "TTL cache hit must skip the estimator entirely"
        );

        // Disable the cache → the estimator runs and finds no such table.
        std::env::set_var("OXIDANT_TABLE_BYTES_CACHE_TTL_MS", "0");
        let uncached = engine.estimate_table_bytes("t").await;
        std::env::remove_var("OXIDANT_TABLE_BYTES_CACHE_TTL_MS");
        assert_eq!(uncached, None, "disabled cache must re-run the estimator");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---------------------------------------------------------------------
    // KAN-81 — the sizing walk must honor a table reference's catalog/namespace qualifier:
    // `glue.db.t` probes Glue's `db` exactly once (previously it discarded the qualifier and
    // brute-force searched every namespace of every external catalog — on Glue, O(databases)
    // `GetTable` calls per table per query). These tests drive `estimate_table_stats` against
    // a catalog that RECORDS every probe so the call shape is asserted directly.
    // ---------------------------------------------------------------------

    /// A fake external catalog that records every `list_namespaces`/`load_table` call and
    /// resolves `"<db>.<table>"` keys to a local parquet dir (so the byte-sizing half of
    /// `estimate_stats_for_metadata` yields real bytes).
    struct RecordingCatalog {
        tables: HashMap<String, String>,
        list_calls: Mutex<Vec<Vec<String>>>,
        load_calls: Mutex<Vec<(Vec<String>, String)>>,
    }

    impl RecordingCatalog {
        fn new(tables: HashMap<String, String>) -> Self {
            Self {
                tables,
                list_calls: Mutex::new(Vec::new()),
                load_calls: Mutex::new(Vec::new()),
            }
        }
        fn list_calls(&self) -> Vec<Vec<String>> {
            self.list_calls.lock().expect("list_calls poisoned").clone()
        }
        fn load_calls(&self) -> Vec<(Vec<String>, String)> {
            self.load_calls.lock().expect("load_calls poisoned").clone()
        }
    }

    #[async_trait::async_trait]
    impl oxidant_catalog::CatalogProvider for RecordingCatalog {
        fn name(&self) -> &str {
            "testcat"
        }
        async fn list_namespaces(
            &self,
            parent: &[String],
        ) -> oxidant_catalog::Result<Vec<Vec<String>>> {
            self.list_calls
                .lock()
                .expect("list_calls poisoned")
                .push(parent.to_vec());
            if parent.is_empty() {
                Ok(vec![vec!["db1".to_string()]])
            } else {
                Ok(vec![])
            }
        }
        async fn list_tables(&self, namespace: &[String]) -> oxidant_catalog::Result<Vec<String>> {
            let prefix = format!("{}.", namespace.join("."));
            Ok(self
                .tables
                .keys()
                .filter_map(|k| k.strip_prefix(&prefix).map(str::to_string))
                .collect())
        }
        async fn load_table(
            &self,
            namespace: &[String],
            table: &str,
        ) -> oxidant_catalog::Result<oxidant_catalog::TableMetadata> {
            self.load_calls
                .lock()
                .expect("load_calls poisoned")
                .push((namespace.to_vec(), table.to_string()));
            let key = format!("{}.{table}", namespace.join("."));
            let location = self
                .tables
                .get(&key)
                .ok_or_else(|| oxidant_catalog::Error::Plan(format!("no such table `{key}`")))?;
            Ok(oxidant_catalog::TableMetadata::new(
                key,
                location.clone(),
                oxidant_catalog::TableFormat::Parquet,
            ))
        }
    }

    /// Write a tiny one-column parquet file into a fresh temp dir and return the dir (the
    /// catalog-table location the sizing walk lists).
    fn kan81_parquet_dir(tag: &str) -> PathBuf {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::parquet::arrow::ArrowWriter;

        let dir = std::env::temp_dir().join(format!(
            "oxidant-kan81-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("part-0.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        dir
    }

    /// A fully-qualified `testcat.db1.orders` estimate probes the named catalog/namespace
    /// exactly once — no namespace enumeration, no other catalog searched.
    #[tokio::test]
    async fn estimate_stats_qualified_name_probes_once_without_enumeration() {
        let dir = kan81_parquet_dir("qualified");
        let mut tables = HashMap::new();
        tables.insert(
            "db1.orders".to_string(),
            format!("file://{}/", dir.display()),
        );
        let catalog = Arc::new(RecordingCatalog::new(tables));

        let engine = Engine::new();
        engine.register_catalog("testcat", catalog.clone());

        let (bytes, _rows) = engine.estimate_table_stats("testcat.db1.orders").await;
        assert!(bytes.is_some(), "the parquet dir must size: {bytes:?}");
        assert_eq!(
            catalog.load_calls(),
            vec![(vec!["db1".to_string()], "orders".to_string())],
            "exactly one load_table against the qualified namespace"
        );
        assert!(
            catalog.list_calls().is_empty(),
            "a qualified reference must never enumerate namespaces: {:?}",
            catalog.list_calls()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A fully-qualified miss (the catalog says not-found) returns "unknown" after the same
    /// single probe — sizing is best-effort and must not fail or fan out.
    #[tokio::test]
    async fn estimate_stats_qualified_miss_is_single_probe_unknown() {
        let catalog = Arc::new(RecordingCatalog::new(HashMap::new()));

        let engine = Engine::new();
        engine.register_catalog("testcat", catalog.clone());

        let stats = engine.estimate_table_stats("testcat.db1.nope").await;
        assert_eq!(stats, (None, None));
        assert_eq!(
            catalog.load_calls(),
            vec![(vec!["db1".to_string()], "nope".to_string())],
        );
        assert!(catalog.list_calls().is_empty());
    }

    /// A bare table under the builtin catalog keeps the legacy fallback: enumerate the
    /// registered external catalogs' namespaces and probe each until one resolves.
    #[tokio::test]
    async fn estimate_stats_bare_name_keeps_legacy_catalog_search() {
        let dir = kan81_parquet_dir("bare");
        let mut tables = HashMap::new();
        tables.insert(
            "db1.orders".to_string(),
            format!("file://{}/", dir.display()),
        );
        let catalog = Arc::new(RecordingCatalog::new(tables));

        let engine = Engine::new();
        engine.register_catalog("testcat", catalog.clone());

        let (bytes, _rows) = engine.estimate_table_stats("orders").await;
        assert!(
            bytes.is_some(),
            "legacy search must find db1.orders: {bytes:?}"
        );
        assert!(
            !catalog.list_calls().is_empty(),
            "the fallback enumerates namespaces"
        );
        assert!(
            catalog
                .load_calls()
                .contains(&(vec!["db1".to_string()], "orders".to_string())),
            "probed db1.orders: {:?}",
            catalog.load_calls()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `USE <external-catalog>` with no current namespace restricts the bare-name search to
    /// that catalog: the table lives ONLY in a second registered catalog, so the search must
    /// miss with the second catalog never probed (deterministic — under the old all-catalog
    /// fan-out the table resolves, regardless of HashMap iteration order).
    #[tokio::test]
    async fn estimate_stats_use_catalog_restricts_search_to_that_catalog() {
        let dir = kan81_parquet_dir("usecat");
        let mut tables = HashMap::new();
        tables.insert(
            "db1.orders".to_string(),
            format!("file://{}/", dir.display()),
        );
        let catalog = Arc::new(RecordingCatalog::new(HashMap::new()));
        let other = Arc::new(RecordingCatalog::new(tables));

        let engine = Engine::new();
        engine.register_catalog("testcat", catalog.clone());
        engine.register_catalog("othercat", other.clone());
        // KAN-84: reachable from SQL now — `USE CATALOG testcat` leaves the external catalog's
        // namespace empty (no default-namespace metadata on providers), which is exactly the
        // external-catalog-with-empty-namespace branch under test.
        engine.sql("USE CATALOG testcat").await.unwrap();

        let stats = engine.estimate_table_stats("orders").await;
        assert_eq!(
            stats,
            (None, None),
            "restricted to testcat, which does not have orders"
        );
        assert!(
            !catalog.list_calls().is_empty(),
            "the current catalog is still searched"
        );
        assert!(
            other.list_calls().is_empty() && other.load_calls().is_empty(),
            "another catalog must not be searched: {:?} / {:?}",
            other.list_calls(),
            other.load_calls()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A 3+-segment name whose catalog isn't registered can never resolve — the walk must not
    /// fan out across every catalog looking for it (zero probes, "unknown").
    #[tokio::test]
    async fn estimate_stats_unknown_qualified_catalog_never_fans_out() {
        let catalog = Arc::new(RecordingCatalog::new(HashMap::new()));

        let engine = Engine::new();
        engine.register_catalog("testcat", catalog.clone());

        let stats = engine.estimate_table_stats("glu.db1.orders").await;
        assert_eq!(stats, (None, None));
        assert!(
            catalog.list_calls().is_empty() && catalog.load_calls().is_empty(),
            "an unregistered catalog qualifier must not probe any catalog: {:?} / {:?}",
            catalog.list_calls(),
            catalog.load_calls()
        );
    }

    // ---------------------------------------------------------------------
    // KAN-52 — Spark's default NULL ordering: ASC → NULLS FIRST, DESC → NULLS LAST
    // (https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-orderby.html:
    // "If null_sort_order is not specified, then NULLs sort first if sort order is ASC
    // and NULLS sort last if sort order is DESC"). oxidant pins it via
    // `datafusion.sql_parser.default_null_ordering = "nulls_min"` in `Engine::new_inner`
    // (DataFusion's own default is Postgres's `nulls_max`, the exact opposite); these
    // tests fail if that setting or the explicit NULLS FIRST/LAST passthrough regresses.
    // ---------------------------------------------------------------------

    /// Collect column `col` across `batches` as ordered `Option<i32>`s so a test can assert
    /// exactly where NULL keys land in a sort.
    fn int32_column(batches: &[RecordBatch], col: usize) -> Vec<Option<i32>> {
        use arrow::array::{Array, Int32Array};
        let mut out = Vec::new();
        for b in batches {
            let arr = b
                .column(col)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("INT column");
            out.extend((0..arr.len()).map(|i| (!arr.is_null(i)).then(|| arr.value(i))));
        }
        out
    }

    /// Register a one-column nullable `INT` table under `name`, rows in the order given.
    fn register_int_table(engine: &Engine, name: &str, values: &[Option<i32>]) {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values.to_vec()))])
            .unwrap();
        engine.register_batches(name, vec![batch]).unwrap();
    }

    /// Spark's default: ASC sorts NULL keys FIRST (`nulls_min`), not Postgres's NULLS LAST.
    #[tokio::test]
    async fn kan52_default_asc_sorts_nulls_first() {
        let engine = Engine::new();
        register_int_table(&engine, "t", &[Some(2), None, Some(1)]);
        let batches = engine.sql("SELECT x FROM t ORDER BY x").await.unwrap();
        assert_eq!(int32_column(&batches, 0), vec![None, Some(1), Some(2)]);
    }

    /// Spark's default: DESC sorts NULL keys LAST.
    #[tokio::test]
    async fn kan52_default_desc_sorts_nulls_last() {
        let engine = Engine::new();
        register_int_table(&engine, "t", &[Some(2), None, Some(1)]);
        let batches = engine.sql("SELECT x FROM t ORDER BY x DESC").await.unwrap();
        assert_eq!(int32_column(&batches, 0), vec![Some(2), Some(1), None]);
    }

    /// Explicit NULLS LAST overrides the ASC default and must be honored as written.
    #[tokio::test]
    async fn kan52_explicit_nulls_last_on_asc_honored() {
        let engine = Engine::new();
        register_int_table(&engine, "t", &[Some(2), None, Some(1)]);
        let batches = engine
            .sql("SELECT x FROM t ORDER BY x ASC NULLS LAST")
            .await
            .unwrap();
        assert_eq!(int32_column(&batches, 0), vec![Some(1), Some(2), None]);
    }

    /// Explicit NULLS FIRST overrides any direction default (TPC-DS q11 spells NULLS FIRST
    /// explicitly — the clause must round-trip untouched).
    #[tokio::test]
    async fn kan52_explicit_nulls_first_honored() {
        let engine = Engine::new();
        register_int_table(&engine, "t", &[Some(2), None, Some(1)]);
        for (sql, expected) in [
            // No direction → ASC with explicit NULLS FIRST.
            (
                "SELECT x FROM t ORDER BY x NULLS FIRST",
                vec![None, Some(1), Some(2)],
            ),
            // DESC with explicit NULLS FIRST (opposite of the DESC default).
            (
                "SELECT x FROM t ORDER BY x DESC NULLS FIRST",
                vec![None, Some(2), Some(1)],
            ),
        ] {
            let batches = engine.sql(sql).await.unwrap();
            assert_eq!(int32_column(&batches, 0), expected, "{sql}");
        }
    }

    /// The ORDER BY … LIMIT boundary (the TPC-DS Q12-class shape): the NULL-keyed row must
    /// fall INSIDE the LIMIT window for default ASC and OUTSIDE it for default DESC — this is
    /// exactly where a wrong default flips the result set, not just the row order.
    #[tokio::test]
    async fn kan52_order_by_limit_boundary_matches_spark() {
        let engine = Engine::new();
        register_int_table(&engine, "t", &[Some(3), None, Some(1), Some(2)]);
        let asc = engine
            .sql("SELECT x FROM t ORDER BY x LIMIT 2")
            .await
            .unwrap();
        assert_eq!(int32_column(&asc, 0), vec![None, Some(1)]);
        let desc = engine
            .sql("SELECT x FROM t ORDER BY x DESC LIMIT 2")
            .await
            .unwrap();
        assert_eq!(int32_column(&desc, 0), vec![Some(3), Some(2)]);
    }

    /// The same default applies to the within-window ORDER BY (where it changes computed
    /// values, not just row order): under Spark's `nulls_min` the NULL-keyed row ranks FIRST
    /// for ASC and LAST for DESC.
    #[tokio::test]
    async fn kan52_window_order_by_uses_spark_default() {
        let engine = Engine::new();
        register_int_table(&engine, "t", &[Some(2), None, Some(1)]);
        let asc = engine
            .sql(
                "SELECT r FROM \
                 (SELECT x, CAST(RANK() OVER (ORDER BY x) AS INT) AS r FROM t) \
                 WHERE x IS NULL",
            )
            .await
            .unwrap();
        assert_eq!(int32_column(&asc, 0), vec![Some(1)]);
        let desc = engine
            .sql(
                "SELECT r FROM \
                 (SELECT x, CAST(RANK() OVER (ORDER BY x DESC) AS INT) AS r FROM t) \
                 WHERE x IS NULL",
            )
            .await
            .unwrap();
        assert_eq!(int32_column(&desc, 0), vec![Some(3)]);
    }

    /// Distributed finalize replay: in a distributed run the driver re-parses the finalize
    /// `ORDER BY` on a *fresh* engine (oxidant-execution's `build_finalize` emits the null
    /// ordering resolved at plan time as an explicit `NULLS FIRST`/`NULLS LAST` clause, so
    /// the finalizer's own default never gets a say). This emulates that hop at the loom
    /// level: gathered rows replayed through the resolved finalize SQL must reproduce the
    /// single-node order.
    #[tokio::test]
    async fn kan52_finalize_replay_on_fresh_engine_matches_single_node() {
        let single = Engine::new();
        register_int_table(&single, "t", &[Some(2), None, Some(1)]);
        for (query, finalize_sql) in [
            (
                "SELECT x FROM t ORDER BY x",
                "SELECT x FROM result ORDER BY x ASC NULLS FIRST",
            ),
            (
                "SELECT x FROM t ORDER BY x DESC",
                "SELECT x FROM result ORDER BY x DESC NULLS LAST",
            ),
        ] {
            let expected = single.sql(query).await.unwrap();
            // The driver's finalize engine sees the gathered stage output as `result`, in
            // whatever order the workers produced (stages are unordered by contract).
            let finalizer = Engine::new();
            register_int_table(&finalizer, "result", &[Some(1), None, Some(2)]);
            let actual = finalizer.sql(finalize_sql).await.unwrap();
            assert_eq!(
                int32_column(&actual, 0),
                int32_column(&expected, 0),
                "{query} vs {finalize_sql}"
            );
        }
    }

    #[tokio::test]
    async fn default_current_catalog_and_namespace() {
        let engine = Engine::new();
        let (catalog, namespace) = engine.current_catalog_and_namespace();
        assert_eq!(catalog, "spark_catalog");
        assert_eq!(namespace, vec!["default".to_string()]);
    }

    #[tokio::test]
    async fn use_namespace_updates_current_namespace() {
        let engine = Engine::new();
        // KAN-86: USE validates existence — register the schema first.
        engine
            .ctx()
            .catalog(oxidant_catalog::DEFAULT_CATALOG)
            .unwrap()
            .register_schema(
                "somedb",
                Arc::new(datafusion::catalog::MemorySchemaProvider::new()),
            )
            .unwrap();
        let batches = engine.sql("USE somedb").await.unwrap();
        assert!(batches.is_empty(), "USE should yield no batches");
        let (catalog, namespace) = engine.current_catalog_and_namespace();
        // Current catalog is unchanged (bare `USE <db>` only switches the namespace).
        assert_eq!(catalog, "spark_catalog");
        assert_eq!(namespace, vec!["somedb".to_string()]);
    }

    /// KAN-86: `USE <missing-db>` fails with Spark's `[SCHEMA_NOT_FOUND]` shape and leaves the
    /// session state untouched — for both the builtin catalog and an external one.
    #[tokio::test]
    async fn use_missing_namespace_errors_schema_not_found() {
        let engine = Engine::new();
        let err = engine.sql("USE nosuchdb").await.unwrap_err();
        match err {
            Error::Plan(msg) => {
                assert!(msg.contains("[SCHEMA_NOT_FOUND]"), "{msg}");
                // Spark's message carries the catalog-qualified name.
                assert!(msg.contains("`spark_catalog`.`nosuchdb`"), "{msg}");
            }
            other => panic!("expected Plan, got {other:?}"),
        }
        let (catalog, namespace) = engine.current_catalog_and_namespace();
        assert_eq!(catalog, "spark_catalog");
        assert_eq!(namespace, vec!["default".to_string()]);

        // External: same shape, validated against the provider's namespace listing.
        let engine = Engine::new();
        engine.register_catalog("testcat", Arc::new(RecordingCatalog::new(HashMap::new())));
        let err = engine.sql("USE testcat.nosuchdb").await.unwrap_err();
        match err {
            Error::Plan(msg) => assert!(msg.contains("[SCHEMA_NOT_FOUND]"), "{msg}"),
            other => panic!("expected Plan, got {other:?}"),
        }
        // The failed USE changed nothing.
        assert_eq!(engine.current_catalog_and_namespace().0, "spark_catalog");
    }

    /// KAN-86: external validation actually walks the provider's namespace listing, and an
    /// existing external namespace validates cleanly.
    #[tokio::test]
    async fn use_external_namespace_validates_via_list_namespaces() {
        let catalog = Arc::new(RecordingCatalog::new(HashMap::new()));
        let engine = Engine::new();
        engine.register_catalog("testcat", catalog.clone());

        engine.sql("USE testcat.db1").await.unwrap();
        assert_eq!(
            engine.current_catalog_and_namespace(),
            ("testcat".to_string(), vec!["db1".to_string()])
        );
        assert!(
            catalog.list_calls().contains(&Vec::new()),
            "validation listed top-level namespaces: {:?}",
            catalog.list_calls()
        );

        // A second level on a single-level provider is SCHEMA_NOT_FOUND (its list_namespaces
        // returns no children for a non-empty parent).
        let err = engine.sql("USE testcat.db1.deeper").await.unwrap_err();
        match err {
            Error::Plan(msg) => assert!(msg.contains("[SCHEMA_NOT_FOUND]"), "{msg}"),
            other => panic!("expected Plan, got {other:?}"),
        }
    }

    // ---------------------------------------------------------------------
    // KAN-85 — per-session catalog/namespace state: one Connect server's sessions share the
    // engine's catalogs/tables/caches but never each other's `USE` state.
    // ---------------------------------------------------------------------

    /// A's `USE CATALOG` leaves B (and the base handle) at the default; re-deriving a session
    /// returns the same cell.
    #[tokio::test]
    async fn for_session_isolates_current_catalog_and_namespace() {
        let engine = Engine::new();
        engine.register_catalog("testcat", Arc::new(RecordingCatalog::new(HashMap::new())));

        let a = engine.for_session("a");
        let b = engine.for_session("b");
        a.sql("USE CATALOG testcat").await.unwrap();

        assert_eq!(a.current_catalog_and_namespace().0, "testcat");
        assert!(
            a.current_catalog_and_namespace().1.is_empty(),
            "external catalog switch clears the namespace (KAN-84)"
        );
        assert_eq!(
            b.current_catalog_and_namespace(),
            ("spark_catalog".to_string(), vec!["default".to_string()]),
            "session B is untouched by A's USE"
        );
        assert_eq!(
            engine.current_catalog_and_namespace().0,
            "spark_catalog",
            "the base handle keeps its own state (CLI/tests unchanged)"
        );
        // Same session id → same cell.
        assert_eq!(
            engine.for_session("a").current_catalog_and_namespace().0,
            "testcat"
        );
    }

    /// Bare-name resolution follows the SESSION's catalog: A (current catalog testcat) answers
    /// `SHOW TABLES` from testcat while B stays on the builtin catalog.
    #[tokio::test]
    async fn for_session_bare_name_resolution_uses_session_catalog() {
        use arrow::array::StringArray;
        let dir = kan81_parquet_dir("kan85-show");
        let mut tables = HashMap::new();
        tables.insert(
            "db1.orders".to_string(),
            format!("file://{}/", dir.display()),
        );
        let engine = Engine::new();
        engine.register_catalog("testcat", Arc::new(RecordingCatalog::new(tables)));

        let a = engine.for_session("a");
        a.sql("USE CATALOG testcat").await.unwrap();
        let b = engine.for_session("b");

        let batches = a.sql("SHOW TABLES").await.unwrap();
        let names: Vec<String> = batches
            .iter()
            .flat_map(|batch| {
                let col = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..batch.num_rows())
                    .map(|i| col.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(names, vec!["orders".to_string()], "A sees testcat's tables");

        // B's bare SHOW TABLES reads the builtin catalog's default schema (no orders there).
        let batches = b.sql("SHOW TABLES").await.unwrap();
        let names: Vec<String> = batches
            .iter()
            .flat_map(|batch| {
                let col = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..batch.num_rows())
                    .map(|i| col.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert!(
            !names.contains(&"orders".to_string()),
            "B must not see A's catalog: {names:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The KAN-81 sizing walk reads the SESSION's catalog state: A (current catalog testcat,
    /// empty namespace) restricts a bare-name estimate to testcat; B (default state) takes the
    /// legacy all-catalog search — and A's cached estimate does not leak into B (the cache key
    /// carries the resolved catalog/namespace).
    #[tokio::test]
    async fn for_session_sizing_walk_uses_session_catalog_state() {
        let dir = kan81_parquet_dir("kan85-size");
        let mut tables = HashMap::new();
        tables.insert(
            "db1.orders".to_string(),
            format!("file://{}/", dir.display()),
        );
        let catalog = Arc::new(RecordingCatalog::new(tables));
        let other = Arc::new(RecordingCatalog::new(HashMap::new()));

        let engine = Engine::new();
        engine.register_catalog("testcat", catalog.clone());
        engine.register_catalog("othercat", other.clone());

        let a = engine.for_session("a");
        a.sql("USE CATALOG testcat").await.unwrap();
        let b = engine.for_session("b");

        // A: restricted to testcat — found there, othercat never probed.
        let (bytes, _) = a.estimate_table_stats("orders").await;
        assert!(bytes.is_some(), "A sizes orders via testcat: {bytes:?}");
        assert!(
            other.list_calls().is_empty() && other.load_calls().is_empty(),
            "A's estimate must not touch othercat: {:?} / {:?}",
            other.list_calls(),
            other.load_calls()
        );

        // B: same bare name under the builtin catalog → legacy all-catalog search. A cache
        // leak would return A's entry with NO new catalog probes; the resolved-triple cache
        // key forces B's own search instead.
        let probes_after_a = catalog.list_calls().len();
        let (bytes, _) = b.estimate_table_stats("orders").await;
        assert!(
            bytes.is_some(),
            "B sizes orders via the legacy search: {bytes:?}"
        );
        assert!(
            catalog.list_calls().len() > probes_after_a,
            "B's estimate runs its own search (no cache leak from A): {:?}",
            catalog.list_calls()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// KAN-86: builtin schema names fold case (Spark's v1 `formatDatabaseName`,
    /// `caseSensitive=false` default) — `USE DEFAULT` is legal and stores the REGISTERED
    /// casing so later schema lookups hit. External catalogs stay exact (v2 `namespaceExists`).
    #[tokio::test]
    async fn use_builtin_namespace_matches_case_insensitively() {
        let engine = Engine::new();
        engine
            .ctx()
            .catalog(oxidant_catalog::DEFAULT_CATALOG)
            .unwrap()
            .register_schema(
                "somedb",
                Arc::new(datafusion::catalog::MemorySchemaProvider::new()),
            )
            .unwrap();
        engine.sql("USE SOMEDB").await.unwrap();
        assert_eq!(
            engine.current_catalog_and_namespace().1,
            vec!["somedb".to_string()],
            "case-folded match stores the registered casing"
        );
        engine.sql("USE DEFAULT").await.unwrap();
        assert_eq!(
            engine.current_catalog_and_namespace().1,
            vec!["default".to_string()]
        );
    }

    /// KAN-87 divergence pin: re-`USE CATALOG` of the builtin under different casing is a
    /// canonical no-op — the namespace is NOT reset. Spark's `setCurrentCatalog` compares the
    /// raw string case-sensitively (`currentCatalog.name() != catalogName`) and WOULD reset to
    /// `["default"]` here; we read that as an implementation accident (its `catalog()` folds
    /// the session-catalog case anyway) and deliberately do not reproduce it.
    #[tokio::test]
    async fn use_catalog_builtin_case_variant_is_noop_not_reset() {
        let engine = Engine::new();
        engine
            .ctx()
            .catalog(oxidant_catalog::DEFAULT_CATALOG)
            .unwrap()
            .register_schema(
                "somedb",
                Arc::new(datafusion::catalog::MemorySchemaProvider::new()),
            )
            .unwrap();
        engine.sql("USE somedb").await.unwrap();
        engine.sql("USE CATALOG Spark_Catalog").await.unwrap();
        assert_eq!(
            engine.current_catalog_and_namespace(),
            ("spark_catalog".to_string(), vec!["somedb".to_string()]),
            "canonical same-catalog no-op keeps the namespace"
        );
    }

    /// KAN-86: a backend failure during external namespace validation propagates (never
    /// silently accepted) and leaves the session state untouched.
    #[tokio::test]
    async fn use_external_namespace_validation_io_propagates() {
        struct ThrottledListCatalog;
        #[async_trait::async_trait]
        impl oxidant_catalog::CatalogProvider for ThrottledListCatalog {
            fn name(&self) -> &str {
                "throttled"
            }
            async fn list_namespaces(
                &self,
                _parent: &[String],
            ) -> oxidant_catalog::Result<Vec<Vec<String>>> {
                Err(oxidant_catalog::Error::Io(
                    "aws glue GetDatabases: ThrottlingException: rate exceeded".to_string(),
                ))
            }
            async fn list_tables(
                &self,
                _namespace: &[String],
            ) -> oxidant_catalog::Result<Vec<String>> {
                Ok(vec![])
            }
            async fn load_table(
                &self,
                _namespace: &[String],
                _table: &str,
            ) -> oxidant_catalog::Result<oxidant_catalog::TableMetadata> {
                unreachable!("validation fails before any load_table")
            }
        }

        let engine = Engine::new();
        engine.register_catalog("throttled", Arc::new(ThrottledListCatalog));
        let err = engine.sql("USE throttled.somedb").await.unwrap_err();
        match err {
            Error::Io(msg) => assert!(msg.contains("ThrottlingException"), "{msg}"),
            other => panic!("expected Io, got {other:?}"),
        }
        assert_eq!(
            engine.current_catalog_and_namespace(),
            ("spark_catalog".to_string(), vec!["default".to_string()]),
            "a failed USE leaves the session state untouched"
        );
    }

    /// KAN-85: `drop_session` evicts the session's cell — the next `for_session` for the id
    /// re-seeds from the default.
    #[tokio::test]
    async fn drop_session_evicts_the_session_cell() {
        let engine = Engine::new();
        engine.register_catalog("testcat", Arc::new(RecordingCatalog::new(HashMap::new())));
        engine
            .for_session("ephemeral")
            .sql("USE CATALOG testcat")
            .await
            .unwrap();
        engine.drop_session("ephemeral");
        assert_eq!(
            engine
                .for_session("ephemeral")
                .current_catalog_and_namespace(),
            ("spark_catalog".to_string(), vec!["default".to_string()]),
            "evicted session re-seeds from the default"
        );
    }

    /// KAN-85: `set_default_catalog` seeds only NEW sessions; an existing session's `USE`
    /// state is not clobbered; an external default seeds the empty namespace (KAN-84).
    #[tokio::test]
    async fn set_default_catalog_seeds_new_sessions_only() {
        let engine = Engine::new();
        engine.register_catalog("testcat", Arc::new(RecordingCatalog::new(HashMap::new())));
        let existing = engine.for_session("existing");
        engine.set_default_catalog("testcat").unwrap();

        let new = engine.for_session("new");
        assert_eq!(
            new.current_catalog_and_namespace(),
            ("testcat".to_string(), vec![]),
            "new session seeds from the default catalog (external → empty namespace)"
        );
        assert_eq!(
            existing.current_catalog_and_namespace(),
            ("spark_catalog".to_string(), vec!["default".to_string()]),
            "existing session keeps its state"
        );
        // An unregistered default is rejected.
        assert!(engine.set_default_catalog("nope").is_err());
    }

    /// KAN-86/RPC: `setCurrentDatabase("")` is rejected — an empty name would otherwise parse
    /// to an empty namespace that validation accepts unconditionally.
    #[tokio::test]
    async fn set_current_namespace_rejects_empty() {
        let engine = Engine::new();
        for blank in ["", "  ", "``"] {
            let err = engine.set_current_namespace(blank).await.unwrap_err();
            assert!(matches!(err, Error::Plan(_)), "{blank:?}: {err:?}");
        }
        assert_eq!(
            engine.current_catalog_and_namespace().1,
            vec!["default".to_string()],
            "state untouched after rejected setCurrentDatabase"
        );
    }

    #[tokio::test]
    async fn use_unknown_catalog_errors() {
        let engine = Engine::new();
        let err = engine.sql("USE nonexistent_catalog.x").await.unwrap_err();
        assert!(
            matches!(err, Error::Plan(_)),
            "expected a Plan error, got {err:?}"
        );
        // Current catalog/namespace are unchanged after the failed USE.
        let (catalog, namespace) = engine.current_catalog_and_namespace();
        assert_eq!(catalog, "spark_catalog");
        assert_eq!(namespace, vec!["default".to_string()]);
    }

    // ---------------------------------------------------------------------
    // KAN-84 — `USE CATALOG <catalog>` resets the current namespace (Spark's
    // `CatalogManager.setCurrentCatalog`: the namespace override is cleared on a switch and
    // the switch is a no-op when the catalog is already current). Builtin `spark_catalog`
    // resets to its default namespace `["default"]`; external catalogs have no
    // default-namespace metadata, so their namespace becomes EMPTY.
    // ---------------------------------------------------------------------

    /// `USE CATALOG <external>` sets the catalog and clears the namespace (empty = "no
    /// database selected", per the provider contract).
    #[tokio::test]
    async fn use_catalog_external_clears_current_namespace() {
        let engine = Engine::new();
        engine.register_catalog("testcat", Arc::new(RecordingCatalog::new(HashMap::new())));
        engine.sql("USE CATALOG testcat").await.unwrap();
        let (catalog, namespace) = engine.current_catalog_and_namespace();
        assert_eq!(catalog, "testcat");
        assert!(
            namespace.is_empty(),
            "external catalog switch clears the namespace: {namespace:?}"
        );
    }

    /// `USE <catalog>.<db>` sets both (namespace given explicitly — nothing to reset).
    #[tokio::test]
    async fn use_catalog_dot_namespace_sets_both() {
        let engine = Engine::new();
        engine.register_catalog("testcat", Arc::new(RecordingCatalog::new(HashMap::new())));
        engine.sql("USE testcat.db1").await.unwrap();
        let (catalog, namespace) = engine.current_catalog_and_namespace();
        assert_eq!(catalog, "testcat");
        assert_eq!(namespace, vec!["db1".to_string()]);
    }

    /// Switching back to the builtin catalog resets the current database to `default` — Spark
    /// resets the v1 session catalog's database on any catalog switch
    /// (`setCurrentCatalog` → `setCurrentDatabase(default)`), so `spark_catalog` → `testcat` →
    /// `spark_catalog` lands on `["default"]`, not whatever namespace the external catalog had.
    #[tokio::test]
    async fn use_catalog_builtin_resets_namespace_to_default() {
        let engine = Engine::new();
        engine.register_catalog("testcat", Arc::new(RecordingCatalog::new(HashMap::new())));
        engine.sql("USE testcat.db1").await.unwrap();
        engine.sql("USE CATALOG spark_catalog").await.unwrap();
        let (catalog, namespace) = engine.current_catalog_and_namespace();
        assert_eq!(catalog, "spark_catalog");
        assert_eq!(namespace, vec!["default".to_string()]);
    }

    /// Spark's `setCurrentCatalog` is a no-op when the catalog is already current — the
    /// namespace override survives a redundant `USE CATALOG`.
    #[tokio::test]
    async fn use_catalog_same_catalog_is_noop() {
        let engine = Engine::new();
        engine.register_catalog("testcat", Arc::new(RecordingCatalog::new(HashMap::new())));
        engine.sql("USE testcat.db1").await.unwrap();
        engine.sql("USE CATALOG testcat").await.unwrap();
        let (catalog, namespace) = engine.current_catalog_and_namespace();
        assert_eq!(catalog, "testcat");
        assert_eq!(
            namespace,
            vec!["db1".to_string()],
            "re-USE of the current catalog keeps the namespace (Spark no-op)"
        );
    }

    /// KAN-87: only the session catalog's name matches case-insensitively — Spark's
    /// `CatalogManager.catalog` does `name.equalsIgnoreCase(SESSION_CATALOG_NAME)` and keeps
    /// v2 plugin catalogs as exact map keys. The canonical lowercase name is stored, so
    /// downstream `== spark_catalog` checks keep working.
    #[tokio::test]
    async fn use_catalog_builtin_matches_case_insensitively() {
        let engine = Engine::new();
        for name in ["SPARK_CATALOG", "Spark_Catalog"] {
            engine.sql(&format!("USE CATALOG {name}")).await.unwrap();
            let (catalog, namespace) = engine.current_catalog_and_namespace();
            assert_eq!(
                catalog, "spark_catalog",
                "canonical name stored for `{name}`"
            );
            assert_eq!(namespace, vec!["default".to_string()]);
        }
    }

    /// KAN-87: external catalog names are exact — a case-mismatched `USE CATALOG TESTCAT`
    /// fails with CATALOG_NOT_FOUND rather than matching the registered `testcat` (Spark: v2
    /// plugin catalogs are exact map keys, no case folding).
    #[tokio::test]
    async fn use_catalog_external_names_are_case_sensitive() {
        let engine = Engine::new();
        engine.register_catalog("testcat", Arc::new(RecordingCatalog::new(HashMap::new())));
        let err = engine.sql("USE CATALOG TESTCAT").await.unwrap_err();
        match err {
            Error::Plan(msg) => assert!(msg.contains("[CATALOG_NOT_FOUND]"), "{msg}"),
            other => panic!("expected Plan, got {other:?}"),
        }
        // The exact-case form resolves.
        engine.sql("USE CATALOG testcat").await.unwrap();
        assert_eq!(engine.current_catalog_and_namespace().0, "testcat");
    }

    /// Bare `SHOW TABLES` after `USE CATALOG <external>` (no current database) lists the union
    /// across the catalog's top-level namespaces — the `SHOW TABLES IN <cat>` shape — instead
    /// of erroring.
    #[tokio::test]
    async fn show_tables_after_use_catalog_lists_across_namespaces() {
        use arrow::array::StringArray;
        let dir = kan81_parquet_dir("usecat-show");
        let mut tables = HashMap::new();
        tables.insert(
            "db1.orders".to_string(),
            format!("file://{}/", dir.display()),
        );
        let engine = Engine::new();
        engine.register_catalog("testcat", Arc::new(RecordingCatalog::new(tables)));
        engine.sql("USE CATALOG testcat").await.unwrap();

        let batches = engine.sql("SHOW TABLES").await.unwrap();
        let rows: Vec<(String, String)> = batches
            .iter()
            .flat_map(|b| {
                let ns = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                let t = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                (0..b.num_rows())
                    .map(|i| (ns.value(i).to_string(), t.value(i).to_string()))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert!(
            rows.contains(&("db1".to_string(), "orders".to_string())),
            "union across top-level namespaces: {rows:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A bare single-table SHOW after `USE CATALOG <external>` can't be qualified (no current
    /// database): the provider is probed with the EMPTY namespace and the not-found surfaces
    /// Spark-shaped as `TABLE_OR_VIEW_NOT_FOUND` naming `testcat.orders` — never a raw
    /// `testcat..orders` SQL syntax error.
    #[tokio::test]
    async fn show_columns_after_use_catalog_probes_empty_namespace() {
        let catalog = Arc::new(RecordingCatalog::new(HashMap::new()));
        let engine = Engine::new();
        engine.register_catalog("testcat", catalog.clone());
        engine.sql("USE CATALOG testcat").await.unwrap();
        let err = engine.sql("SHOW COLUMNS IN orders").await.unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "not-found Plan, got {err:?}");
        assert!(
            err.to_string().contains("testcat.orders"),
            "the error names the qualified table: {err}"
        );
        assert!(
            !err.to_string().contains(".."),
            "must not leak the double-dot SQL shape: {err}"
        );
        assert!(
            catalog
                .load_calls()
                .contains(&(vec![], "orders".to_string())),
            "the provider saw the empty-namespace probe: {:?}",
            catalog.load_calls()
        );
        // Same for DESCRIBE.
        let err = engine.sql("DESCRIBE orders").await.unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "not-found Plan, got {err:?}");
    }

    #[tokio::test]
    async fn create_table_using_records_comment_and_tblproperties() {
        let engine = Engine::new();
        let batches = engine
            .sql("CREATE TABLE t(a int) USING parquet COMMENT 'hi' TBLPROPERTIES ('k'='v')")
            .await
            .unwrap();
        assert!(batches.is_empty(), "CREATE should yield no batches");
        let meta = engine
            .created_table_meta("t")
            .expect("created_table_meta should find t");
        assert_eq!(meta.format, "parquet");
        assert_eq!(meta.comment, Some("hi".to_string()));
        assert_eq!(meta.properties.get("k").map(String::as_str), Some("v"));
    }

    /// The core `SHOW CREATE TABLE` bug fix: previously any `SHOW CREATE TABLE` fell through to
    /// DataFusion's planner and died on "SHOW CREATE TABLE is not supported unless
    /// information_schema is enabled" (see the plan doc this lands from). It must now round-trip a
    /// `CREATE TABLE … USING parquet … COMMENT … TBLPROPERTIES (…)` table into a single
    /// `createtab_stmt` column reconstructing the DDL, matching
    /// `spark-tests/results/show-create-table.sql.out`'s general shape.
    #[tokio::test]
    async fn show_create_table_reconstructs_ddl() {
        use arrow::array::{Array, StringArray};
        let engine = Engine::new();
        engine
            .sql("CREATE TABLE t(a INT, b STRING) USING parquet COMMENT 'hi' TBLPROPERTIES ('k'='v')")
            .await
            .unwrap();
        let batches = engine.sql("SHOW CREATE TABLE t").await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "createtab_stmt");
        let ddl = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string();
        assert!(
            ddl.starts_with("CREATE TABLE spark_catalog.default.t ("),
            "ddl was: {ddl}"
        );
        assert!(ddl.contains("a INT"), "ddl was: {ddl}");
        assert!(ddl.contains("b STRING"), "ddl was: {ddl}");
        assert!(ddl.contains("USING parquet"), "ddl was: {ddl}");
        assert!(ddl.contains("COMMENT 'hi'"), "ddl was: {ddl}");
        assert!(ddl.contains("TBLPROPERTIES"), "ddl was: {ddl}");
        assert!(ddl.contains("'k' = 'v'"), "ddl was: {ddl}");
    }

    /// Regression test: `created_tables` must be keyed the same way `created_table_meta` looks it
    /// up (bare, unquoted table name) — a backtick-quoted `CREATE TABLE` name used to be stored
    /// under its raw source span (`` `t2` ``), so a following `SHOW CREATE TABLE`/`SHOW
    /// TBLPROPERTIES t2` (unquoted lookup) would miss the entry and wrongly report
    /// `TABLE_OR_VIEW_NOT_FOUND` even though the table exists and its COMMENT/TBLPROPERTIES were
    /// captured at CREATE time.
    #[tokio::test]
    async fn show_create_table_finds_backtick_quoted_created_table() {
        use arrow::array::{Array, StringArray};
        let engine = Engine::new();
        engine
            .sql("CREATE TABLE `t2`(a INT) USING parquet COMMENT 'hey'")
            .await
            .unwrap();
        let batches = engine.sql("SHOW CREATE TABLE t2").await.unwrap();
        let ddl = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string();
        assert!(ddl.contains("COMMENT 'hey'"), "ddl was: {ddl}");

        let props = engine.sql("SHOW TBLPROPERTIES t2").await.unwrap();
        assert_eq!(props[0].num_rows(), 0, "no TBLPROPERTIES were set");
    }

    /// `SHOW CREATE TABLE` on an unknown table returns a clean `TABLE_OR_VIEW_NOT_FOUND`-style
    /// plan error rather than falling through to DataFusion's broken `information_schema` error.
    #[tokio::test]
    async fn show_create_table_unknown_table_errors_cleanly() {
        let engine = Engine::new();
        let err = engine.sql("SHOW CREATE TABLE nope").await.unwrap_err();
        assert!(
            matches!(err, Error::Plan(_)),
            "expected a Plan error, got {err:?}"
        );
        assert!(
            format!("{err}").contains("TABLE_OR_VIEW_NOT_FOUND"),
            "unexpected error: {err}"
        );
    }

    /// `SHOW TBLPROPERTIES` answers from the locally captured `TBLPROPERTIES (…)` for a
    /// `CREATE TABLE … USING` table, both for the bare (list-all) and single-key forms.
    #[tokio::test]
    async fn show_tblproperties_lists_and_looks_up_local_table() {
        use arrow::array::{Array, StringArray};
        let engine = Engine::new();
        engine
            .sql("CREATE TABLE t(a INT) USING parquet TBLPROPERTIES ('k'='v', 'k2'='v2')")
            .await
            .unwrap();

        let all = engine.sql("SHOW TBLPROPERTIES t").await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].schema().field(0).name(), "key");
        assert_eq!(all[0].schema().field(1).name(), "value");
        let keys = all[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = all[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let got: std::collections::HashMap<String, String> = (0..keys.len())
            .map(|i| (keys.value(i).to_string(), values.value(i).to_string()))
            .collect();
        assert_eq!(got.get("k").map(String::as_str), Some("v"));
        assert_eq!(got.get("k2").map(String::as_str), Some("v2"));

        let one = engine.sql("SHOW TBLPROPERTIES t('k')").await.unwrap();
        assert_eq!(one[0].num_rows(), 1);
        let key = one[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        let value = one[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(key, "k");
        assert_eq!(value, "v");

        // A missing key doesn't error — Spark reports it as the property's own "value".
        let missing = engine.sql("SHOW TBLPROPERTIES t('nope')").await.unwrap();
        let missing_value = missing[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert!(
            missing_value.contains("does not have property"),
            "unexpected value: {missing_value}"
        );
    }

    /// Two corpus-caught bugs in the `TBLPROPERTIES(…)` path, exercised together since they're on
    /// the same statement: (1) `spark-tests/inputs/show-tblproperties.sql` declares
    /// `TBLPROPERTIES('p1'='v1', password = 'password')` with `password` as a *bare* (unquoted)
    /// key — `parse_properties` used to require every key to be a quoted string literal and
    /// silently stopped parsing at the first bare key, dropping it and everything after it. (2)
    /// Spark redacts any property whose key matches `password`/`secret` (case-insensitively) to
    /// `*********(redacted)` in both `SHOW TBLPROPERTIES` and `SHOW CREATE TABLE`'s
    /// `TBLPROPERTIES (...)` clause, so a credential never round-trips back out in plaintext.
    #[tokio::test]
    async fn show_tblproperties_parses_bare_keys_and_redacts_secrets() {
        use arrow::array::{Array, StringArray};
        let engine = Engine::new();
        engine
            .sql("CREATE TABLE t(a INT) USING parquet TBLPROPERTIES ('p1'='v1', password = 'password', secretKey = 'shh')")
            .await
            .unwrap();

        let all = engine.sql("SHOW TBLPROPERTIES t").await.unwrap();
        let keys = all[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = all[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let got: std::collections::HashMap<String, String> = (0..keys.len())
            .map(|i| (keys.value(i).to_string(), values.value(i).to_string()))
            .collect();
        // The bare key was parsed at all (not dropped) …
        assert_eq!(got.get("p1").map(String::as_str), Some("v1"));
        assert!(got.contains_key("password"), "got {got:?}");
        assert!(got.contains_key("secretKey"), "got {got:?}");
        // … and its value is redacted, not the literal secret.
        assert_eq!(
            got.get("password").map(String::as_str),
            Some("*********(redacted)")
        );
        assert_eq!(
            got.get("secretKey").map(String::as_str),
            Some("*********(redacted)")
        );

        let ddl = engine.sql("SHOW CREATE TABLE t").await.unwrap();
        let ddl_str = ddl[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert!(ddl_str.contains("'p1' = 'v1'"), "ddl was: {ddl_str}");
        assert!(
            ddl_str.contains("'password' = '*********(redacted)'"),
            "ddl was: {ddl_str}"
        );
        assert!(
            !ddl_str.contains("'password' = 'password'"),
            "ddl leaked the secret: {ddl_str}"
        );
    }

    #[tokio::test]
    async fn show_catalogs_includes_spark_catalog() {
        use arrow::array::{Array, StringArray};
        let engine = Engine::new();
        let batches = engine.sql("SHOW CATALOGS").await.unwrap();
        assert_eq!(batches[0].schema().field(0).name(), "catalog");
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let got: Vec<&str> = (0..names.len()).map(|i| names.value(i)).collect();
        assert!(got.contains(&"spark_catalog"), "got {got:?}");
    }

    #[tokio::test]
    async fn show_tables_bare_and_like_filter_local_tables() {
        use arrow::array::{Array, StringArray};
        let engine = Engine::new();
        engine
            .sql("CREATE TABLE show_t1(a INT) USING parquet")
            .await
            .unwrap();
        engine
            .sql("CREATE TABLE show_t2(a INT) USING parquet")
            .await
            .unwrap();
        engine
            .sql("CREATE TABLE other(a INT) USING parquet")
            .await
            .unwrap();

        let all = engine.sql("SHOW TABLES").await.unwrap();
        let names = all[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let got: Vec<&str> = (0..names.len()).map(|i| names.value(i)).collect();
        assert!(got.contains(&"show_t1"), "got {got:?}");
        assert!(got.contains(&"other"), "got {got:?}");

        let filtered = engine.sql("SHOW TABLES LIKE 'show_t%'").await.unwrap();
        let names = filtered[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let got: Vec<&str> = (0..names.len()).map(|i| names.value(i)).collect();
        assert!(got.contains(&"show_t1") && got.contains(&"show_t2"));
        assert!(!got.contains(&"other"), "got {got:?}");
    }

    #[tokio::test]
    async fn show_columns_lists_schema_field_names() {
        use arrow::array::{Array, StringArray};
        let engine = Engine::new();
        engine
            .sql("CREATE TABLE cols_t(a INT, b STRING) USING parquet")
            .await
            .unwrap();
        let batches = engine.sql("SHOW COLUMNS IN cols_t").await.unwrap();
        assert_eq!(batches[0].schema().field(0).name(), "col_name");
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let got: Vec<&str> = (0..names.len()).map(|i| names.value(i)).collect();
        assert_eq!(got, vec!["a", "b"]);
    }

    #[tokio::test]
    async fn show_functions_includes_builtin_and_udf() {
        use arrow::array::{Array, StringArray};
        let engine = Engine::new();
        let batches = engine.sql("SHOW FUNCTIONS").await.unwrap();
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let got: Vec<&str> = (0..names.len()).map(|i| names.value(i)).collect();
        assert!(
            got.contains(&"upper") || got.contains(&"abs"),
            "got {got:?}"
        );
    }

    /// Plain `DESCRIBE TABLE` returns Spark's `col_name`/`data_type`/`comment` shape, previously
    /// unreachable (fell through to DataFusion's own `column_name`/`data_type`/`is_nullable`
    /// shape).
    #[tokio::test]
    async fn describe_table_lists_columns_spark_shape() {
        use arrow::array::{Array, StringArray};
        let engine = Engine::new();
        engine
            .sql("CREATE TABLE desc_t(a INT, b STRING) USING parquet")
            .await
            .unwrap();
        for q in ["DESCRIBE desc_t", "DESC TABLE desc_t", "DESC desc_t"] {
            let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].schema().field(0).name(), "col_name");
            assert_eq!(batches[0].schema().field(1).name(), "data_type");
            assert_eq!(batches[0].schema().field(2).name(), "comment");
            let names = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let types = batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(names.value(0), "a");
            assert_eq!(types.value(0), "int");
            assert_eq!(names.value(1), "b");
            assert_eq!(types.value(1), "string");
        }
    }

    /// `DESCRIBE TABLE EXTENDED` appends the `# Detailed Table Information` block, populating the
    /// fields oxidant can answer (Catalog/Database/Table/Type/Provider/Comment/Table Properties) from
    /// the `created_tables` registry — reusing the same metadata `SHOW CREATE TABLE` reads.
    #[tokio::test]
    async fn describe_table_extended_includes_detailed_information() {
        use arrow::array::{Array, StringArray};
        let engine = Engine::new();
        engine
            .sql("CREATE TABLE ext_t(a INT) USING parquet COMMENT 'hi' TBLPROPERTIES ('k'='v')")
            .await
            .unwrap();
        for q in ["DESCRIBE EXTENDED ext_t", "DESC FORMATTED ext_t"] {
            let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
            let names = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let values = batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let rows: Vec<(String, String)> = (0..names.len())
                .map(|i| (names.value(i).to_string(), values.value(i).to_string()))
                .collect();
            assert!(
                rows.iter()
                    .any(|(k, _)| k == "# Detailed Table Information"),
                "{q}: rows were {rows:?}"
            );
            assert!(
                rows.contains(&("Catalog".to_string(), "spark_catalog".to_string())),
                "{q}: rows were {rows:?}"
            );
            assert!(
                rows.contains(&("Database".to_string(), "default".to_string())),
                "{q}: rows were {rows:?}"
            );
            assert!(
                rows.contains(&("Table".to_string(), "ext_t".to_string())),
                "{q}: rows were {rows:?}"
            );
            assert!(
                rows.contains(&("Type".to_string(), "MANAGED".to_string())),
                "{q}: rows were {rows:?}"
            );
            assert!(
                rows.contains(&("Provider".to_string(), "parquet".to_string())),
                "{q}: rows were {rows:?}"
            );
            assert!(
                rows.contains(&("Comment".to_string(), "hi".to_string())),
                "{q}: rows were {rows:?}"
            );
            assert!(
                rows.contains(&("Table Properties".to_string(), "[k=v]".to_string())),
                "{q}: rows were {rows:?}"
            );
        }
    }

    /// `DESC TABLE ... AS JSON` without `EXTENDED` is Spark's `DESCRIBE_JSON_NOT_EXTENDED` error;
    /// with `EXTENDED` it returns a single `json_metadata` column.
    #[tokio::test]
    async fn describe_table_as_json_requires_extended() {
        let engine = Engine::new();
        engine
            .sql("CREATE TABLE json_t(a INT) USING parquet")
            .await
            .unwrap();
        let err = engine.sql("DESC json_t AS JSON").await.unwrap_err();
        assert!(
            format!("{err}").contains("DESCRIBE_JSON_NOT_EXTENDED"),
            "unexpected error: {err}"
        );
        let batches = engine.sql("DESC EXTENDED json_t AS JSON").await.unwrap();
        assert_eq!(batches[0].schema().field(0).name(), "json_metadata");
        assert_eq!(batches[0].num_rows(), 1);
    }

    /// `DESCRIBE QUERY <select>` / bare `DESC <select>` reuse `Engine::schema()` and report the
    /// same Spark `col_name`/`data_type`/`comment` shape as `DESCRIBE TABLE`.
    #[tokio::test]
    async fn describe_query_reports_select_schema() {
        use arrow::array::{Array, StringArray};
        let engine = Engine::new();
        for q in [
            "DESCRIBE QUERY SELECT 1 AS x, 'a' AS y",
            "DESC SELECT 1 AS x, 'a' AS y",
        ] {
            let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
            assert_eq!(batches[0].schema().field(0).name(), "col_name");
            let names = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let types = batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(names.value(0), "x");
            assert_eq!(types.value(0), "int");
            assert_eq!(names.value(1), "y");
            assert_eq!(types.value(1), "string");
        }
    }

    /// `DESCRIBE DATABASE`/`DESCRIBE CATALOG` — minimal `info_name`/`info_value` shape, using only
    /// the fields oxidant actually knows.
    #[tokio::test]
    async fn describe_database_and_catalog_minimal_fields() {
        use arrow::array::{Array, StringArray};
        let engine = Engine::new();
        let batches = engine.sql("DESCRIBE DATABASE default").await.unwrap();
        assert_eq!(batches[0].schema().field(0).name(), "info_name");
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Namespace Name");

        let err = engine
            .sql("DESCRIBE DATABASE nonexistent_db_xyz")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Plan(_)));

        let batches = engine.sql("DESCRIBE CATALOG spark_catalog").await.unwrap();
        let values = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "spark_catalog");
    }

    /// `DESCRIBE FUNCTION` reports session UDFs (with their SQL body) and built-ins (name + "N/A"
    /// rather than a fabricated description); an unknown function errors.
    #[tokio::test]
    async fn describe_function_session_and_builtin() {
        use arrow::array::{Array, StringArray};
        let engine = Engine::new();
        engine
            .sql("CREATE FUNCTION my_add(x INT, y INT) RETURNS INT RETURN x + y")
            .await
            .unwrap();
        let batches = engine.sql("DESCRIBE FUNCTION my_add").await.unwrap();
        let rows: Vec<String> = (0..batches[0].num_rows())
            .map(|i| {
                batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(i)
                    .to_string()
            })
            .collect();
        assert!(rows.iter().any(|r| r.contains("my_add")), "{rows:?}");

        let batches = engine.sql("DESCRIBE FUNCTION upper").await.unwrap();
        let rows: Vec<String> = (0..batches[0].num_rows())
            .map(|i| {
                batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(i)
                    .to_string()
            })
            .collect();
        assert!(rows.iter().any(|r| r.contains("N/A")), "{rows:?}");

        let err = engine
            .sql("DESCRIBE FUNCTION nonexistent_fn_xyz")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Plan(_)));
    }

    #[test]
    fn sql_like_match_percent_and_underscore() {
        assert!(sql_like_match("show_t%", "show_t1"));
        assert!(sql_like_match("show_t%", "show_t2"));
        assert!(!sql_like_match("show_t%", "other"));
        assert!(sql_like_match("a_c", "abc"));
        assert!(!sql_like_match("a_c", "abbc"));
        assert!(sql_like_match("%", "anything"));
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

    /// A registered catalog whose `create_table` is unimplemented (inherits the trait default) —
    /// enough to prove `CREATE TABLE <cat>.ns.t USING <fmt> AS SELECT ...` routes to the EXTERNAL
    /// catalog's `create_table` (and fails there, since this stub doesn't implement it) instead of
    /// silently lowering to a local-warehouse `CREATE EXTERNAL TABLE` write, which is what used to
    /// happen for this exact spelling before `name_targets_external_catalog` was wired in.
    struct StubExternalCatalog;

    #[async_trait::async_trait]
    impl oxidant_catalog::CatalogProvider for StubExternalCatalog {
        fn name(&self) -> &str {
            "extcat"
        }
        async fn list_namespaces(
            &self,
            _parent: &[String],
        ) -> oxidant_catalog::Result<Vec<Vec<String>>> {
            Ok(vec![])
        }
        async fn list_tables(&self, _ns: &[String]) -> oxidant_catalog::Result<Vec<String>> {
            Ok(vec![])
        }
        async fn load_table(
            &self,
            ns: &[String],
            table: &str,
        ) -> oxidant_catalog::Result<oxidant_catalog::TableMetadata> {
            Err(Error::Plan(format!(
                "no such table: {}.{table}",
                ns.join(".")
            )))
        }
    }

    #[tokio::test]
    async fn qualified_external_catalog_ctas_skips_local_warehouse_lowering() {
        let engine = Engine::new();
        engine.register_catalog("extcat", Arc::new(StubExternalCatalog));

        // Before this fix, this exact spelling (qualified name + `USING <fmt>` + `AS SELECT`)
        // would silently lower to a local-warehouse `CREATE EXTERNAL TABLE`, writing under
        // `warehouse/extcat_ns_t/` instead of routing to `extcat`'s catalog at all.
        let _ = engine
            .sql("CREATE TABLE extcat.ns.t USING parquet AS SELECT 1 AS x")
            .await;
        assert!(
            !engine.dirs.warehouse.join("extcat_ns_t").exists(),
            "must not fall back to writing the local warehouse for an external-catalog-qualified name"
        );
    }

    #[tokio::test]
    async fn qualified_external_catalog_ctas_skips_local_warehouse_lowering_case_insensitively() {
        // Catalogs are registered verbatim ("extcat"), but SQL identifiers are conventionally
        // case-insensitive — a differently-cased reference must still be recognized as external,
        // not silently misrouted to the local warehouse.
        let engine = Engine::new();
        engine.register_catalog("extcat", Arc::new(StubExternalCatalog));
        let _ = engine
            .sql("CREATE TABLE ExtCat.ns.t USING parquet AS SELECT 1 AS x")
            .await;
        assert!(
            !engine.dirs.warehouse.join("ExtCat_ns_t").exists(),
            "a differently-cased catalog reference must still route away from the local warehouse"
        );
    }

    #[tokio::test]
    async fn unqualified_name_matching_a_catalog_name_still_uses_local_warehouse() {
        // A 1-part name is never catalog-qualified, even when it happens to spell a registered
        // catalog's own name (e.g. a local table coincidentally named "extcat"). This must NOT be
        // misclassified as external (the arity check) — a local `CREATE TABLE ... USING <fmt>`
        // must still lower to the local warehouse and round-trip data normally.
        let engine = Engine::new();
        engine.register_catalog("extcat", Arc::new(StubExternalCatalog));
        engine
            .sql("create table extcat(a int) using parquet")
            .await
            .expect("1-part name colliding with a catalog name must still use the local warehouse");
        engine
            .sql("insert into extcat values (1), (2)")
            .await
            .expect("insert into the local table must succeed");
        let out = engine.sql("select a from extcat order by a").await.unwrap();
        let rows: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            rows, 2,
            "a 1-part name must use the local warehouse, not be misrouted as catalog-qualified"
        );
    }

    #[tokio::test]
    async fn local_ctas_streams_select_to_readable_table() {
        // A local-warehouse `CREATE TABLE ... USING <fmt> AS SELECT ...` runs through the streaming
        // write path (`run_create_table_ctas`): the SELECT is drained batch-by-batch straight to the
        // output file, never fully collected into driver memory (so a large source can't OOM the
        // driver). Prove the table is created and reads back every row of the SELECT.
        let engine = Engine::new();
        engine
            .sql("CREATE TABLE ctas_t USING parquet AS SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3")
            .await
            .expect("streamed CTAS should succeed");
        let out = engine
            .sql("SELECT id FROM ctas_t ORDER BY id")
            .await
            .expect("select from the CTAS table");
        let rows: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            rows, 3,
            "the streamed CTAS must persist all rows of the SELECT"
        );
    }

    #[tokio::test]
    async fn sql_with_stats_reports_rows_and_bytes_scanned() {
        // Persist a parquet table (via the streaming CTAS path), then scan it through
        // `sql_with_stats`: the retained physical plan's scan node must report the rows returned
        // and a non-zero `bytes_scanned` read from storage — the metrics `df.collect()` drops.
        let engine = Engine::new();
        engine
            .sql("CREATE TABLE stats_t USING parquet AS SELECT 1 AS a UNION ALL SELECT 2 UNION ALL SELECT 3")
            .await
            .expect("ctas should succeed");
        let (batches, stats) = engine
            .sql_with_stats("SELECT a FROM stats_t")
            .await
            .expect("sql_with_stats should succeed");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 3);
        assert_eq!(stats.output_rows, 3, "output_rows must match the result");
        assert!(
            stats.bytes_scanned > 0,
            "a parquet scan should report bytes_scanned, got {}",
            stats.bytes_scanned
        );
    }

    #[tokio::test]
    async fn sql_with_stats_rejects_multi_arg_count_distinct() {
        // `sql_with_stats` is reached for scan queries via the Spark Connect metrics route; it must
        // apply the same guard as `Engine::sql` so `COUNT(DISTINCT a, b)` returns a clean error
        // instead of panicking DataFusion's planner (which would kill the driver task).
        let engine = Engine::new();
        let result = engine
            .sql_with_stats("SELECT COUNT(DISTINCT a, b) FROM t")
            .await;
        assert!(
            matches!(result, Err(Error::Plan(_))),
            "multi-arg COUNT(DISTINCT) must be a clean Plan error, got {result:?}"
        );
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
            // Bare INTERVAL without leading precision is already DataFusion-legal.
            "SELECT date '1998-12-01' - interval '90' day AS d",
            // day(col) must not be confused with INTERVAL day (N).
            "SELECT day(ts) FROM t",
        ] {
            assert_eq!(normalize_spark_sql(q), q, "should not rewrite: {q}");
        }
    }

    #[test]
    fn normalize_strips_interval_leading_precision() {
        // TPC-H Q1 canonical form — ANSI day (3) leading precision.
        assert_eq!(
            normalize_spark_sql("SELECT date '1998-12-01' - interval '90' day (3) AS d"),
            "SELECT date '1998-12-01' - interval '90' day AS d"
        );
        assert_eq!(
            normalize_spark_sql("SELECT DATE '1998-12-01' - INTERVAL '63' DAY(3) AS d"),
            "SELECT DATE '1998-12-01' - INTERVAL '63' DAY AS d"
        );
        // Precision must not be stripped from string content that merely looks similar.
        let inside = "SELECT 'interval ''90'' day (3)' AS s";
        assert_eq!(normalize_spark_sql(inside), inside);
    }

    // Shuffle-input data lives in a MemTable for the whole stage and cannot be spilled. Before
    // `reserve_external_bytes` the pool never saw it, so it reported headroom that did not
    // exist, downstream operators declined to spill, and the worker was kernel-killed with no
    // `ResourcesExhausted` to act on — measured at 6x the configured pool in ~5 s.
    #[test]
    fn external_reservation_is_visible_to_the_pool_and_released_on_drop() {
        let engine = Engine::new_with_memory_limit(8 * 1024 * 1024);
        let pool = engine.ctx().task_ctx().runtime_env().memory_pool.clone();
        assert_eq!(pool.reserved(), 0);

        let reservation = engine
            .reserve_external_bytes("ShuffleInput[t]", 4 * 1024 * 1024)
            .expect("fits in an 8 MiB pool");
        assert_eq!(
            pool.reserved(),
            4 * 1024 * 1024,
            "the pool must SEE shuffle-input bytes, not just tolerate them"
        );

        drop(reservation);
        assert_eq!(pool.reserved(), 0, "dropping the guard releases the bytes");
    }

    #[test]
    fn external_reservation_past_the_pool_fails_loudly_instead_of_growing_rss() {
        let engine = Engine::new_with_memory_limit(4 * 1024 * 1024);
        let err = engine
            .reserve_external_bytes("ShuffleInput[t]", 64 * 1024 * 1024)
            .expect_err("64 MiB must not fit a 4 MiB pool");
        let msg = err.to_string();
        // The message has to tell an operator what to actually do about it.
        assert!(msg.contains("ShuffleInput[t]"), "names the input: {msg}");
        assert!(
            msg.contains("OXIDANT_WORKER_TASK_SLOTS") && msg.contains("replicate"),
            "offers the remedies: {msg}"
        );
    }

    #[test]
    fn external_reservation_is_a_noop_without_a_bounded_pool() {
        // `OXIDANT_MEMORY_LIMIT_BYTES=0` opts out of bounding; reserving must not start
        // failing queries that previously ran.
        let engine = Engine::new_inner(None);
        engine
            .reserve_external_bytes("ShuffleInput[t]", usize::MAX / 2)
            .expect("unbounded pool accepts any reservation");
    }

    #[test]
    fn resolve_memory_pool_bytes_honours_env_and_autosizes() {
        // Process-global env — keep this test short and restore afterwards.
        let prev = std::env::var("OXIDANT_MEMORY_LIMIT_BYTES").ok();
        let prev_frac = std::env::var("OXIDANT_MEMORY_POOL_FRACTION").ok();

        std::env::set_var("OXIDANT_MEMORY_LIMIT_BYTES", "123456789");
        assert_eq!(resolve_memory_pool_bytes(), Some(123456789));
        // Explicit pool size still seeds shuffle 1:1 (legacy).
        assert_eq!(resolve_shuffle_spill_bytes(), Some(123456789));

        std::env::set_var("OXIDANT_MEMORY_LIMIT_BYTES", "0");
        assert_eq!(resolve_memory_pool_bytes(), None);
        assert_eq!(resolve_shuffle_spill_bytes(), None);

        std::env::remove_var("OXIDANT_MEMORY_LIMIT_BYTES");
        std::env::set_var("OXIDANT_MEMORY_POOL_FRACTION", "0.5");
        let auto = resolve_memory_pool_bytes();
        assert!(
            auto.is_some_and(|n| n >= 64 * 1024 * 1024),
            "unset MEMORY_LIMIT must auto-size from host/cgroup RAM: {auto:?}"
        );
        let shuffle = resolve_shuffle_spill_bytes();
        assert_eq!(
            shuffle,
            auto.map(|n| (n / 4).max(64 * 1024 * 1024)),
            "auto-sized shuffle must be ¼ of the pool, not a second 70% claim"
        );

        match prev {
            Some(v) => std::env::set_var("OXIDANT_MEMORY_LIMIT_BYTES", v),
            None => std::env::remove_var("OXIDANT_MEMORY_LIMIT_BYTES"),
        }
        match prev_frac {
            Some(v) => std::env::set_var("OXIDANT_MEMORY_POOL_FRACTION", v),
            None => std::env::remove_var("OXIDANT_MEMORY_POOL_FRACTION"),
        }
    }

    #[test]
    fn normalize_qualifies_interval_units_inside_the_literal() {
        // TPC-DS Q12/Q20/Q98 spelling — the SF100 parse failure.
        assert_eq!(
            normalize_spark_sql("SELECT (cast('2001-01-12' as date) + interval '30 days') AS d"),
            "SELECT (cast('2001-01-12' as date) + INTERVAL '30' DAY) AS d"
        );
        // Case and plural spellings, and a negative amount.
        assert_eq!(
            normalize_spark_sql("SELECT INTERVAL '1 YEAR', interval '-2 month'"),
            "SELECT INTERVAL '1' YEAR, INTERVAL '-2' MONTH"
        );
        // Multi-unit content becomes a parenthesized sum, safe in any expression position.
        assert_eq!(
            normalize_spark_sql("SELECT ts + interval '1 day 2 hours'"),
            "SELECT ts + (INTERVAL '1' DAY + INTERVAL '2' HOUR)"
        );
        // DataFusion's Postgres-verbose unparser output — what workers re-parse from stage SQL.
        assert_eq!(
            normalize_spark_sql(
                "SELECT INTERVAL '0 YEARS 0 MONS 30 DAYS 0 HOURS 0 MINS 0.00 SECS'"
            ),
            "SELECT INTERVAL '30' DAY"
        );
        // Mixed month + day-time verbose output keeps both surviving terms.
        assert_eq!(
            normalize_spark_sql("SELECT INTERVAL '0 YEARS 3 MONS 0 DAYS 0 HOURS 0 MINS 1.50 SECS'"),
            "SELECT (INTERVAL '3' MONTH + INTERVAL '1.50' SECOND)"
        );
        // An all-zero interval still has to be a legal literal.
        assert_eq!(
            normalize_spark_sql("SELECT INTERVAL '0 YEARS 0 MONS 0 DAYS'"),
            "SELECT INTERVAL '0' SECOND"
        );
    }

    #[test]
    fn normalize_leaves_qualified_and_unparseable_intervals_alone() {
        for q in [
            // Already qualified — every spelling sqlparser accepts as a unit token.
            "SELECT date '1998-12-01' - interval '90' day AS d",
            "SELECT interval '90' days AS d",
            "SELECT interval '1' week AS d",
            "SELECT interval '5' milliseconds AS d",
            "SELECT interval '1-2' year to month AS d",
            // Unit missing entirely, or not a unit we recognize: the original parse error is
            // more useful than a guess.
            "SELECT interval '30'",
            "SELECT interval '30 fortnights'",
            "SELECT interval '1 day 2'",
            "SELECT interval 'abc days'",
            // String content that merely looks like an interval.
            "SELECT 'interval ''30 days''' AS s",
            // A trailing alias must not be mistaken for a unit qualifier.
            "SELECT interval '30' day d FROM t",
        ] {
            assert_eq!(normalize_spark_sql(q), q, "should not rewrite: {q}");
        }
        // Unquoted Spark spelling already parses (the amount is a number, the unit a token).
        let unquoted = "SELECT ts + interval 30 days";
        assert_eq!(normalize_spark_sql(unquoted), unquoted);
    }

    #[test]
    fn normalize_rewrites_tpcds_bare_pg_interval_days() {
        // Official dsqgen spelling — ParseError "Expected: ), found: days" on Connect without this.
        assert_eq!(
            normalize_spark_sql("SELECT (cast('2001-01-12' as date) + 30 days) AS d"),
            "SELECT (cast('2001-01-12' as date) + INTERVAL '30' DAY) AS d"
        );
        assert_eq!(
            normalize_spark_sql(
                "and d_date between (cast ('1998-04-08' as date) - 30 days) and (cast ('1998-04-08' as date) + 30 days)"
            ),
            "and d_date between (cast ('1998-04-08' as date) - INTERVAL '30' DAY) and (cast ('1998-04-08' as date) + INTERVAL '30' DAY)"
        );
        // Column aliases that contain the word "days" must stay untouched.
        let alias = r#"select 1 as "31-60 days" from t"#;
        assert_eq!(normalize_spark_sql(alias), alias);
    }

    #[tokio::test]
    async fn tpch_interval_date_arithmetic() {
        use arrow::array::{Array, Date32Array};
        let engine = Engine::new();
        // Q1 cutoff: 1998-12-01 − 90 days = 1998-09-02 (with and without ANSI precision).
        for sql in [
            "SELECT date '1998-12-01' - interval '90' day AS d",
            "SELECT date '1998-12-01' - interval '90' day (3) AS d",
        ] {
            let batches = engine.sql(sql).await.unwrap();
            let col = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap();
            // Date32 epoch days for 1998-09-02.
            assert_eq!(col.value(0), 10471, "sql={sql}");
        }
        // Q4 / Q10 style month arithmetic.
        let m = engine
            .sql("SELECT date '1993-07-01' + interval '3' month AS d")
            .await
            .unwrap();
        let col = m[0]
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(col.value(0), 8674); // 1993-10-01
        let y = engine
            .sql("SELECT date '1994-01-01' + interval '1' year AS d")
            .await
            .unwrap();
        let col = y[0]
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(col.value(0), 9131); // 1995-01-01
    }

    #[tokio::test]
    async fn tpcds_spark_interval_spelling_executes() {
        use arrow::array::{Array, Date32Array};
        let engine = Engine::new();
        // TPC-DS Q12's date window: the unit inside the literal must agree with the qualified
        // spelling. 2001-01-12 + 30 days = 2001-02-11 (Date32 epoch day 11364).
        for sql in [
            // Official dsqgen bare form (no INTERVAL keyword).
            "SELECT (cast('2001-01-12' as date) + 30 days) AS d",
            "SELECT (cast('2001-01-12' as date) + interval '30 days') AS d",
            "SELECT (cast('2001-01-12' as date) + interval '30' day) AS d",
            // Postgres-verbose form, as DataFusion's unparser emits it into stage SQL.
            "SELECT (cast('2001-01-12' as date) + interval '0 YEARS 0 MONS 30 DAYS 0 HOURS \
             0 MINS 0.00 SECS') AS d",
        ] {
            let batches = engine.sql(sql).await.unwrap();
            let col = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap();
            assert_eq!(col.value(0), 11364, "sql={sql}");
        }
        // Multi-unit content sums its terms: 2001-01-12 + 1 month 2 days = 2001-02-14.
        let multi = engine
            .sql("SELECT (cast('2001-01-12' as date) + interval '1 month 2 days') AS d")
            .await
            .unwrap();
        let col = multi[0]
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(col.value(0), 11367);
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
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
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
        let dir = std::env::temp_dir().join(format!("oxidant-delta-{}", std::process::id()));
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
        let file_size = std::fs::metadata(dir.join("part-0.parquet")).unwrap().len();
        let schema_string = serde_json::json!({
            "type": "struct",
            "fields": [{
                "name": "x",
                "type": "long",
                "nullable": false,
                "metadata": {}
            }]
        })
        .to_string();
        let commit = [
            serde_json::json!({
                "protocol": {"minReaderVersion": 1, "minWriterVersion": 2}
            })
            .to_string(),
            serde_json::json!({
                "metaData": {
                    "id": "00000000-0000-0000-0000-000000000001",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": schema_string,
                    "partitionColumns": [],
                    "configuration": {}
                }
            })
            .to_string(),
            serde_json::json!({
                "add": {
                    "path": "part-0.parquet",
                    "partitionValues": {},
                    "size": file_size,
                    "modificationTime": 0,
                    "dataChange": true
                }
            })
            .to_string(),
        ]
        .join("\n");
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

    /// Write a Delta table holding one data file and then retire it, leaving a table that has a
    /// committed log and a declared schema but **no live files**. Returns its directory.
    ///
    /// `partition` names the partition column when the table is partitioned; the data file then
    /// holds only `x`, exactly as a partitioned writer writes it.
    async fn delta_table_emptied_by_a_remove(partition: Option<(&str, &str)>) -> tempfile::TempDir {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        std::fs::create_dir_all(dir.join("_delta_log")).unwrap();

        let file_path = match partition {
            Some((col, value)) => {
                std::fs::create_dir_all(dir.join(format!("{col}={value}"))).unwrap();
                format!("{col}={value}/part-0.parquet")
            }
            None => "part-0.parquet".to_string(),
        };
        let file_schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            file_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        {
            let f = std::fs::File::create(dir.join(&file_path)).unwrap();
            let mut w = ArrowWriter::try_new(f, file_schema, None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
        let file_size = std::fs::metadata(dir.join(&file_path)).unwrap().len();

        let mut fields = vec![serde_json::json!({
            "name": "x", "type": "long", "nullable": false, "metadata": {}
        })];
        let mut partition_columns = Vec::new();
        let mut partition_values = serde_json::Map::new();
        if let Some((col, value)) = partition {
            fields.push(serde_json::json!({
                "name": col, "type": "string", "nullable": true, "metadata": {}
            }));
            partition_columns.push(col.to_string());
            partition_values.insert(col.to_string(), serde_json::Value::String(value.into()));
        }
        let schema_string = serde_json::json!({"type": "struct", "fields": fields}).to_string();
        let commit0 = [
            serde_json::json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}})
                .to_string(),
            serde_json::json!({
                "metaData": {
                    "id": "00000000-0000-0000-0000-000000000002",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": schema_string,
                    "partitionColumns": partition_columns,
                    "configuration": {}
                }
            })
            .to_string(),
            serde_json::json!({
                "add": {
                    "path": file_path,
                    "partitionValues": partition_values,
                    "size": file_size,
                    "modificationTime": 0,
                    "dataChange": true
                }
            })
            .to_string(),
        ]
        .join("\n");
        std::fs::write(dir.join("_delta_log/00000000000000000000.json"), commit0).unwrap();

        // Everything the table held, retired — what a CDC target drained by deletes, a truncate,
        // or an `INSERT OVERWRITE ... WHERE false` leaves behind.
        let commit1 = serde_json::json!({
            "remove": {
                "path": file_path,
                "deletionTimestamp": 1,
                "dataChange": true,
                "partitionValues": partition_values,
                "size": file_size,
                "extendedFileMetadata": true
            }
        })
        .to_string();
        std::fs::write(dir.join("_delta_log/00000000000000000001.json"), commit1).unwrap();
        tmp
    }

    /// A Delta table whose files have all been removed is **empty**, not unreadable.
    ///
    /// There is no data file left to infer a schema from, so the only thing that can name the
    /// columns is the log's `metaData` — and if the bare-path form does not supply it, the table
    /// cannot be selected from at all. For a read-modify-write sink (AUTO CDC) that is worse than
    /// a failed query: the sink re-reads its target at the start of every run, so a target that
    /// legitimately went empty could never be written to again.
    #[tokio::test]
    async fn reads_an_unpartitioned_delta_table_with_no_live_files() {
        let tmp = delta_table_emptied_by_a_remove(None).await;
        let path = tmp.path().to_str().unwrap();
        let engine = Engine::new();

        // By location, the way `read_delta` (and so the AUTO CDC sink) reads its target.
        let rows = engine.read_delta("emptied", path).await.unwrap();
        assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 0);

        // And by name: the columns still resolve, so a query naming them plans and returns
        // nothing rather than failing on an unknown column.
        engine.register_delta("emptied", path).await.unwrap();
        let out = engine.sql("SELECT x FROM emptied").await.unwrap();
        assert_eq!(out.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
        let counted = engine
            .sql("SELECT COUNT(*) AS c FROM emptied WHERE x > 0")
            .await
            .unwrap();
        assert_eq!(
            counted[0]
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .unwrap()
                .value(0),
            0
        );
    }

    /// The partitioned case has always worked — the declared schema was already supplied for it.
    /// Pinned so the unpartitioned fix cannot be undone by narrowing this back.
    #[tokio::test]
    async fn reads_a_partitioned_delta_table_with_no_live_files() {
        let tmp = delta_table_emptied_by_a_remove(Some(("day", "2024-01-01"))).await;
        let path = tmp.path().to_str().unwrap();
        let engine = Engine::new();

        let rows = engine.read_delta("emptied_parts", path).await.unwrap();
        assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 0);

        engine.register_delta("emptied_parts", path).await.unwrap();
        // The partition column is in the path, never in a data file, so an emptied partitioned
        // table can only know about `day` from the log.
        let out = engine
            .sql("SELECT x, day FROM emptied_parts WHERE day = '2024-01-01'")
            .await
            .unwrap();
        assert_eq!(out.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
    }

    // ---- KAN-25: hash-join build-side memory guard -----------------------------

    /// Serializes the join-guard tests that mutate `OXIDANT_SORT_MERGE_FALLBACK` /
    /// `OXIDANT_TARGET_PARTITIONS` / `OXIDANT_BATCH_SIZE` (process-global env). `pub(crate)` so
    /// `catalog_bridge`'s footer-cache tests serialize their `OXIDANT_PARQUET_SCAN_STATS`
    /// pinning against the same flips.
    pub(crate) static JOIN_GUARD_ENV_LOCK: tokio::sync::Mutex<()> =
        tokio::sync::Mutex::const_new(());

    /// `(k, v)` Int64 batches, `rows` total split across 4 partitions; keys are `i % key_mod`
    /// so the big table joins the small one exactly once per row.
    fn join_guard_kv_batches(rows: i64, key_mod: i64) -> Vec<RecordBatch> {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let per = rows / 4;
        (0..4)
            .map(|p| {
                let start = p * per;
                let ks: Vec<i64> = (start..start + per).map(|i| i % key_mod).collect();
                let vs: Vec<i64> = (start..start + per).collect();
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(ks)),
                        Arc::new(Int64Array::from(vs)),
                    ],
                )
                .unwrap()
            })
            .collect()
    }

    /// `(k, s)` batches with a `width`-char string column, so the in-memory build side is far
    /// larger than the row-count × flat-width estimate the plan-time guard computes. Emits
    /// 1024-row batches (one MemTable partition): external sorters reserve memory per
    /// incoming batch, and a single giant batch would make one `try_grow` exceed the whole
    /// pool — an artifact of the test data layout, not of the guard.
    fn join_guard_wide_batches(rows: i64, key_mod: i64, width: usize) -> Vec<RecordBatch> {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("s", DataType::Utf8, false),
        ]));
        let filler = "x".repeat(width);
        let per = 1024;
        (0..rows)
            .step_by(per as usize)
            .map(|start| {
                let end = (start + per).min(rows);
                let ks: Vec<i64> = (start..end).map(|i| i % key_mod).collect();
                let ss: Vec<&str> = (start..end).map(|_| filler.as_str()).collect();
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(ks)),
                        Arc::new(StringArray::from(ss)),
                    ],
                )
                .unwrap()
            })
            .collect()
    }

    #[test]
    fn join_guard_row_width_counts_fixed_and_variable() {
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Int32, false),
        ]);
        assert_eq!(estimated_row_width(&schema), 8 + 48 + 4);
    }

    /// F4: with a bounded pool the KAN-25 guard plans every `sql_stream` query physically;
    /// the no-reroute path must then execute THAT plan (previously `execute_stream` planned
    /// a second time — a full duplicate physical-plan pass on every worker stage task).
    /// The reused plan is executed through the same merged single-stream contract as the
    /// reroute branches, so rows must match the collect path exactly.
    #[tokio::test]
    async fn sql_stream_under_budget_guard_matches_collect() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        let engine = Engine::new_with_memory_limit(256 * 1024 * 1024);
        engine
            .register_batches("left_t", join_guard_kv_batches(4096, 64))
            .unwrap();
        engine
            .register_batches("right_t", join_guard_kv_batches(4096, 64))
            .unwrap();
        // Tiny keys-only build side (≪ 64 MiB budget) → no reroute: exercises the
        // plan-reuse path on a 4-partition join (merged into one stream).
        let query = "SELECT COUNT(*) AS c, SUM(l.v) AS s \
                     FROM left_t l JOIN right_t r ON l.k = r.k";
        use futures::StreamExt;
        let mut stream = engine.sql_stream(query).await.unwrap();
        let mut streamed = Vec::new();
        while let Some(batch) = stream.next().await {
            streamed.push(batch.unwrap());
        }
        let collected = engine.sql(query).await.unwrap();
        let sum = |batches: &[RecordBatch], col: usize| -> i64 {
            use arrow::array::Int64Array;
            batches
                .iter()
                .map(|b| {
                    b.column(col)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0)
                })
                .sum()
        };
        // Each of the 4096 left rows matches the 64 right rows sharing its key.
        assert_eq!(sum(&streamed, 0), 4096 * 64);
        assert_eq!(sum(&collected, 0), 4096 * 64);
        let expected_s = 64 * (4095 * 4096 / 2);
        assert_eq!(sum(&streamed, 1), expected_s);
        assert_eq!(sum(&collected, 1), expected_s);
    }

    #[test]
    fn join_guard_detects_pool_exhaustion_errors() {
        assert!(is_pool_exhausted(
            &datafusion::error::DataFusionError::ResourcesExhausted(
                "Failed to allocate additional 1024 for HashJoinInput[0]".to_string()
            )
        ));
        assert!(!is_pool_exhausted(
            &datafusion::error::DataFusionError::Execution("boom".to_string())
        ));
    }

    #[tokio::test]
    async fn join_guard_off_without_bounded_pool() {
        // Explicit opt-out: unset now auto-sizes a bounded pool (SF100 default).
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        let prev = std::env::var("OXIDANT_MEMORY_LIMIT_BYTES").ok();
        std::env::set_var("OXIDANT_MEMORY_LIMIT_BYTES", "0");
        let engine = Engine::new();
        assert_eq!(engine.memory_pool_bytes, None);
        assert_eq!(engine.hash_join_build_budget(), None);
        match prev {
            Some(v) => std::env::set_var("OXIDANT_MEMORY_LIMIT_BYTES", v),
            None => std::env::remove_var("OXIDANT_MEMORY_LIMIT_BYTES"),
        }
    }

    #[tokio::test]
    async fn join_guard_estimates_build_side_from_plan_statistics() {
        let engine = Engine::new();
        engine
            .register_batches("left_t", join_guard_kv_batches(100_000, 100_000))
            .unwrap();
        engine
            .register_batches("right_t", join_guard_kv_batches(200_000, 200_000))
            .unwrap();
        // DataFusion's JoinSelection builds the smaller side (by `total_byte_size`), so the
        // build is `left_t`; the scan projects only the join key, i.e. 100k rows × 8 B/row
        // = 800 KB estimated. Big-build plans are the big⋈big shapes (TPC-H Q18/Q21) where
        // even the smaller side blows the budget.
        let plan = engine
            .physical_plan("SELECT COUNT(*) AS c FROM left_t l JOIN right_t r ON l.k = r.k")
            .await
            .unwrap();
        assert!(contains_hash_join(plan.as_ref()));
        assert!(hash_join_build_exceeds(plan.as_ref(), 500_000));
        assert!(!hash_join_build_exceeds(plan.as_ref(), usize::MAX));
    }

    #[tokio::test]
    async fn join_guard_replan_produces_sort_merge_join() {
        let engine = Engine::new();
        engine
            .register_batches("big", join_guard_kv_batches(1_000, 100))
            .unwrap();
        engine
            .register_batches("small", join_guard_kv_batches(100, 100))
            .unwrap();
        let logical = engine
            .logical_plan("SELECT COUNT(*) AS c FROM big b JOIN small s ON b.k = s.k")
            .await
            .unwrap();
        let (_ctx, smj) = engine.sort_merge_physical_plan(logical).await.unwrap();
        let display = datafusion::physical_plan::displayable(smj.as_ref())
            .indent(false)
            .to_string();
        assert!(
            display.contains("SortMergeJoin"),
            "expected a sort-merge join plan, got:\n{display}"
        );
        assert!(!contains_hash_join(smj.as_ref()));
    }

    #[tokio::test]
    async fn join_guard_plan_time_fallback_completes_oversized_build() {
        // 256 MiB pool → 64 MiB build budget (default fraction 0.25). Big⋈big 1:1 join:
        // DataFusion builds the smaller side BY BYTES and prunes it to the join key, so the
        // build is `right_t`'s key column — 9.5M rows × 8 B/row = 76 MB, over budget. The
        // plan-time guard must downgrade to a sort-merge join and the query must complete
        // with correct results. (The pool leaves the `partitions × 2` external sorters
        // honest headroom — the runtime-retry test covers the tighter shape.)
        // KAN-45: the sort-merge fallback is opt-in since DataFusion's sort-merge pipeline
        // can deadlock under a bounded pool — enable it explicitly for this test.
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::set_var("OXIDANT_SORT_MERGE_FALLBACK", "true");
        // Pin the Spark SHJ cap above the pool-fraction budget so this test still exercises
        // the fraction gate (not the EMR-parity per-partition threshold).
        std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "200");
        std::env::set_var(
            "OXIDANT_HASH_JOIN_PER_PARTITION_THRESHOLD_BYTES",
            (64 * 1024 * 1024).to_string(),
        );
        let engine = Engine::new_with_memory_limit(256 * 1024 * 1024);
        const LEFT: i64 = 9_000_000;
        const RIGHT: i64 = 9_500_000;
        engine
            .register_batches("left_t", join_guard_kv_batches(LEFT, LEFT))
            .unwrap();
        engine
            .register_batches("right_t", join_guard_kv_batches(RIGHT, RIGHT))
            .unwrap();
        let query = "SELECT COUNT(*) AS c, SUM(l.v) AS s FROM left_t l JOIN right_t r ON l.k = r.k";
        let plan = engine.physical_plan(query).await.unwrap();
        let budget = engine.hash_join_build_budget().unwrap();
        assert_eq!(budget, 64 * 1024 * 1024);
        assert!(
            hash_join_build_exceeds(plan.as_ref(), budget),
            "the oversized build side must trip the plan-time guard"
        );
        let batches = engine
            .sql(query)
            .await
            .expect("oversized-build join must complete under the bounded pool via sort-merge");
        use arrow::array::Int64Array;
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
        // Unique keys on both sides → 1:1 match for every left row.
        assert_eq!(c, LEFT);
        assert_eq!(s, LEFT * (LEFT - 1) / 2);
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        std::env::remove_var("OXIDANT_SHUFFLE_PARTITIONS");
        std::env::remove_var("OXIDANT_HASH_JOIN_PER_PARTITION_THRESHOLD_BYTES");
    }

    /// Spark EMR parity: the HashJoin build budget is capped by
    /// `threshold × shuffle_partitions / overhead`, not only by pool × 0.25 — so a 40 Gi
    /// worker pool cannot admit a multi-GiB fact build that would still fit 25% of the pool
    /// and then cgroup-OOM on `m8g.4xlarge`.
    #[tokio::test]
    async fn spark_aligned_hash_join_budget_caps_below_pool_fraction() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_HASH_JOIN_MAX_BUILD_FRACTION");
        std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "200");
        std::env::set_var(
            "OXIDANT_HASH_JOIN_PER_PARTITION_THRESHOLD_BYTES",
            (10 * 1024 * 1024).to_string(),
        );
        // 40 Gi pool × 0.25 = 10 Gi, but Spark SHJ cap is 10 MiB × 200 / 2 = 1000 MiB.
        let engine = Engine::new_with_memory_limit(40 * 1024 * 1024 * 1024);
        let budget = engine.hash_join_build_budget().unwrap();
        assert_eq!(budget, 10 * 1024 * 1024 * 200 / 2);
        std::env::remove_var("OXIDANT_SHUFFLE_PARTITIONS");
        std::env::remove_var("OXIDANT_HASH_JOIN_PER_PARTITION_THRESHOLD_BYTES");
    }

    /// SF100-shaped fact⋈fact: ~150M join keys (orders-scale) must plan-time reroute to
    /// SortMergeJoin under the Spark-aligned budget (not stay on non-spillable HashJoin).
    /// Uses measured stats so we do not materialize SF100-scale batches in-process.
    #[tokio::test]
    async fn sf100_scale_build_estimate_reroutes_sort_merge() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "200");
        std::env::set_var(
            "OXIDANT_HASH_JOIN_PER_PARTITION_THRESHOLD_BYTES",
            (10 * 1024 * 1024).to_string(),
        );
        // Modest pool so the test stays light; spark cap (1 Gi) is the binding constraint.
        let engine = Engine::new_with_memory_limit(8 * 1024 * 1024 * 1024);
        // 200M keys × 8 B ≈ 1.6 GiB estimated build > 1 GiB spark cap.
        const N: u64 = 200_000_000;
        engine
            .register_batches_with_stats("orders", join_guard_kv_batches(4_000, 1_000), N)
            .unwrap();
        engine
            .register_batches_with_stats("lineitem", join_guard_kv_batches(4_000, 1_000), N)
            .unwrap();
        let query = "SELECT COUNT(*) AS c FROM orders o JOIN lineitem l ON o.k = l.k";
        let plan = engine.physical_plan(query).await.unwrap();
        assert!(contains_hash_join(plan.as_ref()));
        let budget = engine.hash_join_build_budget().unwrap();
        assert!(
            hash_join_build_exceeds(plan.as_ref(), budget),
            "SF100-scale build must exceed the Spark-aligned HashJoin budget ({budget})"
        );
        assert!(
            engine.plan_time_smj_reroute(plan.as_ref()),
            "auto selection must prefer SortMergeJoin for SF100-scale equi-joins"
        );
        std::env::remove_var("OXIDANT_SHUFFLE_PARTITIONS");
        std::env::remove_var("OXIDANT_HASH_JOIN_PER_PARTITION_THRESHOLD_BYTES");
    }

    /// Spark SMJ match-buffer / external-sort path: duplicate-heavy equi-join under a
    /// tight FairSpillPool must complete via sort-merge (spillable) rather than OOM.
    #[tokio::test]
    async fn sort_merge_duplicate_keys_spill_under_tiny_pool() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::set_var("OXIDANT_PREFER_HASH_JOIN", "false");
        std::env::set_var("OXIDANT_TARGET_PARTITIONS", "2");
        // Small pool: external sort must spill; duplicate keys stress SMJ buffers.
        let engine = Engine::new_with_memory_limit(32 * 1024 * 1024);
        std::env::remove_var("OXIDANT_TARGET_PARTITIONS");
        // 8k rows, 50 distinct keys → moderate fanout without exploding the COUNT result.
        const N: i64 = 8_000;
        const KEYS: i64 = 50;
        engine
            .register_batches("left_t", join_guard_kv_batches(N, KEYS))
            .unwrap();
        engine
            .register_batches("right_t", join_guard_kv_batches(N, KEYS))
            .unwrap();
        let batches = engine
            .sql("SELECT COUNT(*) AS c FROM left_t l JOIN right_t r ON l.k = r.k")
            .await
            .expect("duplicate-key SMJ must complete under a tiny spillable pool");
        use arrow::array::Int64Array;
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        // Each key has N/KEYS rows on each side → (N/KEYS)^2 matches per key × KEYS.
        let per = N / KEYS;
        assert_eq!(c, per * per * KEYS);
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
    }

    /// KAN-2 throughput: the pre-split optimizer's shape gate. SQL table aliases
    /// (`JOIN date_dim d1`) must be left untouched (the splitter re-renders them with
    /// the alias while pushed predicates carry the base qualifier — TPC-DS Q72's
    /// "No field named date_dim.d_year" worker failure); CTE group-key filters
    /// (TPC-DS Q78/Q39) must be rewritten so the predicate reaches the scan.
    #[tokio::test]
    async fn optimize_logical_plan_shape_gate() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        let engine = Engine::new();
        let mk = |name: &str, cols: Vec<&str>| {
            let schema = Arc::new(Schema::new(
                cols.iter()
                    .map(|n| Field::new(*n, DataType::Int64, false))
                    .collect::<Vec<_>>(),
            ));
            let batch = RecordBatch::try_new(
                schema,
                cols.iter()
                    .map(|_| Arc::new(Int64Array::from(vec![1, 2])) as _)
                    .collect(),
            )
            .unwrap();
            engine.register_batches(name, vec![batch]).unwrap();
        };
        mk(
            "catalog_sales",
            vec!["cs_sold_date_sk", "cs_item_sk", "cs_quantity"],
        );
        mk("date_dim", vec!["d_date_sk", "d_year"]);

        // Aliased dim: the gate leaves the plan byte-for-byte identical.
        let aliased = "SELECT d1.d_year, cs_item_sk, SUM(cs_quantity) AS q \
                       FROM catalog_sales JOIN date_dim d1 ON cs_sold_date_sk = d1.d_date_sk \
                       WHERE d1.d_year = 2000 GROUP BY d1.d_year, cs_item_sk";
        let lp = engine.logical_plan(aliased).await.unwrap();
        let before = format!("{}", lp.display_indent());
        let after = format!(
            "{}",
            engine.optimize_logical_plan(lp).unwrap().display_indent()
        );
        assert_eq!(before, after, "aliased-dim shape must be left untouched");

        // CTE group-key filter: rewritten so the predicate reaches the date_dim scan.
        let cte = "WITH ss AS (\
                       SELECT d_year AS y, cs_item_sk, SUM(cs_quantity) AS q \
                       FROM catalog_sales JOIN date_dim ON cs_sold_date_sk = d_date_sk \
                       GROUP BY d_year, cs_item_sk) \
                   SELECT * FROM ss WHERE y = 2000";
        let lp = engine.logical_plan(cte).await.unwrap();
        let opt = engine.optimize_logical_plan(lp).unwrap();
        let display = format!("{}", opt.display_indent());
        assert!(
            display.contains("Filter: date_dim.d_year = Int64(2000)"),
            "group-key filter must reach the scan:\n{display}"
        );
    }

    /// KAN-2 throughput: union plans get the extended rule set — outer `sale_type`/`dyear`
    /// predicates push into the arms, contradictory arms fold to `EmptyRelation` and drop
    /// out, and each shared-CTE occurrence collapses to the single fact slice it filters
    /// (TPC-DS Q4's six `year_total` occurrences). Without the pruning rules pushdown alone
    /// only made the shared arms textually distinct, defeating stage CSE — the v12 Q4
    /// 66-stage explosion that failed workers with do_get transport errors.
    #[tokio::test]
    async fn optimize_logical_plan_union_arm_pruning() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        let engine = Engine::new();
        let mk = |name: &str, cols: Vec<&str>| {
            let schema = Arc::new(Schema::new(
                cols.iter()
                    .map(|n| Field::new(*n, DataType::Int64, false))
                    .collect::<Vec<_>>(),
            ));
            let batch = RecordBatch::try_new(
                schema,
                cols.iter()
                    .map(|_| Arc::new(Int64Array::from(vec![1, 2])) as _)
                    .collect(),
            )
            .unwrap();
            engine.register_batches(name, vec![batch]).unwrap();
        };
        mk("customer", vec!["c_customer_sk", "c_customer_id"]);
        mk(
            "store_sales",
            vec!["ss_customer_sk", "ss_sold_date_sk", "ss_sales_price"],
        );
        mk(
            "catalog_sales",
            vec!["cs_bill_customer_sk", "cs_sold_date_sk", "cs_sales_price"],
        );
        mk("date_dim", vec!["d_date_sk", "d_year"]);

        // Miniature Q4: a two-arm union CTE (per-arm `sale_type` literal) referenced twice
        // with contradictory per-occurrence predicates.
        let sql = "WITH yt AS (\
                       SELECT c_customer_sk AS customer_sk, d_year AS dyear, \
                              SUM(ss_sales_price) AS year_total, 's' AS sale_type \
                       FROM customer, store_sales, date_dim \
                       WHERE c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk \
                       GROUP BY c_customer_sk, d_year \
                       UNION ALL \
                       SELECT c_customer_sk AS customer_sk, d_year AS dyear, \
                              SUM(cs_sales_price) AS year_total, 'c' AS sale_type \
                       FROM customer, catalog_sales, date_dim \
                       WHERE c_customer_sk = cs_bill_customer_sk AND cs_sold_date_sk = d_date_sk \
                       GROUP BY c_customer_sk, d_year) \
                   SELECT a.customer_sk FROM yt a, yt b \
                   WHERE a.customer_sk = b.customer_sk \
                     AND a.sale_type = 's' AND b.sale_type = 'c' \
                     AND a.dyear = 2001 AND b.dyear = 2001 + 1";
        let lp = engine.logical_plan(sql).await.unwrap();
        let opt = engine.optimize_logical_plan(lp).unwrap();
        let display = format!("{}", opt.display_indent());

        // Each occurrence pruned to its one matching arm: no Union survives, and each fact
        // is scanned exactly once (store by the 's' occurrence, catalog by the 'c' one).
        assert!(
            !display.contains("Union"),
            "every union arm must prune to the matching slice:\n{display}"
        );
        assert_eq!(
            display.matches("TableScan: store_sales").count(),
            1,
            "the 's' occurrence keeps only the store_sales arm:\n{display}"
        );
        assert_eq!(
            display.matches("TableScan: catalog_sales").count(),
            1,
            "the 'c' occurrence keeps only the catalog_sales arm:\n{display}"
        );
        // The group-key year predicates (incl. the folded `2001 + 1`) reach the scans.
        assert!(
            display.contains("d_year = Int64(2001)"),
            "dyear = 2001 must push below the aggregate:\n{display}"
        );
        assert!(
            display.contains("d_year = Int64(2002)"),
            "dyear = 2001 + 1 must fold and push below the aggregate:\n{display}"
        );
    }

    /// KAN-2 throughput: a window over a union stays `Skip` — the skipped classes win over
    /// the union-extended rule set (TPC-DS Q47/Q57's window CTE families broke the
    /// splitter's window handlers when outer predicates moved through the frame).
    #[tokio::test]
    async fn optimize_logical_plan_window_over_union_stays_untouched() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        let engine = Engine::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as _,
                Arc::new(Int64Array::from(vec![10, 20])) as _,
            ],
        )
        .unwrap();
        engine.register_batches("t1", vec![batch.clone()]).unwrap();
        engine.register_batches("t2", vec![batch]).unwrap();
        let sql = "WITH u AS (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) \
                   SELECT k, RANK() OVER (ORDER BY v) AS r FROM u WHERE k = 1";
        let lp = engine.logical_plan(sql).await.unwrap();
        let before = format!("{}", lp.display_indent());
        let after = format!(
            "{}",
            engine.optimize_logical_plan(lp).unwrap().display_indent()
        );
        assert_eq!(before, after, "window-over-union must be left untouched");
    }

    /// KAN-2 throughput: the union-extended rule set folds constant *predicates* but must
    /// never simplify projection expressions — folding `CAST(0 AS DECIMAL(7,2))` to a bare
    /// decimal literal makes the stage-SQL unparser emit `0.00`, which re-parses as
    /// `DECIMAL(3,2)`; downstream decimal coercion then shifts result scales and
    /// distributed results drift from single-node (TPC-DS Q5:
    /// `1141124.71` vs `1141124.710000000000000`).
    #[tokio::test]
    async fn optimize_logical_plan_preserves_decimal_casts_in_projections() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        let engine = Engine::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("sk", DataType::Int64, false),
            Field::new("price", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as _,
                Arc::new(Int64Array::from(vec![10, 20])) as _,
            ],
        )
        .unwrap();
        engine.register_batches("t1", vec![batch.clone()]).unwrap();
        engine.register_batches("t2", vec![batch]).unwrap();
        // Union plan (extended rule set) with an explicit decimal cast in the arm
        // projections and a foldable constant predicate outside.
        let sql = "SELECT u.z, u.price FROM \
                     (SELECT sk, price, CAST(0 AS DECIMAL(7,2)) AS z FROM t1 \
                      UNION ALL SELECT sk, price, CAST(0 AS DECIMAL(7,2)) AS z FROM t2) u \
                   WHERE u.sk = 2001 + 1";
        let lp = engine.logical_plan(sql).await.unwrap();
        let opt = engine.optimize_logical_plan(lp).unwrap();
        let display = format!("{}", opt.display_indent());
        assert!(
            display.contains("CAST(Int64(0) AS Decimal128(7, 2))"),
            "the decimal cast must survive optimization byte-for-byte:\n{display}"
        );
        assert!(
            display.contains("Int64(2002)"),
            "the constant predicate must still fold (2001 + 1 → 2002):\n{display}"
        );
    }

    /// KAN-45/KAN-53: with `OXIDANT_PREFER_HASH_JOIN=true` forced (no `auto` selection, no
    /// `OXIDANT_SORT_MERGE_FALLBACK`), an over-budget build estimate must NOT reroute to a
    /// sort-merge plan — the query runs the hash plan (and, if the actual build fits the
    /// pool, completes). A pool overflow instead fails fast with an actionable error
    /// (covered by the message shape in `collect_join_guarded`).
    #[tokio::test]
    async fn join_guard_forced_hash_runs_overbudget_hash_plan() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        std::env::set_var("OXIDANT_PREFER_HASH_JOIN", "true");
        assert!(!Engine::smj_replan_allowed());
        // Same shape as the plan-time-fallback test but with two partitions: the estimate
        // (76 MB) trips the 64 MiB budget, while the actual keys-only build (one copy per
        // output partition, 2 × 76 MB) fits the 256 MiB pool — so the hash plan completes,
        // proving a forced-hash session never detours onto the sort-merge path.
        std::env::set_var("OXIDANT_TARGET_PARTITIONS", "2");
        let engine = Engine::new_with_memory_limit(256 * 1024 * 1024);
        std::env::remove_var("OXIDANT_TARGET_PARTITIONS");
        const LEFT: i64 = 9_000_000;
        const RIGHT: i64 = 9_500_000;
        engine
            .register_batches("left_t", join_guard_kv_batches(LEFT, LEFT))
            .unwrap();
        engine
            .register_batches("right_t", join_guard_kv_batches(RIGHT, RIGHT))
            .unwrap();
        let query = "SELECT COUNT(*) AS c, SUM(l.v) AS s FROM left_t l JOIN right_t r ON l.k = r.k";
        let plan = engine.physical_plan(query).await.unwrap();
        let budget = engine.hash_join_build_budget().unwrap();
        assert!(hash_join_build_exceeds(plan.as_ref(), budget));
        assert!(
            !engine.plan_time_smj_reroute(plan.as_ref()),
            "a forced-hash session must not reroute an over-budget build to sort-merge"
        );
        let batches = engine
            .sql(query)
            .await
            .expect("over-budget estimate must still run (and complete) as a hash join");
        use arrow::array::Int64Array;
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(c, LEFT);
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
    }

    /// KAN-45/KAN-53: when a hash join genuinely exhausts the bounded pool and the
    /// sort-merge re-plan is not allowed (forced `OXIDANT_PREFER_HASH_JOIN=true`, no
    /// `OXIDANT_SORT_MERGE_FALLBACK`), the error must be fast and actionable — naming the
    /// knobs that would allow the retry — not a silent reroute or a wedge.
    #[tokio::test]
    async fn join_guard_forced_hash_pool_overflow_errors_actionably() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        std::env::set_var("OXIDANT_PREFER_HASH_JOIN", "true");
        std::env::set_var("OXIDANT_TARGET_PARTITIONS", "2");
        std::env::set_var("OXIDANT_BATCH_SIZE", "1024");
        let engine = Engine::new_with_memory_limit(64 * 1024 * 1024);
        std::env::remove_var("OXIDANT_TARGET_PARTITIONS");
        std::env::remove_var("OXIDANT_BATCH_SIZE");
        const LEFT: i64 = 170_000;
        const RIGHT: i64 = 340_000;
        engine
            .register_batches("left_wide", join_guard_wide_batches(LEFT, LEFT, 400))
            .unwrap();
        engine
            .register_batches("right_wide", join_guard_wide_batches(RIGHT, RIGHT, 400))
            .unwrap();
        let query = "SELECT COUNT(*) AS c, SUM(length(l.s)) AS sl, SUM(length(r.s)) AS sr \
             FROM left_wide l JOIN right_wide r ON l.k = r.k";
        let err = engine.sql(query).await.expect_err(
            "pool-overflowing hash join must fail fast when the SMJ re-plan is not allowed",
        );
        let msg = err.to_string();
        assert!(
            msg.contains("OXIDANT_SORT_MERGE_FALLBACK"),
            "error must name the opt-in knob, got: {msg}"
        );
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
    }

    /// KAN-53: `OXIDANT_PREFER_HASH_JOIN` is a tri-state — `auto` is the default and the
    /// fallback for empty/unrecognized values; the legacy boolean spellings still force.
    #[tokio::test]
    async fn join_preference_parses_auto_true_false() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        assert_eq!(join_preference(), JoinPreference::Auto);
        for v in ["auto", "AUTO", " auto ", "", "garbage"] {
            std::env::set_var("OXIDANT_PREFER_HASH_JOIN", v);
            assert_eq!(join_preference(), JoinPreference::Auto, "value {v:?}");
        }
        for v in ["true", "1", "TRUE", "on", "yes"] {
            std::env::set_var("OXIDANT_PREFER_HASH_JOIN", v);
            assert_eq!(join_preference(), JoinPreference::ForceHash, "value {v:?}");
        }
        for v in ["false", "0", "FALSE", "off", "no"] {
            std::env::set_var("OXIDANT_PREFER_HASH_JOIN", v);
            assert_eq!(
                join_preference(),
                JoinPreference::ForceSortMerge,
                "value {v:?}"
            );
        }
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
    }

    /// KAN-53: the env override is honored — `false` forces sort-merge even for a tiny
    /// build (no bounded pool needed), `true` forces hash, and an explicit `auto` behaves
    /// like the default.
    #[tokio::test]
    async fn prefer_hash_join_env_override_honored() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        let query = "SELECT COUNT(*) AS c FROM a JOIN b ON a.k = b.k";
        let plan_display = || async {
            let engine = Engine::new();
            engine
                .register_batches("a", join_guard_kv_batches(1_000, 1_000))
                .unwrap();
            engine
                .register_batches("b", join_guard_kv_batches(2_000, 2_000))
                .unwrap();
            let plan = engine.physical_plan(query).await.unwrap();
            let display = datafusion::physical_plan::displayable(plan.as_ref())
                .indent(false)
                .to_string();
            (contains_hash_join(plan.as_ref()), display)
        };

        std::env::set_var("OXIDANT_PREFER_HASH_JOIN", "false");
        let (has_hash, display) = plan_display().await;
        assert!(
            !has_hash && display.contains("SortMergeJoin"),
            "OXIDANT_PREFER_HASH_JOIN=false must force sort-merge, got:\n{display}"
        );

        for v in ["true", "auto"] {
            std::env::set_var("OXIDANT_PREFER_HASH_JOIN", v);
            let (has_hash, display) = plan_display().await;
            assert!(
                has_hash && !display.contains("SortMergeJoin"),
                "OXIDANT_PREFER_HASH_JOIN={v} must plan a hash join for a small build, got:\n{display}"
            );
        }
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
    }

    /// KAN-53: with the default `auto` selection, a build side estimated UNDER the KAN-25
    /// budget keeps the hash fast path — no plan-time reroute, query completes as a hash
    /// join.
    #[tokio::test]
    async fn auto_join_selection_small_build_keeps_hash() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        assert_eq!(join_preference(), JoinPreference::Auto);
        let engine = Engine::new_with_memory_limit(256 * 1024 * 1024);
        engine
            .register_batches("big", join_guard_kv_batches(100_000, 100_000))
            .unwrap();
        engine
            .register_batches("small", join_guard_kv_batches(1_000, 1_000))
            .unwrap();
        let query = "SELECT COUNT(*) AS c FROM big b JOIN small s ON b.k = s.k";
        let plan = engine.physical_plan(query).await.unwrap();
        assert!(contains_hash_join(plan.as_ref()));
        assert!(
            !engine.plan_time_smj_reroute(plan.as_ref()),
            "a build side estimated under the budget must keep the hash join"
        );
        let batches = engine.sql(query).await.unwrap();
        use arrow::array::Int64Array;
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(c, 1_000);
    }

    /// KAN-53: with the default `auto` selection — and crucially WITHOUT the KAN-45
    /// `OXIDANT_SORT_MERGE_FALLBACK` opt-in — an over-budget build estimate reroutes the
    /// query to sort-merge at plan time and the query completes with correct results.
    /// Same shape as `join_guard_plan_time_fallback_completes_oversized_build`, which
    /// enables the opt-in explicitly.
    #[tokio::test]
    async fn auto_join_selection_large_build_reroutes_sort_merge() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        assert!(
            !Engine::smj_fallback_enabled(),
            "auto selection must not rely on the KAN-45 opt-in"
        );
        assert!(Engine::smj_replan_allowed());
        std::env::set_var("OXIDANT_TARGET_PARTITIONS", "2");
        let engine = Engine::new_with_memory_limit(256 * 1024 * 1024);
        std::env::remove_var("OXIDANT_TARGET_PARTITIONS");
        const LEFT: i64 = 9_000_000;
        const RIGHT: i64 = 9_500_000;
        engine
            .register_batches("left_t", join_guard_kv_batches(LEFT, LEFT))
            .unwrap();
        engine
            .register_batches("right_t", join_guard_kv_batches(RIGHT, RIGHT))
            .unwrap();
        let query = "SELECT COUNT(*) AS c, SUM(l.v) AS s FROM left_t l JOIN right_t r ON l.k = r.k";
        let plan = engine.physical_plan(query).await.unwrap();
        let budget = engine.hash_join_build_budget().unwrap();
        assert!(
            hash_join_build_exceeds(plan.as_ref(), budget),
            "the oversized build side must trip the budget estimate"
        );
        assert!(
            engine.plan_time_smj_reroute(plan.as_ref()),
            "auto selection must reroute an over-budget build to sort-merge"
        );
        let batches = engine
            .sql(query)
            .await
            .expect("auto-rerouted sort-merge join must complete under the bounded pool");
        use arrow::array::Int64Array;
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
        assert_eq!(c, LEFT);
        assert_eq!(s, LEFT * (LEFT - 1) / 2);
    }

    /// KAN-53: under the default `auto` selection the runtime pool-exhaustion retry also
    /// engages WITHOUT the KAN-45 opt-in — an under-reporting estimate whose hash build
    /// overflows the pool at runtime is retried as sort-merge and completes. Mirrors
    /// `join_guard_runtime_retry_when_estimate_underreports` (which sets
    /// `OXIDANT_SORT_MERGE_FALLBACK=true`).
    #[tokio::test]
    async fn auto_runtime_retry_when_estimate_underreports() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        std::env::set_var("OXIDANT_TARGET_PARTITIONS", "2");
        std::env::set_var("OXIDANT_BATCH_SIZE", "1024");
        let engine = Engine::new_with_memory_limit(64 * 1024 * 1024);
        std::env::remove_var("OXIDANT_TARGET_PARTITIONS");
        std::env::remove_var("OXIDANT_BATCH_SIZE");
        const LEFT: i64 = 170_000;
        const RIGHT: i64 = 340_000;
        engine
            .register_batches("left_wide", join_guard_wide_batches(LEFT, LEFT, 400))
            .unwrap();
        engine
            .register_batches("right_wide", join_guard_wide_batches(RIGHT, RIGHT, 400))
            .unwrap();
        let query = "SELECT COUNT(*) AS c, SUM(length(l.s)) AS sl, SUM(length(r.s)) AS sr \
             FROM left_wide l JOIN right_wide r ON l.k = r.k";
        let plan = engine.physical_plan(query).await.unwrap();
        assert!(
            !engine.plan_time_smj_reroute(plan.as_ref()),
            "the flat-width estimate must NOT trip the plan-time guard (runtime retry path)"
        );
        let batches = engine
            .sql(query)
            .await
            .expect("auto selection must retry a pool-exhausted hash join as sort-merge");
        use arrow::array::Int64Array;
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let sl = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let sr = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(c, LEFT);
        assert_eq!(sl, LEFT * 400);
        assert_eq!(sr, LEFT * 400);
    }

    /// KAN-53: a stall-retry attempt (`with_join_strategy_flipped`) re-plans with the
    /// OPPOSITE join strategy from the first attempt and completes with identical
    /// results. A hash first-attempt (small build under `auto`) flips to sort-merge.
    #[tokio::test]
    async fn stall_retry_flip_inverts_join_strategy() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        let engine = Engine::new_with_memory_limit(256 * 1024 * 1024);
        engine
            .register_batches("big", join_guard_kv_batches(100_000, 100_000))
            .unwrap();
        engine
            .register_batches("small", join_guard_kv_batches(1_000, 1_000))
            .unwrap();
        let query = "SELECT COUNT(*) AS c FROM big b JOIN small s ON b.k = s.k";
        // First attempt: auto keeps the hash fast path for the small build...
        let plan = engine.physical_plan(query).await.unwrap();
        assert!(contains_hash_join(plan.as_ref()));
        // ...so the flip decision is sort-merge.
        assert!(!engine.flip_prefer_hash(plan.as_ref()));
        let logical = engine.logical_plan(query).await.unwrap();
        let (_ctx, flipped) = engine
            .physical_plan_with_join_preference(logical, engine.flip_prefer_hash(plan.as_ref()))
            .await
            .unwrap();
        let display = datafusion::physical_plan::displayable(flipped.as_ref())
            .indent(false)
            .to_string();
        assert!(
            display.contains("SortMergeJoin") && !contains_hash_join(flipped.as_ref()),
            "the flip of a hash first-attempt must be a sort-merge plan, got:\n{display}"
        );
        // End to end: the flipped collect path returns the same rows as the plain path.
        let plain = engine.sql(query).await.unwrap();
        let retried = with_join_strategy_flipped(engine.sql(query)).await.unwrap();
        assert_eq!(plain, retried);
    }

    /// KAN-53: the flip also inverts the other direction — an over-budget `auto` first
    /// attempt (which ran the plan-time sort-merge reroute) retries as a hash join.
    #[tokio::test]
    async fn stall_retry_flip_inverts_sort_merge_first_attempt() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        let engine = Engine::new_with_memory_limit(256 * 1024 * 1024);
        engine
            .register_batches("left_t", join_guard_kv_batches(9_000_000, 9_000_000))
            .unwrap();
        engine
            .register_batches("right_t", join_guard_kv_batches(9_500_000, 9_500_000))
            .unwrap();
        let query = "SELECT COUNT(*) AS c FROM left_t l JOIN right_t r ON l.k = r.k";
        // The session plan is hash, but the plan-time guard rerouted the first attempt to
        // sort-merge — the flip must therefore re-plan with hash, not sort-merge again.
        let plan = engine.physical_plan(query).await.unwrap();
        assert!(engine.plan_time_smj_reroute(plan.as_ref()));
        assert!(engine.flip_prefer_hash(plan.as_ref()));
        let logical = engine.logical_plan(query).await.unwrap();
        let (_ctx, flipped) = engine
            .physical_plan_with_join_preference(logical, engine.flip_prefer_hash(plan.as_ref()))
            .await
            .unwrap();
        assert!(
            contains_hash_join(flipped.as_ref()),
            "the flip of a sort-merge first-attempt must be a hash plan"
        );
    }

    /// Test-only scan wrapper that hides child statistics — the Glue/S3 parquet shape
    /// behind SF10's unknown-estimate join OOMs (TPC-H Q16/Q21, TPC-DS Q11). Delegates
    /// everything except `partition_statistics`, which keeps the trait default
    /// (`Statistics::new_unknown`).
    #[derive(Debug)]
    struct UnknownStatsExec {
        inner: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        props: Arc<datafusion::physical_plan::PlanProperties>,
    }

    impl UnknownStatsExec {
        fn new(inner: Arc<dyn datafusion::physical_plan::ExecutionPlan>) -> Self {
            use datafusion::physical_plan::ExecutionPlanProperties;
            let props = datafusion::physical_plan::PlanProperties::new(
                inner.properties().eq_properties.clone(),
                inner.output_partitioning().clone(),
                inner.pipeline_behavior(),
                inner.boundedness(),
            );
            Self {
                inner,
                props: props.into(),
            }
        }
    }

    impl datafusion::physical_plan::DisplayAs for UnknownStatsExec {
        fn fmt_as(
            &self,
            _t: datafusion::physical_plan::DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            write!(f, "UnknownStatsExec")
        }
    }

    impl datafusion::physical_plan::ExecutionPlan for UnknownStatsExec {
        fn name(&self) -> &str {
            "UnknownStatsExec"
        }
        fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
            &self.props
        }
        fn children(&self) -> Vec<&Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
            vec![&self.inner]
        }
        fn with_new_children(
            self: Arc<Self>,
            mut children: Vec<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
        ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
            Ok(Arc::new(Self::new(children.remove(0))))
        }
        fn execute(
            &self,
            partition: usize,
            context: Arc<datafusion::execution::TaskContext>,
        ) -> datafusion::error::Result<datafusion::physical_plan::SendableRecordBatchStream>
        {
            self.inner.execute(partition, context)
        }
    }

    /// Test-only table provider whose scans carry NO usable statistics (unknown row count
    /// AND byte size): `TableProvider::statistics` stays at its unknown default and the
    /// physical scan hides the in-memory batches behind [`UnknownStatsExec`].
    #[derive(Debug)]
    struct UnknownStatsTable {
        inner: datafusion::datasource::MemTable,
    }

    #[async_trait::async_trait]
    impl datafusion::catalog::TableProvider for UnknownStatsTable {
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            self.inner.schema()
        }
        fn table_type(&self) -> datafusion::logical_expr::TableType {
            datafusion::logical_expr::TableType::Base
        }
        async fn scan(
            &self,
            state: &dyn datafusion::catalog::Session,
            projection: Option<&Vec<usize>>,
            filters: &[datafusion::logical_expr::Expr],
            limit: Option<usize>,
        ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
            let inner = self.inner.scan(state, projection, filters, limit).await?;
            Ok(Arc::new(UnknownStatsExec::new(inner)))
        }
    }

    /// Register `batches` under `name` with statistics hidden (see [`UnknownStatsTable`]).
    fn register_unknown_stats_table(engine: &Engine, name: &str, batches: Vec<RecordBatch>) {
        let schema = batches[0].schema();
        let inner = datafusion::datasource::MemTable::try_new(schema, vec![batches]).unwrap();
        engine
            .ctx
            .register_table(name, Arc::new(UnknownStatsTable { inner }))
            .unwrap();
    }

    /// KAN-53 follow-up: with a bounded pool, a hash join whose build side has NO usable
    /// estimate must reroute to sort-merge — unknown is not "fits" (the SF10 OOM shape:
    /// the unaccounted build kills the worker before the runtime retry can fire, KAN-57).
    /// The rerouted query must still complete with correct results (DF 54.1.0 fixed the
    /// sort-merge deadlock).
    #[tokio::test]
    async fn auto_join_selection_unknown_estimate_reroutes_sort_merge() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        assert_eq!(join_preference(), JoinPreference::Auto);
        let engine = Engine::new_with_memory_limit(256 * 1024 * 1024);
        register_unknown_stats_table(&engine, "u_big", join_guard_kv_batches(100_000, 100_000));
        register_unknown_stats_table(&engine, "u_small", join_guard_kv_batches(1_000, 1_000));
        let query = "SELECT COUNT(*) AS c FROM u_big b JOIN u_small s ON b.k = s.k";
        let plan = engine.physical_plan(query).await.unwrap();
        assert!(contains_hash_join(plan.as_ref()));
        assert!(
            hash_join_build_estimate_unknown(plan.as_ref()),
            "test shape must have an unknown build-side estimate"
        );
        assert!(
            engine.plan_time_smj_reroute(plan.as_ref()),
            "unknown estimate + bounded pool must reroute to sort-merge"
        );
        let batches = engine
            .sql(query)
            .await
            .expect("unknown-estimate join must complete via sort-merge");
        use arrow::array::Int64Array;
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(c, 1_000);
    }

    /// KAN-53 follow-up: with an explicit unbounded pool (`OXIDANT_MEMORY_LIMIT_BYTES=0`)
    /// there is no budget to guard — an unknown-estimate hash join keeps the hash plan
    /// (behavior unchanged), matching the positive-estimate case
    /// (`auto_join_selection_small_build_keeps_hash`). Unset MEMORY_LIMIT now auto-sizes
    /// a bounded pool, so this test opts out explicitly.
    #[tokio::test]
    async fn auto_join_selection_unknown_estimate_unbounded_keeps_hash() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        let prev = std::env::var("OXIDANT_MEMORY_LIMIT_BYTES").ok();
        std::env::set_var("OXIDANT_MEMORY_LIMIT_BYTES", "0");
        let engine = Engine::new();
        register_unknown_stats_table(&engine, "u_big", join_guard_kv_batches(100_000, 100_000));
        register_unknown_stats_table(&engine, "u_small", join_guard_kv_batches(1_000, 1_000));
        let query = "SELECT COUNT(*) AS c FROM u_big b JOIN u_small s ON b.k = s.k";
        let plan = engine.physical_plan(query).await.unwrap();
        assert!(contains_hash_join(plan.as_ref()));
        assert!(hash_join_build_estimate_unknown(plan.as_ref()));
        assert!(
            !engine.plan_time_smj_reroute(plan.as_ref()),
            "unbounded pool must never reroute — no budget to guard"
        );
        let batches = engine.sql(query).await.unwrap();
        use arrow::array::Int64Array;
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(c, 1_000);
        match prev {
            Some(v) => std::env::set_var("OXIDANT_MEMORY_LIMIT_BYTES", v),
            None => std::env::remove_var("OXIDANT_MEMORY_LIMIT_BYTES"),
        }
    }

    // ---- KAN-8: parquet footer statistics on catalog scans ----------------------

    /// Write `rows` `(k, v)` Int64 rows as parquet part files in a fresh temp dir — the
    /// on-disk counterpart of [`join_guard_kv_batches`], for catalog-parquet scan tests.
    fn write_kv_parquet_dir(rows: i64) -> std::path::PathBuf {
        use datafusion::parquet::arrow::ArrowWriter;
        let dir = std::env::temp_dir().join(format!(
            "oxidant-scan-stats-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        for (i, batch) in join_guard_kv_batches(rows, rows).into_iter().enumerate() {
            let f = std::fs::File::create(dir.join(format!("part-{i}.parquet"))).unwrap();
            let mut w = ArrowWriter::try_new(f, batch.schema(), None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
        dir
    }

    /// Register a parquet dir as a CATALOG parquet table — the `LakehouseTableProvider` path
    /// Glue/Hive/REST parquet (and Delta/Iceberg) scans take — rather than
    /// `register_parquet`'s DataFusion `ListingTable` (which collects footer statistics on
    /// its own via `datafusion.execution.collect_statistics=true`).
    async fn register_catalog_parquet(engine: &Engine, name: &str, dir: &std::path::Path) {
        let md = oxidant_catalog::TableMetadata::new(
            name,
            format!("file://{}", dir.display()),
            oxidant_catalog::TableFormat::Parquet,
        );
        let provider = catalog_bridge::metadata_to_provider(&engine.ctx.state(), &md, name, false)
            .await
            .unwrap()
            .provider;
        engine.ctx.register_table(name, provider).unwrap();
    }

    /// KAN-8: catalog parquet scans carry exact footer row counts — the statistics the
    /// KAN-25 build-side budget guard and DataFusion's own join selection key on. Deliberate
    /// behavior change: before footer-stat attachment every catalog parquet scan reported
    /// `Statistics::new_unknown`, so the unknown-estimate reroute sent every such join to
    /// sort-merge. `OXIDANT_PARQUET_SCAN_STATS=0` restores the old shape (the escape hatch).
    /// KAN-143: column-level min/max/null counts ride along when the declared schema matches
    /// the file's physical columns (see `column_stats_trusted` in catalog_bridge).
    #[tokio::test]
    async fn catalog_parquet_scan_attaches_footer_row_counts() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PARQUET_SCAN_STATS");
        let engine = Engine::new();
        // 4 + 6 rows across two files: the table aggregate must sum footers exactly.
        let dir = write_kv_parquet_dir(4);
        {
            use arrow::array::Int64Array;
            use arrow::datatypes::{DataType, Field, Schema};
            use datafusion::parquet::arrow::ArrowWriter;
            // Replace the helper's four 1-row files with a 4-row and a 6-row file.
            for entry in std::fs::read_dir(&dir).unwrap() {
                std::fs::remove_file(entry.unwrap().path()).unwrap();
            }
            let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
            for (part, rows) in [(0, 4_i64), (1, 6_i64)] {
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int64Array::from((0..rows).collect::<Vec<i64>>()))],
                )
                .unwrap();
                let f = std::fs::File::create(dir.join(format!("part-{part}.parquet"))).unwrap();
                let mut w = ArrowWriter::try_new(f, schema.clone(), None).unwrap();
                w.write(&batch).unwrap();
                w.close().unwrap();
            }
        }
        let md = oxidant_catalog::TableMetadata::new(
            "t",
            format!("file://{}", dir.display()),
            oxidant_catalog::TableFormat::Parquet,
        );
        let provider = catalog_bridge::metadata_to_provider(&engine.ctx.state(), &md, "t", false)
            .await
            .unwrap()
            .provider;
        let scan = provider
            .scan(&engine.ctx.state(), None, &[], None)
            .await
            .unwrap();
        let stats = scan.partition_statistics(None).unwrap();
        assert!(
            matches!(
                stats.num_rows,
                datafusion::common::stats::Precision::Exact(10)
            ),
            "footer row counts must sum to an exact table row count, got {:?}",
            stats.num_rows
        );
        // Per-file counts keep the original file order (part-0 = 4 rows). KAN-143: with a
        // schema that matches the file's physical columns (inferred here, so names/types
        // line up exactly), column-level statistics are ATTACHED — min/max/null counts from
        // the footer. They are dropped only for case/type-mismatched files, which the
        // parquet opener would otherwise read as constant-column proofs (the
        // `declared_schema_matches_columns_case_insensitively` regression).
        let part0 = scan.partition_statistics(Some(0)).unwrap();
        assert!(
            matches!(
                part0.num_rows,
                datafusion::common::stats::Precision::Exact(4)
            ),
            "partition 0 must be part-0.parquet's footer count, got {:?}",
            part0.num_rows
        );
        let part0_x = &part0.column_statistics[0];
        assert!(
            matches!(
                part0_x.min_value.get_value(),
                Some(datafusion::common::ScalarValue::Int64(Some(0)))
            ) && matches!(
                part0_x.max_value.get_value(),
                Some(datafusion::common::ScalarValue::Int64(Some(3)))
            ) && matches!(
                part0_x.null_count,
                datafusion::common::stats::Precision::Exact(0)
            ),
            "part-0 (x in 0..4) must carry footer column statistics, got {:?}",
            part0.column_statistics
        );
        // The table-wide aggregate folds both files: min 0, max 5, exact null count.
        let agg_x = &stats.column_statistics[0];
        assert!(
            matches!(
                agg_x.min_value.get_value(),
                Some(datafusion::common::ScalarValue::Int64(Some(0)))
            ) && matches!(
                agg_x.max_value.get_value(),
                Some(datafusion::common::ScalarValue::Int64(Some(5)))
            ) && matches!(
                agg_x.null_count,
                datafusion::common::stats::Precision::Exact(0)
            ),
            "the table aggregate must span both files' column statistics, got {:?}",
            stats.column_statistics
        );
        // The escape hatch: statistics disabled → the pre-KAN-8 unknown shape.
        std::env::set_var("OXIDANT_PARQUET_SCAN_STATS", "0");
        let scan = provider
            .scan(&engine.ctx.state(), None, &[], None)
            .await
            .unwrap();
        let stats = scan.partition_statistics(None).unwrap();
        assert_eq!(
            stats.num_rows.get_value(),
            None,
            "OXIDANT_PARQUET_SCAN_STATS=0 must restore unknown statistics"
        );
        std::env::remove_var("OXIDANT_PARQUET_SCAN_STATS");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// KAN-8: with footer statistics attached, the default `auto` join selection no longer
    /// reroutes a catalog-parquet join whose build side fits the KAN-25 budget — the hash
    /// fast path Spark also takes. Deliberate behavior change: pre-KAN-8 this exact shape
    /// rerouted to sort-merge because every catalog parquet scan was stats-unknown (the
    /// SF10 TPC-DS all-sort-merge plans).
    #[tokio::test]
    async fn auto_join_selection_statted_parquet_small_build_keeps_hash() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        std::env::remove_var("OXIDANT_PARQUET_SCAN_STATS");
        let engine = Engine::new_with_memory_limit(256 * 1024 * 1024);
        let big = write_kv_parquet_dir(100_000);
        let small = write_kv_parquet_dir(1_000);
        register_catalog_parquet(&engine, "p_big", &big).await;
        register_catalog_parquet(&engine, "p_small", &small).await;
        let query = "SELECT COUNT(*) AS c FROM p_big b JOIN p_small s ON b.k = s.k";
        let plan = engine.physical_plan(query).await.unwrap();
        assert!(contains_hash_join(plan.as_ref()));
        assert!(
            !hash_join_build_estimate_unknown(plan.as_ref()),
            "footer statistics must give every build side an estimate"
        );
        assert!(
            !engine.plan_time_smj_reroute(plan.as_ref()),
            "a footer-sized build under the budget must keep the hash join"
        );
        let batches = engine.sql(query).await.unwrap();
        use arrow::array::Int64Array;
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(c, 1_000);
        let _ = std::fs::remove_dir_all(&big);
        let _ = std::fs::remove_dir_all(&small);
    }

    /// KAN-8 escape hatch: `OXIDANT_PARQUET_SCAN_STATS=0` restores stats-unknown scans, and the
    /// KAN-25 unknown-estimate reroute engages exactly as before (bounded pool ⇒ sort-merge)
    /// — the two unknown-stats selections are still reachable and still correct.
    #[tokio::test]
    async fn auto_join_selection_parquet_stats_disabled_reroutes_sort_merge() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        std::env::set_var("OXIDANT_PARQUET_SCAN_STATS", "0");
        let engine = Engine::new_with_memory_limit(256 * 1024 * 1024);
        let big = write_kv_parquet_dir(100_000);
        let small = write_kv_parquet_dir(1_000);
        register_catalog_parquet(&engine, "p_big", &big).await;
        register_catalog_parquet(&engine, "p_small", &small).await;
        let query = "SELECT COUNT(*) AS c FROM p_big b JOIN p_small s ON b.k = s.k";
        let plan = engine.physical_plan(query).await.unwrap();
        assert!(contains_hash_join(plan.as_ref()));
        assert!(
            hash_join_build_estimate_unknown(plan.as_ref()),
            "statistics disabled must make the build-side estimate unknown again"
        );
        assert!(
            engine.plan_time_smj_reroute(plan.as_ref()),
            "unknown estimate + bounded pool must reroute to sort-merge"
        );
        let batches = engine
            .sql(query)
            .await
            .expect("unknown-estimate join must complete via sort-merge");
        use arrow::array::Int64Array;
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(c, 1_000);
        std::env::remove_var("OXIDANT_PARQUET_SCAN_STATS");
        let _ = std::fs::remove_dir_all(&big);
        let _ = std::fs::remove_dir_all(&small);
    }

    /// KAN-8: statistics change the DEFAULT, not the guard — a catalog-parquet build whose
    /// footer-derived estimate exceeds the KAN-25 budget still reroutes to sort-merge and
    /// completes (the SF10 OOM shape the reroute exists for). The sort-merge path stays the
    /// safety valve for builds that genuinely do not fit.
    #[tokio::test]
    async fn auto_join_selection_statted_parquet_large_build_still_reroutes() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        std::env::remove_var("OXIDANT_PARQUET_SCAN_STATS");
        std::env::set_var("OXIDANT_TARGET_PARTITIONS", "2");
        let engine = Engine::new_with_memory_limit(64 * 1024 * 1024);
        std::env::remove_var("OXIDANT_TARGET_PARTITIONS");
        // 1.5M (k, v) Int64 rows ≈ 24 MB estimated build — over the 16 MiB budget (64 MiB
        // pool × 0.25). The aggregates over BOTH value columns keep the build side from
        // being pruned to keys, mirroring `join_guard_runtime_retry_when_estimate_underreports`
        // (a keys-only build is the shape DataFusion already handles well).
        const ROWS: i64 = 1_500_000;
        let left = write_kv_parquet_dir(ROWS);
        let right = write_kv_parquet_dir(ROWS);
        register_catalog_parquet(&engine, "p_left", &left).await;
        register_catalog_parquet(&engine, "p_right", &right).await;
        let query = "SELECT COUNT(*) AS c, SUM(l.v) AS s, SUM(r.v) AS t \
             FROM p_left l JOIN p_right r ON l.k = r.k";
        let plan = engine.physical_plan(query).await.unwrap();
        let budget = engine.hash_join_build_budget().unwrap();
        assert!(
            hash_join_build_exceeds(plan.as_ref(), budget),
            "the footer-derived oversized build must trip the budget estimate"
        );
        assert!(
            engine.plan_time_smj_reroute(plan.as_ref()),
            "a known over-budget build must still reroute to sort-merge"
        );
        let batches = engine
            .sql(query)
            .await
            .expect("rerouted sort-merge join must complete under the bounded pool");
        use arrow::array::Int64Array;
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
        assert_eq!(c, ROWS);
        assert_eq!(s, ROWS * (ROWS - 1) / 2);
        let _ = std::fs::remove_dir_all(&left);
        let _ = std::fs::remove_dir_all(&right);
    }

    // ---- KAN-8 follow-up: multi-join chains (the TPC-DS Q37/Q82 stage-0 shape) ----

    /// Row counts for the Q37/Q82 join-chain fixture: a 100k-row fact table whose
    /// keys each match exactly one row of dims 1..3.
    const JOIN_CHAIN_FACT: i64 = 100_000;
    const JOIN_CHAIN_DIMS: [i64; 3] = [100, 1_000, 10_000];

    /// The Q37/Q82 stage-0 shape: a 3-join chain over catalog-parquet tables with a
    /// partial-style aggregate on top.
    const JOIN_CHAIN_SQL: &str = "SELECT COUNT(*) AS c, SUM(f.v) AS s \
        FROM fact f \
        JOIN dim1 d1 ON f.k1 = d1.k \
        JOIN dim2 d2 ON f.k2 = d2.k \
        JOIN dim3 d3 ON f.k3 = d3.k";

    /// `(k1, k2, k3, v)` Int64 fact-table parquet dir for multi-join chain tests:
    /// `kN = i % key_mods[N]`, so every fact row matches exactly one row of each
    /// [`write_kv_parquet_dir`] dim table of `key_mods[N]` rows. Four part files,
    /// mirroring [`write_kv_parquet_dir`].
    fn write_fact_parquet_dir(rows: i64, key_mods: [i64; 3]) -> std::path::PathBuf {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;
        let dir = std::env::temp_dir().join(format!(
            "oxidant-scan-stats-fact-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("k1", DataType::Int64, false),
            Field::new("k2", DataType::Int64, false),
            Field::new("k3", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let per = rows / 4;
        for p in 0..4_i64 {
            let idx: Vec<i64> = (p * per..(p + 1) * per).collect();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        idx.iter().map(|&i| i % key_mods[0]).collect::<Vec<i64>>(),
                    )),
                    Arc::new(Int64Array::from(
                        idx.iter().map(|&i| i % key_mods[1]).collect::<Vec<i64>>(),
                    )),
                    Arc::new(Int64Array::from(
                        idx.iter().map(|&i| i % key_mods[2]).collect::<Vec<i64>>(),
                    )),
                    Arc::new(Int64Array::from(idx)),
                ],
            )
            .unwrap();
            let f = std::fs::File::create(dir.join(format!("part-{p}.parquet"))).unwrap();
            let mut w = ArrowWriter::try_new(f, batch.schema(), None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
        dir
    }

    /// Register the 4-table join-chain fixture as catalog-parquet tables; returns
    /// the temp dirs to clean up.
    async fn register_join_chain(engine: &Engine) -> [std::path::PathBuf; 4] {
        let fact = write_fact_parquet_dir(JOIN_CHAIN_FACT, JOIN_CHAIN_DIMS);
        let dim1 = write_kv_parquet_dir(JOIN_CHAIN_DIMS[0]);
        let dim2 = write_kv_parquet_dir(JOIN_CHAIN_DIMS[1]);
        let dim3 = write_kv_parquet_dir(JOIN_CHAIN_DIMS[2]);
        register_catalog_parquet(engine, "fact", &fact).await;
        register_catalog_parquet(engine, "dim1", &dim1).await;
        register_catalog_parquet(engine, "dim2", &dim2).await;
        register_catalog_parquet(engine, "dim3", &dim3).await;
        [fact, dim1, dim2, dim3]
    }

    /// Whether any hash join in `plan` builds on another hash join's output — a
    /// join-chain build side (the unbounded-build shape the KAN-2
    /// [`PreferBoundedJoinBuildSide`] rule re-seats onto row-bounded inputs).
    fn build_side_contains_hash_join(plan: &dyn datafusion::physical_plan::ExecutionPlan) -> bool {
        if let Some(hj) = as_hash_join(plan) {
            if contains_hash_join(hj.left().as_ref()) {
                return true;
            }
        }
        plan.children()
            .iter()
            .any(|c| build_side_contains_hash_join(c.as_ref()))
    }

    /// The highest hash join in `plan` that has another hash join below it (the
    /// output join of a join chain).
    fn top_hash_join(
        plan: &dyn datafusion::physical_plan::ExecutionPlan,
    ) -> Option<&datafusion::physical_plan::joins::HashJoinExec> {
        if let Some(hj) = as_hash_join(plan) {
            if contains_hash_join(hj.left().as_ref()) || contains_hash_join(hj.right().as_ref()) {
                return Some(hj);
            }
        }
        // Consume the children Vec by value so the returned reference borrows from
        // `plan`, not from the temporary Vec.
        for child in plan.children() {
            if let Some(hj) = top_hash_join(child.as_ref()) {
                return Some(hj);
            }
        }
        None
    }

    /// Assert the join-chain aggregate row: every fact row matches exactly one row of
    /// each dim, so the count is the fact row count and the sum is over all `f.v`.
    fn assert_join_chain_result(batches: &[RecordBatch]) {
        use arrow::array::Int64Array;
        let get = |col: usize| {
            batches[0]
                .column(col)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        assert_eq!(get(0), JOIN_CHAIN_FACT);
        assert_eq!(get(1), JOIN_CHAIN_FACT * (JOIN_CHAIN_FACT - 1) / 2);
    }

    /// Q37/Q82 regression guard (KAN-8): with footer statistics attached, the default
    /// `auto` join selection must NOT reroute a multi-join CHAIN over catalog-parquet
    /// tables to sort-merge. Pre-KAN-8 every catalog scan was stats-unknown, so the
    /// unknown-estimate reroute flipped the WHOLE stage-0 chain to sort-merge (the
    /// 10.8x/9.1x SF10 outliers: external sorts of the 117M-row replicated inventory
    /// etc.). With footer row counts, DataFusion 54.1.0 estimates each join output as
    /// `Inexact(min(l, r))` rows — proven directly by
    /// [`join_chain_output_statistics_report_usable_inexact_num_rows`] — which the
    /// KAN-25 guard reads as a usable build-side estimate. (KAN-2 follow-up: those
    /// inexact chain estimates no longer place join outputs on BUILD sides — see
    /// [`auto_join_selection_q62_arm_chain_builds_row_bounded_sides`] — so the kept
    /// hash chain builds on the exact dims, as SF10 Q37/Q82's own plans already did.)
    #[tokio::test]
    async fn auto_join_selection_statted_parquet_join_chain_keeps_hash() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        std::env::remove_var("OXIDANT_PARQUET_SCAN_STATS");
        assert_eq!(join_preference(), JoinPreference::Auto);
        let engine = Engine::new_with_memory_limit(256 * 1024 * 1024);
        let dirs = register_join_chain(&engine).await;
        let plan = engine.physical_plan(JOIN_CHAIN_SQL).await.unwrap();
        // The fixture plans as a left-deep 3-hash-join chain. With the KAN-2
        // bounded-build-side rule the two UPPER builds are the exact dims (their chain
        // inputs report only phantom `Inexact(100)` estimates), not the join outputs —
        // the same build sides SF10 Q37/Q82's plans already picked via DataFusion's raw
        // comparison (the item-side estimate exceeds date_dim's there).
        let display = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(false)
            .to_string();
        assert_eq!(
            display.matches("HashJoinExec").count(),
            3,
            "expected a 3-hash-join chain, got:\n{display}"
        );
        assert!(
            !build_side_contains_hash_join(plan.as_ref()),
            "no build side may be a join output (KAN-2 bounded builds), got:\n{display}"
        );
        assert!(
            !hash_join_build_estimate_unknown(plan.as_ref()),
            "footer statistics must give every build side an estimate"
        );
        assert!(
            !engine.plan_time_smj_reroute(plan.as_ref()),
            "a footer-sized multi-join chain under the budget must keep its hash joins"
        );
        assert_eq!(engine.plan_time_smj_reroute_count(), 0);
        let batches = engine.sql(JOIN_CHAIN_SQL).await.unwrap();
        assert_join_chain_result(&batches);
        for d in dirs {
            let _ = std::fs::remove_dir_all(d);
        }
    }

    /// Non-vacuity for the Q37/Q82 guard: `OXIDANT_PARQUET_SCAN_STATS=0` restores the
    /// pre-KAN-8 unknown-statistics scans, and the SAME multi-join chain reroutes to
    /// sort-merge exactly as pre-fix — so
    /// [`auto_join_selection_statted_parquet_join_chain_keeps_hash`] is sensitive to
    /// footer statistics, not passing vacuously.
    #[tokio::test]
    async fn auto_join_selection_join_chain_stats_disabled_reroutes_sort_merge() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        std::env::set_var("OXIDANT_PARQUET_SCAN_STATS", "0");
        let engine = Engine::new_with_memory_limit(256 * 1024 * 1024);
        let dirs = register_join_chain(&engine).await;
        let plan = engine.physical_plan(JOIN_CHAIN_SQL).await.unwrap();
        assert!(contains_hash_join(plan.as_ref()));
        assert!(
            hash_join_build_estimate_unknown(plan.as_ref()),
            "statistics disabled must make the chain's build-side estimates unknown again"
        );
        assert!(
            engine.plan_time_smj_reroute(plan.as_ref()),
            "unknown estimates + bounded pool must reroute the whole chain to sort-merge"
        );
        let batches = engine
            .sql(JOIN_CHAIN_SQL)
            .await
            .expect("unknown-estimate join chain must complete via sort-merge");
        assert_join_chain_result(&batches);
        std::env::remove_var("OXIDANT_PARQUET_SCAN_STATS");
        for d in dirs {
            let _ = std::fs::remove_dir_all(d);
        }
    }

    /// Assumption check behind the Q37/Q82 guard, directly against DataFusion 54.1.0: with
    /// exact scan row counts AND footer column statistics (KAN-143 — pre-KAN-143 the footer
    /// attachment stripped them, see [`catalog_parquet_scan_attaches_footer_row_counts`]), a
    /// foreign-key join chain's OUTPUT statistics report `num_rows = Inexact(fact_rows)` —
    /// `estimate_inner_join_cardinality` derives the join selectivity from max-distinct
    /// estimates, which the join keys' min/max ranges pin to the dimension key domains
    /// rather than the fact row count (datafusion-physical-plan-54.1.0 `joins/utils.rs`),
    /// and `Precision::Inexact::get_value()` is `Some`, so the KAN-25 guard's
    /// [`hash_join_build_estimated_bytes`] treats a join-output side as estimable instead
    /// of rerouting it to sort-merge. (KAN-2/KAN-143: pre-KAN-143 this estimate was
    /// `Inexact(min(l, r))` — a phantom orders of magnitude under the real fact-sized
    /// output; the chain output is the PROBE side now, and the guard sizes the exact dim
    /// builds it is placed on.)
    #[tokio::test]
    async fn join_chain_output_statistics_report_usable_inexact_num_rows() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PARQUET_SCAN_STATS");
        // Unbounded pool: this pins the statistics themselves, not the guard decision.
        let engine = Engine::new();
        let fact = write_fact_parquet_dir(JOIN_CHAIN_FACT, JOIN_CHAIN_DIMS);
        let dim1 = write_kv_parquet_dir(JOIN_CHAIN_DIMS[0]);
        let dim2 = write_kv_parquet_dir(JOIN_CHAIN_DIMS[1]);
        register_catalog_parquet(&engine, "fact", &fact).await;
        register_catalog_parquet(&engine, "dim1", &dim1).await;
        register_catalog_parquet(&engine, "dim2", &dim2).await;
        let plan = engine
            .physical_plan(
                "SELECT COUNT(*) AS c FROM fact f \
                 JOIN dim1 d1 ON f.k1 = d1.k JOIN dim2 d2 ON f.k2 = d2.k",
            )
            .await
            .unwrap();
        // The upper of the two hash joins carries the CHAIN's output statistics. With
        // footer min/max on the join keys the NDV estimates are the dim key domains
        // (100 and 1_000), so fact ⋈ dim1 estimates fact-sized 100_000 in, and
        // 100_000 ⋈ dim2 estimates 100_000 out (pre-KAN-143: min(l, r) = 100 both times).
        let top = top_hash_join(plan.as_ref()).expect("a 2-join chain");
        let stats =
            datafusion::physical_plan::ExecutionPlan::partition_statistics(top, None).unwrap();
        assert!(
            matches!(
                stats.num_rows,
                datafusion::common::stats::Precision::Inexact(100_000)
            ),
            "join-chain output must estimate fact-sized rows (not Inexact(min(l, r))), got {:?}",
            stats.num_rows
        );
        assert!(
            stats.num_rows.get_value().is_some(),
            "an Inexact num_rows must read as a usable estimate"
        );
        // The guard-facing consequence (KAN-2): the chain output sits on the top join's
        // PROBE side (its estimate must never seat a build), and the dim BUILD side is
        // exactly sized — the propagation the Q37/Q82 fix relies on.
        assert!(
            contains_hash_join(top.right().as_ref()),
            "the fixture must probe the top join with the lower join's output"
        );
        assert!(
            !contains_hash_join(top.left().as_ref()),
            "the fixture must build the top join on the exact dim (KAN-2)"
        );
        assert!(
            hash_join_build_estimated_bytes(top).is_some(),
            "the dim build side must be estimable to the KAN-25 guard"
        );
        let _ = std::fs::remove_dir_all(&fact);
        let _ = std::fs::remove_dir_all(&dim1);
        let _ = std::fs::remove_dir_all(&dim2);
    }

    // ---- KAN-2 A3: driver-measured row counts on shuffle-input scans --------------

    /// A3: a shuffle-input table registered with its barrier-measured row count gives the
    /// join guard a real build-side estimate, so a measured-small build keeps the hash
    /// join under the bounded pool — the runtime SMJ→hash conversion. (DataFusion 54's
    /// MemTable already reports exact batch-derived statistics; the measured path makes
    /// the number the driver counted at the stage barrier authoritative and skips the
    /// per-batch statistics recomputation at plan time.)
    #[tokio::test]
    async fn auto_join_selection_measured_small_build_keeps_hash() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        let engine = Engine::new_with_memory_limit(256 * 1024 * 1024);
        engine
            .register_batches_with_stats("big", join_guard_kv_batches(100_000, 100_000), 100_000)
            .unwrap();
        engine
            .register_batches_with_stats("small", join_guard_kv_batches(1_000, 1_000), 1_000)
            .unwrap();
        assert_eq!(engine.measured_stats_registration_count(), 2);
        let query = "SELECT COUNT(*) AS c FROM big b JOIN small s ON b.k = s.k";
        let plan = engine.physical_plan(query).await.unwrap();
        assert!(contains_hash_join(plan.as_ref()));
        assert!(
            !hash_join_build_estimate_unknown(plan.as_ref()),
            "measured row counts must give every build side an estimate"
        );
        assert!(
            !engine.plan_time_smj_reroute(plan.as_ref()),
            "a measured build under the budget must keep the hash join"
        );
        assert_eq!(engine.plan_time_smj_reroute_count(), 0);
        let batches = engine.sql(query).await.unwrap();
        use arrow::array::Int64Array;
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(c, 1_000);
    }

    /// A3: the safety valve stays — a build whose MEASURED row count blows the budget
    /// still reroutes to sort-merge and completes. Statistics change the default, not the
    /// guard.
    #[tokio::test]
    async fn auto_join_selection_measured_large_build_still_reroutes() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        std::env::set_var("OXIDANT_TARGET_PARTITIONS", "2");
        let engine = Engine::new_with_memory_limit(64 * 1024 * 1024);
        std::env::remove_var("OXIDANT_TARGET_PARTITIONS");
        // 1.5M (k, v) Int64 rows ≈ 24 MB estimated build — over the 16 MiB budget (64 MiB
        // pool × 0.25).
        const ROWS: i64 = 1_500_000;
        engine
            .register_batches_with_stats("left_t", join_guard_kv_batches(ROWS, ROWS), ROWS as u64)
            .unwrap();
        engine
            .register_batches_with_stats("right_t", join_guard_kv_batches(ROWS, ROWS), ROWS as u64)
            .unwrap();
        let query = "SELECT COUNT(*) AS c, SUM(l.v) AS s, SUM(r.v) AS t \
             FROM left_t l JOIN right_t r ON l.k = r.k";
        let plan = engine.physical_plan(query).await.unwrap();
        let budget = engine.hash_join_build_budget().unwrap();
        assert!(
            hash_join_build_exceeds(plan.as_ref(), budget),
            "the measured oversized build must trip the budget estimate"
        );
        assert!(
            engine.plan_time_smj_reroute(plan.as_ref()),
            "a measured over-budget build must still reroute to sort-merge"
        );
        assert!(engine.plan_time_smj_reroute_count() > 0);
        let batches = engine
            .sql(query)
            .await
            .expect("rerouted sort-merge join must complete under the bounded pool");
        use arrow::array::Int64Array;
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
        assert_eq!(c, ROWS);
        assert_eq!(s, ROWS * (ROWS - 1) / 2);
    }

    // ---- KAN-142: per-join runtime strategy conversion --------------------------------

    /// How many sort-merge joins a plan tree contains (KAN-142 shape assertions).
    fn sort_merge_join_count(plan: &dyn datafusion::physical_plan::ExecutionPlan) -> usize {
        let here = usize::from(
            (plan as &dyn std::any::Any)
                .is::<datafusion::physical_plan::joins::SortMergeJoinExec>(),
        );
        here + plan
            .children()
            .iter()
            .map(|c| sort_merge_join_count(c.as_ref()))
            .sum::<usize>()
    }

    /// How many repartition (shuffle) nodes a plan tree contains (KAN-142 broadcast
    /// elision assertions — `CollectLeft` uses a `CoalescePartitionsExec` instead).
    fn repartition_count(plan: &dyn datafusion::physical_plan::ExecutionPlan) -> usize {
        let here = usize::from(
            (plan as &dyn std::any::Any)
                .is::<datafusion::physical_plan::repartition::RepartitionExec>(),
        );
        here + plan
            .children()
            .iter()
            .map(|c| repartition_count(c.as_ref()))
            .sum::<usize>()
    }

    /// The partition mode of every hash join in a plan tree (KAN-142 shape assertions).
    fn hash_join_modes(
        plan: &dyn datafusion::physical_plan::ExecutionPlan,
    ) -> Vec<datafusion::physical_plan::joins::PartitionMode> {
        let mut modes = Vec::new();
        if let Some(hj) = as_hash_join(plan) {
            modes.push(*hj.partition_mode());
        }
        for child in plan.children() {
            modes.extend(hash_join_modes(child.as_ref()));
        }
        modes
    }

    /// Indent-render a plan for assertion messages (KAN-142).
    fn plan_display(plan: &dyn datafusion::physical_plan::ExecutionPlan) -> String {
        datafusion::physical_plan::displayable(plan)
            .indent(false)
            .to_string()
    }

    /// KAN-142: a multi-join stage no longer picks ONE strategy for all joins — the
    /// per-join re-plan converts the over-budget build to sort-merge while the
    /// measured-small dim build keeps a hash join, in ONE physical plan (before
    /// KAN-142 the whole stage went sort-merge). The plan also passes the
    /// whole-plan-fallback safety check, and the mixed plan returns the right rows.
    #[tokio::test]
    async fn per_join_strategy_converts_each_join_on_its_own_build_size() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        std::env::remove_var("OXIDANT_BROADCAST_JOIN_THRESHOLD_BYTES");
        std::env::set_var("OXIDANT_TARGET_PARTITIONS", "2");
        let engine = Engine::new_with_memory_limit(64 * 1024 * 1024);
        std::env::remove_var("OXIDANT_TARGET_PARTITIONS");
        // fact/fact2: 1.5M (k, v) Int64 rows ≈ 24 MB estimated build (the SUMs keep `v`
        // in the scan projection) — over the 16 MiB budget (64 MiB pool × 0.25). dim:
        // 1_000 rows ≈ 16 KB — fits with room.
        const ROWS: i64 = 1_500_000;
        engine
            .register_batches_with_stats("fact", join_guard_kv_batches(ROWS, ROWS), ROWS as u64)
            .unwrap();
        engine
            .register_batches_with_stats("fact2", join_guard_kv_batches(ROWS, ROWS), ROWS as u64)
            .unwrap();
        engine
            .register_batches_with_stats("dim", join_guard_kv_batches(1_000, 1_000), 1_000)
            .unwrap();
        let query = "SELECT COUNT(*) AS c, SUM(f.v) AS sf, SUM(d.v) AS sd, SUM(f2.v) AS s2 \
                     FROM fact f JOIN dim d ON f.k = d.k \
                     JOIN fact2 f2 ON f.k = f2.k";
        // The guard still reads the DEFAULT (all-hash) plan: the fact2 build trips it.
        let plan = engine.physical_plan(query).await.unwrap();
        assert_eq!(sort_merge_join_count(plan.as_ref()), 0);
        assert!(
            engine.plan_time_smj_reroute(plan.as_ref()),
            "the 24 MB fact2 build must trip the budget guard"
        );
        // The KAN-142 re-plan converts PER JOIN: sort-merge ONLY for the fact2 build,
        // the dim join keeps a hash join — one mixed plan, not an all-sort-merge stage.
        let df = engine.plan_spark(query).await.unwrap();
        let (_ctx, pj) = engine
            .per_join_strategy_physical_plan(df.logical_plan().clone())
            .await
            .unwrap();
        assert_eq!(
            sort_merge_join_count(pj.as_ref()),
            1,
            "only the over-budget fact2 build converts to sort-merge:\n{}",
            plan_display(pj.as_ref())
        );
        assert_eq!(
            hash_join_modes(pj.as_ref()).len(),
            1,
            "the measured-small dim build keeps a hash join:\n{}",
            plan_display(pj.as_ref())
        );
        assert!(
            !engine.needs_smj_reroute(pj.as_ref()),
            "no over-budget / un-estimable hash build may remain — the whole-plan \
             sort-merge fallback must not fire"
        );
        // The mixed plan returns the right rows: f ⋈ d is 1_000 rows (dim keys are a
        // unique subset), each joining exactly one fact2 row; every SUM is 0+…+999.
        let batches = engine
            .sql(query)
            .await
            .expect("the per-join mixed plan must complete under the bounded pool");
        use arrow::array::Int64Array;
        let col = |i: usize| {
            batches[0]
                .column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        assert_eq!(col(0), 1_000);
        assert_eq!(col(1), 999 * 1_000 / 2);
        assert_eq!(col(2), 999 * 1_000 / 2);
        assert_eq!(col(3), 999 * 1_000 / 2);
    }

    /// KAN-142 broadcast: a measured build ABOVE DataFusion's own `CollectLeft`
    /// admission (1 MiB / 128K rows — the stock pipeline keeps a partitioned hash join
    /// with both sides shuffled) but at or below the
    /// `OXIDANT_BROADCAST_JOIN_THRESHOLD_BYTES` cap converts to a broadcast hash join,
    /// eliding both sides' shuffle repartitions (Spark AQE's runtime broadcast
    /// conversion). Results are unchanged.
    #[tokio::test]
    async fn per_join_broadcast_converts_measured_small_build() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        std::env::remove_var("OXIDANT_BROADCAST_JOIN_THRESHOLD_BYTES");
        std::env::set_var("OXIDANT_TARGET_PARTITIONS", "2");
        let engine = Engine::new_with_memory_limit(256 * 1024 * 1024);
        std::env::remove_var("OXIDANT_TARGET_PARTITIONS");
        // dim: 300_000 rows ≈ 4.8 MB — over DataFusion's 1 MiB/128K-row CollectLeft
        // admission (stock stays partitioned), under the 10 MiB KAN-142 broadcast cap.
        const DIM: i64 = 300_000;
        engine
            .register_batches_with_stats(
                "fact",
                join_guard_kv_batches(2_000_000, 2_000_000),
                2_000_000,
            )
            .unwrap();
        engine
            .register_batches_with_stats("dim", join_guard_kv_batches(DIM, DIM), DIM as u64)
            .unwrap();
        let query = "SELECT COUNT(*) AS c FROM fact f JOIN dim d ON f.k = d.k";
        let plan = engine.physical_plan(query).await.unwrap();
        assert_eq!(
            hash_join_modes(plan.as_ref()),
            vec![datafusion::physical_plan::joins::PartitionMode::Partitioned],
            "DataFusion's own admission must keep the 4.8 MB build partitioned:\n{}",
            plan_display(plan.as_ref())
        );
        assert!(engine.plan_time_broadcast_upgrade(plan.as_ref()));
        assert!(
            !engine.plan_time_smj_reroute(plan.as_ref()),
            "both builds fit the 64 MiB budget — no sort-merge reroute"
        );
        let df = engine.plan_spark(query).await.unwrap();
        let (_ctx, pj) = engine
            .per_join_strategy_physical_plan(df.logical_plan().clone())
            .await
            .unwrap();
        assert_eq!(
            hash_join_modes(pj.as_ref()),
            vec![datafusion::physical_plan::joins::PartitionMode::CollectLeft],
            "the measured-small build converts to broadcast:\n{}",
            plan_display(pj.as_ref())
        );
        assert!(
            repartition_count(pj.as_ref()) < repartition_count(plan.as_ref()),
            "broadcast must elide shuffle repartitions ({} ≥ {}):\n{}",
            repartition_count(pj.as_ref()),
            repartition_count(plan.as_ref()),
            plan_display(pj.as_ref())
        );
        assert!(!engine.needs_smj_reroute(pj.as_ref()));
        // dim keys are a unique subset of fact keys → one match per dim row.
        let batches = engine
            .sql(query)
            .await
            .expect("the broadcast plan must complete");
        use arrow::array::Int64Array;
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(c, DIM);
    }

    /// KAN-146: broadcast admission must NOT trust an `Inexact` estimate. When both sides
    /// of a join are lower-join outputs, `PreferBoundedJoinBuildSide` has no provably
    /// bounded side to seat, and DataFusion's `Inexact(min(left, right))` join-output
    /// estimate makes a fact-wide intermediate look dimension-small (the KAN-2 Q62 wedge:
    /// `Inexact(min)` ≈ the dim for what is really the full fact-sized output). The old
    /// standard ([`hash_join_build_estimated_bytes`], `Precision::get_value` accepts
    /// `Inexact`) admitted that phantom-small build — coalescing an ever-wider
    /// intermediate to ONE partition and single-thread hash-building it on every
    /// consuming task. The provable-bound standard must refuse it.
    #[tokio::test]
    async fn per_join_broadcast_rejects_inexact_chain_build_estimate() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        std::env::remove_var("OXIDANT_BROADCAST_JOIN_THRESHOLD_BYTES");
        std::env::set_var("OXIDANT_TARGET_PARTITIONS", "2");
        let engine = Engine::new_with_memory_limit(256 * 1024 * 1024);
        std::env::remove_var("OXIDANT_TARGET_PARTITIONS");
        // f1/f2: 2M rows with keys inside the 300K-key dim domain (FK star shape: every
        // fact row matches exactly one dim row, so each lower-join output is really 2M
        // rows wide) — but DataFusion estimates each as `Inexact(min(2M, 300K)) = 300K`
        // and the top join's build as ~2.4 MB, UNDER the 10 MiB cap (the old admission
        // standard fires) and over DataFusion's own 1 MiB/128K-row admission (the stock
        // plan stays partitioned, so only KAN-142 could convert it).
        for name in ["f1", "f2"] {
            engine
                .register_batches_with_stats(
                    name,
                    join_guard_kv_batches(2_000_000, 300_000),
                    2_000_000,
                )
                .unwrap();
        }
        for name in ["d1", "d2"] {
            engine
                .register_batches_with_stats(name, join_guard_kv_batches(300_000, 300_000), 300_000)
                .unwrap();
        }
        let query = "SELECT COUNT(*) AS c \
                     FROM (SELECT f1.k AS k FROM f1 JOIN d1 ON f1.k = d1.k) a \
                     JOIN (SELECT f2.k AS k FROM f2 JOIN d2 ON f2.k = d2.k) b ON a.k = b.k";
        let plan = engine.physical_plan(query).await.unwrap();
        // Fixture check: some hash join carries a build whose estimate undershoots the cap
        // while lacking a provable row bound — exactly what the old standard admitted.
        fn phantom_build(plan: &dyn datafusion::physical_plan::ExecutionPlan) -> Option<usize> {
            if let Some(hj) = as_hash_join(plan) {
                if provable_row_bound(hj.left().as_ref()).is_none() {
                    if let Some(est) = hash_join_build_estimated_bytes(hj) {
                        if est <= 10 * 1024 * 1024 {
                            return Some(est);
                        }
                    }
                }
            }
            plan.children()
                .iter()
                .find_map(|c| phantom_build(c.as_ref()))
        }
        assert!(
            phantom_build(plan.as_ref()).is_some(),
            "the fixture must contain an Inexact phantom-small build (old standard would \
             admit):\n{}",
            plan_display(plan.as_ref())
        );
        // The honest dim builds ARE broadcast candidates (the guard fires for them)…
        assert!(
            engine.plan_time_broadcast_upgrade(plan.as_ref()),
            "the Exact dim builds below the cap must stay admissible"
        );
        // …but the per-join re-plan must convert ONLY those: no broadcast may seat a build
        // without a provable row bound (the phantom chain intermediate stays partitioned).
        let df = engine.plan_spark(query).await.unwrap();
        let (_ctx, pj) = engine
            .per_join_strategy_physical_plan(df.logical_plan().clone())
            .await
            .unwrap();
        fn unbounded_broadcast_build(plan: &dyn datafusion::physical_plan::ExecutionPlan) -> bool {
            if let Some(hj) = as_hash_join(plan) {
                if matches!(
                    hj.partition_mode(),
                    datafusion::physical_plan::joins::PartitionMode::CollectLeft
                ) && provable_row_bound(hj.left().as_ref()).is_none()
                {
                    return true;
                }
            }
            plan.children()
                .iter()
                .any(|c| unbounded_broadcast_build(c.as_ref()))
        }
        assert!(
            !unbounded_broadcast_build(pj.as_ref()),
            "no CollectLeft build may lack a provable row bound:\n{}",
            plan_display(pj.as_ref())
        );
        assert!(
            hash_join_modes(pj.as_ref())
                .contains(&datafusion::physical_plan::joins::PartitionMode::CollectLeft),
            "the honest dim builds must still convert to broadcast:\n{}",
            plan_display(pj.as_ref())
        );
        // The query itself still completes: a and b are each 2M rows wide; per key the
        // counts multiply (200K keys × 7×7 + 100K keys × 6×6).
        let batches = engine.sql(query).await.expect("chain-of-chains join");
        use arrow::array::Int64Array;
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(c, 200_000 * 7 * 7 + 100_000 * 6 * 6);
    }

    /// KAN-142 broadcast admission is clamped by the KAN-25 budget, disabled by
    /// `OXIDANT_BROADCAST_JOIN_THRESHOLD_BYTES=0`, and off under a forced
    /// `OXIDANT_PREFER_HASH_JOIN` (KAN-45 semantics: forced sessions never re-plan) — a
    /// broadcast conversion must never admit a hash build the budget guard would have
    /// rerouted.
    #[tokio::test]
    async fn per_join_broadcast_admission_cap_respects_env_and_budget() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        std::env::remove_var("OXIDANT_BROADCAST_JOIN_THRESHOLD_BYTES");
        let engine = Engine::new_with_memory_limit(256 * 1024 * 1024);
        engine
            .register_batches_with_stats(
                "fact",
                join_guard_kv_batches(2_000_000, 2_000_000),
                2_000_000,
            )
            .unwrap();
        // dim: 600_000 rows ≈ 4.8 MB estimated build (COUNT(*) projects the scan to the
        // 8-byte join key) — under the default 10 MiB broadcast cap, over the 4 MiB cap
        // of a 16 MiB pool.
        engine
            .register_batches_with_stats("dim", join_guard_kv_batches(600_000, 600_000), 600_000)
            .unwrap();
        let query = "SELECT COUNT(*) AS c FROM fact f JOIN dim d ON f.k = d.k";
        let plan = engine.physical_plan(query).await.unwrap();
        // Default: cap = min(10 MiB threshold, 64 MiB budget) = 10 MiB → 4.8 MB build is
        // a broadcast candidate.
        assert_eq!(engine.broadcast_admission_cap(), Some(10 * 1024 * 1024));
        assert!(engine.plan_time_broadcast_upgrade(plan.as_ref()));
        // `=0` disables broadcast conversion entirely.
        std::env::set_var("OXIDANT_BROADCAST_JOIN_THRESHOLD_BYTES", "0");
        assert_eq!(engine.broadcast_admission_cap(), None);
        assert!(!engine.plan_time_broadcast_upgrade(plan.as_ref()));
        std::env::remove_var("OXIDANT_BROADCAST_JOIN_THRESHOLD_BYTES");
        // The cap clamps to the build budget: with a 16 MiB pool the budget is 4 MiB, so
        // the 4.8 MB build is NOT broadcast — it keeps the partitioned hash join the
        // budget guard admits.
        let tight = Engine::new_with_memory_limit(16 * 1024 * 1024);
        assert_eq!(
            tight.broadcast_admission_cap(),
            Some(4 * 1024 * 1024),
            "cap must clamp to the 16 MiB pool × 0.25 budget"
        );
        assert!(!tight.plan_time_broadcast_upgrade(plan.as_ref()));
        // A forced strategy never re-plans (KAN-45), so broadcast stays off.
        std::env::set_var("OXIDANT_PREFER_HASH_JOIN", "true");
        assert_eq!(engine.broadcast_admission_cap(), None);
        std::env::set_var("OXIDANT_PREFER_HASH_JOIN", "false");
        assert_eq!(engine.broadcast_admission_cap(), None);
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        // No bounded pool ⇒ no budget to clamp to ⇒ no broadcast conversion.
        std::env::set_var("OXIDANT_MEMORY_LIMIT_BYTES", "0");
        let unbounded = Engine::new();
        std::env::remove_var("OXIDANT_MEMORY_LIMIT_BYTES");
        assert_eq!(unbounded.broadcast_admission_cap(), None);
    }

    /// KAN-142 keeps the "unknown ⇒ safe" policy PER JOIN: a build side with no usable
    /// statistics converts to sort-merge in the per-join re-plan (where it used to drag
    /// the whole stage along), the converted plan passes the fallback safety check, and
    /// the query completes under the bounded pool.
    #[tokio::test]
    async fn per_join_unknown_build_converts_to_sort_merge() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        std::env::remove_var("OXIDANT_BROADCAST_JOIN_THRESHOLD_BYTES");
        std::env::set_var("OXIDANT_TARGET_PARTITIONS", "2");
        let engine = Engine::new_with_memory_limit(64 * 1024 * 1024);
        std::env::remove_var("OXIDANT_TARGET_PARTITIONS");
        const ROWS: i64 = 1_500_000;
        register_unknown_stats_table(&engine, "m", join_guard_kv_batches(ROWS, ROWS));
        register_unknown_stats_table(&engine, "m2", join_guard_kv_batches(ROWS, ROWS));
        let query = "SELECT COUNT(*) AS c FROM m JOIN m2 ON m.k = m2.k";
        let plan = engine.physical_plan(query).await.unwrap();
        assert!(
            engine.plan_time_smj_reroute(plan.as_ref()),
            "an un-estimable build must still trip the guard (unknown ⇒ safe)"
        );
        let df = engine.plan_spark(query).await.unwrap();
        let (_ctx, pj) = engine
            .per_join_strategy_physical_plan(df.logical_plan().clone())
            .await
            .unwrap();
        assert_eq!(
            sort_merge_join_count(pj.as_ref()),
            1,
            "the unknown build converts to sort-merge:\n{}",
            plan_display(pj.as_ref())
        );
        assert!(
            !engine.needs_smj_reroute(pj.as_ref()),
            "no un-estimable hash build may remain after the conversion"
        );
        let batches = engine
            .sql(query)
            .await
            .expect("unknown-build join completes via the per-join sort-merge conversion");
        use arrow::array::Int64Array;
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(c, ROWS);
    }

    /// (k, v) batches with a NULLABLE key column: every `null_every`-th key is NULL
    /// (`null_every <= 0` ⇒ no NULLs) — the `NOT IN` null-aware anti-join fixture.
    fn nullable_kv_batches(rows: i64, key_mod: i64, null_every: i64) -> Vec<RecordBatch> {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, false),
        ]));
        let per = rows / 4;
        (0..4)
            .map(|p| {
                let start = p * per;
                let ks: Vec<Option<i64>> = (start..start + per)
                    .map(|i| (null_every <= 0 || i % null_every != 0).then_some(i % key_mod))
                    .collect();
                let vs: Vec<i64> = (start..start + per).collect();
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(ks)),
                        Arc::new(Int64Array::from(vs)),
                    ],
                )
                .unwrap()
            })
            .collect()
    }

    /// Whether any hash join in `plan` is NULL-AWARE (KAN-142 null-aware guard assertion).
    fn contains_null_aware_hash_join(plan: &dyn datafusion::physical_plan::ExecutionPlan) -> bool {
        if let Some(hj) = as_hash_join(plan) {
            if hj.null_aware {
                return true;
            }
        }
        plan.children()
            .iter()
            .any(|c| contains_null_aware_hash_join(c.as_ref()))
    }

    /// KAN-142 review regression: the per-join rule must NEVER seat a NULL-AWARE anti
    /// join (`NOT IN` with nullable keys) as sort-merge — `SortMergeJoinExec` has no
    /// null-aware support, so converting would drop NOT-IN NULL semantics (a NULL in the
    /// subquery must empty the result). The fixture drives the per-join path with a
    /// broadcast-only trigger (a partitioned INNER join over a measured-small dim), so
    /// the null-aware anti join rides the same re-plan; it must come out the other side
    /// still a null-aware HASH join, with no sort-merge join in the plan — and the query
    /// must return null-correct counts.
    #[tokio::test]
    async fn per_join_never_converts_null_aware_anti_join_to_sort_merge() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        std::env::remove_var("OXIDANT_BROADCAST_JOIN_THRESHOLD_BYTES");
        std::env::set_var("OXIDANT_TARGET_PARTITIONS", "2");
        let engine = Engine::new_with_memory_limit(64 * 1024 * 1024);
        std::env::remove_var("OXIDANT_TARGET_PARTITIONS");
        // fact: 2M rows, nullable k = i with every 4th NULL. dim: 200_000 rows ≈ 1.6 MB
        // k-only build — over DataFusion's own CollectLeft admission (partitioned in the
        // stock plan), under the 10 MiB KAN-142 broadcast cap.
        const FACT: i64 = 2_000_000;
        engine
            .register_batches_with_stats("fact", nullable_kv_batches(FACT, FACT, 4), FACT as u64)
            .unwrap();
        engine
            .register_batches_with_stats("dim", join_guard_kv_batches(200_000, 200_000), 200_000)
            .unwrap();
        engine
            .register_batches_with_stats("blocklist", nullable_kv_batches(1_000, 1_000, 0), 1_000)
            .unwrap();
        let query = "SELECT COUNT(*) AS c FROM fact f JOIN dim d ON f.k = d.k \
                     WHERE f.k NOT IN (SELECT k FROM blocklist)";
        let plan = engine.physical_plan(query).await.unwrap();
        assert!(
            contains_null_aware_hash_join(plan.as_ref()),
            "the NOT IN over a nullable subquery key must plan as a null-aware anti join:\n{}",
            plan_display(plan.as_ref())
        );
        assert!(
            !engine.plan_time_smj_reroute(plan.as_ref()),
            "every build is positively sized and under budget — the trigger is broadcast-only"
        );
        assert!(engine.plan_time_broadcast_upgrade(plan.as_ref()));
        // The per-join re-plan must leave the null-aware anti join a HASH join (the SMJ
        // branch's !null_aware guard) while converting the dim join.
        let df = engine.plan_spark(query).await.unwrap();
        let (_ctx, pj) = engine
            .per_join_strategy_physical_plan(df.logical_plan().clone())
            .await
            .unwrap();
        assert_eq!(
            sort_merge_join_count(pj.as_ref()),
            0,
            "the null-aware anti join must never convert to sort-merge:\n{}",
            plan_display(pj.as_ref())
        );
        assert!(
            contains_null_aware_hash_join(pj.as_ref()),
            "the null-aware anti join must survive the per-join re-plan as a hash join:\n{}",
            plan_display(pj.as_ref())
        );
        assert!(!engine.needs_smj_reroute(pj.as_ref()));
        // Null-correct results through the per-join path: blocklist = [0, 1000) without
        // NULLs → non-null fact keys in [1000, 200000) qualify: 199_000 values less the
        // 49_750 multiples of 4 (NULL keys).
        let engine_ref = &engine;
        let count = |q: &'static str| async move {
            use arrow::array::Int64Array;
            engine_ref.sql(q).await.unwrap()[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        assert_eq!(count(query).await, 149_250);
        // A NULL in the subquery must EMPTY the result (null-aware semantics) — a
        // wrongly non-null-aware conversion would return 149_250 here instead.
        engine
            .register_batches_with_stats("blocklist", nullable_kv_batches(1_000, 1_000, 2), 1_000)
            .unwrap();
        assert_eq!(count(query).await, 0);
    }

    /// KAN-142 review regression, guard-pinning shape: a null-aware anti join whose build
    /// side is UN-ESTIMABLE is exactly the join the SMJ branch would convert without the
    /// `!null_aware` guard — the per-join re-plan must keep it a hash join, so the
    /// converted plan still trips [`Engine::needs_smj_reroute`] and the query takes the
    /// (documented, pre-existing) whole-plan fallback instead of a per-join plan with
    /// null semantics silently dropped.
    #[tokio::test]
    async fn per_join_null_aware_unknown_build_stays_hash_for_fallback() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        std::env::remove_var("OXIDANT_BROADCAST_JOIN_THRESHOLD_BYTES");
        std::env::set_var("OXIDANT_TARGET_PARTITIONS", "2");
        let engine = Engine::new_with_memory_limit(64 * 1024 * 1024);
        std::env::remove_var("OXIDANT_TARGET_PARTITIONS");
        register_unknown_stats_table(&engine, "mystery", nullable_kv_batches(4_000, 4_000, 4));
        engine
            .register_batches_with_stats("blocklist", nullable_kv_batches(1_000, 1_000, 0), 1_000)
            .unwrap();
        let query = "SELECT COUNT(*) AS c FROM mystery m \
                     WHERE m.k NOT IN (SELECT k FROM blocklist)";
        let plan = engine.physical_plan(query).await.unwrap();
        assert!(
            contains_null_aware_hash_join(plan.as_ref()),
            "the NOT IN over nullable keys must plan as a null-aware anti join:\n{}",
            plan_display(plan.as_ref())
        );
        assert!(
            engine.plan_time_smj_reroute(plan.as_ref()),
            "the un-estimable null-aware build must trip the guard (unknown ⇒ safe)"
        );
        let df = engine.plan_spark(query).await.unwrap();
        let (_ctx, pj) = engine
            .per_join_strategy_physical_plan(df.logical_plan().clone())
            .await
            .unwrap();
        assert_eq!(
            sort_merge_join_count(pj.as_ref()),
            0,
            "the guard must keep the null-aware anti join a hash join even when its \
             build is un-estimable:\n{}",
            plan_display(pj.as_ref())
        );
        assert!(contains_null_aware_hash_join(pj.as_ref()));
        assert!(
            engine.needs_smj_reroute(pj.as_ref()),
            "the unconvertible null-aware build must still trip the post-conversion \
             safety check, engaging the whole-plan fallback"
        );
    }

    #[tokio::test]
    async fn join_guard_runtime_retry_when_estimate_underreports() {
        // Wide strings on both sides; the aggregates over BOTH string columns keep the
        // build side (the smaller input, ~70 MB actual) from being pruned to keys — a
        // keys-only build is the shape DataFusion already handles well. The flat-width
        // estimate (170k × 56 B ≈ 9.5 MB) stays under the 16 MiB budget, so the plan-time
        // guard stays out — but the actual build blows the 64 MiB pool at runtime: the
        // first attempt must fail with `Resources Exhausted` and the guard must retry once
        // with sort-merge joins, completing instead of wedging. The env knobs keep one
        // coalesced/repartitioned batch (~0.4 MB) far below a sorter's fair share of the
        // pool (~10 MiB) — at production pool sizes this headroom exists at any partition
        // count; the window is tight so parallel tests keep their own defaults.
        // KAN-45: the sort-merge fallback is opt-in — enable it explicitly for this test.
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::set_var("OXIDANT_SORT_MERGE_FALLBACK", "true");
        std::env::set_var("OXIDANT_TARGET_PARTITIONS", "2");
        std::env::set_var("OXIDANT_BATCH_SIZE", "1024");
        let engine = Engine::new_with_memory_limit(64 * 1024 * 1024);
        std::env::remove_var("OXIDANT_TARGET_PARTITIONS");
        std::env::remove_var("OXIDANT_BATCH_SIZE");
        const LEFT: i64 = 170_000;
        const RIGHT: i64 = 340_000;
        engine
            .register_batches("left_wide", join_guard_wide_batches(LEFT, LEFT, 400))
            .unwrap();
        engine
            .register_batches("right_wide", join_guard_wide_batches(RIGHT, RIGHT, 400))
            .unwrap();
        let query = "SELECT COUNT(*) AS c, SUM(length(l.s)) AS sl, SUM(length(r.s)) AS sr \
             FROM left_wide l JOIN right_wide r ON l.k = r.k";
        let plan = engine.physical_plan(query).await.unwrap();
        let budget = engine.hash_join_build_budget().unwrap();
        assert!(
            !hash_join_build_exceeds(plan.as_ref(), budget),
            "the flat-width estimate must NOT trip the plan-time guard (runtime retry path)"
        );
        // Non-vacuity: the string column must sit on the build side...
        fn build_schema_has(p: &dyn datafusion::physical_plan::ExecutionPlan, col: &str) -> bool {
            if let Some(hj) = as_hash_join(p) {
                if hj.left().schema().field_with_name(col).is_ok() {
                    return true;
                }
            }
            p.children()
                .iter()
                .any(|c| build_schema_has(c.as_ref(), col))
        }
        assert!(build_schema_has(plan.as_ref(), "s"));
        // ...and the SAME query against a plain DataFusion session on the same 32 MiB pool
        // (no oxidant guard) must fail with `Resources Exhausted`.
        {
            use datafusion::execution::memory_pool::FairSpillPool;
            use datafusion::execution::runtime_env::RuntimeEnvBuilder;
            let env = RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::new(FairSpillPool::new(64 * 1024 * 1024)))
                .build_arc()
                .unwrap();
            let raw = SessionContext::new_with_config_rt(Default::default(), env);
            raw.register_table(
                "left_wide",
                Arc::new(
                    datafusion::datasource::MemTable::try_new(
                        join_guard_wide_batches(LEFT, LEFT, 400)[0].schema(),
                        vec![join_guard_wide_batches(LEFT, LEFT, 400)],
                    )
                    .unwrap(),
                ),
            )
            .unwrap();
            raw.register_table(
                "right_wide",
                Arc::new(
                    datafusion::datasource::MemTable::try_new(
                        join_guard_wide_batches(RIGHT, RIGHT, 400)[0].schema(),
                        vec![join_guard_wide_batches(RIGHT, RIGHT, 400)],
                    )
                    .unwrap(),
                ),
            )
            .unwrap();
            let err = raw
                .sql(query)
                .await
                .unwrap()
                .collect()
                .await
                .expect_err("unguarded hash join must exhaust the pool");
            assert!(
                err.to_string()
                    .to_ascii_lowercase()
                    .contains("resources exhausted"),
                "expected pool exhaustion, got: {err}"
            );
        }
        let batches = engine
            .sql(query)
            .await
            .expect("pool-exhausted hash join must be retried as sort-merge");
        use arrow::array::Int64Array;
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let sl = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let sr = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(c, LEFT);
        assert_eq!(sl, LEFT * 400);
        assert_eq!(sr, LEFT * 400);
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
    }

    // ---- KAN-2 R2: hash-join dynamic filters on the worker star shape --------
    //
    // DataFusion 54.1.0's dynamic-filter pipeline: a hash join's build side publishes
    // a runtime `DynamicFilterPhysicalExpr` over the probe-side join keys (min/max
    // bounds + membership), pushed toward the probe-side scan in the Post
    // filter-pushdown phase (`optimizer.enable_join_dynamic_filter_pushdown`, default
    // true, gated on the probe side being the join's preserved side). The parquet
    // `DataSourceExec` absorbs it into its scan predicate, where `PruningPredicate`
    // snapshots it for row-group statistics / page-index / bloom pruning — the
    // star-shape fast path (sharded parquet fact ⋈ replicated MemTable dims, inner
    // equijoins). These tests prove the pipeline actually fires on that shape through
    // the engine's own `create_physical_plan` path, and pin the correctness guard (no
    // filter may attach when the fact side is the join's preserved side).

    /// Serializes the dynamic-filter tests that mutate `OXIDANT_DYN_FILTER_*`
    /// (process-global env read at `Engine` construction).
    static DYN_FILTER_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// `(k, v)` Int64 single-file parquet dir, keys `0..rows` ASCENDING in
    /// `row_group_rows`-row row groups — a clustered/sorted join key so min/max bounds
    /// pruning has disjoint row-group ranges to eliminate. `v` duplicates `k`. (parquet
    /// 58's `ArrowWriter` buffers batches into one big row group by default, so the max
    /// row-group size must be pinned to get multiple groups.)
    fn write_sorted_kv_parquet_dir(rows: i64, row_group_rows: i64) -> std::path::PathBuf {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::parquet::file::properties::WriterProperties;
        let dir = std::env::temp_dir().join(format!(
            "oxidant-dyn-filter-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(row_group_rows as usize))
            .build();
        let f = std::fs::File::create(dir.join("part-0.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema.clone(), Some(props)).unwrap();
        let mut start = 0;
        while start < rows {
            let end = (start + row_group_rows).min(rows);
            let idx: Vec<i64> = (start..end).collect();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(idx.clone())),
                    Arc::new(Int64Array::from(idx)),
                ],
            )
            .unwrap();
            w.write(&batch).unwrap();
            start = end;
        }
        w.close().unwrap();
        dir
    }

    /// Register a single-batch MemTable dim of `(k, v)` Int64 rows with keys
    /// `start..start+rows` — the replicated-dimension side of the star shape.
    fn register_kv_dim(engine: &Engine, name: &str, start: i64, rows: i64) {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let idx: Vec<i64> = (start..start + rows).collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(idx.clone())),
                Arc::new(Int64Array::from(idx)),
            ],
        )
        .unwrap();
        engine.register_batches(name, vec![batch]).unwrap();
    }

    /// Render `plan`'s tree text (the `EXPLAIN` shape) for plan-structure assertions.
    fn plan_tree_text(plan: &dyn datafusion::physical_plan::ExecutionPlan) -> String {
        datafusion::physical_plan::displayable(plan)
            .indent(false)
            .to_string()
    }

    /// Whether any `DataSourceExec` line of the plan text carries a `DynamicFilter` in
    /// its scan predicate — the observable proof that a join's dynamic filter was
    /// pushed all the way into a (parquet) scan.
    fn scan_predicate_has_dynamic_filter(
        plan: &dyn datafusion::physical_plan::ExecutionPlan,
    ) -> bool {
        plan_tree_text(plan)
            .lines()
            .any(|l| l.contains("DataSourceExec") && l.contains("DynamicFilter"))
    }

    /// Sum a parquet pruning metric (`row_groups_pruned_statistics`, ...) across the
    /// whole executed plan tree as `(pruned, matched)`.
    fn pruning_metric_sums(
        plan: &dyn datafusion::physical_plan::ExecutionPlan,
        metric_name: &str,
    ) -> (usize, usize) {
        use datafusion::physical_plan::metrics::MetricValue;
        fn visit(
            plan: &dyn datafusion::physical_plan::ExecutionPlan,
            metric_name: &str,
            acc: &mut (usize, usize),
        ) {
            if let Some(set) = plan.metrics() {
                for metric in set.iter() {
                    if let MetricValue::PruningMetrics {
                        name,
                        pruning_metrics,
                    } = metric.value()
                    {
                        if name.as_ref() == metric_name {
                            acc.0 += pruning_metrics.pruned();
                            acc.1 += pruning_metrics.matched();
                        }
                    }
                }
            }
            for child in plan.children() {
                visit(child.as_ref(), metric_name, acc);
            }
        }
        let mut acc = (0, 0);
        visit(plan, metric_name, &mut acc);
        acc
    }

    /// The single Int64 value of a one-row aggregate result.
    fn int64_scalar(batches: &[RecordBatch]) -> i64 {
        use arrow::array::Int64Array;
        batches
            .iter()
            .map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0)
            })
            .sum()
    }

    /// R2 Phase 0: the dynamic-filter pipeline FIRES on the worker star shape — a
    /// sharded parquet fact (clustered join key) inner-joined to a replicated
    /// MemTable dim. (a) The plan carries a `DynamicFilter` inside the fact scan's
    /// predicate; (b) after execution the scan's row-group statistics pruning has
    /// eliminated the row groups outside the dim's key range (the fact's key is
    /// sorted, so the filter's min/max bounds disjoint-prune 9 of 10 row groups);
    /// and the result is exact.
    #[tokio::test]
    async fn dynamic_filter_fires_and_prunes_row_groups_on_star_join() {
        let _env = DYN_FILTER_ENV_LOCK.lock().await;
        let engine = Engine::new();
        // 100k fact rows in 10 disjoint 10k-key row groups; the dim covers exactly
        // the 50_000..60_000 group.
        let fact = write_sorted_kv_parquet_dir(100_000, 10_000);
        register_catalog_parquet(&engine, "fact", &fact).await;
        register_kv_dim(&engine, "dim", 50_000, 10_000);
        let query = "SELECT COUNT(*) AS c FROM fact f JOIN dim d ON f.k = d.k";
        let plan = engine.physical_plan(query).await.unwrap();
        // (a) plan structure: a hash join whose dynamic filter reaches the scan.
        let text = plan_tree_text(plan.as_ref());
        assert!(
            contains_hash_join(plan.as_ref()),
            "the star shape must plan a hash join, got:\n{text}"
        );
        assert!(
            scan_predicate_has_dynamic_filter(plan.as_ref()),
            "the join's dynamic filter must reach the fact scan's predicate, got:\n{text}"
        );
        // (b) execution: exact result + row-group pruning from the filter's bounds.
        let batches = engine.execute_plan(plan.clone()).await.unwrap();
        assert_eq!(int64_scalar(&batches), 10_000);
        let (pruned, matched) = pruning_metric_sums(plan.as_ref(), "row_groups_pruned_statistics");
        assert!(
            pruned > 0,
            "dynamic-filter bounds must prune fact row groups \
             (pruned={pruned}, matched={matched}), plan:\n{}",
            plan_tree_text(plan.as_ref())
        );
        assert!(
            matched > 0,
            "the dim's own row group must survive pruning (pruned={pruned})"
        );
        let _ = std::fs::remove_dir_all(&fact);
    }

    /// R2 Phase 0 correctness guard: when the FACT side is the join's preserved side
    /// (`fact LEFT JOIN dim` — every fact row must appear, matched or not), no dynamic
    /// filter may attach to the fact scan: filtering the non-preserved side would drop
    /// rows the join must emit with NULLs. (JoinSelection swaps this shape to
    /// `dim RIGHT JOIN fact`, whose probe side — the fact — is exactly the side
    /// `JoinType::Right` does NOT preserve, so `allow_join_dynamic_filter_pushdown`
    /// gates the filter off.) The exact count is the second half of the guard: a
    /// wrongly-attached filter would drop the 90k unmatched fact rows.
    #[tokio::test]
    async fn dynamic_filter_absent_when_fact_side_is_preserved() {
        let _env = DYN_FILTER_ENV_LOCK.lock().await;
        let engine = Engine::new();
        let fact = write_sorted_kv_parquet_dir(100_000, 10_000);
        register_catalog_parquet(&engine, "fact", &fact).await;
        register_kv_dim(&engine, "dim", 50_000, 10_000);
        let query = "SELECT COUNT(*) AS c FROM fact f LEFT JOIN dim d ON f.k = d.k";
        let plan = engine.physical_plan(query).await.unwrap();
        let text = plan_tree_text(plan.as_ref());
        assert!(
            !scan_predicate_has_dynamic_filter(plan.as_ref()),
            "no dynamic filter may attach when the fact side is preserved, got:\n{text}"
        );
        let batches = engine.execute_plan(plan.clone()).await.unwrap();
        assert_eq!(
            int64_scalar(&batches),
            100_000,
            "LEFT JOIN must emit every fact row exactly once"
        );
        let _ = std::fs::remove_dir_all(&fact);
    }

    /// R2 Phase 1: the dynamic-filter config is pinned and the knobs map to the
    /// session options — pushdown pinned ON (deliberate; DataFusion's own default is
    /// also `true` today), and the IN-list caps default to STOCK DataFusion values
    /// (150 distinct / 128 KiB per build partition; a raised 100k/32 MiB default was
    /// measured 3–6x slower on TPC-DS Q4/Q11/Q18/Q21 at SF10) with env overriding.
    #[tokio::test]
    async fn dyn_filter_knobs_map_to_session_config() {
        /// `(pushdown pinned, max distinct, max bytes)` from the engine's live session
        /// config (values copied out before the temporary `SessionState` drops).
        fn dyn_filter_config(engine: &Engine) -> (bool, usize, usize) {
            let state = engine.ctx.state();
            let o = &state.config().options().optimizer;
            (
                o.enable_join_dynamic_filter_pushdown,
                o.hash_join_inlist_pushdown_max_distinct_values,
                o.hash_join_inlist_pushdown_max_size,
            )
        }
        let _env = DYN_FILTER_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_DYN_FILTER_INLIST_MAX_DISTINCT");
        std::env::remove_var("OXIDANT_DYN_FILTER_INLIST_MAX_BYTES");
        let engine = Engine::new();
        assert_eq!(
            dyn_filter_config(&engine),
            (true, 150, 128 * 1024),
            "pushdown pinned on + stock IN-list caps by default (raised caps regressed SF10)"
        );
        std::env::set_var("OXIDANT_DYN_FILTER_INLIST_MAX_DISTINCT", "12345");
        std::env::set_var("OXIDANT_DYN_FILTER_INLIST_MAX_BYTES", "65536");
        let engine = Engine::new();
        std::env::remove_var("OXIDANT_DYN_FILTER_INLIST_MAX_DISTINCT");
        std::env::remove_var("OXIDANT_DYN_FILTER_INLIST_MAX_BYTES");
        assert_eq!(
            dyn_filter_config(&engine),
            (true, 12_345, 65_536),
            "env vars must override the stock defaults"
        );
    }

    /// R2 Phase 1: the IN-list caps switch the membership strategy. A 10k-distinct-key
    /// dim (10k × 8 B = 80 KB of keys — under either byte cap) exceeds the stock
    /// 150-distinct cap, so the default session degrades to the opaque `hash_lookup`
    /// membership (decoded-batch filtering only; min/max bounds still prune); raising
    /// the cap via env keeps it on the transparent `IN (SET)` strategy the scan can
    /// also prune statistics/bloom filters with. The strategy is observable in the
    /// plan text AFTER execution, once the build side has published the real filter
    /// into the placeholder `DynamicFilter`.
    #[tokio::test]
    async fn dyn_filter_inlist_knobs_switch_membership_strategy() {
        let _env = DYN_FILTER_ENV_LOCK.lock().await;
        let fact = write_sorted_kv_parquet_dir(100_000, 10_000);
        let query = "SELECT COUNT(*) AS c FROM fact f JOIN dim d ON f.k = d.k";

        // oxidant defaults (env unset): stock 150-distinct cap → opaque hash-table
        // lookup for a 10k-value dim (results stay exact either way).
        std::env::remove_var("OXIDANT_DYN_FILTER_INLIST_MAX_DISTINCT");
        std::env::remove_var("OXIDANT_DYN_FILTER_INLIST_MAX_BYTES");
        let engine = Engine::new();
        register_catalog_parquet(&engine, "fact", &fact).await;
        register_kv_dim(&engine, "dim", 0, 10_000);
        let plan = engine.physical_plan(query).await.unwrap();
        let batches = engine.execute_plan(plan.clone()).await.unwrap();
        assert_eq!(int64_scalar(&batches), 10_000);
        let text = plan_tree_text(plan.as_ref());
        assert!(
            text.contains("hash_lookup"),
            "stock caps must degrade to the opaque lookup, got:\n{text}"
        );

        // Raised distinct cap via env: the same 10k-value dim uses the transparent
        // IN-list strategy.
        std::env::set_var("OXIDANT_DYN_FILTER_INLIST_MAX_DISTINCT", "100000");
        let engine = Engine::new();
        std::env::remove_var("OXIDANT_DYN_FILTER_INLIST_MAX_DISTINCT");
        register_catalog_parquet(&engine, "fact", &fact).await;
        register_kv_dim(&engine, "dim", 0, 10_000);
        let plan = engine.physical_plan(query).await.unwrap();
        let batches = engine.execute_plan(plan.clone()).await.unwrap();
        assert_eq!(int64_scalar(&batches), 10_000);
        let text = plan_tree_text(plan.as_ref());
        assert!(
            text.contains(" IN (SET) ("),
            "raised cap must use the transparent IN-list strategy, got:\n{text}"
        );
        let _ = std::fs::remove_dir_all(&fact);
    }

    /// R2 SMJ interaction (measurement only — KAN-53 behavior deliberately
    /// unchanged): with a bounded pool, the KAN-53 `auto` guard reroutes the star
    /// join to sort-merge once the dim's estimated key-only build side (rows × 8 B)
    /// exceeds `OXIDANT_HASH_JOIN_MAX_BUILD_FRACTION` of the pool — and sort-merge joins
    /// carry NO dynamic filters, so the reroute forfeits fact-scan row-group pruning
    /// along with the hash join. Below the budget the hash plan and its dynamic
    /// filter survive. 64 MiB pool × 0.25 = 16 MiB budget ⇒ the crossover sits at
    /// ~2M dim rows (at production pool sizes, e.g. 26 GiB × 0.25 ≈ 6.5 GiB, only
    /// ~800M-row dims reroute — TPC-DS's 2M-row customer is far inside the budget).
    /// The 4M-row fact keeps the dim the SMALLER side at every measured size, so the
    /// dim stays the hash build and the dynamic filter points at the fact scan
    /// (JoinSelection builds the smaller side; a dim bigger than the fact flips the
    /// filter onto the dim's own MemTable scan, which cannot absorb it).
    #[tokio::test]
    async fn bounded_pool_smj_reroute_drops_dynamic_filters() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        // Sorting is irrelevant to the plan-shape measurement — the plain 4-file kv
        // helper (unique keys) is enough, and catalog-parquet registration gives the
        // guard real footer row counts.
        let fact = write_kv_parquet_dir(4_000_000);
        let query = "SELECT COUNT(*) AS c FROM fact f JOIN dim d ON f.k = d.k";

        // Under budget: a 1M-row dim ≈ 8 MB key-only build < 16 MiB ⇒ hash join
        // kept, dynamic filter intact.
        let engine = Engine::new_with_memory_limit(64 * 1024 * 1024);
        register_catalog_parquet(&engine, "fact", &fact).await;
        register_kv_dim(&engine, "dim", 0, 1_000_000);
        let plan = engine.physical_plan(query).await.unwrap();
        assert!(
            !engine.plan_time_smj_reroute(plan.as_ref()),
            "an 8 MB build under the 16 MiB budget must keep the hash join"
        );
        assert!(
            scan_predicate_has_dynamic_filter(plan.as_ref()),
            "the kept hash join must carry its dynamic filter into the fact scan, got:\n{}",
            plan_tree_text(plan.as_ref())
        );

        // Over budget: a 3M-row dim ≈ 24 MB > 16 MiB ⇒ reroute ⇒ no dynamic filter.
        let engine = Engine::new_with_memory_limit(64 * 1024 * 1024);
        register_catalog_parquet(&engine, "fact", &fact).await;
        register_kv_dim(&engine, "dim", 0, 3_000_000);
        let plan = engine.physical_plan(query).await.unwrap();
        assert!(
            engine.plan_time_smj_reroute(plan.as_ref()),
            "a 24 MB build over the 16 MiB budget must reroute to sort-merge"
        );
        let logical = engine.logical_plan(query).await.unwrap();
        let (_ctx, smj) = engine.sort_merge_physical_plan(logical).await.unwrap();
        let text = plan_tree_text(smj.as_ref());
        assert!(
            text.contains("SortMergeJoin"),
            "the reroute must produce a sort-merge plan, got:\n{text}"
        );
        assert!(
            !scan_predicate_has_dynamic_filter(smj.as_ref()),
            "sort-merge joins carry no dynamic filters — the reroute forfeits fact \
             pruning, got:\n{text}"
        );
        let _ = std::fs::remove_dir_all(&fact);
    }

    /// KAN-150: the distributed semi-join leaf shape — a fact leaf stage whose SQL carries
    /// an `IN (SELECT … FROM dim WHERE …)` filter injected by the stage planner (see
    /// `oxidant_execution::plan::join_chain`) — re-planned on the worker must turn the
    /// subquery into a hash SEMI join building on the DIM (never the fact) and push its
    /// dynamic filter into the fact scan's predicate for row-group pruning, with exact
    /// results. This is the worker-side half of the cross-stage runtime filter: the filter
    /// crosses the shuffle boundary as SQL, and DataFusion's own machinery re-materializes
    /// it against the probe-side scan.
    #[tokio::test]
    async fn semi_join_leaf_sql_pushes_dynamic_filter_into_fact_scan() {
        let _env = DYN_FILTER_ENV_LOCK.lock().await;
        let engine = Engine::new();
        // 100k fact rows in 10 disjoint 10k-key row groups; the dim's filtered key set
        // covers exactly the 50_000..60_000 group (the `v >= 0` conjunct keeps the dim
        // scan *filtered* — the shape the stage planner injects).
        let fact = write_sorted_kv_parquet_dir(100_000, 10_000);
        register_catalog_parquet(&engine, "fact", &fact).await;
        register_kv_dim(&engine, "dim", 50_000, 10_000);
        // Emitted-leaf shape: flat projection + IN-subquery semi filter.
        let query = "SELECT fact.k AS fact__k, fact.v AS fact__v FROM fact \
                     WHERE fact.k IN (SELECT dim.k FROM dim WHERE dim.v >= 0)";
        let plan = engine.physical_plan(query).await.unwrap();
        let text = plan_tree_text(plan.as_ref());
        assert!(
            scan_predicate_has_dynamic_filter(plan.as_ref()),
            "the semi join's dynamic filter must reach the fact scan's predicate, got:\n{text}"
        );
        // Execution: exactly the 10k overlapping rows survive, and the filter's bounds
        // prune the fact row groups outside the dim's key range.
        let batches = engine.execute_plan(plan.clone()).await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            rows, 10_000,
            "the semi filter must keep exactly the overlap"
        );
        let (pruned, matched) = pruning_metric_sums(plan.as_ref(), "row_groups_pruned_statistics");
        assert!(
            pruned > 0,
            "semi-join dynamic-filter bounds must prune fact row groups \
             (pruned={pruned}, matched={matched}), plan:\n{}",
            plan_tree_text(plan.as_ref())
        );
        let _ = std::fs::remove_dir_all(&fact);
    }

    // ---- KAN-2: Q62 stage-0 wedge (multi-dim comma-join arm chain) ----

    /// Write `batches` as one-part-per-batch parquet files in a fresh temp dir.
    fn write_parquet_dir(tag: &str, batches: Vec<RecordBatch>) -> std::path::PathBuf {
        use datafusion::parquet::arrow::ArrowWriter;
        let dir = std::env::temp_dir().join(format!(
            "oxidant-q62-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        for (i, batch) in batches.into_iter().enumerate() {
            let f = std::fs::File::create(dir.join(format!("part-{i}.parquet"))).unwrap();
            let mut w = ArrowWriter::try_new(f, batch.schema(), None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
        dir
    }

    /// The Q62 stage-0 fixture: a wide-ish `web_sales` fact (`rows`) plus the four
    /// replicated dims, all as catalog parquet (footer row counts attached, column
    /// stats stripped — the worker stage shape).
    async fn register_q62_fixture(engine: &Engine, rows: i64) -> [std::path::PathBuf; 5] {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        let idx: Vec<i64> = (0..rows).collect();
        let strs = |tag: &str| -> Arc<StringArray> {
            Arc::new(StringArray::from(
                idx.iter()
                    .map(|&i| format!("{tag}-{:040}", i % 10_000))
                    .collect::<Vec<String>>(),
            ))
        };
        let ws_schema = Arc::new(Schema::new(vec![
            Field::new("ws_sold_date_sk", DataType::Int64, false),
            Field::new("ws_ship_date_sk", DataType::Int64, true),
            Field::new("ws_warehouse_sk", DataType::Int64, false),
            Field::new("ws_ship_mode_sk", DataType::Int64, false),
            Field::new("ws_web_site_sk", DataType::Int64, false),
            Field::new("ws_c1", DataType::Int64, false),
            Field::new("ws_c2", DataType::Int64, false),
            Field::new("ws_s1", DataType::Utf8, false),
            Field::new("ws_s2", DataType::Utf8, false),
        ]));
        let ws = RecordBatch::try_new(
            ws_schema,
            vec![
                Arc::new(Int64Array::from(
                    idx.iter().map(|&i| i % 73_000).collect::<Vec<i64>>(),
                )),
                Arc::new(Int64Array::from(
                    idx.iter()
                        .map(|&i| {
                            // ~1/8 NULL ship dates (undelivered rows, like TPC-DS).
                            if i % 8 == 0 {
                                None
                            } else {
                                Some((i * 7) % 73_000)
                            }
                        })
                        .collect::<Vec<Option<i64>>>(),
                )),
                Arc::new(Int64Array::from(
                    idx.iter().map(|&i| i % 20).collect::<Vec<i64>>(),
                )),
                Arc::new(Int64Array::from(
                    idx.iter().map(|&i| i % 20).collect::<Vec<i64>>(),
                )),
                Arc::new(Int64Array::from(
                    idx.iter().map(|&i| i % 54).collect::<Vec<i64>>(),
                )),
                Arc::new(Int64Array::from(idx.clone())),
                Arc::new(Int64Array::from(
                    idx.iter().map(|&i| i * 3).collect::<Vec<i64>>(),
                )),
                strs("a") as Arc<dyn arrow::array::Array>,
                strs("b") as Arc<dyn arrow::array::Array>,
            ],
        )
        .unwrap();
        let dim = |name: &str, key: &str, val: &str, n: i64| {
            let schema = Arc::new(Schema::new(vec![
                Field::new(key, DataType::Int64, false),
                Field::new(val, DataType::Utf8, false),
            ]));
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from((0..n).collect::<Vec<i64>>())),
                    Arc::new(StringArray::from(
                        (0..n)
                            .map(|i| format!("{name}-{:020}", i))
                            .collect::<Vec<String>>(),
                    )),
                ],
            )
            .unwrap()
        };
        let warehouse = dim("warehouse", "w_warehouse_sk", "w_warehouse_name", 20);
        let ship_mode = dim("ship_mode", "sm_ship_mode_sk", "sm_type", 20);
        let web_site = dim("web_site", "web_site_sk", "web_name", 54);
        // date_dim: 73k rows, d_month_seq arranged so BETWEEN 12 AND 23 keeps ~1/10
        // (the stage's d_month_seq BETWEEN 1200 AND 1211 shape at 1/12 the labels).
        let dd_schema = Arc::new(Schema::new(vec![
            Field::new("d_date_sk", DataType::Int64, false),
            Field::new("d_month_seq", DataType::Int64, false),
        ]));
        let date_dim = RecordBatch::try_new(
            dd_schema,
            vec![
                Arc::new(Int64Array::from((0..73_000).collect::<Vec<i64>>())),
                Arc::new(Int64Array::from(
                    (0..73_000).map(|i| i % 120).collect::<Vec<i64>>(),
                )),
            ],
        )
        .unwrap();
        let dirs = [
            write_parquet_dir("ws", vec![ws]),
            write_parquet_dir("wh", vec![warehouse]),
            write_parquet_dir("sm", vec![ship_mode]),
            write_parquet_dir("site", vec![web_site]),
            write_parquet_dir("dd", vec![date_dim]),
        ];
        register_catalog_parquet(engine, "web_sales", &dirs[0]).await;
        register_catalog_parquet(engine, "warehouse", &dirs[1]).await;
        register_catalog_parquet(engine, "ship_mode", &dirs[2]).await;
        register_catalog_parquet(engine, "web_site", &dirs[3]).await;
        register_catalog_parquet(engine, "date_dim", &dirs[4]).await;
        dirs
    }

    /// The Q62 stage-0 web_sales arm SQL, verbatim shape (comma joins rewritten as
    /// CROSS JOIN + WHERE by the stage planner, substr-wrapped warehouse subquery).
    const Q62_STAGE_SQL: &str = "SELECT sq1.w_substr AS g0, ship_mode.sm_type AS g1, \
        web_site.web_name AS g2, \
        sum(CASE WHEN ((web_sales.ws_ship_date_sk - web_sales.ws_sold_date_sk) <= 30) \
            THEN 1 ELSE 0 END) AS a0 \
        FROM web_sales \
        CROSS JOIN (SELECT substr(`warehouse`.w_warehouse_name, 1, 20) AS w_substr, \
            `warehouse`.* FROM \"warehouse\") AS sq1 \
        CROSS JOIN ship_mode CROSS JOIN web_site CROSS JOIN date_dim \
        WHERE date_dim.d_month_seq BETWEEN 12 AND 23 \
         AND web_sales.ws_ship_date_sk = date_dim.d_date_sk \
         AND web_sales.ws_warehouse_sk = sq1.w_warehouse_sk \
         AND web_sales.ws_ship_mode_sk = ship_mode.sm_ship_mode_sk \
         AND web_sales.ws_web_site_sk = web_site.web_site_sk \
        GROUP BY sq1.w_substr, ship_mode.sm_type, web_site.web_name";

    /// Q62 stage-0 regression guard (KAN-2): the comma-join arm chain — 7.2M-row fact
    /// FIRST, then four replicated dims — must plan every hash join with a row-BOUNDED
    /// build side. Pre-fix, DataFusion 54.1.0's `Inexact(min(l, r))` chain estimates
    /// (≈20 phantom rows for the fact-wide intermediates) made `JoinSelection` keep —
    /// and `CollectLeft`-collect — the CHAIN OUTPUT as the build side of the three upper
    /// joins (SF10: three ever-wider 7.2M-row single-partition hash builds under a shared
    /// 12 GiB pool ≈ 100x wedge, slow progress, no pool error for the runtime retry;
    /// local repro: 29 s vs 1.2 s sort-merge at 3M rows). The KAN-25 guard read the same
    /// phantom estimates and saw no risk. [`PreferBoundedJoinBuildSide`] re-seats the
    /// builds onto the positively-sized dims — after which the guard ALSO sees true build
    /// sizes, so the hash fast path is kept (no wholesale sort-merge reroute, KAN-8).
    #[tokio::test]
    async fn auto_join_selection_q62_arm_chain_builds_row_bounded_sides() {
        let _env = JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::remove_var("OXIDANT_PREFER_HASH_JOIN");
        std::env::remove_var("OXIDANT_SORT_MERGE_FALLBACK");
        std::env::remove_var("OXIDANT_PARQUET_SCAN_STATS");
        assert_eq!(join_preference(), JoinPreference::Auto);
        // Pin two partitions for the whole test: the sort-merge parity re-plan below
        // registers partitions × 8 `ExternalSorter` consumers on the 1 GiB
        // `FairSpillPool`, which caps each spilling consumer at pool/num_consumers — at
        // the host's core count that share (1 GiB / ~100 sorters ≈ 9 MiB) is smaller
        // than one wide chain-intermediate batch's FIRST reservation (~10 MiB, which an
        // empty sorter cannot spill its way to), so the re-plan dies with "Not enough
        // memory to continue external sort" whenever parallel test load keeps every
        // sorter registered at once (E-LOOM-FLAKE). At 2 partitions the ~64 MiB share
        // always admits a batch; spilling bounds the rest.
        std::env::set_var("OXIDANT_TARGET_PARTITIONS", "2");
        let engine = Engine::new_with_memory_limit(1024 * 1024 * 1024);
        std::env::remove_var("OXIDANT_TARGET_PARTITIONS");
        let dirs = register_q62_fixture(&engine, 300_000).await;
        let plan = engine.physical_plan(Q62_STAGE_SQL).await.unwrap();
        let display = plan_tree_text(plan.as_ref());
        assert_eq!(
            display.matches("HashJoinExec").count(),
            4,
            "expected the 4-join hash chain (no sort-merge reroute), got:\n{display}"
        );
        // The statistics enabler of the pre-fix pathology, still true post-fix: the
        // chain-output estimate is the phantom `Inexact(min(l, r))` — what changed is
        // that no hash join BUILDS on it anymore.
        fn check_builds(plan: &dyn datafusion::physical_plan::ExecutionPlan) {
            if let Some(hj) = as_hash_join(plan) {
                let build = hj.left();
                // Row-bounded, or a leaf scan: the rule swaps at JoinSelection time,
                // when a filtered dim is still `FilterExec(scan)` and provably bounded;
                // FilterPushdown runs later and folds that filter INTO the scan (its
                // footer-exact rows then read Inexact), but a leaf scan's rows are
                // table-bounded either way. The phantom estimates this guards against
                // only ever come from JOIN outputs.
                assert!(
                    provable_row_bound(build.as_ref()).is_some() || build.children().is_empty(),
                    "hash join build side must be row-bounded or a leaf scan, got:\n{}",
                    plan_tree_text(plan)
                );
                assert!(
                    !contains_hash_join(build.as_ref()),
                    "no hash join may build on a join output, got:\n{}",
                    plan_tree_text(plan)
                );
            }
            for c in plan.children() {
                check_builds(c.as_ref());
            }
        }
        check_builds(plan.as_ref());
        assert!(
            !build_side_contains_hash_join(plan.as_ref()),
            "the arm chain must build on dims, got:\n{display}"
        );
        // With builds on the dims, the dynamic filters prune the FACT scan (the star
        // fast path) instead of the tiny dim scans.
        assert!(
            scan_predicate_has_dynamic_filter(plan.as_ref()),
            "dim builds must push dynamic filters into the fact scan, got:\n{display}"
        );
        assert!(
            !engine.plan_time_smj_reroute(plan.as_ref()),
            "bounded tiny dim builds must keep the hash plan (no guard reroute)"
        );
        assert_eq!(engine.plan_time_smj_reroute_count(), 0);
        // Full result parity with the sort-merge re-plan (the pre-fix escape hatch):
        // the swapped build/probe sides must not mis-map a single output row.
        let batches = engine.sql(Q62_STAGE_SQL).await.unwrap();
        let logical = engine.logical_plan(Q62_STAGE_SQL).await.unwrap();
        let (smj_ctx, smj) = engine.sort_merge_physical_plan(logical).await.unwrap();
        let smj_batches = datafusion::physical_plan::collect(smj, smj_ctx.task_ctx())
            .await
            .unwrap();
        let sorted_lines = |batches: &[RecordBatch]| {
            let pretty = arrow::util::pretty::pretty_format_batches(batches)
                .unwrap()
                .to_string();
            let mut lines: Vec<String> = pretty.lines().map(str::to_string).collect();
            lines.sort_unstable();
            lines
        };
        assert!(!batches.is_empty());
        assert_eq!(
            sorted_lines(&batches),
            sorted_lines(&smj_batches),
            "hash (bounded builds) and sort-merge results must match row-for-row"
        );
        for d in dirs {
            let _ = std::fs::remove_dir_all(d);
        }
    }
}
