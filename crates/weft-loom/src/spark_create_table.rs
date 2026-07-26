//! Faithful lowering of Spark's `CREATE TABLE … USING <fmt>` DDL to DataFusion's real,
//! format-backed `CREATE EXTERNAL TABLE`.
//!
//! sqlparser 0.62 (the Databricks dialect) does not consume Spark's `USING <provider>` clause in
//! `CREATE TABLE`, and DataFusion's `DFParser` only special-cases `CREATE EXTERNAL` /
//! `CREATE UNBOUNDED EXTERNAL`, so `CREATE TABLE t(a int) USING parquet` fails at parse
//! (`found: USING`) — and every downstream statement then errors "table not found". We rewrite the
//! statement (pre-`ctx.sql()`) to
//!   `CREATE EXTERNAL TABLE t (a int) STORED AS PARQUET LOCATION '<warehouse>/t/'`
//! which DataFusion plans into a `ListingTable` backed by **real files** at `LOCATION`: INSERT
//! writes `<fmt>` files there and SELECT reads them back. This is the contract's ALLOWED "lower
//! Spark syntax to an EQUIVALENT DataFusion plan" (genuine durable format-backed storage), and the
//! polar opposite of the FORBIDDEN MemTable shim (no silent in-memory downgrade).
//!
//! Two load-bearing details from the DataFusion source:
//!   1. `LOCATION` MUST end in `'/'` — `ListingTable::insert_into` requires `is_collection()`.
//!   2. The directory must exist before an empty-table SELECT — the caller `create_dir_all`s
//!      `table_dir` at CREATE time (which is also what a real warehouse does on `CREATE TABLE`).
//!
//! Scope (this iteration, conservative): non-CTAS
//! `CREATE TABLE [IF NOT EXISTS] name (cols) USING {parquet|orc|csv|json}`, dropping trailing
//! table-level `COMMENT '…'` / `TBLPROPERTIES(…)` (metadata only, data-faithful). Anything else —
//! CTAS (`AS SELECT`), `PARTITIONED BY`, `OPTIONS(…)` (storage-affecting), `LOCATION`, an unknown
//! tail, `IDENTIFIER(...)` names, or an unrecognized format — returns `None`, leaving the statement
//! byte-identical for the normal path (it keeps failing exactly as today — never a regression).

use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// A lowered, ready-to-execute external-table DDL plus the managed directory to create first.
pub(crate) struct Lowered {
    pub ddl: String,
    pub table_dir: PathBuf,
    /// The parsed (possibly qualified, possibly backticked) table name, verbatim — lets the caller
    /// check whether its first segment names a registered external catalog before committing to
    /// this local-warehouse lowering (see `Engine::sql`'s use of this via
    /// `name_targets_external_catalog`), and is also used to key `Engine::created_tables`.
    pub name: String,
    /// The lowercased `USING <fmt>` provider (`parquet`/`orc`/`csv`/`json`).
    pub format: String,
    /// Table-level `COMMENT '…'`, if present (retained rather than dropped so later `SHOW CREATE
    /// TABLE`/`SHOW TBLPROPERTIES`/`DESCRIBE EXTENDED` work can answer it).
    pub comment: Option<String>,
    /// Table-level `TBLPROPERTIES(…)` key/value pairs, if present. Best-effort parsed: a malformed
    /// entry is simply skipped rather than failing the whole lowering (matches the pre-existing
    /// behavior of accepting any balanced-parens tail here).
    pub properties: HashMap<String, String>,
}

const FORMATS: &[&str] = &["parquet", "orc", "csv", "json"];

/// Return the lowering for a recognized `CREATE TABLE … USING <fmt>` (non-CTAS), else `None`
/// (statement is left untouched for the normal path — never a regression).
pub(crate) fn lower_create_table_using(sql: &str, warehouse: &Path) -> Option<Lowered> {
    let mut t = Tok::new(sql);
    t.kw("create")?;
    // Only the bare `CREATE TABLE` form. `CREATE EXTERNAL/TEMPORARY/OR REPLACE` etc. are handled
    // elsewhere or intentionally unsupported here — the next token must be `table`.
    t.kw("table")?;
    let if_not_exists = t.opt_kw3("if", "not", "exists");
    let name = t.object_name()?;
    // `IDENTIFIER('tab')(c1 INT)` is a function-form name we cannot faithfully manage — defer.
    if name.eq_ignore_ascii_case("identifier") {
        return None;
    }
    // The column list MUST be present (explicit schema). Its absence means CTAS (`USING fmt AS …`)
    // or a schemaless form — both deferred.
    let cols = t.balanced_parens()?;
    t.kw("using")?;
    let fmt = t.ident()?;
    let fmt_l = fmt.to_ascii_lowercase();
    if !FORMATS.contains(&fmt_l.as_str()) {
        return None;
    }

    // Tail: accept only end-of-statement after retaining table-level `COMMENT '…'` /
    // `TBLPROPERTIES(…)` (metadata, data-faithful — no longer dropped, see `Lowered::comment`/
    // `Lowered::properties`). Bail on `AS` (CTAS), `PARTITIONED BY`, `OPTIONS` (storage-affecting),
    // `LOCATION`, `CLUSTERED`, or anything else — never a lossy rewrite.
    let mut comment: Option<String> = None;
    let mut properties: HashMap<String, String> = HashMap::new();
    loop {
        if t.at_end() {
            break;
        }
        if t.kw("comment").is_some() {
            let lit = t.string_literal()?;
            comment = Some(unquote_string_literal(lit));
            continue;
        }
        if t.kw("tblproperties").is_some() {
            let span = t.balanced_parens()?;
            properties = parse_properties(span);
            continue;
        }
        // Unknown / storage-affecting tail — do not risk a lossy rewrite.
        return None;
    }

    let dir_name = sanitize(&name);
    let table_dir = warehouse.join(&dir_name);
    // Trailing slash is REQUIRED so ListingTable treats LOCATION as an (insertable) collection.
    let location = format!("{}/", table_dir.display());
    let ine = if if_not_exists { "IF NOT EXISTS " } else { "" };
    let ddl = format!(
        "CREATE EXTERNAL TABLE {ine}{name} {cols} STORED AS {} LOCATION '{location}'",
        fmt_l.to_uppercase()
    );
    Some(Lowered {
        ddl,
        table_dir,
        name,
        format: fmt_l,
        comment,
        properties,
    })
}

/// A CTAS lowering: materialize `select_sql` into `table_dir`, then run `ddl`.
pub(crate) struct LoweredCtas {
    pub select_sql: String,
    pub fmt: String,
    pub ddl: String,
    pub table_dir: PathBuf,
    /// The parsed (possibly qualified, possibly backticked) table name, verbatim — see
    /// `Lowered::name`; also used to key `Engine::created_tables`.
    pub name: String,
    /// Table-level `COMMENT '…'`. Always `None` today — this CTAS lowering doesn't yet parse a
    /// `COMMENT`/`TBLPROPERTIES` tail between `USING <fmt>` and `AS` (unsupported shape, same as
    /// before this change: such a statement fails to lower and falls through to the normal path).
    pub comment: Option<String>,
    /// Table-level `TBLPROPERTIES(…)`. Always empty today, for the same reason as `comment` above.
    pub properties: HashMap<String, String>,
}

/// Return lowering for `CREATE TABLE [IF NOT EXISTS] name USING fmt AS SELECT …`.
pub(crate) fn lower_create_table_ctas(sql: &str, warehouse: &Path) -> Option<LoweredCtas> {
    let mut t = Tok::new(sql);
    t.kw("create")?;
    t.kw("table")?;
    let if_not_exists = t.opt_kw3("if", "not", "exists");
    let name = t.object_name()?;
    if name.eq_ignore_ascii_case("identifier") {
        return None;
    }
    // CTAS has no column list — skip optional parens only if absent.
    if t.peek_ch() == Some(b'(') {
        return None;
    }
    t.kw("using")?;
    let fmt = t.ident()?;
    let fmt_l = fmt.to_ascii_lowercase();
    if !FORMATS.contains(&fmt_l.as_str()) {
        return None;
    }
    t.kw("as")?;
    let select_start = t.i;
    let select_sql = t
        .rest_from(select_start)
        .trim()
        .trim_end_matches(';')
        .to_string();
    if select_sql.is_empty() {
        return None;
    }
    let dir_name = sanitize(&name);
    let table_dir = warehouse.join(&dir_name);
    let location = format!("{}/", table_dir.display());
    let ine = if if_not_exists { "IF NOT EXISTS " } else { "" };
    // Schema inferred from data at insert time; external table without explicit columns.
    let ddl = format!(
        "CREATE EXTERNAL TABLE {ine}{name} STORED AS {} LOCATION '{location}'",
        fmt_l.to_uppercase()
    );
    Some(LoweredCtas {
        select_sql,
        fmt: fmt_l,
        ddl,
        table_dir,
        name,
        comment: None,
        properties: HashMap::new(),
    })
}

/// Leading-keyword INSERT detector (skips leading whitespace / `--` / `/* */`). Used by
/// `Engine::sql` to run the write for its side effects but return empty batches, matching Spark
/// (DataFusion's INSERT `count` row is dropped — `spark.sql("INSERT …")` is an empty DataFrame).
pub(crate) fn is_insert(sql: &str) -> bool {
    Tok::new(sql).peek_kw("insert")
}

/// Decode a single- or double-quoted string-literal span (as returned by [`Tok::string_literal`],
/// quotes included) into its value. Single-quoted literals go through the same
/// `unescapeSQLString`-faithful decode as the rest of weft's Spark literal handling (`''` collapsed
/// to `'`, `\n`/`\t`/`\uXXXX`/octal escapes, etc. — see [`crate::spark_unescape_sql_string`]);
/// double-quoted literals only ever double their own quote char (`""` → `"`), per Spark's own
/// `unescapeSQLString`, which only rewrites single-quoted literals.
fn unquote_string_literal(lit: &str) -> String {
    if lit.len() < 2 {
        return String::new();
    }
    let content = &lit[1..lit.len() - 1];
    if lit.as_bytes()[0] == b'\'' {
        crate::spark_unescape_sql_string(content)
    } else {
        content.replace("\"\"", "\"")
    }
}

/// Best-effort parse of a `TBLPROPERTIES(…)` balanced-parens span (as returned by
/// [`Tok::balanced_parens`], outer parens included) into its `'key'='value'` pairs. Any entry that
/// doesn't match the expected shape simply stops parsing further entries — this never fails the
/// lowering itself (see `Lowered::properties`'s doc comment).
fn parse_properties(span: &str) -> HashMap<String, String> {
    let inner = span
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .unwrap_or(span);
    let mut map = HashMap::new();
    let mut t = Tok::new(inner);
    loop {
        t.skip_ws_comments();
        // Spark's `TBLPROPERTIES`/`OPTIONS` key grammar accepts either a quoted string literal
        // (`'password'`) or a bare/backtick-quoted identifier (`password`) — `SHOW
        // TBLPROPERTIES(...password = 'password')` in the vendored corpus uses the latter, so an
        // identifier-only key must not silently truncate the whole property list.
        let key = if let Some(key_lit) = t.string_literal() {
            unquote_string_literal(key_lit)
        } else if let Some(id) = t.ident() {
            id
        } else {
            break;
        };
        t.skip_ws_comments();
        if t.peek_ch() != Some(b'=') {
            break;
        }
        t.i += 1;
        t.skip_ws_comments();
        let Some(val_lit) = t.string_literal() else {
            break;
        };
        map.insert(key, unquote_string_literal(val_lit));
        t.skip_ws_comments();
        if t.peek_ch() == Some(b',') {
            t.i += 1;
            continue;
        }
        break;
    }
    map
}

/// Map a (possibly qualified / backticked) table name to a filesystem-safe directory component.
fn sanitize(name: &str) -> String {
    let s: String = name
        .chars()
        .map(|c| match c {
            c if c.is_alphanumeric() || c == '_' || c == '-' => c,
            _ => '_',
        })
        .collect();
    if s.is_empty() {
        "_".to_string()
    } else {
        s
    }
}

// ---------------------------------------------------------------------------
// Tok: a minimal, quote-/comment-aware cursor. Mirrors the defensive scanning discipline already
// proven in `lib.rs::rewrite_spark_typed_literals` (single/double-quote, backtick, `--`, `/* */`
// all skipped verbatim so SQL syntax inside string/identifier literals is never misread).
// ---------------------------------------------------------------------------
struct Tok<'a> {
    s: &'a str,
    b: &'a [u8],
    i: usize,
}

impl<'a> Tok<'a> {
    fn new(s: &'a str) -> Self {
        Self {
            s,
            b: s.as_bytes(),
            i: 0,
        }
    }

    fn skip_ws_comments(&mut self) {
        let (b, n) = (self.b, self.b.len());
        loop {
            while self.i < n && b[self.i].is_ascii_whitespace() {
                self.i += 1;
            }
            // Line comment.
            if self.i + 1 < n && b[self.i] == b'-' && b[self.i + 1] == b'-' {
                while self.i < n && b[self.i] != b'\n' {
                    self.i += 1;
                }
                continue;
            }
            // Block comment.
            if self.i + 1 < n && b[self.i] == b'/' && b[self.i + 1] == b'*' {
                self.i += 2;
                while self.i < n && !(b[self.i] == b'*' && self.i + 1 < n && b[self.i + 1] == b'/')
                {
                    self.i += 1;
                }
                self.i = (self.i + 2).min(n);
                continue;
            }
            break;
        }
    }

    /// Read a bare keyword/identifier word (`[A-Za-z_][A-Za-z0-9_]*`) at the cursor, or `None`.
    fn read_word(&mut self) -> Option<&'a str> {
        self.skip_ws_comments();
        let (b, n) = (self.b, self.b.len());
        let start = self.i;
        if start < n && (b[start].is_ascii_alphabetic() || b[start] == b'_') {
            self.i += 1;
            while self.i < n && (b[self.i].is_ascii_alphanumeric() || b[self.i] == b'_') {
                self.i += 1;
            }
            Some(&self.s[start..self.i])
        } else {
            None
        }
    }

    /// Consume keyword `k` (case-insensitive) if present; otherwise leave the cursor put.
    fn kw(&mut self, k: &str) -> Option<()> {
        let save = self.i;
        match self.read_word() {
            Some(w) if w.eq_ignore_ascii_case(k) => Some(()),
            _ => {
                self.i = save;
                None
            }
        }
    }

    /// True if keyword `k` is next, without consuming it.
    fn peek_kw(&mut self, k: &str) -> bool {
        let save = self.i;
        let hit = matches!(self.read_word(), Some(w) if w.eq_ignore_ascii_case(k));
        self.i = save;
        hit
    }

    /// Consume the three-keyword sequence `a b c` (e.g. `IF NOT EXISTS`) atomically.
    fn opt_kw3(&mut self, a: &str, b: &str, c: &str) -> bool {
        let save = self.i;
        if self.kw(a).is_some() && self.kw(b).is_some() && self.kw(c).is_some() {
            true
        } else {
            self.i = save;
            false
        }
    }

    /// A bare identifier token (e.g. the format name), verbatim.
    fn ident(&mut self) -> Option<String> {
        self.read_word().map(str::to_string)
    }

    /// A (possibly qualified, possibly backticked) object name, returned as its verbatim span.
    fn object_name(&mut self) -> Option<String> {
        self.skip_ws_comments();
        let (b, n) = (self.b, self.b.len());
        let start = self.i;
        loop {
            if self.i < n && b[self.i] == b'`' {
                self.i += 1;
                while self.i < n && b[self.i] != b'`' {
                    self.i += 1;
                }
                if self.i < n {
                    self.i += 1; // closing backtick
                }
            } else {
                let seg = self.i;
                if self.i < n && (b[self.i].is_ascii_alphabetic() || b[self.i] == b'_') {
                    self.i += 1;
                    while self.i < n && (b[self.i].is_ascii_alphanumeric() || b[self.i] == b'_') {
                        self.i += 1;
                    }
                }
                if self.i == seg {
                    break;
                }
            }
            // Qualified name continuation.
            if self.i < n && b[self.i] == b'.' {
                self.i += 1;
                continue;
            }
            break;
        }
        (self.i > start).then(|| self.s[start..self.i].to_string())
    }

    /// If the cursor is at `'('`, return the whole balanced `( … )` span (paren-depth aware,
    /// ignoring parens inside quotes/backticks), else `None`. `decimal(38,18)` nests fine; nested
    /// `array<…>` / `struct<…>` use `<>` not parens.
    fn balanced_parens(&mut self) -> Option<&'a str> {
        self.skip_ws_comments();
        let (b, n) = (self.b, self.b.len());
        if !(self.i < n && b[self.i] == b'(') {
            return None;
        }
        let start = self.i;
        let mut depth = 0usize;
        while self.i < n {
            let c = b[self.i];
            if c == b'\'' || c == b'"' || c == b'`' {
                self.i += 1;
                while self.i < n {
                    if b[self.i] == c {
                        if self.i + 1 < n && b[self.i + 1] == c {
                            self.i += 2;
                            continue;
                        }
                        self.i += 1;
                        break;
                    }
                    self.i += 1;
                }
                continue;
            }
            if c == b'(' {
                depth += 1;
                self.i += 1;
                continue;
            }
            if c == b')' {
                depth -= 1;
                self.i += 1;
                if depth == 0 {
                    return Some(&self.s[start..self.i]);
                }
                continue;
            }
            self.i += 1;
        }
        None
    }

    /// A single- or double-quoted string literal at the cursor (honoring doubled quotes), verbatim.
    fn string_literal(&mut self) -> Option<&'a str> {
        self.skip_ws_comments();
        let (b, n) = (self.b, self.b.len());
        if !(self.i < n && (b[self.i] == b'\'' || b[self.i] == b'"')) {
            return None;
        }
        let q = b[self.i];
        let start = self.i;
        self.i += 1;
        while self.i < n {
            if b[self.i] == q {
                if self.i + 1 < n && b[self.i + 1] == q {
                    self.i += 2;
                    continue;
                }
                self.i += 1;
                return Some(&self.s[start..self.i]);
            }
            self.i += 1;
        }
        None
    }

    /// True if only whitespace / comments / a single trailing `;` remain.
    fn at_end(&mut self) -> bool {
        self.skip_ws_comments();
        if self.i < self.b.len() && self.b[self.i] == b';' {
            self.i += 1;
            self.skip_ws_comments();
        }
        self.i >= self.b.len()
    }

    fn peek_ch(&mut self) -> Option<u8> {
        self.skip_ws_comments();
        self.b.get(self.i).copied()
    }

    fn rest_from(&self, start: usize) -> &str {
        &self.s[start..]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn lowers_plain_parquet() {
        let w = Path::new("/tmp/wh");
        let l = lower_create_table_using("create table t1(a int, b int, c int) using parquet", w)
            .expect("should lower");
        assert!(
            l.ddl
                .starts_with("CREATE EXTERNAL TABLE t1 (a int, b int, c int) STORED AS PARQUET"),
            "ddl was: {}",
            l.ddl
        );
        assert!(
            l.ddl.contains("LOCATION '/tmp/wh/t1/'"),
            "ddl was: {}",
            l.ddl
        );
        assert_eq!(l.table_dir, w.join("t1"));
    }

    #[test]
    fn lowers_if_not_exists_and_decimal_cols() {
        let w = Path::new("/tmp/wh");
        let l = lower_create_table_using(
            "CREATE TABLE IF NOT EXISTS decimals_test(id int, a decimal(38,18), b decimal(38,18)) USING parquet",
            w,
        )
        .expect("should lower");
        assert!(
            l.ddl.contains(
                "IF NOT EXISTS decimals_test (id int, a decimal(38,18), b decimal(38,18))"
            ),
            "ddl: {}",
            l.ddl
        );
        assert!(l.ddl.contains("STORED AS PARQUET"));
    }

    #[test]
    fn retains_trailing_comment_and_tblproperties() {
        let w = Path::new("/tmp/wh");
        let l = lower_create_table_using(
            "create table t(a int) using csv COMMENT 'hi' TBLPROPERTIES('k'='v');",
            w,
        )
        .expect("should lower");
        // The rewritten DDL still doesn't embed COMMENT/TBLPROPERTIES text (DataFusion's
        // `CREATE EXTERNAL TABLE` doesn't understand either) — but the values are no longer
        // discarded, they're carried on `Lowered` for the caller to persist.
        assert!(l
            .ddl
            .starts_with("CREATE EXTERNAL TABLE t (a int) STORED AS CSV"));
        assert!(!l.ddl.contains("COMMENT"));
        assert!(!l.ddl.contains("TBLPROPERTIES"));
        assert_eq!(l.name, "t");
        assert_eq!(l.comment.as_deref(), Some("hi"));
        assert_eq!(l.properties.get("k").map(String::as_str), Some("v"));
    }

    #[test]
    fn case_insensitive_format() {
        let w = Path::new("/tmp/wh");
        assert!(
            lower_create_table_using("create table t(a int) using JSON", w)
                .unwrap()
                .ddl
                .contains("STORED AS JSON")
        );
    }

    #[test]
    fn passes_through_non_using_ctas_partitioned_options_identifier() {
        let w = Path::new("/tmp/wh");
        // Plain CREATE TABLE (no USING) — DataFusion already handles it.
        assert!(lower_create_table_using("CREATE TABLE t(a INT)", w).is_none());
        assert!(lower_create_table_using("select 1", w).is_none());
        // CTAS deferred this iteration.
        assert!(lower_create_table_using("create table t using parquet as select 1", w).is_none());
        assert!(
            lower_create_table_using("create table t(a int) using parquet as select 1", w)
                .is_none()
        );
        // PARTITIONED BY deferred (partition semantics).
        assert!(lower_create_table_using(
            "create table t(a int, b int) using parquet partitioned by (b)",
            w
        )
        .is_none());
        // Storage-affecting OPTIONS / LOCATION must not be dropped.
        assert!(lower_create_table_using(
            "create table t(a int) using csv options (header 'true')",
            w
        )
        .is_none());
        assert!(
            lower_create_table_using("create table t(a int) using parquet location '/x'", w)
                .is_none()
        );
        // IDENTIFIER(...) function-form name deferred.
        assert!(
            lower_create_table_using("CREATE TABLE IDENTIFIER('tab')(c1 INT) USING CSV", w)
                .is_none()
        );
        // Unknown format deferred.
        assert!(lower_create_table_using("create table t(a int) using avro", w).is_none());
    }

    #[test]
    fn handles_qualified_and_backticked_names() {
        let w = Path::new("/tmp/wh");
        let l = lower_create_table_using("CREATE TABLE s.tab(c1 INT) USING CSV", w).unwrap();
        assert!(l.ddl.contains("CREATE EXTERNAL TABLE s.tab (c1 INT)"));
        assert_eq!(l.table_dir, w.join("s_tab"));
        let l2 =
            lower_create_table_using("CREATE TABLE `weird name`(c1 INT) USING parquet", w).unwrap();
        assert!(l2
            .ddl
            .contains("CREATE EXTERNAL TABLE `weird name` (c1 INT)"));
    }

    #[test]
    fn detects_insert() {
        assert!(is_insert("insert into t1 values(1,0,0)"));
        assert!(is_insert("  -- c\n INSERT INTO t SELECT * FROM s"));
        assert!(is_insert("/* x */ insert overwrite t values (1)"));
        assert!(!is_insert("select * from t"));
        assert!(!is_insert("create table t(a int) using parquet"));
    }
}
