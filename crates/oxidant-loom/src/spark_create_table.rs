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
    /// `PARTITIONED BY (…)` columns, emitted into the lowered DDL so DataFusion writes and reads
    /// them as Hive-style directories. Empty when the DDL had no such clause.
    pub partition_columns: Vec<String>,
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

    // Tail: the storage clauses, then end-of-statement. `AS` (CTAS) is handled by
    // `lower_create_table_ctas`; anything unrecognized bails rather than risking a lossy rewrite.
    let clauses = table_clauses(&mut t)?;
    if !t.at_end() {
        return None;
    }

    let (table_dir, location) = resolve_table_dir(&name, clauses.location.as_deref(), warehouse)?;
    let partitioned = partitioned_by_clause(&clauses.partition_columns);
    let options = datafusion_options(&fmt_l, &clauses.options)?;
    let ine = if if_not_exists { "IF NOT EXISTS " } else { "" };
    let ddl = format!(
        "CREATE EXTERNAL TABLE {ine}{name} {cols} STORED AS {}{partitioned} LOCATION \
         '{location}'{options}",
        fmt_l.to_uppercase()
    );
    Some(Lowered {
        ddl,
        table_dir,
        name,
        format: fmt_l,
        comment: clauses.comment,
        properties: clauses.properties,
        partition_columns: clauses.partition_columns,
    })
}

/// Where a lowered table's files go: an explicit `LOCATION`, else `{warehouse}/{sanitized name}/`.
///
/// Returns the directory to create and the `LOCATION` literal to emit — always with the trailing
/// slash `ListingTable::insert_into` requires to treat the location as an insertable collection.
/// `None` for a location this lowering cannot manage: an object-store URL (that belongs to a
/// catalog table, not the local warehouse) or one carrying a quote that would break the DDL.
fn resolve_table_dir(
    name: &str,
    location: Option<&str>,
    warehouse: &Path,
) -> Option<(PathBuf, String)> {
    let Some(location) = location else {
        let dir = warehouse.join(sanitize(name));
        let literal = format!("{}/", dir.display());
        return Some((dir, literal));
    };
    if location.contains('\'') {
        return None;
    }
    let path = location.strip_prefix("file://").unwrap_or(location);
    if path.contains("://") {
        return None;
    }
    let dir = PathBuf::from(path.trim_end_matches('/'));
    if dir.as_os_str().is_empty() {
        return None;
    }
    Some((dir.clone(), format!("{}/", dir.display())))
}

/// The `PARTITIONED BY (…)` clause for the lowered DDL, or empty when there is none.
fn partitioned_by_clause(columns: &[String]) -> String {
    if columns.is_empty() {
        return String::new();
    }
    format!(" PARTITIONED BY ({})", columns.join(", "))
}

/// Translate Spark's `OPTIONS(…)` into DataFusion's `OPTIONS ('format.<key>' '<value>')`.
///
/// An allowlist, not a passthrough. These options decide how the table's own bytes are read — a
/// CSV table that silently ignores `header` reads its header row as data — so an option this
/// cannot translate fails the lowering (leaving the statement to the normal path, exactly as
/// before) rather than being dropped.
fn datafusion_options(format: &str, options: &HashMap<String, String>) -> Option<String> {
    if options.is_empty() {
        return Some(String::new());
    }
    if format != "csv" {
        return None;
    }
    let mut pairs = Vec::with_capacity(options.len());
    for (key, value) in options {
        let translated = match key.to_ascii_lowercase().as_str() {
            "header" => "format.has_header",
            "delimiter" | "sep" => "format.delimiter",
            "quote" => "format.quote",
            "escape" => "format.escape",
            "compression" => "format.compression",
            _ => return None,
        };
        if value.contains('\'') {
            return None;
        }
        pairs.push(format!("'{translated}' '{value}'"));
    }
    // Sorted so the emitted DDL is deterministic regardless of the map's iteration order.
    pairs.sort();
    Some(format!(" OPTIONS ({})", pairs.join(", ")))
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
    /// Table-level `COMMENT '…'` from between `USING <fmt>` and `AS`, if present.
    pub comment: Option<String>,
    /// Table-level `TBLPROPERTIES(…)` from between `USING <fmt>` and `AS`, if present.
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
    let clauses = table_clauses(&mut t)?;
    // The local CTAS writer emits one flat file per table: it has no partitioned-write path, and
    // no reader options to honor beyond what it writes. Accepting either clause here would produce
    // a table that does not match its own DDL, so both are left to the normal path.
    if !clauses.partition_columns.is_empty() || !clauses.options.is_empty() {
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
    let (table_dir, location) = resolve_table_dir(&name, clauses.location.as_deref(), warehouse)?;
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
        comment: clauses.comment,
        properties: clauses.properties,
    })
}

/// Formats accepted on the **external-catalog** CTAS path.
///
/// Broader than [`FORMATS`] (the local warehouse's `ListingTable` set) because a catalog table can
/// be Delta. Iceberg is listed deliberately: naming it here buys a clear "not a write target"
/// error from the write layer instead of a bewildering `found: USING` parse failure.
const CATALOG_FORMATS: &[&str] = &["parquet", "delta", "iceberg", "csv", "json"];

/// The storage clauses Spark allows between `USING <fmt>` and `AS`, in any order.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct TableClauses {
    pub location: Option<String>,
    pub partition_columns: Vec<String>,
    pub options: HashMap<String, String>,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
}

/// A `CREATE TABLE … USING <fmt> … AS SELECT …` bound for an external catalog.
pub(crate) struct ExternalCtas {
    /// The parsed (possibly qualified, possibly backticked) table name, verbatim — the caller
    /// checks whether its first segment names a registered external catalog before acting on this.
    pub name: String,
    /// The lowercased `USING <fmt>` provider, one of [`CATALOG_FORMATS`].
    pub format: String,
    pub clauses: TableClauses,
    /// The statement with every clause DataFusion's parser cannot take removed, so its own CTAS
    /// planning handles it: `CREATE TABLE [IF NOT EXISTS] <name> AS <select>`.
    pub ddl: String,
}

/// Return the parse for `CREATE TABLE [IF NOT EXISTS] <name> USING <fmt> [clauses] AS SELECT …`,
/// else `None` (statement left untouched for the normal path — never a regression).
///
/// This does no warehouse resolution of its own: an external catalog decides where its tables
/// live, so an absent `LOCATION` means "ask the catalog", not "put it under the local warehouse".
pub(crate) fn lower_external_ctas(sql: &str) -> Option<ExternalCtas> {
    let mut t = Tok::new(sql);
    t.kw("create")?;
    t.kw("table")?;
    let if_not_exists = t.opt_kw3("if", "not", "exists");
    let name = t.object_name()?;
    if name.eq_ignore_ascii_case("identifier") {
        return None;
    }
    // An explicit column list means this is not CTAS.
    if t.peek_ch() == Some(b'(') {
        return None;
    }
    t.kw("using")?;
    let fmt = t.ident()?.to_ascii_lowercase();
    if !CATALOG_FORMATS.contains(&fmt.as_str()) {
        return None;
    }
    let clauses = table_clauses(&mut t)?;
    t.kw("as")?;
    let select_sql = t.rest_from(t.i).trim().trim_end_matches(';').trim();
    if select_sql.is_empty() {
        return None;
    }
    let ine = if if_not_exists { "IF NOT EXISTS " } else { "" };
    Some(ExternalCtas {
        ddl: format!("CREATE TABLE {ine}{name} AS {select_sql}"),
        name,
        format: fmt,
        clauses,
    })
}

/// Consume Spark's storage-clause tail, stopping at `AS` or the end of the statement.
///
/// Returns `None` on anything unrecognized (`CLUSTERED BY`, a stray token) so the caller leaves
/// the statement alone rather than rewriting it lossily.
fn table_clauses(t: &mut Tok<'_>) -> Option<TableClauses> {
    let mut clauses = TableClauses::default();
    loop {
        if t.at_end() || t.peek_kw("as") {
            break;
        }
        if t.kw("location").is_some() {
            clauses.location = Some(unquote_string_literal(t.string_literal()?));
            continue;
        }
        if t.kw("comment").is_some() {
            clauses.comment = Some(unquote_string_literal(t.string_literal()?));
            continue;
        }
        if t.kw("tblproperties").is_some() {
            clauses.properties = parse_properties(t.balanced_parens()?);
            continue;
        }
        if t.kw("options").is_some() {
            clauses.options = parse_properties(t.balanced_parens()?);
            continue;
        }
        if t.peek_kw("partitioned") {
            t.kw("partitioned")?;
            t.kw("by")?;
            clauses.partition_columns = parse_identifier_list(t.balanced_parens()?)?;
            continue;
        }
        return None;
    }
    Some(clauses)
}

/// Parse a `( a, b, c )` span into its identifiers.
///
/// Strict on purpose: anything left over after the last identifier (Spark's typed
/// `PARTITIONED BY (event_date DATE)` form, say) fails the whole parse rather than silently
/// dropping partition columns — a table partitioned differently than the DDL asked for is worse
/// than a statement that does not run.
fn parse_identifier_list(span: &str) -> Option<Vec<String>> {
    let inner = span
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .unwrap_or(span);
    let mut out = Vec::new();
    let mut t = Tok::new(inner);
    loop {
        if t.at_end() {
            break;
        }
        out.push(t.object_name()?.trim_matches('`').to_string());
        if t.peek_ch() == Some(b',') {
            t.i += 1;
            continue;
        }
        break;
    }
    (!out.is_empty() && t.at_end()).then_some(out)
}

/// The table an `INSERT` writes to, for cache invalidation.
///
/// `None` for anything that is not the plain `INSERT {INTO|OVERWRITE} [TABLE] <name>` shape —
/// the caller only uses this to evict a cached provider, so a miss costs a stale read at worst
/// and a wrong guess costs a no-op eviction of a name that does not exist.
pub(crate) fn insert_target(sql: &str) -> Option<String> {
    let mut t = Tok::new(sql);
    t.kw("insert")?;
    // Both orders occur: `INSERT INTO t`, `INSERT OVERWRITE t`, `INSERT OVERWRITE INTO t`.
    let overwrite = t.kw("overwrite").is_some();
    let into = t.kw("into").is_some();
    if !overwrite && !into {
        return None;
    }
    let _ = t.kw("table");
    // Re-emitted backticked, so the caller's `split_ident` sees exactly the segments this parsed —
    // including one that arrived double-quoted, which `split_ident` does not itself unquote.
    let segments = t.object_name_segments()?;
    Some(
        segments
            .iter()
            .map(|s| format!("`{s}`"))
            .collect::<Vec<_>>()
            .join("."),
    )
}

/// The byte offset and text of an `INSERT` target's leading segment, when that name is
/// catalog-qualified (3+ segments) and the segment arrived unquoted.
///
/// Exists for one reason: sqlparser consumes `LOCAL` as Hive's `INSERT OVERWRITE LOCAL DIRECTORY`
/// keyword, so `INSERT INTO local.live.t` fails to parse outright for a catalog named `local` —
/// the name this repo's own example config uses. Quoting that one segment turns it back into an
/// identifier. Returns `None` when there is nothing to rewrite (already quoted, or not
/// catalog-qualified).
pub(crate) fn insert_target_catalog(sql: &str) -> Option<(usize, String)> {
    let mut t = Tok::new(sql);
    t.kw("insert")?;
    let overwrite = t.kw("overwrite").is_some();
    let into = t.kw("into").is_some();
    if !overwrite && !into {
        return None;
    }
    let _ = t.kw("table");
    t.skip_ws_comments();
    let start = t.i;
    if t.peek_ch().is_some_and(|c| c == b'`' || c == b'"') {
        return None;
    }
    let segments = t.object_name_segments()?;
    (segments.len() >= 3).then(|| (start, segments[0].clone()))
}

/// Leading-keyword INSERT detector (skips leading whitespace / `--` / `/* */`). Used by
/// `Engine::sql` to run the write for its side effects but return empty batches, matching Spark
/// (DataFusion's INSERT `count` row is dropped — `spark.sql("INSERT …")` is an empty DataFrame).
pub(crate) fn is_insert(sql: &str) -> bool {
    Tok::new(sql).peek_kw("insert")
}

/// Decode a single- or double-quoted string-literal span (as returned by [`Tok::string_literal`],
/// quotes included) into its value. Single-quoted literals go through the same
/// `unescapeSQLString`-faithful decode as the rest of oxidant's Spark literal handling (`''` collapsed
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
        // `TBLPROPERTIES` always spells the `=`; Spark's `OPTIONS` allows `(key 'value')` too, so
        // the separator is optional here and both spellings parse.
        if t.peek_ch() == Some(b'=') {
            t.i += 1;
        }
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

    /// The dot-separated segments of an object name with their quoting removed.
    ///
    /// Handles both spellings a statement can arrive in: Spark's backticks, and the double quotes
    /// DataFusion-flavored SQL uses. Unlike [`Tok::object_name`], which returns the verbatim span
    /// so a rewrite can put it back byte-identical, this is for a caller that needs the identifier
    /// itself.
    fn object_name_segments(&mut self) -> Option<Vec<String>> {
        self.skip_ws_comments();
        let (b, n) = (self.b, self.b.len());
        let mut segments = Vec::new();
        loop {
            if self.i < n && (b[self.i] == b'`' || b[self.i] == b'"') {
                let quote = b[self.i];
                self.i += 1;
                let start = self.i;
                while self.i < n && b[self.i] != quote {
                    self.i += 1;
                }
                segments.push(self.s[start..self.i].to_string());
                if self.i < n {
                    self.i += 1; // closing quote
                }
            } else {
                let start = self.i;
                if self.i < n && (b[self.i].is_ascii_alphabetic() || b[self.i] == b'_') {
                    self.i += 1;
                    while self.i < n && (b[self.i].is_ascii_alphanumeric() || b[self.i] == b'_') {
                        self.i += 1;
                    }
                }
                if self.i == start {
                    break;
                }
                segments.push(self.s[start..self.i].to_string());
            }
            if self.i < n && b[self.i] == b'.' {
                self.i += 1;
                continue;
            }
            break;
        }
        (!segments.is_empty()).then_some(segments)
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
        // An OPTIONS key with no translation must not be dropped — it would change how the
        // table's own bytes are read.
        assert!(lower_create_table_using(
            "create table t(a int) using csv options (nullValue 'NA')",
            w
        )
        .is_none());
        // OPTIONS on a format where they mean nothing here.
        assert!(lower_create_table_using(
            "create table t(a int) using parquet options (header 'true')",
            w
        )
        .is_none());
        // A LOCATION this lowering cannot manage: object-store URLs belong to a catalog table.
        assert!(lower_create_table_using(
            "create table t(a int) using parquet location 's3://bucket/t/'",
            w
        )
        .is_none());
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

    #[test]
    fn lowers_location_partitioned_by_and_translated_options() {
        let w = Path::new("/tmp/wh");
        let low = lower_create_table_using(
            "CREATE TABLE t(a INT, d STRING) USING csv PARTITIONED BY (d) \
             LOCATION '/data/t' OPTIONS (header 'true', sep '|')",
            w,
        )
        .expect("should lower");
        assert_eq!(low.table_dir, Path::new("/data/t"));
        assert_eq!(low.partition_columns, ["d"]);
        // Spark's option names are translated to DataFusion's, not passed through: `header` alone
        // means nothing to `CREATE EXTERNAL TABLE`, and the table would read its header as data.
        assert_eq!(
            low.ddl,
            "CREATE EXTERNAL TABLE t (a INT, d STRING) STORED AS CSV PARTITIONED BY (d) \
             LOCATION '/data/t/' OPTIONS ('format.delimiter' '|', 'format.has_header' 'true')"
        );
    }

    #[test]
    fn an_explicit_location_keeps_the_required_trailing_slash() {
        let w = Path::new("/tmp/wh");
        for written in ["/data/t", "/data/t/", "file:///data/t"] {
            let low = lower_create_table_using(
                &format!("CREATE TABLE t(a INT) USING parquet LOCATION '{written}'"),
                w,
            )
            .unwrap_or_else(|| panic!("should lower: {written}"));
            // `ListingTable::insert_into` requires `is_collection()`, which is the trailing slash.
            assert!(
                low.ddl.contains("LOCATION '/data/t/'"),
                "for {written}: {}",
                low.ddl
            );
        }
    }

    #[test]
    fn a_local_ctas_takes_location_but_not_partitioning() {
        let w = Path::new("/tmp/wh");
        let ctas = lower_create_table_ctas(
            "CREATE TABLE t USING parquet LOCATION '/data/t' AS SELECT 1 AS a",
            w,
        )
        .expect("should lower");
        assert_eq!(ctas.table_dir, Path::new("/data/t"));
        assert_eq!(ctas.select_sql, "SELECT 1 AS a");
        // The local CTAS writer emits one flat file — it cannot partition, so it must not claim to.
        assert!(lower_create_table_ctas(
            "CREATE TABLE t USING parquet PARTITIONED BY (a) AS SELECT 1 AS a",
            w
        )
        .is_none());
    }

    #[test]
    fn parses_an_external_ctas_with_storage_clauses_in_any_order() {
        let ext = lower_external_ctas(
            "CREATE TABLE glue.db.t USING delta PARTITIONED BY (d, region) \
             LOCATION 's3://bucket/t/' AS SELECT a, d, region FROM src",
        )
        .expect("should parse");
        assert_eq!(ext.name, "glue.db.t");
        assert_eq!(ext.format, "delta");
        assert_eq!(ext.clauses.location.as_deref(), Some("s3://bucket/t/"));
        assert_eq!(ext.clauses.partition_columns, ["d", "region"]);
        // The clauses DataFusion cannot parse are gone, and the SELECT is untouched.
        assert_eq!(
            ext.ddl,
            "CREATE TABLE glue.db.t AS SELECT a, d, region FROM src"
        );
    }

    #[test]
    fn an_external_ctas_keeps_if_not_exists_and_lowercases_only_the_format() {
        let ext =
            lower_external_ctas("CREATE TABLE IF NOT EXISTS Glue.Db.T USING DELTA AS SELECT 1")
                .expect("should parse");
        assert_eq!(ext.ddl, "CREATE TABLE IF NOT EXISTS Glue.Db.T AS SELECT 1");
        assert_eq!(ext.format, "delta");
    }

    #[test]
    fn a_typed_partition_list_is_refused_rather_than_silently_truncated() {
        // Spark's non-CTAS `PARTITIONED BY (d DATE)` names a column AND its type. Parsing just the
        // name would produce a table partitioned differently than the DDL asked for.
        assert!(lower_external_ctas(
            "CREATE TABLE g.d.t USING delta PARTITIONED BY (d DATE) AS SELECT 1"
        )
        .is_none());
        // As does an unrecognized clause.
        assert!(lower_external_ctas(
            "CREATE TABLE g.d.t USING delta CLUSTERED BY (a) INTO 4 BUCKETS AS SELECT 1"
        )
        .is_none());
    }

    #[test]
    fn a_non_ctas_or_unknown_format_is_left_alone() {
        // An explicit column list means this is not CTAS.
        assert!(lower_external_ctas("CREATE TABLE g.d.t (a INT) USING delta").is_none());
        assert!(lower_external_ctas("CREATE TABLE g.d.t USING avro AS SELECT 1").is_none());
        assert!(lower_external_ctas("SELECT 1").is_none());
    }

    #[test]
    fn insert_target_normalizes_every_quoting_style() {
        // Backticks (Spark), double quotes (DataFusion-flavored), and bare — all have to reach
        // `refresh_table` as the same segments, or an insert stays invisible until the cache TTL.
        for sql in [
            "INSERT INTO cat.ns.t SELECT 1",
            "INSERT INTO `cat`.`ns`.`t` SELECT 1",
            "INSERT INTO \"cat\".ns.\"t\" SELECT 1",
            "INSERT OVERWRITE TABLE cat.ns.t SELECT 1",
            "INSERT OVERWRITE INTO cat.ns.t SELECT 1",
        ] {
            assert_eq!(
                insert_target(sql).as_deref(),
                Some("`cat`.`ns`.`t`"),
                "for: {sql}"
            );
        }
        assert_eq!(insert_target("SELECT 1"), None);
    }

    #[test]
    fn only_an_unquoted_catalog_qualified_insert_target_is_offered_for_quoting() {
        assert_eq!(
            insert_target_catalog("INSERT INTO local.live.t SELECT 1"),
            Some((12, "local".to_string()))
        );
        // Already quoted, so there is nothing to fix.
        assert_eq!(
            insert_target_catalog("INSERT INTO `local`.live.t SELECT 1"),
            None
        );
        // Two segments cannot name a catalog.
        assert_eq!(insert_target_catalog("INSERT INTO live.t SELECT 1"), None);
    }

    #[test]
    fn options_accept_sparks_separator_less_spelling() {
        let ext = lower_external_ctas(
            "CREATE TABLE g.d.t USING csv OPTIONS ('header' 'true', delimiter = ';') AS SELECT 1",
        )
        .expect("should parse");
        assert_eq!(
            ext.clauses.options.get("header").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            ext.clauses.options.get("delimiter").map(String::as_str),
            Some(";")
        );
    }
}
