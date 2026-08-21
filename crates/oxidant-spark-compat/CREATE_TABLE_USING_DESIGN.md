# CREATE TABLE … USING — faithful lowering design

> **STATUS (2026-06-26, iteration 2): the non-CTAS subset LANDED** in
> `crates/oxidant-loom/src/spark_create_table.rs` (`CREATE TABLE [IF NOT EXISTS] name (cols) USING
> {parquet|orc|csv|json}` → real `CREATE EXTERNAL TABLE … STORED AS <fmt> LOCATION '<warehouse>/name/'`;
> INSERT returns empty for Spark's `struct<>`). missing-relation 2,572→900. **Still deferred** (the
> remaining work this doc specifies): CTAS (`USING fmt AS SELECT`), `PARTITIONED BY`, `OPTIONS`/`LOCATION`,
> `IDENTIFIER(...)` names, exotic column types (varchar(n)/timestamp_ntz/nested struct) — each returns
> `None`, keeping the statement failing as before (no regression).

> Produced by the parity coordinator swarm (design agent, 2026-06-26). This is the **biggest
> remaining cascade lever**: ~922 `missing-relation` rows are downstream of failed
> `CREATE TABLE … USING` setup statements, plus ~34+ direct `parser-unsupported` `found: USING`.
> The faithfulness rule FORBIDS the lossy `USING`-strip shim (turns a persistent format-backed
> table into an in-memory MemTable). The fix lowers to **real format-backed storage**.

## Verdict
- **feasible_this_iteration:** True
- **recommended_scope:** GO for a focused subset this iteration: the NON-CTAS path `CREATE TABLE [IF NOT EXISTS] name (col_defs) USING {parquet|orc|csv|json} [PARTITIONED BY (cols)] [drop trailing COMMENT/TBLPROPERTIES]` lowered to `CREATE EXTERNAL TABLE`, bundled with MANDATORY INSERT count-row suppression. DEFER within the same module (follow-on): CTAS (`USING fmt AS SELECT …` via COPY-then-CREATE EXTERNAL), partitioned CTAS, inline OPTIONS, and exotic column types (varchar(n), timestamp_ntz, nested struct). The deferred items keep failing exactly as today (no regression). This subset already covers the bulk: 62 files / 4,151 blocks / 781 INSERTs.
- **estimated_unlock:** Large — the single biggest corpus lever. Certain core: the ~62 CREATE blocks + 781 INSERT blocks (currently all failing: parser-unsupported / missing-relation) flip to byte-exact `struct<>` STRICT passes once the table is real and the INSERT count is suppressed — realistically ~70-85% succeed (varchar/exotic-type CREATEs and a few INSERTs excepted) ≈ +600 to +800 strict from these trivial blocks ALONE. Plus a few hundred of the ~922 downstream missing-relation SELECTs become correct (strict where naming/types align, otherwise semantic via schema-only). Headline: strict +600 to +1000, semantic +400 to +900 incremental — consistent with ROADMAP's ~900 create-table-using ceiling, with the honest caveat that a big share of the strict gain is mechanical CREATE/INSERT `struct<>` blocks rather than complex query results.
- **files_touched:** ['crates/oxidant-loom/src/lib.rs', 'crates/oxidant-loom/src/spark_create_table.rs']
- **risks:** See risks field above.

## Design
PROBLEM CONFIRMED. sqlparser 0.62 (Databricks dialect) does not consume the Spark `USING <provider>` clause in CREATE TABLE — `parse_create_table` (sqlparser parser/mod.rs:8467-8661) never reads `Keyword::USING`, so `CREATE TABLE t(a int) USING parquet` fails at parse ("found: USING", bucket parser-unsupported) before any planning, and every downstream statement errors "table not found" (bucket missing-relation). DataFusion's own DFParser only special-cases CREATE EXTERNAL/UNBOUNDED EXTERNAL, so there is no AST to intercept — the lowering must be a pre-`ctx.sql()` textual rewrite producing DataFusion's `CREATE EXTERNAL TABLE`.

WHY THE TARGET IS FAITHFUL (contract-allowed, not the forbidden shim). DataFusion 54 `CREATE EXTERNAL TABLE name (cols) STORED AS <fmt> LOCATION '<dir>/'` resolves through ListingTableFactory::create (datafusion-54.0.0/src/datasource/listing_table_factory.rs:51). When a column list is supplied it takes the provided-schema branch (lines 116-141) and does NOT call infer_schema — so an empty/managed dir is fine. The resulting ListingTable is REAL format-backed storage: ListingTable::insert_into (datafusion-catalog-listing-54.0.0/src/table.rs:614) writes actual <fmt> files to LOCATION via the format writer, and scans read them back. Data is durable on disk in the declared format; this is an EQUIVALENT DataFusion plan for Spark's managed format-backed table — exactly the ALLOWED "lower Spark syntax to an equivalent DataFusion plan", and the polar opposite of the FORBIDDEN MemTable shim (no silent in-memory downgrade).

TWO LOAD-BEARING DETAILS from the DF source:
1) insert_into requires table_path.is_collection() == true (table.rs:625) → LOCATION MUST end with a trailing '/'.
2) An empty-table SELECT lists the dir; to guarantee it returns empty rather than erroring on a missing path, the engine must create_dir_all(table_dir) at CREATE time (this is also what a real warehouse does on CREATE TABLE — faithful side effect).

THE INSERT-COUNT TRAP (mandatory, co-shipped). 781 INSERT statements live in these 62 files. DataFusion's INSERT returns a one-row `count` result (schema `struct<count:bigint>`), but every Spark golden for INSERT is `struct<>` + empty (verified: results/null-handling.sql.out). Through classify.rs (attribute_value_diff, classify.rs:163/195) a `count` row vs empty golden lands in **Correctness** — so naively enabling the feature would move ~781 blocks missing-relation→correctness, breaking the "never let correctness rise" guardrail. The fix is also the faithful behavior: Spark's `spark.sql("INSERT …")` returns an empty DataFrame. So the engine must execute the write (collect for side effects) but return `vec![]` for INSERT, which runner.rs renders as `struct<>` (it is non-read-only, so the empty-batch branch emits struct<>+""). This simultaneously removes the regression AND flips all 781 INSERTs to strict passes.

FILE-LEVEL CHANGES.
A) NEW crates/oxidant-loom/src/spark_create_table.rs (sketch provided in proposed_new_files). Pure, string/comment/backtick-aware tokenizer (same defensive scanning style as lib.rs::rewrite_spark_typed_literals):
   - lower_create_table_using(sql, warehouse: &Path) -> Option<Lowered>: matches `CREATE TABLE [IF NOT EXISTS] <name> ( <cols> ) USING <fmt> <tail>`; recognizes fmt ∈ {parquet,orc,csv,json} (case-insensitive); captures the column-list span VERBATIM via a quote-aware balanced-paren scan (decimal(38,18) nests cleanly; array<…>/struct<…> use <> not parens). Tail handling for the MVP: empty/`;` → emit bare external table; leading `PARTITIONED BY (p)` → carry through; trailing table-level `COMMENT '…'`/`TBLPROPERTIES(…)` → drop (metadata only, data-faithful); leading `AS` (CTAS) → return None this iteration (defer). Emits ddl = `CREATE EXTERNAL TABLE [IF NOT EXISTS] <name> (<cols>) STORED AS <FMT> [PARTITIONED BY (p)] LOCATION '<warehouse>/<sanitized-name>/'` and table_dir = <warehouse>/<sanitized-name>. Returns None (untouched, no regression) on anything unrecognized.
   - is_insert(sql) -> bool: leading-keyword INSERT detection after skipping leading whitespace/comments.
B) crates/oxidant-loom/src/lib.rs:
   - add `mod spark_create_table;`
   - Engine gains `warehouse: PathBuf`; Engine::new() (lib.rs:388) sets it to a process+atomic-unique dir under std::env::temp_dir().join("oxidant-warehouse"); add `impl Drop for Engine { fn drop … remove_dir_all(&self.warehouse) }` for cleanup (each file = one Engine, so per-engine isolation avoids cross-file table-name collisions like `t1`).
   - Engine::sql (lib.rs:447) gets two guarded branches BEFORE plan_spark:
       if let Some(low) = spark_create_table::lower_create_table_using(query, &self.warehouse) { std::fs::create_dir_all(&low.table_dir)?; let ddl = normalize_spark_sql(&low.ddl); self.ctx.sql(ddl.as_ref()).await.map_err(Error::Plan)?.collect().await.map_err(Error::Execution)?; return Ok(vec![]); }
       if spark_create_table::is_insert(query) { let q = normalize_spark_sql(query); self.ctx.sql(q.as_ref()).await.map_err(Error::Plan)?.collect().await.map_err(Error::Execution)?; return Ok(vec![]); }
     (Engine::schema is untouched — runner.rs::is_read_only never routes CREATE/INSERT there, and project_spark_names already no-ops on DDL/DML roots: spark_names.rs:46.)
C) No harness changes. runner.rs/classify.rs already render empty batches from a non-read-only stmt as `struct<>`+"" — which is the byte-exact Spark golden for both CREATE and INSERT blocks.

TYPE COVERAGE (bounded, verified in datafusion-sql-54 planner.rs:690): int/bigint/smallint/tinyint→Int*, string/char(n)→Utf8, boolean, double/float, date, decimal(p,s) all convert. varchar(n) hits not_impl ("Varchar with length"), timestamp_ntz/nested-struct may be Custom→not_impl — these CREATEs simply fail as today (charvarchar/collations/timestamp-ntz long tail), no regression.

VALIDATION PLAN (read-only here; for the implementer): cargo run -p oxidant-spark-compat --bin oxidant-parity -- golden, then oxidant-parity file null-handling.sql.out (expect CREATE+INSERT blocks now PASS, SELECTs now produce rows); diff buckets vs parity/baseline.json and CONFIRM correctness/missing-error/null-semantics/decimal-precision/datetime did NOT rise (watch postgreSQL/insert.sql for expected-error INSERTs that oxidant may accept too leniently — that is a pre-existing leniency, separate lever). Re-baseline only if the ratchet holds/raises.

## Proposed implementation sketch — `crates/oxidant-loom/src/spark_create_table.rs`

(NOT yet wired into the tree; reference for the impl swarm.)

```rust
//! DESIGN SKETCH (needs compile verification before landing) — faithful lowering of Spark's
//! `CREATE TABLE … USING <fmt>` DDL to DataFusion's real, format-backed `CREATE EXTERNAL TABLE`.
//!
//! sqlparser 0.62 does not consume Spark's `USING <provider>` clause, and DataFusion's DFParser
//! only special-cases CREATE EXTERNAL/UNBOUNDED EXTERNAL, so `CREATE TABLE t(a int) USING parquet`
//! fails at parse. We rewrite it (pre-`ctx.sql()`) to
//!   `CREATE EXTERNAL TABLE t (a int) STORED AS PARQUET LOCATION '<warehouse>/t/'`
//! which DataFusion plans into a ListingTable backed by real files at LOCATION — genuine
//! format-backed storage (INSERT writes <fmt> files; SELECT reads them). This is the contract's
//! ALLOWED "equivalent DataFusion plan", NOT the forbidden MemTable shim.
//!
//! Two load-bearing details from the DF source: LOCATION must end in '/' (ListingTable::insert_into
//! requires is_collection()), and the dir must exist before an empty-table SELECT (the caller
//! create_dir_all's `table_dir`).
//!
//! Scope (MVP): non-CTAS `CREATE TABLE [IF NOT EXISTS] name (cols) USING {parquet|orc|csv|json}
//! [PARTITIONED BY (cols)]`, dropping trailing table-level COMMENT/TBLPROPERTIES (metadata, data-
//! faithful). CTAS (`AS SELECT`) returns None here (defer to a COPY-then-CREATE-EXTERNAL follow-on).

use std::path::{Path, PathBuf};

/// A lowered, ready-to-execute external-table DDL plus the managed directory to create first.
pub(crate) struct Lowered {
    pub ddl: String,
    pub table_dir: PathBuf,
}

const FORMATS: &[&str] = &["parquet", "orc", "csv", "json"];

/// Return the lowering for a recognized `CREATE TABLE … USING <fmt>` (non-CTAS), else None
/// (statement is left byte-identical for the normal path — never a regression).
pub(crate) fn lower_create_table_using(sql: &str, warehouse: &Path) -> Option<Lowered> {
    let mut t = Tok::new(sql);
    t.kw("create")?;
    t.kw("table")?;
    let if_not_exists = t.opt_kw3("if", "not", "exists");
    let name = t.object_name()?;          // qualified/backtick-aware, verbatim span
    let cols = t.balanced_parens();        // Option<&str> verbatim `( … )` incl. parens, or None
    t.kw("using")?;
    let fmt = t.ident()?;                   // format token
    let fmt_l = fmt.to_ascii_lowercase();
    if !FORMATS.contains(&fmt_l.as_str()) {
        return None;
    }
    // Tail: accept end-of-stmt, an optional `PARTITIONED BY (…)`, and drop table-level
    // COMMENT '…' / TBLPROPERTIES(…). Bail (None) on `AS` (CTAS) or anything unrecognized.
    let partitioned = t.opt_partitioned_by(); // Option<&str> the `(cols)` span, or None
    if t.peek_kw("as") {
        return None; // CTAS deferred
    }
    t.skip_trailing_comment_and_tblproperties();
    if !t.at_end() {
        return None; // unknown tail — do not risk a lossy rewrite
    }

    let dir_name = sanitize(&name);
    let table_dir = warehouse.join(&dir_name);
    // Trailing slash is REQUIRED so ListingTable treats LOCATION as a collection (insertable).
    let location = format!("{}/", table_dir.display());
    let cols_sql = cols.unwrap_or("()"); // explicit schema path; CTAS (no cols) is excluded above
    let ine = if if_not_exists { "IF NOT EXISTS " } else { "" };
    let part = partitioned
        .map(|p| format!(" PARTITIONED BY {p}"))
        .unwrap_or_default();
    let ddl = format!(
        "CREATE EXTERNAL TABLE {ine}{name} {cols_sql} STORED AS {} {part} LOCATION '{location}'",
        fmt_l.to_uppercase()
    );
    Some(Lowered { ddl, table_dir })
}

/// Leading-keyword INSERT detector (skips leading whitespace/`--`/`/* */`). Used by Engine::sql to
/// run the write but return empty batches, matching Spark (DataFusion's INSERT count-row is dropped).
pub(crate) fn is_insert(sql: &str) -> bool {
    let mut t = Tok::new(sql);
    t.peek_kw("insert")
}

fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            '`' | '"' => '_',
            '.' => '_',
            c if c.is_alphanumeric() || c == '_' || c == '-' => c,
            _ => '_',
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Tok: a minimal, quote-/comment-aware cursor. SKETCH — the methods below are the
// contract the implementation must satisfy; bodies are illustrative, not final.
// Reuse the scanning discipline already proven in lib.rs::rewrite_spark_typed_literals
// (single/double-quote, backtick, --, /* */ all copied/skipped verbatim).
// ---------------------------------------------------------------------------
struct Tok<'a> {
    s: &'a str,
    i: usize,
}
impl<'a> Tok<'a> {
    fn new(s: &'a str) -> Self {
        Self { s, i: 0 }
    }
    fn skip_ws_comments(&mut self) { /* skip whitespace, --… , /* … */ */ }
    /// Consume the keyword case-insensitively at the cursor (after ws/comments); None if absent.
    fn kw(&mut self, _k: &str) -> Option<()> { /* … */ Some(()) }
    fn opt_kw3(&mut self, _a: &str, _b: &str, _c: &str) -> bool { false }
    fn peek_kw(&mut self, _k: &str) -> bool { false }
    /// A (possibly qualified, possibly backticked) object name, returned as its verbatim span.
    fn object_name(&mut self) -> Option<String> { None }
    /// A bare identifier token (e.g. the format), verbatim.
    fn ident(&mut self) -> Option<String> { None }
    /// If the cursor is at '(', return the whole balanced `( … )` span (paren-depth aware,
    /// ignoring parens inside quotes/backticks), else None. decimal(38,18) nests fine.
    fn balanced_parens(&mut self) -> Option<&'a str> { None }
    /// `PARTITIONED BY ( … )` → return the `( … )` span; else None.
    fn opt_partitioned_by(&mut self) -> Option<&'a str> { None }
    /// Drop a trailing table-level COMMENT '…' and/or TBLPROPERTIES( … ) (metadata only).
    fn skip_trailing_comment_and_tblproperties(&mut self) {}
    /// True if only whitespace/comments/`;` remain.
    fn at_end(&mut self) -> bool { true }
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
        assert!(l.ddl.starts_with("CREATE EXTERNAL TABLE t1 (a int, b int, c int) STORED AS PARQUET"));
        assert!(l.ddl.contains("LOCATION '/tmp/wh/t1/'"));
        assert_eq!(l.table_dir, w.join("t1"));
    }

    #[test]
    fn passes_through_non_using_and_ctas() {
        let w = Path::new("/tmp/wh");
        assert!(lower_create_table_using("CREATE TABLE t(a INT)", w).is_none());
        assert!(lower_create_table_using("select 1", w).is_none());
        // CTAS deferred this iteration
        assert!(lower_create_table_using("create table t using parquet as select 1", w).is_none());
    }

    #[test]
    fn detects_insert() {
        assert!(is_insert("insert into t1 values(1,0,0)"));
        assert!(is_insert("  -- c\n INSERT INTO t SELECT * FROM s"));
        assert!(!is_insert("select * from t"));
    }
}

```
