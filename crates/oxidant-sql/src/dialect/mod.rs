//! The Spark-SQL → DataFusion dialect layer.
//!
//! Oxidant executes SQL on DataFusion, whose dialect is close to but not identical to Spark SQL. This
//! module rewrites the well-defined, safe differences so existing Spark/Databricks SQL runs
//! unchanged — the "drop-in" migration promise. Per the plan it's **incremental and
//! test-corpus-driven**: a small set of correct rewrites beats a broad, buggy one, and the
//! rewriter is **string-literal-aware** so it never touches the contents of a `'...'` literal.
//!
//! # The staged pipeline
//!
//! [`DialectPipeline`] (see [`pipeline`]) lowers one SQL statement through three additive
//! stages, each a registry of independent rules (design: `oxidant-spark-compat/ROADMAP.md` §2):
//!
//! 1. **String prefilter** ([`str_rule`], pre-parse) — verified-faithful, purely-lexical,
//!    leading-keyword rewrites only (e.g. [`str_rule::StripTemporaryView`], migrated from
//!    `oxidant-loom::normalize_spark_sql`).
//! 2. **Statement intercepts** ([`intercept`], post-parse) — AST-level rules that see a
//!    `sqlparser` `Statement` parsed with the Databricks dialect and either fully own the
//!    statement or pass it through. Home for `USE`, `LIKE ANY/ALL`, `PIVOT`/`UNPIVOT`,
//!    `SHOW`/`DESCRIBE` (later tickets).
//! 3. **Output naming** ([`naming`], post-plan) — renames anonymous result columns to
//!    Spark's `Expression.sql` names via a registry of per-expression naming rules.
//!
//! Rule contract (all stages): **either a rule fires and fully owns its target, or it returns
//! `None` and the next rule sees the input unchanged.** Stage 1 hands every rule the *original*
//! SQL text; Stage 2 parses the Stage-1-rewritten text and hands every intercept the same
//! `Statement`; Stage 3 scans each output expression independently. Ordering is by
//! specificity; two rules claiming the same statement or expression is a hard error in debug
//! builds and first-wins in release, so additions stay conflict-free by construction.
//!
//! Production today calls [`DialectPipeline::rewrite_str`] only (Stage 1). Stages 2 and 3 are
//! scaffolded registries — wiring [`DialectPipeline::lower`] / [`DialectPipeline::apply_output_naming`]
//! into the engine is tracked in follow-up tickets (e.g. KAN-96 USE).
//!
//! Standalone rewrites (not yet pipeline stages):
//! - **Backtick identifiers** — Spark's `` `my col` `` → ANSI double-quoted `"my col"` (DataFusion's
//!   identifier quoting), via [`to_datafusion_sql`]. The #1 source of migration friction. This is a
//!   whole-statement token pass rather than a leading-keyword one, so it cannot become a Stage-1
//!   rule under the fire-and-own contract: it would claim statements an owning rule also claims
//!   (e.g. ``CREATE TEMPORARY VIEW `v` …``). Composing non-owning lexical passes with owning ones
//!   needs a contract extension, which is deliberately out of scope here.

pub mod intercept;
pub mod naming;
pub mod pipeline;
pub mod str_rule;

use std::sync::OnceLock;

pub use pipeline::{DialectPipeline, LowerOutcome};

/// The process-wide [`DialectPipeline::spark`] instance. The pipeline is stateless across
/// statements, so the engine builds it once and borrows it per query.
pub fn spark_pipeline() -> &'static DialectPipeline {
    static PIPELINE: OnceLock<DialectPipeline> = OnceLock::new();
    PIPELINE.get_or_init(DialectPipeline::spark)
}

/// Rewrite a Spark-SQL statement into a DataFusion-compatible one. Conservative: anything not in
/// the known-safe rewrite set is passed through verbatim.
pub fn to_datafusion_sql(spark_sql: &str) -> String {
    rewrite_backtick_identifiers(spark_sql)
}

/// Replace `` `ident` `` with `"ident"` outside of string literals. Single-quoted literals (with
/// `''` escaping) and existing double-quoted identifiers are passed through untouched. A doubled
/// backtick `` `` `` inside a backtick-quoted identifier is a literal backtick (Spark rule) and is
/// emitted as `` `` `` inside the resulting double-quoted identifier-escaped form (`""`).
fn rewrite_backtick_identifiers(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            // Pass single-quoted string literals through verbatim (respecting '' escapes).
            '\'' => {
                out.push('\'');
                while let Some(ch) = chars.next() {
                    out.push(ch);
                    if ch == '\'' {
                        // Doubled '' is an escaped quote — consume the second and continue.
                        if chars.peek() == Some(&'\'') {
                            out.push(chars.next().unwrap());
                        } else {
                            break;
                        }
                    }
                }
            }
            // Pass existing double-quoted identifiers through verbatim.
            '"' => {
                out.push('"');
                for ch in chars.by_ref() {
                    out.push(ch);
                    if ch == '"' {
                        break;
                    }
                }
            }
            // Backtick-quoted identifier → double-quoted identifier.
            '`' => {
                out.push('"');
                while let Some(ch) = chars.next() {
                    if ch == '`' {
                        // Doubled `` is a literal backtick within the identifier.
                        if chars.peek() == Some(&'`') {
                            chars.next();
                            out.push('`');
                        } else {
                            break;
                        }
                    } else if ch == '"' {
                        // Escape a double-quote that appears inside the identifier name.
                        out.push_str("\"\"");
                    } else {
                        out.push(ch);
                    }
                }
                out.push('"');
            }
            other => out.push(other),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backtick_identifier_becomes_double_quoted() {
        assert_eq!(
            to_datafusion_sql("SELECT `my col` FROM `db`.`tbl`"),
            r#"SELECT "my col" FROM "db"."tbl""#
        );
    }

    #[test]
    fn string_literals_are_untouched() {
        // Backticks inside a string literal must NOT be rewritten.
        assert_eq!(
            to_datafusion_sql("SELECT '`not an ident`' AS x"),
            "SELECT '`not an ident`' AS x"
        );
        // Doubled-quote escapes inside a literal are preserved.
        assert_eq!(
            to_datafusion_sql("SELECT 'it''s fine', `c`"),
            r#"SELECT 'it''s fine', "c""#
        );
    }

    #[test]
    fn plain_sql_passes_through() {
        let sql = "SELECT a, b FROM t WHERE a > 1 GROUP BY a";
        assert_eq!(to_datafusion_sql(sql), sql);
    }

    #[test]
    fn existing_double_quotes_preserved() {
        let sql = r#"SELECT "already quoted" FROM t"#;
        assert_eq!(to_datafusion_sql(sql), sql);
    }

    #[test]
    fn doubled_backtick_is_literal_backtick() {
        // Spark: `a``b` is the identifier `a`b`. → "a`b"
        assert_eq!(to_datafusion_sql("SELECT `a``b`"), "SELECT \"a`b\"");
    }
}
