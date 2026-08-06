//! The Spark-SQL → DataFusion dialect shim.
//!
//! Oxidant executes SQL on DataFusion, whose dialect is close to but not identical to Spark SQL. This
//! module rewrites the well-defined, safe differences so existing Spark/Databricks SQL runs
//! unchanged — the "drop-in" migration promise. Per the plan it's **incremental and
//! test-corpus-driven**: a small set of correct rewrites beats a broad, buggy one, and the
//! rewriter is **string-literal-aware** so it never touches the contents of a `'...'` literal.
//!
//! v1 rewrites:
//! - **Backtick identifiers** — Spark's `` `my col` `` → ANSI double-quoted `"my col"` (DataFusion's
//!   identifier quoting). The #1 source of migration friction.
//!
//! Function/semantic rewrites (`from_unixtime`, `date_format`, lateral-view `explode`, …) are the
//! next entries; each lands with corpus tests. A `spark-compat` toggle (off → pass-through) gates
//! the shim so users can opt out.

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
