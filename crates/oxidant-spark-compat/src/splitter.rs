//! `.sql` input-file helpers.
//!
//! The golden `.sql.out` files already enumerate every statement, so the *comparison* path
//! (see [`crate::runner`]) drives off the golden blocks and never needs to re-implement
//! Spark's statement splitter. This module exists for the secondary concerns the golden file
//! can't express on its own: which input files we must **skip** (because they need machinery
//! oxidant doesn't have yet — registered UDFs), resolving `--IMPORT` setup chains, and surfacing
//! the directives so skips are explicit and counted, never silent.

use std::collections::HashSet;
use std::path::Path;

/// Why an input file is skipped for now (recorded in the report, not dropped silently).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    /// Uses `udf(...)` wrappers that require a registered Python/Scala/Java UDF.
    RequiresUdf,
    /// The input file carries an explicit `--SKIP <reason>` directive (used by the
    /// authored Databricks corpus for statements that need machinery oxidant does not
    /// have yet, e.g. Delta storage or a Lake Formation catalog).
    Marked(String),
}

impl SkipReason {
    pub fn as_str(&self) -> &str {
        match self {
            SkipReason::RequiresUdf => "requires-udf-registration",
            SkipReason::Marked(reason) => reason,
        }
    }
}

/// Decide whether an input file is runnable by the golden-replay path today.
pub fn skip_reason(input_sql: &str) -> Option<SkipReason> {
    for l in input_sql.lines() {
        let t = l.trim_start();
        if let Some(reason) = t
            .strip_prefix("--SKIP")
            .filter(|rest| rest.is_empty() || rest.starts_with(char::is_whitespace))
        {
            let reason = reason.trim();
            let reason = if reason.is_empty() {
                "marked-skip".to_string()
            } else {
                reason.to_string()
            };
            return Some(SkipReason::Marked(reason));
        }
        if !t.starts_with("--") && l.contains("udf(") {
            return Some(SkipReason::RequiresUdf);
        }
    }
    None
}

/// Collect setup SQL statements to execute before replaying golden blocks for a file that uses
/// `--IMPORT`. Returns statements from imported files (transitively) plus any non-import,
/// non-comment SQL from the file itself (e.g. `SET` directives, `CREATE VIEW` setup).
pub fn setup_statements(inputs_root: &Path, rel_input: &str) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    collect_setup(inputs_root, rel_input, &mut seen, &mut out);
    out
}

fn collect_setup(
    inputs_root: &Path,
    rel_input: &str,
    seen: &mut HashSet<String>,
    out: &mut Vec<String>,
) {
    if !seen.insert(rel_input.to_string()) {
        return;
    }
    let path = inputs_root.join(rel_input);
    let Ok(text) = std::fs::read_to_string(&path) else {
        return;
    };

    // Process imports first (depth-first), then local statements.
    let mut local_lines = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim_start();
        if let Some(import) = trimmed.strip_prefix("--IMPORT") {
            let import_path = import.trim().trim_start_matches("./");
            collect_setup(inputs_root, import_path, seen, out);
            continue;
        }
        if trimmed.starts_with("--SET ") {
            // Spark test directive: `--SET key=value` → `SET key=value`.
            let kv = trimmed.trim_start_matches("--SET ").trim();
            out.push(format!("SET {kv}"));
            continue;
        }
        if trimmed.starts_with("--") {
            continue;
        }
        local_lines.push(line);
    }
    let local_sql = local_lines.join("\n");
    for stmt in split_statements(&local_sql) {
        let s = stmt.trim();
        if !s.is_empty() {
            out.push(s.to_string());
        }
    }
}

/// Split SQL on semicolons outside of quotes (minimal, sufficient for test setup files).
fn split_statements(sql: &str) -> Vec<String> {
    let mut stmts = Vec::new();
    let mut cur = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    for ch in sql.chars() {
        match ch {
            '\'' if !in_double && !in_backtick => in_single = !in_single,
            '"' if !in_single && !in_backtick => in_double = !in_double,
            '`' if !in_single && !in_double => in_backtick = !in_backtick,
            ';' if !in_single && !in_double && !in_backtick => {
                if !cur.trim().is_empty() {
                    stmts.push(cur.trim().to_string());
                }
                cur.clear();
                continue;
            }
            _ => {}
        }
        cur.push(ch);
    }
    if !cur.trim().is_empty() {
        stmts.push(cur.trim().to_string());
    }
    stmts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_udf_files() {
        let sql = "-- a comment\nSELECT udf(a) FROM t;";
        assert_eq!(skip_reason(sql), Some(SkipReason::RequiresUdf));
    }

    #[test]
    fn detects_import_files() {
        let sql = "--IMPORT subquery/setup.sql\nSELECT 1;";
        assert_eq!(skip_reason(sql), None);
    }

    #[test]
    fn plain_files_run() {
        assert_eq!(skip_reason("SELECT 1; SELECT 2;"), None);
    }

    #[test]
    fn import_files_no_longer_skipped() {
        assert_eq!(skip_reason("--IMPORT subquery/setup.sql\nSELECT 1;"), None);
    }

    use std::path::PathBuf;

    #[test]
    fn resolves_import_setup() {
        let root = PathBuf::from(crate::CORPUS_DIR).join("inputs");
        let stmts = setup_statements(&root, "binary_hex.sql");
        assert!(stmts.iter().any(|s| s.contains("binaryOutputStyle")));
        assert!(stmts.iter().any(|s| s.starts_with("SELECT")));
    }

    #[test]
    fn udf_in_comment_does_not_trip() {
        assert_eq!(skip_reason("-- mentions udf( in prose\nSELECT 1;"), None);
    }

    #[test]
    fn skip_directive_records_reason() {
        let sql = "--SKIP requires-delta-storage\nCOPY INTO t FROM 's3://b/p';";
        assert_eq!(
            skip_reason(sql),
            Some(SkipReason::Marked("requires-delta-storage".into()))
        );
    }

    #[test]
    fn bare_skip_directive_has_default_reason() {
        assert_eq!(
            skip_reason("--SKIP\nSELECT 1;"),
            Some(SkipReason::Marked("marked-skip".into()))
        );
    }

    #[test]
    fn skip_prefix_without_a_separator_is_not_a_directive() {
        assert_eq!(skip_reason("--SKIPPING is only prose\nSELECT 1;"), None);
    }

    #[test]
    fn skip_directive_in_prose_does_not_trip() {
        assert_eq!(
            skip_reason("-- this is not a --SKIP directive\nSELECT 1;"),
            None
        );
    }
}
