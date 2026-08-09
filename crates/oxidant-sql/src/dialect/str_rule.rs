//! Stage 1 — string prefilter registry (pre-parse).
//!
//! Home for **verified-faithful, purely-lexical, leading-keyword** rewrites only. A rule is
//! gated on a leading-token match so it can never fire inside literals or joins, and it must
//! leave the statement body byte-for-byte intact. Anything that needs quoting/comment/`;`
//! awareness beyond a leading token belongs to the Stage-2 AST intercepts
//! ([`crate::dialect::intercept`]) instead — per the roadmap, `USE` was rejected here precisely
//! because a whitespace tokenizer gets those wrong.

use std::borrow::Cow;

/// A pre-parse string rewrite rule. See the module docs for the contract.
pub trait StrRule: Send + Sync {
    /// Stable rule name, used in conflict diagnostics.
    fn name(&self) -> &'static str;

    /// Return the rewritten statement when this rule claims it, or `None` to pass the
    /// original input through to the next rule unchanged.
    fn try_rewrite<'a>(&self, sql: &'a str) -> Option<Cow<'a, str>>;
}

/// `CREATE [OR REPLACE] [GLOBAL] TEMP[ORARY] VIEW …` → `CREATE [OR REPLACE] VIEW …`.
///
/// Spark temporary views are *session*-scoped; a DataFusion session-catalog view is too, so
/// dropping `TEMPORARY`/`GLOBAL` preserves the semantics within a session while letting
/// DataFusion register the view (its `create_view` rejects `temporary` and nothing else). This
/// is the single biggest Spark-parity unlock — almost every Spark SQL test opens with
/// `CREATE OR REPLACE TEMPORARY VIEW testData AS …`.
///
/// Migrated from `oxidant-loom::normalize_spark_sql` (its leading-keyword DDL rewrite); the
/// implementation is a deliberately conservative whitespace tokenizer — the known failure
/// modes of that approach (semicolons glued to tokens, comments, quoting) are all behind the
/// `VIEW` keyword in the body, which this rule never touches.
pub struct StripTemporaryView;

impl StrRule for StripTemporaryView {
    fn name(&self) -> &'static str {
        "strip-temporary-view"
    }

    fn try_rewrite<'a>(&self, sql: &'a str) -> Option<Cow<'a, str>> {
        strip_temporary_view(sql).map(Cow::Owned)
    }
}

/// If `query` begins with `CREATE [OR REPLACE] [GLOBAL] TEMP[ORARY] VIEW`, return the same
/// statement with `GLOBAL TEMP[ORARY]` removed; otherwise `None` (leave the query untouched).
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_temporary_view_variants() {
        assert_eq!(
            strip_temporary_view("CREATE TEMPORARY VIEW t AS SELECT 1 a").as_deref(),
            Some("CREATE VIEW t AS SELECT 1 a")
        );
        assert_eq!(
            strip_temporary_view("CREATE OR REPLACE TEMPORARY VIEW t AS SELECT 1 a").as_deref(),
            Some("CREATE OR REPLACE VIEW t AS SELECT 1 a")
        );
        assert_eq!(
            strip_temporary_view("create global temporary view t as select 1").as_deref(),
            Some("CREATE VIEW t as select 1")
        );
        // Spark's TEMP abbreviation.
        assert_eq!(
            strip_temporary_view("CREATE TEMP VIEW df AS SELECT 1").as_deref(),
            Some("CREATE VIEW df AS SELECT 1")
        );
        // Leading whitespace and mixed case are preserved/ tolerated; the body is verbatim.
        assert_eq!(
            strip_temporary_view("  Create Temporary View v As Select 2").as_deref(),
            Some("  CREATE VIEW v As Select 2")
        );
    }

    #[test]
    fn preserves_the_statement_body_verbatim() {
        // Column list, backticks, and a literal that itself spells the stripped keywords: the
        // rule owns only the leading keywords, so everything from the view name on is byte-identical.
        assert_eq!(
            strip_temporary_view("CREATE GLOBAL TEMP VIEW v(a,b) AS VALUES (1,2)").as_deref(),
            Some("CREATE VIEW v(a,b) AS VALUES (1,2)")
        );
        assert_eq!(
            strip_temporary_view(
                "CREATE TEMPORARY VIEW `my view` AS SELECT 'CREATE TEMPORARY VIEW x' AS s"
            )
            .as_deref(),
            Some("CREATE VIEW `my view` AS SELECT 'CREATE TEMPORARY VIEW x' AS s")
        );
        // Newlines/tabs between the leading keywords tokenize like spaces.
        assert_eq!(
            strip_temporary_view("CREATE\n\tTEMPORARY\n\tVIEW v AS SELECT 1").as_deref(),
            Some("CREATE VIEW v AS SELECT 1")
        );
    }

    #[test]
    fn rewriting_is_idempotent() {
        // The output is a plain `CREATE VIEW`, which the rule declines — so re-running the
        // Stage-1 registry over an already-lowered statement is a no-op.
        let once = strip_temporary_view("CREATE OR REPLACE TEMP VIEW v AS SELECT 1").unwrap();
        assert_eq!(strip_temporary_view(&once), None);
    }

    #[test]
    fn leaves_other_statements_alone() {
        for q in [
            "CREATE VIEW t AS SELECT 1",       // already plain
            "CREATE OR REPLACE VIEW t AS s",   // persistent, OR REPLACE
            "CREATE TEMPORARY TABLE t(i int)", // not a view
            "CREATE OR TEMPORARY VIEW v AS s", // malformed: OR without REPLACE
            "CREATE TEMPORARY",                // truncated before VIEW
            "SELECT 1",
            "CREATE",
            "",
            // `TEMPORARY VIEW` in a non-leading position must never fire: the rule is gated on
            // the leading token, which is what keeps it out of literals and subqueries.
            "SELECT 'CREATE TEMPORARY VIEW v AS SELECT 1'",
            "WITH t AS (SELECT 1) CREATE TEMPORARY VIEW v AS SELECT 1",
        ] {
            assert_eq!(strip_temporary_view(q), None, "should not rewrite: {q}");
        }
    }

    #[test]
    fn rule_adapts_function_result() {
        let rule = StripTemporaryView;
        assert_eq!(rule.name(), "strip-temporary-view");
        assert_eq!(
            rule.try_rewrite("CREATE TEMP VIEW v AS SELECT 1")
                .as_deref(),
            Some("CREATE VIEW v AS SELECT 1")
        );
        // Declining returns `None` — never a `Cow::Owned` copy of the input, so the pipeline
        // can distinguish "claimed" from "passed through".
        assert!(rule.try_rewrite("SELECT 1").is_none());
    }
}
