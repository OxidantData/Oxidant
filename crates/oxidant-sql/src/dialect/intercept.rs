//! Stage 2 — statement-intercept registry (post-parse).
//!
//! Rules here see a parsed `sqlparser` [`Statement`] — parsed with the Databricks dialect, the
//! same dialect DataFusion plans with — and either **fully own** the statement (returning a
//! [`LowerOutcome`]) or return `None` so the next rule, and ultimately `ctx.sql()`, sees the
//! statement unchanged.
//!
//! This is the home for rewrites that are only faithful at AST level, where the parser has
//! already handled `;` terminators, comments, and quoting — the exact failure modes that rule
//! out Stage-1 string rewrites. Later dialect tickets plug in here: `USE` (correct namespace
//! resolution, emitting `SET datafusion.catalog.default_schema = '…'`), `LIKE ANY/ALL`
//! (expression-tree rewrite to OR/AND chains), `PIVOT`/`UNPIVOT` (a rewritten `LogicalPlan`),
//! `SHOW`/`DESCRIBE` (`LowerOutcome::Direct` batches from catalog metadata).

use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::parser::Parser;

use super::pipeline::LowerOutcome;

/// A post-parse statement intercept. See the module docs for the contract.
pub trait StatementIntercept: Send + Sync {
    /// Stable rule name, used in conflict diagnostics.
    fn name(&self) -> &'static str;

    /// Return the outcome when this rule claims `statement`, or `None` to pass it through to
    /// the next rule unchanged. Outcomes are owned (`'static`): an intercept always produces
    /// new SQL text, a plan, or batches — it never borrows the input string.
    fn intercept(
        &self,
        statement: &Statement,
        ctx: &SessionContext,
    ) -> Option<LowerOutcome<'static>>;
}

/// Parse `sql` with the Databricks dialect, returning the statement only when the input is
/// exactly one statement.
///
/// `None` — unparseable input or a multi-statement script — means "not interceptable": the
/// pipeline passes the text through to `ctx.sql()` and lets DataFusion plan it (or report the
/// parse error) exactly as it would without the dialect layer.
pub fn parse_single_statement(sql: &str) -> Option<Statement> {
    let stmts = Parser::parse_sql(&DatabricksDialect {}, sql).ok()?;
    let [stmt] = stmts.as_slice() else {
        return None;
    };
    Some(stmt.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::ast::Use;

    #[test]
    fn parses_single_statement() {
        assert!(matches!(
            parse_single_statement("SELECT 1"),
            Some(Statement::Query(_))
        ));
    }

    #[test]
    fn parses_use_statement() {
        // The statement the `USE` intercept (KAN-96) will claim — verify the Databricks
        // dialect hands it to Stage 2 as a structured AST node, `;` and quoting included.
        match parse_single_statement("USE `my-db`;") {
            Some(Statement::Use(Use::Object(name))) => {
                assert_eq!(name.to_string(), "`my-db`");
            }
            other => panic!("expected USE AST node, got {other:?}"),
        }
    }

    #[test]
    fn trailing_semicolon_is_still_one_statement() {
        assert!(matches!(
            parse_single_statement("SELECT 1;"),
            Some(Statement::Query(_))
        ));
    }

    #[test]
    fn comments_do_not_defeat_the_parser() {
        // The Stage-1 whitespace tokenizer would see `--` as the leading token here; the parser
        // hands Stage 2 the real statement. This is exactly why `USE` moves to Stage 2.
        match parse_single_statement("-- pick the db\nUSE analytics /* now */") {
            Some(Statement::Use(Use::Object(name))) => assert_eq!(name.to_string(), "analytics"),
            other => panic!("expected USE AST node, got {other:?}"),
        }
    }

    #[test]
    fn multi_statement_is_not_interceptable() {
        assert_eq!(parse_single_statement("SELECT 1; SELECT 2"), None);
    }

    #[test]
    fn unparseable_is_not_interceptable() {
        assert_eq!(parse_single_statement("SELEC T FROM WAT"), None);
        assert_eq!(parse_single_statement(""), None);
    }
}
