//! Data-quality expectations on a table's output.
//!
//! Each expectation is a boolean SQL expression over the table's own columns plus an action:
//!
//! | action | effect |
//! |---|---|
//! | `drop` | failing rows are filtered out before the write |
//! | `warn` | every row is written; violations are counted and logged |
//! | `fail` | the update is aborted and the table stays at its last good version |
//!
//! All three are implemented by composing SQL around the table's own query, so no new operator
//! is involved and an expectation costs exactly what the equivalent hand-written predicate would.

use std::collections::BTreeMap;

use oxidant_config::{ExpectAction, Expectation};

/// Wrap `sql` so rows failing any `drop` expectation are filtered out.
///
/// Returns `sql` unchanged when nothing drops, so the common case adds no subquery and no plan
/// nodes at all.
pub fn apply_drops(sql: &str, expectations: &BTreeMap<String, Expectation>) -> String {
    let checks: Vec<&str> = expectations
        .values()
        .filter(|e| e.action == ExpectAction::Drop)
        .map(|e| e.check.trim())
        .filter(|c| !c.is_empty())
        .collect();
    if checks.is_empty() {
        return sql.to_string();
    }
    // Each predicate is parenthesized before being joined: `a > 0 OR b > 0` and `c > 0`
    // conjoined without parentheses would bind as `a > 0 OR (b > 0 AND c > 0)` and drop the
    // wrong rows.
    let predicate = checks
        .iter()
        .map(|c| format!("({c})"))
        .collect::<Vec<_>>()
        .join(" AND ");
    format!(
        "SELECT * FROM ({}) AS _oxidant_expect WHERE {predicate}",
        sql.trim()
    )
}

/// Whether any expectation drops rows.
///
/// Lets a streaming table with no `sql:` of its own synthesize one to hang the filter on, rather
/// than parsing its expectations and then silently ignoring them.
pub fn has_drops(expectations: &BTreeMap<String, Expectation>) -> bool {
    expectations
        .values()
        .any(|e| e.action == ExpectAction::Drop && !e.check.trim().is_empty())
}

/// SQL that counts the rows failing `check` within `sql`.
///
/// `NOT (check)` alone would miss rows where the predicate is NULL — a null column makes a
/// comparison null, not false, so `NOT NULL` is null and the row counts as neither passing nor
/// failing. `IS NOT TRUE` catches both false and null, which is what "did not satisfy the
/// expectation" actually means.
pub fn violation_count_sql(sql: &str, check: &str) -> String {
    format!(
        "SELECT count(*) AS violations FROM ({}) AS _oxidant_expect WHERE ({check}) IS NOT TRUE",
        sql.trim()
    )
}

/// Expectations that need a violation count: `warn` reports one, `fail` acts on one.
///
/// `drop` is absent by design — its rows are already gone from the result, so counting them
/// would mean running the query a second time to report a number nobody can act on.
pub fn counted(expectations: &BTreeMap<String, Expectation>) -> Vec<(&String, &Expectation)> {
    expectations
        .iter()
        .filter(|(_, e)| matches!(e.action, ExpectAction::Warn | ExpectAction::Fail))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expectation(check: &str, action: ExpectAction) -> Expectation {
        Expectation {
            check: check.to_string(),
            action,
        }
    }

    #[test]
    fn no_drop_expectations_leaves_the_query_untouched() {
        let expectations = BTreeMap::from([
            ("a".to_string(), expectation("x > 0", ExpectAction::Warn)),
            ("b".to_string(), expectation("y > 0", ExpectAction::Fail)),
        ]);
        assert_eq!(
            apply_drops("SELECT * FROM t", &expectations),
            "SELECT * FROM t"
        );
    }

    #[test]
    fn drops_are_conjoined_and_individually_parenthesized() {
        // Without the inner parentheses `a > 0 OR b > 0` AND `c > 0` binds as
        // `a > 0 OR (b > 0 AND c > 0)`, which drops a different set of rows.
        let expectations = BTreeMap::from([
            (
                "either".to_string(),
                expectation("a > 0 OR b > 0", ExpectAction::Drop),
            ),
            ("c".to_string(), expectation("c > 0", ExpectAction::Drop)),
        ]);
        let sql = apply_drops("SELECT * FROM t", &expectations);
        assert!(sql.contains("(a > 0 OR b > 0)"), "got: {sql}");
        assert!(sql.contains("(c > 0)"), "got: {sql}");
        assert!(sql.contains(" AND "), "got: {sql}");
    }

    #[test]
    fn only_drop_expectations_filter() {
        let expectations = BTreeMap::from([
            ("kept".to_string(), expectation("x > 0", ExpectAction::Warn)),
            (
                "dropped".to_string(),
                expectation("y > 0", ExpectAction::Drop),
            ),
        ]);
        let sql = apply_drops("SELECT * FROM t", &expectations);
        assert!(sql.contains("(y > 0)"), "the drop must filter: {sql}");
        assert!(
            !sql.contains("x > 0"),
            "a warn expectation must not filter rows: {sql}"
        );
    }

    #[test]
    fn violation_counting_treats_null_as_a_failure() {
        // `NOT (x > 0)` is NULL when x is NULL, so a plain NOT counts such a row as neither
        // passing nor failing and the violation silently disappears.
        let sql = violation_count_sql("SELECT * FROM t", "x > 0");
        assert!(sql.contains("IS NOT TRUE"), "got: {sql}");
        assert!(!sql.contains("NOT (x > 0)"), "got: {sql}");
    }

    #[test]
    fn only_warn_and_fail_are_counted() {
        let expectations = BTreeMap::from([
            ("w".to_string(), expectation("a", ExpectAction::Warn)),
            ("f".to_string(), expectation("b", ExpectAction::Fail)),
            ("d".to_string(), expectation("c", ExpectAction::Drop)),
        ]);
        let counted: Vec<&str> = counted(&expectations)
            .into_iter()
            .map(|(name, _)| name.as_str())
            .collect();
        assert_eq!(counted, vec!["f", "w"]);
    }

    #[test]
    fn an_empty_check_is_ignored_rather_than_producing_invalid_sql() {
        let expectations =
            BTreeMap::from([("blank".to_string(), expectation("   ", ExpectAction::Drop))]);
        assert_eq!(
            apply_drops("SELECT * FROM t", &expectations),
            "SELECT * FROM t"
        );
    }
}
