//! Allowlisted normalizations applied to *both* the golden and the actual output before
//! comparison, mirroring what Spark's `SQLQueryTestSuite` does so we compare like-for-like.
//!
//! The big one: Spark sorts result rows lexicographically for queries that are **not**
//! inherently ordered (no top-level `ORDER BY`), so the golden file stores sorted rows. We do
//! the same — otherwise a perfectly-correct result in a different row order would read as a
//! failure. For queries that *are* ordered, row order is significant and we leave it alone (an
//! ordering bug then shows up as a real `correctness`/`ordering` diff).

/// Does the statement carry a top-level `ORDER BY` / `SORT BY`? Heuristic, but matches Spark's
/// intent for the corpus: if so, row order is meaningful and must be preserved.
pub fn is_order_sensitive(sql: &str) -> bool {
    let lower = sql.to_lowercase();
    lower.contains("order by") || lower.contains("sort by")
}

/// Normalize an output block into a comparable list of row strings. When the query is not
/// order-sensitive, rows are sorted (byte order, matching Spark's `.sorted`).
pub fn normalize_output(sql: &str, output: &str) -> Vec<String> {
    let mut rows: Vec<String> = if output.is_empty() {
        Vec::new()
    } else {
        output.lines().map(|l| l.to_string()).collect()
    };
    if !is_order_sensitive(sql) {
        rows.sort();
    }
    rows
}

/// True when two outputs are equal after order-insensitive normalization.
pub fn outputs_match(sql: &str, golden: &str, actual: &str) -> bool {
    normalize_output(sql, golden) == normalize_output(sql, actual)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unordered_rows_compare_set_wise() {
        let sql = "SELECT a, COUNT(b) FROM t GROUP BY a";
        assert!(outputs_match(sql, "2\t2\n0\t1", "0\t1\n2\t2"));
    }

    #[test]
    fn ordered_rows_compare_position_wise() {
        let sql = "SELECT a FROM t ORDER BY a";
        assert!(!outputs_match(sql, "1\n2\n3", "3\n2\n1"));
        assert!(outputs_match(sql, "1\n2\n3", "1\n2\n3"));
    }

    #[test]
    fn empty_outputs_match() {
        assert!(outputs_match("CREATE VIEW v AS SELECT 1", "", ""));
    }
}
