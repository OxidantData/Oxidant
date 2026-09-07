//! Allowlisted normalizations applied to *both* the golden and the actual output before
//! comparison, mirroring what Spark's `SQLQueryTestSuite` does so we compare like-for-like.
//!
//! The big one: Spark sorts result rows lexicographically for queries that are **not**
//! inherently ordered (no top-level `ORDER BY`), so the golden file stores sorted rows. We do
//! the same — otherwise a perfectly-correct result in a different row order would read as a
//! failure. For queries that *are* ordered, row order is significant and we leave it alone (an
//! ordering bug then shows up as a real `correctness`/`ordering` diff).

/// Does the statement carry a top-level `ORDER BY` / `SORT BY`? Nested clauses (subqueries,
/// `IN (SELECT … ORDER BY …)`) do not make the outer row bag ordered (OxidantData/Oxidant#190).
pub fn is_order_sensitive(sql: &str) -> bool {
    let stripped = strip_parenthesized(&strip_strings_and_comments(sql));
    let lower = stripped.to_lowercase();
    lower.contains("order by") || lower.contains("sort by")
}

/// Normalize an output block into a comparable list of row strings. When the query is not
/// order-sensitive, rows are sorted (byte order, matching Spark's `.sorted`).
pub fn normalize_output(sql: &str, output: &str) -> Vec<String> {
    normalize_rows(sql, &parse_output_rows(output))
}

/// Split a joined golden/actual output string into rows. An empty string is zero rows, not one
/// empty cell — joined text cannot recover that distinction.
pub fn parse_output_rows(output: &str) -> Vec<String> {
    if output.is_empty() {
        Vec::new()
    } else {
        output.lines().map(|l| l.to_string()).collect()
    }
}

fn normalize_rows(sql: &str, rows: &[String]) -> Vec<String> {
    let mut rows = rows.to_vec();
    if !is_order_sensitive(sql) {
        rows.sort();
    }
    rows
}

/// True when two outputs are equal after order-insensitive normalization.
///
/// Joined text cannot tell zero rows from one empty cell; prefer [`rows_match`].
pub fn outputs_match(sql: &str, golden: &str, actual: &str) -> bool {
    normalize_output(sql, golden) == normalize_output(sql, actual)
}

/// Compare row lists without joining. Zero rows and one empty cell are distinct
/// (OxidantData/Oxidant#190): `vec![]` and `vec![""]` both join to `""`.
pub fn rows_match(sql: &str, golden: &[String], actual: &[String]) -> bool {
    normalize_rows(sql, golden) == normalize_rows(sql, actual)
}

fn strip_strings_and_comments(sql: &str) -> String {
    let b = sql.as_bytes();
    let mut out = String::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'-' && i + 1 < b.len() && b[i + 1] == b'-' {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if b[i] == b'/' && i + 1 < b.len() && b[i + 1] == b'*' {
            i += 2;
            while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                i += 1;
            }
            i = i.saturating_add(2).min(b.len());
            continue;
        }
        if b[i] == b'\'' || b[i] == b'"' || b[i] == b'`' {
            let q = b[i];
            i += 1;
            while i < b.len() {
                if b[i] == q {
                    if q == b'\'' && i + 1 < b.len() && b[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            out.push(' ');
            continue;
        }
        out.push(b[i] as char);
        i += 1;
    }
    out
}

fn strip_parenthesized(sql: &str) -> String {
    let mut out = String::new();
    let mut depth = 0u32;
    for c in sql.chars() {
        match c {
            '(' => depth = depth.saturating_add(1),
            ')' => depth = depth.saturating_sub(1),
            _ if depth == 0 => out.push(c),
            _ => {}
        }
    }
    out
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

    #[test]
    fn nested_order_by_does_not_make_the_outer_query_order_sensitive() {
        let sql = "SELECT x FROM t WHERE x IN (SELECT y FROM u ORDER BY y)";
        assert!(!is_order_sensitive(sql));
        assert!(outputs_match(sql, "1\n2", "2\n1"));
    }

    #[test]
    fn empty_row_is_not_zero_rows() {
        let zero: Vec<String> = vec![];
        let one = vec![String::new()];
        assert_eq!(zero.join("\n"), one.join("\n"));
        assert!(!rows_match("SELECT X''", &zero, &one));
    }
}
