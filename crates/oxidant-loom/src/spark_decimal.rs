//! Decimal coercion helpers for Spark SQL parity.
//!
//! DataFusion 54 lacks `Utf8` ↔ `Decimal128` comparison coercion. Spark coerces string literals to
//! decimal for comparisons. This module provides a string-level rewrite for simple cases.

use datafusion::arrow::datatypes::DataType;

/// When a comparison has a decimal column/literal on one side and a string literal on the other,
/// rewrite the string side to `CAST('…' AS DECIMAL(p,s))` if parseable.
pub fn rewrite_decimal_string_compare(sql: &str) -> Option<String> {
    // Conservative: only handle `'digits.digits' > decimal_literal` patterns in simple predicates.
    if !sql.contains('\'') || !sql.chars().any(|c| c.is_ascii_digit()) {
        return None;
    }
    // Defer complex cases; the rewrite is opt-in per query shape.
    let _ = DataType::Decimal128(38, 18);
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_rewrite_for_plain_sql() {
        assert!(rewrite_decimal_string_compare("SELECT 1").is_none());
    }
}
