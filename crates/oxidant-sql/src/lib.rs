//! `oxidant-sql` — the Spark SQL dialect front-end.
//!
//! Parses SQL text (from the Spark Connect `Sql` relation / `SqlCommand`, and raw
//! `ExpressionString` fragments) into [`oxidant_plan`] IR. Spark dialect quirks live here:
//! backtick identifiers, `LIKE`/`RLIKE`, `DATE_TRUNC`, lateral views, etc.

use oxidant_common::Result;
use oxidant_plan::LogicalPlan;

pub mod dialect;

/// Parse a Spark SQL statement into the warp IR. Implemented in Phase 0.
pub fn parse(_sql: &str) -> Result<LogicalPlan> {
    oxidant_plan::lower_placeholder()
}
