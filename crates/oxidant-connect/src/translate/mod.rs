//! Lower Spark Connect `Relation`/`Expression` trees to DataFusion logical plans/expressions —
//! the DataFrame-API path (vs. the SQL string path). This is what lets stock PySpark's
//! `df.select(...).filter(...).groupBy(...).agg(...)` run on Oxidant without going through SQL.

pub mod expr;
pub mod relation;

use tonic::Status;

pub use relation::to_plan;

/// An invalid-argument status with a message.
pub(crate) fn inval(msg: impl Into<String>) -> Status {
    Status::invalid_argument(msg.into())
}
