//! Automatic distributed planning: derive a [`StageDef`](crate::driver::StageDef) DAG from a SQL
//! query, so callers no longer hand-author partial/final stage SQL.
//!
//! The splitter works at the **logical** level and regenerates per-stage SQL with DataFusion's
//! unparser (workers re-plan that SQL against their own locally-registered tables — the model the
//! [`codec`](crate::shuffle::codec) finding showed is the correct one). See
//! [`stage_planner::plan_distributed`].

pub mod shape_extensions;
pub mod stage_planner;

pub use stage_planner::{plan_distributed, DistributedQuery};
