//! Automatic distributed planning: derive a [`StageDef`](crate::driver::StageDef) DAG from a SQL
//! query, so callers no longer hand-author partial/final stage SQL.
//!
//! Primary path: shape-based partial/final aggregation + broadcast/shuffle joins
//! ([`stage_planner`]). Fallback: single-stage [`ExchangeMode::Forward`](crate::driver::ExchangeMode)
//! via [`physical_splitter`] so any locally-plannable SQL still gets a distributed job graph.

pub mod dag_splitter;
pub mod join_chain;
pub mod join_order;
pub mod physical_splitter;
pub mod shape_extensions;
pub mod stage_planner;

pub use stage_planner::{
    base_tables, plan_distributed, plan_distributed_logical, resolve_replicated_tables,
    DistributedQuery,
};
