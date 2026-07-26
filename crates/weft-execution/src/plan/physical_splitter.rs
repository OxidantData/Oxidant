//! Physical / general distributed planning: when the shape-based aggregator splitter cannot
//! lower a query, emit a **Forward** single-stage plan that runs the original SQL on one worker.
//!
//! This is the Sail-like coverage path: any SQL that plans locally also gets a distributed job
//! graph (here, a one-stage DAG). Correctness requires that the scheduled worker has a complete
//! view of every referenced table (fully replicated dims + facts, or shared-storage scans).

use weft_common::Result;
use weft_loom::Engine;

use crate::driver::{ExchangeMode, StageDef};

use super::stage_planner::DistributedQuery;

/// Plan `sql` as a single Forward stage (full SQL on one worker).
pub async fn plan_forward(engine: &Engine, sql: &str) -> Result<DistributedQuery> {
    // Validate the query plans on the driver engine before shipping to a worker.
    let _lp = engine.logical_plan(sql).await?;
    Ok(DistributedQuery {
        stages: vec![StageDef {
            stage_id: 0,
            sql: sql.trim().trim_end_matches(';').trim().to_string(),
            upstream_stage_ids: vec![],
            hash_key_cols: vec![],
            exchange: ExchangeMode::Forward,
            plan_fragment: None,
        }],
        finalize_sql: None,
    })
}
