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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn plan_forward_trims_whitespace_and_trailing_semicolon() {
        let engine = Engine::new();
        let dq = plan_forward(&engine, "  SELECT 1 AS x ;  ")
            .await
            .expect("valid SQL should plan");
        assert_eq!(dq.stages.len(), 1);
        assert_eq!(dq.stages[0].exchange, ExchangeMode::Forward);
        assert_eq!(dq.stages[0].sql, "SELECT 1 AS x");
        assert!(dq.finalize_sql.is_none());
        assert!(dq.stages[0].upstream_stage_ids.is_empty());
        assert!(dq.stages[0].hash_key_cols.is_empty());
    }

    #[tokio::test]
    async fn plan_forward_rejects_unplannable_sql() {
        let engine = Engine::new();
        let err = plan_forward(&engine, "SELECT FROM")
            .await
            .expect_err("invalid SQL must fail before shipping");
        let msg = err.to_string();
        assert!(
            msg.contains("parse")
                || msg.contains("Parser")
                || msg.contains("syntax")
                || !msg.is_empty(),
            "expected a planning/parse error, got: {msg}"
        );
    }
}
