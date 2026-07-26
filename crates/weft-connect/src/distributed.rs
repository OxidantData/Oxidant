//! Route distributable SQL through the driver/worker cluster.

use weft_common::{Error, Result};
use weft_execution::driver::{run_stages_obs, Cluster, ExchangeMode, StageDef};
use weft_execution::flight::sync_udfs_to_worker;
use weft_execution::membership::resolve_membership;
use weft_execution::plan::plan_distributed;
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::Engine;
use weft_observability::QueryTracker;

/// If workers or K8s service discovery is configured and `sql` is auto-splittable, run distributed.
/// Returns `Ok(None)` when the query should fall back to single-node execution.
pub async fn try_run_distributed(
    engine: &Engine,
    workers: &[String],
    sql: &str,
    replicated: &[&str],
    udf_json: Option<&str>,
    tracker: Option<&QueryTracker>,
) -> Result<Option<Vec<RecordBatch>>> {
    let membership = resolve_membership(workers);
    let endpoints = membership.endpoints();
    if endpoints.is_empty() {
        return Ok(None);
    }

    let dq = match plan_distributed(engine, sql, replicated).await {
        Ok(d) => d,
        Err(Error::Unsupported(_)) => return Ok(None),
        Err(e) => return Err(e),
    };
    let cluster = Cluster::from_membership(membership);

    if let Some(json) = udf_json.filter(|s| !s.is_empty() && *s != "[]") {
        for ep in &endpoints {
            sync_udfs_to_worker(ep.clone(), json).await?;
        }
    }

    // Register executors and stage DAG in observability.
    if let Some(t) = tracker {
        let op = t.operation_id().to_string();
        for ep in &endpoints {
            let host = ep
                .trim_start_matches("http://")
                .trim_start_matches("https://");
            t.store()
                .emit(weft_observability::ExecutionEvent::ExecutorRegistered {
                    executor_id: host.to_string(),
                    host_port: host.to_string(),
                });
        }
        for stage in &dq.stages {
            t.store()
                .emit(weft_observability::ExecutionEvent::StageStarted {
                    operation_id: op.clone(),
                    stage_id: stage.stage_id as i32,
                    name: truncate_sql(&stage.sql),
                    num_tasks: stage_num_tasks(stage, &dq.stages, &cluster),
                    submission_time_ms: weft_observability::now_ms(),
                });
        }
        if let Ok(plan) = engine.logical_plan(sql).await {
            if let Ok(text) = engine.explain(&plan, true).await {
                t.set_plan(text, None);
            }
        }
    }

    let store = tracker.map(|t| t.store().clone());
    let operation_id = tracker.map(|t| t.operation_id().to_string());
    let mut batches = run_stages_obs(&cluster, &dq.stages, store, operation_id).await?;

    if let Some(finalize) = dq.finalize_sql {
        engine
            .register_batches("result", batches.clone())
            .map_err(|e| Error::Execution(e.to_string()))?;
        batches = engine.sql(&finalize).await?;
    }

    Ok(Some(batches))
}

fn stage_num_tasks(stage: &StageDef, stages: &[StageDef], cluster: &Cluster) -> i32 {
    if stage.exchange == ExchangeMode::Forward {
        return 1;
    }
    let is_output = !stages
        .iter()
        .any(|s| s.upstream_stage_ids.contains(&stage.stage_id));
    if is_output && !stage.upstream_stage_ids.is_empty() {
        cluster.num_partitions as i32
    } else {
        cluster.worker_count().max(1) as i32
    }
}

/// Parse `spark.weft.workers` or `WEFT_WORKERS` (comma-separated `host:port` list).
pub fn parse_worker_list(config_value: Option<&str>) -> Vec<String> {
    let env_workers = std::env::var("WEFT_WORKERS").ok();
    let raw = config_value
        .filter(|s| !s.is_empty())
        .or(env_workers.as_deref())
        .unwrap_or("");
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|ep| {
            if ep.starts_with("http://") || ep.starts_with("https://") {
                ep.to_string()
            } else {
                format!("http://{ep}")
            }
        })
        .collect()
}

fn truncate_sql(s: &str) -> String {
    let t = s.trim().replace('\n', " ");
    if t.chars().count() <= 120 {
        t
    } else {
        format!("{}…", t.chars().take(119).collect::<String>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_worker_list() {
        let w = parse_worker_list(Some("127.0.0.1:50561,127.0.0.1:50562"));
        assert_eq!(w.len(), 2);
        assert!(w[0].starts_with("http://"));
    }

    #[test]
    fn forward_stage_reports_one_task() {
        let cluster = Cluster::new(vec![
            "http://127.0.0.1:50561".into(),
            "http://127.0.0.1:50562".into(),
        ]);
        let stage = StageDef {
            exchange: ExchangeMode::Forward,
            ..StageDef::default()
        };
        assert_eq!(
            stage_num_tasks(&stage, std::slice::from_ref(&stage), &cluster),
            1
        );
    }
}
