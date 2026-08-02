//! Route distributable SQL and DataFrame logical plans through the driver/worker cluster.

use std::sync::{Arc, Mutex};

use datafusion::logical_expr::LogicalPlan;
use weft_common::{Error, Result};
use weft_execution::autoscale::{
    autoscale_enabled, autoscale_target, parallelism_demand, recommend_worker_count,
    task_slots_per_worker,
};
use weft_execution::driver::{run_stages_obs, Cluster, StageDef};
use weft_execution::flight::sync_udfs_to_worker;
use weft_execution::membership::{resolve_membership, ClusterMembership};
use weft_execution::plan::plan_distributed_logical;
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::Engine;
use weft_observability::{ExecutionEvent, QueryTracker};

/// Resolved membership plus the worker-list configuration it was built from.
type CachedMembership = (Vec<String>, Arc<dyn ClusterMembership>);

/// Per-service caches that keep per-query fixed costs off the driver critical path.
///
/// Membership: a fresh `RefreshingMembership` starts cold, so its first `endpoints()`
/// always re-resolves (DNS/API under k8s). Keying the resolved membership by the
/// worker-list configuration lets the TTL cache inside `RefreshingMembership` survive
/// across queries — refresh semantics are unchanged, only the per-query cold start is gone.
///
/// UDF sync: `sync_udfs_to_worker` fired per endpoint per query even when the UDF set
/// never changes (the common case). We remember the last-synced UDF-json hash per
/// endpoint and skip endpoints already holding the current set; a changed UDF set (hash
/// mismatch) syncs every stale endpoint exactly as before.
#[derive(Default)]
pub struct DistributedCaches {
    membership: Mutex<Option<CachedMembership>>,
    udf_synced: Mutex<std::collections::HashMap<String, u64>>,
}

impl DistributedCaches {
    /// Resolved membership for `workers`, rebuilding only when the worker-list
    /// configuration changed. (Under k8s the discovery source is env-driven and the
    /// static list is empty, so the cache then keys on the empty list.)
    pub fn membership(&self, workers: &[String]) -> Arc<dyn ClusterMembership> {
        let mut guard = self.membership.lock().expect("membership cache poisoned");
        if let Some((key, membership)) = guard.as_ref() {
            if key == workers {
                return Arc::clone(membership);
            }
        }
        let membership = resolve_membership(workers);
        *guard = Some((workers.to_vec(), Arc::clone(&membership)));
        membership
    }

    /// Endpoints whose last-synced UDF-set hash differs from `hash` (never synced, or stale).
    pub fn udf_sync_pending(&self, endpoints: &[String], hash: u64) -> Vec<String> {
        let guard = self.udf_synced.lock().expect("udf sync cache poisoned");
        endpoints
            .iter()
            .filter(|ep| guard.get(*ep) != Some(&hash))
            .cloned()
            .collect()
    }

    /// Record `hash` as the synced UDF set for `endpoints`. Call only after every sync
    /// succeeded, so a failed sync leaves all endpoints pending for the next query.
    pub fn udf_mark_synced(&self, endpoints: &[String], hash: u64) {
        let mut guard = self.udf_synced.lock().expect("udf sync cache poisoned");
        for ep in endpoints {
            guard.insert(ep.clone(), hash);
        }
    }
}

/// Content hash of the UDF registration payload, identifying "the same UDF set" without
/// retaining the JSON per endpoint. In-memory only (per-process hasher keys are fine).
fn udf_hash(json: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    json.hash(&mut hasher);
    hasher.finish()
}

/// If workers or K8s service discovery is configured and `sql` is auto-splittable, run distributed.
/// Returns `Ok(None)` when the query should fall back to single-node execution.
///
/// Test-only convenience wrapper: production callers (the SQL and DataFrame paths in
/// weft-connect) build the plan once themselves and call [`try_run_distributed_plan`]
/// directly — re-parsing the SQL here would be a second planning pass per query.
#[cfg(test)]
pub async fn try_run_distributed(
    engine: &Engine,
    workers: &[String],
    sql: &str,
    replicated: &[&str],
    udf_json: Option<&str>,
    tracker: Option<&QueryTracker>,
) -> Result<Option<Vec<RecordBatch>>> {
    // Capture the exact Delta/Iceberg snapshot identities this query resolves so every
    // worker stage scans the SAME pinned snapshot (KAN-48) — workers reject unpinned
    // lakehouse scans.
    let (lp, lakehouse_snapshot_pins) = engine.logical_plan_with_lakehouse_snapshots(sql).await?;
    // Test-only path: a fresh cache per call (no cross-query reuse to assert on here —
    // `DistributedCaches` itself is unit-tested below).
    let caches = DistributedCaches::default();
    try_run_distributed_plan(
        engine,
        workers,
        &lp,
        sql,
        replicated,
        udf_json,
        tracker,
        &lakehouse_snapshot_pins,
        &caches,
    )
    .await
}

/// Like [`try_run_distributed`], but accepts an already-built logical plan (DataFrame API path).
/// `lakehouse_snapshot_pins` is the driver's captured table→snapshot JSON map
/// ([`Engine::capture_lakehouse_snapshots`]), stamped onto every stage so workers resolve
/// lakehouse tables at the same pinned snapshot; empty when the plan scans no lakehouse tables.
/// `caches` carries the per-service membership / UDF-sync caches (see [`DistributedCaches`]).
#[allow(clippy::too_many_arguments)]
pub async fn try_run_distributed_plan(
    engine: &Engine,
    workers: &[String],
    plan: &LogicalPlan,
    description: &str,
    replicated: &[&str],
    udf_json: Option<&str>,
    tracker: Option<&QueryTracker>,
    lakehouse_snapshot_pins: &str,
    caches: &DistributedCaches,
) -> Result<Option<Vec<RecordBatch>>> {
    let membership = caches.membership(workers);
    let endpoints = membership.endpoints();
    if endpoints.is_empty() {
        return Ok(None);
    }

    let mut dq = match plan_distributed_logical(plan, replicated) {
        Ok(d) => d,
        Err(Error::Unsupported(reason)) => {
            record_distributed_fallback(tracker, &reason);
            if distributed_strict() {
                return Err(Error::Unsupported(reason));
            }
            return Ok(None);
        }
        Err(e) => return Err(e),
    };
    for stage in &mut dq.stages {
        stage.lakehouse_snapshot_pins = lakehouse_snapshot_pins.to_string();
    }
    let cluster = Cluster::from_membership(membership);

    if let Some(json) = udf_json.filter(|s| !s.is_empty() && *s != "[]") {
        // Sync in parallel, and only to endpoints that don't already hold this exact UDF
        // set (hash match). A failed sync marks nothing, so the next query retries all.
        let hash = udf_hash(json);
        let pending = caches.udf_sync_pending(&endpoints, hash);
        if !pending.is_empty() {
            futures::future::join_all(
                pending
                    .iter()
                    .map(|ep| sync_udfs_to_worker(ep.clone(), json)),
            )
            .await
            .into_iter()
            .collect::<Result<()>>()?;
            caches.udf_mark_synced(&pending, hash);
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
        // Note: no physical-plan `explain` here — both callers (the SQL path and the
        // DataFrame path in weft-connect) already set the tracker's plan text before
        // calling in, so this was a second full optimize+physical pass per query.
    }

    let store = tracker.map(|t| t.store().clone());
    let operation_id = tracker.map(|t| t.operation_id().to_string());

    maybe_request_autoscale(&cluster, &dq.stages).await?;

    // WEFT_REOPT_JOIN_ORDER (default off): retain the planner inputs so the driver can
    // re-sequence the shuffle-join chain's tail by barrier-measured leaf cardinalities at
    // the last leaf's barrier and splice the re-derived stages onto the dispatched prefix.
    let reopt = weft_execution::driver::reopt_join_order_enabled()
        .then_some(weft_execution::driver::ReoptContext { plan, replicated });
    let mut batches = run_stages_obs(&cluster, &dq.stages, store, operation_id, reopt).await?;

    if let Some(finalize) = dq.finalize_sql {
        engine
            .register_batches("result", batches.clone())
            .map_err(|e| Error::Execution(e.to_string()))?;
        batches = engine
            .sql(&finalize)
            .await
            .map_err(|e| Error::Execution(format!("finalize `{description}`: {e}")))?;
    }

    Ok(Some(batches))
}

fn stage_num_tasks(stage: &StageDef, stages: &[StageDef], cluster: &Cluster) -> i32 {
    weft_execution::autoscale::stage_num_tasks(stage, stages, cluster) as i32
}

/// When autoscaling is enabled, POST a scale recommendation to the control-plane gateway.
async fn maybe_request_autoscale(cluster: &Cluster, stages: &[StageDef]) -> Result<()> {
    if !autoscale_enabled() {
        return Ok(());
    }
    let (gateway, cluster_id) = match autoscale_target() {
        Some(t) => t,
        None => {
            tracing::debug!("WEFT_AUTOSCALE=1 but WEFT_GATEWAY_URL/WEFT_CLUSTER_ID unset; skipping scale request");
            return Ok(());
        }
    };
    let demand = parallelism_demand(cluster, stages);
    let min_workers = worker_bound("WEFT_WORKER_MIN", cluster.worker_count() as u32);
    let max_workers = worker_bound(
        "WEFT_WORKER_MAX",
        min_workers.saturating_mul(4).max(min_workers),
    );
    let rec = recommend_worker_count(
        cluster.worker_count() as u32,
        min_workers,
        max_workers,
        &demand,
        task_slots_per_worker(),
    );
    tracing::info!(
        target: "weft.autoscale",
        current = rec.current_workers,
        recommended = rec.recommended_workers,
        peak = rec.peak_task_demand,
        should_scale = rec.should_scale,
        reason = %rec.reason,
        "parallelism scale recommendation"
    );
    if !rec.should_scale {
        return Ok(());
    }
    let url = format!("{gateway}/clusters/{cluster_id}/scale");
    let body = serde_json::json!({
        "recommended_workers": rec.recommended_workers,
        "peak_task_demand": rec.peak_task_demand,
        "shuffle_partitions": demand.shuffle_partitions,
        "reason": rec.reason,
    });
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(|e| Error::Io(format!("autoscale POST {url}: {e}")))?;
    if !resp.status().is_success() {
        return Err(Error::Io(format!(
            "autoscale POST {url} returned {}",
            resp.status()
        )));
    }
    Ok(())
}

fn worker_bound(env_key: &str, fallback: u32) -> u32 {
    std::env::var(env_key)
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n: &u32| n > 0)
        .unwrap_or(fallback)
        .max(1)
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

/// When set, an unsupported distributed plan is an error instead of local fallback.
fn distributed_strict() -> bool {
    std::env::var("WEFT_DISTRIBUTED_STRICT")
        .ok()
        .is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
}

fn record_distributed_fallback(tracker: Option<&QueryTracker>, reason: &str) {
    tracing::warn!(
        reason = %reason,
        "distributed planner rejected query; falling back to local execution"
    );
    if let Some(t) = tracker {
        t.store().emit(ExecutionEvent::DistributedFallback {
            operation_id: t.operation_id().to_string(),
            reason: reason.to_string(),
        });
    }
}

#[cfg(test)]
#[allow(clippy::await_holding_lock)] // STRICT_ENV_LOCK serializes process-global env across async tests
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use weft_loom::arrow::array::Int64Array;
    use weft_loom::arrow::datatypes::{DataType, Field, Schema};
    use weft_loom::arrow::record_batch::RecordBatch;
    use weft_observability::AppStateStore;

    /// `WEFT_DISTRIBUTED_STRICT` is process-global; serialize tests that mutate it.
    static STRICT_ENV_LOCK: Mutex<()> = Mutex::new(());

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2])),
                Arc::new(Int64Array::from(vec![10_i64, 20])),
            ],
        )
        .unwrap()
    }

    const UNSUPPORTED_SQL: &str = "SELECT SUM(v) OVER () AS sv FROM t";
    const WORKERS: [&str; 1] = ["http://127.0.0.1:59999"];

    async fn engine_with_t() -> Engine {
        let e = Engine::new();
        e.register_batches("t", vec![test_batch()]).unwrap();
        e
    }

    #[test]
    fn parses_worker_list() {
        let w = parse_worker_list(Some("127.0.0.1:50561,127.0.0.1:50562"));
        assert_eq!(w.len(), 2);
        assert!(w[0].starts_with("http://"));
    }

    #[test]
    fn membership_cached_until_worker_list_changes() {
        let _guard = STRICT_ENV_LOCK.lock().unwrap();
        // Hermetic: force the static-list path even if a k8s env leaks into the test process.
        std::env::remove_var("WEFT_WORKER_SERVICE");
        let caches = DistributedCaches::default();
        let workers: Vec<String> = WORKERS.iter().map(|s| (*s).to_string()).collect();
        let first = caches.membership(&workers);
        let second = caches.membership(&workers);
        assert!(
            Arc::ptr_eq(&first, &second),
            "same worker list must reuse the resolved membership (one resolution)"
        );
        assert_eq!(first.endpoints(), workers);

        let changed = vec!["http://127.0.0.1:50562".to_string()];
        let third = caches.membership(&changed);
        assert!(
            !Arc::ptr_eq(&first, &third),
            "changed worker list must re-resolve"
        );
        assert!(
            Arc::ptr_eq(&third, &caches.membership(&changed)),
            "the new list is now the cached one"
        );
    }

    #[test]
    fn udf_sync_skips_only_endpoints_already_holding_current_set() {
        let caches = DistributedCaches::default();
        let eps: Vec<String> = ["http://a:1", "http://b:1"]
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        let v1 = udf_hash(r#"[{"name":"f"}]"#);
        // Nothing synced yet: every endpoint is pending.
        assert_eq!(caches.udf_sync_pending(&eps, v1), eps);

        caches.udf_mark_synced(&eps, v1);
        // Unchanged UDF set: no endpoint needs a sync (no RPC).
        assert!(caches.udf_sync_pending(&eps, v1).is_empty());

        // Changed UDF set: every endpoint is stale and re-syncs.
        let v2 = udf_hash(r#"[{"name":"f"},{"name":"g"}]"#);
        assert_eq!(caches.udf_sync_pending(&eps, v2), eps);

        // A newly appeared endpoint is pending even when the others are current.
        caches.udf_mark_synced(&eps, v2);
        let mut grown = eps.clone();
        grown.push("http://c:1".to_string());
        assert_eq!(
            caches.udf_sync_pending(&grown, v2),
            vec!["http://c:1".to_string()]
        );
    }

    #[test]
    fn autoscale_recommendation_when_partitions_exceed_worker_slots() {
        use weft_execution::autoscale::{parallelism_demand, recommend_worker_count};

        let cluster = Cluster::new(vec![
            "http://127.0.0.1:50561".into(),
            "http://127.0.0.1:50562".into(),
        ]);
        let mut cluster = cluster;
        cluster.num_partitions = 32;
        let stages = vec![StageDef::new(0, "SELECT 1", vec![], vec![0])];
        let demand = parallelism_demand(&cluster, &stages);
        let rec = recommend_worker_count(2, 2, 16, &demand, 4);
        assert!(rec.should_scale);
        assert_eq!(rec.recommended_workers, 8);
    }

    #[test]
    fn forward_stage_reports_one_task() {
        let cluster = Cluster::new(vec![
            "http://127.0.0.1:50561".into(),
            "http://127.0.0.1:50562".into(),
        ]);
        let stage = StageDef {
            exchange: weft_execution::driver::ExchangeMode::Forward,
            ..StageDef::default()
        };
        assert_eq!(
            stage_num_tasks(&stage, std::slice::from_ref(&stage), &cluster),
            1
        );
    }

    #[tokio::test]
    async fn soft_fallback_returns_none_and_local_still_runs() {
        let _guard = STRICT_ENV_LOCK.lock().unwrap();
        std::env::remove_var("WEFT_DISTRIBUTED_STRICT");
        let e = engine_with_t().await;
        let workers: Vec<String> = WORKERS.iter().map(|s| (*s).to_string()).collect();
        let out = try_run_distributed(&e, &workers, UNSUPPORTED_SQL, &[], None, None)
            .await
            .expect("soft fallback should not error");
        assert!(out.is_none());
        let local = e.sql(UNSUPPORTED_SQL).await.expect("local execution");
        assert!(!local.is_empty());
    }

    #[tokio::test]
    async fn strict_mode_returns_unsupported_reason() {
        let _guard = STRICT_ENV_LOCK.lock().unwrap();
        std::env::set_var("WEFT_DISTRIBUTED_STRICT", "1");
        let e = engine_with_t().await;
        let workers: Vec<String> = WORKERS.iter().map(|s| (*s).to_string()).collect();
        let result = try_run_distributed(&e, &workers, UNSUPPORTED_SQL, &[], None, None).await;
        std::env::remove_var("WEFT_DISTRIBUTED_STRICT");
        let err = result.expect_err("strict mode must fail");
        let msg = err.to_string();
        assert!(
            matches!(err, Error::Unsupported(_)),
            "expected Unsupported, got {err:?}"
        );
        assert!(
            msg.contains("window") || msg.contains("PARTITION BY") || msg.contains("unsupported"),
            "reject reason should be preserved, got: {msg}"
        );
    }

    /// KAN-22 floor: a correlated scalar subquery over a second *sharded* table must surface a
    /// clear, actionable strict-mode rejection naming the shape — never a silent unbounded
    /// driver-local collect.
    #[tokio::test]
    async fn strict_mode_rejects_correlated_subquery_over_sharded_table() {
        let _guard = STRICT_ENV_LOCK.lock().unwrap();
        std::env::set_var("WEFT_DISTRIBUTED_STRICT", "1");
        let e = engine_with_t().await;
        e.register_batches("u", vec![test_batch()]).unwrap();
        // `t` and `u` both sharded: the subquery over `u` cannot stay shard-local and the
        // whole-fact gather handles exactly one sharded fact, so this must reject.
        let sql = "SELECT t.k, t.v FROM t, u WHERE t.k = u.k \
                   AND t.v = (SELECT max(u.v) FROM u WHERE u.k = t.k)";
        let workers: Vec<String> = WORKERS.iter().map(|s| (*s).to_string()).collect();
        let result = try_run_distributed(&e, &workers, sql, &[], None, None).await;
        std::env::remove_var("WEFT_DISTRIBUTED_STRICT");
        let err = result.expect_err("strict mode must fail");
        let msg = err.to_string();
        assert!(
            matches!(err, Error::Unsupported(_)),
            "expected Unsupported, got {err:?}"
        );
        assert!(
            msg.contains("subquery over `u` is only safe when that table is replicated"),
            "reject reason should name the unsupported shape, got: {msg}"
        );
    }

    #[tokio::test]
    async fn empty_workers_skips_strict_and_fallback() {
        let _guard = STRICT_ENV_LOCK.lock().unwrap();
        std::env::set_var("WEFT_DISTRIBUTED_STRICT", "1");
        let e = engine_with_t().await;
        let out = try_run_distributed(&e, &[], UNSUPPORTED_SQL, &[], None, None)
            .await
            .expect("no workers means no distribute attempt");
        std::env::remove_var("WEFT_DISTRIBUTED_STRICT");
        assert!(out.is_none());
    }

    #[tokio::test]
    async fn fallback_event_emitted_when_tracker_present() {
        let _guard = STRICT_ENV_LOCK.lock().unwrap();
        std::env::remove_var("WEFT_DISTRIBUTED_STRICT");
        let store = Arc::new(AppStateStore::new());
        let mut rx = store.subscribe();
        let tracker = QueryTracker::begin(store.clone(), "op-fallback", UNSUPPORTED_SQL);
        let e = engine_with_t().await;
        let workers: Vec<String> = WORKERS.iter().map(|s| (*s).to_string()).collect();
        let out = try_run_distributed(&e, &workers, UNSUPPORTED_SQL, &[], None, Some(&tracker))
            .await
            .expect("soft fallback");
        assert!(out.is_none());

        let mut saw = false;
        while let Ok(event) = rx.try_recv() {
            if let ExecutionEvent::DistributedFallback {
                operation_id,
                reason,
            } = event
            {
                assert_eq!(operation_id, "op-fallback");
                assert!(!reason.is_empty());
                saw = true;
            }
        }
        assert!(saw, "DistributedFallback event should be emitted");

        let sql_rows = store.list_sql();
        assert_eq!(sql_rows.len(), 1);
        assert!(
            sql_rows[0].physical_plan.contains("[distributed fallback]"),
            "fallback reason should appear in sql observability payload"
        );
    }
}
