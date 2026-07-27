//! The distributed driver: orchestrate a stage DAG across workers.
//!
//! A query is expressed as a topologically-ordered list of [`StageDef`]s. Each *producer* stage
//! runs once per shuffle partition on the worker that owns that partition (rendezvous hashing),
//! hash-partitions its output, and caches the buckets. The output stage runs on every partition
//! owner, pulling upstream buckets and returning results.
//!
//! The MVP shape — two stages, `partial-agg → hash shuffle → final-agg` — is the
//! [`DistributedPlan`] convenience built on top of this (see [`DistributedPlan::into_stages`]).
//! Multiple upstreams on the output stage express a **shuffle join**: both sides hash-partition on
//! the join key so matching keys co-locate on one worker, which then joins them locally.
//!
//! Intermediate stages that both consume *and* produce are supported (left-deep join chains).
//! Shuffle partition count defaults to worker count but can be overridden via
//! `WEFT_SHUFFLE_PARTITIONS` (like `spark.sql.shuffle.partitions`) or, when that is unset,
//! `WEFT_DEFAULT_PARALLELISM`. Shuffle buckets spill when over the configured memory budget
//! (see [`crate::shuffle::spill`]); push-based `do_exchange` complements pull-based shuffle reads.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use weft_common::{Error, Result};
use weft_loom::arrow::record_batch::RecordBatch;
use weft_observability::{now_ms, ExecutionEvent, SharedStore, StageStatus, TaskStatus};

use crate::aqe::{aqe_enabled, coalesced_partitions};
use crate::autoscale::{
    autoscale_enabled, parallelism_demand, recommend_worker_count, task_slots_per_worker,
};
use crate::flight::{clear_worker_stages, pull_bucket_with_retry};
use crate::lineage::StageLineage;
use crate::membership::{ClusterMembership, StaticMembership};
use crate::scheduler::run_stage_with_retry;
use crate::shuffle::protocol::StageTicket;

/// Number of hash-shuffle partitions for the next query.
pub fn shuffle_partitions(worker_count: usize) -> u32 {
    std::env::var("WEFT_SHUFFLE_PARTITIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n: &u32| n > 0)
        .or_else(|| {
            std::env::var("WEFT_DEFAULT_PARALLELISM")
                .ok()
                .and_then(|s| s.parse().ok())
                .filter(|&n: &u32| n > 0)
        })
        .unwrap_or(worker_count.max(1) as u32)
}

/// Expected worker fan-out from `WEFT_WORKER_COUNT` (same env workers use for file sharding).
pub fn expected_worker_count_from_env() -> Option<usize> {
    std::env::var("WEFT_WORKER_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
}

/// A cluster snapshot for one query: workers + stable partition→owner mapping.
#[derive(Clone)]
pub struct Cluster {
    /// Unique worker endpoints in this snapshot.
    pub workers: Vec<String>,
    /// Hash-shuffle partition count (may exceed worker count).
    pub num_partitions: u32,
    /// When set (or derived from `WEFT_WORKER_COUNT`), the driver hard-fails if the
    /// visible worker fan-out differs — workers shard files by that modulus.
    pub expected_worker_count: Option<usize>,
    pub(crate) membership: Arc<dyn ClusterMembership>,
}

impl Cluster {
    /// Build a cluster from a fixed endpoint list (tests, CLI).
    pub fn new(workers: Vec<String>) -> Self {
        let membership = Arc::new(StaticMembership::new(workers.clone()));
        let num_partitions = shuffle_partitions(workers.len());
        Self {
            workers,
            num_partitions,
            expected_worker_count: expected_worker_count_from_env(),
            membership,
        }
    }

    /// Snapshot from a live [`ClusterMembership`] provider (EKS DNS, static list, etc.).
    pub fn from_membership(membership: Arc<dyn ClusterMembership>) -> Self {
        let workers = membership.endpoints();
        let num_partitions = shuffle_partitions(workers.len());
        Self {
            workers,
            num_partitions,
            expected_worker_count: expected_worker_count_from_env(),
            membership,
        }
    }

    /// Wrap an existing trait object reference (preserves live membership for DNS refresh).
    pub fn from_membership_ref(membership: &dyn ClusterMembership) -> Self {
        // Caller should prefer `from_membership(Arc<...>)`; this path clones endpoints once.
        Self::from_membership(Arc::new(StaticMembership::new(membership.endpoints())))
    }

    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// The Flight endpoint that owns shuffle partition `p`.
    pub fn owner_endpoint(&self, partition: u32) -> Result<String> {
        self.membership
            .owner_of(partition, self.num_partitions)
            .ok_or_else(|| Error::Execution(format!("no owner for partition {partition}")))
    }

    /// Fail if the visible fan-out disagrees with the workers' file-shard modulus.
    pub fn check_shard_modulus(&self) -> Result<()> {
        let Some(expected) = self.expected_worker_count else {
            return Ok(());
        };
        let observed = self.worker_count();
        if observed != expected {
            return Err(Error::Execution(format!(
                "driver worker fan-out ({observed}) does not match WEFT_WORKER_COUNT ({expected}); \
                 workers shard files as i-of-{expected}, so continuing would silently drop data"
            )));
        }
        Ok(())
    }
}

/// How a stage exchanges data with downstream stages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExchangeMode {
    #[default]
    Hash,
    Broadcast,
    Forward,
}

/// One stage of a distributed query.
#[derive(Debug, Clone)]
pub struct StageDef {
    pub stage_id: u32,
    pub sql: String,
    pub upstream_stage_ids: Vec<u32>,
    pub hash_key_cols: Vec<u32>,
    pub exchange: ExchangeMode,
    pub plan_fragment: Option<Vec<u8>>,
    /// Driver-captured table-name→Delta/Iceberg identity map, serialized as JSON.
    pub lakehouse_snapshot_pins: String,
}

impl Default for StageDef {
    fn default() -> Self {
        Self {
            stage_id: 0,
            sql: String::new(),
            upstream_stage_ids: Vec::new(),
            hash_key_cols: Vec::new(),
            exchange: ExchangeMode::Hash,
            plan_fragment: None,
            lakehouse_snapshot_pins: String::new(),
        }
    }
}

impl StageDef {
    pub fn new(
        stage_id: u32,
        sql: impl Into<String>,
        upstream_stage_ids: Vec<u32>,
        hash_key_cols: Vec<u32>,
    ) -> Self {
        Self {
            stage_id,
            sql: sql.into(),
            upstream_stage_ids,
            hash_key_cols,
            ..Self::default()
        }
    }
}

/// A two-stage distributed aggregation plan: `partial-agg → hash shuffle → final-agg`.
#[derive(Debug, Clone)]
pub struct DistributedPlan {
    pub partial_sql: String,
    pub final_sql: String,
    pub hash_key_cols: Vec<u32>,
}

impl DistributedPlan {
    pub fn into_stages(&self) -> Vec<StageDef> {
        vec![
            StageDef::new(
                0,
                self.partial_sql.clone(),
                vec![],
                self.hash_key_cols.clone(),
            ),
            StageDef::new(1, self.final_sql.clone(), vec![0], vec![]),
        ]
    }
}

pub async fn run_distributed(
    cluster: &Cluster,
    plan: &DistributedPlan,
) -> Result<Vec<RecordBatch>> {
    run_stages_obs(cluster, &plan.into_stages(), None, None).await
}

pub async fn run_distributed_with_membership(
    membership: Arc<dyn ClusterMembership>,
    plan: &DistributedPlan,
) -> Result<Vec<RecordBatch>> {
    run_stages_obs(
        &Cluster::from_membership(membership),
        &plan.into_stages(),
        None,
        None,
    )
    .await
}

pub async fn run_stages_with_membership(
    membership: Arc<dyn ClusterMembership>,
    stages: &[StageDef],
) -> Result<Vec<RecordBatch>> {
    run_stages_obs(&Cluster::from_membership(membership), stages, None, None).await
}

pub async fn run_stages(cluster: &Cluster, stages: &[StageDef]) -> Result<Vec<RecordBatch>> {
    run_stages_obs(cluster, stages, None, None).await
}

pub async fn run_stages_obs(
    cluster: &Cluster,
    stages: &[StageDef],
    store: Option<SharedStore>,
    operation_id: Option<String>,
) -> Result<Vec<RecordBatch>> {
    let lineage = Arc::new(StageLineage::new());
    let stage_map: HashMap<u32, StageDef> =
        stages.iter().map(|s| (s.stage_id, s.clone())).collect();
    let cluster = cluster.clone();
    // Freeze the planning snapshot for this query: never reshape partitions from membership.
    let planned_workers = cluster.workers.clone();
    cluster.check_shard_modulus()?;
    let consumed: HashSet<u32> = stages
        .iter()
        .flat_map(|s| s.upstream_stage_ids.iter().copied())
        .collect();

    let outputs: Vec<&StageDef> = stages
        .iter()
        .filter(|s| !consumed.contains(&s.stage_id))
        .collect();
    let output = match outputs.as_slice() {
        [o] => *o,
        _ => {
            return Err(Error::Plan(format!(
                "distributed plan must have exactly one output stage, found {}",
                outputs.len()
            )))
        }
    };

    if autoscale_enabled() {
        let demand = parallelism_demand(&cluster, stages);
        let rec = recommend_worker_count(
            cluster.worker_count() as u32,
            cluster.worker_count() as u32,
            cluster.worker_count().saturating_mul(4) as u32,
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
            "parallelism scale recommendation (set WEFT_GATEWAY_URL+WEFT_CLUSTER_ID to apply)"
        );
    }

    // Producer / intermediate stages: one invocation per worker endpoint (each runs local SQL
    // and hash-partitions into `num_partitions` buckets). Rendezvous hashing applies to the
    // output stage only.
    for stage in stages.iter().filter(|s| s.stage_id != output.stage_id) {
        ensure_stable_membership(&cluster, &planned_workers)?;
        let np = cluster.num_partitions;
        let mut futs = Vec::new();
        for (i, endpoint) in cluster.workers.iter().enumerate() {
            let ticket = stage_ticket(stage, i as u32, np, &cluster, true);
            let membership = cluster.membership.clone();
            let ep = endpoint.clone();
            let host = ep
                .trim_start_matches("http://")
                .trim_start_matches("https://")
                .to_string();
            let lineage = lineage.clone();
            let stage_map = stage_map.clone();
            let store_c = store.clone();
            let op_c = operation_id.clone();
            let stage_id = stage.stage_id as i32;
            let task_id = store
                .as_ref()
                .map(|s| s.alloc_task_id())
                .unwrap_or(i as i64);
            if let (Some(ref s), Some(ref op)) = (&store_c, &op_c) {
                s.emit(ExecutionEvent::TaskStarted {
                    operation_id: op.clone(),
                    stage_id,
                    task_id,
                    executor_id: host.to_string(),
                    launch_time_ms: now_ms(),
                });
            }
            futs.push(async move {
                let start = std::time::Instant::now();
                let result =
                    run_stage_with_retry(&membership, ep, ticket, &lineage, &stage_map).await;
                if let (Some(s), Some(op)) = (store_c, op_c) {
                    match &result {
                        Ok(batches) => {
                            let rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
                            s.emit(ExecutionEvent::TaskFinished {
                                operation_id: op,
                                stage_id,
                                task_id,
                                executor_id: host.clone(),
                                status: TaskStatus::Success,
                                duration_ms: start.elapsed().as_millis() as i64,
                                shuffle_read_bytes: 0,
                                shuffle_write_bytes: rows * 8,
                                output_rows: rows,
                            });
                        }
                        Err(_) => {
                            s.emit(ExecutionEvent::TaskFinished {
                                operation_id: op,
                                stage_id,
                                task_id,
                                executor_id: host.clone(),
                                status: TaskStatus::Failed,
                                duration_ms: start.elapsed().as_millis() as i64,
                                shuffle_read_bytes: 0,
                                shuffle_write_bytes: 0,
                                output_rows: 0,
                            });
                        }
                    }
                }
                result
            });
        }
        for r in futures::future::join_all(futs).await {
            r?;
        }
        if let (Some(ref s), Some(ref op)) = (&store, &operation_id) {
            s.emit(ExecutionEvent::StageFinished {
                operation_id: op.clone(),
                stage_id: stage.stage_id as i32,
                status: StageStatus::Complete,
                completion_time_ms: now_ms(),
                shuffle_read_bytes: 0,
                shuffle_write_bytes: 0,
                input_rows: 0,
                output_rows: 0,
            });
        }
        // AQE: sample bucket row counts after producer stage when enabled.
        // Observability only — never shrink `num_partitions` here. Producers already wrote
        // `np` buckets; dropping the consumer range to `new_p < np` orphans buckets
        // `new_p..np-1` (silent row loss). Correct coalesced reads would need the consumer
        // to pull `p, p+new_p, …` up to the original modulus; until that exists, keep the
        // planned partition count frozen for the query lifetime.
        if aqe_enabled() {
            let mut counts = vec![0usize; np as usize];
            for p in 0..np {
                if let Ok(ep) = cluster.owner_endpoint(p) {
                    // `pull_bucket` clones from the stage cache — sampling is non-destructive.
                    if let Ok(batches) = pull_bucket_with_retry(ep, stage.stage_id, p).await {
                        counts[p as usize] = batches.iter().map(|b| b.num_rows()).sum();
                    }
                }
            }
            if let Ok(new_p) = coalesced_partitions(cluster.worker_count(), np, &counts) {
                if new_p < np {
                    if let (Some(ref s), Some(ref op)) = (&store, &operation_id) {
                        s.emit(ExecutionEvent::AqeCoalesced {
                            operation_id: op.clone(),
                            stage_id: stage.stage_id as i32,
                            old_partitions: np,
                            new_partitions: new_p,
                        });
                    }
                    tracing::info!(
                        target: "weft.aqe",
                        stage_id = stage.stage_id,
                        old_partitions = np,
                        suggested_partitions = new_p,
                        "AQE would coalesce shuffle partitions; keeping planned count to avoid orphaning buckets"
                    );
                }
            }
        }
    }

    // Output stage:
    // - Forward: run once on a single worker (full-SQL / Sail shared-storage coverage path)
    // - scatter (no upstreams, no hash keys): every worker runs local SQL (global partial agg)
    // - else: per-partition rendezvous shuffle read
    let scatter_output = output.upstream_stage_ids.is_empty() && output.hash_key_cols.is_empty();
    let mut out = Vec::new();
    ensure_stable_membership(&cluster, &planned_workers)?;
    let w = cluster.num_partitions;
    if output.exchange == ExchangeMode::Forward {
        let endpoint =
            cluster.workers.first().cloned().ok_or_else(|| {
                Error::Execution("forward stage requires at least one worker".into())
            })?;
        let host = executor_id(&endpoint);
        let stage_id = output.stage_id as i32;
        let task_id = alloc_task_id(&store, 0);
        emit_task_started(&store, &operation_id, stage_id, task_id, &host);
        let ticket = stage_ticket(output, 0, 1, &cluster, false);
        let start = std::time::Instant::now();
        match run_stage_with_retry(&cluster.membership, endpoint, ticket, &lineage, &stage_map)
            .await
        {
            Ok(batches) => {
                let rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
                emit_task_finished(
                    &store,
                    &operation_id,
                    stage_id,
                    task_id,
                    &host,
                    TaskStatus::Success,
                    start.elapsed().as_millis() as i64,
                    0,
                    0,
                    rows,
                );
                out = batches;
            }
            Err(e) => {
                emit_task_finished(
                    &store,
                    &operation_id,
                    stage_id,
                    task_id,
                    &host,
                    TaskStatus::Failed,
                    start.elapsed().as_millis() as i64,
                    0,
                    0,
                    0,
                );
                return Err(e);
            }
        }
    } else if scatter_output {
        let mut futs = Vec::new();
        for (i, endpoint) in cluster.workers.iter().enumerate() {
            let ticket = stage_ticket(output, i as u32, w, &cluster, false);
            let membership = cluster.membership.clone();
            let ep = endpoint.clone();
            let host = executor_id(&ep);
            let lineage = lineage.clone();
            let stage_map = stage_map.clone();
            let store_c = store.clone();
            let op_c = operation_id.clone();
            let stage_id = output.stage_id as i32;
            let task_id = alloc_task_id(&store, i as i64);
            emit_task_started(&store, &operation_id, stage_id, task_id, &host);
            futs.push(async move {
                let start = std::time::Instant::now();
                let result =
                    run_stage_with_retry(&membership, ep, ticket, &lineage, &stage_map).await;
                match &result {
                    Ok(batches) => {
                        let rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
                        emit_task_finished(
                            &store_c,
                            &op_c,
                            stage_id,
                            task_id,
                            &host,
                            TaskStatus::Success,
                            start.elapsed().as_millis() as i64,
                            0,
                            0,
                            rows,
                        );
                    }
                    Err(_) => {
                        emit_task_finished(
                            &store_c,
                            &op_c,
                            stage_id,
                            task_id,
                            &host,
                            TaskStatus::Failed,
                            start.elapsed().as_millis() as i64,
                            0,
                            0,
                            0,
                        );
                    }
                }
                result
            });
        }
        for r in futures::future::join_all(futs).await {
            out.extend(r?);
        }
    } else {
        // Group partitions by rendezvous owner so concurrent tasks on the same worker do not
        // race on the shared `shuffle_input` registration table.
        let mut by_endpoint: std::collections::BTreeMap<String, Vec<u32>> =
            std::collections::BTreeMap::new();
        for p in 0..w {
            let endpoint = cluster.owner_endpoint(p)?;
            by_endpoint.entry(endpoint).or_default().push(p);
        }
        let mut ep_futs = Vec::new();
        for (endpoint, parts) in by_endpoint {
            let membership = cluster.membership.clone();
            let output = output.clone();
            let cluster = cluster.clone();
            let lineage = lineage.clone();
            let stage_map = stage_map.clone();
            let host = executor_id(&endpoint);
            let store_c = store.clone();
            let op_c = operation_id.clone();
            let stage_id = output.stage_id as i32;
            let task_ids: Vec<(u32, i64)> = parts
                .iter()
                .copied()
                .map(|p| {
                    let task_id = alloc_task_id(&store, p as i64);
                    emit_task_started(&store, &operation_id, stage_id, task_id, &host);
                    (p, task_id)
                })
                .collect();
            ep_futs.push(async move {
                let mut local = Vec::new();
                for (p, task_id) in task_ids {
                    let ticket = stage_ticket(&output, p, w, &cluster, false);
                    let start = std::time::Instant::now();
                    match run_stage_with_retry(
                        &membership,
                        endpoint.clone(),
                        ticket,
                        &lineage,
                        &stage_map,
                    )
                    .await
                    {
                        Ok(batches) => {
                            let rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
                            emit_task_finished(
                                &store_c,
                                &op_c,
                                stage_id,
                                task_id,
                                &host,
                                TaskStatus::Success,
                                start.elapsed().as_millis() as i64,
                                rows * 8,
                                0,
                                rows,
                            );
                            local.extend(batches);
                        }
                        Err(e) => {
                            emit_task_finished(
                                &store_c,
                                &op_c,
                                stage_id,
                                task_id,
                                &host,
                                TaskStatus::Failed,
                                start.elapsed().as_millis() as i64,
                                0,
                                0,
                                0,
                            );
                            return Err(e);
                        }
                    }
                }
                Ok::<_, Error>(local)
            });
        }
        for r in futures::future::join_all(ep_futs).await {
            out.extend(r?);
        }
    }

    if let (Some(ref s), Some(ref op)) = (&store, &operation_id) {
        s.emit(ExecutionEvent::StageFinished {
            operation_id: op.clone(),
            stage_id: output.stage_id as i32,
            status: StageStatus::Complete,
            completion_time_ms: now_ms(),
            shuffle_read_bytes: 0,
            shuffle_write_bytes: 0,
            input_rows: 0,
            output_rows: out.iter().map(|b| b.num_rows() as i64).sum(),
        });
    }

    // Evict stage caches on all workers after the query completes.
    for ep in &cluster.workers {
        let _ = clear_worker_stages(ep.clone()).await;
    }

    let data: Vec<RecordBatch> = out.iter().filter(|b| b.num_rows() > 0).cloned().collect();
    let out = if data.is_empty() {
        out.into_iter().take(1).collect()
    } else {
        data
    };
    Ok(unify_schema(out))
}

fn executor_id(endpoint: &str) -> String {
    endpoint
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .to_string()
}

fn alloc_task_id(store: &Option<SharedStore>, fallback: i64) -> i64 {
    store
        .as_ref()
        .map(|s| s.alloc_task_id())
        .unwrap_or(fallback)
}

fn emit_task_started(
    store: &Option<SharedStore>,
    operation_id: &Option<String>,
    stage_id: i32,
    task_id: i64,
    executor_id: &str,
) {
    if let (Some(s), Some(op)) = (store, operation_id) {
        s.emit(ExecutionEvent::TaskStarted {
            operation_id: op.clone(),
            stage_id,
            task_id,
            executor_id: executor_id.to_string(),
            launch_time_ms: now_ms(),
        });
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_task_finished(
    store: &Option<SharedStore>,
    operation_id: &Option<String>,
    stage_id: i32,
    task_id: i64,
    executor_id: &str,
    status: TaskStatus,
    duration_ms: i64,
    shuffle_read_bytes: i64,
    shuffle_write_bytes: i64,
    output_rows: i64,
) {
    if let (Some(s), Some(op)) = (store, operation_id) {
        s.emit(ExecutionEvent::TaskFinished {
            operation_id: op.clone(),
            stage_id,
            task_id,
            executor_id: executor_id.to_string(),
            status,
            duration_ms,
            shuffle_read_bytes,
            shuffle_write_bytes,
            output_rows,
        });
    }
}

/// Compare endpoint lists as sets (DNS may reorder; partition ownership uses the snapshot order).
fn same_endpoint_set(a: &[String], b: &[String]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut aa = a.to_vec();
    let mut bb = b.to_vec();
    aa.sort();
    bb.sort();
    aa == bb
}

/// At each stage barrier, require the live membership to match the query planning snapshot.
///
/// Previously `refresh_cluster_workers` mutated `workers` + `num_partitions` mid-query, which
/// silently dropped shuffle buckets when the modulus shrank. Transient per-task retries
/// ([`crate::scheduler::run_stage_with_retry`]) still consult live membership for alternate
/// endpoints; only the planned fan-out is frozen here. `num_partitions` is never recomputed from
/// membership (AQE may still coalesce partitions deliberately when enabled).
fn ensure_stable_membership(cluster: &Cluster, planned_workers: &[String]) -> Result<()> {
    let fresh = cluster.membership.endpoints();
    if fresh.is_empty() {
        return Err(Error::Execution(
            "cluster membership became empty mid-query; refusing to continue".into(),
        ));
    }
    if !same_endpoint_set(&fresh, planned_workers) {
        return Err(Error::Execution(format!(
            "cluster membership changed mid-query: expected {planned_workers:?}, observed {fresh:?}; \
             refusing to continue to avoid silent shuffle row loss"
        )));
    }
    Ok(())
}

fn unify_schema(batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
    use std::sync::Arc;
    use weft_loom::arrow::datatypes::{Field, Schema};
    let Some(first) = batches.first() else {
        return batches;
    };
    let fields: Vec<Field> = first
        .schema()
        .fields()
        .iter()
        .map(|f| Field::new(f.name(), f.data_type().clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    batches
        .into_iter()
        .filter_map(|b| RecordBatch::try_new(schema.clone(), b.columns().to_vec()).ok())
        .collect()
}

fn stage_ticket(
    stage: &StageDef,
    partition_id: u32,
    num_partitions: u32,
    cluster: &Cluster,
    produce: bool,
) -> StageTicket {
    StageTicket {
        stage_id: stage.stage_id,
        partition_id,
        num_partitions,
        upstream_endpoints: if stage.upstream_stage_ids.is_empty() {
            vec![]
        } else {
            cluster.workers.clone()
        },
        stage_sql: stage.sql.clone(),
        plan_fragment: stage.plan_fragment.clone().unwrap_or_default(),
        hash_key_cols: stage.hash_key_cols.clone(),
        upstream_stage_ids: stage.upstream_stage_ids.clone(),
        produce,
        lakehouse_snapshot_pins: stage.lakehouse_snapshot_pins.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_snapshots_membership_at_scheduling_time() {
        let membership = Arc::new(StaticMembership::new(vec![
            "a:50561".into(),
            "b:50561".into(),
        ]));
        let cluster = Cluster::from_membership(membership);
        assert_eq!(cluster.worker_count(), 2);
        assert!(cluster.num_partitions >= 2);
    }

    #[test]
    fn owner_endpoint_uses_rendezvous() {
        let cluster = Cluster::new(vec!["a:1".into(), "b:1".into()]);
        let o0 = cluster.owner_endpoint(0).unwrap();
        let o1 = cluster.owner_endpoint(1).unwrap();
        assert!(o0 == "a:1" || o0 == "b:1");
        assert!(o1 == "a:1" || o1 == "b:1");
    }

    #[test]
    fn stage_def_constructor_sets_additive_defaults() {
        let mut stage = StageDef::new(7, "SELECT 1", vec![3], vec![0]);
        assert_eq!(stage.exchange, ExchangeMode::Hash);
        assert_eq!(stage.plan_fragment, None);

        stage.plan_fragment = Some(vec![1, 2, 3]);
        let cluster = Cluster::new(vec!["a:1".into()]);
        let ticket = stage_ticket(&stage, 0, 1, &cluster, true);
        assert_eq!(ticket.plan_fragment, vec![1, 2, 3]);
    }

    #[test]
    fn same_endpoint_set_ignores_order() {
        assert!(same_endpoint_set(
            &["a".into(), "b".into()],
            &["b".into(), "a".into()]
        ));
        assert!(!same_endpoint_set(&["a".into()], &["a".into(), "b".into()]));
    }

    #[test]
    fn check_shard_modulus_rejects_mismatch() {
        let mut cluster = Cluster::new(vec!["http://127.0.0.1:1".into()]);
        cluster.expected_worker_count = Some(2);
        let err = cluster.check_shard_modulus().unwrap_err().to_string();
        assert!(err.contains("WEFT_WORKER_COUNT"));
        assert!(err.contains("fan-out"));
    }

    #[test]
    fn check_shard_modulus_allows_match_and_unset() {
        let mut cluster = Cluster::new(vec![
            "http://127.0.0.1:1".into(),
            "http://127.0.0.1:2".into(),
        ]);
        assert!(cluster.check_shard_modulus().is_ok());
        cluster.expected_worker_count = Some(2);
        assert!(cluster.check_shard_modulus().is_ok());
    }

    #[test]
    fn ensure_stable_membership_rejects_shrink() {
        let cluster = Cluster::new(vec!["a:1".into(), "b:1".into()]);
        // Swap in a membership that reports a different set.
        let mut cluster = cluster;
        cluster.membership = Arc::new(StaticMembership::new(vec!["a:1".into()]));
        let err = ensure_stable_membership(&cluster, &["a:1".into(), "b:1".into()])
            .unwrap_err()
            .to_string();
        assert!(err.contains("membership changed mid-query"));
        assert!(err.contains("expected"));
        assert!(err.contains("observed"));
    }
}
