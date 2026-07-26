//! The distributed driver: orchestrate a stage DAG across a static set of workers.
//!
//! A query is expressed as a topologically-ordered list of [`StageDef`]s. Each *producer* stage
//! (one that another stage consumes) runs on every worker, hash-partitions its output into one
//! bucket per worker, and caches the buckets; the single *output* stage runs on every worker as a
//! consumer, pulling its bucket of each upstream from every worker and returning the result.
//!
//! The MVP shape — two stages, `partial-agg → hash shuffle → final-agg` — is the
//! [`DistributedPlan`] convenience built on top of this (see [`DistributedPlan::into_stages`]).
//! Multiple upstreams on the output stage express a **shuffle join**: both sides hash-partition on
//! the join key so matching keys co-locate on one worker, which then joins them locally.
//!
//! v1 limits: producer stages must be leaves (an intermediate stage that both consumes *and*
//! produces — needed for chains of joins like TPC-H Q5 — is a follow-up); static worker list.
//! Shuffle buckets spill to `WEFT_SPILL_DIR` when over `WEFT_SHUFFLE_SPILL_BYTES` (see
//! [`crate::shuffle::spill`]); streaming `do_exchange` is still a stub.

use std::collections::HashSet;

use weft_common::{Error, Result};
use weft_loom::arrow::record_batch::RecordBatch;

use crate::flight::run_stage_on_worker;
use crate::membership::ClusterMembership;
use crate::shuffle::protocol::StageTicket;

/// A cluster of worker Flight endpoints (e.g. `http://127.0.0.1:50561`) for one query.
///
/// This is a **snapshot** of the worker set taken at stage-scheduling time. For a static cluster
/// it's built directly from a list ([`Cluster::new`]); on EKS it's snapshotted from a live
/// [`ClusterMembership`] ([`Cluster::from_membership`]) so the partition count tracks the workers
/// that are actually up when the query starts.
#[derive(Debug, Clone)]
pub struct Cluster {
    /// Worker endpoints. Partition `i` is owned by `workers[i]`.
    pub workers: Vec<String>,
}

impl Cluster {
    /// Build a cluster from a list of endpoints.
    pub fn new(workers: Vec<String>) -> Self {
        Self { workers }
    }

    /// Snapshot the current worker set from a live [`ClusterMembership`] provider. This is the seam
    /// the EKS `K8sMembership` (DNS-SRV / EndpointSlice watch) drives: resolve the workers that are
    /// up *now*, then schedule the query against that snapshot.
    pub fn from_membership(membership: &dyn ClusterMembership) -> Self {
        Self {
            workers: membership.endpoints(),
        }
    }

    /// Number of workers (== number of shuffle partitions) in this snapshot.
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }
}

/// One stage of a distributed query.
#[derive(Debug, Clone)]
pub struct StageDef {
    /// Identifies this stage's output across the cluster (consumers cache/read under this id).
    pub stage_id: u32,
    /// SQL to run for this stage. A leaf reads the worker's local base tables; a consumer reads
    /// `shuffle_input` (one upstream) or `shuffle_input_{i}` (the i-th of several).
    pub sql: String,
    /// Upstream stage ids this stage consumes, in `shuffle_input_{i}` order (empty == a leaf).
    pub upstream_stage_ids: Vec<u32>,
    /// Output column indices to hash-partition this stage's result on, so its consumer's key rows
    /// co-locate (the shuffle key). Ignored for the output stage (nothing consumes it).
    pub hash_key_cols: Vec<u32>,
}

/// A two-stage distributed aggregation plan: `partial-agg → hash shuffle → final-agg`.
///
/// Retained as the ergonomic entry point (and CLI surface) for the common single-shuffle
/// aggregation; it lowers to the general [`StageDef`] DAG via [`Self::into_stages`].
#[derive(Debug, Clone)]
pub struct DistributedPlan {
    /// Stage-0 SQL, run per worker over its local table(s); its output is shuffled.
    pub partial_sql: String,
    /// Stage-1 SQL, run per worker over the `shuffle_input` table built from pulled buckets.
    pub final_sql: String,
    /// Output column indices of the stage-0 result to hash-partition on (the group key).
    pub hash_key_cols: Vec<u32>,
}

impl DistributedPlan {
    /// Lower to the general two-stage [`StageDef`] DAG: a leaf partial stage (id 0) feeding a
    /// final consumer stage (id 1).
    pub fn into_stages(&self) -> Vec<StageDef> {
        vec![
            StageDef {
                stage_id: 0,
                sql: self.partial_sql.clone(),
                upstream_stage_ids: vec![],
                hash_key_cols: self.hash_key_cols.clone(),
            },
            StageDef {
                stage_id: 1,
                sql: self.final_sql.clone(),
                upstream_stage_ids: vec![0],
                hash_key_cols: vec![],
            },
        ]
    }
}

/// Run a two-stage [`DistributedPlan`] across `cluster` and return the concatenated final result.
pub async fn run_distributed(
    cluster: &Cluster,
    plan: &DistributedPlan,
) -> Result<Vec<RecordBatch>> {
    run_stages(cluster, &plan.into_stages()).await
}

/// Run a two-stage [`DistributedPlan`] against a live [`ClusterMembership`], snapshotting the
/// worker set at scheduling time. The membership-driven entry point the Spark Connect server's
/// `--cluster` mode calls on EKS (vs. the static-list [`run_distributed`]).
pub async fn run_distributed_with_membership(
    membership: &dyn ClusterMembership,
    plan: &DistributedPlan,
) -> Result<Vec<RecordBatch>> {
    run_stages(&Cluster::from_membership(membership), &plan.into_stages()).await
}

/// Run an arbitrary stage DAG against a live [`ClusterMembership`], snapshotting the worker set at
/// scheduling time. See [`run_stages`] for the DAG contract.
pub async fn run_stages_with_membership(
    membership: &dyn ClusterMembership,
    stages: &[StageDef],
) -> Result<Vec<RecordBatch>> {
    run_stages(&Cluster::from_membership(membership), stages).await
}

/// Run an arbitrary stage DAG across `cluster` and return the output stage's concatenated result.
///
/// `stages` must be topologically ordered (every upstream appears before the stage that consumes
/// it). Exactly one stage must be an *output* (no other stage lists it as an upstream); it is run
/// last as a consumer on every worker and its per-worker results are concatenated.
pub async fn run_stages(cluster: &Cluster, stages: &[StageDef]) -> Result<Vec<RecordBatch>> {
    let w = cluster.workers.len() as u32;
    let consumed: HashSet<u32> = stages
        .iter()
        .flat_map(|s| s.upstream_stage_ids.iter().copied())
        .collect();

    // Identify the single output stage (consumed by nothing).
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

    // Run every non-output stage on every worker (barrier after each). `stages` is topologically
    // ordered, so by the time a stage runs its upstreams are already cached. Intermediate stages
    // (those that both consume upstreams and produce for downstreams) run here too.
    for stage in stages.iter().filter(|s| s.stage_id != output.stage_id) {
        let mut futs = Vec::new();
        for (i, endpoint) in cluster.workers.iter().enumerate() {
            futs.push(run_stage_on_worker(
                endpoint.clone(),
                stage_ticket(stage, i as u32, w, cluster, true),
            ));
        }
        for r in futures::future::join_all(futs).await {
            r?; // surface any stage failure
        }
    }

    // Run the output (consumer) stage on every worker; concatenate the per-worker slices.
    let mut futs = Vec::new();
    for (p, endpoint) in cluster.workers.iter().enumerate() {
        futs.push(run_stage_on_worker(
            endpoint.clone(),
            stage_ticket(output, p as u32, w, cluster, false),
        ));
    }
    let mut out = Vec::new();
    for r in futures::future::join_all(futs).await {
        out.extend(r?);
    }
    // Drop zero-row batches: a worker that produced nothing returns a schema-only padding batch
    // (the shuffle transport recovers an empty result as one typed-but-empty batch), and an empty
    // result can infer a divergent schema. Keep just one if the whole result is empty.
    let data: Vec<RecordBatch> = out.iter().filter(|b| b.num_rows() > 0).cloned().collect();
    let out = if data.is_empty() {
        out.into_iter().take(1).collect()
    } else {
        data
    };
    Ok(unify_schema(out))
}

/// Coerce gathered batches to one common schema (all fields nullable). Different workers can infer
/// slightly different nullability for the same output (e.g. an empty result vs a populated one), and
/// the concatenated result must be schema-consistent so a caller can re-register or concatenate it.
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

/// Build the [`StageTicket`] for running `stage` as partition `partition_id` on the cluster.
/// `produce` is true for any non-output stage (hash-partition + cache), false for the output.
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
        // A consumer pulls each upstream's bucket from every worker; a leaf has no upstreams.
        upstream_endpoints: if stage.upstream_stage_ids.is_empty() {
            vec![]
        } else {
            cluster.workers.clone()
        },
        stage_sql: stage.sql.clone(),
        plan_fragment: vec![],
        hash_key_cols: stage.hash_key_cols.clone(),
        upstream_stage_ids: stage.upstream_stage_ids.clone(),
        produce,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::StaticMembership;

    #[test]
    fn cluster_snapshots_membership_at_scheduling_time() {
        let membership = StaticMembership::new(vec!["a:50561".into(), "b:50561".into()]);
        let cluster = Cluster::from_membership(&membership);
        assert_eq!(cluster.worker_count(), 2);
        assert_eq!(
            cluster.workers,
            vec!["a:50561".to_string(), "b:50561".to_string()]
        );
    }
}
