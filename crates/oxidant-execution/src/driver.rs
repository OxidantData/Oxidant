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
//! `OXIDANT_SHUFFLE_PARTITIONS` (like `spark.sql.shuffle.partitions`) or, when that is unset,
//! `OXIDANT_DEFAULT_PARALLELISM`. Shuffle buckets spill when over the configured memory budget
//! (see [`crate::shuffle::spill`]); push-based `do_exchange` complements pull-based shuffle reads.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use datafusion::logical_expr::LogicalPlan;
use datafusion::scalar::ScalarValue;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use futures::FutureExt;
use oxidant_common::{Error, Result};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_observability::{now_ms, ExecutionEvent, SharedStore, StageStatus, TaskStatus};

use crate::aqe::{aqe_enabled, coalesced_partitions, stage_input_stats_enabled};
use crate::autoscale::{
    autoscale_enabled, parallelism_demand, recommend_worker_count, task_slots_per_worker,
};
use crate::dag_dispatch::StageDag;
use crate::flight::{
    bucket_row_counts, cancel_stage_on_worker, clear_worker_stages, pull_bucket_with_retry,
};
use crate::lineage::StageLineage;
use crate::membership::{ClusterMembership, StaticMembership};
use crate::scheduler::run_stage_with_retry;
use crate::shuffle::protocol::StageTicket;

/// Number of hash-shuffle partitions for the next query.
pub fn shuffle_partitions(worker_count: usize) -> u32 {
    std::env::var("OXIDANT_SHUFFLE_PARTITIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n: &u32| n > 0)
        .or_else(|| {
            std::env::var("OXIDANT_DEFAULT_PARALLELISM")
                .ok()
                .and_then(|s| s.parse().ok())
                .filter(|&n: &u32| n > 0)
        })
        .unwrap_or(worker_count.max(1) as u32)
}

/// Expected worker fan-out from `OXIDANT_WORKER_COUNT` (same env workers use for file sharding).
pub fn expected_worker_count_from_env() -> Option<usize> {
    std::env::var("OXIDANT_WORKER_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
}

/// Query-level cancel flags (KAN-17): the Spark Connect `Interrupt` RPC trips the flag for an
/// in-flight query; the driver polls it between stages and a watcher aborts the plan's stages
/// still running on workers so their task slots free.
static QUERY_CANCELS: OnceLock<Mutex<HashMap<String, Arc<AtomicBool>>>> = OnceLock::new();

fn query_cancels() -> &'static Mutex<HashMap<String, Arc<AtomicBool>>> {
    QUERY_CANCELS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Register `query_id` for cancellation; returns the flag the driver polls between stages.
pub fn register_query_cancel(query_id: &str) -> Arc<AtomicBool> {
    query_cancels()
        .lock()
        .expect("query cancels poisoned")
        .entry(query_id.to_string())
        .or_default()
        .clone()
}

/// Trip the cancel flag for `query_id`. Returns false when the query is not currently
/// executing (nothing to cancel).
pub fn cancel_query(query_id: &str) -> bool {
    match query_cancels()
        .lock()
        .expect("query cancels poisoned")
        .get(query_id)
    {
        Some(flag) => {
            flag.store(true, Ordering::Relaxed);
            true
        }
        None => false,
    }
}

/// Drop the registration when the query finishes (any outcome).
pub fn unregister_query_cancel(query_id: &str) {
    query_cancels()
        .lock()
        .expect("query cancels poisoned")
        .remove(query_id);
}

/// Fallback id for queries that arrive without an operation id (CLI driver, tests).
fn new_query_id() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    format!(
        "q-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed) + 1
    )
}

/// Watch the query cancel flag; when tripped, best-effort cancel this plan's stages on every
/// worker so in-flight stage tasks abort and free their slots instead of running to the stage
/// timeout.
fn spawn_cancel_watcher(
    workers: &[String],
    stages: &[StageDef],
    cancel: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    let workers = workers.to_vec();
    let stage_ids: Vec<u32> = stages.iter().map(|s| s.stage_id).collect();
    tokio::spawn(async move {
        while !cancel.load(Ordering::Relaxed) {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        cancel_stages_on_workers(&workers, stage_ids.iter().copied()).await;
    })
}

/// Fire the `cancel_stage` action for each stage id at every worker; all errors ignored
/// (cancel is best-effort).
async fn cancel_stages_on_workers(workers: &[String], stage_ids: impl Iterator<Item = u32>) {
    let stage_ids: Vec<u32> = stage_ids.collect();
    let futs: Vec<_> = workers
        .iter()
        .flat_map(|ep| {
            stage_ids
                .iter()
                .map(move |&id| cancel_stage_on_worker(ep.clone(), id))
        })
        .collect();
    let _ = futures::future::join_all(futs).await;
}

/// Evict cached stage outputs on every worker (KAN-18); all errors ignored. Concurrent per
/// worker — a dead or slow worker must not delay the eviction of the others (or, on the
/// client's result path, the query's own return).
async fn clear_stages_on_workers(workers: &[String]) {
    let futs: Vec<_> = workers
        .iter()
        .map(|ep| clear_worker_stages(ep.clone()))
        .collect();
    let _ = futures::future::join_all(futs).await;
}

/// KAN-46: RAII backstop for [`run_stages_obs`] cleanup. When the driver future is dropped
/// mid-query — tonic cancels the Spark Connect `ExecutePlan` handler future as soon as the
/// client's call goes away (disconnect, client-side timeout) — ordinary cleanup written
/// after the inner await never runs: worker stage tasks kept burning slots until the stage
/// timeout, cached shuffle buckets stayed resident (SF10: 17–27 GB of worker RSS pinned by
/// abandoned queries), and both the cancel-registry entry and the watcher task leaked. On
/// drop, abort the watcher, unregister the query, and spawn the same best-effort stage
/// cancel + `clear_stages` sweep the normal exit path runs inline.
struct QueryAbortGuard {
    workers: Vec<String>,
    stage_ids: Vec<u32>,
    query_id: String,
    watcher: Option<tokio::task::JoinHandle<()>>,
    armed: bool,
}

impl QueryAbortGuard {
    fn new(
        cluster: &Cluster,
        stages: &[StageDef],
        query_id: &str,
        watcher: tokio::task::JoinHandle<()>,
    ) -> Self {
        Self {
            workers: cluster.workers.clone(),
            stage_ids: stages.iter().map(|s| s.stage_id).collect(),
            query_id: query_id.to_string(),
            watcher: Some(watcher),
            armed: true,
        }
    }

    /// Normal exit: hand the watcher back so the caller can abort it; the guard stands down
    /// and the caller runs the cleanup inline (awaited, exactly once).
    fn disarm(&mut self) -> tokio::task::JoinHandle<()> {
        self.armed = false;
        self.watcher.take().expect("abort guard disarmed once")
    }
}

impl Drop for QueryAbortGuard {
    fn drop(&mut self) {
        if let Some(watcher) = self.watcher.take() {
            watcher.abort();
        }
        if !self.armed {
            return;
        }
        unregister_query_cancel(&self.query_id);
        let workers = std::mem::take(&mut self.workers);
        let stage_ids = std::mem::take(&mut self.stage_ids);
        // Detached and best-effort: the query's result is already abandoned. During runtime
        // shutdown there may be nothing left to spawn onto; the stage timeout (KAN-17) and
        // the stage-output TTL (KAN-18) remain the backstops there.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                cancel_stages_on_workers(&workers, stage_ids.iter().copied()).await;
                clear_stages_on_workers(&workers).await;
            });
        }
    }
}

/// A cluster snapshot for one query: workers + stable partition→owner mapping.
#[derive(Clone)]
pub struct Cluster {
    /// Unique worker endpoints in this snapshot.
    pub workers: Vec<String>,
    /// Hash-shuffle partition count (may exceed worker count).
    pub num_partitions: u32,
    /// When set (or derived from `OXIDANT_WORKER_COUNT`), the driver hard-fails if the
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
                "driver worker fan-out ({observed}) does not match OXIDANT_WORKER_COUNT ({expected}); \
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
    /// Comma-separated tables the driver classified as fully replicated (auto-broadcast /
    /// `OXIDANT_REPLICATED_TABLES` override). Empty keeps older tickets wire-compatible.
    pub replicated_tables: String,
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
            replicated_tables: String::new(),
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

/// KAN-27: marker for the one-row broadcast of an **uncorrelated** scalar subquery (TPC-H Q11's
/// global HAVING threshold). The planner emits the scalar's partial/combine stages, then embeds
/// `'{TOKEN}'` (a quoted string literal) in the dependent stage's SQL where the scalar value
/// belongs. The scalar stage is identified **positionally** — the unique stage no other stage
/// lists as an upstream, apart from the output stage itself — so the marker stays valid for
/// callers that rebase stage ids before dispatch (oxidant-bench namespaces ids per query). The
/// driver runs the scalar stages first (the stage list is topologically ordered), pulls the
/// single resulting row, and replaces the token with the computed literal before dispatching the
/// dependent stage — Spark's subquery-execution + literal-injection pattern — so workers never
/// see the token.
pub(crate) const SCALAR_TOKEN: &str = "__OXIDANT_SCALAR_STAGE__";

/// Replace the scalar token in `stage`'s SQL (if any) with the literal computed from the scalar
/// stage's one-row output (pulled from the workers on first use). The scalar stage must precede
/// `stage` in the topologically-ordered stage list, i.e. it has already completed by the time
/// `stage` is dispatched.
async fn substitute_scalar_tokens(
    cluster: &Cluster,
    stage: &StageDef,
    stages: &[StageDef],
    scalar_stage: Option<&StageDef>,
    literal: &mut Option<String>,
) -> Result<StageDef> {
    if !stage.sql.contains(SCALAR_TOKEN) {
        return Ok(stage.clone());
    }
    let Some(scalar) = scalar_stage else {
        return Err(Error::Plan(format!(
            "stage {} references {SCALAR_TOKEN} but the plan has no scalar stage",
            stage.stage_id
        )));
    };
    let pos = stages
        .iter()
        .position(|s| s.stage_id == stage.stage_id)
        .ok_or_else(|| Error::Plan(format!("stage {} not in the stage list", stage.stage_id)))?;
    let spos = stages
        .iter()
        .position(|s| s.stage_id == scalar.stage_id)
        .ok_or_else(|| {
            Error::Plan(format!(
                "scalar stage {} not in the stage list",
                scalar.stage_id
            ))
        })?;
    if spos >= pos {
        return Err(Error::Plan(format!(
            "scalar stage {} must precede stage {} in the stage list",
            scalar.stage_id, stage.stage_id
        )));
    }
    let lit = match literal {
        Some(l) => l.clone(),
        None => {
            let l = pull_scalar_literal(cluster, scalar.stage_id).await?;
            *literal = Some(l.clone());
            l
        }
    };
    let sql = stage.sql.replace(&format!("'{SCALAR_TOKEN}'"), &lit);
    Ok(StageDef {
        sql,
        ..stage.clone()
    })
}

/// Pull a scalar stage's complete (single-row, single-column) output from the workers and render
/// it as a SQL literal. Zero rows — a global aggregate over an empty input, suppressed by the
/// combine stage's `HAVING COUNT(a0) > 0` — render as `NULL`.
async fn pull_scalar_literal(cluster: &Cluster, stage_id: u32) -> Result<String> {
    let mut values: Vec<ScalarValue> = Vec::new();
    for ep in &cluster.workers {
        for p in 0..cluster.num_partitions {
            for b in pull_bucket_with_retry(ep.clone(), stage_id, p).await? {
                for row in 0..b.num_rows() {
                    values.push(ScalarValue::try_from_array(b.column(0), row).map_err(|e| {
                        Error::Execution(format!("scalar stage {stage_id}: extract value: {e}"))
                    })?);
                }
            }
        }
    }
    match values.as_slice() {
        [] => Ok("NULL".to_string()),
        [v] => scalar_literal_sql(v),
        _ => Err(Error::Execution(format!(
            "scalar stage {stage_id} produced {} rows (expected at most one)",
            values.len()
        ))),
    }
}

/// Types [`scalar_literal_sql`] can inline as a SQL literal. The planner checks this at plan
/// time so an off-type scalar subquery stays on the gather fallback instead of failing mid-query.
pub(crate) fn scalar_literal_supported(dt: &datafusion::arrow::datatypes::DataType) -> bool {
    use datafusion::arrow::datatypes::DataType as DT;
    matches!(
        dt,
        DT::Null
            | DT::Boolean
            | DT::Int8
            | DT::Int16
            | DT::Int32
            | DT::Int64
            | DT::UInt8
            | DT::UInt16
            | DT::UInt32
            | DT::UInt64
            | DT::Float32
            | DT::Float64
            | DT::Decimal128(_, _)
            | DT::Utf8
            | DT::LargeUtf8
    )
}

/// Render one scalar value as a SQL literal the worker dialects can re-parse.
fn scalar_literal_sql(v: &ScalarValue) -> Result<String> {
    if v.is_null() {
        return Ok("NULL".to_string());
    }
    let lit = match v {
        ScalarValue::Int8(Some(x)) => x.to_string(),
        ScalarValue::Int16(Some(x)) => x.to_string(),
        ScalarValue::Int32(Some(x)) => x.to_string(),
        ScalarValue::Int64(Some(x)) => x.to_string(),
        ScalarValue::UInt8(Some(x)) => x.to_string(),
        ScalarValue::UInt16(Some(x)) => x.to_string(),
        ScalarValue::UInt32(Some(x)) => x.to_string(),
        ScalarValue::UInt64(Some(x)) => x.to_string(),
        ScalarValue::Float32(Some(x)) => float_literal(f64::from(*x))?,
        ScalarValue::Float64(Some(x)) => float_literal(*x)?,
        ScalarValue::Decimal128(Some(x), _, scale) => decimal_literal(*x, *scale)?,
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            format!("'{}'", s.replace('\'', "''"))
        }
        ScalarValue::Boolean(Some(b)) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        other => {
            return Err(Error::Unsupported(format!(
                "auto-distribute: scalar subquery result of type {} cannot be inlined",
                other.data_type()
            )))
        }
    };
    Ok(lit)
}

/// Shortest round-trip float literal in scientific notation (`1.88e-1`), which every supported
/// worker SQL dialect parses back to the same `f64`.
fn float_literal(x: f64) -> Result<String> {
    if !x.is_finite() {
        return Err(Error::Unsupported(
            "auto-distribute: non-finite scalar subquery result cannot be inlined".into(),
        ));
    }
    Ok(format!("{x:e}"))
}

/// Render a Decimal128 mantissa/scale as a plain decimal literal (`-123.45`).
fn decimal_literal(v: i128, scale: i8) -> Result<String> {
    if scale <= 0 {
        let factor = 10i128.checked_pow((-scale) as u32).ok_or_else(|| {
            Error::Unsupported(format!(
                "auto-distribute: decimal scalar with scale {scale} cannot be inlined"
            ))
        })?;
        return v.checked_mul(factor).map(|x| x.to_string()).ok_or_else(|| {
            Error::Unsupported(format!(
                "auto-distribute: decimal scalar with scale {scale} cannot be inlined"
            ))
        });
    }
    let scale = scale as usize;
    let neg = v < 0;
    let digits = format!("{:0>width$}", v.unsigned_abs(), width = scale + 1);
    let (int, frac) = digits.split_at(digits.len() - scale);
    Ok(format!("{}{int}.{frac}", if neg { "-" } else { "" }))
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

/// Planner inputs retained for adaptive join-order re-optimization
/// (`OXIDANT_REOPT_JOIN_ORDER`): the logical plan the stage DAG was derived from and the
/// replicated-table classification, so the driver can re-derive the shuffle-join chain's
/// tail from barrier-measured leaf cardinalities and splice it onto the dispatched prefix
/// (see [`crate::plan::join_chain::replan_chain_tail`]).
pub struct ReoptContext<'a> {
    pub plan: &'a LogicalPlan,
    pub replicated: &'a [&'a str],
}

/// Adaptive join-order re-optimization is **off by default** (`OXIDANT_REOPT_JOIN_ORDER=1` to
/// enable): when on, the driver front-loads leaf producer stages and, at the last leaf's
/// stage barrier, re-sequences the shuffle-join chain's tail by barrier-measured leaf row
/// counts — the reorder Spark AQE structurally lacks (it never re-plans join order once the
/// stage graph is fixed). Off means byte-identical behavior, dispatch order included.
pub fn reopt_join_order_enabled() -> bool {
    std::env::var("OXIDANT_REOPT_JOIN_ORDER")
        .ok()
        .as_deref()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Dependency-aware concurrent stage dispatch is **on by default** (env
/// `OXIDANT_CONCURRENT_STAGES`): a stage dispatches as soon as ALL of its upstream stages
/// complete instead of waiting for the whole previous stage, so independent branch arms
/// (TPC-DS Q4/Q61/Q78 shapes) overlap — a consumer still waits for every upstream, so
/// per-consumer barrier semantics are unchanged. `0`/`false`/`off`/`no` restores the
/// strictly-sequential dispatch (also the automatic fallback while `OXIDANT_REOPT_JOIN_ORDER`
/// re-optimization is active, since it splices the stage list mid-dispatch).
///
/// KNOWN ANOMALY (KAN-2 follow-up): at SF10, simple linear star-scan queries
/// (Q63/Q13/Q96 class) run their leaf scan task ~2x slower with concurrent dispatch on
/// (Q63 hot 10.0s vs 2.97s with it off) — same worker, same shard, same stage SQL, and
/// nothing to overlap in a linear chain, so the mechanism is not yet understood. Measured
/// on the full 99-query matrix the arm-overlap win outweighs the penalty ~5:1
/// (319s on vs 369s off), so the default stays on; set the env off for star-scan-heavy
/// workloads until the leaf slowdown is root-caused.
pub fn concurrent_stages_enabled() -> bool {
    std::env::var("OXIDANT_CONCURRENT_STAGES")
        .ok()
        .as_deref()
        .map(|v| {
            !matches!(
                v.to_ascii_lowercase().as_str(),
                "0" | "false" | "off" | "no"
            )
        })
        .unwrap_or(true)
}

pub async fn run_distributed(
    cluster: &Cluster,
    plan: &DistributedPlan,
) -> Result<Vec<RecordBatch>> {
    run_stages_obs(cluster, &plan.into_stages(), None, None, None).await
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
        None,
    )
    .await
}

pub async fn run_stages_with_membership(
    membership: Arc<dyn ClusterMembership>,
    stages: &[StageDef],
) -> Result<Vec<RecordBatch>> {
    run_stages_obs(
        &Cluster::from_membership(membership),
        stages,
        None,
        None,
        None,
    )
    .await
}

pub async fn run_stages(cluster: &Cluster, stages: &[StageDef]) -> Result<Vec<RecordBatch>> {
    run_stages_obs(cluster, stages, None, None, None).await
}

pub async fn run_stages_obs(
    cluster: &Cluster,
    stages: &[StageDef],
    store: Option<SharedStore>,
    operation_id: Option<String>,
    reopt: Option<ReoptContext<'_>>,
) -> Result<Vec<RecordBatch>> {
    // KAN-17: register a query-level cancel flag so a Spark Connect `Interrupt` stops the
    // driver at the next stage barrier and aborts this plan's stages still running on workers.
    let query_id = operation_id
        .clone()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(new_query_id);
    let cancel = register_query_cancel(&query_id);
    let watcher = spawn_cancel_watcher(&cluster.workers, stages, cancel.clone());
    // KAN-46: when this future is dropped mid-query (a disconnected Spark Connect client
    // cancels the handler future), the guard's Drop runs the same cancel + eviction the
    // normal exit path below runs inline.
    let mut abort_guard = QueryAbortGuard::new(cluster, stages, &query_id, watcher);
    let result = run_stages_obs_inner(
        cluster,
        stages,
        store,
        operation_id,
        &query_id,
        &cancel,
        reopt,
    )
    .await;
    abort_guard.disarm().abort();
    unregister_query_cancel(&query_id);
    if result.is_err() {
        // Best-effort: abort any of this plan's stages still running on workers so wedged
        // tasks free their slots instead of pinning them until the stage timeout.
        cancel_stages_on_workers(&cluster.workers, stages.iter().map(|s| s.stage_id)).await;
    }
    // KAN-18: evict stage caches on every exit path — success, stage error, cancel, or
    // timeout — so failed/wedged queries don't leak producer buckets on workers. Best-effort
    // per worker: a dead worker must not mask the query's own result.
    clear_stages_on_workers(&cluster.workers).await;
    result
}

/// Check the query cancel flag at a stage barrier (KAN-17).
fn check_query_cancelled(query_id: &str, cancel: &AtomicBool) -> Result<()> {
    if cancel.load(Ordering::Relaxed) {
        return Err(Error::Execution(format!(
            "query {query_id} cancelled via interrupt"
        )));
    }
    Ok(())
}

async fn run_stages_obs_inner(
    cluster: &Cluster,
    stages: &[StageDef],
    store: Option<SharedStore>,
    operation_id: Option<String>,
    query_id: &str,
    cancel: &AtomicBool,
    reopt: Option<ReoptContext<'_>>,
) -> Result<Vec<RecordBatch>> {
    let lineage = Arc::new(StageLineage::new());
    // Owned so a re-optimized join tail can splice in mid-query (OXIDANT_REOPT_JOIN_ORDER);
    // with the gate off the clone changes nothing observable.
    let mut stages: Vec<StageDef> = stages.to_vec();
    let mut stage_map: HashMap<u32, StageDef> =
        stages.iter().map(|s| (s.stage_id, s.clone())).collect();
    let cluster = cluster.clone();
    // Freeze the planning snapshot for this query: never reshape partitions from membership.
    let planned_workers = cluster.workers.clone();
    cluster.check_shard_modulus()?;
    let consumed: HashSet<u32> = stages
        .iter()
        .flat_map(|s| s.upstream_stage_ids.iter().copied())
        .collect();
    // AQE coalesce decisions for this query (producer stage id -> coalesced read modulus),
    // recorded at each producer's stage barrier. A later consumer stage of THIS query reads
    // a coalesced upstream through the modulus mapping; the planned partition count stays
    // frozen at `cluster.num_partitions` everywhere else.
    let mut coalesced: HashMap<u32, u32> = HashMap::new();
    // Barrier-measured per-bucket row totals of each producer stage's output (producer stage
    // id -> `num_partitions` bucket totals, summed across workers), recorded at the same
    // barrier when `OXIDANT_STAGE_INPUT_STATS` sampling is on. Consumer tickets carry their
    // upstreams' totals (`StageTicket::upstream_bucket_rows`) so workers attach exact
    // per-task row counts to the `shuffle_input*` scans — the plan-time join guard then
    // sizes hash builds from measured data instead of unknown statistics.
    let mut stage_rows: HashMap<u32, Vec<u64>> = HashMap::new();

    let outputs: Vec<&StageDef> = stages
        .iter()
        .filter(|s| !consumed.contains(&s.stage_id))
        .collect();
    // KAN-27: a scalar-token plan has one extra unconsumed stage — the scalar combine — which
    // the driver consumes directly (literal injection) instead of a downstream worker stage. It
    // is matched positionally (so callers may rebase stage ids); the output stage is the last
    // stage of the topologically-ordered list.
    let token_present = stages.iter().any(|s| s.sql.contains(SCALAR_TOKEN));
    // Owned: a re-optimized tail swap rebuilds the stage vec, and the driver re-reads the
    // (re-derived) output stage from it afterwards.
    let (mut output, scalar_stage): (StageDef, Option<StageDef>) = match outputs.as_slice() {
        [o] if !token_present => ((*o).clone(), None),
        [s, o] if token_present && Some(o.stage_id) == stages.last().map(|t| t.stage_id) => {
            ((*o).clone(), Some((*s).clone()))
        }
        _ => {
            return Err(Error::Plan(format!(
                "distributed plan must have exactly one output stage, found {}",
                outputs.len()
            )))
        }
    };

    if autoscale_enabled() {
        let demand = parallelism_demand(&cluster, &stages);
        let rec = recommend_worker_count(
            cluster.worker_count() as u32,
            cluster.worker_count() as u32,
            cluster.worker_count().saturating_mul(4) as u32,
            &demand,
            task_slots_per_worker(),
        );
        tracing::info!(
            target: "oxidant.autoscale",
            current = rec.current_workers,
            recommended = rec.recommended_workers,
            peak = rec.peak_task_demand,
            should_scale = rec.should_scale,
            reason = %rec.reason,
            "parallelism scale recommendation (set OXIDANT_GATEWAY_URL+OXIDANT_CLUSTER_ID to apply)"
        );
    }

    // Producer stages: one invocation per worker endpoint (each runs local SQL
    // and hash-partitions into `num_partitions` buckets). Rendezvous hashing applies to
    // intermediate stages (one task per partition, so every upstream bucket is consumed —
    // KAN-32) and to the output stage.
    //
    // A `Forward` producer (a replicated-only UNION/aggregation arm — see
    // `stage_planner::reject_unsafe_broadcast_shapes`/`try_split_broadcast_union`) is run on
    // exactly the first worker instead of every worker: every worker holds the *same* full copy
    // of a replicated-only arm's data, so running it on all of them and hash-partitioning each
    // worker's identical output would land every worker's copy in the same target bucket(s),
    // multiplying that arm's contribution by the worker count once a downstream combine stage
    // sums the buckets back together. Downstream consumers still list every worker as an
    // upstream endpoint for this stage id (`stage_ticket`'s shared `upstream_endpoints`), but
    // workers that never produced it simply have no cache entry and `read_shuffle` serves them
    // an empty bucket rather than erroring — so a single real producer is sufficient.
    let mut scalar_literal: Option<String> = None;
    // OXIDANT_REOPT_JOIN_ORDER: when the gate is on (and the plan is re-optimizable — a
    // scalar-token plan's positional literal pipeline must keep its dispatch order),
    // stable-partition the worklist so zero-upstream leaf producers dispatch before the
    // join stages. Topological order is preserved (leaves have no upstreams), and with the
    // gate off the dispatch order is byte-identical to before.
    let reopt_active = reopt.is_some() && reopt_join_order_enabled() && !token_present;
    // OXIDANT_CONCURRENT_STAGES (default on): dispatch each stage as soon as every one of its
    // upstreams has completed instead of waiting out the whole previous stage — independent
    // branch arms overlap, while a consumer still waits for ALL of its upstreams (the stage
    // barrier becomes per-consumer, not globally ordered). The reopt path splices a
    // re-planned tail mid-dispatch, so it keeps the strictly-sequential loop.
    if concurrent_stages_enabled() && !reopt_active {
        run_stages_concurrent(
            &cluster,
            &stages,
            &stage_map,
            output.stage_id,
            scalar_stage.as_ref(),
            &mut scalar_literal,
            &planned_workers,
            query_id,
            cancel,
            &lineage,
            &store,
            &operation_id,
            &mut coalesced,
            &mut stage_rows,
        )
        .await?;
    } else {
        let mut order: Vec<u32> = stages
            .iter()
            .filter(|s| s.stage_id != output.stage_id)
            .map(|s| s.stage_id)
            .collect();
        if reopt_active {
            order.sort_by_key(|id| {
                !stage_map
                    .get(id)
                    .is_some_and(|s| s.upstream_stage_ids.is_empty())
            });
        }
        let mut dispatched_ids: HashSet<u32> = HashSet::new();
        let mut reopt_attempted = false;
        let mut cursor = 0usize;
        while cursor < order.len() {
            let current_id = order[cursor];
            let current = stage_map
                .get(&current_id)
                .cloned()
                .ok_or_else(|| Error::Plan(format!("stage {current_id} not in the stage list")))?;
            // KAN-27: inline any scalar-subquery tokens (the referenced scalar stages have already
            // run — the stage list is topologically ordered) before dispatching this stage.
            let stage = &substitute_scalar_tokens(
                &cluster,
                &current,
                &stages,
                scalar_stage.as_ref(),
                &mut scalar_literal,
            )
            .await?;
            ensure_stable_membership(&cluster, &planned_workers)?;
            check_query_cancelled(query_id, cancel)?;
            let np = cluster.num_partitions;
            // KAN-32: an intermediate stage (consumes upstream buckets and produces new ones)
            // must run once per *shuffle partition*, not once per worker — per-worker dispatch
            // only ever consumes buckets 0..workers-1, silently dropping the rest of the
            // upstream when OXIDANT_SHUFFLE_PARTITIONS exceeds the worker count (SF10 ran 16
            // partitions on 2 workers, and Q18's semi-shuffle plan lost 7/8 of its join rows).
            // Leaf producers still run once per worker: each scans its local shard of the data.
            // (Under an AQE coalesce decision the fan-in drops to `read_mod` reader partitions,
            // each consuming a modulus class of upstream buckets — still every bucket exactly
            // once; see `consumer_read_modulus`.)
            if stage.exchange == ExchangeMode::Hash && !stage.upstream_stage_ids.is_empty() {
                let read_mod = consumer_read_modulus(stage, &coalesced).unwrap_or(np);
                let futs = intermediate_task_futures(
                    &cluster,
                    stage,
                    np,
                    read_mod,
                    &lineage,
                    &stage_map,
                    &store,
                    &operation_id,
                    &stage_rows,
                )?;
                join_stage_tasks(futs).await?;
                finish_stage_barrier(
                    &cluster,
                    stage,
                    np,
                    &store,
                    &operation_id,
                    &mut coalesced,
                    &mut stage_rows,
                )
                .await;
                dispatched_ids.insert(current_id);
                cursor += 1;
                continue;
            }
            let futs = producer_task_futures(
                &cluster,
                stage,
                np,
                &lineage,
                &stage_map,
                &store,
                &operation_id,
                &stage_rows,
            )?;
            join_stage_tasks(futs).await?;
            finish_stage_barrier(
                &cluster,
                stage,
                np,
                &store,
                &operation_id,
                &mut coalesced,
                &mut stage_rows,
            )
            .await;
            dispatched_ids.insert(current_id);
            // OXIDANT_REOPT_JOIN_ORDER trigger: the barrier of the LAST leaf producer (the next
            // stage to dispatch is the first join stage) is the one moment every chain leaf
            // has a measured row count while a permutable tail is still pending. Attempt the
            // re-optimization once per query; any bail keeps the original stages.
            if reopt_active && !reopt_attempted && stage.upstream_stage_ids.is_empty() {
                let last_leaf_barrier = order.get(cursor + 1).is_some_and(|next_id| {
                    stage_map
                        .get(next_id)
                        .is_some_and(|n| !n.upstream_stage_ids.is_empty())
                });
                if last_leaf_barrier {
                    reopt_attempted = true;
                    if let Some(ctx) = &reopt {
                        if let Some((spliced, remaining, new_output)) = attempt_reopt_tail(
                            ctx,
                            &stages,
                            &stage_rows,
                            &dispatched_ids,
                            &store,
                            &operation_id,
                        ) {
                            stage_map = spliced.iter().map(|s| (s.stage_id, s.clone())).collect();
                            stages = spliced;
                            order = remaining;
                            output = new_output;
                            cursor = 0;
                            continue;
                        }
                    }
                }
            }
            cursor += 1;
        }
    }

    // Output stage:
    // - Forward: run once on a single worker (full-SQL / Sail shared-storage coverage path)
    // - scatter (no upstreams, no hash keys): every worker runs local SQL (global partial agg)
    // - else: per-partition rendezvous shuffle read
    //
    // KAN-27: the output stage may carry scalar-subquery tokens (e.g. a HAVING threshold);
    // substitute them with the computed literals before dispatch.
    let output = &substitute_scalar_tokens(
        &cluster,
        &output,
        &stages,
        scalar_stage.as_ref(),
        &mut scalar_literal,
    )
    .await?;
    let scatter_output = output.upstream_stage_ids.is_empty() && output.hash_key_cols.is_empty();
    let mut out = Vec::new();
    ensure_stable_membership(&cluster, &planned_workers)?;
    check_query_cancelled(query_id, cancel)?;
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
        let ticket = stage_ticket(output, 0, 1, 0, &cluster, false, &stage_map, &stage_rows);
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
            let ticket = stage_ticket(
                output,
                i as u32,
                w,
                0,
                &cluster,
                false,
                &stage_map,
                &stage_rows,
            );
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
        // One task per output partition, all dispatched concurrently (F2): the worker
        // scopes its shuffle-input registrations per task (`localize_shuffle_input_sql`),
        // so same-worker tasks no longer race — the per-endpoint serialization that used
        // to protect the shared `shuffle_input` table names is gone. Tasks beyond a
        // worker's slot count queue server-side (`acquire_task_slot`). When AQE coalesced
        // an upstream, only `read_mod` reader partitions are dispatched, each pulling its
        // whole modulus class of producer buckets.
        let read_mod = consumer_read_modulus(output, &coalesced).unwrap_or(w);
        let mut futs = Vec::new();
        for p in 0..read_mod {
            let endpoint = cluster.owner_endpoint(p)?;
            let membership = cluster.membership.clone();
            let output = output.clone();
            let cluster = cluster.clone();
            let lineage = lineage.clone();
            let stage_map = stage_map.clone();
            let stage_rows = stage_rows.clone();
            let host = executor_id(&endpoint);
            let store_c = store.clone();
            let op_c = operation_id.clone();
            let stage_id = output.stage_id as i32;
            let task_id = alloc_task_id(&store, p as i64);
            emit_task_started(&store, &operation_id, stage_id, task_id, &host);
            futs.push(async move {
                let ticket = stage_ticket(
                    &output,
                    p,
                    w,
                    read_mod,
                    &cluster,
                    false,
                    &stage_map,
                    &stage_rows,
                );
                let start = std::time::Instant::now();
                let result = run_stage_with_retry(
                    &membership,
                    endpoint.clone(),
                    ticket,
                    &lineage,
                    &stage_map,
                )
                .await;
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
                            rows * 8,
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
                // Tag with the partition so the merged output keeps partition order
                // (completion order is arbitrary under concurrent dispatch).
                result.map(|batches| (p, batches))
            });
        }
        let mut first_err = None;
        let mut indexed: Vec<(u32, Vec<RecordBatch>)> = Vec::new();
        for r in futures::future::join_all(futs).await {
            match r {
                Ok(v) => indexed.push(v),
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }
        if let Some(e) = first_err {
            return Err(e);
        }
        indexed.sort_by_key(|(p, _)| *p);
        for (_, batches) in indexed {
            out.extend(batches);
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

/// Build one cold task future per producer slot of a leaf / broadcast / forward stage
/// (worker-indexed dispatch; a `Forward` producer runs exactly once on the first worker —
/// see the dispatch-loop comment above for why a single real producer is sufficient). Each
/// task's `TaskStarted` event is emitted synchronously, before the future exists: the
/// futures only run once the caller polls them (sequential path: [`join_stage_tasks`];
/// concurrent path: `FuturesUnordered`), so a wave of ready stages attributes all of its
/// `TaskStarted` events before any task of the wave can finish. Worker-indexed dispatch
/// keeps the legacy one-bucket read: the AQE modulus mapping only applies to the
/// per-partition rendezvous dispatch path ([`intermediate_task_futures`]).
#[allow(clippy::too_many_arguments)]
fn producer_task_futures(
    cluster: &Cluster,
    stage: &StageDef,
    num_partitions: u32,
    lineage: &Arc<StageLineage>,
    stage_map: &HashMap<u32, StageDef>,
    store: &Option<SharedStore>,
    operation_id: &Option<String>,
    stage_rows: &HashMap<u32, Vec<u64>>,
) -> Result<Vec<BoxFuture<'static, Result<()>>>> {
    let producers: Vec<(usize, String)> = if stage.exchange == ExchangeMode::Forward {
        let first = cluster.workers.first().cloned().ok_or_else(|| {
            Error::Execution("forward producer stage requires at least one worker".into())
        })?;
        vec![(0, first)]
    } else {
        cluster.workers.iter().cloned().enumerate().collect()
    };
    let mut futs = Vec::new();
    for (i, endpoint) in producers {
        let ticket = stage_ticket(
            stage,
            i as u32,
            num_partitions,
            0,
            cluster,
            true,
            stage_map,
            stage_rows,
        );
        let membership = cluster.membership.clone();
        let ep = endpoint;
        let host = executor_id(&ep);
        let lineage = lineage.clone();
        let stage_map = stage_map.clone();
        let store_c = store.clone();
        let op_c = operation_id.clone();
        let stage_id = stage.stage_id as i32;
        let task_id = store
            .as_ref()
            .map(|s| s.alloc_task_id())
            .unwrap_or(i as i64);
        emit_task_started(store, operation_id, stage_id, task_id, &host);
        futs.push(
            async move {
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
                            rows * 8,
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
                result.map(|_| ())
            }
            .boxed(),
        );
    }
    Ok(futs)
}

/// Build one cold task future per shuffle partition of an intermediate (consume + produce)
/// stage — or, when AQE coalesced an upstream, one task per coalesced reader partition
/// (`read_modulus < num_partitions`), each pulling its whole modulus class of every
/// upstream. The worker scopes its shuffle-input registrations per task
/// (`localize_shuffle_input_sql`), so same-worker tasks no longer race on a shared
/// `shuffle_input` table — the per-endpoint serialization (KAN-32) is gone; tasks beyond a
/// worker's slot count queue server-side (`acquire_task_slot`). Every task still
/// hash-partitions its output into the full `num_partitions` buckets, so downstream reads
/// are unaffected by the coalesced fan-in. `TaskStarted` events are emitted synchronously
/// (see [`producer_task_futures`]).
#[allow(clippy::too_many_arguments)]
fn intermediate_task_futures(
    cluster: &Cluster,
    stage: &StageDef,
    num_partitions: u32,
    read_modulus: u32,
    lineage: &Arc<StageLineage>,
    stage_map: &HashMap<u32, StageDef>,
    store: &Option<SharedStore>,
    operation_id: &Option<String>,
    stage_rows: &HashMap<u32, Vec<u64>>,
) -> Result<Vec<BoxFuture<'static, Result<()>>>> {
    let mut futs = Vec::new();
    let num_tasks = read_modulus.clamp(1, num_partitions);
    for p in 0..num_tasks {
        let endpoint = cluster.owner_endpoint(p)?;
        let ticket = stage_ticket(
            stage,
            p,
            num_partitions,
            num_tasks,
            cluster,
            true,
            stage_map,
            stage_rows,
        );
        let membership = cluster.membership.clone();
        let lineage = lineage.clone();
        let stage_map = stage_map.clone();
        let host = executor_id(&endpoint);
        let store_c = store.clone();
        let op_c = operation_id.clone();
        let stage_id = stage.stage_id as i32;
        let task_id = alloc_task_id(store, p as i64);
        emit_task_started(store, operation_id, stage_id, task_id, &host);
        futs.push(
            async move {
                let start = std::time::Instant::now();
                let result = run_stage_with_retry(
                    &membership,
                    endpoint.clone(),
                    ticket,
                    &lineage,
                    &stage_map,
                )
                .await;
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
                            rows * 8,
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
                result.map(|_| ())
            }
            .boxed(),
        );
    }
    Ok(futs)
}

/// Await one stage's task futures to completion. Every task runs to completion even when a
/// sibling fails (join_all drains the set, so a failed task's siblings still free their
/// worker slots); the first task error — in task order, matching the pre-concurrency
/// behavior — is surfaced.
async fn join_stage_tasks(futs: Vec<BoxFuture<'static, Result<()>>>) -> Result<()> {
    let mut first_err = None;
    for r in futures::future::join_all(futs).await {
        if let Err(e) = r {
            if first_err.is_none() {
                first_err = Some(e);
            }
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Dependency-aware concurrent dispatch of every non-output stage (`OXIDANT_CONCURRENT_STAGES`,
/// default on): a stage is dispatched as soon as ALL of its upstream stages have completed
/// ([`StageDag`] computes the dependency sets, including the implicit KAN-27 scalar-token
/// edge), so independent branch arms overlap instead of serializing behind the previous
/// stage's barrier. A consumer still waits for every upstream, and each completed stage's
/// barrier ([`finish_stage_barrier`]) runs inline in this loop before its dependents are
/// released, so a consumer's ticket construction observes the complete AQE / stage-rows
/// state of all of its upstreams — the same snapshots the sequential loop produced.
///
/// Failure semantics: the first stage error skips every transitive dependent (they can
/// never become ready) and is returned immediately. Dropping the still in-flight sibling
/// stages cancels their Flight streams client-side; the caller's exit path then runs the
/// same best-effort cancel + eviction sweep on the workers the sequential path runs. No
/// new concurrency bound is introduced: in-flight tasks stay bounded by the per-stage task
/// counts and the workers' server-side task slots, exactly as within one stage today.
#[allow(clippy::too_many_arguments)]
async fn run_stages_concurrent(
    cluster: &Cluster,
    stages: &[StageDef],
    stage_map: &HashMap<u32, StageDef>,
    output_stage_id: u32,
    scalar_stage: Option<&StageDef>,
    scalar_literal: &mut Option<String>,
    planned_workers: &[String],
    query_id: &str,
    cancel: &AtomicBool,
    lineage: &Arc<StageLineage>,
    store: &Option<SharedStore>,
    operation_id: &Option<String>,
    coalesced: &mut HashMap<u32, u32>,
    stage_rows: &mut HashMap<u32, Vec<u64>>,
) -> Result<()> {
    let mut dag = StageDag::new(stages, output_stage_id, scalar_stage.map(|s| s.stage_id));
    let np = cluster.num_partitions;
    let mut in_flight: FuturesUnordered<BoxFuture<'static, (u32, Result<()>)>> =
        FuturesUnordered::new();
    // Stage ids currently running (pushed to `in_flight`, not yet completed). Used so a
    // failure can cancel siblings on workers before dropping their Flight futures — otherwise
    // slots stay held and the next query fails with `worker has no free task slots`.
    let mut in_flight_ids: HashSet<u32> = HashSet::new();
    loop {
        // Dispatch every stage whose upstreams all completed. The task futures are cold,
        // so this sweep emits every ready stage's TaskStarted events before any task of
        // the wave is polled (deterministic per-stage event attribution).
        while let Some(id) = dag.take_ready() {
            let current = stage_map
                .get(&id)
                .cloned()
                .ok_or_else(|| Error::Plan(format!("stage {id} not in the stage list")))?;
            // KAN-27: the scalar-token edge in the dependency layer guarantees the scalar
            // stage completed before this stage dispatches.
            let stage =
                substitute_scalar_tokens(cluster, &current, stages, scalar_stage, scalar_literal)
                    .await?;
            ensure_stable_membership(cluster, planned_workers)?;
            check_query_cancelled(query_id, cancel)?;
            let task_futs =
                if stage.exchange == ExchangeMode::Hash && !stage.upstream_stage_ids.is_empty() {
                    let read_mod = consumer_read_modulus(&stage, coalesced).unwrap_or(np);
                    intermediate_task_futures(
                        cluster,
                        &stage,
                        np,
                        read_mod,
                        lineage,
                        stage_map,
                        store,
                        operation_id,
                        stage_rows,
                    )?
                } else {
                    producer_task_futures(
                        cluster,
                        &stage,
                        np,
                        lineage,
                        stage_map,
                        store,
                        operation_id,
                        stage_rows,
                    )?
                };
            in_flight_ids.insert(id);
            in_flight.push(
                async move {
                    let result = join_stage_tasks(task_futs).await;
                    (id, result)
                }
                .boxed(),
            );
        }
        let Some((id, result)) = in_flight.next().await else {
            break;
        };
        in_flight_ids.remove(&id);
        match result {
            Ok(()) => {
                let stage = stage_map
                    .get(&id)
                    .expect("dispatched stage is in the stage map");
                finish_stage_barrier(
                    cluster,
                    stage,
                    np,
                    store,
                    operation_id,
                    coalesced,
                    stage_rows,
                )
                .await;
                dag.complete(id);
            }
            Err(e) => {
                // Skip dependents, cancel sibling stages on workers, then briefly drain
                // in-flight Flight futures so task slots free before the next query.
                // Preserve `e` as the returned root cause (do not replace with cancel noise).
                let skipped = dag.fail(id);
                let sibling_ids: Vec<u32> = in_flight_ids.iter().copied().collect();
                tracing::warn!(
                    target: "oxidant.driver",
                    stage_id = id,
                    ?skipped,
                    ?sibling_ids,
                    "stage failed; cancelling in-flight siblings and surfacing the root error"
                );
                if !sibling_ids.is_empty() {
                    cancel_stages_on_workers(&cluster.workers, sibling_ids.into_iter()).await;
                }
                // Bounded drain: drop remaining futures after a short wait so a wedged sibling
                // cannot pin this query until the full stage timeout. QueryAbortGuard /
                // OXIDANT_STAGE_TIMEOUT_MS remain the hard backstops.
                let drain = std::time::Duration::from_secs(5);
                let _ = tokio::time::timeout(drain, async {
                    while in_flight.next().await.is_some() {}
                })
                .await;
                in_flight_ids.clear();
                return Err(e);
            }
        }
    }
    if dag.unfinished() != 0 {
        return Err(Error::Plan(format!(
            "stage DAG has a dependency cycle: {} stage(s) never became dispatchable",
            dag.unfinished()
        )));
    }
    Ok(())
}

/// Emit the StageFinished event for a producer stage and (when AQE is enabled) sample its
/// per-partition bucket row counts, recording a coalesced read modulus in `coalesced` when
/// every bucket is small. The planned partition count is never shrunk mid-query — producers
/// already wrote `num_partitions` buckets — so downstream consumers of THIS query read the
/// coalesced stage through the modulus mapping (`aqe::coalesced_read_buckets`) instead of a
/// plain `0..new_p` range (which would orphan buckets `new_p..num_partitions-1`).
///
/// The same sample — one `bucket_row_counts` action round trip per worker, a local
/// in-memory row count (KAN-32) shared by both consumers — also feeds the stage-input
/// statistics path (`OXIDANT_STAGE_INPUT_STATS`, default on): the exact per-bucket totals
/// (summed across every producing worker) are recorded in `stage_rows` and ride
/// downstream tickets as [`StageTicket::upstream_bucket_rows`], so workers attach measured
/// row counts to their `shuffle_input*` scans and the plan-time join-strategy guard sizes
/// hash builds from data instead of unknown statistics (Spark AQE's runtime
/// SMJ→hash/broadcast conversion).
async fn finish_stage_barrier(
    cluster: &Cluster,
    stage: &StageDef,
    num_partitions: u32,
    store: &Option<SharedStore>,
    operation_id: &Option<String>,
    coalesced: &mut HashMap<u32, u32>,
    stage_rows: &mut HashMap<u32, Vec<u64>>,
) {
    if let (Some(ref s), Some(ref op)) = (store, operation_id) {
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
    if !aqe_enabled() && !stage_input_stats_enabled() {
        return;
    }
    let per_worker = sample_per_worker_bucket_counts(cluster, stage.stage_id).await;
    // AQE: on a coalesce decision, remember it for the rest of this query: downstream
    // consumers dispatch `new_p` reader partitions, partition `p` pulling producer buckets
    // `p, p+new_p, …` (every `b ≡ p mod new_p`), so each of the `num_partitions` written
    // buckets is read exactly once while the planned count stays frozen for the query
    // lifetime.
    if aqe_enabled() {
        let counts =
            owner_bucket_row_counts(cluster, stage.stage_id, num_partitions, &per_worker).await;
        if let Ok(new_p) = coalesced_partitions(cluster.worker_count(), num_partitions, &counts) {
            if new_p < num_partitions {
                coalesced.insert(stage.stage_id, new_p);
                if let (Some(ref s), Some(ref op)) = (store, operation_id) {
                    s.emit(ExecutionEvent::AqeCoalesced {
                        operation_id: op.clone(),
                        stage_id: stage.stage_id as i32,
                        old_partitions: num_partitions,
                        new_partitions: new_p,
                    });
                }
                tracing::info!(
                    target: "oxidant.aqe",
                    stage_id = stage.stage_id,
                    old_partitions = num_partitions,
                    coalesced_partitions = new_p,
                    "AQE coalesced shuffle read; consumers pull buckets p, p+m, … (planned count unchanged)"
                );
            }
        }
    }
    // Stage-input statistics: exact per-bucket totals = every worker's contribution summed
    // (a Forward producer ran on one worker; the others contribute nothing). Skip the stage
    // when any worker could not answer — an undercounted build-side estimate would steer
    // the join guard toward hash where the real build does not fit (the safe direction is
    // no stats, which falls back to the worker's own MemTable statistics).
    if stage_input_stats_enabled() && per_worker.values().all(|c| c.is_some()) {
        let mut totals = vec![0u64; num_partitions as usize];
        for counts in per_worker.values().flatten() {
            for (b, n) in counts.iter().enumerate() {
                if let Some(t) = totals.get_mut(b) {
                    *t += *n as u64;
                }
            }
        }
        stage_rows.insert(stage.stage_id, totals);
    }
}

/// OXIDANT_REOPT_JOIN_ORDER: at the last leaf stage's barrier, re-derive the shuffle-join
/// chain's tail from the barrier-measured leaf cardinalities and splice it onto the
/// dispatched prefix. Returns the swapped-in stage DAG (topologically ordered), the
/// remaining dispatch order, and the re-derived output stage; `None` keeps the original
/// stages (every bail is zero-cost — nothing has been mutated yet).
fn attempt_reopt_tail(
    ctx: &ReoptContext,
    stages: &[StageDef],
    stage_rows: &HashMap<u32, Vec<u64>>,
    dispatched_ids: &HashSet<u32>,
    store: &Option<SharedStore>,
    operation_id: &Option<String>,
) -> Option<(Vec<StageDef>, Vec<u32>, StageDef)> {
    let replanned =
        crate::plan::join_chain::replan_chain_tail(ctx.plan, ctx.replicated, stages, stage_rows)?;
    let spliced = splice_replanned_tail(stages, dispatched_ids, replanned.stages)?;
    // The spliced DAG must keep the single-output shape (the unconsumed stage, last in
    // topological order) the output dispatch below relies on.
    let consumed: HashSet<u32> = spliced
        .iter()
        .flat_map(|s| s.upstream_stage_ids.iter().copied())
        .collect();
    let mut outputs = spliced.iter().filter(|s| !consumed.contains(&s.stage_id));
    let (Some(new_output), None) = (outputs.next(), outputs.next()) else {
        return None;
    };
    if Some(new_output.stage_id) != spliced.last().map(|s| s.stage_id) {
        return None;
    }
    let new_output = new_output.clone();
    // Remaining dispatch order: the spliced DAG in topological order, minus the dispatched
    // prefix and the output stage. Every remaining stage has upstreams (the trigger fired
    // at the last leaf), so no further front-loading is needed.
    let remaining: Vec<u32> = spliced
        .iter()
        .map(|s| s.stage_id)
        .filter(|id| !dispatched_ids.contains(id) && *id != new_output.stage_id)
        .collect();
    if let (Some(s), Some(op)) = (store, operation_id) {
        s.emit(ExecutionEvent::ReoptimizedJoinOrder {
            operation_id: op.clone(),
            stage_ids: remaining.iter().map(|&id| id as i32).collect(),
            detail: replanned.detail.clone(),
        });
    }
    tracing::info!(
        target: "oxidant.reopt",
        tail_stage_ids = ?remaining,
        detail = %replanned.detail,
        "spliced re-optimized join tail onto the dispatched prefix"
    );
    Some((spliced, remaining, new_output))
}

/// Splice a re-planned stage DAG onto the already-dispatched prefix
/// (`OXIDANT_REOPT_JOIN_ORDER`), preserving every dispatched stage id.
///
/// Worker plan caches and the driver's lineage key on stage id + SQL, so a dispatched stage
/// id's SQL must never change: every dispatched stage requires an exact
/// (sql, hash_key_cols, exchange) match in the re-planned DAG and keeps its id; the
/// remaining re-planned stages (the new tail) take the un-dispatched tail's leftover ids in
/// increasing order, so the stage count and the full id set are preserved and the
/// cancel-watcher / abort-guard captured id lists stay valid. Upstream references are
/// rewritten through the id map. ANY mismatch returns `None` — the caller keeps the
/// original stages untouched.
fn splice_replanned_tail(
    stages: &[StageDef],
    dispatched_ids: &HashSet<u32>,
    replanned: Vec<StageDef>,
) -> Option<Vec<StageDef>> {
    if replanned.len() != stages.len() {
        return None;
    }
    // re-planned stage id -> preserved (final) stage id.
    let mut id_map: HashMap<u32, u32> = HashMap::new();
    let mut matched_new: HashSet<u32> = HashSet::new();
    for &d in dispatched_ids {
        let old = stages.iter().find(|s| s.stage_id == d)?;
        let new = replanned.iter().find(|n| {
            !matched_new.contains(&n.stage_id)
                && n.sql == old.sql
                && n.hash_key_cols == old.hash_key_cols
                && n.exchange == old.exchange
        })?;
        id_map.insert(new.stage_id, d);
        matched_new.insert(new.stage_id);
    }
    let mut leftover_ids: Vec<u32> = stages
        .iter()
        .map(|s| s.stage_id)
        .filter(|id| !dispatched_ids.contains(id))
        .collect();
    leftover_ids.sort_unstable();
    let mut leftover_new: Vec<u32> = replanned
        .iter()
        .map(|s| s.stage_id)
        .filter(|id| !matched_new.contains(id))
        .collect();
    leftover_new.sort_unstable();
    if leftover_ids.len() != leftover_new.len() {
        return None;
    }
    for (new_id, final_id) in leftover_new.into_iter().zip(leftover_ids) {
        id_map.insert(new_id, final_id);
    }
    let mut out = Vec::with_capacity(replanned.len());
    for n in &replanned {
        let stage_id = *id_map.get(&n.stage_id)?;
        let upstream_stage_ids = n
            .upstream_stage_ids
            .iter()
            .map(|u| id_map.get(u).copied())
            .collect::<Option<Vec<_>>>()?;
        out.push(StageDef {
            stage_id,
            upstream_stage_ids,
            ..n.clone()
        });
    }
    Some(out)
}

/// Ask every worker for its cached per-partition row counts of a producer stage (the cheap
/// KAN-32 `bucket_row_counts` action — a local in-memory count, no data movement). `None`
/// for a worker that errored or predates the action (mixed-version cluster). The action
/// exists because the pre-KAN-32 sampler pulled every partition's bucket over Flight just
/// to count rows, shipping the whole stage output through the driver after every producer
/// stage (at SF10, ~90M rows for Q18 alone).
async fn sample_per_worker_bucket_counts(
    cluster: &Cluster,
    stage_id: u32,
) -> HashMap<String, Option<Vec<usize>>> {
    let mut per_worker = HashMap::new();
    for ep in &cluster.workers {
        let c = bucket_row_counts(ep.clone(), stage_id).await.ok();
        per_worker.insert(ep.clone(), c);
    }
    per_worker
}

/// The AQE owner sample: per-partition row counts of a producer stage's cached output,
/// taking each bucket's count from its owner worker. Workers that do not know the action
/// (mixed-version cluster) fall back to pulling the owner's bucket over Flight (the
/// pre-KAN-32 behavior — expensive, but correct and non-destructive).
async fn owner_bucket_row_counts(
    cluster: &Cluster,
    stage_id: u32,
    num_partitions: u32,
    per_worker: &HashMap<String, Option<Vec<usize>>>,
) -> Vec<usize> {
    let mut counts = vec![0usize; num_partitions as usize];
    for p in 0..num_partitions {
        let Ok(ep) = cluster.owner_endpoint(p) else {
            continue;
        };
        match per_worker.get(&ep) {
            Some(Some(c)) => {
                if let Some(n) = c.get(p as usize) {
                    counts[p as usize] = *n;
                }
            }
            _ => {
                if let Ok(batches) = pull_bucket_with_retry(ep, stage_id, p).await {
                    counts[p as usize] = batches.iter().map(|b| b.num_rows()).sum();
                }
            }
        }
    }
    counts
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
    use oxidant_loom::arrow::datatypes::{Field, Schema};
    use std::sync::Arc;
    let Some(first) = batches.first() else {
        return batches;
    };
    // Zero-field placeholder batches (a worker's schema-less empty reply) cannot be rebuilt:
    // arrow's `RecordBatch::try_new` rejects zero-column batches without an explicit row
    // count, so the `filter_map` below would silently drop them — leaving an empty vec that
    // surfaces as "register `result`: no batches" (KAN-28). Return them untouched.
    if first.schema().fields().is_empty() {
        return batches;
    }
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

/// AQE read modulus for a consumer stage: the smallest coalesced partition count recorded at
/// any of its upstreams' stage barriers, or `None` when no upstream was coalesced (the no-AQE
/// path keeps the legacy one-bucket read). One modulus for the whole stage preserves
/// shuffle-join co-location: bucket `b` of every upstream is read by the same consumer
/// partition `b % m`. An upstream without a decision simply follows along — all of its
/// buckets are still read exactly once.
fn consumer_read_modulus(stage: &StageDef, coalesced: &HashMap<u32, u32>) -> Option<u32> {
    stage
        .upstream_stage_ids
        .iter()
        .filter_map(|s| coalesced.get(s).copied())
        .min()
}

/// The subset of `stage`'s upstreams produced in `ExchangeMode::Forward`: each ran exactly
/// once, on the first endpoint of the consumer's `upstream_endpoints` (see the producer
/// dispatch above), so consumers pull those upstreams from that endpoint only.
pub(crate) fn forward_upstreams(stage: &StageDef, stages: &HashMap<u32, StageDef>) -> Vec<u32> {
    stage
        .upstream_stage_ids
        .iter()
        .copied()
        .filter(|id| {
            stages
                .get(id)
                .is_some_and(|d| d.exchange == ExchangeMode::Forward)
        })
        .collect()
}

/// The flattened [`StageTicket::upstream_bucket_rows`] for a consumer stage: every
/// upstream's barrier-measured per-bucket row totals in `upstream_stage_ids` order, or
/// empty when any upstream has no measurement (`OXIDANT_STAGE_INPUT_STATS=0`, an incomplete
/// sample, or a producer this query did not measure) — the worker then registers its
/// shuffle inputs without measured statistics and falls back to the MemTable's own.
fn measured_upstream_bucket_rows(
    stage: &StageDef,
    num_partitions: u32,
    stage_rows: &HashMap<u32, Vec<u64>>,
) -> Vec<u64> {
    if stage.upstream_stage_ids.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(stage.upstream_stage_ids.len() * num_partitions as usize);
    for up in &stage.upstream_stage_ids {
        let Some(rows) = stage_rows.get(up) else {
            return Vec::new();
        };
        if rows.len() != num_partitions as usize {
            return Vec::new();
        }
        out.extend_from_slice(rows);
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn stage_ticket(
    stage: &StageDef,
    partition_id: u32,
    num_partitions: u32,
    coalesce_read_modulus: u32,
    cluster: &Cluster,
    produce: bool,
    stages: &HashMap<u32, StageDef>,
    stage_rows: &HashMap<u32, Vec<u64>>,
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
        replicated_tables: stage.replicated_tables.clone(),
        coalesce_read_modulus,
        forward_upstream_stage_ids: forward_upstreams(stage, stages),
        upstream_bucket_rows: measured_upstream_bucket_rows(stage, num_partitions, stage_rows),
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
        let stages = HashMap::from([(3, StageDef::new(3, "SELECT 1", vec![], vec![0]))]);
        let ticket = stage_ticket(&stage, 0, 1, 0, &cluster, true, &stages, &HashMap::new());
        assert_eq!(ticket.plan_fragment, vec![1, 2, 3]);
    }

    #[test]
    fn stage_ticket_marks_forward_upstreams() {
        // Upstream 1 is Forward (produced once, on the first endpoint); upstream 0 is an
        // ordinary hash shuffle. The consumer's ticket must mark only stage 1.
        let stages = HashMap::from([
            (0, StageDef::new(0, "SELECT 1", vec![], vec![0])),
            (1, {
                let mut s = StageDef::new(1, "SELECT 2", vec![], vec![0]);
                s.exchange = ExchangeMode::Forward;
                s
            }),
        ]);
        let consumer = StageDef::new(2, "SELECT * FROM shuffle_input_0", vec![0, 1], vec![]);
        let cluster = Cluster::new(vec!["a:1".into(), "b:1".into()]);
        let ticket = stage_ticket(
            &consumer,
            0,
            2,
            0,
            &cluster,
            false,
            &stages,
            &HashMap::new(),
        );
        assert_eq!(ticket.forward_upstream_stage_ids, vec![1]);

        // A consumer of only hash upstreams marks nothing (legacy pull-everywhere read).
        let hash_consumer = StageDef::new(3, "SELECT * FROM shuffle_input_0", vec![0], vec![]);
        let ticket = stage_ticket(
            &hash_consumer,
            0,
            2,
            0,
            &cluster,
            false,
            &stages,
            &HashMap::new(),
        );
        assert!(ticket.forward_upstream_stage_ids.is_empty());
    }

    #[test]
    fn stage_ticket_carries_measured_upstream_bucket_rows() {
        let stages = HashMap::from([
            (0, StageDef::new(0, "SELECT 1", vec![], vec![0])),
            (1, StageDef::new(1, "SELECT 2", vec![], vec![0])),
        ]);
        let consumer = StageDef::new(
            2,
            "SELECT * FROM shuffle_input_0 JOIN shuffle_input_1 USING (k)",
            vec![0, 1],
            vec![],
        );
        let cluster = Cluster::new(vec!["a:1".into(), "b:1".into()]);
        // Both upstreams measured at their barriers: the ticket flattens the per-bucket
        // totals in upstream order (`num_partitions` entries each).
        let stage_rows = HashMap::from([(0, vec![10u64, 20]), (1, vec![30u64, 40])]);
        let ticket = stage_ticket(&consumer, 0, 2, 0, &cluster, false, &stages, &stage_rows);
        assert_eq!(ticket.upstream_bucket_rows, vec![10, 20, 30, 40]);

        // Any unmeasured upstream (or a partition-count mismatch) drops the whole field —
        // the worker falls back to its own MemTable statistics rather than trusting a
        // partial measurement.
        let partial = HashMap::from([(0, vec![10u64, 20])]);
        let ticket = stage_ticket(&consumer, 0, 2, 0, &cluster, false, &stages, &partial);
        assert!(ticket.upstream_bucket_rows.is_empty());
        let wrong_len = HashMap::from([(0, vec![10u64]), (1, vec![30u64, 40])]);
        let ticket = stage_ticket(&consumer, 0, 2, 0, &cluster, false, &stages, &wrong_len);
        assert!(ticket.upstream_bucket_rows.is_empty());

        // A leaf stage never carries measurements.
        let leaf = StageDef::new(0, "SELECT 1", vec![], vec![0]);
        let ticket = stage_ticket(&leaf, 0, 2, 0, &cluster, true, &stages, &stage_rows);
        assert!(ticket.upstream_bucket_rows.is_empty());
    }

    /// A four-table chain DAG as build_chain emits it: leaves interleaved with joins,
    /// partial agg, final agg.
    fn chain4_stages() -> Vec<StageDef> {
        vec![
            StageDef::new(0, "SELECT k AS ta__k FROM ta", vec![], vec![0]),
            StageDef::new(1, "SELECT k AS tb__k FROM tb", vec![], vec![0]),
            StageDef::new(2, "SELECT … join ta tb", vec![0, 1], vec![1]),
            StageDef::new(3, "SELECT k AS tc__k FROM tc", vec![], vec![0]),
            StageDef::new(4, "SELECT … join _ tc", vec![2, 3], vec![1]),
            StageDef::new(5, "SELECT k AS td__k FROM td", vec![], vec![0]),
            StageDef::new(6, "SELECT … join _ td", vec![4, 5], vec![0]),
            StageDef::new(7, "SELECT … final agg", vec![6], vec![]),
        ]
    }

    #[test]
    fn splice_replanned_tail_maps_ids_and_upstreams() {
        let stages = chain4_stages();
        let dispatched: HashSet<u32> = [0, 1, 3, 5].into_iter().collect();
        // The re-planned DAG (fresh sequential ids) permuted the tail joins: the td leaf
        // sits at position 3, tc at 5; join/final SQL differs (unmatched).
        let replanned = vec![
            StageDef::new(0, "SELECT k AS ta__k FROM ta", vec![], vec![0]),
            StageDef::new(1, "SELECT k AS tb__k FROM tb", vec![], vec![0]),
            StageDef::new(2, "SELECT … join ta tb (rekeyed)", vec![0, 1], vec![1]),
            StageDef::new(3, "SELECT k AS td__k FROM td", vec![], vec![0]),
            StageDef::new(4, "SELECT … join _ td (new)", vec![2, 3], vec![1]),
            StageDef::new(5, "SELECT k AS tc__k FROM tc", vec![], vec![0]),
            StageDef::new(6, "SELECT … join _ tc (new)", vec![4, 5], vec![0]),
            StageDef::new(7, "SELECT … final agg (new)", vec![6], vec![]),
        ];
        let spliced = splice_replanned_tail(&stages, &dispatched, replanned).expect("must splice");

        // Equal count, identical id set — cancel-watcher / abort-guard id lists stay valid.
        assert_eq!(spliced.len(), stages.len());
        let mut ids: Vec<u32> = spliced.iter().map(|s| s.stage_id).collect();
        ids.sort_unstable();
        assert_eq!(ids, (0..8).collect::<Vec<_>>());

        // Every dispatched leaf kept its id AND its SQL (worker caches key on both).
        for d in [0u32, 1, 3, 5] {
            let old = stages.iter().find(|s| s.stage_id == d).unwrap();
            let new = spliced.iter().find(|s| s.stage_id == d).unwrap();
            assert_eq!(new.sql, old.sql, "dispatched stage {d} SQL must not change");
            assert_eq!(new.hash_key_cols, old.hash_key_cols);
        }
        // The td leaf moved to chain position 3 but kept stage id 5; the join consuming it
        // (chain position 4, leftover id 4) rewrote its upstreams through the map.
        assert_eq!(spliced[3].stage_id, 5);
        assert_eq!(spliced[3].sql, "SELECT k AS td__k FROM td");
        assert_eq!(spliced[4].stage_id, 4);
        assert_eq!(spliced[4].upstream_stage_ids, vec![2, 5]);
        assert_eq!(spliced[5].stage_id, 3);
        assert_eq!(spliced[6].upstream_stage_ids, vec![4, 3]);
        assert_eq!(spliced[7].stage_id, 7);
        assert_eq!(spliced[7].upstream_stage_ids, vec![6]);
    }

    #[test]
    fn splice_replanned_tail_bails_on_mismatch() {
        let stages = chain4_stages();
        let original_sql: Vec<String> = stages.iter().map(|s| s.sql.clone()).collect();
        let dispatched: HashSet<u32> = [0, 1, 3, 5].into_iter().collect();

        // A dispatched leaf's SQL changed in the re-plan: cannot preserve its id → None,
        // and the input stages stay untouched (zero-cost rollback).
        let mut bad = chain4_stages();
        bad[3].sql = "SELECT k AS tc__k FROM tc WHERE k > 0".into();
        assert!(splice_replanned_tail(&stages, &dispatched, bad).is_none());
        assert_eq!(
            stages.iter().map(|s| s.sql.clone()).collect::<Vec<_>>(),
            original_sql
        );

        // A hash-key change on a dispatched leaf is a mismatch too.
        let mut bad = chain4_stages();
        bad[1].hash_key_cols = vec![1];
        assert!(splice_replanned_tail(&stages, &dispatched, bad).is_none());

        // Stage-count mismatch → None.
        let short = chain4_stages()[..7].to_vec();
        assert!(splice_replanned_tail(&stages, &dispatched, short).is_none());
    }

    #[test]
    fn consumer_read_modulus_takes_min_of_coalesced_upstreams() {
        let stage = StageDef::new(
            2,
            "SELECT k FROM shuffle_input_0 JOIN shuffle_input_1 USING (k)",
            vec![0, 1],
            vec![0],
        );
        let mut coalesced = HashMap::new();
        // No decision recorded: the legacy one-bucket read (identity modulus).
        assert_eq!(consumer_read_modulus(&stage, &coalesced), None);

        // Two coalesced upstreams: one shared modulus keeps join co-location.
        coalesced.insert(0, 4);
        coalesced.insert(1, 2);
        assert_eq!(consumer_read_modulus(&stage, &coalesced), Some(2));

        // An upstream without a decision does not block coalescing the read.
        coalesced.remove(&1);
        assert_eq!(consumer_read_modulus(&stage, &coalesced), Some(4));

        // A leaf stage has no upstreams and never gets a modulus.
        let leaf = StageDef::new(0, "SELECT 1", vec![], vec![0]);
        assert_eq!(consumer_read_modulus(&leaf, &coalesced), None);
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
        assert!(err.contains("OXIDANT_WORKER_COUNT"));
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

    #[test]
    fn unify_schema_keeps_zero_field_placeholders() {
        // KAN-28: a schema-less placeholder batch (a worker's empty reply) cannot be rebuilt
        // by `RecordBatch::try_new` — it must pass through untouched instead of being
        // silently dropped (which surfaced as "register `result`: no batches").
        use oxidant_loom::arrow::array::{Int64Array, RecordBatch};
        use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};

        let placeholder = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let out = unify_schema(vec![placeholder]);
        assert_eq!(out.len(), 1, "placeholder must not be dropped");
        assert!(out[0].schema().fields().is_empty());

        // Typed batches still unify (nullability relaxed) as before.
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let typed = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))])
            .expect("typed batch");
        let out = unify_schema(vec![typed]);
        assert_eq!(out.len(), 1);
        assert!(out[0].schema().field(0).is_nullable());
        assert_eq!(out[0].num_rows(), 1);
    }
}
