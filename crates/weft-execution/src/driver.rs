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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use datafusion::scalar::ScalarValue;
use weft_common::{Error, Result};
use weft_loom::arrow::record_batch::RecordBatch;
use weft_observability::{now_ms, ExecutionEvent, SharedStore, StageStatus, TaskStatus};

use crate::aqe::{aqe_enabled, coalesced_partitions};
use crate::autoscale::{
    autoscale_enabled, parallelism_demand, recommend_worker_count, task_slots_per_worker,
};
use crate::flight::{
    bucket_row_counts, cancel_stage_on_worker, clear_worker_stages, pull_bucket_with_retry,
};
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
                for ep in &workers {
                    let _ = clear_worker_stages(ep.clone()).await;
                }
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
    /// Comma-separated tables the driver classified as fully replicated (auto-broadcast /
    /// `WEFT_REPLICATED_TABLES` override). Empty keeps older tickets wire-compatible.
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
/// callers that rebase stage ids before dispatch (weft-bench namespaces ids per query). The
/// driver runs the scalar stages first (the stage list is topologically ordered), pulls the
/// single resulting row, and replaces the token with the computed literal before dispatching the
/// dependent stage — Spark's subquery-execution + literal-injection pattern — so workers never
/// see the token.
pub(crate) const SCALAR_TOKEN: &str = "__WEFT_SCALAR_STAGE__";

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
    let result =
        run_stages_obs_inner(cluster, stages, store, operation_id, &query_id, &cancel).await;
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
    for ep in &cluster.workers {
        let _ = clear_worker_stages(ep.clone()).await;
    }
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
    // KAN-27: a scalar-token plan has one extra unconsumed stage — the scalar combine — which
    // the driver consumes directly (literal injection) instead of a downstream worker stage. It
    // is matched positionally (so callers may rebase stage ids); the output stage is the last
    // stage of the topologically-ordered list.
    let token_present = stages.iter().any(|s| s.sql.contains(SCALAR_TOKEN));
    let (output, scalar_stage) = match outputs.as_slice() {
        [o] if !token_present => (*o, None),
        [s, o] if token_present && Some(o.stage_id) == stages.last().map(|t| t.stage_id) => {
            (*o, Some(*s))
        }
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
    for stage in stages.iter().filter(|s| s.stage_id != output.stage_id) {
        // KAN-27: inline any scalar-subquery tokens (the referenced scalar stages have already
        // run — the stage list is topologically ordered) before dispatching this stage.
        let stage =
            &substitute_scalar_tokens(&cluster, stage, stages, scalar_stage, &mut scalar_literal)
                .await?;
        ensure_stable_membership(&cluster, &planned_workers)?;
        check_query_cancelled(query_id, cancel)?;
        let np = cluster.num_partitions;
        // KAN-32: an intermediate stage (consumes upstream buckets and produces new ones)
        // must run once per *shuffle partition*, not once per worker — per-worker dispatch
        // only ever consumes buckets 0..workers-1, silently dropping the rest of the
        // upstream when WEFT_SHUFFLE_PARTITIONS exceeds the worker count (SF10 ran 16
        // partitions on 2 workers, and Q18's semi-shuffle plan lost 7/8 of its join rows).
        // Leaf producers still run once per worker: each scans its local shard of the data.
        if stage.exchange == ExchangeMode::Hash && !stage.upstream_stage_ids.is_empty() {
            run_intermediate_stage(
                &cluster,
                stage,
                np,
                &lineage,
                &stage_map,
                &store,
                &operation_id,
            )
            .await?;
            finish_stage_barrier(&cluster, stage, np, &store, &operation_id).await;
            continue;
        }
        let mut futs = Vec::new();
        let producers: Vec<(usize, String)> = if stage.exchange == ExchangeMode::Forward {
            let first = cluster.workers.first().cloned().ok_or_else(|| {
                Error::Execution("forward producer stage requires at least one worker".into())
            })?;
            vec![(0, first)]
        } else {
            cluster.workers.iter().cloned().enumerate().collect()
        };
        for (i, endpoint) in producers {
            let ticket = stage_ticket(stage, i as u32, np, &cluster, true);
            let membership = cluster.membership.clone();
            let ep = endpoint;
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
        finish_stage_barrier(&cluster, stage, np, &store, &operation_id).await;
    }

    // Output stage:
    // - Forward: run once on a single worker (full-SQL / Sail shared-storage coverage path)
    // - scatter (no upstreams, no hash keys): every worker runs local SQL (global partial agg)
    // - else: per-partition rendezvous shuffle read
    //
    // KAN-27: the output stage may carry scalar-subquery tokens (e.g. a HAVING threshold);
    // substitute them with the computed literals before dispatch.
    let output =
        &substitute_scalar_tokens(&cluster, output, stages, scalar_stage, &mut scalar_literal)
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
        // One task per output partition, all dispatched concurrently (F2): the worker
        // scopes its shuffle-input registrations per task (`localize_shuffle_input_sql`),
        // so same-worker tasks no longer race — the per-endpoint serialization that used
        // to protect the shared `shuffle_input` table names is gone. Tasks beyond a
        // worker's slot count queue server-side (`acquire_task_slot`).
        let mut futs = Vec::new();
        for p in 0..w {
            let endpoint = cluster.owner_endpoint(p)?;
            let membership = cluster.membership.clone();
            let output = output.clone();
            let cluster = cluster.clone();
            let lineage = lineage.clone();
            let stage_map = stage_map.clone();
            let host = executor_id(&endpoint);
            let store_c = store.clone();
            let op_c = operation_id.clone();
            let stage_id = output.stage_id as i32;
            let task_id = alloc_task_id(&store, p as i64);
            emit_task_started(&store, &operation_id, stage_id, task_id, &host);
            futs.push(async move {
                let ticket = stage_ticket(&output, p, w, &cluster, false);
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

/// Run an intermediate (consume + produce) stage: one task per shuffle partition, all
/// dispatched concurrently (F2). The worker scopes its shuffle-input registrations per
/// task (`localize_shuffle_input_sql`), so same-worker tasks no longer race on a shared
/// `shuffle_input` table — the per-endpoint serialization (KAN-32) is gone; tasks beyond
/// a worker's slot count queue server-side (`acquire_task_slot`).
async fn run_intermediate_stage(
    cluster: &Cluster,
    stage: &StageDef,
    num_partitions: u32,
    lineage: &Arc<StageLineage>,
    stage_map: &HashMap<u32, StageDef>,
    store: &Option<SharedStore>,
    operation_id: &Option<String>,
) -> Result<()> {
    let mut futs = Vec::new();
    for p in 0..num_partitions {
        let endpoint = cluster.owner_endpoint(p)?;
        let membership = cluster.membership.clone();
        let stage = stage.clone();
        let cluster = cluster.clone();
        let lineage = lineage.clone();
        let stage_map = stage_map.clone();
        let host = executor_id(&endpoint);
        let store_c = store.clone();
        let op_c = operation_id.clone();
        let stage_id = stage.stage_id as i32;
        let task_id = alloc_task_id(store, p as i64);
        emit_task_started(store, operation_id, stage_id, task_id, &host);
        futs.push(async move {
            let ticket = stage_ticket(&stage, p, num_partitions, &cluster, true);
            let start = std::time::Instant::now();
            let result =
                run_stage_with_retry(&membership, endpoint.clone(), ticket, &lineage, &stage_map)
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
        });
    }
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

/// Emit the StageFinished event for a producer stage and (when AQE is enabled) sample its
/// per-partition bucket row counts. The sample is observability-only: the planned partition
/// count is never shrunk mid-query (see [`sample_bucket_row_counts`]).
async fn finish_stage_barrier(
    cluster: &Cluster,
    stage: &StageDef,
    num_partitions: u32,
    store: &Option<SharedStore>,
    operation_id: &Option<String>,
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
    // AQE: sample bucket row counts after producer stage when enabled.
    // Observability only — never shrink `num_partitions` here. Producers already wrote
    // `np` buckets; dropping the consumer range to `new_p < np` orphans buckets
    // `new_p..np-1` (silent row loss). Correct coalesced reads would need the consumer
    // to pull `p, p+new_p, …` up to the original modulus; until that exists, keep the
    // planned partition count frozen for the query lifetime.
    if aqe_enabled() {
        let counts = sample_bucket_row_counts(cluster, stage.stage_id, num_partitions).await;
        if let Ok(new_p) = coalesced_partitions(cluster.worker_count(), num_partitions, &counts) {
            if new_p < num_partitions {
                if let (Some(ref s), Some(ref op)) = (store, operation_id) {
                    s.emit(ExecutionEvent::AqeCoalesced {
                        operation_id: op.clone(),
                        stage_id: stage.stage_id as i32,
                        old_partitions: num_partitions,
                        new_partitions: new_p,
                    });
                }
                tracing::info!(
                    target: "weft.aqe",
                    stage_id = stage.stage_id,
                    old_partitions = num_partitions,
                    suggested_partitions = new_p,
                    "AQE would coalesce shuffle partitions; keeping planned count to avoid orphaning buckets"
                );
            }
        }
    }
}

/// Sample per-partition row counts of a producer stage's cached output for AQE. Workers
/// answer a cheap row-count action (KAN-32) — the previous implementation pulled every
/// partition's bucket over Flight just to count rows, shipping the whole stage output
/// through the driver after every producer stage (at SF10, ~90M rows for Q18 alone).
/// Workers that do not know the action (mixed-version cluster) fall back to that pull.
async fn sample_bucket_row_counts(
    cluster: &Cluster,
    stage_id: u32,
    num_partitions: u32,
) -> Vec<usize> {
    let mut counts = vec![0usize; num_partitions as usize];
    let mut per_worker: HashMap<String, Option<Vec<usize>>> = HashMap::new();
    for ep in &cluster.workers {
        let c = bucket_row_counts(ep.clone(), stage_id).await.ok();
        per_worker.insert(ep.clone(), c);
    }
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
            // Older worker without the row-count action: pull the owner's bucket (the
            // pre-KAN-32 behavior — expensive, but correct and non-destructive).
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
    use std::sync::Arc;
    use weft_loom::arrow::datatypes::{Field, Schema};
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
        replicated_tables: stage.replicated_tables.clone(),
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

    #[test]
    fn unify_schema_keeps_zero_field_placeholders() {
        // KAN-28: a schema-less placeholder batch (a worker's empty reply) cannot be rebuilt
        // by `RecordBatch::try_new` — it must pass through untouched instead of being
        // silently dropped (which surfaced as "register `result`: no batches").
        use weft_loom::arrow::array::{Int64Array, RecordBatch};
        use weft_loom::arrow::datatypes::{DataType, Field, Schema};

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
