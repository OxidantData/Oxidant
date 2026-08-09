//! Distributed execution over Arrow Flight.
//!
//! A [`Worker`] is an Arrow Flight server. Its `do_get` ticket is one of three things
//! (see [`crate::shuffle::protocol`]):
//!
//! - a legacy raw-SQL string — run it and stream the result (the single-stage MVP);
//! - a [`StageTicket`] — run a stage. A *leaf* stage (no upstreams) runs its SQL on local
//!   data, hash-partitions the output into per-downstream buckets, caches them, and returns an
//!   empty stream; a *consumer* stage (with upstreams) pulls its bucket from every upstream,
//!   registers it as `shuffle_input`, runs its SQL, and streams the result back;
//! - a [`ShuffleReadTicket`] — stream one cached bucket of a prior stage's output.
//!
//! This is the two-stage `partial-agg → hash shuffle → final-agg` shape; the driver in
//! [`crate::driver`] orchestrates it.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::{StreamExt, TryStreamExt};
use oxidant_common::{Error, Result};
use oxidant_loom::arrow::datatypes::{Schema, SchemaRef};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;
use tonic::{Request, Response, Status, Streaming};

use crate::shuffle::protocol::{self, ShuffleExchangeHeader, ShuffleReadTicket, StageTicket};
use crate::shuffle::spill::{enforce_total_budget, BucketCache, SpillStore};
use crate::shuffle::{hash_partition, PUSH_SRC};

/// Flight `do_action` type: evict all cached stage outputs on this worker.
pub const ACTION_CLEAR_STAGES: &str = "clear_stages";
/// Flight `do_action` type: register session UDF definitions (JSON payload).
pub const ACTION_REGISTER_UDFS: &str = "register_udfs";
/// Flight `do_action` type: liveness probe (driver heartbeats).
pub const ACTION_HEALTH: &str = "health";
/// Flight `do_action` type: liveness + slot probe.
pub const ACTION_HEARTBEAT: &str = "heartbeat";
/// Flight `do_action` type: accept/report simple task status payloads.
pub const ACTION_TASK_STATUS: &str = "task_status";
/// Flight `do_action` type: report per-partition row counts of a cached stage (body: decimal
/// stage id). Lets the driver sample shuffle sizes for AQE without pulling the buckets
/// themselves (KAN-32).
pub const ACTION_BUCKET_ROW_COUNTS: &str = "bucket_row_counts";

/// Max gRPC message size for Arrow Flight (KAN-6).
///
/// Mirrors `oxidant-connect`'s Spark Connect limit. tonic defaults to 4 MiB decode, which SF100
/// shuffle/DoGet frames exceed (TPC-DS Q65 ~58 MiB). Encode is unlimited by default, but we
/// raise both sides so DoExchange inbound frames are accepted too.
const MAX_MSG: usize = 256 * 1024 * 1024;
/// Cap rows per Flight encode input so a single `FlightData` frame stays well under `MAX_MSG`
/// even when size estimation under-counts wide string/binary batches.
const FLIGHT_CHUNK_ROWS: usize = 8192;

/// Estimated bytes of staged producer output that trigger a flush into the (spill-aware)
/// bucket cache (KAN-32). Bounds streaming-producer memory between flushes while keeping
/// spill segment files large enough that per-bucket segment counts stay small.
const PRODUCER_FLUSH_BYTES: usize = 128 * 1024 * 1024;

/// One producing task's cached output: schema + partitioned buckets (memory or spilled).
type CachedStage = (SchemaRef, BucketCache);

/// Per-task cached stage output, keyed by `(stage_id, src)` where `src` is the producing
/// task's partition id ([`crate::shuffle::PUSH_SRC`] for `do_exchange` pushes). One stage can
/// be produced by several tasks on the same worker (KAN-32: intermediate stages dispatch one
/// task per shuffle partition), so keying by stage id alone would let a later task overwrite
/// an earlier task's buckets; shuffle reads union all entries for the stage.
type StageCache = Arc<Mutex<HashMap<(u32, u32), CachedStage>>>;

/// A Flight worker that runs stages on its local engine and serves shuffle buckets.
pub struct Worker {
    engine: Arc<Engine>,
    stage_outputs: StageCache,
    spill: Option<SpillStore>,
    /// When true, `clear_stages` leaves on-disk spill files (tests asserting spill happened).
    keep_spill: bool,
    task_slots: usize,
    active_tasks: Arc<Mutex<usize>>,
    /// Explicit shard assignment for file-list sharding (in-process workers / tests sharing
    /// one process env). Installed as a task-local around every stage execution; `None`
    /// leaves `ShardAssignment::from_env` authoritative (live workers).
    shard_assignment: Option<oxidant_loom::shard::ShardAssignment>,
    /// Bounded-wait admission for stage tasks (F2): the driver dispatches all of a stage's
    /// partition tasks concurrently, so tasks beyond `task_slots` must queue here until a
    /// slot frees instead of bouncing back to the driver as `resource_exhausted`.
    task_slots_sem: Arc<tokio::sync::Semaphore>,
    last_task_status: Arc<Mutex<Option<Vec<u8>>>>,
    /// Per-stage cancel flags, tripped by the `cancel_stage` action (KAN-17).
    stage_cancels: Arc<Mutex<HashMap<u32, Arc<AtomicBool>>>>,
    /// Insert timestamps for `stage_outputs` entries; backs the lazy TTL sweep (KAN-18).
    stage_inserted: Arc<Mutex<HashMap<(u32, u32), std::time::Instant>>>,
    /// Retention for cached stage outputs; zero disables time-based expiry.
    stage_output_ttl: std::time::Duration,
}

impl Worker {
    /// Wrap an engine as a worker.
    pub fn new(engine: Arc<Engine>) -> Self {
        engine.require_lakehouse_snapshot_pins();
        let spill = SpillStore::from_env();
        match std::env::var("OXIDANT_MEMORY_LIMIT_BYTES") {
            Ok(bytes) if !bytes.trim().is_empty() => {
                eprintln!(
                    "Oxidant worker memory budget: OXIDANT_MEMORY_LIMIT_BYTES={bytes} (DataFusion spill pool + shuffle threshold)"
                );
            }
            _ => {
                if let Some(bytes) = oxidant_loom::resolve_memory_pool_bytes() {
                    eprintln!(
                        "Oxidant worker memory budget: auto-sized {bytes} bytes \
                         (cgroup/host × OXIDANT_MEMORY_POOL_FRACTION; set \
                         OXIDANT_MEMORY_LIMIT_BYTES to override, or =0 for unbounded)"
                    );
                }
            }
        }
        let slots = worker_task_slots();
        Self {
            engine,
            stage_outputs: Arc::new(Mutex::new(HashMap::new())),
            spill,
            keep_spill: false,
            task_slots: slots,
            active_tasks: Arc::new(Mutex::new(0)),
            shard_assignment: None,
            task_slots_sem: Arc::new(tokio::sync::Semaphore::new(slots)),
            last_task_status: Arc::new(Mutex::new(None)),
            stage_cancels: Arc::new(Mutex::new(HashMap::new())),
            stage_inserted: Arc::new(Mutex::new(HashMap::new())),
            stage_output_ttl: stage_output_ttl(),
        }
    }

    /// Wrap an engine as a worker whose stage executions shard file listings by an explicit
    /// assignment instead of the process env — the in-process multi-worker harness form,
    /// where `OXIDANT_SHARD_INDEX` cannot differ per worker.
    pub fn with_shard_assignment(
        engine: Arc<Engine>,
        assignment: oxidant_loom::shard::ShardAssignment,
    ) -> Self {
        let mut worker = Self::new(engine);
        worker.shard_assignment = Some(assignment);
        worker
    }

    /// Wrap an engine with an explicit spill store (tests / custom budgets).
    pub fn with_spill(engine: Arc<Engine>, spill: SpillStore) -> Self {
        let slots = worker_task_slots();
        Self {
            engine,
            stage_outputs: Arc::new(Mutex::new(HashMap::new())),
            spill: Some(spill),
            keep_spill: false,
            task_slots: slots,
            active_tasks: Arc::new(Mutex::new(0)),
            shard_assignment: None,
            task_slots_sem: Arc::new(tokio::sync::Semaphore::new(slots)),
            last_task_status: Arc::new(Mutex::new(None)),
            stage_cancels: Arc::new(Mutex::new(HashMap::new())),
            stage_inserted: Arc::new(Mutex::new(HashMap::new())),
            stage_output_ttl: stage_output_ttl(),
        }
    }

    /// Like [`with_spill`], but leave spill files on disk after stage eviction (mixture assertions).
    pub fn with_spill_keep(engine: Arc<Engine>, spill: SpillStore) -> Self {
        let mut w = Self::with_spill(engine, spill);
        w.keep_spill = true;
        w
    }

    /// Whether shuffle spilling is active on this worker.
    pub fn spill_enabled(&self) -> bool {
        self.spill.is_some()
    }

    fn clear_stages(&self) {
        // Tests may set `OXIDANT_KEEP_SHUFFLE_SPILL=1` or construct via [`with_spill_keep`] to inspect
        // spill files after the query; production always clears on-disk buckets with the cache.
        let keep_spill = self.keep_spill
            || std::env::var("OXIDANT_KEEP_SHUFFLE_SPILL")
                .ok()
                .is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));
        if let Some(spill) = &self.spill {
            if !keep_spill {
                let guard = self.stage_outputs.lock().expect("stage cache poisoned");
                let stage_ids: std::collections::HashSet<u32> =
                    guard.keys().map(|(stage_id, _)| *stage_id).collect();
                for stage_id in stage_ids {
                    spill.clear_stage(stage_id);
                }
            }
        }
        self.stage_outputs
            .lock()
            .expect("stage cache poisoned")
            .clear();
        self.stage_inserted
            .lock()
            .expect("stage stamps poisoned")
            .clear();
    }

    fn active_task_count(&self) -> usize {
        *self.active_tasks.lock().expect("task counter poisoned")
    }

    fn heartbeat_payload(&self) -> String {
        serde_json::json!({
            "ok": true,
            "slots_total": self.task_slots,
            "slots_used": self.active_task_count(),
        })
        .to_string()
    }

    fn task_status_payload(&self) -> String {
        let last_task_status = self
            .last_task_status
            .lock()
            .expect("task status poisoned")
            .as_ref()
            .map(|body| String::from_utf8_lossy(body).into_owned());
        serde_json::json!({
            "ok": true,
            "slots_total": self.task_slots,
            "slots_used": self.active_task_count(),
            "last_task_status": last_task_status,
        })
        .to_string()
    }

    /// Acquire a task slot, waiting for one to free when the worker is momentarily full
    /// (F2): the driver dispatches all of a stage's partition tasks concurrently, so tasks
    /// beyond `task_slots` must queue here instead of bouncing back to the driver (whose
    /// 3-attempt × 100ms retry window would spuriously fail them behind a long wave).
    /// Bounded by `OXIDANT_TASK_SLOT_WAIT_MS` (default = the stage timeout): a worker still
    /// saturated after that is genuinely overloaded and rejects exactly as before.
    async fn acquire_task_slot(&self) -> std::result::Result<TaskSlotGuard, Status> {
        let wait = task_slot_wait();
        let permit =
            match tokio::time::timeout(wait, self.task_slots_sem.clone().acquire_owned()).await {
                Ok(Ok(permit)) => permit,
                Ok(Err(_closed)) => {
                    return Err(Status::resource_exhausted("task slot semaphore closed"));
                }
                Err(_) => {
                    return Err(Status::resource_exhausted(format!(
                        "no task slots available after {}s wait ({}/{})",
                        wait.as_secs(),
                        self.active_task_count(),
                        self.task_slots
                    )));
                }
            };
        *self.active_tasks.lock().expect("task counter poisoned") += 1;
        Ok(TaskSlotGuard {
            active_tasks: self.active_tasks.clone(),
            _permit: permit,
        })
    }

    /// Register (or join) the cancel flag for `stage_id` while a stage task runs.
    fn register_stage_cancel(&self, stage_id: u32) -> Arc<AtomicBool> {
        self.stage_cancels
            .lock()
            .expect("stage cancels poisoned")
            .entry(stage_id)
            .or_default()
            .clone()
    }

    /// Drop the registration when the stage task finishes, unless a newer task for the same
    /// stage id has already replaced it.
    fn unregister_stage_cancel(&self, stage_id: u32, flag: &Arc<AtomicBool>) {
        let mut map = self.stage_cancels.lock().expect("stage cancels poisoned");
        if map.get(&stage_id).is_some_and(|f| Arc::ptr_eq(f, flag)) {
            map.remove(&stage_id);
        }
    }

    /// Trip the cancel flag for a running stage (`cancel_stage` action). Returns false when no
    /// stage with this id is currently running.
    fn cancel_stage(&self, stage_id: u32) -> bool {
        let map = self.stage_cancels.lock().expect("stage cancels poisoned");
        match map.get(&stage_id) {
            Some(flag) => {
                flag.store(true, Ordering::Relaxed);
                true
            }
            None => false,
        }
    }

    /// Record a `stage_outputs` insert and lazily evict entries older than the TTL (KAN-18).
    /// Lock order: `stage_inserted` then `stage_outputs`; no caller holds the cache lock here.
    fn note_stage_output_insert(&self, key: (u32, u32)) {
        let now = std::time::Instant::now();
        let mut stamps = self.stage_inserted.lock().expect("stage stamps poisoned");
        stamps.insert(key, now);
        if self.stage_output_ttl.is_zero() {
            return;
        }
        let expired: Vec<(u32, u32)> = stamps
            .iter()
            .filter(|(_, t)| now.duration_since(**t) >= self.stage_output_ttl)
            .map(|(&id, _)| id)
            .collect();
        if expired.is_empty() {
            return;
        }
        let mut guard = self.stage_outputs.lock().expect("stage cache poisoned");
        for id in expired {
            stamps.remove(&id);
            // Only the in-memory entry is dropped here. Its spill files (if any) are left in
            // place: `SpillStore::clear_stage` wipes *every* producer scope of the stage,
            // which would destroy a still-live sibling entry's spilled buckets (KAN-32). The
            // driver's end-of-query `clear_stages` reclaims the files.
            guard.remove(&id);
        }
    }
}

struct TaskSlotGuard {
    active_tasks: Arc<Mutex<usize>>,
    /// Held for the task's lifetime so the next queued task unblocks on drop.
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Drop for TaskSlotGuard {
    fn drop(&mut self) {
        let mut active = self.active_tasks.lock().expect("task counter poisoned");
        *active = active.saturating_sub(1);
    }
}

/// Deregister a stage's `shuffle_input*` MemTables when the stage task exits — success,
/// error, timeout, or cancel (the bounded future's locals drop with it). Without this, the
/// last task's pulled shuffle input stays resident in the shared worker session (KAN-19).
struct ShuffleInputGuard {
    engine: Arc<Engine>,
    names: Vec<String>,
}

impl Drop for ShuffleInputGuard {
    fn drop(&mut self) {
        for name in &self.names {
            self.engine.deregister_table(name);
        }
    }
}

/// The measured input rows of one upstream for this consumer task, from the ticket's
/// driver-measured per-bucket totals ([`StageTicket::upstream_bucket_rows`]): the sum of
/// that upstream's bucket totals over exactly the buckets this task pulls (`read_buckets`
/// — its own bucket, or its AQE modulus class). `None` when the ticket carries no complete
/// measurement (older driver, `OXIDANT_STAGE_INPUT_STATS=0`, or a partial sample) — the
/// caller then registers the plain MemTable and DataFusion's own statistics apply.
fn measured_upstream_rows(
    t: &StageTicket,
    read_buckets: &[u32],
    upstream_idx: usize,
) -> Option<u64> {
    let np = t.num_partitions as usize;
    if np == 0 || t.upstream_bucket_rows.len() != t.upstream_stage_ids.len() * np {
        return None;
    }
    let base = upstream_idx * np;
    Some(
        read_buckets
            .iter()
            .map(|&b| t.upstream_bucket_rows[base + b as usize])
            .sum(),
    )
}

/// KAN-31/KAN-46: reap a producer stage task's spill scope when the task exits without
/// committing its output into the stage cache — execution error, driver cancel, no-progress
/// watchdog abort, stage timeout, or the do_get future being dropped because the Flight
/// client (the driver) went away mid-stage. [`BucketCache`] has no `Drop` of its own, and an
/// uncommitted stage id never reaches the cache, so neither `clear_stages` (KAN-18) nor the
/// TTL sweep could ever find those files: failed/ENOSPC stages at SF10 left 25–38 GB of
/// orphaned `stage_*_src*_part_*.segN.arrow` segments per worker, and the following queries
/// then hit ENOSPC. Disarm only when the task's output is committed — the cache entry then
/// owns the files and `clear_stages` / the TTL sweep reaps them.
struct StageSpillReaper {
    spill: Option<SpillStore>,
    stage_outputs: StageCache,
    stage_inserted: Arc<Mutex<HashMap<(u32, u32), std::time::Instant>>>,
    stage_id: u32,
    src: u32,
    armed: bool,
}

impl StageSpillReaper {
    /// The task committed its output: the cache entry owns the spill files now.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StageSpillReaper {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // The `enforce_total_budget` failure path can leave a half-committed entry behind;
        // drop it with its files. Locks are taken and released one at a time (never nested)
        // to stay clear of the stamps→cache order used by `read_shuffle` /
        // `note_stage_output_insert`.
        self.stage_outputs
            .lock()
            .expect("stage cache poisoned")
            .remove(&(self.stage_id, self.src));
        self.stage_inserted
            .lock()
            .expect("stage stamps poisoned")
            .remove(&(self.stage_id, self.src));
        if let Some(spill) = &self.spill {
            spill.clear_scoped_stage(self.stage_id, self.src);
        }
    }
}

/// Number of concurrent stage tasks this worker should admit.
pub fn worker_task_slots() -> usize {
    std::env::var("OXIDANT_WORKER_TASK_SLOTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n: &usize| n > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        })
        .max(1)
}

/// How long a stage task may queue for a free worker task slot before being rejected
/// (env: `OXIDANT_TASK_SLOT_WAIT_MS`, default = the stage timeout, 10 minutes). See
/// [`Worker::acquire_task_slot`].
pub fn task_slot_wait() -> std::time::Duration {
    std::env::var("OXIDANT_TASK_SLOT_WAIT_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&ms: &u64| ms > 0)
        .map(std::time::Duration::from_millis)
        .unwrap_or_else(stage_timeout)
}

/// Server-side per-stage wall-clock limit (env: `OXIDANT_STAGE_TIMEOUT_MS`, default 10 minutes).
/// A stage that exceeds it errors out non-retryably so its task slot frees (KAN-17).
pub fn stage_timeout() -> std::time::Duration {
    let ms = std::env::var("OXIDANT_STAGE_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&ms: &u64| ms > 0)
        .unwrap_or(600_000);
    std::time::Duration::from_millis(ms)
}

/// Retention for cached stage outputs (env: `OXIDANT_STAGE_OUTPUT_TTL_SECS`, default 1 hour;
/// 0 disables time-based expiry). Defense in depth for KAN-18: the driver evicts stage caches
/// on every query exit, but a crashed or unreachable driver must not pin worker memory
/// forever. Entries are swept lazily on insert (same idiom as oxidant-connect's `CompletedOps`).
pub fn stage_output_ttl() -> std::time::Duration {
    std::env::var("OXIDANT_STAGE_OUTPUT_TTL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(std::time::Duration::from_secs)
        .unwrap_or_else(|| std::time::Duration::from_secs(3600))
}

/// Server-side per-stage no-progress budget (env: `OXIDANT_STAGE_NO_PROGRESS_SECS`, default 10
/// minutes). When a stage task's batch heartbeat, the worker's memory-pool activity, and
/// the spill dirs all stay frozen for this long, the stage is aborted with an actionable
/// error (KAN-47) so its task slot frees — instead of burning the full wall-clock timeout
/// on a parked query with zero diagnostics.
pub fn stage_no_progress_budget() -> std::time::Duration {
    let secs = std::env::var("OXIDANT_STAGE_NO_PROGRESS_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&s: &u64| s > 0)
        .unwrap_or(600);
    std::time::Duration::from_secs(secs)
}

/// Watchdog sampling cadence: a quarter of the budget, clamped to [100 ms, 30 s] so small
/// test budgets still sample promptly and large production budgets stay cheap.
fn stage_watchdog_interval(budget: std::time::Duration) -> std::time::Duration {
    (budget / 4).clamp(
        std::time::Duration::from_millis(100),
        std::time::Duration::from_secs(30),
    )
}

/// Per-stage-task progress signals for the no-progress watchdog (KAN-47). The batch
/// heartbeat is bumped by the stage's streaming execution paths; the abort age is stamped
/// by the watchdog when it fires, for the stage summary line.
#[derive(Debug, Default)]
pub struct StageProgress {
    batches: AtomicU64,
    no_progress_age_ms: AtomicU64,
}

impl StageProgress {
    /// Record one more batch produced/consumed by this stage task.
    pub fn note_batch(&self) {
        self.batches.fetch_add(1, Ordering::Relaxed);
    }

    /// Batches seen so far.
    pub fn batches(&self) -> u64 {
        self.batches.load(Ordering::Relaxed)
    }

    fn note_no_progress_abort(&self, age: std::time::Duration) {
        self.no_progress_age_ms
            .store(age.as_millis() as u64, Ordering::Relaxed);
    }

    /// Age of the last progress signal when the watchdog fired; `None` unless it did.
    pub fn no_progress_age(&self) -> Option<std::time::Duration> {
        let ms = self.no_progress_age_ms.load(Ordering::Relaxed);
        (ms > 0).then(|| std::time::Duration::from_millis(ms))
    }
}

/// Point-in-time sample of a stage task's progress signals (KAN-47).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct ProgressSample {
    /// Batches this stage task has produced/consumed so far (per-task signal).
    batches: u64,
    /// Last memory-pool activity, ms since engine start (worker-wide signal: any operator
    /// work grows/shrinks reservations, a parked query goes silent).
    pool_activity_ms: u64,
    /// DataFusion + oxidant shuffle spill bytes on disk (worker-wide signal, frozen under the
    /// spill-pool deadlock class).
    spill_bytes: u64,
}

/// The no-progress watchdog loop (KAN-47): every `interval`, compare the signals against
/// the last sample; when none have changed for `budget`, stamp the abort age on `progress`
/// and return an actionable message (deliberately non-retryable per
/// [`crate::scheduler::is_retryable`]). Pends forever while any signal advances, so it is
/// raced against the stage body in [`run_stage_bounded`] — blast radius is one stage task,
/// never the worker.
async fn watch_stage_progress(
    mut sample: impl FnMut() -> ProgressSample + Send,
    interval: std::time::Duration,
    budget: std::time::Duration,
    stage_id: u32,
    progress: Arc<StageProgress>,
) -> String {
    let mut last = sample();
    let mut last_change = std::time::Instant::now();
    loop {
        tokio::time::sleep(interval).await;
        let now = sample();
        if now != last {
            last = now;
            last_change = std::time::Instant::now();
            continue;
        }
        let age = last_change.elapsed();
        if age >= budget {
            progress.note_no_progress_abort(age);
            return format!(
                "stage {stage_id} made no progress for {} s (batch heartbeat, memory-pool \
                 activity, and spill bytes all frozen — possible DataFusion spill-pool \
                 deadlock, KAN-47); aborting the stage so its task slot frees \
                 (OXIDANT_STAGE_NO_PROGRESS_SECS)",
                age.as_secs()
            );
        }
    }
}

/// Render the per-stage observability summary emitted at every stage-task exit (KAN-47):
/// identity, shape, progress counters, and — when the watchdog fired — how stale the last
/// progress signal was.
fn stage_summary_line(
    stage_id: u32,
    partition_id: u32,
    num_partitions: u32,
    progress: &StageProgress,
    spill_bytes: u64,
    duration: std::time::Duration,
    status: &str,
) -> String {
    let mut line = format!(
        "Oxidant stage summary: stage_id={stage_id} partition_id={partition_id} \
         num_partitions={num_partitions} batches={} spill_bytes={spill_bytes} \
         duration_ms={} status={status}",
        progress.batches(),
        duration.as_millis(),
    );
    if let Some(age) = progress.no_progress_age() {
        line.push_str(&format!(" last_progress_age_ms={}", age.as_millis()));
    }
    line
}

/// Total spill bytes currently on disk for this worker: the engine's DataFusion spill dir
/// plus the oxidant shuffle spill dir (KAN-47 progress signal + observability).
fn total_spill_bytes(engine: &Engine, spill: Option<&SpillStore>) -> u64 {
    engine.spill_dir_bytes() + spill.map_or(0, |s| oxidant_loom::dir_bytes(s.root()))
}
/// Bound a stage task's wall-clock time, abort it when the driver cancels the stage, and
/// abort it when the no-progress watchdog (KAN-47) fires. The cancel and watchdog paths
/// race the whole stage body, so either interrupts a running stage (dropping the future
/// unwinds in-flight pulls and execution) instead of only being checked between
/// operations. All exit messages are deliberately non-retryable per
/// [`crate::scheduler::is_retryable`].
async fn run_stage_bounded(
    run: impl std::future::Future<Output = std::result::Result<Vec<RecordBatch>, Status>>,
    timeout: std::time::Duration,
    cancel: &Arc<AtomicBool>,
    watchdog: impl std::future::Future<Output = String>,
) -> std::result::Result<Vec<RecordBatch>, Status> {
    tokio::select! {
        res = tokio::time::timeout(timeout, run) => match res {
            Ok(r) => r,
            Err(_) => Err(Status::resource_exhausted(format!(
                "stage timed out after {} ms (OXIDANT_STAGE_TIMEOUT_MS)",
                timeout.as_millis()
            ))),
        },
        _ = wait_stage_cancel(cancel) => Err(Status::aborted(
            "stage cancelled by driver".to_string(),
        )),
        msg = watchdog => Err(Status::aborted(msg)),
    }
}

/// Whether a stage-task failure is the KAN-47 no-progress watchdog abort — the one failure
/// KAN-53 retries once on the worker with the flipped join strategy (a strategy-dependent
/// wedge class: hash-build memory pressure vs. sort-merge spill). Driver cancels
/// ("stage cancelled by driver") and wall-clock timeouts carry different messages/codes
/// and stay final.
fn is_no_progress_abort(status: &Status) -> bool {
    status.code() == tonic::Code::Aborted && status.message().contains("made no progress")
}

/// Poll the per-stage cancel flag (set by the `cancel_stage` action); 100 ms granularity.
async fn wait_stage_cancel(flag: &Arc<AtomicBool>) {
    while !flag.load(Ordering::Relaxed) {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Test hook (KAN-17 tests): `OXIDANT_TEST_STAGE_DELAY_MS` sleeps inside the bounded stage body so
/// the timeout and cancel paths can hold a task slot deterministically. Never set in production.
async fn test_stage_delay() {
    let Some(ms) = std::env::var("OXIDANT_TEST_STAGE_DELAY_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&ms| ms > 0)
    else {
        return;
    };
    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
}

/// Test hook (KAN-47 tests): `OXIDANT_TEST_STAGE_BATCH_DELAY_MS` sleeps after each batch in
/// the stage streaming paths so a slow-but-progressing stage can be simulated. Never set
/// in production.
async fn test_batch_delay() {
    let Some(ms) = std::env::var("OXIDANT_TEST_STAGE_BATCH_DELAY_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&ms| ms > 0)
    else {
        return;
    };
    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
}

/// Test hook (KAN-53 tests): `OXIDANT_TEST_STAGE_STALL_ONCE_MS` sleeps inside the bounded
/// stage body like [`test_stage_delay`] but only on the FIRST attempt in the process — the
/// KAN-53 stall-retry then runs unimpeded, simulating a strategy-specific wedge the
/// flipped retry escapes. Never set in production.
async fn test_stage_stall_once() {
    static STALLED: AtomicBool = AtomicBool::new(false);
    let Some(ms) = std::env::var("OXIDANT_TEST_STAGE_STALL_ONCE_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&ms| ms > 0)
    else {
        return;
    };
    if STALLED.swap(true, Ordering::SeqCst) {
        return;
    }
    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
}

type FlightStream<T> =
    Pin<Box<dyn futures::Stream<Item = std::result::Result<T, Status>> + Send + 'static>>;

fn unimpl<T>(what: &str) -> std::result::Result<Response<T>, Status> {
    Err(Status::unimplemented(format!(
        "flight {what} not implemented"
    )))
}

/// Slice batches so each encode input is at most [`FLIGHT_CHUNK_ROWS`] rows.
fn chunk_batches_for_flight(batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    for batch in batches {
        let rows = batch.num_rows();
        if rows <= FLIGHT_CHUNK_ROWS {
            out.push(batch);
            continue;
        }
        let mut offset = 0;
        while offset < rows {
            let end = (offset + FLIGHT_CHUNK_ROWS).min(rows);
            out.push(batch.slice(offset, end - offset));
            offset = end;
        }
    }
    out
}

/// Rebuild `batch` onto `schema` when its columns fit (erasing nullability/metadata drift
/// between schema sources — e.g. a planned schema versus an execution-stream schema,
/// KAN-39). `None` when the batch genuinely does not conform (different fields/types), so
/// callers can keep it and fail loudly downstream rather than silently dropping data.
fn align_batch_schema(batch: &RecordBatch, schema: &SchemaRef) -> Option<RecordBatch> {
    if batch.schema().as_ref() == schema.as_ref() {
        return Some(batch.clone());
    }
    RecordBatch::try_new(schema.clone(), batch.columns().to_vec()).ok()
}

/// Build a Flight `do_get` response stream from a set of record batches.
fn batches_to_stream(batches: Vec<RecordBatch>) -> FlightStream<FlightData> {
    let batches = chunk_batches_for_flight(batches);
    let schema = match batches.first() {
        Some(b) => b.schema(),
        None => Arc::new(Schema::empty()),
    };
    let input = futures::stream::iter(batches.into_iter().map(Ok::<_, FlightError>));
    FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(input)
        .map_err(|e| Status::internal(e.to_string()))
        .boxed()
}

fn action_response(body: impl Into<Vec<u8>>) -> Response<FlightStream<arrow_flight::Result>> {
    let body = arrow_flight::Result {
        body: body.into().into(),
    };
    Response::new(futures::stream::iter(vec![Ok(body)]).boxed())
}

impl Worker {
    /// Build the KAN-47 no-progress watchdog for one stage-task attempt: samples the
    /// task's batch heartbeat plus worker-wide progress signals (memory-pool activity,
    /// spill bytes); fires with an actionable abort when all stay frozen for the budget.
    /// Constructed per attempt so the KAN-53 stall-retry gets a fresh budget.
    fn stage_watchdog(
        &self,
        stage_id: u32,
        progress: Arc<StageProgress>,
    ) -> impl std::future::Future<Output = String> {
        let engine = self.engine.clone();
        let spill_root = self.spill.as_ref().map(|s| s.root().to_path_buf());
        let sampler_progress = progress.clone();
        let sample = move || ProgressSample {
            batches: sampler_progress.batches(),
            pool_activity_ms: engine.pool_activity_ms(),
            spill_bytes: engine.spill_dir_bytes()
                + spill_root.as_deref().map_or(0, oxidant_loom::dir_bytes),
        };
        let budget = stage_no_progress_budget();
        watch_stage_progress(
            sample,
            stage_watchdog_interval(budget),
            budget,
            stage_id,
            progress,
        )
    }

    /// Run a [`StageTicket`]. First, if it has upstreams, pull this partition's bucket of each
    /// upstream stage from every worker — or, for a ticket-marked `Forward` upstream, only
    /// from its single producer endpoint, or, when the driver AQE-coalesced the read
    /// (`coalesce_read_modulus`), this partition's whole modulus class of buckets
    /// (`p, p+m, …`) — and register them under this task's localized `shuffle_input` names
    /// (one per upstream). Then run the stage SQL. If `produce` is set, hash-partition the result
    /// by `hash_key_cols` and cache it for downstreams (returning empty); otherwise return the
    /// result (the output stage). A stage can both consume *and* produce — an intermediate stage
    /// of a multi-shuffle DAG.
    async fn run_stage(
        &self,
        t: StageTicket,
        progress: &StageProgress,
    ) -> std::result::Result<Vec<RecordBatch>, Status> {
        // In-process workers (tests / local harnesses) carry an explicit shard assignment:
        // scope it around the whole stage so every file-listing resolution — producer and
        // output paths, including their collect fallbacks — sees this worker's shard.
        match self.shard_assignment {
            Some(assignment) => {
                oxidant_loom::shard::with_shard_assignment(
                    assignment,
                    self.run_stage_inner(t, progress),
                )
                .await
            }
            None => self.run_stage_inner(t, progress).await,
        }
    }

    async fn run_stage_inner(
        &self,
        t: StageTicket,
        progress: &StageProgress,
    ) -> std::result::Result<Vec<RecordBatch>, Status> {
        // R5-4: the canonical (pre-localization) stage SQL is the stage plan cache's key
        // component — identical across this stage's tasks, unlike the per-task localized
        // text below. Captured before the rewrite.
        let canonical_sql = t.stage_sql.clone();
        // F2: scope this task's shuffle-input tables (and the stage SQL referencing them)
        // to (stage_id, partition_id) so sibling partition tasks on this worker register
        // disjoint MemTables and run concurrently — they previously shared the fixed
        // `shuffle_input*` names, which forced the driver to serialize per-worker tasks.
        let t = if t.upstream_stage_ids.is_empty() {
            t
        } else {
            StageTicket {
                stage_sql: crate::shuffle::localize_shuffle_input_sql(
                    &t.stage_sql,
                    t.stage_id,
                    t.partition_id,
                    t.upstream_stage_ids.len(),
                ),
                ..t
            }
        };
        // Pull + register each upstream's bucket (no-op for a leaf). The guard deregisters
        // the `shuffle_input*` tables on every exit path (KAN-19).
        let mut shuffle_inputs = ShuffleInputGuard {
            engine: self.engine.clone(),
            names: Vec::new(),
        };
        // R5-4: this task's registered shuffle-input providers (upstream order), handed to
        // the stage plan cache so a template hit rebinds scans to THIS task's data (and its
        // measured row totals) instead of re-planning.
        let mut shuffle_providers = Vec::new();
        // AQE-coalesced read: a modulus `m < num_partitions` makes this consumer partition
        // pull its whole modulus class (`p, p+m, …`) of each upstream instead of only bucket
        // `p`; the driver dispatches exactly `m` such readers, so every producer bucket is
        // read exactly once. `0` (or an oversized value) is the legacy one-bucket read.
        let read_mod = if t.coalesce_read_modulus == 0 {
            t.num_partitions
        } else {
            t.coalesce_read_modulus.min(t.num_partitions)
        };
        let read_buckets =
            crate::aqe::coalesced_read_buckets(t.num_partitions, read_mod, t.partition_id);
        let single = t.upstream_stage_ids.len() == 1;
        // Pull every (bucket, endpoint) pair of every upstream concurrently (shuffle reads
        // take no server-side task slot, so this cannot starve stage tasks), then
        // concatenate per upstream in the legacy nested-loop order — bucket-major, then
        // endpoint — which ordered consumers rely on; the first error in that order wins.
        let per_upstream =
            futures::future::join_all(t.upstream_stage_ids.iter().map(|&up_stage| {
                // A `Forward`-mode upstream ran exactly once, on the first endpoint (the driver
                // dispatches it there); the other endpoints would only serve schema-less
                // placeholder buckets, so don't round-trip them at all.
                let endpoints = if t.forward_upstream_stage_ids.contains(&up_stage) {
                    &t.upstream_endpoints[..t.upstream_endpoints.len().min(1)]
                } else {
                    &t.upstream_endpoints[..]
                };
                let pulls: Vec<_> = read_buckets
                    .iter()
                    .flat_map(|&bucket| {
                        endpoints
                            .iter()
                            .map(move |ep| pull_bucket(ep.clone(), up_stage, bucket))
                    })
                    .collect();
                futures::future::join_all(pulls)
            }))
            .await;
        for (i, results) in per_upstream.into_iter().enumerate() {
            let mut input = Vec::new();
            for part in results {
                input.extend(part.map_err(|e| Status::internal(e.to_string()))?);
            }
            // A `Forward`-mode upstream (a replicated-only UNION/aggregation arm — see
            // `stage_planner::try_split_broadcast_union`) runs on exactly one worker; unless
            // the ticket marked it (`forward_upstream_stage_ids`, which restricted the pull
            // above to that worker), every other worker listed in `upstream_endpoints` has
            // no cache entry for that stage, so its
            // `do_get` round-trips a placeholder batch with an unknown (zero-field) schema rather
            // than the stage's real schema (see `Worker::read_shuffle` / `do_get_batches_once`).
            // Once at least one batch carries the real schema, drop those schema-less
            // placeholders so `register_batches` doesn't see mismatched Arrow schemas.
            if input.len() > 1 && input.iter().any(|b| !b.schema().fields().is_empty()) {
                input.retain(|b| !b.schema().fields().is_empty());
            }
            // KAN-39: erase benign schema drift (nullability/metadata) between endpoints —
            // e.g. a typed zero-row placeholder recovered from the Flight stream schema
            // (KAN-28) versus data batches from a sibling worker's execution stream — so the
            // MemTable registration below never trips "Mismatch between schema and batches"
            // on it. Batches that genuinely don't fit the declared schema are kept, so real
            // incompatibilities still fail loudly at registration.
            if let Some(declared) = input.first().map(|b| b.schema()) {
                input = input
                    .into_iter()
                    .map(|b| align_batch_schema(&b, &declared).unwrap_or(b))
                    .collect();
            }
            let name = if single {
                crate::shuffle::localized_shuffle_input_name(t.stage_id, t.partition_id, None)
            } else {
                crate::shuffle::localized_shuffle_input_name(t.stage_id, t.partition_id, Some(i))
            };
            // KAN-2 A3: when the ticket carries the driver's barrier-measured bucket
            // totals, register the input with that exact row count attached — the
            // plan-time join-strategy guard then sizes hash builds from measured data.
            // Otherwise the plain MemTable registration applies (DataFusion's own
            // batch-derived statistics).
            let provider = match measured_upstream_rows(&t, &read_buckets, i) {
                Some(rows) => self.engine.register_batches_with_stats(&name, input, rows),
                None => self.engine.register_batches(&name, input),
            }
            .map_err(|e| Status::internal(e.to_string()))?;
            shuffle_providers.push(provider);
            shuffle_inputs.names.push(name);
        }

        // R5-4: plan once per stage per worker. The key covers the canonical SQL, snapshot
        // pins, replicated classification, and these inputs' schemas — everything that
        // determines the plan; the per-task measured row totals deliberately stay OUT (they
        // re-enter via the hit-path provider rebind). See oxidant_loom::stage_plan_cache.
        let plan_request = self.engine.stage_plan_request(
            &canonical_sql,
            t.stage_id,
            &t.lakehouse_snapshot_pins,
            &t.replicated_tables,
            shuffle_providers,
        );

        if t.produce {
            // Producer: run the stage, hash-partition its output into per-downstream buckets,
            // and cache them under this task's `(stage_id, partition_id)` key for downstreams
            // (returning empty). Per-task keys let several producing tasks of one stage
            // coexist on this worker (KAN-32 per-partition intermediate dispatch).
            let (schema, cache) = self.run_producer_stage(&t, progress, &plan_request).await?;
            let key = (t.stage_id, t.partition_id);
            {
                let mut guard = self.stage_outputs.lock().expect("stage cache poisoned");
                guard.insert(key, (schema, cache));
                // Bound worker-wide in-memory shuffle bytes across stages (largest spills first).
                if let Some(spill) = self.spill.as_ref() {
                    enforce_total_budget(&mut guard, spill)
                        .map_err(|e| Status::internal(e.to_string()))?;
                }
            }
            self.note_stage_output_insert(key);
            Ok(Vec::new())
        } else {
            // Output stage: run and return the result. The stage SQL executes as a stream
            // (like the producer path) so each emitted batch bumps the per-task heartbeat
            // the no-progress watchdog samples (KAN-47); a parked execution freezes it. On
            // a mid-stream failure the partial output is discarded and the stage is re-run
            // through the guarded collect path (`Engine::sql`, carrying the KAN-25
            // sort-merge retry), mirroring the producer fallback. The schema comes from the
            // stream so a zero-row result is still returned as a typed empty batch — a
            // truly empty vec would make `batches_to_stream` fall back to a zero-field
            // schema, and the driver's `unify_schema` cannot rebuild zero-column batches,
            // silently dropping the placeholder and surfacing as "register `result`: no
            // batches" (KAN-28).
            let (schema, batches) =
                oxidant_loom::shard::with_replicated_tables(&t.replicated_tables, async {
                    let stream = match self
                        .engine
                        .sql_stream_stage_with_lakehouse_snapshots(
                            &t.stage_sql,
                            &t.lakehouse_snapshot_pins,
                            Some(&plan_request),
                        )
                        .await
                    {
                        Ok(stream) => stream,
                        // Planning failed; the collect path plans identically and would
                        // fail the same way, so surface this error directly instead of
                        // planning twice.
                        Err(e) => return Err(Status::internal(e.to_string())),
                    };
                    let schema = stream.schema();
                    match Self::collect_stage_stream(stream, progress).await {
                        Ok(batches) => Ok((schema, batches)),
                        Err(stream_err) => {
                            tracing::warn!(
                                stage_id = t.stage_id,
                                partition_id = t.partition_id,
                                error = %stream_err,
                                "streaming output stage failed mid-stream; falling back to guarded collect"
                            );
                            let schema = self
                                .engine
                                .schema_with_lakehouse_snapshots(
                                    &t.stage_sql,
                                    &t.lakehouse_snapshot_pins,
                                )
                                .await
                                .map_err(|e| Status::internal(e.to_string()))?;
                            let batches = self
                                .engine
                                .sql_with_lakehouse_snapshots(
                                    &t.stage_sql,
                                    &t.lakehouse_snapshot_pins,
                                )
                                .await
                                .map_err(|e| Status::internal(e.to_string()))?;
                            Ok::<_, Status>((schema, batches))
                        }
                    }
                })
                .await?;
            if batches.is_empty() {
                return Ok(vec![RecordBatch::new_empty(schema)]);
            }
            Ok(batches)
        }
    }

    /// Drain a stage stream into memory, bumping the per-task batch heartbeat (KAN-47) per
    /// batch so the output stage's execution liveness is visible to the no-progress
    /// watchdog exactly like the producer's [`Worker::partition_stage_stream`] loop.
    async fn collect_stage_stream(
        mut stream: datafusion::physical_plan::SendableRecordBatchStream,
        progress: &StageProgress,
    ) -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| Error::Execution(format!("stage stream: {e}")))?;
            progress.note_batch();
            test_batch_delay().await;
            batches.push(batch);
        }
        Ok(batches)
    }

    /// Produce one stage task's partitioned output cache (KAN-32). The stage SQL executes as a
    /// stream, hash-partitioned batch-by-batch into a spill-aware [`BucketCache`], so a large
    /// join/aggregate output never sits fully materialized in worker memory *outside* the
    /// DataFusion pool — the collect-then-partition path held two full copies of the stage
    /// output, which at SF10 pushed Q18's join stage past 25 GB RSS on a 16 GiB pool. On a
    /// mid-stream failure (e.g. pool exhaustion under a non-spillable hash join) the partial
    /// cache is discarded and the stage is re-run through the guarded collect path
    /// ([`produce_stage_collect`]), which carries the KAN-25 sort-merge retry.
    async fn run_producer_stage(
        &self,
        t: &StageTicket,
        progress: &StageProgress,
        plan_request: &oxidant_loom::stage_plan_cache::StagePlanRequest,
    ) -> std::result::Result<(SchemaRef, BucketCache), Status> {
        let key_cols: Vec<usize> = t.hash_key_cols.iter().map(|&c| c as usize).collect();
        oxidant_loom::shard::with_replicated_tables(&t.replicated_tables, async {
            let stream = match self
                .engine
                .sql_stream_stage_with_lakehouse_snapshots(
                    &t.stage_sql,
                    &t.lakehouse_snapshot_pins,
                    Some(plan_request),
                )
                .await
            {
                Ok(stream) => stream,
                // Planning failed; the collect path plans identically and would fail the
                // same way, so surface this error directly instead of planning twice.
                Err(e) => return Err(Status::internal(e.to_string())),
            };
            match self
                .partition_stage_stream(t, stream, &key_cols, progress)
                .await
            {
                Ok(done) => Ok(done),
                Err(stream_err) => {
                    // Discard this task's partial spill files before the retry rewrites them.
                    if let Some(spill) = self.spill.as_ref() {
                        spill.clear_scoped_stage(t.stage_id, t.partition_id);
                    }
                    tracing::warn!(
                        stage_id = t.stage_id,
                        partition_id = t.partition_id,
                        error = %stream_err,
                        "streaming producer failed mid-stream; falling back to guarded collect"
                    );
                    self.produce_stage_collect(t, &key_cols).await
                }
            }
        })
        .await
    }

    /// Consume the stage stream, hash-partitioning each batch and flushing bucket buffers into
    /// the cache (spill-aware) whenever they reach [`PRODUCER_FLUSH_BYTES`].
    async fn partition_stage_stream(
        &self,
        t: &StageTicket,
        mut stream: datafusion::physical_plan::SendableRecordBatchStream,
        key_cols: &[usize],
        progress: &StageProgress,
    ) -> Result<(SchemaRef, BucketCache)> {
        let schema = stream.schema();
        let num_partitions = t.num_partitions as usize;
        let mut cache = BucketCache::from_memory(vec![Vec::new(); num_partitions]);
        let mut staging: Vec<Vec<RecordBatch>> = (0..num_partitions).map(|_| Vec::new()).collect();
        let mut staged_bytes = 0usize;
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| Error::Execution(format!("stage stream: {e}")))?;
            // Heartbeat per received batch (KAN-47): even an empty batch proves the
            // execution stream is alive.
            progress.note_batch();
            test_batch_delay().await;
            if batch.num_rows() == 0 {
                continue;
            }
            let parts = hash_partition(std::slice::from_ref(&batch), key_cols, num_partitions)?;
            for (p, part) in parts.into_iter().enumerate() {
                staged_bytes += crate::shuffle::estimated_batch_bytes(&part);
                staging[p].extend(part);
            }
            if staged_bytes >= PRODUCER_FLUSH_BYTES {
                self.flush_staging(&mut cache, &mut staging, &schema, t)?;
                staged_bytes = 0;
            }
        }
        self.flush_staging(&mut cache, &mut staging, &schema, t)?;
        Ok((schema, cache))
    }

    /// Drain the per-bucket staging buffers into `cache` (spilling at the configured threshold).
    fn flush_staging(
        &self,
        cache: &mut BucketCache,
        staging: &mut [Vec<RecordBatch>],
        schema: &SchemaRef,
        t: &StageTicket,
    ) -> Result<()> {
        for (p, batches) in staging.iter_mut().enumerate() {
            if batches.is_empty() {
                continue;
            }
            cache.append_partition(
                schema.clone(),
                t.stage_id,
                t.partition_id,
                p as u32,
                std::mem::take(batches),
                self.spill.as_ref(),
            )?;
        }
        Ok(())
    }

    /// The pre-KAN-32 produce path: run the stage SQL to completion via the guarded collect
    /// (carrying the KAN-25 sort-merge retry on pool exhaustion), then hash-partition and
    /// cache. Fallback for when streaming execution fails mid-stream.
    async fn produce_stage_collect(
        &self,
        t: &StageTicket,
        key_cols: &[usize],
    ) -> std::result::Result<(SchemaRef, BucketCache), Status> {
        let schema = self
            .engine
            .schema_with_lakehouse_snapshots(&t.stage_sql, &t.lakehouse_snapshot_pins)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let batches = self
            .engine
            .sql_with_lakehouse_snapshots(&t.stage_sql, &t.lakehouse_snapshot_pins)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let buckets = hash_partition(&batches, key_cols, t.num_partitions as usize)
            .map_err(|e| Status::internal(e.to_string()))?;
        let cache = BucketCache::maybe_spill(
            schema.clone(),
            buckets,
            t.stage_id,
            t.partition_id,
            self.spill.as_ref(),
        )
        .map_err(|e| Status::internal(e.to_string()))?;
        Ok((schema, cache))
    }

    /// Serve one cached shuffle bucket, unioned across every producing task's cache entry for
    /// the stage (KAN-32). An empty union is served as a single schema-carrying empty batch
    /// (never a truly empty stream) so the consumer can always register the input table —
    /// important for shuffle joins where some key buckets legitimately have no rows.
    fn read_shuffle(&self, r: ShuffleReadTicket) -> std::result::Result<Vec<RecordBatch>, Status> {
        // Lock order matches `note_stage_output_insert`: stamps, then the cache.
        let stamps = self.stage_inserted.lock().expect("stage stamps poisoned");
        let guard = self.stage_outputs.lock().expect("stage cache poisoned");
        // KAN-39: stage ids repeat across queries (the planner numbers each plan from 0), and
        // a timed-out/cancelled query's producer can insert its cache entry after the
        // driver's best-effort stage cleanup has already raced past it (KAN-18/19). Unioning
        // that stale entry with the current query's same-id entries served mixed-schema
        // bucket sets, which failed the consumer's MemTable registration ("Mismatch between
        // schema and batches"). The current query's entries are always the most recently
        // inserted (the driver dispatches consumers only after every producer of the stage
        // finished), so the freshest entry declares the served schema; entries that cannot
        // align to it are foreign leftovers and are skipped rather than served.
        let mut entries: Vec<(&(u32, u32), &CachedStage)> = guard
            .iter()
            .filter(|((stage_id, _), _)| *stage_id == r.stage_id)
            .collect();
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        entries.sort_by_key(|(key, _)| stamps.get(key));
        let schema = entries
            .last()
            .map(|(_, (s, _))| s.clone())
            .expect("non-empty entries");
        let mut batches = Vec::new();
        for (key, (entry_schema, cache)) in entries {
            // A spilled bucket that fails to read back must fail the pull, not serve a
            // silently truncated bucket (wrong aggregates downstream — SF10 TPC-H Q16).
            let part = cache
                .read_partition(r.target_partition as usize)
                .map_err(|e| {
                    Status::internal(format!(
                        "shuffle read stage {} partition {} src {}: {e}",
                        r.stage_id, r.target_partition, key.1
                    ))
                })?;
            if entry_schema.as_ref() == schema.as_ref() {
                batches.extend(part);
                continue;
            }
            // Schema drift within one query is benign (a planned schema vs an execution
            // stream schema, differing only in nullability/metadata) and aligns cleanly;
            // anything else is a stale entry from another query and must not be served.
            let mut aligned = Vec::with_capacity(part.len());
            let mut conforms = true;
            for b in part {
                match align_batch_schema(&b, &schema) {
                    Some(b) => aligned.push(b),
                    None => {
                        conforms = false;
                        break;
                    }
                }
            }
            if conforms {
                batches.extend(aligned);
            } else {
                tracing::warn!(
                    stage_id = r.stage_id,
                    src = key.1,
                    "skipping cache entry whose schema does not fit the stage's freshest schema (stale leftover from an earlier query?)"
                );
            }
        }
        // Sibling entries contribute typed zero-row placeholders for their empty buckets;
        // drop them once any real rows exist.
        let data: Vec<RecordBatch> = batches.into_iter().filter(|b| b.num_rows() > 0).collect();
        Ok(if data.is_empty() {
            vec![RecordBatch::new_empty(schema)]
        } else {
            data
        })
    }

    /// Append one pushed shuffle batch to the stage cache so future pull-based
    /// `ShuffleReadTicket`s observe the same data. Pushed entries live under the
    /// [`PUSH_SRC`] scope (the producing task's partition id is unknown to the receiver).
    fn cache_append_batch(
        &self,
        header: ShuffleExchangeHeader,
        schema: SchemaRef,
        batch: RecordBatch,
    ) -> Result<()> {
        let key = (header.stage_id, PUSH_SRC);
        {
            let mut guard = self.stage_outputs.lock().expect("stage cache poisoned");
            match guard.get_mut(&key) {
                Some((existing_schema, cache)) => {
                    if existing_schema.as_ref() != schema.as_ref() {
                        return Err(Error::Execution(format!(
                            "do_exchange schema mismatch for stage {} partition {}",
                            header.stage_id, header.partition_id
                        )));
                    }
                    cache.append_batch(
                        existing_schema.clone(),
                        header.stage_id,
                        PUSH_SRC,
                        header.partition_id,
                        batch,
                        self.spill.as_ref(),
                    )?;
                }
                None => {
                    let cache = BucketCache::from_partition(
                        schema.clone(),
                        header.stage_id,
                        PUSH_SRC,
                        header.partition_id,
                        vec![batch],
                        self.spill.as_ref(),
                    )?;
                    guard.insert(key, (schema, cache));
                }
            }
            // Bound worker-wide in-memory shuffle bytes across stages (largest spills first).
            if let Some(spill) = self.spill.as_ref() {
                enforce_total_budget(&mut guard, spill)?;
            }
        }
        self.note_stage_output_insert(key);
        Ok(())
    }
}

#[tonic::async_trait]
impl FlightService for Worker {
    type HandshakeStream = FlightStream<HandshakeResponse>;
    type ListFlightsStream = FlightStream<FlightInfo>;
    type DoGetStream = FlightStream<FlightData>;
    type DoPutStream = FlightStream<PutResult>;
    type DoActionStream = FlightStream<arrow_flight::Result>;
    type ListActionsStream = FlightStream<ActionType>;
    type DoExchangeStream = FlightStream<FlightData>;

    /// Dispatch on the ticket kind: legacy SQL, a stage, or a shuffle-bucket read.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        let bytes = request.into_inner().ticket.to_vec();
        let ticket = protocol::decode_ticket(&bytes)
            .map_err(|e| Status::invalid_argument(format!("decode ticket: {e}")))?;
        let batches = match ticket {
            protocol::Ticket::Sql(sql) => self
                .engine
                .sql(&sql)
                .await
                .map_err(|e| Status::internal(e.to_string()))?,
            protocol::Ticket::Stage(t) => {
                let _slot = self.acquire_task_slot().await?;
                crate::fault_inject::maybe_fault_exit(&t);
                let stage_id = t.stage_id;
                let partition_id = t.partition_id;
                let num_partitions = t.num_partitions;
                let cancel = self.register_stage_cancel(stage_id);
                let progress = Arc::new(StageProgress::default());
                let start = std::time::Instant::now();
                let retry_ticket = t.clone();
                // KAN-31/KAN-46: reap this producer task's spill scope when the task exits
                // without committing — error return, or this future being dropped because
                // the Flight client went away mid-stage. Armed only for producers: a
                // consumer task's `(stage_id, partition_id)` scope files belong to that
                // stage's producer tasks, not to it.
                let mut spill_reaper = StageSpillReaper {
                    spill: self.spill.clone(),
                    stage_outputs: self.stage_outputs.clone(),
                    stage_inserted: self.stage_inserted.clone(),
                    stage_id,
                    src: partition_id,
                    armed: t.produce,
                };
                let run = async {
                    test_stage_delay().await;
                    test_stage_stall_once().await;
                    self.run_stage(t, &progress).await
                };
                let watchdog = self.stage_watchdog(stage_id, progress.clone());
                let result = run_stage_bounded(run, stage_timeout(), &cancel, watchdog).await;
                // KAN-53 stall-retry: a stage task aborted by the no-progress watchdog (a
                // strategy-dependent wedge class — hash-build memory pressure vs.
                // sort-merge spill) is re-run ONCE with the opposite join strategy
                // (`with_join_strategy_flipped` drives the engine's planning) before the
                // query is failed. Driver cancels and wall-clock timeouts stay final. A
                // fresh watchdog bounds the retry; a second abort surfaces to the driver.
                let result = match result {
                    Err(status) if is_no_progress_abort(&status) => {
                        tracing::warn!(
                            stage_id,
                            partition_id,
                            error = %status.message(),
                            "stage aborted by the no-progress watchdog; retrying once with \
                             the flipped join strategy (KAN-53)"
                        );
                        // Drop a producer attempt's partial spill files before the retry
                        // rewrites them.
                        if let Some(spill) = self.spill.as_ref() {
                            spill.clear_scoped_stage(stage_id, partition_id);
                        }
                        let retry_watchdog = self.stage_watchdog(stage_id, progress.clone());
                        let retry = async {
                            test_stage_delay().await;
                            test_stage_stall_once().await;
                            oxidant_loom::with_join_strategy_flipped(
                                self.run_stage(retry_ticket, &progress),
                            )
                            .await
                        };
                        run_stage_bounded(retry, stage_timeout(), &cancel, retry_watchdog).await
                    }
                    result => result,
                };
                if result.is_ok() {
                    // Committed: the cache entry owns its spill files now (reaped by
                    // `clear_stages` / the TTL sweep), so the reaper stands down.
                    spill_reaper.disarm();
                }
                self.unregister_stage_cancel(stage_id, &cancel);
                // Per-stage observability summary (KAN-47): one line per stage-task exit.
                eprintln!(
                    "{}",
                    stage_summary_line(
                        stage_id,
                        partition_id,
                        num_partitions,
                        &progress,
                        total_spill_bytes(&self.engine, self.spill.as_ref()),
                        start.elapsed(),
                        if result.is_ok() { "ok" } else { "error" },
                    )
                );
                result?
            }
            protocol::Ticket::ShuffleRead(r) => self.read_shuffle(r)?,
        };
        Ok(Response::new(batches_to_stream(batches)))
    }

    async fn handshake(
        &self,
        _r: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        unimpl("handshake")
    }
    async fn list_flights(
        &self,
        _r: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        unimpl("list_flights")
    }
    async fn get_flight_info(
        &self,
        _r: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        unimpl("get_flight_info")
    }
    async fn poll_flight_info(
        &self,
        _r: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<PollInfo>, Status> {
        unimpl("poll_flight_info")
    }
    async fn get_schema(
        &self,
        _r: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        unimpl("get_schema")
    }
    async fn do_put(
        &self,
        _r: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        unimpl("do_put")
    }
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        match action.r#type.as_str() {
            ACTION_CLEAR_STAGES => {
                self.clear_stages();
                Ok(action_response(b"ok".to_vec()))
            }
            ACTION_REGISTER_UDFS => {
                let payload = String::from_utf8_lossy(&action.body);
                self.engine
                    .register_udfs_json(&payload)
                    .map_err(|e| Status::internal(e.to_string()))?;
                Ok(action_response(b"ok".to_vec()))
            }
            ACTION_HEALTH => Ok(action_response(b"ok".to_vec())),
            protocol::ACTION_CANCEL_STAGE => {
                let body = String::from_utf8_lossy(&action.body);
                let stage_id: u32 = body.trim().parse().map_err(|_| {
                    Status::invalid_argument("cancel_stage body must be a decimal stage id")
                })?;
                let cancelled = self.cancel_stage(stage_id);
                Ok(action_response(if cancelled {
                    b"cancelled".to_vec()
                } else {
                    b"idle".to_vec()
                }))
            }
            ACTION_HEARTBEAT => Ok(action_response(self.heartbeat_payload().into_bytes())),
            ACTION_BUCKET_ROW_COUNTS => {
                let body = String::from_utf8_lossy(&action.body);
                let stage_id: u32 = body.trim().parse().map_err(|_| {
                    Status::invalid_argument("bucket_row_counts body must be a decimal stage id")
                })?;
                // Sum per-partition rows across every producer scope of the stage.
                let mut counts: Vec<usize> = Vec::new();
                {
                    let guard = self.stage_outputs.lock().expect("stage cache poisoned");
                    for ((sid, _), (_, cache)) in guard.iter() {
                        if *sid != stage_id {
                            continue;
                        }
                        for (p, rows) in cache.partition_row_counts().into_iter().enumerate() {
                            if counts.len() <= p {
                                counts.resize(p + 1, 0);
                            }
                            counts[p] += rows;
                        }
                    }
                }
                let body = serde_json::json!({ "counts": counts }).to_string();
                Ok(action_response(body.into_bytes()))
            }
            ACTION_TASK_STATUS => {
                if !action.body.is_empty() {
                    *self.last_task_status.lock().expect("task status poisoned") =
                        Some(action.body.to_vec());
                }
                Ok(action_response(self.task_status_payload().into_bytes()))
            }
            other => Err(Status::unimplemented(format!(
                "flight do_action `{other}` not implemented"
            ))),
        }
    }
    async fn list_actions(
        &self,
        _r: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        unimpl("list_actions")
    }
    /// Streaming shuffle exchange. The first frame is a metadata-only exchange header
    /// (`stage_id` + `partition_id`), followed by normal Arrow IPC FlightData frames. Each
    /// received batch is appended into the stage cache as it arrives (gRPC flow control provides
    /// backpressure); spill policy matches pull-based [`ShuffleReadTicket`] caching.
    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        let mut stream = request.into_inner();
        let first = stream
            .message()
            .await
            .map_err(|e| Status::internal(format!("do_exchange header: {e}")))?
            .ok_or_else(|| Status::invalid_argument("do_exchange missing header"))?;
        let header_bytes = if first.app_metadata.is_empty() {
            first.data_header.as_ref()
        } else {
            first.app_metadata.as_ref()
        };
        let header = ShuffleExchangeHeader::decode(header_bytes)
            .map_err(|e| Status::invalid_argument(format!("decode do_exchange header: {e}")))?;

        let mut rb = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
            stream.map_err(|s| FlightError::Tonic(Box::new(s))),
        );
        let mut schema: Option<SchemaRef> = None;
        let mut saw_batch = false;
        while let Some(batch) = rb.next().await {
            let batch = batch.map_err(|e| Status::internal(format!("flight decode: {e}")))?;
            saw_batch = true;
            let batch_schema = batch.schema();
            if let Some(existing) = &schema {
                if existing.as_ref() != batch_schema.as_ref() {
                    return Err(Status::invalid_argument(format!(
                        "do_exchange schema mismatch for stage {} partition {}",
                        header.stage_id, header.partition_id
                    )));
                }
            } else {
                schema = Some(batch_schema);
            }
            self.cache_append_batch(header, schema.clone().unwrap(), batch)
                .map_err(|e| Status::internal(e.to_string()))?;
        }
        if !saw_batch {
            let schema = rb
                .schema()
                .cloned()
                .ok_or_else(|| Status::invalid_argument("do_exchange missing Arrow schema"))?;
            self.cache_append_batch(
                header,
                schema.clone(),
                RecordBatch::new_empty(schema.clone()),
            )
            .map_err(|e| Status::internal(e.to_string()))?;
        }

        let ack = FlightData {
            flight_descriptor: None,
            data_header: Vec::new().into(),
            app_metadata: b"ok".to_vec().into(),
            data_body: Vec::new().into(),
        };
        Ok(Response::new(futures::stream::iter(vec![Ok(ack)]).boxed()))
    }
}

/// Serve a worker on `0.0.0.0:port` until the process exits.
pub async fn serve_worker(port: u16, engine: Arc<Engine>) -> Result<()> {
    serve_flight_worker(port, Worker::new(engine)).await
}

/// Serve a worker whose stage executions shard file listings by an explicit assignment
/// (in-process multi-worker tests, where the process env can name only one shard).
pub async fn serve_worker_with_assignment(
    port: u16,
    engine: Arc<Engine>,
    assignment: oxidant_loom::shard::ShardAssignment,
) -> Result<()> {
    serve_flight_worker(port, Worker::with_shard_assignment(engine, assignment)).await
}

/// Serve a worker constructed with an explicit spill store (threshold / mixture tests).
pub async fn serve_worker_with_spill(
    port: u16,
    engine: Arc<Engine>,
    spill: SpillStore,
    keep_spill: bool,
) -> Result<()> {
    let worker = if keep_spill {
        Worker::with_spill_keep(engine, spill)
    } else {
        Worker::with_spill(engine, spill)
    };
    serve_flight_worker(port, worker).await
}

async fn serve_flight_worker(port: u16, worker: Worker) -> Result<()> {
    let addr = format!("0.0.0.0:{port}")
        .parse()
        .map_err(|e| Error::Io(format!("bad worker addr: {e}")))?;
    // Keepalive matches the client in [`connect_flight`]: detect dead peers and keep
    // long-idle pooled connections alive across stage gaps at SF100.
    tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(std::time::Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(std::time::Duration::from_secs(10)))
        .add_service(
            FlightServiceServer::new(worker)
                .max_decoding_message_size(MAX_MSG)
                .max_encoding_message_size(MAX_MSG),
        )
        .serve(addr)
        .await
        .map_err(|e| Error::Io(format!("worker serve: {e}")))?;
    Ok(())
}

/// Format a tonic [`Status`] with its `source()` chain so transport failures are diagnosable.
///
/// tonic's `transport::Error` displays only as `"transport error"`; the real cause
/// (connection reset, GOAWAY, incomplete message) lives in `Status::source()`, which
/// `Status::from_error` populates. SF100 TPC-DS Q10 failed with the opaque string alone.
fn status_detail(status: &tonic::Status) -> String {
    error_detail(status)
}

/// Append the `source()` chain of any [`std::error::Error`] (tonic Status, transport Error, …).
fn error_detail(err: &(dyn std::error::Error + 'static)) -> String {
    let mut out = err.to_string();
    // Prefer Status::message() when present — Display on Status includes the code prefix.
    if let Some(status) = err.downcast_ref::<tonic::Status>() {
        out = status.message().to_string();
    }
    let mut src = err.source();
    while let Some(e) = src {
        out.push_str(": ");
        out.push_str(&e.to_string());
        src = e.source();
    }
    out
}

/// Connect to a worker and run one `do_get` with retries on transient errors.
async fn do_get_batches(endpoint: String, ticket_bytes: Vec<u8>) -> Result<Vec<RecordBatch>> {
    const MAX_TRIES: u32 = 3;
    let mut last_err = None;
    for attempt in 0..MAX_TRIES {
        match do_get_batches_once(endpoint.clone(), ticket_bytes.clone()).await {
            Ok(b) => return Ok(b),
            Err(e) => {
                // KAN-15: only retry failures where the stage never started server-side;
                // deadline/timeout and execution errors are terminal (see
                // [`crate::scheduler::is_retryable`]).
                if !crate::scheduler::is_retryable(&e) || attempt + 1 == MAX_TRIES {
                    return Err(e);
                }
                // Transport-class failure: the pooled channel may be a dead connection
                // (worker restart) — drop it so the retry dials fresh.
                evict_flight_channel(&endpoint);
                last_err = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(100 * (attempt as u64 + 1)))
                    .await;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| Error::Execution("do_get failed".into())))
}

/// Process-wide Flight channel pool: one eagerly-established HTTP/2 channel per endpoint,
/// cheaply cloned per RPC (tonic `Channel` clones share the connection). Previously every
/// `do_get`/`do_action`/`do_exchange` paid a fresh TCP+HTTP/2 handshake — with per-stage
/// task/fetch fan-out that is dozens of connection establishments per stage per query.
static FLIGHT_CHANNELS: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<String, tonic::transport::Channel>>,
> = std::sync::OnceLock::new();

fn flight_channels(
) -> &'static std::sync::Mutex<std::collections::HashMap<String, tonic::transport::Channel>> {
    FLIGHT_CHANNELS.get_or_init(Default::default)
}

/// Drop a cached channel after a transport-class error (worker restart, broken h2
/// connection): tonic does not redial an eagerly-connected `Channel`, so without eviction
/// every later RPC to that endpoint would keep failing on the dead connection.
fn evict_flight_channel(endpoint: &str) {
    if let Some(map) = FLIGHT_CHANNELS.get() {
        map.lock()
            .expect("flight channels poisoned")
            .remove(endpoint);
    }
}

fn flight_client(
    channel: tonic::transport::Channel,
) -> FlightServiceClient<tonic::transport::Channel> {
    FlightServiceClient::new(channel)
        .max_decoding_message_size(MAX_MSG)
        .max_encoding_message_size(MAX_MSG)
}

/// Connect to a Flight worker with a short connect timeout so CI port conflicts fail fast
/// instead of hanging on the default TCP SYN retry window (~minutes). Channels are pooled
/// per endpoint (see [`FLIGHT_CHANNELS`]).
///
/// KAN-15: there is deliberately no blanket per-request timeout here — tonic applies it to the
/// whole `do_get` stream, so any stage slower than the limit failed client-side while the
/// original attempt kept running server-side (the retry storm / slot exhaustion). Long-running
/// stages are bounded server-side instead (see [`stage_timeout`]).
async fn connect_flight(
    endpoint: String,
) -> Result<FlightServiceClient<tonic::transport::Channel>> {
    if let Some(channel) = flight_channels()
        .lock()
        .expect("flight channels poisoned")
        .get(&endpoint)
        .cloned()
    {
        return Ok(flight_client(channel));
    }
    let channel = tonic::transport::Endpoint::from_shared(endpoint.clone())
        .map_err(|e| Error::Io(format!("endpoint: {e}")))?
        .connect_timeout(std::time::Duration::from_secs(2))
        // Detect dead workers and keep long-idle pooled channels alive between stages
        // (SF100 TPC-DS Q10: opaque `do_get: transport error` after a peer died).
        .http2_keep_alive_interval(std::time::Duration::from_secs(30))
        .keep_alive_timeout(std::time::Duration::from_secs(10))
        .keep_alive_while_idle(true)
        .tcp_keepalive(Some(std::time::Duration::from_secs(30)))
        .connect()
        .await
        .map_err(|e| Error::Io(format!("connect worker: {}", error_detail(&e))))?;
    flight_channels()
        .lock()
        .expect("flight channels poisoned")
        .insert(endpoint, channel.clone());
    Ok(flight_client(channel))
}

async fn do_get_batches_once(endpoint: String, ticket_bytes: Vec<u8>) -> Result<Vec<RecordBatch>> {
    let mut client = connect_flight(endpoint).await?;
    let ticket = Ticket {
        ticket: ticket_bytes.into(),
    };
    let stream = client
        .do_get(ticket)
        .await
        .map_err(|e| Error::Execution(format!("do_get: {}", status_detail(&e))))?
        .into_inner();

    let mut rb = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
        stream.map_err(|s| FlightError::Tonic(Box::new(s))),
    );
    let mut out = Vec::new();
    while let Some(batch) = rb.next().await {
        out.push(batch.map_err(|e| Error::Execution(format!("flight decode: {e}")))?);
    }
    // The Flight encoder sends the schema but drops zero-row batches, so an empty result arrives as
    // no batches at all. Recover a schema-carrying empty batch from the stream so a downstream
    // consumer can still register the (empty) shuffle input — otherwise an all-empty bucket set
    // would surface as "no batches".
    if out.is_empty() {
        if let Some(schema) = rb.schema() {
            out.push(RecordBatch::new_empty(schema.clone()));
        }
    }
    Ok(out)
}

/// Evict all cached shuffle stages on a worker (post-query cleanup).
pub async fn clear_worker_stages(endpoint: String) -> Result<()> {
    do_action(endpoint, ACTION_CLEAR_STAGES, b"").await
}

/// Push UDF definitions to a worker before stage execution.
pub async fn sync_udfs_to_worker(endpoint: String, udf_json: &str) -> Result<()> {
    do_action(endpoint, ACTION_REGISTER_UDFS, udf_json.as_bytes()).await
}

/// Liveness probe — returns `Ok(())` when the worker responds to `ACTION_HEALTH`.
pub async fn health_check_worker(endpoint: String) -> Result<()> {
    do_action(endpoint, ACTION_HEALTH, b"").await
}

/// Best-effort cancel of a stage currently running on a worker (KAN-17). Errors are the
/// caller's to ignore: cancel is best-effort and the stage may already be gone.
pub async fn cancel_stage_on_worker(endpoint: String, stage_id: u32) -> Result<()> {
    do_action(
        endpoint,
        protocol::ACTION_CANCEL_STAGE,
        stage_id.to_string().as_bytes(),
    )
    .await
}

/// Per-partition row counts of a worker's cached output for `stage_id` — the driver's AQE
/// sample of shuffle sizes without transferring the buckets themselves (KAN-32).
pub async fn bucket_row_counts(endpoint: String, stage_id: u32) -> Result<Vec<usize>> {
    let bodies = do_action_collect(
        endpoint,
        ACTION_BUCKET_ROW_COUNTS,
        stage_id.to_string().as_bytes(),
    )
    .await?;
    let body = bodies
        .last()
        .ok_or_else(|| Error::Execution("bucket_row_counts: empty response".into()))?;
    let value: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| Error::Execution(format!("bucket_row_counts: parse response: {e}")))?;
    let counts = value
        .get("counts")
        .and_then(|c| c.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_u64().map(|n| n as usize))
                .collect()
        })
        .unwrap_or_default();
    Ok(counts)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WorkerHeartbeat {
    pub slots_total: Option<usize>,
    pub slots_used: Option<usize>,
}

impl WorkerHeartbeat {
    pub fn has_available_slot(&self) -> bool {
        match (self.slots_total, self.slots_used) {
            (Some(total), Some(used)) => used < total,
            _ => true,
        }
    }
}

/// Heartbeat probe. New workers return slot metadata; older `ok`-only workers are accepted.
pub async fn heartbeat_worker(endpoint: String) -> Result<WorkerHeartbeat> {
    let bodies = do_action_collect(endpoint, ACTION_HEARTBEAT, b"").await?;
    Ok(parse_heartbeat_bodies(&bodies))
}

/// Send or fetch the worker's simple task status payload.
pub async fn task_status_worker(endpoint: String, payload: &[u8]) -> Result<Vec<Vec<u8>>> {
    do_action_collect(endpoint, ACTION_TASK_STATUS, payload).await
}

async fn do_action(endpoint: String, action_type: &str, body: &[u8]) -> Result<()> {
    do_action_collect(endpoint, action_type, body).await?;
    Ok(())
}

async fn do_action_collect(
    endpoint: String,
    action_type: &str,
    body: &[u8],
) -> Result<Vec<Vec<u8>>> {
    let mut client = connect_flight(endpoint.clone()).await?;
    let action = Action {
        r#type: action_type.to_string(),
        body: body.to_vec().into(),
    };
    let mut stream = client
        .do_action(action)
        .await
        .map_err(|e| {
            // Transport-class failure: drop the pooled channel so the next action dials fresh.
            evict_flight_channel(&endpoint);
            Error::Execution(format!("do_action: {}", status_detail(&e)))
        })?
        .into_inner();
    let mut bodies = Vec::new();
    while let Some(item) = stream.next().await {
        bodies.push(
            item.map_err(|e| Error::Execution(format!("do_action stream: {e}")))?
                .body
                .to_vec(),
        );
    }
    Ok(bodies)
}

fn parse_heartbeat_bodies(bodies: &[Vec<u8>]) -> WorkerHeartbeat {
    bodies
        .iter()
        .find_map(|body| parse_heartbeat_payload(body))
        .unwrap_or_default()
}

fn parse_heartbeat_payload(body: &[u8]) -> Option<WorkerHeartbeat> {
    let text = std::str::from_utf8(body).ok()?.trim();
    if text.is_empty() || text == "ok" {
        return None;
    }
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    Some(WorkerHeartbeat {
        slots_total: value
            .get("slots_total")
            .and_then(|v| v.as_u64())
            .map(|v| v as usize),
        slots_used: value
            .get("slots_used")
            .and_then(|v| v.as_u64())
            .map(|v| v as usize),
    })
}

/// Driver: send raw `sql` to a worker over Flight and collect the result (single-stage path).
pub async fn query_worker(endpoint: String, sql: &str) -> Result<Vec<RecordBatch>> {
    do_get_batches(endpoint, sql.as_bytes().to_vec()).await
}

/// Driver: run a [`StageTicket`] on a worker and collect whatever it streams back.
pub async fn run_stage_on_worker(
    endpoint: String,
    ticket: StageTicket,
) -> Result<Vec<RecordBatch>> {
    do_get_batches(endpoint, ticket.to_ticket_bytes()).await
}

/// Pull one shuffle bucket (`target_partition`) of `stage_id` from a worker.
pub async fn pull_bucket(
    endpoint: String,
    stage_id: u32,
    target_partition: u32,
) -> Result<Vec<RecordBatch>> {
    pull_bucket_with_retry(endpoint, stage_id, target_partition).await
}

/// Pull a shuffle bucket with transient retries (shuffle durability on read path).
pub async fn pull_bucket_with_retry(
    endpoint: String,
    stage_id: u32,
    target_partition: u32,
) -> Result<Vec<RecordBatch>> {
    const MAX_TRIES: u32 = 3;
    let ticket = ShuffleReadTicket {
        stage_id,
        target_partition,
    };
    let bytes = ticket.to_ticket_bytes();
    let mut last = None;
    for attempt in 0..MAX_TRIES {
        match do_get_batches(endpoint.clone(), bytes.clone()).await {
            Ok(b) => return Ok(b),
            Err(e) if is_pull_retryable(&e) && attempt + 1 < MAX_TRIES => {
                last = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(50 * (attempt as u64 + 1)))
                    .await;
            }
            Err(e) => return Err(e),
        }
    }
    Err(last.unwrap_or_else(|| Error::Execution("pull_bucket failed".into())))
}

/// Push one shuffle bucket to a worker over Flight `do_exchange`.
///
/// The receiver appends the batches into its local cache under `(stage_id, target_partition)`, so
/// the same data remains readable via [`pull_bucket`] as a fallback path.
pub async fn push_bucket(
    endpoint: String,
    stage_id: u32,
    target_partition: u32,
    batches: Vec<RecordBatch>,
) -> Result<()> {
    let schema = batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new(Schema::empty()));
    push_bucket_with_schema(endpoint, stage_id, target_partition, schema, batches).await
}

/// Push one shuffle bucket with an explicit schema, useful when the bucket has zero rows.
pub async fn push_bucket_with_schema(
    endpoint: String,
    stage_id: u32,
    target_partition: u32,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<()> {
    const MAX_TRIES: u32 = 3;
    let mut last = None;
    for attempt in 0..MAX_TRIES {
        match push_bucket_once(
            endpoint.clone(),
            stage_id,
            target_partition,
            schema.clone(),
            batches.clone(),
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(e) if is_pull_retryable(&e) && attempt + 1 < MAX_TRIES => {
                // Transport-class failure: drop the pooled channel so the retry dials fresh.
                evict_flight_channel(&endpoint);
                last = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(50 * (attempt as u64 + 1)))
                    .await;
            }
            Err(e) => return Err(e),
        }
    }
    Err(last.unwrap_or_else(|| Error::Execution("push_bucket failed".into())))
}

async fn push_bucket_once(
    endpoint: String,
    stage_id: u32,
    target_partition: u32,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<()> {
    let header = exchange_header_frame(stage_id, target_partition);
    let mut frames = vec![header];
    let batches = chunk_batches_for_flight(batches);
    let input = futures::stream::iter(batches.into_iter().map(Ok::<_, FlightError>));
    let mut encoded = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(input);
    while let Some(frame) = encoded.next().await {
        frames.push(frame.map_err(|e| Error::Execution(format!("flight encode: {e}")))?);
    }

    let mut client = connect_flight(endpoint).await?;
    let mut stream = client
        .do_exchange(futures::stream::iter(frames))
        .await
        .map_err(|e| Error::Execution(format!("do_exchange: {}", status_detail(&e))))?
        .into_inner();
    while let Some(item) = stream.next().await {
        item.map_err(|e| Error::Execution(format!("do_exchange stream: {e}")))?;
    }
    Ok(())
}

fn exchange_header_frame(stage_id: u32, partition_id: u32) -> FlightData {
    let header = ShuffleExchangeHeader {
        stage_id,
        partition_id,
    }
    .encode();
    FlightData {
        flight_descriptor: None,
        data_header: Vec::new().into(),
        app_metadata: header.to_vec().into(),
        data_body: Vec::new().into(),
    }
}

fn is_pull_retryable(err: &Error) -> bool {
    let s = err.to_string().to_ascii_lowercase();
    s.contains("connect")
        || s.contains("unavailable")
        || s.contains("transport")
        || s.contains("goaway")
        || s.contains("incomplete message")
        || s.contains("do_get")
        || s.contains("do_exchange")
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidant_loom::arrow::array::{Int32Array, Int64Array};
    use oxidant_loom::arrow::datatypes::{DataType, Field};

    #[test]
    fn align_batch_schema_erases_nullability_drift() {
        // KAN-39: a typed zero-row placeholder (planned schema, nullable) and a data batch
        // (execution-stream schema, non-nullable) must land in one MemTable without
        // "Mismatch between schema and batches".
        let declared = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let data_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let placeholder = RecordBatch::new_empty(declared.clone());
        let data = RecordBatch::try_new(
            data_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![3, 4])),
            ],
        )
        .unwrap();
        let aligned = [&placeholder, &data]
            .into_iter()
            .map(|b| align_batch_schema(b, &declared).unwrap_or_else(|| b.clone()))
            .collect::<Vec<_>>();
        // Both batches now conform: MemTable construction (the `register_batches` path)
        // accepts them.
        datafusion::datasource::MemTable::try_new(declared, vec![aligned])
            .expect("drifted batches must align onto the declared schema");
    }

    #[test]
    fn align_batch_schema_rejects_genuine_mismatch() {
        let declared = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let foreign = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("b", DataType::Utf8, false)])),
            vec![Arc::new(oxidant_loom::arrow::array::StringArray::from(
                vec!["x"],
            ))],
        )
        .unwrap();
        assert!(align_batch_schema(&foreign, &declared).is_none());
    }

    #[test]
    fn worker_enables_shuffle_spill_from_memory_limit_env() {
        // Use a budget large enough that a racing Engine::new() in another test is not
        // starved (4 KiB previously poisoned parallel DF pools). Spill policy still enables.
        std::env::set_var("OXIDANT_MEMORY_LIMIT_BYTES", "67108864");
        let worker = Worker::new(Arc::new(Engine::new()));
        assert!(worker.spill_enabled());
        std::env::remove_var("OXIDANT_MEMORY_LIMIT_BYTES");
    }

    #[test]
    fn worker_enables_shuffle_spill_from_auto_sized_budget() {
        // Unset memory env → resolve_memory_pool_bytes auto-sizes from host RAM, and
        // SpillStore::from_env must pick that up (SF100 default path).
        std::env::remove_var("OXIDANT_MEMORY_LIMIT_BYTES");
        std::env::remove_var("OXIDANT_SHUFFLE_SPILL_BYTES");
        std::env::remove_var("OXIDANT_SHUFFLE_SPILL_DIR");
        assert!(oxidant_loom::resolve_memory_pool_bytes().is_some());
        let worker = Worker::new(Arc::new(Engine::new()));
        assert!(worker.spill_enabled());
    }

    #[test]
    fn heartbeat_payload_parses_slots() {
        let heartbeat =
            parse_heartbeat_payload(br#"{"ok":true,"slots_total":4,"slots_used":2}"#).unwrap();
        assert_eq!(heartbeat.slots_total, Some(4));
        assert_eq!(heartbeat.slots_used, Some(2));
        assert!(heartbeat.has_available_slot());

        let full =
            parse_heartbeat_payload(br#"{"ok":true,"slots_total":4,"slots_used":4}"#).unwrap();
        assert!(!full.has_available_slot());
    }

    #[test]
    fn ok_heartbeat_is_backward_compatible() {
        let heartbeat = parse_heartbeat_bodies(&[b"ok".to_vec()]);
        assert_eq!(heartbeat, WorkerHeartbeat::default());
        assert!(heartbeat.has_available_slot());
    }

    #[tokio::test]
    async fn distributed_single_stage_roundtrip() {
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let engine = Arc::new(Engine::new());
        tokio::spawn(async move {
            let _ = serve_worker(port, engine).await;
        });
        // Retry until the worker is up and the distributed query returns.
        let endpoint = format!("http://127.0.0.1:{port}");
        let mut batches = None;
        for _ in 0..50 {
            match query_worker(endpoint.clone(), "SELECT 21 + 21 AS answer").await {
                Ok(b) => {
                    batches = Some(b);
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
            }
        }
        let batches = batches.expect("worker did not become ready / query failed");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);
        // `21 + 21` is Spark `IntegerType` (Int32) — oxidant types integer literals as Int32 to match
        // Spark (real PySpark `SELECT 21 + 21` → IntegerType), not DataFusion's native i64.
        let v = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32")
            .value(0);
        assert_eq!(v, 42);
    }

    #[tokio::test]
    async fn stage_ticket_runs_as_leaf() {
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let engine = Arc::new(Engine::new());
        tokio::spawn(async move {
            let _ = serve_worker(port, engine).await;
        });
        let endpoint = format!("http://127.0.0.1:{port}");
        // A leaf stage caches and returns empty; assert it doesn't error and returns 0 rows.
        let ticket = StageTicket {
            stage_id: 0,
            partition_id: 0,
            num_partitions: 1,
            upstream_endpoints: vec![],
            stage_sql: "SELECT 1 AS k, 2 AS v".into(),
            plan_fragment: vec![],
            hash_key_cols: vec![0],
            upstream_stage_ids: vec![],
            produce: true,
            lakehouse_snapshot_pins: String::new(),
            replicated_tables: String::new(),
            coalesce_read_modulus: 0,
            forward_upstream_stage_ids: vec![],
            upstream_bucket_rows: vec![],
        };
        let mut out = None;
        for _ in 0..50 {
            match run_stage_on_worker(endpoint.clone(), ticket.clone()).await {
                Ok(b) => {
                    out = Some(b);
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
            }
        }
        let out = out.expect("worker not ready");
        assert_eq!(out.iter().map(|b| b.num_rows()).sum::<usize>(), 0);

        // The cached bucket 0 should now be pullable and contain the row.
        let pulled = pull_bucket(endpoint, 0, 0).await.unwrap();
        assert_eq!(pulled.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }

    #[tokio::test]
    async fn shuffle_pull_is_non_destructive() {
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let engine = Arc::new(Engine::new());
        tokio::spawn(async move {
            let _ = serve_worker(port, engine).await;
        });
        let endpoint = format!("http://127.0.0.1:{port}");
        let ticket = StageTicket {
            stage_id: 42,
            partition_id: 0,
            num_partitions: 1,
            upstream_endpoints: vec![],
            stage_sql: "SELECT 7 AS k, 9 AS v".into(),
            plan_fragment: vec![],
            hash_key_cols: vec![0],
            upstream_stage_ids: vec![],
            produce: true,
            lakehouse_snapshot_pins: String::new(),
            replicated_tables: String::new(),
            coalesce_read_modulus: 0,
            forward_upstream_stage_ids: vec![],
            upstream_bucket_rows: vec![],
        };
        for _ in 0..50 {
            if run_stage_on_worker(endpoint.clone(), ticket.clone())
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let first = pull_bucket(endpoint.clone(), 42, 0).await.unwrap();
        let second = pull_bucket(endpoint, 42, 0).await.unwrap();
        assert_eq!(
            first.iter().map(|b| b.num_rows()).sum::<usize>(),
            1,
            "first AQE-style sample pull"
        );
        assert_eq!(
            second.iter().map(|b| b.num_rows()).sum::<usize>(),
            1,
            "second pull must still see the bucket (sampling is non-destructive)"
        );
    }

    /// KAN-18: the worker-side TTL lazily sweeps expired stage-output entries on insert, so a
    /// crashed or unreachable driver can't pin worker memory forever.
    #[tokio::test]
    async fn stage_output_ttl_sweeps_expired_entries() {
        let mut worker = Worker::new(Arc::new(Engine::new()));
        worker.stage_output_ttl = std::time::Duration::from_millis(50);
        let ticket = |stage_id: u32| StageTicket {
            stage_id,
            partition_id: 0,
            num_partitions: 1,
            upstream_endpoints: vec![],
            stage_sql: "SELECT 1 AS k, 2 AS v".into(),
            plan_fragment: vec![],
            hash_key_cols: vec![0],
            upstream_stage_ids: vec![],
            produce: true,
            lakehouse_snapshot_pins: String::new(),
            replicated_tables: String::new(),
            coalesce_read_modulus: 0,
            forward_upstream_stage_ids: vec![],
            upstream_bucket_rows: vec![],
        };
        worker
            .run_stage(ticket(910), &StageProgress::default())
            .await
            .unwrap();
        assert!(worker
            .stage_outputs
            .lock()
            .expect("stage cache poisoned")
            .contains_key(&(910, 0)));

        // Let stage 910 age past the TTL; the next insert's lazy sweep evicts it but keeps 911.
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
        worker
            .run_stage(ticket(911), &StageProgress::default())
            .await
            .unwrap();
        let guard = worker.stage_outputs.lock().expect("stage cache poisoned");
        assert!(
            !guard.contains_key(&(910, 0)),
            "expired stage output not swept"
        );
        assert!(
            guard.contains_key(&(911, 0)),
            "fresh stage output wrongly swept"
        );
    }

    #[tokio::test]
    async fn empty_shuffle_bucket_carries_producer_schema() {
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let engine = Arc::new(Engine::new());
        tokio::spawn(async move {
            let _ = serve_worker(port, engine).await;
        });
        let endpoint = format!("http://127.0.0.1:{port}");
        // One row hashes into exactly one of three buckets; the other two are empty but must still
        // expose the producer schema so consumers can plan `SELECT k FROM shuffle_input`.
        let ticket = StageTicket {
            stage_id: 7,
            partition_id: 0,
            num_partitions: 3,
            upstream_endpoints: vec![],
            stage_sql: "SELECT 1 AS k, 2 AS v".into(),
            plan_fragment: vec![],
            hash_key_cols: vec![0],
            upstream_stage_ids: vec![],
            produce: true,
            lakehouse_snapshot_pins: String::new(),
            replicated_tables: String::new(),
            coalesce_read_modulus: 0,
            forward_upstream_stage_ids: vec![],
            upstream_bucket_rows: vec![],
        };
        for _ in 0..50 {
            if run_stage_on_worker(endpoint.clone(), ticket.clone())
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let mut empty_bucket = None;
        for bucket in [1u32, 2] {
            for _ in 0..50 {
                match pull_bucket(endpoint.clone(), 7, bucket).await {
                    Ok(b) if !b.is_empty() => {
                        empty_bucket = Some((bucket, b));
                        break;
                    }
                    Ok(_) => {}
                    Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
                }
            }
            if empty_bucket.is_some() {
                break;
            }
        }
        let (bucket, batches) = empty_bucket.expect("expected an empty typed bucket");
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
        assert_eq!(batches[0].schema().field(0).name(), "k");

        // Consumer over an empty upstream bucket must still resolve column names.
        let consume = StageTicket {
            stage_id: 8,
            partition_id: bucket,
            num_partitions: 3,
            upstream_endpoints: vec![endpoint.clone()],
            stage_sql: "SELECT k, sum(v) AS s FROM shuffle_input GROUP BY k".into(),
            plan_fragment: vec![],
            hash_key_cols: vec![],
            upstream_stage_ids: vec![7],
            produce: false,
            lakehouse_snapshot_pins: String::new(),
            replicated_tables: String::new(),
            coalesce_read_modulus: 0,
            forward_upstream_stage_ids: vec![],
            upstream_bucket_rows: vec![],
        };
        let mut out = None;
        for _ in 0..50 {
            match run_stage_on_worker(endpoint.clone(), consume.clone()).await {
                Ok(b) => {
                    out = Some(b);
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
            }
        }
        out.expect("consumer over empty typed bucket should plan and run");
    }

    /// AQE-coalesced read at the worker: a consumer ticket carrying `coalesce_read_modulus = m`
    /// pulls its whole modulus class (`p, p+m, …`) of each upstream; across the `m` dispatched
    /// readers every producer bucket must be read exactly once.
    #[tokio::test]
    async fn coalesced_consumer_reads_every_bucket_once() {
        let bind = || {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let (p0, p1) = (bind(), bind());
        // Worker 0 holds keys 0..8, worker 1 keys 100..108 — 16 rows total, hash-partitioned
        // into 4 buckets on each worker.
        let make_engine = |start: i64, end: i64| {
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("v", DataType::Int64, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from((start..end).collect::<Vec<_>>())),
                    Arc::new(Int64Array::from((start..end).collect::<Vec<_>>())),
                ],
            )
            .unwrap();
            let engine = Arc::new(Engine::new());
            engine.register_batches("t", vec![batch]).unwrap();
            engine
        };
        tokio::spawn(async move {
            let _ = serve_worker(p0, make_engine(0, 8)).await;
        });
        tokio::spawn(async move {
            let _ = serve_worker(p1, make_engine(100, 108)).await;
        });
        let endpoints: Vec<String> = [p0, p1]
            .iter()
            .map(|p| format!("http://127.0.0.1:{p}"))
            .collect();

        // Leaf producer stage 7 on both workers: `SELECT k, v FROM t` into 4 buckets.
        let producer = |partition_id: u32| StageTicket {
            stage_id: 7,
            partition_id,
            num_partitions: 4,
            upstream_endpoints: vec![],
            stage_sql: "SELECT k, v FROM t".into(),
            plan_fragment: vec![],
            hash_key_cols: vec![0],
            upstream_stage_ids: vec![],
            produce: true,
            lakehouse_snapshot_pins: String::new(),
            replicated_tables: String::new(),
            coalesce_read_modulus: 0,
            forward_upstream_stage_ids: vec![],
            upstream_bucket_rows: vec![],
        };
        for (i, ep) in endpoints.iter().enumerate() {
            for _ in 0..50 {
                if run_stage_on_worker(ep.clone(), producer(i as u32))
                    .await
                    .is_ok()
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }

        // Coalesced consumer stage 8: modulus 2 over 4 buckets — reader 0 pulls buckets
        // {0, 2}, reader 1 pulls {1, 3}, each from BOTH workers.
        let mut seen: Vec<i64> = Vec::new();
        for p in 0..2u32 {
            let consume = StageTicket {
                stage_id: 8,
                partition_id: p,
                num_partitions: 4,
                upstream_endpoints: endpoints.clone(),
                stage_sql: "SELECT k, v FROM shuffle_input".into(),
                plan_fragment: vec![],
                hash_key_cols: vec![],
                upstream_stage_ids: vec![7],
                produce: false,
                lakehouse_snapshot_pins: String::new(),
                replicated_tables: String::new(),
                coalesce_read_modulus: 2,
                forward_upstream_stage_ids: vec![],
                upstream_bucket_rows: vec![],
            };
            // Retry transport/connect races the same way the producer boot loop does —
            // under a full workspace suite the peer can flap once between produce and consume.
            let mut out = None;
            let mut last = None;
            for _ in 0..50 {
                match run_stage_on_worker(endpoints[0].clone(), consume.clone()).await {
                    Ok(b) => {
                        out = Some(b);
                        break;
                    }
                    Err(e) => {
                        let s = e.to_string().to_ascii_lowercase();
                        if s.contains("connect")
                            || s.contains("transport")
                            || s.contains("unavailable")
                        {
                            last = Some(e);
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                            continue;
                        }
                        panic!("coalesced consumer run: {e}");
                    }
                }
            }
            let out = out.unwrap_or_else(|| {
                panic!(
                    "coalesced consumer run: {}",
                    last.map(|e| e.to_string())
                        .unwrap_or_else(|| "no attempts".into())
                )
            });
            for b in &out {
                let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                seen.extend((0..k.len()).map(|i| k.value(i)));
            }
        }
        seen.sort_unstable();
        let expected: Vec<i64> = (0..8).chain(100..108).collect();
        assert_eq!(
            seen, expected,
            "every bucket read exactly once across readers"
        );
    }

    #[tokio::test]
    async fn do_exchange_pushes_partition_then_pull_reads_it() {
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let engine = Arc::new(Engine::new());
        tokio::spawn(async move {
            let _ = serve_worker(port, engine).await;
        });
        let endpoint = format!("http://127.0.0.1:{port}");
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
        )
        .unwrap();

        let mut pushed = false;
        for _ in 0..50 {
            match push_bucket_with_schema(
                endpoint.clone(),
                99,
                2,
                schema.clone(),
                vec![batch.clone()],
            )
            .await
            {
                Ok(()) => {
                    pushed = true;
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
            }
        }
        assert!(pushed, "worker did not accept do_exchange push");

        let pulled = pull_bucket(endpoint, 99, 2).await.unwrap();
        assert_eq!(pulled.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
        let values = pulled[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.value(0), 10);
        assert_eq!(values.value(2), 30);
    }

    /// F3: Flight channels are pooled per endpoint — repeat connects hand back clones of
    /// the one HTTP/2 connection instead of re-dialing — and a transport-class failure
    /// evicts the pooled channel so the next RPC dials fresh (tonic never redials an
    /// eagerly-connected `Channel`, so without eviction a worker restart would wedge
    /// every later RPC to that endpoint).
    #[tokio::test]
    async fn flight_channel_pool_reuses_then_evicts() {
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let engine = Arc::new(Engine::new());
        tokio::spawn(async move {
            let _ = serve_worker(port, engine).await;
        });
        let endpoint = format!("http://127.0.0.1:{port}");
        evict_flight_channel(&endpoint); // clean slate: another test may have used this port

        // Retry the first connect until the spawned server is accepting.
        let mut connected = false;
        for _ in 0..50 {
            if connect_flight(endpoint.clone()).await.is_ok() {
                connected = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(connected, "worker did not accept connections");

        // A second connect must be a pool hit — the endpoint stays cached exactly once.
        connect_flight(endpoint.clone()).await.unwrap();
        assert!(
            flight_channels().lock().unwrap().contains_key(&endpoint),
            "channel must be pooled for reuse"
        );

        // Eviction drops it; the next connect re-dials and re-pools.
        evict_flight_channel(&endpoint);
        assert!(
            !flight_channels().lock().unwrap().contains_key(&endpoint),
            "eviction must drop the pooled channel"
        );
        connect_flight(endpoint.clone()).await.unwrap();
        assert!(
            flight_channels().lock().unwrap().contains_key(&endpoint),
            "reconnect after eviction must re-dial and re-pool"
        );
        // Leave no stale channel behind for this test's now-doomed server.
        evict_flight_channel(&endpoint);
    }

    fn int64_batch(schema: &SchemaRef, start: i64, len: i64) -> RecordBatch {
        let values: Vec<i64> = (start..start + len).collect();
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    async fn push_batches_stream(
        endpoint: String,
        stage_id: u32,
        partition_id: u32,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> std::result::Result<(), oxidant_common::Error> {
        let header = exchange_header_frame(stage_id, partition_id);
        let mut frames = vec![header];
        let batches = chunk_batches_for_flight(batches);
        let input = futures::stream::iter(batches.into_iter().map(Ok::<_, FlightError>));
        let mut encoded = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(input);
        while let Some(frame) = encoded.next().await {
            frames
                .push(frame.map_err(|e| {
                    oxidant_common::Error::Execution(format!("flight encode: {e}"))
                })?);
        }

        let mut client = connect_flight(endpoint).await?;
        let mut stream = client
            .do_exchange(futures::stream::iter(frames))
            .await
            .map_err(|e| {
                oxidant_common::Error::Execution(format!("do_exchange: {}", status_detail(&e)))
            })?
            .into_inner();
        while let Some(item) = stream.next().await {
            item.map_err(|e| oxidant_common::Error::Execution(format!("do_exchange stream: {e}")))?;
        }
        Ok(())
    }

    /// KAN-6: payloads above tonic's default 4 MiB decode limit must round-trip via Flight.
    #[tokio::test]
    async fn flight_round_trips_payload_over_default_4mb_limit() {
        use oxidant_loom::arrow::array::StringArray;

        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let engine = Arc::new(Engine::new());
        tokio::spawn(async move {
            let _ = serve_worker(port, engine).await;
        });
        let endpoint = format!("http://127.0.0.1:{port}");
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));

        // One ~5.5 MiB string batch — above tonic's 4 MiB default decode limit.
        let blob = "x".repeat(5 * 1024 * 1024 + 512 * 1024);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![blob.as_str()]))],
        )
        .unwrap();
        let expected_bytes = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .len();
        assert!(
            expected_bytes > 4 * 1024 * 1024,
            "fixture must exceed tonic's default 4 MiB decode limit"
        );

        let mut pushed = false;
        for _ in 0..50 {
            match push_batches_stream(
                endpoint.clone(),
                650,
                0,
                schema.clone(),
                vec![batch.clone()],
            )
            .await
            {
                Ok(()) => {
                    pushed = true;
                    break;
                }
                Err(e) => {
                    let msg = e.to_string();
                    assert!(
                        !msg.contains("decoded message length too large"),
                        "KAN-6: Flight push hit 4 MiB limit: {msg}"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
        assert!(pushed, "worker did not accept >4 MiB do_exchange push");

        let pulled = pull_bucket(endpoint, 650, 0)
            .await
            .expect("pull >4 MiB bucket");
        assert_eq!(pulled.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        let got = pulled[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(got.len(), expected_bytes);
    }

    #[tokio::test]
    async fn do_exchange_streams_large_partition_under_memory_budget() {
        let spill_dir =
            std::env::temp_dir().join(format!("oxidant-xchg-spill-{}", std::process::id()));
        std::env::remove_var("OXIDANT_SHUFFLE_SPILL_DIR");
        let _ = std::fs::remove_dir_all(&spill_dir);

        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let engine = Arc::new(Engine::new());
        tokio::spawn(async move {
            // Small shuffle cache budget forces spill while batches stream in.
            std::env::set_var("OXIDANT_SHUFFLE_SPILL_BYTES", "8192");
            let _ = serve_worker(port, engine).await;
        });
        let endpoint = format!("http://127.0.0.1:{port}");
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));

        // ~100 rows per batch × 80 batches — far above 8 KiB if buffered entirely in memory.
        let batches: Vec<RecordBatch> = (0..80)
            .map(|i| int64_batch(&schema, i * 100, 100))
            .collect();
        let expected_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        let mut pushed = false;
        for _ in 0..50 {
            match push_batches_stream(endpoint.clone(), 501, 0, schema.clone(), batches.clone())
                .await
            {
                Ok(()) => {
                    pushed = true;
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
            }
        }
        assert!(pushed, "worker did not accept streaming do_exchange push");

        let pulled = pull_bucket(endpoint, 501, 0).await.unwrap();
        assert_eq!(
            pulled.iter().map(|b| b.num_rows()).sum::<usize>(),
            expected_rows
        );

        // SpillStore creates a temp dir when only OXIDANT_SHUFFLE_SPILL_BYTES is set; verify spill
        // files exist under the default oxidant-shuffle-spill prefix. do_exchange pushes land in
        // the PUSH_SRC (u32::MAX) producer scope.
        let parent = std::env::temp_dir().join("oxidant-shuffle-spill");
        let spilled = std::fs::read_dir(&parent)
            .map(|entries| {
                entries.flatten().any(|ent| {
                    std::fs::read_dir(ent.path())
                        .map(|sub| {
                            sub.flatten().any(|f| {
                                f.file_name()
                                    .to_string_lossy()
                                    .starts_with("stage_501_src4294967295_part_0")
                            })
                        })
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false);
        assert!(
            spilled,
            "expected spilled shuffle file for stage 501 partition 0"
        );

        std::env::remove_var("OXIDANT_SHUFFLE_SPILL_BYTES");
    }

    #[tokio::test]
    async fn watchdog_fires_when_all_signals_freeze() {
        // Every signal frozen from the start: the watchdog must fire after ~budget and
        // stamp the abort age for the summary line (KAN-47).
        let sample = || ProgressSample {
            batches: 0,
            pool_activity_ms: 0,
            spill_bytes: 0,
        };
        let progress = Arc::new(StageProgress::default());
        let msg = watch_stage_progress(
            sample,
            std::time::Duration::from_millis(10),
            std::time::Duration::from_millis(100),
            7,
            progress.clone(),
        )
        .await;
        assert!(
            msg.contains("stage 7 made no progress"),
            "expected actionable no-progress message, got: {msg}"
        );
        assert!(msg.contains("KAN-47"), "message should name the fix: {msg}");
        assert!(
            progress.no_progress_age().is_some(),
            "watchdog must stamp the abort age"
        );
    }

    #[tokio::test]
    async fn watchdog_stays_quiet_while_any_signal_advances() {
        // A slowly advancing heartbeat: gaps far below the budget, but the stage runs for
        // several budget lengths — the watchdog must never fire (KAN-47).
        let ticks = Arc::new(AtomicU64::new(0));
        let sampler_ticks = ticks.clone();
        let sample = move || ProgressSample {
            batches: sampler_ticks.load(Ordering::Relaxed),
            pool_activity_ms: 0,
            spill_bytes: 0,
        };
        let progress = Arc::new(StageProgress::default());
        let watchdog = watch_stage_progress(
            sample,
            std::time::Duration::from_millis(10),
            std::time::Duration::from_millis(150),
            7,
            progress,
        );
        let advance = async move {
            for _ in 0..20 {
                ticks.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        };
        tokio::select! {
            msg = watchdog => panic!("watchdog fired on a progressing stage: {msg}"),
            _ = advance => {}
        }
    }

    #[test]
    fn no_progress_abort_detection_is_specific() {
        // The KAN-53 stall-retry fires only on the KAN-47 watchdog abort — never on a
        // driver cancel or a wall-clock timeout, which stay final.
        assert!(is_no_progress_abort(&Status::aborted(
            "stage 7 made no progress for 600 s (… KAN-47 …)"
        )));
        assert!(!is_no_progress_abort(&Status::aborted(
            "stage cancelled by driver"
        )));
        assert!(!is_no_progress_abort(&Status::resource_exhausted(
            "stage timed out after 600000 ms (OXIDANT_STAGE_TIMEOUT_MS)"
        )));
        assert!(!is_no_progress_abort(&Status::internal("boom")));
    }

    #[test]
    fn stage_summary_line_reports_progress_counters() {
        let progress = StageProgress::default();
        for _ in 0..3 {
            progress.note_batch();
        }
        progress.note_no_progress_abort(std::time::Duration::from_millis(2500));
        let line = stage_summary_line(
            4,
            2,
            8,
            &progress,
            1024,
            std::time::Duration::from_millis(1234),
            "error",
        );
        for needle in [
            "stage_id=4",
            "partition_id=2",
            "num_partitions=8",
            "batches=3",
            "spill_bytes=1024",
            "duration_ms=1234",
            "status=error",
            "last_progress_age_ms=2500",
        ] {
            assert!(
                line.contains(needle),
                "summary line missing {needle}: {line}"
            );
        }
        // Without a watchdog abort the age field is omitted entirely.
        let clean = stage_summary_line(
            4,
            2,
            8,
            &StageProgress::default(),
            0,
            std::time::Duration::from_millis(5),
            "ok",
        );
        assert!(!clean.contains("last_progress_age_ms"), "{clean}");
    }
}
