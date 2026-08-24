//! REST statement-execution API + catalog + cluster status, served from the driver's UI HTTP port.
//!
//! Merged into the `oxidant-ui-server` router by [`crate::serve`] (via
//! `UiServerConfig::merge_router`), so the same axum listener that hosts the monitoring UI
//! also exposes a Livy-style SQL statement API:
//!
//! - `POST /api/v1/statements` (`?wait=true&timeout=<secs>`) — submit SQL for execution.
//! - `GET  /api/v1/statements` — newest-first statement list (cap 100).
//! - `GET  /api/v1/statements/{id}` — status / schema / row count / error for one statement.
//! - `GET  /api/v1/statements/{id}/result?format=json|csv&limit=N` — result rows.
//! - `POST /api/v1/statements/{id}/cancel` — best-effort cancellation.
//! - `GET  /api/v1/catalogs` — list catalogs.
//! - `GET  /api/v1/catalogs/{catalog}/namespaces` — list databases/schemas.
//! - `GET  /api/v1/catalogs/{catalog}/tables?namespace=...` — list tables.
//! - `GET  /api/v1/catalogs/{catalog}/tables/{table}/columns?namespace=...` — list columns.
//! - `GET  /api/v1/catalogs/autocomplete?prefix=...` — catalog/schema/table/column suggestions.
//! - `GET  /api/v1/cluster/status` — single-node / local-cluster / distributed + workers + process metrics.
//! - `GET /api/v1/logs` — recent process log lines (in-memory ring buffer). **Bearer token
//!   required** — the same `OXIDANT_STATUS_TOKEN` that gates `/api/status` and the pipeline
//!   routes, checked by the same code
//!   ([`oxidant_ui_server::status::deny_unless_authorized`]). The buffer captures every
//!   `tracing` event at every enabled level, field values included, so it names hosts, slots,
//!   tables and query text; and this router is served under a permissive CORS layer, which
//!   made an ungated buffer readable cross-site by any origin an operator's browser visits.
//!   Unset token, `404`: the route does not exist, exactly like `/api/status`.
//!
//! Statements execute through [`OxidantService::execute_sql`], i.e. the exact `Sql`-relation
//! arm of the gRPC path: same distributed routing, same observability hooks (statements show
//! up on the monitoring /sql page).

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use datafusion::arrow::json::{ArrayWriter, WriterBuilder};
use oxidant_catalog::DEFAULT_CATALOG;
use oxidant_loom::arrow::array::Array;
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;
use serde::Deserialize;
use serde_json::{json, Value};
use sysinfo::{Pid, System};
use tokio::sync::{watch, Notify};
use tracing::Subscriber;
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use tracing_subscriber::Layer;
use uuid::Uuid;

use crate::history::{
    disk, now_rfc3339, rfc3339_from_ms, FoldedStatement, HistoryConfig, HistoryRuntime,
    JournalRecord, RecordKind, ResultPersist, ResultPointer, Source, SpillJob, SpillOutcome,
    SqlMode, StatementStatus, RECORD_VERSION, RESULT_TOO_LARGE,
};
use crate::OxidantService;

/// Retention for a statement (result batches included); mirrors the 1h default of the gRPC
/// reattach buffer (`completed_ops_ttl`).
const STATEMENT_TTL: Duration = Duration::from_secs(3600);
/// Count cap on retained statements; the oldest entries are evicted first.
const MAX_STATEMENTS: usize = 1000;
/// Statements returned by `GET /api/v1/statements` (newest first).
const LIST_CAP: usize = 100;
/// Default `limit` for the result endpoint.
const DEFAULT_RESULT_LIMIT: usize = 10_000;
/// Default `?wait=true` blocking timeout (seconds).
const DEFAULT_WAIT_TIMEOUT_SECS: u64 = 30;
/// Max retained log lines served by `GET /api/v1/logs`.
const MAX_LOG_LINES: usize = 1000;
/// Minimum wall-clock gap between two full retention passes over the history tier.
const SWEEP_INTERVAL_MS: i64 = 60_000;

/// In-memory ring buffer of recent log lines shared by the tracing layer and the logs endpoint.
#[derive(Clone)]
pub struct LogBuffer {
    inner: Arc<Mutex<Vec<String>>>,
    cap: usize,
}

impl LogBuffer {
    fn new(cap: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::with_capacity(cap))),
            cap,
        }
    }

    fn push(&self, line: String) {
        let mut inner = self.inner.lock().expect("log buffer poisoned");
        if inner.len() >= self.cap {
            inner.remove(0);
        }
        inner.push(line);
    }

    fn lines(&self) -> Vec<String> {
        self.inner.lock().expect("log buffer poisoned").clone()
    }
}

impl<S: Subscriber> Layer<S> for LogBuffer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        self.push(format_event(event));
    }
}

fn format_event(event: &tracing::Event<'_>) -> String {
    let mut visitor = LogVisitor(String::new());
    event.record(&mut visitor);
    let meta = event.metadata();
    if visitor.0.is_empty() {
        format!("[{}] {}", meta.level(), meta.target())
    } else {
        format!("[{}] {} - {}", meta.level(), meta.target(), visitor.0)
    }
}

struct LogVisitor(String);

impl LogVisitor {
    fn push_field(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Display) {
        if !self.0.is_empty() {
            self.0.push_str(", ");
        }
        self.0.push_str(field.name());
        self.0.push('=');
        self.0.push_str(&value.to_string());
    }
}

impl tracing::field::Visit for LogVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.push_field(field, &format!("{:?}", value));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.push_field(field, &format!("{:?}", value));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.push_field(field, &value);
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.push_field(field, &value);
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.push_field(field, &value);
    }

    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        self.push_field(field, &value);
    }
}

static LOG_BUFFER: OnceLock<LogBuffer> = OnceLock::new();

/// Initialize process-wide log capture into an in-memory ring buffer and a compact
/// `tracing` fmt subscriber. Idempotent: subsequent calls are ignored.
pub fn init_logging() {
    let buffer = LOG_BUFFER
        .get_or_init(|| LogBuffer::new(MAX_LOG_LINES))
        .clone();
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_max_level(tracing::Level::INFO)
        .finish()
        .with(buffer)
        .try_init();
}

/// Cached sysinfo `System` for process metrics. Kept in a `Mutex` so successive
/// `refresh_process_specifics` calls can compute a meaningful CPU percentage.
static SYSTEM: OnceLock<std::sync::Mutex<System>> = OnceLock::new();

fn system_snapshot() -> System {
    let mut sys = System::new_all();
    sys.refresh_all();
    sys
}

/// One tracked statement: request text, lifecycle state, and the retained result batches the
/// result endpoint serves from.
struct Statement {
    sql: String,
    status: StatementStatus,
    error: Option<String>,
    /// `{"name","type"}` pairs of the result schema (Arrow type names via `Display`).
    schema: Option<Vec<(String, String)>>,
    row_count: Option<usize>,
    batches: Vec<RecordBatch>,
    submitted_at_ms: i64,
    /// Monotonic submit instant backing `duration_ms`.
    ///
    /// Live duration only. Age arithmetic uses `submitted_at_ms`, because `Instant` has no epoch
    /// and cannot be reconstructed for a statement that came off the journal 30 days later.
    submitted: std::time::Instant,
    duration_ms: Option<i64>,
    /// Signals the execution task to drop the query future (best-effort cancel).
    cancel: watch::Sender<bool>,
    /// Insertion order; drives oldest-first eviction and newest-first listing. Shared with the
    /// journal's sequence, so a replayed statement and a live one sort against each other.
    seq: u64,
    /// Where the statement was submitted from — `rest` or `connect` (#134).
    source: Source,
    /// The Connect session, when there is one. Half of the `client_op_id` alias key.
    session: Option<String>,
    /// The client's own operation id, validated. Never used as a path or a fold key.
    client_op_id: Option<String>,
    /// Fires when this statement's terminal record is fsynced; taken by whoever answers the
    /// client, at most once.
    durable_ack: Option<tokio::sync::oneshot::Receiver<()>>,
    /// Can `/result` still answer from [`Self::batches`]?
    ///
    /// False for the Connect path (its batches are already streaming to the gRPC client as Arrow
    /// IPC and the store deliberately keeps no second copy) and false once the in-memory result
    /// budget has released the rows to `results/<id>.arrow`. Without it the statement is hot,
    /// succeeded, and has an empty `batches` — so the result endpoint answered
    /// `200 {"rows":[]}` for a query whose own status document said `rowCount: 5`.
    rows_in_memory: bool,
    /// What [`Self::batches`] costs the store's in-memory result budget (§5, F8). Zero whenever
    /// the rows are not here.
    result_bytes: u64,
    /// The spilled result file, once one is durable (§5).
    result_file: Option<ResultPointer>,
    /// Why there is none — [`RESULT_TOO_LARGE`].
    result_refused: Option<String>,
    /// A spill is in flight for this statement; the budget must not pick it as a victim twice.
    spilling: bool,
    /// Release the in-memory rows the moment the spill lands — the `on_pressure` path. `always`
    /// spills without releasing, and lets the budget decide later.
    release_on_spill: bool,
}

impl Statement {
    fn snapshot(&self, id: &str) -> StatementSnapshot {
        StatementSnapshot {
            id: id.to_string(),
            sql: self.sql.clone(),
            status: self.status,
            error: self.error.clone(),
            schema: self.schema.clone(),
            row_count: self.row_count,
            submitted_at_ms: self.submitted_at_ms,
            duration_ms: self.duration_ms,
            source: self.source,
            client_op_id: self.client_op_id.clone(),
            tier: Tier::Hot,
            result_refused: self.result_refused.clone(),
        }
    }

    /// The journal record for this statement's current (terminal) state — self-contained, so
    /// compaction can drop everything older without losing it.
    fn to_folded(&self, id: &str, sql_mode: SqlMode, last_seq: u64) -> FoldedStatement {
        FoldedStatement {
            id: id.to_string(),
            client_op_id: self.client_op_id.clone(),
            session: self.session.clone(),
            source: self.source,
            sql: sql_mode.encode(&self.sql),
            sql_encoding: sql_mode.as_str().to_string(),
            status: self.status,
            error: self.error.clone(),
            schema: self.schema.clone(),
            rows: self.row_count.map(|r| r as u64),
            submitted_at_ms: self.submitted_at_ms,
            duration_ms: self.duration_ms,
            result: self.result_file.clone(),
            result_refused: self.result_refused.clone(),
            seq: self.seq,
            last_seq,
            rank: RecordKind::Snapshot.rank(),
        }
    }
}

/// Which tier answered a read: `hot` is live and cancellable, `history` is archival.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tier {
    Hot,
    History,
}

impl Tier {
    fn as_str(self) -> &'static str {
        match self {
            Self::Hot => "hot",
            Self::History => "history",
        }
    }
}

/// A point-in-time copy of a statement's public state (no result batches).
#[derive(Clone)]
pub(crate) struct StatementSnapshot {
    id: String,
    sql: String,
    pub(crate) status: StatementStatus,
    pub(crate) error: Option<String>,
    schema: Option<Vec<(String, String)>>,
    row_count: Option<usize>,
    submitted_at_ms: i64,
    duration_ms: Option<i64>,
    source: Source,
    client_op_id: Option<String>,
    tier: Tier,
    /// Why there is no spilled result — [`RESULT_TOO_LARGE`]. Surfaced on the status document so
    /// a client can tell "past the size cap" from "aged out".
    result_refused: Option<String>,
}

impl StatementSnapshot {
    fn from_history(st: &FoldedStatement) -> Self {
        Self {
            id: st.id.clone(),
            sql: st.sql.clone(),
            status: st.status,
            error: st.error.clone(),
            schema: st.schema.clone(),
            row_count: st.rows.map(|r| r as usize),
            submitted_at_ms: st.submitted_at_ms,
            duration_ms: st.duration_ms,
            source: st.source,
            client_op_id: st.client_op_id.clone(),
            tier: Tier::History,
            result_refused: st.result_refused.clone(),
        }
    }
}

/// Terminal result of an execution task, folded into the store by [`StatementStore::finish`].
pub(crate) enum ExecOutcome {
    Succeeded(Vec<RecordBatch>),
    /// A succeeded statement whose rows the store does not retain — the Connect path, whose
    /// batches are already on their way to the client as Arrow IPC. Result retention for those
    /// is PR2 (`results/<id>.arrow`), not a second copy in memory.
    SucceededSummary {
        rows: Option<usize>,
        schema: Option<Vec<(String, String)>>,
    },
    Failed(String),
    Canceled,
}

/// Where `/result` reads a statement's rows from (§5).
enum ResultSource {
    /// The hot tier still holds them.
    Memory(Vec<RecordBatch>),
    /// `results/<id>.arrow` — read back off disk, possibly after a restart.
    Disk,
    /// Neither tier has them: `410 result_expired`.
    Gone,
}

/// Outcome of a cancel request.
#[derive(Debug, PartialEq, Eq)]
enum CancelOutcome {
    Canceled,
    NotFound,
    AlreadyTerminal,
}

/// The caps and TTLs the store enforces. Defaults are today's behaviour exactly, which is what
/// `OXIDANT_HISTORY=off` reverts to.
#[derive(Debug, Clone)]
struct Limits {
    history_on: bool,
    /// Hot-tier count cap, and the history tier's too when history is on.
    max_records: usize,
    hot_ttl: Duration,
    max_per_session: usize,
    retention_days: i64,
    /// In-memory ceiling across every retained result (§5, F8). `u64::MAX` when there is nowhere
    /// durable to release rows *to* — with history off, or under
    /// `OXIDANT_RESULT_PERSIST=never` — because a budget that evicts rows into thin air is not a
    /// budget, it is a silent data loss the old store never had.
    result_budget: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            history_on: false,
            max_records: MAX_STATEMENTS,
            hot_ttl: STATEMENT_TTL,
            max_per_session: usize::MAX,
            retention_days: 0,
            result_budget: u64::MAX,
        }
    }
}

#[derive(Default)]
struct StoreInner {
    /// Hot tier: live and recently-terminal statements, with their batches and cancel channels.
    statements: std::collections::HashMap<String, Statement>,
    /// History tier: folded snapshots off the journal. No batches, no cancel channel, and
    /// **never touched by TTL eviction** — replay that the first new submit deletes is not replay.
    ///
    /// Mutated only through [`StoreInner::history_insert`] / [`StoreInner::history_remove`], so
    /// the two eviction indexes below cannot drift out of step with it.
    history: std::collections::HashMap<String, FoldedStatement>,
    /// `(seq, id)` of every history-tier statement, oldest first — the global cap's eviction
    /// order without an O(n) `min_by_key` per victim.
    by_seq: std::collections::BTreeSet<(u64, String)>,
    /// The same, partitioned by session, so the per-session share is checked with an O(1)
    /// `len()` per session instead of rebuilding a map of the whole tier on every submit.
    by_session: std::collections::HashMap<String, std::collections::BTreeSet<(u64, String)>>,
    /// `(session, client_op_id) → stmt-id`. The pair is the key: `client_op_id` alone would merge
    /// two sessions that both used `op-1`.
    alias: std::collections::HashMap<(String, String), String>,
    next_seq: u64,
    limits: Limits,
    /// How the SQL text is written down — echoed into demoted entries so a statement reads the
    /// same before and after a restart.
    sql_mode: SqlMode,
    /// Wall clock of the last retention pass, so the sweep is not re-run on every submit.
    last_sweep_ms: i64,
    /// Bytes of retained result batches across the hot tier — the budget `on_pressure` triggers
    /// on. Maintained as batches are attached and released, which is accounting today's store
    /// has none of.
    result_bytes: u64,
}

impl StoreInner {
    /// Add a statement to the history tier, keeping the eviction indexes in step.
    fn history_insert(&mut self, id: String, st: FoldedStatement) {
        let key = (st.seq, id.clone());
        if let Some(session) = &st.session {
            self.by_session
                .entry(session.clone())
                .or_default()
                .insert(key.clone());
        }
        self.by_seq.insert(key);
        self.history.insert(id, st);
    }

    /// Take a statement out of the history tier, keeping the eviction indexes in step.
    fn history_remove(&mut self, id: &str) -> Option<FoldedStatement> {
        let st = self.history.remove(id)?;
        let key = (st.seq, id.to_string());
        self.by_seq.remove(&key);
        if let Some(session) = &st.session {
            if let Some(ids) = self.by_session.get_mut(session) {
                ids.remove(&key);
                if ids.is_empty() {
                    self.by_session.remove(session);
                }
            }
        }
        Some(st)
    }

    /// Drop hot entries older than the hot TTL.
    ///
    /// Age is wall-clock (`submitted_at_ms`), not `Instant`: a replayed statement has no
    /// `Instant`, and synthesizing one from a 30-day-old timestamp saturates. With history on, an
    /// expiring terminal statement is *demoted* to the history tier rather than dropped, and a
    /// non-terminal one is never evicted at all.
    fn evict_expired(&mut self) {
        let now = oxidant_observability::now_ms();
        let ttl_ms = self.limits.hot_ttl.as_millis() as i64;
        let expired: Vec<String> = self
            .statements
            .iter()
            .filter(|(_, s)| now.saturating_sub(s.submitted_at_ms) >= ttl_ms)
            .filter(|(_, s)| !self.limits.history_on || s.status.is_terminal())
            .map(|(id, _)| id.clone())
            .collect();
        for id in expired {
            self.demote(&id);
        }
    }

    /// Release one statement's in-memory rows, returning the bytes freed.
    ///
    /// Called only when the rows are already durable (or the caller has decided they are being
    /// dropped anyway), because `/result` answers from the spilled file afterwards and from
    /// nowhere at all if there is none.
    fn release_rows(&mut self, id: &str) -> u64 {
        let Some(st) = self.statements.get_mut(id) else {
            return 0;
        };
        let freed = st.result_bytes;
        st.result_bytes = 0;
        st.batches = Vec::new();
        st.rows_in_memory = false;
        st.release_on_spill = false;
        self.result_bytes = self.result_bytes.saturating_sub(freed);
        freed
    }

    /// The statements whose rows must leave memory for the result budget to hold,
    /// oldest-terminal-first (§5).
    ///
    /// Each is marked `spilling` + `release_on_spill` so a second call cannot pick it again while
    /// its write is in flight, and so the spill's completion knows to free the memory. A
    /// non-terminal statement is never a victim: its rows do not exist yet.
    fn budget_victims(&mut self) -> Vec<String> {
        let budget = self.limits.result_budget;
        if budget == u64::MAX || self.result_bytes <= budget {
            return Vec::new();
        }
        let mut candidates: Vec<(u64, String)> = self
            .statements
            .iter()
            .filter(|(_, st)| {
                st.status.is_terminal() && st.rows_in_memory && !st.spilling && st.result_bytes > 0
            })
            .map(|(id, st)| (st.seq, id.clone()))
            .collect();
        candidates.sort_by_key(|(seq, _)| *seq);

        let mut projected = self.result_bytes;
        let mut victims = Vec::new();
        for (_, id) in candidates {
            if projected <= budget {
                break;
            }
            let Some(st) = self.statements.get_mut(&id) else {
                continue;
            };
            projected = projected.saturating_sub(st.result_bytes);
            st.spilling = true;
            st.release_on_spill = true;
            victims.push(id);
        }
        victims
    }

    /// Take a statement out of the hot tier, keeping its folded state when history is on.
    fn demote(&mut self, id: &str) {
        let Some(st) = self.statements.remove(id) else {
            return;
        };
        // The rows go with the hot entry; the spilled file (and its pointer) do not.
        self.result_bytes = self.result_bytes.saturating_sub(st.result_bytes);
        if !self.limits.history_on {
            // Nothing outlives the hot tier here, so neither may its alias: with history off
            // there is no sweeper to prune it later.
            if let (Some(session), Some(op)) = (&st.session, &st.client_op_id) {
                self.alias.remove(&(session.clone(), op.clone()));
            }
            return;
        }
        let last_seq = st.seq;
        let folded = st.to_folded(id, self.sql_mode, last_seq);
        self.history_insert(id.to_string(), folded);
    }

    /// Enforce the hot-tier count cap, oldest first.
    fn enforce_hot_cap(&mut self) {
        while self.statements.len() > self.limits.max_records {
            // With history on, a non-terminal statement is never the victim: it holds no result
            // batches, so evicting it buys almost no memory, and it would drop the cancel channel
            // of a query that is still running. The cap yields instead — bounded in practice by
            // how many statements can be in flight at once. With history off this is today's
            // unconditional oldest-first eviction, unchanged.
            let oldest = self
                .statements
                .iter()
                .filter(|(_, s)| !self.limits.history_on || s.status.is_terminal())
                .min_by_key(|(_, s)| s.seq)
                .map(|(id, _)| id.clone());
            let Some(oldest) = oldest else { break };
            self.demote(&oldest);
        }
    }
}

/// In-memory statement registry shared by the REST handlers and the execution tasks, backed by
/// the durable journal when history is on.
#[derive(Clone)]
pub(crate) struct StatementStore {
    inner: Arc<Mutex<StoreInner>>,
    /// Wakes `?wait=true` submitters when any statement reaches a terminal state.
    notify: Arc<Notify>,
    /// `None` — `OXIDANT_HISTORY=off` — is exactly today's volatile store.
    history: Option<Arc<HistoryRuntime>>,
    /// What the last disk sweep found: over the engine's own budget, and/or under the volume's
    /// free-space floor. Both are cleared by the first sweep that finds them false again, with
    /// no restart (§3).
    disk: Arc<DiskHealth>,
}

impl StatementStore {
    /// Today's volatile store: 1000 statements, 1 h TTL, nothing on disk.
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(StoreInner::default())),
            notify: Arc::new(Notify::new()),
            history: None,
            disk: Arc::new(DiskHealth::default()),
        }
    }

    /// Boot the durable store: lock the data dir, replay the journal into the history tier, and
    /// keep writing to it. `Err` fails the process's boot, loudly, with the reason.
    pub(crate) fn with_history(cfg: HistoryConfig) -> Result<Self, String> {
        let (runtime, fold) = HistoryRuntime::boot(cfg)?;
        let limits = Limits {
            history_on: true,
            max_records: runtime.cfg.max_records,
            hot_ttl: runtime.cfg.hot_ttl,
            max_per_session: runtime.cfg.max_per_session,
            retention_days: runtime.cfg.retention_days,
            // With nowhere durable to release rows to, the budget is not enforced by eviction —
            // see [`Limits::result_budget`].
            result_budget: if runtime.cfg.result_persist.spills_at_all() {
                runtime.cfg.result_memory_budget_bytes
            } else {
                u64::MAX
            },
        };
        let mut inner = StoreInner {
            next_seq: fold.max_seq + 1,
            limits,
            sql_mode: runtime.cfg.sql_mode,
            ..Default::default()
        };
        for (id, st) in fold.statements {
            if let (Some(session), Some(op)) = (st.session.clone(), st.client_op_id.clone()) {
                inner.alias.insert((session, op), id.clone());
            }
            inner.history_insert(id, st);
        }
        let replayed = inner.history.len();
        let store = Self {
            inner: Arc::new(Mutex::new(inner)),
            notify: Arc::new(Notify::new()),
            history: Some(Arc::new(runtime)),
            disk: Arc::new(DiskHealth::default()),
        };
        store.install_spill_sink();
        store.publish_status_counters();
        store.sweep_history();
        // §3: the sweeper runs at boot and every 5 minutes.
        store.sweep_disk();
        store.spawn_disk_sweeper();
        tracing::info!(
            statements = replayed,
            dir = %store
                .history
                .as_ref()
                .expect("history was just built")
                .cfg
                .statements_dir
                .display(),
            "statement history replayed"
        );
        Ok(store)
    }

    /// Teach the spill thread how to report back into the store.
    ///
    /// A [`std::sync::Weak`] rather than a clone: the callback lives on the spill thread for the
    /// thread's whole life, and a strong reference would keep a test's store — and its data-dir
    /// lock — alive past the end of the test.
    fn install_spill_sink(&self) {
        let Some(history) = self.history.as_ref() else {
            return;
        };
        let weak = Arc::downgrade(&self.inner);
        history.results.set_sink(Box::new(move |id, outcome| {
            let Some(inner) = weak.upgrade() else {
                return;
            };
            let mut inner = inner.lock().expect("statement store poisoned");
            // The statement may have been demoted while its rows were being written, in which
            // case the pointer belongs on the history-tier entry: the spill's own journal record
            // already carries it, and this keeps the in-memory tier from disagreeing with the
            // file on disk until the next boot.
            if !inner.statements.contains_key(id) {
                if let Some(st) = inner.history.get_mut(id) {
                    match outcome {
                        SpillOutcome::Spilled(pointer) => st.result = Some(pointer.clone()),
                        SpillOutcome::TooLarge => {
                            st.result_refused = Some(RESULT_TOO_LARGE.to_string())
                        }
                        SpillOutcome::Failed => {}
                    }
                }
                return;
            }
            let release = {
                let st = inner
                    .statements
                    .get_mut(id)
                    .expect("checked immediately above");
                st.spilling = false;
                match outcome {
                    SpillOutcome::Spilled(pointer) => {
                        st.result_file = Some(pointer.clone());
                        st.result_refused = None;
                        st.release_on_spill
                    }
                    // Nothing is on disk, so nothing may leave memory: the rows are all that is
                    // left of this result and dropping them would turn a size cap (or a full
                    // disk) into silent data loss.
                    SpillOutcome::TooLarge => {
                        st.result_refused = Some(RESULT_TOO_LARGE.to_string());
                        st.release_on_spill = false;
                        false
                    }
                    SpillOutcome::Failed => {
                        st.release_on_spill = false;
                        false
                    }
                }
            };
            if release {
                inner.release_rows(id);
            }
        }));
    }

    /// Build from the environment: the durable store, or today's volatile one under
    /// `OXIDANT_HISTORY=off`.
    pub(crate) fn from_env(role: &str, port: u16) -> Result<Self, String> {
        let cfg = HistoryConfig::from_env(role, port)?;
        if !cfg.enabled {
            return Ok(Self::new());
        }
        Self::with_history(cfg)
    }

    /// Insert a new `pending` statement from the REST API.
    fn insert(&self, sql: &str) -> (String, watch::Receiver<bool>) {
        self.insert_from(sql, Source::Rest, None, None)
    }

    /// Insert a new `pending` statement, journal its `submitted` record, and return the
    /// engine-minted id plus the receiver end of its cancel watch.
    ///
    /// The id is **always** `stmt-<uuid-v4>`, on both paths. A client-supplied `operation_id` is
    /// kept as a validated alias and never reaches a path or a fold key: Connect op ids are
    /// client-controlled and scoped per session, so using one as a filename is a traversal bug
    /// and using it as an identity merges two sessions that both said `op-1` (§4b).
    pub(crate) fn insert_from(
        &self,
        sql: &str,
        source: Source,
        session: Option<&str>,
        client_op_id: Option<&str>,
    ) -> (String, watch::Receiver<bool>) {
        let (tx, rx) = watch::channel(false);
        let id = format!("stmt-{}", Uuid::new_v4());
        let alias = match client_op_id {
            Some(raw) if !raw.is_empty() => match validate_alias(raw) {
                Some(valid) => Some(valid),
                None => {
                    // A logging concern must not break a query: the statement runs, the alias is
                    // dropped, and the session is named once.
                    tracing::warn!(
                        session = session.unwrap_or("-"),
                        "connect operation_id is not a valid alias ([A-Za-z0-9._:-]{{1,128}}); \
                         recording it as null"
                    );
                    None
                }
            },
            _ => None,
        };
        let submitted_at_ms = oxidant_observability::now_ms();
        let seq = {
            let mut inner = self.inner.lock().expect("statement store poisoned");
            inner.evict_expired();
            let seq = self.next_seq(&mut inner);
            inner.statements.insert(
                id.clone(),
                Statement {
                    sql: sql.to_string(),
                    status: StatementStatus::Pending,
                    error: None,
                    schema: None,
                    row_count: None,
                    batches: Vec::new(),
                    submitted_at_ms,
                    submitted: std::time::Instant::now(),
                    duration_ms: None,
                    cancel: tx,
                    seq,
                    source,
                    session: session.map(str::to_string),
                    client_op_id: alias.clone(),
                    durable_ack: None,
                    rows_in_memory: true,
                    result_bytes: 0,
                    result_file: None,
                    result_refused: None,
                    spilling: false,
                    release_on_spill: false,
                },
            );
            // The alias index exists to resolve a Connect `(session, operation_id)` back to an
            // engine-minted id across a restart, which only history can do. With
            // `OXIDANT_HISTORY=off` nothing ever prunes it — `sweep_history` returns immediately
            // and `demote` drops the statement without touching `history` — and both halves of
            // the key are client-supplied over Spark Connect, so a client could grow it without
            // limit at one entry per `ExecutePlan`. §8 says `off` restores today's behaviour
            // exactly, and today there is no alias map at all.
            if inner.limits.history_on {
                if let (Some(session), Some(alias)) = (session, alias.as_deref()) {
                    inner
                        .alias
                        .insert((session.to_string(), alias.to_string()), id.clone());
                }
            }
            inner.enforce_hot_cap();
            seq
        };

        if let Some(history) = &self.history {
            // §4a: the `submitted` record is the crash trace, and it is the only record that
            // carries the SQL. It is never dropped — a full channel overflows instead — and the
            // enqueue never blocks, so this stays safe to call from a tokio worker.
            history.journal.append_retained(JournalRecord {
                v: RECORD_VERSION,
                kind: RecordKind::Submitted,
                seq,
                last_seq: None,
                id: id.clone(),
                client_op_id: alias,
                session: session.map(str::to_string),
                source: Some(source.as_str().to_string()),
                sql: Some(history.cfg.sql_mode.encode(sql)),
                sql_encoding: Some(history.cfg.sql_mode.as_str().to_string()),
                status: Some(StatementStatus::Pending),
                error: None,
                schema: None,
                rows: None,
                submitted_at_ms,
                duration_ms: None,
                result: None,
                result_refused: None,
                ts: rfc3339_from_ms(submitted_at_ms),
            });
            self.sweep_history();
        }
        (id, rx)
    }

    /// Next sequence. With history on it comes from the journal, so statement order and record
    /// order are one line; without, it is the store's own counter, exactly as today.
    fn next_seq(&self, inner: &mut StoreInner) -> u64 {
        match &self.history {
            Some(history) => history.journal.next_seq(),
            None => {
                let seq = inner.next_seq;
                inner.next_seq += 1;
                seq
            }
        }
    }

    /// `pending` → `running`. A cancel that landed before the task started wins (the
    /// statement is already terminal and left alone).
    pub(crate) fn mark_running(&self, id: &str) {
        let record = {
            let mut inner = self.inner.lock().expect("statement store poisoned");
            let Some(st) = inner.statements.get_mut(id) else {
                return;
            };
            if st.status != StatementStatus::Pending {
                return;
            }
            st.status = StatementStatus::Running;
            self.history.as_ref().map(|_| JournalRecord {
                v: RECORD_VERSION,
                kind: RecordKind::Running,
                seq: st.seq,
                last_seq: None,
                id: id.to_string(),
                client_op_id: st.client_op_id.clone(),
                session: st.session.clone(),
                source: Some(st.source.as_str().to_string()),
                sql: None,
                sql_encoding: None,
                status: Some(StatementStatus::Running),
                error: None,
                schema: None,
                rows: None,
                submitted_at_ms: st.submitted_at_ms,
                duration_ms: None,
                result: None,
                result_refused: None,
                ts: now_rfc3339(),
            })
        };
        // Progress chatter: dropped and counted if the writer is behind, never waited on.
        if let (Some(history), Some(record)) = (&self.history, record) {
            history.journal.append(record);
        }
    }

    /// Fold an execution task's terminal outcome into the store. Never overwrites a terminal
    /// state — a cancel that landed first keeps the statement `canceled` (and the late result
    /// batches are dropped here, freeing their memory).
    ///
    /// The terminal record is handed to the journal *before* memory is published and the waiters
    /// are woken, and its ack is parked on the statement for whoever answers the client. The
    /// query itself never waits: the wait is on the response path and is bounded by
    /// `OXIDANT_HISTORY_ACK_TIMEOUT_MS`.
    pub(crate) fn finish(&self, id: &str, outcome: ExecOutcome) {
        let (record, spills) = {
            let mut inner = self.inner.lock().expect("statement store poisoned");
            let Some(st) = inner.statements.get_mut(id) else {
                return; // evicted
            };
            if st.status.is_terminal() {
                return;
            }
            st.duration_ms = Some(st.submitted.elapsed().as_millis() as i64);
            match outcome {
                ExecOutcome::Succeeded(batches) => {
                    st.row_count = Some(batches.iter().map(|b| b.num_rows()).sum());
                    st.schema = schema_fields(&batches);
                    st.result_bytes = retained_bytes(&batches);
                    st.batches = batches;
                    st.status = StatementStatus::Succeeded;
                }
                ExecOutcome::SucceededSummary { rows, schema } => {
                    st.row_count = rows;
                    st.schema = schema;
                    st.status = StatementStatus::Succeeded;
                    // The batches went to the gRPC client, not into the store. `/result` must say
                    // so (`410 result_expired`) rather than answer an empty row set that
                    // contradicts the `rowCount` in this statement's own status document.
                    st.rows_in_memory = false;
                }
                ExecOutcome::Failed(error) => {
                    st.error = Some(error);
                    st.status = StatementStatus::Failed;
                }
                ExecOutcome::Canceled => {
                    st.status = StatementStatus::Canceled;
                }
            }
            let record = self.terminal_record(id, st);
            let added = st.result_bytes;
            inner.result_bytes += added;
            let spills = self.plan_spills(&mut inner, id);
            (record, spills)
        };
        // Handing the record over can wait for room in the writer channel, so it happens with
        // the store mutex released — every submit, list and status call takes that mutex, and a
        // slow disk must not be able to stall them. The ack is parked before the waiters are
        // woken, so whoever answers the client finds it.
        self.hand_over_terminal(id, record);
        // Same rule, and the load-bearing one for §5: encoding Arrow IPC is the *only* genuinely
        // large thing history does, and it happens on the spill thread with this mutex released.
        self.queue_spills(spills);
        self.notify.notify_waiters();
    }

    /// Decide what leaves memory for `results/`, under the store lock but doing no I/O.
    ///
    /// Two triggers, one code path: `always` spills the statement that just finished, and any
    /// mode spills the oldest terminal results the in-memory budget can no longer hold. A victim
    /// already on disk needs no write at all — its rows are released here and now.
    fn plan_spills(&self, inner: &mut StoreInner, finished: &str) -> Vec<SpillJob> {
        let Some(history) = self.history.as_ref() else {
            return Vec::new();
        };
        let persist: ResultPersist = history.results.persist();
        if !persist.spills_at_all() {
            return Vec::new();
        }
        let mut wanted: Vec<String> = Vec::new();
        if persist.spills_eagerly() {
            wanted.push(finished.to_string());
        }
        for id in inner.budget_victims() {
            if !wanted.contains(&id) {
                wanted.push(id);
            }
        }

        let sql_mode = history.cfg.sql_mode;
        let mut jobs = Vec::new();
        for id in wanted {
            // A victim whose file already landed does not need a second write; releasing its
            // rows is the whole point, and that is free.
            let already_on_disk = inner
                .statements
                .get(&id)
                .is_some_and(|st| st.result_file.is_some());
            if already_on_disk {
                inner.release_rows(&id);
                continue;
            }
            let Some(st) = inner.statements.get_mut(&id) else {
                continue;
            };
            // Nothing to spill: not a succeeded statement, its rows were never retained here
            // (the Connect path), the encoding was already refused, or it produced no batches —
            // an Arrow IPC stream needs a schema and there is none.
            if st.status != StatementStatus::Succeeded
                || !st.rows_in_memory
                || st.result_refused.is_some()
                || st.batches.is_empty()
            {
                st.spilling = false;
                st.release_on_spill = false;
                continue;
            }
            st.spilling = true;
            let folded = st.to_folded(&id, sql_mode, st.seq);
            let batches = Arc::new(st.batches.clone());
            jobs.push(SpillJob {
                id,
                batches,
                folded: Box::new(folded),
            });
        }
        jobs
    }

    /// Hand the planned spills to the writer thread. Never called with the store lock held.
    ///
    /// A job the writer refuses — a full queue, or spills paused by the free-space floor — is
    /// handed straight back to [`Self::abandon_spills`]. `plan_spills` marked each victim
    /// `spilling` under the lock, and `spilling` is what keeps the memory budget from picking
    /// it twice; leaving that flag set on a job nobody took pinned the rows in memory until the
    /// hot TTL expired and quietly removed the statement from the budget's reach (H2).
    fn queue_spills(&self, jobs: Vec<SpillJob>) {
        if jobs.is_empty() {
            return;
        }
        let Some(history) = self.history.as_ref() else {
            return;
        };
        let mut refused: Vec<String> = Vec::new();
        for job in jobs {
            let id = job.id.clone();
            if !history.results.spill(job) {
                refused.push(id);
            }
        }
        self.abandon_spills(refused);
    }

    /// Put statements whose spill never reached the writer back into the memory budget.
    ///
    /// The rows are still in memory and still counted in `result_bytes` — nothing was lost —
    /// so clearing `spilling` is all it takes for the next `budget_victims` pass to select them
    /// again. That retry *is* the backoff: it happens on the next terminal statement, by which
    /// time the queue has had a chance to drain.
    fn abandon_spills(&self, ids: Vec<String>) {
        if ids.is_empty() {
            return;
        }
        {
            let mut inner = self.inner.lock().expect("statement store poisoned");
            for id in &ids {
                if let Some(st) = inner.statements.get_mut(id) {
                    st.spilling = false;
                    st.release_on_spill = false;
                }
            }
        }
        tracing::warn!(
            statements = ids.len(),
            "result spill refused before it reached the writer; the rows stay in memory and \
             remain candidates for the next budget pass"
        );
    }

    /// Build the self-contained terminal record for a statement, if history is on.
    fn terminal_record(&self, id: &str, st: &Statement) -> Option<JournalRecord> {
        let history = self.history.as_ref()?;
        let last_seq = history.journal.next_seq();
        Some(
            st.to_folded(id, history.cfg.sql_mode, last_seq)
                .to_snapshot(),
        )
    }

    /// Queue a terminal record and park its durability ack on the statement.
    fn hand_over_terminal(&self, id: &str, record: Option<JournalRecord>) {
        let (Some(history), Some(record)) = (self.history.clone(), record) else {
            return;
        };
        let ack = history.journal.append_durable(record);
        let mut inner = self.inner.lock().expect("statement store poisoned");
        if let Some(st) = inner.statements.get_mut(id) {
            st.durable_ack = ack;
        }
    }

    /// Wait for a statement's terminal record to be durable. `true` means it is not, and the
    /// answer must say so rather than implying a durability it does not have.
    pub(crate) async fn await_durable(&self, id: &str) -> bool {
        let Some(history) = self.history.clone() else {
            return false;
        };
        let ack = {
            let mut inner = self.inner.lock().expect("statement store poisoned");
            match inner.statements.get_mut(id) {
                Some(st) => st.durable_ack.take(),
                // Already demoted to the history tier, which only happens after the record was
                // written; or the ack was taken by whoever answered first.
                None => return false,
            }
        };
        let Some(ack) = ack else {
            return history.journal.is_degraded();
        };
        match tokio::time::timeout(history.cfg.ack_timeout, ack).await {
            // The writer completes the ack only after a successful fsync. A *dropped* sender is
            // the writer saying the append or the fsync was refused, and it lands in the arm
            // below with the timeout — both mean "we cannot claim this is on disk".
            Ok(Ok(())) => history.journal.is_degraded(),
            _ => {
                history.journal.mark_degraded();
                true
            }
        }
    }

    /// Best-effort cancel: mark `canceled` and signal the execution task to drop the query
    /// future. Terminal statements are left untouched, and so is anything in the history tier —
    /// an archival statement has no future to cancel.
    fn cancel(&self, id: &str) -> CancelOutcome {
        let (outcome, record) = {
            let mut inner = self.inner.lock().expect("statement store poisoned");
            let archival = !inner.statements.contains_key(id) && inner.history.contains_key(id);
            match inner.statements.get_mut(id) {
                None if archival => (CancelOutcome::AlreadyTerminal, None),
                None => (CancelOutcome::NotFound, None),
                Some(st) if st.status.is_terminal() => (CancelOutcome::AlreadyTerminal, None),
                Some(st) => {
                    st.status = StatementStatus::Canceled;
                    st.duration_ms = Some(st.submitted.elapsed().as_millis() as i64);
                    let _ = st.cancel.send(true);
                    let record = self.terminal_record(id, st);
                    (CancelOutcome::Canceled, record)
                }
            }
        };
        if outcome == CancelOutcome::Canceled {
            self.hand_over_terminal(id, record);
            self.notify.notify_waiters();
        }
        outcome
    }

    /// Hot tier first, then history — `GET /api/v1/statements/{id}` reads through both.
    pub(crate) fn snapshot(&self, id: &str) -> Option<StatementSnapshot> {
        let inner = self.inner.lock().expect("statement store poisoned");
        inner
            .statements
            .get(id)
            .map(|st| st.snapshot(id))
            .or_else(|| inner.history.get(id).map(StatementSnapshot::from_history))
    }

    /// Snapshot + where the result endpoint can read the rows from: memory, then the spilled
    /// file, then nowhere (§5).
    ///
    /// The fall-through order is the whole of PR2's read side. Memory first because it is free;
    /// disk second because it is what makes "Show rows" and CSV survive a restart; and
    /// [`ResultSource::Gone`] only when both are genuinely absent — which is what
    /// `410 result_expired` means and `404` would not.
    fn result(&self, id: &str) -> Option<(StatementSnapshot, ResultSource)> {
        let inner = self.inner.lock().expect("statement store poisoned");
        if let Some(st) = inner.statements.get(id) {
            let source = if st.rows_in_memory {
                ResultSource::Memory(st.batches.clone())
            } else if st.result_file.is_some() {
                ResultSource::Disk
            } else {
                ResultSource::Gone
            };
            return Some((st.snapshot(id), source));
        }
        inner.history.get(id).map(|st| {
            let source = if st.result.is_some() {
                ResultSource::Disk
            } else {
                ResultSource::Gone
            };
            (StatementSnapshot::from_history(st), source)
        })
    }

    /// Decode a spilled result off disk, off the tokio worker.
    ///
    /// `None` covers both "history is off" and "the file is not readable any more" — a sweep or
    /// an operator may have removed it between the snapshot and this read, and the honest answer
    /// to that race is the same `410 result_expired` as if it had never existed.
    async fn read_spilled(&self, id: &str) -> Option<Vec<RecordBatch>> {
        let results = Arc::clone(&self.history.as_ref()?.results);
        let owned = id.to_string();
        match tokio::task::spawn_blocking(move || results.read(&owned)).await {
            Ok(Ok(batches)) => Some(batches),
            Ok(Err(e)) => {
                tracing::warn!(
                    statement = %id,
                    error = %e,
                    "spilled result could not be read back; answering 410 result_expired"
                );
                None
            }
            Err(_) => None,
        }
    }

    /// Newest-first snapshots across both tiers, capped at [`LIST_CAP`].
    pub(crate) fn list(&self) -> Vec<StatementSnapshot> {
        let inner = self.inner.lock().expect("statement store poisoned");
        let mut items: Vec<(u64, StatementSnapshot)> = inner
            .statements
            .iter()
            .map(|(id, st)| (st.seq, st.snapshot(id)))
            .collect();
        items.extend(
            inner
                .history
                .iter()
                .filter(|(id, _)| !inner.statements.contains_key(*id))
                .map(|(_, st)| (st.seq, StatementSnapshot::from_history(st))),
        );
        items.sort_by(|a, b| b.0.cmp(&a.0));
        items.into_iter().take(LIST_CAP).map(|(_, s)| s).collect()
    }

    /// Block until every queued spill has been written and reported back.
    ///
    /// The spill thread reports each outcome to the sink *before* it answers a `Drain`, so this
    /// returning means the store's own bookkeeping — released rows, pointers, `result_too_large`
    /// — is settled too. The disk sweeper needs the same barrier, and drives it directly on the
    /// result store: accounting a spill that has not landed yet would prune against a byte total
    /// that is about to change.
    #[cfg(test)]
    fn drain_spills(&self) {
        if let Some(history) = &self.history {
            history.results.drain_blocking();
        }
    }

    /// Flush the journal and stop its writer thread — the clean-shutdown seam a restart test
    /// needs so the next boot reads a settled directory.
    #[cfg(test)]
    fn shutdown_for_test(&self) {
        if let Some(history) = &self.history {
            // Results first: a spill's own journal record is appended by the spill thread, so
            // stopping the journal before it would drop the pointer this store just published.
            history.results.shutdown();
            history.journal.shutdown();
        }
    }

    /// Resolve a Connect `(session, operation_id)` alias to the engine-minted statement id.
    #[cfg_attr(not(test), allow(dead_code))]
    fn resolve_alias(&self, session: &str, client_op_id: &str) -> Option<String> {
        let inner = self.inner.lock().expect("statement store poisoned");
        inner
            .alias
            .get(&(session.to_string(), client_op_id.to_string()))
            .cloned()
    }

    /// Retention over the history tier: age, then the per-session share, then the global cap.
    ///
    /// Statement-granular by construction — each eviction appends a tombstone and lets compaction
    /// physically drop the record, so a segment is never deleted out from under a statement whose
    /// `submitted` and terminal records straddle it. A non-terminal statement is never evicted.
    fn sweep_history(&self) {
        let Some(history) = self.history.clone() else {
            return;
        };
        let now = oxidant_observability::now_ms();
        let tombstones = {
            let mut inner = self.inner.lock().expect("statement store poisoned");
            // The age sweep is O(n); throttle it so a submit storm does not re-scan 10,000
            // records per statement. The caps below are checked every time.
            let due = now.saturating_sub(inner.last_sweep_ms) >= SWEEP_INTERVAL_MS;
            if due {
                inner.last_sweep_ms = now;
            }
            let mut evicted: Vec<(String, i64)> = Vec::new();
            if due && inner.limits.retention_days > 0 {
                // `retention_days` is an operator-supplied `u64` widened to `i64`, so the
                // multiplication is not obviously in range: saturate rather than panic in debug
                // and wrap in release.
                let span = inner.limits.retention_days.saturating_mul(86_400_000);
                let cutoff = now.saturating_sub(span);
                let stale: Vec<String> = inner
                    .history
                    .iter()
                    .filter(|(_, st)| st.status.is_terminal() && st.submitted_at_ms < cutoff)
                    .map(|(id, _)| id.clone())
                    .collect();
                for id in stale {
                    if let Some(st) = inner.history_remove(&id) {
                        evicted.push((id, st.submitted_at_ms));
                    }
                }
            }
            // Per-session share first, so a noisy session evicts itself before it can push
            // another tenant's history out of the global cap.
            //
            // This runs on *every* submit — the caps, unlike the age scan above, are not
            // throttled, because a throttled cap is a cap that a submit storm can overshoot by a
            // whole sweep interval. That is affordable only because both passes read the
            // `by_session` / `by_seq` indexes: no map of the whole tier is rebuilt, no id or
            // session string is cloned, and nothing is sorted. Rebuilding it here is what made a
            // 10,000-record tier cost an allocation and a sort per statement submitted, inside
            // the mutex every list, status, result and cancel call also takes.
            let per_session = inner.limits.max_per_session;
            if per_session < usize::MAX {
                let mut victims: Vec<String> = Vec::new();
                {
                    let StoreInner {
                        history,
                        by_session,
                        ..
                    } = &mut *inner;
                    for ids in by_session.values() {
                        // O(1) per session, and the tier is only walked for a session that is
                        // actually over its share.
                        let Some(mut excess) = ids.len().checked_sub(per_session) else {
                            continue;
                        };
                        for (_, id) in ids.iter() {
                            if excess == 0 {
                                break;
                            }
                            // Oldest first (the set is keyed on `seq`), and never a statement
                            // that is still running.
                            if history.get(id).is_some_and(|st| st.status.is_terminal()) {
                                victims.push(id.clone());
                                excess -= 1;
                            }
                        }
                    }
                }
                for id in victims {
                    if let Some(st) = inner.history_remove(&id) {
                        evicted.push((id, st.submitted_at_ms));
                    }
                }
            }
            while inner.history.len() > inner.limits.max_records {
                let oldest = {
                    let StoreInner {
                        history, by_seq, ..
                    } = &mut *inner;
                    by_seq
                        .iter()
                        .find(|(_, id)| history.get(id).is_some_and(|st| st.status.is_terminal()))
                        .map(|(_, id)| id.clone())
                };
                let Some(oldest) = oldest else {
                    // Everything left is non-terminal: running is never evicted, so the cap
                    // yields rather than the statement.
                    break;
                };
                if let Some(st) = inner.history_remove(&oldest) {
                    evicted.push((oldest, st.submitted_at_ms));
                }
            }
            if !evicted.is_empty() {
                let gone: std::collections::HashSet<&str> =
                    evicted.iter().map(|(id, _)| id.as_str()).collect();
                inner
                    .alias
                    .retain(|_, target| !gone.contains(target.as_str()));
            }
            evicted
        };
        // Best-effort, like every other non-terminal write: a tombstone lost to backpressure
        // means the statement is folded again at the next boot and re-evicted by the next sweep
        // — self-healing, and never a lost statement.
        //
        // The result file goes in the *same* sweep, before the tombstone is considered complete
        // (§5, F13). The journal is the authority: nothing here decides a result's lifetime, it
        // only follows the statement's. A crash between the two leaves an orphan, which is
        // exactly what boot's `reconcile` is for.
        for (id, submitted_at_ms) in tombstones {
            history.results.unlink(&id);
            let seq = history.journal.next_seq();
            history
                .journal
                .append(JournalRecord::tombstone(&id, seq, submitted_at_ms));
        }
    }

    /// Every statement id either tier still knows about.
    ///
    /// The union, not the folded set: a hot statement has no snapshot on disk yet, and a
    /// *running* one has no journal record beyond its `submitted` — deleting either's result
    /// would be the one thing retention must never do.
    fn live_ids(&self) -> std::collections::HashSet<String> {
        let inner = self.inner.lock().expect("statement store poisoned");
        inner
            .statements
            .keys()
            .chain(inner.history.keys())
            .cloned()
            .collect()
    }

    /// The disk-budget sweep (§3): measure everything the engine owns under the root, and prune
    /// in the documented order until it fits — or until there is nothing left to prune, which is
    /// what `disk: over_budget` reports.
    ///
    /// Runs at boot and every `OXIDANT_DISK_SWEEP_SECS` (300). Returns what it did, which is both
    /// the log line and the seam the test asserts the order through.
    pub(crate) fn sweep_disk(&self) -> disk::SweepReport {
        let Some(history) = self.history.clone() else {
            return disk::SweepReport::default();
        };
        // A queued spill is bytes that are about to exist; accounting without it would prune
        // against a total that changes a millisecond later.
        history.results.drain_blocking();

        let cfg = &history.cfg;
        let roots = budget_roots(cfg);
        let used = |roots: &[std::path::PathBuf]| -> u64 {
            roots.iter().map(|r| disk::subtree_bytes(r)).sum()
        };
        let over = |roots: &[std::path::PathBuf]| -> bool {
            if used(roots) > cfg.disk_max_bytes {
                return true;
            }
            disk::free_bytes(&cfg.root).is_some_and(|free| free < cfg.disk_min_free_bytes)
        };

        let mut report = disk::SweepReport::default();
        let before = used(&roots);

        // 1. Oldest rolled logs. The live file is never a candidate — it rotates (PR3).
        for file in disk::rolled_logs(&cfg.logs_dir) {
            if !over(&roots) {
                break;
            }
            report.freed_bytes += disk::remove(&file);
            report.rolled_logs_removed += 1;
        }
        // 2. Oldest dumps.
        for file in disk::dumps(&cfg.dumps_dir) {
            if !over(&roots) {
                break;
            }
            report.freed_bytes += disk::remove(&file);
            report.dumps_removed += 1;
        }
        // 3. Result files whose statement is already pruned. Orphans are garbage whether or not
        // the budget is tight, so this pass is unconditional.
        report.orphan_results_removed = history.results.reconcile(&self.live_ids());

        // 4. Oldest journal *statements* — statement-granular, never a raw segment unlink (F2).
        while over(&roots) {
            if !self.prune_oldest_statement() {
                break;
            }
            report.statements_pruned += 1;
        }
        // 5. Oldest live result files. The rows go, the statement stays, and `/result` answers
        // `410 result_expired` for it from here on.
        for (id, _) in history.results.files() {
            if !over(&roots) {
                break;
            }
            if self.drop_result_file(&id) {
                report.live_results_removed += 1;
            }
        }

        report.used_bytes = used(&roots);
        report.freed_bytes = before.saturating_sub(report.used_bytes);
        report.over_budget = over(&roots);
        if report.removed_anything() || report.over_budget {
            tracing::info!(
                used_bytes = report.used_bytes,
                freed_bytes = report.freed_bytes,
                budget_bytes = cfg.disk_max_bytes,
                rolled_logs = report.rolled_logs_removed,
                dumps = report.dumps_removed,
                orphan_results = report.orphan_results_removed,
                statements = report.statements_pruned,
                live_results = report.live_results_removed,
                over_budget = report.over_budget,
                "disk sweep"
            );
        }
        self.disk
            .over_budget
            .store(report.over_budget, std::sync::atomic::Ordering::Relaxed);
        report
    }

    /// Evict the single oldest terminal statement, tombstone it, and unlink its result.
    /// `false` means there was nothing evictable — everything left is still running.
    fn prune_oldest_statement(&self) -> bool {
        let Some(history) = self.history.clone() else {
            return false;
        };
        let victim = (|| {
            let mut inner = self.inner.lock().expect("statement store poisoned");
            let oldest = {
                let StoreInner {
                    history, by_seq, ..
                } = &mut *inner;
                by_seq
                    .iter()
                    .find(|(_, id)| history.get(id).is_some_and(|st| st.status.is_terminal()))
                    .map(|(_, id)| id.clone())
            };
            // Nothing in the history tier: fall back to the oldest *terminal* hot statement, so a
            // driver whose whole history is still hot can still be pruned under pressure.
            let oldest = oldest.or_else(|| {
                inner
                    .statements
                    .iter()
                    .filter(|(_, st)| st.status.is_terminal())
                    .min_by_key(|(_, st)| st.seq)
                    .map(|(id, _)| id.clone())
            });
            let id = oldest?;
            let submitted_at_ms = match inner.history_remove(&id) {
                Some(st) => Some(st.submitted_at_ms),
                None => inner.statements.remove(&id).map(|st| {
                    inner.result_bytes = inner.result_bytes.saturating_sub(st.result_bytes);
                    st.submitted_at_ms
                }),
            }?;
            inner.alias.retain(|_, target| target != &id);
            Some((id, submitted_at_ms))
        })();
        let Some((id, submitted_at_ms)) = victim else {
            return false;
        };
        history.results.unlink(&id);
        let seq = history.journal.next_seq();
        history
            .journal
            .append(JournalRecord::tombstone(&id, seq, submitted_at_ms));
        true
    }

    /// Unlink one statement's result file but keep the statement, and journal the clearing so a
    /// restart does not read a pointer to a file that is gone.
    fn drop_result_file(&self, id: &str) -> bool {
        let Some(history) = self.history.clone() else {
            return false;
        };
        if !history.results.unlink(id) {
            return false;
        }
        let cleared = {
            let mut inner = self.inner.lock().expect("statement store poisoned");
            if let Some(st) = inner.statements.get_mut(id) {
                st.result_file = None;
                let last_seq = history.journal.next_seq();
                Some(st.to_folded(id, history.cfg.sql_mode, last_seq))
            } else if let Some(st) = inner.history.get_mut(id) {
                st.result = None;
                st.last_seq = history.journal.next_seq();
                st.rank = RecordKind::Snapshot.rank();
                Some(st.clone())
            } else {
                None
            }
        };
        if let Some(folded) = cleared {
            history.journal.append_retained(folded.to_snapshot());
        }
        true
    }

    /// Publish this store's counters to `/api/status`.
    ///
    /// A [`std::sync::Weak`] of the store's own two halves rather than a clone: the callback is
    /// process-global and would otherwise keep a test's store — and its data-dir lock — alive for
    /// the rest of the binary. When it cannot upgrade, the counters simply go absent, which is
    /// the same thing `OXIDANT_HISTORY=off` reports.
    fn publish_status_counters(&self) {
        let Some(history) = self.history.as_ref() else {
            return;
        };
        let history = Arc::downgrade(history);
        let disk = Arc::clone(&self.disk);
        oxidant_observability::set_history_status_source(move || match history.upgrade() {
            Some(history) => counters_for(&history, &disk),
            // The store this was published for is gone (only a test drops one). Report the
            // quiet defaults rather than a stale reading of a journal that no longer exists.
            None => oxidant_observability::HistoryStatus {
                history_writes: oxidant_observability::history_writes::OK.to_string(),
                history_dropped_events: 0,
                results_on_disk_bytes: 0,
                result_writes: oxidant_observability::history_writes::OK.to_string(),
                result_write_failures: 0,
                disk: oxidant_observability::disk_state::OK.to_string(),
            },
        });
    }

    /// This store's durability counters, exactly as `/api/status` reports them. `None` is
    /// `OXIDANT_HISTORY=off`, where the four fields are absent from the response entirely.
    ///
    /// The endpoint itself reads them through the published source above rather than through a
    /// store handle — `oxidant-observability` sits below this crate — so this is the seam the
    /// tests assert the same values through.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn status_counters(&self) -> Option<oxidant_observability::HistoryStatus> {
        let history = self.history.as_ref()?;
        Some(counters_for(history, &self.disk))
    }

    /// Run the disk sweep every `OXIDANT_DISK_SWEEP_SECS` for as long as the store lives.
    ///
    /// A plain thread rather than a tokio task: the store is built at boot, before there is
    /// necessarily a runtime to spawn onto, and the sweep is blocking filesystem work either way.
    /// It holds only weak references and ticks once a second so it notices a dropped store
    /// promptly instead of up to five minutes later — which is what keeps a test binary from
    /// accumulating one sleeping thread per store it builds.
    fn spawn_disk_sweeper(&self) {
        let Some(history) = self.history.as_ref() else {
            return;
        };
        let interval = history.cfg.disk_sweep_interval;
        let inner = Arc::downgrade(&self.inner);
        let history = Arc::downgrade(history);
        let disk = Arc::clone(&self.disk);
        // The *same* `Notify`, not a fresh one: `prune_oldest_statement` can remove a statement
        // a `?wait=true` caller is parked on, and a sweeper holding its own channel would leave
        // that caller blocked to its timeout instead of waking it.
        let notify = Arc::downgrade(&self.notify);
        let spawned = std::thread::Builder::new()
            .name("oxidant-disk-sweep".to_string())
            .spawn(move || {
                let tick = Duration::from_secs(1).min(interval);
                let mut waited = Duration::ZERO;
                loop {
                    std::thread::sleep(tick);
                    let (Some(inner), Some(history), Some(notify)) =
                        (inner.upgrade(), history.upgrade(), notify.upgrade())
                    else {
                        return;
                    };
                    waited += tick;
                    if waited < interval {
                        continue;
                    }
                    waited = Duration::ZERO;
                    // Rebuilt rather than captured, so the thread holds nothing strong between
                    // ticks and the store can be dropped while it sleeps.
                    let store = StatementStore {
                        inner,
                        notify,
                        history: Some(history),
                        disk: Arc::clone(&disk),
                    };
                    store.sweep_disk();
                }
            });
        if let Err(e) = spawned {
            tracing::warn!(
                error = %e,
                "could not start the disk sweeper; the disk budget will only be enforced at boot"
            );
        }
    }

    /// Block until the statement reaches a terminal state or `timeout` elapses (returns the
    /// statement's then-current snapshot either way; `None` only if the id is unknown).
    async fn wait_terminal(&self, id: &str, timeout: Duration) -> Option<StatementSnapshot> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            // Register the waiter BEFORE re-checking state, so a transition between the
            // check and the await is not missed (tokio `Notified::enable` pattern).
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let snap = self.snapshot(id)?;
            if snap.status.is_terminal() {
                return Some(snap);
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Some(snap);
            }
            let _ = tokio::time::timeout_at(deadline, notified).await;
        }
    }
}

/// Read the durability counters off a booted history runtime (§3, §7).
///
/// Three subsystems, three flags, one aggregate. `history_writes` is `degraded` while *any* of
/// the journal, the spill writer or the disk sweep is, and each of those three is sticky until a
/// success **of its own** clears it. Reporting them through one flag was H3: the journal clears
/// its own on every successful append, so a permanently failing result volume read `ok` again
/// the microsecond the next statement was submitted.
///
/// `history_dropped_events` still counts the journal's dropped records *and* the spill jobs the
/// writer had no room for: both are work history gave up on under pressure, and an operator
/// watching one number for "am I losing history" should not have to know there are two queues.
/// Spills the disk *refused* are a different thing and get their own `result_write_failures`.
fn counters_for(
    history: &HistoryRuntime,
    disk: &DiskHealth,
) -> oxidant_observability::HistoryStatus {
    let result_degraded = history.results.is_degraded();
    let low_free = disk.low_free.load(std::sync::atomic::Ordering::Relaxed);
    let over_budget = disk.over_budget.load(std::sync::atomic::Ordering::Relaxed);
    let flag = |degraded: bool| {
        if degraded {
            oxidant_observability::history_writes::DEGRADED
        } else {
            oxidant_observability::history_writes::OK
        }
        .to_string()
    };
    oxidant_observability::HistoryStatus {
        history_writes: flag(history.journal.is_degraded() || result_degraded || low_free),
        history_dropped_events: history.journal.dropped_events() + history.results.dropped_spills(),
        results_on_disk_bytes: history.results.on_disk_bytes(),
        result_writes: flag(result_degraded),
        result_write_failures: history.results.write_failures(),
        // `over_budget` wins when both hold: it is the condition the engine can act on, and the
        // one whose remedy (raise the budget, or let the sweeper work) is the operator's.
        disk: if over_budget {
            oxidant_observability::disk_state::OVER_BUDGET
        } else if low_free {
            oxidant_observability::disk_state::LOW_FREE
        } else {
            oxidant_observability::disk_state::OK
        }
        .to_string(),
    }
}

/// The disk sweep's own two flags, shared between the store, its sweeper thread and
/// `/api/status`.
///
/// They are deliberately separate booleans rather than one: `over_budget` means *the engine
/// overspent its own budget and has nothing left to prune*, and `low_free` means *the volume is
/// short for reasons that may have nothing to do with the engine*. Collapsing them is what let a
/// co-tenant filling the disk delete the entire statement history (H1).
#[derive(Debug, Default)]
pub(crate) struct DiskHealth {
    over_budget: std::sync::atomic::AtomicBool,
    low_free: std::sync::atomic::AtomicBool,
}

/// The distinct subtrees the disk budget covers, deduped so nothing is billed twice.
///
/// `OXIDANT_HISTORY_DIR` / `OXIDANT_RESULT_DIR` / `OXIDANT_LOG_DIR` / `OXIDANT_DUMP_DIR` each win
/// over the root and may point *outside* it, and §3 says an overridden subtree is still counted
/// against the budget — so the candidates are measured as a set, with any path already contained
/// in a shallower one dropped.
///
/// `event_log_dir` is deliberately absent: §8/F16 brings it under the budget by *rolling*
/// `events.jsonl`, and that writer is PR3. Counting it here without being able to prune it would
/// pin `disk: over_budget` on for anyone with a large Spark-history-server directory.
fn budget_roots(cfg: &HistoryConfig) -> Vec<std::path::PathBuf> {
    let history_dir = cfg
        .statements_dir
        .parent()
        .map(std::path::Path::to_path_buf)
        .unwrap_or_else(|| cfg.statements_dir.clone());
    let mut candidates = vec![
        cfg.root.clone(),
        history_dir,
        cfg.results_dir.clone(),
        cfg.logs_dir.clone(),
        cfg.dumps_dir.clone(),
    ];
    candidates.sort_by_key(|p| p.components().count());
    let mut kept: Vec<std::path::PathBuf> = Vec::new();
    for candidate in candidates {
        if kept.iter().any(|k| candidate.starts_with(k)) {
            continue;
        }
        kept.push(candidate);
    }
    kept
}

/// What a set of retained batches costs the store's in-memory result budget.
///
/// `get_array_memory_size` over-counts buffers two batches share, so this is an upper bound. That
/// is the right direction for a budget: over-counting spills a little early, under-counting is
/// how a 512 MiB ceiling becomes an OOM.
fn retained_bytes(batches: &[RecordBatch]) -> u64 {
    batches
        .iter()
        .map(|b| b.get_array_memory_size() as u64)
        .sum()
}

/// `^[A-Za-z0-9._:-]{1,128}$`, hand-rolled because the tree has no regex dependency.
fn validate_alias(raw: &str) -> Option<String> {
    if raw.is_empty() || raw.len() > 128 {
        return None;
    }
    raw.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '-'))
        .then(|| raw.to_string())
}

/// Backtick-quote an identifier, stripping any existing backticks first so we
/// do not double-quote. This is Spark SQL's identifier-quoting rule.
fn quote_identifier(id: &str) -> String {
    format!("`{}`", id.replace('`', ""))
}

/// Fetch column (name, type) pairs for a fully qualified table, or None if the
/// table cannot be described.
async fn fetch_columns(
    engine: Arc<Engine>,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> Option<Vec<(String, String)>> {
    let ns_parts: Vec<&str> = namespace.split('.').collect();
    let quoted_ns = ns_parts
        .iter()
        .map(|p| quote_identifier(p))
        .collect::<Vec<_>>()
        .join(".");
    let qualified = format!(
        "{}.{n}.{t}",
        quote_identifier(catalog),
        n = quoted_ns,
        t = quote_identifier(table)
    );
    let sql = format!("DESCRIBE TABLE {qualified}");
    let batches = engine.sql(&sql).await.ok()?;
    let mut columns = Vec::new();
    for batch in batches {
        let col_names = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>();
        let data_types = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>();
        if let (Some(names), Some(types)) = (col_names, data_types) {
            for i in 0..names.len() {
                columns.push((names.value(i).to_string(), types.value(i).to_string()));
            }
        }
    }
    Some(columns)
}

/// `{"name","type"}` pairs of a result's Arrow schema (type names via `Display`, e.g. "Int64").
pub(crate) fn schema_fields(batches: &[RecordBatch]) -> Option<Vec<(String, String)>> {
    batches.first().map(|b| {
        b.schema()
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().to_string()))
            .collect()
    })
}

/// The `{"fields":[{"name","type"},...]}` schema document shared by the status and result
/// responses.
fn schema_json(schema: Option<&[(String, String)]>) -> Value {
    let fields: Vec<Value> = schema
        .unwrap_or_default()
        .iter()
        .map(|(name, ty)| json!({ "name": name, "type": ty }))
        .collect();
    json!({ "fields": fields })
}

/// The full status document: `GET /{id}` and the `?wait=true` submit response.
fn snapshot_json(s: &StatementSnapshot) -> Value {
    let mut v = json!({
        "statementId": s.id,
        "sql": s.sql,
        "status": s.status.as_str(),
        "submittedAtMs": s.submitted_at_ms,
        // Where it came from (`rest` / `connect`) and which tier answered (`hot` / `history`).
        // The tier is what tells a client a statement is still cancellable.
        "source": s.source.as_str(),
        "tier": s.tier.as_str(),
    });
    if let Some(op) = &s.client_op_id {
        v["clientOperationId"] = json!(op);
    }
    if let Some(error) = &s.error {
        v["error"] = json!(error);
    }
    if let Some(d) = s.duration_ms {
        v["durationMs"] = json!(d);
    }
    if let Some(rc) = s.row_count {
        v["rowCount"] = json!(rc);
    }
    if let Some(schema) = &s.schema {
        v["schema"] = schema_json(Some(schema));
    }
    if let Some(refused) = &s.result_refused {
        // `result_too_large`: the rows were past `OXIDANT_RESULT_MAX_BYTES`, so `/result` will
        // say `410 result_expired` once they leave memory. Saying *why* here is what keeps that
        // `410` from reading as "it merely aged out" (§5).
        v["resultStatus"] = json!(refused);
    }
    v
}

fn error_response(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "error": message }))).into_response()
}

/// Shared handler state: the Spark Connect service statements execute on, plus the registry.
#[derive(Clone)]
struct RestState {
    service: Arc<OxidantService>,
    store: StatementStore,
    log_buffer: LogBuffer,
    /// Shared bearer token guarding `GET /api/v1/logs`. `None` — the default — makes that one
    /// route answer `404`; nothing else in this router is authenticated.
    status_token: Option<Arc<str>>,
}

/// Build the REST statement-execution router around a shared Spark Connect service.
pub fn router(service: Arc<OxidantService>) -> Router {
    init_logging();
    let log_buffer = LOG_BUFFER
        .get_or_init(|| LogBuffer::new(MAX_LOG_LINES))
        .clone();
    // The statement store is attached to the service at boot ([`init_statement_store`]) so the
    // Connect path writes into the same history this router reads. A service that never had one
    // attached (an embedded caller building the router directly) gets today's volatile store.
    let store = service
        .statement_store()
        .cloned()
        .unwrap_or_else(StatementStore::new);
    app(RestState {
        service,
        store,
        log_buffer,
        status_token: oxidant_ui_server::status::status_token_from_env().map(Into::into),
    })
}

/// Build the process's statement store from the environment and attach it to `service`.
///
/// Called once at boot, before anything can execute, because both the REST API and Connect's
/// `ExecutePlan` record into it. `Err` is a boot failure with the reason already spelled out —
/// a data dir another process holds, or a root that names an object store.
pub fn init_statement_store(service: &OxidantService, role: &str, port: u16) -> Result<(), String> {
    let store = StatementStore::from_env(role, port)?;
    service.attach_statement_store(store);
    Ok(())
}

fn app(state: RestState) -> Router {
    Router::new()
        .route(
            "/api/v1/statements",
            post(submit_statement).get(list_statements),
        )
        .route("/api/v1/statements/{id}", get(get_statement))
        .route("/api/v1/statements/{id}/result", get(get_result))
        .route("/api/v1/statements/{id}/cancel", post(cancel_statement))
        .route("/api/v1/catalogs", get(list_catalogs))
        .route("/api/v1/catalogs/autocomplete", get(autocomplete_catalog))
        .route(
            "/api/v1/catalogs/{catalog}/namespaces",
            get(list_namespaces),
        )
        .route("/api/v1/catalogs/{catalog}/tables", get(list_tables))
        .route(
            "/api/v1/catalogs/{catalog}/tables/{table}/columns",
            get(list_columns),
        )
        .route("/api/v1/cluster/status", get(cluster_status))
        .route("/api/v1/logs", get(list_logs))
        .with_state(state)
}

#[derive(Debug, Deserialize)]
struct SubmitParams {
    wait: Option<bool>,
    timeout: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct SubmitBody {
    sql: String,
}

#[derive(Debug, Deserialize)]
struct ResultParams {
    format: Option<String>,
    limit: Option<usize>,
}

async fn submit_statement(
    State(state): State<RestState>,
    Query(params): Query<SubmitParams>,
    Json(body): Json<SubmitBody>,
) -> Response {
    let (id, cancel_rx) = state.store.insert(&body.sql);
    spawn_execution(state.clone(), id.clone(), body.sql, cancel_rx);
    if params.wait.unwrap_or(false) {
        let timeout = Duration::from_secs(params.timeout.unwrap_or(DEFAULT_WAIT_TIMEOUT_SECS));
        return match state.store.wait_terminal(&id, timeout).await {
            // On timeout the snapshot may still be `running` — the contract allows that.
            Some(snap) => {
                let mut body = snapshot_json(&snap);
                // The response, not the query, waits for the terminal record to be durable —
                // bounded by `OXIDANT_HISTORY_ACK_TIMEOUT_MS`. If the wait times out the answer
                // still goes out, saying plainly that history is degraded for this statement.
                if snap.status.is_terminal() && state.store.await_durable(&id).await {
                    body["history"] = json!("degraded");
                }
                (StatusCode::OK, Json(body)).into_response()
            }
            None => error_response(StatusCode::NOT_FOUND, "unknown statement id"),
        };
    }
    (
        StatusCode::ACCEPTED,
        Json(json!({ "statementId": id, "status": StatementStatus::Pending.as_str() })),
    )
        .into_response()
}

/// Run a submitted statement on a tokio task, folding the terminal outcome into the store.
/// Cancellation is cooperative: the cancel watch fires and the in-flight execution future is
/// dropped (DataFusion stops polling the query), so cancel is best-effort.
fn spawn_execution(
    state: RestState,
    id: String,
    sql: String,
    mut cancel_rx: watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        state.store.mark_running(&id);
        let service = Arc::clone(&state.service);
        let outcome = tokio::select! {
            result = service.execute_sql(&sql, &id) => match result {
                Ok((batches, _stats)) => ExecOutcome::Succeeded(batches),
                Err(e) => ExecOutcome::Failed(e.message().to_string()),
            },
            _ = cancel_rx.changed() => ExecOutcome::Canceled,
        };
        state.store.finish(&id, outcome);
    });
}

async fn list_statements(State(state): State<RestState>) -> Json<Value> {
    let statements: Vec<Value> = state
        .store
        .list()
        .iter()
        .map(|s| {
            let mut v = json!({
                "statementId": s.id,
                "sql": s.sql,
                "status": s.status.as_str(),
                "submittedAtMs": s.submitted_at_ms,
                "source": s.source.as_str(),
                "tier": s.tier.as_str(),
            });
            if let Some(op) = &s.client_op_id {
                v["clientOperationId"] = json!(op);
            }
            if let Some(d) = s.duration_ms {
                v["durationMs"] = json!(d);
            }
            v
        })
        .collect();
    Json(json!({ "statements": statements }))
}

async fn get_statement(State(state): State<RestState>, Path(id): Path<String>) -> Response {
    match state.store.snapshot(&id) {
        Some(snap) => Json(snapshot_json(&snap)).into_response(),
        None => error_response(StatusCode::NOT_FOUND, "unknown statement id"),
    }
}

/// `GET /api/v1/statements/{id}/result?format=json|csv` — memory, then disk, then `410`.
///
/// Memory → spilled file → `410 result_expired` is §5's read model in three lines. `404` still
/// means "no such id" and `409` still means "not succeeded yet"; `410` means the statement is
/// known and succeeded and its rows are gone — which is why answering `200 {"rows":[]}` here
/// would be a lie about a statement whose own status document reports `rowCount: 5`.
async fn get_result(
    State(state): State<RestState>,
    Path(id): Path<String>,
    Query(params): Query<ResultParams>,
) -> Response {
    let Some((snap, source)) = state.store.result(&id) else {
        return error_response(StatusCode::NOT_FOUND, "unknown statement id");
    };
    if snap.status != StatementStatus::Succeeded {
        return error_response(
            StatusCode::CONFLICT,
            "statement result is only available once it has succeeded",
        );
    }
    let batches = match source {
        ResultSource::Memory(batches) => batches,
        ResultSource::Disk => match state.store.read_spilled(&id).await {
            Some(batches) => batches,
            None => return error_response(StatusCode::GONE, "result_expired"),
        },
        ResultSource::Gone => return error_response(StatusCode::GONE, "result_expired"),
    };
    // The journaled schema is what the pre-restart answer carried, so it is what the post-restart
    // answer carries too; the batches' own schema is the fallback for a statement the fold has no
    // schema for.
    let schema = snap
        .schema
        .clone()
        .or_else(|| schema_fields(&batches))
        .unwrap_or_default();
    let limit = params.limit.unwrap_or(DEFAULT_RESULT_LIMIT);
    match params.format.as_deref().unwrap_or("json") {
        "json" => json_result(Some(&schema), &batches, limit),
        "csv" => csv_result(&batches, limit),
        other => error_response(
            StatusCode::BAD_REQUEST,
            &format!("unknown result format `{other}` (expected json or csv)"),
        ),
    }
}

/// Rows as an array of objects (`[{"col":value},...]`) via Arrow's JSON writer, honoring
/// `limit`; `truncated` reports whether rows were cut. `rowCount` counts the returned rows.
fn json_result(
    schema: Option<&[(String, String)]>,
    batches: &[RecordBatch],
    limit: usize,
) -> Response {
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    let truncated = total > limit;
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer: ArrayWriter<&mut Vec<u8>> = WriterBuilder::new()
            .with_explicit_nulls(true)
            .build(&mut buf);
        let mut remaining = limit;
        for batch in batches {
            if remaining == 0 {
                break;
            }
            let n = batch.num_rows().min(remaining);
            if let Err(e) = writer.write(&batch.slice(0, n)) {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("json encode: {e}"),
                );
            }
            remaining -= n;
        }
        if let Err(e) = writer.finish() {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("json encode: {e}"),
            );
        }
    }
    let rows: Value = match serde_json::from_slice(&buf) {
        Ok(v) => v,
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("json encode: {e}"),
            )
        }
    };
    let row_count = rows.as_array().map(Vec::len).unwrap_or(0);
    Json(json!({
        "schema": schema_json(schema),
        "rows": rows,
        "rowCount": row_count,
        "truncated": truncated,
    }))
    .into_response()
}

/// `text/csv` body via Arrow's CSV writer (header row on, honoring `limit`).
fn csv_result(batches: &[RecordBatch], limit: usize) -> Response {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = datafusion::arrow::csv::Writer::new(&mut buf);
        let mut remaining = limit;
        for batch in batches {
            if remaining == 0 {
                break;
            }
            let n = batch.num_rows().min(remaining);
            if let Err(e) = writer.write(&batch.slice(0, n)) {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("csv encode: {e}"),
                );
            }
            remaining -= n;
        }
        // Dropping the writer flushes the underlying csv buffer into `buf`.
    }
    match String::from_utf8(buf) {
        Ok(body) => ([(header::CONTENT_TYPE, "text/csv")], body).into_response(),
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("csv encode: {e}"),
        ),
    }
}

async fn cancel_statement(State(state): State<RestState>, Path(id): Path<String>) -> Response {
    match state.store.cancel(&id) {
        CancelOutcome::Canceled => Json(json!({
            "statementId": id,
            "status": StatementStatus::Canceled.as_str(),
        }))
        .into_response(),
        CancelOutcome::NotFound => error_response(StatusCode::NOT_FOUND, "unknown statement id"),
        CancelOutcome::AlreadyTerminal => {
            error_response(StatusCode::CONFLICT, "statement is already terminal")
        }
    }
}

// ---- catalog handlers ------------------------------------------------------

#[derive(Debug, Deserialize)]
struct NamespaceQuery {
    namespace: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AutocompleteQuery {
    prefix: String,
}

async fn list_catalogs(State(state): State<RestState>) -> Json<Value> {
    let registry = state.service.registry();
    let names = registry.catalog_names();
    let current = state
        .service
        .engine()
        .for_session(crate::REST_SESSION_ID)
        .current_catalog_and_namespace()
        .0;
    let catalogs: Vec<Value> = names
        .into_iter()
        .map(|name| {
            json!({
                "name": name,
                "isCurrent": name == current,
            })
        })
        .collect();
    Json(json!({ "catalogs": catalogs }))
}

async fn list_namespaces(
    State(state): State<RestState>,
    Path(catalog): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let engine = state.service.engine();
    let registry = state.service.registry();
    let namespaces = if catalog == DEFAULT_CATALOG {
        engine.builtin_namespaces()
    } else {
        let provider = registry.provider(&catalog).ok_or(StatusCode::NOT_FOUND)?;
        provider
            .list_namespaces(&[])
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .into_iter()
            .map(|ns| ns.join("."))
            .collect()
    };
    Ok(Json(json!({ "namespaces": namespaces })))
}

async fn list_tables(
    State(state): State<RestState>,
    Path(catalog): Path<String>,
    Query(q): Query<NamespaceQuery>,
) -> Result<Json<Value>, StatusCode> {
    let engine = state.service.engine();
    let registry = state.service.registry();
    let namespace = q.namespace.as_deref().unwrap_or("default");
    // Namespaces are dot-joined everywhere else; split on '.' for consistency.
    let ns_parts: Vec<String> = namespace.split('.').map(|s| s.to_string()).collect();
    let table_names = if catalog == DEFAULT_CATALOG {
        let schema = ns_parts
            .last()
            .cloned()
            .unwrap_or_else(|| "default".to_string());
        engine.builtin_table_names(&schema)
    } else {
        let provider = registry.provider(&catalog).ok_or(StatusCode::NOT_FOUND)?;
        provider
            .list_tables(&ns_parts)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    };
    let tables: Vec<Value> = table_names
        .into_iter()
        .map(|name| json!({ "name": name, "type": "TABLE" }))
        .collect();
    Ok(Json(json!({ "tables": tables })))
}

async fn list_columns(
    State(state): State<RestState>,
    Path((catalog, table)): Path<(String, String)>,
    Query(q): Query<NamespaceQuery>,
) -> Response {
    let engine = state.service.engine();
    let namespace = q.namespace.as_deref().unwrap_or("default");
    match fetch_columns(engine, &catalog, namespace, &table).await {
        Some(columns) => {
            let columns: Vec<Value> = columns
                .into_iter()
                .map(|(name, ty)| json!({ "name": name, "type": ty }))
                .collect();
            Json(json!({ "columns": columns })).into_response()
        }
        None => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "describe table: unable to fetch columns",
        ),
    }
}

async fn autocomplete_catalog(
    State(state): State<RestState>,
    Query(q): Query<AutocompleteQuery>,
) -> Json<Value> {
    let engine = state.service.engine();
    let registry = state.service.registry();
    let prefix = q.prefix.trim();
    let mut suggestions = Vec::new();

    // Tokenize the prefix on dots (last token is the partial identifier).
    let parts: Vec<&str> = prefix.split('.').collect();
    let partial = parts.last().copied().unwrap_or("").to_ascii_lowercase();

    // Helper to push matches.
    let mut push = |kind: &str, name: &str, qualified: String| {
        if partial.is_empty() || name.to_ascii_lowercase().starts_with(&partial) {
            suggestions.push(json!({
                "kind": kind,
                "name": name,
                "qualified": qualified,
            }));
        }
    };

    // The REST session's current catalog (KAN-85) — shared by both branches below.
    let current = engine
        .for_session(crate::REST_SESSION_ID)
        .current_catalog_and_namespace()
        .0;

    if parts.len() <= 1 {
        // Suggest catalogs.
        for name in registry.catalog_names() {
            push("catalog", &name, name.clone());
        }
        // Suggest namespaces in the current catalog.
        let namespaces = if current == DEFAULT_CATALOG {
            engine.builtin_namespaces()
        } else if let Some(provider) = registry.provider(&current) {
            provider
                .list_namespaces(&[])
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|ns| ns.join("."))
                .collect()
        } else {
            Vec::new()
        };
        for ns in namespaces {
            push("namespace", &ns, format!("{current}.{ns}"));
        }
    } else {
        // We have a catalog (and maybe namespace/table) prefix. Try to resolve it.
        let first = parts[0];
        let (catalog, namespace_parts) = if registry.contains(first) {
            (first.to_string(), parts[1..parts.len() - 1].to_vec())
        } else {
            (current.clone(), parts[..parts.len() - 1].to_vec())
        };
        let namespace_str = namespace_parts.join(".");
        let ns_vec: Vec<String> = namespace_parts.iter().map(|s| s.to_string()).collect();

        if namespace_parts.is_empty() {
            // Suggest namespaces for the catalog.
            let namespaces = if catalog == DEFAULT_CATALOG {
                engine.builtin_namespaces()
            } else if let Some(provider) = registry.provider(&catalog) {
                provider
                    .list_namespaces(&[])
                    .await
                    .unwrap_or_default()
                    .into_iter()
                    .map(|ns| ns.join("."))
                    .collect()
            } else {
                Vec::new()
            };
            for ns in namespaces {
                push("namespace", &ns, format!("{catalog}.{ns}"));
            }
        }

        let mut resolved_table = false;
        if let Some(table) = namespace_parts.last() {
            // The last part might be a table; if so, suggest its columns.
            let col_namespace = namespace_parts[..namespace_parts.len() - 1].join(".");
            if let Some(columns) =
                fetch_columns(Arc::clone(&engine), &catalog, &col_namespace, table).await
            {
                resolved_table = true;
                for (name, _ty) in columns {
                    let qualified = if namespace_str.is_empty() {
                        format!("{catalog}.{table}.{name}")
                    } else {
                        format!("{catalog}.{namespace_str}.{name}")
                    };
                    push("column", &name, qualified);
                }
            }
        }

        // If the last part was not a resolvable table, fall back to table suggestions.
        if !resolved_table {
            let table_namespace = if namespace_parts.len() > 1 {
                namespace_parts[..namespace_parts.len() - 1].join(".")
            } else {
                namespace_str.clone()
            };
            let table_ns_vec: Vec<String> = if namespace_parts.len() > 1 {
                namespace_parts[..namespace_parts.len() - 1]
                    .iter()
                    .map(|s| s.to_string())
                    .collect()
            } else {
                ns_vec.clone()
            };
            let table_names = if catalog == DEFAULT_CATALOG {
                let schema = table_ns_vec
                    .last()
                    .cloned()
                    .unwrap_or_else(|| "default".to_string());
                engine.builtin_table_names(&schema)
            } else if let Some(provider) = registry.provider(&catalog) {
                provider
                    .list_tables(&table_ns_vec)
                    .await
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
            for t in table_names {
                let qualified = if table_namespace.is_empty() {
                    format!("{catalog}.{t}")
                } else {
                    format!("{catalog}.{table_namespace}.{t}")
                };
                push("table", &t, qualified);
            }
        }
    }

    Json(json!({ "suggestions": suggestions }))
}

/// "single-node" with no workers, "local-cluster" for loopback worker endpoints, else
/// "distributed".
fn cluster_mode(workers: &[String]) -> &'static str {
    if workers.is_empty() {
        "single-node"
    } else if workers
        .iter()
        .any(|w| w.contains("127.0.0.1") || w.contains("localhost"))
    {
        "local-cluster"
    } else {
        "distributed"
    }
}

async fn cluster_status(State(state): State<RestState>) -> Json<Value> {
    let workers = state.service.workers_from_config();
    let (memory_mb, memory_total_mb, cpu_pct) = process_metrics();
    Json(json!({
        "mode": cluster_mode(&workers),
        "workers": workers,
        "version": env!("CARGO_PKG_VERSION"),
        "process": {
            "memoryUsedMb": memory_mb,
            "memoryTotalMb": memory_total_mb,
            "cpuPercent": cpu_pct,
        }
    }))
}

/// Snapshot current process CPU and memory via sysinfo. Uses a cached `System`
/// so that successive calls can compute a delta-based CPU percentage.
fn process_metrics() -> (Option<u64>, Option<u64>, Option<f32>) {
    let pid = Pid::from_u32(std::process::id());
    let mut sys = SYSTEM
        .get_or_init(|| std::sync::Mutex::new(system_snapshot()))
        .lock()
        .expect("system metrics poisoned");
    sys.refresh_processes_specifics(
        sysinfo::ProcessesToUpdate::Some(&[pid]),
        false,
        sysinfo::ProcessRefreshKind::everything(),
    );
    sys.process(pid)
        .map(|p| {
            let mem = p.memory();
            let total = sys.total_memory();
            (
                Some(mem / 1024 / 1024),
                Some(total / 1024 / 1024),
                Some(p.cpu_usage()),
            )
        })
        .unwrap_or((None, None, None))
}

/// `GET /api/v1/logs` — the driver's `tracing` ring buffer, for the monitoring UI's
/// Observability page.
///
/// Gated by the same shared token as `/api/status`, through the same code: this is the
/// driver's own log, not monitoring decoration.
async fn list_logs(State(state): State<RestState>, headers: header::HeaderMap) -> Response {
    if let Some(denied) =
        oxidant_ui_server::status::deny_unless_authorized(state.status_token.as_deref(), &headers)
    {
        return denied;
    }
    Json(json!({ "logs": state.log_buffer.lines() })).into_response()
}

#[cfg(test)]
#[allow(clippy::await_holding_lock)] // env_lock() serializes process-global env across async tests
mod tests {
    use std::sync::MutexGuard;

    use super::*;
    use axum::body::Body;
    use http_body_util::BodyExt;
    use oxidant_proto::spark::connect as sc;
    use sc::spark_connect_service_server::SparkConnectService;
    use tower::ServiceExt;

    /// An ephemeral loopback port, taken by binding and immediately releasing it — the
    /// fixed-port schemes elsewhere in this workspace collide when two `cargo test` runs
    /// overlap.
    fn ephemeral_port() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        listener.local_addr().expect("local_addr").port()
    }

    /// [`test_state`] for a driver with `workers` attached, so statements take the
    /// distributed path.
    fn test_state_with_workers(
        workers: Vec<String>,
    ) -> (MutexGuard<'static, ()>, RestState, Router) {
        let guard = crate::distributed::env_lock();
        let mut service = OxidantService::new();
        service.workers = workers;
        let state = RestState {
            service: Arc::new(service),
            store: StatementStore::new(),
            log_buffer: LogBuffer::new(MAX_LOG_LINES),
            status_token: None,
        };
        (guard, state.clone(), app(state))
    }

    /// Build the test router, holding the process-global env lock for the caller's test.
    ///
    /// These tests execute real SQL, and the engine reads `OXIDANT_DISTRIBUTED_STRICT` /
    /// `OXIDANT_WORKERS` / `OXIDANT_WORKER_SERVICE` at query time. `distributed::tests`
    /// mutates exactly those vars, and cargo runs both modules as threads in ONE process,
    /// so without this lock a sibling's in-flight mutation intermittently made these
    /// queries fail with a strict-mode refusal. Callers must bind the guard for the whole
    /// test body (`let (_env, _state, app) = test_state();`).
    fn test_state() -> (MutexGuard<'static, ()>, RestState, Router) {
        let guard = crate::distributed::env_lock();
        let state = RestState {
            service: Arc::new(OxidantService::new()),
            store: StatementStore::new(),
            log_buffer: LogBuffer::new(MAX_LOG_LINES),
            status_token: None,
        };
        (guard, state.clone(), app(state))
    }

    async fn post_json(app: &Router, uri: &str, body: Value) -> (StatusCode, Value) {
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    async fn post_empty(app: &Router, uri: &str) -> (StatusCode, Value) {
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    async fn get_raw(app: &Router, uri: &str) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("GET")
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let headers = resp.headers().clone();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        (status, headers, bytes.to_vec())
    }

    async fn get_json(app: &Router, uri: &str) -> (StatusCode, Value) {
        let (status, _, bytes) = get_raw(app, uri).await;
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    #[tokio::test]
    async fn submit_wait_and_fetch_json_result() {
        let (_env, _state, app) = test_state();
        let (status, body) = post_json(
            &app,
            "/api/v1/statements?wait=true",
            json!({ "sql": "SELECT 1 AS hello" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "succeeded");
        let id = body["statementId"].as_str().unwrap().to_string();
        assert_eq!(body["rowCount"], 1);
        assert_eq!(body["schema"]["fields"][0]["name"], "hello");
        // Spark integer-literal semantics: `SELECT 1` is Int32, not DataFusion's Int64.
        assert_eq!(body["schema"]["fields"][0]["type"], "Int32");
        assert!(body["submittedAtMs"].as_i64().unwrap() > 0);
        assert!(body["durationMs"].as_i64().is_some());

        let (status, result) = get_json(&app, &format!("/api/v1/statements/{id}/result")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result["rowCount"], 1);
        assert_eq!(result["truncated"], false);
        assert_eq!(result["rows"].as_array().unwrap().len(), 1);
        assert_eq!(result["rows"][0]["hello"], 1);
        assert_eq!(result["schema"]["fields"][0]["name"], "hello");
    }

    #[tokio::test]
    async fn submit_without_wait_returns_202_pending() {
        let (_env, _state, app) = test_state();
        let (status, body) = post_json(
            &app,
            "/api/v1/statements",
            json!({ "sql": "SELECT 1 AS hello" }),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(body["status"], "pending");
        assert!(!body["statementId"].as_str().unwrap().is_empty());
    }

    #[tokio::test]
    async fn invalid_sql_fails_and_result_conflicts() {
        let (_env, _state, app) = test_state();
        let (status, body) = post_json(
            &app,
            "/api/v1/statements?wait=true",
            json!({ "sql": "SELECT * FROM table_that_does_not_exist" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "failed");
        assert!(!body["error"].as_str().unwrap().is_empty());
        let id = body["statementId"].as_str().unwrap();

        let (status, _) = get_json(&app, &format!("/api/v1/statements/{id}/result")).await;
        assert_eq!(status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn unknown_statement_id_returns_404() {
        let (_env, _state, app) = test_state();
        let id = Uuid::new_v4();
        let (status, _) = get_json(&app, &format!("/api/v1/statements/{id}")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = get_json(&app, &format!("/api/v1/statements/{id}/result")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = post_empty(&app, &format!("/api/v1/statements/{id}/cancel")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn cancel_pending_statement_then_conflict_on_second_cancel() {
        let (_env, state, app) = test_state();
        // Insert WITHOUT spawning an execution task: the statement stays pending, so the
        // cancel route is exercised deterministically (no submit/cancel race).
        let (id, _cancel_rx) = state.store.insert("SELECT 1 AS never_ran");

        let (status, body) = post_empty(&app, &format!("/api/v1/statements/{id}/cancel")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["statementId"], id.as_str());
        assert_eq!(body["status"], "canceled");

        let (status, body) = get_json(&app, &format!("/api/v1/statements/{id}")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "canceled");
        assert!(body["durationMs"].as_i64().is_some());

        // Terminal statements reject cancel.
        let (status, _) = post_empty(&app, &format!("/api/v1/statements/{id}/cancel")).await;
        assert_eq!(status, StatusCode::CONFLICT);
        // ... and never expose a result.
        let (status, _) = get_json(&app, &format!("/api/v1/statements/{id}/result")).await;
        assert_eq!(status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn cancel_terminal_statement_conflicts() {
        let (_env, _state, app) = test_state();
        let (_, body) = post_json(
            &app,
            "/api/v1/statements?wait=true",
            json!({ "sql": "SELECT 1 AS hello" }),
        )
        .await;
        assert_eq!(body["status"], "succeeded");
        let id = body["statementId"].as_str().unwrap();
        let (status, _) = post_empty(&app, &format!("/api/v1/statements/{id}/cancel")).await;
        assert_eq!(status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn cluster_status_reports_single_node_and_process_metrics() {
        let (_env, _state, app) = test_state();
        let (status, body) = get_json(&app, "/api/v1/cluster/status").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["mode"], "single-node");
        assert_eq!(body["workers"], json!([]));
        assert!(!body["version"].as_str().unwrap().is_empty());
        assert!(body["process"]["memoryUsedMb"].as_u64().is_some());
        assert!(body["process"]["memoryTotalMb"].as_u64().is_some());
    }

    /// The driver's log buffer carries every `tracing` field value — hosts, slots, tables,
    /// query text — and this router is served under a permissive CORS layer, so while it was
    /// ungated any origin an operator's browser visited could read it cross-site. It is gated
    /// exactly like `/api/status` and the pipeline routes, by the same code: no token
    /// configured is `404` (the route does not exist), a missing or wrong credential is `401`
    /// with the scheme advertised, and the token the UI stores works as a bearer header.
    #[tokio::test]
    async fn logs_endpoint_is_gated_by_the_status_token() {
        const TOKEN: &str = "s3cret-status-token";
        let (_env, state, ungated) = test_state();
        assert_eq!(
            get_json(&ungated, "/api/v1/logs").await.0,
            StatusCode::NOT_FOUND
        );

        let mut gated_state = state.clone();
        gated_state.status_token = Some(TOKEN.into());
        let gated = app(gated_state);

        for auth in [None, Some("Bearer wrong"), Some("Basic x")] {
            let mut req = axum::http::Request::builder().uri("/api/v1/logs");
            if let Some(auth) = auth {
                req = req.header(header::AUTHORIZATION, auth);
            }
            let resp = gated
                .clone()
                .oneshot(req.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{auth:?}");
            assert_eq!(
                resp.headers().get(header::WWW_AUTHENTICATE).unwrap(),
                "Bearer",
                "{auth:?}"
            );
        }

        let resp = gated
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/v1/logs")
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert!(body["logs"].is_array());
    }

    #[test]
    fn cluster_mode_classification() {
        assert_eq!(cluster_mode(&[]), "single-node");
        assert_eq!(
            cluster_mode(&["http://127.0.0.1:9001".to_string()]),
            "local-cluster"
        );
        assert_eq!(
            cluster_mode(&["localhost:9001".to_string()]),
            "local-cluster"
        );
        assert_eq!(cluster_mode(&["10.0.0.5:9001".to_string()]), "distributed");
    }

    #[tokio::test]
    async fn csv_result_includes_header_row() {
        let (_env, _state, app) = test_state();
        let (_, body) = post_json(
            &app,
            "/api/v1/statements?wait=true",
            json!({ "sql": "SELECT 1 AS hello" }),
        )
        .await;
        let id = body["statementId"].as_str().unwrap();
        let (status, headers, bytes) =
            get_raw(&app, &format!("/api/v1/statements/{id}/result?format=csv")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            headers.get(header::CONTENT_TYPE).unwrap().to_str().unwrap(),
            "text/csv"
        );
        let text = String::from_utf8(bytes).unwrap();
        let mut lines = text.lines();
        assert_eq!(lines.next(), Some("hello"));
        assert_eq!(lines.next(), Some("1"));
    }

    #[tokio::test]
    async fn list_statements_newest_first() {
        let (_env, _state, app) = test_state();
        let mut ids = Vec::new();
        for n in [1, 2] {
            let (_, body) = post_json(
                &app,
                "/api/v1/statements?wait=true",
                json!({ "sql": format!("SELECT {n} AS v") }),
            )
            .await;
            ids.push(body["statementId"].as_str().unwrap().to_string());
        }
        let (status, body) = get_json(&app, "/api/v1/statements").await;
        assert_eq!(status, StatusCode::OK);
        let statements = body["statements"].as_array().unwrap();
        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0]["statementId"], ids[1].as_str());
        assert_eq!(statements[1]["statementId"], ids[0].as_str());
        assert_eq!(statements[0]["status"], "succeeded");
        assert!(statements[0]["submittedAtMs"].as_i64().unwrap() > 0);
        assert!(statements[0]["durationMs"].as_i64().is_some());
    }

    #[tokio::test]
    async fn result_limit_truncates() {
        let (_env, _state, app) = test_state();
        let (_, body) = post_json(
            &app,
            "/api/v1/statements?wait=true",
            json!({ "sql": "SELECT * FROM (VALUES (1), (2), (3)) AS t(v)" }),
        )
        .await;
        assert_eq!(body["status"], "succeeded");
        let id = body["statementId"].as_str().unwrap();
        let (status, result) =
            get_json(&app, &format!("/api/v1/statements/{id}/result?limit=2")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result["rowCount"], 2);
        assert_eq!(result["truncated"], true);
        assert_eq!(result["rows"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn store_evicts_oldest_beyond_cap() {
        let store = StatementStore::new();
        let (first_id, _) = store.insert("SELECT 0");
        let mut last_id = first_id.clone();
        for _ in 0..MAX_STATEMENTS {
            let (id, _) = store.insert("SELECT 1");
            last_id = id;
        }
        let inner = store.inner.lock().expect("statement store poisoned");
        assert_eq!(inner.statements.len(), MAX_STATEMENTS);
        assert!(!inner.statements.contains_key(&first_id));
        assert!(inner.statements.contains_key(&last_id));
    }

    #[tokio::test]
    async fn catalog_list_includes_spark_catalog() {
        let (_env, _state, app) = test_state();
        let (status, body) = get_json(&app, "/api/v1/catalogs").await;
        assert_eq!(status, StatusCode::OK);
        let catalogs = body["catalogs"].as_array().unwrap();
        assert!(catalogs.iter().any(|c| c["name"] == "spark_catalog"));
    }

    #[tokio::test]
    async fn catalog_namespaces_and_tables() {
        let (_env, _state, app) = test_state();
        // Create a temp view so the default namespace has a table.
        let (_, _) = post_json(
            &app,
            "/api/v1/statements?wait=true",
            json!({ "sql": "CREATE OR REPLACE TEMP VIEW rest_cat_v AS SELECT 1 AS a, 'x' AS b" }),
        )
        .await;

        let (status, body) = get_json(&app, "/api/v1/catalogs/spark_catalog/namespaces").await;
        assert_eq!(status, StatusCode::OK);
        let namespaces = body["namespaces"].as_array().unwrap();
        assert!(namespaces.iter().any(|n| n == "default"));

        let (status, body) = get_json(
            &app,
            "/api/v1/catalogs/spark_catalog/tables?namespace=default",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let tables = body["tables"].as_array().unwrap();
        assert!(tables.iter().any(|t| t["name"] == "rest_cat_v"));

        let (status, body) = get_json(
            &app,
            "/api/v1/catalogs/spark_catalog/tables/rest_cat_v/columns?namespace=default",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let columns = body["columns"].as_array().unwrap();
        assert!(columns.iter().any(|c| c["name"] == "a"));
        assert!(columns.iter().any(|c| c["name"] == "b"));
    }

    #[tokio::test]
    async fn catalog_autocomplete_suggests_catalogs_and_tables() {
        let (_env, _state, app) = test_state();
        let (_, _) = post_json(
            &app,
            "/api/v1/statements?wait=true",
            json!({ "sql": "CREATE OR REPLACE TEMP VIEW acme_orders AS SELECT 1 AS id" }),
        )
        .await;

        // Empty prefix suggests catalogs.
        let (status, body) = get_json(&app, "/api/v1/catalogs/autocomplete?prefix=").await;
        assert_eq!(status, StatusCode::OK);
        let suggestions = body["suggestions"].as_array().unwrap();
        assert!(suggestions
            .iter()
            .any(|s| s["kind"] == "catalog" && s["name"] == "spark_catalog"));

        // Namespace-qualified prefix suggests tables.
        let (status, body) = get_json(
            &app,
            "/api/v1/catalogs/autocomplete?prefix=spark_catalog.default.ac",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let suggestions = body["suggestions"].as_array().unwrap();
        assert!(suggestions
            .iter()
            .any(|s| s["kind"] == "table" && s["name"] == "acme_orders"));

        // Table-qualified prefix suggests columns.
        let (status, body) = get_json(
            &app,
            "/api/v1/catalogs/autocomplete?prefix=spark_catalog.default.acme_orders.i",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let suggestions = body["suggestions"].as_array().unwrap();
        assert!(suggestions
            .iter()
            .any(|s| s["kind"] == "column" && s["name"] == "id"));
    }

    /// Regression for issue #130: a `CREATE TABLE … AS SELECT` submitted through the statement
    /// API must be readable by a *later* statement on the same server. The write and the read
    /// are separate REST requests, which is exactly the split the bug lived in — `SHOW TABLES`
    /// listed the table while `SELECT` reported it missing.
    #[tokio::test]
    async fn ctas_is_readable_by_a_later_statement() {
        let (_env, _state, app) = test_state();

        for sql in [
            "CREATE SCHEMA ctas_demo",
            "CREATE TABLE ctas_demo.sales USING parquet AS \
             SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)",
        ] {
            let (status, body) =
                post_json(&app, "/api/v1/statements?wait=true", json!({ "sql": sql })).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body["status"], "succeeded", "`{sql}` failed: {body}");
        }

        // The metastore lists it ...
        let (status, body) = get_json(
            &app,
            "/api/v1/catalogs/spark_catalog/tables?namespace=ctas_demo",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            body["tables"]
                .as_array()
                .unwrap()
                .iter()
                .any(|t| t["name"] == "sales"),
            "SHOW TABLES equivalent did not list the CTAS table: {body}"
        );

        // ... and so must a plain SELECT in a second statement.
        let (status, body) = post_json(
            &app,
            "/api/v1/statements?wait=true",
            json!({ "sql": "SELECT id, name FROM ctas_demo.sales ORDER BY id" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "succeeded", "read-back failed: {body}");
        assert_eq!(body["rowCount"], 2);

        let id = body["statementId"].as_str().unwrap().to_string();
        let (status, result) = get_json(&app, &format!("/api/v1/statements/{id}/result")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result["rows"][0]["id"], 1);
        assert_eq!(result["rows"][0]["name"], "a");
        assert_eq!(result["rows"][1]["id"], 2);
        assert_eq!(result["rows"][1]["name"], "b");
    }

    /// The #130 repro, and the reason the fix lives in the distributed decision rather than in
    /// the catalog: with workers attached, `SELECT * FROM ctas_demo.sales` used to be split into
    /// stage SQL and shipped to a worker whose engine has never heard of `ctas_demo.sales`,
    /// coming back as `do_get: … table 'spark_catalog.ctas_demo.sales' not found` one statement
    /// after the CTAS reported success. The worker here is a bare `Engine` — exactly a real
    /// worker's view of a table the driver created for itself.
    #[tokio::test]
    async fn ctas_is_readable_by_a_later_statement_with_workers_attached() {
        let worker_port = ephemeral_port();
        tokio::spawn(async move {
            let _ = oxidant_execution::flight::serve_worker(
                worker_port,
                Arc::new(oxidant_loom::Engine::new()),
            )
            .await;
        });
        let (_env, _state, app) =
            test_state_with_workers(vec![format!("http://127.0.0.1:{worker_port}")]);
        // The worker's Flight listener comes up asynchronously; the driver's membership probe
        // silently drops an unreachable endpoint, which would take the query local and pass the
        // test for the wrong reason.
        for _ in 0..100 {
            if std::net::TcpStream::connect(("127.0.0.1", worker_port)).is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        for sql in [
            "CREATE SCHEMA ctas_demo_dist",
            "CREATE TABLE ctas_demo_dist.sales USING parquet AS \
             SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)",
        ] {
            let (status, body) =
                post_json(&app, "/api/v1/statements?wait=true", json!({ "sql": sql })).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body["status"], "succeeded", "`{sql}` failed: {body}");
        }

        let (status, body) = post_json(
            &app,
            "/api/v1/statements?wait=true",
            json!({ "sql": "SELECT * FROM ctas_demo_dist.sales" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "succeeded", "read-back failed: {body}");
        assert_eq!(body["rowCount"], 2);

        // The bare (no `USING`) form registers a DataFusion `MemTable` instead, and is just as
        // invisible to a worker — issue #130 reports both shapes failing identically.
        for sql in [
            "CREATE SCHEMA ctas_demo_mem",
            "CREATE TABLE ctas_demo_mem.sales AS \
             SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)",
        ] {
            let (status, body) =
                post_json(&app, "/api/v1/statements?wait=true", json!({ "sql": sql })).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body["status"], "succeeded", "`{sql}` failed: {body}");
        }
        let (status, body) = post_json(
            &app,
            "/api/v1/statements?wait=true",
            json!({ "sql": "SELECT count(*) AS n FROM ctas_demo_mem.sales" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["status"], "succeeded",
            "MemTable read-back failed: {body}"
        );
        let id = body["statementId"].as_str().unwrap().to_string();
        let (_, result) = get_json(&app, &format!("/api/v1/statements/{id}/result")).await;
        assert_eq!(result["rows"][0]["n"], 2);
    }

    // ---- Durable statement history (docs/query-history-durability.md, PR1) ----

    /// A durable store rooted at a tempdir, with the shipped defaults.
    fn history_store(dir: &std::path::Path) -> StatementStore {
        StatementStore::with_history(HistoryConfig::for_root(dir)).expect("boot history")
    }

    fn history_store_with(
        dir: &std::path::Path,
        tune: impl FnOnce(&mut HistoryConfig),
    ) -> StatementStore {
        let mut cfg = HistoryConfig::for_root(dir);
        tune(&mut cfg);
        StatementStore::with_history(cfg).expect("boot history")
    }

    /// The #134 acceptance: a Connect `SqlCommand` joins the statements rail, tagged
    /// `source: "connect"`, and is still there after the process restarts.
    #[tokio::test]
    async fn a_connect_sql_command_lands_in_the_rail_and_survives_a_restart() {
        let _env = crate::distributed::env_lock();
        let dir = tempfile::tempdir().expect("tempdir");
        let service = Arc::new(OxidantService::new());
        service.attach_statement_store(history_store(dir.path()));

        // Exactly the shape PySpark's `spark.sql(...)` sends.
        let request = tonic::Request::new(sc::ExecutePlanRequest {
            session_id: "sess-1".to_string(),
            operation_id: Some("op-1".to_string()),
            plan: Some(sc::Plan {
                op_type: Some(sc::plan::OpType::Command(sc::Command {
                    command_type: Some(sc::command::CommandType::SqlCommand(sc::SqlCommand {
                        input: Some(crate::sql_relation("SELECT 1 AS n")),
                        ..Default::default()
                    })),
                })),
            }),
            ..Default::default()
        });
        <OxidantService as SparkConnectService>::execute_plan(&service, request)
            .await
            .expect("execute_plan");

        // It is on the rail, over the real HTTP route, before any restart.
        let state = RestState {
            service: Arc::clone(&service),
            store: service.statement_store().expect("attached").clone(),
            log_buffer: LogBuffer::new(MAX_LOG_LINES),
            status_token: None,
        };
        let (status, body) = get_json(&app(state), "/api/v1/statements").await;
        assert_eq!(status, StatusCode::OK);
        let rows = body["statements"].as_array().expect("statements");
        let connect_row = rows
            .iter()
            .find(|r| r["sql"] == "SELECT 1 AS n")
            .expect("the connect statement is listed");
        assert_eq!(connect_row["source"], "connect");
        assert_eq!(connect_row["tier"], "hot");
        assert_eq!(connect_row["clientOperationId"], "op-1");
        let id = connect_row["statementId"].as_str().expect("id").to_string();
        assert!(id.starts_with("stmt-"), "engine-minted id, got {id}");

        // Restart: same data dir, brand new store.
        service
            .statement_store()
            .expect("attached")
            .shutdown_for_test();
        drop(service);
        let replayed = history_store(dir.path());
        let row = replayed
            .list()
            .into_iter()
            .find(|s| s.id == id)
            .expect("the connect statement survived the restart");
        assert_eq!(row.source, Source::Connect);
        assert_eq!(row.tier, Tier::History);
        assert_eq!(row.sql, "SELECT 1 AS n");
        assert_eq!(row.status, StatementStatus::Succeeded);
        assert_eq!(row.client_op_id.as_deref(), Some("op-1"));
        // And the alias index came back with it.
        assert_eq!(replayed.resolve_alias("sess-1", "op-1"), Some(id));
    }

    /// The F5 regression: replay that the first new submit deletes is not replay.
    #[tokio::test]
    async fn replayed_history_survives_the_first_new_submit() {
        let dir = tempfile::tempdir().expect("tempdir");
        // A hot TTL of zero makes every eviction path fire on the very next insert.
        let store = history_store_with(dir.path(), |c| c.hot_ttl = Duration::from_millis(0));
        let (old_id, _) = store.insert("SELECT 'replayed'");
        store.finish(&old_id, ExecOutcome::Succeeded(Vec::new()));
        store.shutdown_for_test();
        drop(store);

        let store = history_store_with(dir.path(), |c| c.hot_ttl = Duration::from_millis(0));
        assert_eq!(store.list().len(), 1, "replayed into the history tier");
        let (_new, _) = store.insert("SELECT 'fresh'");
        let listed = store.list();
        assert!(
            listed.iter().any(|s| s.id == old_id),
            "the replayed statement must outlive the first new submit: {:?}",
            listed.iter().map(|s| s.sql.clone()).collect::<Vec<_>>()
        );
        let inner = store.inner.lock().expect("lock");
        assert_eq!(inner.history.len(), 1, "eviction touched only the hot tier");
    }

    /// Eviction age is wall-clock, so a statement journaled 40 days ago folds and ages without
    /// any `Instant` reconstruction (which would saturate past process uptime).
    #[test]
    fn retention_ages_on_submitted_at_ms_and_never_evicts_a_running_statement() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store_with(dir.path(), |c| c.max_records = 2);
        let (running_a, _) = store.insert("SELECT 'a'");
        store.mark_running(&running_a);
        let (done_b, _) = store.insert("SELECT 'b'");
        store.finish(&done_b, ExecOutcome::Failed("boom".to_string()));
        let (running_c, _) = store.insert("SELECT 'c'");
        store.mark_running(&running_c);
        let (done_d, _) = store.insert("SELECT 'd'");
        store.finish(&done_d, ExecOutcome::Succeeded(Vec::new()));

        let inner = store.inner.lock().expect("lock");
        assert!(
            inner.statements.contains_key(&running_a) && inner.statements.contains_key(&running_c),
            "a running statement is never the eviction victim"
        );
        assert!(
            inner.history.contains_key(&done_b),
            "the terminal statement was demoted, not dropped"
        );
        drop(inner);

        // A 40-day-old terminal statement ages out on `submitted_at_ms` alone.
        {
            let mut inner = store.inner.lock().expect("lock");
            let fortyish = oxidant_observability::now_ms() - 40 * 86_400_000;
            if let Some(st) = inner.history.get_mut(&done_b) {
                st.submitted_at_ms = fortyish;
            }
            inner.last_sweep_ms = 0;
        }
        store.sweep_history();
        assert!(
            !store
                .inner
                .lock()
                .expect("lock")
                .history
                .contains_key(&done_b),
            "past OXIDANT_HISTORY_RETENTION_DAYS the statement is pruned"
        );
        assert!(
            store.snapshot(&running_a).is_some(),
            "and the running statement is still there"
        );
    }

    /// `OXIDANT_HISTORY=off` is today's store: no journal, 1000 statements, the 1 h TTL.
    #[test]
    fn history_off_reverts_to_todays_behaviour() {
        let _env = crate::distributed::env_lock();
        std::env::set_var("OXIDANT_HISTORY", "off");
        let store = StatementStore::from_env("driver", 0).expect("store");
        std::env::remove_var("OXIDANT_HISTORY");
        assert!(store.history.is_none(), "no journal");
        {
            let inner = store.inner.lock().expect("lock");
            assert_eq!(inner.limits.max_records, MAX_STATEMENTS);
            assert_eq!(inner.limits.hot_ttl, STATEMENT_TTL);
            assert!(!inner.limits.history_on);
        }
        for _ in 0..MAX_STATEMENTS + 5 {
            store.insert("SELECT 1");
        }
        let inner = store.inner.lock().expect("lock");
        assert_eq!(inner.statements.len(), MAX_STATEMENTS, "today's cap");
        assert!(inner.history.is_empty(), "no history tier");
    }

    /// M5: with `OXIDANT_HISTORY=off` the alias map had no pruner at all — `sweep_history`
    /// returns immediately and `demote` drops the hot statement without touching `history` — so
    /// it grew forever. Both halves of the key are client-supplied over Spark Connect, so the
    /// growth rate is one entry per `ExecutePlan` with a fresh `operation_id`.
    ///
    /// §8 says `off` restores today's behaviour exactly, and today there is no alias map at all.
    #[test]
    fn history_off_keeps_no_alias_entries() {
        let _env = crate::distributed::env_lock();
        std::env::set_var("OXIDANT_HISTORY", "off");
        let store = StatementStore::from_env("driver", 0).expect("store");
        std::env::remove_var("OXIDANT_HISTORY");

        for i in 0..(MAX_STATEMENTS + 500) {
            store.insert_from(
                "SELECT 1",
                Source::Connect,
                Some("sess-1"),
                Some(&format!("op-{i}")),
            );
        }
        let inner = store.inner.lock().expect("lock");
        assert!(
            inner.alias.is_empty(),
            "history off keeps no aliases, got {}",
            inner.alias.len()
        );
        // The hot tier is still capped exactly as it is today.
        assert_eq!(inner.statements.len(), MAX_STATEMENTS);
    }

    /// The alias map is bounded with history *on* too: an entry never outlives the statement it
    /// points at, on either eviction path.
    #[test]
    fn an_alias_never_outlives_its_statement() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store_with(dir.path(), |c| {
            c.max_records = 4;
            c.hot_ttl = Duration::from_millis(0);
        });
        for i in 0..40 {
            let (id, _) = store.insert_from(
                &format!("SELECT {i}"),
                Source::Connect,
                Some("sess-1"),
                Some(&format!("op-{i}")),
            );
            store.finish(&id, ExecOutcome::Succeeded(Vec::new()));
        }
        store.sweep_history();

        let inner = store.inner.lock().expect("lock");
        assert!(
            inner.alias.len() <= inner.limits.max_records * 2,
            "the alias map is bounded by the record caps, got {}",
            inner.alias.len()
        );
        for target in inner.alias.values() {
            assert!(
                inner.statements.contains_key(target) || inner.history.contains_key(target),
                "alias points at {target}, which is in neither tier"
            );
        }
    }

    /// Two sessions that both said `op-1` are two statements, not one merged entry.
    #[test]
    fn the_alias_key_is_the_session_and_op_id_pair() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store(dir.path());
        let (one, _) = store.insert_from("SELECT 1", Source::Connect, Some("s1"), Some("op-1"));
        let (two, _) = store.insert_from("SELECT 2", Source::Connect, Some("s2"), Some("op-1"));
        assert_ne!(one, two);
        assert_eq!(store.resolve_alias("s1", "op-1").as_ref(), Some(&one));
        assert_eq!(store.resolve_alias("s2", "op-1").as_ref(), Some(&two));
        assert_eq!(store.list().len(), 2);
    }

    /// A client string never reaches a path: the id is engine-minted and a traversal-shaped
    /// alias is recorded as null rather than failing the query.
    #[test]
    fn a_traversal_shaped_operation_id_is_recorded_as_null() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store(dir.path());
        let (id, _) = store.insert_from(
            "SELECT 1",
            Source::Connect,
            Some("s1"),
            Some("../../../../home/oxidant/.ssh/authorized_keys"),
        );
        assert!(id.starts_with("stmt-"));
        assert!(
            id.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
            "the id is [a-z0-9-] by construction: {id}"
        );
        let snap = store.snapshot(&id).expect("statement runs anyway");
        assert_eq!(snap.client_op_id, None);
        assert!(store
            .resolve_alias("s1", "../../../../home/oxidant/.ssh/authorized_keys")
            .is_none());
    }

    /// `OXIDANT_HISTORY_SQL=redacted` keeps a credential out of the file, not just out of the
    /// response.
    #[test]
    fn redacted_sql_mode_keeps_the_secret_off_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store_with(dir.path(), |c| c.sql_mode = SqlMode::Redacted);
        let (id, _) = store.insert("CREATE TABLE t USING delta OPTIONS(secret 'hunter2')");
        store.finish(&id, ExecOutcome::Succeeded(Vec::new()));
        store.shutdown_for_test();
        drop(store);

        let journal_dir = dir.path().join("history").join("statements");
        let mut on_disk = String::new();
        for entry in std::fs::read_dir(&journal_dir).expect("read dir").flatten() {
            if entry.path().is_file() {
                on_disk.push_str(&std::fs::read_to_string(entry.path()).unwrap_or_default());
            }
        }
        assert!(!on_disk.is_empty(), "the journal wrote something");
        assert!(!on_disk.contains("hunter2"), "secret reached the journal");

        let replayed = history_store(dir.path());
        let snap = replayed.snapshot(&id).expect("replayed");
        assert!(snap.sql.contains("<redacted>"), "{}", snap.sql);
    }

    /// A statement whose rows are gone is `410 result_expired`, not `404 unknown statement id`.
    #[tokio::test]
    async fn a_history_tier_result_is_gone_not_unknown() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store_with(dir.path(), |c| c.max_records = 1);
        let (first, _) = store.insert("SELECT 1");
        store.finish(&first, ExecOutcome::Succeeded(Vec::new()));
        // Push it out of the hot tier.
        let (second, _) = store.insert("SELECT 2");
        store.finish(&second, ExecOutcome::Succeeded(Vec::new()));
        assert_eq!(
            store.snapshot(&first).expect("still known").tier,
            Tier::History
        );

        let _env = crate::distributed::env_lock();
        let state = RestState {
            service: Arc::new(OxidantService::new()),
            store,
            log_buffer: LogBuffer::new(MAX_LOG_LINES),
            status_token: None,
        };
        let router = app(state);
        let (status, body) = get_json(&router, &format!("/api/v1/statements/{first}/result")).await;
        assert_eq!(status, StatusCode::GONE);
        assert_eq!(body["error"], "result_expired");
        let (status, _) = get_json(&router, "/api/v1/statements/stmt-nope/result").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    /// H1, end to end: a client is never told `succeeded` with an implied durability the disk
    /// refused. `docs/api.md` makes the *absence* of `history` the promise that the record is on
    /// disk, so a failed write has to produce `"history": "degraded"` — and move the counter.
    #[tokio::test]
    async fn a_terminal_record_the_disk_refused_is_answered_degraded() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store(dir.path());
        // The writer opens its first segment lazily; a directory in its place fails every
        // append with EISDIR, which is the ENOSPC/EIO shape without a fault injector.
        let statements_dir = dir.path().join("history").join("statements");
        std::fs::create_dir(statements_dir.join("seg-000000.jsonl")).expect("block the segment");

        let (id, _) = store.insert("SELECT 1");
        store.finish(&id, ExecOutcome::Succeeded(Vec::new()));
        assert!(
            store.await_durable(&id).await,
            "a record the disk refused must be reported degraded, not durable"
        );
        let journal = &store.history.as_ref().expect("history on").journal;
        assert!(journal.is_degraded());
        assert!(
            journal.write_failures() >= 1,
            "the /api/status-level failure counter moved"
        );
    }

    /// M2: a Connect statement's `/result` must not answer `200 {"rows":[]}` while its own
    /// status document says `rowCount: 5`.
    ///
    /// Its batches stream to the gRPC client as Arrow IPC and the store keeps no second copy, so
    /// the statement sits in the hot tier, succeeded, with an empty `batches` — falling straight
    /// through to the JSON encoder. `docs/api.md` has `410 result_expired` for exactly this: the
    /// id is known and the query succeeded, the rows are simply not here.
    #[tokio::test]
    async fn a_connect_statements_result_is_gone_not_an_empty_row_set() {
        let _env = crate::distributed::env_lock();
        let dir = tempfile::tempdir().expect("tempdir");
        let service = Arc::new(OxidantService::new());
        service.attach_statement_store(history_store(dir.path()));

        let request = tonic::Request::new(sc::ExecutePlanRequest {
            session_id: "sess-m2".to_string(),
            operation_id: Some("op-m2".to_string()),
            plan: Some(sc::Plan {
                op_type: Some(sc::plan::OpType::Root(crate::sql_relation(
                    "SELECT * FROM (VALUES (1),(2),(3),(4),(5)) AS t(n)",
                ))),
            }),
            ..Default::default()
        });
        <OxidantService as SparkConnectService>::execute_plan(&service, request)
            .await
            .expect("execute_plan");

        let store = service.statement_store().expect("attached").clone();
        let id = store
            .list()
            .into_iter()
            .find(|s| s.source == Source::Connect)
            .expect("the connect statement is on the rail")
            .id;
        let state = RestState {
            service: Arc::clone(&service),
            store,
            log_buffer: LogBuffer::new(MAX_LOG_LINES),
            status_token: None,
        };
        let router = app(state);

        // The status document claims rows, and it is right — they went to the gRPC client.
        let (status, body) = get_json(&router, &format!("/api/v1/statements/{id}")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "succeeded");
        assert_eq!(body["source"], "connect");
        assert_eq!(body["tier"], "hot", "still hot, which is what exposed this");
        assert_eq!(body["rowCount"], 5);

        // So the result endpoint must not claim there were none.
        let (status, body) = get_json(&router, &format!("/api/v1/statements/{id}/result")).await;
        assert_eq!(
            status,
            StatusCode::GONE,
            "a hot Connect statement whose rows were never retained is `gone`, not empty: {body}"
        );
        assert_eq!(body["error"], "result_expired");
    }

    /// The counterpart: a REST statement that genuinely returned no rows still answers `200`
    /// with an empty set. "No rows" and "rows not here" are different answers.
    #[tokio::test]
    async fn a_rest_statement_with_no_rows_is_still_an_empty_result_not_gone() {
        let (_env, _state, app) = test_state();
        let (status, body) = post_json(
            &app,
            "/api/v1/statements?wait=true",
            json!({ "sql": "SELECT 1 AS n WHERE 1 = 0" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "succeeded", "{body}");
        let id = body["statementId"].as_str().expect("id");
        let (status, body) = get_json(&app, &format!("/api/v1/statements/{id}/result")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["rowCount"], 0);
        assert_eq!(body["rows"].as_array().expect("rows").len(), 0);
    }

    /// One session cannot evict another's history: the per-session share is swept first.
    #[test]
    fn a_noisy_session_evicts_itself_before_another_tenant() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store_with(dir.path(), |c| {
            c.max_per_session = 2;
            // Terminal statements demote to the history tier on the next insert, which is where
            // the per-session sweep applies.
            c.hot_ttl = Duration::from_millis(0);
        });
        for i in 0..5 {
            let (id, _) =
                store.insert_from(&format!("SELECT {i}"), Source::Connect, Some("loud"), None);
            store.finish(&id, ExecOutcome::Succeeded(Vec::new()));
        }
        let (quiet, _) = store.insert_from("SELECT 'quiet'", Source::Connect, Some("quiet"), None);
        store.finish(&quiet, ExecOutcome::Succeeded(Vec::new()));
        // One more submit to demote the last terminal statement out of the hot tier.
        store.insert("SELECT 'flush'");
        store.sweep_history();

        let inner = store.inner.lock().expect("lock");
        let loud = inner
            .history
            .values()
            .filter(|st| st.session.as_deref() == Some("loud"))
            .count();
        assert_eq!(loud, 2, "the noisy session is trimmed to its own share");
        assert!(
            inner.history.contains_key(&quiet),
            "the quiet session's history is untouched"
        );
    }

    /// M4: the per-session and global caps are enforced on every submit, so they cannot be
    /// throttled — which is affordable only if they read an index instead of rebuilding a map of
    /// the whole history tier (an allocation, a clone per id and session, and a sort per
    /// statement submitted, inside the mutex every read also takes).
    ///
    /// The index is only correct if it never drifts from `history`, so that is what this pins.
    #[test]
    fn the_eviction_indexes_track_the_history_tier_exactly() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store_with(dir.path(), |c| {
            c.max_per_session = 3;
            c.max_records = 6;
            c.hot_ttl = Duration::from_millis(0);
        });
        for i in 0..12 {
            let session = if i % 2 == 0 { "even" } else { "odd" };
            let (id, _) =
                store.insert_from(&format!("SELECT {i}"), Source::Connect, Some(session), None);
            store.finish(&id, ExecOutcome::Succeeded(Vec::new()));
        }
        // A running statement, which no cap may evict.
        let (live, _) = store.insert_from("SELECT 'live'", Source::Connect, Some("even"), None);
        store.mark_running(&live);
        store.insert("SELECT 'flush'");
        store.sweep_history();

        let inner = store.inner.lock().expect("lock");
        // Every index entry names a statement that is really there, with the right seq...
        for (seq, id) in inner.by_seq.iter() {
            let st = inner
                .history
                .get(id)
                .unwrap_or_else(|| panic!("by_seq names {id}, which is not in the tier"));
            assert_eq!(st.seq, *seq);
        }
        // ...and every statement in the tier is in both indexes.
        for (id, st) in inner.history.iter() {
            assert!(
                inner.by_seq.contains(&(st.seq, id.clone())),
                "{id} is missing from by_seq"
            );
            let session = st
                .session
                .clone()
                .expect("connect statements carry a session");
            assert!(
                inner.by_session[&session].contains(&(st.seq, id.clone())),
                "{id} is missing from by_session[{session}]"
            );
        }
        let indexed: usize = inner.by_session.values().map(|ids| ids.len()).sum();
        assert_eq!(indexed, inner.history.len(), "no stale by_session entries");
        assert_eq!(inner.by_seq.len(), inner.history.len());

        // And the caps they drive still hold.
        for session in ["even", "odd"] {
            let n = inner
                .history
                .values()
                .filter(|st| st.session.as_deref() == Some(session))
                .count();
            assert!(n <= 3, "{session} kept {n} statements, over its share of 3");
        }
        assert!(inner.history.len() <= 6, "global cap");
    }

    /// `retention_days` is an operator-supplied `u64` widened to `i64`, so
    /// `retention_days * 86_400_000` overflowed — a panic in debug, a wrap in release, on a
    /// value a config file can carry.
    #[test]
    fn an_absurd_retention_does_not_overflow_the_age_cutoff() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store_with(dir.path(), |c| {
            c.retention_days = i64::MAX / 1_000;
            c.hot_ttl = Duration::from_millis(0);
        });
        let (id, _) = store.insert("SELECT 1");
        store.finish(&id, ExecOutcome::Succeeded(Vec::new()));
        store.insert("SELECT 2");
        {
            let mut inner = store.inner.lock().expect("lock");
            inner.last_sweep_ms = 0;
        }
        store.sweep_history();
        assert!(
            store.snapshot(&id).is_some(),
            "an unreachable cutoff prunes nothing, and above all does not panic"
        );
    }

    /// The rail says where a statement came from.
    #[tokio::test]
    async fn the_statements_rail_reports_source_and_tier() {
        let (_env, state, app) = test_state();
        let (status, _) = post_json(
            &app,
            "/api/v1/statements?wait=true&timeout=30",
            json!({ "sql": "SELECT 1" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (_, body) = get_json(&app, "/api/v1/statements").await;
        let row = &body["statements"][0];
        assert_eq!(row["source"], "rest");
        assert_eq!(row["tier"], "hot");
        drop(state);
    }

    // ---- Result spill and disk read-back (docs/query-history-durability.md §5, PR2) ----

    /// `n` rows of `Int64`, wide enough that a byte budget can be set between one and two of them.
    fn rows_batch(start: i64, n: i64) -> RecordBatch {
        use oxidant_loom::arrow::array::Int64Array;
        use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
        let values: Vec<i64> = (start..start + n).collect();
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).expect("batch")
    }

    /// Every row a `/result` response returned, in order — the value the read-back has to match
    /// byte-for-byte across a restart.
    fn result_values(body: &Value) -> Vec<i64> {
        body["rows"]
            .as_array()
            .expect("rows")
            .iter()
            .map(|r| r["n"].as_i64().expect("n"))
            .collect()
    }

    fn rest_state(store: StatementStore) -> RestState {
        RestState {
            service: Arc::new(OxidantService::new()),
            store,
            log_buffer: LogBuffer::new(MAX_LOG_LINES),
            status_token: None,
        }
    }

    /// F8's trigger, working: the budget is exceeded, the *oldest* terminal result is written to
    /// `results/` and its rows leave memory — the newest one, which a client is most likely to
    /// ask for next, stays.
    #[tokio::test]
    async fn spill_on_pressure_writes_the_oldest_terminal_result_and_frees_its_memory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let one = retained_bytes(&[rows_batch(0, 4)]);
        // Room for exactly one result: admitting the second must spill the first.
        let store = history_store_with(dir.path(), |c| {
            c.result_persist = ResultPersist::OnPressure;
            c.result_memory_budget_bytes = one + one / 2;
        });

        let (first, _) = store.insert("SELECT 'first'");
        store.finish(&first, ExecOutcome::Succeeded(vec![rows_batch(0, 4)]));
        store.drain_spills();
        assert!(
            !dir.path()
                .join("history/results")
                .join(format!("{first}.arrow"))
                .exists(),
            "nothing pressures a single result under the budget: no file yet"
        );

        let (second, _) = store.insert("SELECT 'second'");
        store.finish(&second, ExecOutcome::Succeeded(vec![rows_batch(100, 4)]));
        store.drain_spills();

        let spilled = dir
            .path()
            .join("history/results")
            .join(format!("{first}.arrow"));
        assert!(
            spilled.exists(),
            "the oldest terminal result must be on disk"
        );
        assert!(
            !dir.path()
                .join("history/results")
                .join(format!("{second}.arrow"))
                .exists(),
            "the newest result stays in memory; only the victim spills"
        );
        {
            let inner = store.inner.lock().expect("lock");
            let victim = inner.statements.get(&first).expect("still hot");
            assert!(!victim.rows_in_memory, "the victim's rows were released");
            assert!(victim.batches.is_empty(), "and actually dropped");
            assert_eq!(victim.result_bytes, 0);
            assert!(victim.result_file.is_some(), "with a pointer to the file");
            assert!(
                inner.result_bytes <= one + one / 2,
                "the budget holds after the spill: {} bytes retained",
                inner.result_bytes
            );
            assert!(
                inner.statements.get(&second).expect("hot").rows_in_memory,
                "the newest result is still served from memory"
            );
        }
        // And the spilled rows are still readable — from disk, in the same process.
        let app = app(rest_state(store.clone()));
        let (status, body) = get_json(&app, &format!("/api/v1/statements/{first}/result")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result_values(&body), vec![0, 1, 2, 3]);
        store.shutdown_for_test();
    }

    /// Goal 2, end to end: a result written to `results/` is served by `/result` **and** by the
    /// CSV path after the process restarts, byte-for-byte identical to the pre-restart answer.
    #[tokio::test]
    async fn a_spilled_result_reads_back_identically_after_a_restart() {
        let dir = tempfile::tempdir().expect("tempdir");
        let boot = |dir: &std::path::Path| {
            history_store_with(dir, |c| c.result_persist = ResultPersist::Always)
        };
        let store = boot(dir.path());
        let (id, _) = store.insert("SELECT n FROM t");
        store.finish(
            &id,
            ExecOutcome::Succeeded(vec![rows_batch(1, 3), rows_batch(4, 2)]),
        );
        store.drain_spills();

        let before = app(rest_state(store.clone()));
        let (status, json_before) =
            get_json(&before, &format!("/api/v1/statements/{id}/result")).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _, csv_before) = get_raw(
            &before,
            &format!("/api/v1/statements/{id}/result?format=csv"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result_values(&json_before), vec![1, 2, 3, 4, 5]);

        store.shutdown_for_test();
        drop(before);
        drop(store);

        // Restart: same data dir, brand new store, nothing in memory.
        let replayed = boot(dir.path());
        {
            let inner = replayed.inner.lock().expect("lock");
            assert!(
                inner.statements.is_empty(),
                "replay populates the history tier only"
            );
            assert!(
                inner.history.get(&id).expect("replayed").result.is_some(),
                "and the result pointer came back with it"
            );
        }
        let after = app(rest_state(replayed.clone()));
        let (status, json_after) =
            get_json(&after, &format!("/api/v1/statements/{id}/result")).await;
        assert_eq!(status, StatusCode::OK, "{json_after}");
        assert_eq!(
            json_after, json_before,
            "the disk read-back must be identical to the memory answer"
        );
        let (status, _, csv_after) = get_raw(
            &after,
            &format!("/api/v1/statements/{id}/result?format=csv"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(csv_after, csv_before, "and so must the CSV");
        replayed.shutdown_for_test();
    }

    /// Past `OXIDANT_RESULT_MAX_BYTES` the file is refused rather than half-written: the
    /// statement records `result_too_large`, `results/` is left with no `.arrow` and no `.tmp`,
    /// and the rows stay in memory because they are now the only copy.
    #[tokio::test]
    async fn an_oversized_result_is_refused_and_recorded_as_result_too_large() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store_with(dir.path(), |c| {
            c.result_persist = ResultPersist::Always;
            c.result_max_bytes = 64; // smaller than any real Arrow IPC stream
        });
        let (id, _) = store.insert("SELECT n FROM big");
        store.finish(&id, ExecOutcome::Succeeded(vec![rows_batch(0, 64)]));
        store.drain_spills();

        let results = dir.path().join("history/results");
        assert!(
            !results.join(format!("{id}.arrow")).exists(),
            "an oversized result must not be published"
        );
        let leftovers: Vec<String> = std::fs::read_dir(&results)
            .expect("results dir")
            .flatten()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert!(
            leftovers.is_empty(),
            "and must leave no tmp behind: {leftovers:?}"
        );

        {
            let inner = store.inner.lock().expect("lock");
            let st = inner.statements.get(&id).expect("hot");
            assert_eq!(st.result_refused.as_deref(), Some(RESULT_TOO_LARGE));
            assert!(
                st.rows_in_memory,
                "the rows are the only copy left; they stay"
            );
        }
        // The status document says why, so the eventual `410` does not read as "aged out".
        let live = app(rest_state(store.clone()));
        let (_, body) = get_json(&live, &format!("/api/v1/statements/{id}")).await;
        assert_eq!(body["resultStatus"], RESULT_TOO_LARGE);
        // The live path still answers, which §5 names as the answer past the budget.
        let (status, body) = get_json(&live, &format!("/api/v1/statements/{id}/result")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result_values(&body).len(), 64);

        // And it survives the restart as a recorded refusal, not as a phantom pointer.
        store.shutdown_for_test();
        drop(live);
        drop(store);
        let replayed = history_store_with(dir.path(), |c| c.result_persist = ResultPersist::Always);
        let after = app(rest_state(replayed.clone()));
        let (_, body) = get_json(&after, &format!("/api/v1/statements/{id}")).await;
        assert_eq!(body["resultStatus"], RESULT_TOO_LARGE);
        let (status, body) = get_json(&after, &format!("/api/v1/statements/{id}/result")).await;
        assert_eq!(status, StatusCode::GONE, "{body}");
        assert_eq!(body["error"], "result_expired");
        replayed.shutdown_for_test();
    }

    /// `OXIDANT_RESULT_PERSIST=never` writes nothing, ever — and, because nothing is durable
    /// under it, never releases rows on the byte budget either. Silently dropping a result to
    /// honour a budget with no disk behind it would be data loss the old store never had.
    #[tokio::test]
    async fn result_persist_never_disables_spill_entirely() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store_with(dir.path(), |c| {
            c.result_persist = ResultPersist::Never;
            c.result_memory_budget_bytes = 1; // every result is "over budget"
        });
        let mut ids = Vec::new();
        for i in 0..3 {
            let (id, _) = store.insert(&format!("SELECT {i}"));
            store.finish(&id, ExecOutcome::Succeeded(vec![rows_batch(i * 10, 4)]));
            ids.push(id);
        }
        store.drain_spills();

        let files: Vec<String> = std::fs::read_dir(dir.path().join("history/results"))
            .expect("results dir exists even under never")
            .flatten()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert!(files.is_empty(), "never means never: {files:?}");
        {
            let inner = store.inner.lock().expect("lock");
            for id in &ids {
                let st = inner.statements.get(id).expect("hot");
                assert!(st.rows_in_memory, "{id} kept its rows");
                assert!(st.result_file.is_none());
            }
        }
        let app = app(rest_state(store.clone()));
        for (i, id) in ids.iter().enumerate() {
            let (status, body) = get_json(&app, &format!("/api/v1/statements/{id}/result")).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(result_values(&body).len(), 4, "{i}");
        }
        store.shutdown_for_test();
    }

    // ---- Result GC, the disk budget, and the status counters (§3, §5, PR2) ----

    /// F13: a result file no folded id names is garbage, and boot is what proves it — the crash
    /// window between "tombstone appended" and "file unlinked" closes here.
    #[tokio::test]
    async fn boot_unlinks_result_files_no_statement_references() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store_with(dir.path(), |c| c.result_persist = ResultPersist::Always);
        let (kept, _) = store.insert("SELECT 'kept'");
        store.finish(&kept, ExecOutcome::Succeeded(vec![rows_batch(0, 2)]));
        store.drain_spills();
        store.shutdown_for_test();
        drop(store);

        // An orphan exactly as a crash between the two steps would leave it.
        let results = dir.path().join("history/results");
        let orphan = results.join("stmt-00000000-0000-4000-8000-000000000000.arrow");
        std::fs::write(&orphan, b"not a real arrow stream").expect("plant an orphan");
        assert!(orphan.exists());

        let replayed = history_store_with(dir.path(), |c| c.result_persist = ResultPersist::Always);
        assert!(
            !orphan.exists(),
            "boot must unlink a result no statement names"
        );
        assert!(
            results.join(format!("{kept}.arrow")).exists(),
            "and must not touch one a folded statement still points at"
        );
        replayed.shutdown_for_test();
    }

    /// The one thing retention must never do. A running statement has no journal snapshot yet, so
    /// a reconcile against the *folded* set alone would delete a result out from under a query
    /// that is still going.
    #[tokio::test]
    async fn a_running_statements_result_is_never_swept() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Retention that expires everything, a cap of one, and a budget of nothing: every
        // eviction path fires on every call.
        let store = history_store_with(dir.path(), |c| {
            c.result_persist = ResultPersist::Always;
            c.retention_days = 0;
            c.max_records = 1;
            c.hot_ttl = Duration::from_millis(0);
        });
        // A statement that finished and spilled, then is *re-marked* running: the shape a hot
        // entry has while its rows are on disk and its query is not done.
        let (running, _) = store.insert("SELECT 'still going'");
        store.finish(&running, ExecOutcome::Succeeded(vec![rows_batch(0, 2)]));
        store.drain_spills();
        {
            let mut inner = store.inner.lock().expect("lock");
            let st = inner.statements.get_mut(&running).expect("hot");
            st.status = StatementStatus::Running;
        }
        let spilled = dir
            .path()
            .join("history/results")
            .join(format!("{running}.arrow"));
        assert!(spilled.exists());

        // Force every sweep there is.
        for i in 0..3 {
            let (other, _) = store.insert(&format!("SELECT {i}"));
            store.finish(&other, ExecOutcome::Succeeded(Vec::new()));
        }
        store.drain_spills();
        store.sweep_disk();
        assert!(
            spilled.exists(),
            "a non-terminal statement's result survived neither retention nor the disk sweep"
        );
        {
            let inner = store.inner.lock().expect("lock");
            assert!(
                inner.statements.contains_key(&running),
                "and the statement itself is still hot"
            );
        }
        store.shutdown_for_test();
    }

    /// Pruning a statement unlinks its result in the same sweep — the journal is the authority
    /// and a result never outlives its record (§5).
    #[tokio::test]
    async fn pruning_a_statement_unlinks_its_result_in_the_same_sweep() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store_with(dir.path(), |c| {
            c.result_persist = ResultPersist::Always;
            c.hot_ttl = Duration::from_millis(0);
            c.max_records = 1;
        });
        let (doomed, _) = store.insert("SELECT 'doomed'");
        store.finish(&doomed, ExecOutcome::Succeeded(vec![rows_batch(0, 2)]));
        store.drain_spills();
        let file = dir
            .path()
            .join("history/results")
            .join(format!("{doomed}.arrow"));
        assert!(file.exists(), "spilled");

        // Two more terminal statements push it out of a one-record history tier.
        for i in 0..2 {
            let (id, _) = store.insert(&format!("SELECT {i}"));
            store.finish(&id, ExecOutcome::Succeeded(Vec::new()));
        }
        store.drain_spills();
        {
            let inner = store.inner.lock().expect("lock");
            assert!(
                !inner.history.contains_key(&doomed) && !inner.statements.contains_key(&doomed),
                "the statement was evicted"
            );
        }
        assert!(
            !file.exists(),
            "so its result went with it, in the same sweep"
        );
        store.shutdown_for_test();
    }

    /// §3's prune order, asserted step by step: rolled logs go before dumps, dumps before result
    /// files, and the live log never goes at all.
    #[tokio::test]
    async fn the_disk_sweep_prunes_in_the_documented_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Booted first, and with a budget the empty tree already fits, so the boot sweep is a
        // no-op and the pass under test is the explicit one below.
        let store = history_store_with(dir.path(), |c| {
            c.result_persist = ResultPersist::Always;
            c.disk_max_bytes = 6_000;
            c.disk_min_free_bytes = 0;
        });
        let (id, _) = store.insert("SELECT 'rows'");
        store.finish(&id, ExecOutcome::Succeeded(vec![rows_batch(0, 8)]));
        store.drain_spills();
        let result_file = dir
            .path()
            .join("history/results")
            .join(format!("{id}.arrow"));
        assert!(result_file.exists(), "spilled before the sweep");

        let plant = |rel: &str, bytes: usize| {
            let path = dir.path().join(rel);
            std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
            std::fs::write(&path, vec![b'x'; bytes]).expect("write");
            path
        };
        // Ordering within a directory is (mtime, name), so the two rolled logs prune oldest-first
        // whether or not the filesystem resolved their mtimes apart — no sleep, no fake clock.
        let live = plant("logs/oxidant.log", 4096);
        let rolled_old = plant("logs/oxidant-2026-08-20.log", 4096);
        let rolled_new = plant("logs/oxidant-2026-08-21.log", 4096);
        let dump = plant("dumps/dump-1.parquet", 4096);

        let report = store.sweep_disk();
        assert!(live.exists(), "the live log is never deleted — it rotates");
        assert!(
            !rolled_old.exists(),
            "the oldest rolled log goes first: {report:?}"
        );
        assert_eq!(
            report.rolled_logs_removed, 2,
            "both rolled logs: {report:?}"
        );
        assert!(!rolled_new.exists());
        assert_eq!(
            report.dumps_removed, 1,
            "dumps go only after the logs are exhausted: {report:?}"
        );
        assert!(!dump.exists());
        // The load-bearing half of the order: unlinking the logs and the dump was enough, so the
        // sweeper stopped — a statement's rows are not spent to save a rolled log.
        assert_eq!(report.statements_pruned, 0, "{report:?}");
        assert_eq!(report.live_results_removed, 0, "{report:?}");
        assert!(result_file.exists(), "results outlive everything cheaper");
        assert!(!report.over_budget, "and it fits again: {report:?}");
        store.shutdown_for_test();
    }

    /// The tail of §3's order: journal statements before *live* result files, and the live result
    /// only when there is nothing else left. The statement survives losing its rows — the sweeper
    /// takes the file, not the history entry — and `/result` says `410 result_expired`.
    #[tokio::test]
    async fn the_disk_sweep_reaches_live_results_only_after_everything_else() {
        let dir = tempfile::tempdir().expect("tempdir");
        // A budget nothing can satisfy, so the sweeper walks the whole list.
        let store = history_store_with(dir.path(), |c| {
            c.result_persist = ResultPersist::Always;
            c.disk_max_bytes = 0;
            c.disk_min_free_bytes = 0;
        });
        let (terminal, _) = store.insert("SELECT 'terminal'");
        store.finish(&terminal, ExecOutcome::Succeeded(vec![rows_batch(0, 8)]));
        // A statement whose rows are on disk but which is still going: step 4 cannot evict it, so
        // step 5 is the only thing that can reclaim its file.
        let (running, _) = store.insert("SELECT 'still going'");
        store.finish(&running, ExecOutcome::Succeeded(vec![rows_batch(100, 8)]));
        store.drain_spills();
        {
            let mut inner = store.inner.lock().expect("lock");
            inner.statements.get_mut(&running).expect("hot").status = StatementStatus::Running;
        }
        let running_file = dir
            .path()
            .join("history/results")
            .join(format!("{running}.arrow"));
        assert!(running_file.exists(), "spilled before the sweep");

        let report = store.sweep_disk();
        assert!(report.statements_pruned >= 1, "step 4 ran: {report:?}");
        assert_eq!(report.live_results_removed, 1, "then step 5: {report:?}");
        assert!(!running_file.exists());
        assert!(report.over_budget, "nothing left to prune: {report:?}");
        {
            let inner = store.inner.lock().expect("lock");
            let st = inner
                .statements
                .get(&running)
                .expect("the statement itself survives losing its rows");
            assert!(st.result_file.is_none(), "and its pointer was cleared");
        }
        // The pointer is cleared in the journal too, so a restart does not read a file that is
        // gone: it answers `410` from the fold, not from a failed open.
        store.shutdown_for_test();
        let replayed = history_store_with(dir.path(), |c| {
            c.result_persist = ResultPersist::Always;
            c.disk_min_free_bytes = 0;
        });
        {
            let inner = replayed.inner.lock().expect("lock");
            if let Some(st) = inner.history.get(&running) {
                assert!(st.result.is_none(), "a cleared pointer stays cleared");
            }
        }
        replayed.shutdown_for_test();
    }

    /// A budget nothing can satisfy reports `over_budget` rather than pretending, and says so
    /// through `/api/status`.
    #[tokio::test]
    async fn an_unsatisfiable_budget_reports_over_budget() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store_with(dir.path(), |c| {
            c.result_persist = ResultPersist::Always;
            // Zero bytes: the journal segment alone is over it and cannot be deleted.
            c.disk_max_bytes = 0;
            c.disk_min_free_bytes = 0;
        });
        let (id, _) = store.insert("SELECT 'rows'");
        store.finish(&id, ExecOutcome::Succeeded(vec![rows_batch(0, 4)]));
        store.drain_spills();
        let report = store.sweep_disk();
        assert!(report.over_budget, "{report:?}");
        assert_eq!(
            store.status_counters().map(|c| c.disk),
            Some(oxidant_observability::disk_state::OVER_BUDGET.to_string())
        );
        store.shutdown_for_test();
    }

    /// §7's honesty, on the status endpoint: a failing writer flips `history_writes` to
    /// `degraded`, and a recovered disk flips it back with no restart.
    #[tokio::test]
    async fn the_status_counters_flip_degraded_and_recover() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store_with(dir.path(), |c| {
            c.result_persist = ResultPersist::Always;
            c.flush_interval = Duration::from_millis(20);
        });
        let (ok_id, _) = store.insert("SELECT 'ok'");
        store.finish(&ok_id, ExecOutcome::Succeeded(vec![rows_batch(0, 2)]));
        store.drain_spills();
        let counters = store.status_counters().expect("history is on");
        assert_eq!(
            counters.history_writes,
            oxidant_observability::history_writes::OK
        );
        assert_eq!(counters.disk, oxidant_observability::disk_state::OK);
        assert!(
            counters.results_on_disk_bytes > 0,
            "the spilled result is accounted: {counters:?}"
        );

        // The failing-writer shim: a *directory* where the next segment file belongs makes every
        // open fail with EISDIR — the ENOSPC/EIO shape, deterministically, with no fault injector.
        let blocked = {
            let history = store.history.as_ref().expect("history");
            let next = history.cfg.statements_dir.join("seg-000001.jsonl");
            history.journal.compact_blocking(); // seals seg-000000, so seg-000001 is next
            std::fs::create_dir_all(&next).expect("block the segment");
            next
        };
        let (broken, _) = store.insert("SELECT 'broken'");
        store.finish(&broken, ExecOutcome::Succeeded(Vec::new()));
        assert!(
            store.await_durable(&broken).await,
            "a refused terminal write must answer degraded"
        );
        assert_eq!(
            store.status_counters().map(|c| c.history_writes),
            Some(oxidant_observability::history_writes::DEGRADED.to_string())
        );

        // Recovery is automatic: the writer reopens on the next append.
        std::fs::remove_dir(&blocked).expect("unblock");
        let (healthy, _) = store.insert("SELECT 'healthy'");
        store.finish(&healthy, ExecOutcome::Succeeded(Vec::new()));
        assert!(
            !store.await_durable(&healthy).await,
            "and a recovered disk acks again"
        );
        assert_eq!(
            store.status_counters().map(|c| c.history_writes),
            Some(oxidant_observability::history_writes::OK.to_string()),
            "history_writes must flip back without a restart"
        );
        store.shutdown_for_test();
    }

    /// H2: a spill job the queue had no room for must go back into the memory budget.
    ///
    /// `plan_spills` marks each victim `spilling` under the store lock, and `budget_victims`
    /// filters on `!spilling` — so a job that vanished on the way to the writer used to exclude
    /// its statement from eviction *permanently*: rows pinned in memory, never written, and the
    /// store stuck over budget until the hot TTL (an hour) or the 10,000-record cap. A
    /// terminal-result burst past the writer's throughput is exactly when that fires.
    #[tokio::test]
    async fn a_dropped_spill_goes_back_into_the_budget_and_lands_on_the_next_pass() {
        let dir = tempfile::tempdir().expect("tempdir");
        let one = retained_bytes(&[rows_batch(0, 4)]);
        let budget = one + one / 2; // room for exactly one retained result
        let store = history_store_with(dir.path(), |c| {
            c.result_persist = ResultPersist::OnPressure;
            c.result_memory_budget_bytes = budget;
        });

        // Park the writer: a 256-deep queue cannot be filled against one that is draining it.
        let release = store
            .history
            .as_ref()
            .expect("history")
            .results
            .block_writer();
        for i in 0..(crate::history::SPILL_QUEUE + 8) as i64 {
            let (id, _) = store.insert(&format!("SELECT {i}"));
            store.finish(&id, ExecOutcome::Succeeded(vec![rows_batch(i * 10, 4)]));
        }
        let dropped = store
            .history
            .as_ref()
            .expect("history")
            .results
            .dropped_spills();
        assert!(
            dropped > 0,
            "the queue must have overflowed to test the drop"
        );
        {
            let inner = store.inner.lock().expect("lock");
            let stuck = inner.statements.values().filter(|st| st.spilling).count();
            assert!(
                stuck <= crate::history::SPILL_QUEUE,
                "only jobs the writer actually took may still be marked spilling: \
                 {stuck} > {}",
                crate::history::SPILL_QUEUE
            );
        }

        // Let the writer catch up, then drive one more budget pass.
        drop(release);
        store.drain_spills();
        let (last, _) = store.insert("SELECT 'last'");
        store.finish(&last, ExecOutcome::Succeeded(vec![rows_batch(9_000, 4)]));
        store.drain_spills();

        let inner = store.inner.lock().expect("lock");
        assert!(
            inner.result_bytes <= budget,
            "the dropped spills must be retried, not stranded: {} retained against a \
             {budget}-byte budget ({dropped} jobs were dropped)",
            inner.result_bytes
        );
        assert!(
            !inner.statements.values().any(|st| st.spilling),
            "no statement is left mid-spill once the writer has drained"
        );
        drop(inner);
        store.shutdown_for_test();
    }

    /// The other half of H2: a spill the *disk* refused. The statement stays in the budget's
    /// accounting and in its candidate set, and the next pass writes it successfully.
    #[tokio::test]
    async fn a_failed_spill_keeps_its_statement_in_the_budget_and_retries() {
        let dir = tempfile::tempdir().expect("tempdir");
        let one = retained_bytes(&[rows_batch(0, 4)]);
        let budget = one + one / 2;
        let store = history_store_with(dir.path(), |c| {
            c.result_persist = ResultPersist::OnPressure;
            c.result_memory_budget_bytes = budget;
        });
        let results = dir.path().join("history/results");

        let (victim, _) = store.insert("SELECT 'victim'");
        store.finish(&victim, ExecOutcome::Succeeded(vec![rows_batch(0, 4)]));
        // Block the victim's tmp path, then push it over the budget so it is chosen.
        let blocked = results.join(format!("{victim}.arrow.tmp"));
        std::fs::create_dir_all(&blocked).expect("block the spill");
        let (second, _) = store.insert("SELECT 'second'");
        store.finish(&second, ExecOutcome::Succeeded(vec![rows_batch(100, 4)]));
        store.drain_spills();

        {
            let inner = store.inner.lock().expect("lock");
            let st = inner.statements.get(&victim).expect("hot");
            assert!(st.rows_in_memory, "a failed spill keeps the rows");
            assert!(st.result_bytes > 0);
            assert!(!st.spilling, "and is a candidate again");
            assert!(
                inner.result_bytes >= st.result_bytes,
                "the budget must still account for what memory actually holds"
            );
        }
        assert_eq!(
            store
                .status_counters()
                .map(|c| c.result_write_failures)
                .unwrap_or_default(),
            1
        );

        // Retry: the disk recovers, the next budget pass picks the same statement, and it lands.
        std::fs::remove_dir(&blocked).expect("unblock");
        let (third, _) = store.insert("SELECT 'third'");
        store.finish(&third, ExecOutcome::Succeeded(vec![rows_batch(200, 4)]));
        store.drain_spills();
        assert!(
            results.join(format!("{victim}.arrow")).exists(),
            "the retry must write the result the first attempt could not"
        );
        {
            let inner = store.inner.lock().expect("lock");
            assert!(inner.result_bytes <= budget, "{}", inner.result_bytes);
        }
        store.shutdown_for_test();
    }

    /// H3: degraded is **per subsystem**, and a subsystem's flag is cleared only by a success of
    /// its own. A failing `OXIDANT_RESULT_DIR` used to read `ok` again the microsecond the next
    /// statement was submitted, because the spill reported through the *journal's* flag and the
    /// journal clears that on every successful append.
    #[tokio::test]
    async fn a_failed_spill_stays_degraded_across_a_successful_journal_append() {
        use oxidant_observability::history_writes::{DEGRADED, OK};
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store_with(dir.path(), |c| c.result_persist = ResultPersist::Always);
        let results = dir.path().join("history/results");

        // A *directory* where the spill's tmp file belongs: `create_secure` fails with EISDIR —
        // the ENOSPC/EIO shape, deterministically, with no fault injector.
        let (broken, _) = store.insert("SELECT 'broken'");
        let blocked = results.join(format!("{broken}.arrow.tmp"));
        std::fs::create_dir_all(&blocked).expect("block the spill");
        store.finish(&broken, ExecOutcome::Succeeded(vec![rows_batch(0, 4)]));
        store.drain_spills();

        let counters = store.status_counters().expect("history is on");
        assert_eq!(counters.result_writes, DEGRADED, "{counters:?}");
        assert_eq!(counters.result_write_failures, 1, "{counters:?}");
        assert_eq!(
            counters.history_writes, DEGRADED,
            "the aggregate must carry it: {counters:?}"
        );

        // The regression: a healthy journal append (this submits *and* fsyncs a terminal record)
        // must not clear the spill writer's flag.
        let (chatter, _) = store.insert("SELECT 'chatter'");
        store.finish(&chatter, ExecOutcome::Succeeded(Vec::new()));
        assert!(
            !store.await_durable(&chatter).await,
            "the journal itself is healthy"
        );
        let counters = store.status_counters().expect("history is on");
        assert_eq!(
            counters.result_writes, DEGRADED,
            "a journal success is not a spill success: {counters:?}"
        );
        assert_eq!(counters.history_writes, DEGRADED, "{counters:?}");

        // Only a spill that lands clears it — and the failure stays counted.
        std::fs::remove_dir(&blocked).expect("unblock");
        let (healthy, _) = store.insert("SELECT 'healthy'");
        store.finish(&healthy, ExecOutcome::Succeeded(vec![rows_batch(0, 4)]));
        store.drain_spills();
        let counters = store.status_counters().expect("history is on");
        assert_eq!(counters.result_writes, OK, "{counters:?}");
        assert_eq!(counters.history_writes, OK, "{counters:?}");
        assert_eq!(counters.result_write_failures, 1, "{counters:?}");
        store.shutdown_for_test();
    }

    /// With history off there are no durability counters at all — §8 says `off` restores today's
    /// behaviour exactly, and today `/api/status` has no such fields.
    #[test]
    fn history_off_publishes_no_status_counters() {
        assert!(StatementStore::new().status_counters().is_none());
    }
}
