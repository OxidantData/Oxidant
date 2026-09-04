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
//! - `PUT  /api/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/comment` — set/clear
//!   a table's catalog comment.
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
use axum::response::sse::{self, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use datafusion::arrow::json::{ArrayWriter, WriterBuilder};
use futures::StreamExt;
use oxidant_catalog::DEFAULT_CATALOG;
use oxidant_loom::arrow::array::Array;
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;
use serde::Deserialize;
use serde_json::{json, Value};
use sysinfo::{Pid, System};
use tokio::sync::{watch, Notify};
use uuid::Uuid;

use crate::logging::{LogBuffer, LogView};

use crate::history::{
    disk, now_rfc3339, rfc3339_from_ms, FoldedStatement, HistoryConfig, HistoryRuntime,
    JournalRecord, RecordKind, ResultPersist, ResultPointer, Source, SpillJob, SpillOutcome,
    SqlMode, StatementStatus, RECORD_VERSION, RESULT_EMPTY, RESULT_TOO_LARGE,
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
/// Minimum wall-clock gap between two full retention passes over the history tier.
const SWEEP_INTERVAL_MS: i64 = 60_000;

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
    /// `results/<id>.arrow` — read back off disk, possibly after a restart. Carries the file
    /// name the journaled pointer names, so a pointer that disagrees with the id is rejected
    /// rather than silently ignored.
    Disk(Option<String>),
    /// The statement succeeded with no batches at all. There is nothing to read and nothing
    /// was lost: `200 {"rows": []}`, before and after a restart alike.
    Empty,
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
        // Nothing is in flight for a statement with no rows left. Today the `result_bytes > 0`
        // filter would exclude it anyway; leaving a stale `spilling` behind is the shape H2 was.
        st.spilling = false;
        self.result_bytes = self.result_bytes.saturating_sub(freed);
        freed
    }

    /// The statements whose rows must leave memory for the result budget to hold,
    /// oldest-terminal-first (§5).
    ///
    /// Each is marked `spilling` + `release_on_spill` so a second call cannot pick it again while
    /// its write is in flight, and so the spill's completion knows to free the memory. A
    /// non-terminal statement is never a victim: its rows do not exist yet.
    ///
    /// A statement whose spill was **refused** (`result_refused`, i.e. past
    /// `OXIDANT_RESULT_MAX_BYTES`) is not a candidate either. Its rows are the only copy left and
    /// are never dropped to honour a budget — so re-selecting it every pass only meant
    /// `plan_spills` declined it again, having already counted its bytes as freed in the
    /// projection below. The consequence is stated rather than hidden: the in-memory ceiling is
    /// `OXIDANT_RESULT_MEMORY_BUDGET_BYTES` **plus** every refused result still in the hot tier,
    /// and those leave on the hot TTL or the record cap like any other statement. The sink logs
    /// one line per refusal saying so.
    fn budget_victims(&mut self) -> Vec<String> {
        let budget = self.limits.result_budget;
        if budget == u64::MAX || self.result_bytes <= budget {
            return Vec::new();
        }
        let mut candidates: Vec<(u64, String)> = self
            .statements
            .iter()
            .filter(|(_, st)| {
                st.status.is_terminal()
                    && st.rows_in_memory
                    && !st.spilling
                    && st.result_refused.is_none()
                    && st.result_bytes > 0
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

    /// The config this store booted with — **the one resolver every writer under the data dir
    /// shares**.
    ///
    /// `HistoryConfig::from_env` folds `(role, port)` into the root under
    /// `OXIDANT_DATA_DIR_PER_PROCESS`, so re-reading the environment with a different port
    /// resolves a *different tree*. That is how the dump store came to write into
    /// `<root>/driver-0/dumps/` while the sweeper pruned `<root>/driver-<port>/dumps/` and the
    /// disk budget measured neither: bundles that never expired and an up-front `507` that
    /// measured an empty directory. Handing out the booted config — rather than reading the env
    /// a third time — is what makes "the same env the writer and the sweeper read" a fact.
    ///
    /// `None` is a volatile store (`OXIDANT_HISTORY=off`, or an embedded caller that never
    /// attached one), which promises that nothing is written under the data dir.
    pub(crate) fn history_config(&self) -> Option<&HistoryConfig> {
        self.history.as_ref().map(|h| &h.cfg)
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
        store.install_roll_sweep_hook();
        store.sweep_history();
        // §3: the sweeper runs at boot, at roll time, and every 5 minutes.
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

    /// Teach the rolling log writer to sweep the disk when it rolls (§3: "The sweeper runs at
    /// roll time, at boot, and every 5 minutes").
    ///
    /// A roll is the one moment the engine's own footprint jumps by a whole file, and it is also
    /// the moment `OXIDANT_LOG_KEEP_DAYS` has a new file to consider — waiting up to five
    /// minutes for the timer would let a chatty size-rolling driver run several files past its
    /// budget.
    ///
    /// [`std::sync::Weak`], like the spill sink and the sweeper thread: the hook lives in a
    /// process-global for the writer's whole life, and a strong reference would keep a test's
    /// store — and its data-dir lock — alive past the end of the test.
    fn install_roll_sweep_hook(&self) {
        let Some(history) = self.history.as_ref() else {
            return;
        };
        let inner = Arc::downgrade(&self.inner);
        let history = Arc::downgrade(history);
        let notify = Arc::downgrade(&self.notify);
        let disk = Arc::clone(&self.disk);
        crate::logging::set_sweep_hook(move || {
            let (Some(inner), Some(history), Some(notify)) =
                (inner.upgrade(), history.upgrade(), notify.upgrade())
            else {
                return;
            };
            StatementStore {
                inner,
                notify,
                history: Some(history),
                disk: Arc::clone(&disk),
            }
            .sweep_disk();
        });
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
        // The liveness question §5's late-spill guard asks. A store that is gone answers "dead",
        // which is the honest answer: nothing is left to own the file.
        let live_weak = Arc::downgrade(&self.inner);
        let live: Box<dyn Fn(&str) -> bool + Send + Sync> = Box::new(move |id: &str| {
            let Some(inner) = live_weak.upgrade() else {
                return false;
            };
            let inner = inner.lock().expect("statement store poisoned");
            inner.statements.contains_key(id) || inner.history.contains_key(id)
        });
        history.results.set_sink(
            Box::new(move |id, outcome| {
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
                        SpillOutcome::Failed | SpillOutcome::Abandoned => {}
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
                        tracing::warn!(
                            statement = %id,
                            retained_bytes = st.result_bytes,
                            "result too large to spill: its rows are the only copy and stay in \
                             memory, so they are excluded from the result budget's eviction \
                             candidates. The effective in-memory ceiling is \
                             OXIDANT_RESULT_MEMORY_BUDGET_BYTES plus every such result until it \
                             ages out of the hot tier"
                        );
                        false
                    }
                    // The disk refused it, or it completed for a statement that had been
                    // evicted (in which case this branch is unreachable — the id is in neither
                    // tier by definition — but a flag left set is how H2 happened).
                    SpillOutcome::Failed | SpillOutcome::Abandoned => {
                        st.release_on_spill = false;
                        false
                    }
                }
            };
            if release {
                inner.release_rows(id);
            }
            }),
            live,
        );
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
                    // No batches means no schema, and an Arrow IPC stream cannot be written
                    // without one — so there will never be a file for this statement. Recording
                    // *why* on the terminal snapshot (which is already being written) is what
                    // makes `/result` answer `200 {"rows": []}` for it after a restart, exactly
                    // as it does before one, instead of the `410 result_expired` that means "the
                    // rows aged out". Costs no extra write and no extra byte on disk.
                    if batches.is_empty() {
                        st.result_refused = Some(RESULT_EMPTY.to_string());
                    }
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
        // Order matters (the finished statement first, then oldest victims), so this is a vec
        // with a set beside it rather than a `contains` scan per candidate.
        let mut wanted: Vec<String> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        if persist.spills_eagerly() {
            seen.insert(finished.to_string());
            wanted.push(finished.to_string());
        }
        for id in inner.budget_victims() {
            if seen.insert(id.clone()) {
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
        let empty = |refused: Option<&String>| refused.is_some_and(|r| r == RESULT_EMPTY);
        if let Some(st) = inner.statements.get(id) {
            let source = if st.rows_in_memory {
                ResultSource::Memory(st.batches.clone())
            } else if let Some(pointer) = st.result_file.as_ref() {
                ResultSource::Disk(Some(pointer.file.clone()))
            } else if empty(st.result_refused.as_ref()) {
                ResultSource::Empty
            } else {
                ResultSource::Gone
            };
            return Some((st.snapshot(id), source));
        }
        inner.history.get(id).map(|st| {
            let source = if let Some(pointer) = st.result.as_ref() {
                ResultSource::Disk(Some(pointer.file.clone()))
            } else if empty(st.result_refused.as_ref()) {
                ResultSource::Empty
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
    async fn read_spilled(&self, id: &str, journaled: Option<String>) -> Option<Vec<RecordBatch>> {
        let results = Arc::clone(&self.history.as_ref()?.results);
        let owned = id.to_string();
        match tokio::task::spawn_blocking(move || results.read(&owned, journaled.as_deref())).await
        {
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
        // Retained rather than droppable. A tombstone is not `running` chatter: the result file
        // is unlinked in this same pass, so a tombstone lost to backpressure leaves the
        // statement's *snapshot* — pointer and all — in the journal naming a file that no longer
        // exists. `/result` degrades to `410` from the failed open, which is the right answer,
        // but it is a worse one than not replaying the statement at all, and the overflow queue
        // is 65,536 deep against a sweep that evicts at most `max_records`.
        //
        // The result file goes in the *same* sweep, before the tombstone is considered complete
        // (§5, F13). The journal is the authority: nothing here decides a result's lifetime, it
        // only follows the statement's. A crash between the two leaves an orphan, which is
        // exactly what boot's `reconcile` is for.
        for (id, submitted_at_ms) in tombstones {
            let _ = history.results.unlink(&id);
            let seq = history.journal.next_seq();
            history
                .journal
                .append_retained(JournalRecord::tombstone(&id, seq, submitted_at_ms));
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
    /// **Pruning is driven by `OXIDANT_DISK_MAX_BYTES` and by nothing else.** The engine deletes
    /// its own files when its own subtree is over its own budget: its mess, its documented order.
    /// The free-space floor is a *separate* condition with a separate answer — the engine stops
    /// spilling and reports `disk: low_free` + `history_writes: degraded`, and deletes nothing.
    ///
    /// That separation is H1. The two conditions used to be one boolean driving one unbounded
    /// prune loop, and unlike the byte budget the floor cannot be *made* satisfiable by pruning:
    /// a co-tenant filling the volume — a CI cache, another container, a `target/` directory —
    /// ran the loop until every terminal statement in both tiers was gone, then took every
    /// remaining result file, then did it again five minutes later, having reclaimed kilobytes
    /// against a shortfall it never caused.
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
        let roots = disk::budget_roots(cfg);

        let mut report = disk::SweepReport::default();
        // Retention runs before the budget, and unconditionally: `OXIDANT_LOG_KEEP_DAYS` and
        // `OXIDANT_LOG_MAX_TOTAL_BYTES` are the `logs/` subtree's own contract, and
        // `OXIDANT_EVENT_LOG_MAX_BYTES` is `event_log_dir`'s. Both must hold whether or not the
        // global budget is tight — a driver far under 8 GiB still may not keep 90 days of logs.
        let now = chrono::Utc::now();
        let logs = disk::prune_expired_logs(
            &cfg.logs_dir,
            cfg.log_keep_days,
            cfg.log_max_total_bytes,
            now,
        );
        report.logs_expired = logs.expired;
        report.logs_over_cap = logs.over_cap;
        // Retention's own bytes. They are freed *before* `before` is measured — which is the
        // correct order for the prune loop, since it stops it from spending a second time what
        // retention already reclaimed — so they have to be added back at the end or the line
        // reads `logs_expired=2, event_logs_pruned=1, freed_bytes=0`, which looks like a bug in
        // the sweeper (M3).
        let mut retention_freed = logs.freed_bytes;
        // §6b: "the bundle expires after 24 h and is swept like results". Unconditional, like
        // the logs' own retention: a bundle nobody collected is not the budget's business, it
        // is a promise about how long a copy of the cluster's logs sits on the driver's disk.
        let expired_dumps =
            disk::prune_expired_dumps(&cfg.dumps_dir, crate::logging::DUMP_TTL_SECS, now);
        report.dumps_expired = expired_dumps.expired;
        retention_freed = retention_freed.saturating_add(expired_dumps.freed_bytes);
        if let Some(dir) = &cfg.event_log_dir {
            let events = disk::roll_event_log(dir, cfg.event_log_max_bytes, cfg.log_roll, now);
            report.event_logs_pruned = events.pruned;
            report.event_log_rolled = events.rolled;
            retention_freed = retention_freed.saturating_add(events.freed_bytes);
        }
        let before = disk::measure_roots(&roots).billed;
        #[cfg(test)]
        disk::sweep_midpoint();
        // The running total, decremented by what each unlink reports. The whole tree used to be
        // re-walked *per candidate* — pruning 10,000 statements meant 10,000 full recursive
        // directory walks interleaved with 10,000 lock/unlock cycles of the store mutex that
        // every submit, list, status and result call also takes, on a host already short on I/O.
        //
        // It is an estimate between the two measurements — a tombstone appended for a pruned
        // statement grows the journal by a record the total does not see — but it errs *low*,
        // which stops the loop early rather than deleting more than the budget asked for. The
        // number `/api/status` and the log line report is re-measured once, below.
        let mut used = before;
        let spend = |used: &mut u64, freed: u64| *used = used.saturating_sub(freed);

        // 1. Oldest rolled logs. The live file is never a candidate — it rotates (PR3).
        for file in disk::rolled_logs(&cfg.logs_dir) {
            if used <= cfg.disk_max_bytes {
                break;
            }
            if let Some(freed) = disk::remove(&file) {
                spend(&mut used, freed);
                report.rolled_logs_removed += 1;
            }
        }
        // 2. Oldest dumps.
        for file in disk::dumps(&cfg.dumps_dir) {
            if used <= cfg.disk_max_bytes {
                break;
            }
            if let Some(freed) = disk::remove(&file) {
                spend(&mut used, freed);
                report.dumps_removed += 1;
            }
        }
        // 3. Result files whose statement is already pruned. Orphans are garbage whether or not
        // the budget is tight, so this pass is unconditional.
        let (orphans, orphan_bytes) = history.results.reconcile(&self.live_ids());
        report.orphan_results_removed = orphans;
        spend(&mut used, orphan_bytes);

        // 4. Oldest journal *statements* — statement-granular, never a raw segment unlink (F2).
        while used > cfg.disk_max_bytes {
            let Some(freed) = self.prune_oldest_statement() else {
                break;
            };
            spend(&mut used, freed);
            report.statements_pruned += 1;
        }
        // 5. Oldest live result files. The rows go, the statement stays, and `/result` answers
        // `410 result_expired` for it from here on.
        for (id, _) in history.results.files() {
            if used <= cfg.disk_max_bytes {
                break;
            }
            if let Some(freed) = self.drop_result_file(&id) {
                spend(&mut used, freed);
                report.live_results_removed += 1;
            }
        }

        // One re-measure at the end: what the running total estimated is not what `/api/status`
        // reports.
        let usage = disk::measure_roots(&roots);
        report.used_bytes = usage.billed;
        report.foreign_bytes = usage.foreign;
        // `before - used`, NOT `before - used_bytes`: the bytes this sweep actually unlinked,
        // summed from the `spend` calls above, rather than the difference between two walks of
        // the tree.
        //
        // The difference is not equivalent, because the two walks are not of the same tree.
        // Nothing here holds a lock over the filesystem: a spill landing, a journal segment
        // being rewritten, any concurrent write between the two `measure_roots` calls lands in
        // `before - used_bytes` and is reported as though the sweeper had reclaimed it. That is
        // wrong twice over — it misreports the log line an operator reads, and it made
        // `a_free_space_shortfall_the_engine_did_not_cause_deletes_nothing` flaky: that test
        // asserts the sweeper freed nothing, and CI saw `freed_bytes: 4376` with every removal
        // counter at zero, i.e. bytes "freed" by a sweep that unlinked nothing. Byte-identical
        // trees passed and failed the same assertion minutes apart.
        //
        // Summing the unlinks is both deterministic and closer to what the field means. It does
        // report gross rather than net: pruning a statement that appends a tombstone now counts
        // the bytes removed, not the removal minus the tombstone. `used_bytes` remains the
        // re-measured truth for what is on disk, which is the number `/api/status` serves.
        report.freed_bytes = before.saturating_sub(used).saturating_add(retention_freed);
        report.over_budget = report.used_bytes > cfg.disk_max_bytes;

        // The free-space floor, measured *after* the pruning it does not drive — nothing above
        // this line read it — and measured against the mount of **every** managed directory, not
        // just the root's (§3: "a subtree moved to another volume is floored against that
        // volume"). Asking only about the root meant an `OXIDANT_RESULT_DIR` on a second volume
        // was never checked, and a healthy results volume was reported short because the root's
        // was. One mount-table probe answers all of them.
        let mounts = mounts_for(cfg);
        let mut lowest: Option<(&std::path::Path, u64)> = None;
        for root in &roots {
            let Some(free) = mounts.free_bytes(root.path()) else {
                continue;
            };
            if lowest.map_or(true, |(_, seen)| free < seen) {
                lowest = Some((root.path(), free));
            }
        }
        report.free_bytes = lowest.map(|(_, free)| free);
        report.low_free = report
            .free_bytes
            .is_some_and(|free| free < cfg.disk_min_free_bytes);
        // Stop writing rather than start deleting: a spill is by far the largest write the
        // engine makes, and the rows it would have written are still in memory and still serve
        // `/result`. The journal keeps writing — its records are small, and refusing them would
        // lose exactly the statement history this guard exists to protect.
        history.results.set_paused(report.low_free);

        if report.removed_anything() || report.over_budget || report.low_free {
            tracing::info!(
                used_bytes = report.used_bytes,
                // Bytes another tool wrote into `OXIDANT_EVENT_LOG_DIR`. Reported so an operator
                // can see why the directory is large, never billed: the engine cannot prune one
                // of them (H2/F16).
                foreign_bytes = report.foreign_bytes,
                freed_bytes = report.freed_bytes,
                budget_bytes = cfg.disk_max_bytes,
                free_bytes = report.free_bytes,
                min_free_bytes = cfg.disk_min_free_bytes,
                rolled_logs = report.rolled_logs_removed,
                logs_expired = report.logs_expired,
                logs_over_cap = report.logs_over_cap,
                dumps_expired = report.dumps_expired,
                event_logs_pruned = report.event_logs_pruned,
                event_log_rolled = report.event_log_rolled,
                dumps = report.dumps_removed,
                orphan_results = report.orphan_results_removed,
                statements = report.statements_pruned,
                live_results = report.live_results_removed,
                over_budget = report.over_budget,
                low_free = report.low_free,
                "disk sweep"
            );
        }
        if report.low_free {
            tracing::warn!(
                free_bytes = report.free_bytes,
                directory = lowest.map(|(root, _)| root.display().to_string()),
                min_free_bytes = cfg.disk_min_free_bytes,
                used_bytes = report.used_bytes,
                budget_bytes = cfg.disk_max_bytes,
                "the volume is below OXIDANT_DISK_MIN_FREE_BYTES: result spill is paused and \
                 /api/status reports disk: low_free. No statement history is deleted for a \
                 shortfall the engine's own budget did not condemn — raise the floor, free \
                 space, or lower OXIDANT_DISK_MAX_BYTES to make the engine prune its own."
            );
        }
        let ordering = std::sync::atomic::Ordering::Relaxed;
        self.disk.over_budget.store(report.over_budget, ordering);
        self.disk.low_free.store(report.low_free, ordering);
        if report.removed_anything() {
            // A pruned statement is one a `?wait=true` caller may be parked on, and its answer
            // is now "gone" rather than "still running". This reaches the background sweeper's
            // callers only because that thread shares the store's `Notify` rather than
            // rebuilding one per tick.
            self.notify.notify_waiters();
        }
        report
    }

    /// Evict the single oldest terminal statement, tombstone it, and unlink its result.
    ///
    /// `None` means there was nothing evictable — everything left is still running. `Some(bytes)`
    /// is what the result unlink freed, which is what lets the sweeper keep a running total
    /// instead of re-walking the data directory per victim (M2).
    fn prune_oldest_statement(&self) -> Option<u64> {
        let history = self.history.clone()?;
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
        let (id, submitted_at_ms) = victim?;
        let freed = history.results.unlink(&id).unwrap_or(0);
        let seq = history.journal.next_seq();
        // Retained: the file is already gone, so a dropped tombstone would leave a pointer in
        // the journal naming nothing. See `sweep_history` for the same reasoning.
        history
            .journal
            .append_retained(JournalRecord::tombstone(&id, seq, submitted_at_ms));
        Some(freed)
    }

    /// Unlink one statement's result file but keep the statement, and journal the clearing so a
    /// restart does not read a pointer to a file that is gone.
    fn drop_result_file(&self, id: &str) -> Option<u64> {
        let history = self.history.clone()?;
        let freed = history.results.unlink(id)?;
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
        Some(freed)
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
        // The seam is process-global by design — a production process boots exactly one store —
        // so in a test binary every store would clobber every other one's counters and the
        // end-to-end test would read whichever store booted last. The test that drives
        // `/api/status` for real claims the seam on its own thread first; every other store —
        // including one booted concurrently by another test — publishes nothing and those tests
        // read their own counters through `status_counters()`.
        #[cfg(test)]
        if !tests::status_seam::is_owner() {
            return;
        }
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
    /// accumulating one sleeping thread per store it builds. The one thing it does **not**
    /// rebuild per tick is the `Notify`: a sweep can prune a statement a `?wait=true` caller is
    /// parked on, and a private channel would leave that caller blocked to its timeout.
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

/// The mount table this sweep pass reads free space from — a test's synthetic one, or the host's.
///
/// The floor is the one disk guard that cannot be exercised from a tempdir — it depends on how
/// full the *host* volume is, and on which volume each managed directory sits — and it is also
/// the guard whose misbehaviour deleted the whole statement history, so it gets a seam rather
/// than going untested.
fn mounts_for(cfg: &HistoryConfig) -> disk::Mounts {
    match cfg.mounts_override() {
        Some(entries) => disk::Mounts::from_entries(entries),
        None => disk::Mounts::probe(),
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

/// Backtick-quote an identifier, **doubling** any backtick inside it.
///
/// This is the Databricks dialect's own escape — its backquoted-identifier rule is
/// `` '`' ( ~'`' | '``' )* '`' ``, with no backslash escape — so doubling is both the complete
/// way to keep a name from breaking out of its quotes and the only way to keep it *the same
/// name*. Stripping the backticks instead (what this did) is equally safe against injection
/// and quietly wrong: a table genuinely called `` we`ird `` was described as `weird`, so
/// `DESCRIBE TABLE` found nothing and the rail's column expand answered `500` on a row whose
/// **Preview** — which quotes by the same rule in `catalog_rail.js` — worked.
fn quote_identifier(id: &str) -> String {
    format!("`{}`", id.replace('`', "``"))
}

/// Fetch column (name, type) pairs for a fully qualified table, or None if the
/// table cannot be described.
///
/// **Known gap, one layer down.** The `DESCRIBE TABLE` built here now quotes the way the
/// dialect escapes, but `oxidant-loom`'s `parse_qualified_name` unquotes by dropping every
/// backtick it sees — so a table genuinely called `` we`ird `` is looked up as `weird` and this
/// still answers `None`. `SELECT * FROM` on the same name works (DataFusion tokenizes it
/// itself), which is why the catalog rail can preview such a table and not expand its columns.
/// Fixing it means teaching the engine's own identifier unescaping and probe-SQL re-quoting
/// about doubled backticks, which is not a change to these routes.
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
    /// Where the rolled exec logs are, for `GET /api/v1/logs?file=`. `dir: None` — no rolling
    /// writer in this process — makes every `?file=` answer `404`.
    logs: LogView,
    /// Shared bearer token guarding `GET /api/v1/logs`. `None` — the default — makes that one
    /// route answer `404`; nothing else in this router is authenticated.
    status_token: Option<Arc<str>>,
    /// §6b's diagnostic dumps. `None` under `OXIDANT_HISTORY=off`, which promises that nothing
    /// is written under the data dir — and a support bundle is the largest thing that would be.
    dumps: Option<Arc<crate::logging::DumpStore>>,
}

/// Build the REST statement-execution router around a shared Spark Connect service.
pub fn router(service: Arc<OxidantService>) -> Router {
    // Fallback for an embedded caller that builds this router directly: the Connect bootstrap
    // and `run_worker` both call `logging::init(role, port)` first, and it is idempotent, so
    // this only ever wins when nothing else initialized the process's logging.
    crate::logging::init("driver", 0);
    let log_buffer = crate::logging::buffer();
    // The statement store is attached to the service at boot ([`init_statement_store`]) so the
    // Connect path writes into the same history this router reads. A service that never had one
    // attached (an embedded caller building the router directly) gets today's volatile store.
    let store = service
        .statement_store()
        .cloned()
        .unwrap_or_else(StatementStore::new);
    // **The config the store booted with, never a fresh read of the environment.** The dump
    // store must land in the directory the sweeper prunes and the disk budget measures, and
    // under `OXIDANT_DATA_DIR_PER_PROCESS` that directory depends on the process's own
    // `(role, port)` — which this function does not have and used to guess as `0`.
    let dumps = dumps_for(&store);
    app(RestState {
        service,
        store,
        log_buffer,
        logs: LogView::process(),
        status_token: oxidant_ui_server::status::status_token_from_env().map(Into::into),
        dumps,
    })
}

/// The dump store a router gets: built from *this* store's own [`HistoryConfig`], so
/// `DumpStore.dir` is `cfg.dumps_dir` — the same path [`StatementStore::sweep_disk`] prunes and
/// the same tree `disk::budget_roots` bills. Its own seam so the wiring is testable.
fn dumps_for(store: &StatementStore) -> Option<Arc<crate::logging::DumpStore>> {
    store
        .history_config()
        .and_then(crate::logging::DumpStore::from_config)
}

/// Build the process's statement store from the environment and attach it to `service`.
///
/// Called once at boot, before anything can execute, because both the REST API and Connect's
/// `ExecutePlan` record into it. `Err` is a boot failure with the reason already spelled out —
/// a data dir another process holds, or a root that names an object store.
pub fn init_statement_store(service: &OxidantService, role: &str, port: u16) -> Result<(), String> {
    if service.statement_store().is_some() {
        return Ok(());
    }
    // A second server in the same process shares the first's journal rather than opening a
    // second writer on the same files — the lockfile is a cross-process guard, not an
    // in-process one (the durable-history spec §4c). Several of the connect integration tests
    // boot servers concurrently in one process; without this share, every boot after the
    // first fails the lock and the server never comes up.
    static PROCESS_STORE: Mutex<Option<StatementStore>> = Mutex::new(None);
    let mut shared = PROCESS_STORE.lock().expect("statement store init poisoned");
    let store = match shared.as_ref() {
        Some(existing) => existing.clone(),
        None => {
            let built = StatementStore::from_env(role, port)?;
            *shared = Some(built.clone());
            built
        }
    };
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
        .route(
            "/api/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/stats",
            get(table_stats),
        )
        .route(
            "/api/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/comment",
            put(set_table_comment),
        )
        .route("/api/v1/cluster/status", get(cluster_status))
        .route("/api/v1/logs", get(list_logs))
        .route("/api/v1/logs/files", get(list_log_files))
        .route("/api/v1/logs/tail", get(tail_logs))
        .route("/api/v1/logs/workers", get(list_log_workers))
        .route("/api/v1/logs/dump", post(create_dump))
        .route("/api/v1/logs/dump/{dump_id}", get(get_dump))
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
        ResultSource::Disk(journaled) => match state.store.read_spilled(&id, journaled).await {
            Some(batches) => batches,
            None => return error_response(StatusCode::GONE, "result_expired"),
        },
        // A correct empty answer, not a lost one. `resultStatus: "result_empty"` on the status
        // document says which, and this answers the same before and after a restart.
        ResultSource::Empty => Vec::new(),
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

/// `GET /api/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/stats` — freshness a
/// harvester can poll without scanning the table: the current Iceberg snapshot's summary, or
/// Delta's latest commit timestamp. Never runs a query and never scans a data file.
///
/// Three outcomes a caller must be able to tell apart, so they get three different answers:
/// a table with freshness reports it (`"stats_source": "snapshot_metadata"`); a readable table
/// with nothing to report — a format that carries no snapshot metadata, an Iceberg table with
/// no snapshot yet — reports nulls with `"unavailable"`; and a table whose metadata could not
/// be read (corrupt `metadata.json`, unreachable store) is a `500`, never a `200` that a
/// harvester would file away as "this table just has no stats".
async fn table_stats(
    State(state): State<RestState>,
    Path((catalog, namespace, table)): Path<(String, String, String)>,
) -> Response {
    let engine = state.service.engine();
    let registry = state.service.registry();
    // Namespaces are dot-joined everywhere else; split on '.' for consistency.
    let ns_parts: Vec<String> = namespace.split('.').map(|s| s.to_string()).collect();

    let stats = if catalog == DEFAULT_CATALOG {
        // Builtin tables are session-registered Parquet/CSV/temp views, not a lakehouse format
        // with snapshot metadata — but a wrong name still 404s like every other route here.
        let schema = ns_parts
            .last()
            .cloned()
            .unwrap_or_else(|| "default".to_string());
        if !engine
            .builtin_table_names(&schema)
            .iter()
            .any(|name| name == &table)
        {
            return error_response(StatusCode::NOT_FOUND, "unknown table");
        }
        oxidant_loom::catalog_bridge::TableFreshnessStats {
            row_count: None,
            data_updated_at: None,
            format: "unknown",
            stats_source: "unavailable",
        }
    } else {
        let Some(provider) = registry.provider(&catalog) else {
            return error_response(StatusCode::NOT_FOUND, "unknown catalog");
        };
        let md = match provider.load_table(&ns_parts, &table).await {
            Ok(md) => md,
            // `Error::Plan` is the provider's "doesn't exist" classification (see
            // `CatalogProvider::table_exists`'s doc comment) — everything else is a genuine
            // backend failure, reported the same way `list_tables`/`list_namespaces` do.
            Err(oxidant_catalog::Error::Plan(_)) => {
                return error_response(StatusCode::NOT_FOUND, "unknown table")
            }
            Err(e) => {
                tracing::warn!(catalog = %catalog, namespace = %namespace, table = %table,
                    error = %e, "table stats: loading catalog metadata failed");
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "table stats: unable to load the table from its catalog",
                );
            }
        };
        match oxidant_loom::catalog_bridge::table_freshness_stats(&engine.session_state(), &md)
            .await
        {
            Ok(stats) => stats,
            // The table exists and the engine could not read its own format metadata. The
            // detail names object-store locations, so it goes to the log, not the body.
            Err(e) => {
                tracing::warn!(catalog = %catalog, namespace = %namespace, table = %table,
                    format = e.format, error = %e.detail,
                    "table stats: reading snapshot metadata failed");
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!(
                        "table stats: unable to read this table's {} snapshot metadata",
                        e.format
                    ),
                );
            }
        }
    };

    Json(json!({
        "row_count": stats.row_count,
        "data_updated_at": stats.data_updated_at,
        "format": stats.format,
        "stats_source": stats.stats_source,
    }))
    .into_response()
}

/// Glue caps a table `Description` at 2048 characters — the tightest limit among the catalogs
/// this route can write to. Checking it here turns "the caller sent too much text" into a `400`
/// that names the limit, instead of the opaque `500` a provider-side `ValidationException`
/// becomes. A provider may still reject a shorter comment for its own reasons; that stays a
/// `500`.
const MAX_COMMENT_CHARS: usize = 2048;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SetCommentBody {
    /// Absent, explicit `null`, `""`, and whitespace-only all clear the comment
    /// (`SetComment(None)`); any other string is stored verbatim, leading/trailing whitespace
    /// included. `#[serde(default)]` is what makes "absent" and "null" the same case — there is
    /// no reason to reject a client that just omits the field.
    ///
    /// `deny_unknown_fields` is load-bearing *because* the field is optional: without it a
    /// misspelled or wrong-shaped body (`{"description": "..."}` — Glue's own name for this
    /// field) would deserialize to "no comment given" and silently wipe an existing comment with
    /// a `200`. A body this route does not understand must be rejected, never guessed at.
    #[serde(default)]
    comment: Option<String>,
}

/// `PUT /api/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/comment` — set or clear
/// one table's catalog-level comment (`{"comment": "..."}`) via
/// `CatalogProvider::alter_table`'s `TableChange::SetComment`, answering with the post-write
/// state.
///
/// The body is the whole desired state of the comment, so an absent/`null`/empty/whitespace-only
/// `comment` clears it. The response reports `alter_table`'s returned metadata, which the SPI
/// defines as the *post-alter* state (Glue re-reads the table with a fresh `GetTable`;
/// `LocalCatalog` returns the persisted manifest entry) — not an echo of the request.
///
/// Only a registered external catalog implements `alter_table`; the builtin `spark_catalog` has
/// no `CatalogProvider` to alter, so it `404`s exactly like any other unregistered catalog name —
/// the same "wrong coordinates" answer `table_stats` gives, never a silent no-op.
///
/// The failure mapping separates *whose fault it is*, because the caller acts on each one
/// differently:
///
/// - `403` — a provider's access-denied refusal, carrying the catalog's own message verbatim, so
///   the Oxidant Platform shows *why* the write was refused instead of a generic "internal
///   error".
/// - `404` — the table does not exist.
/// - `409` — the table exists but this catalog will not alter it (a `LocalCatalog` table declared
///   in configuration). `Error::Plan` is *both* "doesn't exist" and "exists, refused", so the
///   two are told apart by asking the provider whether the table exists; answering `404` for a
///   table the caller can see listed and queried would be a lie.
/// - `501` — the catalog has no `alter_table` at all (`Error::Unsupported`: the SPI default, e.g.
///   Hive and REST catalogs). Never `500`: this is permanent, and a client that retries it is
///   wasting its time.
/// - `500` — everything else (throttling, network, a malformed table definition). The detail can
///   name object-store locations, so it goes to the log, not the body.
async fn set_table_comment(
    State(state): State<RestState>,
    Path((catalog, namespace, table)): Path<(String, String, String)>,
    Json(body): Json<SetCommentBody>,
) -> Response {
    let registry = state.service.registry();
    // Namespaces are dot-joined everywhere else; split on '.' for consistency.
    let ns_parts: Vec<String> = namespace.split('.').map(|s| s.to_string()).collect();

    let Some(provider) = registry.provider(&catalog) else {
        return error_response(StatusCode::NOT_FOUND, "unknown catalog");
    };
    // There is no such thing as a table comment that is `""` — or `"   "` — rather than unset:
    // both render as nothing and mean nothing, so they clear. A comment that survives that is
    // stored verbatim; trimming a caller's text would be editing their content behind their back.
    let comment = body.comment.filter(|c| !c.trim().is_empty());
    if let Some(c) = comment.as_deref() {
        let len = c.chars().count();
        if len > MAX_COMMENT_CHARS {
            return error_response(
                StatusCode::BAD_REQUEST,
                &format!(
                    "table comment: {len} characters exceeds the {MAX_COMMENT_CHARS}-character \
                     limit"
                ),
            );
        }
    }
    let change = oxidant_catalog::TableChange::SetComment(comment);
    match provider.alter_table(&ns_parts, &table, vec![change]).await {
        Ok(md) => Json(json!({ "comment": md.comment })).into_response(),
        // `Error::Plan` is the provider's "doesn't exist" classification (see
        // `CatalogProvider::table_exists`'s doc comment), but it is *also* how a provider refuses
        // to alter a table that plainly exists — `LocalCatalog` answers it for a table declared
        // in configuration. Ask before choosing: "unknown table" about a table the caller can
        // list, query and read stats for would send them hunting for a typo that isn't there.
        Err(oxidant_catalog::Error::Plan(detail)) => {
            match provider.table_exists(&ns_parts, &table).await {
                // The provider's own sentence says what to do about it ("edit the config file
                // instead"), which is the whole value of surfacing it — same rule as `403`.
                Ok(true) => error_response(StatusCode::CONFLICT, &detail),
                // Missing, or the existence probe itself failed: "not found" is the honest
                // reading of a `Plan` error with nothing to contradict it.
                _ => error_response(StatusCode::NOT_FOUND, "unknown table"),
            }
        }
        // The provider's own refusal, not the engine's: pass its message through verbatim rather
        // than flattening it into a `500` like the backend failures below.
        Err(oxidant_catalog::Error::Io(detail)) if is_access_denied(&detail) => {
            error_response(StatusCode::FORBIDDEN, &detail)
        }
        // Not a failure — a capability this catalog does not have and never will on this build.
        Err(oxidant_catalog::Error::Unsupported(detail)) => {
            error_response(StatusCode::NOT_IMPLEMENTED, &detail)
        }
        Err(e) => {
            tracing::warn!(catalog = %catalog, namespace = %namespace, table = %table,
                error = %e, "table comment: altering the table in its catalog failed");
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "table comment: unable to alter the table in its catalog",
            )
        }
    }
}

/// Whether a provider's [`oxidant_catalog::Error::Io`] detail names an access-control refusal
/// rather than a network/throttling/other backend failure.
///
/// This is a **heuristic over free-form provider text**, and it is worth being precise about why
/// it cannot be anything better here. The SPI collapses every non-"doesn't exist" failure into
/// one `Error::Io(String)`, so the only thing left to read is the message. AWS providers do keep
/// the service error code in it on purpose — `classify_glue_failure` renders
/// `aws glue UpdateTable: AccessDeniedException: ...`, `classify_lakeformation_failure` the same
/// for `aws lakeformation ...` — but a non-AWS provider is under no such obligation, hence the
/// lowercase phrasings below (S3's code is a bare `AccessDenied`, and plenty of catalogs just
/// say "access denied").
///
/// The two limits a caller should know about:
///
/// - **False negatives are the safe side.** A provider that phrases its refusal some other way
///   falls through to `500`, which is wrong but not misleading about permissions.
/// - **False positives are possible**, because the detail may quote caller-supplied text (a
///   comment containing "access denied", echoed back in a validation error). That is exactly why
///   the `403` body is the provider's own sentence rather than a claim of our own about the
///   caller's permissions.
fn is_access_denied(detail: &str) -> bool {
    let detail = detail.to_ascii_lowercase();
    // `accessdenied` covers `AccessDeniedException` and S3's bare `AccessDenied`; the IAM
    // message body reliably carries "not authorized to perform".
    ["accessdenied", "access denied", "not authorized to perform"]
        .iter()
        .any(|needle| detail.contains(needle))
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

/// The log-browser query string (§6, §6b) — one struct for every log route, because they take
/// the same filters and a caller must not have to remember which route learned which one.
#[derive(Debug, Default, Deserialize)]
struct LogsParams {
    /// `current`, or a `LogPeriod` in §6's grammar with an optional `.N` split. Absent keeps
    /// today's answer: the in-memory ring.
    file: Option<String>,
    /// Lines per page. Defaults to `logging::MAX_LOG_LINES` — the same 1000 the ring serves — and is
    /// clamped to `logging::MAX_LOG_PAGE`.
    limit: Option<usize>,
    /// PR3's oldest-first walk: lines to skip from the start of the file.
    offset: Option<usize>,
    /// PR4's backward cursor: serve the lines *before* this row index, newest-first (§6b).
    before: Option<u64>,
    /// The follow cursor: the matches at or after this row index, oldest-first, with the
    /// position to resume from. What `/api/v1/logs/tail` rides against a worker.
    after: Option<u64>,
    /// `desc` asks for the newest-first page without passing a filter to imply it.
    order: Option<String>,
    /// Severity floor: `warn` is "warn **and** error".
    level: Option<String>,
    /// Target prefix — `oxidant_execution` matches `oxidant_execution::plan`.
    target: Option<String>,
    /// Free text over the rendered line, case-insensitive.
    q: Option<String>,
    /// RFC-3339 bounds on `ts`, matched against the column §6's writer emits. Half-open.
    from: Option<String>,
    to: Option<String>,
    /// Federation: read *that worker's* logs instead of this node's, over its own Flight surface
    /// (§6b). Never a raw address — see [`resolve_worker`].
    worker: Option<String>,
}

impl LogsParams {
    /// The transport-independent query [`crate::logging::answer`] takes.
    fn query(&self) -> crate::logging::LogQuery {
        crate::logging::LogQuery {
            op: None,
            file: self.file.clone(),
            level: self.level.clone(),
            target: self.target.clone(),
            q: self.q.clone(),
            from: self.from.clone(),
            to: self.to.clone(),
            limit: self.limit,
            offset: self.offset,
            before: self.before,
            after: self.after,
            order: self.order.clone(),
        }
    }
}

/// `GET /api/v1/logs` — one node's exec log, for the monitoring UI's Observability page.
///
/// Four answers, one route:
///
/// - no `?file=` — today's shape, `{"logs": [...]}` from the in-memory ring, **unchanged**
///   except that each line now leads with an RFC-3339 UTC timestamp (§6);
/// - `?file=current` — the live `oxidant.log` on disk;
/// - `?file=<period>[.N]` — one rolled file, served from its `.parquet` if it has been
///   converted and from its `.log` if it has not. The caller never names an extension: which
///   one exists is §6's conversion state machine, not the caller's business;
/// - `?worker=<id>` — any of the above, from **that worker's own files**, proxied over its
///   Flight surface (§6b). No worker log bytes reach this driver's disk on this path.
///
/// Filters (`level`, `target`, `q`, `from`, `to`) and the backward cursor (`before`) compose
/// over all four. Passing any of them switches the answer to §6b's newest-first cursor page;
/// passing none keeps PR3's oldest-first `?offset=` page byte-for-byte.
///
/// Gated by the same shared token as `/api/status`, through the same code — restated here
/// because it now matters more. The endpoint used to expose 1000 lines of ring buffer; it now
/// exposes up to `OXIDANT_LOG_KEEP_DAYS` of every enabled `tracing` field value, on every node
/// in the cluster, and this router is served under a permissive CORS layer. Unset
/// `OXIDANT_STATUS_TOKEN`, `404`: the route does not exist, exactly like `/api/status`.
async fn list_logs(
    State(state): State<RestState>,
    headers: header::HeaderMap,
    uri: axum::http::Uri,
) -> Response {
    let Some(params) = gate_log_params(&state, &headers, &uri) else {
        return log_gate_response(&state, &headers, &uri);
    };
    run_log_query(&state, &params, params.query()).await
}

/// `GET /api/v1/logs/files` — every log file this node still has, newest period first (§6b).
///
/// "The visible history is always honestly what exists": it is a directory read, so a file
/// retention took is simply absent rather than offered and then `404`ing.
async fn list_log_files(
    State(state): State<RestState>,
    headers: header::HeaderMap,
    uri: axum::http::Uri,
) -> Response {
    let Some(params) = gate_log_params(&state, &headers, &uri) else {
        return log_gate_response(&state, &headers, &uri);
    };
    let mut query = params.query();
    query.op = Some("files".to_string());
    run_log_query(&state, &params, query).await
}

/// `GET /api/v1/logs/workers` — the worker picker's list: every worker this driver is configured
/// with, and whether it answers (§6b).
///
/// **A worker that does not answer is listed `reachable: false` with the reason, never silently
/// skipped.** A log browser that quietly drops a node is worse than one that has none: the
/// operator reads "no errors on worker 2" when what happened is "worker 2 is dead", which is the
/// error they were looking for.
async fn list_log_workers(State(state): State<RestState>, headers: header::HeaderMap) -> Response {
    if let Some(denied) =
        oxidant_ui_server::status::deny_unless_authorized(state.status_token.as_deref(), &headers)
    {
        return denied;
    }
    // The same list `?worker=` resolves against, so the picker cannot offer an id the read
    // route would then refuse — nor a node a `spark.conf.set` put there.
    let workers = state.service.configured_workers();
    let probes = workers.iter().map(|address| {
        let address = address.clone();
        async move {
            let id = worker_id(&address);
            // The existing liveness action, with a bound: an unreachable worker must not hold
            // the picker open for a TCP timeout on every page load.
            let probe = tokio::time::timeout(
                WORKER_PROBE_TIMEOUT,
                oxidant_execution::flight::health_check_worker(address.clone()),
            )
            .await;
            let (reachable, error) = match probe {
                Ok(Ok(())) => (true, None),
                Ok(Err(e)) => (false, Some(e.to_string())),
                Err(_) => (false, Some("timed out".to_string())),
            };
            json!({
                "worker_id": id,
                "address": address,
                "reachable": reachable,
                "error": error,
            })
        }
    });
    let mut rows = futures::future::join_all(probes).await;
    // "driver" is a member of the picker, not a special case above it: the same filters read the
    // same way whichever node is selected, which is the whole point of one `answer`.
    rows.insert(
        0,
        json!({
            "worker_id": "driver",
            "address": Value::Null,
            "reachable": true,
            "error": Value::Null,
        }),
    );
    Json(json!({ "workers": rows })).into_response()
}

/// How long a worker has to answer the liveness probe behind `GET /api/v1/logs/workers` before
/// it is reported unreachable. Short on purpose: this runs once per page load, per worker.
const WORKER_PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// `GET /api/v1/logs/tail` — SSE follow (§6b).
///
/// **The driver's tail rides `tracing` itself, not a file poll.** The rolling writer's queue,
/// its dedup hold and its 5 s timer all sit between an event and the file, so a follow that
/// re-read `oxidant.log` would lag by up to that timer and re-decode the tail every tick. The
/// stream therefore marks itself `"dedup": false` — it is the *ring*'s view, and §6/F21 says the
/// file is authoritative — which is the same statement `?file=current`'s `"dedup": true` makes
/// from the other side.
///
/// **A worker's tail is a poll, and says so.** Flight's `do_action` returns a stream, but a
/// long-lived one would pin a worker-side task to a browser tab; instead the driver re-asks the
/// worker's `?file=current` on a timer and forwards what is new. The `mode` field on the first
/// event is `"follow"` for the driver and `"poll"` for a worker, so a reader is never told a
/// 2 s-granular feed is live.
///
/// **What a tail will follow is exactly one source per node**, and naming another is a `400`
/// rather than a silent substitution — see [`check_tail_source`].
async fn tail_logs(
    State(state): State<RestState>,
    headers: header::HeaderMap,
    uri: axum::http::Uri,
) -> Response {
    let Some(params) = gate_log_params(&state, &headers, &uri) else {
        return log_gate_response(&state, &headers, &uri);
    };
    let filter = match crate::logging::LogFilter::parse(
        params.level.as_deref(),
        params.target.as_deref(),
        params.q.as_deref(),
        params.from.as_deref(),
        params.to.as_deref(),
    ) {
        Ok(f) => f,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e),
    };
    if let Err(refusal) = check_tail_source(params.worker.as_deref(), params.file.as_deref()) {
        return error_response(StatusCode::BAD_REQUEST, &refusal);
    }
    match &params.worker {
        None => Sse::new(driver_tail(filter)).into_response(),
        Some(requested) => {
            let endpoint = match resolve_worker(&state, requested) {
                Ok(endpoint) => endpoint,
                Err(response) => return response,
            };
            Sse::new(worker_tail(endpoint, params, state.status_token.clone())).into_response()
        }
    }
}

/// Whether this node can follow what the caller named — and a refusal that says why if not.
///
/// **A follow shows what a node is writing now, and each node has exactly one such source.**
/// `worker_tail` used to override `file` to `current` unconditionally, so **Node = worker, File
/// = memory ring** painted a page out of the worker's in-memory ring and then appended a tail
/// out of the worker's `oxidant.log` — two sources with different dedup semantics concatenated
/// into one pane, under an `open` event asserting `"dedup": true` about a ring that is never
/// deduped. On a worker with `OXIDANT_LOG_ROLL=off` it was worse: there is no `current` to
/// poll, so the substitution produced an `error` event every 2 s forever under a caption
/// reading "following".
///
/// The ring is followable on the **driver** and only there, because the driver's tail is not a
/// file poll at all — it is the `tracing` broadcast, which is the very stream the ring holds.
/// A worker's is a `?file=` poll over Flight, and a rolling buffer has no forward cursor: an
/// index into it names a different line every time the node logs one (F9), so a poll cannot
/// tell a new line from one that shifted.
fn check_tail_source(worker: Option<&str>, file: Option<&str>) -> Result<(), String> {
    match (worker, file) {
        (_, Some("current")) => Ok(()),
        (None, None) => Ok(()),
        (Some(worker), None) => Err(format!(
            "worker `{worker}`'s memory ring cannot be followed: it is a rolling buffer with no              forward cursor, so a poll cannot tell a new line from one that shifted. Add              `file=current` to follow that worker's live file, or read the ring without              following it. The driver's own ring is followable because its tail is the `tracing`              stream itself rather than a poll"
        )),
        (_, Some(rolled)) => Err(format!(
            "`{rolled}` is a rolled file: it will never grow again, so there is nothing to              follow. Use `file=current` for the live file, or read the rolled file with              `/api/v1/logs`"
        )),
    }
}

/// The driver's own follow: every `tracing` event, filtered, as it happens.
fn driver_tail(
    filter: crate::logging::LogFilter,
) -> impl futures::Stream<Item = Result<sse::Event, std::convert::Infallible>> {
    let rx = crate::logging::subscribe_tail();
    let open = sse::Event::default()
        .event("open")
        .data(json!({ "mode": "follow", "worker": "driver", "dedup": false }).to_string());
    futures::stream::once(async move { Ok(open) }).chain(futures::stream::unfold(
        (rx, filter),
        |(mut rx, filter)| async move {
            loop {
                match rx.recv().await {
                    Ok(line) => {
                        if filter.is_empty()
                            || filter.keeps(&crate::logging::parse_for_filter(&line), &line)
                        {
                            let event = sse::Event::default().event("line").data(line);
                            return Some((Ok(event), (rx, filter)));
                        }
                    }
                    // **The gap is never silent.** A reader that falls behind the fan-out is
                    // told exactly how many lines it lost, in its own stream, rather than
                    // seeing a jump it cannot account for.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        let event = sse::Event::default()
                            .event("dropped")
                            .data(json!({ "dropped": n }).to_string());
                        return Some((Ok(event), (rx, filter)));
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                }
            }
        },
    ))
}

/// How often a worker tail re-asks. Matches the Observability page's own poll: a follow that
/// asked faster would cost a Flight round-trip per second per open tab and buy nothing a reader
/// can see.
const WORKER_TAIL_POLL: Duration = Duration::from_secs(2);

/// A worker's tail: `?file=current` re-asked on a timer, forwarding only what is new.
///
/// "New" is the **follow cursor**, not a text comparison: two identical lines a second apart are
/// two events, and comparing text would swallow the second. The cursor is a scan position rather
/// than a match position, so a selective filter does not make the poll re-read the rows it
/// already rejected. A cursor that comes back *smaller* than the one sent means the worker rolled
/// its live file mid-follow, and the stream says so and restarts at row 0 rather than waiting for
/// the new file to grow past the old one's length.
fn worker_tail(
    endpoint: String,
    params: LogsParams,
    token: Option<Arc<str>>,
) -> impl futures::Stream<Item = Result<sse::Event, std::convert::Infallible>> {
    let worker = params.worker.clone().unwrap_or_default();
    let open = sse::Event::default().event("open").data(
        json!({
            "mode": "poll",
            "worker": worker,
            "dedup": true,
            "poll_ms": WORKER_TAIL_POLL.as_millis() as u64,
        })
        .to_string(),
    );
    let start = Follow {
        endpoint,
        params,
        token,
        after: None,
        started: false,
    };
    futures::stream::once(async move { Ok(open) }).chain(futures::stream::unfold(
        start,
        |mut st| async move {
            loop {
                if st.started {
                    tokio::time::sleep(WORKER_TAIL_POLL).await;
                }
                st.started = true;
                let value = match federate(&st.endpoint, &st.query(), st.token.as_deref()).await {
                    Ok(v) => v,
                    Err(e) => {
                        // The follow does not end because one poll failed — a worker restart is
                        // exactly when an operator is watching — but the failure is said out
                        // loud rather than looking like a quiet worker.
                        let event = sse::Event::default()
                            .event("error")
                            .data(json!({ "error": e.message, "status": e.status }).to_string());
                        st.absorb_failure();
                        return Some((Ok(event), st));
                    }
                };
                match st.absorb(&value) {
                    TailStep::Rolled => {
                        let event = sse::Event::default()
                            .event("rolled")
                            .data(json!({ "worker": st.params.worker }).to_string());
                        return Some((Ok(event), st));
                    }
                    TailStep::Lines(lines) => {
                        let event = sse::Event::default()
                            .event("lines")
                            .data(json!(lines).to_string());
                        return Some((Ok(event), st));
                    }
                    TailStep::Idle => continue,
                }
            }
        },
    ))
}

/// One worker follow, as a value: where to ask next, and what an answer means for that.
///
/// Its own type because the arithmetic below is the whole contract of a federated follow and it
/// used to be inline in a `stream::unfold` closure, which no test could reach.
struct Follow {
    endpoint: String,
    params: LogsParams,
    /// The credential this follow presents to the worker on every poll — the same bearer the
    /// caller presented to open the stream (F16). Held for the life of the follow because a
    /// tail re-asks forever, and re-reading the env per poll would let a stream outlive the
    /// configuration it was authorized under.
    token: Option<Arc<str>>,
    /// The forward cursor: a **scan** position in the worker's live file. `None` until a poll
    /// has named the end of that file.
    after: Option<u64>,
    /// Whether a poll has been issued at all. Deliberately *not* `after.is_some()`: a poll that
    /// failed leaves the cursor unset, and without this flag the retry would spin without its
    /// 2 s sleep.
    started: bool,
}

/// What one poll's answer means for the follow.
#[derive(Debug, PartialEq, Eq)]
enum TailStep {
    /// Lines to forward.
    Lines(Vec<String>),
    /// The worker rolled its live file under the follow.
    Rolled,
    /// Nothing to say — the seeding probe, or a tick with no new lines.
    Idle,
}

impl Follow {
    /// The query for the next poll.
    ///
    /// **The first poll asks for a position, not a page.** It used to ask for
    /// `order=desc&limit=200` and emit the answer, which duplicated lines twice over: the pane
    /// had already painted the newest 500 lines of the same file a moment earlier, and the
    /// cursor it derived — `next_before + lines.len()` — mixed a *match* position with a match
    /// *count*. `ForwardPage`'s own doc states the rule that violated: a cursor built from the
    /// last match re-reads, and re-emits, every non-matching row after it on every poll, so the
    /// tighter the filter the worse the duplication. Asking for the rows *after the end of the
    /// file* names the end and returns nothing, which is what "a follow starts where the log is"
    /// means — and it is what the driver's own tail, which has no seed, already did.
    fn query(&self) -> crate::logging::LogQuery {
        let mut query = self.params.query();
        // The caller's file, *not* an unconditional `current`. Overriding it here is what made
        // a worker's "memory ring" stream the worker's `oxidant.log` instead — see
        // [`check_tail_source`], which is the gate that makes this line safe by having already
        // refused every value but `current`.
        query.before = None;
        query.offset = None;
        query.order = None;
        query.limit = Some(TAIL_PAGE);
        query.after = Some(self.after.unwrap_or(u64::MAX));
        query
    }

    /// Fold a *failed* poll into the cursor: it does not move, including when it is unset.
    ///
    /// This used to be `self.after = self.after.or(Some(0))`, under a comment saying a recovered
    /// worker would resume rather than replay. `Some(0)` **is** the replay, and the arm is only
    /// reached when the cursor is unset — i.e. when the *first* poll failed, which is the common
    /// case: it is the poll issued the instant a worker goes down, or the first poll against one
    /// already down. The worker would come back and the stream would walk its entire live file
    /// from the top, `TAIL_PAGE` rows every 2 s — hours of ancient history under a caption
    /// reading "following". Left unset, the next successful poll re-seeds at the end of the
    /// file, which is all [`Follow::query`] ever needed.
    fn absorb_failure(&mut self) {}

    /// Fold one node's answer into the cursor.
    fn absorb(&mut self, value: &Value) -> TailStep {
        let lines: Vec<String> = value
            .get("logs")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        match (self.after, value.get("next_after").and_then(Value::as_u64)) {
            // Backward: the worker rolled its live file, so row indices restart.
            (Some(sent), Some(next)) if next < sent => {
                self.after = Some(0);
                TailStep::Rolled
            }
            (_, Some(next)) => {
                self.after = Some(next);
                if lines.is_empty() {
                    TailStep::Idle
                } else {
                    TailStep::Lines(lines)
                }
            }
            // No cursor at all: this was not a forward page. Leave the cursor where it is and
            // re-ask — inventing one from whatever else the envelope carries is how the seed
            // came to re-emit its matches forever.
            (_, None) => TailStep::Idle,
        }
    }
}

/// Lines a worker tail asks for per poll. Two seconds of a chatty worker, bounded.
const TAIL_PAGE: usize = 500;

/// Authorize, then parse — **in that order**, and never with an extractor.
///
/// Axum runs extractors in declaration order and short-circuits on rejection, so a
/// `Query<LogsParams>` parameter answered `400` for `?file=a&file=b` before
/// `deny_unless_authorized` ran — and with `OXIDANT_STATUS_TOKEN` unset these routes' contract
/// is `404`, "the route does not exist, exactly like `/api/status`". A `400`-vs-`404` split
/// tells an unauthenticated caller the route is there (L1).
///
/// `None` means "the caller gets [`log_gate_response`]'s answer instead", which is either the
/// authorization refusal or the parse refusal, in that order.
fn gate_log_params(
    state: &RestState,
    headers: &header::HeaderMap,
    uri: &axum::http::Uri,
) -> Option<LogsParams> {
    if oxidant_ui_server::status::deny_unless_authorized(state.status_token.as_deref(), headers)
        .is_some()
    {
        return None;
    }
    Query::<LogsParams>::try_from_uri(uri).ok().map(|q| q.0)
}

fn log_gate_response(
    state: &RestState,
    headers: &header::HeaderMap,
    uri: &axum::http::Uri,
) -> Response {
    if let Some(denied) =
        oxidant_ui_server::status::deny_unless_authorized(state.status_token.as_deref(), headers)
    {
        return denied;
    }
    let _ = uri;
    error_response(
        StatusCode::BAD_REQUEST,
        "invalid query: expected at most one each of `file`, `limit`, `offset`, `before`, \
         `level`, `target`, `q`, `from`, `to` and `worker`",
    )
}

/// Run one query — locally, or against the named worker.
async fn run_log_query(
    state: &RestState,
    params: &LogsParams,
    query: crate::logging::LogQuery,
) -> Response {
    match &params.worker {
        // "driver" is spelled out in the picker, so accept it here rather than making the UI
        // strip it back off.
        None => local_log_query(state, query).await,
        Some(id) if id == "driver" => local_log_query(state, query).await,
        Some(requested) => {
            let endpoint = match resolve_worker(state, requested) {
                Ok(endpoint) => endpoint,
                Err(response) => return response,
            };
            match federate(&endpoint, &query, state.status_token.as_deref()).await {
                Ok(mut value) => {
                    // §6b: "labels the rows with their worker". The rows are the worker's; the
                    // envelope says whose, so a UI concatenating two nodes cannot lose track.
                    value["worker"] = json!(requested);
                    Json(value).into_response()
                }
                Err(e) => log_error_response(&e),
            }
        }
    }
}

async fn local_log_query(state: &RestState, query: crate::logging::LogQuery) -> Response {
    let view = state.logs.clone();
    let ring = state.log_buffer.clone();
    // **`spawn_blocking`.** Reading a log is `std::fs` I/O plus, for a converted file, a full
    // Parquet decode. Doing that inline on a tokio worker parks a thread that is also serving
    // `ExecutePlan`, for as long as the read takes.
    match tokio::task::spawn_blocking(move || crate::logging::answer(&query, &view, &ring)).await {
        Ok(Ok(mut value)) => {
            value["worker"] = json!("driver");
            Json(value).into_response()
        }
        Ok(Err(e)) => log_error_response(&e),
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("reading the log file panicked: {e}"),
        ),
    }
}

fn log_error_response(e: &crate::logging::LogError) -> Response {
    error_response(
        StatusCode::from_u16(e.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        &e.message,
    )
}

/// The worker id a `?worker=` value must match: `host:port`, the address without its scheme —
/// stable, meaningful, and the same string `/api/v1/cluster/status` already prints.
fn worker_id(address: &str) -> String {
    address
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_end_matches('/')
        .to_string()
}

/// Resolve a `?worker=` value to a dialable endpoint — **only** if it is one of this driver's
/// configured workers.
///
/// This is the SSRF gate, and it is the reason the id is matched rather than the address used.
/// `?worker=` is a query parameter on a route an operator's browser calls; letting it name an
/// arbitrary host would turn the driver into a request forwarder for anything its network can
/// reach, on an endpoint whose token an operator hands to a monitoring page.
fn resolve_worker(state: &RestState, requested: &str) -> Result<String, Response> {
    // **This driver's own configuration, not the session config map.** See
    // [`OxidantService::configured_workers`]: `spark.oxidant.workers` is writable by any client
    // that can reach the unauthenticated Connect port, so matching against it would make the
    // id-not-address discipline below decorative.
    let workers = state.service.configured_workers();
    match workers.iter().find(|w| worker_id(w) == requested) {
        Some(address) => Ok(address.clone()),
        None if workers.is_empty() => Err(error_response(
            StatusCode::NOT_FOUND,
            "this driver has no workers configured (set OXIDANT_WORKERS or OXIDANT_WORKER_SERVICE)",
        )),
        None => Err(error_response(
            StatusCode::NOT_FOUND,
            &format!(
                "unknown worker `{requested}`: expected `driver` or one of {}",
                workers
                    .iter()
                    .map(|w| worker_id(w))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        )),
    }
}

/// Ask one worker, over its own Flight surface, and hand back what it said.
///
/// **Nothing is written to this driver's disk.** The page is a `Value` in memory, forwarded to
/// the caller and dropped — §6b's "federation, not shipping", and the reason the diagnostic dump
/// is a separate, explicitly-named route.
async fn federate(
    endpoint: &str,
    query: &crate::logging::LogQuery,
    token: Option<&str>,
) -> Result<Value, crate::logging::LogError> {
    let body = serde_json::to_vec(query).map_err(|e| crate::logging::LogError {
        status: 500,
        message: format!("could not encode the log query: {e}"),
    })?;
    // The worker's `logs` action wants the same bearer this route already required of *its*
    // caller (F16). Every path here is behind `deny_unless_authorized`, so a `None` at this
    // point is not "an unauthenticated read got through" — it is unreachable — but it is sent
    // as no credential rather than as a blank one, and the worker answers accordingly.
    let call = oxidant_execution::flight::worker_logs(endpoint.to_string(), body, token);
    match tokio::time::timeout(WORKER_QUERY_TIMEOUT, call).await {
        Ok(Ok(bytes)) => crate::logging::decode_worker_answer(&bytes),
        // **Honest, and named.** A worker that cannot be reached is reported as *that*, with the
        // transport's own message — never an empty page, which reads as "this worker logged
        // nothing" and is the one answer a log browser must never invent.
        Ok(Err(e)) => Err(crate::logging::LogError {
            status: 502,
            message: format!("worker {endpoint} did not answer: {e}"),
        }),
        Err(_) => Err(crate::logging::LogError {
            status: 504,
            message: format!(
                "worker {endpoint} did not answer within {}s",
                WORKER_QUERY_TIMEOUT.as_secs()
            ),
        }),
    }
}

/// A federated page's deadline. Longer than the liveness probe — a worker really is decoding a
/// Parquet — and far short of the browser's own patience.
const WORKER_QUERY_TIMEOUT: Duration = Duration::from_secs(30);

/// `POST /api/v1/logs/dump` — the request body (§6b).
#[derive(Debug, Default, Deserialize)]
struct DumpRequest {
    /// `driver`, `all`, or one worker id. Defaults to `all`: an operator opening a support case
    /// wants the cluster, and asking for one node is the narrower, deliberate act.
    worker: Option<String>,
    /// RFC-3339 window. Both absent defaults to the **last hour** — see [`DEFAULT_DUMP_WINDOW`].
    from: Option<String>,
    to: Option<String>,
    /// The same filters the browser takes, so "dump what I am looking at" is one request rather
    /// than a second query language.
    level: Option<String>,
    target: Option<String>,
    q: Option<String>,
}

/// How far back a dump reaches when the caller names no window.
///
/// **A default matters here in a way it does not for a page read.** With no bound, the obvious
/// `POST /api/v1/logs/dump` with an empty body means "every node, thirty days", which the 1 GiB
/// cap would then refuse after minutes of Flight round-trips — a refusal that is correct and
/// useless. An hour is the window an operator reaching for a support bundle almost always wants,
/// and the effective window is echoed in the `202` so nobody has to guess which they got.
const DEFAULT_DUMP_WINDOW: Duration = Duration::from_secs(3600);

/// `POST /api/v1/logs/dump` — assemble a bounded support bundle into `dumps/` (§6b).
///
/// **This is the one place log bytes move**, and it is deliberately its own route rather than a
/// mode of the browser: an operator who copies a cluster's logs onto the driver's disk should
/// have had to say so. Token-guarded like every other log route, bounded by
/// `OXIDANT_LOG_DUMP_MAX_BYTES` *and* §3's budget and free-space floor, refused with `507`
/// rather than truncated, and swept 24 h later by the dump pass that shipped in PR2.
///
/// Answers `202 {dumpId}` and assembles on a task: six nodes and a day is minutes of Flight
/// round-trips, and an HTTP client that gave up halfway would otherwise leave a half-written
/// file with nobody to finish or remove it.
async fn create_dump(
    State(state): State<RestState>,
    headers: header::HeaderMap,
    body: Option<Json<DumpRequest>>,
) -> Response {
    if let Some(denied) =
        oxidant_ui_server::status::deny_unless_authorized(state.status_token.as_deref(), &headers)
    {
        return denied;
    }
    let request = body.map(|Json(b)| b).unwrap_or_default();
    let Some(dumps) = state.dumps.clone() else {
        return error_response(
            StatusCode::NOT_FOUND,
            "this node writes no dumps (OXIDANT_HISTORY=off)",
        );
    };
    // The window, resolved *before* anything is minted, so a bad instant is a `400` on the
    // request rather than a failed dump an operator collects later.
    let now = chrono::Utc::now();
    // Both instants are judged before either is defaulted, so a malformed value is reported as
    // malformed rather than as whatever the pairing rule below would have said about it.
    for (name, raw) in [("from", &request.from), ("to", &request.to)] {
        if let Some(raw) = raw {
            if chrono::DateTime::parse_from_rfc3339(raw).is_err() {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    &format!("invalid {name} `{raw}`: expected an RFC-3339 instant"),
                );
            }
        }
    }
    let stamp =
        |t: chrono::DateTime<chrono::Utc>| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let (from, to) = match (&request.from, &request.to) {
        (None, None) => (
            stamp(now - chrono::Duration::from_std(DEFAULT_DUMP_WINDOW).unwrap_or_default()),
            stamp(now),
        ),
        // **A one-sided upper bound is a refusal, not a default.** `to` alone fell through to
        // `from = 1970-01-01`, so `{"to": "…"}` walked the *entire* retention on every node —
        // precisely the "every node, thirty days" request `DEFAULT_DUMP_WINDOW` exists to keep
        // an empty body from meaning, arrived at by supplying one field instead of none. It is
        // also the expensive direction: a dump reads what its window names (F2), so the cost of
        // the mistake is the whole retention decoded on every node before the 1 GiB cap refuses
        // it — "a refusal that is correct and useless", which is the reasoning the default was
        // given in the first place.
        //
        // `from` alone is a different shape and stays: its open end is `now`, so the window is
        // bounded by the instant the caller did supply.
        (None, Some(to)) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                &format!(
                    "`to` was given as `{to}` with no `from`, which would bundle everything \
                     recorded before it — up to OXIDANT_LOG_KEEP_DAYS on every node. Supply \
                     `from` as well, or omit both for the last hour"
                ),
            );
        }
        (Some(from), to) => (from.clone(), to.clone().unwrap_or_else(|| stamp(now))),
    };
    // The nodes, resolved here so `?worker=` gets the same SSRF gate the browser's does.
    let nodes = match dump_nodes(&state, request.worker.as_deref()) {
        Ok(nodes) => nodes,
        Err(response) => return response,
    };
    // §3's guards, before the id exists: a refusal must land on the request. Minting the id is
    // the same step, under the same lock, because the id *is* the reservation — see
    // [`DumpStore::admit_and_begin`].
    let id = match dumps.admit_and_begin() {
        Ok(id) => id,
        Err(e) => return log_error_response(&e),
    };
    let query = crate::logging::LogQuery {
        level: request.level.clone(),
        target: request.target.clone(),
        q: request.q.clone(),
        from: Some(from.clone()),
        to: Some(to.clone()),
        ..Default::default()
    };
    tokio::spawn(assemble_dump(
        state.clone(),
        dumps.clone(),
        id.clone(),
        nodes.clone(),
        query,
    ));
    (
        StatusCode::ACCEPTED,
        Json(json!({
            "dumpId": id,
            "status": "building",
            "from": from,
            "to": to,
            "nodes": nodes.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>(),
            "maxBytes": dumps.max_bytes(),
        })),
    )
        .into_response()
}

/// The nodes one dump covers: `(id, endpoint)`, with `None` for the driver's own files.
fn dump_nodes(
    state: &RestState,
    requested: Option<&str>,
) -> Result<Vec<(String, Option<String>)>, Response> {
    // `all` means this driver's configured cluster. A session-config override would make a
    // bundle silently cover a node nobody deployed and silently omit the ones they did.
    let workers = state.service.configured_workers();
    match requested.unwrap_or("all") {
        "driver" => Ok(vec![("driver".to_string(), None)]),
        "all" => {
            let mut nodes = vec![("driver".to_string(), None)];
            nodes.extend(workers.iter().map(|w| (worker_id(w), Some(w.clone()))));
            Ok(nodes)
        }
        // One named worker goes through the same gate as `?worker=`: an id from this driver's
        // own configuration, never an address.
        other => {
            resolve_worker(state, other).map(|endpoint| vec![(other.to_string(), Some(endpoint))])
        }
    }
}

/// Walk every node's files inside the window and write them into one Parquet.
///
/// **A node that could not be reached is recorded and the dump still completes.** A support
/// bundle that silently omits the node that died is worse than no bundle at all: the missing
/// node is the one the case is about.
async fn assemble_dump(
    state: RestState,
    dumps: Arc<crate::logging::DumpStore>,
    id: String,
    nodes: Vec<(String, Option<String>)>,
    query: crate::logging::LogQuery,
) {
    let mut writer = match dumps.open(&id) {
        Ok(w) => w,
        Err(e) => return dumps.set(&id, crate::logging::DumpState::Failed(e)),
    };
    let instant = |raw: &Option<String>| -> Option<i64> {
        raw.as_deref()
            .and_then(|v| chrono::DateTime::parse_from_rfc3339(v).ok())
            .map(|t| t.timestamp_millis())
    };
    let (from_ms, to_ms) = (instant(&query.from), instant(&query.to));
    for (node, endpoint) in &nodes {
        let files = {
            let mut listing = query.clone();
            listing.op = Some("files".to_string());
            match ask_node(&state, endpoint.as_deref(), &listing).await {
                Ok(v) => v,
                Err(e) => {
                    writer.note_node(node, Some(e.message));
                    continue;
                }
            }
        };
        // **The window prunes what is read, not only what is kept.** `FileInfo` already carries
        // `first_ts`/`last_ts`, so a file the window puts wholly outside is never opened — the
        // documented default is the last hour, and without this a one-hour bundle walked up to
        // `OXIDANT_LOG_KEEP_DAYS` of every node's history, `DUMP_PAGE` rows at a time, to keep
        // an hour of it.
        let names: Vec<String> = files["files"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter(|f| file_in_window(f, from_ms, to_ms))
                    .filter_map(|f| f["file"].as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let mut failure: Option<String> = None;
        'files: for name in names {
            // The **forward** cursor, so each file is walked once end to end and the walk
            // resumes exactly where it stopped however selective the filter is.
            let mut after = 0u64;
            loop {
                let mut page = query.clone();
                page.file = Some(name.clone());
                page.after = Some(after);
                page.limit = Some(DUMP_PAGE);
                let value = match ask_node(&state, endpoint.as_deref(), &page).await {
                    Ok(v) => v,
                    Err(e) => {
                        // A file that vanished under the walk (retention took it mid-dump) is
                        // not a failed dump; an unreachable node is.
                        if e.status == 404 {
                            continue 'files;
                        }
                        failure = Some(e.message);
                        break 'files;
                    }
                };
                for line in value["logs"].as_array().into_iter().flatten() {
                    let Some(line) = line.as_str() else { continue };
                    if let Err(e) = writer.push(node, line) {
                        // The cap. Refused, not truncated: the id reports it and no bundle is
                        // published.
                        return dumps.set(&id, crate::logging::DumpState::Failed(e));
                    }
                }
                // **An empty page ends the file.** A forward page stops only at `DUMP_PAGE`
                // matches or at the end of the file, so no lines means the scan reached the end
                // with nothing left to match. Without this the walk of a *live* file chases its
                // own tail: `next_after` is the growing EOF, so `next <= after` never holds on
                // a node logging faster than the dump reads — the busiest node in the cluster
                // being the one whose dump never completes.
                if value["logs"].as_array().map_or(true, |a| a.is_empty()) {
                    break;
                }
                let next = value["next_after"].as_u64().unwrap_or(after);
                if next <= after {
                    break;
                }
                after = next;
            }
        }
        writer.note_node(node, failure);
    }
    match writer.finish() {
        Ok(finished) => {
            // **The audited half of "an explicit, token-guarded, audited action"** (§6b). This
            // is the one path that copies log bytes between nodes, so its completion is a log
            // line of its own — which node, how many rows, how large, and which nodes did not
            // answer — and it lands in the very log it just copied.
            if let crate::logging::DumpState::Ready {
                bytes, rows, nodes, ..
            } = &finished
            {
                let unreachable: Vec<&str> = nodes
                    .iter()
                    .filter(|(_, err)| err.is_some())
                    .map(|(node, _)| node.as_str())
                    .collect();
                tracing::info!(
                    dump = %id,
                    rows,
                    bytes,
                    nodes = nodes.len(),
                    unreachable = unreachable.join(","),
                    "diagnostic dump assembled"
                );
            }
            dumps.set(&id, finished)
        }
        Err(e) => {
            tracing::warn!(dump = %id, error = %e.message, "diagnostic dump failed");
            dumps.set(&id, crate::logging::DumpState::Failed(e))
        }
    }
}

/// Could this listing entry hold a line inside `[from, to)`? Decided from the bounds the
/// listing already carries, so a file outside the window is never opened.
///
/// **This is coarser than the row filter, deliberately, and it is the one place the dump is.**
/// `first_ts`/`last_ts` are the first and last *parseable* timestamps, so a rolled file every
/// one of whose timestamps sits outside the window is skipped — including any unjudgeable line
/// it holds, which the row filter would have served under rule 1. Those lines are the
/// continuations of lines that are themselves outside the window, so skipping them keeps the
/// bundle's window honest; a `?file=` read of the same file still serves them, because there
/// the caller named the file.
///
/// Two asymmetries, each because of what a file can still become:
/// - a file the writer holds open (`rolled: false`) only ever grows **newer**, so its recorded
///   `last_ts` cannot rule it out of a window that is still open;
/// - a file with no parseable bound at either end is unjudgeable and is read.
fn file_in_window(file: &Value, from_ms: Option<i64>, to_ms: Option<i64>) -> bool {
    let ts = |key: &str| {
        file[key]
            .as_str()
            .and_then(|v| chrono::DateTime::parse_from_rfc3339(v).ok())
            .map(|t| t.timestamp_millis())
    };
    // `rolled` absent is read as "rolled": a node too old to say is not a reason to re-read its
    // whole history.
    if file["rolled"].as_bool().unwrap_or(true) {
        if let (Some(last), Some(from)) = (ts("last_ts"), from_ms) {
            if last < from {
                return false;
            }
        }
    }
    // `to` is exclusive, so a file whose oldest line is at or after it holds nothing.
    if let (Some(first), Some(to)) = (ts("first_ts"), to_ms) {
        if first >= to {
            return false;
        }
    }
    true
}

/// Lines per page while assembling. Larger than a browser page — nobody is waiting on one
/// round-trip — and still bounded.
const DUMP_PAGE: usize = 5_000;

/// Ask one node — this driver, or a worker over Flight.
async fn ask_node(
    state: &RestState,
    endpoint: Option<&str>,
    query: &crate::logging::LogQuery,
) -> Result<Value, crate::logging::LogError> {
    match endpoint {
        Some(endpoint) => federate(endpoint, query, state.status_token.as_deref()).await,
        None => {
            let view = state.logs.clone();
            let ring = state.log_buffer.clone();
            let query = query.clone();
            tokio::task::spawn_blocking(move || crate::logging::answer(&query, &view, &ring))
                .await
                .unwrap_or_else(|e| {
                    Err(crate::logging::LogError {
                        status: 500,
                        message: format!("reading the log file panicked: {e}"),
                    })
                })
        }
    }
}

/// `GET /api/v1/logs/dump/{dump_id}` — collect a bundle (§6b).
///
/// Four answers, and each is a distinct fact: `202` it is still assembling, `200` here it is,
/// the assembly's own status (`507` past a cap) with its reason, or `404` no such dump. A
/// half-assembled bundle is never served as a whole one.
async fn get_dump(
    State(state): State<RestState>,
    headers: header::HeaderMap,
    Path(dump_id): Path<String>,
) -> Response {
    if let Some(denied) =
        oxidant_ui_server::status::deny_unless_authorized(state.status_token.as_deref(), &headers)
    {
        return denied;
    }
    let Some(dumps) = state.dumps.clone() else {
        return error_response(
            StatusCode::NOT_FOUND,
            "this node writes no dumps (OXIDANT_HISTORY=off)",
        );
    };
    match dumps.get(&dump_id) {
        None => error_response(StatusCode::NOT_FOUND, "unknown dump id"),
        Some(crate::logging::DumpState::Building) => (
            StatusCode::ACCEPTED,
            Json(json!({ "dumpId": dump_id, "status": "building" })),
        )
            .into_response(),
        Some(crate::logging::DumpState::Failed(e)) => log_error_response(&e),
        Some(crate::logging::DumpState::Ready { path, bytes, .. }) => {
            // **Streamed, never buffered.** A bundle may be a gigabyte, and the driver's whole
            // result budget is 512 MiB; reading it into a `Vec<u8>` to hand to axum would be the
            // same unbounded read `?file=`'s page cap exists to avoid, one route along.
            let body = axum::body::Body::from_stream(dump_chunks(path));
            (
                StatusCode::OK,
                [
                    (
                        header::CONTENT_TYPE,
                        "application/vnd.apache.parquet".to_string(),
                    ),
                    (header::CONTENT_LENGTH, bytes.to_string()),
                    (
                        header::CONTENT_DISPOSITION,
                        format!("attachment; filename=\"{dump_id}.parquet\""),
                    ),
                ],
                body,
            )
                .into_response()
        }
    }
}

/// Bytes of a bundle, in bounded chunks off the reactor.
const DUMP_CHUNK: usize = 64 * 1024;

/// Where a download is: not opened yet, or holding the one handle it will use throughout.
enum DumpRead {
    /// The first poll opens the file — off the reactor, like every other read here.
    Unopened(std::path::PathBuf),
    /// **One handle for the whole download.** It used to `open` + `seek` per chunk, which is
    /// 16,384 round-trips for a 1 GiB bundle and, worse, is not atomic: the sweeper's 24 h TTL
    /// can unlink the bundle at any moment, and it does not have to wait for a lull. Between two
    /// chunks the next `open` would then fail — after `Content-Length` had already promised the
    /// whole file — and the reader would get a truncated download reported as a stream error.
    /// Holding the descriptor makes the download atomic with respect to retention on POSIX: the
    /// bytes stay readable until the last one is served, whatever the sweeper does to the name.
    Open(std::fs::File),
}

fn dump_chunks(
    path: std::path::PathBuf,
) -> impl futures::Stream<Item = Result<Vec<u8>, std::io::Error>> {
    futures::stream::unfold(Some(DumpRead::Unopened(path)), |st| async move {
        let state = st?;
        let read = tokio::task::spawn_blocking(move || {
            use std::io::Read;
            // Sequential reads advance the handle's own offset, so there is no seek to get
            // wrong either.
            let mut file = match state {
                DumpRead::Unopened(path) => std::fs::File::open(&path)?,
                DumpRead::Open(file) => file,
            };
            let mut buf = vec![0u8; DUMP_CHUNK];
            let mut filled = 0usize;
            while filled < buf.len() {
                match file.read(&mut buf[filled..])? {
                    0 => break,
                    n => filled += n,
                }
            }
            buf.truncate(filled);
            Ok::<_, std::io::Error>((file, buf))
        })
        .await;
        match read {
            Ok(Ok((_, buf))) if buf.is_empty() => None,
            Ok(Ok((file, buf))) => Some((Ok(buf), Some(DumpRead::Open(file)))),
            Ok(Err(e)) => Some((Err(e), None)),
            Err(e) => Some((
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("dump read panicked: {e}"),
                )),
                None,
            )),
        }
    })
}

#[cfg(test)]
#[allow(clippy::await_holding_lock)] // env_lock() serializes process-global env across async tests
mod tests {
    use std::sync::MutexGuard;

    use super::*;
    use crate::logging::MAX_LOG_LINES;
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
            logs: LogView::default(),
            status_token: None,
            dumps: None,
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
            logs: LogView::default(),
            status_token: None,
            dumps: None,
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

    /// Repo-root-relative path to the committed sample tables (same fixture tree
    /// `tests/local_catalog.rs` reads through SQL).
    fn sample_data_dir() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../sample-data")
            .canonicalize()
            .expect("sample-data tree is committed at the repo root")
    }

    /// A `local` catalog config over one table per format `table_stats` has to guard: Parquet
    /// (no snapshot metadata), Delta, Iceberg — plus `nation_broken`, an Iceberg table whose
    /// `metadata.json` is not parseable, written into `warehouse` so the "could not read" path
    /// has a table to fail on.
    fn sample_catalog_conf(
        warehouse: &std::path::Path,
    ) -> std::collections::HashMap<String, String> {
        let root = sample_data_dir();
        let broken = warehouse.join("nation_broken");
        std::fs::create_dir_all(broken.join("metadata")).expect("broken fixture dir");
        std::fs::write(
            broken.join("metadata/00001-broken.metadata.json"),
            b"{ this is not iceberg metadata",
        )
        .expect("broken fixture metadata");
        let tables = serde_json::json!({
            "samples.nation_parquet": {
                "format": "parquet",
                "location": root.join("parquet/tpch_nation.parquet").to_string_lossy(),
            },
            "samples.nation_delta": {
                "format": "delta",
                "location": root.join("delta/tpch_nation").to_string_lossy(),
            },
            "samples.nation_iceberg": {
                "format": "iceberg",
                "location": root.join("iceberg/tpch_nation").to_string_lossy(),
            },
            "samples.nation_broken": {
                "format": "iceberg",
                "location": broken.to_string_lossy(),
            },
        });
        std::collections::HashMap::from([
            (
                "spark.sql.catalog.local.type".to_string(),
                "local".to_string(),
            ),
            (
                "spark.sql.catalog.local.warehouse".to_string(),
                warehouse.to_string_lossy().to_string(),
            ),
            (
                "spark.sql.catalog.local.tables".to_string(),
                tables.to_string(),
            ),
        ])
    }

    /// [`test_state`] built around a declared catalog rather than a bare engine, for tests that
    /// need a registered `CatalogProvider` (e.g. `table_stats`).
    async fn test_state_with_catalog(
        conf: std::collections::HashMap<String, String>,
    ) -> (RestState, Router) {
        let state = RestState {
            service: Arc::new(OxidantService::with_catalogs(conf).await),
            store: StatementStore::new(),
            log_buffer: LogBuffer::new(MAX_LOG_LINES),
            logs: LogView::default(),
            status_token: None,
            dumps: None,
        };
        (state.clone(), app(state))
    }

    #[tokio::test]
    async fn table_stats_reads_the_iceberg_current_snapshot_summary() {
        // The exact `total-records` extraction is guarded at the `oxidant-datasource` unit
        // level (`iceberg_snapshot_stats_reads_total_records_and_timestamp_from_the_summary`)
        // against a fixture whose summary carries one; this fixture (shared with
        // `tests/local_catalog.rs`) doesn't record `total-records`, so `row_count` is
        // correctly null here — this test guards the REST wiring end-to-end instead.
        let warehouse = tempfile::tempdir().expect("tempdir");
        let (_state, app) = test_state_with_catalog(sample_catalog_conf(warehouse.path())).await;
        let (status, body) = get_json(
            &app,
            "/api/v1/catalogs/local/namespaces/samples/tables/nation_iceberg/stats",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["format"], "iceberg", "{body}");
        assert_eq!(body["stats_source"], "snapshot_metadata", "{body}");
        assert!(
            body["data_updated_at"].as_i64().unwrap_or(0) > 0,
            "expected timestamp-ms from the current snapshot: {body}"
        );
    }

    #[tokio::test]
    async fn table_stats_reads_delta_commit_timestamp_but_not_a_scanned_row_count() {
        let warehouse = tempfile::tempdir().expect("tempdir");
        let (_state, app) = test_state_with_catalog(sample_catalog_conf(warehouse.path())).await;
        let (status, body) = get_json(
            &app,
            "/api/v1/catalogs/local/namespaces/samples/tables/nation_delta/stats",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["format"], "delta", "{body}");
        assert_eq!(body["stats_source"], "snapshot_metadata", "{body}");
        assert!(
            body["data_updated_at"].as_i64().unwrap_or(0) > 0,
            "expected the latest commit's timestamp: {body}"
        );
        // No cheap row count in the delta-kernel log metadata this reads — never a data scan
        // to compute one.
        assert!(body["row_count"].is_null(), "{body}");
    }

    #[tokio::test]
    async fn table_stats_reports_unavailable_for_a_format_with_no_snapshot_metadata() {
        let warehouse = tempfile::tempdir().expect("tempdir");
        let (_state, app) = test_state_with_catalog(sample_catalog_conf(warehouse.path())).await;
        let (status, body) = get_json(
            &app,
            "/api/v1/catalogs/local/namespaces/samples/tables/nation_parquet/stats",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        // The format is known — it just carries no snapshot metadata. Reporting it as
        // `unknown` would tell a harvester the engine cannot identify the table at all.
        assert_eq!(body["format"], "parquet", "{body}");
        assert_eq!(body["stats_source"], "unavailable", "{body}");
        assert!(body["row_count"].is_null(), "{body}");
        assert!(body["data_updated_at"].is_null(), "{body}");
    }

    /// A table the engine cannot read is not a table with no stats: a harvester that polls
    /// `200 {"stats_source": "unavailable"}` files it away as "this format has nothing" and
    /// never looks again, so a corrupt `metadata.json` has to surface as a failure.
    #[tokio::test]
    async fn table_stats_500s_when_the_iceberg_metadata_cannot_be_read() {
        let warehouse = tempfile::tempdir().expect("tempdir");
        let (_state, app) = test_state_with_catalog(sample_catalog_conf(warehouse.path())).await;
        let (status, body) = get_json(
            &app,
            "/api/v1/catalogs/local/namespaces/samples/tables/nation_broken/stats",
        )
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
        assert!(
            body["error"]
                .as_str()
                .unwrap_or_default()
                .contains("iceberg"),
            "{body}"
        );
        // The object-store location the read failed on stays in the log, not the body.
        assert!(
            !body.to_string().contains("nation_broken/metadata"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn table_stats_404s_on_a_missing_table_in_a_registered_catalog() {
        let warehouse = tempfile::tempdir().expect("tempdir");
        let (_state, app) = test_state_with_catalog(sample_catalog_conf(warehouse.path())).await;
        let (status, body) = get_json(
            &app,
            "/api/v1/catalogs/local/namespaces/samples/tables/does_not_exist/stats",
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    }

    #[tokio::test]
    async fn table_stats_404s_on_an_unregistered_catalog() {
        let (_guard, _state, app) = test_state();
        let (status, body) = get_json(
            &app,
            "/api/v1/catalogs/nope/namespaces/samples/tables/nation_iceberg/stats",
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    }

    #[tokio::test]
    async fn table_stats_404s_on_a_missing_builtin_table() {
        let (_guard, _state, app) = test_state();
        let (status, body) = get_json(
            &app,
            "/api/v1/catalogs/spark_catalog/namespaces/default/tables/does_not_exist/stats",
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    }

    // ---- set_table_comment -----------------------------------------------------------------

    /// [`sample_catalog_conf`] declares every table from config, and `LocalCatalog::alter_table`
    /// refuses to alter a declared table (config is the operator's statement of intent) — so
    /// these tests need a *managed* table, created straight through the provider the way
    /// `LakeSink` would, over a bare local catalog with nothing declared.
    fn empty_local_catalog_conf(
        warehouse: &std::path::Path,
    ) -> std::collections::HashMap<String, String> {
        std::collections::HashMap::from([
            (
                "spark.sql.catalog.local.type".to_string(),
                "local".to_string(),
            ),
            (
                "spark.sql.catalog.local.warehouse".to_string(),
                warehouse.to_string_lossy().to_string(),
            ),
        ])
    }

    fn comment_test_schema() -> oxidant_catalog::arrow::datatypes::SchemaRef {
        use oxidant_catalog::arrow::datatypes::{DataType, Field, Schema};
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]))
    }

    async fn put_json(app: &Router, uri: &str, body: Value) -> (StatusCode, Value) {
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("PUT")
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

    #[tokio::test]
    async fn set_table_comment_sets_reads_back_and_clears() {
        let warehouse = tempfile::tempdir().expect("tempdir");
        let (state, app) =
            test_state_with_catalog(empty_local_catalog_conf(warehouse.path())).await;
        let provider = state
            .service
            .registry()
            .provider("local")
            .expect("local catalog registered");
        provider
            .create_table(
                &["live".to_string()],
                "orders",
                comment_test_schema(),
                oxidant_catalog::TableFormat::Delta,
                None,
                &[],
            )
            .await
            .expect("create managed table");

        let uri = "/api/v1/catalogs/local/namespaces/live/tables/orders/comment";
        let (status, body) = put_json(&app, uri, json!({ "comment": "hot path" })).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["comment"], "hot path", "{body}");
        let loaded = provider
            .load_table(&["live".to_string()], "orders")
            .await
            .expect("load");
        assert_eq!(loaded.comment.as_deref(), Some("hot path"));

        // An empty string clears the comment, same as an explicit `null`.
        let (status, body) = put_json(&app, uri, json!({ "comment": "" })).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(body["comment"].is_null(), "{body}");
        let loaded = provider
            .load_table(&["live".to_string()], "orders")
            .await
            .expect("load");
        assert_eq!(loaded.comment, None);

        put_json(&app, uri, json!({ "comment": "set again" })).await;
        let (status, body) = put_json(&app, uri, json!({ "comment": null })).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(body["comment"].is_null(), "{body}");
        let loaded = provider
            .load_table(&["live".to_string()], "orders")
            .await
            .expect("load");
        assert_eq!(loaded.comment, None);
    }

    #[tokio::test]
    async fn set_table_comment_404s_on_a_missing_table() {
        let warehouse = tempfile::tempdir().expect("tempdir");
        let (_state, app) = test_state_with_catalog(sample_catalog_conf(warehouse.path())).await;
        let (status, body) = put_json(
            &app,
            "/api/v1/catalogs/local/namespaces/samples/tables/does_not_exist/comment",
            json!({ "comment": "x" }),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    }

    #[tokio::test]
    async fn set_table_comment_404s_on_an_unregistered_catalog() {
        let (_guard, _state, app) = test_state();
        let (status, body) = put_json(
            &app,
            "/api/v1/catalogs/nope/namespaces/samples/tables/nation_iceberg/comment",
            json!({ "comment": "x" }),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    }

    #[tokio::test]
    async fn set_table_comment_404s_on_the_builtin_catalog() {
        // `spark_catalog` has no `CatalogProvider` to alter — wrong coordinates, same as the
        // unregistered-catalog case above, never a silent no-op.
        let (_guard, _state, app) = test_state();
        let (status, body) = put_json(
            &app,
            "/api/v1/catalogs/spark_catalog/namespaces/default/tables/does_not_exist/comment",
            json!({ "comment": "x" }),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    }

    /// A catalog provider that unconditionally refuses `alter_table` the way Glue does when the
    /// caller's IAM principal lacks `glue:UpdateTable` — `Error::Io` carrying
    /// `AccessDeniedException` in the message (see `classify_glue_failure` in
    /// `oxidant-catalog-glue`).
    struct DenyingCatalog;

    #[async_trait::async_trait]
    impl oxidant_catalog::CatalogProvider for DenyingCatalog {
        fn name(&self) -> &str {
            "denying"
        }

        async fn list_namespaces(
            &self,
            _parent: &[String],
        ) -> oxidant_catalog::Result<Vec<Vec<String>>> {
            Ok(vec![])
        }

        async fn list_tables(&self, _namespace: &[String]) -> oxidant_catalog::Result<Vec<String>> {
            Ok(vec![])
        }

        async fn load_table(
            &self,
            _namespace: &[String],
            _table: &str,
        ) -> oxidant_catalog::Result<oxidant_catalog::TableMetadata> {
            Err(oxidant_catalog::Error::Plan("not used by this test".into()))
        }

        async fn alter_table(
            &self,
            _namespace: &[String],
            _table: &str,
            _changes: Vec<oxidant_catalog::TableChange>,
        ) -> oxidant_catalog::Result<oxidant_catalog::TableMetadata> {
            Err(oxidant_catalog::Error::Io(
                "aws glue UpdateTable: AccessDeniedException: User: arn:aws:iam::123:user/kai is \
                 not authorized to perform: glue:UpdateTable"
                    .to_string(),
            ))
        }
    }

    #[tokio::test]
    async fn set_table_comment_403s_when_the_provider_denies_access() {
        let (_guard, state, app) = test_state();
        state
            .service
            .registry()
            .register("denying", Arc::new(DenyingCatalog));
        let (status, body) = put_json(
            &app,
            "/api/v1/catalogs/denying/namespaces/ns/tables/t/comment",
            json!({ "comment": "x" }),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert!(
            body["error"]
                .as_str()
                .unwrap_or_default()
                .contains("AccessDeniedException"),
            "{body}"
        );
    }

    /// A catalog that simply does not implement `alter_table` — the SPI default, which is what
    /// `HiveCatalog` and `RestCatalog` currently use.
    struct InertCatalog;

    #[async_trait::async_trait]
    impl oxidant_catalog::CatalogProvider for InertCatalog {
        fn name(&self) -> &str {
            "inert"
        }

        async fn list_namespaces(
            &self,
            _parent: &[String],
        ) -> oxidant_catalog::Result<Vec<Vec<String>>> {
            Ok(vec![])
        }

        async fn list_tables(&self, _namespace: &[String]) -> oxidant_catalog::Result<Vec<String>> {
            Ok(vec![])
        }

        async fn load_table(
            &self,
            _namespace: &[String],
            _table: &str,
        ) -> oxidant_catalog::Result<oxidant_catalog::TableMetadata> {
            Err(oxidant_catalog::Error::Plan("not used by this test".into()))
        }
    }

    #[tokio::test]
    async fn set_table_comment_501s_on_a_catalog_that_cannot_alter_tables() {
        // `Error::Unsupported` is permanent, not a backend hiccup: a `500` would tell the Platform
        // to retry a thing that can never work, and hide that the affordance should be off.
        let (_guard, state, app) = test_state();
        state
            .service
            .registry()
            .register("inert", Arc::new(InertCatalog));
        let (status, body) = put_json(
            &app,
            "/api/v1/catalogs/inert/namespaces/ns/tables/t/comment",
            json!({ "comment": "x" }),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{body}");
        assert!(
            body["error"]
                .as_str()
                .unwrap_or_default()
                .contains("does not support altering tables"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn set_table_comment_409s_on_a_config_declared_table() {
        // A declared table is listed, queryable and has stats — calling it an "unknown table"
        // would send the caller hunting for a typo that isn't there. It exists; it is just not
        // ours to edit, and the provider's own sentence says where to edit it instead.
        let warehouse = tempfile::tempdir().expect("tempdir");
        let (state, app) = test_state_with_catalog(sample_catalog_conf(warehouse.path())).await;
        let (status, body) = put_json(
            &app,
            "/api/v1/catalogs/local/namespaces/samples/tables/nation_parquet/comment",
            json!({ "comment": "x" }),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert!(
            body["error"]
                .as_str()
                .unwrap_or_default()
                .contains("declared in configuration"),
            "{body}"
        );
        let loaded = state
            .service
            .registry()
            .provider("local")
            .expect("local catalog registered")
            .load_table(&["samples".to_string()], "nation_parquet")
            .await
            .expect("load");
        assert_eq!(
            loaded.comment, None,
            "the refused write must not have landed"
        );
    }

    #[tokio::test]
    async fn set_table_comment_treats_a_whitespace_only_comment_as_clearing() {
        let warehouse = tempfile::tempdir().expect("tempdir");
        let (state, app) =
            test_state_with_catalog(empty_local_catalog_conf(warehouse.path())).await;
        let provider = state
            .service
            .registry()
            .provider("local")
            .expect("local catalog registered");
        provider
            .create_table(
                &["live".to_string()],
                "orders",
                comment_test_schema(),
                oxidant_catalog::TableFormat::Delta,
                None,
                &[],
            )
            .await
            .expect("create managed table");

        let uri = "/api/v1/catalogs/local/namespaces/live/tables/orders/comment";
        put_json(&app, uri, json!({ "comment": "hot path" })).await;
        // `"   "` is not a comment that happens to be blank, it is no comment — same as `""`.
        let (status, body) = put_json(&app, uri, json!({ "comment": " \t\n " })).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(body["comment"].is_null(), "{body}");
        let loaded = provider
            .load_table(&["live".to_string()], "orders")
            .await
            .expect("load");
        assert_eq!(loaded.comment, None);

        // Interior and edge whitespace around real text is the caller's content, kept verbatim.
        let (status, body) = put_json(&app, uri, json!({ "comment": "  hot path  " })).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["comment"], "  hot path  ", "{body}");
    }

    #[tokio::test]
    async fn set_table_comment_rejects_an_oversized_comment_without_touching_the_table() {
        let warehouse = tempfile::tempdir().expect("tempdir");
        let (state, app) =
            test_state_with_catalog(empty_local_catalog_conf(warehouse.path())).await;
        let provider = state
            .service
            .registry()
            .provider("local")
            .expect("local catalog registered");
        provider
            .create_table(
                &["live".to_string()],
                "orders",
                comment_test_schema(),
                oxidant_catalog::TableFormat::Delta,
                None,
                &[],
            )
            .await
            .expect("create managed table");

        let uri = "/api/v1/catalogs/local/namespaces/live/tables/orders/comment";
        put_json(&app, uri, json!({ "comment": "keep me" })).await;

        // One over Glue's `Description` cap: a `400` naming the limit, not the generic `500` a
        // provider-side `ValidationException` would turn into.
        let too_long = "x".repeat(MAX_COMMENT_CHARS + 1);
        let (status, body) = put_json(&app, uri, json!({ "comment": too_long })).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(
            body["error"].as_str().unwrap_or_default().contains("2048"),
            "{body}"
        );
        let loaded = provider
            .load_table(&["live".to_string()], "orders")
            .await
            .expect("load");
        assert_eq!(
            loaded.comment.as_deref(),
            Some("keep me"),
            "a rejected comment must leave the stored one alone"
        );

        // The cap counts characters, not bytes, because that is what Glue's length constraint
        // counts — a multi-byte comment at the limit is accepted, not rejected at a third of it.
        let at_limit = "é".repeat(MAX_COMMENT_CHARS);
        let (status, body) = put_json(&app, uri, json!({ "comment": at_limit })).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body["comment"].as_str().map(|c| c.chars().count()),
            Some(MAX_COMMENT_CHARS),
            "{body}"
        );
    }

    #[tokio::test]
    async fn set_table_comment_rejects_an_unrecognized_body_instead_of_clearing() {
        // The field is optional, so an unknown key would otherwise deserialize to "no comment
        // given" and silently wipe the stored one with a `200`. `{"description": ...}` is the
        // likeliest way to get this wrong — it is Glue's own name for the field.
        let warehouse = tempfile::tempdir().expect("tempdir");
        let (state, app) =
            test_state_with_catalog(empty_local_catalog_conf(warehouse.path())).await;
        let provider = state
            .service
            .registry()
            .provider("local")
            .expect("local catalog registered");
        provider
            .create_table(
                &["live".to_string()],
                "orders",
                comment_test_schema(),
                oxidant_catalog::TableFormat::Delta,
                None,
                &[],
            )
            .await
            .expect("create managed table");

        let uri = "/api/v1/catalogs/local/namespaces/live/tables/orders/comment";
        put_json(&app, uri, json!({ "comment": "keep me" })).await;
        let (status, body) = put_json(&app, uri, json!({ "description": "oops" })).await;
        assert!(status.is_client_error(), "{status} {body}");
        let loaded = provider
            .load_table(&["live".to_string()], "orders")
            .await
            .expect("load");
        assert_eq!(
            loaded.comment.as_deref(),
            Some("keep me"),
            "an unrecognized body must not clear the comment"
        );
    }

    #[test]
    fn is_access_denied_reads_the_refusals_providers_actually_send() {
        // What `classify_glue_failure` / `classify_lakeformation_failure` render.
        assert!(is_access_denied(
            "aws glue UpdateTable: AccessDeniedException: User: arn:aws:iam::123:user/kai is not \
             authorized to perform: glue:UpdateTable"
        ));
        assert!(is_access_denied(
            "aws lakeformation GetTemporaryGlueTableCredentials: AccessDeniedException: \
             Insufficient Lake Formation permission(s) on orders"
        ));
        // S3's code is a bare `AccessDenied`, and non-AWS providers use prose.
        assert!(is_access_denied("s3 PutObject: AccessDenied"));
        assert!(is_access_denied("rest catalog: 403 Access Denied"));
        assert!(is_access_denied("rest catalog: 403 access denied"));

        // A backend failure is not a permission refusal — these must stay `500`s.
        assert!(!is_access_denied(
            "aws glue UpdateTable: ThrottlingException: Rate exceeded"
        ));
        assert!(!is_access_denied(
            "aws glue UpdateTable: ValidationException: Description too long"
        ));
        assert!(!is_access_denied(
            "connection closed before message completed"
        ));
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

    /// The token every `?file=` test carries: the endpoint 404s outright without one, so a
    /// test that forgot it would be asserting the gate rather than the grammar.
    const LOGS_TOKEN: &str = "s3cret-status-token";

    /// A router whose `?file=` reads a tempdir of rolled files, gated exactly like the real one.
    fn logs_state(dir: &std::path::Path, dedup: bool) -> (MutexGuard<'static, ()>, Router) {
        let guard = crate::distributed::env_lock();
        let state = RestState {
            service: Arc::new(OxidantService::new()),
            store: StatementStore::new(),
            log_buffer: LogBuffer::new(MAX_LOG_LINES),
            logs: LogView {
                dir: Some(dir.to_path_buf()),
                dedup,
            },
            status_token: Some(LOGS_TOKEN.into()),
            dumps: None,
        };
        (guard, app(state))
    }

    /// `get_json` with the logs bearer token attached.
    async fn get_logs(app: &Router, uri: &str) -> (StatusCode, Value) {
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(uri)
                    .header(header::AUTHORIZATION, format!("Bearer {LOGS_TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, body)
    }

    /// `?file=` serves a rolled file in whichever form it exists in — and the *server* picks
    /// the extension, so a caller never has to know whether yesterday has been converted yet.
    #[tokio::test]
    async fn the_file_parameter_serves_rolled_logs_in_either_form() {
        let dir = tempfile::tempdir().expect("tempdir");
        let text = "2026-08-23T14:00:00.500Z [INFO] oxidant_execution - message=stage done, rows=7";
        std::fs::write(
            dir.path().join("oxidant-2026-08-23.log"),
            format!("{text}\n"),
        )
        .expect("write");
        // A second period, converted, plus a size split — three shapes, one grammar.
        std::fs::write(
            dir.path().join("oxidant-2026-08-24-09.2.log"),
            "2026-08-24T09:30:00.000Z [WARN] oxidant_connect - message=pool exhausted\n",
        )
        .expect("write");
        let converted =
            crate::logging::convert_for_test(&dir.path().join("oxidant-2026-08-24-09.2.log"))
                .expect("convert");
        assert!(converted.ends_with("oxidant-2026-08-24-09.2.parquet"));
        std::fs::write(dir.path().join("oxidant.log"), "live line\n").expect("write");

        let (_env, app) = logs_state(dir.path(), true);

        let (status, body) = get_logs(&app, "/api/v1/logs?file=2026-08-23").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["file"], "2026-08-23");
        assert_eq!(body["format"], "text");
        assert_eq!(body["dedup"], true, "the file is deduped and says so (F21)");
        assert_eq!(body["logs"], json!([text]));

        let (status, body) = get_logs(&app, "/api/v1/logs?file=2026-08-24-09.2").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["file"], "2026-08-24-09.2");
        assert_eq!(body["format"], "parquet", "the server chose the extension");
        assert_eq!(
            body["logs"],
            json!(["2026-08-24T09:30:00.000Z [WARN] oxidant_connect - message=pool exhausted"]),
            "a converted day reads back as the text it was"
        );

        let (status, body) = get_logs(&app, "/api/v1/logs?file=current").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["file"], "current");
        assert_eq!(body["format"], "text");
        assert_eq!(body["logs"], json!(["live line"]));

        // And with no `?file=` at all the answer is exactly what it was before PR3: the ring.
        let (status, body) = get_logs(&app, "/api/v1/logs").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["logs"].is_array());
        assert!(
            body.get("format").is_none(),
            "the ring envelope is unchanged"
        );
        assert!(body.get("dedup").is_none());
    }

    /// The grammar is the security boundary: every traversal shape, every extension a caller
    /// must not name, and every near-miss is a `400` — never a `404`, and never a path join.
    #[tokio::test]
    async fn the_file_grammar_refuses_traversal_and_404s_what_does_not_exist() {
        let dir = tempfile::tempdir().expect("tempdir");
        let secret = dir.path().join("secret.txt");
        std::fs::write(&secret, "not a log").expect("write");
        let (_env, app) = logs_state(dir.path(), false);

        for bad in [
            "..",
            "../../etc/passwd",
            "..%2F..%2Fetc%2Fpasswd",
            "/etc/passwd",
            "2026-08-23/../secret.txt",
            "2026-08-23.log",
            "2026-08-23.parquet",
            "secret",
            "oxidant.log",
            "2026-8-23",
            "2026-13-01",
            "2026-08-23-24",
            "2027-W53",
            "2026-08-23.1",
            "2026-08-23.1000",
            "",
        ] {
            let uri = format!("/api/v1/logs?file={}", urlencode(bad));
            let (status, body) = get_logs(&app, &uri).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{bad:?} -> {body}");
            assert!(
                body["error"].as_str().unwrap().contains("invalid file"),
                "{bad:?} -> {body}"
            );
        }
        assert!(
            secret.exists(),
            "nothing here reads or removes another file"
        );

        // A well-formed period with no file on disk is the other answer: 404, not 400.
        let (status, body) = get_logs(&app, "/api/v1/logs?file=2019-01-01").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "log file not found");
        // `current` before the process has written one is the same.
        let (status, _) = get_logs(&app, "/api/v1/logs?file=current").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    /// A router with `?file=` over a tempdir **and** workers configured, for the federation
    /// tests. The workers need not be alive: half of what federation owes an operator is what it
    /// says when one is not.
    fn logs_state_with_workers(
        dir: &std::path::Path,
        workers: Vec<String>,
    ) -> (MutexGuard<'static, ()>, Router) {
        let guard = crate::distributed::env_lock();
        let mut service = OxidantService::new();
        service.workers = workers;
        let state = RestState {
            service: Arc::new(service),
            store: StatementStore::new(),
            log_buffer: LogBuffer::new(MAX_LOG_LINES),
            logs: LogView {
                dir: Some(dir.to_path_buf()),
                dedup: true,
            },
            status_token: Some(LOGS_TOKEN.into()),
            dumps: None,
        };
        (guard, app(state))
    }

    /// Six lines over three targets and four levels — enough for every filter to have both a
    /// match and a non-match, and one line the parser cannot decompose.
    const BROWSE_LINES: [&str; 6] = [
        "2026-08-23T14:00:00.000Z [INFO] oxidant_execution - message=stage 0 start",
        "2026-08-23T14:00:01.000Z [WARN] oxidant_connect - message=pool exhausted",
        "2026-08-23T14:00:02.000Z [ERROR] oxidant_execution::plan - message=stage 0 failed",
        "2026-08-23T14:00:03.000Z [INFO] oxidant_connect - message=retrying",
        "   at oxidant_execution::plan (a continuation line)",
        "2026-08-23T14:00:04.000Z [DEBUG] oxidant_execution - message=stage 0 done",
    ];

    fn write_browse_log(dir: &std::path::Path, name: &str) {
        std::fs::write(dir.join(name), format!("{}\n", BROWSE_LINES.join("\n"))).expect("write");
    }

    /// **§6b's filters, over the route.** Each one composes, each one answers the same over a
    /// rolled `.log` and its converted `.parquet`, and passing any of them switches the envelope
    /// to the newest-first cursor.
    #[tokio::test]
    async fn the_log_routes_filter_by_level_target_text_and_time() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_browse_log(dir.path(), "oxidant-2026-08-23.log");
        write_browse_log(dir.path(), "oxidant-2026-08-24.log");
        crate::logging::convert_for_test(&dir.path().join("oxidant-2026-08-24.log"))
            .expect("convert");
        let (_env, app) = logs_state(dir.path(), true);

        for file in ["2026-08-23", "2026-08-24"] {
            let (status, body) =
                get_logs(&app, &format!("/api/v1/logs?file={file}&level=warn")).await;
            assert_eq!(status, StatusCode::OK, "{file}: {body}");
            assert_eq!(
                body["logs"],
                json!([BROWSE_LINES[1], BROWSE_LINES[2], BROWSE_LINES[4]]),
                "{file}: a level floor keeps warn AND error, plus the line it cannot judge"
            );
            assert!(
                body.get("offset").is_none(),
                "{file}: a filter switches to the cursor envelope: {body}"
            );
            assert_eq!(body["next_before"], Value::Null, "{file}");

            let (_, body) = get_logs(
                &app,
                &format!("/api/v1/logs?file={file}&target=oxidant_connect"),
            )
            .await;
            assert_eq!(
                body["logs"],
                json!([BROWSE_LINES[1], BROWSE_LINES[3]]),
                "{file}: a target prefix, and the unjudgeable line is not in it"
            );

            let (_, body) = get_logs(&app, &format!("/api/v1/logs?file={file}&q=POOL")).await;
            assert_eq!(
                body["logs"],
                json!([BROWSE_LINES[1]]),
                "{file}: free text is case-insensitive"
            );

            let (_, body) = get_logs(
                &app,
                &format!(
                    "/api/v1/logs?file={file}&from={}&to={}",
                    urlencode("2026-08-23T14:00:01Z"),
                    urlencode("2026-08-23T14:00:03Z")
                ),
            )
            .await;
            assert_eq!(
                body["logs"],
                json!([BROWSE_LINES[1], BROWSE_LINES[2], BROWSE_LINES[4]]),
                "{file}: the range is half-open"
            );

            let (_, body) = get_logs(
                &app,
                &format!("/api/v1/logs?file={file}&level=error&target=oxidant_execution&q=failed"),
            )
            .await;
            assert_eq!(
                body["logs"],
                json!([BROWSE_LINES[2]]),
                "{file}: and they compose"
            );
        }
    }

    /// An invalid filter is a `400` that names the value. A filter that silently did nothing
    /// would be read as "there were no errors" — the one answer a log browser must not invent.
    #[tokio::test]
    async fn an_invalid_filter_is_rejected_by_name() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_browse_log(dir.path(), "oxidant-2026-08-23.log");
        let (_env, app) = logs_state(dir.path(), true);
        for (query, needle) in [
            ("level=loud", "loud"),
            ("from=yesterday", "yesterday"),
            ("to=nownow", "nownow"),
        ] {
            let (status, body) =
                get_logs(&app, &format!("/api/v1/logs?file=2026-08-23&{query}")).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{query} -> {body}");
            assert!(
                body["error"].as_str().unwrap().contains(needle),
                "{query} -> {body}"
            );
        }
    }

    /// The backward cursor pages a file exactly once, and `?offset=` still answers PR3's page —
    /// the released contract is not broken by the route learning a second one.
    #[tokio::test]
    async fn the_cursor_pages_backward_and_offset_still_answers_the_old_shape() {
        let dir = tempfile::tempdir().expect("tempdir");
        let line = |i: usize| {
            format!("2026-08-23T14:00:00.500Z [INFO] oxidant_execution - message=line {i}")
        };
        let body: String = (0..25).map(|i| format!("{}\n", line(i))).collect();
        std::fs::write(dir.path().join("oxidant-2026-08-23.log"), &body).expect("write");
        let (_env, app) = logs_state(dir.path(), true);

        // Old shape, untouched: no filter and no cursor is the oldest-first page.
        let (status, page) = get_logs(&app, "/api/v1/logs?file=2026-08-23&limit=10").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(page["offset"], 0);
        assert_eq!(page["next_offset"], 10);
        assert_eq!(page["logs"][0], line(0));
        assert!(page.get("next_before").is_none(), "{page}");

        // New shape: `order=desc` asks for the newest page without implying it with a filter.
        let mut seen: Vec<String> = Vec::new();
        let mut uri = "/api/v1/logs?file=2026-08-23&limit=10&order=desc".to_string();
        loop {
            let (status, page) = get_logs(&app, &uri).await;
            assert_eq!(status, StatusCode::OK, "{page}");
            let mut head: Vec<String> = page["logs"]
                .as_array()
                .expect("logs")
                .iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect();
            head.extend(seen);
            seen = head;
            match page["next_before"].as_u64() {
                Some(cursor) => {
                    uri =
                        format!("/api/v1/logs?file=2026-08-23&limit=10&order=desc&before={cursor}")
                }
                None => break,
            }
        }
        assert_eq!(
            seen,
            (0..25).map(line).collect::<Vec<_>>(),
            "the pages reassemble the file exactly, with no gap and no repeat"
        );
    }

    /// `GET /api/v1/logs/files` is a directory read: what it lists is what exists, and it is
    /// ordered by `(period end, split)` rather than by name — `.2` sorts *before* the plain name
    /// lexicographically while being the newer generation.
    #[tokio::test]
    async fn the_files_route_lists_what_is_on_disk_newest_first() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_browse_log(dir.path(), "oxidant-2026-08-23.log");
        write_browse_log(dir.path(), "oxidant-2026-08-23.2.log");
        write_browse_log(dir.path(), "oxidant-2026-09-01.log");
        write_browse_log(dir.path(), "oxidant.log");
        crate::logging::convert_for_test(&dir.path().join("oxidant-2026-09-01.log"))
            .expect("convert");
        std::fs::write(dir.path().join("syslog"), b"not ours").expect("write");
        let (_env, app) = logs_state(dir.path(), true);

        let (status, body) = get_logs(&app, "/api/v1/logs/files").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let files = body["files"].as_array().expect("files");
        assert_eq!(
            files.iter().map(|f| &f["file"]).collect::<Vec<_>>(),
            vec!["current", "2026-09-01", "2026-08-23.2", "2026-08-23"],
        );
        assert_eq!(files[0]["rolled"], false);
        assert_eq!(files[1]["format"], "parquet", "the server picks the form");
        assert_eq!(files[2]["format"], "text");
        assert_eq!(files[1]["first_ts"], "2026-08-23T14:00:00.000Z");
        assert_eq!(files[1]["last_ts"], "2026-08-23T14:00:04.000Z");
        assert!(files[0]["size_bytes"].as_u64().unwrap() > 0);
        assert_eq!(body["worker"], "driver");
    }

    /// The listing and the tail inherit the same gate as `?file=`, for the same reason: they
    /// reach the same 30 days of every enabled `tracing` field value.
    #[tokio::test]
    async fn every_log_route_inherits_the_status_token_gate() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_browse_log(dir.path(), "oxidant.log");
        let _env = crate::distributed::env_lock();
        let ungated = app(RestState {
            service: Arc::new(OxidantService::new()),
            store: StatementStore::new(),
            log_buffer: LogBuffer::new(MAX_LOG_LINES),
            logs: LogView {
                dir: Some(dir.path().to_path_buf()),
                dedup: true,
            },
            status_token: None,
            dumps: None,
        });
        for route in [
            "/api/v1/logs",
            "/api/v1/logs/files",
            "/api/v1/logs/tail",
            "/api/v1/logs/workers",
        ] {
            assert_eq!(
                get_json(&ungated, route).await.0,
                StatusCode::NOT_FOUND,
                "{route}: unset OXIDANT_STATUS_TOKEN means the route does not exist"
            );
        }
    }

    /// **The SSRF gate.** `?worker=` names an id from this driver's own configuration; it never
    /// names an address. Letting it would turn the driver into a request forwarder for anything
    /// its network can reach, on a route whose token an operator pastes into a monitoring page.
    #[tokio::test]
    async fn the_worker_parameter_only_accepts_configured_workers() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_browse_log(dir.path(), "oxidant.log");
        let (_env, app) =
            logs_state_with_workers(dir.path(), vec!["http://10.0.0.7:50051".to_string()]);
        for hostile in [
            "169.254.169.254",
            "127.0.0.1:1",
            "http://evil.example.com",
            "10.0.0.7:50052",
        ] {
            let (status, body) = get_logs(
                &app,
                &format!("/api/v1/logs?file=current&worker={}", urlencode(hostile)),
            )
            .await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{hostile} -> {body}");
            assert!(
                body["error"].as_str().unwrap().contains("unknown worker"),
                "{hostile} -> {body}"
            );
            // And it names the workers that *are* configured, so the caller can fix it.
            assert!(
                body["error"].as_str().unwrap().contains("10.0.0.7:50051"),
                "{hostile} -> {body}"
            );
        }
        // `driver` is a member of the picker, not a special case above it.
        let (status, body) = get_logs(&app, "/api/v1/logs?file=current&worker=driver").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["worker"], "driver");
    }

    /// **A download is atomic with respect to retention.**
    ///
    /// `dump_chunks` reopened the file and seeked to the offset for every 64 KiB — 16,384
    /// `open` + `seek` + `spawn_blocking` round-trips for a 1 GiB bundle, and not merely
    /// wasteful: a bundle's 24 h TTL can expire between two chunks, and the sweeper does not
    /// wait for a lull. The next `open` then failed *after* `Content-Length` had promised the
    /// whole file, so the operator got a truncated bundle reported as a stream error. Holding
    /// the descriptor is what POSIX gives for free.
    #[tokio::test]
    async fn a_download_survives_the_sweeper_unlinking_the_bundle_under_it() {
        use futures::StreamExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("bundle.parquet");
        let body: Vec<u8> = (0..DUMP_CHUNK * 3 + 17).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &body).expect("write");

        let mut chunks = Box::pin(dump_chunks(path.clone()));
        let mut got = chunks
            .next()
            .await
            .expect("a first chunk")
            .expect("the first read");
        assert_eq!(got.len(), DUMP_CHUNK, "bounded chunks, off the reactor");

        // The TTL lands mid-download, which is the whole point: the reader is holding a
        // `Content-Length` for bytes that no longer have a name.
        std::fs::remove_file(&path).expect("unlink");

        while let Some(chunk) = chunks.next().await {
            got.extend(chunk.expect("the rest of a download that was already promised"));
        }
        assert_eq!(
            got, body,
            "every byte, from the handle the download opened once"
        );
    }

    /// **A follow shows what a node is writing now, and never substitutes another source.**
    ///
    /// `worker_tail` overrode `file` to `current` unconditionally, so **Node = worker, File =
    /// memory ring** painted the page out of the worker's in-memory ring and appended the tail
    /// out of the worker's `oxidant.log` — two sources with different dedup semantics
    /// concatenated into one pane, under an `open` event asserting `"dedup": true` about a ring
    /// that is never deduped. On a worker with `OXIDANT_LOG_ROLL=off` the substitution polled a
    /// file that does not exist, so the stream emitted an `error` every 2 s forever under a
    /// caption reading "following".
    ///
    /// The ring is followable on the **driver** and only there: the driver's tail is the
    /// `tracing` broadcast, which is the stream the ring holds. A worker's is a `?file=` poll,
    /// and a rolling buffer has no forward cursor to poll (F9).
    #[test]
    fn a_worker_tail_follows_what_the_caller_named_or_refuses_to_follow_at_all() {
        assert!(
            check_tail_source(None, None).is_ok(),
            "the driver's ring *is* its tracing stream, and that is what driver_tail follows"
        );
        assert!(check_tail_source(None, Some("current")).is_ok());
        assert!(check_tail_source(Some("w1"), Some("current")).is_ok());

        let refusal = check_tail_source(Some("w1"), None).expect_err("a worker's ring");
        assert!(
            refusal.contains("w1") && refusal.contains("file=current"),
            "the refusal must name the node and the value that works: {refusal}"
        );
        assert!(
            refusal.contains("rolling buffer"),
            "and say why, since the driver's ring *is* followable: {refusal}"
        );

        for rolled in ["2026-08-23", "2026-08-23.2", "2026-W34"] {
            for worker in [None, Some("w1")] {
                let refusal =
                    check_tail_source(worker, Some(rolled)).expect_err("a rolled file: {rolled}");
                assert!(
                    refusal.contains(rolled) && refusal.contains("never grow"),
                    "a rolled file is not followable on any node: {refusal}"
                );
            }
        }

        // And the query the follow actually issues carries the caller's file rather than one of
        // its own choosing — the line that made the substitution possible.
        let follow = Follow {
            token: None,
            endpoint: "unused".to_string(),
            params: LogsParams {
                worker: Some("w1".to_string()),
                file: Some("current".to_string()),
                ..Default::default()
            },
            after: Some(7),
            started: true,
        };
        assert_eq!(follow.query().file.as_deref(), Some("current"));
    }

    /// **A failed poll leaves the cursor alone.** The arm that handles it used to say
    /// `st.after = st.after.or(Some(0))` under the comment "seed the cursor so a recovered
    /// worker resumes rather than replaying" — but it is only reached when the cursor is
    /// *unset*, which is exactly the first poll, which is exactly the poll issued the moment a
    /// worker goes down or against one already down. `Some(0)` is the replay: the recovered
    /// worker's whole live file, 500 rows per 2 s tick, under a caption reading "following".
    #[test]
    fn a_worker_that_comes_back_resumes_at_the_end_of_its_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let body: String = (0..5_000)
            .map(|i| {
                format!(
                    "2026-08-23T14:00:00.000Z [INFO] oxidant_execution - message=old line {i}\n"
                )
            })
            .collect();
        std::fs::write(dir.path().join(crate::history::disk::LIVE_LOG), &body).expect("write");
        let view = LogView {
            dir: Some(dir.path().to_path_buf()),
            dedup: true,
        };
        let ring = LogBuffer::new(MAX_LOG_LINES);

        // The pane is opened against a worker that is already down, so the *first* poll fails.
        let mut follow = Follow {
            token: None,
            endpoint: "unused".to_string(),
            // `file` is the caller's, not an override the follow applies for itself — see
            // `check_tail_source`, which is why every worker follow reaching here says
            // `current`.
            params: LogsParams {
                file: Some("current".to_string()),
                ..Default::default()
            },
            after: None,
            started: false,
        };
        follow.started = true;
        follow.absorb_failure();
        assert_eq!(
            follow.after, None,
            "a failed poll must not invent a cursor — least of all row 0"
        );

        // The worker comes back. The next poll is the seed, and it replays nothing.
        let query = follow.query();
        assert_eq!(query.after, Some(u64::MAX), "the retry re-seeds at the end");
        let step = follow.absorb(&crate::logging::answer(&query, &view, &ring).expect("answer"));
        assert_eq!(
            step,
            TailStep::Idle,
            "no line of the 5,000 already on disk is replayed"
        );
        assert_eq!(
            follow.after,
            Some(5_000),
            "and the follow resumes at the end of the file, not at row 0"
        );

        // What the old spelling did, for contrast: every one of those rows, a page at a time.
        let mut replaying = Follow {
            token: None,
            endpoint: "unused".to_string(),
            params: LogsParams {
                file: Some("current".to_string()),
                ..Default::default()
            },
            after: Some(0),
            started: true,
        };
        let step =
            replaying.absorb(&crate::logging::answer(&replaying.query(), &view, &ring).unwrap());
        assert!(
            matches!(step, TailStep::Lines(lines) if lines.len() == TAIL_PAGE),
            "a cursor of 0 walks the file from the top, which is what this test exists to stop"
        );
    }

    /// **A followed worker never sees a line twice**, however selective the filter — the one
    /// property a follow has, and the one the seeding poll broke twice over.
    ///
    /// The seed asked for `order=desc&limit=200` and emitted the answer as a `lines` frame,
    /// which the pane appends *after* having just painted the newest 500 lines of the same file.
    /// Worse, the cursor it derived from that page was `next_before + lines.len()` — a **match**
    /// position plus a match **count**. `ForwardPage`'s doc states the rule verbatim: a cursor
    /// built from the last match re-reads, and re-emits, every non-matching row after it on
    /// every poll. On a file with 10,000 rows and five errors, `level=error` re-printed those
    /// five errors every 2 s, forever, and the tighter the filter the worse it got.
    ///
    /// Driven against the real read path — `logging::answer` over a real file — because the
    /// arithmetic is only wrong in combination with what a page actually returns.
    #[tokio::test]
    async fn a_filtered_follow_emits_each_match_exactly_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        // A live file whose matches are sparse and *late*: the shape that makes a match
        // position and a scan position disagree by thousands of rows.
        let mut body = String::new();
        for i in 0..2_000 {
            let level = if i >= 1_500 && i % 100 == 0 {
                "ERROR"
            } else {
                "INFO"
            };
            body.push_str(&format!(
                "2026-08-23T14:00:00.000Z [{level}] oxidant_execution - message=line {i}\n"
            ));
        }
        std::fs::write(dir.path().join(crate::history::disk::LIVE_LOG), &body).expect("write");
        let view = LogView {
            dir: Some(dir.path().to_path_buf()),
            dedup: true,
        };
        let ring = LogBuffer::new(MAX_LOG_LINES);
        // The worker's side of the Flight hop, without the hop.
        let node = |query: &crate::logging::LogQuery| {
            crate::logging::answer(query, &view, &ring).expect("the node answers")
        };

        let mut follow = Follow {
            token: None,
            endpoint: "unused".to_string(),
            params: LogsParams {
                worker: Some("10.0.0.7:50051".to_string()),
                file: Some("current".to_string()),
                level: Some("error".to_string()),
                ..Default::default()
            },
            after: None,
            started: false,
        };

        // Poll 1 — the seed. It is a *position*: no lines, and the cursor is the end of the file.
        let seeded = follow.absorb(&node(&follow.query()));
        assert_eq!(
            seeded,
            TailStep::Idle,
            "the seed emits nothing: the pane has already painted this file"
        );
        assert_eq!(
            follow.after,
            Some(2_000),
            "and it names the end of the file, not the last match"
        );

        // Nothing has been appended, so every further poll is silent.
        for tick in 0..3 {
            assert_eq!(
                follow.absorb(&node(&follow.query())),
                TailStep::Idle,
                "tick {tick}: a quiet file emits nothing"
            );
            assert_eq!(follow.after, Some(2_000), "tick {tick}: and does not move");
        }

        // Append one more error, with a thousand non-matching rows after it.
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(dir.path().join(crate::history::disk::LIVE_LOG))
                .expect("open");
            writeln!(
                file,
                "2026-08-23T14:00:01.000Z [ERROR] oxidant_execution - message=the new one"
            )
            .expect("append");
            for i in 0..1_000 {
                writeln!(
                    file,
                    "2026-08-23T14:00:02.000Z [INFO] oxidant_execution - message=after {i}"
                )
                .expect("append");
            }
        }

        let mut emitted: Vec<String> = Vec::new();
        for _ in 0..5 {
            if let TailStep::Lines(lines) = follow.absorb(&node(&follow.query())) {
                emitted.extend(lines);
            }
        }
        assert_eq!(
            emitted,
            vec!["2026-08-23T14:00:01.000Z [ERROR] oxidant_execution - message=the new one"],
            "the appended match, once — not once per poll"
        );
        assert_eq!(
            follow.after,
            Some(3_001),
            "the cursor is the scan position: past the thousand rows that did not match"
        );
    }

    /// **And the list it matches against is not settable by a Connect client.**
    ///
    /// `workers_from_config` lets `spark.oxidant.workers` win — that is how per-session worker
    /// pinning works — and it reads one *process-global* map that the Spark Connect `Config`/`Set`
    /// RPC writes into unconditionally, on an unauthenticated port. So the id-not-address
    /// discipline above was gated behind a value any client could choose: one
    /// `spark.conf.set("spark.oxidant.workers", "attacker.internal:80")` and the driver would
    /// open an HTTP/2 connection from inside its own network the next time a token holder loaded
    /// the Observability page — and the *real* workers would vanish from the picker and from
    /// `POST /api/v1/logs/dump {"worker":"all"}`, which is precisely the bundle-that-omits-the-node
    /// failure the manifest exists to prevent.
    #[tokio::test]
    async fn a_connect_client_cannot_add_a_worker_the_log_routes_will_dial() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_browse_log(dir.path(), "oxidant.log");
        let guard = crate::distributed::env_lock();
        std::env::remove_var("OXIDANT_WORKERS");
        std::env::remove_var("OXIDANT_WORKER_SERVICE");
        let mut service = OxidantService::new();
        service.workers = vec!["http://10.0.0.7:50051".to_string()];
        // What any client reaching the Connect port can do, verbatim.
        service.config.lock().expect("config").insert(
            "spark.oxidant.workers".to_string(),
            "attacker.internal:80".to_string(),
        );
        let service = Arc::new(service);
        let app = app(RestState {
            service: service.clone(),
            store: StatementStore::new(),
            log_buffer: LogBuffer::new(MAX_LOG_LINES),
            logs: LogView {
                dir: Some(dir.path().to_path_buf()),
                dedup: true,
            },
            status_token: Some(LOGS_TOKEN.into()),
            dumps: None,
        });
        assert_eq!(
            service.workers_from_config(),
            vec!["http://attacker.internal:80".to_string()],
            "query routing still honours the session pin — that half is deliberate"
        );

        // The picker lists the deployment, not the pin.
        let (status, body) = get_logs(&app, "/api/v1/logs/workers").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let ids: Vec<String> = body["workers"]
            .as_array()
            .expect("workers")
            .iter()
            .map(|w| w["worker_id"].as_str().unwrap_or_default().to_string())
            .collect();
        assert_eq!(
            ids,
            vec!["driver".to_string(), "10.0.0.7:50051".to_string()],
            "the real worker is still there and the injected one is not: {body}"
        );

        // And the injected id is not dialable.
        let (status, body) = get_logs(
            &app,
            "/api/v1/logs?file=current&worker=attacker.internal:80",
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert!(
            body["error"].as_str().unwrap().contains("unknown worker"),
            "{body}"
        );
        // The real worker is still resolvable, so this is a filter and not a blanket refusal.
        let (status, body) =
            get_logs(&app, "/api/v1/logs?file=current&worker=10.0.0.7:50051").await;
        assert_eq!(
            status,
            StatusCode::BAD_GATEWAY,
            "the configured worker is dialed and unreachable, which is a different answer: {body}"
        );
        drop(guard);
    }

    /// **An unreachable worker is reported, never skipped** (§6b, §9). Silence would read as
    /// "this worker logged nothing", which is exactly the opposite of what happened.
    #[tokio::test]
    async fn an_unreachable_worker_is_reported_with_its_reason() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_browse_log(dir.path(), "oxidant.log");
        // A port nothing is listening on — taken and released, so it is genuinely dead rather
        // than someone else's service.
        let dead = ephemeral_port();
        let (_env, app) =
            logs_state_with_workers(dir.path(), vec![format!("http://127.0.0.1:{dead}")]);

        let (status, body) = get_logs(
            &app,
            &format!("/api/v1/logs?file=current&worker=127.0.0.1:{dead}"),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_GATEWAY,
            "an unreachable worker is a named failure, not an empty page: {body}"
        );
        let error = body["error"].as_str().expect("a reason");
        assert!(error.contains(&dead.to_string()), "{error}");
        assert!(
            body.get("logs").is_none(),
            "and it never comes back as zero lines: {body}"
        );

        let (status, body) = get_logs(&app, "/api/v1/logs/workers").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let workers = body["workers"].as_array().expect("workers");
        assert_eq!(workers[0]["worker_id"], "driver");
        assert_eq!(workers[0]["reachable"], true);
        assert_eq!(workers[1]["worker_id"], format!("127.0.0.1:{dead}"));
        assert_eq!(
            workers[1]["reachable"], false,
            "listed with reachable:false, not dropped from the picker: {body}"
        );
        assert!(
            workers[1]["error"].as_str().is_some_and(|e| !e.is_empty()),
            "with a reason: {body}"
        );
    }

    /// A driver with no workers says so, rather than answering `unknown worker` and leaving the
    /// operator hunting for a typo in a list that does not exist.
    #[tokio::test]
    async fn a_driver_with_no_workers_says_so() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_browse_log(dir.path(), "oxidant.log");
        let (_env, app) = logs_state(dir.path(), true);
        let (status, body) = get_logs(&app, "/api/v1/logs?file=current&worker=w1").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("no workers configured"),
            "{body}"
        );
        let (_, body) = get_logs(&app, "/api/v1/logs/workers").await;
        assert_eq!(
            body["workers"].as_array().expect("workers").len(),
            1,
            "the driver is always in the picker: {body}"
        );
    }

    /// **A one-sided upper bound is a refusal, not "since the epoch".**
    ///
    /// `to` alone fell through to `from = 1970-01-01`, so `{"to": "…"}` bundled the *entire*
    /// retention on every node — which is exactly the request the last-hour default exists to
    /// keep an empty body from making, reached by supplying one field instead of none. It is
    /// also the expensive direction: a dump reads what its window names (F2), so the mistake
    /// costs the whole retention decoded on every node before the 1 GiB cap refuses it — the
    /// "correct and useless" refusal the default was introduced to avoid.
    ///
    /// `from` alone is a different shape and still works: its open end is `now`.
    #[tokio::test]
    async fn a_dump_window_with_only_an_upper_bound_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let logs = dir.path().join("logs");
        let dumps = dir.path().join("dumps");
        std::fs::create_dir_all(&logs).expect("logs");
        std::fs::write(
            logs.join(crate::history::disk::LIVE_LOG),
            "2026-08-23T14:00:00.000Z [INFO] oxidant_execution - message=a line\n",
        )
        .expect("write");
        let (_guard, app) = dump_state(&logs, &dumps, Vec::new(), 1 << 30, 1 << 20);

        let (status, body) = post_dump(&app, json!({ "to": "2026-08-23T15:00:00Z" })).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        let refusal = body["error"].as_str().unwrap_or_default();
        assert!(
            refusal.contains("`from`") && refusal.contains("last hour"),
            "the refusal must name what to supply and what omitting both means: {refusal}"
        );
        assert!(
            refusal.contains("OXIDANT_LOG_KEEP_DAYS"),
            "and what it would otherwise have cost: {refusal}"
        );

        // A malformed `to` is still reported as malformed rather than as the pairing rule: the
        // caller has two things wrong and the parse error is the one they can act on first.
        let (status, body) = post_dump(&app, json!({ "to": "yesterday" })).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            body["error"]
                .as_str()
                .unwrap_or_default()
                .contains("RFC-3339"),
            "{body}"
        );

        // `from` alone keeps working, and the `202` echoes the window it resolved to.
        let (status, body) = post_dump(&app, json!({ "from": "2026-08-23T13:00:00Z" })).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        assert_eq!(body["from"], "2026-08-23T13:00:00Z");
        assert!(
            body["to"].as_str().unwrap_or_default() > "2026-08-23T13:00:00Z",
            "its open end is `now`, which is bounded by the instant supplied: {body}"
        );

        // And so does neither, which is the documented last hour.
        let (status, body) = post_dump(&app, json!({})).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        let from = body["from"].as_str().expect("from").to_string();
        let to = body["to"].as_str().expect("to").to_string();
        let span = chrono::DateTime::parse_from_rfc3339(&to).expect("to")
            - chrono::DateTime::parse_from_rfc3339(&from).expect("from");
        assert_eq!(span.num_seconds(), DEFAULT_DUMP_WINDOW.as_secs() as i64);
    }

    /// A router with a real `dumps/` directory behind it — §6b's one sanctioned copy.
    fn dump_state(
        logs: &std::path::Path,
        dumps: &std::path::Path,
        workers: Vec<String>,
        disk_max_bytes: u64,
        dump_max_bytes: u64,
    ) -> (MutexGuard<'static, ()>, Router) {
        let guard = crate::distributed::env_lock();
        let mut cfg = crate::history::HistoryConfig::for_root(logs.parent().unwrap_or(logs));
        cfg.logs_dir = logs.to_path_buf();
        cfg.dumps_dir = dumps.to_path_buf();
        cfg.disk_max_bytes = disk_max_bytes;
        cfg.disk_min_free_bytes = 0;
        cfg.mounts_override = Some(Vec::new());
        cfg.log_dump_max_bytes = dump_max_bytes;
        let mut service = OxidantService::new();
        service.workers = workers;
        let state = RestState {
            service: Arc::new(service),
            store: StatementStore::new(),
            log_buffer: LogBuffer::new(MAX_LOG_LINES),
            logs: LogView {
                dir: Some(logs.to_path_buf()),
                dedup: true,
            },
            status_token: Some(LOGS_TOKEN.into()),
            dumps: crate::logging::DumpStore::from_config(&cfg),
        };
        (guard, app(state))
    }

    async fn post_dump(app: &Router, body: Value) -> (StatusCode, Value) {
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/logs/dump")
                    .header(header::AUTHORIZATION, format!("Bearer {LOGS_TOKEN}"))
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

    /// Poll a minted dump id until it stops building. The assembly is a task, so a test that
    /// asserted on the first `GET` would be asserting on the scheduler.
    async fn await_dump(app: &Router, id: &str) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
        for _ in 0..200 {
            let resp = app
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .uri(format!("/api/v1/logs/dump/{id}"))
                        .header(header::AUTHORIZATION, format!("Bearer {LOGS_TOKEN}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            if resp.status() == StatusCode::ACCEPTED {
                tokio::time::sleep(Duration::from_millis(20)).await;
                continue;
            }
            let status = resp.status();
            let headers = resp.headers().clone();
            let bytes = resp
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .to_vec();
            return (status, headers, bytes);
        }
        panic!("the dump never left `building`");
    }

    /// **The one time log bytes move.** A dump assembles the driver's own window into
    /// `dumps/dump-<uuid>.parquet`, answers `202` with the id, downloads as one queryable table
    /// with a `node` column, and takes the shape the existing dump prune already recognises.
    #[tokio::test]
    async fn a_dump_assembles_a_bounded_bundle_and_downloads() {
        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let root = tempfile::tempdir().expect("tempdir");
        let logs = root.path().join("logs");
        let dumps = root.path().join("dumps");
        std::fs::create_dir_all(&logs).expect("mkdir");
        write_browse_log(&logs, "oxidant-2026-08-23.log");
        write_browse_log(&logs, "oxidant.log");
        let (_env, app) = dump_state(&logs, &dumps, Vec::new(), u64::MAX, 1 << 30);

        let (status, body) = post_dump(
            &app,
            json!({
                "worker": "driver",
                "from": "2026-08-23T00:00:00Z",
                "to": "2026-08-24T00:00:00Z",
            }),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        let id = body["dumpId"].as_str().expect("a dump id").to_string();
        assert!(id.starts_with("dump-"), "{id}");
        assert_eq!(body["status"], "building");
        assert_eq!(body["from"], "2026-08-23T00:00:00Z");
        assert_eq!(body["nodes"], json!(["driver"]));

        let (status, headers, bytes) = await_dump(&app, &id).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            headers.get(header::CONTENT_TYPE).unwrap(),
            "application/vnd.apache.parquet"
        );
        assert!(headers
            .get(header::CONTENT_DISPOSITION)
            .unwrap()
            .to_str()
            .unwrap()
            .contains(&id));
        assert_eq!(
            bytes.len(),
            headers
                .get(header::CONTENT_LENGTH)
                .unwrap()
                .to_str()
                .unwrap()
                .parse::<usize>()
                .unwrap(),
            "the streamed body is the whole file"
        );

        // One table, and the rows are labelled with the node they came from.
        // Reopened from what the *route* streamed, not from the file on disk: the assertion is
        // that a caller who downloads a bundle gets a readable Parquet.
        let downloaded = root.path().join("downloaded.parquet");
        std::fs::write(&downloaded, &bytes).expect("write");
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&downloaded).unwrap())
                .expect("builder")
                .build()
                .expect("reader");
        let mut nodes: Vec<String> = Vec::new();
        let mut targets: Vec<String> = Vec::new();
        for batch in reader {
            let batch = batch.expect("batch");
            let node = batch
                .column(0)
                .as_any()
                .downcast_ref::<oxidant_loom::arrow::array::StringArray>()
                .expect("node");
            let target = batch
                .column(3)
                .as_any()
                .downcast_ref::<oxidant_loom::arrow::array::StringArray>()
                .expect("target");
            for i in 0..batch.num_rows() {
                nodes.push(node.value(i).to_string());
                targets.push(target.value(i).to_string());
            }
        }
        assert!(!nodes.is_empty(), "the bundle has rows");
        assert!(nodes.iter().all(|n| n == "driver"), "{nodes:?}");
        assert!(
            targets.iter().any(|t| t == "oxidant.dump"),
            "the manifest is in the bundle, queryable: {targets:?}"
        );
        assert!(
            targets.iter().any(|t| t.starts_with("oxidant_execution")),
            "and so are the real log rows: {targets:?}"
        );

        // The file is in `dumps/` under the name the existing prune step recognises.
        let names: Vec<String> = std::fs::read_dir(&dumps)
            .expect("read_dir")
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec![format!("{id}.parquet")]);
        assert!(crate::history::disk::is_dump(&names[0]), "{names:?}");
    }

    /// **A time-windowed dump must prune what it *reads*, not only what it keeps.**
    ///
    /// The walk used to list every file in `logs/` and scan each one end to end: the documented
    /// default is the last hour, and `OXIDANT_LOG_KEEP_DAYS` is 30, so an empty-bodied
    /// `POST /api/v1/logs/dump` fully decoded a month of every node's history — 5,000 rows a
    /// round-trip, over Flight — to assemble an hour of it. No dump test had more history than
    /// window, which is why it was invisible.
    ///
    /// Observable because of rule 1: a line with no parseable timestamp is *unjudgeable*, so the
    /// row filter serves it whatever the window. A file that is never opened cannot contribute
    /// one — which is exactly the coarser, file-level rule [`file_in_window`] documents.
    #[tokio::test]
    async fn a_windowed_dump_does_not_read_the_files_outside_its_window() {
        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let root = tempfile::tempdir().expect("tempdir");
        let logs = root.path().join("logs");
        let dumps = root.path().join("dumps");
        std::fs::create_dir_all(&logs).expect("mkdir");
        // A month of retained history. Every day carries a continuation line with no timestamp
        // of its own — the marker that says whether the file was opened.
        for day in 1..=30 {
            let name = format!("oxidant-2026-07-{day:02}.log");
            std::fs::write(
                logs.join(&name),
                format!(
                    "2026-07-{day:02}T09:00:00.000Z [INFO] oxidant_execution - message=day {day}\n\
                        at oxidant_execution::plan (continuation of day {day})\n"
                ),
            )
            .expect("write");
        }
        // The one day the window is about.
        std::fs::write(
            logs.join("oxidant-2026-08-23.log"),
            "2026-08-23T13:30:00.000Z [ERROR] oxidant_execution - message=the incident\n   \
             at oxidant_execution::plan (continuation of the incident)\n",
        )
        .expect("write");
        let (_env, app) = dump_state(&logs, &dumps, Vec::new(), u64::MAX, 1 << 30);

        let (status, body) = post_dump(
            &app,
            json!({
                "worker": "driver",
                "from": "2026-08-23T13:00:00Z",
                "to": "2026-08-23T14:00:00Z",
            }),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        let id = body["dumpId"].as_str().expect("a dump id").to_string();
        let (status, _, bytes) = await_dump(&app, &id).await;
        assert_eq!(status, StatusCode::OK);

        let downloaded = root.path().join("bundle.parquet");
        std::fs::write(&downloaded, &bytes).expect("write");
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&downloaded).unwrap())
                .expect("builder")
                .build()
                .expect("reader");
        let mut messages: Vec<String> = Vec::new();
        for batch in reader {
            let batch = batch.expect("batch");
            let message = batch
                .column(4)
                .as_any()
                .downcast_ref::<oxidant_loom::arrow::array::StringArray>()
                .expect("message");
            for i in 0..batch.num_rows() {
                if message.is_valid(i) {
                    messages.push(message.value(i).to_string());
                }
            }
        }
        assert!(
            messages.iter().any(|m| m.contains("the incident")),
            "the window's own rows are in the bundle: {messages:?}"
        );
        assert!(
            messages
                .iter()
                .any(|m| m.contains("continuation of the incident")),
            "including the unjudgeable continuation of a line inside the window: {messages:?}"
        );
        assert!(
            !messages.iter().any(|m| m.contains("continuation of day")),
            "but no line from a file the window puts wholly outside — those files are never \
             opened: {messages:?}"
        );
    }

    /// The listing carries `first_ts`/`last_ts`, and that is all the dump needs to decide
    /// whether to open a file. The two asymmetries are the ones that matter: a live file only
    /// grows *newer*, and a file with no parseable bound is unjudgeable and is read.
    #[test]
    fn a_file_is_read_only_when_the_window_could_reach_it() {
        let from = chrono::DateTime::parse_from_rfc3339("2026-08-23T13:00:00Z")
            .unwrap()
            .timestamp_millis();
        let to = chrono::DateTime::parse_from_rfc3339("2026-08-23T14:00:00Z")
            .unwrap()
            .timestamp_millis();
        let rolled = |first: Value, last: Value| json!({"file": "x", "rolled": true, "first_ts": first, "last_ts": last});
        let cases: Vec<(&str, Value, bool)> = vec![
            (
                "wholly before the window",
                rolled(
                    json!("2026-08-22T00:00:00.000Z"),
                    json!("2026-08-22T23:59:59.000Z"),
                ),
                false,
            ),
            (
                "wholly after the window — `to` is exclusive",
                rolled(
                    json!("2026-08-23T14:00:00.000Z"),
                    json!("2026-08-23T15:00:00.000Z"),
                ),
                false,
            ),
            (
                "overlapping at the low end",
                rolled(
                    json!("2026-08-23T12:00:00.000Z"),
                    json!("2026-08-23T13:30:00.000Z"),
                ),
                true,
            ),
            (
                "spanning the window",
                rolled(
                    json!("2026-08-01T00:00:00.000Z"),
                    json!("2026-08-31T00:00:00.000Z"),
                ),
                true,
            ),
            (
                "no parseable bound at either end is unjudgeable, and is read",
                rolled(Value::Null, Value::Null),
                true,
            ),
            (
                "the live file only ever grows newer, so an old `last_ts` cannot rule it out",
                json!({"file": "current", "rolled": false,
                       "first_ts": "2026-08-20T00:00:00.000Z",
                       "last_ts": "2026-08-22T00:00:00.000Z"}),
                true,
            ),
            (
                "but a live file that starts after the window still holds nothing",
                json!({"file": "current", "rolled": false,
                       "first_ts": "2026-08-24T00:00:00.000Z",
                       "last_ts": "2026-08-24T01:00:00.000Z"}),
                false,
            ),
        ];
        for (label, file, expected) in cases {
            assert_eq!(
                file_in_window(&file, Some(from), Some(to)),
                expected,
                "{label}: {file}"
            );
        }
        // An unbounded side never prunes.
        assert!(file_in_window(
            &rolled(
                json!("2020-01-01T00:00:00.000Z"),
                json!("2020-01-01T01:00:00.000Z")
            ),
            None,
            Some(to)
        ));
    }

    /// **A node that could not be reached is named in the bundle, and the dump completes.** A
    /// support bundle that silently omits the node that died is worse than no bundle: the
    /// missing node is the one the case is about.
    #[tokio::test]
    async fn a_dump_names_the_node_it_could_not_reach_and_still_completes() {
        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let root = tempfile::tempdir().expect("tempdir");
        let logs = root.path().join("logs");
        let dumps = root.path().join("dumps");
        std::fs::create_dir_all(&logs).expect("mkdir");
        write_browse_log(&logs, "oxidant.log");
        let dead = ephemeral_port();
        let (_env, app) = dump_state(
            &logs,
            &dumps,
            vec![format!("http://127.0.0.1:{dead}")],
            u64::MAX,
            1 << 30,
        );

        let (status, body) = post_dump(&app, json!({ "worker": "all" })).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        assert_eq!(
            body["nodes"],
            json!(["driver", format!("127.0.0.1:{dead}")]),
            "`all` means the driver and every configured worker"
        );
        let id = body["dumpId"].as_str().expect("id").to_string();

        let (status, _, bytes) = await_dump(&app, &id).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "an unreachable worker does not fail the dump"
        );
        let downloaded = root.path().join("downloaded.parquet");
        std::fs::write(&downloaded, &bytes).expect("write");
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&downloaded).unwrap())
                .expect("builder")
                .build()
                .expect("reader");
        let mut manifest: Vec<(String, String)> = Vec::new();
        for batch in reader {
            let batch = batch.expect("batch");
            let text = |i: usize| {
                batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<oxidant_loom::arrow::array::StringArray>()
                    .expect("string column")
            };
            let (node, target, message) = (text(0), text(3), text(4));
            for i in 0..batch.num_rows() {
                if target.value(i) == "oxidant.dump" {
                    manifest.push((node.value(i).to_string(), message.value(i).to_string()));
                }
            }
        }
        assert!(
            manifest
                .iter()
                .any(|(node, msg)| node == "driver" && msg.contains("answered")),
            "{manifest:?}"
        );
        assert!(
            manifest
                .iter()
                .any(|(node, msg)| node == &format!("127.0.0.1:{dead}")
                    && msg.contains("unreachable")),
            "the dead worker is named in the bundle, not omitted from it: {manifest:?}"
        );
    }

    /// **Refused, not truncated** — and refused on the *request*, so an operator does not learn
    /// about it when they come back for the file.
    #[tokio::test]
    async fn a_dump_that_would_breach_the_disk_budget_is_refused_with_507() {
        let root = tempfile::tempdir().expect("tempdir");
        let logs = root.path().join("logs");
        let dumps = root.path().join("dumps");
        std::fs::create_dir_all(&logs).expect("mkdir");
        write_browse_log(&logs, "oxidant.log");
        // A budget smaller than one dump's reserve.
        let (_env, app) = dump_state(&logs, &dumps, Vec::new(), 1024, 1 << 20);
        let (status, body) = post_dump(&app, json!({ "worker": "driver" })).await;
        assert_eq!(status, StatusCode::INSUFFICIENT_STORAGE, "{body}");
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("OXIDANT_DISK_MAX_BYTES"),
            "{body}"
        );
        assert!(
            !dumps.exists() || std::fs::read_dir(&dumps).unwrap().next().is_none(),
            "nothing is written for a refused dump"
        );
    }

    /// A dump past `OXIDANT_LOG_DUMP_MAX_BYTES` fails with `507` on collection and publishes no
    /// bundle: a shorter file an operator would carry to a support case believing it held the
    /// window they asked for is the outcome the cap exists to prevent.
    #[tokio::test]
    async fn a_dump_past_the_byte_cap_reports_507_and_publishes_nothing() {
        let root = tempfile::tempdir().expect("tempdir");
        let logs = root.path().join("logs");
        let dumps = root.path().join("dumps");
        std::fs::create_dir_all(&logs).expect("mkdir");
        let body: String = (0..40_000)
            .map(|i| {
                format!(
                    "2026-08-23T14:00:00.000Z [INFO] oxidant_execution - message=line {i}, \
                     payload=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa{i}\n"
                )
            })
            .collect();
        std::fs::write(logs.join("oxidant.log"), body).expect("write");
        let (_env, app) = dump_state(&logs, &dumps, Vec::new(), u64::MAX, 4096);

        // An explicit window: the default is the last hour, and these lines are dated.
        let (status, meta) = post_dump(
            &app,
            json!({
                "worker": "driver",
                "from": "2026-08-23T00:00:00Z",
                "to": "2026-08-24T00:00:00Z",
            }),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{meta}");
        assert_eq!(meta["maxBytes"], 4096);
        let id = meta["dumpId"].as_str().expect("id").to_string();

        let (status, _, bytes) = await_dump(&app, &id).await;
        assert_eq!(status, StatusCode::INSUFFICIENT_STORAGE);
        let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("OXIDANT_LOG_DUMP_MAX_BYTES"),
            "{body}"
        );
        let names: Vec<String> = std::fs::read_dir(&dumps)
            .map(|d| {
                d.flatten()
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        assert!(names.is_empty(), "no half-bundle and no .tmp: {names:?}");
    }

    /// The dump routes inherit the same gate, and a dump id is validated rather than joined —
    /// the same discipline as `?file=`'s typed period.
    #[tokio::test]
    async fn the_dump_routes_are_gated_and_the_id_is_validated() {
        let root = tempfile::tempdir().expect("tempdir");
        let logs = root.path().join("logs");
        let dumps = root.path().join("dumps");
        std::fs::create_dir_all(&logs).expect("mkdir");
        write_browse_log(&logs, "oxidant.log");
        let (_env, app) = dump_state(&logs, &dumps, Vec::new(), u64::MAX, 1 << 20);

        // No credential: `401`, with the scheme advertised, exactly like every other log route.
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/logs/dump")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        for hostile in ["dump-not-a-uuid", "..", "dump-", "stmt-x"] {
            let (status, body) =
                get_logs(&app, &format!("/api/v1/logs/dump/{}", urlencode(hostile))).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{hostile} -> {body}");
            assert_eq!(body["error"], "unknown dump id", "{hostile}");
        }
        // And a bad instant is a `400` on the request, before an id exists.
        let (status, body) = post_dump(&app, json!({ "from": "yesterday" })).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            body["error"].as_str().unwrap().contains("yesterday"),
            "{body}"
        );
    }

    /// §6b: "the bundle expires after 24 h and is swept like results" — through the prune step
    /// that shipped in PR2 with its own tests, on the retention pass rather than the pressure
    /// one, so it holds whether or not the budget is tight.
    #[test]
    fn a_dump_expires_after_twenty_four_hours() {
        let dir = tempfile::tempdir().expect("tempdir");
        let dumps = dir.path().join("dumps");
        std::fs::create_dir_all(&dumps).expect("mkdir");
        let old = dumps.join("dump-00000000-0000-0000-0000-000000000001.parquet");
        let fresh = dumps.join("dump-00000000-0000-0000-0000-000000000002.parquet");
        let foreign = dumps.join("someone-elses-bundle.parquet");
        for path in [&old, &fresh, &foreign] {
            std::fs::write(path, vec![0u8; 64]).expect("write");
        }
        let now = chrono::Utc::now();
        let report = disk::prune_expired_dumps(&dumps, crate::logging::DUMP_TTL_SECS, now);
        assert_eq!(report.expired, 0, "nothing is 24 h old yet");
        assert!(old.exists() && fresh.exists());

        // Now with a clock 25 h ahead of both files.
        let later = now + chrono::Duration::hours(25);
        let report = disk::prune_expired_dumps(&dumps, crate::logging::DUMP_TTL_SECS, later);
        assert_eq!(report.expired, 2);
        assert!(report.freed_bytes >= 128);
        assert!(!old.exists() && !fresh.exists());
        assert!(
            foreign.exists(),
            "a bundle the engine did not write is measured and never unlinked"
        );
    }

    /// **One resolver, shared.** The dump store must resolve the data root exactly the way the
    /// rest of the process does, or every promise made about a bundle is made about a directory
    /// nothing else looks at.
    ///
    /// `OXIDANT_DATA_DIR_PER_PROCESS=1` is `docs/runtime-contract.md`'s recommended setting for
    /// a container that co-locates a driver and a worker, and it folds `(role, port)` into the
    /// root. The router used to build its `DumpStore` by reading the environment a *third* time
    /// with `port = 0`, so bundles landed in `<root>/driver-0/dumps/` while the sweeper pruned
    /// `<root>/driver-<port>/dumps/`: a support bundle that never expired, was never billed
    /// against `OXIDANT_DISK_MAX_BYTES`, and whose up-front `507` measured an empty tree.
    #[test]
    fn the_dump_store_writes_where_the_sweeper_prunes_under_a_per_process_data_dir() {
        let _env = crate::distributed::env_lock();
        let root = tempfile::tempdir().expect("tempdir");
        std::env::set_var("OXIDANT_DATA_DIR", root.path());
        std::env::set_var("OXIDANT_DATA_DIR_PER_PROCESS", "1");
        std::env::remove_var("OXIDANT_HISTORY");
        std::env::remove_var("OXIDANT_DUMP_DIR");
        // The port this process really booted on — the one `logging::init` and
        // `init_statement_store` are handed, and the one the sweeper's config carries.
        let store = StatementStore::from_env("driver", 50051).expect("store");
        std::env::remove_var("OXIDANT_DATA_DIR");
        std::env::remove_var("OXIDANT_DATA_DIR_PER_PROCESS");

        let cfg = store.history_config().expect("a durable store").clone();
        let dumps = dumps_for(&store).expect("a dump store");
        assert_eq!(
            dumps.dir(),
            cfg.dumps_dir,
            "a bundle must land in the directory `sweep_disk` prunes"
        );
        assert!(
            cfg.dumps_dir.starts_with(root.path().join("driver-50051")),
            "and that directory is this process's own: {:?}",
            cfg.dumps_dir
        );
        // The tree `admit()` measures is the tree the budget bills, so the up-front `507` sees
        // what the engine actually wrote.
        assert!(
            disk::budget_roots(&cfg)
                .iter()
                .any(|r| dumps.dir().starts_with(r.path())),
            "the dump directory must sit under a measured budget root: {:?}",
            disk::budget_roots(&cfg)
                .iter()
                .map(|r| r.path().to_path_buf())
                .collect::<Vec<_>>()
        );
    }

    /// `?file=` inherits the endpoint's gate unchanged — and it matters more than it did: the
    /// route now reaches up to `OXIDANT_LOG_KEEP_DAYS` of every enabled `tracing` field value
    /// rather than a 1000-line ring.
    #[tokio::test]
    async fn the_file_parameter_inherits_the_status_token_gate() {
        const TOKEN: &str = "s3cret-status-token";
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("oxidant-2026-08-23.log"), "line\n").expect("write");
        let _env = crate::distributed::env_lock();
        let mut state = RestState {
            service: Arc::new(OxidantService::new()),
            store: StatementStore::new(),
            log_buffer: LogBuffer::new(MAX_LOG_LINES),
            logs: LogView {
                dir: Some(dir.path().to_path_buf()),
                dedup: true,
            },
            status_token: None,
            dumps: None,
        };
        // Unset token: the whole endpoint 404s, `?file=` included.
        assert_eq!(
            get_json(&app(state.clone()), "/api/v1/logs?file=2026-08-23")
                .await
                .0,
            StatusCode::NOT_FOUND
        );
        state.status_token = Some(TOKEN.into());
        let gated = app(state);
        let resp = gated
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/v1/logs?file=2026-08-23")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        let resp = gated
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/v1/logs?file=2026-08-23")
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// **H4.** `?file=` is paged, and the page is bounded whatever the caller asks for.
    ///
    /// There was no line cap, no `limit` and no cursor: `MAX_LOG_LINES` applied only to the ring.
    /// A full live log is `OXIDANT_LOG_MAX_FILE_BYTES` (256 MiB, ~2M lines), so one request built
    /// a `Vec<String>` of every line and `serde_json` then serialised a second copy into the body
    /// — over half a GiB transient on a driver whose whole *result* budget is 512 MiB, multiplied
    /// by every concurrent request, on an endpoint the Observability page polls every 5 s.
    #[tokio::test]
    async fn the_file_parameter_pages_and_never_serves_a_whole_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        // A fixed millisecond: the point here is paging, and a per-line timestamp would only
        // re-test the Parquet round trip's 3-digit rendering.
        let line = |i: usize| {
            format!("2026-08-23T14:00:00.500Z [INFO] oxidant_execution - message=line {i}")
        };
        let body: String = (0..2_500).map(|i| format!("{}\n", line(i))).collect();
        std::fs::write(dir.path().join("oxidant-2026-08-23.log"), &body).expect("write");
        std::fs::write(dir.path().join("oxidant.log"), &body).expect("write");
        let (_env, app) = logs_state(dir.path(), true);

        // The default page is the ring's 1000, not the file's 2500.
        let (status, page) = get_logs(&app, "/api/v1/logs?file=2026-08-23").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(page["logs"].as_array().expect("logs").len(), 1_000);
        assert_eq!(page["offset"], 0);
        assert_eq!(page["limit"], 1_000);
        assert_eq!(page["next_offset"], 1_000, "and it says where to continue");
        assert_eq!(page["logs"][0], line(0));

        // The cursor walks the file and stops honestly at the end.
        let (_, page) = get_logs(&app, "/api/v1/logs?file=2026-08-23&offset=1000").await;
        assert_eq!(page["logs"][0], line(1_000));
        assert_eq!(page["next_offset"], 2_000);
        let (_, page) = get_logs(&app, "/api/v1/logs?file=2026-08-23&offset=2000").await;
        assert_eq!(page["logs"].as_array().expect("logs").len(), 500);
        assert_eq!(
            page["next_offset"],
            Value::Null,
            "the last page has no successor"
        );
        assert_eq!(page["logs"][499], line(2_499));

        // A caller cannot ask for the whole file by asking for a huge page.
        let (_, page) = get_logs(&app, "/api/v1/logs?file=current&limit=999999").await;
        assert_eq!(page["limit"], 10_000, "clamped to MAX_LOG_PAGE");
        assert_eq!(page["logs"].as_array().expect("logs").len(), 2_500);
        assert_eq!(page["next_offset"], Value::Null);

        // Same contract through the Parquet path, where an unbounded read is worse still.
        crate::logging::convert_for_test(&dir.path().join("oxidant-2026-08-23.log"))
            .expect("convert");
        let (_, page) = get_logs(&app, "/api/v1/logs?file=2026-08-23&offset=2400&limit=10").await;
        assert_eq!(page["format"], "parquet");
        assert_eq!(
            page["logs"],
            Value::from((2_400..2_410).map(line).collect::<Vec<_>>())
        );
        assert_eq!(page["next_offset"], 2_410);
    }

    /// **L1.** Axum runs extractors in declaration order and short-circuits on rejection, so a
    /// `Query<LogsParams>` parameter answered `400 Bad Request` for `?file=a&file=b` before
    /// `deny_unless_authorized` ran — with the token unset, where the endpoint's stated contract
    /// is "`404`: the route does not exist, exactly like `/api/status`". A `400`-vs-`404` split
    /// tells an unauthenticated caller the route is there. The query is parsed after the gate.
    #[tokio::test]
    async fn a_malformed_query_answers_the_gate_first() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_env, app) = logs_state(dir.path(), true);
        // Gated: a bad query is a bad query, once you are through the door.
        let (status, _) = get_logs(&app, "/api/v1/logs?file=a&file=b").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = get_logs(&app, "/api/v1/logs?limit=lots").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // Ungated: every shape is `404`, malformed or not — the route does not exist.
        // (`logs_state` already holds the process-global env lock for this test.)
        let ungated = app_with_no_token(dir.path());
        for uri in [
            "/api/v1/logs",
            "/api/v1/logs?file=a&file=b",
            "/api/v1/logs?limit=lots",
            "/api/v1/logs?file=2026-08-23",
        ] {
            assert_eq!(
                get_json(&ungated, uri).await.0,
                StatusCode::NOT_FOUND,
                "{uri} must not reveal that the route exists"
            );
        }
    }

    /// A router whose logs endpoint has no token — the `404`-for-everything shape.
    fn app_with_no_token(dir: &std::path::Path) -> Router {
        app(RestState {
            service: Arc::new(OxidantService::new()),
            store: StatementStore::new(),
            log_buffer: LogBuffer::new(MAX_LOG_LINES),
            logs: LogView {
                dir: Some(dir.to_path_buf()),
                dedup: true,
            },
            status_token: None,
            dumps: None,
        })
    }

    /// A node with no rolling writer answers `404`, which is the honest answer: there are no
    /// files. It must not 500, and it must not fall back to the ring and call it a file.
    #[tokio::test]
    async fn a_node_without_a_rolling_writer_has_no_files() {
        let (_env, mut state, _) = test_state();
        state.status_token = Some(LOGS_TOKEN.into());
        let app = app(state);
        let (status, body) = get_logs(&app, "/api/v1/logs?file=2026-08-23").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("no rolled exec logs"),
            "{body}"
        );
    }

    /// Percent-encode a `?file=` value so a traversal attempt reaches the handler as the caller
    /// typed it rather than being normalized away by the URI parser.
    fn urlencode(raw: &str) -> String {
        raw.bytes()
            .map(|b| match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    (b as char).to_string()
                }
                _ => format!("%{b:02X}"),
            })
            .collect()
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

    /// **The server and the browser must escape a backtick the same way.**
    ///
    /// `quote_identifier` used to *strip* backticks. That cannot break out of the quote, so it
    /// was never an injection — it just named a **different table**. The catalog rail
    /// (`catalog_rail.js`, pinned by `ui/src/lib/catalogRail.test.ts`) doubles them, which is
    /// the Databricks dialect's own escape: its backquoted-identifier rule is
    /// `` '`' ( ~'`' | '``' )* '`' ``, with no backslash escape. Two rules for one name is how
    /// a table previews fine from the rail and answers `500` when its columns are expanded.
    ///
    /// Round trip through the engine's own tokenizer rather than against a literal, so a
    /// dialect change is caught here instead of in a warehouse.
    #[test]
    fn quoting_an_identifier_doubles_a_backtick_so_it_round_trips() {
        use datafusion::sql::sqlparser::dialect::DatabricksDialect;
        use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};

        assert_eq!(quote_identifier("we`ird"), "`we``ird`");
        assert_eq!(quote_identifier("orders"), "`orders`");

        for name in [
            "orders",
            "we`ird",
            "`",
            "``",
            "a`b`c",
            "sales.2024",
            "Mixed Case",
        ] {
            let quoted = quote_identifier(name);
            let tokens = Tokenizer::new(&DatabricksDialect {}, &quoted)
                .tokenize()
                .unwrap_or_else(|e| panic!("`{quoted}` does not tokenize: {e}"));
            // One token, and the name that comes back out is the name that went in.
            match tokens.as_slice() {
                [Token::Word(w)] => {
                    assert_eq!(w.quote_style, Some('`'), "{quoted} is not backquoted");
                    assert_eq!(w.value, name, "`{name}` did not survive `{quoted}`");
                }
                other => panic!("`{quoted}` is not one identifier: {other:?}"),
            }
        }
    }

    /// The other half of the same finding: the browser that calls these routes has to escape a
    /// backtick the way they do. `catalog_rail.js` is the rail's whole quoting implementation
    /// and it is spliced into the served page verbatim, so this reads it — like
    /// `oxidant-ui-server`'s connector-event test reads the connector, a missing sibling crate
    /// skips rather than fails.
    #[test]
    fn the_rail_escapes_a_backtick_the_way_these_routes_do() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../oxidant-ui-server/src/catalog_rail.js");
        let Ok(source) = std::fs::read_to_string(&path) else {
            eprintln!("skipping: {} is not in this checkout", path.display());
            return;
        };
        assert!(
            source.contains(r"s.replace(/`/g, '``')"),
            "the rail no longer doubles a backtick; it and `quote_identifier` would build two \
             different names for one table"
        );
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
            logs: LogView::default(),
            status_token: None,
            dumps: None,
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
        // Rows that really existed: a *zero-batch* result is a correct empty answer and keeps
        // answering 200 across the demotion (see `an_empty_result_answers_200_before_and_after_a_restart`).
        let (first, _) = store.insert("SELECT 1");
        store.finish(&first, ExecOutcome::Succeeded(vec![rows_batch(1, 2)]));
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
            logs: LogView::default(),
            status_token: None,
            dumps: None,
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
            logs: LogView::default(),
            status_token: None,
            dumps: None,
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
            logs: LogView::default(),
            status_token: None,
            dumps: None,
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

    /// Log retention runs on every sweep, whether or not the global budget is tight: a driver
    /// far under `OXIDANT_DISK_MAX_BYTES` still may not keep 90 days of logs.
    ///
    /// Driven through `sweep_disk` rather than through `prune_expired_logs` directly, because
    /// the wiring is the part that was missing — the pass existed in PR2's module and nothing
    /// called it.
    #[tokio::test]
    async fn the_sweep_expires_rolled_logs_by_period_with_the_budget_untouched() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store_with(dir.path(), |c| {
            // A budget nothing here can breach: the only thing that may delete a log is
            // retention.
            c.disk_max_bytes = u64::MAX;
            c.log_keep_days = 30;
            c.log_max_total_bytes = u64::MAX;
        });
        let (id, _) = store.insert("SELECT 'kept'");
        store.finish(&id, ExecOutcome::Succeeded(vec![rows_batch(0, 2)]));

        let plant = |rel: &str| {
            let path = dir.path().join(rel);
            std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
            std::fs::write(&path, vec![b'x'; 128]).expect("write");
            path
        };
        let now = chrono::Utc::now();
        let old = now - chrono::Duration::days(90);
        let recent = now - chrono::Duration::days(2);
        let live = plant("logs/oxidant.log");
        let expired = plant(&format!("logs/oxidant-{}.log", old.format("%Y-%m-%d")));
        let expired_parquet = plant(&format!(
            "logs/oxidant-{}.parquet",
            (old + chrono::Duration::days(1)).format("%Y-%m-%d")
        ));
        let kept = plant(&format!("logs/oxidant-{}.log", recent.format("%Y-%m-%d")));

        let report = store.sweep_disk();
        assert_eq!(report.logs_expired, 2, "{report:?}");
        assert_eq!(report.logs_over_cap, 0, "{report:?}");
        assert_eq!(
            report.rolled_logs_removed, 0,
            "the budget step took nothing — this was retention: {report:?}"
        );
        assert!(!expired.exists() && !expired_parquet.exists());
        assert!(kept.exists(), "two days old is inside the window");
        assert!(live.exists(), "the live file is never a candidate");
        assert_eq!(
            store.snapshot(&id).expect("statement").status,
            StatementStatus::Succeeded,
            "no statement history is spent on log retention"
        );
        store.shutdown_for_test();
    }

    /// **M3.** `freed_bytes` in the sweep line must count what *retention* freed too.
    ///
    /// The two retention passes run before `before` is measured — which is the right order for
    /// the prune loop, since it stops it spending a second time what retention already reclaimed
    /// — so their bytes fall outside `before - used_bytes`. Both passes report a `freed_bytes` and
    /// both were being thrown away, so the operator-facing line said `logs_expired=2,
    /// event_logs_pruned=1, freed_bytes=0`: files deleted, zero bytes freed. That reads as a bug
    /// in the sweeper.
    #[tokio::test]
    async fn the_sweep_line_counts_the_bytes_retention_freed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let events = dir.path().join("spark-events");
        std::fs::create_dir_all(&events).expect("mkdir");
        let store = history_store_with(dir.path(), |c| {
            // Nothing here can breach the budget: every byte freed is retention's.
            c.disk_max_bytes = u64::MAX;
            c.log_keep_days = 30;
            c.log_max_total_bytes = u64::MAX;
            c.event_log_dir = Some(events.clone());
            c.event_log_max_bytes = 1_000;
        });
        let old = chrono::Utc::now() - chrono::Duration::days(90);
        let logs = dir.path().join("logs");
        std::fs::create_dir_all(&logs).expect("mkdir");
        std::fs::write(
            logs.join(format!("oxidant-{}.log", old.format("%Y-%m-%d"))),
            vec![b'x'; 4_096],
        )
        .expect("write");
        // Two generations already rolled, plus a live file over half the cap: the pass rolls the
        // live one and then prunes every generation but that newest one.
        for day in ["2020-01-01", "2020-01-02"] {
            std::fs::write(
                events.join(format!("events-{day}.jsonl")),
                vec![b'{'; 2_048],
            )
            .expect("write");
        }
        std::fs::write(events.join("events.jsonl"), vec![b'{'; 800]).expect("write");

        let report = store.sweep_disk();
        assert_eq!(report.logs_expired, 1, "{report:?}");
        assert_eq!(report.event_logs_pruned, 2, "{report:?}");
        assert!(report.event_log_rolled, "{report:?}");
        assert!(
            report.freed_bytes >= 4_096 + 2 * 2_048,
            "the expired log and both pruned generations are in freed_bytes: {report:?}"
        );
        store.shutdown_for_test();
    }

    /// `event_log_dir` joins the budget in PR3 — by rolling, over the real sweep route.
    #[tokio::test]
    async fn the_sweep_rolls_the_event_log_and_counts_it_against_the_budget() {
        let dir = tempfile::tempdir().expect("tempdir");
        let events = dir.path().join("spark-events");
        std::fs::create_dir_all(&events).expect("mkdir");

        let store = history_store_with(dir.path(), |c| {
            c.disk_max_bytes = u64::MAX;
            c.event_log_dir = Some(events.clone());
            c.event_log_max_bytes = 1_000;
        });
        // Written *after* the boot sweep, so the pass under test is the explicit one below.
        std::fs::write(events.join("events.jsonl"), vec![b'{'; 4_000]).expect("write");
        // Somebody else's file in the same directory — an operator points this at a
        // Spark-history-server path that other tools write.
        std::fs::write(events.join("application_1_0001"), b"not ours").expect("write");

        let report = store.sweep_disk();
        assert!(report.event_log_rolled, "{report:?}");
        assert!(
            !events.join("events.jsonl").exists(),
            "rolled, not deleted: the next emit recreates it"
        );
        let rolled: Vec<String> = std::fs::read_dir(&events)
            .expect("read_dir")
            .flatten()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.starts_with("events-"))
            .collect();
        assert_eq!(rolled.len(), 1, "{rolled:?}");
        assert!(
            events.join("application_1_0001").exists(),
            "the sweeper unlinks only the shape it writes"
        );
        assert!(
            report.used_bytes >= 4_000,
            "the directory is billed to the budget now: {report:?}"
        );
        store.shutdown_for_test();
    }

    /// **H2.** `OXIDANT_EVENT_LOG_DIR` exists to be pointed at a Spark-history-server path that
    /// *other tools write*, and the engine can prune exactly one shape there: the
    /// `events[-<period>].jsonl` files it rolled itself. Billing the rest to
    /// `OXIDANT_DISK_MAX_BYTES` pins `used` over the budget for ever, so the sweep runs the whole
    /// prune order to exhaustion — every rolled log, every dump, then `prune_oldest_statement()`
    /// in a `while used > disk_max_bytes` loop until the journal is empty — every five minutes,
    /// to pay for a co-tenant's bytes it can never reclaim.
    ///
    /// The co-tenant here is 5 MiB against a 1 MiB budget. Nothing may be deleted for it.
    #[tokio::test]
    async fn a_spark_history_co_tenant_does_not_cost_the_statement_history() {
        let dir = tempfile::tempdir().expect("tempdir");
        let events = dir.path().join("spark-events");
        std::fs::create_dir_all(&events).expect("mkdir");

        let store = history_store_with(dir.path(), |c| {
            c.disk_max_bytes = 1_000_000;
            c.disk_min_free_bytes = 0;
            c.event_log_dir = Some(events.clone());
            c.event_log_max_bytes = 1_000_000;
        });
        let (id, _) = store.insert("SELECT 'kept'");
        store.finish(&id, ExecOutcome::Succeeded(vec![rows_batch(0, 2)]));
        // Five times the whole budget, and not one byte of it prunable.
        let foreign = events.join("application_1755_0001");
        std::fs::write(&foreign, vec![b'x'; 5_000_000]).expect("write");

        let report = store.sweep_disk();
        assert_eq!(
            report.statements_pruned, 0,
            "the journal is not spent on a co-tenant's bytes: {report:?}"
        );
        assert_eq!(report.rolled_logs_removed, 0, "{report:?}");
        assert_eq!(report.live_results_removed, 0, "{report:?}");
        assert!(
            !report.over_budget,
            "the engine's own subtree is well inside its budget: {report:?}"
        );
        assert!(
            report.used_bytes < 1_000_000,
            "only the engine's own bytes are billed: {report:?}"
        );
        assert!(
            report.foreign_bytes >= 5_000_000,
            "…and the co-tenant's are reported, so a large directory stays explicable: {report:?}"
        );
        assert!(foreign.exists(), "and nothing of theirs is unlinked");
        assert_eq!(
            store.snapshot(&id).expect("statement").status,
            StatementStatus::Succeeded,
            "the statement history survives"
        );
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
        // The counter has to agree with the directory afterwards, or `results_on_disk_bytes`
        // drifts a little further from the truth with every sweep.
        let results_dir = dir.path().join("history/results");
        let on_disk: u64 = std::fs::read_dir(&results_dir)
            .expect("results dir")
            .flatten()
            .filter_map(|e| e.metadata().ok())
            .filter(|m| m.is_file())
            .map(|m| m.len())
            .sum();
        assert_eq!(
            store
                .history
                .as_ref()
                .expect("history")
                .results
                .on_disk_bytes(),
            on_disk,
            "results_on_disk_bytes must match the directory after a prune: {report:?}"
        );
        assert_eq!(on_disk, 0, "and everything was taken: {report:?}");
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

    /// Schema fidelity is one of PR2's two promises, and every other spill test uses a single
    /// `Int64` column. This one round-trips a dictionary, a nullable struct with a null, and a
    /// timestamp *with* a timezone, through the file and through a restart.
    #[tokio::test]
    async fn a_wide_schema_survives_the_spill_round_trip() {
        use oxidant_loom::arrow::array::{
            ArrayRef, Int32Array, StringDictionaryBuilder, StructArray, TimestampMillisecondArray,
        };
        use oxidant_loom::arrow::buffer::NullBuffer;
        use oxidant_loom::arrow::datatypes::{
            DataType, Field, Fields, Int32Type, Schema, TimeUnit,
        };

        let mut dict = StringDictionaryBuilder::<Int32Type>::new();
        for value in ["alpha", "beta", "alpha"] {
            dict.append_value(value);
        }
        let dict: ArrayRef = Arc::new(dict.finish());

        let inner_fields: Fields = vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]
        .into();
        let strukt: ArrayRef = Arc::new(StructArray::new(
            inner_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(oxidant_loom::arrow::array::StringArray::from(vec![
                    Some("x"),
                    None,
                    Some("z"),
                ])) as ArrayRef,
            ],
            // A null struct, so the outer validity buffer has to survive too.
            Some(NullBuffer::from(vec![true, false, true])),
        ));

        let tz: Arc<str> = Arc::from("America/New_York");
        let ts: ArrayRef = Arc::new(
            TimestampMillisecondArray::from(vec![0_i64, 1_700_000_000_000, -86_400_000])
                .with_timezone(Arc::clone(&tz)),
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "label",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("nested", DataType::Struct(inner_fields), true),
            Field::new(
                "at",
                DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::clone(&tz))),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(schema, vec![dict, strukt, ts]).expect("batch");

        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store_with(dir.path(), |c| c.result_persist = ResultPersist::Always);
        let (id, _) = store.insert("SELECT * FROM wide");
        store.finish(&id, ExecOutcome::Succeeded(vec![batch.clone()]));
        store.drain_spills();

        let before = app(rest_state(store.clone()));
        let (status, json_before) =
            get_json(&before, &format!("/api/v1/statements/{id}/result")).await;
        assert_eq!(status, StatusCode::OK, "{json_before}");
        store.shutdown_for_test();
        drop(before);
        drop(store);

        // Off disk, in a new process's worth of state.
        let replayed = history_store_with(dir.path(), |c| c.result_persist = ResultPersist::Always);
        let read_back = replayed
            .history
            .as_ref()
            .expect("history")
            .results
            .read(&id, None)
            .expect("read the spilled result back");
        assert_eq!(read_back.len(), 1);
        assert_eq!(
            read_back[0].schema(),
            batch.schema(),
            "the schema must survive the IPC round trip verbatim, timezone and all"
        );
        assert_eq!(read_back[0], batch, "and so must every value");

        let after = app(rest_state(replayed.clone()));
        let (status, json_after) =
            get_json(&after, &format!("/api/v1/statements/{id}/result")).await;
        assert_eq!(status, StatusCode::OK, "{json_after}");
        assert_eq!(json_after, json_before, "byte-for-byte across the restart");
        replayed.shutdown_for_test();
    }

    /// L4: a sweep that removed statements wakes the store's waiters.
    ///
    /// `prune_oldest_statement` removes statements a `?wait=true` caller may be parked on. The
    /// background sweeper rebuilds a `StatementStore` from weak handles every tick and used to
    /// give it a **fresh** `Notify`, so anyone parked would have blocked to their timeout; it
    /// now carries the store's own, which is what makes this wake-up reach them.
    #[tokio::test]
    async fn a_pruning_sweep_wakes_the_stores_waiters() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store_with(dir.path(), |c| {
            c.disk_max_bytes = 0; // nothing fits: the sweep prunes
            c.disk_min_free_bytes = 0;
        });
        let (id, _) = store.insert("SELECT 1");
        store.finish(&id, ExecOutcome::Succeeded(Vec::new()));

        let notified = store.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let report = store.sweep_disk();
        assert!(report.removed_anything(), "{report:?}");
        assert!(
            tokio::time::timeout(Duration::from_secs(1), notified)
                .await
                .is_ok(),
            "a waiter parked on a statement the sweep removed must be woken, not left to its \
             timeout"
        );
        store.shutdown_for_test();
    }

    /// L4: a journaled `result.file` that does not name this statement's own file is rejected
    /// with a reason, not silently ignored — and never joined onto `results/`, which would be a
    /// path-traversal primitive fed from a file on disk.
    #[tokio::test]
    async fn a_result_pointer_naming_another_file_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store_with(dir.path(), |c| c.result_persist = ResultPersist::Always);
        let (id, _) = store.insert("SELECT n FROM t");
        store.finish(&id, ExecOutcome::Succeeded(vec![rows_batch(0, 4)]));
        store.drain_spills();

        // The honest pointer reads back.
        let app = app(rest_state(store.clone()));
        let (status, body) = get_json(&app, &format!("/api/v1/statements/{id}/result")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result_values(&body), vec![0, 1, 2, 3]);

        // Rewrite the pointer to name something else, exactly as a corrupted or hand-edited
        // journal would.
        {
            let mut inner = store.inner.lock().expect("lock");
            let st = inner.statements.get_mut(&id).expect("hot");
            // Rows released, so `/result` falls through to the disk — the path the pointer is
            // read on.
            st.rows_in_memory = false;
            st.batches = Vec::new();
            st.result_file = Some(ResultPointer {
                file: "../../etc/passwd".to_string(),
                bytes: 1,
            });
        }
        let (status, body) = get_json(&app, &format!("/api/v1/statements/{id}/result")).await;
        assert_eq!(status, StatusCode::GONE, "{body}");
        assert_eq!(body["error"], "result_expired");
        store.shutdown_for_test();
    }

    /// Exclusive ownership of the process-global `/api/status` publish seam.
    ///
    /// [`StatementStore::publish_status_counters`] writes to a process-global slot. In a test
    /// binary that means every store that boots clobbers every other one's counters, so the
    /// store whose counters reach `/api/status` is whichever booted last. A test that wants to
    /// read the endpoint claims the seam for its duration; stores booted by other tests
    /// meanwhile publish nothing and are unaffected (they read `status_counters()` directly).
    pub(super) mod status_seam {
        use std::sync::{Mutex, MutexGuard};
        use std::thread::ThreadId;

        static LOCK: Mutex<()> = Mutex::new(());
        static OWNER: Mutex<Option<ThreadId>> = Mutex::new(None);

        pub(super) struct Claim(#[allow(dead_code)] MutexGuard<'static, ()>);

        /// Block until the seam is free, then own it until the returned guard drops.
        pub(super) fn claim() -> Claim {
            let guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
            oxidant_observability::clear_history_status_source();
            set_owner(Some(std::thread::current().id()));
            Claim(guard)
        }

        /// May the *calling* thread publish? Ownership is per thread, not global: another test
        /// booting a store on another thread while the seam is claimed must not overwrite the
        /// owner's counters.
        pub(crate) fn is_owner() -> bool {
            OWNER
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_some_and(|owner| owner == std::thread::current().id())
        }

        fn set_owner(id: Option<ThreadId>) {
            *OWNER.lock().unwrap_or_else(|e| e.into_inner()) = id;
        }

        impl Drop for Claim {
            fn drop(&mut self) {
                set_owner(None);
                oxidant_observability::clear_history_status_source();
            }
        }
    }

    /// L3: the durability counters go over the wire through `GET /api/status` — the real route,
    /// the real published source, the real serde flattening — not through `status_counters()`.
    ///
    /// Every other test in this PR asserts the `#[cfg(test)]` seam, so the wiring that puts the
    /// counters on the wire had no coverage at all, and `docs/query-history-durability.md` §9
    /// promises exactly this endpoint's behaviour.
    #[tokio::test]
    async fn the_status_endpoint_carries_the_durability_counters_end_to_end() {
        let _seam = status_seam::claim();
        let ui_store: oxidant_observability::SharedStore =
            Arc::new(oxidant_observability::AppStateStore::new());
        let router = oxidant_ui_server::app_router_with(
            Arc::clone(&ui_store),
            Some("status-token".to_string()),
            oxidant_ui_server::DashboardStore::in_memory(),
        );
        let status_json = |router: Router| async move {
            let resp = router
                .oneshot(
                    axum::http::Request::builder()
                        .method("GET")
                        .uri("/api/status")
                        .header(header::AUTHORIZATION, "Bearer status-token")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let status = resp.status();
            let bytes = resp.into_body().collect().await.unwrap().to_bytes();
            (
                status,
                serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null),
            )
        };

        // Nothing published — the `OXIDANT_HISTORY=off` shape. §8 says `off` restores today's
        // behaviour exactly, and today there are no such fields.
        let (status, body) = status_json(router.clone()).await;
        assert_eq!(status, StatusCode::OK);
        let keys = body.as_object().expect("object");
        for field in [
            "history_writes",
            "history_dropped_events",
            "results_on_disk_bytes",
            "result_writes",
            "result_write_failures",
            "disk",
        ] {
            assert!(
                !keys.contains_key(field),
                "with history off the endpoint must not carry {field}: {body}"
            );
        }

        // Boot a durable store. Its counters now reach the endpoint through the published
        // source, flattened into the same object.
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store_with(dir.path(), |c| c.result_persist = ResultPersist::Always);
        let (spilled, _) = store.insert("SELECT n FROM t");
        store.finish(&spilled, ExecOutcome::Succeeded(vec![rows_batch(0, 8)]));
        store.drain_spills();

        let (status, body) = status_json(router.clone()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["history_writes"], "ok", "{body}");
        assert_eq!(body["result_writes"], "ok", "{body}");
        assert_eq!(body["result_write_failures"], 0, "{body}");
        assert_eq!(body["history_dropped_events"], 0, "{body}");
        assert_eq!(body["disk"], "ok", "{body}");
        let counters = store.status_counters().expect("history is on");
        assert!(counters.results_on_disk_bytes > 0);
        assert_eq!(
            body["results_on_disk_bytes"], counters.results_on_disk_bytes,
            "the endpoint must report *this* store's bytes: {body}"
        );
        // And the non-durability half of the snapshot is untouched by the flattening.
        assert!(body["version"].is_string(), "{body}");
        assert!(body["queries"].is_array(), "{body}");

        // Break the spill disk: the endpoint — not just the test seam — says so.
        let (broken, _) = store.insert("SELECT 'broken'");
        std::fs::create_dir_all(
            dir.path()
                .join("history/results")
                .join(format!("{broken}.arrow.tmp")),
        )
        .expect("block the spill");
        store.finish(&broken, ExecOutcome::Succeeded(vec![rows_batch(0, 4)]));
        store.drain_spills();

        let (status, body) = status_json(router).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["result_writes"], "degraded", "{body}");
        assert_eq!(body["result_write_failures"], 1, "{body}");
        assert_eq!(
            body["history_writes"], "degraded",
            "the aggregate has to carry it over the wire too: {body}"
        );
        store.shutdown_for_test();
    }

    /// L2: a succeeded statement with **no batches** — DDL, and plenty of ordinary empty result
    /// sets — is a correct empty answer, and must read the same before and after a restart.
    ///
    /// It used to answer `200 {"rows": []}` live and `410 result_expired` after a restart, with
    /// nothing on the status document saying why, so a correct empty result was indistinguishable
    /// from data loss. There is no file to write (an Arrow IPC stream needs a schema and there is
    /// none), so the marker rides on the terminal snapshot that was being written anyway.
    #[tokio::test]
    async fn an_empty_result_answers_200_before_and_after_a_restart() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store_with(dir.path(), |c| c.result_persist = ResultPersist::Always);
        let (id, _) = store.insert("CREATE TABLE t (a INT)");
        store.finish(&id, ExecOutcome::Succeeded(Vec::new()));
        store.drain_spills();

        let before = app(rest_state(store.clone()));
        let (status, json_before) =
            get_json(&before, &format!("/api/v1/statements/{id}/result")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json_before["rows"].as_array().expect("rows").len(), 0);
        let (_, status_doc) = get_json(&before, &format!("/api/v1/statements/{id}")).await;
        assert_eq!(
            status_doc["resultStatus"], RESULT_EMPTY,
            "the status document says *why* there is no result file"
        );
        let (status, _, csv_before) = get_raw(
            &before,
            &format!("/api/v1/statements/{id}/result?format=csv"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            !dir.path()
                .join("history/results")
                .join(format!("{id}.arrow"))
                .exists(),
            "and there is nothing on disk for it — the marker is the whole record"
        );

        store.shutdown_for_test();
        drop(before);
        drop(store);

        let replayed = history_store_with(dir.path(), |c| c.result_persist = ResultPersist::Always);
        let after = app(rest_state(replayed.clone()));
        let (status, json_after) =
            get_json(&after, &format!("/api/v1/statements/{id}/result")).await;
        assert_eq!(status, StatusCode::OK, "{json_after}");
        assert_eq!(
            json_after, json_before,
            "an empty result must read identically across a restart, not become a 410"
        );
        let (status, _, csv_after) = get_raw(
            &after,
            &format!("/api/v1/statements/{id}/result?format=csv"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(csv_after, csv_before);
        let (_, status_doc) = get_json(&after, &format!("/api/v1/statements/{id}")).await;
        assert_eq!(status_doc["resultStatus"], RESULT_EMPTY);
        replayed.shutdown_for_test();
    }

    /// L1: a spill that completes after its statement was evicted must publish nothing.
    ///
    /// `SpillWriter::handle` appended `folded.to_snapshot()` unconditionally, so a statement
    /// tombstoned while its write was in flight left `tombstone(seq=N)` followed by
    /// `snapshot(seq=M>N)` carrying a live result pointer. `Fold::apply` rejects records for a
    /// tombstoned id, so replay ignored it — **unless** a segment roll and compaction fell
    /// between the two, because `compact_sealed` does not carry tombstones into the new
    /// generation. Then the next boot had no tombstone to reject it and the statement came back,
    /// pointing at a file. This test builds exactly that window.
    #[tokio::test]
    async fn a_spill_that_lands_after_its_statement_was_evicted_publishes_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store_with(dir.path(), |c| c.result_persist = ResultPersist::Always);
        let history = Arc::clone(store.history.as_ref().expect("history"));

        // Park the writer so the statement can be evicted mid-write.
        let release = history.results.block_writer();
        let (doomed, _) = store.insert("SELECT 'doomed'");
        store.finish(&doomed, ExecOutcome::Succeeded(vec![rows_batch(0, 4)]));
        assert!(
            store.prune_oldest_statement().is_some(),
            "the only terminal statement is evicted while its write is parked"
        );
        // Seal and compact, which drops the tombstone: from here the journal has no record that
        // `doomed` ever died.
        history.journal.sync_blocking();
        history.journal.compact_blocking();

        // Now let the write finish. Nothing may be published for a statement neither tier knows.
        drop(release);
        store.drain_spills();
        let file = dir
            .path()
            .join("history/results")
            .join(format!("{doomed}.arrow"));
        assert!(
            !file.exists(),
            "a spill for an evicted statement must leave no file behind"
        );
        assert_eq!(
            history.results.on_disk_bytes(),
            0,
            "and must not be counted against results_on_disk_bytes"
        );

        store.shutdown_for_test();
        drop(history);
        drop(store);
        let replayed = history_store_with(dir.path(), |c| c.result_persist = ResultPersist::Always);
        assert!(
            replayed.snapshot(&doomed).is_none(),
            "the evicted statement must not resurrect with a live result pointer"
        );
        replayed.shutdown_for_test();
    }

    /// M4: `OXIDANT_LOG_DIR`, `OXIDANT_DUMP_DIR` and `OXIDANT_RESULT_DIR` are operator-set paths
    /// that may be shared — `/var/log` is a plausible value for the first. A sweep under disk
    /// pressure must unlink only what the engine itself wrote.
    #[tokio::test]
    async fn the_sweeper_never_unlinks_a_file_the_engine_did_not_write() {
        let dir = tempfile::tempdir().expect("tempdir");
        let plant = |rel: &str, bytes: usize| {
            let path = dir.path().join(rel);
            std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
            std::fs::write(&path, vec![b'x'; bytes]).expect("write");
            path
        };
        // Planted *before* boot, so `clear_tmp` and boot's `reconcile` see them too.
        let foreign_result = plant("history/results/customer-export.arrow", 512);
        let foreign_tmp = plant("history/results/postgres-restore.tmp", 512);
        let store = history_store_with(dir.path(), |c| {
            c.result_persist = ResultPersist::Always;
            // A budget nothing can satisfy: the sweeper walks every step of the order.
            c.disk_max_bytes = 0;
            c.disk_min_free_bytes = 0;
        });
        // Planted after boot so the pass under test is the explicit one below, not the boot one.
        let foreign_log = plant("logs/syslog", 4096);
        let foreign_dump = plant("dumps/customer-facts.parquet", 4096);
        let ours_log = plant("logs/oxidant-2026-08-20.log", 4096);
        let ours_dump = plant("dumps/dump-1.parquet", 4096);
        let report = store.sweep_disk();

        assert!(!ours_log.exists(), "our rolled log goes: {report:?}");
        assert!(!ours_dump.exists(), "our dump goes: {report:?}");
        assert!(
            foreign_log.exists(),
            "a foreign file in the logs dir must survive: {report:?}"
        );
        assert!(
            foreign_dump.exists(),
            "and so must one in the dumps dir: {report:?}"
        );
        assert!(
            foreign_result.exists(),
            "and one in the results dir, which boot both counts and reconciles: {report:?}"
        );
        assert!(
            foreign_tmp.exists(),
            "clear_tmp takes stmt-*.arrow.tmp, not every *.tmp: {report:?}"
        );
        assert_eq!(report.rolled_logs_removed, 1, "{report:?}");
        assert_eq!(report.dumps_removed, 1, "{report:?}");
        store.shutdown_for_test();
    }

    /// M3: the floor is measured against the mount of **every** managed directory, not just the
    /// root's. `OXIDANT_RESULT_DIR` on a second volume was never checked, and a healthy results
    /// volume was reported short because the root's volume was.
    #[tokio::test]
    async fn the_free_space_floor_covers_every_managed_directory_not_just_the_root() {
        let root = tempfile::tempdir().expect("tempdir");
        let other_volume = tempfile::tempdir().expect("tempdir");
        let root_mount = root.path().canonicalize().expect("canonicalize");
        let other_mount = other_volume.path().canonicalize().expect("canonicalize");
        let results_dir = other_volume.path().join("results");

        // The root's volume is roomy; the one `OXIDANT_RESULT_DIR` was moved to is not.
        let mounts = vec![(root_mount, 1 << 40), (other_mount.clone(), 1024)];
        let store = history_store_with(root.path(), |c| {
            c.results_dir = results_dir.clone();
            c.disk_max_bytes = u64::MAX;
            c.disk_min_free_bytes = 1 << 30;
            c.mounts_override = Some(mounts.clone());
        });
        let report = store.sweep_disk();
        assert!(
            report.low_free,
            "the results volume is 1 KiB free against a 1 GiB floor: {report:?}"
        );
        assert_eq!(
            report.free_bytes,
            Some(1024),
            "and the report carries the *lowest* reading, not the root's: {report:?}"
        );
        assert_eq!(report.statements_pruned, 0, "still nothing is pruned");
        assert_eq!(
            store.status_counters().map(|c| c.disk),
            Some(oxidant_observability::disk_state::LOW_FREE.to_string())
        );
        store.shutdown_for_test();
        drop(store);

        // Converse: a short *root* volume with a roomy results volume still trips, and a pair
        // that are both roomy does not.
        let store = history_store_with(root.path(), |c| {
            c.results_dir = results_dir.clone();
            c.disk_max_bytes = u64::MAX;
            c.disk_min_free_bytes = 1 << 30;
            c.mounts_override = Some(vec![
                (other_mount, 1 << 40),
                (std::path::PathBuf::from("/"), 1 << 40),
            ]);
        });
        let report = store.sweep_disk();
        assert!(!report.low_free, "both volumes are roomy: {report:?}");
        assert_eq!(
            store.status_counters().map(|c| c.disk),
            Some(oxidant_observability::disk_state::OK.to_string())
        );
        store.shutdown_for_test();
    }

    /// M2: the sweeper measures the tree **twice** per pass — once before, once after — however
    /// many statements it prunes.
    ///
    /// `over()` used to call a full recursive `subtree_bytes` of the root per loop iteration, so
    /// pruning 10,000 statements meant 10,000 recursive walks of every journal segment, every
    /// compacted generation and every result file, interleaved with 10,000 lock/unlock cycles of
    /// the store mutex that every submit, list, status and result call also takes.
    #[test]
    fn the_disk_sweep_measures_the_tree_twice_however_many_statements_it_prunes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store_with(dir.path(), |c| {
            c.result_persist = ResultPersist::Always;
            c.disk_max_bytes = 0; // nothing can satisfy it: the loop walks the whole list
            c.disk_min_free_bytes = 0;
        });
        for i in 0..40i64 {
            let (id, _) = store.insert(&format!("SELECT {i}"));
            store.finish(&id, ExecOutcome::Succeeded(vec![rows_batch(i, 2)]));
        }

        crate::history::disk::reset_subtree_walks();
        let report = store.sweep_disk();
        let walks = crate::history::disk::subtree_walks();

        assert!(
            report.statements_pruned >= 20,
            "the pass has to actually prune for this to mean anything: {report:?}"
        );
        // One budget root for a plain tempdir (every override is under it), measured before and
        // after — and nothing per victim.
        assert_eq!(
            walks, 2,
            "{} statements pruned cost {walks} recursive walks of the data directory",
            report.statements_pruned
        );
        store.shutdown_for_test();
    }

    /// M1: a result the file cap refused keeps its rows — they are the only copy — but it stops
    /// being a budget victim, so the projection stops claiming bytes it will never free.
    ///
    /// The consequence is documented rather than papered over: the in-memory ceiling is the
    /// budget **plus** every refused result still in the hot tier. Nothing is truncated and
    /// nothing is dropped; refused rows leave on the hot TTL or the record cap.
    #[tokio::test]
    async fn a_refused_result_stays_in_the_budget_and_stops_being_a_victim() {
        let dir = tempfile::tempdir().expect("tempdir");
        let small = retained_bytes(&[rows_batch(0, 4)]);
        let budget = small + small / 2;
        let store = history_store_with(dir.path(), |c| {
            c.result_persist = ResultPersist::OnPressure;
            c.result_memory_budget_bytes = budget;
            // Small enough that a 64-row result's IPC encoding is refused and a 4-row one is not.
            c.result_max_bytes = 512;
        });

        let (refused, _) = store.insert("SELECT * FROM big");
        store.finish(&refused, ExecOutcome::Succeeded(vec![rows_batch(0, 64)]));
        // Pressure, so the big result is actually offered to the writer and refused.
        let (nudge, _) = store.insert("SELECT 'nudge'");
        store.finish(&nudge, ExecOutcome::Succeeded(vec![rows_batch(0, 4)]));
        store.drain_spills();
        let refused_bytes = {
            let inner = store.inner.lock().expect("lock");
            let st = inner.statements.get(&refused).expect("hot");
            assert_eq!(st.result_refused.as_deref(), Some(RESULT_TOO_LARGE));
            assert!(st.rows_in_memory, "the rows are the only copy left");
            st.result_bytes
        };
        assert!(refused_bytes > 0);

        // Ten more small results: each spills and releases, and the refused one is never chosen.
        for i in 0..10i64 {
            let (id, _) = store.insert(&format!("SELECT {i}"));
            store.finish(&id, ExecOutcome::Succeeded(vec![rows_batch(i * 100, 4)]));
            store.drain_spills();
        }
        {
            let mut inner = store.inner.lock().expect("lock");
            let victims = inner.budget_victims();
            assert!(
                !victims.contains(&refused),
                "a refused result must not be re-selected every pass: {victims:?}"
            );
            // Undo what asking cost us: `budget_victims` marks whoever it picked.
            for id in victims {
                if let Some(st) = inner.statements.get_mut(&id) {
                    st.spilling = false;
                    st.release_on_spill = false;
                }
            }
            assert!(
                inner
                    .statements
                    .get(&refused)
                    .expect("still hot")
                    .rows_in_memory,
                "and it certainly must not be released — nothing is on disk for it"
            );
            // The documented ceiling: the budget, plus the refused result. Not unbounded, and
            // not a budget that silently lets go of what memory actually holds.
            let ceiling = budget + refused_bytes;
            assert!(
                inner.result_bytes <= ceiling,
                "retained {} against a ceiling of {ceiling} (budget {budget} + refused \
                 {refused_bytes})",
                inner.result_bytes
            );
            assert!(
                inner.result_bytes >= refused_bytes,
                "and the budget still counts the refused rows: {} < {refused_bytes}",
                inner.result_bytes
            );
        }
        store.shutdown_for_test();
    }

    /// H1, the finding that blocks: a free-space shortfall the engine did **not** cause must
    /// delete nothing.
    ///
    /// The floor used to drive the same unbounded prune loop as the byte budget, and unlike the
    /// byte budget it cannot be made satisfiable by pruning — so a driver sharing a volume with
    /// a CI cache lost every statement record and every spilled result within five minutes of
    /// that volume dipping under 1 GiB free, while `OXIDANT_DISK_MAX_BYTES` was 8 GiB and the
    /// engine was using kilobytes.
    #[tokio::test]
    async fn a_free_space_shortfall_the_engine_did_not_cause_deletes_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        // First boot: a healthy volume, five spilled results.
        let store = history_store_with(dir.path(), |c| c.result_persist = ResultPersist::Always);
        let mut ids = Vec::new();
        for i in 0..5i64 {
            let (id, _) = store.insert(&format!("SELECT {i}"));
            store.finish(&id, ExecOutcome::Succeeded(vec![rows_batch(i * 10, 4)]));
            ids.push(id);
        }
        store.drain_spills();
        let results = dir.path().join("history/results");
        for id in &ids {
            assert!(results.join(format!("{id}.arrow")).exists(), "spilled");
        }
        // Files the sweeper would take *first* if it were pruning at all.
        let plant = |rel: &str, bytes: usize| {
            let path = dir.path().join(rel);
            std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
            std::fs::write(&path, vec![b'x'; bytes]).expect("write");
            path
        };
        let rolled = plant("logs/oxidant-2026-08-20.log", 4096);
        let dump = plant("dumps/dump-1.parquet", 4096);
        store.shutdown_for_test();
        drop(store);

        // Second boot: the engine is nowhere near its own budget, and the volume is "full" for
        // reasons that have nothing to do with it. `with_history` sweeps at boot.
        let store = history_store_with(dir.path(), |c| {
            c.result_persist = ResultPersist::Always;
            c.disk_max_bytes = u64::MAX;
            c.disk_min_free_bytes = 8 * 1024 * 1024 * 1024;
            // 1 KiB free on a volume wanting 8 GiB.
            c.mounts_override = Some(vec![(std::path::PathBuf::from("/"), 1024)]);
        });
        let report = store.sweep_disk();

        assert!(report.low_free, "the floor must be reported: {report:?}");
        assert!(
            !report.over_budget,
            "the engine is not over its own budget: {report:?}"
        );
        assert_eq!(report.statements_pruned, 0, "{report:?}");
        assert_eq!(report.live_results_removed, 0, "{report:?}");
        assert_eq!(report.orphan_results_removed, 0, "{report:?}");
        assert_eq!(report.rolled_logs_removed, 0, "{report:?}");
        assert_eq!(report.dumps_removed, 0, "{report:?}");
        // Substance before arithmetic: these say "nothing was deleted" directly, so a real
        // regression fails on the file it lost rather than on a byte count that only implies it.
        assert!(rolled.exists() && dump.exists(), "{report:?}");
        for id in &ids {
            assert!(
                results.join(format!("{id}.arrow")).exists(),
                "{id} lost its result to a shortfall the engine did not cause"
            );
        }
        assert_eq!(
            store.list().len(),
            5,
            "and the whole history is still there"
        );
        // Exact, and safe to assert exactly: `freed_bytes` sums what the sweep unlinked. It was
        // once `before - used_bytes`, a difference between two separate walks of the tree, which
        // made this line flaky — CI hit `freed_bytes: 4376` here with every removal counter at
        // zero, because a concurrent write between the walks counts as bytes reclaimed.
        assert_eq!(report.freed_bytes, 0, "{report:?}");

        // What the engine does instead: stop writing the large thing, and say so.
        let counters = store.status_counters().expect("history is on");
        assert_eq!(
            counters.disk,
            oxidant_observability::disk_state::LOW_FREE,
            "the operator must be able to tell a host shortfall from an engine overspend: \
             {counters:?}"
        );
        assert_eq!(
            counters.history_writes,
            oxidant_observability::history_writes::DEGRADED,
            "{counters:?}"
        );
        assert!(
            store.history.as_ref().expect("history").results.is_paused(),
            "spill is paused while the volume is short"
        );
        store.shutdown_for_test();
    }

    /// Regression for the flake in
    /// [`a_free_space_shortfall_the_engine_did_not_cause_deletes_nothing`]: `freed_bytes` counts
    /// what the sweep unlinked, never the difference between two walks of the tree.
    ///
    /// It used to be `before - used_bytes` — two separate `measure_roots` calls with the whole
    /// prune pass between them and nothing holding the filesystem still. Any byte that left the
    /// tree in that window was reported as bytes the sweeper reclaimed, so CI saw
    /// `freed_bytes: 4376` on a sweep whose every removal counter was zero, and byte-identical
    /// trees passed and failed the assertion minutes apart.
    ///
    /// The hook makes that window deterministic: a file disappears mid-sweep, by a hand that is
    /// not the sweeper's. Under the old arithmetic this test reports 4096 and fails.
    #[tokio::test]
    async fn freed_bytes_counts_what_the_sweep_unlinked_not_the_walk_delta() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Nowhere near any limit, so the sweeper has no reason to unlink anything.
        let store = history_store_with(dir.path(), |c| {
            c.disk_max_bytes = u64::MAX;
            c.disk_min_free_bytes = 0;
        });

        // Measured (it is inside a budget root) but not a shape the sweeper prunes or retention
        // expires — the same rule as `the_sweeper_never_unlinks_a_file_the_engine_did_not_write`.
        let interloper = dir.path().join("logs").join("not-a-log.txt");
        std::fs::create_dir_all(interloper.parent().expect("parent")).expect("mkdir");
        std::fs::write(&interloper, vec![b'x'; 4_096]).expect("write");

        let victim = interloper.clone();
        disk::SWEEP_MIDPOINT.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                std::fs::remove_file(&victim).expect("the interloper removes its own file");
            }));
        });
        let report = store.sweep_disk();
        disk::SWEEP_MIDPOINT.with(|hook| *hook.borrow_mut() = None);

        assert!(
            !interloper.exists(),
            "the hook must actually have changed the tree, or this proves nothing"
        );
        assert!(
            !report.removed_anything(),
            "the sweeper unlinked nothing: {report:?}"
        );
        assert_eq!(
            report.freed_bytes, 0,
            "4 KiB left the tree under the sweeper, but the sweeper did not free them: {report:?}"
        );
        store.shutdown_for_test();
    }

    /// The other side of H1: the engine still prunes — in the documented order — when its *own*
    /// subtree is past its *own* budget, and it does so whether or not the volume is also short.
    /// `disk:` reports `over_budget` when both hold, because that is the one the operator can act
    /// on.
    #[tokio::test]
    async fn the_engine_prunes_for_its_own_budget_even_while_the_volume_is_short() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = history_store_with(dir.path(), |c| {
            c.result_persist = ResultPersist::Always;
            // Nothing can satisfy this, so the sweeper walks the whole order.
            c.disk_max_bytes = 0;
            c.disk_min_free_bytes = u64::MAX;
            c.mounts_override = Some(vec![(std::path::PathBuf::from("/"), 0)]);
        });
        let plant = |rel: &str, bytes: usize| {
            let path = dir.path().join(rel);
            std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
            std::fs::write(&path, vec![b'x'; bytes]).expect("write");
            path
        };
        let live = plant("logs/oxidant.log", 4096);
        let rolled = plant("logs/oxidant-2026-08-20.log", 4096);
        let dump = plant("dumps/dump-1.parquet", 4096);
        for i in 0..3i64 {
            let (id, _) = store.insert(&format!("SELECT {i}"));
            store.finish(&id, ExecOutcome::Succeeded(Vec::new()));
        }

        let report = store.sweep_disk();
        assert!(report.low_free, "{report:?}");
        assert!(report.over_budget, "{report:?}");
        assert_eq!(report.rolled_logs_removed, 1, "logs first: {report:?}");
        assert_eq!(report.dumps_removed, 1, "then dumps: {report:?}");
        assert!(report.statements_pruned >= 1, "then statements: {report:?}");
        assert!(!rolled.exists() && !dump.exists());
        assert!(live.exists(), "the live log is never deleted — it rotates");
        assert_eq!(
            store.status_counters().map(|c| c.disk),
            Some(oxidant_observability::disk_state::OVER_BUDGET.to_string()),
            "over_budget wins when both hold"
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
