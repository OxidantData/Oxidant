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
    now_rfc3339, rfc3339_from_ms, FoldedStatement, HistoryConfig, HistoryRuntime, JournalRecord,
    RecordKind, Source, SqlMode, StatementStatus, RECORD_VERSION,
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
    /// Are this statement's result rows retained *here*?
    ///
    /// False for the Connect path: its batches are already streaming to the gRPC client as Arrow
    /// IPC and the store deliberately keeps no second copy (PR2 spills them to
    /// `results/<id>.arrow`). Without this the statement is hot, succeeded, and has an empty
    /// `batches` — so the result endpoint answered `200 {"rows":[]}` for a query whose own status
    /// document said `rowCount: 5`.
    rows_retained: bool,
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
            rows_retained: self.rows_retained,
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
    status: StatementStatus,
    error: Option<String>,
    schema: Option<Vec<(String, String)>>,
    row_count: Option<usize>,
    submitted_at_ms: i64,
    duration_ms: Option<i64>,
    source: Source,
    client_op_id: Option<String>,
    tier: Tier,
    /// Whether `/result` can still answer with rows, or must say `410 result_expired`.
    rows_retained: bool,
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
            // The history tier holds no batches by construction.
            rows_retained: false,
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
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            history_on: false,
            max_records: MAX_STATEMENTS,
            hot_ttl: STATEMENT_TTL,
            max_per_session: usize::MAX,
            retention_days: 0,
        }
    }
}

#[derive(Default)]
struct StoreInner {
    /// Hot tier: live and recently-terminal statements, with their batches and cancel channels.
    statements: std::collections::HashMap<String, Statement>,
    /// History tier: folded snapshots off the journal. No batches, no cancel channel, and
    /// **never touched by TTL eviction** — replay that the first new submit deletes is not replay.
    history: std::collections::HashMap<String, FoldedStatement>,
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
}

impl StoreInner {
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

    /// Take a statement out of the hot tier, keeping its folded state when history is on.
    fn demote(&mut self, id: &str) {
        let Some(st) = self.statements.remove(id) else {
            return;
        };
        if !self.limits.history_on {
            return;
        }
        let last_seq = st.seq;
        let folded = st.to_folded(id, self.sql_mode, last_seq);
        self.history.insert(id.to_string(), folded);
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
}

impl StatementStore {
    /// Today's volatile store: 1000 statements, 1 h TTL, nothing on disk.
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(StoreInner::default())),
            notify: Arc::new(Notify::new()),
            history: None,
        }
    }

    /// Boot the durable store: lock the data dir, replay the journal into the history tier, and
    /// keep writing to it. `Err` fails the process's boot, loudly, with the reason.
    fn with_history(cfg: HistoryConfig) -> Result<Self, String> {
        let (runtime, fold) = HistoryRuntime::boot(cfg)?;
        let limits = Limits {
            history_on: true,
            max_records: runtime.cfg.max_records,
            hot_ttl: runtime.cfg.hot_ttl,
            max_per_session: runtime.cfg.max_per_session,
            retention_days: runtime.cfg.retention_days,
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
            inner.history.insert(id, st);
        }
        let replayed = inner.history.len();
        let store = Self {
            inner: Arc::new(Mutex::new(inner)),
            notify: Arc::new(Notify::new()),
            history: Some(Arc::new(runtime)),
        };
        store.sweep_history();
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
                    rows_retained: true,
                },
            );
            if let (Some(session), Some(alias)) = (session, alias.as_deref()) {
                inner
                    .alias
                    .insert((session.to_string(), alias.to_string()), id.clone());
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
        let record = {
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
                    st.rows_retained = false;
                }
                ExecOutcome::Failed(error) => {
                    st.error = Some(error);
                    st.status = StatementStatus::Failed;
                }
                ExecOutcome::Canceled => {
                    st.status = StatementStatus::Canceled;
                }
            }
            self.terminal_record(id, st)
        };
        // Handing the record over can wait for room in the writer channel, so it happens with
        // the store mutex released — every submit, list and status call takes that mutex, and a
        // slow disk must not be able to stall them. The ack is parked before the waiters are
        // woken, so whoever answers the client finds it.
        self.hand_over_terminal(id, record);
        self.notify.notify_waiters();
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

    /// Snapshot + retained result batches for the result endpoint. A history-tier statement has
    /// no batches: the caller answers `410 result_expired`, not an empty result set.
    fn result(&self, id: &str) -> Option<(StatementSnapshot, Vec<RecordBatch>)> {
        let inner = self.inner.lock().expect("statement store poisoned");
        if let Some(st) = inner.statements.get(id) {
            return Some((st.snapshot(id), st.batches.clone()));
        }
        inner
            .history
            .get(id)
            .map(|st| (StatementSnapshot::from_history(st), Vec::new()))
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

    /// Flush the journal and stop its writer thread — the clean-shutdown seam a restart test
    /// needs so the next boot reads a settled directory.
    #[cfg(test)]
    fn shutdown_for_test(&self) {
        if let Some(history) = &self.history {
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
                let cutoff = now - inner.limits.retention_days * 86_400_000;
                let stale: Vec<String> = inner
                    .history
                    .iter()
                    .filter(|(_, st)| st.status.is_terminal() && st.submitted_at_ms < cutoff)
                    .map(|(id, _)| id.clone())
                    .collect();
                for id in stale {
                    if let Some(st) = inner.history.remove(&id) {
                        evicted.push((id, st.submitted_at_ms));
                    }
                }
            }
            // Per-session share first, so a noisy session evicts itself before it can push
            // another tenant's history out of the global cap.
            let per_session = inner.limits.max_per_session;
            if per_session < usize::MAX {
                let mut by_session: std::collections::HashMap<String, Vec<(u64, String)>> =
                    std::collections::HashMap::new();
                for (id, st) in inner.history.iter() {
                    if let Some(session) = &st.session {
                        if st.status.is_terminal() {
                            by_session
                                .entry(session.clone())
                                .or_default()
                                .push((st.seq, id.clone()));
                        }
                    }
                }
                for (_, mut ids) in by_session {
                    if ids.len() <= per_session {
                        continue;
                    }
                    ids.sort_by_key(|(seq, _)| *seq);
                    let excess = ids.len() - per_session;
                    for (_, id) in ids.into_iter().take(excess) {
                        if let Some(st) = inner.history.remove(&id) {
                            evicted.push((id, st.submitted_at_ms));
                        }
                    }
                }
            }
            while inner.history.len() > inner.limits.max_records {
                let oldest = inner
                    .history
                    .iter()
                    .filter(|(_, st)| st.status.is_terminal())
                    .min_by_key(|(_, st)| st.seq)
                    .map(|(id, _)| id.clone());
                let Some(oldest) = oldest else {
                    // Everything left is non-terminal: running is never evicted, so the cap
                    // yields rather than the statement.
                    break;
                };
                if let Some(st) = inner.history.remove(&oldest) {
                    evicted.push((oldest, st.submitted_at_ms));
                }
            }
            let gone: std::collections::HashSet<&str> =
                evicted.iter().map(|(id, _)| id.as_str()).collect();
            inner
                .alias
                .retain(|_, target| !gone.contains(target.as_str()));
            drop(gone);
            evicted
        };
        // Best-effort, like every other non-terminal write: a tombstone lost to backpressure
        // means the statement is folded again at the next boot and re-evicted by the next sweep
        // — self-healing, and never a lost statement.
        for (id, submitted_at_ms) in tombstones {
            let seq = history.journal.next_seq();
            history
                .journal
                .append(JournalRecord::tombstone(&id, seq, submitted_at_ms));
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

async fn get_result(
    State(state): State<RestState>,
    Path(id): Path<String>,
    Query(params): Query<ResultParams>,
) -> Response {
    let Some((snap, batches)) = state.store.result(&id) else {
        return error_response(StatusCode::NOT_FOUND, "unknown statement id");
    };
    if snap.status != StatementStatus::Succeeded {
        return error_response(
            StatusCode::CONFLICT,
            "statement result is only available once it has succeeded",
        );
    }
    if snap.tier == Tier::History || !snap.rows_retained {
        // The statement is known and succeeded, but its rows are not here: it was replayed from
        // the journal, its hot entry aged out, or it came in over Connect and its batches went
        // straight to the gRPC client. `404` would say "no such id", which is false — and so
        // would `200 {"rows":[]}`, which contradicts the `rowCount` this same statement reports.
        // Reading the rows back off disk is PR2 (`results/<id>.arrow`); until then this is the
        // honest answer, and it is the same code PR2 falls through to.
        return error_response(StatusCode::GONE, "result_expired");
    }
    let limit = params.limit.unwrap_or(DEFAULT_RESULT_LIMIT);
    match params.format.as_deref().unwrap_or("json") {
        "json" => json_result(snap.schema.as_deref(), &batches, limit),
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
}
