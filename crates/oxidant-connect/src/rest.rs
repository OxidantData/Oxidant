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

/// Statement lifecycle, serialized lowercase exactly as the API contract spells it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatementStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Canceled,
}

impl StatementStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
        }
    }

    fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Canceled)
    }
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
    /// Monotonic submit instant backing `duration_ms` and the TTL eviction.
    submitted: std::time::Instant,
    duration_ms: Option<i64>,
    /// Signals the execution task to drop the query future (best-effort cancel).
    cancel: watch::Sender<bool>,
    /// Insertion order; drives oldest-first eviction and newest-first listing.
    seq: u64,
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
        }
    }
}

/// A point-in-time copy of a statement's public state (no result batches).
#[derive(Clone)]
struct StatementSnapshot {
    id: String,
    sql: String,
    status: StatementStatus,
    error: Option<String>,
    schema: Option<Vec<(String, String)>>,
    row_count: Option<usize>,
    submitted_at_ms: i64,
    duration_ms: Option<i64>,
}

/// Terminal result of an execution task, folded into the store by [`StatementStore::finish`].
enum ExecOutcome {
    Succeeded(Vec<RecordBatch>),
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

#[derive(Default)]
struct StoreInner {
    statements: std::collections::HashMap<String, Statement>,
    next_seq: u64,
}

impl StoreInner {
    /// Drop entries older than [`STATEMENT_TTL`].
    fn evict_expired(&mut self) {
        let now = std::time::Instant::now();
        self.statements
            .retain(|_, s| now.duration_since(s.submitted) < STATEMENT_TTL);
    }
}

/// In-memory statement registry shared by the REST handlers and the execution tasks.
#[derive(Clone)]
struct StatementStore {
    inner: Arc<Mutex<StoreInner>>,
    /// Wakes `?wait=true` submitters when any statement reaches a terminal state.
    notify: Arc<Notify>,
}

impl StatementStore {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(StoreInner::default())),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Insert a new `pending` statement, evicting expired entries and the oldest entries once
    /// the count cap is exceeded. Returns the statement id and the receiver end of its cancel
    /// watch (the execution task selects on it).
    fn insert(&self, sql: &str) -> (String, watch::Receiver<bool>) {
        let (tx, rx) = watch::channel(false);
        let id = Uuid::new_v4().to_string();
        let mut inner = self.inner.lock().expect("statement store poisoned");
        inner.evict_expired();
        let seq = inner.next_seq;
        inner.next_seq += 1;
        inner.statements.insert(
            id.clone(),
            Statement {
                sql: sql.to_string(),
                status: StatementStatus::Pending,
                error: None,
                schema: None,
                row_count: None,
                batches: Vec::new(),
                submitted_at_ms: oxidant_observability::now_ms(),
                submitted: std::time::Instant::now(),
                duration_ms: None,
                cancel: tx,
                seq,
            },
        );
        while inner.statements.len() > MAX_STATEMENTS {
            let Some(oldest) = inner
                .statements
                .iter()
                .min_by_key(|(_, s)| s.seq)
                .map(|(id, _)| id.clone())
            else {
                break;
            };
            inner.statements.remove(&oldest);
        }
        (id, rx)
    }

    /// `pending` → `running`. A cancel that landed before the task started wins (the
    /// statement is already terminal and left alone).
    fn mark_running(&self, id: &str) {
        let mut inner = self.inner.lock().expect("statement store poisoned");
        if let Some(st) = inner.statements.get_mut(id) {
            if st.status == StatementStatus::Pending {
                st.status = StatementStatus::Running;
            }
        }
    }

    /// Fold an execution task's terminal outcome into the store. Never overwrites a terminal
    /// state — a cancel that landed first keeps the statement `canceled` (and the late result
    /// batches are dropped here, freeing their memory).
    fn finish(&self, id: &str, outcome: ExecOutcome) {
        {
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
                ExecOutcome::Failed(error) => {
                    st.error = Some(error);
                    st.status = StatementStatus::Failed;
                }
                ExecOutcome::Canceled => {
                    st.status = StatementStatus::Canceled;
                }
            }
        }
        self.notify.notify_waiters();
    }

    /// Best-effort cancel: mark `canceled` and signal the execution task to drop the query
    /// future. Terminal statements are left untouched.
    fn cancel(&self, id: &str) -> CancelOutcome {
        let outcome = {
            let mut inner = self.inner.lock().expect("statement store poisoned");
            match inner.statements.get_mut(id) {
                None => CancelOutcome::NotFound,
                Some(st) if st.status.is_terminal() => CancelOutcome::AlreadyTerminal,
                Some(st) => {
                    st.status = StatementStatus::Canceled;
                    st.duration_ms = Some(st.submitted.elapsed().as_millis() as i64);
                    let _ = st.cancel.send(true);
                    CancelOutcome::Canceled
                }
            }
        };
        if outcome == CancelOutcome::Canceled {
            self.notify.notify_waiters();
        }
        outcome
    }

    fn snapshot(&self, id: &str) -> Option<StatementSnapshot> {
        let inner = self.inner.lock().expect("statement store poisoned");
        inner.statements.get(id).map(|st| st.snapshot(id))
    }

    /// Snapshot + retained result batches for the result endpoint.
    fn result(&self, id: &str) -> Option<(StatementSnapshot, Vec<RecordBatch>)> {
        let inner = self.inner.lock().expect("statement store poisoned");
        inner
            .statements
            .get(id)
            .map(|st| (st.snapshot(id), st.batches.clone()))
    }

    /// Newest-first snapshots, capped at [`LIST_CAP`].
    fn list(&self) -> Vec<StatementSnapshot> {
        let inner = self.inner.lock().expect("statement store poisoned");
        let mut items: Vec<(&String, &Statement)> = inner.statements.iter().collect();
        items.sort_by(|a, b| b.1.seq.cmp(&a.1.seq));
        items
            .into_iter()
            .take(LIST_CAP)
            .map(|(id, st)| st.snapshot(id))
            .collect()
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
fn schema_fields(batches: &[RecordBatch]) -> Option<Vec<(String, String)>> {
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
    });
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
    app(RestState {
        service,
        store: StatementStore::new(),
        log_buffer,
        status_token: oxidant_ui_server::status::status_token_from_env().map(Into::into),
    })
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
            Some(snap) => (StatusCode::OK, Json(snapshot_json(&snap))).into_response(),
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
            });
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
}
