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
//! - `GET /api/v1/logs` — recent process log lines (in-memory ring buffer).
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

impl tracing::field::Visit for LogVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if !self.0.is_empty() {
            self.0.push_str(", ");
        }
        self.0.push_str(field.name());
        self.0.push_str("=");
        self.0.push_str(&format!("{:?}", value));
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

/// `{"name","type"}` pairs of a result's Arrow schema (type names via `Display`, e.g. "Int64").
/// Backtick-quote an identifier, stripping any existing backticks first so we
/// do not double-quote. This is Spark SQL's identifier-quoting rule.
fn quote_identifier(id: &str) -> String {
    format!("`{}`", id.replace('`', ""))
}

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
    let current = registry.current_catalog();
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
    let ns_parts: Vec<String> = namespace.split(',').map(|s| s.to_string()).collect();
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
    // Quote each identifier part to avoid SQL injection / reserved-word issues.
    let ns_parts: Vec<&str> = namespace.split('.').collect();
    let quoted_ns = ns_parts
        .iter()
        .map(|p| quote_identifier(p))
        .collect::<Vec<_>>()
        .join(".");
    let qualified = format!(
        "{}.{n}.{t}",
        quote_identifier(&catalog),
        n = quoted_ns,
        t = quote_identifier(&table)
    );
    let sql = format!("DESCRIBE TABLE {qualified}");
    match engine.sql(&sql).await {
        Ok(batches) => {
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
                        columns.push(json!({
                            "name": names.value(i).to_string(),
                            "type": types.value(i).to_string(),
                        }));
                    }
                }
            }
            Json(json!({ "columns": columns })).into_response()
        }
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("describe table: {e}"),
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

    if parts.len() <= 1 {
        // Suggest catalogs.
        for name in registry.catalog_names() {
            push("catalog", &name, name.clone());
        }
        // Suggest namespaces in the current catalog.
        let current = registry.current_catalog();
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
        // We have a catalog (and maybe namespace) prefix. Try to resolve it.
        let first = parts[0];
        let (catalog, namespace_parts) = if registry.contains(first) {
            (first.to_string(), parts[1..parts.len() - 1].to_vec())
        } else {
            (
                registry.current_catalog(),
                parts[..parts.len() - 1].to_vec(),
            )
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

        // Suggest tables in the resolved namespace.
        let table_names = if catalog == DEFAULT_CATALOG {
            let schema = ns_vec
                .last()
                .cloned()
                .unwrap_or_else(|| "default".to_string());
            engine.builtin_table_names(&schema)
        } else if let Some(provider) = registry.provider(&catalog) {
            provider.list_tables(&ns_vec).await.unwrap_or_default()
        } else {
            Vec::new()
        };
        for t in table_names {
            let qualified = if namespace_str.is_empty() {
                format!("{catalog}.{t}")
            } else {
                format!("{catalog}.{namespace_str}.{t}")
            };
            push("table", &t, qualified);
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

/// Snapshot current process CPU and memory via sysinfo.
fn process_metrics() -> (Option<u64>, Option<u64>, Option<f32>) {
    let mut sys = System::new_all();
    sys.refresh_all();
    let pid = Pid::from_u32(std::process::id());
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

async fn list_logs(State(state): State<RestState>) -> Json<Value> {
    Json(json!({ "logs": state.log_buffer.lines() }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn test_state() -> (RestState, Router) {
        let state = RestState {
            service: Arc::new(OxidantService::new()),
            store: StatementStore::new(),
            log_buffer: LogBuffer::new(MAX_LOG_LINES),
        };
        (state.clone(), app(state))
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
        let (_state, app) = test_state();
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
        let (_state, app) = test_state();
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
        let (_state, app) = test_state();
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
        let (_state, app) = test_state();
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
        let (state, app) = test_state();
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
        let (_state, app) = test_state();
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
        let (_state, app) = test_state();
        let (status, body) = get_json(&app, "/api/v1/cluster/status").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["mode"], "single-node");
        assert_eq!(body["workers"], json!([]));
        assert!(!body["version"].as_str().unwrap().is_empty());
        assert!(body["process"]["memoryUsedMb"].as_u64().is_some());
        assert!(body["process"]["memoryTotalMb"].as_u64().is_some());
    }

    #[tokio::test]
    async fn logs_endpoint_returns_array() {
        let (_state, app) = test_state();
        let (status, body) = get_json(&app, "/api/v1/logs").await;
        assert_eq!(status, StatusCode::OK);
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
        let (_state, app) = test_state();
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
        let (_state, app) = test_state();
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
        let (_state, app) = test_state();
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
        let (_state, app) = test_state();
        let (status, body) = get_json(&app, "/api/v1/catalogs").await;
        assert_eq!(status, StatusCode::OK);
        let catalogs = body["catalogs"].as_array().unwrap();
        assert!(catalogs.iter().any(|c| c["name"] == "spark_catalog"));
    }

    #[tokio::test]
    async fn catalog_namespaces_and_tables() {
        let (_state, app) = test_state();
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
        let (_state, app) = test_state();
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
    }
}
