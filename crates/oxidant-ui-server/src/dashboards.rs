//! Dashboard documents — CRUD over `/api/dashboards`, persisted as one JSON file per dashboard.
//!
//! A dashboard is a serializable document: a react-grid-layout `layout` array plus a list of
//! widget specs, each of which is a SQL statement the UI runs on demand through the existing
//! statement API (`/api/v1/statements`, served by `oxidant-connect` on this same port). The
//! engine never executes a widget's SQL from here — this module only stores and validates.
//!
//! ## Storage
//!
//! There is no metadata database in the OSS engine, and dashboards do not deserve one. Each
//! dashboard is a file `<dir>/<id>.json`, written atomically (write temp + rename) and read
//! back into an in-memory map at boot. `<dir>` comes from `OXIDANT_DASHBOARD_DIR`, defaulting
//! to `$XDG_DATA_HOME/oxidant/dashboards` (or `$HOME/.oxidant/dashboards`). When no directory
//! can be resolved — or the one configured is unwritable — the store degrades to in-memory
//! only, so a read-only container still gets a working page rather than a boot failure.
//!
//! ## Validation
//!
//! Requests are validated explicitly rather than by `serde` alone, because the caller is a
//! browser and the useful answer is `400` with a message, not `422` with a serde string:
//! unknown widget type, empty SQL, empty name, duplicate widget id, and a layout entry that
//! names no widget are all rejected.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Path as UrlPath, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

/// Env var pointing at the dashboard directory. Set it to keep dashboards next to the rest of
/// a deployment's state (or on a mounted volume) instead of in the user's home.
pub const DASHBOARD_DIR_ENV: &str = "OXIDANT_DASHBOARD_DIR";

/// Widgets per dashboard. A dashboard is a screen, not a data warehouse; each widget is a
/// live query against the engine, so the cap is also a politeness limit on refresh storms.
const MAX_WIDGETS: usize = 64;
/// Bytes of SQL per widget.
const MAX_SQL_BYTES: usize = 64 * 1024;
/// Characters in a dashboard or widget title.
const MAX_TITLE_CHARS: usize = 200;
/// Dashboards per server. The list page is unpaginated; keep it a list.
const MAX_DASHBOARDS: usize = 500;
/// Auto-refresh bounds, in seconds. Below 5s the UI would spend its life re-querying.
const MIN_REFRESH_SECS: u64 = 5;
const MAX_REFRESH_SECS: u64 = 86_400;

/// The widget types shipped by dashboards v1. Anything richer (funnel, gauge, sankey,
/// heatmap, combo, pivot) is deliberately not here — see issue #35.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WidgetKind {
    Bar,
    Line,
    Area,
    Pie,
    Scatter,
    Table,
    Kpi,
}

impl WidgetKind {
    /// Every accepted `type` value, in the order the error message lists them.
    pub const ALL: [WidgetKind; 7] = [
        WidgetKind::Bar,
        WidgetKind::Line,
        WidgetKind::Area,
        WidgetKind::Pie,
        WidgetKind::Scatter,
        WidgetKind::Table,
        WidgetKind::Kpi,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            WidgetKind::Bar => "bar",
            WidgetKind::Line => "line",
            WidgetKind::Area => "area",
            WidgetKind::Pie => "pie",
            WidgetKind::Scatter => "scatter",
            WidgetKind::Table => "table",
            WidgetKind::Kpi => "kpi",
        }
    }

    fn known() -> String {
        WidgetKind::ALL
            .iter()
            .map(|k| k.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// One widget: a title, a SQL statement, and free-form per-type render options.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Widget {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: WidgetKind,
    pub title: String,
    pub sql: String,
    /// Per-type options (stacked bars, KPI unit/format, table page size…). Opaque to the
    /// server: the mapping from SQL results to an ECharts option lives in the browser.
    #[serde(default)]
    pub options: Map<String, Value>,
}

/// One react-grid-layout entry. `i` is the widget id; unknown RGL keys ride along in `extra`
/// so a future grid feature does not need a server change to persist.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LayoutItem {
    pub i: String,
    pub x: i64,
    pub y: i64,
    pub w: i64,
    pub h: i64,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// A dashboard document as stored on disk and returned by the API.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Dashboard {
    pub id: String,
    pub name: String,
    pub layout: Vec<LayoutItem>,
    pub widgets: Vec<Widget>,
    /// View-mode auto-refresh period. `None` = manual refresh only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_interval_secs: Option<u64>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl Dashboard {
    /// The shape `GET /api/dashboards` returns — enough for the list page without shipping
    /// every widget's SQL.
    fn summary(&self) -> Value {
        json!({
            "id": self.id,
            "name": self.name,
            "widgetCount": self.widgets.len(),
            "refreshIntervalSecs": self.refresh_interval_secs,
            "createdAtMs": self.created_at_ms,
            "updatedAtMs": self.updated_at_ms,
        })
    }
}

// ── Errors ────────────────────────────────────────────────────────────────────────────────

/// An API error rendered as `{"error": "..."}` — the shape `ui/src/lib/api.ts` already reads.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn not_found(id: &str) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: format!("no dashboard `{id}`"),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}

type ApiResult<T> = std::result::Result<T, ApiError>;

// ── Store ─────────────────────────────────────────────────────────────────────────────────

/// In-memory dashboards with write-through to one JSON file each.
#[derive(Clone)]
pub struct DashboardStore {
    inner: Arc<Inner>,
}

struct Inner {
    /// `None` once persistence has been given up on (no directory, or it is unwritable).
    dir: Option<PathBuf>,
    dashboards: RwLock<BTreeMap<String, Dashboard>>,
}

impl DashboardStore {
    /// Store backed by `dir`, loading whatever is already there. A directory that cannot be
    /// created (read-only rootfs, no permission) downgrades to in-memory with a warning
    /// rather than failing the server's boot.
    pub fn with_dir(dir: PathBuf) -> Self {
        if let Err(e) = std::fs::create_dir_all(&dir) {
            tracing::warn!(
                "dashboards: {} is not usable ({e}); dashboards will not persist",
                dir.display()
            );
            return Self::in_memory();
        }
        let dashboards = load_dir(&dir);
        Self {
            inner: Arc::new(Inner {
                dir: Some(dir),
                dashboards: RwLock::new(dashboards),
            }),
        }
    }

    /// Store that never touches the filesystem — tests, and the fallback when no directory
    /// resolves.
    pub fn in_memory() -> Self {
        Self {
            inner: Arc::new(Inner {
                dir: None,
                dashboards: RwLock::new(BTreeMap::new()),
            }),
        }
    }

    /// Store rooted at [`default_dashboard_dir`], or in-memory when that resolves to nothing.
    pub fn from_env() -> Self {
        match default_dashboard_dir() {
            Some(dir) => Self::with_dir(dir),
            None => {
                tracing::warn!(
                    "dashboards: no home or {DASHBOARD_DIR_ENV}; dashboards will not persist"
                );
                Self::in_memory()
            }
        }
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<String, Dashboard>> {
        self.inner
            .dashboards
            .read()
            .unwrap_or_else(|e| e.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, BTreeMap<String, Dashboard>> {
        self.inner
            .dashboards
            .write()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// Newest-updated first — the list page's order.
    pub fn list(&self) -> Vec<Dashboard> {
        let mut all: Vec<Dashboard> = self.read().values().cloned().collect();
        all.sort_by(|a, b| {
            b.updated_at_ms
                .cmp(&a.updated_at_ms)
                .then_with(|| a.name.cmp(&b.name))
        });
        all
    }

    pub fn get(&self, id: &str) -> Option<Dashboard> {
        self.read().get(id).cloned()
    }

    pub fn len(&self) -> usize {
        self.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn insert(&self, dashboard: Dashboard) -> ApiResult<Dashboard> {
        self.persist(&dashboard)?;
        self.write().insert(dashboard.id.clone(), dashboard.clone());
        Ok(dashboard)
    }

    fn remove(&self, id: &str) -> ApiResult<bool> {
        let existed = self.write().remove(id).is_some();
        if existed {
            if let Some(dir) = &self.inner.dir {
                let path = dir.join(format!("{id}.json"));
                if let Err(e) = std::fs::remove_file(&path) {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        return Err(ApiError::internal(format!(
                            "delete {}: {e}",
                            path.display()
                        )));
                    }
                }
            }
        }
        Ok(existed)
    }

    /// Write temp + rename, so a crash mid-write leaves the previous document intact rather
    /// than a half-written one that fails to parse on the next boot.
    fn persist(&self, dashboard: &Dashboard) -> ApiResult<()> {
        let Some(dir) = &self.inner.dir else {
            return Ok(());
        };
        let final_path = dir.join(format!("{}.json", dashboard.id));
        let tmp_path = dir.join(format!(".{}.json.tmp", dashboard.id));
        let body = serde_json::to_vec_pretty(dashboard)
            .map_err(|e| ApiError::internal(format!("serialize dashboard: {e}")))?;
        std::fs::write(&tmp_path, &body)
            .map_err(|e| ApiError::internal(format!("write {}: {e}", tmp_path.display())))?;
        std::fs::rename(&tmp_path, &final_path).map_err(|e| {
            let _ = std::fs::remove_file(&tmp_path);
            ApiError::internal(format!("rename into {}: {e}", final_path.display()))
        })?;
        Ok(())
    }
}

/// Read every `*.json` in `dir`. A file that does not parse is logged and skipped: one bad
/// document must not take the dashboards page down.
fn load_dir(dir: &Path) -> BTreeMap<String, Dashboard> {
    let mut out = BTreeMap::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) => {
            tracing::warn!("dashboards: cannot read {}: {e}", dir.display());
            return out;
        }
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        match std::fs::read(&path)
            .map_err(|e| e.to_string())
            .and_then(|b| serde_json::from_slice::<Dashboard>(&b).map_err(|e| e.to_string()))
        {
            Ok(d) if d.id == path.file_stem().and_then(|s| s.to_str()).unwrap_or("") => {
                out.insert(d.id.clone(), d);
            }
            Ok(d) => tracing::warn!(
                "dashboards: skipping {} — document id `{}` does not match the filename",
                path.display(),
                d.id
            ),
            Err(e) => tracing::warn!("dashboards: skipping {}: {e}", path.display()),
        }
    }
    out
}

/// `$OXIDANT_DASHBOARD_DIR`, else `$XDG_DATA_HOME/oxidant/dashboards`, else
/// `$HOME/.oxidant/dashboards`. `None` when the process has none of those.
pub fn default_dashboard_dir() -> Option<PathBuf> {
    if let Some(dir) = non_empty_env(DASHBOARD_DIR_ENV) {
        return Some(PathBuf::from(dir));
    }
    if let Some(data_home) = non_empty_env("XDG_DATA_HOME") {
        return Some(PathBuf::from(data_home).join("oxidant").join("dashboards"));
    }
    non_empty_env("HOME").map(|home| PathBuf::from(home).join(".oxidant").join("dashboards"))
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.trim().is_empty())
}

// ── Routes ────────────────────────────────────────────────────────────────────────────────

/// The dashboard CRUD routes, with the store already applied as state so this merges into the
/// UI router (whose own state is [`crate::routes::AppState`]).
pub fn router(store: DashboardStore) -> Router {
    Router::new()
        .route("/api/dashboards", get(list).post(create))
        .route(
            "/api/dashboards/{id}",
            get(fetch).patch(update).delete(destroy),
        )
        .with_state(store)
}

async fn list(State(store): State<DashboardStore>) -> Json<Value> {
    let summaries: Vec<Value> = store.list().iter().map(Dashboard::summary).collect();
    Json(json!({ "dashboards": summaries }))
}

async fn fetch(
    State(store): State<DashboardStore>,
    UrlPath(id): UrlPath<String>,
) -> ApiResult<Json<Dashboard>> {
    let id = checked_id(&id)?;
    store
        .get(id)
        .map(Json)
        .ok_or_else(|| ApiError::not_found(id))
}

async fn create(
    State(store): State<DashboardStore>,
    Json(body): Json<Value>,
) -> ApiResult<(StatusCode, Json<Dashboard>)> {
    let body = as_object(&body)?;
    reject_unknown_keys(body, &["name", "layout", "widgets", "refreshIntervalSecs"])?;
    if store.len() >= MAX_DASHBOARDS {
        return Err(ApiError::bad_request(format!(
            "dashboard limit reached ({MAX_DASHBOARDS}); delete one first"
        )));
    }

    let name = parse_name(
        body.get("name")
            .ok_or_else(|| ApiError::bad_request("`name` is required"))?,
    )?;
    let widgets = match body.get("widgets") {
        Some(v) => parse_widgets(v)?,
        None => Vec::new(),
    };
    let layout = match body.get("layout") {
        Some(v) => parse_layout(v, &widgets)?,
        None => Vec::new(),
    };
    let refresh_interval_secs = parse_refresh(body.get("refreshIntervalSecs"))?;

    let now = now_ms();
    store
        .insert(Dashboard {
            id: new_id(),
            name,
            layout,
            widgets,
            refresh_interval_secs,
            created_at_ms: now,
            updated_at_ms: now,
        })
        .map(|d| (StatusCode::CREATED, Json(d)))
}

async fn update(
    State(store): State<DashboardStore>,
    UrlPath(id): UrlPath<String>,
    Json(body): Json<Value>,
) -> ApiResult<Json<Dashboard>> {
    let id = checked_id(&id)?;
    let body = as_object(&body)?;
    reject_unknown_keys(body, &["name", "layout", "widgets", "refreshIntervalSecs"])?;
    let mut dashboard = store.get(id).ok_or_else(|| ApiError::not_found(id))?;

    if let Some(v) = body.get("name") {
        dashboard.name = parse_name(v)?;
    }
    if let Some(v) = body.get("widgets") {
        dashboard.widgets = parse_widgets(v)?;
    }
    if let Some(v) = body.get("layout") {
        dashboard.layout = parse_layout(v, &dashboard.widgets)?;
    } else if body.contains_key("widgets") {
        // Widgets changed without a new layout: drop layout entries for widgets that are
        // gone, so the stored document never references a widget that does not exist.
        let ids: HashSet<&str> = dashboard.widgets.iter().map(|w| w.id.as_str()).collect();
        dashboard
            .layout
            .retain(|item| ids.contains(item.i.as_str()));
    }
    // `refreshIntervalSecs: null` clears auto-refresh; an absent key leaves it alone.
    if body.contains_key("refreshIntervalSecs") {
        dashboard.refresh_interval_secs = parse_refresh(body.get("refreshIntervalSecs"))?;
    }
    dashboard.updated_at_ms = now_ms();
    store.insert(dashboard).map(Json)
}

async fn destroy(
    State(store): State<DashboardStore>,
    UrlPath(id): UrlPath<String>,
) -> ApiResult<StatusCode> {
    let id = checked_id(&id)?;
    if store.remove(id)? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::not_found(id))
    }
}

// ── Validation ────────────────────────────────────────────────────────────────────────────

/// Ids reach the filesystem as `<id>.json`, so anything outside `[A-Za-z0-9_-]` is refused
/// before a path is ever built from it — `..` and `/` included.
fn checked_id(id: &str) -> ApiResult<&str> {
    let ok = !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    if ok {
        Ok(id)
    } else {
        Err(ApiError::bad_request(
            "dashboard id must be 1–64 characters of [A-Za-z0-9_-]",
        ))
    }
}

fn as_object(body: &Value) -> ApiResult<&Map<String, Value>> {
    body.as_object()
        .ok_or_else(|| ApiError::bad_request("body must be a JSON object"))
}

fn reject_unknown_keys(body: &Map<String, Value>, allowed: &[&str]) -> ApiResult<()> {
    for key in body.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(ApiError::bad_request(format!(
                "unknown field `{key}`; allowed: {}",
                allowed.join(", ")
            )));
        }
    }
    Ok(())
}

fn parse_name(value: &Value) -> ApiResult<String> {
    let name = value
        .as_str()
        .ok_or_else(|| ApiError::bad_request("`name` must be a string"))?
        .trim();
    if name.is_empty() {
        return Err(ApiError::bad_request("`name` must not be empty"));
    }
    if name.chars().count() > MAX_TITLE_CHARS {
        return Err(ApiError::bad_request(format!(
            "`name` must be at most {MAX_TITLE_CHARS} characters"
        )));
    }
    Ok(name.to_string())
}

fn parse_refresh(value: Option<&Value>) -> ApiResult<Option<u64>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(v) => {
            let secs = v.as_u64().ok_or_else(|| {
                ApiError::bad_request("`refreshIntervalSecs` must be a positive integer or null")
            })?;
            if !(MIN_REFRESH_SECS..=MAX_REFRESH_SECS).contains(&secs) {
                return Err(ApiError::bad_request(format!(
                    "`refreshIntervalSecs` must be between {MIN_REFRESH_SECS} and {MAX_REFRESH_SECS}"
                )));
            }
            Ok(Some(secs))
        }
    }
}

fn parse_widgets(value: &Value) -> ApiResult<Vec<Widget>> {
    let items = value
        .as_array()
        .ok_or_else(|| ApiError::bad_request("`widgets` must be an array"))?;
    if items.len() > MAX_WIDGETS {
        return Err(ApiError::bad_request(format!(
            "at most {MAX_WIDGETS} widgets per dashboard"
        )));
    }
    let mut seen: HashSet<String> = HashSet::new();
    let mut widgets = Vec::with_capacity(items.len());
    for (idx, item) in items.iter().enumerate() {
        let widget = parse_widget(item, idx)?;
        if !seen.insert(widget.id.clone()) {
            return Err(ApiError::bad_request(format!(
                "duplicate widget id `{}`",
                widget.id
            )));
        }
        widgets.push(widget);
    }
    Ok(widgets)
}

fn parse_widget(value: &Value, idx: usize) -> ApiResult<Widget> {
    let obj = value
        .as_object()
        .ok_or_else(|| ApiError::bad_request(format!("widget[{idx}] must be an object")))?;
    reject_unknown_keys(obj, &["id", "type", "title", "sql", "options"])?;

    let id = obj
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::bad_request(format!("widget[{idx}] `id` must be a string")))?;
    let id = checked_id(id)
        .map_err(|e| ApiError::bad_request(format!("widget[{idx}]: {}", e.message)))?;

    // Unknown widget type is the headline 400: name the offender and list what v1 accepts.
    let kind_value = obj
        .get("type")
        .ok_or_else(|| ApiError::bad_request(format!("widget[{idx}] `type` is required")))?;
    let kind: WidgetKind = serde_json::from_value(kind_value.clone()).map_err(|_| {
        ApiError::bad_request(format!(
            "widget[{idx}] has unknown type {kind_value}; expected one of: {}",
            WidgetKind::known()
        ))
    })?;

    let title = match obj.get("title") {
        None | Some(Value::Null) => String::new(),
        Some(v) => {
            let title = v
                .as_str()
                .ok_or_else(|| {
                    ApiError::bad_request(format!("widget[{idx}] `title` must be a string"))
                })?
                .trim();
            if title.chars().count() > MAX_TITLE_CHARS {
                return Err(ApiError::bad_request(format!(
                    "widget[{idx}] `title` must be at most {MAX_TITLE_CHARS} characters"
                )));
            }
            title.to_string()
        }
    };

    let sql = obj
        .get("sql")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::bad_request(format!("widget[{idx}] `sql` must be a string")))?
        .trim();
    if sql.is_empty() {
        return Err(ApiError::bad_request(format!(
            "widget[{idx}] `sql` must not be empty"
        )));
    }
    if sql.len() > MAX_SQL_BYTES {
        return Err(ApiError::bad_request(format!(
            "widget[{idx}] `sql` must be at most {MAX_SQL_BYTES} bytes"
        )));
    }

    let options = match obj.get("options") {
        None | Some(Value::Null) => Map::new(),
        Some(Value::Object(map)) => map.clone(),
        Some(_) => {
            return Err(ApiError::bad_request(format!(
                "widget[{idx}] `options` must be an object"
            )))
        }
    };

    Ok(Widget {
        id: id.to_string(),
        kind,
        title,
        sql: sql.to_string(),
        options,
    })
}

fn parse_layout(value: &Value, widgets: &[Widget]) -> ApiResult<Vec<LayoutItem>> {
    let items = value
        .as_array()
        .ok_or_else(|| ApiError::bad_request("`layout` must be an array"))?;
    let ids: HashSet<&str> = widgets.iter().map(|w| w.id.as_str()).collect();
    let mut layout = Vec::with_capacity(items.len());
    for (idx, item) in items.iter().enumerate() {
        let parsed: LayoutItem = serde_json::from_value(item.clone())
            .map_err(|e| ApiError::bad_request(format!("layout[{idx}] is not a grid item: {e}")))?;
        if !ids.contains(parsed.i.as_str()) {
            return Err(ApiError::bad_request(format!(
                "layout[{idx}] `i` is `{}`, which is not a widget on this dashboard",
                parsed.i
            )));
        }
        if parsed.w < 1 || parsed.h < 1 || parsed.x < 0 || parsed.y < 0 {
            return Err(ApiError::bad_request(format!(
                "layout[{idx}] must have w >= 1, h >= 1 and non-negative x/y"
            )));
        }
        layout.push(parsed);
    }
    Ok(layout)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Ids are server-generated so they always satisfy [`checked_id`] and never collide with a
/// filename the store did not create.
fn new_id() -> String {
    format!("d{}", uuid::Uuid::new_v4().simple())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn bar_widget(id: &str) -> Value {
        json!({ "id": id, "type": "bar", "title": "Bar", "sql": "SELECT 1 AS a, 2 AS b" })
    }

    fn layout_for(id: &str) -> Value {
        json!([{ "i": id, "x": 0, "y": 0, "w": 6, "h": 8 }])
    }

    async fn send(
        app: &Router,
        method: &str,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let request = Request::builder().method(method).uri(uri);
        let request = match body {
            Some(b) => request
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&b).unwrap()))
                .unwrap(),
            None => request.body(Body::empty()).unwrap(),
        };
        let resp = app.clone().oneshot(request).await.unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(Value::Null)
        };
        (status, value)
    }

    /// The guarding test for the document contract: create → list → get → patch → delete,
    /// with every step reading back exactly what the previous one wrote.
    #[tokio::test]
    async fn crud_roundtrip() {
        let app = router(DashboardStore::in_memory());

        let (status, empty) = send(&app, "GET", "/api/dashboards", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(empty["dashboards"].as_array().unwrap().len(), 0);

        let (status, created) = send(
            &app,
            "POST",
            "/api/dashboards",
            Some(json!({
                "name": "Sales",
                "widgets": [bar_widget("w1")],
                "layout": layout_for("w1"),
                "refreshIntervalSecs": 30,
            })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{created}");
        let id = created["id"].as_str().unwrap().to_string();
        assert_eq!(created["name"], "Sales");
        assert_eq!(created["widgets"][0]["type"], "bar");
        assert_eq!(created["widgets"][0]["sql"], "SELECT 1 AS a, 2 AS b");
        assert_eq!(created["layout"][0]["w"], 6);
        assert_eq!(created["refreshIntervalSecs"], 30);

        let (status, listed) = send(&app, "GET", "/api/dashboards", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(listed["dashboards"][0]["id"], id.as_str());
        assert_eq!(listed["dashboards"][0]["widgetCount"], 1);

        let (status, fetched) = send(&app, "GET", &format!("/api/dashboards/{id}"), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(fetched, created);

        let (status, patched) = send(
            &app,
            "PATCH",
            &format!("/api/dashboards/{id}"),
            Some(json!({
                "name": "Sales v2",
                "widgets": [bar_widget("w1"), { "id": "w2", "type": "kpi", "title": "Total", "sql": "SELECT 42" }],
            })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{patched}");
        assert_eq!(patched["name"], "Sales v2");
        assert_eq!(patched["widgets"].as_array().unwrap().len(), 2);
        // Untouched fields survive a partial update.
        assert_eq!(patched["refreshIntervalSecs"], 30);
        assert_eq!(patched["createdAtMs"], created["createdAtMs"]);
        assert_eq!(patched["layout"][0]["i"], "w1");

        let (status, body) = send(&app, "DELETE", &format!("/api/dashboards/{id}"), None).await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
        let (status, _) = send(&app, "GET", &format!("/api/dashboards/{id}"), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = send(&app, "DELETE", &format!("/api/dashboards/{id}"), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    /// `refreshIntervalSecs: null` is the only way to turn auto-refresh back off, so it has
    /// to mean "clear" rather than "absent".
    #[tokio::test]
    async fn patching_refresh_to_null_clears_it() {
        let app = router(DashboardStore::in_memory());
        let (_, created) = send(
            &app,
            "POST",
            "/api/dashboards",
            Some(json!({ "name": "D", "refreshIntervalSecs": 60 })),
        )
        .await;
        let id = created["id"].as_str().unwrap().to_string();
        let (status, patched) = send(
            &app,
            "PATCH",
            &format!("/api/dashboards/{id}"),
            Some(json!({ "refreshIntervalSecs": Value::Null })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(patched.get("refreshIntervalSecs").is_none(), "{patched}");
    }

    /// Removing a widget without sending a layout must not leave the document pointing at a
    /// widget that no longer exists.
    #[tokio::test]
    async fn dropping_a_widget_drops_its_layout_entry() {
        let app = router(DashboardStore::in_memory());
        let (_, created) = send(
            &app,
            "POST",
            "/api/dashboards",
            Some(json!({
                "name": "D",
                "widgets": [bar_widget("w1"), bar_widget("w2")],
                "layout": [
                    { "i": "w1", "x": 0, "y": 0, "w": 6, "h": 8 },
                    { "i": "w2", "x": 6, "y": 0, "w": 6, "h": 8 },
                ],
            })),
        )
        .await;
        let id = created["id"].as_str().unwrap().to_string();
        let (status, patched) = send(
            &app,
            "PATCH",
            &format!("/api/dashboards/{id}"),
            Some(json!({ "widgets": [bar_widget("w1")] })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let layout = patched["layout"].as_array().unwrap();
        assert_eq!(layout.len(), 1, "{patched}");
        assert_eq!(layout[0]["i"], "w1");
    }

    /// Every widget-spec rejection in one table: the API's contract with the editor form.
    #[tokio::test]
    async fn invalid_specs_are_rejected_with_400() {
        let app = router(DashboardStore::in_memory());
        let cases: Vec<(&str, Value)> = vec![
            (
                "unknown widget type",
                json!({ "name": "D", "widgets": [{ "id": "w1", "type": "funnel", "sql": "SELECT 1" }] }),
            ),
            (
                "empty sql",
                json!({ "name": "D", "widgets": [{ "id": "w1", "type": "bar", "sql": "   " }] }),
            ),
            (
                "missing sql",
                json!({ "name": "D", "widgets": [{ "id": "w1", "type": "bar" }] }),
            ),
            ("empty name", json!({ "name": "  " })),
            ("missing name", json!({ "widgets": [] })),
            (
                "duplicate widget ids",
                json!({ "name": "D", "widgets": [bar_widget("w1"), bar_widget("w1")] }),
            ),
            (
                "widget id with a path separator",
                json!({ "name": "D", "widgets": [{ "id": "../evil", "type": "bar", "sql": "SELECT 1" }] }),
            ),
            (
                "layout entry naming no widget",
                json!({ "name": "D", "widgets": [bar_widget("w1")], "layout": layout_for("ghost") }),
            ),
            (
                "zero-width layout entry",
                json!({ "name": "D", "widgets": [bar_widget("w1")], "layout": [{ "i": "w1", "x": 0, "y": 0, "w": 0, "h": 4 }] }),
            ),
            (
                "refresh interval below the floor",
                json!({ "name": "D", "refreshIntervalSecs": 1 }),
            ),
            (
                "unknown top-level field",
                json!({ "name": "D", "colour": "red" }),
            ),
            (
                "unknown widget field",
                json!({ "name": "D", "widgets": [{ "id": "w1", "type": "bar", "sql": "SELECT 1", "drill": true }] }),
            ),
            ("body is not an object", json!([{ "name": "D" }])),
        ];
        for (label, body) in cases {
            let (status, response) = send(&app, "POST", "/api/dashboards", Some(body)).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{label} -> {response}");
            assert!(
                response["error"].is_string(),
                "{label} must explain itself: {response}"
            );
        }
        // …and nothing was stored along the way.
        let (_, listed) = send(&app, "GET", "/api/dashboards", None).await;
        assert_eq!(listed["dashboards"].as_array().unwrap().len(), 0);
    }

    /// The unknown-type message must name the accepted set — the editor shows it verbatim.
    #[tokio::test]
    async fn unknown_type_error_lists_the_v1_widgets() {
        let app = router(DashboardStore::in_memory());
        let (status, response) = send(
            &app,
            "POST",
            "/api/dashboards",
            Some(json!({ "name": "D", "widgets": [{ "id": "w1", "type": "sankey", "sql": "SELECT 1" }] })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let message = response["error"].as_str().unwrap();
        for kind in WidgetKind::ALL {
            assert!(message.contains(kind.as_str()), "{message} omits {kind:?}");
        }
    }

    /// Each v1 widget type round-trips through the API unchanged.
    #[tokio::test]
    async fn every_v1_widget_type_is_accepted() {
        let app = router(DashboardStore::in_memory());
        let widgets: Vec<Value> = WidgetKind::ALL
            .iter()
            .enumerate()
            .map(|(i, kind)| {
                json!({
                    "id": format!("w{i}"),
                    "type": kind.as_str(),
                    "title": kind.as_str(),
                    "sql": "SELECT 1 AS a, 2 AS b",
                    "options": { "stacked": true },
                })
            })
            .collect();
        let (status, created) = send(
            &app,
            "POST",
            "/api/dashboards",
            Some(json!({ "name": "All widgets", "widgets": widgets })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{created}");
        let stored = created["widgets"].as_array().unwrap();
        assert_eq!(stored.len(), WidgetKind::ALL.len());
        for (widget, kind) in stored.iter().zip(WidgetKind::ALL) {
            assert_eq!(widget["type"], kind.as_str());
            assert_eq!(widget["options"]["stacked"], true);
        }
    }

    /// A path id that escapes the store directory is refused before any path is built.
    #[tokio::test]
    async fn traversal_ids_are_refused() {
        let app = router(DashboardStore::in_memory());
        for id in ["..%2f..%2fetc%2fpasswd", "a.b", "a%20b"] {
            let (status, _) = send(&app, "GET", &format!("/api/dashboards/{id}"), None).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "id {id} must be refused");
        }
    }

    /// Dashboards must survive a restart: write through one store, read back through a fresh
    /// one over the same directory.
    #[tokio::test]
    async fn dashboards_persist_across_a_restart() {
        let dir = std::env::temp_dir().join(format!("oxidant-dash-test-{}", uuid::Uuid::new_v4()));
        let app = router(DashboardStore::with_dir(dir.clone()));
        let (status, created) = send(
            &app,
            "POST",
            "/api/dashboards",
            Some(json!({
                "name": "Persisted",
                "widgets": [bar_widget("w1")],
                "layout": layout_for("w1"),
            })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{created}");
        let id = created["id"].as_str().unwrap().to_string();

        let reopened = DashboardStore::with_dir(dir.clone());
        assert_eq!(reopened.get(&id).map(|d| d.name), Some("Persisted".into()));
        assert_eq!(reopened.get(&id).unwrap().widgets[0].kind, WidgetKind::Bar);

        // …and a delete really removes the file, not just the map entry.
        let (status, _) = send(&app, "DELETE", &format!("/api/dashboards/{id}"), None).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(DashboardStore::with_dir(dir.clone()).is_empty());
        assert!(!dir.join(format!("{id}.json")).exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A corrupt file must cost exactly one dashboard, not the whole page.
    #[test]
    fn a_corrupt_document_is_skipped_not_fatal() {
        let dir = std::env::temp_dir().join(format!("oxidant-dash-bad-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("dgood.json"),
            serde_json::to_vec(&json!({
                "id": "dgood",
                "name": "Good",
                "layout": [],
                "widgets": [],
                "createdAtMs": 1,
                "updatedAtMs": 1,
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(dir.join("dbad.json"), b"{ not json").unwrap();
        // A document whose id disagrees with its filename would be unreachable after a
        // rename, so it is skipped too.
        std::fs::write(
            dir.join("dmismatch.json"),
            serde_json::to_vec(&json!({
                "id": "somethingelse",
                "name": "Mismatched",
                "layout": [],
                "widgets": [],
                "createdAtMs": 1,
                "updatedAtMs": 1,
            }))
            .unwrap(),
        )
        .unwrap();

        let store = DashboardStore::with_dir(dir.clone());
        assert_eq!(store.len(), 1);
        assert_eq!(store.get("dgood").unwrap().name, "Good");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An unwritable directory degrades to in-memory instead of taking the server down.
    #[test]
    fn an_unusable_directory_falls_back_to_memory() {
        // A path whose parent is a file can never be created as a directory.
        let file = std::env::temp_dir().join(format!("oxidant-dash-file-{}", uuid::Uuid::new_v4()));
        std::fs::write(&file, b"not a directory").unwrap();
        let store = DashboardStore::with_dir(file.join("dashboards"));
        assert!(store.is_empty());
        assert!(store.inner.dir.is_none(), "persistence must be disabled");
        let _ = std::fs::remove_file(&file);
    }
}
