use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive},
        Sse,
    },
    routing::{get, post},
    Json, Router,
};
use futures::StreamExt;
use oxidant_observability::SharedStore;
use serde::Deserialize;
use serde_json::json;
use tokio_stream::wrappers::BroadcastStream;
use tower_http::services::{ServeDir, ServeFile};

use crate::{
    dashboards::{self, DashboardStore},
    lifecycle, pipelines, static_files, status,
};

#[derive(Clone)]
pub struct AppState {
    pub store: SharedStore,
    /// Shared bearer token guarding `GET /api/status` and both pipeline routes (the listing
    /// and the connector-log tail). `None` disables all three; nothing else on this server is
    /// authenticated. See [`crate::status`].
    pub status_token: Option<std::sync::Arc<str>>,
    /// Where connector logs are read from, resolved once from the pipeline checkpoint root
    /// ([`pipelines::CHECKPOINT_DIR_ENV`]) — a directory on the driver, or a prefix in the
    /// object store the pipeline checkpoints to. `None` — the default, and also an unresolvable
    /// root — makes both pipeline routes answer 404.
    pub logs: Option<pipelines::LogStore>,
}

#[derive(Debug, Deserialize)]
pub struct StatusQuery {
    pub status: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct StageQuery {
    pub status: Option<String>,
    pub details: Option<String>,
    #[serde(rename = "withSummaries")]
    #[allow(dead_code)]
    pub with_summaries: Option<String>,
}

/// Router with the status token taken from `OXIDANT_STATUS_TOKEN` and dashboards persisted
/// under `OXIDANT_DASHBOARD_DIR` (see [`crate::dashboards`]).
pub fn app_router(store: SharedStore) -> Router {
    app_router_with_status_token(store, status::status_token_from_env())
}

/// Router with an explicit status token — the env-free form, used by tests and by callers
/// that resolve the token themselves.
pub fn app_router_with_status_token(store: SharedStore, status_token: Option<String>) -> Router {
    app_router_with(store, status_token, DashboardStore::from_env())
}

/// Fully explicit form: nothing is read from the environment. Tests use this to point the
/// dashboard store at a temp directory (or at memory) instead of the user's home.
pub fn app_router_with(
    store: SharedStore,
    status_token: Option<String>,
    dashboard_store: DashboardStore,
) -> Router {
    app_router_with_spa(
        store,
        status_token,
        dashboard_store,
        static_files::spa_dir(),
        pipelines::checkpoint_root_from_env(),
    )
}

/// As [`app_router_with`], with the SPA directory and the pipeline checkpoint root passed in
/// rather than read from the environment — the form tests use to exercise both static-file
/// paths and the connector-log route.
///
/// `checkpoint_root` is a filesystem path or an object-store URL, and is resolved once here
/// rather than per request: resolving an `s3://` root builds a client, and doing that on every
/// poll of the Pipelines page would make the cheapest route on this server the most expensive.
pub fn app_router_with_spa(
    store: SharedStore,
    status_token: Option<String>,
    dashboard_store: DashboardStore,
    spa_dir: Option<std::path::PathBuf>,
    checkpoint_root: Option<String>,
) -> Router {
    let state = AppState {
        store,
        status_token: status::normalize_token(status_token).map(Into::into),
        logs: checkpoint_root
            .as_deref()
            .and_then(pipelines::LogStore::resolve),
    };
    let router = Router::new()
        .route("/api/status", get(status::status))
        .route("/api/v1/applications", get(list_applications))
        .route("/api/v1/applications/{app_id}", get(get_application))
        .route("/api/v1/applications/{app_id}/jobs", get(list_jobs))
        .route("/api/v1/applications/{app_id}/stages", get(list_stages))
        .route(
            "/api/v1/applications/{app_id}/stages/{stage_id}/{attempt_id}",
            get(get_stage),
        )
        .route("/api/v1/applications/{app_id}/sql", get(list_sql))
        .route(
            "/api/v1/applications/{app_id}/executors",
            get(list_executors),
        )
        .route(
            "/api/v1/applications/{app_id}/environment",
            get(list_environment),
        )
        .route("/api/v1/pipelines", get(pipelines::list_pipelines))
        .route(
            "/api/v1/pipelines/{name}/logs",
            get(pipelines::pipeline_logs),
        )
        .route(
            "/api/v1/pipelines/lifecycle",
            post(lifecycle::pipeline_lifecycle),
        )
        .route("/api/v1/events/stream", get(events_stream))
        .route("/health", get(|| async { "ok" }));

    // Either the built React app on disk, or the page compiled into the binary. `ServeDir`'s
    // own fallback sends unknown paths to index.html so client-side routes (`/dashboards/<id>`)
    // survive a hard refresh; it also rejects traversal out of the directory.
    let router = match spa_dir {
        Some(dir) => router
            .fallback_service(ServeDir::new(&dir).fallback(ServeFile::new(dir.join("index.html")))),
        None => router.fallback(static_files::serve_static),
    };

    router
        .with_state(state)
        // Dashboard CRUD carries its own state, so it is merged after `with_state`. It brings
        // no fallback of its own, leaving the fallback above in effect.
        .merge(dashboards::router(dashboard_store))
}

async fn list_applications(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(json!(state.store.list_applications()))
}

async fn get_application(
    State(state): State<AppState>,
    Path(_app_id): Path<String>,
) -> Json<serde_json::Value> {
    Json(json!(state.store.application_info()))
}

async fn list_jobs(
    State(state): State<AppState>,
    Path(_app_id): Path<String>,
    Query(q): Query<StatusQuery>,
) -> Json<serde_json::Value> {
    Json(json!(state.store.list_jobs(q.status.as_deref())))
}

async fn list_stages(
    State(state): State<AppState>,
    Path(_app_id): Path<String>,
    Query(q): Query<StageQuery>,
) -> Json<serde_json::Value> {
    let details = q.details.as_deref() == Some("true");
    Json(json!(state.store.list_stages(q.status.as_deref(), details)))
}

async fn get_stage(
    State(state): State<AppState>,
    Path((_app_id, stage_id, attempt_id)): Path<(String, i32, i32)>,
    Query(q): Query<StageQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let details = q.details.as_deref() == Some("true");
    state
        .store
        .get_stage(stage_id, attempt_id, details)
        .map(|s| Json(json!(s)))
        .ok_or(StatusCode::NOT_FOUND)
}

async fn list_sql(
    State(state): State<AppState>,
    Path(_app_id): Path<String>,
) -> Json<serde_json::Value> {
    Json(json!(state.store.list_sql()))
}

async fn list_executors(
    State(state): State<AppState>,
    Path(_app_id): Path<String>,
) -> Json<serde_json::Value> {
    Json(json!(state.store.list_executors()))
}

async fn list_environment(
    State(state): State<AppState>,
    Path(_app_id): Path<String>,
) -> Json<serde_json::Value> {
    let entries = state.store.list_environment();
    let map: std::collections::HashMap<String, String> =
        entries.into_iter().map(|e| (e.key, e.value)).collect();
    Json(json!({
        "runtime": { "javaVersion": "N/A (Rust/DataFusion)" },
        "sparkProperties": map,
    }))
}

async fn events_stream(
    State(state): State<AppState>,
) -> Sse<impl futures::Stream<Item = Result<Event, std::convert::Infallible>>> {
    let rx = state.store.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|result| async move {
        match result {
            Ok(event) => {
                let json = serde_json::to_string(&event).ok()?;
                Some(Ok(Event::default().data(json)))
            }
            Err(_) => None,
        }
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http_body_util::BodyExt;
    use oxidant_observability::AppStateStore;
    use std::sync::Arc;
    use tower::ServiceExt;

    #[tokio::test]
    async fn applications_endpoint_returns_app() {
        let store = Arc::new(AppStateStore::new());
        // Explicit form: this test must not create a dashboard directory in the user's home.
        let app = app_router_with(store, None, DashboardStore::in_memory());
        let resp = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/v1/applications")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let apps: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
        assert!(!apps.is_empty());
    }

    fn spa_router(dir: Option<std::path::PathBuf>) -> Router {
        let store = Arc::new(AppStateStore::new());
        app_router_with_spa(store, None, DashboardStore::in_memory(), dir, None)
    }

    async fn get_body(app: &Router, uri: &str) -> (StatusCode, String) {
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    /// Without `OXIDANT_UI_DIR`, nothing changes: the page compiled into the binary is served
    /// for `/` and for client-side routes alike.
    #[tokio::test]
    async fn without_a_spa_dir_the_embedded_page_is_served() {
        let app = spa_router(None);
        for uri in ["/", "/dashboards", "/dashboards/d1"] {
            let (status, body) = get_body(&app, uri).await;
            assert_eq!(status, StatusCode::OK, "{uri}");
            assert!(
                body.contains("<html"),
                "{uri} should serve the embedded page"
            );
        }
    }

    /// With one, the built app is served — and a deep client-side route still resolves to
    /// `index.html` so a hard refresh on `/dashboards/<id>` does not 404.
    #[tokio::test]
    async fn a_spa_dir_serves_the_built_app_and_survives_a_deep_refresh() {
        let dir = std::env::temp_dir().join(format!("oxidant-spa-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(dir.join("assets")).unwrap();
        std::fs::write(
            dir.join("index.html"),
            "<!doctype html><title>built</title>",
        )
        .unwrap();
        std::fs::write(dir.join("assets/app.js"), "console.log(1)").unwrap();

        let app = spa_router(Some(dir.clone()));
        let (status, body) = get_body(&app, "/").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("built"), "{body}");

        let (status, body) = get_body(&app, "/assets/app.js").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "console.log(1)");

        let (status, body) = get_body(&app, "/dashboards/d1").await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            body.contains("built"),
            "deep route must fall back to index.html"
        );

        // The API keeps precedence over the file server.
        let (status, body) = get_body(&app, "/api/dashboards").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("dashboards"), "{body}");

        // …and the file server cannot be walked out of. A traversal is not an error page:
        // `ServeDir` refuses it, and the SPA fallback answers with index.html — so the check
        // that matters is what came back, not the status.
        for uri in [
            "/../../etc/passwd",
            "/%2e%2e%2f%2e%2e%2fetc%2fpasswd",
            "/assets/../../../etc/passwd",
        ] {
            let (_, body) = get_body(&app, uri).await;
            assert!(!body.contains("root:"), "{uri} escaped the SPA directory");
            assert!(
                body.contains("built"),
                "{uri} should fall back to index.html"
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A mistyped path must not take the UI down — it degrades to the embedded page.
    ///
    /// Uses the crate-wide [`crate::ENV_LOCK`]: this mutates `OXIDANT_UI_DIR`, process-wide
    /// environment that `lifecycle.rs` and `pipelines.rs` tests also touch under different var
    /// names — one shared mutex is what actually serializes `std::env::set_var`/`var` across
    /// all of them; a module-private one only excludes tests in this same file.
    #[test]
    fn a_spa_dir_without_an_index_is_ignored() {
        let _guard = crate::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("oxidant-spa-empty-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var(static_files::UI_DIR_ENV, &dir);
        assert!(static_files::spa_dir().is_none());
        std::env::remove_var(static_files::UI_DIR_ENV);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
