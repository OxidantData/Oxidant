//! Weft control-plane gateway: provision clusters and expose worker endpoints.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use weft_orchestrator::{
    backend::{ClusterBackend, ClusterInfo, K8sBackend, StaticBackend},
    spec::ClusterSpec,
};

#[derive(Clone)]
struct AppState {
    backend: Arc<dyn ClusterBackend>,
}

#[derive(Debug, Deserialize)]
struct ProvisionRequest {
    cluster_id: String,
    #[serde(default = "default_workers")]
    worker_count: u32,
}

fn default_workers() -> u32 {
    2
}

#[derive(Debug, Serialize)]
struct ProvisionResponse {
    cluster_id: String,
    connect_endpoint: String,
    worker_endpoints: Vec<String>,
}

#[tokio::main]
async fn main() {
    let port: u16 = std::env::var("WEFT_GATEWAY_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8080);

    let backend: Arc<dyn ClusterBackend> = match std::env::var("WEFT_ORCHESTRATOR").ok().as_deref()
    {
        Some("k8s") => Arc::new(K8sBackend::default()),
        _ => Arc::new(StaticBackend::from_env().unwrap_or_else(|| StaticBackend::new(vec![]))),
    };

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/clusters", post(provision))
        .route("/clusters/{id}", delete(delete_cluster))
        .route("/clusters/{id}/workers", get(list_workers))
        .with_state(AppState { backend });

    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("bind gateway");
    eprintln!("weft-gateway listening on {addr}");
    axum::serve(listener, app).await.expect("serve");
}

async fn provision(
    State(state): State<AppState>,
    Json(req): Json<ProvisionRequest>,
) -> Json<ProvisionResponse> {
    let worker_image =
        std::env::var("WEFT_WORKER_IMAGE").unwrap_or_else(|_| "weft/worker:latest".into());
    let connect_image =
        std::env::var("WEFT_CLUSTER_IMAGE").unwrap_or_else(|_| "weft/connect-server:latest".into());
    let spec = ClusterSpec {
        cluster_id: req.cluster_id.clone(),
        namespace: format!("weft-cl-{}", req.cluster_id),
        worker_count: req.worker_count,
        worker_port: 50561,
        min_workers: req.worker_count,
        max_workers: req.worker_count.saturating_mul(4).max(req.worker_count),
        worker_image,
        connect_image,
    };
    let info = state
        .backend
        .provision(&spec)
        .unwrap_or_else(|_e| ClusterInfo {
            cluster_id: req.cluster_id.clone(),
            connect_endpoint: "sc://127.0.0.1:50051".into(),
            worker_endpoints: vec![],
        });
    Json(ProvisionResponse {
        cluster_id: info.cluster_id,
        connect_endpoint: info.connect_endpoint,
        worker_endpoints: info.worker_endpoints,
    })
}

async fn delete_cluster(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Json<serde_json::Value> {
    let _ = state.backend.delete(&id);
    Json(serde_json::json!({ "deleted": id }))
}

async fn list_workers(State(state): State<AppState>, Path(id): Path<String>) -> Json<Vec<String>> {
    let spec = ClusterSpec::local_demo(&id, 2);
    let eps = state.backend.worker_endpoints(&spec).unwrap_or_default();
    Json(eps)
}
