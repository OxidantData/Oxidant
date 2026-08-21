//! Oxidant control-plane gateway: provision clusters and expose worker endpoints.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use oxidant_execution::autoscale::ParallelismDemand;
use oxidant_orchestrator::{
    autoscale::{recommend_for_cluster, scale_if_needed},
    backend::{ClusterBackend, ClusterInfo, K8sBackend, StaticBackend},
    spec::ClusterSpec,
};
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Deserialize)]
struct ScaleRequest {
    recommended_workers: u32,
    peak_task_demand: u32,
    shuffle_partitions: u32,
    #[serde(default)]
    reason: String,
}

#[derive(Debug, Serialize)]
struct ScaleResponse {
    scaled: bool,
    recommended_workers: u32,
    current_workers: u32,
    reason: String,
    worker_endpoints: Vec<String>,
}

// See oxidant-cli: generous thread stacks for deep SQL parser/optimizer recursion.
fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(32 * 1024 * 1024)
        .build()
        .expect("tokio runtime")
        .block_on(gateway_main())
}

async fn gateway_main() {
    let port: u16 = std::env::var("OXIDANT_GATEWAY_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8080);

    let backend: Arc<dyn ClusterBackend> =
        match std::env::var("OXIDANT_ORCHESTRATOR").ok().as_deref() {
            Some("k8s") => Arc::new(K8sBackend::default()),
            _ => Arc::new(StaticBackend::from_env().unwrap_or_else(|| StaticBackend::new(vec![]))),
        };

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/clusters", post(provision))
        .route("/clusters/{id}", delete(delete_cluster))
        .route("/clusters/{id}/workers", get(list_workers))
        .route("/clusters/{id}/scale", post(scale_cluster))
        .with_state(AppState { backend });

    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("bind gateway");
    eprintln!("oxidant-gateway listening on {addr}");
    axum::serve(listener, app).await.expect("serve");
}

async fn provision(
    State(state): State<AppState>,
    Json(req): Json<ProvisionRequest>,
) -> Json<ProvisionResponse> {
    let worker_image =
        std::env::var("OXIDANT_WORKER_IMAGE").unwrap_or_else(|_| "oxidant/worker:latest".into());
    let connect_image = std::env::var("OXIDANT_CLUSTER_IMAGE")
        .unwrap_or_else(|_| "oxidant/connect-server:latest".into());
    let worker_memory_limit_bytes = std::env::var("OXIDANT_WORKER_MEMORY_LIMIT_BYTES")
        .ok()
        .and_then(|s| s.parse().ok());
    let spec = ClusterSpec {
        cluster_id: req.cluster_id.clone(),
        namespace: format!("oxidant-cl-{}", req.cluster_id),
        worker_count: req.worker_count,
        worker_port: 50561,
        min_workers: req.worker_count,
        max_workers: req.worker_count.saturating_mul(4).max(req.worker_count),
        worker_image,
        connect_image,
        worker_memory_limit_bytes,
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

fn cluster_spec_for_id(id: &str) -> ClusterSpec {
    let worker_count = std::env::var("OXIDANT_WORKER_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);
    let mut spec = ClusterSpec::local_demo(id, worker_count);
    if let Some(min) = std::env::var("OXIDANT_WORKER_MIN")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        spec.min_workers = min;
    }
    if let Some(max) = std::env::var("OXIDANT_WORKER_MAX")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        spec.max_workers = max;
    }
    if let Some(bytes) = std::env::var("OXIDANT_WORKER_MEMORY_LIMIT_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        spec.worker_memory_limit_bytes = Some(bytes);
    }
    spec.cluster_id = id.to_string();
    spec.namespace = format!("oxidant-cl-{id}");
    spec
}

async fn scale_cluster(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<ScaleRequest>,
) -> Json<ScaleResponse> {
    let spec = cluster_spec_for_id(&id);
    let demand = ParallelismDemand {
        shuffle_partitions: req.shuffle_partitions.max(1),
        peak_stage_tasks: req.peak_task_demand.max(1),
        stages: vec![],
    };
    let mut rec = recommend_for_cluster(
        &spec,
        &demand,
        oxidant_execution::autoscale::task_slots_per_worker(),
    );
    if req.recommended_workers > rec.recommended_workers {
        rec.recommended_workers = req.recommended_workers.min(spec.max_workers);
        rec.should_scale = rec.recommended_workers > rec.current_workers;
    }
    if !req.reason.is_empty() {
        rec.reason = req.reason;
    }

    match scale_if_needed(state.backend.as_ref(), &spec, &rec) {
        Ok(Some(info)) => Json(ScaleResponse {
            scaled: true,
            recommended_workers: rec.recommended_workers,
            current_workers: info.worker_endpoints.len() as u32,
            reason: rec.reason,
            worker_endpoints: info.worker_endpoints,
        }),
        Ok(None) => Json(ScaleResponse {
            scaled: false,
            recommended_workers: rec.recommended_workers,
            current_workers: rec.current_workers,
            reason: rec.reason,
            worker_endpoints: state.backend.worker_endpoints(&spec).unwrap_or_default(),
        }),
        Err(e) => Json(ScaleResponse {
            scaled: false,
            recommended_workers: rec.recommended_workers,
            current_workers: rec.current_workers,
            reason: format!("scale failed: {e}"),
            worker_endpoints: vec![],
        }),
    }
}
