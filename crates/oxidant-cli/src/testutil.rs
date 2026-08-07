//! Test-only mock of the REST statement-execution API (`oxidant-connect::rest`), shared by
//! the `client` and `mcp` unit tests.
//!
//! Statement behavior is driven by the SQL text so tests stay deterministic without an engine:
//! - contains `FAIL` → immediately `failed` with error `mock execution failed`;
//! - contains `PENDING` → stays `running` forever (exercises polling, 409s, cancel);
//! - anything else → immediately `succeeded` with a one-row `hello = 1` result.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};

#[derive(Clone)]
struct MockStatement {
    sql: String,
    status: &'static str,
    error: Option<String>,
    seq: u64,
}

#[derive(Clone, Default)]
struct MockStore {
    inner: Arc<Mutex<(HashMap<String, MockStatement>, u64)>>,
}

impl MockStore {
    fn insert(&self, sql: &str) -> String {
        let mut inner = self.inner.lock().unwrap();
        let seq = inner.1;
        inner.1 += 1;
        let (status, error) = if sql.contains("FAIL") {
            ("failed", Some("mock execution failed".to_string()))
        } else if sql.contains("PENDING") {
            ("running", None)
        } else {
            ("succeeded", None)
        };
        let id = format!("mock-{seq}");
        inner.0.insert(
            id.clone(),
            MockStatement {
                sql: sql.to_string(),
                status,
                error,
                seq,
            },
        );
        id
    }

    fn snapshot_json(id: &str, st: &MockStatement) -> Value {
        let mut v = json!({
            "statementId": id,
            "sql": st.sql,
            "status": st.status,
            "submittedAtMs": 1_700_000_000_000i64,
            "durationMs": 3,
        });
        if let Some(error) = &st.error {
            v["error"] = json!(error);
        }
        if st.status == "succeeded" {
            v["rowCount"] = json!(1);
            v["schema"] = json!({ "fields": [{ "name": "hello", "type": "Int32" }] });
        }
        v
    }
}

fn not_found() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": "unknown statement id" })),
    )
        .into_response()
}

#[derive(serde::Deserialize)]
struct SubmitParams {
    wait: Option<bool>,
}

#[derive(serde::Deserialize)]
struct SubmitBody {
    sql: String,
}

#[derive(serde::Deserialize)]
struct ResultParams {
    format: Option<String>,
}

async fn submit(
    State(store): State<MockStore>,
    Query(params): Query<SubmitParams>,
    Json(body): Json<SubmitBody>,
) -> Response {
    let id = store.insert(&body.sql);
    let inner = store.inner.lock().unwrap();
    let st = &inner.0[&id];
    if params.wait.unwrap_or(false) {
        return Json(MockStore::snapshot_json(&id, st)).into_response();
    }
    (
        StatusCode::ACCEPTED,
        Json(json!({ "statementId": id, "status": "pending" })),
    )
        .into_response()
}

async fn get_one(State(store): State<MockStore>, Path(id): Path<String>) -> Response {
    let inner = store.inner.lock().unwrap();
    match inner.0.get(&id) {
        Some(st) => Json(MockStore::snapshot_json(&id, st)).into_response(),
        None => not_found(),
    }
}

async fn list(State(store): State<MockStore>) -> Response {
    let inner = store.inner.lock().unwrap();
    let mut items: Vec<(&String, &MockStatement)> = inner.0.iter().collect();
    items.sort_by(|a, b| b.1.seq.cmp(&a.1.seq));
    let statements: Vec<Value> = items
        .into_iter()
        .map(|(id, st)| MockStore::snapshot_json(id, st))
        .collect();
    Json(json!({ "statements": statements })).into_response()
}

async fn result(
    State(store): State<MockStore>,
    Path(id): Path<String>,
    Query(params): Query<ResultParams>,
) -> Response {
    let inner = store.inner.lock().unwrap();
    let Some(st) = inner.0.get(&id) else {
        return not_found();
    };
    if st.status != "succeeded" {
        return (
            StatusCode::CONFLICT,
            Json(json!({ "error": "statement result is only available once it has succeeded" })),
        )
            .into_response();
    }
    match params.format.as_deref().unwrap_or("json") {
        "csv" => ([(header::CONTENT_TYPE, "text/csv")], "hello\n1\n").into_response(),
        _ => Json(json!({
            "schema": { "fields": [{ "name": "hello", "type": "Int32" }] },
            "rows": [{ "hello": 1 }],
            "rowCount": 1,
            "truncated": false,
        }))
        .into_response(),
    }
}

async fn cancel(State(store): State<MockStore>, Path(id): Path<String>) -> Response {
    let mut inner = store.inner.lock().unwrap();
    match inner.0.get_mut(&id) {
        None => not_found(),
        Some(st) if st.status == "running" => {
            st.status = "canceled";
            Json(json!({ "statementId": id, "status": "canceled" })).into_response()
        }
        Some(_) => (
            StatusCode::CONFLICT,
            Json(json!({ "error": "statement is already terminal" })),
        )
            .into_response(),
    }
}

async fn cluster_status() -> Response {
    Json(json!({ "mode": "single-node", "workers": [], "version": "0.0.0" })).into_response()
}

fn mock_router() -> Router {
    Router::new()
        .route("/api/v1/statements", post(submit).get(list))
        .route("/api/v1/statements/{id}", get(get_one))
        .route("/api/v1/statements/{id}/result", get(result))
        .route("/api/v1/statements/{id}/cancel", post(cancel))
        .route("/api/v1/cluster/status", get(cluster_status))
        .with_state(MockStore::default())
}

/// Spawn the mock on an ephemeral loopback port; returns the base URL (`http://127.0.0.1:P`).
pub(crate) async fn spawn_mock() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock listener");
    let port = listener.local_addr().expect("local_addr").port();
    tokio::spawn(async move {
        axum::serve(listener, mock_router())
            .await
            .expect("mock serve");
    });
    format!("http://127.0.0.1:{port}")
}
