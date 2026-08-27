//! `GET /api/v1/pipelines` and `/{name}/logs` against connector logs that live in a bucket.
//!
//! This is the half of the S3 checkpoint story the platform sees. The console reads the driver's
//! HTTP API, not the object store, so moving the logs into the bucket only works if the driver
//! reads them back out on the console's behalf — the *shape* of both responses has to be
//! identical to the on-disk case, or the Pipelines page goes blank on exactly the deployments
//! the move was for.
//!
//! Skipped unless `OXIDANT_MINIO_TEST=1`; see `crates/oxidant-streaming/tests/minio_checkpoints.rs`
//! for how to bring MinIO up locally.

use axum::body::Body;
use axum::http::{header, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use oxidant_observability::AppStateStore;
use oxidant_streaming::{checkpoint_store, Engine};
use oxidant_ui_server::{app_router_with_spa, DashboardStore};
use serde_json::Value;
use std::sync::Arc;
use tower::ServiceExt;

const BUCKET: &str = "oxidant-test";
const TOKEN: &str = "s3cret-status-token";

fn minio_enabled() -> bool {
    std::env::var("OXIDANT_MINIO_TEST")
        .ok()
        .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
}

fn use_minio() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let endpoint = std::env::var("OXIDANT_MINIO_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9000".into());
        std::env::set_var("AWS_ENDPOINT", endpoint);
        std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin123");
        std::env::set_var("AWS_REGION", "us-east-1");
        std::env::set_var("AWS_ALLOW_HTTP", "true");
    });
}

fn unique_root() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("a clock after 1970")
        .as_nanos();
    format!(
        "s3://{BUCKET}/oxidant-ui-test/logs-{}-{nanos}",
        std::process::id()
    )
}

async fn get(app: Router, uri: &str) -> (StatusCode, Value) {
    let request = axum::http::Request::builder()
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn the_driver_serves_connector_logs_that_live_in_the_bucket() {
    if !minio_enabled() {
        eprintln!("skipping: set OXIDANT_MINIO_TEST=1 (and start MinIO) to run this");
        return;
    }
    use_minio();
    let root = unique_root();
    let engine = Engine::new();
    let store = checkpoint_store(&engine, &root).expect("resolves");

    // Written the way a connector writes it: one JSON object per line, under `logs/`.
    let body = concat!(
        "{\"ts\":\"2026-08-26T10:00:00.000Z\",\"event\":\"snapshot_start\"}\n",
        "{\"ts\":\"2026-08-26T10:00:01.000Z\",\"event\":\"batch\",\"rows\":3}\n",
        "not json\n",
        "{\"ts\":\"2026-08-26T10:00:02.000Z\",\"event\":\"commit\",\"lsn\":\"0/16B3748\"}\n",
    );
    store
        .write("logs/orders_live.jsonl", body.as_bytes().to_vec())
        .await
        .expect("writes the log object");
    // A rotated generation is history, not a pipeline, and must not be listed.
    store
        .write(
            "logs/orders_live.jsonl.1",
            b"{\"event\":\"old\"}\n".to_vec(),
        )
        .await
        .expect("writes a rotated generation");

    let router = || {
        app_router_with_spa(
            Arc::new(AppStateStore::new()),
            Some(TOKEN.to_string()),
            DashboardStore::in_memory(),
            None,
            Some(root.clone()),
        )
    };

    let (status, listing) = get(router(), "/api/v1/pipelines").await;
    assert_eq!(status, StatusCode::OK, "got: {listing}");
    let pipelines = listing["pipelines"].as_array().expect("an array");
    assert_eq!(
        pipelines.len(),
        1,
        "the live log is a pipeline and the rotated generation is not: {listing}"
    );
    assert_eq!(pipelines[0]["name"], "orders_live");
    assert_eq!(pipelines[0]["sizeBytes"], body.len());
    assert!(
        pipelines[0]["modifiedMs"].as_u64().is_some(),
        "the object store reports a last-write time: {listing}"
    );

    let (status, tail) = get(router(), "/api/v1/pipelines/orders_live/logs?tail=2").await;
    assert_eq!(status, StatusCode::OK, "got: {tail}");
    let events = tail["events"].as_array().expect("an array");
    assert_eq!(events.len(), 1, "the window holds two lines, one parses");
    assert_eq!(events[0]["event"], "commit");
    assert_eq!(tail["malformed"], 1, "the unparseable line is counted");
    assert_eq!(tail["truncated"], false);

    // A name with no object behind it is the same 404 the on-disk route answers.
    let (status, _) = get(router(), "/api/v1/pipelines/not_a_pipeline/logs").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
