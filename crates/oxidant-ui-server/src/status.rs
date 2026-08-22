//! `GET /api/status` — authenticated driver status for a control plane.
//!
//! Unlike the rest of this server (the monitoring UI and the Spark-compatible `/api/v1`
//! routes, which are unauthenticated), this endpoint carries operational signals a control
//! plane acts on — auto-termination and autoscaling — so it is gated behind a shared bearer
//! token and is **off unless that token is configured**:
//!
//! | `OXIDANT_STATUS_TOKEN` | `Authorization` header | Response |
//! |------------------------|------------------------|----------|
//! | unset / empty          | anything               | `404 Not Found` (endpoint disabled) |
//! | set                    | missing or wrong       | `401 Unauthorized` + `WWW-Authenticate: Bearer` |
//! | set                    | `Bearer <token>`       | `200 OK` + [`StatusSnapshot`] JSON |
//!
//! Trust model: the token authenticates the *poller*, not the transport. The engine serves
//! plain HTTP, so a token on the wire is only as private as the network under it. Deploy the
//! driver's HTTP port inside a private subnet / security group reachable only by the control
//! plane, exactly as the monitoring UI already requires. See `docs/api.md`.

use axum::{
    extract::{Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use oxidant_observability::StatusSnapshot;
use serde::Deserialize;

use crate::routes::AppState;

/// Queries returned when the caller does not ask for a specific count.
const DEFAULT_QUERY_LIMIT: usize = 20;
/// Hard cap on `?limit=` — a status poll must stay cheap.
const MAX_QUERY_LIMIT: usize = 200;

#[derive(Debug, Deserialize)]
pub struct StatusParams {
    /// How many recent queries to include (default 20, capped at 200). The counters are
    /// unaffected — they always cover every query the store holds.
    pub limit: Option<usize>,
}

/// Resolve the shared status token from the process environment.
///
/// An empty or whitespace-only value is treated as **unset**: it would otherwise make
/// `Authorization: Bearer ` a valid credential.
pub fn status_token_from_env() -> Option<String> {
    normalize_token(std::env::var("OXIDANT_STATUS_TOKEN").ok())
}

/// Applied at the router boundary ([`crate::app_router_with_status_token`]) so *every*
/// caller gets the same "blank means disabled" rule, not just the env one.
pub(crate) fn normalize_token(raw: Option<String>) -> Option<String> {
    raw.map(|t| t.trim().to_string()).filter(|t| !t.is_empty())
}

pub async fn status(
    State(state): State<AppState>,
    Query(params): Query<StatusParams>,
    headers: HeaderMap,
) -> Response {
    // No token configured: the endpoint does not exist. 404 rather than 403 so an
    // unconfigured driver leaks nothing about whether the feature is there at all.
    let Some(expected) = state.status_token.as_deref() else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let presented = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(bearer_token);
    let authorized = presented
        .map(|t| constant_time_eq(t.as_bytes(), expected.as_bytes()))
        .unwrap_or(false);
    if !authorized {
        return (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer")],
        )
            .into_response();
    }

    let limit = params
        .limit
        .unwrap_or(DEFAULT_QUERY_LIMIT)
        .min(MAX_QUERY_LIMIT);
    let snapshot: StatusSnapshot = state.store.status_snapshot(limit);
    Json(snapshot).into_response()
}

/// Extract the credential from an `Authorization: Bearer <token>` header value. The scheme
/// is case-insensitive per RFC 7235; the token itself is not.
fn bearer_token(value: &str) -> Option<&str> {
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    (!token.is_empty()).then_some(token)
}

/// Compare two secrets without an early exit on the first differing byte. Like every
/// practical constant-time compare (including `subtle`'s own slice impl) this still reveals
/// the *length* of the expected token, which is not the secret.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    a.ct_eq(b).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::app_router_with_status_token;
    use axum::body::Body;
    use http_body_util::BodyExt;
    use oxidant_observability::{AppStateStore, QueryTracker, SharedStore};
    use std::sync::Arc;
    use tower::ServiceExt;

    const TOKEN: &str = "s3cret-status-token";

    fn store() -> SharedStore {
        Arc::new(AppStateStore::new())
    }

    async fn get(
        store: SharedStore,
        token: Option<&str>,
        auth: Option<&str>,
    ) -> (StatusCode, serde_json::Value) {
        let app = app_router_with_status_token(store, token.map(str::to_string));
        let mut req = axum::http::Request::builder().uri("/api/status");
        if let Some(auth) = auth {
            req = req.header(header::AUTHORIZATION, auth);
        }
        let resp = app.oneshot(req.body(Body::empty()).unwrap()).await.unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, json)
    }

    /// Default posture: no token configured, no endpoint. It must not fall through to the
    /// SPA fallback and answer 200 with index.html.
    #[tokio::test]
    async fn disabled_without_a_configured_token() {
        let (status, _) = get(store(), None, Some(&format!("Bearer {TOKEN}"))).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    /// An empty `OXIDANT_STATUS_TOKEN` must disable the endpoint, not authorize
    /// `Authorization: Bearer ` against an empty secret.
    #[tokio::test]
    async fn blank_token_is_treated_as_unset() {
        assert_eq!(normalize_token(Some("   ".into())), None);
        assert_eq!(normalize_token(Some(String::new())), None);
        assert_eq!(normalize_token(None), None);
        assert_eq!(
            normalize_token(Some(format!("  {TOKEN}  "))),
            Some(TOKEN.to_string())
        );

        let (status, _) = get(store(), Some("   "), Some("Bearer ")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn rejects_missing_wrong_and_malformed_credentials() {
        for auth in [
            None,
            Some("Bearer"),
            Some("Bearer "),
            Some("Bearer wrong-token"),
            Some(&format!("Basic {TOKEN}")[..]),
            // A prefix of the real token must not pass.
            Some(&format!("Bearer {}", &TOKEN[..5])[..]),
        ] {
            let (status, _) = get(store(), Some(TOKEN), auth).await;
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "credential {auth:?} should not authenticate"
            );
        }
    }

    #[tokio::test]
    async fn unauthorized_response_advertises_the_bearer_scheme() {
        let app = app_router_with_status_token(store(), Some(TOKEN.to_string()));
        let resp = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            resp.headers().get(header::WWW_AUTHENTICATE).unwrap(),
            "Bearer"
        );
    }

    /// The scheme is case-insensitive; the token is not.
    #[tokio::test]
    async fn accepts_a_case_insensitive_bearer_scheme() {
        let (status, _) = get(store(), Some(TOKEN), Some(&format!("bEaReR {TOKEN}"))).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn returns_the_documented_shape_with_a_valid_token() {
        let (status, body) = get(store(), Some(TOKEN), Some(&format!("Bearer {TOKEN}"))).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
        assert!(body["uptime_secs"].is_i64());
        assert!(body["last_query_at"].is_null());
        assert_eq!(body["active_queries"], 0);
        assert_eq!(body["queued_queries"], 0);
        assert_eq!(body["queries"].as_array().unwrap().len(), 0);
    }

    /// The endpoint reads the live query lifecycle, not a static snapshot taken at boot.
    #[tokio::test]
    async fn last_query_at_advances_after_a_query_runs() {
        let store = store();
        let auth = format!("Bearer {TOKEN}");

        let (_, before) = get(store.clone(), Some(TOKEN), Some(&auth)).await;
        assert!(before["last_query_at"].is_null());

        let mut tracker = QueryTracker::begin(store.clone(), "op-live", "SELECT 1");
        tracker.begin_local_stage("local", 1);

        let (_, during) = get(store.clone(), Some(TOKEN), Some(&auth)).await;
        assert_eq!(during["active_queries"], 1);
        let started = during["last_query_at"].as_str().unwrap().to_string();
        assert_eq!(during["queries"][0]["id"], "op-live");
        assert_eq!(during["queries"][0]["state"], "running");

        tracker.finish_success(3);

        let (_, after) = get(store.clone(), Some(TOKEN), Some(&auth)).await;
        assert_eq!(after["active_queries"], 0);
        assert_eq!(after["queries"][0]["state"], "finished");
        assert_eq!(after["queries"][0]["rows"], 3);
        let finished = after["last_query_at"].as_str().unwrap().to_string();
        assert!(
            finished >= started,
            "last_query_at regressed: {started} -> {finished}"
        );
    }

    /// `?limit=` bounds the per-query list; the counters must still see every query.
    #[tokio::test]
    async fn limit_is_honored_and_clamped() {
        let store = store();
        for i in 0..4 {
            QueryTracker::begin(store.clone(), format!("op-{i}"), "SELECT 1");
        }
        let app = app_router_with_status_token(store.clone(), Some(TOKEN.to_string()));
        let resp = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/status?limit=2")
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["queries"].as_array().unwrap().len(), 2);
        assert_eq!(body["active_queries"], 4);

        // An absurd limit is clamped, not honored, and does not error.
        let app = app_router_with_status_token(store, Some(TOKEN.to_string()));
        let resp = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/status?limit=100000")
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// `OXIDANT_STATUS_TOKEN` must actually reach the router built by [`crate::app_router`]
    /// — the wiring `serve()` depends on — and must not then leak out of the
    /// *unauthenticated* `/environment` endpoint, which dumps every `OXIDANT_*` var.
    #[tokio::test]
    // ENV_LOCK serializes this test's process-global env mutation against any sibling that
    // grows one; the guard must therefore span the awaits it protects.
    #[allow(clippy::await_holding_lock)]
    async fn env_token_enables_the_endpoint_without_leaking_through_environment() {
        static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var("OXIDANT_STATUS_TOKEN", TOKEN);

        let store = store();
        let resp = crate::app_router(store.clone())
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/status")
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let resp = crate::app_router(store)
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/v1/applications/app/environment")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        std::env::remove_var("OXIDANT_STATUS_TOKEN");
        assert!(
            !body.contains(TOKEN),
            "status token leaked through /environment: {body}"
        );
        assert!(body.contains("OXIDANT_STATUS_TOKEN"));
    }

    /// Enabling `/api/status` must not put a token in front of the monitoring UI.
    #[tokio::test]
    async fn does_not_authenticate_the_rest_of_the_server() {
        let app = app_router_with_status_token(store(), Some(TOKEN.to_string()));
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
    }
}
