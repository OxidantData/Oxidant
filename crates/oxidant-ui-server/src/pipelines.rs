//! `GET /api/v1/pipelines/{name}/logs?tail=N` — the tail of one connector's JSONL log.
//!
//! Streaming connectors (`postgres_cdc` and friends) append one JSON object per line to
//! `<checkpoints>/logs/<name>.jsonl`, next to the offset/commit logs they already keep. The
//! Pipelines page in the monitoring UI shows the last few of those events in a pipeline's
//! detail drawer, which is the only place a connector's own words — "slot created",
//! "replication stream lost", "snapshot complete" — are visible without shelling into the box.
//!
//! ## Where the file comes from
//!
//! [`CHECKPOINT_DIR_ENV`] names the pipeline checkpoint root — the same directory
//! `pipeline.checkpoints` points at in `oxidant.yaml`. Logs are one subdirectory below it.
//! Nothing here writes: this endpoint only reads what a connector left behind.
//!
//! ## When it answers 404
//!
//! Every absence answers 404, deliberately — a caller learns "there is nothing here"
//! and nothing more:
//!
//! | Situation | Response |
//! |---|---|
//! | `OXIDANT_STATUS_TOKEN` unset | `404` — like [`crate::status`], the route does not exist |
//! | `OXIDANT_CHECKPOINT_DIR` unset | `404` |
//! | `<checkpoints>/logs` does not exist | `404` |
//! | no `<name>.jsonl` in it | `404` |
//!
//! The UI reads that 404 as "this build does not serve connector logs" and hides the section,
//! so the page is correct on a driver whose connectors write no log at all.
//!
//! ## Auth
//!
//! Guarded by exactly the same bearer token as `/api/status` ([`crate::status::denied`]) —
//! a connector log names slots, tables and hosts, so it is operational data, not monitoring
//! decoration.

use std::path::{Path, PathBuf};

use axum::{
    extract::{Path as UrlPath, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{routes::AppState, status};

/// Env var naming the pipeline checkpoint root. Set it to the same absolute path as
/// `pipeline.checkpoints` in `oxidant.yaml`; unset, this endpoint is absent.
pub const CHECKPOINT_DIR_ENV: &str = "OXIDANT_CHECKPOINT_DIR";

/// Connector logs live in one subdirectory of the checkpoint root.
const LOGS_SUBDIR: &str = "logs";
/// Log file extension, one JSON object per line.
const LOG_EXT: &str = "jsonl";

/// Events returned when the caller does not ask for a count.
const DEFAULT_TAIL: usize = 100;
/// Hard cap on `?tail=` — this is a drawer, not a log shipper.
const MAX_TAIL: usize = 1_000;
/// Bytes scanned back from the end of the file. A connector log grows without bound; the tail
/// must cost the same whether the file is 2 KiB or 2 GiB, so the read is a bounded window on
/// the end rather than a parse of the whole thing.
const MAX_TAIL_BYTES: u64 = 1 << 20;
/// Longest accepted pipeline name. Names address a file, so this is also a bound on how much
/// of a path a caller gets to choose.
const MAX_NAME_CHARS: usize = 128;

/// The configured checkpoint root, if set to something non-blank.
///
/// Existence is **not** checked here: the logs directory appears when the first connector
/// starts, which is usually after this router was built. Resolution happens per request.
pub fn checkpoint_dir_from_env() -> Option<PathBuf> {
    let raw = std::env::var(CHECKPOINT_DIR_ENV).ok()?;
    let trimmed = raw.trim();
    (!trimmed.is_empty()).then(|| PathBuf::from(trimmed))
}

#[derive(Debug, Deserialize)]
pub struct LogsParams {
    /// How many trailing events to return (default 100, capped at 1000).
    pub tail: Option<usize>,
}

/// `GET /api/v1/pipelines/{name}/logs?tail=N`.
pub async fn pipeline_logs(
    State(state): State<AppState>,
    UrlPath(name): UrlPath<String>,
    Query(params): Query<LogsParams>,
    headers: HeaderMap,
) -> Response {
    // Authenticate before anything else, so an unauthorized caller cannot use the shape of the
    // reply (400 vs 404) to probe for names on disk.
    if let Some(denied) = status::denied(&state, &headers) {
        return denied;
    }

    // A name addresses a file. Anything that is not a plain filename is a client bug — say so,
    // rather than quietly resolving it somewhere surprising.
    if !is_plain_name(&name) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "pipeline name must be a plain [A-Za-z0-9._-] filename" })),
        )
            .into_response();
    }

    let Some(root) = state.checkpoint_dir.as_deref() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let logs_dir = root.join(LOGS_SUBDIR);
    if !logs_dir.is_dir() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let path = logs_dir.join(format!("{name}.{LOG_EXT}"));
    if !path.is_file() {
        return StatusCode::NOT_FOUND.into_response();
    }

    let tail = params.tail.unwrap_or(DEFAULT_TAIL).clamp(1, MAX_TAIL);
    let read = tokio::task::spawn_blocking(move || read_tail(&path, tail)).await;
    let (lines, truncated) = match read {
        Ok(Ok(v)) => v,
        // The file was there a moment ago and is not readable now (rotated, or permissions):
        // that is the same "nothing to show" the UI already handles.
        Ok(Err(_)) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    // A half-written last line is normal for a log being appended to right now. Count those
    // rather than dropping them silently or failing the whole request.
    let mut events = Vec::with_capacity(lines.len());
    let mut malformed = 0usize;
    for line in lines {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(line) {
            Ok(value) => events.push(value),
            Err(_) => malformed += 1,
        }
    }

    Json(json!({
        "name": name,
        "tail": tail,
        "events": events,
        "malformed": malformed,
        "truncated": truncated,
    }))
    .into_response()
}

/// Whether `name` is a plain filename component — no separators, no `..`, no dotfile.
///
/// Axum percent-decodes path parameters before they reach here, so `%2e%2e%2f` arrives as
/// `../` and is rejected by the character whitelist rather than by a string comparison.
fn is_plain_name(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('.')
        && name.chars().count() <= MAX_NAME_CHARS
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

/// Last `tail` lines of `path`, plus whether the scan window started mid-file.
fn read_tail(path: &Path, tail: usize) -> std::io::Result<(Vec<String>, bool)> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    let start = len.saturating_sub(MAX_TAIL_BYTES);
    let truncated = start > 0;
    file.seek(SeekFrom::Start(start))?;
    let mut buf = Vec::with_capacity((len - start).min(MAX_TAIL_BYTES) as usize);
    file.read_to_end(&mut buf)?;

    let text = String::from_utf8_lossy(&buf);
    let mut lines: Vec<&str> = text.lines().collect();
    // A window that starts mid-file starts mid-line; that fragment is not a record.
    if truncated && !lines.is_empty() {
        lines.remove(0);
    }
    let from = lines.len().saturating_sub(tail);
    Ok((
        lines[from..].iter().map(|l| (*l).to_string()).collect(),
        truncated,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dashboards::DashboardStore;
    use crate::routes::app_router_with_spa;
    use axum::body::Body;
    use axum::http::header;
    use axum::Router;
    use http_body_util::BodyExt;
    use oxidant_observability::{AppStateStore, SharedStore};
    use std::sync::Arc;
    use tower::ServiceExt;

    const TOKEN: &str = "s3cret-status-token";

    fn store() -> SharedStore {
        Arc::new(AppStateStore::new())
    }

    /// A temp checkpoint root with `logs/<name>.jsonl` written from `lines`.
    fn checkpoint_root(name: &str, lines: &[&str]) -> PathBuf {
        let root = std::env::temp_dir().join(format!("oxidant-ckpt-{}", uuid::Uuid::new_v4()));
        let logs = root.join(LOGS_SUBDIR);
        std::fs::create_dir_all(&logs).unwrap();
        let mut body = lines.join("\n");
        body.push('\n');
        std::fs::write(logs.join(format!("{name}.{LOG_EXT}")), body).unwrap();
        root
    }

    fn router(token: Option<&str>, checkpoint_dir: Option<PathBuf>) -> Router {
        app_router_with_spa(
            store(),
            token.map(str::to_string),
            DashboardStore::in_memory(),
            None,
            checkpoint_dir,
        )
    }

    async fn get(app: Router, uri: &str, auth: Option<&str>) -> (StatusCode, Value) {
        let mut req = axum::http::Request::builder().uri(uri);
        if let Some(auth) = auth {
            req = req.header(header::AUTHORIZATION, auth);
        }
        let resp = app.oneshot(req.body(Body::empty()).unwrap()).await.unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    fn bearer() -> String {
        format!("Bearer {TOKEN}")
    }

    /// Same default posture as `/api/status`: no token configured, no endpoint — even when the
    /// log file is right there on disk.
    #[tokio::test]
    async fn disabled_without_a_configured_token() {
        let root = checkpoint_root("orders", &[r#"{"msg":"hi"}"#]);
        let (status, _) = get(
            router(None, Some(root.clone())),
            "/api/v1/pipelines/orders/logs",
            Some(&bearer()),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn rejects_missing_and_wrong_credentials() {
        let root = checkpoint_root("orders", &[r#"{"msg":"hi"}"#]);
        for auth in [None, Some("Bearer wrong"), Some("Basic x")] {
            let resp = router(Some(TOKEN), Some(root.clone()))
                .oneshot({
                    let mut req =
                        axum::http::Request::builder().uri("/api/v1/pipelines/orders/logs");
                    if let Some(auth) = auth {
                        req = req.header(header::AUTHORIZATION, auth);
                    }
                    req.body(Body::empty()).unwrap()
                })
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{auth:?}");
            assert_eq!(
                resp.headers().get(header::WWW_AUTHENTICATE).unwrap(),
                "Bearer"
            );
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    /// No checkpoint root, no logs directory, and no file for this name are all the same
    /// answer: the UI hides the section on a 404 and must not have to tell them apart.
    #[tokio::test]
    async fn absent_when_there_is_nothing_to_serve() {
        let (status, _) = get(
            router(Some(TOKEN), None),
            "/api/v1/pipelines/orders/logs",
            Some(&bearer()),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "no checkpoint root");

        let bare = std::env::temp_dir().join(format!("oxidant-ckpt-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&bare).unwrap();
        let (status, _) = get(
            router(Some(TOKEN), Some(bare.clone())),
            "/api/v1/pipelines/orders/logs",
            Some(&bearer()),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "no logs dir");
        let _ = std::fs::remove_dir_all(&bare);

        let root = checkpoint_root("orders", &[r#"{"msg":"hi"}"#]);
        let (status, _) = get(
            router(Some(TOKEN), Some(root.clone())),
            "/api/v1/pipelines/other/logs",
            Some(&bearer()),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "no file for this name");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn returns_the_tail_newest_last_and_counts_malformed_lines() {
        let root = checkpoint_root(
            "orders",
            &[
                r#"{"ts":"1","event":"slot_created"}"#,
                r#"{"ts":"2","event":"snapshot_done"}"#,
                "{not json",
                r#"{"ts":"3","event":"stream_started"}"#,
            ],
        );
        let (status, body) = get(
            router(Some(TOKEN), Some(root.clone())),
            "/api/v1/pipelines/orders/logs",
            Some(&bearer()),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["name"], "orders");
        assert_eq!(body["malformed"], 1);
        assert_eq!(body["truncated"], false);
        let events = body["events"].as_array().unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0]["event"], "slot_created");
        assert_eq!(events[2]["event"], "stream_started");

        // `?tail=` keeps the *newest* records, and is clamped rather than honored blindly.
        let (_, body) = get(
            router(Some(TOKEN), Some(root.clone())),
            "/api/v1/pipelines/orders/logs?tail=1",
            Some(&bearer()),
        )
        .await;
        assert_eq!(body["events"].as_array().unwrap().len(), 1);
        assert_eq!(body["events"][0]["event"], "stream_started");

        let (status, body) = get(
            router(Some(TOKEN), Some(root.clone())),
            "/api/v1/pipelines/orders/logs?tail=999999",
            Some(&bearer()),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["tail"], MAX_TAIL as u64);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// The name chooses a filename, so it must not be able to choose a *path*.
    #[tokio::test]
    async fn a_name_cannot_walk_out_of_the_logs_directory() {
        let root = checkpoint_root("orders", &[r#"{"msg":"hi"}"#]);
        std::fs::write(root.join("secret.jsonl"), r#"{"msg":"secret"}"#).unwrap();

        for name in [
            "..%2fsecret",
            "%2e%2e%2fsecret",
            "sub%2forders",
            ".hidden",
            "..",
        ] {
            let (status, body) = get(
                router(Some(TOKEN), Some(root.clone())),
                &format!("/api/v1/pipelines/{name}/logs"),
                Some(&bearer()),
            )
            .await;
            assert!(
                status == StatusCode::BAD_REQUEST || status == StatusCode::NOT_FOUND,
                "{name} answered {status}"
            );
            assert!(
                !body.to_string().contains("secret"),
                "{name} escaped the logs directory"
            );
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A log larger than the scan window costs one bounded read, still ends at the newest
    /// record, and says it only saw the end of the file.
    #[tokio::test]
    async fn a_huge_log_is_read_as_a_bounded_window_on_its_end() {
        let root = std::env::temp_dir().join(format!("oxidant-ckpt-{}", uuid::Uuid::new_v4()));
        let logs = root.join(LOGS_SUBDIR);
        std::fs::create_dir_all(&logs).unwrap();
        let filler = "x".repeat(4_000);
        let mut body = String::new();
        for i in 0..400 {
            body.push_str(&format!("{{\"i\":{i},\"pad\":\"{filler}\"}}\n"));
        }
        assert!(
            body.len() as u64 > MAX_TAIL_BYTES,
            "fixture must exceed the window"
        );
        std::fs::write(logs.join("big.jsonl"), body).unwrap();

        let (status, out) = get(
            router(Some(TOKEN), Some(root.clone())),
            "/api/v1/pipelines/big/logs?tail=5",
            Some(&bearer()),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(out["truncated"], true);
        assert_eq!(
            out["malformed"], 0,
            "the mid-line fragment must be dropped, not counted"
        );
        let events = out["events"].as_array().unwrap();
        assert_eq!(events.len(), 5);
        assert_eq!(events[4]["i"], 399);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn plain_names_only() {
        for ok in ["orders", "orders_live", "pg-cdc.v2", "a", "A1_2-3.4"] {
            assert!(is_plain_name(ok), "{ok} should be accepted");
        }
        for bad in [
            "",
            ".",
            "..",
            ".hidden",
            "a/b",
            "a\\b",
            "a b",
            "a\0b",
            &"n".repeat(129),
        ] {
            assert!(!is_plain_name(bad), "{bad:?} should be rejected");
        }
    }

    /// A blank env var is unset — otherwise the endpoint would resolve logs against the
    /// process working directory.
    #[test]
    fn blank_checkpoint_dir_is_treated_as_unset() {
        static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var(CHECKPOINT_DIR_ENV, "   ");
        assert_eq!(checkpoint_dir_from_env(), None);
        std::env::set_var(CHECKPOINT_DIR_ENV, " /srv/ckpt ");
        assert_eq!(checkpoint_dir_from_env(), Some(PathBuf::from("/srv/ckpt")));
        std::env::remove_var(CHECKPOINT_DIR_ENV);
        assert_eq!(checkpoint_dir_from_env(), None);
    }
}
