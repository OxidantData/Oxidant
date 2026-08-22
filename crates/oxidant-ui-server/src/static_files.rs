//! What answers a request that is not an API route: either the page compiled into the binary,
//! or a built React SPA on disk.
//!
//! The binary carries a single self-contained HTML page (`embedded_ui.html`) so `oxidant spark
//! server` needs no asset pipeline and no npm at runtime. That page can import nothing, which
//! is fine for the monitoring tables — and impossible for dashboards, which are a charting
//! library, a grid engine and a query cache.
//!
//! So the richer app in `ui/` can be served instead: point `OXIDANT_UI_DIR` at its `npm run
//! build` output (`ui/dist`) and the server hands out those files, with unknown paths falling
//! through to `index.html` for client-side routing. Unset — the default — nothing changes and
//! the embedded page is served exactly as before.

use std::path::PathBuf;

use axum::{
    body::Body,
    http::{header, StatusCode, Uri},
    response::Response,
};

/// Directory of a built SPA (`ui/dist`) to serve in place of the embedded page.
pub const UI_DIR_ENV: &str = "OXIDANT_UI_DIR";

/// The configured SPA directory, if it is set and actually contains an `index.html`.
///
/// A path that does not resolve is a warning rather than an error: an operator who mistypes it
/// gets a working monitoring UI plus a log line, not a server that refuses to start.
pub fn spa_dir() -> Option<PathBuf> {
    let raw = std::env::var(UI_DIR_ENV).ok()?;
    let dir = PathBuf::from(raw.trim());
    if raw.trim().is_empty() {
        return None;
    }
    if !dir.join("index.html").is_file() {
        tracing::warn!(
            "{UI_DIR_ENV}={} has no index.html; serving the embedded UI instead",
            dir.display()
        );
        return None;
    }
    tracing::info!("serving the built UI from {}", dir.display());
    Some(dir)
}

/// Serve the embedded SPA fallback (works without a separate `ui` build).
pub async fn serve_static(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    if path.is_empty() || path == "index.html" {
        return html_response(EMBEDDED_INDEX);
    }
    // Asset requests fall back to index for SPA routing.
    if !path.starts_with("api/") {
        return html_response(EMBEDDED_INDEX);
    }
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::empty())
        .unwrap()
}

fn html_response(html: &str) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .body(Body::from(html.to_string()))
        .unwrap()
}

const EMBEDDED_INDEX: &str = include_str!("embedded_ui.html");
