use axum::{
    body::Body,
    http::{header, StatusCode, Uri},
    response::Response,
};

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
