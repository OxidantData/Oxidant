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

#[cfg(test)]
mod tests {
    use super::EMBEDDED_INDEX;

    /// The page is a single `include_str!` blob with no build step and no asset route, so the
    /// things that would silently break it are structural: a tab whose panel is missing, a
    /// theme token the CSS references but never declares, or an asset that has to be fetched.
    /// This is the cheap guard for all three.
    #[test]
    fn the_embedded_page_is_self_contained_and_carries_every_tab() {
        // Air-gap: a driver may have no egress, so nothing may be fetched from off-box.
        for offender in [
            "https://",
            "http://fonts",
            "cdn.",
            "unpkg",
            "jsdelivr",
            "googleapis",
        ] {
            assert!(
                !EMBEDDED_INDEX.contains(offender),
                "embedded page reaches off-box for `{offender}`"
            );
        }

        // Every nav button needs the panel it reveals.
        for tab in [
            "jobs",
            "stages",
            "sql",
            "pipelines",
            "editor",
            "notebook",
            "executors",
            "environment",
            "observability",
        ] {
            assert!(
                EMBEDDED_INDEX.contains(&format!("data-tab=\"{tab}\"")),
                "no nav button for {tab}"
            );
            assert!(
                EMBEDDED_INDEX.contains(&format!("id=\"panel-{tab}\"")),
                "no panel for {tab}"
            );
        }

        // The Pipelines page reads the connector logs — the pipeline *list* and one tail per
        // pipeline — plus `/api/status` as a cross-check. Streaming work never reaches the
        // execution store, so deriving this page from `/sql` would leave it permanently empty;
        // see `crate::pipelines`.
        assert!(EMBEDDED_INDEX.contains("fetch('/api/v1/pipelines'"));
        assert!(EMBEDDED_INDEX.contains("/api/v1/pipelines/' + encodeURIComponent(name)"));
        assert!(EMBEDDED_INDEX.contains("/api/status?limit="));
        assert!(EMBEDDED_INDEX.contains("/api/v1/events/stream"));

        // The Observability page reads these three, and the log buffer is the only one of them
        // that is not already on refresh().
        assert!(EMBEDDED_INDEX.contains("fetch('/api/v1/logs'"));
        assert!(EMBEDDED_INDEX.contains("api('/jobs')"));
        assert!(EMBEDDED_INDEX.contains("api('/stages?details=true')"));
        assert!(EMBEDDED_INDEX.contains("api('/sql')"));

        // The Compare page and the Spark proxy that existed only to feed it are gone; nothing
        // in the console reaches for another engine's UI.
        for gone in [
            "spark-proxy",
            "data-tab=\"compare\"",
            "panel-compare",
            "compare-grid",
        ] {
            assert!(
                !EMBEDDED_INDEX.contains(gone),
                "the removed Compare page left `{gone}` behind"
            );
        }

        // The platform component vocabulary, as classes the JS actually emits.
        for class in [
            ".chip",
            ".error-state",
            ".empty-state",
            ".metric",
            ".drawer",
            ".eyebrow",
            ".filter-chip",
            ".logwrap",
        ] {
            assert!(
                EMBEDDED_INDEX.contains(&format!("{class} "))
                    || EMBEDDED_INDEX.contains(&format!("{class} {{")),
                "component `{class}` is not styled"
            );
        }
    }

    /// Every `var(--oxidant-*)` the page uses must be declared by the page: there is no
    /// stylesheet behind this file to inherit a missing token from, and an undeclared one
    /// renders as *nothing* rather than as an error.
    #[test]
    fn every_theme_token_used_is_also_declared() {
        let declared: std::collections::HashSet<&str> = EMBEDDED_INDEX
            .match_indices("--oxidant-")
            .filter_map(|(i, _)| {
                let rest = &EMBEDDED_INDEX[i..];
                let name =
                    &rest[..rest.find(|c: char| !(c.is_ascii_alphanumeric() || c == '-'))?];
                // A declaration is `--oxidant-foo:`; a use is `var(--oxidant-foo)`.
                rest[name.len()..].starts_with(':').then_some(name)
            })
            .collect();

        let mut used = Vec::new();
        let mut rest = EMBEDDED_INDEX;
        while let Some(i) = rest.find("var(--oxidant-") {
            rest = &rest[i + 4..];
            let end = rest.find(')').expect("unterminated var()");
            used.push(rest[..end].trim());
        }
        assert!(!used.is_empty(), "the page stopped using theme tokens");
        for token in used {
            assert!(
                declared.contains(token),
                "`{token}` is used but never declared"
            );
        }
    }
}
