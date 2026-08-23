//! What answers a request that is not an API route: either the page compiled into the binary,
//! or a built React SPA on disk.
//!
//! The binary carries a single self-contained HTML page (`embedded_ui.html`) so `oxidant spark
//! server` needs no asset pipeline and no npm at runtime. That page can import nothing, which
//! is fine for the monitoring tables — and impossible for dashboards, which are a charting
//! library, a grid engine and a query cache.
//!
//! One exception to "no build step": the Pipelines page's derivation — the reducer that turns
//! a connector's JSONL log into what an operator is told about a running pipeline — lives in
//! `pipeline_derive.js` and is spliced into the page at [`DERIVE_MARKER`] the first time the
//! page is served. The served page is still one self-contained file that fetches nothing; the
//! split exists so that ~250 lines of decision-making can be evaluated by a test
//! (`ui/src/lib/pipelineDerive.test.ts`) instead of only being grepped for.
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
        return html_response(embedded_index());
    }
    // Asset requests fall back to index for SPA routing.
    if !path.starts_with("api/") {
        return html_response(embedded_index());
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

/// The page as it is written, still carrying [`DERIVE_MARKER`].
const EMBEDDED_TEMPLATE: &str = include_str!("embedded_ui.html");
/// The Pipelines page's derivation, spliced in where the marker is. It is *this page's* code,
/// not a library: it declares `__oxidantPipelines` and nothing else.
const PIPELINE_DERIVE_JS: &str = include_str!("pipeline_derive.js");
/// Where the derivation goes. A JS block comment, so the template is still valid JavaScript
/// on its own — an editor, a formatter or a browser opening the file directly all cope.
const DERIVE_MARKER: &str = "/*__PIPELINE_DERIVE_JS__*/";

/// The page actually served: the template with the derivation spliced in, assembled once.
///
/// `OnceLock` rather than a `const`: the splice is a runtime string operation. It happens on
/// the first page request and never again, and the result is what every test below asserts on
/// — a test that read the template instead could pass while the served page was broken.
fn embedded_index() -> &'static str {
    static PAGE: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    PAGE.get_or_init(|| EMBEDDED_TEMPLATE.replace(DERIVE_MARKER, PIPELINE_DERIVE_JS))
        .as_str()
}

#[cfg(test)]
mod tests {
    use super::{embedded_index, DERIVE_MARKER, EMBEDDED_TEMPLATE, PIPELINE_DERIVE_JS};

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
                !embedded_index().contains(offender),
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
                embedded_index().contains(&format!("data-tab=\"{tab}\"")),
                "no nav button for {tab}"
            );
            assert!(
                embedded_index().contains(&format!("id=\"panel-{tab}\"")),
                "no panel for {tab}"
            );
        }

        // The Pipelines page reads the connector logs — the pipeline *list* and one tail per
        // pipeline — plus `/api/status` as a cross-check. Streaming work never reaches the
        // execution store, so deriving this page from `/sql` would leave it permanently empty;
        // see `crate::pipelines`.
        assert!(embedded_index().contains("fetch('/api/v1/pipelines'"));
        assert!(embedded_index().contains("/api/v1/pipelines/' + encodeURIComponent(name)"));
        assert!(embedded_index().contains("/api/status?limit="));
        assert!(embedded_index().contains("/api/v1/events/stream"));

        // The Observability page reads these three, and the log buffer is the only one of them
        // that is not already on refresh().
        assert!(embedded_index().contains("fetch('/api/v1/logs'"));
        assert!(embedded_index().contains("api('/jobs')"));
        assert!(embedded_index().contains("api('/stages?details=true')"));
        assert!(embedded_index().contains("api('/sql')"));

        // The Compare page and the Spark proxy that existed only to feed it are gone; nothing
        // in the console reaches for another engine's UI.
        for gone in [
            "spark-proxy",
            "data-tab=\"compare\"",
            "panel-compare",
            "compare-grid",
        ] {
            assert!(
                !embedded_index().contains(gone),
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
                embedded_index().contains(&format!("{class} "))
                    || embedded_index().contains(&format!("{class} {{")),
                "component `{class}` is not styled"
            );
        }
    }

    /// Every `var(--oxidant-*)` the page uses must be declared by the page: there is no
    /// stylesheet behind this file to inherit a missing token from, and an undeclared one
    /// renders as *nothing* rather than as an error.
    #[test]
    fn every_theme_token_used_is_also_declared() {
        let page = embedded_index();
        let declared: std::collections::HashSet<&str> = page
            .match_indices("--oxidant-")
            .filter_map(|(i, _)| {
                let rest = &page[i..];
                let name =
                    &rest[..rest.find(|c: char| !(c.is_ascii_alphanumeric() || c == '-'))?];
                // A declaration is `--oxidant-foo:`; a use is `var(--oxidant-foo)`.
                rest[name.len()..].starts_with(':').then_some(name)
            })
            .collect();

        let mut used = Vec::new();
        let mut rest = page;
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

    /// The split has to be invisible to whoever loads the page: the marker is replaced, the
    /// derivation lands, and no copy of it survives inline in the template (which would drift).
    #[test]
    fn the_pipeline_derivation_is_spliced_into_the_served_page() {
        assert!(
            EMBEDDED_TEMPLATE.contains(DERIVE_MARKER),
            "the page lost its `{DERIVE_MARKER}` splice point; the derivation would never load"
        );
        let page = embedded_index();
        assert!(
            !page.contains(DERIVE_MARKER),
            "the marker survived the splice — the page is serving a comment, not the derivation"
        );
        // The derivation is *only* in the separate file: an inline copy would be the one the
        // browser runs and the one no test ever evaluates.
        assert!(
            !EMBEDDED_TEMPLATE.contains("function pipeFromLog"),
            "`pipeFromLog` is inline in the page again; it belongs in pipeline_derive.js"
        );
        for symbol in [
            "var __oxidantPipelines",
            "function pipeFromLog",
            "function statusQueryFor",
            "function shouldRefetchTail",
            "function capturePipeScroll",
            "function warningText",
        ] {
            assert!(
                page.contains(symbol),
                "the served page is missing `{symbol}`"
            );
        }
        // And the page still consumes it.
        assert!(page.contains("} = __oxidantPipelines;"));
    }

    /// The one drift this page cannot see: the connector writes an event kind the reducer does
    /// not read, and the drawer silently renders the kind's name instead of its content —
    /// which is exactly what `value_dropped` did.
    ///
    /// Reads the connector source from the workspace rather than `include_str!`ing it, so this
    /// crate still builds when it is packaged on its own; a missing sibling crate skips.
    #[test]
    fn every_connector_event_kind_is_read_by_the_page() {
        let connector = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../oxidant-streaming/src/postgres_cdc.rs");
        let Ok(source) = std::fs::read_to_string(&connector) else {
            eprintln!("skipping: {} is not in this checkout", connector.display());
            return;
        };

        // `self.log.event("slot_metrics", json!({ … }))` → `slot_metrics`.
        let mut kinds: Vec<&str> = source
            .match_indices(".event(")
            .filter_map(|(i, _)| {
                let rest = &source[i + ".event(".len()..];
                let rest = rest.trim_start();
                let rest = rest.strip_prefix('"')?;
                let end = rest.find('"')?;
                Some(&rest[..end])
            })
            .collect();
        // `ConnectorLog::error` is written by the log itself, not by a call site here.
        kinds.push("error");
        kinds.sort_unstable();
        kinds.dedup();
        assert!(kinds.len() > 5, "found no connector events: {kinds:?}");

        // Kinds the log pane renders verbatim and the reducer deliberately ignores: protocol
        // chatter with nothing an operator would act on. Anything else must be read.
        const RENDERED_VERBATIM: [&str; 3] = ["keepalive", "reply_requested", "standby_status"];

        for kind in kinds {
            if RENDERED_VERBATIM.contains(&kind) {
                assert!(
                    !PIPELINE_DERIVE_JS.contains(&format!("case '{kind}'")),
                    "`{kind}` is now read by the reducer; take it out of RENDERED_VERBATIM"
                );
                continue;
            }
            assert!(
                PIPELINE_DERIVE_JS.contains(&format!("case '{kind}'")),
                "the connector writes `{kind}` and pipeline_derive.js never reads it"
            );
        }

        // `value_dropped`'s content is in `column` / `reason` / `rows_in_this_batch`, and in
        // none of the generic keys the warning row used to read.
        for field in ["column", "reason", "rows_in_this_batch"] {
            assert!(
                PIPELINE_DERIVE_JS.contains(field),
                "a value_dropped warning would drop its `{field}`"
            );
        }
    }

    /// The behaviours whose *logic* is tested in `ui/src/lib/pipelineDerive.test.ts` still have
    /// to be wired up here, and this page has no build step to catch it if they are not. These
    /// are string greps, deliberately: they assert the page reaches for the tested code rather
    /// than re-implementing it inline.
    #[test]
    fn the_page_uses_the_tested_derivation_rather_than_its_own_copy() {
        let page = embedded_index();

        // A tail is due when `shouldRefetchTail` says so — not on an ad-hoc comparison that
        // re-stamped itself on completion and halved the refresh rate.
        assert!(page.contains("if (!shouldRefetchTail(cur, now, force)) return;"));
        assert!(
            !page.contains("now - (cur.at || 0) < PIPE_POLL_MS"),
            "the old freshness guard is back; tails would refresh every other tick"
        );

        // The drawer repaints wholesale, so it has to put the reader back where they were.
        assert!(page.contains("capturePipeScroll(host)"));
        assert!(page.contains("restorePipeScroll(host, scroll)"));
        for pane in ["data-scroll=\"body\"", "data-scroll=\"log\""] {
            assert!(page.contains(pane), "no scroll anchor for {pane}");
        }

        // The list's "Last batch" column is a label that moves, not a window-edge ordinal.
        assert!(page.contains("p.lastBatchLabel"));
        assert!(
            !page.contains("'#' + b.ordinal"),
            "the list is showing a tail ordinal again; it pins at #39 on any busy pipeline"
        );
    }

    /// One byte count, one unit, on every tab. `fmt` is `toLocaleString` — a raw count with
    /// thousands separators — so a byte field formatted with it reads `247,483,904` two clicks
    /// away from `236.0 MiB` for the same number.
    #[test]
    fn byte_counts_use_the_byte_formatter_everywhere() {
        let page = embedded_index();
        let mut checked = 0;
        let mut rest = page;
        while let Some(i) = rest.find("fmt(") {
            // Skip `fmtBytes(`, `fmtMs(`, `fmtRate(` … only bare `fmt(` is the raw count, and
            // only a call boundary counts (`${fmt(`, ` fmt(`, `(fmt(`).
            let before = rest[..i].chars().next_back().unwrap_or(' ');
            rest = &rest[i + "fmt(".len()..];
            if before.is_ascii_alphanumeric() {
                continue;
            }
            let end = rest.find(')').expect("unterminated fmt(");
            let arg = &rest[..end];
            assert!(
                !arg.contains("Bytes") && !arg.contains("Shuffle"),
                "`fmt({arg})` prints bytes as a raw count; use fmtBytes"
            );
            checked += 1;
        }
        assert!(checked > 3, "the page stopped using fmt() at all");

        // The executor's tone says whether it is *working*; its label says whether it is
        // registered. `active` maps to the one tone that pulses, and a registered-but-idle
        // worker pulsing forever reads as a cluster busy with nothing.
        assert!(page.contains("e.activeTasks > 0 ? 'running' : 'idle'"));
        assert!(!page.contains("chip(e.isActive === false ? 'dead' : 'active'"));
    }
}
