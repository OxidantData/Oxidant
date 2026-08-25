//! What answers a request that is not an API route: either the page compiled into the binary,
//! or a built React SPA on disk.
//!
//! The binary carries a single self-contained HTML page (`embedded_ui.html`) so `oxidant spark
//! server` needs no asset pipeline and no npm at runtime. That page can import nothing, which
//! is fine for the monitoring tables — and impossible for dashboards, which are a charting
//! library, a grid engine and a query cache.
//!
//! Two exceptions to "no build step", on the same seam. The Pipelines page's derivation — the
//! reducer that turns a connector's JSONL log into what an operator is told about a running
//! pipeline — lives in `pipeline_derive.js`, and the catalog rail's tree logic — which rows a
//! lazily-loaded catalog tree shows once a filter narrows it, and what a click on one inserts —
//! lives in `catalog_rail.js`. Both are spliced into the page at their markers
//! ([`DERIVE_MARKER`], [`CATALOG_MARKER`]) the first time the page is served. The served page is
//! still one self-contained file that fetches nothing; the split exists so that decision-making
//! can be evaluated by a test (`ui/src/lib/pipelineDerive.test.ts`,
//! `ui/src/lib/catalogRail.test.ts`) instead of only being grepped for.
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
/// The catalog rail's tree logic, on the same terms: it declares `__oxidantCatalog` and
/// nothing else, and the Editor and Notebook panels are its only callers.
const CATALOG_RAIL_JS: &str = include_str!("catalog_rail.js");
/// Where the rail's logic goes.
const CATALOG_MARKER: &str = "/*__CATALOG_RAIL_JS__*/";

/// The page actually served: the template with the derivation spliced in, assembled once.
///
/// `OnceLock` rather than a `const`: the splice is a runtime string operation. It happens on
/// the first page request and never again, and the result is what every test below asserts on
/// — a test that read the template instead could pass while the served page was broken.
fn embedded_index() -> &'static str {
    static PAGE: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    PAGE.get_or_init(|| {
        EMBEDDED_TEMPLATE
            .replace(DERIVE_MARKER, PIPELINE_DERIVE_JS)
            .replace(CATALOG_MARKER, CATALOG_RAIL_JS)
    })
    .as_str()
}

#[cfg(test)]
mod tests {
    use super::{
        embedded_index, CATALOG_MARKER, CATALOG_RAIL_JS, DERIVE_MARKER, EMBEDDED_TEMPLATE,
        PIPELINE_DERIVE_JS,
    };

    /// The source of one function in a spliced JS module: from its opening line to the first
    /// `}` at the module's own two-space indentation. Deliberately crude — it is a grep with a
    /// scope, so that "the rule is in `needsQuoting`" is a different assertion from "the rule
    /// appears somewhere in the file". It fails loudly (an empty body, which every caller
    /// asserts on) rather than quietly matching the wrong span.
    fn js_fn_body(source: &str, header: &str) -> String {
        source
            .split_once(header)
            .and_then(|(_, rest)| rest.split_once("\n  }"))
            .map(|(body, _)| body.to_string())
            .unwrap_or_default()
    }

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

        // The Observability page's jobs/stages/SQL sections ride refresh(); its log pane is the
        // one part with routes of its own — the §6b browser, in full.
        assert!(embedded_index().contains("api('/jobs')"));
        assert!(embedded_index().contains("api('/stages?details=true')"));
        assert!(embedded_index().contains("api('/sql')"));
        for route in [
            "'/api/v1/logs?'",        // the filtered, cursor-paged read
            "'/api/v1/logs/files?'",  // the file picker
            "'/api/v1/logs/workers'", // the worker picker
            "'/api/v1/logs/tail?'",   // tail-follow
            "'/api/v1/logs/dump'",    // the diagnostic bundle
        ] {
            assert!(
                embedded_index().contains(route),
                "the log pane must reach {route}"
            );
        }
        // **The tail carries the bearer header.** `EventSource` cannot, and the only ways around
        // that put the status token in a URL — proxy logs, history, `Referer` — or invent a
        // cookie on a router served under permissive CORS. Pinned, because "use EventSource, it
        // is simpler" is the obvious change to make here later.
        assert!(
            !embedded_index().contains("new EventSource('/api/v1/logs/tail"),
            "the log tail must not be an EventSource: it cannot carry the status token"
        );
        // Pinned as the *identifier* next to the route, not as the whole argument list: the
        // point is that the tail sends the same bearer header every other log route sends, and
        // an assertion on the exact spelling of an argument list breaks on a reformat that
        // changes nothing.
        let tail_call = embedded_index()
            .split_once("'/api/v1/logs/tail?'")
            .map(|(_, rest)| rest.chars().take(200).collect::<String>())
            .unwrap_or_default();
        assert!(
            tail_call.contains("obsAuthHeaders()"),
            "the log tail reads SSE by hand so it can send the same bearer header as every \
             other log route: {tail_call}"
        );

        // **The followed page's scroll-back cursor is never left stale.** `obsAppend` used to
        // trim the oldest lines off the front of the page with `.slice(-OBS_PAGE * 2)` and leave
        // `page.next_before` naming one of them, so `Load older lines` fetched the page before a
        // line that was no longer on screen and the gap was presented as continuous log. The
        // trimmed prefix becomes an `older` page instead.
        assert!(
            !embedded_index().contains(".slice(-OBS_PAGE * 2)"),
            "trimming the live page must not silently orphan its `next_before`"
        );
        assert!(
            embedded_index().contains("obsTrimScrollback"),
            "the pane must release scroll-back a whole page at a time, from the oldest end"
        );

        // **The memory ring is read whole, never paged.** Every other `next_before` is a row
        // index into an append-only file and names the same line forever; the ring's is an index
        // into a buffer that *rolls*, so every line the node logs between two requests shifts it
        // by one and a `before=` walk repeats lines at one end and loses them at the other. The
        // ring is also the fallback view on an `OXIDANT_LOG_ROLL=off` node — precisely where it
        // is the only view there is. One page holds 10,000 lines and the ring holds 1,000, so
        // the pane asks for all of it and suppresses the button rather than paging a cursor the
        // API itself labels `"cursor": "best-effort"`.
        assert!(
            embedded_index().contains("const OBS_RING_LINES = 1000"),
            "the pane must ask for the whole ring in one page"
        );
        assert!(
            embedded_index().contains("const more = !ring &&"),
            "`Load older lines` must not be offered over a cursor that rolls under the reader"
        );
        assert!(
            embedded_index().contains("rolls as this node logs; pick a file for a stable history"),
            "and the caption must say so, since the ring is the only view on a roll=off node"
        );
        // **Follow is offered only where there is something to follow.** The pane used to
        // enable it for the ring on every node, and the server papered over the incoherent half
        // by rewriting `file` to `current` — so a worker's "memory ring" painted a page from the
        // ring and appended a tail from `oxidant.log`. The server now answers `400`; this is the
        // switch that keeps the pane from asking, and the one place that decides it.
        assert!(
            embedded_index().contains("function obsCanFollow()"),
            "the pane must have one owner for whether the selection is followable"
        );
        assert!(
            !embedded_index().contains("obsState.file !== 'current' && obsState.file !== 'ring'"),
            "the ring is followable on the driver and not on a worker, so `file` alone cannot \
             decide it"
        );
        assert!(
            embedded_index()
                .contains("return obsState.file === 'ring' && obsState.worker === 'driver'"),
            "the driver's ring is its `tracing` stream; a worker's has no cursor a poll resumes \
             from"
        );

        // §6b's controls, as the ids the JS binds. The level chips existed and matched nothing
        // once PR3 put a timestamp in front of `[LEVEL]`; these are the rest of the pane.
        for control in [
            "id=\"obs-worker\"",
            "id=\"obs-file\"",
            "id=\"obs-target\"",
            "id=\"obs-q\"",
            "id=\"obs-from\"",
            "id=\"obs-to\"",
            "id=\"obs-follow\"",
            "id=\"obs-dump\"",
            "id=\"obs-filters\"",
        ] {
            assert!(
                embedded_index().contains(control),
                "the log pane is missing {control}"
            );
        }
        // **The chip row covers every level the API ranks.** It stopped at `debug` while
        // `level_rank` gives `TRACE` a rank of its own, so on a node running `RUST_LOG=trace`
        // the chip labelled as the most permissive floor *hid* lines the unfiltered view showed
        // — a severity floor whose quietest value is not the quietest level.
        assert!(
            embedded_index()
                .contains("const OBS_LEVELS = ['error', 'warn', 'info', 'debug', 'trace']"),
            "a floor's quietest chip must be the quietest level the API accepts"
        );
        assert!(
            !embedded_index().contains("lvl === 'trace' ? 'debug' : lvl"),
            "and a `TRACE` line must not be painted as a `DEBUG` one now that both are filterable"
        );

        // The level regex is unanchored, and pinned as the literal it is. PR3 moved `[LEVEL]`
        // off the start of the line behind an RFC-3339 timestamp; the old `^`-anchored form
        // then matched nothing, every line came back with a null level, and the chips
        // `docs/web-ui.md` documented could not hide one line. Both halves are asserted
        // because the negative alone passes on a page with no level match at all.
        assert!(
            embedded_index().contains("/\\[(ERROR|WARN|INFO|DEBUG|TRACE)\\]/"),
            "the pane must colour a line by its `[LEVEL]` wherever in the line it sits"
        );
        assert!(
            !embedded_index().contains("/^\\s*\\[("),
            "the level match must not assume `[LEVEL]` is line-initial"
        );
        // **A node that logs but does not roll still has logs.** `OXIDANT_LOG_ROLL=off` keeps
        // durable statement history with stderr-only logs, so every `?file=` is a `404` while
        // the memory ring answers. The pane drops to the ring once and says so, rather than
        // reporting "no logs to read here" about a node that is logging.
        assert!(
            embedded_index().contains(
                "if (code === 404 && obsState.file === 'current' && !obsState.currentMissing)"
            ),
            "a node with no rolled files must fall back to the memory ring"
        );
        assert!(
            embedded_index().contains("this node writes no rolled files"),
            "and the caption must say why the ring is what is on screen"
        );

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

    /// The catalog rail's splice, on the same terms as the derivation's: the marker is
    /// replaced, the logic lands, and no copy of it survives inline in the template.
    #[test]
    fn the_catalog_rail_logic_is_spliced_into_the_served_page() {
        assert!(
            EMBEDDED_TEMPLATE.contains(CATALOG_MARKER),
            "the page lost its `{CATALOG_MARKER}` splice point; the rail would never load"
        );
        let page = embedded_index();
        assert!(
            !page.contains(CATALOG_MARKER),
            "the marker survived the splice — the page is serving a comment, not the rail"
        );
        assert!(
            !EMBEDDED_TEMPLATE.contains("var __oxidantCatalog"),
            "`__oxidantCatalog` is inline in the page again; it belongs in catalog_rail.js, \
             where `ui/src/lib/catalogRail.test.ts` can evaluate it"
        );
        for symbol in [
            "var __oxidantCatalog",
            "function railRows",
            "function pendingLoads",
            "function insertAtCursor",
            "function quoteIdent",
            "function wantsSuggestions",
        ] {
            assert!(
                page.contains(symbol),
                "the served page is missing `{symbol}`"
            );
        }
        // And the page consumes it rather than shadowing it.
        assert!(page.contains("const CAT = __oxidantCatalog;"));

        // **No NUL byte reaches the document.** `nodeKey` separates a node's coordinates with
        // one, because a quoted identifier may contain anything else — and an HTML parser
        // rewrites U+0000 inside an attribute value to U+FFFD, so a key that made the round
        // trip through the DOM would come back matching nothing. It is written as an escape in
        // the source and the row actions carry an index instead; this pins both.
        assert!(
            !page.contains('\0'),
            "the served page carries a raw NUL byte; an attribute value holding one is rewritten \
             by the parser"
        );
        assert!(
            !page.contains("data-key="),
            "a rail row must carry its index, not its node key: a key contains a NUL separator"
        );
        assert!(page.contains("data-act=\"toggle\" data-i="));
    }

    /// **A bare identifier does not survive the parser; the rail's names have to.**
    ///
    /// The engine leaves `sql_parser.enable_ident_normalization` at DataFusion's default of
    /// `true`, so an unquoted identifier is lowercased at parse time — while the catalog routes
    /// hand back the warehouse's real, case-preserved names. A table `Orders` in schema `Sales`
    /// inserted bare therefore reaches the planner as `sales.orders`: a different table, or
    /// none, and `Preview` turns that into a statement recorded as failed.
    ///
    /// The behaviour itself is evaluated by `ui/src/lib/catalogRail.test.ts`, which no CI job
    /// runs. This is the gate that does.
    #[test]
    fn the_rail_quotes_a_mixed_case_name_rather_than_let_the_parser_lowercase_it() {
        let rule = "if (s !== s.toLowerCase()) return true;";
        let body = js_fn_body(CATALOG_RAIL_JS, "function needsQuoting(name) {");
        assert!(
            !body.is_empty(),
            "`needsQuoting` is gone; every inserted name is now unquoted"
        );
        // In `needsQuoting` and not merely somewhere in the file: `insertTextFor`,
        // `qualifiedName`, `previewSql` and `suggestionInsertText` all quote through it, and a
        // rule bolted onto one caller would leave the other three inserting `Orders` bare.
        assert!(
            body.contains(rule),
            "`needsQuoting` no longer quotes a mixed-case name, so `Orders` inserts bare and \
             resolves to `orders`: {body}"
        );
        assert!(
            embedded_index().contains(rule),
            "the rule is in the module but not in the served page"
        );
    }

    /// **An untouched textarea reports a cursor at offset 0, and it does not have one.**
    ///
    /// The first click on a catalog name is the likeliest first interaction with this rail, and
    /// it lands in an editor nobody has typed in yet. Read `selectionStart` there and the name
    /// is *prepended*: `spark_catalog.sales.orders SELECT 1 AS hello`. Both hosts have to say
    /// whether their target has ever been focused, and both have to actually track it.
    #[test]
    fn an_insertion_into_a_textarea_with_no_caret_goes_to_the_end_of_it() {
        let page = embedded_index();
        assert!(
            page.contains("const seen = catCaretSeen.has(ta) || document.activeElement === ta;"),
            "`catInsert` reads the caret without asking whether there is one"
        );
        assert!(
            page.contains("const at = CAT.caretRange(seen, ta.selectionStart, ta.selectionEnd);"),
            "the caret fallback must go through `caretRange`, which the vitest suite pins"
        );
        assert!(
            page.contains("function caretRange(hasCaret, selStart, selEnd) {"),
            "`caretRange` is not in the served page"
        );

        // And that `caretRange` still answers "no caret" with an offset rather than with the
        // zero it was handed. `insertAtCursor` reads `null` as end-of-buffer; a `caretRange`
        // that passed `selStart` through would be the original bug with a function around it.
        let body = js_fn_body(
            CATALOG_RAIL_JS,
            "function caretRange(hasCaret, selStart, selEnd) {",
        );
        assert!(
            body.contains("if (!hasCaret) return { start: null, end: null };"),
            "`caretRange` no longer sends an untouched textarea to the end of its buffer: {body}"
        );

        // A `WeakSet` nothing ever adds to is the same bug with more code: both the Editor's
        // textarea and every Notebook cell have to register.
        assert!(page.contains("catCaretSeen.add(ta)"));
        assert_eq!(
            page.matches("catTrackCaret(").count(),
            3,
            "the caret tracker is declared once, and registered by exactly the Editor and the \
             Notebook cell renderer"
        );
        assert!(page.contains("catTrackCaret(document.getElementById('editor-sql'));"));
    }

    /// One rail, two mounts, and a layout it adds a column to rather than wraps.
    #[test]
    fn the_catalog_rail_is_mounted_on_both_query_pages() {
        let page = embedded_index();

        // The Editor and the Notebook render the *same* component against the same tree: the
        // hazard a second copy introduces is not a second rail on screen, it is a second cache
        // that disagrees with the first about what the warehouse holds.
        for host in ["editor", "notebook"] {
            assert!(
                page.contains(&format!("catRailHtml('{host}')")),
                "the {host} page does not mount the catalog rail"
            );
            assert!(
                page.contains(&format!("mountCatalogRail('{host}')")),
                "the {host} page never binds or paints its rail"
            );
            assert!(
                page.contains(&format!("rail: 'cat-rail-{host}'")),
                "the {host} host has no rail element to paint into"
            );
        }

        // **Hiding the rail must restore the previous layout exactly, not approximately.**
        // `.workbench` puts one column *in front of* the grid the Editor already had; the grid
        // itself is untouched, so a collapsed rail hands its width straight back. A rail that
        // had been built by editing `.editor-grid`'s own template would make "collapsed" a
        // different layout rather than the old one.
        assert!(
            page.contains(
                ".editor-grid { display: grid; grid-template-columns: 2fr 1fr; gap: 16px; \
                 align-items: start; }"
            ),
            "the rail changed the Editor's own grid; it may only add a column beside it"
        );
        assert!(page.contains(".workbench { display: grid; grid-template-columns: 264px"));
        assert!(
            page.contains(".workbench[data-rail=\"closed\"] { grid-template-columns: 34px"),
            "the rail must collapse to a strip, not disappear and reflow the page"
        );
        assert!(
            page.contains(".workbench[data-rail=\"closed\"] .cat-body { display: none; }"),
            "a collapsed rail still paints its tree; the strip is what hides it"
        );

        // The controls, as the classes and actions the delegated handler binds.
        for control in [
            "class=\"cat-q\"",
            "class=\"cat-suggest\"",
            "class=\"cat-tree\"",
            "data-act=\"open\"",
            "data-act=\"close\"",
            "data-act=\"refresh\"",
            "data-act=\"retry\"",
            "data-act=\"insert\"",
            "data-act=\"preview\"",
            "data-act=\"suggest\"",
        ] {
            assert!(
                page.contains(control),
                "the catalog rail is missing {control}"
            );
        }

        // Failure and emptiness use the platform's components, not prose in a div — the two
        // consoles drifted apart the first time by rendering states any other way.
        assert!(page.contains("return errorState(rows[0].message, rows[0].detail) +"));
        assert!(page.contains("return emptyState('No catalogs',"));

        // The remembered state, under the key `docs/web-ui.md` names.
        assert!(page.contains("var CAT_PREFS_KEY = 'oxidant.catalogRail.v1';"));
    }

    /// The rail reaches the catalog API only through the module that is under test, runs its
    /// preview through the one statement API, and asks for a level only when a row says so.
    #[test]
    fn the_catalog_rail_uses_the_tested_logic_rather_than_its_own_copy() {
        let page = embedded_index();

        // **Every catalog URL is built in `catalog_rail.js`.** `ui/src/lib/catalogRail.test.ts`
        // pins that each segment is encoded — a namespace is a dot-joined query parameter and a
        // table is a path segment a slash must not split — and a second spelling built inline
        // would be the one the browser uses and the one no test ever evaluates.
        assert!(
            !EMBEDDED_TEMPLATE.contains("/api/v1/catalogs"),
            "the page builds a catalog URL by hand; it must go through catalog_rail.js"
        );
        for call in [
            "catJson(CAT.catalogsUrl())",
            "catJson(CAT.childrenUrl(node))",
            "catJson(CAT.autocompleteUrl(raw))",
        ] {
            assert!(page.contains(call), "the rail must fetch through `{call}`");
        }

        // **The rows are the request queue.** Expanding a node sets a bit and repaints;
        // `pendingLoads` turns the placeholder rows that are now on screen into the requests.
        // A `catToggle` that fetched directly would be a second path that could ask for a
        // level the paint did not show — and would make the filter, which shares this code,
        // able to crawl the warehouse.
        assert!(page.contains("CAT.pendingLoads(catRows).forEach(catLoadChildren);"));
        let toggle = page
            .split_once("function catToggle(key) {")
            .and_then(|(_, rest)| rest.split_once("\n    }"))
            .map(|(body, _)| body.to_string())
            .unwrap_or_default();
        assert!(
            !toggle.is_empty(),
            "`catToggle` is gone; the tree cannot be opened"
        );
        assert!(
            !toggle.contains("catLoadChildren") && !toggle.contains("fetch("),
            "expanding a node must not fetch; painting its placeholder row is what asks: \
             {toggle}"
        );

        // **`Refresh` must not be undone by the request it interrupted.** A level whose fetch
        // was already out when the tree was dropped would otherwise write its stale answer into
        // the new tree, and the button pressed to get rid of those rows would repaint them.
        assert!(
            page.contains("if (gen !== catGen) return;"),
            "an in-flight level must not write back into a tree that `Refresh` replaced"
        );

        // A preview is a statement like any other — same API as the Run button, so it lands in
        // the recent-statements rail and on the SQL page instead of being invisible work.
        assert!(page.contains("const doc = await runStatement(sql, host.onUpdate);"));
        assert!(
            page.contains("const sql = CAT.previewSql(node);"),
            "the preview must use the pinned `SELECT * FROM … LIMIT n`, not its own string"
        );

        // **An insertion is spliced, and announced.** A Notebook cell persists itself from its
        // textarea's `input` event, so an insertion that only assigned `.value` would be on
        // screen and absent from localStorage the moment the page reloaded.
        assert!(page.contains("const r = CAT.insertAtCursor(ta.value, at.start, at.end, text);"));
        assert!(
            page.contains("ta.dispatchEvent(new Event('input', { bubbles: true }));"),
            "an inserted name must fire `input`, or the Notebook never persists it"
        );

        // The tree repaints wholesale, so it has to put the reader back where they were —
        // expanding a schema near the bottom of a long catalog must not scroll to the top the
        // moment its tables land.
        assert!(page.contains("const scroll = treeEl.scrollTop;"));
        assert!(page.contains("treeEl.scrollTop = scroll;"));

        // The filter box mirrors between the two mounts, and a repaint that lands mid-word must
        // not move the caret out from under the typist.
        assert!(
            page.contains("if (q && q.value !== catFilter) q.value = catFilter;"),
            "the filter box must be mirrored, never re-set: assigning the same value still \
             collapses a selection"
        );
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
