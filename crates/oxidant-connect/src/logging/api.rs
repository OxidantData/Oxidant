//! One node's answer to a log-browser question — and the Flight action that lets the driver ask
//! it of a worker (§6b).
//!
//! **Why this is not just handler code in `rest.rs`.** §6b's federation is "the driver proxies
//! the query, with the same filters, and labels the rows with their worker". The moment the
//! driver's route and the worker's route are two implementations, "the same filters" is a claim
//! rather than a fact — a `level=warn` that means one thing on the driver and another on a
//! worker is exactly the silent divergence a log browser must not have. So there is one
//! [`answer`], one [`LogQuery`], and two transports into it: axum on the driver, Flight on the
//! worker.
//!
//! **Why Flight and not a small HTTP surface on the worker.** A worker speaks Flight and nothing
//! else. Giving it an HTTP listener means a second port to open, a second bind to configure in
//! every deployment template, a second CORS decision, and a second place to get the status-token
//! gate right. The Flight port already exists, is already the driver→worker interconnect, is
//! already connection-pooled ([`connect_flight`]'s channel cache), and already carries actions
//! with exactly this shape (`heartbeat`, `bucket_row_counts`).
//!
//! **And it carries the same gate.** The action requires `OXIDANT_STATUS_TOKEN` as
//! `authorization: Bearer <token>`, checked by the same `bearer_is_authorized` the driver's
//! `GET /api/v1/logs` uses — see [`install_flight_handler`]. The Flight port is a **trusted
//! network boundary** in the sense that `Ticket::Sql` accepts arbitrary stage SQL from anyone
//! who can reach it; that is a reason to firewall it, not a licence to serve logs to the same
//! peer. SQL reads the data this worker can reach; a log page reads every enabled `tracing`
//! field value, which is where credentials live. Keeping the port off the public internet is
//! still the operator's job, exactly as it was before this action existed — the gate only keeps
//! this action from making that job harder.
//!
//! **No log bytes are copied.** The action returns one bounded page of rendered lines, the same
//! page the worker's own `?file=` would have returned; the driver forwards it to its caller and
//! keeps nothing. The one exception is the diagnostic dump (`rest::create_dump`), which §6b
//! sanctions explicitly and which says so in its own name.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{LogFilter, LogView, MAX_LOG_LINES};

/// The largest page any log route will serve, however large a `?limit=` is asked for.
///
/// A rolled file may hold `OXIDANT_LOG_MAX_FILE_BYTES` (256 MiB, ~2M lines) and PR3's read path
/// had no cap at all: `?file=current` built a `Vec<String>` of every line and `serde_json` then
/// serialised a second copy into the body — well over half a GiB transient on a driver whose
/// whole *result* budget is 512 MiB, multiplied by every concurrent request, on an endpoint the
/// Observability page polls every 5 s.
pub(crate) const MAX_LOG_PAGE: usize = 10_000;

/// What a caller wants from one node's logs. The wire form of the Flight action, and the parsed
/// form of the driver's query string — the same struct, so the two cannot drift.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct LogQuery {
    /// `files` lists; anything else (or absent) is a page read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op: Option<String>,
    /// `current`, a `LogPeriod` stem with an optional `.N`, or absent for the in-memory ring.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub level: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub q: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    /// PR3's oldest-first walk. Mutually exclusive with `before` (§6b replaced it).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
    /// PR4's backward cursor: serve the lines *before* this row index, newest-first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<u64>,
    /// The **follow** cursor: serve the matches at or after this row index, oldest-first, and
    /// report where to resume. Wins over `before` when both are given — a follow is a follow.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<u64>,
    /// `desc` asks for §6b's newest-first page explicitly, for a caller that wants it without
    /// passing a filter to imply it. Anything else is ignored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<String>,
}

/// A refusal, carrying the HTTP status the driver answers with — including when the refusal came
/// back over Flight from a worker, so a caller sees the worker's own `400`/`404` rather than a
/// blanket `502` that hides which node objected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogError {
    pub status: u16,
    pub message: String,
}

impl LogError {
    fn new(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    /// The wire form, so a federated refusal keeps its status across the Flight hop.
    fn to_json(&self) -> Value {
        json!({ "status": self.status, "error": self.message })
    }

    fn from_json(v: &Value) -> Option<Self> {
        Some(Self {
            status: v.get("status")?.as_u64()? as u16,
            message: v.get("error")?.as_str()?.to_string(),
        })
    }
}

/// The grammar rejection, spelled once. Both transports answer it verbatim.
const BAD_FILE: &str = "invalid file: expected `current`, `YYYY-MM-DD`, `YYYY-MM-DD-HH` or \
                        `YYYY-Www`, each with an optional `.N` split (2..999) and no extension";

/// Answer one query against this process's own logs. **Blocking** — `std::fs` plus, for a
/// converted file, a real Parquet decode. Every caller runs it off the reactor.
pub(crate) fn answer(
    query: &LogQuery,
    view: &LogView,
    ring: &super::LogBuffer,
) -> Result<Value, LogError> {
    let filter = LogFilter::parse(
        query.level.as_deref(),
        query.target.as_deref(),
        query.q.as_deref(),
        query.from.as_deref(),
        query.to.as_deref(),
    )
    .map_err(|e| LogError::new(400, e))?;
    if query.op.as_deref() == Some("files") {
        return files(view);
    }
    let limit = query.limit.unwrap_or(MAX_LOG_LINES).clamp(1, MAX_LOG_PAGE);
    // **The cursor chooses the mode.** `before=` — or any filter — means §6b's newest-first
    // walk; nothing at all means PR3's oldest-first `?offset=` page, byte-identical to what it
    // answered before this route grew filters. Two shapes on one route is the price of not
    // breaking a released contract, and which one you get is decided by what you asked for
    // rather than by a version flag.
    let backward =
        query.before.is_some() || query.order.as_deref() == Some("desc") || !filter.is_empty();
    let Some(requested) = query.file.clone() else {
        return Ok(ring_page(ring, &filter, query, limit, backward));
    };
    let Some(dir) = view.dir.as_deref() else {
        return Err(LogError::new(
            404,
            "no rolled exec logs on this node (OXIDANT_LOG_ROLL=off, or OXIDANT_HISTORY=off)",
        ));
    };
    let (file, label) = if requested == "current" {
        (super::resolve_current(dir), "current".to_string())
    } else {
        // Parsed into a typed `LogPeriod` and the filename *reconstructed* from it — never
        // string-joined into a path. `..`, `/`, an extension and an absolute path all fail the
        // grammar by construction, so no traversal shape ever reaches the join (§6, F12).
        let Some((period, split)) = super::LogPeriod::parse(&requested) else {
            return Err(LogError::new(400, BAD_FILE));
        };
        (super::resolve(dir, period, split), {
            let stem = period.stem();
            if split <= 1 {
                stem
            } else {
                format!("{stem}.{split}")
            }
        })
    };
    let Some(file) = file else {
        return Err(LogError::new(404, "log file not found"));
    };
    let format = file.format();
    let mut body = json!({
        "file": label,
        "format": format,
        // The file is authoritative and it *is* deduped when the knob is on; the SSE tail marks
        // itself `false`. Saying so in the envelope is what keeps an operator from reading a
        // collapsed run as a gap (§6, F21).
        "dedup": view.dedup,
        "limit": limit,
    });
    if let Some(after) = query.after {
        let page = file
            .read_forward(&filter, after, limit)
            .map_err(|e| LogError::new(500, format!("could not read the log file: {e}")))?;
        body["after"] = json!(after);
        // A scan position, not a match position: re-asking with it reads every row exactly once
        // however selective the filter is. It going *backward* means the file rolled under the
        // follow, and the caller restarts from 0 rather than waiting for the new file to grow
        // past the old one's length.
        body["next_after"] = json!(page.next_after);
        body["logs"] = json!(page.lines);
    } else if backward {
        let page = file
            .read_filtered(&filter, query.before, limit)
            .map_err(|e| LogError::new(500, format!("could not read the log file: {e}")))?;
        body["before"] = json!(query.before);
        // `null` when this page reached the start of the file. A page may also be cut short of
        // `limit` by the read path's byte budget, so paging follows this rather than counting.
        body["next_before"] = json!(page.next_before);
        body["logs"] = json!(page.lines);
    } else {
        let offset = query.offset.unwrap_or(0);
        let page = file
            .read(offset, limit)
            .map_err(|e| LogError::new(500, format!("could not read the log file: {e}")))?;
        let next_offset = page
            .has_more
            .then(|| offset.saturating_add(page.lines.len()));
        body["offset"] = json!(offset);
        body["next_offset"] = json!(next_offset);
        body["logs"] = json!(page.lines);
    }
    Ok(body)
}

/// The in-memory ring — `GET /api/v1/logs` with no `?file=`.
///
/// With no filter and no cursor this is **byte-identical** to what PR3 answered: `{"logs": […]}`
/// and nothing else. That is deliberate; the envelope was documented and released.
fn ring_page(
    ring: &super::LogBuffer,
    filter: &LogFilter,
    query: &LogQuery,
    limit: usize,
    backward: bool,
) -> Value {
    let lines = ring.lines();
    if !backward {
        return json!({ "logs": lines });
    }
    let page = super::browse::filter_ring(&lines, filter, query.before, limit);
    json!({
        "file": "ring",
        // The ring is never deduped — dedup applies to the *file* (§6, F21).
        "dedup": false,
        "limit": limit,
        "before": query.before,
        "next_before": page.next_before,
        // **The ring's cursor is the one that is not stable, and the envelope says so.**
        // Everywhere else `next_before` is a row index from the start of an append-only file, so
        // it names the same line forever. Here it is an index into a buffer that *rolls*: every
        // `LogBuffer::push` between two requests shifts every index by one, so paging backward
        // with it silently repeats or skips lines. A caller that wants a stable cursor wants
        // `?file=current`. This field is how a caller can tell without reading this comment —
        // and why the Observability pane reads the whole ring in one page instead of paging it.
        "cursor": "best-effort",
        "logs": page.lines,
    })
}

/// `GET /api/v1/logs/files` — every file the sweeper has not pruned, newest period first.
///
/// "The visible history is always honestly what exists" (§6b): the listing is a directory read,
/// not a computed range, so a file retention took is simply absent rather than offered and then
/// `404`ing.
fn files(view: &LogView) -> Result<Value, LogError> {
    let Some(dir) = view.dir.as_deref() else {
        return Err(LogError::new(
            404,
            "no rolled exec logs on this node (OXIDANT_LOG_ROLL=off, or OXIDANT_HISTORY=off)",
        ));
    };
    let files: Vec<Value> = super::list_files(dir)
        .into_iter()
        .map(|f| {
            json!({
                "file": f.file,
                "rolled": f.rolled,
                "format": f.format,
                "size_bytes": f.size_bytes,
                "first_ts": f.first_ts,
                "last_ts": f.last_ts,
            })
        })
        .collect();
    Ok(json!({ "dir": dir.display().to_string(), "dedup": view.dedup, "files": files }))
}

/// Install the Flight action that lets a driver ask this process the questions above.
///
/// Called from [`super::init`], so **every** node that logs can be browsed — the driver too,
/// which costs nothing and keeps `oxidant driver`'s in-process worker from being the one node
/// whose logs the federation cannot reach. That reach is exactly why the action is gated: on a
/// deployment that co-locates a Flight worker in the driver process, an ungated action made the
/// *driver's* own log readable with no `OXIDANT_STATUS_TOKEN` — the one gate the HTTP side is
/// careful about, walked around by a port that was never meant to serve this.
///
/// **The token is resolved once, here, from the same env the HTTP routes read.** `rest.rs`
/// builds its `RestState` the same way at startup, so a node cannot end up with one idea of the
/// secret on its axum surface and another on its Flight one.
pub(crate) fn install_flight_handler() {
    let expected = oxidant_ui_server::status::status_token_from_env();
    oxidant_execution::flight::set_log_query_handler(move |body, credential| {
        use oxidant_execution::flight::LogQueryRefusal;
        // Authorize, then parse — the same order `rest::gate_log_params` takes, and for the same
        // reason: a `400 invalid log query` answered before the credential is checked tells an
        // unauthenticated caller that this worker has a logs API at all.
        let Some(expected) = expected.as_deref() else {
            return Err(LogQueryRefusal::NotConfigured);
        };
        if !oxidant_ui_server::status::bearer_is_authorized(expected, credential) {
            return Err(LogQueryRefusal::Unauthenticated);
        }
        let query: LogQuery = serde_json::from_slice(body)
            .map_err(|e| {
                serde_json::to_vec(&LogError::new(400, format!("invalid log query: {e}")).to_json())
                    .unwrap_or_else(|_| e.to_string().into_bytes())
            })
            .map_err(|bytes| {
                LogQueryRefusal::Failed(String::from_utf8_lossy(&bytes).into_owned())
            })?;
        match answer(&query, &LogView::process(), &super::buffer()) {
            Ok(v) => serde_json::to_vec(&v).map_err(|e| LogQueryRefusal::Failed(e.to_string())),
            // A refusal travels as a *body*, not as a gRPC error: the driver must be able to
            // hand its caller the worker's own `400 invalid file` rather than flattening every
            // worker-side objection into one `502`.
            Err(e) => {
                serde_json::to_vec(&e.to_json()).map_err(|e| LogQueryRefusal::Failed(e.to_string()))
            }
        }
    });
}

/// Decode one worker's answer, splitting its own refusal back out of the envelope.
pub(crate) fn decode_worker_answer(body: &[u8]) -> Result<Value, LogError> {
    let value: Value = serde_json::from_slice(body)
        .map_err(|e| LogError::new(502, format!("worker sent an unreadable log answer: {e}")))?;
    match LogError::from_json(&value) {
        Some(err) => Err(err),
        None => Ok(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An empty ring: every test here reads a file, and the ring's own paths are covered in
    /// `browse::tests`.
    fn ring() -> super::super::LogBuffer {
        super::super::LogBuffer::new(8)
    }

    fn view(dir: &std::path::Path) -> LogView {
        LogView {
            dir: Some(dir.to_path_buf()),
            dedup: true,
        }
    }

    const LINES: [&str; 3] = [
        "2026-08-23T14:00:00.000Z [INFO] oxidant_execution - message=stage 0 start",
        "2026-08-23T14:00:01.000Z [WARN] oxidant_connect - message=pool exhausted",
        "2026-08-23T14:00:02.000Z [ERROR] oxidant_execution - message=stage 0 failed",
    ];

    fn write(dir: &std::path::Path, name: &str) {
        std::fs::write(dir.join(name), format!("{}\n", LINES.join("\n"))).expect("write");
    }

    /// **The whole reason this module exists.** The mode is chosen by what the caller asked
    /// for: no filter and no cursor is PR3's oldest-first page, verbatim; a filter or a cursor
    /// is §6b's newest-first walk.
    #[test]
    fn the_cursor_chooses_the_mode_and_the_old_shape_is_untouched() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(dir.path(), "oxidant-2026-08-23.log");
        let base = LogQuery {
            file: Some("2026-08-23".to_string()),
            ..Default::default()
        };

        let old = answer(&base, &view(dir.path()), &ring()).expect("page");
        assert_eq!(old["offset"], 0, "PR3's shape, untouched: {old}");
        assert_eq!(old["next_offset"], Value::Null);
        assert!(old.get("next_before").is_none(), "and no new key: {old}");
        assert_eq!(old["logs"], json!(LINES));

        let filtered = answer(
            &LogQuery {
                level: Some("warn".to_string()),
                ..base.clone()
            },
            &view(dir.path()),
            &ring(),
        )
        .expect("page");
        assert!(filtered.get("offset").is_none(), "a filter switches modes");
        assert_eq!(filtered["next_before"], Value::Null);
        assert_eq!(filtered["logs"], json!([LINES[1], LINES[2]]));
    }

    /// A refusal keeps its status across the Flight hop, so a caller learns *which* node
    /// objected and why — not a blanket `502`.
    #[test]
    fn a_refusal_survives_the_wire_with_its_status() {
        let dir = tempfile::tempdir().expect("tempdir");
        let err = answer(
            &LogQuery {
                file: Some("../../etc/passwd".to_string()),
                ..Default::default()
            },
            &view(dir.path()),
            &ring(),
        )
        .expect_err("must refuse");
        assert_eq!(err.status, 400);
        let wire = serde_json::to_vec(&err.to_json()).expect("encode");
        assert_eq!(
            decode_worker_answer(&wire).expect_err("still an error"),
            err
        );
    }

    /// A node with no rolling writer says so, for every `?file=` value, on both transports.
    #[test]
    fn a_node_with_no_writer_answers_404_with_a_reason() {
        let empty = LogView::default();
        for query in [
            LogQuery {
                file: Some("current".to_string()),
                ..Default::default()
            },
            LogQuery {
                op: Some("files".to_string()),
                ..Default::default()
            },
        ] {
            let err = answer(&query, &empty, &ring()).expect_err("must refuse");
            assert_eq!(err.status, 404);
            assert!(err.message.contains("OXIDANT_LOG_ROLL=off"), "{err:?}");
        }
    }

    /// The listing is the same answer on either transport, and it is a directory read.
    #[test]
    fn the_file_listing_reports_what_is_on_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(dir.path(), "oxidant-2026-08-23.log");
        write(dir.path(), crate::history::disk::LIVE_LOG);
        let body = answer(
            &LogQuery {
                op: Some("files".to_string()),
                ..Default::default()
            },
            &view(dir.path()),
            &ring(),
        )
        .expect("files");
        let files = body["files"].as_array().expect("an array").clone();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0]["file"], "current");
        assert_eq!(files[0]["rolled"], false);
        assert_eq!(files[1]["file"], "2026-08-23");
        assert_eq!(files[1]["format"], "text");
        assert_eq!(files[1]["first_ts"], "2026-08-23T14:00:00.000Z");
    }

    /// **The ring's cursor is best-effort and the envelope admits it.** Every other
    /// `next_before` is a row index from the start of an append-only file and names the same
    /// line forever; the ring's is an index into a buffer that rolls under the reader. A caller
    /// pacing a file's cursor and a ring's cursor the same way silently repeats or skips lines,
    /// and the ring is the *fallback* view on an `OXIDANT_LOG_ROLL=off` node — precisely where
    /// it is the only view there is.
    #[test]
    fn the_rings_cursor_is_labelled_best_effort_and_a_files_is_not() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(dir.path(), "oxidant-2026-08-23.log");
        let ring = super::super::LogBuffer::new(8);
        for line in LINES {
            ring.push(line.to_string());
        }
        let from_ring = answer(
            &LogQuery {
                order: Some("desc".to_string()),
                ..Default::default()
            },
            &view(dir.path()),
            &ring,
        )
        .expect("ring");
        assert_eq!(from_ring["file"], "ring");
        assert_eq!(
            from_ring["cursor"], "best-effort",
            "the ring rolls under its own cursor: {from_ring}"
        );

        let from_file = answer(
            &LogQuery {
                file: Some("2026-08-23".to_string()),
                order: Some("desc".to_string()),
                ..Default::default()
            },
            &view(dir.path()),
            &ring,
        )
        .expect("file");
        assert!(
            from_file.get("cursor").is_none(),
            "a file's cursor is exact and carries no caveat: {from_file}"
        );
    }

    /// `limit` is clamped, not trusted: the page cap is the memory bound, and a caller asking
    /// for a million lines is asking for the driver's whole result budget.
    #[test]
    fn the_page_limit_is_clamped() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(dir.path(), "oxidant-2026-08-23.log");
        let body = answer(
            &LogQuery {
                file: Some("2026-08-23".to_string()),
                limit: Some(usize::MAX),
                ..Default::default()
            },
            &view(dir.path()),
            &ring(),
        )
        .expect("page");
        assert_eq!(body["limit"], MAX_LOG_PAGE);
    }
}
