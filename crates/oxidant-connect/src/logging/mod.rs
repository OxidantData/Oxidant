//! Process-level logging init, and the rolling exec-log writer behind it (§6, §6c).
//!
//! **Why this is not in `rest.rs` any more.** `init_logging()` was the only
//! `tracing_subscriber` init in the tree and it was called from `rest::router`, which is built
//! only in the Connect server bootstrap. A standalone `oxidant worker --port …` therefore
//! installed no subscriber at all and would get no durable log — and worker OOMs are exactly
//! what operators dig for. So the whole init (the [`LogBuffer`] ring, the rolling writer, and
//! the stderr fmt layer) is hoisted here into [`init`], which both the Connect server bootstrap
//! and `run_worker` call.
//!
//! **Collection stays per-node.** Every node writes its own `logs/` under its own root (§3c);
//! the driver does not ingest worker logs. PR4 federates *reads* over them, which is the same
//! statement from the other side. Statement history remains driver-scoped — workers run no
//! statements.
//!
//! That last clause is also why `logs/` needs a lock of its own: a worker runs no statements, so
//! it takes no *journal* lock, and with `OXIDANT_DATA_DIR_PER_PROCESS` unset a co-located driver
//! and worker would share one `logs/oxidant.log` and unlink it out from under each other.
//! [`open_writer`] takes `<logs-dir>/.lock` and refuses the second writer rather than merging it
//! — see `history::lock::acquire_logs_dir`.
//!
//! What lives where:
//!
//! - [`naming`] — UTC names, the `?file=` grammar, ISO weeks, `.N` size splits;
//! - [`line`] — one event's three forms (live tail string, text line, Parquet row);
//! - [`writer`] — the live file, the two roll triggers, dedup, and the converter thread;
//! - [`columnar`] — text → zstd Parquet, and reading either form back.

mod columnar;
mod line;
mod naming;
mod writer;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use tracing::Subscriber;
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use tracing_subscriber::Layer;

/// `oxidant-<period>[.N].<ext>` → its parts, or `None` for anything this writer did not produce
/// (the live `oxidant.log` included). The disk sweeper reads it to answer "what period does this
/// file cover" and "is this mine to delete".
pub(crate) use naming::parse_file_name as parse_rolled_name;
pub use naming::{LogPeriod, LogRoll};
pub(crate) use writer::RollingWriter;

use crate::history::HistoryConfig;
use line::{LogLine, TS_FORMAT};

/// Max retained log lines served by `GET /api/v1/logs` with no `?file=`.
pub(crate) const MAX_LOG_LINES: usize = 1000;

/// In-memory ring buffer of recent log lines shared by the tracing layer and the logs endpoint.
///
/// **Not** deduped: dedup applies to the *file* (§6). The same window can therefore read
/// differently through the ring and through `?file=current`, and the file is authoritative —
/// which is why the `?file=` envelope carries `dedup` and the ring's does not.
#[derive(Clone)]
pub struct LogBuffer {
    inner: Arc<Mutex<Vec<String>>>,
    cap: usize,
}

impl LogBuffer {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::with_capacity(cap))),
            cap,
        }
    }

    fn push(&self, line: String) {
        let mut inner = self.inner.lock().expect("log buffer poisoned");
        if inner.len() >= self.cap {
            inner.remove(0);
        }
        inner.push(line);
    }

    pub(crate) fn lines(&self) -> Vec<String> {
        self.inner.lock().expect("log buffer poisoned").clone()
    }
}

/// The one `tracing` layer both sinks hang off.
///
/// The rolling writer is a layer **in its own right**, not a re-serializer of [`LogBuffer`]
/// strings: it taps `tracing` directly and keeps the level, target and fields apart, which is
/// what gives the Parquet its columns. Both sinks read the same [`LogLine`], so the live tail
/// and the file cannot drift on anything but dedup.
struct Capture {
    buffer: LogBuffer,
    file: Option<Arc<RollingWriter>>,
}

impl<S: Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let line = format_event(event);
        self.buffer.push(line.render());
        if let Some(file) = &self.file {
            file.write(line);
        }
    }
}

/// Decompose a `tracing` event into [`LogLine`]'s columns, timestamped in UTC.
fn format_event(event: &tracing::Event<'_>) -> LogLine {
    let mut visitor = LogVisitor(String::new());
    event.record(&mut visitor);
    let meta = event.metadata();
    LogLine {
        ts: chrono::Utc::now().format(TS_FORMAT).to_string(),
        level: meta.level().as_str(),
        target: meta.target().to_string(),
        fields: visitor.0,
    }
}

struct LogVisitor(String);

impl LogVisitor {
    /// One field, `k=v`, with any newline in `v` escaped.
    ///
    /// **Escaping is not optional here.** `record_debug` is what `tracing` calls both for
    /// `%value` — `DisplayValue`'s `Debug` forwards to `Display` — and for the message itself,
    /// which is a `format_args!` under `{:?}`; neither is quoted, so a newline in either produced
    /// a second physical line that `parse_line` accepts as a genuine event. See
    /// [`line::escape_line_breaks`] for what that buys an attacker holding a gRPC status string,
    /// and for the plain correctness cost with no attacker at all.
    fn push_field(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Display) {
        if !self.0.is_empty() {
            self.0.push_str(", ");
        }
        self.0.push_str(&line::escape_line_breaks(field.name()));
        self.0.push('=');
        self.0
            .push_str(&line::escape_line_breaks(&value.to_string()));
    }
}

impl tracing::field::Visit for LogVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.push_field(field, &format!("{:?}", value));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.push_field(field, &format!("{:?}", value));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.push_field(field, &value);
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.push_field(field, &value);
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.push_field(field, &value);
    }

    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        self.push_field(field, &value);
    }
}

static LOG_BUFFER: OnceLock<LogBuffer> = OnceLock::new();
static ROLLING: OnceLock<Option<Arc<RollingWriter>>> = OnceLock::new();

/// The disk sweep a roll triggers (§3). Published by the statement store at boot rather than
/// reached from here: only the store knows which statements are still running, and the writer
/// exists before the store does.
type SweepHook = Box<dyn Fn() + Send + Sync>;
static SWEEP_HOOK: RwLock<Option<SweepHook>> = RwLock::new(None);

/// Install the roll-time disk sweep. **Last writer wins**, for the same reason
/// `set_history_status_source` is: a `OnceLock` here would let the first store booted in a test
/// process own the hook after it had been dropped.
pub(crate) fn set_sweep_hook(hook: impl Fn() + Send + Sync + 'static) {
    if let Ok(mut slot) = SWEEP_HOOK.write() {
        *slot = Some(Box::new(hook));
    }
}

/// Run the roll-time sweep, if one is installed.
///
/// Called only from the converter thread. The sweep logs, and a log line can roll — but a roll
/// only *queues* another job on the converter's channel, so this never re-enters itself and the
/// read guard cannot be taken recursively.
fn run_sweep_hook() {
    if let Ok(slot) = SWEEP_HOOK.read() {
        if let Some(hook) = slot.as_ref() {
            hook();
        }
    }
}

/// Initialize process-wide log capture: the in-memory ring, the rolling file writer, and a
/// compact stderr `tracing` subscriber. **Idempotent** — the first call in a process wins and
/// later ones are ignored, so the Connect bootstrap's `init("driver", port)` beats
/// `rest::router`'s fallback.
///
/// `role`/`port` are what `OXIDANT_DATA_DIR_PER_PROCESS=1` derives `<root>/<role>-<port>/` from,
/// so a driver and a co-located worker write to distinct trees. **Deviation from §6c**, which
/// spells this `init(role)`: without the port, two processes sharing a root would derive
/// `<root>/driver/` and `<root>/worker/` for their logs while the journal derives
/// `<role>-<port>`, and one process's logs and its statement history would land in two
/// different trees.
///
/// Never fails the caller. A misconfigured root (an object-store URL) is reported on stderr and
/// leaves the process with today's behaviour — stderr plus the ring buffer — because the *same*
/// misconfiguration fails the boot loudly a moment later in `init_statement_store`, and taking
/// the logger down first would hide that message.
pub fn init(role: &str, port: u16) {
    let buffer = LOG_BUFFER
        .get_or_init(|| LogBuffer::new(MAX_LOG_LINES))
        .clone();
    let file = ROLLING.get_or_init(|| open_writer(role, port)).clone();
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_max_level(tracing::Level::INFO)
        .finish()
        .with(Capture {
            buffer,
            file: file.clone(),
        })
        .try_init();
    // First line of the process's own log, and the one an operator greps for when `?file=`
    // answers 404: where the files are and which boundary closes them.
    if let Some(writer) = file {
        tracing::info!(
            role,
            dir = %writer.dir().display(),
            roll = writer.roll().as_str(),
            dedup = writer.dedup_enabled(),
            "rolling exec log open"
        );
    }
}

fn open_writer(role: &str, port: u16) -> Option<Arc<RollingWriter>> {
    let cfg = match HistoryConfig::from_env(role, port) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("oxidant: rolling exec logs are disabled: {e}");
            return None;
        }
    };
    // `OXIDANT_HISTORY=off` promises that nothing is written under the data dir; rolled exec
    // logs are the largest thing that would be.
    if !cfg.enabled {
        return None;
    }
    // A `.parquet.tmp` a crash left behind is billed against the disk budget — it is a file under
    // a budget root — but no prune step can recognise it, and the converter thread that would
    // delete it does not run under `OXIDANT_LOG_ROLL=off`. So sweep it here, at boot, before this
    // process has a writer and therefore before anything can be converting into it.
    let freed = crate::history::disk::clear_log_tmp(&cfg.logs_dir);
    if freed > 0 {
        eprintln!(
            "oxidant: removed {freed} bytes of unfinished parquet conversions from {}",
            cfg.logs_dir.display()
        );
    }
    // `OXIDANT_LOG_ROLL=off` turns the file writer off on its own, for an operator who wants
    // durable statement history and stderr-only logs.
    if cfg.log_roll == LogRoll::Off {
        return None;
    }
    // **One rolling writer per log directory.** A worker takes no journal lock — it runs no
    // statements — so with a driver and a worker co-located on the default root, nothing else
    // stops the two from sharing `logs/oxidant.log`. Refusing here costs this process its durable
    // log and says how to get it back; sharing costs it the same log silently, plus the peer's.
    let lock = match crate::history::lock::acquire_logs_dir(&cfg) {
        Ok(lock) => lock,
        Err(e) => {
            eprintln!("{e}");
            return None;
        }
    };
    let wcfg = writer::WriterConfig {
        dir: cfg.logs_dir.clone(),
        lock: Some(lock),
        roll: cfg.log_roll,
        max_file_bytes: cfg.log_max_file_bytes,
        parquet: cfg.log_parquet,
        dedup: cfg.log_dedup,
        headroom: writer::Headroom {
            roots: crate::history::disk::budget_roots(&cfg),
            max_bytes: cfg.disk_max_bytes,
            min_free_bytes: cfg.disk_min_free_bytes,
            reserve_bytes: cfg.log_max_file_bytes,
            mounts: cfg.mounts_override(),
        },
    };
    match RollingWriter::open(wcfg) {
        Ok(writer) => Some(writer),
        Err(e) => {
            eprintln!("oxidant: rolling exec logs are disabled: {e}");
            None
        }
    }
}

/// Flush and quiesce the process's rolling exec log — §6's "at shutdown" flush trigger.
///
/// **This was documented in three places and was dead code.** `RollingWriter::shutdown` carried
/// `#[cfg_attr(not(test), allow(dead_code))]`, which was the proof: nothing in a release build
/// called it. `Drop` did the same work, but the writer lives in a `static OnceLock` and Rust runs
/// no destructors for statics at process exit, so it never fired either. §6, §9 and
/// `runtime-contract.md` all listed a trigger that could not fire. The bound was small — the 5 s
/// dedup timer caps what a held repeat can lose — but the `sync_all()` in the same block never
/// ran, so a host that went down behind the process lost a page cache's worth of tail lines.
///
/// The close line is emitted *through* `tracing`, before the flush, for two reasons: a different
/// line arriving is itself a dedup flush trigger, so it lands the held summary in the right file;
/// and its presence is what tells an operator the process was *stopped* rather than killed. It is
/// the bookend to the `rolling exec log open` line [`init`] writes.
///
/// Idempotent, and a no-op when no writer is installed (`OXIDANT_LOG_ROLL=off`, `OXIDANT_HISTORY=off`).
pub fn shutdown() {
    let Some(writer) = rolling() else { return };
    tracing::info!(
        dir = %writer.dir().display(),
        "rolling exec log closed"
    );
    writer.shutdown();
}

/// Wire [`shutdown`] to the signals a supervisor actually sends, then exit.
///
/// Called from the two long-lived entry points — `serve` and `oxidant worker` — and deliberately
/// **not** from [`init`]: installing a signal handler is a process-wide decision, and an embedded
/// caller that links this crate into its own binary must be the one to make it.
///
/// The flush runs on the blocking pool because [`shutdown`] joins the converter thread, and the
/// process then exits `128 + signo` — the status a shell reports for a process killed by that
/// signal, so a supervisor reading exit codes sees what it saw before. Draining in-flight gRPC
/// first was the other option and was rejected: a long Connect stream would hold `docker stop`
/// open to its full timeout, and the *log* has nothing to wait for.
#[cfg(unix)]
pub fn install_shutdown_flush() {
    tokio::spawn(async {
        let signo = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            Ok(mut term) => {
                tokio::select! {
                    _ = term.recv() => 15,
                    _ = tokio::signal::ctrl_c() => 2,
                }
            }
            // No SIGTERM handler (a sandbox that forbids it): Ctrl-C alone still flushes.
            Err(_) => {
                if tokio::signal::ctrl_c().await.is_err() {
                    return;
                }
                2
            }
        };
        let _ = tokio::task::spawn_blocking(shutdown).await;
        std::process::exit(128 + signo);
    });
}

/// Non-unix: Ctrl-C only, and no `128 + signo` convention to honour.
#[cfg(not(unix))]
pub fn install_shutdown_flush() {
    tokio::spawn(async {
        if tokio::signal::ctrl_c().await.is_err() {
            return;
        }
        let _ = tokio::task::spawn_blocking(shutdown).await;
        std::process::exit(130);
    });
}

/// The process's log ring, creating it if [`init`] has not run (an embedded caller building the
/// REST router directly).
pub(crate) fn buffer() -> LogBuffer {
    LOG_BUFFER
        .get_or_init(|| LogBuffer::new(MAX_LOG_LINES))
        .clone()
}

/// The process's rolling writer, if one is installed.
pub(crate) fn rolling() -> Option<Arc<RollingWriter>> {
    ROLLING.get().cloned().flatten()
}

/// What `GET /api/v1/logs` needs to answer a `?file=`: where the files are, and whether the file
/// it serves was deduped.
#[derive(Clone, Debug, Default)]
pub(crate) struct LogView {
    pub dir: Option<PathBuf>,
    pub dedup: bool,
}

impl LogView {
    /// The view over this process's own rolling writer. `dir: None` — no writer — makes every
    /// `?file=` answer `404`, which is the honest answer: there are no files.
    pub(crate) fn process() -> Self {
        match rolling() {
            Some(w) => Self {
                dir: Some(w.dir().to_path_buf()),
                dedup: w.dedup_enabled(),
            },
            None => Self::default(),
        }
    }
}

/// One bounded page of a log file — see [`columnar::Page`].
pub(crate) use columnar::Page as LogPage;

/// One rolled (or live) log file, resolved from a `?file=` value.
pub(crate) enum LogFile {
    Text(PathBuf),
    Parquet(PathBuf),
}

impl LogFile {
    pub(crate) fn format(&self) -> &'static str {
        match self {
            Self::Text(_) => "text",
            Self::Parquet(_) => "parquet",
        }
    }

    /// One bounded page of the file. **Blocking**: the caller runs it on a blocking thread.
    pub(crate) fn read(&self, offset: usize, limit: usize) -> Result<LogPage, String> {
        match self {
            Self::Text(p) => columnar::read_text_lines(p, offset, limit),
            Self::Parquet(p) => columnar::read_lines(p, offset, limit),
        }
    }
}

/// Resolve a validated `?file=` period to the file on disk.
///
/// **The extension is the server's choice, never the caller's** (§6): `.parquet` if it exists,
/// else `.log`, else `None` → `404`. That is §6's conversion state machine read from the
/// outside, so a caller never has to know whether yesterday has been converted yet.
pub(crate) fn resolve(dir: &Path, period: LogPeriod, split: u32) -> Option<LogFile> {
    let parquet = dir.join(period.file_name(split, "parquet"));
    if parquet.is_file() {
        return Some(LogFile::Parquet(parquet));
    }
    let text = dir.join(period.file_name(split, "log"));
    if text.is_file() {
        return Some(LogFile::Text(text));
    }
    None
}

/// Convert a rolled text log to Parquet — the seam `rest`'s `?file=` test uses to produce a
/// converted file without booting a writer.
#[cfg(test)]
pub(crate) fn convert_for_test(text: &Path) -> Result<String, String> {
    columnar::convert(text).map(|p| p.display().to_string())
}

/// The live `oxidant.log`, or `None` if it does not exist yet.
///
/// **Drains the writer queue first.** The `write(2)` runs on a dedicated thread now, so an event
/// emitted a microsecond ago may still be in flight; `?file=current` reads the file, not the
/// queue, and would otherwise answer a page that is missing the very lines the caller just
/// triggered. The barrier costs one round-trip to a thread that is almost always idle, and it is
/// taken only for `current` — a rolled file is closed and has nothing in flight.
pub(crate) fn resolve_current(dir: &Path) -> Option<LogFile> {
    if let Some(writer) = rolling() {
        if writer.dir() == dir {
            writer.drain();
        }
    }
    let live = dir.join(crate::history::disk::LIVE_LOG);
    live.is_file().then_some(LogFile::Text(live))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The live tail's strings now carry the RFC-3339 UTC prefix that gives the Parquet its
    /// `ts` column — an announced change in what `GET /api/v1/logs` returns (§8).
    #[test]
    fn rendered_lines_lead_with_a_utc_timestamp() {
        let buffer = LogBuffer::new(8);
        let layer = Capture {
            buffer: buffer.clone(),
            file: None,
        };
        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
            tracing::info!(rows = 7, "stage done")
        });
        let lines = buffer.lines();
        assert_eq!(lines.len(), 1, "{lines:?}");
        let parsed = line::parse_line(&lines[0]);
        assert!(parsed.ts_ms.is_some(), "no usable ts in {:?}", lines[0]);
        assert_eq!(parsed.level.as_deref(), Some("INFO"));
        assert_eq!(parsed.message.as_deref(), Some("stage done"));
        assert_eq!(parsed.fields_json.as_deref(), Some(r#"{"rows":"7"}"#));
    }

    /// A writer straight onto a tempdir, with every guard wide open.
    fn test_writer_config(dir: &Path) -> writer::WriterConfig {
        writer::WriterConfig {
            dir: dir.to_path_buf(),
            roll: LogRoll::Daily,
            max_file_bytes: u64::MAX,
            parquet: false,
            dedup: false,
            headroom: writer::Headroom {
                roots: vec![crate::history::disk::BudgetRoot::subtree(dir.to_path_buf())],
                max_bytes: u64::MAX,
                min_free_bytes: 0,
                reserve_bytes: 0,
                mounts: Some(Vec::new()),
            },
            lock: None,
        }
    }

    /// The rolling writer is a layer in its own right: the same event reaches the file with its
    /// level and target intact, not as a re-serialized ring-buffer string.
    #[test]
    fn one_event_reaches_both_the_ring_and_the_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let writer = RollingWriter::open(test_writer_config(dir.path())).expect("writer");
        let buffer = LogBuffer::new(8);
        let layer = Capture {
            buffer: buffer.clone(),
            file: Some(Arc::clone(&writer)),
        };
        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
            tracing::warn!(slot = 3, "pool exhausted")
        });
        writer.shutdown();
        let body = std::fs::read_to_string(dir.path().join(crate::history::disk::LIVE_LOG))
            .expect("live file");
        assert_eq!(body.lines().count(), 1, "{body}");
        assert!(body.contains("[WARN]"), "{body}");
        assert!(body.contains("message=pool exhausted"), "{body}");
        assert!(body.contains("slot=3"), "{body}");
        assert_eq!(
            buffer.lines(),
            vec![body.trim_end().to_string()],
            "the ring and the file hold the same line"
        );
    }

    /// **M1.** A remote peer's error string must not be able to forge a durable log row.
    ///
    /// `record_debug` handles both `%value` (`DisplayValue`'s `Debug` forwards to `Display`) and
    /// the message (`format_args!` under `{:?}`), and neither is quoted, so a newline inside one
    /// produced a second physical line with a fully parseable prefix — an attacker-chosen
    /// timestamp, level, target, message and fields, indistinguishable in the Parquet from a real
    /// engine event. `flight.rs` logs `error = %status.message()` straight from a worker.
    #[test]
    fn a_newline_in_a_field_value_cannot_forge_a_second_line() {
        let dir = tempfile::tempdir().expect("tempdir");
        let writer = RollingWriter::open(test_writer_config(dir.path())).expect("writer");
        let buffer = LogBuffer::new(8);
        let layer = Capture {
            buffer: buffer.clone(),
            file: Some(Arc::clone(&writer)),
        };
        let hostile = "connect failed\n2026-08-24T00:00:00.000Z [INFO] oxidant_connect - \
                       message=all clear, rows=0";
        tracing::subscriber::with_default(
            tracing_subscriber::registry().with(layer),
            || tracing::warn!(error = %hostile, "worker unreachable"),
        );
        writer.shutdown();

        let body = std::fs::read_to_string(dir.path().join(crate::history::disk::LIVE_LOG))
            .expect("live file");
        assert_eq!(
            body.lines().count(),
            1,
            "one event is one line, always: {body:?}"
        );
        assert_eq!(buffer.lines().len(), 1, "and the ring agrees");
        let parsed = line::parse_line(body.trim_end());
        assert_eq!(parsed.level.as_deref(), Some("WARN"), "{parsed:?}");
        assert_eq!(
            parsed.message.as_deref(),
            Some("worker unreachable"),
            "the forged message must not become the row's message: {parsed:?}"
        );
        assert!(
            body.contains("connect failed\\n"),
            "the newline is escaped, not dropped — the text is still there: {body:?}"
        );
        // The payload is still in the file — escaped, inside the `error` field of the one real
        // row — so nothing is lost and nothing is forged.
        let fields = parsed.fields_json.expect("the error field survives");
        assert!(
            fields.contains("all clear") && fields.contains("[INFO]"),
            "the whole hostile string stays in the one field it was: {fields}"
        );
        assert!(
            !body.trim_end().contains('\n'),
            "and it never becomes a second physical line: {body:?}"
        );
    }

    /// The ring is a ring: the cap holds and the oldest line goes.
    #[test]
    fn the_ring_buffer_keeps_the_newest_lines() {
        let buffer = LogBuffer::new(2);
        for i in 0..5 {
            buffer.push(format!("line {i}"));
        }
        assert_eq!(buffer.lines(), vec!["line 3", "line 4"]);
    }

    /// The server picks the extension, and `404` is the answer for a period with no file.
    #[test]
    fn resolution_prefers_parquet_then_text_then_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (period, split) = LogPeriod::parse("2026-08-23").expect("grammar");
        assert!(resolve(dir.path(), period, split).is_none(), "404");

        std::fs::write(dir.path().join("oxidant-2026-08-23.log"), b"x").expect("write");
        assert!(matches!(
            resolve(dir.path(), period, split),
            Some(LogFile::Text(_))
        ));

        std::fs::write(dir.path().join("oxidant-2026-08-23.parquet"), b"x").expect("write");
        assert!(
            matches!(
                resolve(dir.path(), period, split),
                Some(LogFile::Parquet(_))
            ),
            "a converted file wins over the text one still awaiting its unlink"
        );
    }

    /// A roll triggers the disk sweep §3 promises ("at roll time, at boot, and every 5
    /// minutes"). Driven through the writer's own hook rather than the process-global one, which
    /// any sibling test booting a statement store would otherwise swap out mid-assertion.
    #[test]
    fn a_roll_runs_the_disk_sweep_hook() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&hits);
        let writer = RollingWriter::open_with_hook(
            writer::WriterConfig {
                dir: dir.path().to_path_buf(),
                roll: LogRoll::Daily,
                max_file_bytes: u64::MAX,
                parquet: false,
                dedup: false,
                headroom: writer::Headroom {
                    roots: vec![crate::history::disk::BudgetRoot::subtree(
                        dir.path().to_path_buf(),
                    )],
                    max_bytes: u64::MAX,
                    min_free_bytes: 0,
                    reserve_bytes: 0,
                    mounts: Some(Vec::new()),
                },
                lock: None,
            },
            move || {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            },
        )
        .expect("writer");
        writer.roll_now(chrono::Utc::now());
        // The hook runs on the converter thread; shutdown drains the queue and joins it.
        writer.shutdown();
        assert_eq!(
            hits.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "§3: the sweeper runs at boot and at roll time — one of each here"
        );
    }
}
