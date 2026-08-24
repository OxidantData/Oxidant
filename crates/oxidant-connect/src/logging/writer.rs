//! The rolling exec-log writer (§6): two roll triggers, `.N` size splits, repeated-line
//! suppression, and Parquet-on-roll on a background thread.
//!
//! **Two roll triggers, whichever fires first**: the UTC clock boundary (`daily` by default,
//! `hourly`/`weekly` via `OXIDANT_LOG_ROLL`) or the size cap `OXIDANT_LOG_MAX_FILE_BYTES`,
//! which produces a `.N` split — a chatty hour rotates early instead of growing without bound.
//!
//! **Conversion happens after close, never during**, and on a *different thread*: the roll
//! itself is close + fsync + rename + fsync-dir + reopen, all of it under the writer lock and
//! all of it bounded; reading a 256 MiB text file back and writing zstd Parquet is not, and
//! doing it inline would stall every `tracing` event in the process behind it.
//!
//! **Nothing in this file logs while holding the writer lock.** A `tracing::warn!` from inside
//! the critical section re-enters this very layer on the same thread, and `std::sync::Mutex` is
//! not reentrant — it would deadlock the process's logging, permanently. Messages are collected
//! and emitted after the guard drops.
//!
//! **The `write(2)` is off the emitting thread.** Every `tracing` event used to take one
//! process-global mutex and do a blocking `write_all` on whatever thread emitted it — a tokio
//! worker, a Flight data-path thread, a rayon worker. On a slow or full volume that stalls the
//! reactor, and a chatty distributed stage serialises every logging thread on one lock. So an
//! event is now *rendered* on the emitting thread and handed to a bounded queue with
//! [`std::sync::mpsc::SyncSender::try_send`]: never blocking, never allocating unboundedly, and
//! never the caller's problem. One dedicated thread owns the file, the dedup state and both roll
//! triggers; a second owns Parquet conversion and the sweep, because converting a 256 MiB file
//! takes seconds and log lines must not queue behind it.
//!
//! **The two costs, stated.** A full queue *drops* lines rather than blocking — the count is
//! kept and written into the log as its own `WARN` the moment there is room, so the gap is
//! visible rather than silent. And `?file=current` can miss the last few microseconds of
//! events; [`RollingWriter::drain`] is the barrier that closes that window, and shutdown takes
//! it.

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};

use super::columnar;
use super::line::LogLine;
use super::naming::{parse_file_name, LogPeriod, LogRoll, PREFIX};
use crate::history::disk::{Mounts, LIVE_LOG};
use crate::history::fs_util;

/// How long a held repeat waits before its `… repeated N times` summary is flushed anyway, so a
/// process that repeats one line and then goes quiet still writes the count promptly and the
/// file's last entry is never stale (§6, F21).
pub(crate) const DEDUP_FLUSH: Duration = Duration::from_secs(5);
/// How often the background thread wakes to check the dedup timer.
const TICK: Duration = Duration::from_millis(500);
/// A conversion that fails this many times is abandoned: the `.log` stays on disk permanently,
/// one loud line is logged, and `?file=` serves it as text (§6).
const MAX_CONVERSION_ATTEMPTS: u32 = 2;
/// How many rendered events the writer queue holds before it starts dropping them.
///
/// The queue is what keeps the `write(2)` off the emitting thread, and it has to be bounded or a
/// disk slower than the process's log rate becomes unbounded memory instead of a stall. 8192
/// `LogLine`s is a few MiB at the sizes this tree emits, and a full queue means the writer thread
/// is more than 8192 events behind — a state worth a `WARN` in the log itself, which is exactly
/// what [`RollingWriter::note_drops`] writes.
const QUEUE_DEPTH: usize = 8192;

/// What the writer needs to know to decide whether a conversion fits (§3, "Conversion
/// headroom"). Parquet conversion transiently holds both the text file and its output, so the
/// converter reserves one `OXIDANT_LOG_MAX_FILE_BYTES` against **both** the byte budget and the
/// free-space floor. A conversion never pushes the disk over a guard: if the reservation cannot
/// be met the conversion is *skipped*, the text file is left in place, and it is retried at the
/// next roll or boot.
#[derive(Clone, Debug)]
pub(crate) struct Headroom {
    pub roots: Vec<crate::history::disk::BudgetRoot>,
    pub max_bytes: u64,
    pub min_free_bytes: u64,
    pub reserve_bytes: u64,
    /// A synthetic mount table, tests only — the same seam `history::disk` uses.
    pub mounts: Option<Vec<(PathBuf, u64)>>,
}

impl Headroom {
    /// `Err(reason)` means "skip this conversion and retry later", never "give up".
    fn check(&self, dir: &Path) -> Result<(), String> {
        let used = crate::history::disk::measure_roots(&self.roots).billed;
        if used.saturating_add(self.reserve_bytes) > self.max_bytes {
            return Err(format!(
                "converting would need {} bytes of headroom against a {}-byte budget already \
                 holding {used}",
                self.reserve_bytes, self.max_bytes
            ));
        }
        let mounts = match &self.mounts {
            Some(entries) => Mounts::from_entries(entries.clone()),
            None => Mounts::probe(),
        };
        if let Some(free) = mounts.free_bytes(dir) {
            if free < self.min_free_bytes.saturating_add(self.reserve_bytes) {
                return Err(format!(
                    "converting would need {} bytes above a {}-byte free-space floor with only \
                     {free} free",
                    self.reserve_bytes, self.min_free_bytes
                ));
            }
        }
        Ok(())
    }
}

/// Everything `logging::init` resolved for the writer.
#[derive(Debug)]
pub(crate) struct WriterConfig {
    pub dir: PathBuf,
    pub roll: LogRoll,
    pub max_file_bytes: u64,
    /// `OXIDANT_LOG_PARQUET=off` keeps rolled files as plain text.
    pub parquet: bool,
    /// `OXIDANT_LOG_DEDUP`.
    pub dedup: bool,
    pub headroom: Headroom,
    /// Exclusive claim on `dir`, released when the writer drops. `None` in the tests that build a
    /// writer straight onto a tempdir; [`super::open_writer`] always takes one.
    ///
    /// **One rolling writer per log directory** (§3c). Two of them each roll `oxidant.log` on
    /// their own schedule and each converter unlinks a rolled file the other may still hold open,
    /// so the loser's lines go to a deleted inode with no error until its own roll fires. The
    /// lock is what makes §3c's "every node writes its own `logs/` under its own root" true
    /// rather than merely documented — see `history::lock::acquire_logs_dir`.
    // Never read: it is an RAII guard, and holding it *is* the behaviour.
    #[allow(dead_code)]
    pub lock: Option<crate::history::lock::JournalDirLock>,
}

struct Held {
    line: LogLine,
    count: u64,
    /// When the *first* suppressed repeat arrived — what the 5 s timer measures against.
    since: DateTime<Utc>,
}

struct WriterState {
    file: std::fs::File,
    bytes: u64,
    /// The period the currently-open live file belongs to.
    period: Option<LogPeriod>,
    held: Option<Held>,
}

enum Job {
    /// Something rolled (or the process just booted): convert whatever text files are pending,
    /// then run the sweep the roll triggers (§3: the sweeper runs at roll time, at boot, and
    /// every 5 minutes). The path is not carried — the converter scans the directory, which is
    /// also how it picks up what a crash left behind.
    Rolled,
    Stop,
}

/// What the emitting thread hands to the writer thread.
enum Entry {
    /// One event, with the instant it was emitted — not the instant it is written, so a queued
    /// line still rolls into the file its own timestamp belongs to.
    Line(Box<LogLine>, DateTime<Utc>),
    /// Everything queued before this is on disk. The reply is sent after the append, which is
    /// what makes `drain` a barrier rather than a hint.
    Barrier(Sender<()>),
    /// Drain, flush the held repeat, `fsync`, and exit.
    Stop,
}

/// Everything the writer thread needs to put a line on disk — and **nothing that owns a thread
/// handle**, which is what lets that thread hold a strong `Arc` without making a cycle.
///
/// The converter thread holds a `Weak<RollingWriter>` instead, because it needs `on_roll` and
/// `convert_pending`; it can afford to give up if the writer is gone, and the writer thread
/// cannot — it owns the only path to the file, and a queued line still has to land.
struct Shared {
    cfg: WriterConfig,
    state: Mutex<WriterState>,
    /// Events dropped because the queue was full, not yet accounted for in the file.
    dropped: AtomicU64,
    /// Tell the converter thread that something rolled.
    jobs: Sender<Job>,
}

/// The live `oxidant.log`, its roll policy, the thread that writes it, and the thread that
/// converts and sweeps.
pub(crate) struct RollingWriter {
    shared: Arc<Shared>,
    /// What a roll triggers besides conversion. `None` uses the process-global sweep hook the
    /// statement store publishes; a test passes its own so a sibling test booting a store
    /// cannot swap the global out from under it mid-assertion.
    on_roll: Option<Box<dyn Fn() + Send + Sync>>,
    /// Serializes [`Self::convert_pending`]. The worker thread runs it after every roll and a
    /// test may drive it directly; two concurrent passes would race on one `.parquet.tmp`.
    converting: Mutex<()>,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// The bounded hand-off to the writer thread. `try_send` on the hot path, so an emitting
    /// thread never waits on a `write(2)`.
    lines: SyncSender<Entry>,
    scribe: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl RollingWriter {
    /// Open (or reopen) `logs/oxidant.log` and start the converter thread.
    ///
    /// Boot does three corrections here, all of them for states a crash can leave (§6):
    ///
    /// - a `.parquet.tmp` is **deleted** and its conversion redone — Parquet's footer sits at the
    ///   end, so a half-written one is not partially readable at all and there is nothing to
    ///   salvage;
    /// - a rolled `.log` with no `.parquet` sibling is queued for conversion — this is exactly
    ///   the state a crash between the roll and the convert leaves;
    /// - a live `oxidant.log` whose last write falls in an *earlier* period is rolled under
    ///   **that** period, not silently appended to under today's name.
    pub(crate) fn open(cfg: WriterConfig) -> Result<Arc<Self>, String> {
        Self::open_at(cfg, Utc::now(), None)
    }

    /// [`Self::open`] with an explicit roll hook — the seam that keeps the roll-time-sweep test
    /// off the process-global hook.
    #[cfg(test)]
    pub(crate) fn open_with_hook(
        cfg: WriterConfig,
        hook: impl Fn() + Send + Sync + 'static,
    ) -> Result<Arc<Self>, String> {
        Self::open_at(cfg, Utc::now(), Some(Box::new(hook)))
    }

    fn open_at(
        cfg: WriterConfig,
        now: DateTime<Utc>,
        on_roll: Option<Box<dyn Fn() + Send + Sync>>,
    ) -> Result<Arc<Self>, String> {
        fs_util::create_dir_secure(&cfg.dir)
            .map_err(|e| format!("creating {}: {e}", cfg.dir.display()))?;
        let live = cfg.dir.join(LIVE_LOG);

        // A live file left by a previous run that belongs to an earlier period is rolled under
        // that period rather than appended to. Its mtime is the only evidence of when it was
        // last written, and it is enough.
        if let Ok(meta) = live.metadata() {
            if meta.len() > 0 {
                let stamp: DateTime<Utc> = meta.modified().map(DateTime::from).unwrap_or(now);
                let was = LogPeriod::of(stamp, cfg.roll);
                if was.is_some() && was != LogPeriod::of(now, cfg.roll) {
                    if let Some(period) = was {
                        let _ = rename_live(&cfg.dir, &live, period);
                    }
                }
            }
        }

        let file = fs_util::append_secure(&live)
            .map_err(|e| format!("opening {}: {e}", live.display()))?;
        let bytes = file.metadata().map(|m| m.len()).unwrap_or(0);
        let (tx, rx) = std::sync::mpsc::channel();
        let (lines, line_rx) = std::sync::mpsc::sync_channel(QUEUE_DEPTH);
        let shared = Arc::new(Shared {
            state: Mutex::new(WriterState {
                file,
                bytes,
                period: LogPeriod::of(now, cfg.roll),
                held: None,
            }),
            dropped: AtomicU64::new(0),
            jobs: tx,
            cfg,
        });
        let writer = Arc::new(Self {
            shared: Arc::clone(&shared),
            converting: Mutex::new(()),
            on_roll,
            worker: Mutex::new(None),
            lines,
            scribe: Mutex::new(None),
        });
        let worker = Arc::downgrade(&writer);
        let handle = std::thread::Builder::new()
            .name("oxidant-log-roll".to_string())
            .spawn(move || background(worker, rx))
            .map_err(|e| format!("starting the log converter thread: {e}"))?;
        *writer.worker.lock().expect("log worker poisoned") = Some(handle);
        let scribe = std::thread::Builder::new()
            .name("oxidant-log-write".to_string())
            .spawn(move || scribe(shared, line_rx))
            .map_err(|e| format!("starting the log writer thread: {e}"))?;
        *writer.scribe.lock().expect("log scribe poisoned") = Some(scribe);
        // Whatever a crash left: a boot-time pass over the directory, on the worker thread.
        let _ = writer.shared.jobs.send(Job::Rolled);
        Ok(writer)
    }

    pub(crate) fn dir(&self) -> &Path {
        &self.shared.cfg.dir
    }

    pub(crate) fn dedup_enabled(&self) -> bool {
        self.shared.cfg.dedup
    }

    pub(crate) fn roll(&self) -> LogRoll {
        self.shared.cfg.roll
    }

    /// Append one event. **Never blocks**, never logs, never panics on a failed write: a log
    /// line that cannot be written is not worth taking the process down for, and a `tracing`
    /// event is emitted from tokio workers and the Flight data path, where a `write(2)` on a
    /// slow volume would stall the reactor.
    ///
    /// The line is rendered on the caller's thread and handed to the writer thread through a
    /// bounded queue. A full queue *drops* the line and counts it — [`Shared::note_drops`] writes
    /// the count into the log itself as soon as there is room, so the gap is never silent.
    pub(crate) fn write(&self, line: LogLine) {
        self.write_at(line, Utc::now());
    }

    pub(crate) fn write_at(&self, line: LogLine, now: DateTime<Utc>) {
        // The event's *own* instant travels with it, so a line that waited in the queue still
        // rolls into the file its timestamp belongs to.
        if let Err(TrySendError::Full(_)) = self.lines.try_send(Entry::Line(Box::new(line), now)) {
            self.shared.dropped.fetch_add(1, Ordering::Relaxed);
        }
        // `Disconnected` means the writer thread has stopped — after `shutdown`, or because it
        // panicked. Counting those as drops would write a growing number into a file nobody is
        // writing any more, so they are simply ignored.
    }

    /// Block until everything queued before this call is on disk.
    ///
    /// The barrier the queue makes necessary. `?file=current` reads the file, not the queue, so
    /// without this a caller can miss the last few microseconds of events; shutdown takes it, and
    /// so does every test that asserts on file contents right after a write.
    pub(crate) fn drain(&self) {
        let (tx, rx) = std::sync::mpsc::channel();
        if self.lines.send(Entry::Barrier(tx)).is_ok() {
            let _ = rx.recv();
        }
    }

    /// The 5 s timer half of §6's flush rule. The writer thread drives it from its idle tick;
    /// a test drives it from an explicit clock, which is the only way to test a 5 s timer in
    /// milliseconds.
    #[cfg(test)]
    fn flush_stale(&self, now: DateTime<Utc>) {
        self.shared.flush_stale(now)
    }

    /// Force a roll now — the seam the size/clock tests drive, and the shape a future
    /// operator-triggered rotate would take.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn roll_now(&self, now: DateTime<Utc>) -> Option<PathBuf> {
        self.shared.roll_now(now)
    }
}

impl Shared {
    /// Apply one queued event: roll if due, dedup, append. **Writer thread only** — this is the
    /// blocking `write(2)` that used to run on whatever thread emitted the event.
    fn apply(&self, line: LogLine, now: DateTime<Utc>) {
        let rolled = {
            let mut st = self.state.lock().expect("log writer poisoned");
            let rolled = self.roll_if_due(&mut st, now);
            self.note_drops(&mut st, now);
            let suppressed = self.cfg.dedup
                && match st.held.as_mut() {
                    Some(held) if line.is_repeat_of(&held.line) => {
                        held.count += 1;
                        true
                    }
                    _ => {
                        self.flush_held(&mut st, now);
                        st.held = Some(Held {
                            line: line.clone(),
                            count: 0,
                            since: now,
                        });
                        false
                    }
                };
            if !suppressed {
                self.append(&mut st, &line.render());
            }
            rolled
        };
        if rolled.is_some() {
            let _ = self.jobs.send(Job::Rolled);
        }
    }

    /// Write the count of events the full queue dropped, as its own `WARN`, and reset it.
    ///
    /// Not a `tracing::warn!`: this runs with the writer lock held, and a `tracing` event from
    /// in here re-enters the layer that feeds this very queue. The line is built directly and
    /// appended, which is also the only way to guarantee it lands *next to* the gap it describes.
    fn note_drops(&self, st: &mut WriterState, now: DateTime<Utc>) {
        let missed = self.dropped.swap(0, Ordering::Relaxed);
        if missed == 0 {
            return;
        }
        // A drop notice is never itself deduped away: it ends the held run first, so the count
        // it interrupts is written where it happened.
        self.flush_held(st, now);
        st.held = None;
        let notice = LogLine {
            ts: now.format(super::line::TS_FORMAT).to_string(),
            level: "WARN",
            target: "oxidant_connect::logging".to_string(),
            fields: format!(
                "message=the rolling exec log dropped events: the writer queue was full, \
                 dropped={missed}, queue_depth={QUEUE_DEPTH}"
            ),
        };
        self.append(st, &notice.render());
    }

    /// The 5 s timer half of §6's flush rule.
    fn flush_stale(&self, now: DateTime<Utc>) {
        let mut st = self.state.lock().expect("log writer poisoned");
        let stale = st.held.as_ref().is_some_and(|h| {
            h.count > 0
                && now
                    .signed_duration_since(h.since)
                    .to_std()
                    .unwrap_or_default()
                    >= DEDUP_FLUSH
        });
        if stale {
            self.flush_held(&mut st, now);
        }
    }

    /// Write the `… repeated N times` summary, if a run is being held. Caller holds the lock.
    fn flush_held(&self, st: &mut WriterState, now: DateTime<Utc>) {
        let Some(held) = st.held.as_mut() else { return };
        if held.count == 0 {
            return;
        }
        let summary = held.line.repeat_summary(now, held.count).render();
        held.count = 0;
        held.since = now;
        self.append(st, &summary);
    }

    fn append(&self, st: &mut WriterState, rendered: &str) {
        // `write_all` of one `writeln!` is one syscall on a file opened `O_APPEND`, which is what
        // keeps a line from interleaving with another thread's mid-line.
        let mut buf = String::with_capacity(rendered.len() + 1);
        buf.push_str(rendered);
        buf.push('\n');
        if st.file.write_all(buf.as_bytes()).is_ok() {
            st.bytes += buf.len() as u64;
        }
    }

    /// Roll if the clock crossed a boundary or the file hit its size cap. Caller holds the lock;
    /// returns the rolled path for the caller to queue *after* releasing it.
    fn roll_if_due(&self, st: &mut WriterState, now: DateTime<Utc>) -> Option<PathBuf> {
        let period = LogPeriod::of(now, self.cfg.roll)?;
        let current = st.period?;
        let clock_due = current != period;
        let size_due = st.bytes >= self.cfg.max_file_bytes;
        if !clock_due && !size_due {
            return None;
        }
        // The summary belongs to the file it summarises — flushed *before* the rename (§6).
        self.flush_held(st, now);
        st.held = None;
        let live = self.cfg.dir.join(LIVE_LOG);
        let _ = st.file.sync_all();
        let rolled = rename_live(&self.cfg.dir, &live, current).ok();
        // Reopen even if the rename failed: the alternative is a process that stops logging.
        if let Ok(file) = fs_util::append_secure(&live) {
            st.file = file;
            st.bytes = 0;
        }
        st.period = Some(period);
        rolled
    }

    fn roll_now(&self, now: DateTime<Utc>) -> Option<PathBuf> {
        let rolled = {
            let mut st = self.state.lock().expect("log writer poisoned");
            let current = st.period?;
            self.flush_held(&mut st, now);
            st.held = None;
            let live = self.cfg.dir.join(LIVE_LOG);
            let _ = st.file.sync_all();
            let rolled = rename_live(&self.cfg.dir, &live, current).ok();
            if let Ok(file) = fs_util::append_secure(&live) {
                st.file = file;
                st.bytes = 0;
            }
            st.period = LogPeriod::of(now, self.cfg.roll);
            rolled
        };
        if rolled.is_some() {
            let _ = self.jobs.send(Job::Rolled);
        }
        rolled
    }

    /// Flush the held repeat and `fsync`. The last thing the writer thread does.
    fn close(&self, now: DateTime<Utc>) {
        let mut st = self.state.lock().expect("log writer poisoned");
        self.note_drops(&mut st, now);
        self.flush_held(&mut st, now);
        // Without this the tail sits in the page cache, and §6's "flushed … at shutdown" buys
        // nothing on a host that goes down right after the process does.
        let _ = st.file.sync_all();
    }
}

impl RollingWriter {
    /// Convert every rolled `.log` with no `.parquet` sibling, and delete every `.parquet.tmp`.
    /// Runs on the converter thread at boot and after every roll.
    fn convert_pending(&self, attempts: &mut HashMap<PathBuf, u32>) {
        let _serialized = self.converting.lock().expect("log converter poisoned");
        let Ok(entries) = std::fs::read_dir(&self.shared.cfg.dir) else {
            return;
        };
        let mut pending: Vec<PathBuf> = Vec::new();
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if name.ends_with(".parquet.tmp") {
                // Not partially readable, so there is nothing to salvage: delete and redo.
                let _ = std::fs::remove_file(entry.path());
                continue;
            }
            let Some((period, split, ext)) = parse_file_name(name) else {
                continue;
            };
            if ext != "log" {
                continue;
            }
            if self
                .shared
                .cfg
                .dir
                .join(period.file_name(split, "parquet"))
                .exists()
            {
                // Converted already; the text file is a leftover from a crash between the
                // footer read-back and the unlink.
                let _ = std::fs::remove_file(entry.path());
                continue;
            }
            pending.push(entry.path());
        }
        if !self.shared.cfg.parquet || pending.is_empty() {
            return;
        }
        pending.sort();
        for path in pending {
            if attempts.get(&path).copied().unwrap_or(0) >= MAX_CONVERSION_ATTEMPTS {
                continue;
            }
            if let Err(reason) = self.shared.cfg.headroom.check(&self.shared.cfg.dir) {
                tracing::info!(
                    file = %path.display(),
                    reason = %reason,
                    "skipping a rolled log's parquet conversion: it would breach a disk guard. \
                     The text file is kept and the conversion is retried at the next roll."
                );
                // A skip is not an attempt: the conversion was never tried.
                return;
            }
            match columnar::convert(&path) {
                Ok(_) => {
                    attempts.remove(&path);
                }
                Err(e) => {
                    let n = attempts.entry(path.clone()).or_insert(0);
                    *n += 1;
                    if *n >= MAX_CONVERSION_ATTEMPTS {
                        tracing::error!(
                            file = %path.display(),
                            error = %e,
                            attempts = *n,
                            "a rolled exec log failed to convert to parquet twice; it stays on \
                             disk as text and GET /api/v1/logs?file= serves it as text. It is \
                             still counted against OXIDANT_DISK_MAX_BYTES and still pruned by \
                             OXIDANT_LOG_KEEP_DAYS."
                        );
                    } else {
                        tracing::warn!(
                            file = %path.display(),
                            error = %e,
                            "converting a rolled exec log to parquet failed; retrying at the \
                             next roll"
                        );
                    }
                }
            }
        }
    }

    /// Flush a held repeat, `fsync`, and stop the worker. **Idempotent**, and the same work
    /// [`Drop`] does.
    ///
    /// §6 lists shutdown as one of the four flush triggers, but for a while nothing in a
    /// non-test build called this: the writer lives in a process-global `OnceLock`, and Rust runs
    /// no destructors for statics at exit, so `Drop` never fired either. The claim is made true
    /// by [`super::install_shutdown_flush`], which wires SIGINT/SIGTERM to
    /// [`super::shutdown`]; this stays idempotent because a test also calls it directly to make
    /// the converter thread quiesce before asserting.
    pub(crate) fn shutdown(&self) {
        // `Stop` goes down the *same* queue the lines do, so FIFO order alone guarantees
        // everything already emitted is written before the flush — including the
        // `rolling exec log closed` line `super::shutdown` emits just before calling this. A
        // blocking `send` rather than `try_send`: this is the one place waiting is correct.
        let _ = self.lines.send(Entry::Stop);
        if let Some(handle) = self.scribe.lock().expect("log scribe poisoned").take() {
            let _ = handle.join();
        }
        // A second call finds no thread to join, so do the flush here too — and after the join
        // the writer thread is gone, which makes this the only thread that can.
        self.shared.close(Utc::now());
        let _ = self.shared.jobs.send(Job::Stop);
        if let Some(handle) = self.worker.lock().expect("log worker poisoned").take() {
            let _ = handle.join();
        }
    }
}

impl Drop for RollingWriter {
    fn drop(&mut self) {
        // The same work, for the writers that *are* dropped — every one in a test, and any
        // future embedded caller that owns its writer rather than parking it in a static.
        self.shutdown();
    }
}

/// `rename` the live file to `oxidant-<period>[.N].log`, picking `N` by scanning for the highest
/// split of that period **on disk** — so a restart mid-period appends a new split instead of
/// overwriting the one the previous run wrote.
fn rename_live(dir: &Path, live: &Path, period: LogPeriod) -> std::io::Result<PathBuf> {
    let target = dir.join(period.file_name(next_split(dir, period), "log"));
    fs_util::rename_durable(live, &target, dir)?;
    Ok(target)
}

/// The next free `.N` for `period`, counting `.log` and `.parquet` alike: a split whose text has
/// already been converted still owns its number.
fn next_split(dir: &Path, period: LogPeriod) -> u32 {
    let stem = period.stem();
    let mut highest = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if !name.starts_with(PREFIX) {
                continue;
            }
            let Some((found, split, _)) = parse_file_name(name) else {
                continue;
            };
            if found.stem() == stem {
                highest = highest.max(split);
            }
        }
    }
    highest + 1
}

/// The writer thread: the *only* thread that touches the live file.
///
/// It owns the blocking `write(2)`, both roll triggers and the dedup state, and it holds a
/// strong `Arc<Shared>` — a queued line has to land even if the last `RollingWriter` handle is
/// being dropped, and `Shared` owns no thread handle, so there is no cycle to leak.
///
/// The idle tick is §6's 5 s dedup timer. It lives here rather than on the converter thread
/// because a 256 MiB Parquet conversion takes seconds, and the summary of a run that has already
/// gone quiet must not wait behind one.
fn scribe(shared: Arc<Shared>, rx: Receiver<Entry>) {
    loop {
        match rx.recv_timeout(TICK) {
            Ok(Entry::Line(line, now)) => shared.apply(*line, now),
            Ok(Entry::Barrier(reply)) => {
                let _ = reply.send(());
            }
            Ok(Entry::Stop) | Err(RecvTimeoutError::Disconnected) => {
                shared.close(Utc::now());
                return;
            }
            Err(RecvTimeoutError::Timeout) => shared.flush_stale(Utc::now()),
        }
    }
}

/// The converter/sweeper thread: a rolled file to convert, and the roll-time disk sweep.
fn background(writer: std::sync::Weak<RollingWriter>, rx: Receiver<Job>) {
    let mut attempts: HashMap<PathBuf, u32> = HashMap::new();
    loop {
        match rx.recv() {
            Ok(Job::Stop) | Err(_) => return,
            Ok(Job::Rolled) => {
                let Some(writer) = writer.upgrade() else {
                    return;
                };
                writer.convert_pending(&mut attempts);
                // §3: the sweeper runs at roll time, at boot, and every 5 minutes. Dropping the
                // `Arc` first keeps a long sweep from pinning the writer alive.
                match &writer.on_roll {
                    Some(hook) => {
                        let hook: &(dyn Fn() + Send + Sync) = hook.as_ref();
                        // The `Arc` is held across a test hook, which is cheap and cannot sweep.
                        hook();
                    }
                    None => {
                        // Dropping the `Arc` first keeps a long sweep from pinning the writer.
                        drop(writer);
                        super::run_sweep_hook();
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn utc(y: i32, m: u32, d: u32, h: u32, min: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, min, 0).unwrap()
    }

    fn line(fields: &str) -> LogLine {
        LogLine {
            ts: "2026-08-23T14:00:00.000Z".to_string(),
            level: "INFO",
            target: "oxidant_test".to_string(),
            fields: fields.to_string(),
        }
    }

    fn cfg(dir: &Path, roll: LogRoll, max_file_bytes: u64) -> WriterConfig {
        WriterConfig {
            dir: dir.to_path_buf(),
            roll,
            max_file_bytes,
            // Conversion is exercised in its own tests; the roll tests assert names, and a
            // converter racing them would delete the very `.log` they check for.
            parquet: false,
            dedup: false,
            lock: None,
            headroom: Headroom {
                roots: vec![crate::history::disk::BudgetRoot::subtree(dir.to_path_buf())],
                max_bytes: u64::MAX,
                min_free_bytes: 0,
                reserve_bytes: 0,
                mounts: Some(Vec::new()),
            },
        }
    }

    fn names(dir: &Path) -> Vec<String> {
        let mut out: Vec<String> = std::fs::read_dir(dir)
            .expect("read_dir")
            .flatten()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        out.sort();
        out
    }

    /// The clock trigger, at each of the three boundaries, driven by a fake UTC clock.
    #[test]
    fn the_clock_rolls_at_daily_hourly_and_weekly_boundaries() {
        for (roll, before, after, expected) in [
            (
                LogRoll::Daily,
                utc(2026, 8, 23, 23, 59),
                utc(2026, 8, 24, 0, 0),
                "oxidant-2026-08-23.log",
            ),
            (
                LogRoll::Hourly,
                utc(2026, 8, 23, 14, 59),
                utc(2026, 8, 23, 15, 0),
                "oxidant-2026-08-23-14.log",
            ),
            (
                // ISO 2026-W34 ends Sunday 2026-08-23; Monday the 24th opens W35.
                LogRoll::Weekly,
                utc(2026, 8, 23, 12, 0),
                utc(2026, 8, 24, 0, 0),
                "oxidant-2026-W34.log",
            ),
        ] {
            let dir = tempfile::tempdir().expect("tempdir");
            let w = RollingWriter::open_at(cfg(dir.path(), roll, u64::MAX), before, None)
                .expect("open");
            w.write_at(line("message=before"), before);
            w.drain();
            assert_eq!(names(dir.path()), vec![LIVE_LOG.to_string()]);
            w.write_at(line("message=after"), after);
            w.drain();
            assert_eq!(
                names(dir.path()),
                vec![expected.to_string(), LIVE_LOG.to_string()],
                "{roll:?} must roll at the boundary"
            );
            let rolled = std::fs::read_to_string(dir.path().join(expected)).expect("rolled");
            assert!(rolled.contains("message=before"), "{rolled}");
            assert!(!rolled.contains("message=after"), "{rolled}");
            let live = std::fs::read_to_string(dir.path().join(LIVE_LOG)).expect("live");
            assert!(live.contains("message=after"), "{live}");
            w.shutdown();
        }
    }

    /// The size trigger fires mid-period and produces `.2`, `.3` splits that never collide with
    /// the clock-rolled name.
    #[test]
    fn the_size_roll_splits_within_a_period() {
        let dir = tempfile::tempdir().expect("tempdir");
        let now = utc(2026, 8, 23, 14, 0);
        // Small enough that every line trips the cap on the *next* write.
        let w =
            RollingWriter::open_at(cfg(dir.path(), LogRoll::Daily, 32), now, None).expect("open");
        for i in 0..4 {
            w.write_at(
                line(&format!("message=line {i} padded out past the cap")),
                now,
            );
        }
        w.drain();
        assert_eq!(
            names(dir.path()),
            vec![
                "oxidant-2026-08-23.2.log".to_string(),
                "oxidant-2026-08-23.3.log".to_string(),
                "oxidant-2026-08-23.log".to_string(),
                LIVE_LOG.to_string(),
            ],
            "three size rolls produce the plain name then .2 and .3"
        );
        // And the clock roll that follows takes the next free split, not the plain name again.
        w.write_at(line("message=tomorrow"), utc(2026, 8, 24, 0, 0));
        w.drain();
        assert!(
            dir.path().join("oxidant-2026-08-23.4.log").exists(),
            "a clock roll after size splits must not overwrite: {:?}",
            names(dir.path())
        );
        w.shutdown();
    }

    /// A restart mid-period must not overwrite the split the previous run wrote.
    ///
    /// The **real** clock, not a fake one: the boot check compares the live file's *mtime*
    /// against the boot period, and a fake `now` in another period would make this a test of the
    /// boot roll instead of a test of the split sequence.
    #[test]
    fn a_restart_mid_period_picks_the_next_split() {
        let dir = tempfile::tempdir().expect("tempdir");
        let now = Utc::now();
        let today = LogPeriod::of(now, LogRoll::Daily).expect("daily");
        let plain = today.file_name(1, "log");
        let first =
            RollingWriter::open_at(cfg(dir.path(), LogRoll::Daily, 32), now, None).expect("open");
        first.write_at(line("message=run one, padded well past the cap"), now);
        first.write_at(line("message=run one again, also padded past"), now);
        // No `drain` here on purpose: `shutdown` is the barrier, and this is what proves it.
        first.shutdown();
        drop(first);
        assert!(dir.path().join(&plain).exists(), "{:?}", names(dir.path()));
        let before = std::fs::read_to_string(dir.path().join(&plain)).unwrap();

        let second =
            RollingWriter::open_at(cfg(dir.path(), LogRoll::Daily, 32), now, None).expect("open");
        second.write_at(line("message=run two, padded well past the cap"), now);
        second.write_at(line("message=run two again, also padded past"), now);
        second.shutdown();
        assert_eq!(
            std::fs::read_to_string(dir.path().join(&plain)).unwrap(),
            before,
            "the first run's rolled file is untouched"
        );
        assert!(
            dir.path().join(today.file_name(2, "log")).exists()
                && dir.path().join(today.file_name(3, "log")).exists(),
            "the second run continues the split sequence: {:?}",
            names(dir.path())
        );
    }

    /// A live file left behind by a run that ended in an earlier period is rolled under **that**
    /// period at boot, not appended to under today's name.
    #[test]
    fn boot_rolls_a_live_file_from_an_earlier_period() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join(LIVE_LOG),
            "2026-08-22T10:00:00.000Z [INFO] oxidant_test - message=yesterday\n",
        )
        .expect("seed");
        // The seeded file's mtime is *now*, so drive the period from an explicit `now` far in
        // the future: the boot check compares the live file's period with the boot period.
        let boot = Utc::now() + chrono::Duration::days(3);
        let w = RollingWriter::open_at(cfg(dir.path(), LogRoll::Daily, u64::MAX), boot, None)
            .expect("open");
        let rolled: Vec<String> = names(dir.path())
            .into_iter()
            .filter(|n| n != LIVE_LOG)
            .collect();
        assert_eq!(rolled.len(), 1, "{rolled:?}");
        assert!(
            std::fs::read_to_string(dir.path().join(&rolled[0]))
                .unwrap()
                .contains("yesterday"),
            "the previous period's lines went to the previous period's file"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join(LIVE_LOG)).unwrap(),
            "",
            "the live file is fresh"
        );
        w.shutdown();
    }

    /// Dedup: a hot loop collapses to one line plus a count, and the count is flushed when a
    /// different line arrives.
    #[test]
    fn a_repeated_line_collapses_and_flushes_on_a_different_line() {
        let dir = tempfile::tempdir().expect("tempdir");
        let now = utc(2026, 8, 23, 14, 0);
        let mut c = cfg(dir.path(), LogRoll::Daily, u64::MAX);
        c.dedup = true;
        let w = RollingWriter::open_at(c, now, None).expect("open");
        for _ in 0..500 {
            w.write_at(line("message=pool exhausted"), now);
        }
        w.write_at(line("message=recovered"), now);
        w.shutdown();
        let body = std::fs::read_to_string(dir.path().join(LIVE_LOG)).expect("live");
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 3, "{body}");
        assert!(lines[0].ends_with("message=pool exhausted"), "{body}");
        assert!(
            lines[1].ends_with("… repeated 499 times"),
            "the run collapses to a count: {body}"
        );
        assert!(lines[1].contains("[INFO] oxidant_test"), "{body}");
        assert!(lines[2].ends_with("message=recovered"), "{body}");
    }

    /// The 5 s timer half of the rule: a process that repeats one line and then goes quiet still
    /// writes the count, so the file's last entry is never stale.
    #[test]
    fn a_held_repeat_flushes_on_the_timer_with_no_further_input() {
        let dir = tempfile::tempdir().expect("tempdir");
        let now = utc(2026, 8, 23, 14, 0);
        let mut c = cfg(dir.path(), LogRoll::Daily, u64::MAX);
        c.dedup = true;
        let w = RollingWriter::open_at(c, now, None).expect("open");
        for _ in 0..10 {
            w.write_at(line("message=pool exhausted"), now);
        }
        w.drain();
        // Nothing else arrives; only the clock moves past the flush interval.
        w.flush_stale(now + chrono::Duration::seconds(4));
        let body = std::fs::read_to_string(dir.path().join(LIVE_LOG)).expect("live");
        assert_eq!(body.lines().count(), 1, "4 s is inside the window: {body}");
        w.flush_stale(now + chrono::Duration::seconds(6));
        let body = std::fs::read_to_string(dir.path().join(LIVE_LOG)).expect("live");
        assert_eq!(body.lines().count(), 2, "{body}");
        assert!(body.contains("… repeated 9 times"), "{body}");
        // A further repeat starts a new count rather than resuming the flushed one.
        w.write_at(
            line("message=pool exhausted"),
            now + chrono::Duration::seconds(7),
        );
        w.drain();
        w.flush_stale(now + chrono::Duration::seconds(20));
        let body = std::fs::read_to_string(dir.path().join(LIVE_LOG)).expect("live");
        assert!(body.contains("… repeated 1 times"), "{body}");
        w.shutdown();
    }

    /// **L2.** Every `tracing` event used to take one process-global mutex and do a blocking
    /// `write_all` on whatever thread emitted it — a tokio worker, a Flight data-path thread. On
    /// a slow or full volume that stalls the reactor, and a chatty distributed stage serialises
    /// every logging thread on one lock.
    ///
    /// Two halves, and this test needs both because either alone is easy to fake:
    ///
    /// - **The emitting thread does not wait for the file.** The test holds the very lock the
    ///   writer thread needs for every append, and then emits. On the old code `write_at` took
    ///   that lock itself and this deadlocked; the timing assertion is what turns that into a
    ///   clean failure instead of a hang.
    /// - **Nothing is lost silently.** The queue is bounded, so a writer thread that far behind
    ///   *drops* — and the count of what it dropped is written into the log itself. Every line
    ///   emitted is either in the file or in that count, exactly once.
    #[test]
    fn an_event_never_waits_on_the_file_and_a_full_queue_says_what_it_dropped() {
        let dir = tempfile::tempdir().expect("tempdir");
        let now = utc(2026, 8, 23, 14, 0);
        let w = RollingWriter::open_at(cfg(dir.path(), LogRoll::Daily, u64::MAX), now, None)
            .expect("open");

        // Stall the writer thread: it cannot append without this lock.
        let held = w.shared.state.lock().expect("state");
        let total = QUEUE_DEPTH + 200;
        let start = std::time::Instant::now();
        for i in 0..total {
            w.write_at(line(&format!("message=event {i}")), now);
        }
        let emitting = start.elapsed();
        drop(held);
        w.drain();
        w.shutdown();

        assert!(
            emitting < Duration::from_secs(5),
            "{total} events took {emitting:?} with the file lock held — the write is back on the \
             emitting thread"
        );

        let body = std::fs::read_to_string(dir.path().join(LIVE_LOG)).expect("live");
        let notices: Vec<&str> = body
            .lines()
            .filter(|l| l.contains("the rolling exec log dropped events"))
            .collect();
        assert_eq!(
            notices.len(),
            1,
            "a full queue must say so, once, in the log it is dropping from: {}",
            body.lines().take(3).collect::<Vec<_>>().join("\n")
        );
        let dropped: usize = notices[0]
            .split("dropped=")
            .nth(1)
            .and_then(|rest| rest.split(',').next())
            .and_then(|n| n.trim().parse().ok())
            .unwrap_or_else(|| panic!("the notice must carry a count: {}", notices[0]));
        assert!(dropped > 0, "the queue really did overflow: {}", notices[0]);
        assert_eq!(
            body.lines().count() - notices.len() + dropped,
            total,
            "every event is either in the file or in the dropped count — never neither"
        );
    }

    /// `drain` is a barrier, not a hint: what it returns from is on disk. `?file=current` takes
    /// it, because the queue means the file can lag the caller's own last event.
    #[test]
    fn drain_returns_only_once_the_queue_is_on_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let now = utc(2026, 8, 23, 14, 0);
        let w = RollingWriter::open_at(cfg(dir.path(), LogRoll::Daily, u64::MAX), now, None)
            .expect("open");
        for i in 0..200 {
            w.write_at(line(&format!("message=event {i}")), now);
        }
        w.drain();
        let body = std::fs::read_to_string(dir.path().join(LIVE_LOG)).expect("live");
        assert_eq!(body.lines().count(), 200, "{}", body.lines().count());
        w.shutdown();
    }

    /// A roll flushes the held count into the file it summarises, not into the next one.
    #[test]
    fn a_roll_flushes_the_held_count_into_the_file_it_summarises() {
        let dir = tempfile::tempdir().expect("tempdir");
        let now = utc(2026, 8, 23, 23, 0);
        let mut c = cfg(dir.path(), LogRoll::Daily, u64::MAX);
        c.dedup = true;
        let w = RollingWriter::open_at(c, now, None).expect("open");
        for _ in 0..4 {
            w.write_at(line("message=pool exhausted"), now);
        }
        w.write_at(line("message=tomorrow"), utc(2026, 8, 24, 0, 1));
        w.shutdown();
        let rolled =
            std::fs::read_to_string(dir.path().join("oxidant-2026-08-23.log")).expect("rolled");
        assert!(rolled.contains("… repeated 3 times"), "{rolled}");
        let live = std::fs::read_to_string(dir.path().join(LIVE_LOG)).expect("live");
        assert!(!live.contains("repeated"), "{live}");
        assert!(live.contains("message=tomorrow"), "{live}");
    }

    /// Conversion is **skipped, not attempted**, when the headroom reservation would breach a
    /// guard — and the text file survives to be retried.
    #[test]
    fn conversion_is_skipped_when_it_would_breach_a_disk_guard() {
        let dir = tempfile::tempdir().expect("tempdir");
        let now = utc(2026, 8, 23, 14, 0);
        let mut c = cfg(dir.path(), LogRoll::Daily, u64::MAX);
        c.parquet = true;
        // A budget the directory is already at: the reservation cannot be met.
        c.headroom.max_bytes = 1;
        c.headroom.reserve_bytes = 256 * 1024 * 1024;
        let w = RollingWriter::open_at(c, now, None).expect("open");
        w.write_at(line("message=before"), now);
        w.drain();
        w.roll_now(utc(2026, 8, 24, 0, 0));
        let mut attempts = HashMap::new();
        w.convert_pending(&mut attempts);
        assert!(
            dir.path().join("oxidant-2026-08-23.log").exists(),
            "the text file is kept: {:?}",
            names(dir.path())
        );
        assert!(!dir.path().join("oxidant-2026-08-23.parquet").exists());
        assert!(attempts.is_empty(), "a skip is not a failed attempt");
        w.shutdown();
    }

    /// The same roll with headroom converts, and the text file goes.
    #[test]
    fn a_rolled_log_converts_when_the_headroom_is_there() {
        let dir = tempfile::tempdir().expect("tempdir");
        let now = utc(2026, 8, 23, 14, 0);
        let mut c = cfg(dir.path(), LogRoll::Daily, u64::MAX);
        c.parquet = true;
        let w = RollingWriter::open_at(c, now, None).expect("open");
        w.write_at(line("message=before"), now);
        w.drain();
        w.roll_now(utc(2026, 8, 24, 0, 0));
        let mut attempts = HashMap::new();
        w.convert_pending(&mut attempts);
        assert!(
            dir.path().join("oxidant-2026-08-23.parquet").exists(),
            "{:?}",
            names(dir.path())
        );
        assert!(!dir.path().join("oxidant-2026-08-23.log").exists());
        w.shutdown();
    }

    /// A crash between the roll and the convert leaves the `.log`; the next boot converts it.
    /// A `.parquet.tmp` found at boot is deleted, never trusted.
    #[test]
    fn boot_converts_what_a_crash_left_and_deletes_a_half_written_parquet() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("oxidant-2026-08-20.log"),
            "2026-08-20T10:00:00.000Z [INFO] oxidant_test - message=crashed before converting\n",
        )
        .expect("seed");
        std::fs::write(
            dir.path().join("oxidant-2026-08-21.parquet.tmp"),
            b"not a footer",
        )
        .expect("seed");
        let mut c = cfg(dir.path(), LogRoll::Daily, u64::MAX);
        c.parquet = true;
        let w = RollingWriter::open_at(c, utc(2026, 8, 23, 14, 0), None).expect("open");
        let mut attempts = HashMap::new();
        w.convert_pending(&mut attempts);
        assert!(
            !dir.path().join("oxidant-2026-08-21.parquet.tmp").exists(),
            "a half-written parquet has no readable prefix; it is deleted and redone"
        );
        assert!(dir.path().join("oxidant-2026-08-20.parquet").exists());
        assert!(!dir.path().join("oxidant-2026-08-20.log").exists());
        assert_eq!(
            columnar::read_lines(&dir.path().join("oxidant-2026-08-20.parquet"), 0, 100)
                .expect("read")
                .lines,
            vec![
                "2026-08-20T10:00:00.000Z [INFO] oxidant_test - message=crashed before converting"
            ]
        );
        w.shutdown();
    }

    /// `OXIDANT_LOG_PARQUET=off` keeps rolled files as text — subject to the same budget.
    #[test]
    fn parquet_off_keeps_rolled_files_as_text() {
        let dir = tempfile::tempdir().expect("tempdir");
        let now = utc(2026, 8, 23, 14, 0);
        let w = RollingWriter::open_at(cfg(dir.path(), LogRoll::Daily, u64::MAX), now, None)
            .expect("open");
        w.write_at(line("message=before"), now);
        w.drain();
        w.roll_now(utc(2026, 8, 24, 0, 0));
        let mut attempts = HashMap::new();
        w.convert_pending(&mut attempts);
        assert!(dir.path().join("oxidant-2026-08-23.log").exists());
        assert!(!dir.path().join("oxidant-2026-08-23.parquet").exists());
        w.shutdown();
    }

    /// `LogRoll::Off` never rolls, whatever the clock or the size cap say. (`logging::init`
    /// builds no writer at all under `Off`; this pins the writer's own half of the contract.)
    #[test]
    fn roll_off_never_rolls() {
        let dir = tempfile::tempdir().expect("tempdir");
        let now = utc(2026, 8, 23, 14, 0);
        let w = RollingWriter::open_at(cfg(dir.path(), LogRoll::Off, 1), now, None).expect("open");
        w.write_at(line("message=one"), now);
        w.write_at(line("message=two"), utc(2027, 1, 1, 0, 0));
        w.drain();
        assert_eq!(names(dir.path()), vec![LIVE_LOG.to_string()]);
        w.shutdown();
    }
}
