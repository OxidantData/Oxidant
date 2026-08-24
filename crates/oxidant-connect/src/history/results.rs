//! Result spill: `$OXIDANT_DATA_DIR/history/results/<statement-id>.arrow` (§5).
//!
//! A terminal succeeded statement's rows are the one part of history that is genuinely large, so
//! this file exists mostly to say where the work happens:
//!
//! - **Never under the store mutex.** Encoding 256 MiB of Arrow IPC inside `finish()`, holding
//!   the `std::sync::Mutex` that every submit/list/status/result call takes, is the exact
//!   opposite of "a query never waits on history". The store hands an `Arc` of the batches to a
//!   dedicated writer thread — the same shape as the journal's — and releases the lock.
//! - **The pointer is journaled after the file is durable**, never before: write
//!   `<id>.arrow.tmp`, fsync it, rename, fsync `results/`, *then* append the snapshot carrying
//!   `result: {file, bytes}`. A pointer that replay reads therefore always named a real file.
//! - **The journal is the authority for GC** (§5/F13). Nothing here decides a result's lifetime;
//!   [`ResultStore::reconcile`] deletes what the folded id set does not name, and
//!   [`ResultStore::unlink`] is called by the statement's own eviction.
//!
//! `OXIDANT_RESULT_MAX_BYTES` is enforced *while writing*, not from an in-memory estimate:
//! `RecordBatch::get_array_memory_size` over-counts shared buffers, so refusing on the estimate
//! would refuse results that encode well under the cap. The cost of being right is at most one
//! `OXIDANT_RESULT_MAX_BYTES` tmp file, unlinked on the way out.

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, OnceLock};

use oxidant_loom::arrow::ipc::reader::StreamReader;
use oxidant_loom::arrow::ipc::writer::StreamWriter;
use oxidant_loom::arrow::record_batch::RecordBatch;

use super::config::{HistoryConfig, ResultPersist};
use super::fs_util;
use super::journal::Journal;
use super::record::{FoldedStatement, ResultPointer, RESULT_TOO_LARGE};

/// Bounded spill queue. Full means the disk is not keeping up with terminal results; the job is
/// dropped and counted, which costs a spill and never a statement.
const SPILL_QUEUE: usize = 256;

/// What the spill thread does with a statement's rows.
pub(crate) struct SpillJob {
    pub id: String,
    /// Shared with the store, which keeps its own reference until the release lands: the spill
    /// thread never owns the only copy of a result a client can still ask for.
    pub batches: Arc<Vec<RecordBatch>>,
    /// The statement's folded state at hand-over. The record published once the file is durable
    /// is this, plus the pointer — self-contained, exactly as §4a requires.
    pub folded: Box<FoldedStatement>,
}

/// How a spill ended, handed back to the store through [`ResultStore::set_sink`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SpillOutcome {
    /// The file is durable and its pointer is journaled.
    Spilled(ResultPointer),
    /// Refused: the encoding passed `OXIDANT_RESULT_MAX_BYTES`. `result_too_large` is journaled
    /// on the statement in place of a pointer.
    TooLarge,
    /// The disk refused the write. Nothing is on disk, nothing was journaled, and the rows stay
    /// in memory for as long as they would have anyway.
    Failed,
}

/// Told the outcome of every spill, on the spill thread. Installed by the statement store, which
/// is what actually owns the in-memory rows this frees.
type Sink = Box<dyn Fn(&str, &SpillOutcome) + Send + Sync>;

enum Msg {
    Spill(Box<SpillJob>),
    /// Flush the queue and answer — the seam tests drive a spill through synchronously.
    Drain(SyncSender<()>),
    Stop(SyncSender<()>),
}

/// The spilled-result tier: one writer thread, one directory, and the byte counter
/// `/api/status` reports as `results_on_disk_bytes`.
pub(crate) struct ResultStore {
    dir: PathBuf,
    persist: ResultPersist,
    tx: SyncSender<Msg>,
    on_disk_bytes: Arc<AtomicU64>,
    /// Spill jobs the queue had no room for. A lost spill is not a lost statement: the rows are
    /// still in memory and the journal record is unchanged, so the only cost is that `/result`
    /// answers `410 result_expired` after a restart instead of reading the file.
    dropped: Arc<AtomicU64>,
    sink: Arc<OnceLock<Sink>>,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl std::fmt::Debug for ResultStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResultStore")
            .field("dir", &self.dir)
            .field("persist", &self.persist.as_str())
            .field("on_disk_bytes", &self.on_disk_bytes())
            .finish()
    }
}

impl ResultStore {
    /// Create `results/` at 0700 and start the writer thread.
    pub(crate) fn open(cfg: &HistoryConfig, journal: Arc<Journal>) -> io::Result<Arc<Self>> {
        fs_util::create_dir_secure(&cfg.results_dir)?;
        // A `.tmp` is a spill that never got renamed: the pass is simply redone (or not — the
        // rows may be long gone), and either way a half-written result must not be counted
        // against the disk budget or mistaken for a publishable file.
        clear_tmp(&cfg.results_dir);

        let (tx, rx) = sync_channel(SPILL_QUEUE);
        let on_disk_bytes = Arc::new(AtomicU64::new(scan_bytes(&cfg.results_dir)));
        let sink: Arc<OnceLock<Sink>> = Arc::new(OnceLock::new());
        let writer = SpillWriter {
            dir: cfg.results_dir.clone(),
            max_bytes: cfg.result_max_bytes,
            journal,
            on_disk_bytes: Arc::clone(&on_disk_bytes),
            sink: Arc::clone(&sink),
        };
        let thread = std::thread::Builder::new()
            .name("oxidant-result-spill".to_string())
            .spawn(move || writer.run(rx))?;

        Ok(Arc::new(Self {
            dir: cfg.results_dir.clone(),
            persist: cfg.result_persist,
            tx,
            on_disk_bytes,
            dropped: Arc::new(AtomicU64::new(0)),
            sink,
            thread: Mutex::new(Some(thread)),
        }))
    }

    /// Install the callback the writer thread reports every outcome to. Called once, by the
    /// statement store, right after boot.
    pub(crate) fn set_sink(&self, sink: Sink) {
        // A second install would silently orphan the first store's rows; there is exactly one.
        let _ = self.sink.set(sink);
    }

    pub(crate) fn persist(&self) -> ResultPersist {
        self.persist
    }

    /// `results_on_disk_bytes` for `/api/status`.
    pub(crate) fn on_disk_bytes(&self) -> u64 {
        self.on_disk_bytes.load(Ordering::Relaxed)
    }

    /// Spill jobs the queue had no room for.
    pub(crate) fn dropped_spills(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Queue a spill. Never blocks and never runs I/O on the caller's thread — the caller is
    /// `finish()`, on a tokio worker, having just released the store mutex.
    pub(crate) fn spill(&self, job: SpillJob) {
        if !self.persist.spills_at_all() {
            return;
        }
        if let Err(TrySendError::Full(_)) = self.tx.try_send(Msg::Spill(Box::new(job))) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Read a spilled result back. Blocking by construction — the caller wraps it in
    /// `spawn_blocking` so a 256 MiB decode never sits on a tokio worker.
    pub(crate) fn read(&self, id: &str) -> io::Result<Vec<RecordBatch>> {
        let path = self.path_for(id);
        let file = std::fs::File::open(&path)?;
        let reader = StreamReader::try_new(std::io::BufReader::new(file), None)
            .map_err(|e| io_other(&format!("arrow ipc: {e}")))?;
        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch.map_err(|e| io_other(&format!("arrow ipc: {e}")))?);
        }
        Ok(batches)
    }

    /// Delete one statement's result file, if it has one. Called by the statement's own eviction
    /// — the journal is the authority (§5), so this never decides a lifetime of its own.
    pub(crate) fn unlink(&self, id: &str) -> bool {
        let path = self.path_for(id);
        let Ok(meta) = std::fs::metadata(&path) else {
            return false;
        };
        if std::fs::remove_file(&path).is_err() {
            return false;
        }
        self.on_disk_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |b| {
                Some(b.saturating_sub(meta.len()))
            })
            .ok();
        fs_util::fsync_dir(&self.dir);
        true
    }

    /// Delete every result file no live statement names — the boot pass that closes the crash
    /// window between "tombstone appended" and "file unlinked" (§5, F13).
    ///
    /// `live` must be the union of both tiers, not just the folded set: a statement still in the
    /// hot tier has no snapshot on disk yet, and deleting its result would be the one thing
    /// retention must never do.
    pub(crate) fn reconcile(&self, live: &std::collections::HashSet<String>) -> usize {
        let mut removed = 0;
        for (id, _) in self.files() {
            if live.contains(&id) {
                continue;
            }
            if self.unlink(&id) {
                removed += 1;
            }
        }
        if removed > 0 {
            tracing::info!(
                removed,
                dir = %self.dir.display(),
                "statement history: unlinked result files no statement references"
            );
        }
        removed
    }

    /// Every published result file as `(statement-id, size)`, oldest-modified first — the order
    /// the disk sweeper prunes in.
    pub(crate) fn files(&self) -> Vec<(String, u64)> {
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return Vec::new();
        };
        let mut out: Vec<(String, u64, std::time::SystemTime)> = Vec::new();
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            let Some(id) = name.strip_suffix(".arrow") else {
                continue;
            };
            let Ok(meta) = entry.metadata() else { continue };
            if !meta.is_file() {
                continue;
            }
            let mtime = meta.modified().unwrap_or(std::time::UNIX_EPOCH);
            out.push((id.to_string(), meta.len(), mtime));
        }
        out.sort_by(|a, b| a.2.cmp(&b.2).then_with(|| a.0.cmp(&b.0)));
        out.into_iter().map(|(id, len, _)| (id, len)).collect()
    }

    fn path_for(&self, id: &str) -> PathBuf {
        self.dir.join(ResultPointer::file_name(id))
    }

    /// Block until every queued spill has been written (tests, and the disk sweeper, which must
    /// not race a spill it is about to account for).
    pub(crate) fn drain_blocking(&self) {
        let (tx, rx) = sync_channel(1);
        if self.tx.send(Msg::Drain(tx)).is_ok() {
            let _ = rx.recv();
        }
    }

    /// Flush and stop the writer thread — the clean-shutdown seam a restart test needs.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn shutdown(&self) {
        let (tx, rx) = sync_channel(1);
        if self.tx.send(Msg::Stop(tx)).is_ok() {
            let _ = rx.recv();
        }
        if let Some(handle) = self.thread.lock().expect("spill thread poisoned").take() {
            let _ = handle.join();
        }
    }
}

/// The spill thread's state. The only place in this design that writes `results/`.
struct SpillWriter {
    dir: PathBuf,
    max_bytes: u64,
    journal: Arc<Journal>,
    on_disk_bytes: Arc<AtomicU64>,
    sink: Arc<OnceLock<Sink>>,
}

impl SpillWriter {
    fn run(self, rx: Receiver<Msg>) {
        while let Ok(msg) = rx.recv() {
            match msg {
                Msg::Spill(job) => self.handle(*job),
                Msg::Drain(done) => {
                    let _ = done.send(());
                }
                Msg::Stop(done) => {
                    let _ = done.send(());
                    return;
                }
            }
        }
    }

    fn handle(&self, job: SpillJob) {
        let outcome = match self.write(&job.id, &job.batches) {
            Ok(Some(pointer)) => {
                self.on_disk_bytes
                    .fetch_add(pointer.bytes, Ordering::Relaxed);
                // The pointer is journaled only now — after the rename and the `results/` fsync
                // — so a pointer replay reads has always named a file that reached the disk.
                let mut folded = *job.folded;
                folded.result = Some(pointer.clone());
                folded.result_refused = None;
                folded.last_seq = self.journal.next_seq();
                self.journal.append_retained(folded.to_snapshot());
                SpillOutcome::Spilled(pointer)
            }
            Ok(None) => {
                tracing::warn!(
                    statement = %job.id,
                    max_bytes = self.max_bytes,
                    "result spill refused: the encoding is past OXIDANT_RESULT_MAX_BYTES; \
                     recording result_too_large (the live /result and CSV paths are the answer)"
                );
                let mut folded = *job.folded;
                folded.result = None;
                folded.result_refused = Some(RESULT_TOO_LARGE.to_string());
                folded.last_seq = self.journal.next_seq();
                self.journal.append_retained(folded.to_snapshot());
                SpillOutcome::TooLarge
            }
            Err(e) => {
                tracing::warn!(
                    statement = %job.id,
                    error = %e,
                    "result spill failed; the rows stay in memory and history is degraded, \
                     execution is not"
                );
                self.journal.mark_degraded();
                SpillOutcome::Failed
            }
        };
        if let Some(sink) = self.sink.get() {
            sink(&job.id, &outcome);
        }
    }

    /// Encode, publish, and return the pointer. `Ok(None)` is the refusal: past `max_bytes`.
    ///
    /// The five steps §4d makes mandatory for every rename in this design: write the tmp, fsync
    /// the file, rename, fsync the directory, and only then let the caller journal the pointer.
    fn write(&self, id: &str, batches: &[RecordBatch]) -> io::Result<Option<ResultPointer>> {
        // Nothing to encode and no schema to encode it against: an Arrow IPC stream cannot be
        // written without one, so a zero-batch result has no file and `/result` answers
        // `410 result_expired` for it after a restart. Stated rather than faked.
        let Some(first) = batches.first() else {
            return Ok(None);
        };
        let name = ResultPointer::file_name(id);
        let target = self.dir.join(&name);
        let tmp = self.dir.join(format!("{name}.tmp"));

        let mut counted = Counting {
            inner: std::io::BufWriter::new(fs_util::create_secure(&tmp)?),
            written: 0,
            limit: self.max_bytes,
            exceeded: false,
        };
        let encoded = (|| -> io::Result<()> {
            let mut writer = StreamWriter::try_new(&mut counted, first.schema_ref())
                .map_err(|e| io_other(&format!("arrow ipc: {e}")))?;
            for batch in batches {
                writer
                    .write(batch)
                    .map_err(|e| io_other(&format!("arrow ipc: {e}")))?;
            }
            writer
                .finish()
                .map_err(|e| io_other(&format!("arrow ipc: {e}")))?;
            Ok(())
        })();
        let too_large = counted.exceeded;
        let bytes = counted.written;
        let flushed = counted
            .inner
            .flush()
            .and_then(|()| counted.inner.get_ref().sync_all());

        if too_large || encoded.is_err() || flushed.is_err() {
            let _ = std::fs::remove_file(&tmp);
            fs_util::fsync_dir(&self.dir);
            if too_large {
                return Ok(None);
            }
            return Err(encoded.err().or(flushed.err()).unwrap_or_else(|| {
                io_other("result spill failed for a reason the writer did not record")
            }));
        }
        fs_util::rename_durable(&tmp, &target, &self.dir)?;
        Ok(Some(ResultPointer { file: name, bytes }))
    }
}

/// A writer that refuses to exceed `limit`, so the cap is enforced on the encoding rather than
/// on an in-memory estimate that over-counts shared Arrow buffers.
struct Counting<W: Write> {
    inner: W,
    written: u64,
    limit: u64,
    exceeded: bool,
}

impl<W: Write> Write for Counting<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.written.saturating_add(buf.len() as u64) > self.limit {
            self.exceeded = true;
            return Err(io_other(RESULT_TOO_LARGE));
        }
        let n = self.inner.write(buf)?;
        self.written += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn io_other(msg: &str) -> io::Error {
    // `io::Error::other` is 1.74; this crate's MSRV is 1.72.
    io::Error::new(io::ErrorKind::Other, msg.to_string())
}

/// Total size of every published result file — the boot value of `results_on_disk_bytes`.
fn scan_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .to_string()
                .ends_with(".arrow")
        })
        .filter_map(|e| e.metadata().ok())
        .filter(|m| m.is_file())
        .map(|m| m.len())
        .sum()
}

/// Delete every `*.arrow.tmp` — a spill that never reached its rename.
fn clear_tmp(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut removed = false;
    for entry in entries.flatten() {
        if entry.file_name().to_string_lossy().ends_with(".tmp") {
            removed |= std::fs::remove_file(entry.path()).is_ok();
        }
    }
    if removed {
        fs_util::fsync_dir(dir);
    }
}
