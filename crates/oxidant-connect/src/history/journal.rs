//! The append-only statement journal: `$OXIDANT_DATA_DIR/history/statements/seg-NNNNNN.jsonl`.
//!
//! One dedicated writer thread owns every file handle. Producers (the statement store, on
//! whatever task or thread it happens to be) hand records to a bounded channel and never touch
//! the disk, which is what makes the guarantee in §7 true:
//!
//! > *a statement's terminal state is durable before its client is told the statement finished;
//! > intermediate lifecycle events may be lost, up to one flush interval, on a crash.*
//!
//! Concretely: `running` records are dropped and counted when the channel is full — they are
//! progress chatter and the fold needs none of them — and so are `tombstone` records, whose
//! loss is self-healing (the statement is folded again at the next boot and re-evicted by the
//! next sweep). `submitted` and `snapshot` records are **never** dropped and never coalesced:
//! when the channel has no room they go to an overflow queue the writer drains before anything
//! else, so a producer neither blocks nor loses the record. A terminal record carries a oneshot
//! the writer completes *after* its fsync — and only then — and the response path (not the
//! query) waits on that for at most `OXIDANT_HISTORY_ACK_TIMEOUT_MS`.
//!
//! No producer ever blocks on the disk. `append`, `append_retained` and `append_durable` are all
//! non-blocking, which is what lets `finish()` be called from a tokio worker without a stalled
//! disk parking that worker (§7: *"execution never waits on history"*).

use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use super::config::HistoryConfig;
use super::fs_util;
use super::record::{Fold, FoldedStatement, JournalRecord};

/// Bounded writer channel (§7). Full means the disk is not keeping up.
const CHANNEL_CAPACITY: usize = 4096;
/// Compaction runs when this share of the records in sealed inputs is superseded.
const COMPACT_SUPERSEDED_RATIO: f64 = 0.5;
/// Hard bound on records the channel had no room for.
///
/// The overflow queue is what makes `submitted` and `snapshot` un-droppable without blocking
/// their producer, but "never dropped" cannot mean "unbounded": a disk that never answers would
/// otherwise trade a lost history record for an OOM. Past this many queued records even a
/// retained one is dropped, counted, and the journal is degraded — honestly, and in memory the
/// process can survive.
const OVERFLOW_CAPACITY: usize = 65_536;

/// A record the channel had no room for, with the ack (if any) its client is waiting on.
type Overflow = Mutex<VecDeque<(Box<JournalRecord>, Option<tokio::sync::oneshot::Sender<()>>)>>;

fn seg_name(n: u64) -> String {
    format!("seg-{n:06}.jsonl")
}

fn gen_name(n: u64) -> String {
    format!("gen-{n:06}.jsonl")
}

/// Numerically parse `<prefix>NNNNNN.jsonl`. Directory iteration order is never trusted and the
/// index is never compared as a string — `seg-000010` must sort after `seg-000009`, and will keep
/// doing so past six digits.
fn parse_index(name: &str, prefix: &str) -> Option<u64> {
    name.strip_prefix(prefix)?
        .strip_suffix(".jsonl")?
        .parse()
        .ok()
}

/// Every `seg-*.jsonl` in the statements dir, ascending by parsed index.
fn segments(dir: &Path) -> Vec<(u64, PathBuf)> {
    indexed_files(dir, "seg-")
}

/// Every `gen-*.jsonl` in the compacted dir, ascending by parsed index.
fn generations(dir: &Path) -> Vec<(u64, PathBuf)> {
    indexed_files(dir, "gen-")
}

fn indexed_files(dir: &Path, prefix: &str) -> Vec<(u64, PathBuf)> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(index) = parse_index(&name, prefix) {
            out.push((index, entry.path()));
        }
    }
    out.sort_by_key(|(index, _)| *index);
    out
}

/// What the writer thread accepts.
enum Msg {
    Append(Box<JournalRecord>, Option<tokio::sync::oneshot::Sender<()>>),
    /// Flush + fsync now and answer (tests, shutdown).
    Sync(std::sync::mpsc::SyncSender<()>),
    /// Seal the open segment and run a compaction pass unconditionally. Compaction otherwise
    /// runs on its own at roll time; this is the seam a test drives it through.
    #[cfg_attr(not(test), allow(dead_code))]
    Compact(std::sync::mpsc::SyncSender<()>),
    /// Flush and stop the writer thread — a clean shutdown. The server itself just exits, which
    /// is the crash path the fold is built to survive, so today only tests send this.
    #[cfg_attr(not(test), allow(dead_code))]
    Stop(std::sync::mpsc::SyncSender<()>),
}

/// Handle on the journal: a channel to the writer thread plus the counters `/api/status` reads.
pub(crate) struct Journal {
    tx: SyncSender<Msg>,
    /// Records that must not be dropped and found the channel full. The writer drains this
    /// before dispatching anything it received, so an overflowed record is on disk before the
    /// next `Sync`/`Stop`/`Compact` answers.
    overflow: Arc<Overflow>,
    /// Monotonic sequence shared with the statement store: a statement's submit sequence and
    /// every later record's write sequence come from here, so both are globally ordered.
    seq: AtomicU64,
    dropped: Arc<AtomicU64>,
    /// Appends or fsyncs the disk refused (ENOSPC/EIO) — `history_write_failures`. A terminal
    /// record counted here was never acked, so its client was told `history: degraded`.
    write_failures: Arc<AtomicU64>,
    degraded: Arc<AtomicBool>,
    statements_dir: PathBuf,
    /// Joined by [`Journal::shutdown`].
    #[cfg_attr(not(test), allow(dead_code))]
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl std::fmt::Debug for Journal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Journal")
            .field("statements_dir", &self.statements_dir)
            .field("dropped", &self.dropped_events())
            .field("write_failures", &self.write_failures())
            .field("degraded", &self.is_degraded())
            .finish()
    }
}

impl Journal {
    /// Recover, replay, and start the writer.
    ///
    /// Returns the folded state of everything on disk. Boot is never blocked by history: a
    /// corrupt file is quarantined and the remaining files are folded around it.
    pub(crate) fn open(cfg: &HistoryConfig) -> std::io::Result<(Arc<Journal>, Fold)> {
        fs_util::create_dir_secure(&cfg.statements_dir)?;
        fs_util::create_dir_secure(&cfg.compacted_dir)?;
        recover_swap(cfg);

        let fold = replay(cfg);
        let next_seq = fold.max_seq + 1;
        // A fresh segment every boot: the writer never reopens a file another process may have
        // been mid-append to, and every existing segment is therefore sealed and compactable.
        let next_segment = segments(&cfg.statements_dir)
            .last()
            .map(|(i, _)| i + 1)
            .unwrap_or(0);

        let (tx, rx) = sync_channel(CHANNEL_CAPACITY);
        let dropped = Arc::new(AtomicU64::new(0));
        let write_failures = Arc::new(AtomicU64::new(0));
        let degraded = Arc::new(AtomicBool::new(false));
        let overflow: Arc<Overflow> = Arc::new(Mutex::new(VecDeque::new()));
        let writer = Writer {
            cfg: cfg.clone(),
            file: None,
            segment: next_segment,
            len: 0,
            dirty: false,
            degraded: Arc::clone(&degraded),
            write_failures: Arc::clone(&write_failures),
            overflow: Arc::clone(&overflow),
        };
        let thread = std::thread::Builder::new()
            .name("oxidant-history".to_string())
            .spawn(move || writer.run(rx))?;

        Ok((
            Arc::new(Journal {
                tx,
                overflow,
                seq: AtomicU64::new(next_seq),
                dropped,
                write_failures,
                degraded,
                statements_dir: cfg.statements_dir.clone(),
                thread: Mutex::new(Some(thread)),
            }),
            fold,
        ))
    }

    /// The next global sequence. Also the statement store's submit sequence, so `seq` orders
    /// statements and records in one line.
    pub(crate) fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst)
    }

    /// Append a best-effort record: `running` chatter and tombstones. Dropped and counted if
    /// the writer is behind (§7). Never blocks.
    pub(crate) fn append(&self, rec: JournalRecord) {
        if let Err(TrySendError::Full(_)) = self.tx.try_send(Msg::Append(Box::new(rec), None)) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            self.degraded.store(true, Ordering::Relaxed);
        }
    }

    /// Append a record that must not be lost, without an ack: the `submitted` record (§4a — a
    /// crash mid-statement still leaves a trace *with its SQL*) and the boot correction pass.
    ///
    /// A full channel sends it to the overflow queue rather than dropping it, and rather than
    /// parking the producer — which is on a tokio worker.
    pub(crate) fn append_retained(&self, rec: JournalRecord) {
        match self.tx.try_send(Msg::Append(Box::new(rec), None)) {
            Ok(()) => {}
            Err(TrySendError::Full(Msg::Append(rec, ack))) => {
                self.push_overflow(rec, ack);
            }
            Err(_) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                self.degraded.store(true, Ordering::Relaxed);
            }
        }
    }

    /// Append a record that must not be lost, and hand back the ack the response waits on.
    ///
    /// Never blocks: a full channel overflows rather than parking the caller, so a stalled disk
    /// delays a *response* (bounded by `OXIDANT_HISTORY_ACK_TIMEOUT_MS`, awaited in
    /// `await_durable`) and never a tokio worker. `None` means the record could not be held at
    /// all — the overflow bound is exhausted, or the writer is gone — and the caller degrades.
    pub(crate) fn append_durable(
        &self,
        rec: JournalRecord,
    ) -> Option<tokio::sync::oneshot::Receiver<()>> {
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        match self.tx.try_send(Msg::Append(Box::new(rec), Some(ack_tx))) {
            Ok(()) => Some(ack_rx),
            Err(TrySendError::Full(Msg::Append(rec, ack))) => {
                self.push_overflow(rec, ack).then_some(ack_rx)
            }
            Err(_) => {
                self.degraded.store(true, Ordering::Relaxed);
                None
            }
        }
    }

    /// Hold a record the channel had no room for. `false` means even the overflow bound is
    /// exhausted: the record is dropped, counted, and the journal is degraded.
    fn push_overflow(
        &self,
        rec: Box<JournalRecord>,
        ack: Option<tokio::sync::oneshot::Sender<()>>,
    ) -> bool {
        let mut queue = self.overflow.lock().expect("journal overflow poisoned");
        if queue.len() >= OVERFLOW_CAPACITY {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            self.degraded.store(true, Ordering::Relaxed);
            return false;
        }
        queue.push_back((rec, ack));
        true
    }

    /// Records dropped because the writer was behind — `history_dropped_events`.
    pub(crate) fn dropped_events(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Appends or fsyncs the disk refused — `history_write_failures`. Moves on exactly the
    /// events that make a terminal answer say `history: degraded` instead of implying durability.
    pub(crate) fn write_failures(&self) -> u64 {
        self.write_failures.load(Ordering::Relaxed)
    }

    /// Has a write failed (ENOSPC/EIO) or a record been dropped since the last success?
    pub(crate) fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::Relaxed)
    }

    /// Record that a client was answered without its terminal record being durable.
    pub(crate) fn mark_degraded(&self) {
        self.degraded.store(true, Ordering::Relaxed);
    }

    /// Block until everything queued so far is fsynced (tests, and the boot correction pass).
    pub(crate) fn sync_blocking(&self) {
        let (tx, rx) = sync_channel(1);
        if self.tx.send(Msg::Sync(tx)).is_ok() {
            let _ = rx.recv();
        }
    }

    /// Seal and compact now, synchronously (tests; the writer does this on its own at roll time).
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn compact_blocking(&self) {
        let (tx, rx) = sync_channel(1);
        if self.tx.send(Msg::Compact(tx)).is_ok() {
            let _ = rx.recv();
        }
    }

    /// Stop the writer thread after flushing. Used by tests to simulate a clean shutdown; the
    /// server itself simply exits, which is the crash path the fold is built to survive.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn shutdown(&self) {
        let (tx, rx) = sync_channel(1);
        if self.tx.send(Msg::Stop(tx)).is_ok() {
            let _ = rx.recv();
        }
        if let Some(handle) = self.thread.lock().expect("journal thread poisoned").take() {
            let _ = handle.join();
        }
    }
}

/// Boot recovery for the five-step compaction swap (§4d).
///
/// A `.tmp` is a generation that was never renamed: delete it, the pass is simply redone. A
/// `.done` marker means the rename landed but the input unlinks may not have: redo them, then
/// remove the marker. A crash anywhere leaves at worst a double-fold, which the seq-monotone fold
/// absorbs.
fn recover_swap(cfg: &HistoryConfig) {
    let Ok(entries) = std::fs::read_dir(&cfg.compacted_dir) else {
        return;
    };
    let mut markers = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".tmp") {
            let _ = std::fs::remove_file(entry.path());
        } else if name.ends_with(".done") {
            markers.push(entry.path());
        }
    }
    for marker in markers {
        if let Ok(body) = std::fs::read_to_string(&marker) {
            for input in body.lines().filter(|l| !l.trim().is_empty()) {
                let path = cfg.statements_dir.join(input);
                let _ = std::fs::remove_file(path);
            }
        }
        fs_util::fsync_dir(&cfg.statements_dir);
        fs_util::fsync_dir(&cfg.compacted_dir);
        let _ = std::fs::remove_file(&marker);
        fs_util::fsync_dir(&cfg.compacted_dir);
    }
}

/// Read the journal back into a [`Fold`].
///
/// Files are read newest-first — live segments descending, then compacted generations descending
/// — and reading stops once `max_records` statements are in hand, which is what bounds boot
/// (Goal 5). Order does not affect the result: the fold is seq-monotone, so a compacted snapshot
/// that is newer than a live event for the same id wins whichever file was read first.
fn replay(cfg: &HistoryConfig) -> Fold {
    let mut fold = Fold::default();
    let mut files: Vec<PathBuf> = segments(&cfg.statements_dir)
        .into_iter()
        .rev()
        .map(|(_, p)| p)
        .collect();
    files.extend(
        generations(&cfg.compacted_dir)
            .into_iter()
            .rev()
            .map(|(_, p)| p),
    );
    for path in files {
        if fold.statements.len() >= cfg.max_records {
            // Bounded replay: the rest is older than the cap and is left for the sweeper.
            break;
        }
        read_into(&path, &mut fold);
    }
    fold
}

/// Fold one file, quarantining it at the first line that does not parse.
///
/// The bad line stops replay *of that file*; the file is renamed `…jsonl.corrupt` — kept, not
/// deleted — and boot continues with the rest. History must never be the reason the engine does
/// not start.
fn read_into(path: &Path, fold: &mut Fold) {
    let Ok(file) = File::open(path) else {
        return;
    };
    let reader = BufReader::new(file);
    let mut corrupt = false;
    for line in reader.lines() {
        let Ok(line) = line else {
            corrupt = true;
            break;
        };
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<JournalRecord>(&line) {
            Ok(rec) => fold.apply(rec),
            Err(_) => {
                corrupt = true;
                break;
            }
        }
    }
    if corrupt {
        let quarantine = quarantine_path(path);
        tracing::warn!(
            file = %path.display(),
            quarantined = %quarantine.display(),
            "statement journal: corrupt record, quarantining the file and continuing boot"
        );
        let _ = std::fs::rename(path, &quarantine);
        if let Some(parent) = path.parent() {
            fs_util::fsync_dir(parent);
        }
    }
}

/// `x.jsonl` → `x.jsonl.corrupt`, `x.jsonl.corrupt.2`, … — a quarantine never overwrites an
/// earlier one, because the earlier one is evidence.
fn quarantine_path(path: &Path) -> PathBuf {
    let base = format!("{}.corrupt", path.display());
    let first = PathBuf::from(&base);
    if !first.exists() {
        return first;
    }
    for n in 2..1000 {
        let candidate = PathBuf::from(format!("{base}.{n}"));
        if !candidate.exists() {
            return candidate;
        }
    }
    first
}

/// The writer thread's state: exactly one open segment, and the only file handles in the design.
struct Writer {
    cfg: HistoryConfig,
    file: Option<File>,
    segment: u64,
    len: u64,
    dirty: bool,
    degraded: Arc<AtomicBool>,
    write_failures: Arc<AtomicU64>,
    overflow: Arc<Overflow>,
}

impl Writer {
    fn run(mut self, rx: Receiver<Msg>) {
        // Every segment on disk is sealed at this point (boot always starts a fresh one), so a
        // compaction pass can run here — on this thread, off the boot path, and gated on the
        // same superseded ratio as a roll-time pass.
        self.compact(false);
        let mut last_sync = Instant::now();
        let overflow = Arc::clone(&self.overflow);
        loop {
            let msg = rx.recv_timeout(self.cfg.flush_interval);
            // Drain between the receive and the dispatch, never after it: a producer that
            // overflowed a record and *then* asked for a `Sync`/`Stop` must find that record on
            // disk when the answer comes back.
            while let Some((rec, ack)) = pop(&overflow) {
                self.handle_append(&rec, ack, &mut last_sync);
            }
            match msg {
                Ok(Msg::Append(rec, ack)) => {
                    self.handle_append(&rec, ack, &mut last_sync);
                }
                Ok(Msg::Sync(done)) => {
                    self.sync();
                    last_sync = Instant::now();
                    let _ = done.send(());
                }
                Ok(Msg::Compact(done)) => {
                    self.seal();
                    self.compact(true);
                    let _ = done.send(());
                }
                Ok(Msg::Stop(done)) => {
                    self.sync();
                    let _ = done.send(());
                    return;
                }
                Err(RecvTimeoutError::Timeout) => {
                    if self.dirty && last_sync.elapsed() >= self.cfg.flush_interval {
                        self.sync();
                        last_sync = Instant::now();
                    }
                }
                Err(RecvTimeoutError::Disconnected) => {
                    self.sync();
                    return;
                }
            }
        }
    }

    fn path(&self) -> PathBuf {
        self.cfg.statements_dir.join(seg_name(self.segment))
    }

    /// Write one record and, when a client is waiting on it, fsync before answering.
    ///
    /// The ack is completed **only** when both the append and the fsync succeeded; otherwise the
    /// sender is dropped, which `await_durable` reads as "not durable" (§7). Acking a write that
    /// never landed is the one thing this design must not do.
    fn handle_append(
        &mut self,
        rec: &JournalRecord,
        ack: Option<tokio::sync::oneshot::Sender<()>>,
        last_sync: &mut Instant,
    ) {
        let written = self.append(rec);
        if let Some(ack) = ack {
            let durable = written && self.sync();
            *last_sync = Instant::now();
            if durable {
                let _ = ack.send(());
            }
        }
        self.roll_if_full();
    }

    /// `true` when the record reached the page cache. `false` is a real failure the caller must
    /// propagate — it is never safe to ack a record this returned `false` for.
    fn append(&mut self, rec: &JournalRecord) -> bool {
        let Ok(mut line) = serde_json::to_string(rec) else {
            self.write_failures.fetch_add(1, Ordering::Relaxed);
            self.degraded.store(true, Ordering::Relaxed);
            return false;
        };
        line.push('\n');
        if self.file.is_none() {
            match fs_util::append_secure(&self.path()) {
                Ok(file) => {
                    self.len = file.metadata().map(|m| m.len()).unwrap_or(0);
                    self.file = Some(file);
                    fs_util::fsync_dir(&self.cfg.statements_dir);
                }
                Err(e) => {
                    self.fail("open segment", &e);
                    return false;
                }
            }
        }
        let Some(file) = self.file.as_mut() else {
            return false;
        };
        match file.write_all(line.as_bytes()) {
            Ok(()) => {
                self.len += line.len() as u64;
                self.dirty = true;
                self.degraded.store(false, Ordering::Relaxed);
                true
            }
            Err(e) => {
                self.fail("append", &e);
                false
            }
        }
    }

    /// `true` when everything appended so far is on the disk. Nothing dirty is vacuously true;
    /// a refused `fsync_data` is not.
    fn sync(&mut self) -> bool {
        if !self.dirty {
            return true;
        }
        let Some(file) = self.file.as_mut() else {
            return false;
        };
        match file.sync_data() {
            Ok(()) => {
                self.dirty = false;
                self.degraded.store(false, Ordering::Relaxed);
                true
            }
            Err(e) => {
                self.fail("fsync", &e);
                false
            }
        }
    }

    /// Close the open segment. The seal point is the synchronization primitive between the
    /// writer and compaction: a sealed segment is one nothing will append to again.
    fn seal(&mut self) {
        self.sync();
        if self.file.take().is_some() {
            fs_util::fsync_dir(&self.cfg.statements_dir);
            self.segment += 1;
            self.len = 0;
        }
    }

    fn roll_if_full(&mut self) {
        if self.len >= self.cfg.segment_max_bytes {
            self.seal();
            self.compact(false);
        }
    }

    /// Rewrite the sealed inputs as one generation of self-contained snapshots.
    ///
    /// Runs on the writer thread, which is why there is no lock: the thread that would append to
    /// a segment is the one deciding to compact it, and it has already sealed everything it is
    /// about to read.
    fn compact(&mut self, force: bool) {
        if let Err(e) = compact_sealed(&self.cfg, self.segment, force) {
            tracing::warn!(error = %e, "statement journal: compaction pass failed");
        }
    }

    fn fail(&mut self, what: &str, e: &std::io::Error) {
        self.degraded.store(true, Ordering::Relaxed);
        self.write_failures.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            error = %e,
            operation = what,
            "statement journal write failed; history is degraded, execution is not"
        );
        // A failed handle is not reused: the next append reopens, which is also what makes a
        // recovered disk flip the flag back without a restart.
        self.file = None;
        self.dirty = false;
    }
}

/// Fold every sealed input and, if enough of it is superseded, publish one new generation.
fn compact_sealed(cfg: &HistoryConfig, open_segment: u64, force: bool) -> std::io::Result<bool> {
    let sealed: Vec<(u64, PathBuf)> = segments(&cfg.statements_dir)
        .into_iter()
        .filter(|(index, _)| *index < open_segment)
        .collect();
    let gens = generations(&cfg.compacted_dir);
    if sealed.is_empty() {
        return Ok(false);
    }

    let mut fold = Fold::default();
    let mut inputs: Vec<String> = Vec::new();
    for (_, path) in &gens {
        read_into(path, &mut fold);
        inputs.push(format!("compacted/{}", file_name(path)));
    }
    for (_, path) in &sealed {
        read_into(path, &mut fold);
        inputs.push(file_name(path));
    }
    let surviving = fold.statements.len() as f64;
    let records = fold.records as f64;
    let superseded = if records > 0.0 {
        1.0 - (surviving / records)
    } else {
        0.0
    };
    if !force && superseded < COMPACT_SUPERSEDED_RATIO {
        return Ok(false);
    }

    let next_gen = gens.last().map(|(i, _)| i + 1).unwrap_or(0);
    let target = cfg.compacted_dir.join(gen_name(next_gen));
    let tmp = cfg
        .compacted_dir
        .join(format!("{}.tmp", gen_name(next_gen)));

    // 1. write the generation and fsync the file.
    {
        let mut out = fs_util::create_secure(&tmp)?;
        let ordered: BTreeMap<u64, &FoldedStatement> =
            fold.statements.values().map(|st| (st.seq, st)).collect();
        for st in ordered.values() {
            let mut line = serde_json::to_string(&st.to_snapshot())?;
            line.push('\n');
            out.write_all(line.as_bytes())?;
        }
        out.sync_all()?;
    }
    // 2. rename into place and fsync the directory — a rename is not durable without it.
    fs_util::rename_durable(&tmp, &target, &cfg.compacted_dir)?;
    // 3. record the swap intent: which inputs this generation replaces.
    let marker = cfg.compacted_dir.join(format!("gen-{next_gen:06}.done"));
    {
        let mut out = fs_util::create_secure(&marker)?;
        out.write_all(inputs.join("\n").as_bytes())?;
        out.write_all(b"\n")?;
        out.sync_all()?;
    }
    fs_util::fsync_dir(&cfg.compacted_dir);
    // 4. unlink the inputs.
    for input in &inputs {
        let _ = std::fs::remove_file(cfg.statements_dir.join(input));
    }
    fs_util::fsync_dir(&cfg.statements_dir);
    fs_util::fsync_dir(&cfg.compacted_dir);
    // 5. the marker goes last: while it exists, boot redoes step 4.
    let _ = std::fs::remove_file(&marker);
    fs_util::fsync_dir(&cfg.compacted_dir);
    Ok(true)
}

/// Take the next overflowed record, holding the queue lock for exactly that long.
fn pop(
    overflow: &Overflow,
) -> Option<(Box<JournalRecord>, Option<tokio::sync::oneshot::Sender<()>>)> {
    overflow
        .lock()
        .expect("journal overflow poisoned")
        .pop_front()
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::history::record::{
        FoldedStatement, JournalRecord, RecordKind, Source, StatementStatus, RECORD_VERSION,
    };

    fn cfg_for(dir: &Path) -> HistoryConfig {
        let mut cfg = HistoryConfig::for_root(dir);
        cfg.flush_interval = Duration::from_millis(20);
        cfg
    }

    fn folded(id: &str, seq: u64, status: StatementStatus) -> FoldedStatement {
        FoldedStatement {
            id: id.to_string(),
            client_op_id: Some("op-7".to_string()),
            session: Some("sess-1".to_string()),
            source: Source::Connect,
            sql: format!("SELECT {seq}"),
            sql_encoding: "text".to_string(),
            status,
            error: None,
            schema: Some(vec![("n".to_string(), "Int64".to_string())]),
            rows: Some(1),
            submitted_at_ms: 1_700_000_000_000 + seq as i64,
            duration_ms: Some(7),
            seq,
            last_seq: seq,
            rank: 2,
        }
    }

    fn submitted(id: &str, seq: u64) -> JournalRecord {
        JournalRecord {
            v: RECORD_VERSION,
            kind: RecordKind::Submitted,
            seq,
            last_seq: None,
            id: id.to_string(),
            client_op_id: None,
            session: Some("sess-1".to_string()),
            source: Some("rest".to_string()),
            sql: Some(format!("SELECT {seq}")),
            sql_encoding: Some("text".to_string()),
            status: Some(StatementStatus::Pending),
            error: None,
            schema: None,
            rows: None,
            submitted_at_ms: 1_700_000_000_000 + seq as i64,
            duration_ms: None,
            ts: crate::history::record::now_rfc3339(),
        }
    }

    /// The `running` record `mark_running` builds: progress chatter, and — the detail H3 turns
    /// on — carrying **no** `sql`, while ranking above the `submitted` record that does.
    fn running(id: &str, seq: u64) -> JournalRecord {
        JournalRecord {
            kind: RecordKind::Running,
            sql: None,
            sql_encoding: None,
            status: Some(StatementStatus::Running),
            ..submitted(id, seq)
        }
    }

    #[test]
    fn a_terminal_ack_means_the_record_is_on_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = cfg_for(dir.path());
        let (journal, fold) = Journal::open(&cfg).expect("open");
        assert!(fold.statements.is_empty());
        let ack = journal
            .append_durable(folded("stmt-a", 1, StatementStatus::Succeeded).to_snapshot())
            .expect("queued");
        // The ack fires only after the writer's fsync; blocking on it here is the same wait the
        // response path does.
        futures::executor::block_on(ack).expect("acked");
        let body = std::fs::read_to_string(cfg.statements_dir.join(seg_name(0))).expect("segment");
        assert!(body.contains("stmt-a"), "{body}");
        journal.shutdown();
    }

    /// H1: an append or an fsync the disk refused must never resolve the durability ack. The
    /// ack is the whole of §7's promise — "on ack, the client's answer is durable" — so a
    /// terminal record that never reached the disk has to leave the sender *dropped*, which is
    /// what `await_durable` reads as `history: degraded`.
    #[test]
    fn a_failed_write_never_resolves_the_durability_ack() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = cfg_for(dir.path());
        let (journal, _) = Journal::open(&cfg).expect("open");
        // The writer opens its segment lazily, on the first append. A directory where that file
        // belongs makes every open fail with EISDIR — the ENOSPC/EIO shape, deterministically,
        // and without a filesystem fault injector.
        std::fs::create_dir(cfg.statements_dir.join(seg_name(0))).expect("block the segment");

        let ack = journal
            .append_durable(folded("stmt-a", 1, StatementStatus::Succeeded).to_snapshot())
            .expect("queued");
        assert!(
            futures::executor::block_on(ack).is_err(),
            "a write that failed must drop the ack, not complete it"
        );
        assert!(
            journal.is_degraded(),
            "and the journal knows it is degraded"
        );
        assert!(
            journal.write_failures() >= 1,
            "and the failure counter moved"
        );
        journal.shutdown();
    }

    #[test]
    fn crash_during_append_keeps_acked_terminals_and_marks_non_terminals_interrupted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = cfg_for(dir.path());
        let (journal, _) = Journal::open(&cfg).expect("open");
        // One statement that finished (acked, so durable) and one still running (chatter, which
        // may or may not have made it — either way it must replay as failed, not running).
        let ack = journal
            .append_durable(folded("stmt-done", 1, StatementStatus::Succeeded).to_snapshot())
            .expect("queued");
        futures::executor::block_on(ack).expect("acked");
        journal.append(submitted("stmt-live", 2));
        journal.sync_blocking();
        // No shutdown: drop the handle as a crash would, then boot again.
        drop(journal);

        let (journal2, mut fold) = Journal::open(&cfg).expect("reopen");
        let marked = fold.mark_interrupted();
        assert_eq!(marked, vec!["stmt-live".to_string()]);
        assert_eq!(
            fold.statements["stmt-done"].status,
            StatementStatus::Succeeded
        );
        assert_eq!(fold.statements["stmt-done"].sql, "SELECT 1");
        let live = &fold.statements["stmt-live"];
        assert_eq!(live.status, StatementStatus::Failed);
        assert_eq!(
            live.error.as_deref(),
            Some(crate::history::record::INTERRUPTED_BY_RESTART)
        );
        journal2.shutdown();
    }

    /// H3 on disk: a statement's `submitted` and `running` records in *different* segments.
    ///
    /// Production reaches this when a 64 MiB roll lands between the two appends, which are
    /// milliseconds apart; two boots reproduce it deterministically, since every boot opens a
    /// fresh segment. Replay reads segments newest-first, so the `running` record — which carries
    /// no `sql` and outranks `submitted` — is folded first.
    #[test]
    fn a_statement_whose_records_straddle_a_segment_keeps_its_sql() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = cfg_for(dir.path());
        // Boot 1: the `submitted` record, with the SQL.
        let (journal, _) = Journal::open(&cfg).expect("open");
        journal.append_retained(submitted("stmt-x", 1));
        journal.sync_blocking();
        journal.shutdown();
        // Boot 2: the `running` record, in the next segment, with no SQL.
        let (journal, _) = Journal::open(&cfg).expect("reopen");
        journal.append(running("stmt-x", 1));
        journal.sync_blocking();
        journal.shutdown();

        let indexes: Vec<u64> = segments(&cfg.statements_dir)
            .into_iter()
            .map(|(i, _)| i)
            .collect();
        assert_eq!(indexes, vec![0, 1], "the pair straddles a segment boundary");

        let (journal, mut fold) = Journal::open(&cfg).expect("replay");
        assert_eq!(
            fold.statements["stmt-x"].sql, "SELECT 1",
            "newest-first replay must not lose the SQL"
        );
        // And the crash trace §4a promises is complete: failed, with the reason, *and* the SQL
        // that makes the row worth having.
        assert_eq!(fold.mark_interrupted(), vec!["stmt-x".to_string()]);
        let st = &fold.statements["stmt-x"];
        assert_eq!(st.status, StatementStatus::Failed);
        assert_eq!(
            st.error.as_deref(),
            Some(crate::history::record::INTERRUPTED_BY_RESTART)
        );
        assert_eq!(st.sql, "SELECT 1");
        journal.shutdown();
    }

    #[test]
    fn a_corrupt_tail_is_quarantined_and_boot_continues() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = cfg_for(dir.path());
        // Two boots, so there are two segments: the one that will be torn, and an intact one
        // that must still fold around it.
        for (id, seq) in [("stmt-a", 1u64), ("stmt-b", 2)] {
            let (journal, _) = Journal::open(&cfg).expect("open");
            journal.append(submitted(id, seq));
            journal.sync_blocking();
            journal.shutdown();
        }
        // A torn tail on the older segment: half a line, exactly what a crash mid-append leaves.
        let seg = cfg.statements_dir.join(seg_name(0));
        let mut f = fs_util::append_secure(&seg).expect("append");
        f.write_all(b"{\"v\":1,\"kind\":\"submi").expect("write");
        f.sync_all().expect("sync");
        drop(f);

        let (journal, fold) = Journal::open(&cfg).expect("boot over the corruption");
        assert!(
            fold.statements.contains_key("stmt-a"),
            "records before the torn line survive"
        );
        assert!(
            fold.statements.contains_key("stmt-b"),
            "other segments are still folded"
        );
        assert!(
            cfg.statements_dir.join("seg-000000.jsonl.corrupt").exists(),
            "the bad file is kept as evidence, not deleted"
        );
        assert!(
            !cfg.statements_dir.join(seg_name(0)).exists(),
            "and it is out of the replay set, so the next boot does not re-read it"
        );
        journal.shutdown();
    }

    #[test]
    fn compaction_preserves_the_full_folded_state_across_segments() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = cfg_for(dir.path());
        // Roll after a single record so `submitted` and the terminal snapshot land in different
        // segments — the cross-segment case F1 is about.
        cfg.segment_max_bytes = 1;
        let (journal, _) = Journal::open(&cfg).expect("open");
        journal.append(submitted("stmt-x", 1));
        journal.sync_blocking();
        let mut snap = folded("stmt-x", 1, StatementStatus::Succeeded);
        snap.last_seq = 5;
        let ack = journal.append_durable(snap.to_snapshot()).expect("queued");
        futures::executor::block_on(ack).expect("acked");
        journal.compact_blocking();
        journal.shutdown();

        let gens = generations(&cfg.compacted_dir);
        assert_eq!(gens.len(), 1, "one generation published");
        assert!(
            segments(&cfg.statements_dir).is_empty(),
            "compacted inputs are unlinked"
        );

        let (journal, fold) = Journal::open(&cfg).expect("reopen");
        let st = fold.statements.get("stmt-x").expect("statement survived");
        assert_eq!(st.status, StatementStatus::Succeeded);
        assert_eq!(st.sql, "SELECT 1");
        assert_eq!(st.source, Source::Connect);
        assert_eq!(st.session.as_deref(), Some("sess-1"));
        assert_eq!(st.client_op_id.as_deref(), Some("op-7"));
        assert_eq!(st.schema.as_ref().expect("schema")[0].0, "n");
        assert_eq!(st.rows, Some(1));
        assert_eq!(st.duration_ms, Some(7));
        assert_eq!(st.submitted_at_ms, 1_700_000_000_001);
        assert_eq!(st.seq, 1);
        journal.shutdown();
    }

    #[test]
    fn a_double_folded_generation_changes_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = cfg_for(dir.path());
        let mut fold = Fold::default();
        let snapshot = folded("stmt-x", 3, StatementStatus::Succeeded).to_snapshot();
        fold.apply(snapshot.clone());
        let once = fold.statements["stmt-x"].clone();
        fold.apply(snapshot);
        let twice = &fold.statements["stmt-x"];
        assert_eq!(once.status, twice.status);
        assert_eq!(once.sql, twice.sql);
        assert_eq!(once.last_seq, twice.last_seq);
        drop(cfg);
    }

    #[test]
    fn an_interrupted_swap_converges_to_one_copy() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = cfg_for(dir.path());
        cfg.segment_max_bytes = 1;
        let (journal, _) = Journal::open(&cfg).expect("open");
        journal.append(submitted("stmt-x", 1));
        journal.sync_blocking();
        journal.append(folded("stmt-x", 1, StatementStatus::Succeeded).to_snapshot());
        journal.sync_blocking();
        journal.compact_blocking();
        journal.shutdown();

        // Recreate the state a crash between step 3 and step 5 leaves: the generation is in
        // place, the marker still names inputs that are already gone.
        let marker = cfg.compacted_dir.join("gen-000000.done");
        std::fs::write(&marker, "seg-000000.jsonl\nseg-000001.jsonl\n").expect("marker");
        let (journal, fold) = Journal::open(&cfg).expect("recover");
        assert_eq!(fold.statements.len(), 1);
        assert_eq!(fold.statements["stmt-x"].status, StatementStatus::Succeeded);
        assert!(!marker.exists(), "recovery removes the marker last");
        journal.shutdown();
    }

    #[test]
    fn replay_is_bounded_by_max_records() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = cfg_for(dir.path());
        cfg.segment_max_bytes = 1; // one record per segment
        let (journal, _) = Journal::open(&cfg).expect("open");
        for seq in 0..8u64 {
            journal.append(submitted(&format!("stmt-{seq}"), seq));
            journal.sync_blocking();
        }
        journal.shutdown();

        cfg.max_records = 3;
        let (journal, fold) = Journal::open(&cfg).expect("reopen");
        assert!(
            fold.statements.len() <= 4,
            "replay stops once the cap is reached, got {}",
            fold.statements.len()
        );
        // Newest-first: the last statement written is always in hand.
        assert!(fold.statements.contains_key("stmt-7"));
        journal.shutdown();
    }

    #[test]
    fn files_are_0600_and_dirs_0700_at_creation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = cfg_for(dir.path());
        let (journal, _) = Journal::open(&cfg).expect("open");
        journal.append(submitted("stmt-a", 1));
        journal.sync_blocking();
        journal.shutdown();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let seg = cfg.statements_dir.join(seg_name(0));
            let mode = std::fs::metadata(&seg)
                .expect("segment")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600, "segment mode {mode:o}");
            let dir_mode = std::fs::metadata(&cfg.statements_dir)
                .expect("dir")
                .permissions()
                .mode();
            assert_eq!(dir_mode & 0o777, 0o700, "dir mode {dir_mode:o}");
        }
    }

    /// H2: §7 says `submitted` and `snapshot` records are *never* dropped and never coalesced,
    /// and only `running` chatter may be. The previous version of this test flooded the channel
    /// with `RecordKind::Submitted` and so proved the opposite of its own name.
    ///
    /// The flood is `running` records, and both a `submitted` and a terminal record queued
    /// during it have to survive — the `submitted` one with its SQL, which is the whole of §4a's
    /// crash trace.
    #[test]
    fn running_records_are_dropped_under_backpressure_submitted_and_terminals_are_not() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = cfg_for(dir.path());
        let (journal, _) = Journal::open(&cfg).expect("open");
        // Fill the channel far past its capacity as fast as a producer can; the writer drains it
        // concurrently, so this asserts the *policy*, not a specific drop count.
        for seq in 0..(CHANNEL_CAPACITY as u64 * 2) {
            journal.append(running("stmt-chatter", seq));
        }
        // Queued while the writer is behind: neither may be lost.
        journal.append_retained(submitted("stmt-late", 7));
        let ack = journal
            .append_durable(folded("stmt-terminal", 1, StatementStatus::Succeeded).to_snapshot())
            .expect("a terminal record is never refused while the disk answers");
        futures::executor::block_on(ack).expect("acked");
        journal.shutdown();

        let (journal, fold) = Journal::open(&cfg).expect("reopen");
        assert_eq!(
            fold.statements["stmt-terminal"].status,
            StatementStatus::Succeeded
        );
        let late = fold
            .statements
            .get("stmt-late")
            .expect("a submitted record queued under backpressure is never dropped");
        assert_eq!(late.sql, "SELECT 7", "and it still carries its SQL (§4a)");
        journal.shutdown();
    }

    /// The overflow queue is what makes "never dropped" true without parking the producer, so it
    /// has to actually hold more than the channel does — and hand every record to the writer.
    #[test]
    fn retained_records_survive_a_channel_that_is_full() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = cfg_for(dir.path());
        let (journal, _) = Journal::open(&cfg).expect("open");
        // Far more retained records than the channel can hold, from one thread, so the queue is
        // certainly overflowed at some point during the burst.
        let total = CHANNEL_CAPACITY as u64 * 2;
        for seq in 0..total {
            journal.append_retained(submitted(&format!("stmt-{seq}"), seq));
        }
        journal.shutdown();

        cfg.max_records = total as usize * 2;
        let (journal, fold) = Journal::open(&cfg).expect("reopen");
        assert_eq!(
            fold.statements.len(),
            total as usize,
            "every retained record reached the disk"
        );
        journal.shutdown();
    }
}
