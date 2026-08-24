//! The disk guards from §3: one budget over everything the engine owns under the root, a
//! free-space floor, and a prune order that never deletes the live log file.
//!
//! This module owns the *file-level* half of the sweep — measuring the subtrees and unlinking
//! rolled logs, dumps and orphaned results. The statement-granular half (tombstone a statement,
//! unlink its result, let compaction drop the record) belongs to the statement store, because
//! only the store knows which statements are still running; `StatementStore::sweep_disk` drives
//! both in the documented order.
//!
//! **Prune order** (§3, as F2 corrected it):
//!
//! 1. oldest rolled logs — never `oxidant.log`, which rotates rather than being deleted;
//! 2. oldest dumps;
//! 3. oldest result files whose statement is already pruned (orphans);
//! 4. oldest journal *statements* — a tombstone plus its result, never a raw segment unlink:
//!    a statement whose `submitted` lived in `seg-41` and whose snapshot lives in `seg-42` must
//!    not lose its SQL because `seg-41` aged out;
//! 5. oldest *live* result files — the rows go, the statement stays, and `/result` answers
//!    `410 result_expired`.
//!
//! If the budget is still exceeded after everything prunable is gone, `/api/status` reports
//! `disk: over_budget`, the engine keeps serving, and each pass logs one line naming what it
//! removed and why.
//!
//! **The free-space floor does not prune.** `OXIDANT_DISK_MAX_BYTES` is the only thing that
//! drives the order above: the engine deletes its own files when its own subtree is over its own
//! budget. `OXIDANT_DISK_MIN_FREE_BYTES` is a separate condition with a separate answer — the
//! engine pauses result spill and reports `disk: low_free` + `history_writes: degraded`. The two
//! were one boolean once, driving one unbounded loop, and because pruning cannot *make* a
//! free-space floor satisfiable that loop ran until every terminal statement in both tiers was
//! gone — every five minutes, for a shortfall a co-tenant caused.
//!
//! **The sweeper unlinks only files it can recognise as its own.** `OXIDANT_LOG_DIR`,
//! `OXIDANT_DUMP_DIR` and `OXIDANT_RESULT_DIR` are operator-set paths validated only for `://`,
//! so "every flat file in this directory" made `OXIDANT_LOG_DIR=/var/log` a command to delete
//! other services' logs under disk pressure. The shapes are `oxidant-*.log` (never
//! `oxidant.log`), `dump-*.parquet` / `oxidant-*.parquet`, and `stmt-*.arrow` /
//! `stmt-*.arrow.tmp`.
//!
//! **Before any of that**, two subtree-local passes run unconditionally, because they are
//! retention rather than pressure and they must happen whether or not the global budget is
//! tight (§3, §6, §8):
//!
//! - [`prune_expired_logs`] deletes rolled logs whose **whole period** is older than
//!   `OXIDANT_LOG_KEEP_DAYS`, and then trims the `logs/` subtree to `OXIDANT_LOG_MAX_TOTAL_BYTES`
//!   oldest-first;
//! - [`roll_event_log`] brings `event_log_dir` under the budget the way F16 asks for it — by
//!   **rolling** `events.jsonl` to `events-<UTC-period>.jsonl` and pruning oldest-first, never by
//!   deleting the live file other tools are reading.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};

use super::config::HistoryConfig;
use crate::logging::{LogPeriod, LogRoll};

/// A file the sweeper may delete, with the mtime it is ordered by.
#[derive(Debug, Clone)]
pub(crate) struct Prunable {
    pub path: PathBuf,
    pub bytes: u64,
    pub mtime: std::time::SystemTime,
}

/// What one sweep pass did, for the log line and for `/api/status`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct SweepReport {
    /// Bytes the engine owns under the root after the pass.
    pub used_bytes: u64,
    pub rolled_logs_removed: usize,
    /// Rolled logs whose whole *period* fell out of `OXIDANT_LOG_KEEP_DAYS`. Retention, not
    /// pressure: counted separately so a test can tell "the 30 days expired" from "the budget
    /// was tight".
    pub logs_expired: usize,
    /// Rolled logs taken to bring `logs/` back under `OXIDANT_LOG_MAX_TOTAL_BYTES`.
    pub logs_over_cap: usize,
    /// Rolled `events-*.jsonl` files taken to hold `OXIDANT_EVENT_LOG_MAX_BYTES` (§8).
    pub event_logs_pruned: usize,
    /// The live `events.jsonl` was rolled this pass. Never deleted (§8, F16).
    pub event_log_rolled: bool,
    pub dumps_removed: usize,
    pub orphan_results_removed: usize,
    pub statements_pruned: usize,
    pub live_results_removed: usize,
    pub freed_bytes: u64,
    /// The engine's own subtree is still past `OXIDANT_DISK_MAX_BYTES` with nothing left to
    /// prune. This is the only condition that drives the prune loop.
    pub over_budget: bool,
    /// The volume is below `OXIDANT_DISK_MIN_FREE_BYTES`. Reported and acted on by pausing
    /// spill — **never** by pruning: pruning cannot make the floor satisfiable, and the
    /// shortfall is very often not the engine's (H1).
    pub low_free: bool,
    /// What the free-space probe read, or `None` when no mount matched.
    pub free_bytes: Option<u64>,
}

impl SweepReport {
    pub(crate) fn removed_anything(&self) -> bool {
        self.rolled_logs_removed
            + self.logs_expired
            + self.logs_over_cap
            + self.event_logs_pruned
            + self.dumps_removed
            + self.orphan_results_removed
            + self.statements_pruned
            + self.live_results_removed
            > 0
            || self.event_log_rolled
    }
}

/// The live exec log. Never deleted — §3 says it rotates instead, and PR3 is what rotates it.
pub(crate) const LIVE_LOG: &str = "oxidant.log";

/// Recursive byte total of `dir`, ignoring what it cannot read.
///
/// Symlinks are not followed: `read_dir` + `symlink_metadata` means a link into another subtree
/// is counted as the link, not as its target, so one file cannot be billed twice. [`flat_files`]
/// uses the same rule, so what the sweeper measures and what it unlinks agree.
pub(crate) fn subtree_bytes(dir: &Path) -> u64 {
    #[cfg(test)]
    SUBTREE_WALKS.with(|c| c.set(c.get() + 1));
    subtree_bytes_inner(dir)
}

// How many times *this thread* has started a recursive walk. The sweeper's cost is dominated by
// these, and the guard the M2 test needs is a count, not a stopwatch: a thread-local keeps it
// exact under `cargo test`'s parallelism.
#[cfg(test)]
thread_local! {
    static SUBTREE_WALKS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn subtree_walks() -> u64 {
    SUBTREE_WALKS.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn reset_subtree_walks() {
    SUBTREE_WALKS.with(|c| c.set(0));
}

fn subtree_bytes_inner(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut total = 0;
    for entry in entries.flatten() {
        let Ok(meta) = entry.path().symlink_metadata() else {
            continue;
        };
        if meta.is_dir() {
            total += subtree_bytes_inner(&entry.path());
        } else if meta.is_file() {
            total += meta.len();
        }
    }
    total
}

/// Is `name` a rolled exec log this engine wrote? `oxidant-<UTC-period>[.N].{log,parquet}`,
/// never the live `oxidant.log`.
///
/// `OXIDANT_LOG_DIR` is operator-set and validated only for `://`, so "every flat file in the
/// directory except the live one" made `OXIDANT_LOG_DIR=/var/log` a command to delete other
/// services' logs the first time the engine went over its disk budget. The sweeper unlinks files
/// it can recognise as its own and nothing else — and as of PR3 "its own" is the *grammar*, not
/// a prefix and a suffix, so `oxidant-something.log` no longer qualifies.
///
/// **Both extensions.** A rolled log spends most of its life as `.parquet`; matching only `.log`
/// meant step 1 of the prune order silently skipped every converted file and the budget was paid
/// for out of statement history instead.
pub(crate) fn is_rolled_log(name: &str) -> bool {
    if name == LIVE_LOG {
        return false;
    }
    matches!(
        crate::logging::parse_rolled_name(name),
        Some((_, _, "log" | "parquet"))
    )
}

/// Is `name` a support-bundle dump this engine wrote? `dump-*.parquet` (§6b), or the
/// `oxidant-*.parquet` shape a bundle named after the process takes.
///
/// A name that parses as a rolled exec log is **not** a dump, even in the dump directory: with
/// `OXIDANT_DUMP_DIR` and `OXIDANT_LOG_DIR` pointed at one path, both passes would otherwise
/// select the same converted log and the second would log a failed-unlink warning for a file the
/// first had already taken.
pub(crate) fn is_dump(name: &str) -> bool {
    if crate::logging::parse_rolled_name(name).is_some() {
        return false;
    }
    (name.starts_with("dump-") || name.starts_with("oxidant-")) && name.ends_with(".parquet")
}

/// Rolled log files in `logs/`, oldest first. The live file, and anything the engine did not
/// write, are filtered out here rather than at the delete site, so no caller can forget.
pub(crate) fn rolled_logs(logs_dir: &Path) -> Vec<Prunable> {
    flat_files(logs_dir, is_rolled_log)
}

/// Support-bundle dumps (§6b), oldest first.
pub(crate) fn dumps(dumps_dir: &Path) -> Vec<Prunable> {
    flat_files(dumps_dir, is_dump)
}

/// Flat files in `dir` whose name `owned` recognises, oldest first.
fn flat_files(dir: &Path, owned: fn(&str) -> bool) -> Vec<Prunable> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out: Vec<Prunable> = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !owned(name) {
            continue;
        }
        // `symlink_metadata`, matching `subtree_bytes`: a symlink is not a file here, so it is
        // neither measured as ~0 nor unlinked as if it were its target's size — `freed_bytes`
        // would have been wrong in both directions.
        let Ok(meta) = entry.path().symlink_metadata() else {
            continue;
        };
        if !meta.is_file() {
            continue;
        }
        out.push(Prunable {
            path: entry.path(),
            bytes: meta.len(),
            mtime: meta.modified().unwrap_or(std::time::UNIX_EPOCH),
        });
    }
    // mtime, then name: two files written in the same millisecond still prune deterministically,
    // which is what makes the order assertable in a test.
    out.sort_by(|a, b| a.mtime.cmp(&b.mtime).then_with(|| a.path.cmp(&b.path)));
    out
}

/// The mount table, read once and then queried per directory.
///
/// Enumerating mounts means a `statfs` per mount; a 300-second sweeper does not need a fresh one
/// per candidate file, and the free-space floor has to be asked about *every* managed directory
/// (§3: a subtree moved to another volume is floored against that volume), so one probe answers
/// all of them.
#[derive(Debug, Clone, Default)]
pub(crate) struct Mounts {
    /// `(mount point, available bytes)`.
    entries: Vec<(PathBuf, u64)>,
}

impl Mounts {
    /// Read the mount table. One `sysinfo::Disks` enumeration, once per sweep pass.
    pub(crate) fn probe() -> Self {
        let disks = sysinfo::Disks::new_with_refreshed_list();
        Self {
            entries: disks
                .list()
                .iter()
                .map(|d| (d.mount_point().to_path_buf(), d.available_space()))
                .collect(),
        }
    }

    /// A synthetic mount table — the seam that lets a test drive the free-space floor without
    /// filling a volume, and the only way to exercise "a subtree on another volume is floored
    /// against *that* volume" on one machine.
    pub(crate) fn from_entries(entries: Vec<(PathBuf, u64)>) -> Self {
        Self { entries }
    }

    /// Free bytes on the filesystem holding `path`, or `None` when no mount matches.
    ///
    /// The mount is chosen by **longest-prefix match** of the mount point against the
    /// canonicalized path: the naive "first disk" answer is wrong the moment `OXIDANT_LOG_DIR`
    /// points at another volume.
    ///
    /// Caveat worth naming: on macOS `canonicalize("/tmp/x")` yields `/private/tmp/x`, which
    /// longest-prefix-matches `/` rather than `/System/Volumes/Data`. It gives the right *number*
    /// (APFS shares the container's free space between them) but it is not the match this doc
    /// describes.
    pub(crate) fn free_bytes(&self, path: &Path) -> Option<u64> {
        // The directory may not exist yet (a root configured but never written to), so walk up
        // until an ancestor resolves: the mount is the same either way.
        let target = path.ancestors().find_map(|p| p.canonicalize().ok())?;
        let mut best: Option<(usize, u64)> = None;
        for (mount, free) in &self.entries {
            if !target.starts_with(mount) {
                continue;
            }
            let depth = mount.components().count();
            if best.map(|(d, _)| depth > d).unwrap_or(true) {
                best = Some((depth, *free));
            }
        }
        best.map(|(_, free)| free)
    }
}

/// Free bytes on the filesystem holding `path`, reading the mount table for this one question.
/// The sweeper does not use this — it probes once per pass and asks about every managed
/// directory — but it is what proves a real directory resolves to a real mount at all.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn free_bytes(path: &Path) -> Option<u64> {
    Mounts::probe().free_bytes(path)
}

/// Delete `file`, returning the bytes it freed. `None` is a *failed* unlink, which the caller
/// must not count as a removal: on a read-only or EPERM-ing `logs/` mount the sweep line
/// otherwise claimed "removed N rolled logs" having removed none.
///
/// Directory fsync is the caller's: a sweep pass unlinks many files from one directory and
/// syncs it once.
pub(crate) fn remove(file: &Prunable) -> Option<u64> {
    match std::fs::remove_file(&file.path) {
        Ok(()) => Some(file.bytes),
        Err(e) => {
            tracing::warn!(
                file = %file.path.display(),
                error = %e,
                "disk sweep: could not remove a file it selected for pruning"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn touch(path: &Path, bytes: usize) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("mkdir");
        }
        std::fs::write(path, vec![b'x'; bytes]).expect("write");
    }

    /// The one file the sweeper may never take.
    #[test]
    fn the_live_log_is_never_prunable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let logs = dir.path().join("logs");
        touch(&logs.join(LIVE_LOG), 10);
        touch(&logs.join("oxidant-2026-08-22.log"), 10);
        let names: Vec<String> = rolled_logs(&logs)
            .iter()
            .map(|p| p.path.file_name().unwrap().to_string_lossy().to_string())
            .collect();
        assert_eq!(names, vec!["oxidant-2026-08-22.log".to_string()]);
    }

    /// `OXIDANT_LOG_DIR` and `OXIDANT_DUMP_DIR` are operator-set paths that may well be shared
    /// — `/var/log` is a plausible value. The sweeper prunes only what it can recognise as its
    /// own, so a co-tenant's files survive a disk-pressure pass.
    #[test]
    fn the_sweeper_only_recognises_files_the_engine_wrote() {
        let dir = tempfile::tempdir().expect("tempdir");
        let logs = dir.path().join("logs");
        touch(&logs.join("oxidant-2026-08-22.log"), 10);
        touch(&logs.join("syslog"), 10);
        touch(&logs.join("nginx-access.log"), 10);
        touch(&logs.join("oxidant-2026-08-22.log.gz"), 10);
        let names: Vec<String> = rolled_logs(&logs)
            .iter()
            .map(|p| p.path.file_name().unwrap().to_string_lossy().to_string())
            .collect();
        assert_eq!(names, vec!["oxidant-2026-08-22.log".to_string()]);

        let dumps_dir = dir.path().join("dumps");
        touch(&dumps_dir.join("dump-1.parquet"), 10);
        touch(&dumps_dir.join("oxidant-bundle.parquet"), 10);
        touch(&dumps_dir.join("customer-facts.parquet"), 10);
        touch(&dumps_dir.join("notes.txt"), 10);
        let mut names: Vec<String> = dumps(&dumps_dir)
            .iter()
            .map(|p| p.path.file_name().unwrap().to_string_lossy().to_string())
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "dump-1.parquet".to_string(),
                "oxidant-bundle.parquet".to_string()
            ]
        );
    }

    /// Every subtree under the root counts against one budget, nested directories included.
    #[test]
    fn subtree_bytes_counts_nested_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        touch(&dir.path().join("a.log"), 100);
        touch(&dir.path().join("deep/b.log"), 50);
        assert_eq!(subtree_bytes(dir.path()), 150);
    }

    /// Free space is read per mount; the root always resolves to *some* mount, so a `None` here
    /// would silently disable the floor.
    #[test]
    fn free_space_resolves_for_a_real_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(
            free_bytes(dir.path()).is_some(),
            "the free-space floor cannot be enforced without a mount match"
        );
    }

    /// The mount is chosen by longest-prefix match, not by "the first disk": a subtree moved to
    /// another volume must be floored against that volume, not against the root's.
    #[test]
    fn a_mount_is_chosen_by_longest_prefix_not_by_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        let deep = dir.path().join("volume");
        std::fs::create_dir_all(&deep).expect("mkdir");
        let root = dir.path().canonicalize().expect("canonicalize");
        let deep_canonical = deep.canonicalize().expect("canonicalize");
        // The shallower mount is listed first, and holds far more free space.
        let mounts =
            Mounts::from_entries(vec![(root.clone(), 1_000_000), (deep_canonical.clone(), 7)]);
        assert_eq!(mounts.free_bytes(&deep), Some(7), "the deeper mount wins");
        assert_eq!(mounts.free_bytes(&root), Some(1_000_000));
        // A path under no listed mount has no answer, which disables the floor for it rather
        // than inventing one.
        assert_eq!(
            Mounts::from_entries(Vec::new()).free_bytes(&root),
            None,
            "no mount match must be None, never 0 — 0 would read as a full volume"
        );
    }
}

/// The distinct subtrees the disk budget covers, deduped so nothing is billed twice.
///
/// `OXIDANT_HISTORY_DIR` / `OXIDANT_RESULT_DIR` / `OXIDANT_LOG_DIR` / `OXIDANT_DUMP_DIR` each win
/// over the root and may point *outside* it, and §3 says an overridden subtree is still counted
/// against the budget — so the candidates are measured as a set, with any path already contained
/// in a shallower one dropped.
///
/// `event_log_dir` is in the set as of PR3 (§8/F16). PR2 left it out deliberately: F16's
/// mechanism for it is a *roll*, the rolling writer was PR3, and counting a directory it could
/// not prune would have pinned `disk: over_budget` on for every operator pointing
/// `OXIDANT_EVENT_LOG_DIR` at a large Spark-history-server path. Now that [`roll_event_log`]
/// exists, it counts.
pub(crate) fn budget_roots(cfg: &HistoryConfig) -> Vec<PathBuf> {
    let history_dir = cfg
        .statements_dir
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| cfg.statements_dir.clone());
    let mut candidates = vec![
        cfg.root.clone(),
        history_dir,
        cfg.results_dir.clone(),
        cfg.logs_dir.clone(),
        cfg.dumps_dir.clone(),
    ];
    // Only when it is actually bounded: with `OXIDANT_EVENT_LOG_MAX_BYTES=0` the directory is
    // unbounded by the operator's explicit choice, and billing an unprunable tree against the
    // budget would make the sweeper delete statement history to pay for it.
    if let (Some(dir), true) = (&cfg.event_log_dir, cfg.event_log_max_bytes > 0) {
        candidates.push(dir.clone());
    }
    candidates.sort_by_key(|p| p.components().count());
    let mut kept: Vec<PathBuf> = Vec::new();
    for candidate in candidates {
        if kept.iter().any(|k| candidate.starts_with(k)) {
            continue;
        }
        kept.push(candidate);
    }
    kept
}

/// What one `logs/` retention pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct LogPruneReport {
    /// Files whose whole period fell out of `OXIDANT_LOG_KEEP_DAYS`.
    pub expired: usize,
    /// Files taken to bring the subtree back under `OXIDANT_LOG_MAX_TOTAL_BYTES`.
    pub over_cap: usize,
    pub freed_bytes: u64,
}

/// Rolled logs and their converted siblings, oldest period first.
///
/// Ordered by **period**, not mtime: a rolled `.log` that failed to convert keeps being touched
/// by retry passes, and ordering by mtime would make it look newer than the day after it.
fn rolled_by_period(logs_dir: &Path) -> Vec<(LogPeriod, u32, Prunable)> {
    let Ok(entries) = std::fs::read_dir(logs_dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some((period, split, ext)) = crate::logging::parse_rolled_name(name) else {
            continue;
        };
        if ext != "log" && ext != "parquet" {
            continue;
        }
        let Ok(meta) = entry.path().symlink_metadata() else {
            continue;
        };
        if !meta.is_file() {
            continue;
        }
        out.push((
            period,
            split,
            Prunable {
                path: entry.path(),
                bytes: meta.len(),
                mtime: meta.modified().unwrap_or(std::time::UNIX_EPOCH),
            },
        ));
    }
    out.sort_by(|a, b| {
        a.0.end()
            .cmp(&b.0.end())
            .then_with(|| a.1.cmp(&b.1))
            .then_with(|| a.2.path.cmp(&b.2.path))
    });
    out
}

/// §3/§6 retention for `logs/`: period-based expiry, then the subtree cap.
///
/// `keep_days` is evaluated against the *period* a file covers, not its name parsed as a day:
/// **a rolled file is deleted only when its whole period is older than `keep_days`.** Weekly
/// therefore rounds up — a week file survives until its last day falls out of the window,
/// retaining up to six extra days. That is the stated operator contract; deleting `W30` on day
/// 30 would discard days that are inside retention.
pub(crate) fn prune_expired_logs(
    logs_dir: &Path,
    keep_days: i64,
    max_total_bytes: u64,
    now: DateTime<Utc>,
) -> LogPruneReport {
    let mut report = LogPruneReport::default();
    let mut files = rolled_by_period(logs_dir);
    if keep_days > 0 {
        let cutoff = now - chrono::Duration::days(keep_days);
        files.retain(|(period, _, file)| {
            // No parseable end means no evidence it expired; keep it and let the cap decide.
            let expired = period.end().is_some_and(|end| end <= cutoff);
            if expired {
                if let Some(freed) = remove(file) {
                    report.expired += 1;
                    report.freed_bytes += freed;
                    return false;
                }
            }
            true
        });
    }
    let mut total: u64 = files.iter().map(|(_, _, f)| f.bytes).sum();
    for (_, _, file) in &files {
        if total <= max_total_bytes {
            break;
        }
        if let Some(freed) = remove(file) {
            total = total.saturating_sub(freed);
            report.over_cap += 1;
            report.freed_bytes += freed;
        }
    }
    report
}

/// The live Spark-history-server event log. Never deleted — it is rolled (§8, F16).
pub(crate) const LIVE_EVENT_LOG: &str = "events.jsonl";
/// Prefix of a rolled event log: `events-<UTC-period>[.N].jsonl`.
const ROLLED_EVENT_PREFIX: &str = "events-";

/// Is `name` a rolled event log this engine wrote?
///
/// Same discipline as [`is_rolled_log`]: `OXIDANT_EVENT_LOG_DIR` is an operator-set path that
/// other tools read, so the sweeper unlinks only the shape it writes itself and never the live
/// file.
pub(crate) fn is_rolled_event_log(name: &str) -> bool {
    name != LIVE_EVENT_LOG && name.starts_with(ROLLED_EVENT_PREFIX) && name.ends_with(".jsonl")
}

/// The name a rolled event log takes for `period`/`split` — §3's naming rules, with `events-`
/// in place of `oxidant-`.
pub(crate) fn rolled_event_log_name(period: LogPeriod, split: u32) -> String {
    let stem = period.stem();
    if split <= 1 {
        format!("{ROLLED_EVENT_PREFIX}{stem}.jsonl")
    } else {
        format!("{ROLLED_EVENT_PREFIX}{stem}.{split}.jsonl")
    }
}

/// Rolled event logs, oldest first.
pub(crate) fn rolled_event_logs(dir: &Path) -> Vec<Prunable> {
    flat_files(dir, is_rolled_event_log)
}

/// What one `event_log_dir` pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct EventLogReport {
    /// The live `events.jsonl` was renamed to a periodised file.
    pub rolled: bool,
    pub pruned: usize,
    pub freed_bytes: u64,
}

/// Bring `event_log_dir` under `OXIDANT_EVENT_LOG_MAX_BYTES` (§8, F16).
///
/// **The live file is rolled, never deleted.** `AppStateStore::emit` appends every execution
/// event to a single `events.jsonl` that was never rolled and never pruned, and
/// `load_event_log` read the whole file back with `fs::read_to_string` — the one existing path
/// that genuinely fills a server. But an operator points this directory at a Spark-history-server
/// path that *other tools read*, so the fix is a rename plus an oldest-first prune, not a
/// truncate. `max_bytes == 0` restores today's unbounded behaviour exactly.
///
/// `emit` opens the path per event, so a rename between two of its opens is safe: the next
/// append creates a fresh `events.jsonl`, and an event that raced the rename lands in the rolled
/// file rather than being lost.
pub(crate) fn roll_event_log(
    dir: &Path,
    max_bytes: u64,
    roll: LogRoll,
    now: DateTime<Utc>,
) -> EventLogReport {
    let mut report = EventLogReport::default();
    if max_bytes == 0 {
        return report;
    }
    let live = dir.join(LIVE_EVENT_LOG);
    let live_bytes = live.metadata().map(|m| m.len()).unwrap_or(0);
    if live_bytes > max_bytes {
        // `LogRoll::Off` turns the *exec* log writer off; the event log still needs a period to
        // name its roll after, and daily is the design's default.
        let roll = if roll == LogRoll::Off {
            LogRoll::Daily
        } else {
            roll
        };
        if let Some(period) = LogPeriod::of(now, roll) {
            let mut split = 1;
            while dir.join(rolled_event_log_name(period, split)).exists() && split < 999 {
                split += 1;
            }
            let target = dir.join(rolled_event_log_name(period, split));
            match super::fs_util::rename_durable(&live, &target, dir) {
                Ok(()) => report.rolled = true,
                Err(e) => tracing::warn!(
                    dir = %dir.display(),
                    error = %e,
                    "could not roll the event log; it stays unbounded until the next sweep"
                ),
            }
        }
    }
    let mut total: u64 = subtree_bytes(dir);
    for file in rolled_event_logs(dir) {
        if total <= max_bytes {
            break;
        }
        if let Some(freed) = remove(&file) {
            total = total.saturating_sub(freed);
            report.pruned += 1;
            report.freed_bytes += freed;
        }
    }
    if report.rolled || report.pruned > 0 {
        tracing::info!(
            dir = %dir.display(),
            rolled = report.rolled,
            pruned = report.pruned,
            freed_bytes = report.freed_bytes,
            max_bytes,
            "event log rolled under OXIDANT_EVENT_LOG_MAX_BYTES"
        );
    }
    report
}
