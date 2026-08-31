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
    /// Bytes the engine owns under the root after the pass — the only number
    /// `OXIDANT_DISK_MAX_BYTES` is compared against.
    pub used_bytes: u64,
    /// Bytes another tool wrote into a directory the engine shares with it — today only
    /// `OXIDANT_EVENT_LOG_DIR`, whose whole purpose is to be a Spark-history-server path. Measured
    /// and reported so a large directory is explicable; **never billed**, because the engine
    /// cannot prune a single one of them and a budget it can never satisfy runs the prune order to
    /// exhaustion every five minutes (H2, F16).
    pub foreign_bytes: u64,
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
    /// Support bundles past their 24 h life (§6b). Retention, not pressure — counted apart from
    /// `dumps_removed` so a test can tell "the bundle expired" from "the budget was tight".
    pub dumps_expired: usize,
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
            + self.dumps_expired
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

/// Is `name` a Parquet conversion that never reached its rename — `oxidant-<period>[.N].parquet.tmp`?
///
/// The grammar, not the suffix. This runs against an operator-set `OXIDANT_LOG_DIR`, and "every
/// `*.tmp`" would make pointing that knob at a shared directory destructive — the same reason
/// `results::clear_tmp` matches `stmt-*.arrow.tmp` rather than `*.tmp`.
pub(crate) fn is_rolled_log_tmp(name: &str) -> bool {
    let Some(stem) = name.strip_suffix(".tmp") else {
        return false;
    };
    matches!(
        crate::logging::parse_rolled_name(stem),
        Some((_, _, "parquet"))
    )
}

/// Delete every orphaned `oxidant-*.parquet.tmp` in `logs/`, answering the bytes freed.
///
/// **Billed but unprunable, otherwise.** `subtree_bytes` counts a `.parquet.tmp` — it is a file
/// under a budget root — but `parse_rolled_name` rejects the name (`LogPeriod::parse` fails on
/// `"2026-08-23.parquet"`), so `rolled_logs` never offers it and step 1 of the prune order cannot
/// take it. The only other code that removes one is the converter thread's `convert_pending`,
/// which does not run at all under `OXIDANT_LOG_ROLL=off` or `OXIDANT_HISTORY=off`. A tmp left by
/// a crash in a previous run was therefore billed against `OXIDANT_DISK_MAX_BYTES` forever, and
/// paid for by pruning statement history — exactly the class of unprunable-but-counted bytes H2
/// was about.
///
/// **Boot only**, from `logging::init`, before this process has a writer: Parquet's footer sits
/// at the end, so a `.tmp` is never salvageable and deleting one costs nothing — but deleting one
/// mid-conversion would waste the conversion, and at boot there is none in flight. One writer per
/// `logs/` is enforced by the directory lock, so no peer's conversion is in flight either.
pub(crate) fn clear_log_tmp(logs_dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(logs_dir) else {
        return 0;
    };
    let mut freed = 0u64;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !is_rolled_log_tmp(name) {
            continue;
        }
        let Ok(meta) = entry.path().symlink_metadata() else {
            continue;
        };
        if !meta.is_file() {
            continue;
        }
        if std::fs::remove_file(entry.path()).is_ok() {
            freed = freed.saturating_add(meta.len());
        }
    }
    if freed > 0 {
        super::fs_util::fsync_dir(logs_dir);
    }
    freed
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

/// Rolled log files in `logs/`, oldest **period** first. The live file, and anything the engine
/// did not write, are filtered out here rather than at the delete site, so no caller can forget.
///
/// Ordered by the period the file covers rather than by mtime: a rolled `.log` that failed to
/// convert is touched again by every retry pass, and `oxidant-2026-08-23.2.log` sorts *before*
/// `oxidant-2026-08-23.log` lexicographically (`2` < `l`), so an mtime-then-name sort put the
/// newest split first and pruned it ahead of the oldest.
pub(crate) fn rolled_logs(logs_dir: &Path) -> Vec<Prunable> {
    rolled_by_period(logs_dir)
        .into_iter()
        .map(|(_, _, file)| file)
        .collect()
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
thread_local! {
    /// Test seam: runs between the sweep's two [`measure_roots`] calls.
    ///
    /// That window is the one place the sweep's accounting can be lied to — the tree is walked
    /// twice with no lock over the filesystem in between, so a spill landing or a journal
    /// segment being rewritten changes the second walk. A test sets this hook to make that
    /// change happen on purpose. Nothing in production ever sets it.
    pub(crate) static SWEEP_MIDPOINT: std::cell::RefCell<Option<Box<dyn Fn()>>> =
        const { std::cell::RefCell::new(None) };
}

/// Fire the [`SWEEP_MIDPOINT`] hook, if a test set one.
#[cfg(test)]
pub(crate) fn sweep_midpoint() {
    SWEEP_MIDPOINT.with(|hook| {
        if let Some(f) = hook.borrow().as_ref() {
            f();
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    /// Sorted directory listing, for assertions that name every survivor.
    fn names(dir: &Path) -> Vec<String> {
        let mut out: Vec<String> = std::fs::read_dir(dir)
            .expect("read_dir")
            .flatten()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        out.sort();
        out
    }

    fn touch(path: &Path, bytes: usize) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("mkdir");
        }
        std::fs::write(path, vec![b'x'; bytes]).expect("write");
    }

    /// **L4.** An orphaned `.parquet.tmp` was billed to the budget and invisible to every prune
    /// step: `subtree_bytes` counts it, but `parse_rolled_name("oxidant-2026-08-23.parquet.tmp")`
    /// is `None`, so `rolled_logs` never offers it. The only code that removed one was the
    /// converter thread, which does not run under `OXIDANT_LOG_ROLL=off` — so a tmp left by a
    /// crash was billed forever and paid for by pruning statement history.
    ///
    /// The grammar is load-bearing: this runs against an operator-set `OXIDANT_LOG_DIR`.
    #[test]
    fn an_orphan_parquet_tmp_is_swept_and_nothing_else_is() {
        let dir = tempfile::tempdir().expect("tempdir");
        let logs = dir.path().join("logs");
        touch(&logs.join("oxidant-2026-08-22.parquet.tmp"), 100);
        touch(&logs.join("oxidant-2026-08-23-14.3.parquet.tmp"), 50);
        // Everything below must survive: two are the engine's own finished files, and the rest
        // are a co-tenant's, in a directory `OXIDANT_LOG_DIR` may point at.
        touch(&logs.join("oxidant-2026-08-22.log"), 10);
        touch(&logs.join("oxidant-2026-08-21.parquet"), 10);
        touch(&logs.join(LIVE_LOG), 10);
        touch(&logs.join("postgres.parquet.tmp"), 10);
        touch(&logs.join("oxidant-notaperiod.parquet.tmp"), 10);
        touch(&logs.join("oxidant-2026-08-20.log.tmp"), 10);
        touch(&logs.join("application_1_0001.tmp"), 10);

        let before = subtree_bytes(&logs);
        let freed = clear_log_tmp(&logs);

        assert_eq!(freed, 150, "both tmps, and their bytes reported");
        assert_eq!(
            names(&logs),
            vec![
                "application_1_0001.tmp".to_string(),
                "oxidant-2026-08-20.log.tmp".to_string(),
                "oxidant-2026-08-21.parquet".to_string(),
                "oxidant-2026-08-22.log".to_string(),
                "oxidant-notaperiod.parquet.tmp".to_string(),
                LIVE_LOG.to_string(),
                "postgres.parquet.tmp".to_string(),
            ],
            "only the engine's own unfinished conversions go"
        );
        assert_eq!(
            subtree_bytes(&logs),
            before - 150,
            "and the budget stops being billed for them"
        );
        assert_eq!(clear_log_tmp(&logs), 0, "idempotent");
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

    /// A converted rolled log is still the engine's own file. Matching only `.log` meant step 1
    /// of the prune order skipped every `.parquet` — i.e. every rolled log more than one sweep
    /// old — and the budget got paid for out of statement history instead.
    #[test]
    fn a_converted_rolled_log_is_still_prunable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let logs = dir.path().join("logs");
        touch(&logs.join(LIVE_LOG), 10);
        touch(&logs.join("oxidant-2026-08-22.log"), 10);
        touch(&logs.join("oxidant-2026-08-21.parquet"), 10);
        touch(&logs.join("oxidant-2026-08-23-14.2.parquet"), 10);
        // Not ours: the grammar, not a prefix and a suffix.
        touch(&logs.join("oxidant-nightly.log"), 10);
        touch(&logs.join("oxidant-backup.parquet"), 10);
        let mut names: Vec<String> = rolled_logs(&logs)
            .iter()
            .map(|p| p.path.file_name().unwrap().to_string_lossy().to_string())
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "oxidant-2026-08-21.parquet".to_string(),
                "oxidant-2026-08-22.log".to_string(),
                "oxidant-2026-08-23-14.2.parquet".to_string(),
            ]
        );
        // And a rolled exec log is never mistaken for a support bundle, so one path pointed at
        // by both `OXIDANT_LOG_DIR` and `OXIDANT_DUMP_DIR` does not select it twice.
        assert!(!is_dump("oxidant-2026-08-21.parquet"));
        assert!(is_dump("oxidant-backup.parquet"), "still a bundle shape");
    }

    /// Retention is evaluated against a file's **whole period**, not its name parsed as a day —
    /// and weekly therefore rounds *up*, keeping up to six extra days rather than discarding
    /// days that are inside the window.
    #[test]
    fn log_retention_expires_whole_periods_and_weekly_rounds_up() {
        let dir = tempfile::tempdir().expect("tempdir");
        let logs = dir.path().join("logs");
        let now = chrono::Utc.with_ymd_and_hms(2026, 9, 20, 12, 0, 0).unwrap();
        touch(&logs.join(LIVE_LOG), 10);
        // 30 days back from 2026-09-20 is 2026-08-21. A daily file's period ends the next day,
        // so 08-19 and 08-20 are wholly outside the window and 08-21 is not.
        touch(&logs.join("oxidant-2026-08-19.parquet"), 10);
        touch(&logs.join("oxidant-2026-08-20.parquet"), 10);
        touch(&logs.join("oxidant-2026-08-21.log"), 10);
        touch(&logs.join("oxidant-2026-09-19.log"), 10);
        // ISO 2026-W34 is Mon 08-17 .. Sun 08-23; its period ends 08-24, inside the window, so
        // it survives even though most of the week is older than 30 days. W33 ends 08-17 and
        // does not.
        touch(&logs.join("oxidant-2026-W34.parquet"), 10);
        touch(&logs.join("oxidant-2026-W33.parquet"), 10);

        let report = prune_expired_logs(&logs, 30, u64::MAX, now);
        assert_eq!(report.expired, 3, "{:?}", report);
        assert_eq!(report.over_cap, 0);
        let mut left: Vec<String> = std::fs::read_dir(&logs)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        left.sort();
        assert_eq!(
            left,
            vec![
                "oxidant-2026-08-21.log".to_string(),
                "oxidant-2026-09-19.log".to_string(),
                "oxidant-2026-W34.parquet".to_string(),
                LIVE_LOG.to_string(),
            ],
            "the live file is never a candidate, and W34's last day is inside the window"
        );
    }

    /// `OXIDANT_LOG_KEEP_DAYS=0` disables age-based expiry, and the subtree cap still holds —
    /// oldest period first, live file untouched.
    #[test]
    fn the_logs_subtree_cap_prunes_oldest_first_and_never_the_live_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let logs = dir.path().join("logs");
        let now = chrono::Utc.with_ymd_and_hms(2026, 8, 24, 0, 0, 0).unwrap();
        touch(&logs.join(LIVE_LOG), 500);
        touch(&logs.join("oxidant-2026-08-21.log"), 100);
        touch(&logs.join("oxidant-2026-08-22.log"), 100);
        touch(&logs.join("oxidant-2026-08-23.log"), 100);

        let report = prune_expired_logs(&logs, 0, 150, now);
        assert_eq!(report.expired, 0, "keep_days=0 expires nothing");
        assert_eq!(report.over_cap, 2);
        assert_eq!(report.freed_bytes, 200);
        let mut left: Vec<String> = std::fs::read_dir(&logs)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        left.sort();
        assert_eq!(
            left,
            vec!["oxidant-2026-08-23.log".to_string(), LIVE_LOG.to_string()],
            "the newest rolled file stays and the live file is never counted or deleted"
        );
    }

    /// `event_log_dir` joins the budget by **rolling**, never by deleting the live file that
    /// other tools are reading (§8, F16) — and a roll never ends with an empty directory.
    #[test]
    fn the_event_log_rolls_rather_than_being_deleted_and_prunes_oldest_first() {
        let dir = tempfile::tempdir().expect("tempdir");
        let events = dir.path().join("events");
        let now = chrono::Utc.with_ymd_and_hms(2026, 8, 24, 9, 0, 0).unwrap();
        let cap = 1_000;

        // Under half the cap — the roll threshold — nothing happens at all.
        touch(&events.join(LIVE_EVENT_LOG), 400);
        assert_eq!(
            roll_event_log(&events, cap, LogRoll::Daily, now),
            EventLogReport {
                used_bytes: 400,
                ..EventLogReport::default()
            },
            "a live file inside the roll threshold is left alone"
        );
        assert_eq!(names(&events), vec![LIVE_EVENT_LOG.to_string()]);

        // Past it: rolled, and **kept**. A roll that immediately prunes what it created would
        // make every roll a data loss.
        touch(&events.join(LIVE_EVENT_LOG), 600);
        let report = roll_event_log(&events, cap, LogRoll::Daily, now);
        assert!(report.rolled, "the live file is renamed, not truncated");
        assert_eq!(report.pruned, 0, "{report:?}");
        assert_eq!(
            names(&events),
            vec!["events-2026-08-24.jsonl".to_string()],
            "the next `emit` recreates the live file; nothing here deletes it"
        );

        // The second roll takes `.2` and pushes the directory over, so the **oldest** goes —
        // and the generation just rolled stays.
        touch(&events.join(LIVE_EVENT_LOG), 600);
        let report = roll_event_log(&events, cap, LogRoll::Daily, now);
        assert!(report.rolled);
        assert_eq!(report.pruned, 1, "oldest rolled event log goes first");
        assert_eq!(
            names(&events),
            vec!["events-2026-08-24.2.jsonl".to_string()],
            "`.2` sorts before the plain name lexicographically; ordering by period and split \
             is what keeps the prune from taking the newer file"
        );

        // And it converges: every further roll leaves exactly one generation — never none, and
        // always the one just rolled. (The split number restarts once its predecessor has been
        // pruned, which is harmless: the name it reuses is the name of a file that is gone.)
        for gen in 3..8 {
            let body = format!("generation {gen} ").repeat(60);
            std::fs::write(events.join(LIVE_EVENT_LOG), &body).expect("write");
            roll_event_log(&events, cap, LogRoll::Daily, now);
            let left = names(&events);
            assert_eq!(left.len(), 1, "generation {gen}: {left:?}");
            assert_eq!(
                std::fs::read_to_string(events.join(&left[0])).expect("read"),
                body,
                "the survivor is the generation just rolled, not an older one"
            );
        }
    }

    /// `OXIDANT_EVENT_LOG_MAX_BYTES=0` restores today's unbounded behaviour exactly.
    #[test]
    fn a_zero_event_log_cap_is_todays_unbounded_behaviour() {
        let dir = tempfile::tempdir().expect("tempdir");
        let events = dir.path().join("events");
        touch(&events.join(LIVE_EVENT_LOG), 10_000);
        touch(&events.join("events-2020-01-01.jsonl"), 10_000);
        let report = roll_event_log(&events, 0, LogRoll::Daily, chrono::Utc::now());
        assert_eq!(report, EventLogReport::default(), "not even measured");
        assert!(events.join(LIVE_EVENT_LOG).exists());
        assert!(events.join("events-2020-01-01.jsonl").exists());
    }

    /// The event-log sweeper unlinks only the shape it writes. An operator points
    /// `OXIDANT_EVENT_LOG_DIR` at a Spark-history-server path that other tools also write.
    #[test]
    fn the_event_log_sweeper_only_recognises_its_own_rolled_files() {
        assert!(is_rolled_event_log("events-2026-08-24.jsonl"));
        assert!(is_rolled_event_log("events-2026-08-24.2.jsonl"));
        assert!(!is_rolled_event_log(LIVE_EVENT_LOG));
        assert!(!is_rolled_event_log("application_1234_0001"));
        assert!(!is_rolled_event_log("spark-events.jsonl"));
        assert!(!is_rolled_event_log("events-2026-08-24.jsonl.gz"));
    }

    /// `event_log_dir` counts against the disk budget as of PR3 — but only when it is bounded.
    /// Billing an unprunable tree would make the sweeper delete statement history to pay for it.
    #[test]
    fn the_event_log_dir_joins_the_budget_only_when_it_is_bounded() {
        let dir = tempfile::tempdir().expect("tempdir");
        let events = dir.path().join("elsewhere/events");
        let mut cfg = HistoryConfig::for_root(&dir.path().join("data"));
        let has_events = |cfg: &HistoryConfig| {
            budget_roots(cfg)
                .into_iter()
                .find(|r| r.path() == events)
                .map(|r| r.owned_only)
        };
        assert_eq!(has_events(&cfg), None, "unset: not in the budget");

        cfg.event_log_dir = Some(events.clone());
        cfg.event_log_max_bytes = 2 * 1024 * 1024 * 1024;
        assert_eq!(
            has_events(&cfg),
            Some(true),
            "bounded: counted, and only for the generations the engine can prune"
        );

        cfg.event_log_max_bytes = 0;
        assert_eq!(
            has_events(&cfg),
            None,
            "unbounded by the operator's explicit choice: not billed to a budget that cannot \
             prune it"
        );
    }

    /// **H2, and PR2's stated hazard.** The whole reason an operator sets
    /// `OXIDANT_EVENT_LOG_DIR` is to point it at a Spark-history-server path *other tools write*.
    /// Those bytes cannot be pruned — `roll_event_log` only ever touches `events[-<period>].jsonl`
    /// — so billing them to `OXIDANT_DISK_MAX_BYTES` pins `used` over the budget permanently and
    /// runs the prune order to exhaustion every five minutes: every rolled log, every dump, then
    /// `prune_oldest_statement()` until the journal is empty, then every live result file.
    ///
    /// The engine pays for what it can prune and reports the rest.
    #[test]
    fn a_co_tenant_in_the_event_log_dir_is_reported_but_never_billed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let events = dir.path().join("spark-events");
        let mut cfg = HistoryConfig::for_root(&dir.path().join("data"));
        cfg.event_log_dir = Some(events.clone());
        cfg.event_log_max_bytes = 1024;

        // A Spark history server's own files, and ours.
        touch(&events.join("application_1755_0001"), 20_000);
        touch(&events.join("application_1755_0002.inprogress"), 5_000);
        touch(&events.join(LIVE_EVENT_LOG), 300);
        touch(&events.join("events-2026-08-23.jsonl"), 100);

        let usage = measure_roots(&budget_roots(&cfg));
        assert_eq!(
            usage.billed, 400,
            "only the live event log and the generations the engine rolled"
        );
        assert_eq!(
            usage.foreign, 25_000,
            "the co-tenant's bytes are measured and reported, never billed"
        );
    }

    /// The same rule when the directory sits *inside* the data dir: containment would otherwise
    /// hand it to the root's recursive walk, which bills everything it finds.
    #[test]
    fn an_event_log_dir_inside_the_root_is_still_billed_owned_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("data");
        let events = root.join("spark-events");
        let mut cfg = HistoryConfig::for_root(&root);
        cfg.event_log_dir = Some(events.clone());
        cfg.event_log_max_bytes = 1024;

        touch(&root.join("logs").join("oxidant.log"), 70);
        touch(&events.join("application_1755_0001"), 20_000);
        touch(&events.join(LIVE_EVENT_LOG), 30);

        let usage = measure_roots(&budget_roots(&cfg));
        assert_eq!(usage.billed, 100, "the live log plus our own event log");
        assert_eq!(usage.foreign, 20_000);
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

/// One subtree the disk budget covers, and how much of it the engine is willing to pay for.
///
/// **The engine bills itself only for files it can prune.** Every root but one is measured whole,
/// because everything under it is the engine's: it created the directory and it wrote every file
/// in it. `event_log_dir` is the exception, and it is the whole reason this type exists — see
/// [`budget_roots`].
#[derive(Debug, Clone)]
pub(crate) struct BudgetRoot {
    path: PathBuf,
    /// Bill only the live `events.jsonl` and the `events-<period>[.N].jsonl` generations this
    /// engine rolled; report everything else as foreign.
    owned_only: bool,
    /// Subtrees of this root measured as roots in their own right, so nothing is billed twice —
    /// an `event_log_dir` that happens to sit *inside* the data dir still gets its own rule.
    excluded: Vec<PathBuf>,
}

/// What one root costs: what the budget charges for, and what a co-tenant put there.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RootUsage {
    /// Bytes the engine owns and can prune. This is what `OXIDANT_DISK_MAX_BYTES` governs.
    pub billed: u64,
    /// Bytes in a shared directory that another tool wrote. Reported, never billed.
    pub foreign: u64,
}

impl RootUsage {
    fn add(self, other: Self) -> Self {
        Self {
            billed: self.billed.saturating_add(other.billed),
            foreign: self.foreign.saturating_add(other.foreign),
        }
    }
}

impl BudgetRoot {
    /// A root the engine owns outright.
    pub(crate) fn subtree(path: PathBuf) -> Self {
        Self {
            path,
            owned_only: false,
            excluded: Vec::new(),
        }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn measure(&self) -> RootUsage {
        if !self.owned_only {
            return RootUsage {
                billed: subtree_bytes_excluding(&self.path, &self.excluded),
                foreign: 0,
            };
        }
        let billed = owned_event_log_bytes(&self.path);
        RootUsage {
            billed,
            foreign: subtree_bytes(&self.path).saturating_sub(billed),
        }
    }
}

/// Total the roots: what the budget charges for, and what it merely reports.
pub(crate) fn measure_roots(roots: &[BudgetRoot]) -> RootUsage {
    roots
        .iter()
        .fold(RootUsage::default(), |acc, root| acc.add(root.measure()))
}

/// The bytes the engine itself wrote into `event_log_dir`: the live file plus its rolled
/// generations, flat, because those are the only names it ever creates there.
fn owned_event_log_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut total = 0;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name != LIVE_EVENT_LOG && !is_rolled_event_log(name) {
            continue;
        }
        let Ok(meta) = entry.path().symlink_metadata() else {
            continue;
        };
        if meta.is_file() {
            total += meta.len();
        }
    }
    total
}

/// [`subtree_bytes`], skipping subtrees that are measured as roots of their own.
fn subtree_bytes_excluding(dir: &Path, excluded: &[PathBuf]) -> u64 {
    if excluded.is_empty() {
        return subtree_bytes(dir);
    }
    #[cfg(test)]
    SUBTREE_WALKS.with(|c| c.set(c.get() + 1));
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut total = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(meta) = path.symlink_metadata() else {
            continue;
        };
        if meta.is_dir() {
            if excluded.iter().any(|e| e == &path) {
                continue;
            }
            total += subtree_bytes_excluding(&path, excluded);
        } else if meta.is_file() {
            total += meta.len();
        }
    }
    total
}

/// The distinct subtrees the disk budget covers, deduped so nothing is billed twice.
///
/// `OXIDANT_HISTORY_DIR` / `OXIDANT_RESULT_DIR` / `OXIDANT_LOG_DIR` / `OXIDANT_DUMP_DIR` each win
/// over the root and may point *outside* it, and §3 says an overridden subtree is still counted
/// against the budget — so the candidates are measured as a set, with any path already contained
/// in a shallower one dropped.
///
/// **`event_log_dir` is billed for the engine's own files only** (§8/F16). PR2 left it out of the
/// budget entirely and said why: counting a directory the engine could not prune "would pin
/// `disk: over_budget` on for anyone with a large Spark-history-server directory". PR3 gained the
/// ability to roll and prune the `events-<period>[.N].jsonl` files it writes there — but *only*
/// those. The whole reason an operator sets `OXIDANT_EVENT_LOG_DIR` is to point it at a path other
/// tools write, and a recursive `subtree_bytes` over it bills every one of their bytes to a budget
/// that can never reclaim them: `used` stays over `disk_max_bytes` forever, so the sweep runs the
/// full prune order to exhaustion — every rolled log, every dump, then `prune_oldest_statement()`
/// in a `while used > disk_max_bytes` loop until the journal is empty, then every live result
/// file. Every five minutes, to pay for a co-tenant's 20 GiB.
///
/// So the rule is: **the engine pays for what it can prune, and reports the rest.** The foreign
/// bytes are measured and named in the sweep line; they drive nothing.
pub(crate) fn budget_roots(cfg: &HistoryConfig) -> Vec<BudgetRoot> {
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
    candidates.sort_by_key(|p| p.components().count());
    // Only when it is actually bounded: with `OXIDANT_EVENT_LOG_MAX_BYTES=0` the directory is
    // unbounded by the operator's explicit choice, nothing there is rolled or pruned, and even
    // the engine's own bytes in it are none of the budget's business.
    let event_log = match (&cfg.event_log_dir, cfg.event_log_max_bytes > 0) {
        (Some(dir), true) => Some(dir.clone()),
        _ => None,
    };
    let mut kept: Vec<BudgetRoot> = Vec::new();
    for candidate in candidates {
        if kept.iter().any(|k| candidate.starts_with(&k.path)) {
            continue;
        }
        kept.push(BudgetRoot::subtree(candidate));
    }
    if let Some(dir) = event_log {
        // A directory that *is* one of the engine's own roots is not a co-tenant's: the operator
        // pointed the event log at the data dir, and the budget already owns everything in it.
        if !kept.iter().any(|root| root.path == dir) {
            // Its own entry even when it sits inside a kept root: containment would otherwise
            // hand it to that root's recursive walk, which bills every foreign byte in it.
            for root in &mut kept {
                if dir.starts_with(&root.path) {
                    root.excluded.push(dir.clone());
                }
            }
            kept.push(BudgetRoot {
                path: dir,
                owned_only: true,
                excluded: Vec::new(),
            });
        }
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
        // One predicate for "is this file mine", shared with the budget's step-1 candidate list
        // and with `is_dump`'s complement — so what the sweeper measures, what it orders and
        // what it unlinks cannot disagree.
        if !is_rolled_log(name) {
            continue;
        }
        let Some((period, split, _)) = crate::logging::parse_rolled_name(name) else {
            continue;
        };
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

/// §6b retention for `dumps/`: a support bundle expires 24 h after it was written.
///
/// Age is the file's **mtime**, not a period parsed from its name: a dump's name is a uuid, and
/// the window it covers is the operator's, not the clock's. Only names [`is_dump`] recognises
/// are candidates, so a bundle an operator dropped into `OXIDANT_DUMP_DIR` by hand is measured
/// and never unlinked — the same rule as `logs/`.
pub(crate) fn prune_expired_dumps(
    dumps_dir: &Path,
    ttl_secs: i64,
    now: DateTime<Utc>,
) -> LogPruneReport {
    let mut report = LogPruneReport::default();
    if ttl_secs <= 0 {
        return report;
    }
    let cutoff = now - chrono::Duration::seconds(ttl_secs);
    for file in dumps(dumps_dir) {
        let age: DateTime<Utc> = file.mtime.into();
        if age > cutoff {
            continue;
        }
        if let Some(freed) = remove(&file) {
            report.expired += 1;
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

/// Rolled event logs, oldest **period** first.
///
/// Ordered by period and split, not by mtime-then-name: `events-2026-08-24.2.jsonl` sorts
/// *before* `events-2026-08-24.jsonl` lexicographically (`2` < `j`), so a name-tiebroken sort
/// would prune the second-newest generation and keep the oldest.
pub(crate) fn rolled_event_logs(dir: &Path) -> Vec<Prunable> {
    let mut keyed: Vec<(Option<DateTime<Utc>>, u32, Prunable)> =
        flat_files(dir, is_rolled_event_log)
            .into_iter()
            .map(|file| {
                let (end, split) = file
                    .path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(parse_rolled_event_log_name)
                    .map(|(period, split)| (period.end(), split))
                    .unwrap_or((None, 0));
                (end, split, file)
            })
            .collect();
    keyed.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| a.1.cmp(&b.1))
            .then_with(|| a.2.path.cmp(&b.2.path))
    });
    keyed.into_iter().map(|(_, _, file)| file).collect()
}

/// The next split for `period`: one past the highest on disk, so the sequence is monotone
/// while any file of the period survives.
fn next_event_log_split(dir: &Path, period: LogPeriod) -> u32 {
    let stem = period.stem();
    let mut highest = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some((found, split)) = parse_rolled_event_log_name(name) else {
                continue;
            };
            if found.stem() == stem {
                highest = highest.max(split);
            }
        }
    }
    highest + 1
}

/// `events-<period>[.N].jsonl` → its period and split.
fn parse_rolled_event_log_name(name: &str) -> Option<(LogPeriod, u32)> {
    let body = name
        .strip_prefix(ROLLED_EVENT_PREFIX)?
        .strip_suffix(".jsonl")?;
    LogPeriod::parse(body)
}

/// What one `event_log_dir` pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct EventLogReport {
    /// The live `events.jsonl` was renamed to a periodised file.
    pub rolled: bool,
    pub pruned: usize,
    pub freed_bytes: u64,
    /// The engine's **own** bytes left in the directory after the pass — the live file plus its
    /// rolled generations. May exceed `OXIDANT_EVENT_LOG_MAX_BYTES` by up to the live file plus
    /// one rolled generation: the newest rolled file is never taken, and the live one is rolled
    /// rather than deleted. Files other tools wrote here are not counted; they are neither
    /// prunable nor the engine's to bill.
    pub used_bytes: u64,
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
/// The live file rolls at **half** `max_bytes` so the directory keeps roughly two generations
/// rather than oscillating between one full file and none — see the comment on the trigger.
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
    // **Deviation from §8's literal wording**, which says the roll happens "when the cap is
    // exceeded". Rolling only once the live file has reached the *whole* cap makes the very
    // first prune pass delete the file it just created — the directory then oscillates between
    // "one file at the cap" and "empty", and an operator loses every event at each roll.
    // Rolling at half the cap keeps roughly two generations: roll, roll, and only the third
    // roll prunes the oldest. The ceiling is therefore **not** exact — see the prune below,
    // which never takes the newest generation and says by how much that can overshoot.
    if live_bytes > (max_bytes / 2).max(1) {
        // `LogRoll::Off` turns the *exec* log writer off; the event log still needs a period to
        // name its roll after, and daily is the design's default.
        let roll = if roll == LogRoll::Off {
            LogRoll::Daily
        } else {
            roll
        };
        if let Some(period) = LogPeriod::of(now, roll) {
            // **Highest existing + 1**, never "the first free number" — the same rule
            // `logging::writer::next_split` uses, and for a sharper reason here. Splits are
            // pruned out from under the allocator, so "first free" hands out `1` again after
            // `.1` has gone; the file just rolled would then sort as the *oldest* generation of
            // its period and the very next prune would take it, keeping the stale `.2`.
            let target = dir.join(rolled_event_log_name(
                period,
                next_event_log_split(dir, period),
            ));
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
    // Oldest-first, **stopping short of the newest rolled generation**. Only the live file is
    // protected in §3's exec-log prune order, but the event log has no equivalent of "rotate
    // instead of delete" for its rolled files: one generation can be larger than the whole cap
    // (the sweep runs every five minutes, and `emit` does not stop between passes), and taking
    // it would mean every roll ended with an empty directory. So the ceiling may be exceeded by
    // at most the live file plus one rolled generation, and the sweep line says how much is
    // there.
    //
    // **The total counts only the engine's own files.** `subtree_bytes` here would count the
    // Spark history server's `application_*` files too, and since the engine cannot prune one
    // byte of those, a co-tenant larger than `max_bytes` made the loop take every prunable
    // generation on every pass and still never reach the cap — it would converge on keeping
    // exactly one generation forever, with no signal that the cap was structurally unreachable.
    let mut total: u64 = owned_event_log_bytes(dir);
    let rolled = rolled_event_logs(dir);
    let prunable = rolled.len().saturating_sub(1);
    for file in rolled.iter().take(prunable) {
        if total <= max_bytes {
            break;
        }
        if let Some(freed) = remove(file) {
            total = total.saturating_sub(freed);
            report.pruned += 1;
            report.freed_bytes += freed;
        }
    }
    report.used_bytes = total;
    if report.rolled || report.pruned > 0 {
        tracing::info!(
            dir = %dir.display(),
            rolled = report.rolled,
            pruned = report.pruned,
            freed_bytes = report.freed_bytes,
            used_bytes = report.used_bytes,
            max_bytes,
            "event log rolled under OXIDANT_EVENT_LOG_MAX_BYTES"
        );
    }
    report
}
