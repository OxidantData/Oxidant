//! One process per journal (§3c).
//!
//! Two processes sharing a journal would interleave `O_APPEND` writes — atomic only for small
//! writes, so a large `sql` line tears — and both would roll to the same next segment name.
//! `local-cluster` workers are in-process and fine; `oxidant worker --port` is a separate
//! process, and the Docker/EC2 topologies routinely start a driver and a worker from one
//! working directory.
//!
//! **The lock is taken on the effective statements directory, not on `cfg.root`.** §3c says
//! "one process per data dir", but `OXIDANT_HISTORY_DIR` is an independent knob and an explicit
//! override *wins over the root* (§3, "Root and precedence"). Locking the root therefore guards
//! the wrong thing in both directions: two processes with distinct roots and one
//! `OXIDANT_HISTORY_DIR` take two different locks and both succeed onto one journal — exactly
//! the tearing this lock exists to prevent — while two processes with one root and distinct
//! history dirs are refused for no reason. The journal's own directory is what must be
//! exclusive, so that is what is locked; the holder record carries the data dir it was booted
//! with, so a root/history-dir disagreement is named in the error rather than left to be guessed.
//!
//! The lock is a pid-stamped file with a boot-time staleness check — the fallback §3c names,
//! taken because the `flock` path would mean linking `libc`/`rustix` directly for one syscall and
//! this design adds no dependencies. Liveness is read through `sysinfo`, which this crate already
//! depends on for `/api/v1/cluster/status`.
//!
//! Two details make the fallback safe against the races an `O_CREAT|O_EXCL`-then-`write` spelling
//! has:
//!
//! 1. **The body is complete before the name exists.** The holder record is written and fsynced
//!    to a private temporary, then `hard_link`ed into place — the link is the exclusive create,
//!    and it publishes a file that already has its contents. `create_new` followed by `write_all`
//!    leaves a window, a full file write plus an fsync wide, in which a competing acquirer reads
//!    an empty file, parses it as `pid: 0`, finds pid 0 not running, and deletes a *live* lock.
//!    An empty or unparseable lock younger than [`BODY_GRACE`] is treated as held anyway, so even
//!    a lock written by some older spelling is not stolen mid-write.
//! 2. **Taking over a stale lock is itself exclusive.** Two processes that read the same dead
//!    holder would both remove and both create — the second removing the first's *fresh* lock,
//!    leaving two writers on one journal, which is the `O_APPEND` tearing §3c exists to prevent.
//!    A takeover therefore happens under `.lock.claim`, and the holder is re-read under it.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use super::config::HistoryConfig;
use super::fs_util;

/// Holds the statements dir for this process; releases it on drop.
#[derive(Debug)]
pub(crate) struct JournalDirLock {
    path: PathBuf,
    /// A re-entrant acquisition (same pid, e.g. a second server in one test process) does not
    /// own the file and must not remove it.
    owned: bool,
}

impl Drop for JournalDirLock {
    fn drop(&mut self) {
        if self.owned {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

impl JournalDirLock {
    #[cfg(test)]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

/// How long an empty or unparseable lock body is still treated as *held*.
///
/// With the `hard_link` publish below there is no window in which a lock exists without its
/// body, so this only covers a file some other spelling left behind. Erring towards "held" is the
/// safe direction: refusing to boot is recoverable, two writers on one journal is not.
const BODY_GRACE: Duration = Duration::from_secs(30);

/// Serial for this process's temporaries, so two threads never share one `.tmp` name.
static TMP_SERIAL: AtomicU64 = AtomicU64::new(0);

/// The lockfile that guards a journal: `<statements-dir>/.lock`.
///
/// Not `<root>/.lock`: the journal is what must be exclusive, and `OXIDANT_HISTORY_DIR` can put
/// it anywhere (see the module docs).
fn lock_path(cfg: &HistoryConfig) -> PathBuf {
    cfg.statements_dir.join(".lock")
}

/// The holder record. `root` is in it because the journal dir and the data dir are independent
/// knobs: when they disagree, this is what makes the conflict readable in the error.
fn holder_body(pid: u32, cfg: &HistoryConfig) -> String {
    // Built through `serde_json` rather than `format!`, because a path can contain a quote or a
    // backslash and an unescaped one would publish a lock nobody can parse.
    format!(
        "{}\n",
        serde_json::json!({
            "pid": pid,
            "role": cfg.role,
            "port": cfg.port,
            "root": cfg.root.display().to_string(),
        })
    )
}

/// Take the exclusive lock on the effective statements dir, or explain exactly who holds it.
pub(crate) fn acquire(cfg: &HistoryConfig) -> Result<JournalDirLock, String> {
    let dir = cfg.statements_dir.clone();
    fs_util::create_dir_secure(&dir).map_err(|e| {
        format!(
            "oxidant: cannot create the statement journal dir {}: {e}",
            dir.display()
        )
    })?;
    sweep_abandoned_temporaries(&dir);
    let path = lock_path(cfg);
    let body = holder_body(std::process::id(), cfg);

    // The uncontended path, and the overwhelmingly common one.
    match publish(&path, &body, &dir) {
        Ok(()) => return claim_verified(cfg, path),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => {
            return Err(format!(
                "oxidant: cannot create lockfile {}: {e}",
                path.display()
            ))
        }
    }

    if let Some(shared) = re_entrant(&path, cfg)? {
        return Ok(shared);
    }

    // The holder is gone. Taking over is exclusive: without the claim, two processes that read
    // the same dead holder both remove and both create, and the second removes the first's fresh
    // lock — two writers on one journal.
    let claim = Claim::take(&dir)?;
    // Re-read under the claim: whoever else was deciding has finished by now.
    if let Some(shared) = re_entrant(&path, cfg)? {
        return Ok(shared);
    }
    let _ = std::fs::remove_file(&path);
    fs_util::fsync_dir(&dir);
    publish(&path, &body, &dir).map_err(|e| {
        format!(
            "oxidant: cannot take over the stale lockfile {}: {e}",
            path.display()
        )
    })?;
    drop(claim);
    claim_verified(cfg, path)
}

/// Is the existing lock one we may share or must refuse? `None` means it is stale and takeable.
///
/// `Ok(Some(_))` is the same-process case (a second server in one process — the in-process
/// `local-cluster` shape, and every test that boots two services): share the directory rather
/// than refusing to start against ourselves.
fn re_entrant(path: &Path, cfg: &HistoryConfig) -> Result<Option<JournalDirLock>, String> {
    let holder = read_holder(path);
    if holder.pid == std::process::id() {
        return Ok(Some(JournalDirLock {
            path: path.to_path_buf(),
            owned: false,
        }));
    }
    if holder_is_held(&holder, path) {
        return Err(lock_error(cfg, &holder));
    }
    Ok(None)
}

/// Confirm the lock we just published is still ours before reporting that we own it.
///
/// A process that lost a takeover race would otherwise report success while another process's
/// record sits in the file — and would delete that process's lock on drop.
fn claim_verified(cfg: &HistoryConfig, path: PathBuf) -> Result<JournalDirLock, String> {
    let holder = read_holder(&path);
    if holder.pid == std::process::id() {
        return Ok(JournalDirLock { path, owned: true });
    }
    Err(lock_error(cfg, &holder))
}

/// Write `body` to a private temporary, fsync it, and `hard_link` it into `path`.
///
/// The link is the exclusive create — `AlreadyExists` means someone else holds it — and what it
/// publishes already has its full, durable contents. There is no instant at which `path` exists
/// and is empty.
fn publish(path: &Path, body: &str, dir: &Path) -> std::io::Result<()> {
    let serial = TMP_SERIAL.fetch_add(1, Ordering::Relaxed);
    let tmp = dir.join(format!(".lock.{}.{serial}.tmp", std::process::id()));
    {
        let mut file = fs_util::create_secure(&tmp)?;
        file.write_all(body.as_bytes())?;
        file.sync_all()?;
    }
    let linked = std::fs::hard_link(&tmp, path);
    let _ = std::fs::remove_file(&tmp);
    fs_util::fsync_dir(dir);
    linked
}

/// Does this lock still speak for a running process?
///
/// A `pid` of 0 means the body was empty or did not parse. That is either a lock some other
/// spelling is in the middle of writing or a corrupt one; within [`BODY_GRACE`] of its last
/// modification it is treated as held, so a live lock is never mistaken for a dead one.
fn holder_is_held(holder: &Holder, path: &Path) -> bool {
    if holder.pid != 0 {
        return pid_is_alive(holder.pid);
    }
    modified_within(path, BODY_GRACE)
}

fn modified_within(path: &Path, grace: Duration) -> bool {
    let Ok(modified) = std::fs::metadata(path).and_then(|m| m.modified()) else {
        return false;
    };
    SystemTime::now()
        .duration_since(modified)
        .map(|age| age < grace)
        .unwrap_or(true)
}

/// Remove `.lock.<pid>.<n>.tmp` files a crashed boot left behind, so they cannot accumulate.
fn sweep_abandoned_temporaries(root: &Path) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(".lock.")
            && name.ends_with(".tmp")
            && !modified_within(&entry.path(), BODY_GRACE)
        {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

/// Exclusive right to take over a stale `.lock`, released on drop.
struct Claim {
    path: PathBuf,
}

impl Drop for Claim {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

impl Claim {
    fn take(dir: &Path) -> Result<Self, String> {
        let path = dir.join(".lock.claim");
        let body = format!("{{\"pid\":{}}}\n", std::process::id());
        for attempt in 0..2 {
            match publish(&path, &body, dir) {
                Ok(()) => return Ok(Self { path }),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    let holder = read_holder(&path);
                    if attempt == 0 && !holder_is_held(&holder, &path) {
                        // The claimant died mid-takeover. Clear it and try once.
                        let _ = std::fs::remove_file(&path);
                        fs_util::fsync_dir(dir);
                        continue;
                    }
                    return Err(format!(
                        "oxidant: the statement journal ({}) is being taken over by pid {}.\n         \
                         Another process is recovering this directory's lock; retry in a moment, \
                         or set OXIDANT_HISTORY_DIR (or OXIDANT_DATA_DIR) to a distinct path for \
                         this process.",
                        dir.display(),
                        holder.pid
                    ));
                }
                Err(e) => {
                    return Err(format!(
                        "oxidant: cannot create takeover claim {}: {e}",
                        path.display()
                    ))
                }
            }
        }
        unreachable!("the loop returns on every path")
    }
}

/// The second-process error — §3c's text, re-pointed at what is actually locked.
///
/// It names the journal (the thing that must be exclusive), the holder's pid/role/port, and the
/// data dir the holder booted with. That last field is what makes the interesting case
/// diagnosable: when the holder's root is *not* this process's root, the two are colliding
/// through `OXIDANT_HISTORY_DIR`, and the §3c advice — a distinct `OXIDANT_DATA_DIR`, or
/// `OXIDANT_DATA_DIR_PER_PROCESS=1` — cannot help, because an explicit history dir wins over the
/// root. Printing the shipped advice there would send an operator to the one knob that does
/// nothing, so the disagreement is called out and the history dir is what is suggested.
fn lock_error(cfg: &HistoryConfig, holder: &Holder) -> String {
    let ours = cfg.root.display().to_string();
    let mut out = format!(
        "oxidant: the statement journal ({}) is locked by pid {} (role={}, port={}, data dir={}).",
        cfg.statements_dir.display(),
        holder.pid,
        holder.role,
        holder.port,
        holder.root_or_unknown(),
    );
    if holder.disagrees_with_root(&ours) {
        out.push_str(&format!(
            "\n         \
             This process's data dir ({ours}) is not the holder's ({}), so the two are sharing one\n         \
             journal through OXIDANT_HISTORY_DIR. An explicit history dir wins over the root:\n         \
             set OXIDANT_HISTORY_DIR to a distinct path for this process. Changing OXIDANT_DATA_DIR,\n         \
             including OXIDANT_DATA_DIR_PER_PROCESS=1, will not separate them while it is set.",
            holder.root,
        ));
        return out;
    }
    out.push_str(&format!(
        "\n         \
         History and logs are per-process. Set OXIDANT_DATA_DIR (or OXIDANT_HISTORY_DIR, which\n         \
         moves only the journal) to a distinct path for this process, or set\n         \
         OXIDANT_DATA_DIR_PER_PROCESS=1 to use {ours}/<role>-<port>/."
    ));
    out
}

#[derive(Debug)]
struct Holder {
    pid: u32,
    role: String,
    port: u16,
    /// The data dir the holder booted with; empty when the lock predates this field.
    root: String,
}

impl Holder {
    fn root_or_unknown(&self) -> &str {
        if self.root.is_empty() {
            "unknown"
        } else {
            &self.root
        }
    }

    /// Do the holder and this process disagree about which root owns the journal? A lock without
    /// a recorded root says nothing either way, so it is not a disagreement.
    fn disagrees_with_root(&self, ours: &str) -> bool {
        !self.root.is_empty() && self.root != ours
    }
}

fn read_holder(path: &Path) -> Holder {
    let mut body = String::new();
    if let Ok(mut f) = std::fs::File::open(path) {
        let _ = f.read_to_string(&mut body);
    }
    let parsed: Option<serde_json::Value> = serde_json::from_str(&body).ok();
    let v = parsed.unwrap_or(serde_json::Value::Null);
    Holder {
        pid: v.get("pid").and_then(|x| x.as_u64()).unwrap_or(0) as u32,
        role: v
            .get("role")
            .and_then(|x| x.as_str())
            .unwrap_or("unknown")
            .to_string(),
        port: v.get("port").and_then(|x| x.as_u64()).unwrap_or(0) as u16,
        root: v
            .get("root")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string(),
    }
}

/// Is the recorded holder still running? A pid of 0 never is.
fn pid_is_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    let mut sys = sysinfo::System::new();
    sys.refresh_processes_specifics(
        sysinfo::ProcessesToUpdate::Some(&[sysinfo::Pid::from_u32(pid)]),
        true,
        sysinfo::ProcessRefreshKind::nothing(),
    );
    sys.process(sysinfo::Pid::from_u32(pid)).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create the journal dir a test is about to plant a lockfile in, and hand back its path.
    ///
    /// `acquire` creates it too; a test that writes the lockfile *before* acquiring has to.
    fn journal_dir(cfg: &HistoryConfig) -> PathBuf {
        std::fs::create_dir_all(&cfg.statements_dir).expect("mkdir statements dir");
        cfg.statements_dir.clone()
    }

    #[test]
    fn a_live_holder_makes_the_second_acquisition_fail_loudly() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = HistoryConfig::for_root(dir.path());
        let _held = acquire(&cfg).expect("first acquisition");
        // Rewrite the lockfile as if another live process (this test binary's parent shell is
        // not portable; pid 1 always exists) holds it.
        std::fs::write(
            lock_path(&cfg),
            "{\"pid\":1,\"role\":\"driver\",\"port\":15002}",
        )
        .expect("write");
        let err = acquire(&cfg).expect_err("must refuse a live holder");
        assert!(err.contains("is locked by pid 1"), "{err}");
        assert!(err.contains("role=driver, port=15002"), "{err}");
        assert!(err.contains("OXIDANT_DATA_DIR_PER_PROCESS=1"), "{err}");
    }

    /// The lock guards the journal, so it lives in the journal's own directory — not at
    /// `<root>/.lock`, which `OXIDANT_HISTORY_DIR` can point away from.
    #[test]
    fn the_lock_lives_in_the_effective_statements_dir() {
        let root = tempfile::tempdir().expect("tempdir");
        let history = tempfile::tempdir().expect("tempdir");
        let cfg = HistoryConfig::for_root_with_history_dir(root.path(), history.path());
        let lock = acquire(&cfg).expect("acquire");
        assert_eq!(lock.path(), history.path().join("statements").join(".lock"));
        assert!(
            !root.path().join(".lock").exists(),
            "and nothing is left at the root, which guards no journal"
        );
    }

    /// The hole this closes: `OXIDANT_HISTORY_DIR` is independent of the root, so two processes
    /// with distinct roots can share one journal. Locking `cfg.root` gave them two *different*
    /// locks, both acquired, and two `O_APPEND` writers on one set of segments — the tearing §3c
    /// exists to prevent. Locking the journal dir makes the second one fail, and the holder
    /// record names the data dir it booted with so the collision is diagnosable.
    #[test]
    fn two_roots_sharing_one_history_dir_contend_for_one_lock() {
        let history = tempfile::tempdir().expect("tempdir");
        let root_a = tempfile::tempdir().expect("tempdir");
        let root_b = tempfile::tempdir().expect("tempdir");
        let mut cfg_a = HistoryConfig::for_root_with_history_dir(root_a.path(), history.path());
        cfg_a.role = "driver".to_string();
        cfg_a.port = 15002;
        let mut cfg_b = HistoryConfig::for_root_with_history_dir(root_b.path(), history.path());
        cfg_b.role = "worker".to_string();
        cfg_b.port = 15003;
        assert_eq!(
            cfg_a.statements_dir, cfg_b.statements_dir,
            "the two roots resolve to one journal, which is the whole point"
        );
        assert_ne!(cfg_a.root, cfg_b.root);

        // Process A holds the journal. Pid 1 stands in for "another live process" — the body is
        // written by the production encoder, so this is exactly what A would have published.
        let dir = journal_dir(&cfg_a);
        std::fs::write(dir.join(".lock"), holder_body(1, &cfg_a)).expect("write");

        let err = acquire(&cfg_b).expect_err("the second process must not get the journal too");
        assert!(err.contains("is locked by pid 1"), "{err}");
        assert!(err.contains("role=driver, port=15002"), "{err}");
        // The holder is named by the dir it locked *and* the root it booted with, which is what
        // makes a root/history-dir disagreement readable rather than baffling.
        assert!(
            err.contains(&cfg_a.statements_dir.display().to_string()),
            "{err}"
        );
        assert!(err.contains(&cfg_a.root.display().to_string()), "{err}");
        assert!(err.contains(&cfg_b.root.display().to_string()), "{err}");
        assert!(err.contains("OXIDANT_HISTORY_DIR"), "{err}");
        // And it must not send the operator to the knob that cannot separate them.
        assert!(
            err.contains("will not separate them"),
            "the per-process root knob is useless here and the error must say so: {err}"
        );
        assert!(
            std::fs::read_to_string(dir.join(".lock"))
                .expect("lock")
                .contains("\"pid\":1"),
            "and the holder's lock is untouched"
        );
    }

    #[test]
    fn a_stale_lockfile_is_taken_over() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = HistoryConfig::for_root(dir.path());
        // A pid that cannot be running: the max pid value is not allocatable.
        std::fs::write(
            journal_dir(&cfg).join(".lock"),
            "{\"pid\":4294967294,\"role\":\"driver\",\"port\":1}",
        )
        .expect("write");
        let lock = acquire(&cfg).expect("stale lock is taken over");
        assert!(lock.path().exists());
    }

    /// Backdate a file's mtime, so the [`BODY_GRACE`] branches can both be exercised.
    fn age(path: &Path, by: Duration) {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open for set_times");
        let when = SystemTime::now() - by;
        file.set_times(std::fs::FileTimes::new().set_modified(when))
            .expect("set_times");
    }

    /// M6, race 2: a lock whose body is mid-write must not read as stale.
    ///
    /// `create_new` then `write_all` then `sync_all` leaves a window — a full file write plus an
    /// fsync — in which the file exists and is empty. A competing acquirer read that as
    /// `pid: 0`, found pid 0 not running, and deleted a *live* process's lock.
    ///
    /// Publishing through `hard_link` closes the window outright; the grace below is what keeps
    /// a file written by any other spelling from being stolen.
    #[test]
    fn a_lock_whose_body_is_still_being_written_is_not_treated_as_stale() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = HistoryConfig::for_root(dir.path());
        // Exactly what the old spelling left visible between `create_new` and `write_all`.
        std::fs::write(journal_dir(&cfg).join(".lock"), b"").expect("write");

        let err = acquire(&cfg).expect_err("an empty lock body must not read as a dead holder");
        assert!(err.contains("is locked by pid 0"), "{err}");
        assert!(
            lock_path(&cfg).exists(),
            "and the other process's lock is still there"
        );
    }

    /// The grace is a grace, not a deadlock: a lock that has been unparseable for longer than
    /// any write could take is genuinely corrupt and is taken over.
    #[test]
    fn a_long_dead_unparseable_lock_is_still_taken_over() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = HistoryConfig::for_root(dir.path());
        let path = journal_dir(&cfg).join(".lock");
        std::fs::write(&path, b"not json at all").expect("write");
        age(&path, BODY_GRACE * 2);

        let lock = acquire(&cfg).expect("a long-abandoned lock is takeable");
        assert!(lock.path().exists());
        assert_eq!(read_holder(lock.path()).pid, std::process::id());
    }

    /// M6, race 1: two processes that read the same stale holder must not both take over — the
    /// second would remove the first's *fresh* lock, leaving two writers on one journal, which is
    /// the `O_APPEND` tearing §3c exists to prevent.
    ///
    /// The takeover happens under `.lock.claim`, so a second acquirer that arrives mid-takeover
    /// backs off instead of removing anything.
    #[test]
    fn a_takeover_in_progress_blocks_a_second_one() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = HistoryConfig::for_root(dir.path());
        let journal = journal_dir(&cfg);
        // A stale lock, which on its own would be taken over...
        std::fs::write(
            journal.join(".lock"),
            "{\"pid\":4294967294,\"role\":\"driver\",\"port\":1}",
        )
        .expect("write");
        // ...but another live process is already taking it over. Pid 1 always exists.
        std::fs::write(journal.join(".lock.claim"), "{\"pid\":1}").expect("write");

        let err = acquire(&cfg).expect_err("a takeover in progress must not be barged into");
        assert!(err.contains("is being taken over by pid 1"), "{err}");
        assert!(
            std::fs::read_to_string(journal.join(".lock"))
                .expect("lock")
                .contains("4294967294"),
            "and the stale lock is left for the process that claimed it"
        );
    }

    /// A claimant that died mid-takeover must not wedge the directory forever.
    #[test]
    fn a_claim_from_a_dead_process_is_cleared() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = HistoryConfig::for_root(dir.path());
        let journal = journal_dir(&cfg);
        std::fs::write(
            journal.join(".lock"),
            "{\"pid\":4294967294,\"role\":\"driver\",\"port\":1}",
        )
        .expect("write");
        std::fs::write(journal.join(".lock.claim"), "{\"pid\":4294967293}").expect("write");

        let lock = acquire(&cfg).expect("a dead claimant does not wedge the directory");
        assert_eq!(read_holder(lock.path()).pid, std::process::id());
        assert!(
            !journal.join(".lock.claim").exists(),
            "and the claim is released"
        );
    }

    /// The lockfile is complete and durable the instant its name exists, it records the data dir
    /// this process booted with, and the temporary it was written through does not survive.
    #[test]
    fn the_published_lock_body_is_complete_and_leaves_no_temporary() {
        let dir = tempfile::tempdir().expect("tempdir");
        let history = tempfile::tempdir().expect("tempdir");
        let mut cfg = HistoryConfig::for_root_with_history_dir(dir.path(), history.path());
        cfg.role = "worker".to_string();
        cfg.port = 15003;
        let lock = acquire(&cfg).expect("acquire");

        let holder = read_holder(lock.path());
        assert_eq!(holder.pid, std::process::id());
        assert_eq!(holder.role, "worker");
        assert_eq!(holder.port, 15003);
        assert_eq!(holder.root, dir.path().display().to_string());

        let leftovers: Vec<String> = std::fs::read_dir(&cfg.statements_dir)
            .expect("read dir")
            .flatten()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.ends_with(".tmp"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "temporaries left behind: {leftovers:?}"
        );
    }

    /// A path with a quote in it must still produce a lockfile the next process can parse —
    /// `format!`-ing the body would publish a lock that reads as `pid: 0`.
    #[test]
    fn a_root_containing_json_punctuation_is_still_parseable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let awkward = dir.path().join("we\"ird\\root");
        let cfg = HistoryConfig::for_root(&awkward);
        let lock = acquire(&cfg).expect("acquire");
        let holder = read_holder(lock.path());
        assert_eq!(holder.pid, std::process::id());
        assert_eq!(holder.root, awkward.display().to_string());
    }

    /// A temporary a crashed boot abandoned is swept, so they cannot accumulate in the data dir.
    #[test]
    fn abandoned_temporaries_are_swept() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = HistoryConfig::for_root(dir.path());
        let orphan = journal_dir(&cfg).join(".lock.999999.0.tmp");
        std::fs::write(&orphan, "{\"pid\":999999}").expect("write");
        age(&orphan, BODY_GRACE * 2);

        let _lock = acquire(&cfg).expect("acquire");
        assert!(!orphan.exists(), "a stale temporary is removed");
    }

    #[test]
    fn the_same_process_may_re_enter() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = HistoryConfig::for_root(dir.path());
        let first = acquire(&cfg).expect("first");
        let second = acquire(&cfg).expect("same pid re-enters");
        drop(second);
        // The re-entrant guard must not have removed the file the first one owns.
        assert!(first.path().exists());
    }
}
