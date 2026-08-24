//! One process per data dir (§3c).
//!
//! Two processes sharing a root would interleave `O_APPEND` writes — atomic only for small
//! writes, so a large `sql` line tears — and both would roll to the same next segment name.
//! `local-cluster` workers are in-process and fine; `oxidant worker --port` is a separate
//! process, and the Docker/EC2 topologies routinely start a driver and a worker from one
//! working directory.
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

/// Holds the data dir for this process; releases it on drop.
#[derive(Debug)]
pub(crate) struct DataDirLock {
    path: PathBuf,
    /// A re-entrant acquisition (same pid, e.g. a second server in one test process) does not
    /// own the file and must not remove it.
    owned: bool,
}

impl Drop for DataDirLock {
    fn drop(&mut self) {
        if self.owned {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

impl DataDirLock {
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

/// Take the exclusive lock on `cfg.root`, or explain exactly who holds it.
pub(crate) fn acquire(cfg: &HistoryConfig) -> Result<DataDirLock, String> {
    fs_util::create_dir_secure(&cfg.root).map_err(|e| {
        format!(
            "oxidant: cannot create data dir {}: {e}",
            cfg.root.display()
        )
    })?;
    sweep_abandoned_temporaries(&cfg.root);
    let path = cfg.root.join(".lock");
    let body = format!(
        "{{\"pid\":{},\"role\":\"{}\",\"port\":{}}}\n",
        std::process::id(),
        cfg.role,
        cfg.port
    );

    // The uncontended path, and the overwhelmingly common one.
    match publish(&path, &body, &cfg.root) {
        Ok(()) => return claim_verified(&cfg.root, path),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => {
            return Err(format!(
                "oxidant: cannot create lockfile {}: {e}",
                path.display()
            ))
        }
    }

    if let Some(shared) = re_entrant(&path, &cfg.root)? {
        return Ok(shared);
    }

    // The holder is gone. Taking over is exclusive: without the claim, two processes that read
    // the same dead holder both remove and both create, and the second removes the first's fresh
    // lock — two writers on one journal.
    let claim = Claim::take(&cfg.root)?;
    // Re-read under the claim: whoever else was deciding has finished by now.
    if let Some(shared) = re_entrant(&path, &cfg.root)? {
        return Ok(shared);
    }
    let _ = std::fs::remove_file(&path);
    fs_util::fsync_dir(&cfg.root);
    publish(&path, &body, &cfg.root).map_err(|e| {
        format!(
            "oxidant: cannot take over the stale lockfile {}: {e}",
            path.display()
        )
    })?;
    drop(claim);
    claim_verified(&cfg.root, path)
}

/// Is the existing lock one we may share or must refuse? `None` means it is stale and takeable.
///
/// `Ok(Some(_))` is the same-process case (a second server in one process — the in-process
/// `local-cluster` shape, and every test that boots two services): share the directory rather
/// than refusing to start against ourselves.
fn re_entrant(path: &Path, root: &Path) -> Result<Option<DataDirLock>, String> {
    let holder = read_holder(path);
    if holder.pid == std::process::id() {
        return Ok(Some(DataDirLock {
            path: path.to_path_buf(),
            owned: false,
        }));
    }
    if holder_is_held(&holder, path) {
        return Err(lock_error(root, &holder));
    }
    Ok(None)
}

/// Confirm the lock we just published is still ours before reporting that we own it.
///
/// A process that lost a takeover race would otherwise report success while another process's
/// record sits in the file — and would delete that process's lock on drop.
fn claim_verified(root: &Path, path: PathBuf) -> Result<DataDirLock, String> {
    let holder = read_holder(&path);
    if holder.pid == std::process::id() {
        return Ok(DataDirLock { path, owned: true });
    }
    Err(lock_error(root, &holder))
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
    fn take(root: &Path) -> Result<Self, String> {
        let path = root.join(".lock.claim");
        let body = format!("{{\"pid\":{}}}\n", std::process::id());
        for attempt in 0..2 {
            match publish(&path, &body, root) {
                Ok(()) => return Ok(Self { path }),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    let holder = read_holder(&path);
                    if attempt == 0 && !holder_is_held(&holder, &path) {
                        // The claimant died mid-takeover. Clear it and try once.
                        let _ = std::fs::remove_file(&path);
                        fs_util::fsync_dir(root);
                        continue;
                    }
                    return Err(format!(
                        "oxidant: $OXIDANT_DATA_DIR ({}) is being taken over by pid {}.\n         \
                         Another process is recovering this directory's lock; retry in a moment, \
                         or set OXIDANT_DATA_DIR to a distinct path for this process.",
                        root.display(),
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

/// The second-process error, verbatim from §3c — it names the holder and both ways out.
fn lock_error(root: &Path, holder: &Holder) -> String {
    format!(
        "oxidant: $OXIDANT_DATA_DIR ({}) is locked by pid {} (role={}, port={}).\n         \
         History and logs are per-process. Set OXIDANT_DATA_DIR to a distinct path for\n         \
         this process, or set OXIDANT_DATA_DIR_PER_PROCESS=1 to use {}/<role>-<port>/.",
        root.display(),
        holder.pid,
        holder.role,
        holder.port,
        root.display()
    )
}

#[derive(Debug)]
struct Holder {
    pid: u32,
    role: String,
    port: u16,
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

    #[test]
    fn a_live_holder_makes_the_second_acquisition_fail_loudly() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = HistoryConfig::for_root(dir.path());
        let _held = acquire(&cfg).expect("first acquisition");
        // Rewrite the lockfile as if another live process (this test binary's parent shell is
        // not portable; pid 1 always exists) holds it.
        std::fs::write(
            dir.path().join(".lock"),
            "{\"pid\":1,\"role\":\"driver\",\"port\":15002}",
        )
        .expect("write");
        let err = acquire(&cfg).expect_err("must refuse a live holder");
        assert!(err.contains("is locked by pid 1"), "{err}");
        assert!(err.contains("role=driver, port=15002"), "{err}");
        assert!(err.contains("OXIDANT_DATA_DIR_PER_PROCESS=1"), "{err}");
    }

    #[test]
    fn a_stale_lockfile_is_taken_over() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = HistoryConfig::for_root(dir.path());
        std::fs::create_dir_all(dir.path()).expect("mkdir");
        // A pid that cannot be running: the max pid value is not allocatable.
        std::fs::write(
            dir.path().join(".lock"),
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
        std::fs::create_dir_all(dir.path()).expect("mkdir");
        // Exactly what the old spelling left visible between `create_new` and `write_all`.
        std::fs::write(dir.path().join(".lock"), b"").expect("write");

        let err = acquire(&cfg).expect_err("an empty lock body must not read as a dead holder");
        assert!(err.contains("is locked by pid 0"), "{err}");
        assert!(
            dir.path().join(".lock").exists(),
            "and the other process's lock is still there"
        );
    }

    /// The grace is a grace, not a deadlock: a lock that has been unparseable for longer than
    /// any write could take is genuinely corrupt and is taken over.
    #[test]
    fn a_long_dead_unparseable_lock_is_still_taken_over() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = HistoryConfig::for_root(dir.path());
        std::fs::create_dir_all(dir.path()).expect("mkdir");
        let path = dir.path().join(".lock");
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
        std::fs::create_dir_all(dir.path()).expect("mkdir");
        // A stale lock, which on its own would be taken over...
        std::fs::write(
            dir.path().join(".lock"),
            "{\"pid\":4294967294,\"role\":\"driver\",\"port\":1}",
        )
        .expect("write");
        // ...but another live process is already taking it over. Pid 1 always exists.
        std::fs::write(dir.path().join(".lock.claim"), "{\"pid\":1}").expect("write");

        let err = acquire(&cfg).expect_err("a takeover in progress must not be barged into");
        assert!(err.contains("is being taken over by pid 1"), "{err}");
        assert!(
            std::fs::read_to_string(dir.path().join(".lock"))
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
        std::fs::create_dir_all(dir.path()).expect("mkdir");
        std::fs::write(
            dir.path().join(".lock"),
            "{\"pid\":4294967294,\"role\":\"driver\",\"port\":1}",
        )
        .expect("write");
        std::fs::write(dir.path().join(".lock.claim"), "{\"pid\":4294967293}").expect("write");

        let lock = acquire(&cfg).expect("a dead claimant does not wedge the directory");
        assert_eq!(read_holder(lock.path()).pid, std::process::id());
        assert!(
            !dir.path().join(".lock.claim").exists(),
            "and the claim is released"
        );
    }

    /// The lockfile is complete and durable the instant its name exists, and the temporary it was
    /// written through does not survive.
    #[test]
    fn the_published_lock_body_is_complete_and_leaves_no_temporary() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = HistoryConfig::for_root(dir.path());
        cfg.role = "worker".to_string();
        cfg.port = 15003;
        let lock = acquire(&cfg).expect("acquire");

        let holder = read_holder(lock.path());
        assert_eq!(holder.pid, std::process::id());
        assert_eq!(holder.role, "worker");
        assert_eq!(holder.port, 15003);

        let leftovers: Vec<String> = std::fs::read_dir(dir.path())
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

    /// A temporary a crashed boot abandoned is swept, so they cannot accumulate in the data dir.
    #[test]
    fn abandoned_temporaries_are_swept() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = HistoryConfig::for_root(dir.path());
        std::fs::create_dir_all(dir.path()).expect("mkdir");
        let orphan = dir.path().join(".lock.999999.0.tmp");
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
