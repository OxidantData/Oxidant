//! One process per data dir (§3c).
//!
//! Two processes sharing a root would interleave `O_APPEND` writes — atomic only for small
//! writes, so a large `sql` line tears — and both would roll to the same next segment name.
//! `local-cluster` workers are in-process and fine; `oxidant worker --port` is a separate
//! process, and the Docker/EC2 topologies routinely start a driver and a worker from one
//! working directory.
//!
//! The lock is a pid-stamped `O_CREAT|O_EXCL` file with a boot-time staleness check — the
//! fallback §3c names, taken because the `flock` path would mean linking `libc`/`rustix`
//! directly for one syscall and this design adds no dependencies. Liveness is read through
//! `sysinfo`, which this crate already depends on for `/api/v1/cluster/status`.

use std::io::Read;
use std::path::{Path, PathBuf};

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

/// Take the exclusive lock on `cfg.root`, or explain exactly who holds it.
pub(crate) fn acquire(cfg: &HistoryConfig) -> Result<DataDirLock, String> {
    fs_util::create_dir_secure(&cfg.root).map_err(|e| {
        format!(
            "oxidant: cannot create data dir {}: {e}",
            cfg.root.display()
        )
    })?;
    let path = cfg.root.join(".lock");
    let body = format!(
        "{{\"pid\":{},\"role\":\"{}\",\"port\":{}}}\n",
        std::process::id(),
        cfg.role,
        cfg.port
    );
    for attempt in 0..2 {
        match fs_util::create_new_secure(&path) {
            Ok(mut file) => {
                use std::io::Write;
                let _ = file.write_all(body.as_bytes());
                let _ = file.sync_all();
                return Ok(DataDirLock { path, owned: true });
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                let holder = read_holder(&path);
                if holder.pid == std::process::id() {
                    // Same process, second server (the in-process `local-cluster` shape, and
                    // every test that boots two services): share the directory rather than
                    // refusing to start against ourselves.
                    return Ok(DataDirLock { path, owned: false });
                }
                if pid_is_alive(holder.pid) {
                    return Err(lock_error(&cfg.root, &holder));
                }
                if attempt == 0 {
                    // Stale: the holder is gone. Clear it and take the lock once.
                    let _ = std::fs::remove_file(&path);
                    continue;
                }
                return Err(lock_error(&cfg.root, &holder));
            }
            Err(e) => {
                return Err(format!(
                    "oxidant: cannot create lockfile {}: {e}",
                    path.display()
                ))
            }
        }
    }
    unreachable!("the loop returns on every path")
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
