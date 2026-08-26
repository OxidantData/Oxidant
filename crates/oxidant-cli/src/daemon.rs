//! `oxidant start | stop | status | restart` — daemon control for the long-running roles.
//!
//! The rule this module makes real: **every long-running oxidant process is a daemon.** Running
//! `oxidant spark server` in a terminal was how nine orphaned engines accumulated on the
//! founder's machine (the incident [`crate::portguard`] was written for) — each one tied to a
//! shell that had long since closed, none of them findable without `lsof` archaeology. A daemon
//! has a pidfile, so "is it running?", "on what ports?" and "stop it" are all one command.
//!
//! Three things this has to get right, and each one is a way the naive version breaks:
//!
//! * **A recycled PID is never killed.** A pidfile alone says nothing: the process it names may
//!   have died a week ago and the number since been handed to something else. So the pidfile
//!   records an [`Identity`] — the executable and an opaque per-OS *start token* — and `stop`
//!   re-reads that identity from the live process and demands a match before it signals
//!   anything. No match, no signal, ever; the pidfile is reported and left alone.
//! * **A stale pidfile never blocks a start.** The other half of the same check. `SIGKILL` the
//!   daemon (or lose the machine) and the pidfile survives with a pid that is dead or has been
//!   reused; `start` sees liveness+identity fail, drops the file and proceeds. This is what makes
//!   `Restart=on-failure` and a plain reboot work.
//! * **A failed start leaks nothing.** The window between `spawn` and a written pidfile is the
//!   one in which a live, detached engine exists that nothing on disk names, so the child is
//!   owned by a [`ChildGuard`] that reaps it on every path out — and the whole check-then-spawn
//!   sequence runs under an exclusive `run/.lock`, so two concurrent `start`s cannot both
//!   decide nothing is running.
//! * **The guard never counts the starting process itself.** `start` spawns
//!   `oxidant spark server … --foreground`, so at the moment the child boots there are two
//!   oxidant processes in the tree by construction. Anything that scans for "another oxidant
//!   server" (see [`single_instance_conflict`]) walks past self and the whole parent chain.
//!
//! State lives under the engine's own data root — `oxidant_connect::data_root()`, i.e.
//! `$OXIDANT_DATA_DIR` — in `run/oxidant.pid` and `run/oxidant.log`, next to `history/` and
//! `logs/`. One daemon per root, which is also what lets the test suite run any number of them
//! by handing each its own `OXIDANT_DATA_DIR`.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use crate::portguard;

/// How long `stop` waits after `SIGTERM` before escalating to `SIGKILL`.
///
/// The daemon's own shutdown path flushes the log ring and the statement journal
/// (`logging::install_shutdown_flush`), so this is a budget for a real flush, not a formality.
pub const DEFAULT_STOP_GRACE: Duration = Duration::from_secs(15);

/// How long `stop` waits for `SIGKILL` to land before reporting the process unkillable.
///
/// SIGKILL is not instant: the kernel still has to unwind a process that may be mid-syscall,
/// and one stuck in uninterruptible I/O never unwinds at all. Without this the escalation path
/// would signal and immediately declare failure, having given the kernel no time.
const SIGKILL_GRACE: Duration = Duration::from_secs(5);

/// How long `start` waits for the child to answer its health endpoint before giving up and
/// reporting the tail of the log.
const START_TIMEOUT: Duration = Duration::from_secs(90);

/// How long `start` waits for another `start` to release `run/.lock` before giving up.
///
/// The lock is held across a whole start, readiness wait included, so the wait has to outlast
/// [`START_TIMEOUT`] or a concurrent `start` would report a lock timeout for a first start that
/// is merely slow. Past that it is a real failure — a crashed `start` cannot hold the lock
/// (the kernel drops `flock` with the fd), so what remains is a wedged one.
const START_LOCK_WAIT: Duration = Duration::from_secs(100);

/// Lines of `run/oxidant.log` shown when a start fails.
const LOG_TAIL_LINES: usize = 20;

/// `status` exit codes, chosen so `oxidant status` drops into a shell script.
///
/// Follows the LSB init convention where it has one (`3` = not running) and extends it for the
/// case init scripts have no word for: the process is up but not answering.
mod exit {
    /// Running and the health endpoint answered.
    pub const RUNNING: i32 = 0;
    /// Not running (no pidfile, or a stale one).
    pub const STOPPED: i32 = 3;
    /// The process is alive but its HTTP endpoint did not answer.
    pub const UNHEALTHY: i32 = 4;
}

// ---------------------------------------------------------------------------------------------
// Where daemon state lives
// ---------------------------------------------------------------------------------------------

/// `$OXIDANT_DATA_DIR/run` — the pidfile and the daemon's captured stdio.
pub fn run_dir() -> PathBuf {
    oxidant_connect::data_root().join("run")
}

/// `$OXIDANT_DATA_DIR/run/oxidant.pid`.
pub fn pid_path() -> PathBuf {
    run_dir().join("oxidant.pid")
}

/// `$OXIDANT_DATA_DIR/run/oxidant.log` — the daemon's stdout and stderr.
///
/// Not the same file as the engine's `logs/oxidant.log`: this one catches everything the
/// process writes to its file descriptors, including the boot banner, a config error printed
/// before any subscriber exists, and a panic. It is the only place those go once nobody is
/// attached to a terminal.
pub fn log_path() -> PathBuf {
    run_dir().join("oxidant.log")
}

// ---------------------------------------------------------------------------------------------
// The pidfile
// ---------------------------------------------------------------------------------------------

/// What `run/oxidant.pid` holds.
///
/// More than a number, because a number cannot answer "is this still the process I started?".
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PidFile {
    pub pid: u32,
    /// The executable `start` spawned, as an absolute path.
    pub exe: String,
    /// An opaque per-OS process start token — see [`Identity`]. Compared verbatim.
    pub start_token: String,
    /// Wall-clock start, RFC 3339. Display only; `start_token` is what verification uses.
    pub started_at: String,
    /// The Spark Connect port.
    pub port: u16,
    /// The UI/REST port, or `None` under `--no-ui`.
    pub ui_port: Option<u16>,
    /// `--ui-bind`, so `status` probes the address the UI actually listens on.
    pub ui_bind: String,
    /// The flags this daemon was started with, so `restart` can replay them exactly.
    pub args: Vec<String>,
    /// Where its stdio went.
    pub log: String,
}

impl PidFile {
    fn read(path: &std::path::Path) -> Option<Self> {
        serde_json::from_slice(&std::fs::read(path).ok()?).ok()
    }

    /// Write atomically: a half-written pidfile read by a concurrent `stop` is a pidfile that
    /// parses as garbage and is treated as "not running", which would strand a live daemon.
    fn write(&self, path: &std::path::Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("pid.tmp");
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(
            &serde_json::to_vec_pretty(self)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?,
        )?;
        f.write_all(b"\n")?;
        f.sync_all()?;
        drop(f);
        std::fs::rename(&tmp, path)
    }

    /// The base URL of this daemon's HTTP surface, or `None` under `--no-ui`.
    ///
    /// A daemon bound to `0.0.0.0` is probed on loopback: `0.0.0.0` is not a destination
    /// address, and the point of the probe is to reach *this host's* listener.
    fn http_base(&self) -> Option<String> {
        let ui = self.ui_port?;
        let host = match self.ui_bind.as_str() {
            "0.0.0.0" | "::" | "" => "127.0.0.1".to_string(),
            other if other.contains(':') => format!("[{other}]"),
            other => other.to_string(),
        };
        Some(format!("http://{host}:{ui}"))
    }

    /// The UI endpoint as an operator should read it, or `None` under `--no-ui`.
    ///
    /// Deliberately not [`PidFile::http_base`]. That one exists to *probe*, and rewrites a
    /// wildcard bind to loopback because `0.0.0.0` is not a destination address — correct for a
    /// health check and a lie in a terminal. `--ui-bind` defaults to `0.0.0.0`, so `start` and
    /// `status` printed `http://127.0.0.1:4451` for a UI listening on every interface, directly
    /// under an honest `sc://0.0.0.0:50451`. The misleading half is the security-relevant one:
    /// `docs/web-ui.md` warns the UI has no auth and should be bound to loopback on reachable
    /// hosts, and this line told the operator they already had.
    ///
    /// So: name the address that was actually bound, and carry the loopback URL alongside it
    /// rather than in place of it.
    fn ui_endpoint(&self) -> Option<String> {
        let ui = self.ui_port?;
        let bind = if self.ui_bind.is_empty() {
            "0.0.0.0"
        } else {
            self.ui_bind.as_str()
        };
        let host = if bind.contains(':') {
            format!("[{bind}]")
        } else {
            bind.to_string()
        };
        match bind {
            "0.0.0.0" | "::" => Some(format!(
                "http://{host}:{ui}  (all interfaces; local {})",
                self.http_base()?
            )),
            _ => Some(format!("http://{host}:{ui}")),
        }
    }

    /// Uptime derived from `started_at`, for display.
    fn uptime(&self) -> Option<Duration> {
        let started = chrono::DateTime::parse_from_rfc3339(&self.started_at).ok()?;
        (chrono::Utc::now() - started.with_timezone(&chrono::Utc))
            .to_std()
            .ok()
    }
}

// ---------------------------------------------------------------------------------------------
// Process identity — the anti-recycled-PID check
// ---------------------------------------------------------------------------------------------

/// Enough about a live process to answer "is this still the one we started?".
///
/// `start_token` is deliberately opaque and only ever compared for equality: on Linux it is
/// field 22 of `/proc/<pid>/stat` (start time in clock ticks since boot), elsewhere `ps -o
/// lstart=`. Either way a pid that has been recycled since we recorded it gets a different
/// token, because the new process started later.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub exe: String,
    pub start_token: String,
}

/// Read `pid`'s identity, or `None` when we cannot.
///
/// `None` is never treated as a match — see [`verify`].
pub fn identity(pid: u32) -> Option<Identity> {
    #[cfg(target_os = "linux")]
    if let Some(id) = proc_identity(pid) {
        return Some(id);
    }
    ps_identity(pid)
}

#[cfg(target_os = "linux")]
fn proc_identity(pid: u32) -> Option<Identity> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // Field 2 is the comm in parens and may itself contain spaces and parens, so the fields
    // resume after the *last* `)`. There they restart at 3, putting starttime (22) at index 19.
    let after_comm = &stat[stat.rfind(')')? + 1..];
    let start_token = after_comm.split_whitespace().nth(19)?.to_string();
    // `/proc/<pid>/exe` is the exact binary; when it is unreadable (a hardened kernel, or the
    // process belongs to another user) argv[0] is the honest fallback.
    let exe = std::fs::read_link(format!("/proc/{pid}/exe"))
        .ok()
        .map(|p| p.to_string_lossy().into_owned())
        .or_else(|| proc_argv0(pid))?;
    Some(Identity { exe, start_token })
}

#[cfg(target_os = "linux")]
fn proc_argv0(pid: u32) -> Option<String> {
    let raw = std::fs::read(format!("/proc/{pid}/cmdline")).ok()?;
    let argv0 = raw.split(|b| *b == 0).find(|a| !a.is_empty())?;
    Some(String::from_utf8_lossy(argv0).into_owned())
}

/// The portable path: `ps`, which macOS always has and every non-slim Linux image does too.
fn ps_identity(pid: u32) -> Option<Identity> {
    let start_token = run("ps", &["-p", &pid.to_string(), "-o", "lstart="])?
        .trim()
        .to_string();
    let args = run("ps", &["-p", &pid.to_string(), "-o", "args="])?;
    let exe = args.split_whitespace().next()?.to_string();
    if start_token.is_empty() || exe.is_empty() {
        return None;
    }
    Some(Identity { exe, start_token })
}

fn run(program: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(program).args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Is `pid` alive? `kill(pid, 0)` — the question without the signal.
///
/// `EPERM` counts as alive: the process exists, we simply may not signal it.
#[cfg(unix)]
pub fn alive(pid: u32) -> bool {
    let Some(pid) = real_pid(pid) else {
        return false;
    };
    // SAFETY: `kill` with signal 0 sends nothing; it only performs the existence and
    // permission check. `pid` is a plain integer and there is no memory involved.
    let rc = unsafe { libc::kill(pid, 0) };
    rc == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// A pid that names exactly one process, or `None`.
///
/// `kill(2)` overloads its first argument: `0` means *every process in the caller's own group*
/// and anything negative means a whole group. A pidfile holding `0` — from a truncated write, a
/// hand edit, or a JSON default — would otherwise pass the liveness check and then have `stop`
/// SIGKILL the entire group, this process included. So a pid outside `1..=pid_t::MAX` is not a
/// process we will look at, let alone signal.
#[cfg(unix)]
fn real_pid(pid: u32) -> Option<libc::pid_t> {
    (pid >= 1 && pid <= libc::pid_t::MAX as u32).then_some(pid as libc::pid_t)
}

#[cfg(not(unix))]
pub fn alive(_pid: u32) -> bool {
    false
}

/// Does the live process `pid` match what the pidfile recorded?
///
/// Three independent conditions, and the answer is `false` unless **all** hold:
///
/// 1. the pid is alive;
/// 2. its start token equals the recorded one — this is what a recycled pid fails;
/// 3. its executable is the recorded one.
///
/// Unreadable identity is a `false`, not a shrug. `stop` turns this into a signal, so the
/// failure mode of guessing is killing a stranger; the failure mode of refusing is a printed
/// `kill` command for the operator to run. Only one of those is recoverable.
///
/// The executable is compared by basename as well as in full, because the recorded path and the
/// live one legitimately differ in form: Linux appends ` (deleted)` to `/proc/<pid>/exe` after
/// an in-place upgrade, and `ps -o args=` reports argv[0] as the caller wrote it. Basename
/// equality plus an exact start-token match is already far past coincidence.
pub fn verify(recorded: &PidFile, live: Option<&Identity>) -> bool {
    if !alive(recorded.pid) {
        return false;
    }
    let Some(live) = live else {
        return false;
    };
    if live.start_token != recorded.start_token {
        return false;
    }
    exe_matches(&recorded.exe, &live.exe)
}

fn exe_matches(recorded: &str, live: &str) -> bool {
    let strip = |s: &str| {
        portguard::basename(s.trim().trim_end_matches(" (deleted)"))
            .trim_end_matches(".exe")
            .to_string()
    };
    recorded == live || (!recorded.is_empty() && strip(recorded) == strip(live))
}

/// What this data root's pidfile currently means. Three states, not two.
pub enum Daemon {
    /// A live process whose identity matches the pidfile.
    Running(Box<PidFile>),
    /// No pidfile, or one describing a process that is gone (which is also deleted here).
    Stopped,
    /// The pid is alive but is *not* us. Never a "stopped" and never a "running".
    Stranger(Box<PidFile>, Option<Identity>),
}

/// Read the pidfile and decide which of the three states it describes.
///
/// [`Daemon::Stopped`] covers both "no pidfile" and "a pidfile that no longer describes a live
/// process", and the latter also deletes the file — that is the whole stale-pidfile story, and
/// the reason a `SIGKILL`ed daemon (or a reboot) never blocks the next `start`.
pub fn running() -> Daemon {
    let path = pid_path();
    let Some(recorded) = PidFile::read(&path) else {
        return Daemon::Stopped;
    };
    let live = identity(recorded.pid);
    if verify(&recorded, live.as_ref()) {
        return Daemon::Running(Box::new(recorded));
    }
    if alive(recorded.pid) {
        // Alive but a stranger. Never silently reuse or clear this — a hand-written pidfile
        // pointing at someone else's process is exactly the case where deleting it and moving
        // on would look like success right up until `stop` killed the wrong thing.
        return Daemon::Stranger(Box::new(recorded), live);
    }
    // Dead pid: the pidfile is a leftover from a SIGKILL, an OOM kill or a reboot. Drop it.
    let _ = std::fs::remove_file(&path);
    Daemon::Stopped
}

// ---------------------------------------------------------------------------------------------
// Signals
// ---------------------------------------------------------------------------------------------

#[cfg(unix)]
fn signal(pid: u32, sig: i32) -> std::io::Result<()> {
    // The same guard as `alive`, restated where the consequence is fatal rather than merely
    // wrong: `kill(0, SIGKILL)` takes down the caller's whole process group.
    let pid = real_pid(pid).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{pid} is not a process id"),
        )
    })?;
    // SAFETY: `kill` takes two integers and touches no memory owned by this process. The pid
    // has been verified as our own daemon by `verify` before any caller reaches here.
    if unsafe { libc::kill(pid, sig) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(unix))]
fn signal(_pid: u32, _sig: i32) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "daemon control needs unix signals; run with --foreground under a supervisor instead",
    ))
}

#[cfg(unix)]
const SIGTERM: i32 = libc::SIGTERM;
#[cfg(unix)]
const SIGKILL: i32 = libc::SIGKILL;
#[cfg(not(unix))]
const SIGTERM: i32 = 15;
#[cfg(not(unix))]
const SIGKILL: i32 = 9;

/// Put the spawned child in a session of its own.
///
/// `setsid` and not `Command::process_group`: setpgid leaves the child in the shell's *session*,
/// so closing the terminal that ran `oxidant start` still delivers SIGHUP to it. A daemon that
/// dies when you close the laptop lid is not a daemon.
#[cfg(unix)]
fn detach(cmd: &mut Command) {
    use std::os::unix::process::CommandExt;
    // SAFETY: `pre_exec` runs between fork and exec, where only async-signal-safe calls are
    // allowed. `setsid` is on that list, allocates nothing, and is the only call made here.
    unsafe {
        cmd.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(unix))]
fn detach(_cmd: &mut Command) {}

// ---------------------------------------------------------------------------------------------
// The bare-invocation refusal
// ---------------------------------------------------------------------------------------------

/// The roles that may not run attached to a terminal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// `oxidant spark server`.
    Server,
    /// `oxidant worker`.
    Worker,
}

impl Role {
    fn invocation(self) -> &'static str {
        match self {
            Role::Server => "oxidant spark server",
            Role::Worker => "oxidant worker",
        }
    }
}

/// Was `--foreground` passed?
pub fn foreground(args: &[String]) -> bool {
    args.iter().any(|a| a == "--foreground")
}

/// Refuse a bare long-running invocation, or return so the caller can serve.
///
/// Short on purpose. The reader of this message is either a human who typed the old command out
/// of habit, or a supervisor unit that someone forgot to migrate; both need one line telling
/// them which of the two doors to take, not an essay.
pub fn require_foreground(args: &[String], role: Role) {
    if foreground(args) {
        return;
    }
    eprint!("{}", foreground_refusal(role));
    std::process::exit(1);
}

/// The refusal text. Pure so its exact shape is pinned by a unit test.
fn foreground_refusal(role: Role) -> String {
    let start_hint = match role {
        Role::Server => "  run it as a daemon:  oxidant start\n",
        // There is no `oxidant start worker`: a worker is started by its supervisor (systemd on
        // the AMI, the Flight worker unit in the cluster), never by a human at a prompt. Saying
        // "use oxidant start" here would be a lie.
        Role::Worker => "  workers are started by their supervisor, which passes --foreground\n",
    };
    format!(
        "error: `{}` runs a long-lived process, and those run as daemons\n{start_hint}  or supervise it yourself:  {} … --foreground\n",
        role.invocation(),
        role.invocation()
    )
}

// ---------------------------------------------------------------------------------------------
// Single-instance enforcement (release builds only)
// ---------------------------------------------------------------------------------------------

/// One row of the machine's process table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcRow {
    pub pid: u32,
    pub ppid: u32,
    pub command: String,
}

/// Every process on the machine we can see, or an empty list when we cannot tell.
fn process_table() -> Vec<ProcRow> {
    match run("ps", &["-Ao", "pid=,ppid=,args="]) {
        Some(out) => parse_ps_table(&out),
        None => proc_table(),
    }
}

fn parse_ps_table(stdout: &str) -> Vec<ProcRow> {
    stdout
        .lines()
        .filter_map(|line| {
            // `ps` right-aligns the pid columns, so the separators are runs of spaces, not
            // single ones — `splitn(3, char::is_whitespace)` would hand back an empty second
            // field and drop every row on a machine that pads. Hence `split_whitespace`, which
            // also normalizes the runs inside the command, exactly as the `/proc` path does.
            let mut it = line.split_whitespace();
            let pid = it.next()?.parse().ok()?;
            let ppid = it.next()?.parse().ok()?;
            let command = it.collect::<Vec<_>>().join(" ");
            (!command.is_empty()).then_some(ProcRow { pid, ppid, command })
        })
        .collect()
}

/// `/proc` fallback for slim images with no `ps`.
#[cfg(target_os = "linux")]
fn proc_table() -> Vec<ProcRow> {
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|e| {
            let pid: u32 = e.file_name().to_str()?.parse().ok()?;
            let stat = std::fs::read_to_string(e.path().join("stat")).ok()?;
            // Fields resume after the last `)` at 3 (state), so ppid (4) is index 1 here.
            let ppid = stat[stat.rfind(')')? + 1..]
                .split_whitespace()
                .nth(1)?
                .parse()
                .ok()?;
            let raw = std::fs::read(e.path().join("cmdline")).ok()?;
            let command = raw
                .split(|b| *b == 0)
                .filter(|a| !a.is_empty())
                .map(String::from_utf8_lossy)
                .collect::<Vec<_>>()
                .join(" ");
            (!command.is_empty()).then_some(ProcRow { pid, ppid, command })
        })
        .collect()
}

#[cfg(not(target_os = "linux"))]
fn proc_table() -> Vec<ProcRow> {
    Vec::new()
}

/// Is this command line an oxidant Spark Connect server?
///
/// Mirrors `async_main`'s dispatch exactly, and it has to: that function matches a *named*
/// subcommand first and only then falls back to "any argument is `server`". Testing for the
/// word alone would read `oxidant sql -e "SELECT * FROM server_events"` as a running engine and
/// refuse to start the real one.
///
/// argv[0] must also really be the oxidant binary — [`portguard::is_oxidant_command`] exists
/// because every test binary under `target/debug/deps/` has "oxidant" somewhere in its path.
fn is_server_role(command: &str) -> bool {
    if !portguard::is_oxidant_command(command) {
        return false;
    }
    let toks: Vec<&str> = command.split_whitespace().collect();
    match toks.get(1).copied() {
        // Every subcommand `async_main` dispatches by name before the `server` fallback.
        Some(
            "worker" | "driver" | "history-server" | "pipeline" | "sql" | "mcp" | "start" | "stop"
            | "status" | "restart",
        ) => false,
        _ => toks.iter().skip(1).any(|t| *t == "server"),
    }
}

/// Another server-role oxidant on this machine, if there is one.
///
/// Pure, and takes the process table as an argument, so the rule is unit-testable in a debug
/// build even though [`enforce_single_instance`] only calls it in release ones.
///
/// `me` and everything above it in the parent chain are excluded. That is not defensive
/// tidiness: `oxidant start` spawns `oxidant spark server --foreground`, so the child *always*
/// has an oxidant ancestor, and `restart` can add another. A guard that counted them would
/// refuse every single start.
pub fn single_instance_conflict(table: &[ProcRow], me: u32) -> Option<&ProcRow> {
    let mut excluded = std::collections::BTreeSet::from([me]);
    // Walk up the parent chain, bounded by the table size so a cycle in a synthetic table (or a
    // pid reused as its own ancestor between two `ps` reads) cannot spin forever.
    let mut cursor = me;
    for _ in 0..table.len() {
        let Some(row) = table.iter().find(|r| r.pid == cursor) else {
            break;
        };
        if row.ppid == 0 || !excluded.insert(row.ppid) {
            break;
        }
        cursor = row.ppid;
    }
    table
        .iter()
        .find(|r| !excluded.contains(&r.pid) && is_server_role(&r.command))
}

/// Refuse to be the second Spark Connect server on this machine.
///
/// **Release builds only.** Debug builds must multiply freely: this repo's own test suite runs
/// half a dozen servers at once on ephemeral ports (`cli_port_guard`, `cli_sql`,
/// `cli_rest_statements`), and `--mode local-cluster` is a supported topology. The rule is a
/// production-deployment guarantee, not an invariant of the binary — so the *decision* lives in
/// [`single_instance_conflict`], which is unit-tested in debug, and only the call site is gated.
pub fn enforce_single_instance() {
    // `if cfg!` and not `#[cfg]`: the gate is on the *behaviour*, so the code below still
    // compiles and is still borrow-checked in a debug build. A `#[cfg]` block would let this
    // path rot unnoticed until someone cut a release.
    if cfg!(debug_assertions) {
        return;
    }
    let table = process_table();
    if let Some(other) = single_instance_conflict(&table, std::process::id()) {
        eprintln!("error: another oxidant server is already running on this machine");
        eprintln!(
            "  pid:      {} ({})",
            other.pid,
            portguard::elide(&other.command, 96)
        );
        eprintln!("  status:   oxidant status");
        eprintln!("  stop it:  oxidant stop   (or `kill {}`)", other.pid);
        std::process::exit(1);
    }
}

// ---------------------------------------------------------------------------------------------
// start
// ---------------------------------------------------------------------------------------------

/// The spawned server, owned from `spawn` until the pidfile is authoritative.
///
/// Between those two moments the child is a live, `setsid`'d, un-pidfiled Spark Connect server —
/// exactly the state this module exists to abolish. Every early return in that window used to
/// leak it: a `?` on the pidfile write returned before the one `child.kill()` in the function,
/// leaving a full engine holding its ports with `status` reporting "not running" and `stop`
/// reporting "nothing to stop". So the child is owned by a guard that reaps it on drop, and only
/// a recorded, ready daemon disarms that guard.
struct ChildGuard {
    child: std::process::Child,
    /// `false` once the child is either the pidfile's daemon or already reaped.
    armed: bool,
}

impl ChildGuard {
    fn new(child: std::process::Child) -> Self {
        ChildGuard { child, armed: true }
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    /// This child is now the daemon the pidfile names. Stop owning it.
    fn disarm(&mut self) {
        self.armed = false;
    }

    /// Kill the child and reap it, so no zombie is left behind either.
    ///
    /// Idempotent, and a no-op once the child has already exited on its own — which is the
    /// common case, since the usual reason a start fails is that the server died at boot.
    fn reap(&mut self) {
        self.armed = false;
        if let Ok(Some(_)) = self.child.try_wait() {
            return;
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    fn child_mut(&mut self) -> &mut std::process::Child {
        &mut self.child
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.armed {
            self.reap();
        }
    }
}

/// Remove the pidfile only if it still names `pid`.
///
/// The same identity discipline `stop` applies before it *signals*, applied before it *unlinks*.
/// A blind `remove_file` on a failed start deletes whatever is on disk — which, after a racing
/// `start` won the file, is another live daemon's pidfile, orphaning it permanently.
fn remove_pidfile_if_ours(path: &std::path::Path, pid: u32) {
    if PidFile::read(path).is_some_and(|p| p.pid == pid) {
        let _ = std::fs::remove_file(path);
    }
}

/// An exclusive `flock` on `$OXIDANT_DATA_DIR/run/.lock`, held for the whole of `start`.
///
/// `running()` → port guard → `spawn` → `write` is a check-then-act sequence, and two `start`s
/// on distinct ports against one data root both used to observe `Stopped` and both proceed: two
/// engines, one pidfile, one of them untrackable forever after. The engine already takes
/// exclusive locks on its own state (`docs/runtime-contract.md`); this is the same discipline
/// for the *daemon bookkeeping*, and it is what makes `start`'s idempotence promise true under
/// concurrency rather than only in a quiet shell.
///
/// The lock lives on the fd, so it is released by the kernel however this process ends — a
/// `SIGKILL`ed `start` cannot wedge the next one.
struct StartLock {
    /// Held only for its file descriptor: dropping the file releases the lock.
    _file: std::fs::File,
}

#[cfg(unix)]
async fn lock_start() -> oxidant_common::Result<StartLock> {
    use std::os::unix::io::AsRawFd;
    let dir = run_dir();
    std::fs::create_dir_all(&dir)
        .map_err(|e| oxidant_common::Error::Io(format!("create {}: {e}", dir.display())))?;
    let path = dir.join(".lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|e| oxidant_common::Error::Io(format!("open {}: {e}", path.display())))?;
    let deadline = Instant::now() + START_LOCK_WAIT;
    loop {
        // SAFETY: `flock` takes a file descriptor this process owns for the lifetime of `file`
        // and an integer of flags; it touches no memory of ours.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            return Ok(StartLock { _file: file });
        }
        let e = std::io::Error::last_os_error();
        if e.raw_os_error() != Some(libc::EWOULDBLOCK) {
            return Err(oxidant_common::Error::Io(format!(
                "lock {}: {e}",
                path.display()
            )));
        }
        if Instant::now() >= deadline {
            return Err(oxidant_common::Error::Io(format!(
                "another `oxidant start` has held {} for {}s; it is stuck — check `oxidant status`",
                path.display(),
                START_LOCK_WAIT.as_secs()
            )));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Without `flock` there is nothing to serialize on, and `start` is unix-only anyway
/// (`signal` returns `Unsupported` off unix).
#[cfg(not(unix))]
async fn lock_start() -> oxidant_common::Result<StartLock> {
    let path = run_dir().join(".lock");
    std::fs::create_dir_all(run_dir())
        .map_err(|e| oxidant_common::Error::Io(format!("create {}: {e}", run_dir().display())))?;
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|e| oxidant_common::Error::Io(format!("open {}: {e}", path.display())))?;
    Ok(StartLock { _file: file })
}

/// Everything `start` needs to know about the server it is about to spawn.
pub struct ServerPorts {
    pub port: u16,
    pub ui_port: Option<u16>,
    pub ui_bind: std::net::IpAddr,
}

/// `oxidant start [spark server flags]`.
///
/// `flags` are the user's arguments with `start` itself removed; they are replayed verbatim to
/// `oxidant spark server … --foreground` and recorded in the pidfile so `restart` can repeat
/// them.
pub async fn start(flags: &[String], ports: ServerPorts) -> oxidant_common::Result<()> {
    // Everything from here to the pidfile write is one critical section. Held first, before the
    // state is even read: the race is between two `start`s *deciding* nothing is running.
    let _lock = lock_start().await?;
    match running() {
        // Idempotent by contract: a second `start` reports the first and exits 0. Anything else
        // (a refusal, a second engine) turns `oxidant start` in a provisioning script from a
        // convergent operation into a coin flip.
        Daemon::Running(existing) => {
            println!(
                "oxidant is already running (pid {}, since {})",
                existing.pid, existing.started_at
            );
            print_endpoints(&existing);
            return Ok(());
        }
        Daemon::Stranger(recorded, live) => refuse_stranger(&recorded, live.as_ref()),
        Daemon::Stopped => {}
    }

    // Before spawning anything: if a port is taken, the rich who-owns-it report belongs here,
    // in the terminal the operator is looking at. The child would print the same block — into
    // `run/oxidant.log`, where nobody would think to look.
    portguard::ensure_available(
        std::net::SocketAddr::from((std::net::IpAddr::from([0, 0, 0, 0]), ports.port)),
        portguard::PortKind::SparkConnect,
    );
    if let Some(ui) = ports.ui_port {
        portguard::ensure_available(
            std::net::SocketAddr::from((ports.ui_bind, ui)),
            portguard::PortKind::Ui,
        );
    }

    let log = log_path();
    std::fs::create_dir_all(run_dir())
        .map_err(|e| oxidant_common::Error::Io(format!("create {}: {e}", run_dir().display())))?;
    // Append, never truncate: the log of the run that failed to start is the thing an operator
    // reads *after* the next start attempt, and a fresh file would have thrown it away.
    let out = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log)
        .map_err(|e| oxidant_common::Error::Io(format!("open {}: {e}", log.display())))?;
    let err = out
        .try_clone()
        .map_err(|e| oxidant_common::Error::Io(format!("dup {}: {e}", log.display())))?;

    let exe = std::env::current_exe()
        .map_err(|e| oxidant_common::Error::Io(format!("resolve the oxidant binary: {e}")))?;
    let mut cmd = Command::new(&exe);
    cmd.arg("spark")
        .arg("server")
        .args(flags)
        .arg("--foreground")
        .stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err));
    detach(&mut cmd);
    // Owned from here on. Every early return below reaps it — see [`ChildGuard`].
    let mut child = ChildGuard::new(
        cmd.spawn()
            .map_err(|e| oxidant_common::Error::Io(format!("spawn {}: {e}", exe.display())))?,
    );
    let pid = child.pid();

    // Record before waiting for health, not after. A daemon that is still booting is already a
    // process someone may need to stop, and a `start` interrupted at second 40 of a 90-second
    // boot must not leave an unreachable orphan.
    let recorded = PidFile {
        pid,
        exe: exe.to_string_lossy().into_owned(),
        start_token: identity(pid).map(|i| i.start_token).unwrap_or_default(),
        started_at: chrono::Utc::now().to_rfc3339(),
        port: ports.port,
        ui_port: ports.ui_port,
        ui_bind: ports.ui_bind.to_string(),
        args: flags.to_vec(),
        log: log.to_string_lossy().into_owned(),
    };
    let path = pid_path();
    recorded
        .write(&path)
        .map_err(|e| oxidant_common::Error::Io(format!("write {}: {e}", path.display())))?;

    match await_ready(child.child_mut(), &recorded).await {
        Ok(()) => {
            // Recorded, ready, and now the pidfile's to manage: not ours to kill.
            child.disarm();
            println!("oxidant started (pid {pid})");
            print_endpoints(&recorded);
            Ok(())
        }
        Err(why) => {
            // The child is gone or wedged and the pidfile would outlive it as a lie. Reap it
            // first so nothing survives this function, clear the file only if it is still ours,
            // and hand over the tail of the log — the message that explains the failure was
            // written there, not here.
            child.reap();
            remove_pidfile_if_ours(&path, pid);
            Err(oxidant_common::Error::Io(format!(
                "{why}\n  log: {}\n{}",
                log.display(),
                tail(&log, LOG_TAIL_LINES)
            )))
        }
    }
}

/// Poll until the daemon answers, it exits, or [`START_TIMEOUT`] passes.
async fn await_ready(child: &mut std::process::Child, recorded: &PidFile) -> Result<(), String> {
    let deadline = Instant::now() + START_TIMEOUT;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|e| format!("build the health client: {e}"))?;
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            return Err(format!("oxidant exited during startup with {status}"));
        }
        // Under `--no-ui` there is no HTTP surface at all, so the strongest available claim is
        // that the gRPC listener accepts a connection. Without this branch a `--no-ui` start
        // would poll an endpoint that can never answer and fail after the full timeout.
        let ready = match recorded.http_base() {
            Some(_) => health(&client, recorded).await.is_some(),
            None => grpc_reachable(recorded),
        };
        if ready {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "oxidant did not become ready within {}s",
                START_TIMEOUT.as_secs()
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// One health probe. `Some` is a healthy daemon, and carries the body for `status` to summarize.
///
/// `/api/v1/cluster/status` is the endpoint and not `/api/status`: the latter is gated behind
/// `OXIDANT_STATUS_TOKEN`, and a health probe that needs a secret is a health probe that reports
/// "down" on every machine that has not configured one.
async fn health(client: &reqwest::Client, recorded: &PidFile) -> Option<serde_json::Value> {
    let base = recorded.http_base()?;
    let resp = client
        .get(format!("{base}/api/v1/cluster/status"))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    resp.json::<serde_json::Value>().await.ok()
}

/// Under `--no-ui` there is no HTTP surface at all, so liveness is the strongest claim
/// available: the gRPC port accepts a connection.
fn grpc_reachable(recorded: &PidFile) -> bool {
    std::net::TcpStream::connect_timeout(
        &std::net::SocketAddr::from(([127, 0, 0, 1], recorded.port)),
        Duration::from_secs(2),
    )
    .is_ok()
}

fn print_endpoints(p: &PidFile) {
    println!("  spark connect:  sc://0.0.0.0:{}", p.port);
    match p.ui_endpoint() {
        Some(base) => println!("  ui + rest:      {base}"),
        None => println!("  ui + rest:      disabled (--no-ui)"),
    }
    println!("  log:            {}", p.log);
    println!("  pidfile:        {}", pid_path().display());
}

/// The last `n` lines of a file, indented, or an empty string when there are none.
fn tail(path: &std::path::Path, n: usize) -> String {
    let Ok(text) = std::fs::read_to_string(path) else {
        return String::new();
    };
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(n)..]
        .iter()
        .map(|l| format!("  | {l}\n"))
        .collect()
}

// ---------------------------------------------------------------------------------------------
// stop
// ---------------------------------------------------------------------------------------------

/// `oxidant stop`.
///
/// Exits 0 when there is nothing to stop. "Not running" is the state the caller asked for, and
/// a `stop` in a teardown script that fails because the thing was already down is a script that
/// needs `|| true` bolted on.
pub fn stop(grace: Duration) -> oxidant_common::Result<()> {
    let path = pid_path();
    match running() {
        Daemon::Stopped => {
            println!("oxidant is not running");
            Ok(())
        }
        Daemon::Stranger(recorded, live) => refuse_stranger(&recorded, live.as_ref()),
        Daemon::Running(recorded) => {
            let pid = recorded.pid;
            signal(pid, SIGTERM).map_err(|e| {
                oxidant_common::Error::Io(format!("signal oxidant (pid {pid}): {e}"))
            })?;
            let escalated = if wait_for_exit(&recorded, grace) {
                false
            } else {
                signal(pid, SIGKILL).map_err(|e| {
                    oxidant_common::Error::Io(format!("SIGKILL oxidant (pid {pid}): {e}"))
                })?;
                if !wait_for_exit(&recorded, SIGKILL_GRACE) {
                    return Err(oxidant_common::Error::Io(format!(
                        "oxidant (pid {pid}) survived SIGKILL; it is probably stuck in \
                         uninterruptible I/O — the pidfile at {} is left in place",
                        path.display()
                    )));
                }
                true
            };
            let _ = std::fs::remove_file(&path);
            if escalated {
                println!(
                    "oxidant stopped (pid {pid}, SIGKILL after {}s)",
                    grace.as_secs()
                );
            } else {
                println!("oxidant stopped (pid {pid})");
            }
            Ok(())
        }
    }
}

/// Poll until the recorded daemon is gone, or `within` elapses. `true` if it went.
///
/// The loop spins on liveness, which is a single `kill(pid, 0)`; the full identity check runs
/// once, at the deadline. That ordering is deliberate on both counts. Identity is what decides
/// whether the *next* signal is safe — if the daemon exited during the wait and the kernel
/// handed its number straight to something else, the pid is alive but there is nothing left to
/// kill, and this returns `true` so the caller escalates to nothing. And it is expensive:
/// reading identity on macOS is two `ps` invocations, so checking it every 100ms would spawn
/// three hundred processes across a 15-second grace to answer a question that only matters at
/// the end of it.
///
/// A hair of TOCTOU survives between that check and the caller's `kill`, as it must without
/// `pidfd_send_signal`. The window is microseconds against a pid space the kernel walks
/// sequentially; the check that matters — the one before the *first* signal — happens in
/// `running()` before anything is sent at all.
fn wait_for_exit(recorded: &PidFile, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    loop {
        if !alive(recorded.pid) {
            return true;
        }
        if Instant::now() >= deadline {
            return !verify(recorded, identity(recorded.pid).as_ref());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The report when the pidfile names a live process that is not our daemon.
///
/// The pidfile is left on disk. Deleting it would be the tidy-looking move and the wrong one:
/// the file is evidence that something wrote a pid we did not, and an operator who reads this
/// needs to see it to decide whether the daemon really is gone.
///
/// Pure so its exact shape is unit-testable; [`refuse_stranger`] is what prints it. Like
/// [`portguard::ensure_available`] it bypasses `main`'s error path, which would prefix the
/// first line with `oxidant: io error:` and leave the other three dangling.
fn stranger_report(recorded: &PidFile, live: Option<&Identity>) -> String {
    let actual = match live {
        Some(id) => id.exe.clone(),
        None => "unreadable — the process belongs to another user".to_string(),
    };
    let path = pid_path();
    format!(
        "error: pid {} is alive but is not this oxidant daemon — refusing to signal it\n  \
         pidfile:  {} (recorded {})\n  \
         actual:   {actual}\n\
         a recycled pid is never killed. If the daemon really is gone, remove the pidfile: rm {}\n",
        recorded.pid,
        path.display(),
        recorded.exe,
        path.display(),
    )
}

fn refuse_stranger(recorded: &PidFile, live: Option<&Identity>) -> ! {
    eprint!("{}", stranger_report(recorded, live));
    std::process::exit(1)
}

// ---------------------------------------------------------------------------------------------
// status
// ---------------------------------------------------------------------------------------------

/// `oxidant status`. Never returns — the exit code *is* the answer (see [`exit`]).
pub async fn status() -> ! {
    let recorded = match running() {
        Daemon::Running(p) => p,
        Daemon::Stopped => {
            println!("oxidant is not running");
            std::process::exit(exit::STOPPED);
        }
        // A pidfile naming a stranger is neither "running" nor "stopped", and a script must not
        // read it as either. `refuse_stranger` exits 1 with the report.
        Daemon::Stranger(recorded, live) => refuse_stranger(&recorded, live.as_ref()),
    };

    println!("oxidant is running");
    println!("  pid:            {}", recorded.pid);
    if let Some(up) = recorded.uptime() {
        println!("  uptime:         {}", portguard::humanize(up));
    }
    println!("  spark connect:  sc://0.0.0.0:{}", recorded.port);
    match recorded.ui_endpoint() {
        Some(base) => println!("  ui + rest:      {base}"),
        None => println!("  ui + rest:      disabled (--no-ui)"),
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build health client");
    let healthy = match recorded.http_base() {
        Some(_) => match health(&client, &recorded).await {
            Some(body) => {
                let mode = body["mode"].as_str().unwrap_or("unknown");
                let version = body["version"].as_str().unwrap_or("unknown");
                println!("  health:         ok ({mode}, version {version})");
                true
            }
            None => {
                println!("  health:         UNREACHABLE (process alive, HTTP not answering)");
                false
            }
        },
        // No HTTP surface to probe; say exactly what was checked instead of claiming "ok".
        None => {
            let ok = grpc_reachable(&recorded);
            let verdict = if ok { "ok" } else { "UNREACHABLE" };
            println!("  health:         {verdict} (grpc connect; no ui to probe)");
            ok
        }
    };
    println!("  log:            {}", recorded.log);
    println!("  pidfile:        {}", pid_path().display());
    if !recorded.args.is_empty() {
        println!("  flags:          {}", recorded.args.join(" "));
    }
    std::process::exit(if healthy {
        exit::RUNNING
    } else {
        exit::UNHEALTHY
    });
}

/// The pidfile exactly as it sits on disk, whatever state it describes.
///
/// [`running()`] is the wrong reader for `restart`. It *deletes* a pidfile whose process is gone
/// — the stale-pidfile path every other caller wants — so on a crashed node the flags an
/// operator most needs replayed are thrown away by the read that was supposed to recover them.
/// `restart` reads the file first and lets `stop` do the deleting.
pub fn recorded_pidfile() -> Option<PidFile> {
    PidFile::read(&pid_path())
}

/// The flags a `restart` should replay: the recorded ones, with anything typed laid over the top.
///
/// "Same flags preserved" cuts both ways — a bare `oxidant restart` must come back on the same
/// ports it went down on, and `oxidant restart --ui-port 4050` must be able to move the UI
/// *without* dropping the `--port` the daemon has been serving on for a month.
///
/// Wholesale replacement got the second half wrong in the most dangerous direction: any typed
/// argument at all silently reverted every flag it did not mention to its default, so
/// `oxidant restart --ui-port 4050` moved a production server from 50452 to 50051 — and it did
/// not even take a server flag to trigger, because `restart` passes its own `--timeout` through
/// this function too.
///
/// Override is per flag *name*, and it removes every recorded occurrence of that name, so a
/// repeatable flag (`--catalog-conf k=v --catalog-conf j=w`) is replaced as a set rather than
/// appended to.
pub fn restart_flags(typed: &[String], recorded: Option<&PidFile>) -> Vec<String> {
    let recorded = recorded.map(|p| p.args.clone()).unwrap_or_default();
    let mut overridden: std::collections::BTreeSet<String> = flag_entries(typed)
        .into_iter()
        .map(|(name, _)| name)
        .filter(|name| !name.is_empty())
        .collect();
    // Some flags do not merely shadow their recorded twin, they contradict it: `--no-ui` and
    // `--ui-port` cannot both be what the operator meant, and leaving the recorded one in place
    // would silently win. Whichever was typed decides.
    for (a, b) in [("--no-ui", "--ui-port"), ("--no-ui", "--ui-bind")] {
        if overridden.contains(a) {
            overridden.insert(b.to_string());
        }
        if overridden.contains(b) {
            overridden.insert(a.to_string());
        }
    }
    let mut merged: Vec<String> = flag_entries(&recorded)
        .into_iter()
        .filter(|(name, _)| !overridden.contains(name))
        .flat_map(|(_, tokens)| tokens)
        .collect();
    merged.extend(typed.iter().cloned());
    merged
}

/// Split a flag list into `(name, tokens)` entries.
///
/// `--port 50051` and `--port=50051` are both one entry named `--port`; `--no-ui` is one entry
/// with no value. A token that is not a flag and is not consumed as one's value keeps itself,
/// under the empty name, so it is carried through and never treated as overridable.
///
/// The "next token is the value unless it starts with `--`" rule is the same one [`crate::flag`]
/// reads with, which is what makes the merge agree with the parser it feeds.
fn flag_entries(flags: &[String]) -> Vec<(String, Vec<String>)> {
    let mut out: Vec<(String, Vec<String>)> = Vec::new();
    let mut i = 0;
    while i < flags.len() {
        let tok = flags[i].clone();
        let Some(rest) = tok.strip_prefix("--") else {
            out.push((String::new(), vec![tok]));
            i += 1;
            continue;
        };
        let name = format!("--{}", rest.split('=').next().unwrap_or_default());
        let mut tokens = vec![tok.clone()];
        if !tok.contains('=') && flags.get(i + 1).is_some_and(|next| !next.starts_with("--")) {
            tokens.push(flags[i + 1].clone());
            i += 1;
        }
        i += 1;
        out.push((name, tokens));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pidfile(pid: u32, exe: &str, token: &str) -> PidFile {
        PidFile {
            pid,
            exe: exe.to_string(),
            start_token: token.to_string(),
            started_at: "2026-08-25T00:00:00Z".to_string(),
            port: 50051,
            ui_port: Some(4040),
            ui_bind: "0.0.0.0".to_string(),
            args: vec!["--port".into(), "50051".into()],
            log: "/tmp/oxidant.log".to_string(),
        }
    }

    fn row(pid: u32, ppid: u32, command: &str) -> ProcRow {
        ProcRow {
            pid,
            ppid,
            command: command.to_string(),
        }
    }

    // --- identity / verification -------------------------------------------------------------

    #[test]
    fn a_live_process_with_the_recorded_identity_verifies() {
        let me = std::process::id();
        let live = identity(me).expect("this process's own identity is readable");
        let recorded = pidfile(me, &live.exe, &live.start_token);
        assert!(verify(&recorded, Some(&live)));
    }

    /// The recycled-pid case, reduced to its decision: same pid, still alive, different start
    /// token. This is the check that stands between `stop` and someone else's process.
    #[test]
    fn a_recycled_pid_never_verifies() {
        let me = std::process::id();
        let live = identity(me).unwrap();
        let recorded = pidfile(me, &live.exe, "a-token-from-a-process-that-is-gone");
        assert!(!verify(&recorded, Some(&live)));
    }

    #[test]
    fn a_different_executable_never_verifies() {
        let me = std::process::id();
        let live = identity(me).unwrap();
        let recorded = pidfile(me, "/bin/sleep", &live.start_token);
        assert!(!verify(&recorded, Some(&live)));
    }

    /// The stale-pidfile case: the recorded process is gone, so liveness fails before identity
    /// is even consulted. `pid_t::MAX` is above every configured `pid_max` in practice, which
    /// makes it a pid nothing can be running under.
    #[test]
    fn a_dead_pid_never_verifies_even_with_a_matching_identity() {
        let recorded = pidfile(i32::MAX as u32, "/usr/local/bin/oxidant", "12345");
        let live = Identity {
            exe: "/usr/local/bin/oxidant".into(),
            start_token: "12345".into(),
        };
        assert!(!verify(&recorded, Some(&live)));
    }

    /// `kill(0, …)` signals the caller's whole process group, so a pidfile holding `0` must
    /// never read as alive — otherwise `stop` would SIGTERM, and then SIGKILL, this process and
    /// everything beside it.
    #[test]
    fn pid_zero_is_never_alive_and_is_never_signalled() {
        assert!(!alive(0));
        let recorded = pidfile(0, "/usr/local/bin/oxidant", "12345");
        let live = Identity {
            exe: "/usr/local/bin/oxidant".into(),
            start_token: "12345".into(),
        };
        assert!(!verify(&recorded, Some(&live)));
        #[cfg(unix)]
        {
            assert!(signal(0, SIGTERM).is_err());
            assert_eq!(real_pid(0), None);
            assert_eq!(real_pid(1), Some(1));
        }
    }

    /// Unreadable identity is a refusal, not a pass. `stop` turns a `true` here into a signal.
    #[test]
    fn an_unreadable_identity_never_verifies() {
        let recorded = pidfile(std::process::id(), "/usr/local/bin/oxidant", "12345");
        assert!(!verify(&recorded, None));
    }

    #[test]
    fn exe_comparison_tolerates_deleted_and_argv0_spellings() {
        // In-place upgrade: Linux appends " (deleted)" to /proc/<pid>/exe.
        assert!(exe_matches(
            "/usr/local/bin/oxidant",
            "/usr/local/bin/oxidant (deleted)"
        ));
        // `ps -o args=` reports argv[0] as the caller wrote it.
        assert!(exe_matches("/usr/local/bin/oxidant", "./oxidant"));
        assert!(!exe_matches("/usr/local/bin/oxidant", "/bin/sleep"));
        assert!(!exe_matches("/usr/local/bin/oxidant", "/bin/oxidantd"));
    }

    // --- the bare-invocation refusal ---------------------------------------------------------

    #[test]
    fn the_server_refusal_points_at_start_and_at_foreground() {
        let msg = foreground_refusal(Role::Server);
        assert_eq!(
            msg,
            "error: `oxidant spark server` runs a long-lived process, and those run as daemons\n\
             \x20 run it as a daemon:  oxidant start\n\
             \x20 or supervise it yourself:  oxidant spark server … --foreground\n"
        );
    }

    /// A worker has no `oxidant start` form, so its refusal must not invent one.
    #[test]
    fn the_worker_refusal_does_not_promise_a_start_subcommand() {
        let msg = foreground_refusal(Role::Worker);
        assert!(!msg.contains("oxidant start"), "{msg}");
        assert!(msg.contains("oxidant worker … --foreground"), "{msg}");
    }

    #[test]
    fn foreground_is_detected_anywhere_in_the_arguments() {
        let args: Vec<String> = ["oxidant", "worker", "--port", "1", "--foreground"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert!(foreground(&args));
        assert!(!foreground(&args[..4]));
    }

    // --- single instance ---------------------------------------------------------------------

    /// The release-only rule's decision function, exercised in a debug build: a second server
    /// on the machine is a conflict.
    #[test]
    fn a_second_server_is_a_conflict() {
        let table = [
            row(1, 0, "/sbin/init"),
            row(100, 1, "/usr/local/bin/oxidant spark server --port 50051"),
            row(200, 1, "/usr/local/bin/oxidant spark server --port 50052"),
        ];
        let found = single_instance_conflict(&table, 200).expect("the other server");
        assert_eq!(found.pid, 100);
    }

    /// The guard must never count the starting process itself, nor its parents — `oxidant start`
    /// spawns `oxidant spark server --foreground`, so an oxidant ancestor is the normal case.
    #[test]
    fn the_starting_process_and_its_parent_chain_are_never_counted() {
        let table = [
            row(1, 0, "/sbin/init"),
            row(50, 1, "-zsh"),
            row(100, 50, "/usr/local/bin/oxidant start --port 50051"),
            row(
                200,
                100,
                "/usr/local/bin/oxidant spark server --port 50051 --foreground",
            ),
        ];
        // pid 200 is the process that would bind, and the only oxidant server in the table is
        // itself. Its parent is an `oxidant start`, which is not a server role to begin with.
        assert!(single_instance_conflict(&table, 200).is_none());
    }

    /// Excluding the chain must not blind the guard to an unrelated server elsewhere.
    #[test]
    fn a_stranger_server_is_still_found_past_the_parent_chain() {
        let table = [
            row(1, 0, "/sbin/init"),
            row(
                70,
                1,
                "/usr/local/bin/oxidant spark server --port 50051 --foreground",
            ),
            row(100, 1, "/usr/local/bin/oxidant start --port 50052"),
            row(
                200,
                100,
                "/usr/local/bin/oxidant spark server --port 50052 --foreground",
            ),
        ];
        assert_eq!(
            single_instance_conflict(&table, 200).map(|r| r.pid),
            Some(70)
        );
    }

    #[test]
    fn workers_and_client_subcommands_are_not_server_roles() {
        assert!(!is_server_role(
            "/usr/local/bin/oxidant worker --port 50561"
        ));
        assert!(!is_server_role("/usr/local/bin/oxidant sql -e 'SELECT 1'"));
        // The word `server` inside a client subcommand's arguments is not a running engine.
        // `async_main` dispatches `sql` by name long before its `any(== "server")` fallback.
        assert!(!is_server_role(
            "/usr/local/bin/oxidant sql -e SELECT * FROM server_events"
        ));
        assert!(!is_server_role(
            "/usr/local/bin/oxidant pipeline run --table server"
        ));
        assert!(!is_server_role("/usr/local/bin/oxidant start"));
        assert!(!is_server_role("/usr/local/bin/oxidant status"));
        // A test binary whose *path* contains "oxidant" is a stranger, not one of ours.
        assert!(!is_server_role(
            "/w/oxidant/target/debug/deps/cli_daemon-1234 server"
        ));
        assert!(is_server_role("/usr/local/bin/oxidant spark server"));
        // The bare `server` alias `async_main` still accepts.
        assert!(is_server_role("/usr/local/bin/oxidant server --port 50051"));
    }

    #[test]
    fn a_cyclic_process_table_does_not_hang_the_ancestor_walk() {
        let table = [row(10, 20, "a"), row(20, 10, "b")];
        assert!(single_instance_conflict(&table, 10).is_none());
    }

    // --- pidfile -----------------------------------------------------------------------------

    #[test]
    fn the_pidfile_round_trips_through_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("run/oxidant.pid");
        let written = pidfile(4242, "/usr/local/bin/oxidant", "9911");
        written.write(&path).unwrap();
        let read = PidFile::read(&path).expect("read back");
        assert_eq!(read.pid, 4242);
        assert_eq!(read.start_token, "9911");
        assert_eq!(read.args, vec!["--port".to_string(), "50051".to_string()]);
    }

    #[test]
    fn a_corrupt_pidfile_reads_as_absent_rather_than_panicking() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("oxidant.pid");
        std::fs::write(&path, "12345\n").unwrap();
        assert!(PidFile::read(&path).is_none());
    }

    #[test]
    fn the_health_probe_targets_loopback_when_the_ui_binds_every_interface() {
        let mut p = pidfile(1, "x", "y");
        assert_eq!(p.http_base().as_deref(), Some("http://127.0.0.1:4040"));
        p.ui_bind = "10.0.0.4".into();
        assert_eq!(p.http_base().as_deref(), Some("http://10.0.0.4:4040"));
        p.ui_bind = "::1".into();
        assert_eq!(p.http_base().as_deref(), Some("http://[::1]:4040"));
        p.ui_port = None;
        assert_eq!(p.http_base(), None);
    }

    /// ... and what is *printed* is the address that was bound, not the probe target. A UI on
    /// every interface reported as `http://127.0.0.1:4040` reads as loopback-only, which is the
    /// one thing `docs/web-ui.md` tells an operator to check before trusting an unauthenticated
    /// UI on a reachable host.
    #[test]
    fn the_printed_ui_endpoint_names_the_interface_it_is_bound_to() {
        let mut p = pidfile(1, "x", "y");
        assert_eq!(
            p.ui_endpoint().as_deref(),
            Some("http://0.0.0.0:4040  (all interfaces; local http://127.0.0.1:4040)")
        );
        p.ui_bind = "::".into();
        assert_eq!(
            p.ui_endpoint().as_deref(),
            Some("http://[::]:4040  (all interfaces; local http://127.0.0.1:4040)")
        );
        // A real address is already the truth and gets no parenthetical.
        p.ui_bind = "127.0.0.1".into();
        assert_eq!(p.ui_endpoint().as_deref(), Some("http://127.0.0.1:4040"));
        p.ui_bind = "10.0.0.4".into();
        assert_eq!(p.ui_endpoint().as_deref(), Some("http://10.0.0.4:4040"));
        p.ui_port = None;
        assert_eq!(p.ui_endpoint(), None);
    }

    #[test]
    fn the_stranger_report_names_both_executables_and_the_pidfile_to_remove() {
        let recorded = pidfile(4242, "/usr/local/bin/oxidant", "tok");
        let live = Identity {
            exe: "/bin/sleep".into(),
            start_token: "tok".into(),
        };
        let report = stranger_report(&recorded, Some(&live));
        assert!(report.starts_with("error: pid 4242 is alive but is not this oxidant daemon"));
        assert!(
            report.contains("recorded /usr/local/bin/oxidant"),
            "{report}"
        );
        assert!(report.contains("actual:   /bin/sleep"), "{report}");
        assert!(
            report.contains("a recycled pid is never killed"),
            "{report}"
        );
        // Unreadable identity says so rather than leaving the line blank.
        let blind = stranger_report(&recorded, None);
        assert!(blind.contains("belongs to another user"), "{blind}");
    }

    // --- the spawned child is never abandoned -------------------------------------------------

    /// The leak this guard exists to stop, reduced to its decision: a `ChildGuard` that goes out
    /// of scope without being disarmed leaves nothing running. `start` used to return through a
    /// bare `?` between `spawn` and the pidfile write, and the detached engine on the other side
    /// of it was invisible to `status` and `stop` forever.
    #[test]
    fn a_child_guard_that_is_dropped_undisarmed_reaps_the_child() {
        let child = Command::new("sleep")
            .arg("300")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn sleep");
        let pid = child.id();
        assert!(alive(pid), "the test child did not start");
        drop(ChildGuard::new(child));
        // `reap` waits, so by the time `drop` returns the process is gone, not merely signalled.
        assert!(!alive(pid), "pid {pid} outlived the guard that owned it");
    }

    /// ... and a disarmed one does not, because by then it is the pidfile's daemon.
    #[test]
    fn a_disarmed_child_guard_leaves_the_daemon_alone() {
        let child = Command::new("sleep")
            .arg("300")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn sleep");
        let pid = child.id();
        let mut guard = ChildGuard::new(child);
        guard.disarm();
        drop(guard);
        assert!(alive(pid), "a started daemon was killed by its own start");
        let _ = signal(pid, SIGKILL);
    }

    /// A failed start clears *its own* pidfile and no one else's. The blind `remove_file` this
    /// replaces would delete a racing daemon's file and orphan it.
    #[test]
    fn the_failure_path_only_removes_a_pidfile_that_still_names_us() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("oxidant.pid");

        pidfile(4242, "/usr/local/bin/oxidant", "tok")
            .write(&path)
            .unwrap();
        remove_pidfile_if_ours(&path, 4242);
        assert!(!path.exists(), "our own pidfile must be cleaned up");

        // The racing case: the file on disk belongs to the daemon that won.
        pidfile(9999, "/usr/local/bin/oxidant", "tok")
            .write(&path)
            .unwrap();
        remove_pidfile_if_ours(&path, 4242);
        assert!(
            path.exists(),
            "another daemon's pidfile was deleted by our failure path"
        );
        assert_eq!(PidFile::read(&path).unwrap().pid, 9999);
    }

    // --- restart -----------------------------------------------------------------------------

    fn strs(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn restart_replays_the_recorded_flags_unless_new_ones_were_typed() {
        let recorded = pidfile(1, "x", "y");
        assert_eq!(
            restart_flags(&[], Some(&recorded)),
            strs(&["--port", "50051"])
        );
        let typed = strs(&["--port", "50052"]);
        assert_eq!(restart_flags(&typed, Some(&recorded)), typed);
        assert!(restart_flags(&[], None).is_empty());
    }

    /// The finding: typing *any* argument used to revert every flag it did not mention to its
    /// default. Moving the UI must not move the Spark Connect port with it.
    #[test]
    fn typed_flags_are_laid_over_the_recorded_ones_not_swapped_for_them() {
        let mut recorded = pidfile(1, "x", "y");
        recorded.args = strs(&[
            "--port",
            "50452",
            "--ui-port",
            "4452",
            "--sample-data",
            "/d",
        ]);
        assert_eq!(
            restart_flags(&strs(&["--ui-port", "4050"]), Some(&recorded)),
            strs(&[
                "--port",
                "50452",
                "--sample-data",
                "/d",
                "--ui-port",
                "4050"
            ])
        );
        // `--port=50452` is the same entry as `--port 50452`, and is overridden the same way.
        recorded.args = strs(&["--port=50452", "--ui-port", "4452"]);
        assert_eq!(
            restart_flags(&strs(&["--port", "50999"]), Some(&recorded)),
            strs(&["--ui-port", "4452", "--port", "50999"])
        );
    }

    /// A repeatable flag is replaced as a set: three recorded `--catalog-conf` entries and one
    /// typed means one, not four (two of which the parser would silently shadow).
    #[test]
    fn a_repeated_flag_is_overridden_as_a_whole_set() {
        let mut recorded = pidfile(1, "x", "y");
        recorded.args = strs(&[
            "--catalog-conf",
            "a=1",
            "--catalog-conf",
            "b=2",
            "--port",
            "50452",
        ]);
        assert_eq!(
            restart_flags(&strs(&["--catalog-conf", "c=3"]), Some(&recorded)),
            strs(&["--port", "50452", "--catalog-conf", "c=3"])
        );
    }

    /// `--no-ui` and `--ui-port` contradict each other, so the recorded one must not survive the
    /// typed one and quietly win.
    #[test]
    fn typing_no_ui_drops_the_recorded_ui_port_and_the_reverse() {
        let mut recorded = pidfile(1, "x", "y");
        recorded.args = strs(&[
            "--port",
            "50452",
            "--ui-port",
            "4452",
            "--ui-bind",
            "127.0.0.1",
        ]);
        assert_eq!(
            restart_flags(&strs(&["--no-ui"]), Some(&recorded)),
            strs(&["--port", "50452", "--no-ui"])
        );
        recorded.args = strs(&["--port", "50452", "--no-ui"]);
        assert_eq!(
            restart_flags(&strs(&["--ui-port", "4050"]), Some(&recorded)),
            strs(&["--port", "50452", "--ui-port", "4050"])
        );
    }

    // --- ps parsing --------------------------------------------------------------------------

    #[test]
    fn the_process_table_parses_ps_output_with_padded_columns() {
        let out = "    1     0 /sbin/init\n \
                   99724     1 /usr/local/bin/oxidant spark server --port 50051\n\
                   header junk\n";
        let table = parse_ps_table(out);
        assert_eq!(table.len(), 2);
        assert_eq!(table[1].pid, 99724);
        assert_eq!(table[1].ppid, 1);
        assert!(table[1].command.ends_with("--port 50051"));
    }

    /// The real table, from the real `ps`: this process must appear in it, with a command line
    /// that is not empty. Guards the column order against a platform whose `ps` disagrees.
    #[test]
    fn the_real_process_table_contains_this_process() {
        let table = process_table();
        // Not `if table.is_empty() { return }`: an empty table is precisely what a parser that
        // has stopped understanding this platform's `ps` produces, and skipping on it is how
        // the padded-column bug survived its own test.
        assert!(
            !table.is_empty(),
            "neither `ps` nor /proc yielded a process table"
        );
        let me = table
            .iter()
            .find(|r| r.pid == std::process::id())
            .expect("this test process is in the table");
        assert!(!me.command.is_empty());
    }

    #[test]
    fn the_log_tail_indents_and_bounds_what_it_shows() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("oxidant.log");
        std::fs::write(
            &path,
            (0..50).map(|i| format!("line {i}\n")).collect::<String>(),
        )
        .unwrap();
        let shown = tail(&path, 3);
        assert_eq!(shown, "  | line 47\n  | line 48\n  | line 49\n");
        assert_eq!(tail(&tmp.path().join("nope.log"), 3), "");
    }
}
