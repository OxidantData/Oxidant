//! Friendly port-conflict guard for every port the CLI binds.
//!
//! The failure this exists for: nine `oxidant` processes accumulated on a laptop — leftovers
//! from agent and test runs that never exited — and the one holding port 4040 had been up for
//! days. Starting a fresh server on that port failed with a bare "address in use" from deep
//! inside tonic, which never said *who* held it, so the only recovery was `lsof` archaeology.
//!
//! So before each listener is handed to tonic/axum we probe the address ourselves. A probe that
//! fails with [`AddrInUse`](std::io::ErrorKind::AddrInUse) is the one case we intercept: we look
//! up the listening process, and if it is another `oxidant` we print its PID, command line,
//! uptime and *every* port it holds, plus the `kill` that frees them.
//!
//! Two rules keep this from becoming its own bug source:
//!
//! * **Only a real conflict fires it.** Nothing here counts oxidant processes or objects to
//!   several of them — `--mode local-cluster`, `OXIDANT_COLOCATED_ENGINES` and the test suite
//!   all run many at once on ephemeral ports, which is fine and stays fine. The guard triggers
//!   when the user asked for a port that is *taken*, and all it does is name who took it.
//! * **Detection is an enhancement, never a blocker.** Every lookup below is best-effort. If
//!   `lsof` is missing, `/proc` is unreadable or the occupier belongs to another user, the user
//!   still gets a clear "port N is already in use" and the flag that moves this process off it.
//!
//! The probe binds and immediately drops a listener, so there is a hair of TOCTOU between the
//! check and the real bind. Losing that race just returns the old bare error from tonic — the
//! guard can only ever add information, never remove it.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::process::Command;
use std::time::Duration;

/// What this process wants a port for.
///
/// Decides which flag we tell the user to change, and how we label the *occupier's* ports when
/// its command line explains them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PortKind {
    /// `--port` on `spark server`: the Spark Connect gRPC listener.
    SparkConnect,
    /// `--ui-port` on `spark server`. One listener, two APIs: `oxidant-connect`'s REST
    /// statement router is merged into the UI router, so the REST/HTTP API has no port of its
    /// own — freeing the UI port frees both.
    Ui,
    /// `--port` on `worker`: the Arrow Flight listener.
    Flight,
    /// `--port` on `history-server`: the standalone UI over a replayed event log.
    History,
}

impl PortKind {
    /// The flag that moves *this* process to a different port.
    fn flag(self) -> &'static str {
        match self {
            PortKind::Ui => "--ui-port",
            PortKind::SparkConnect | PortKind::Flight | PortKind::History => "--port",
        }
    }

    /// How the closing hint names the thing being started.
    fn role(self) -> &'static str {
        match self {
            PortKind::SparkConnect | PortKind::Ui => "this server",
            PortKind::Flight => "this worker",
            PortKind::History => "this history server",
        }
    }

    /// The label used for a port of this kind in the occupier's `ports:` line.
    fn label(self) -> &'static str {
        match self {
            PortKind::SparkConnect => "spark connect",
            PortKind::Ui => "ui + rest",
            PortKind::Flight => "flight",
            PortKind::History => "history ui",
        }
    }
}

/// A process found listening on a port we wanted.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Occupant {
    pub pid: u32,
    /// Full command line, as `ps` / `/proc/<pid>/cmdline` reports it. Empty when unreadable.
    pub command: String,
    /// How long it has been running, when we could read it.
    pub uptime: Option<Duration>,
    /// Every TCP port it listens on, ascending and deduplicated (one port shows up once per
    /// address family in `lsof`).
    pub ports: Vec<u16>,
}

/// Probe `addr`, and on a port conflict print the report and exit non-zero.
///
/// This exits the process rather than returning an error because the report is a multi-line
/// terminal block: `main`'s error path prefixes what it prints with `oxidant: `, which would
/// bury the first line and leave the rest dangling.
pub fn ensure_available(addr: SocketAddr, kind: PortKind) {
    let Err(e) = std::net::TcpListener::bind(addr) else {
        return;
    };
    // Anything that is not a conflict (EACCES on a privileged port, an unroutable bind address)
    // is not ours to explain: let the real listener fail with its own message so we never turn a
    // different problem into a misleading "port is in use".
    if e.kind() != std::io::ErrorKind::AddrInUse {
        return;
    }
    eprint!(
        "{}",
        conflict_report(addr.port(), kind, find_occupant(addr.port()).as_ref())
    );
    std::process::exit(1);
}

/// The terminal block printed on a conflict. Pure so its exact shape is unit-testable.
fn conflict_report(port: u16, kind: PortKind, occupant: Option<&Occupant>) -> String {
    let mut out = String::new();
    let move_hint = format!(
        "start {} on a different port with {}",
        kind.role(),
        kind.flag()
    );
    let Some(occ) = occupant else {
        // Detection failed (no `lsof`, unreadable `/proc`, or the holder is another user's
        // process). Still say the useful half.
        out.push_str(&format!(
            "error: port {port} is already in use, and the process holding it could not be identified\n"
        ));
        out.push_str(&format!("{move_hint}\n"));
        return out;
    };

    let oxidant = is_oxidant_command(&occ.command);
    if oxidant {
        out.push_str(&format!(
            "error: port {port} is already held by another oxidant process\n"
        ));
    } else {
        out.push_str(&format!("error: port {port} is already in use\n"));
    }
    out.push_str(&format!(
        "  pid:      {} ({})\n",
        occ.pid,
        elide(&occ.command, 96)
    ));
    if let Some(up) = occ.uptime {
        out.push_str(&format!("  running:  {}\n", humanize(up)));
    }
    if !occ.ports.is_empty() {
        let listed = order_ports(&occ.ports, port)
            .iter()
            .map(|p| {
                // Only an oxidant command line may be read for oxidant roles. `port_label`
                // falls back to *our* defaults for a port its flags do not explain, so on a
                // stranger it invents: an `ssh` multiplexer forwarding 50051 and 4040 was
                // reported as running a Spark Connect server and an Oxidant UI. The headline
                // above is already neutral for a stranger, which made the contradiction worse —
                // the labelled `ports:` line is the more specific-looking claim of the two.
                match oxidant.then(|| port_label(&occ.command, *p)).flatten() {
                    Some(what) => format!("{p} ({what})"),
                    None => p.to_string(),
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(&format!("  ports:    {listed}\n"));
    }
    let stop = if oxidant {
        "stop it"
    } else {
        "stop that process"
    };
    out.push_str(&format!("{stop} with `kill {}`, or {move_hint}\n", occ.pid));
    out
}

/// The occupier's ports with the one that was asked for first, the rest ascending.
///
/// The conflict is the reason the user is reading this line; the other ports are the bonus
/// ("killing it also frees these").
fn order_ports(ports: &[u16], conflicting: u16) -> Vec<u16> {
    let mut ordered: Vec<u16> = ports
        .iter()
        .copied()
        .filter(|p| *p != conflicting)
        .collect();
    ordered.sort_unstable();
    if ports.contains(&conflicting) {
        ordered.insert(0, conflicting);
    }
    ordered
}

/// Is this command line another oxidant process?
///
/// Only the *basename* of argv[0] is considered. The whole string is riddled with false
/// positives — every test binary under `…/oxidant/target/debug/deps/` has "oxidant" in its
/// path, and a plain `TcpListener` held by the test suite must read as a stranger, not as one
/// of ours.
pub fn is_oxidant_command(command: &str) -> bool {
    let Some(argv0) = command.split_whitespace().next() else {
        return false;
    };
    let name = basename(argv0);
    let name = name.strip_suffix(".exe").unwrap_or(name);
    name == "oxidant" || name.starts_with("oxidant-")
}

/// What the occupier uses `port` for, read off its own command line.
///
/// Returns `None` when the flags do not explain the port — an unlabeled number is honest, a
/// guessed label is not.
///
/// **Only ever called on a command line [`is_oxidant_command`] has accepted.** The default
/// branch below applies oxidant's own defaults to a command line that did not mention a port,
/// which is only a reading of the flags when the flags are ours to read.
fn port_label(command: &str, port: u16) -> Option<&'static str> {
    let toks: Vec<&str> = command.split_whitespace().collect();
    let value = |name: &str| -> Option<u16> {
        toks.iter().enumerate().find_map(|(i, t)| {
            if *t == name {
                toks.get(i + 1)?.parse().ok()
            } else {
                t.strip_prefix(name)?.strip_prefix('=')?.parse().ok()
            }
        })
    };
    let subcommand = toks.get(1).copied().unwrap_or_default();
    let explicit_port = value("--port");
    if explicit_port == Some(port) {
        return Some(match subcommand {
            "worker" => PortKind::Flight.label(),
            "history-server" => PortKind::History.label(),
            _ => PortKind::SparkConnect.label(),
        });
    }
    if value("--ui-port") == Some(port) {
        return Some(PortKind::Ui.label());
    }
    // Defaults, which is how the incident's oldest process was launched: `oxidant spark server`
    // with no flags at all still holds 50051 and 4040.
    if matches!(subcommand, "worker" | "history-server" | "driver") {
        return None;
    }
    if port == 50051 && explicit_port.is_none() {
        return Some(PortKind::SparkConnect.label());
    }
    if port == 4040 && value("--ui-port").is_none() && !toks.contains(&"--no-ui") {
        return Some(PortKind::Ui.label());
    }
    None
}

/// Fit a command line onto a terminal line, shedding the least useful part first.
///
/// For `/Users/…/target/debug/oxidant spark server --port 50051` the flags are the whole point
/// and the install path is not, so argv[0] loses its directory before anything is truncated.
pub fn elide(command: &str, max: usize) -> String {
    if command.trim().is_empty() {
        return "unknown command".to_string();
    }
    if command.chars().count() <= max {
        return command.to_string();
    }
    let short = match command.split_once(char::is_whitespace) {
        Some((argv0, rest)) => format!("{} {rest}", basename(argv0)),
        None => basename(command).to_string(),
    };
    if short.chars().count() <= max {
        return short;
    }
    let head: String = short.chars().take(max).collect();
    format!("{}…", head.trim_end())
}

pub fn basename(path: &str) -> &str {
    path.rsplit(['/', '\\']).next().unwrap_or(path)
}

/// Coarse, human-scale uptime — "2 days" is the useful fact ("this is a leftover"), not
/// "2 days, 7 hours, 15 minutes and 24 seconds".
pub fn humanize(d: Duration) -> String {
    let secs = d.as_secs();
    let (n, unit) = match secs {
        0..=89 => (secs.max(1), "second"),
        90..=5399 => ((secs + 30) / 60, "minute"),
        5400..=86399 => ((secs + 1800) / 3600, "hour"),
        _ => (secs / 86400, "day"),
    };
    if n == 1 {
        format!("{n} {unit}")
    } else {
        format!("{n} {unit}s")
    }
}

// ---------------------------------------------------------------------------------------------
// Detection. Every function here returns "no information" rather than an error: the caller
// already has a message to print without it.
// ---------------------------------------------------------------------------------------------

/// The process listening on `port`, if we can name it.
fn find_occupant(port: u16) -> Option<Occupant> {
    let pid = listener_pid(port)?;
    Some(Occupant {
        pid,
        command: process_command(pid).unwrap_or_default(),
        uptime: process_uptime(pid),
        ports: listening_ports(pid),
    })
}

/// PID listening on `port`. `lsof` first (present by default on macOS, and the only option
/// there), then a pure-`/proc` walk on Linux for the images that ship without it.
fn listener_pid(port: u16) -> Option<u32> {
    let out = run(
        "lsof",
        &["-nP", &format!("-iTCP:{port}"), "-sTCP:LISTEN", "-t"],
    );
    if let Some(pid) = out.and_then(|o| parse_lsof_pids(&o).into_iter().next()) {
        return Some(pid);
    }
    proc_listener_pid(port)
}

/// Every TCP port `pid` is listening on. Empty when we cannot tell.
fn listening_ports(pid: u32) -> Vec<u16> {
    let out = run(
        "lsof",
        &[
            "-nP",
            "-a",
            "-p",
            &pid.to_string(),
            "-iTCP",
            "-sTCP:LISTEN",
            "-F",
            "n",
        ],
    );
    if let Some(ports) = out.map(|o| parse_lsof_ports(&o)).filter(|p| !p.is_empty()) {
        return ports;
    }
    proc_listening_ports(pid)
}

/// Full command line of `pid`.
fn process_command(pid: u32) -> Option<String> {
    #[cfg(target_os = "linux")]
    if let Some(cmd) = proc_cmdline(pid) {
        return Some(cmd);
    }
    let out = run("ps", &["-p", &pid.to_string(), "-o", "args="])?;
    let cmd = out.trim();
    (!cmd.is_empty()).then(|| cmd.to_string())
}

/// How long `pid` has been running.
fn process_uptime(pid: u32) -> Option<Duration> {
    #[cfg(target_os = "linux")]
    if let Some(up) = proc_uptime(pid) {
        return Some(up);
    }
    parse_etime(&run("ps", &["-p", &pid.to_string(), "-o", "etime="])?)
}

/// Run a helper and capture stdout, or `None` if it is missing or fails.
fn run(program: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(program).args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8(out.stdout).ok()
}

/// `lsof -t`: one PID per line.
fn parse_lsof_pids(stdout: &str) -> Vec<u32> {
    stdout
        .lines()
        .filter_map(|l| l.trim().parse::<u32>().ok())
        .collect()
}

/// `lsof -F n`: field-per-line output where `n` lines are addresses (`n*:50051`,
/// `n127.0.0.1:4040`, `n[::1]:4040`). One port appears once per address family, so dedup.
fn parse_lsof_ports(stdout: &str) -> Vec<u16> {
    stdout
        .lines()
        .filter_map(|l| l.strip_prefix('n'))
        .filter_map(|addr| addr.rsplit_once(':'))
        .filter_map(|(_, port)| port.trim().parse::<u16>().ok())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// `ps -o etime=`: `[[DD-]HH:]MM:SS`.
fn parse_etime(raw: &str) -> Option<Duration> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    let (days, clock) = match raw.split_once('-') {
        Some((d, rest)) => (d.trim().parse::<u64>().ok()?, rest),
        None => (0, raw),
    };
    let mut secs = 0u64;
    for part in clock.split(':') {
        secs = secs * 60 + part.trim().parse::<u64>().ok()?;
    }
    Some(Duration::from_secs(days * 86_400 + secs))
}

// --- Linux-only pure-Rust fallbacks -----------------------------------------------------------
//
// `lsof` is absent from plenty of slim container images, and `/proc` answers the same questions
// without spawning anything. Both paths are bounded by the same permission the user already
// has: fds of another user's process are unreadable either way, and then we simply say less.

/// Socket inode → PID by scanning `/proc/<pid>/fd/*` for `socket:[inode]`.
#[cfg(target_os = "linux")]
fn proc_pid_for_inodes(inodes: &BTreeSet<u64>) -> Option<u32> {
    let wanted: BTreeSet<String> = inodes.iter().map(|i| format!("socket:[{i}]")).collect();
    for entry in std::fs::read_dir("/proc").ok()?.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|n| n.parse::<u32>().ok())
        else {
            continue;
        };
        let Ok(fds) = std::fs::read_dir(entry.path().join("fd")) else {
            continue;
        };
        for fd in fds.flatten() {
            if let Ok(target) = std::fs::read_link(fd.path()) {
                if wanted.contains(target.to_string_lossy().as_ref()) {
                    return Some(pid);
                }
            }
        }
    }
    None
}

/// Listening sockets from `/proc/net/tcp{,6}` as `(port, inode)`.
#[cfg(target_os = "linux")]
fn proc_listening_sockets() -> Vec<(u16, u64)> {
    let mut out = Vec::new();
    for path in ["/proc/net/tcp", "/proc/net/tcp6"] {
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        out.extend(parse_proc_net_tcp(&text));
    }
    out
}

/// Parse `/proc/net/tcp`: `sl local_address rem_address st … uid timeout inode`.
/// `st == 0A` is `TCP_LISTEN`; the port is the hex tail of `local_address`.
#[cfg(target_os = "linux")]
fn parse_proc_net_tcp(text: &str) -> Vec<(u16, u64)> {
    text.lines()
        .skip(1)
        .filter_map(|line| {
            let f: Vec<&str> = line.split_whitespace().collect();
            if f.len() < 10 || f[3] != "0A" {
                return None;
            }
            let port = u16::from_str_radix(f[1].rsplit_once(':')?.1, 16).ok()?;
            Some((port, f[9].parse::<u64>().ok()?))
        })
        .collect()
}

#[cfg(target_os = "linux")]
fn proc_listener_pid(port: u16) -> Option<u32> {
    let inodes: BTreeSet<u64> = proc_listening_sockets()
        .into_iter()
        .filter(|(p, _)| *p == port)
        .map(|(_, inode)| inode)
        .collect();
    if inodes.is_empty() {
        return None;
    }
    proc_pid_for_inodes(&inodes)
}

#[cfg(not(target_os = "linux"))]
fn proc_listener_pid(_port: u16) -> Option<u32> {
    None
}

#[cfg(target_os = "linux")]
fn proc_listening_ports(pid: u32) -> Vec<u16> {
    let Ok(fds) = std::fs::read_dir(format!("/proc/{pid}/fd")) else {
        return Vec::new();
    };
    let mine: BTreeSet<u64> = fds
        .flatten()
        .filter_map(|fd| std::fs::read_link(fd.path()).ok())
        .filter_map(|target| {
            target
                .to_string_lossy()
                .strip_prefix("socket:[")?
                .strip_suffix(']')?
                .parse::<u64>()
                .ok()
        })
        .collect();
    proc_listening_sockets()
        .into_iter()
        .filter(|(_, inode)| mine.contains(inode))
        .map(|(port, _)| port)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[cfg(not(target_os = "linux"))]
fn proc_listening_ports(_pid: u32) -> Vec<u16> {
    Vec::new()
}

#[cfg(target_os = "linux")]
fn proc_cmdline(pid: u32) -> Option<String> {
    let raw = std::fs::read(format!("/proc/{pid}/cmdline")).ok()?;
    let cmd = raw
        .split(|b| *b == 0)
        .filter(|a| !a.is_empty())
        .map(String::from_utf8_lossy)
        .collect::<Vec<_>>()
        .join(" ");
    (!cmd.is_empty()).then_some(cmd)
}

/// `/proc/uptime` minus field 22 of `/proc/<pid>/stat` (start time, in clock ticks).
///
/// `USER_HZ` is 100 on every Linux Rust supports; this is a display string, not arithmetic
/// anyone depends on, so we do not pull in `libc` for `sysconf(_SC_CLK_TCK)`.
#[cfg(target_os = "linux")]
fn proc_uptime(pid: u32) -> Option<Duration> {
    const USER_HZ: f64 = 100.0;
    let boot_secs: f64 = std::fs::read_to_string("/proc/uptime")
        .ok()?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let started = parse_proc_stat_starttime(&stat)?;
    let alive = boot_secs - (started as f64 / USER_HZ);
    (alive >= 0.0).then(|| Duration::from_secs_f64(alive))
}

/// Field 22 of `/proc/<pid>/stat`. Field 2 is the executable name in parentheses and may itself
/// contain spaces and parens, so the split starts after the *last* `)`.
#[cfg(target_os = "linux")]
fn parse_proc_stat_starttime(stat: &str) -> Option<u64> {
    let after_comm = &stat[stat.rfind(')')? + 1..];
    // After `)` the fields resume at 3 (state), so start time (22) is index 19 here.
    after_comm.split_whitespace().nth(19)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn occupant(command: &str, ports: &[u16]) -> Occupant {
        Occupant {
            pid: 41219,
            command: command.to_string(),
            uptime: Some(Duration::from_secs(2 * 86_400)),
            ports: ports.to_vec(),
        }
    }

    /// The whole point of the feature: the message names the PID, what it is running, how long
    /// it has been up, and every port killing it would free.
    #[test]
    fn conflict_report_names_the_oxidant_occupier() {
        let occ = occupant(
            "/usr/local/bin/oxidant spark server --port 50051 --ui-port 4040",
            &[4040, 50051],
        );
        let report = conflict_report(50051, PortKind::SparkConnect, Some(&occ));
        assert_eq!(
            report,
            "error: port 50051 is already held by another oxidant process\n\
             \x20 pid:      41219 (/usr/local/bin/oxidant spark server --port 50051 --ui-port 4040)\n\
             \x20 running:  2 days\n\
             \x20 ports:    50051 (spark connect), 4040 (ui + rest)\n\
             stop it with `kill 41219`, or start this server on a different port with --port\n"
        );
    }

    /// A `--ui-port` conflict must point at `--ui-port`; telling the user to change `--port`
    /// would move the listener that was never in conflict.
    #[test]
    fn conflict_report_on_ui_port_points_at_ui_port_flag() {
        let occ = occupant("oxidant spark server --port 50051 --ui-port 4040", &[4040]);
        let report = conflict_report(4040, PortKind::Ui, Some(&occ));
        assert!(report.starts_with("error: port 4040 is already held by another oxidant process\n"));
        assert!(report.contains("  ports:    4040 (ui + rest)\n"));
        assert!(report.ends_with(
            "stop it with `kill 41219`, or start this server on a different port with --ui-port\n"
        ));
    }

    #[test]
    fn conflict_report_on_worker_port_says_worker() {
        let occ = occupant("oxidant worker --port 50561", &[50561]);
        let report = conflict_report(50561, PortKind::Flight, Some(&occ));
        assert!(report.contains("  ports:    50561 (flight)\n"));
        assert!(report.ends_with(
            "stop it with `kill 41219`, or start this worker on a different port with --port\n"
        ));
    }

    /// A stranger on the port is not "another oxidant process", and we do not tell the user
    /// their own oxidant is at fault.
    #[test]
    fn conflict_report_for_a_stranger_stays_neutral() {
        let occ = occupant("nginx: master process /usr/sbin/nginx", &[50051]);
        let report = conflict_report(50051, PortKind::SparkConnect, Some(&occ));
        assert!(report.starts_with("error: port 50051 is already in use\n"));
        assert!(!report.contains("oxidant process"));
        assert!(report.contains("stop that process with `kill 41219`"));
    }

    /// ... and neither are a stranger's *ports*. Reproduced on the reviewer's machine, where an
    /// `ssh` multiplexer forwards both of oxidant's defaults: the report told the operator that
    /// an SSH tunnel was running a Spark Connect server and an Oxidant UI. `port_label` reads
    /// oxidant's defaults off a command line that never mentioned a port, so on anything but
    /// oxidant it is not reading, it is guessing.
    #[test]
    fn a_strangers_ports_are_never_given_oxidant_roles() {
        let occ = occupant(
            "ssh: /Users/vamsi/.colima/_lima/colima/ssh.sock [mux]",
            &[50051, 4040, 5500, 55432],
        );
        let report = conflict_report(50051, PortKind::SparkConnect, Some(&occ));
        assert!(
            report.contains("  ports:    50051, 4040, 5500, 55432\n"),
            "a stranger's ports must be bare numbers: {report}"
        );
        assert!(!report.contains("spark connect"), "{report}");
        assert!(!report.contains("ui + rest"), "{report}");
    }

    /// Detection is an enhancement, never a blocker: with no occupier at all the user still
    /// learns the port is taken and which flag moves them off it.
    #[test]
    fn conflict_report_without_detection_still_says_the_useful_half() {
        let report = conflict_report(4040, PortKind::Ui, None);
        assert_eq!(
            report,
            "error: port 4040 is already in use, and the process holding it could not be identified\n\
             start this server on a different port with --ui-port\n"
        );
    }

    #[test]
    fn is_oxidant_command_matches_the_binary_not_the_path() {
        assert!(is_oxidant_command("/usr/local/bin/oxidant spark server"));
        assert!(is_oxidant_command("oxidant worker --port 50561"));
        assert!(is_oxidant_command("oxidant-worker.exe --port 50561"));
        // Every integration-test binary lives under `…/oxidant/target/debug/deps/`, so a
        // substring match would report the test suite's own listener as one of ours.
        assert!(!is_oxidant_command(
            "/Users/x/src/oxidant/target/debug/deps/cli_port_guard-9f3a"
        ));
        assert!(!is_oxidant_command("nginx: master process"));
        assert!(!is_oxidant_command(""));
    }

    #[test]
    fn port_label_reads_the_occupiers_flags() {
        let server = "oxidant spark server --port 50051 --ui-port 4040";
        assert_eq!(port_label(server, 50051), Some("spark connect"));
        assert_eq!(port_label(server, 4040), Some("ui + rest"));
        assert_eq!(port_label(server, 9999), None);
        assert_eq!(
            port_label("oxidant worker --port 50561", 50561),
            Some("flight")
        );
        assert_eq!(
            port_label("oxidant history-server --dir logs --port 18080", 18080),
            Some("history ui")
        );
        assert_eq!(
            port_label("oxidant spark server --port=7077", 7077),
            Some("spark connect")
        );
    }

    /// The process that started the incident was launched with no flags at all and still held
    /// both defaults.
    #[test]
    fn port_label_knows_the_defaults() {
        assert_eq!(
            port_label("oxidant spark server", 50051),
            Some("spark connect")
        );
        assert_eq!(port_label("oxidant spark server", 4040), Some("ui + rest"));
        // ... unless the flags say otherwise.
        assert_eq!(port_label("oxidant spark server --no-ui", 4040), None);
        assert_eq!(port_label("oxidant spark server --port 9000", 50051), None);
        // A worker has no UI, so 50051 on one is not the Spark Connect port.
        assert_eq!(port_label("oxidant worker --port 4040", 50051), None);
    }

    #[test]
    fn order_ports_leads_with_the_conflicting_one() {
        assert_eq!(
            order_ports(&[4040, 50051, 18080], 50051),
            vec![50051, 4040, 18080]
        );
        assert_eq!(order_ports(&[8080, 4040], 9999), vec![4040, 8080]);
        assert!(order_ports(&[], 4040).is_empty());
    }

    #[test]
    fn elide_drops_the_install_path_before_truncating() {
        let long = "/Users/somebody/very/long/path/target/debug/oxidant spark server --port 50051 --ui-port 4040";
        assert_eq!(
            elide(long, 60),
            "oxidant spark server --port 50051 --ui-port 4040"
        );
        assert_eq!(elide("oxidant spark server", 60), "oxidant spark server");
        assert_eq!(
            elide("oxidant spark server --port 50051", 20),
            "oxidant spark server…"
        );
        assert_eq!(elide("", 60), "unknown command");
    }

    #[test]
    fn humanize_is_coarse_on_purpose() {
        assert_eq!(humanize(Duration::from_secs(1)), "1 second");
        assert_eq!(humanize(Duration::from_secs(0)), "1 second");
        assert_eq!(humanize(Duration::from_secs(45)), "45 seconds");
        assert_eq!(humanize(Duration::from_secs(600)), "10 minutes");
        assert_eq!(humanize(Duration::from_secs(7200)), "2 hours");
        assert_eq!(humanize(Duration::from_secs(2 * 86_400 + 26_124)), "2 days");
    }

    #[test]
    fn parse_lsof_pids_takes_the_bare_numbers() {
        assert_eq!(parse_lsof_pids("41219\n41220\n"), vec![41219, 41220]);
        assert!(parse_lsof_pids("").is_empty());
    }

    /// `lsof -F n` repeats a port once per address family; the report must not.
    #[test]
    fn parse_lsof_ports_dedups_across_address_families() {
        let stdout =
            "p41219\nf9\nn*:50051\nf10\nn*:50051\nf11\nn127.0.0.1:4040\nf12\nn[::1]:4040\n";
        assert_eq!(parse_lsof_ports(stdout), vec![4040, 50051]);
        assert!(parse_lsof_ports("p41219\n").is_empty());
    }

    #[test]
    fn parse_etime_handles_every_ps_shape() {
        assert_eq!(parse_etime(" 14:15"), Some(Duration::from_secs(855)));
        assert_eq!(parse_etime("03:14:15"), Some(Duration::from_secs(11_655)));
        assert_eq!(
            parse_etime("02-07:15:24\n"),
            Some(Duration::from_secs(2 * 86_400 + 26_124))
        );
        assert_eq!(parse_etime(""), None);
        assert_eq!(parse_etime("?"), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parse_proc_net_tcp_keeps_only_listeners() {
        let text = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n   \
             0: 00000000:C383 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 4213762 1 0 0 10 0\n   \
             1: 0100007F:0FC8 0100007F:C384 01 00000000:00000000 00:00000000 00000000  1000        0 4213999 1 0 0 10 0\n";
        // 0xC383 = 50051, and the ESTABLISHED (01) row is not a listener.
        assert_eq!(parse_proc_net_tcp(text), vec![(50051, 4_213_762)]);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parse_proc_stat_starttime_survives_a_comm_full_of_parens() {
        let stat = "1234 (odd )name() ) S 1 1234 1234 0 -1 4194304 100 0 0 0 5 3 0 0 20 0 8 0 987654 123 456 789";
        assert_eq!(parse_proc_stat_starttime(stat), Some(987_654));
    }

    /// The guard is about conflicts, not multiplicity: an unbound port is simply available and
    /// nothing counts how many oxidant processes are already running.
    #[test]
    fn ensure_available_returns_on_a_free_port() {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe");
        let port = probe.local_addr().expect("addr").port();
        drop(probe);
        let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
        // Would `exit(1)` on a conflict, so reaching the next line is the assertion.
        ensure_available(addr, PortKind::SparkConnect);
        ensure_available(addr, PortKind::SparkConnect);
    }
}
