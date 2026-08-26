//! Daemon control, driven through the real `oxidant` binary.
//!
//! `oxidant start | stop | status | restart` is the only way a human runs a long-lived engine
//! now, so these are the things it has to get right or the founder's laptop grows another nine
//! orphaned processes:
//!
//! * the round trip — start, `status` reports the right ports, a second `start` is idempotent
//!   and names the first pid, `stop`, `status` reports stopped;
//! * a bare `spark server` refuses, and `--foreground` still serves;
//! * a stale pidfile from a `SIGKILL`ed daemon never blocks the next `start` — this is the
//!   systemd-restart-safety pin, and the AMI depends on it;
//! * a pidfile pointing at a live process that is *not* ours is never signalled.
//!
//! Every spawned server gets its own `OXIDANT_DATA_DIR` tempdir. Not tidiness: the durable
//! history journal takes an exclusive lock on its data dir (PR #139), so two test servers
//! sharing the default make the second exit 1 for a reason that has nothing to do with daemons.
//! It is also what makes the pidfile per-root, so these tests can run concurrently at all.
//!
//! Lives in `oxidant-cli` so Cargo sets `CARGO_BIN_EXE_oxidant` when the test binary is built.

use std::net::TcpListener;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

mod common;
use common::oxidant_bin;

/// A port nobody is listening on *yet*. Racy against the rest of the machine by nature, but it
/// is what every other CLI test here uses and the ephemeral range is wide.
fn pick_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local_addr")
        .port()
}

/// One `oxidant` invocation against a private data dir, run to completion.
///
/// Everything below is a subcommand that exits on its own; nothing here spawns a server
/// directly. `start` does that, and cleaning it up is [`Daemon::drop`]'s job.
struct Run {
    out: Output,
}

impl Run {
    fn stdout(&self) -> String {
        String::from_utf8_lossy(&self.out.stdout).into_owned()
    }
    fn stderr(&self) -> String {
        String::from_utf8_lossy(&self.out.stderr).into_owned()
    }
    fn code(&self) -> i32 {
        self.out.status.code().unwrap_or(-1)
    }
    /// Both streams, for assertion failure messages — a refusal on the wrong one is still a bug
    /// worth seeing rather than an empty diff.
    fn all(&self) -> String {
        format!("stdout:\n{}stderr:\n{}", self.stdout(), self.stderr())
    }
}

/// A daemon under test: its private data dir, its ports, and the guarantee that it is gone when
/// the test ends however the test ends.
struct Daemon {
    data_dir: tempfile::TempDir,
    port: u16,
    ui_port: u16,
    /// `--ui-bind`. Loopback by default so a conflict collides on the *same* address; a test
    /// that cares what the UI is bound to sets it before starting.
    ui_bind: String,
}

impl Daemon {
    fn new() -> Self {
        Daemon {
            data_dir: common::data_dir(),
            port: pick_port(),
            ui_port: pick_port(),
            ui_bind: "127.0.0.1".to_string(),
        }
    }

    /// Re-draw both ports. `pick_port` only promises a port was free a moment ago, and under a
    /// full `cargo test --workspace` something else on the machine can take it in between —
    /// which shows up as a *port guard* refusal, not a daemon bug. `started_ok` retries through
    /// that; the same three-attempt shape `cli_rolling_logs::spawn_with_retry` uses.
    fn repick(&mut self) {
        self.port = pick_port();
        self.ui_port = pick_port();
    }

    fn run(&self, args: &[&str]) -> Run {
        let out = Command::new(oxidant_bin())
            .env("OXIDANT_DATA_DIR", self.data_dir.path())
            .args(args)
            .stdin(Stdio::null())
            .output()
            .expect("run oxidant");
        Run { out }
    }

    /// The flags every `start`/`restart` in this file uses. Loopback for the UI so a conflict
    /// collides on the *same* address — macOS lets `0.0.0.0:P` and `127.0.0.1:P` coexist.
    fn server_flags(&self) -> Vec<String> {
        vec![
            "--port".into(),
            self.port.to_string(),
            "--ui-port".into(),
            self.ui_port.to_string(),
            "--ui-bind".into(),
            self.ui_bind.clone(),
        ]
    }

    fn start(&self) -> Run {
        let mut args = vec!["start".to_string()];
        args.extend(self.server_flags());
        self.run(&args.iter().map(String::as_str).collect::<Vec<_>>())
    }

    /// `start`, retried past a port another process grabbed between `pick_port` and the bind.
    ///
    /// Only a port conflict is retried, and it is identified by the guard's own words. Any other
    /// non-zero exit is a real failure and is reported with both streams — an assertion here
    /// with no message is one nobody can diagnose from a CI log.
    fn started_ok(&mut self) -> Run {
        for attempt in 0..3 {
            let run = self.start();
            if run.code() == 0 {
                return run;
            }
            let stole_the_port = run.stderr().contains("is already in use")
                || run
                    .stderr()
                    .contains("is already held by another oxidant process");
            assert!(
                stole_the_port,
                "start failed for a reason that is not a port conflict:\n{}",
                run.all()
            );
            eprintln!("attempt {attempt}: something took our ephemeral port, re-drawing");
            self.repick();
        }
        panic!("could not hold an ephemeral port pair for three attempts");
    }

    fn pid_path(&self) -> std::path::PathBuf {
        self.data_dir.path().join("run/oxidant.pid")
    }

    fn pid(&self) -> u32 {
        let raw = std::fs::read_to_string(self.pid_path()).expect("read pidfile");
        let doc: serde_json::Value = serde_json::from_str(&raw).expect("pidfile is json");
        doc["pid"].as_u64().expect("pid field") as u32
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        // `stop` is the supported path and it verifies identity, so it can never kill a
        // bystander even if the test left the pidfile pointing somewhere strange.
        let _ = self.run(&["stop"]);
    }
}

/// Is `pid` still around? `kill -0`, without linking libc into the test binary.
fn alive(pid: u32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn wait_until_gone(pid: u32, within: Duration) {
    let deadline = Instant::now() + within;
    while alive(pid) {
        assert!(
            Instant::now() < deadline,
            "pid {pid} was still alive after {within:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

// -------------------------------------------------------------------------------------------
// The round trip
// -------------------------------------------------------------------------------------------

/// The whole contract in one pass: start, observe, start again, stop, observe.
///
/// The exit codes are asserted alongside the text because `status` is meant to be scriptable —
/// 0 running, 3 stopped — and a message that reads right with the wrong code is a silent
/// failure in every `if oxidant status; then` anyone writes.
#[test]
fn start_status_start_again_stop_status() {
    let mut d = Daemon::new();

    let started = d.started_ok();
    assert!(
        started.stdout().contains("oxidant started (pid "),
        "{}",
        started.all()
    );
    let pid = d.pid();
    assert!(alive(pid), "start returned but pid {pid} is not running");

    let status = d.run(&["status"]);
    assert_eq!(status.code(), 0, "status: {}", status.all());
    let text = status.stdout();
    assert!(text.contains("oxidant is running"), "{text}");
    assert!(text.contains(&format!("pid:            {pid}")), "{text}");
    // The ports it actually got, not the defaults — the pidfile is what `status` reads, and a
    // `status` that reported 50051/4040 for a server on ephemeral ports would be useless.
    assert!(
        text.contains(&format!("sc://0.0.0.0:{}", d.port)),
        "the spark connect port: {text}"
    );
    assert!(
        text.contains(&format!("http://127.0.0.1:{}", d.ui_port)),
        "the ui port: {text}"
    );
    // The health probe reached the running engine, not just the pidfile.
    assert!(text.contains("health:         ok ("), "{text}");
    assert!(text.contains("run/oxidant.log"), "the log path: {text}");

    // Idempotent: a second start names the first and spawns nothing.
    let again = d.start();
    assert_eq!(again.code(), 0, "second start: {}", again.all());
    assert!(
        again
            .stdout()
            .contains(&format!("oxidant is already running (pid {pid}, since ")),
        "the second start must name the first pid and when it started: {}",
        again.all()
    );
    assert_eq!(d.pid(), pid, "a second daemon was started behind our back");

    let stopped = d.run(&["stop"]);
    assert_eq!(stopped.code(), 0, "stop: {}", stopped.all());
    assert!(
        stopped
            .stdout()
            .contains(&format!("oxidant stopped (pid {pid})")),
        "{}",
        stopped.all()
    );
    wait_until_gone(pid, Duration::from_secs(10));
    assert!(!d.pid_path().exists(), "stop left the pidfile behind");

    let after = d.run(&["status"]);
    assert_eq!(
        after.code(),
        3,
        "a stopped daemon must exit 3 for scripts: {}",
        after.all()
    );
    assert!(
        after.stdout().contains("oxidant is not running"),
        "{}",
        after.all()
    );
}

/// Stopping nothing is a success, not an error. A teardown script that has to write `|| true`
/// around `oxidant stop` is a script that will also swallow the failures that matter.
#[test]
fn stopping_a_daemon_that_is_not_running_is_clean() {
    let d = Daemon::new();
    let stopped = d.run(&["stop"]);
    assert_eq!(stopped.code(), 0, "{}", stopped.all());
    assert!(
        stopped.stdout().contains("oxidant is not running"),
        "{}",
        stopped.all()
    );
}

/// `restart` replays the flags the daemon was started with — a bare `restart` must come back on
/// the same ports, which is the only thing that makes it usable in an ops runbook.
#[test]
fn restart_comes_back_on_the_same_ports_with_a_new_pid() {
    let mut d = Daemon::new();
    d.started_ok();
    let first = d.pid();

    let restarted = d.run(&["restart"]);
    assert_eq!(restarted.code(), 0, "restart: {}", restarted.all());
    let second = d.pid();
    assert_ne!(second, first, "restart reused the old pid");
    wait_until_gone(first, Duration::from_secs(10));

    let status = d.run(&["status"]);
    assert_eq!(status.code(), 0, "{}", status.all());
    assert!(
        status
            .stdout()
            .contains(&format!("sc://0.0.0.0:{}", d.port)),
        "restart moved the server off its port: {}",
        status.all()
    );
}

/// **The orphaned-engine pin.** Two concurrent `start`s against one data root must never leave
/// a live server the pidfile does not name.
///
/// The reproduced incident: both `start`s read the pidfile, both saw "stopped", both spawned.
/// The loser's pidfile write lost the rename race, returned through a bare `?` — and left a
/// detached Spark Connect server holding its ports with `status` reporting "not running" and
/// `stop` reporting "nothing to stop". `lsof` archaeology, restored.
///
/// Distinct ports on purpose: with a shared port the port guard would arbitrate and the daemon
/// bookkeeping would never be tested. The assertion is a conservation law — the set of live
/// `spark server` processes on our two ports is exactly what the pidfile claims, 0 or 1, never
/// a mismatch.
#[test]
fn two_concurrent_starts_never_leave_an_engine_the_pidfile_does_not_name() {
    let dir = common::data_dir();
    let ports = [pick_port(), pick_port()];

    let children: Vec<Child> = ports
        .iter()
        .map(|port| {
            Command::new(oxidant_bin())
                .env("OXIDANT_DATA_DIR", dir.path())
                .args(["start", "--port", &port.to_string(), "--no-ui"])
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("spawn oxidant start")
        })
        .collect();
    let outs: Vec<Output> = children
        .into_iter()
        .map(|c| c.wait_with_output().expect("wait for start"))
        .collect();
    let transcript = outs
        .iter()
        .enumerate()
        .map(|(i, o)| {
            format!(
                "start {i} (port {}) exit={:?}\nstdout:\n{}stderr:\n{}",
                ports[i],
                o.status.code(),
                String::from_utf8_lossy(&o.stdout),
                String::from_utf8_lossy(&o.stderr)
            )
        })
        .collect::<String>();

    let pid_path = dir.path().join("run/oxidant.pid");
    let claimed: Option<u32> = std::fs::read_to_string(&pid_path)
        .ok()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
        .and_then(|doc| doc["pid"].as_u64())
        .map(|p| p as u32);
    let live = servers_on(&ports);

    // Clean up before asserting: a leak is exactly the case where the assertion fires, and a
    // panic here would leave the orphan behind for the rest of the suite to trip over.
    for pid in &live {
        if Some(*pid) != claimed {
            let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
        }
    }
    let _ = Command::new(oxidant_bin())
        .env("OXIDANT_DATA_DIR", dir.path())
        .arg("stop")
        .output();

    let expected: Vec<u32> = claimed.into_iter().collect();
    assert_eq!(
        live, expected,
        "live `spark server` processes on ports {ports:?} do not match the pidfile \
         (pidfile: {claimed:?}) — a start leaked a detached engine\n{transcript}"
    );
}

/// The pids of every live `oxidant spark server` started on one of `ports`, ascending.
///
/// Read from the machine's process table rather than from the ports themselves: a leaked engine
/// that has not finished binding yet is still a leaked engine.
fn servers_on(ports: &[u16]) -> Vec<u32> {
    let out = Command::new("ps")
        .args(["-Ao", "pid=,args="])
        .output()
        .expect("ps -Ao pid=,args=");
    let mut found: Vec<u32> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|line| {
            let mut it = line.split_whitespace();
            let pid: u32 = it.next()?.parse().ok()?;
            let command: Vec<&str> = it.collect();
            if !command.contains(&"server") {
                return None;
            }
            let names_our_port = command
                .windows(2)
                .any(|w| w[0] == "--port" && w[1].parse::<u16>().is_ok_and(|p| ports.contains(&p)));
            names_our_port.then_some(pid)
        })
        .collect();
    found.sort_unstable();
    found
}

/// `oxidant restart --timeout <n>` restarts. It used to stop the daemon and then fail to start
/// it, leaving the operator with nothing running.
///
/// `--timeout` is `stop`'s flag, and `restart` parses it by handing its argv to `run_stop` — but
/// the replay set treated *any* typed argument as a wholesale override of the recorded server
/// flags. So `--timeout 30` discarded `--port`/`--ui-port` and was itself replayed to
/// `oxidant spark server --timeout 30 --foreground`. On a machine where the defaults happened to
/// be free it was worse than a failure: a silent move of a production server to 50051.
#[test]
fn restart_with_a_stop_timeout_keeps_the_server_on_its_own_ports() {
    let mut d = Daemon::new();
    d.started_ok();
    let first = d.pid();

    let restarted = d.run(&["restart", "--timeout", "20"]);
    assert_eq!(
        restarted.code(),
        0,
        "restart --timeout: {}",
        restarted.all()
    );
    let second = d.pid();
    assert_ne!(second, first, "restart reused the old pid");
    wait_until_gone(first, Duration::from_secs(10));

    let status = d.run(&["status"]);
    assert_eq!(status.code(), 0, "{}", status.all());
    let text = status.stdout();
    assert!(
        text.contains(&format!("sc://0.0.0.0:{}", d.port)),
        "restart --timeout moved the server off its port: {text}"
    );
    assert!(
        text.contains(&format!("http://127.0.0.1:{}", d.ui_port)),
        "restart --timeout moved the UI off its port: {text}"
    );
    // And the flag was never handed to the server it started.
    assert!(
        !text.contains("--timeout"),
        "`--timeout` is stop's flag and was replayed to the server: {text}"
    );
}

/// A `--timeout` too large to add to an `Instant` must be refused, not panicked on — and above
/// all not panicked on *after* the daemon has already been SIGTERMed.
///
/// The reproduced shape: `oxidant stop --timeout 18446744073709551615` delivered SIGTERM, then
/// panicked in `wait_for_exit` with "overflow when adding duration to instant" and exit 101.
/// The daemon was dead, the pidfile intact, and neither a success nor a failure line was
/// printed. The order of operations is what makes this a data-loss bug rather than a cosmetic
/// one: the refusal has to land before the signal.
#[test]
fn a_stop_timeout_that_cannot_be_added_to_an_instant_is_refused_before_sigterm() {
    let mut d = Daemon::new();
    d.started_ok();
    let pid = d.pid();

    let refused = d.run(&["stop", "--timeout", &u64::MAX.to_string()]);
    assert_eq!(
        refused.code(),
        1,
        "an out-of-range --timeout must be a refusal, not a panic (101): {}",
        refused.all()
    );
    assert!(
        refused.stderr().contains("invalid --timeout"),
        "{}",
        refused.all()
    );
    assert!(!refused.stderr().contains("panicked"), "{}", refused.all());
    // The load-bearing half: nothing was signalled on the way to that refusal.
    assert!(
        alive(pid),
        "the daemon was SIGTERMed before the --timeout was validated"
    );
    let status = d.run(&["status"]);
    assert_eq!(status.code(), 0, "{}", status.all());
}

/// `restart` is not atomic — it stops first — so a restart that *cannot* start must refuse
/// before it stops anything. Otherwise the operator is left with nothing running, and on a
/// crashed-node runbook that is the whole engine gone for the sake of a typo.
#[test]
fn a_restart_onto_a_taken_port_leaves_the_running_daemon_alone() {
    let mut d = Daemon::new();
    d.started_ok();
    let running = d.pid();

    // Bound on every interface, because that is what the server binds: macOS is happy to let
    // `0.0.0.0:P` and `127.0.0.1:P` coexist, so a loopback blocker would not collide at all.
    let blocker = TcpListener::bind("0.0.0.0:0").expect("bind a blocker");
    let taken = blocker.local_addr().expect("local_addr").port();

    let refused = d.run(&["restart", "--port", &taken.to_string()]);
    assert_eq!(
        refused.code(),
        1,
        "a restart onto a taken port must fail: {}",
        refused.all()
    );
    assert!(
        refused.stderr().contains("is already in use")
            || refused
                .stderr()
                .contains("is already held by another oxidant process"),
        "{}",
        refused.all()
    );
    // The load-bearing assertion: the daemon that was running still is.
    assert!(
        alive(running),
        "restart stopped the daemon it could not restart"
    );
    assert_eq!(d.pid(), running, "the pidfile no longer names it either");
    let status = d.run(&["status"]);
    assert_eq!(
        status.code(),
        0,
        "the daemon must still be healthy on its original ports: {}",
        status.all()
    );
    drop(blocker);
}

/// Moving one port must not reset the others — the same wholesale-override defect, in the form
/// an operator meets it on purpose.
#[test]
fn restart_with_a_new_ui_port_keeps_the_recorded_connect_port() {
    let mut d = Daemon::new();
    d.started_ok();
    let moved = pick_port();

    let restarted = d.run(&["restart", "--ui-port", &moved.to_string()]);
    assert_eq!(
        restarted.code(),
        0,
        "restart --ui-port: {}",
        restarted.all()
    );

    let status = d.run(&["status"]);
    assert_eq!(status.code(), 0, "{}", status.all());
    let text = status.stdout();
    assert!(
        text.contains(&format!("sc://0.0.0.0:{}", d.port)),
        "moving the UI moved the connect port too: {text}"
    );
    assert!(
        text.contains(&format!(":{moved}")),
        "the UI did not move to the port that was asked for: {text}"
    );
    d.ui_port = moved;
}

/// **The crashed-node pin.** `restart` after a SIGKILL must come back on the ports the dead
/// daemon held, not on the defaults.
///
/// A crashed node is the single most likely time anyone runs `oxidant restart`, and it used to
/// be the one time the flags were lost: `restart` asked `daemon::running()` for them, and
/// `running()` deletes the pidfile of a process that is gone before returning "stopped". The
/// flags were on disk a moment earlier and were thrown away by the read meant to recover them,
/// so the server silently came back on 50051/4040 — reporting itself healthy while every client
/// pointed at the old port was broken.
#[test]
fn restart_after_a_crash_replays_the_dead_daemons_ports() {
    let mut d = Daemon::new();
    d.started_ok();
    let killed = d.pid();

    assert!(
        Command::new("kill")
            .args(["-9", &killed.to_string()])
            .status()
            .expect("kill -9")
            .success(),
        "could not SIGKILL pid {killed}"
    );
    wait_until_gone(killed, Duration::from_secs(10));

    // No flags: everything `restart` knows now comes from the pidfile the corpse left behind.
    let restarted = d.run(&["restart"]);
    assert_eq!(restarted.code(), 0, "restart: {}", restarted.all());
    assert_ne!(d.pid(), killed, "the pidfile still names the dead process");

    let status = d.run(&["status"]);
    assert_eq!(status.code(), 0, "{}", status.all());
    assert!(
        status
            .stdout()
            .contains(&format!("sc://0.0.0.0:{}", d.port)),
        "restart moved the crashed server off its port: {}",
        status.all()
    );
    assert!(
        status
            .stdout()
            .contains(&format!("http://127.0.0.1:{}", d.ui_port)),
        "restart moved the crashed server's UI off its port: {}",
        status.all()
    );
}

/// `start` and `status` must name the interface the UI is actually bound to.
///
/// `--ui-bind` defaults to `0.0.0.0`, and both commands printed the *probe* URL — the one
/// rewritten to loopback because `0.0.0.0` is not a destination address. So a UI reachable from
/// the whole network was reported as `http://127.0.0.1:4451`, one line under an honest
/// `sc://0.0.0.0:50451`. `docs/web-ui.md` warns the UI has no auth and to bind loopback on
/// reachable hosts; this line told the operator they had.
#[test]
fn start_and_status_name_the_interface_the_ui_is_bound_to() {
    let mut d = Daemon::new();
    d.ui_bind = "0.0.0.0".to_string();
    let started = d.started_ok();
    let wildcard = format!("http://0.0.0.0:{}", d.ui_port);
    let loopback = format!("http://127.0.0.1:{}", d.ui_port);

    for (what, text) in [
        ("start", started.stdout()),
        ("status", d.run(&["status"]).stdout()),
    ] {
        let ui = text
            .lines()
            .find(|l| l.trim_start().starts_with("ui + rest:"))
            .unwrap_or_else(|| panic!("{what} printed no ui line:\n{text}"))
            .to_string();
        assert!(
            ui.contains(&wildcard),
            "{what} must name the address that was bound: {ui}"
        );
        // The loopback URL is still there — as the way to reach it locally, not as the claim
        // about what is exposed.
        assert!(
            ui.contains(&loopback) && ui.contains("all interfaces"),
            "{what} must not drop the local URL: {ui}"
        );
        assert!(
            !ui.trim()
                .starts_with(&format!("ui + rest:      {loopback}")),
            "{what} still reports a wildcard bind as loopback-only: {ui}"
        );
    }
}

// -------------------------------------------------------------------------------------------
// The bare-invocation refusal
// -------------------------------------------------------------------------------------------

/// A bare `spark server` is refused, fast, and the message says both ways out.
///
/// The speed is half the assertion: without the guard this process would sit in `serve` forever
/// rather than exit, so `output()` returning at all is the test.
#[test]
fn a_bare_spark_server_refuses_and_points_at_the_daemon() {
    let d = Daemon::new();
    let refused = d.run(&["spark", "server", "--port", &d.port.to_string(), "--no-ui"]);
    assert_eq!(refused.code(), 1, "{}", refused.all());
    let err = refused.stderr();
    assert!(
        err.contains("runs a long-lived process, and those run as daemons"),
        "{err}"
    );
    assert!(err.contains("oxidant start"), "{err}");
    assert!(err.contains("--foreground"), "{err}");
    // Refused *before* any of the boot work: a banner followed by an error is the confusing
    // shape this whole rule exists to replace.
    assert!(!err.contains("listening on"), "{err}");
    assert!(!d.pid_path().exists(), "a refusal must not write a pidfile");
}

/// The same rule for a worker — and its refusal must not promise an `oxidant start worker`
/// subcommand, because there is none. Workers are started by their supervisor.
#[test]
fn a_bare_worker_refuses_without_inventing_a_start_subcommand() {
    let d = Daemon::new();
    let refused = d.run(&["worker", "--port", &d.port.to_string()]);
    assert_eq!(refused.code(), 1, "{}", refused.all());
    let err = refused.stderr();
    assert!(
        err.contains("`oxidant worker` runs a long-lived process"),
        "{err}"
    );
    assert!(err.contains("oxidant worker … --foreground"), "{err}");
    assert!(!err.contains("oxidant start"), "{err}");
}

/// The escape hatch still works: `--foreground` serves in the foreground, exactly as the
/// systemd units and the rest of this test suite need it to.
///
/// This is the same shape the migrated tests use, restated here so the *reason* they were
/// changed has its own failing test if the flag ever stops being honoured.
#[test]
fn foreground_still_serves() {
    let d = Daemon::new();
    let child = Command::new(oxidant_bin())
        .env("OXIDANT_DATA_DIR", d.data_dir.path())
        .args([
            "spark",
            "server",
            "--foreground",
            "--port",
            &d.port.to_string(),
            "--ui-port",
            &d.ui_port.to_string(),
            "--ui-bind",
            "127.0.0.1",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn foreground server");
    struct Kill(Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let url = format!("http://127.0.0.1:{}/api/v1/cluster/status", d.ui_port);
    let mut guard = Kill(child);
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        if let Some(status) = guard.0.try_wait().expect("try_wait") {
            panic!("--foreground server exited with {status}");
        }
        if reqwest::blocking::get(&url)
            .map(|r| r.status() == 200)
            .unwrap_or(false)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "--foreground server never came up"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    // And it wrote no pidfile: a foreground process is its supervisor's to track, not ours.
    assert!(
        !d.pid_path().exists(),
        "--foreground must not write a daemon pidfile"
    );
}

/// `--no-ui` has no HTTP surface, so `start` has nothing to poll and `status` has nothing to
/// probe. Both must fall back to the gRPC listener rather than wait out the full start timeout
/// and then report a healthy server as unreachable.
///
/// Not hypothetical: `docs/catalogs-unity.md` starts the engine exactly this way.
#[test]
fn start_and_status_work_without_a_ui_port() {
    let d = Daemon::new();
    let started = d.run(&["start", "--port", &d.port.to_string(), "--no-ui"]);
    assert_eq!(started.code(), 0, "--no-ui start: {}", started.all());
    assert!(
        started
            .stdout()
            .contains("ui + rest:      disabled (--no-ui)"),
        "{}",
        started.all()
    );

    let status = d.run(&["status"]);
    assert_eq!(status.code(), 0, "--no-ui status: {}", status.all());
    assert!(
        status
            .stdout()
            .contains("health:         ok (grpc connect; no ui to probe)"),
        "the health line must say what it actually checked: {}",
        status.all()
    );
}

// -------------------------------------------------------------------------------------------
// Stale and stranger pidfiles
// -------------------------------------------------------------------------------------------

/// **The systemd restart-safety pin.** `SIGKILL` the daemon — no chance to clean up, exactly
/// what an OOM kill or a lost machine does — and the next `start` must succeed.
///
/// The AMI's `Restart=on-failure` and every reboot depend on this. A pidfile that outlives its
/// process and blocks the next start is a node that never comes back.
#[test]
fn a_stale_pidfile_from_a_sigkilled_daemon_does_not_block_a_start() {
    let mut d = Daemon::new();
    d.started_ok();
    let killed = d.pid();

    assert!(
        Command::new("kill")
            .args(["-9", &killed.to_string()])
            .status()
            .expect("kill -9")
            .success(),
        "could not SIGKILL pid {killed}"
    );
    wait_until_gone(killed, Duration::from_secs(10));
    // The pidfile survived the kill — that is the whole point of the test.
    assert!(
        d.pid_path().exists(),
        "SIGKILL should have left the pidfile behind; nothing to prove otherwise"
    );

    let restarted = d.start();
    assert_eq!(
        restarted.code(),
        0,
        "a stale pidfile blocked the restart: {}",
        restarted.all()
    );
    assert!(
        restarted.stdout().contains("oxidant started (pid "),
        "{}",
        restarted.all()
    );
    assert_ne!(d.pid(), killed, "the pidfile still names the dead process");
}

/// A pidfile pointing at a live process that is not ours is never signalled.
///
/// Written by hand against the test's own `sleep` child — the realistic version of this is a
/// pid the kernel recycled after the daemon died, and there is no way to arrange that on demand.
/// The decision is the same one either way: liveness alone says nothing, so `stop` compares the
/// recorded executable and start token against the live process and refuses on a mismatch.
///
/// The load-bearing assertion is the last one. Everything before it is about the message.
#[test]
fn stop_refuses_to_kill_a_pid_that_is_not_our_daemon() {
    let d = Daemon::new();
    let mut bystander = Command::new("sleep")
        .arg("300")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sleep");
    let stranger = bystander.id();

    // Its real start token, so the *only* thing that differs is the executable. A test that
    // also got the token wrong would pass without proving the exe check runs at all.
    let lstart = String::from_utf8_lossy(
        &Command::new("ps")
            .args(["-p", &stranger.to_string(), "-o", "lstart="])
            .output()
            .expect("ps lstart")
            .stdout,
    )
    .trim()
    .to_string();
    std::fs::create_dir_all(d.data_dir.path().join("run")).expect("mkdir run");
    std::fs::write(
        d.pid_path(),
        serde_json::to_vec_pretty(&serde_json::json!({
            "pid": stranger,
            "exe": "/usr/local/bin/oxidant",
            "start_token": lstart,
            "started_at": "2026-08-25T00:00:00Z",
            "port": d.port,
            "ui_port": d.ui_port,
            "ui_bind": "127.0.0.1",
            "args": [],
            "log": d.data_dir.path().join("run/oxidant.log").to_string_lossy(),
        }))
        .expect("encode pidfile"),
    )
    .expect("write pidfile");

    let refused = d.run(&["stop"]);
    assert_eq!(refused.code(), 1, "{}", refused.all());
    let err = refused.stderr();
    assert!(
        err.contains(&format!(
            "error: pid {stranger} is alive but is not this oxidant daemon"
        )),
        "{err}"
    );
    assert!(err.contains("a recycled pid is never killed"), "{err}");
    assert!(err.contains("recorded /usr/local/bin/oxidant"), "{err}");
    // The pidfile is evidence, not litter: clearing it would hide that something wrote a pid
    // we did not, and the operator needs to see it to decide whether the daemon is really gone.
    assert!(d.pid_path().exists(), "the pidfile must be left in place");

    assert!(
        alive(stranger),
        "oxidant stop killed a process that was not its daemon"
    );
    let _ = bystander.kill();
    let _ = bystander.wait();
    // Nothing for `Daemon::drop` to stop, and it must not try: remove the trap we planted.
    let _ = std::fs::remove_file(d.pid_path());
}

/// A daemon that has died but not yet been reaped reads as **stopped**, not as a stranger.
///
/// A zombie answers `kill(pid, 0)` and `ps -o lstart=` still returns its real start token, so
/// the only thing that says it is dead is `ps -o args=` — which prints `<defunct>`, not the
/// empty string the identity guard was checking for. So `identity()` handed back
/// `exe: "<defunct>"`, the executable comparison failed, and a corpse was classified as a live
/// process that had hijacked our pid: exit 1, and `stop` refusing to clean up after a process
/// that no longer exists.
#[test]
fn a_dead_but_unreaped_daemon_reads_as_stopped_not_as_a_stranger() {
    let d = Daemon::new();
    // `sleep 0` exits at once; `exec` replaces the shell, so the zombie's parent is a `sleep`
    // that never calls wait() and the corpse persists for the whole test.
    let mut parent = Command::new("sh")
        .arg("-c")
        .arg("sleep 0 & echo $!; exec sleep 300")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn the zombie's parent");
    let zombie: u32 = {
        use std::io::Read;
        let mut buf = [0u8; 32];
        let n = parent
            .stdout
            .as_mut()
            .expect("stdout")
            .read(&mut buf)
            .expect("read the child pid");
        String::from_utf8_lossy(&buf[..n])
            .trim()
            .parse()
            .expect("a pid")
    };

    let is_defunct = |pid: u32| {
        let out = Command::new("ps")
            .args(["-p", &pid.to_string(), "-o", "args="])
            .output()
            .expect("ps args=");
        String::from_utf8_lossy(&out.stdout).contains("<defunct>")
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while !is_defunct(zombie) {
        assert!(
            Instant::now() < deadline,
            "pid {zombie} never became a zombie"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    // The trap that made this a stranger: the corpse's start token is perfectly readable.
    let lstart = String::from_utf8_lossy(
        &Command::new("ps")
            .args(["-p", &zombie.to_string(), "-o", "lstart="])
            .output()
            .expect("ps lstart")
            .stdout,
    )
    .trim()
    .to_string();
    assert!(
        !lstart.is_empty(),
        "a zombie's lstart is readable; that is the point"
    );

    std::fs::create_dir_all(d.data_dir.path().join("run")).expect("mkdir run");
    std::fs::write(
        d.pid_path(),
        serde_json::to_vec(&serde_json::json!({
            "pid": zombie,
            "exe": "/usr/local/bin/oxidant",
            "start_token": lstart,
            "started_at": "2026-08-25T00:00:00Z",
            "port": d.port,
            "ui_port": d.ui_port,
            "ui_bind": "127.0.0.1",
            "args": [],
            "log": "/tmp/oxidant.log",
        }))
        .expect("encode pidfile"),
    )
    .expect("write pidfile");

    let status = d.run(&["status"]);
    let stopped = d.run(&["stop"]);
    let _ = parent.kill();
    let _ = parent.wait();

    assert_eq!(
        status.code(),
        3,
        "a dead daemon is stopped, not a hijacked pid: {}",
        status.all()
    );
    assert!(
        !status.stderr().contains("is not this oxidant daemon"),
        "{}",
        status.all()
    );
    // And `stop` cleans up after it instead of refusing to.
    assert_eq!(stopped.code(), 0, "stop: {}", stopped.all());
    assert!(
        !d.pid_path().exists(),
        "the stale pidfile of a reaped daemon must be dropped"
    );
}

/// `status` on the same stranger pidfile: neither "running" nor "stopped", because a script
/// that read it as either would go on to do the wrong thing.
#[test]
fn status_refuses_a_stranger_pidfile_rather_than_calling_it_stopped() {
    let d = Daemon::new();
    let mut bystander = Command::new("sleep")
        .arg("300")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sleep");
    std::fs::create_dir_all(d.data_dir.path().join("run")).expect("mkdir run");
    std::fs::write(
        d.pid_path(),
        serde_json::to_vec(&serde_json::json!({
            "pid": bystander.id(),
            "exe": "/usr/local/bin/oxidant",
            "start_token": "a-token-that-does-not-match",
            "started_at": "2026-08-25T00:00:00Z",
            "port": d.port,
            "ui_port": d.ui_port,
            "ui_bind": "127.0.0.1",
            "args": [],
            "log": "/tmp/oxidant.log",
        }))
        .expect("encode pidfile"),
    )
    .expect("write pidfile");

    let status = d.run(&["status"]);
    assert_ne!(
        status.code(),
        0,
        "a stranger pidfile is not healthy: {}",
        status.all()
    );
    assert_ne!(
        status.code(),
        3,
        "and it is not 'stopped' either — a script must not skip cleanup: {}",
        status.all()
    );
    assert!(alive(bystander.id()), "status must never signal anything");
    let _ = bystander.kill();
    let _ = bystander.wait();
    let _ = std::fs::remove_file(d.pid_path());
}
