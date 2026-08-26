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
}

impl Daemon {
    fn new() -> Self {
        Daemon {
            data_dir: common::data_dir(),
            port: pick_port(),
            ui_port: pick_port(),
        }
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
            "127.0.0.1".into(),
        ]
    }

    fn start(&self) -> Run {
        let mut args = vec!["start".to_string()];
        args.extend(self.server_flags());
        self.run(&args.iter().map(String::as_str).collect::<Vec<_>>())
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
    let d = Daemon::new();

    let started = d.start();
    assert_eq!(started.code(), 0, "start failed: {}", started.all());
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
    let d = Daemon::new();
    assert_eq!(d.start().code(), 0);
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
    let d = Daemon::new();
    assert_eq!(d.start().code(), 0);
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
