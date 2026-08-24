//! `oxidant driver` **and** `oxidant worker` write a durable rolling exec log
//! (`docs/query-history-durability.md` §6c).
//!
//! This is a subprocess test rather than a unit test on purpose. The bug it guards is a *wiring*
//! bug: `init_logging()` used to be the only `tracing_subscriber` init in the tree and it was
//! called from `rest::router`, which a standalone `oxidant worker --port …` never builds. The
//! worker therefore installed no subscriber at all, and worker OOMs are exactly what operators
//! dig for. Nothing short of running the real binary can tell you whether the init is reachable
//! from both entry points.
//!
//! Every node writes its own `logs/` under its own root (§3c); collection stays per-node and the
//! driver federates *reads* over worker logs in PR4 rather than ingesting them here.

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

mod common;
use common::oxidant_bin;

/// An ephemeral loopback port. The fixed-port schemes elsewhere in this workspace collide when
/// two `cargo test` runs overlap, and on this project's Macs `rapportd` squats on 50603/50604.
///
/// Binding `:0` and dropping the listener leaves a TOCTOU window before the child binds it. It is
/// small and strictly better than a fixed port, but it is not nothing, so the callers that can
/// detect a lost race retry — see [`spawn_with_retry`].
fn pick_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local_addr")
        .port()
}

/// Spawn `build(port)` on a fresh ephemeral port, retrying if the child dies immediately — which
/// is what losing the `pick_port` race looks like from out here.
fn spawn_with_retry(build: impl Fn(u16) -> Command) -> (Child, u16) {
    for _ in 0..3 {
        let port = pick_port();
        let mut child = build(port).spawn().expect("spawn");
        std::thread::sleep(Duration::from_millis(200));
        match child.try_wait() {
            Ok(Some(_)) => continue, // exited already: almost certainly the port was taken
            _ => return (child, port),
        }
    }
    panic!("could not get an ephemeral port that stayed free for three attempts");
}

/// Wait for `logs/oxidant.log` under `root` to exist and carry a line, or give up.
fn wait_for_log(root: &std::path::Path, child: &mut Child, what: &str) -> String {
    let live = root.join("logs").join("oxidant.log");
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(body) = std::fs::read_to_string(&live) {
            if !body.trim().is_empty() {
                return body;
            }
        }
        if let Ok(Some(status)) = child.try_wait() {
            panic!("{what} exited before writing a log: {status}");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "{what} wrote no {} within 30s (dir listing: {:?})",
        live.display(),
        std::fs::read_dir(root.join("logs"))
            .map(|d| d.flatten().map(|e| e.file_name()).collect::<Vec<_>>())
            .unwrap_or_default()
    );
}

/// Every line the writer produces leads with an RFC-3339 UTC timestamp — the whole reason a
/// rolled log has a `ts` column for §6b's time-range filters to filter on.
fn assert_timestamped(body: &str, what: &str) {
    let first = body.lines().next().unwrap_or_default();
    let stamp = first.split(' ').next().unwrap_or_default();
    assert!(
        chrono::DateTime::parse_from_rfc3339(stamp).is_ok(),
        "{what}'s log lines must lead with an RFC-3339 UTC timestamp, got {first:?}"
    );
    assert!(
        stamp.ends_with('Z'),
        "{what}'s timestamps are UTC and carry no offset: {stamp:?}"
    );
}

/// A standalone `oxidant worker` builds no REST router. It must still get the subscriber, and it
/// must write into *its own* root.
#[test]
fn a_standalone_worker_writes_a_rolling_log() {
    let oxidant = oxidant_bin();
    let root = TempDir::new().expect("tempdir");
    let port = pick_port();
    let mut worker = Command::new(&oxidant)
        .args(["worker", "--port", &port.to_string()])
        .env("OXIDANT_DATA_DIR", root.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn worker");

    wait_for_log(root.path(), &mut worker, "the worker");
    // **Not just the init line.** `wait_for_log` returns as soon as the file is non-empty, so
    // asserting only on `rolling exec log open` — the line `init` itself emits — would still pass
    // if the `Capture` layer were installed and then detached (another crate winning `try_init`,
    // or the layer being dropped). A *later* event, emitted after the whole catalog bootstrap,
    // is what proves the layer is still attached.
    let live = root.path().join("logs").join("oxidant.log");
    let body = wait_for_text(&live, "oxidant worker listening on Flight", "the worker");
    let _ = worker.kill();
    let _ = worker.wait();

    assert_timestamped(&body, "the worker");
    assert!(
        body.lines().count() >= 2,
        "an event later than the init line must land too: {body}"
    );
    assert!(
        body.contains(r#"role="worker""#),
        "the worker's own log must say it is a worker: {body}"
    );
    assert!(
        body.contains("rolling exec log open"),
        "the writer announces itself so an operator can find the files: {body}"
    );
    assert!(
        root.path().join("logs").join("oxidant.log").is_file(),
        "and it is the live file, not a rolled one"
    );
}

/// The other half of §6c: the driver keeps the durable log it already had, now through the same
/// process-level init, and under its own root.
#[test]
fn the_driver_writes_a_rolling_log_under_its_own_root() {
    let oxidant = oxidant_bin();
    let root = TempDir::new().expect("tempdir");
    let port = pick_port();
    let mut server = Command::new(&oxidant)
        .args(["spark", "server", "--port", &port.to_string(), "--no-ui"])
        .env("OXIDANT_DATA_DIR", root.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn server");

    let body = wait_for_log(root.path(), &mut server, "the driver");
    let _ = server.kill();
    let _ = server.wait();

    assert_timestamped(&body, "the driver");
    assert!(
        body.contains(r#"role="driver""#),
        "the driver's own log must say it is a driver: {body}"
    );
    // The journal lives under the same root, so the two subsystems agree on where "here" is —
    // which is what `OXIDANT_DATA_DIR_PER_PROCESS` splitting on `<role>-<port>` depends on.
    assert!(
        root.path().join("history").join("statements").is_dir(),
        "logs and history share one root: {:?}",
        std::fs::read_dir(root.path())
            .map(|d| d.flatten().map(|e| e.file_name()).collect::<Vec<_>>())
            .unwrap_or_default()
    );
}

/// `OXIDANT_LOG_ROLL=off` writes nothing under `logs/` — the way out for an operator who wants
/// durable statement history and stderr-only logs.
#[test]
fn log_roll_off_writes_no_file() {
    let oxidant = oxidant_bin();
    let root = TempDir::new().expect("tempdir");
    let port = pick_port();
    let mut worker = Command::new(&oxidant)
        .args(["worker", "--port", &port.to_string()])
        .env("OXIDANT_DATA_DIR", root.path())
        .env("OXIDANT_LOG_ROLL", "off")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn worker");

    // No file to wait for, so wait for the port instead: once the worker accepts a connection,
    // its logging init has long since run. (Probing by *binding* the port does not work — the
    // worker listens on `0.0.0.0` and a second bind on `127.0.0.1` is accepted alongside it.)
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut listening = false;
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            listening = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let live = root.path().join("logs").join("oxidant.log");
    let existed = live.exists();
    let _ = worker.kill();
    let _ = worker.wait();

    assert!(listening, "the worker never came up");
    assert!(
        !existed,
        "OXIDANT_LOG_ROLL=off must write no {}",
        live.display()
    );
}

/// Wait for `path` to contain `needle`, or give up.
fn wait_for_text(path: &std::path::Path, needle: &str, what: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = String::new();
    while Instant::now() < deadline {
        if let Ok(body) = std::fs::read_to_string(path) {
            if body.contains(needle) {
                return body;
            }
            last = body;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "{what}: {} never contained {needle:?}\n{last}",
        path.display()
    );
}

/// **H3.** A worker takes no journal lock — it runs no statements — so with a driver and a
/// co-located worker on the *default* root (`OXIDANT_DATA_DIR_PER_PROCESS` is "the recommended
/// setting", not a required one) nothing stopped the two from opening the same
/// `logs/oxidant.log`.
///
/// What that costs: the driver's roll renames the live file and reopens a fresh one, the worker
/// keeps appending to the fd it still holds — now the *rolled* inode — and either process's
/// converter then finds that rolled file, converts it and unlinks the text. From that instant
/// every worker log line is written to a deleted inode, silently, until the worker's own roll
/// trigger fires. The worker's log is exactly what an operator digs through after an OOM.
///
/// Two entry points, one root: the second one refuses, loudly, and the first one's log is
/// untouched.
#[test]
fn a_worker_sharing_the_driver_s_root_refuses_to_open_a_second_log_writer() {
    let oxidant = oxidant_bin();
    let root = TempDir::new().expect("tempdir");

    let (mut server, _) = spawn_with_retry(|port| {
        let mut cmd = Command::new(&oxidant);
        cmd.args(["spark", "server", "--port", &port.to_string(), "--no-ui"])
            .env("OXIDANT_DATA_DIR", root.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        cmd
    });
    let driver_log = wait_for_log(root.path(), &mut server, "the driver");
    assert!(driver_log.contains(r#"role="driver""#), "{driver_log}");

    // The worker's refusal goes to stderr — it has no file to write it to, which is the point.
    let worker_err = root.path().join("worker.err");
    let (mut worker, _) = spawn_with_retry(|port| {
        let mut cmd = Command::new(&oxidant);
        cmd.args(["worker", "--port", &port.to_string()])
            .env("OXIDANT_DATA_DIR", root.path())
            .stdout(Stdio::null())
            .stderr(Stdio::from(
                std::fs::File::create(&worker_err).expect("create worker.err"),
            ));
        cmd
    });
    let refusal = wait_for_text(&worker_err, "exec log dir", "the worker");

    let after = std::fs::read_to_string(root.path().join("logs").join("oxidant.log"))
        .expect("the driver's live log");
    let _ = worker.kill();
    let _ = worker.wait();
    let _ = server.kill();
    let _ = server.wait();

    assert!(
        refusal.contains("in use by pid"),
        "the refusal must name the holder: {refusal}"
    );
    for way_out in [
        "OXIDANT_DATA_DIR_PER_PROCESS=1",
        "OXIDANT_LOG_DIR",
        "OXIDANT_LOG_ROLL=off",
    ] {
        assert!(
            refusal.contains(way_out),
            "the refusal must say how to fix it ({way_out}): {refusal}"
        );
    }
    assert!(
        !after.contains(r#"role="worker""#),
        "the worker must not have written into the driver's log: {after}"
    );
    assert!(
        after.starts_with(driver_log.lines().next().expect("a first line")),
        "and the driver's log is still its own: {after}"
    );
    // One writer, one lock, and no rolled generation invented by the loser.
    let logs: Vec<String> = std::fs::read_dir(root.path().join("logs"))
        .expect("logs dir")
        .flatten()
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.starts_with("oxidant"))
        .collect();
    assert_eq!(logs, vec!["oxidant.log".to_string()], "{logs:?}");
}

/// The other half of H3: `OXIDANT_DATA_DIR_PER_PROCESS=1` — the way out the refusal prints — puts
/// both processes' logs under their own `<role>-<port>/` tree, so a co-located pair keeps *two*
/// durable logs. §3c's "every node writes its own `logs/` under its own root" is now enforced in
/// both directions rather than merely documented.
#[test]
fn per_process_roots_give_a_co_located_driver_and_worker_two_logs() {
    let oxidant = oxidant_bin();
    let root = TempDir::new().expect("tempdir");

    let mut children: Vec<Child> = Vec::new();
    let mut roots: Vec<std::path::PathBuf> = Vec::new();
    for (role, args) in [
        ("driver", vec!["spark", "server", "--no-ui"]),
        ("worker", vec!["worker"]),
    ] {
        let (child, port) = spawn_with_retry(|port| {
            let mut cmd = Command::new(&oxidant);
            cmd.args(&args)
                .args(["--port", &port.to_string()])
                .env("OXIDANT_DATA_DIR", root.path())
                .env("OXIDANT_DATA_DIR_PER_PROCESS", "1")
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            cmd
        });
        children.push(child);
        roots.push(root.path().join(format!("{role}-{port}")));
    }

    let mut bodies = Vec::new();
    for (i, role) in ["driver", "worker"].iter().enumerate() {
        bodies.push(wait_for_log(&roots[i], &mut children[i], role));
    }
    for child in &mut children {
        let _ = child.kill();
        let _ = child.wait();
    }
    for (body, role) in bodies.iter().zip(["driver", "worker"]) {
        assert_timestamped(body, role);
        assert!(
            body.contains(&format!(r#"role="{role}""#)),
            "{role} wrote into the wrong tree: {body}"
        );
    }
    assert_ne!(
        roots[0], roots[1],
        "two roots, two logs, no shared live file"
    );
}
