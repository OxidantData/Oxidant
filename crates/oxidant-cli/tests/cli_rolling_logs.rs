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
fn pick_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local_addr")
        .port()
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

    let body = wait_for_log(root.path(), &mut worker, "the worker");
    let _ = worker.kill();
    let _ = worker.wait();

    assert_timestamped(&body, "the worker");
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
    assert!(!existed, "OXIDANT_LOG_ROLL=off must write no {}", live.display());
}
