//! The port-conflict guard, driven through the real `oxidant` binary.
//!
//! The incident these pin down: leftover `oxidant` processes from earlier agent/test runs held
//! 50051 and 4040 for days, and starting a fresh server on those ports failed with a bare
//! "address in use" that never named the holder. So every assertion here is about *identifying*
//! the occupier — its PID, and the ports killing it would free.
//!
//! The other half is just as important: the guard must not object to several oxidants running
//! at once. `--mode local-cluster`, `OXIDANT_COLOCATED_ENGINES` and this very test suite all do
//! that on purpose, which is what `a_second_server_on_a_free_port_still_starts` protects.
//!
//! Lives in `oxidant-cli` so Cargo sets `CARGO_BIN_EXE_oxidant` when the test binary is built.

use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

mod common;
use common::oxidant_bin;

/// A port nobody is listening on *yet*. Inherently racy against the rest of the machine, but it
/// is what every other CLI test here uses and the ephemeral range is wide.
fn pick_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local_addr")
        .port()
}

struct ServerGuard(Child);

impl ServerGuard {
    fn pid(&self) -> u32 {
        self.0.id()
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A server that is fully listening: REST answers on the UI port *and* the gRPC port accepts
/// connections. Both matter — a conflict test that starts before the listener it means to
/// collide with is a flake, not a test.
struct Running {
    server: ServerGuard,
    #[allow(dead_code)]
    data_dir: tempfile::TempDir,
    port: u16,
    ui_port: u16,
}

async fn start_server(oxidant: &std::path::Path) -> Running {
    let port = pick_port();
    let ui_port = pick_port();
    // The history journal locks its data dir; a shared default makes a second test server exit 1
    // at startup for a reason that has nothing to do with ports (see `common::data_dir`).
    let data_dir = common::data_dir();
    let child = Command::new(oxidant)
        .env("OXIDANT_DATA_DIR", data_dir.path())
        .args([
            "spark",
            "server",
            "--port",
            &port.to_string(),
            "--ui-port",
            &ui_port.to_string(),
            // Both listeners on loopback: the UI conflict below has to collide on the *same*
            // address, because macOS lets `0.0.0.0:P` and `127.0.0.1:P` coexist.
            "--ui-bind",
            "127.0.0.1",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn oxidant spark server");
    let mut server = ServerGuard(child);

    let client = reqwest::Client::new();
    let mut up = false;
    for _ in 0..80 {
        if let Some(status) = server.0.try_wait().expect("try_wait server") {
            panic!("oxidant spark server exited early with {status}");
        }
        let rest_ok = matches!(
            client
                .get(format!("http://127.0.0.1:{ui_port}/api/v1/cluster/status"))
                .send()
                .await,
            Ok(resp) if resp.status() == 200
        );
        if rest_ok && TcpStream::connect(("127.0.0.1", port)).is_ok() {
            up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(up, "server never came up on {port}/{ui_port}");
    Running {
        server,
        data_dir,
        port,
        ui_port,
    }
}

/// Run `oxidant` expecting it to refuse and exit. Returns its stderr.
///
/// The deadline *is* the "exits quickly" assertion: without the guard the process would sit in
/// `serve` forever (or die much later, deep inside tonic).
fn refused(args: &[&str], data_dir: &std::path::Path) -> String {
    let mut child = Command::new(oxidant_bin())
        .env("OXIDANT_DATA_DIR", data_dir)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn oxidant");
    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        match child.try_wait().expect("try_wait") {
            Some(status) => break status,
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("oxidant did not exit within 20s for args {args:?}");
            }
            None => std::thread::sleep(Duration::from_millis(50)),
        }
    };
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .expect("stderr pipe")
        .read_to_string(&mut stderr)
        .expect("read stderr");
    assert!(
        !status.success(),
        "expected a non-zero exit for {args:?}, got {status}; stderr:\n{stderr}"
    );
    // Refusal has to come *before* the server announces itself. A banner followed by an error
    // is the confusing shape the guard exists to replace.
    assert!(
        !stderr.contains("listening on"),
        "guard fired after the server had already announced itself:\n{stderr}"
    );
    stderr
}

#[tokio::test]
async fn a_second_server_on_a_taken_grpc_port_names_the_first() {
    let oxidant = oxidant_bin();
    let first = start_server(&oxidant).await;
    let data_dir = common::data_dir();

    let stderr = refused(
        &[
            "spark",
            "server",
            "--port",
            &first.port.to_string(),
            "--ui-port",
            &pick_port().to_string(),
            "--ui-bind",
            "127.0.0.1",
        ],
        data_dir.path(),
    );

    assert!(
        stderr.contains(&format!(
            "error: port {} is already held by another oxidant process",
            first.port
        )),
        "stderr did not identify the holder as an oxidant process:\n{stderr}"
    );
    assert!(
        stderr.contains(&format!("pid:      {}", first.server.pid())),
        "stderr did not name pid {}:\n{stderr}",
        first.server.pid()
    );
    // Both of the first server's ports, labelled — "kill this and you get 4040 back too" is the
    // fact that was missing during the incident.
    assert!(
        stderr.contains(&format!("{} (spark connect)", first.port))
            && stderr.contains(&format!("{} (ui + rest)", first.ui_port)),
        "stderr did not list both of the holder's ports:\n{stderr}"
    );
    assert!(
        stderr.contains(&format!("kill {}", first.server.pid())),
        "stderr did not say how to stop it:\n{stderr}"
    );
    assert!(
        stderr.contains("--port"),
        "stderr did not offer the flag that moves this server:\n{stderr}"
    );
}

#[tokio::test]
async fn a_ui_port_conflict_points_at_ui_port() {
    let oxidant = oxidant_bin();
    let first = start_server(&oxidant).await;
    let data_dir = common::data_dir();

    // Fresh gRPC port, colliding UI port: the guard must report the one that actually conflicts.
    let stderr = refused(
        &[
            "spark",
            "server",
            "--port",
            &pick_port().to_string(),
            "--ui-port",
            &first.ui_port.to_string(),
            "--ui-bind",
            "127.0.0.1",
        ],
        data_dir.path(),
    );

    assert!(
        stderr.contains(&format!(
            "error: port {} is already held by another oxidant process",
            first.ui_port
        )),
        "stderr blamed the wrong port:\n{stderr}"
    );
    assert!(
        stderr.contains(&format!("pid:      {}", first.server.pid())),
        "stderr did not name pid {}:\n{stderr}",
        first.server.pid()
    );
    assert!(
        stderr.contains("with --ui-port"),
        "stderr pointed at the wrong flag — moving --port would not clear a UI conflict:\n{stderr}"
    );
}

/// The guard is about conflicts, not multiplicity. Two servers on different ports is a normal,
/// supported thing to do, and a guard that broke it would break `--mode local-cluster` and this
/// suite along with it.
#[tokio::test]
async fn a_second_server_on_a_free_port_still_starts() {
    let oxidant = oxidant_bin();
    let first = start_server(&oxidant).await;
    let mut second = start_server(&oxidant).await;

    assert_ne!(first.port, second.port);
    assert!(
        second.server.0.try_wait().expect("try_wait").is_none(),
        "the second server exited even though its ports were free"
    );
}

/// A stranger on the port: detection must still run, still not crash, and still not accuse an
/// oxidant that is not there. The occupier here is the test process itself.
#[test]
fn a_non_oxidant_occupier_gets_a_clear_message() {
    let port = pick_port();
    // Bind the exact address `serve` would (`0.0.0.0`), not loopback: on macOS the two coexist,
    // so a loopback listener is not actually a conflict for the server's wildcard bind.
    let _held = TcpListener::bind(("0.0.0.0", port)).expect("hold the port");
    let data_dir = common::data_dir();

    let stderr = refused(
        &["spark", "server", "--port", &port.to_string(), "--no-ui"],
        data_dir.path(),
    );

    assert!(
        stderr.contains(&format!("error: port {port} is already in use")),
        "stderr was not a clear AddrInUse message:\n{stderr}"
    );
    assert!(
        !stderr.contains("another oxidant process"),
        "a plain TcpListener was misreported as an oxidant process:\n{stderr}"
    );
    // Detection ran against a non-oxidant holder without panicking, and found this very process.
    assert!(
        stderr.contains(&format!("pid:      {}", std::process::id())),
        "stderr did not identify the test process as the holder:\n{stderr}"
    );
}

/// `worker --port` binds Arrow Flight, and gets the same guard as the server.
#[test]
fn a_worker_on_a_taken_port_is_refused() {
    let port = pick_port();
    let _held = TcpListener::bind(("0.0.0.0", port)).expect("hold the port");
    let data_dir = common::data_dir();

    let stderr = refused(&["worker", "--port", &port.to_string()], data_dir.path());

    assert!(
        stderr.contains(&format!("error: port {port} is already in use")),
        "worker did not report the conflict:\n{stderr}"
    );
    assert!(
        stderr.contains("start this worker on a different port with --port"),
        "worker hint did not name itself:\n{stderr}"
    );
}

/// `history-server --port` binds the standalone UI, and gets the same guard.
#[test]
fn a_history_server_on_a_taken_port_is_refused() {
    let port = pick_port();
    let _held = TcpListener::bind(("0.0.0.0", port)).expect("hold the port");
    let data_dir = common::data_dir();
    let log_dir = tempfile::tempdir().expect("event log dir");

    let stderr = refused(
        &[
            "history-server",
            "--dir",
            log_dir.path().to_str().expect("utf-8 log dir"),
            "--port",
            &port.to_string(),
        ],
        data_dir.path(),
    );

    assert!(
        stderr.contains(&format!("error: port {port} is already in use")),
        "history server did not report the conflict:\n{stderr}"
    );
    assert!(
        stderr.contains("start this history server on a different port with --port"),
        "history server hint did not name itself:\n{stderr}"
    );
}
