//! The driver federates a log query over a **real** worker, and says so honestly when it cannot
//! (`docs/query-history-durability.md` §6b, §9).
//!
//! This is a subprocess test for the same reason `cli_rolling_logs.rs` is: the thing under test
//! is wiring across a process boundary. Every piece of it exists only when two real binaries are
//! running — the worker's `logging::init` installing the Flight `ACTION_LOGS` handler, the
//! driver's `?worker=` resolving an id against its *own* configuration, `worker_logs` dialling
//! the interconnect, and the answer coming back as the worker's own lines rather than the
//! driver's. A unit test can assert the shape of each; only this can assert they meet.
//!
//! The founder rule this pins: **logs are read through an interface and never copied off a
//! worker.** The assertion is not just that the lines arrive — it is that the driver's own
//! `logs/` directory is unchanged afterwards, byte for byte.

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

mod common;
use common::oxidant_bin;

const TOKEN: &str = "federation-status-token";

fn pick_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local_addr")
        .port()
}

/// Wait for `path` to contain `needle`, or give up.
fn wait_for_text(path: &std::path::Path, needle: &str, what: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(60);
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

/// Wait for an HTTP GET to answer `200`, so the assertions below are about federation rather
/// than about whether the UI server has finished binding.
fn wait_for_http(client: &reqwest::blocking::Client, url: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut last = String::new();
    while Instant::now() < deadline {
        match client.get(url).bearer_auth(TOKEN).send() {
            Ok(r) if r.status().is_success() => return,
            Ok(r) => last = format!("HTTP {}", r.status()),
            Err(e) => last = e.to_string(),
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    panic!("{url} never answered 200 within 60s (last: {last})");
}

fn get_json(client: &reqwest::blocking::Client, url: &str) -> (u16, serde_json::Value) {
    let resp = client
        .get(url)
        .bearer_auth(TOKEN)
        .send()
        .unwrap_or_else(|e| panic!("GET {url}: {e}"));
    let status = resp.status().as_u16();
    let body = resp.text().unwrap_or_default();
    (
        status,
        serde_json::from_str(&body).unwrap_or(serde_json::Value::String(body)),
    )
}

/// Kill a child and reap it, so the next assertion is not racing a zombie.
fn stop(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// A real worker on `port`, sharing the driver's token, returned once it has said it is
/// listening — so the assertions are about federation rather than about a bind race, and there
/// is a line *later* than the init line for a query to find.
fn spawn_worker(oxidant: &std::path::Path, root: &std::path::Path, port: u16) -> Child {
    let child = Command::new(oxidant)
        .args(["worker", "--foreground", "--port", &port.to_string()])
        .env("OXIDANT_DATA_DIR", root)
        .env("OXIDANT_STATUS_TOKEN", TOKEN)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn worker");
    wait_for_text(
        &root.join("logs").join("oxidant.log"),
        "oxidant worker listening on Flight",
        "the worker",
    );
    child
}

/// **The worker's `logs` action is a credential away, not a port away.**
///
/// It was justified by "this port already accepts arbitrary stage SQL, so a log page is not a
/// new privilege". The premise is true and the conclusion does not follow: SQL reads the data
/// *this* worker can reach, while a log page reads up to `OXIDANT_LOG_KEEP_DAYS` of every
/// enabled `tracing` field value — DSNs, object-store keys inside table URIs, bearer tokens in
/// connector config — which are reusable **off** this worker. So the action carries the same
/// `OXIDANT_STATUS_TOKEN` that gates the identical bytes on the driver's `GET /api/v1/logs`.
///
/// Dialled directly, not through the driver: the driver always presents its token, so only a
/// raw call can show what a stranger on the interconnect gets.
#[test]
fn the_worker_serves_its_log_only_to_a_caller_with_the_token() {
    let oxidant = oxidant_bin();
    let root = TempDir::new().expect("tempdir");
    let port = pick_port();
    let mut worker = spawn_worker(&oxidant, root.path(), port);

    let endpoint = format!("http://127.0.0.1:{port}");
    let query = br#"{"file":"current","q":"listening on Flight","limit":50}"#.to_vec();
    let ask = |token: Option<&'static str>| {
        let endpoint = endpoint.clone();
        let query = query.clone();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(async move {
                oxidant_execution::flight::worker_logs(endpoint, query, token).await
            })
    };

    // No credential: refused, and the refusal names what to present.
    let refusal = ask(None).expect_err("an uncredentialed logs action must be refused");
    let refusal = refusal.to_string();
    assert!(
        refusal.contains("OXIDANT_STATUS_TOKEN"),
        "the refusal must name the credential it wants: {refusal}"
    );
    assert!(
        !refusal.contains("oxidant worker listening"),
        "and must not carry a log line with it: {refusal}"
    );

    // Wrong credential: refused the same way. A near-miss must not be more informative than a
    // no-show.
    let wrong = ask(Some("federation-status-token-x"))
        .expect_err("a wrong credential must be refused")
        .to_string();
    assert_eq!(wrong, refusal, "a wrong token must not be told it is close");

    // The right one serves — the same worker, the same query, one header apart.
    let body = ask(Some(TOKEN)).expect("the token must serve");
    let page: serde_json::Value = serde_json::from_slice(&body).expect("a JSON page");
    let lines = page["logs"].as_array().expect("logs");
    assert!(
        lines.iter().any(|l| l
            .as_str()
            .unwrap_or_default()
            .contains("listening on Flight")),
        "the page must be this worker's own log: {page}"
    );

    stop(&mut worker);
}

/// The whole of §6b's federation, end to end over two real processes.
#[test]
fn the_driver_federates_a_log_query_over_a_real_worker_and_copies_nothing() {
    let oxidant = oxidant_bin();
    let worker_root = TempDir::new().expect("tempdir");
    let driver_root = TempDir::new().expect("tempdir");
    let worker_port = pick_port();

    // The worker needs the *same* `OXIDANT_STATUS_TOKEN` as the driver: its `logs` action is
    // gated on it (F16), so a cluster whose workers do not share the driver's token federates
    // nothing. That is the deployment cost of the gate and it is asserted here by the fact that
    // the federation below works at all.
    let mut worker = spawn_worker(&oxidant, worker_root.path(), worker_port);

    let connect_port = pick_port();
    let ui_port = pick_port();
    let mut driver = Command::new(&oxidant)
        .args([
            "spark",
            "server",
            "--foreground",
            "--port",
            &connect_port.to_string(),
            "--ui-port",
            &ui_port.to_string(),
        ])
        .env("OXIDANT_DATA_DIR", driver_root.path())
        .env("OXIDANT_STATUS_TOKEN", TOKEN)
        .env("OXIDANT_WORKERS", format!("127.0.0.1:{worker_port}"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn driver");
    let driver_log = driver_root.path().join("logs").join("oxidant.log");
    wait_for_text(&driver_log, "rolling exec log open", "the driver");

    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("client");
    let base = format!("http://127.0.0.1:{ui_port}");
    wait_for_http(&client, &format!("{base}/api/v1/logs/files"));

    let worker_id = format!("127.0.0.1:{worker_port}");

    // 1. The picker knows the worker, and knows it is alive.
    let (status, body) = get_json(&client, &format!("{base}/api/v1/logs/workers"));
    assert_eq!(status, 200, "{body}");
    let workers = body["workers"].as_array().expect("workers");
    assert_eq!(workers[0]["worker_id"], "driver");
    assert_eq!(workers[1]["worker_id"], worker_id, "{body}");
    assert_eq!(
        workers[1]["reachable"], true,
        "a live worker answers the probe: {body}"
    );

    // 2. The worker's own files, listed through the driver.
    let (status, body) = get_json(
        &client,
        &format!("{base}/api/v1/logs/files?worker={worker_id}"),
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["worker"], worker_id, "the rows are labelled: {body}");
    let files = body["files"].as_array().expect("files");
    assert!(
        files.iter().any(|f| f["file"] == "current"),
        "the worker's live file: {body}"
    );
    assert_eq!(
        body["dir"]
            .as_str()
            .map(|d| d.contains(worker_root.path().file_name().unwrap().to_str().unwrap())),
        Some(true),
        "and it is the *worker's* directory, not the driver's: {body}"
    );

    // 3. The worker's own lines, through the driver, with the filters applied on the worker.
    let (status, body) = get_json(
        &client,
        &format!("{base}/api/v1/logs?worker={worker_id}&file=current&q=worker&limit=200"),
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["worker"], worker_id);
    let lines: Vec<&str> = body["logs"]
        .as_array()
        .expect("logs")
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    assert!(
        lines.iter().any(|l| l.contains(r#"role="worker""#)),
        "these must be the worker's lines, not the driver's: {lines:?}"
    );
    assert!(
        lines
            .iter()
            .any(|l| l.contains("oxidant worker listening on Flight")),
        "including one written after the worker's whole bootstrap: {lines:?}"
    );

    // The driver's *own* answer to the same query has none of them — which is what makes the
    // above evidence of federation rather than of a shared directory.
    let (status, own) = get_json(
        &client,
        &format!("{base}/api/v1/logs?file=current&q=worker&limit=200"),
    );
    assert_eq!(status, 200, "{own}");
    assert_eq!(own["worker"], "driver");
    assert!(
        !own["logs"]
            .as_array()
            .expect("logs")
            .iter()
            .any(|l| l.as_str().is_some_and(|l| l.contains(r#"role="worker""#))),
        "the driver's own log holds no worker lines: {own}"
    );

    // 4. **The founder rule.** Reading a worker's log must not put a byte of it on the driver.
    // `.lock` is PR3's one-writer-per-log-directory guard, not a log file; everything else in
    // here would have to be a byte the driver ingested.
    let mut driver_logs: Vec<String> = std::fs::read_dir(driver_root.path().join("logs"))
        .expect("driver logs dir")
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n != ".lock")
        .collect();
    driver_logs.sort();
    assert_eq!(
        driver_logs,
        vec!["oxidant.log".to_string()],
        "federation writes nothing into the driver's logs/"
    );
    let driver_body = std::fs::read_to_string(&driver_log).expect("driver log");
    assert!(
        !driver_body.contains(r#"role="worker""#),
        "and it ingests nothing from the worker: {driver_body}"
    );
    assert!(
        !driver_root.path().join("dumps").exists()
            || std::fs::read_dir(driver_root.path().join("dumps"))
                .map(|d| d.count())
                .unwrap_or(0)
                == 0,
        "the dump directory is the *only* path that copies, and nothing asked for one"
    );

    // 5. A worker that stops answering is reported, not skipped. Silence would read as "this
    //    worker logged nothing", which is the opposite of what happened.
    stop(&mut worker);
    let (status, body) = get_json(
        &client,
        &format!("{base}/api/v1/logs?worker={worker_id}&file=current"),
    );
    assert!(
        status == 502 || status == 504,
        "a dead worker is a named failure, got {status}: {body}"
    );
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains(&worker_port.to_string())),
        "and the reason names the node: {body}"
    );
    assert!(body.get("logs").is_none(), "never an empty page: {body}");

    let (status, body) = get_json(&client, &format!("{base}/api/v1/logs/workers"));
    assert_eq!(status, 200, "{body}");
    let workers = body["workers"].as_array().expect("workers");
    assert_eq!(workers[1]["worker_id"], worker_id);
    assert_eq!(
        workers[1]["reachable"], false,
        "listed with reachable:false rather than dropped from the picker: {body}"
    );
    assert!(
        workers[1]["error"].as_str().is_some_and(|e| !e.is_empty()),
        "with a reason: {body}"
    );

    stop(&mut driver);
}
