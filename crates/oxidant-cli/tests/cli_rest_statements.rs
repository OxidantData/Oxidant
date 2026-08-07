//! End-to-end smoke test for the REST statement API on the driver's UI port: spawn
//! `oxidant spark server` with NO workers, submit a statement over HTTP with `?wait=true`,
//! and assert it succeeds with the correct row. This locks the "no workers ⇒ driver executes
//! locally" guarantee at the API layer (the engine-level fallback lives in
//! `oxidant_connect::distributed::try_run_distributed_plan`).
//!
//! Lives in `oxidant-cli` so Cargo sets `CARGO_BIN_EXE_oxidant` when the test binary is built.

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn pick_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local_addr")
        .port()
}

fn oxidant_bin() -> std::path::PathBuf {
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_oxidant") {
        return std::path::PathBuf::from(p);
    }
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "debug".into());
    let mut candidates: Vec<std::path::PathBuf> = Vec::new();
    if let Ok(td) = std::env::var("CARGO_TARGET_DIR") {
        candidates.push(std::path::PathBuf::from(td).join(&profile).join("oxidant"));
    }
    let workspace_target =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target");
    candidates.push(workspace_target.join(&profile).join("oxidant"));
    // `cargo llvm-cov` uses a separate target dir; CI pre-builds oxidant-cli there.
    candidates.push(
        workspace_target
            .join("llvm-cov-target")
            .join(&profile)
            .join("oxidant"),
    );
    for c in &candidates {
        if c.exists() {
            return c.clone();
        }
    }
    candidates
        .into_iter()
        .next()
        .unwrap_or_else(|| workspace_target.join(&profile).join("oxidant"))
}

struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn rest_statement_executes_locally_without_workers() {
    let oxidant = oxidant_bin();
    assert!(
        oxidant.exists(),
        "oxidant binary not found at {}; run `cargo build -p oxidant-cli` first",
        oxidant.display()
    );

    let port = pick_port();
    let ui_port = pick_port();

    let server = Command::new(&oxidant)
        .args([
            "spark",
            "server",
            "--port",
            &port.to_string(),
            "--ui-port",
            &ui_port.to_string(),
            "--ui-bind",
            "127.0.0.1",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn oxidant spark server");
    let mut server = ServerGuard(server);

    let client = reqwest::Client::new();
    let submit_url = format!("http://127.0.0.1:{ui_port}/api/v1/statements?wait=true");

    // The UI listener comes up asynchronously; retry until it accepts connections, but fail
    // fast if the server process died (port conflict, bind error, ...).
    let mut submitted = None;
    for _ in 0..80 {
        if let Some(status) = server.0.try_wait().expect("try_wait server") {
            panic!("oxidant spark server exited early with {status}");
        }
        match client
            .post(&submit_url)
            .json(&serde_json::json!({ "sql": "SELECT 1 AS hello" }))
            .send()
            .await
        {
            Ok(resp) => {
                submitted = Some(resp);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(250)).await,
        }
    }
    let resp = submitted.expect("REST statement API never came up on the UI port");
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.expect("submit response json");
    assert_eq!(
        body["status"], "succeeded",
        "statement must succeed with no workers configured: {body}"
    );
    let statement_id = body["statementId"]
        .as_str()
        .expect("statementId")
        .to_string();

    let result: serde_json::Value = client
        .get(format!(
            "http://127.0.0.1:{ui_port}/api/v1/statements/{statement_id}/result"
        ))
        .send()
        .await
        .expect("fetch result")
        .json()
        .await
        .expect("result json");
    assert_eq!(result["rowCount"], 1);
    assert_eq!(result["rows"][0]["hello"], 1);

    // No workers were started: the cluster must report single-node.
    let status: serde_json::Value = client
        .get(format!("http://127.0.0.1:{ui_port}/api/v1/cluster/status"))
        .send()
        .await
        .expect("cluster status")
        .json()
        .await
        .expect("cluster status json");
    assert_eq!(status["mode"], "single-node");
    assert_eq!(status["workers"], serde_json::json!([]));
}
