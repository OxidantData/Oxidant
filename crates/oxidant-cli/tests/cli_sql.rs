//! End-to-end smoke tests for the `oxidant sql` and `oxidant mcp` subcommands against a real
//! `oxidant spark server` (no workers — the driver executes locally). `sql` is asserted on
//! table/csv/json output and failure exit codes; `mcp` is driven as a JSON-RPC subprocess:
//! an `initialize` frame then a `tools/call run_sql` frame on its stdin, responses read back
//! from framed stdout lines.
//!
//! Lives in `oxidant-cli` so Cargo sets `CARGO_BIN_EXE_oxidant` when the test binary is built.

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

mod common;
use common::oxidant_bin;

fn pick_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local_addr")
        .port()
}

struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Spawn `oxidant spark server` (no workers) and wait until the statements API answers.
/// Returns the base URL of the REST API on the UI port.
async fn start_server(oxidant: &std::path::Path) -> (ServerGuard, String) {
    let port = pick_port();
    let ui_port = pick_port();
    let server = Command::new(oxidant)
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
    let base_url = format!("http://127.0.0.1:{ui_port}");

    // The UI listener comes up asynchronously; retry until it accepts connections, but fail
    // fast if the server process died (port conflict, bind error, ...).
    let client = reqwest::Client::new();
    let mut up = false;
    for _ in 0..80 {
        if let Some(status) = server.0.try_wait().expect("try_wait server") {
            panic!("oxidant spark server exited early with {status}");
        }
        match client
            .get(format!("{base_url}/api/v1/cluster/status"))
            .send()
            .await
        {
            Ok(resp) if resp.status() == 200 => {
                up = true;
                break;
            }
            _ => tokio::time::sleep(Duration::from_millis(250)).await,
        }
    }
    assert!(up, "statements API never came up at {base_url}");
    (server, base_url)
}

#[tokio::test]
async fn cli_sql_runs_statement_and_prints_results() {
    let oxidant = oxidant_bin();
    assert!(
        oxidant.exists(),
        "oxidant binary not found at {}; run `cargo build -p oxidant-cli` first",
        oxidant.display()
    );
    let (_server, base_url) = start_server(&oxidant).await;

    // Default table format: header + value + row-count line.
    let out = Command::new(&oxidant)
        .args(["sql", "--url", &base_url, "-e", "SELECT 1 AS hello"])
        .output()
        .expect("run oxidant sql");
    assert!(
        out.status.success(),
        "oxidant sql failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("hello"), "table header missing: {stdout}");
    assert!(stdout.contains("| 1"), "table row missing: {stdout}");
    assert!(
        stdout.contains("(1 row)"),
        "row count line missing: {stdout}"
    );

    // CSV passthrough: header line then the value.
    let out = Command::new(&oxidant)
        .args([
            "sql",
            "--url",
            &base_url,
            "--format",
            "csv",
            "-e",
            "SELECT 1 AS hello",
        ])
        .output()
        .expect("run oxidant sql --format csv");
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(stdout.lines().next(), Some("hello"));
    assert!(stdout.lines().nth(1).is_some_and(|l| l.contains('1')));

    // JSON: the pretty-printed result document.
    let out = Command::new(&oxidant)
        .args([
            "sql",
            "--url",
            &base_url,
            "--format",
            "json",
            "-e",
            "SELECT 1 AS hello",
        ])
        .output()
        .expect("run oxidant sql --format json");
    assert!(out.status.success());
    let doc: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("json format output parses");
    assert_eq!(doc["rows"][0]["hello"], 1);

    // Failed statements print the error and exit non-zero.
    let out = Command::new(&oxidant)
        .args([
            "sql",
            "--url",
            &base_url,
            "-e",
            "SELECT * FROM table_that_does_not_exist",
        ])
        .output()
        .expect("run oxidant sql on bad query");
    assert!(!out.status.success(), "failed statement must exit non-zero");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("failed"),
        "stderr must carry the error: {stderr}"
    );
}

/// Write one JSON-RPC frame line to the MCP child's stdin and read back the single response
/// line, parsed as JSON. Panics (with a timeout) if no response arrives.
async fn exchange(
    stdin: &mut tokio::process::ChildStdin,
    lines: &mut tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    frame: &str,
) -> serde_json::Value {
    stdin
        .write_all(frame.as_bytes())
        .await
        .expect("write frame");
    stdin.write_all(b"\n").await.expect("write newline");
    stdin.flush().await.expect("flush frame");
    let line = tokio::time::timeout(Duration::from_secs(30), lines.next_line())
        .await
        .expect("timed out waiting for MCP response")
        .expect("read response line")
        .expect("MCP stdout closed before response");
    serde_json::from_str::<serde_json::Value>(&line).expect("response is one JSON doc")
}

#[tokio::test]
async fn cli_mcp_serves_initialize_and_run_sql_over_stdio() {
    let oxidant = oxidant_bin();
    assert!(
        oxidant.exists(),
        "oxidant binary not found at {}; run `cargo build -p oxidant-cli` first",
        oxidant.display()
    );
    let (_server, base_url) = start_server(&oxidant).await;

    let mut child = tokio::process::Command::new(&oxidant)
        .args(["mcp", "--url", &base_url])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn oxidant mcp");
    let mut stdin = child.stdin.take().expect("mcp stdin");
    let mut lines = BufReader::new(child.stdout.take().expect("mcp stdout")).lines();

    let initialize = exchange(
        &mut stdin,
        &mut lines,
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke","version":"0"}}}"#,
    )
    .await;
    assert_eq!(initialize["id"], 1);
    assert_eq!(initialize["result"]["protocolVersion"], "2024-11-05");
    assert!(initialize["result"]["capabilities"]["tools"].is_object());

    // A notification: consumed silently (no response line to read).
    stdin
        .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
        .await
        .expect("write initialized notification");
    stdin.flush().await.expect("flush notification");

    let called = exchange(
        &mut stdin,
        &mut lines,
        r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"run_sql","arguments":{"sql":"SELECT 1 AS hello"}}}"#,
    )
    .await;
    assert_eq!(called["id"], 2);
    assert_eq!(
        called["result"]["isError"], false,
        "run_sql tool failed: {called}"
    );
    let text = called["result"]["content"][0]["text"]
        .as_str()
        .expect("text content");
    let doc: serde_json::Value = serde_json::from_str(text).expect("tool text is JSON");
    assert_eq!(doc["rows"][0]["hello"], 1);
    assert_eq!(doc["schema"]["fields"][0]["name"], "hello");

    let _ = child.kill().await;
    let _ = child.wait().await;
}
