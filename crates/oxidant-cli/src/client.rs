//! Async HTTP client for the Oxidant REST statement-execution API
//! (`oxidant-connect::rest`, served on the driver's UI port).
//!
//! Shared by the `oxidant sql` subcommand and the `oxidant mcp` stdio server. Base URL comes
//! from `--url` / `OXIDANT_URL` / [`DEFAULT_URL`] (see [`resolve_url`]). Server-side
//! `{"error": "..."}` bodies and HTTP failures are mapped onto [`oxidant_common::Error`], the
//! error type `main` already reports.

use std::time::Duration;

use oxidant_common::{Error, Result};
use serde_json::{json, Value};

/// Default statements-API base URL (the UI port of a local `oxidant spark server`).
pub const DEFAULT_URL: &str = "http://localhost:4040";
/// Overall wall-clock budget for one `oxidant sql` / `run_sql` statement.
pub const DEFAULT_STATEMENT_TIMEOUT: Duration = Duration::from_secs(300);
/// How long a single `?wait=true` submit blocks server-side before the client starts polling
/// `GET /statements/{id}` instead.
const WAIT_SUBMIT_CHUNK_SECS: u64 = 30;
/// Interval between status polls while a statement is still `pending`/`running`.
pub const POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Per-request HTTP timeout; comfortably above [`WAIT_SUBMIT_CHUNK_SECS`] so a blocking
/// submit is never cut short client-side.
const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

/// Resolve the statements-API base URL: explicit `--url` value first, then `OXIDANT_URL`,
/// then [`DEFAULT_URL`].
pub fn resolve_url(cli: Option<String>) -> String {
    cli.or_else(|| std::env::var("OXIDANT_URL").ok())
        .map(|u| u.trim_end_matches('/').to_string())
        .filter(|u| !u.is_empty())
        .unwrap_or_else(|| DEFAULT_URL.to_string())
}

/// Result format of `GET /api/v1/statements/{id}/result`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultFormat {
    Json,
    Csv,
}

impl ResultFormat {
    fn as_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Csv => "csv",
        }
    }
}

/// Body of a successful result fetch: parsed JSON document or raw `text/csv` payload.
#[derive(Debug)]
pub enum ResultBody {
    Json(Value),
    Csv(String),
}

/// Minimal async client for the statements API. Clone-cheap (wraps one `reqwest::Client`).
#[derive(Debug, Clone)]
pub struct StatementClient {
    base_url: String,
    http: reqwest::Client,
}

impl StatementClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(HTTP_REQUEST_TIMEOUT)
            .build()
            .map_err(|e| Error::Io(format!("build HTTP client: {e}")))?;
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http,
        })
    }

    /// `POST /api/v1/statements`. With `wait`, appends `?wait=true&timeout=<secs>` and the
    /// server answers `200` with the full status doc (possibly still `running`); otherwise
    /// the response is the `202 {"statementId","status":"pending"}` doc.
    pub async fn submit(&self, sql: &str, wait: bool, timeout_secs: u64) -> Result<Value> {
        let mut url = format!("{}/api/v1/statements", self.base_url);
        if wait {
            url = format!("{url}?wait=true&timeout={timeout_secs}");
        }
        let resp = self
            .http
            .post(&url)
            .json(&json!({ "sql": sql }))
            .send()
            .await
            .map_err(|e| self.transport_error(e))?;
        Self::json_response(resp).await
    }

    /// `GET /api/v1/statements/{id}` — full status doc for one statement.
    pub async fn get_statement(&self, id: &str) -> Result<Value> {
        let resp = self
            .http
            .get(format!("{}/api/v1/statements/{id}", self.base_url))
            .send()
            .await
            .map_err(|e| self.transport_error(e))?;
        Self::json_response(resp).await
    }

    /// `GET /api/v1/statements/{id}/result?format=…&limit=…`. The server answers `409` unless
    /// the statement has `succeeded`; that surfaces here as an [`Error::Execution`].
    pub async fn get_result(
        &self,
        id: &str,
        format: ResultFormat,
        limit: Option<usize>,
    ) -> Result<ResultBody> {
        let base = &self.base_url;
        let fmt = format.as_str();
        let mut url = format!("{base}/api/v1/statements/{id}/result?format={fmt}");
        if let Some(limit) = limit {
            url = format!("{url}&limit={limit}");
        }
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| self.transport_error(e))?;
        if !resp.status().is_success() {
            return Err(Self::error_response(resp).await);
        }
        match format {
            ResultFormat::Json => {
                let body = resp
                    .json::<Value>()
                    .await
                    .map_err(|e| self.transport_error(e))?;
                Ok(ResultBody::Json(body))
            }
            ResultFormat::Csv => {
                let body = resp.text().await.map_err(|e| self.transport_error(e))?;
                Ok(ResultBody::Csv(body))
            }
        }
    }

    /// `POST /api/v1/statements/{id}/cancel` — `200 {"statementId","status":"canceled"}`,
    /// `404` unknown id, `409` already terminal.
    pub async fn cancel(&self, id: &str) -> Result<Value> {
        let resp = self
            .http
            .post(format!("{}/api/v1/statements/{id}/cancel", self.base_url))
            .send()
            .await
            .map_err(|e| self.transport_error(e))?;
        Self::json_response(resp).await
    }

    /// `GET /api/v1/statements` — `{"statements":[...]}` newest-first.
    pub async fn list_statements(&self) -> Result<Value> {
        let resp = self
            .http
            .get(format!("{}/api/v1/statements", self.base_url))
            .send()
            .await
            .map_err(|e| self.transport_error(e))?;
        Self::json_response(resp).await
    }

    /// `GET /api/v1/cluster/status` — `{"mode","workers","version"}`.
    pub async fn cluster_status(&self) -> Result<Value> {
        let resp = self
            .http
            .get(format!("{}/api/v1/cluster/status", self.base_url))
            .send()
            .await
            .map_err(|e| self.transport_error(e))?;
        Self::json_response(resp).await
    }

    /// Return `snapshot` (a submit/get status doc) once the statement reaches a terminal
    /// state, polling `GET /statements/{id}` every [`POLL_INTERVAL`] until `timeout` elapses.
    pub async fn wait_terminal(&self, snapshot: Value, timeout: Duration) -> Result<Value> {
        let deadline = std::time::Instant::now() + timeout;
        let mut snap = snapshot;
        loop {
            let status = snap["status"].as_str().unwrap_or("unknown");
            if is_terminal(status) {
                return Ok(snap);
            }
            let now = std::time::Instant::now();
            let id = snap["statementId"].as_str().unwrap_or("?").to_string();
            if now >= deadline {
                return Err(Error::Execution(format!(
                    "statement {id} still `{status}` after {}s (use --timeout to raise the limit)",
                    timeout.as_secs()
                )));
            }
            tokio::time::sleep(POLL_INTERVAL.min(deadline - now)).await;
            snap = self.get_statement(&id).await?;
        }
    }

    /// Wrap a transport-level failure (DNS, refused connection, body decode) as [`Error::Io`].
    fn transport_error(&self, e: reqwest::Error) -> Error {
        Error::Io(format!(
            "statements API request to {} failed: {e}",
            self.base_url
        ))
    }

    /// Parse a JSON response body on success, or map a non-2xx response to an error.
    async fn json_response(resp: reqwest::Response) -> Result<Value> {
        if resp.status().is_success() {
            return resp
                .json::<Value>()
                .await
                .map_err(|e| Error::Io(format!("statements API returned invalid JSON: {e}")));
        }
        Err(Self::error_response(resp).await)
    }

    /// Map a non-2xx response to an [`Error::Execution`], preferring the server's
    /// `{"error": "..."}` body over the raw payload.
    async fn error_response(resp: reqwest::Response) -> Error {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        let detail = serde_json::from_str::<Value>(&body)
            .ok()
            .and_then(|v| v["error"].as_str().map(str::to_string))
            .filter(|s| !s.is_empty())
            .unwrap_or(body);
        Error::Execution(format!("statements API error (HTTP {status}): {detail}"))
    }
}

/// `true` for the API's terminal lifecycle states.
pub fn is_terminal(status: &str) -> bool {
    matches!(status, "succeeded" | "failed" | "canceled")
}

/// Submit `sql` with a blocking wait and poll until terminal — the shared "run a statement to
/// completion" path of `oxidant sql` and the MCP `run_sql` tool. Returns the terminal status
/// doc; a still-running statement after `timeout` is an error.
pub async fn run_to_completion(
    client: &StatementClient,
    sql: &str,
    timeout: Duration,
) -> Result<Value> {
    let wait_secs = timeout.as_secs().clamp(1, WAIT_SUBMIT_CHUNK_SECS);
    let submitted = client.submit(sql, true, wait_secs).await?;
    client.wait_terminal(submitted, timeout).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil;

    #[tokio::test]
    async fn submit_wait_then_fetch_json_result() {
        let base = testutil::spawn_mock().await;
        let client = StatementClient::new(&base).unwrap();
        let snap = client.submit("SELECT 1 AS hello", true, 30).await.unwrap();
        assert_eq!(snap["status"], "succeeded");
        let id = snap["statementId"].as_str().unwrap();
        match client
            .get_result(id, ResultFormat::Json, None)
            .await
            .unwrap()
        {
            ResultBody::Json(doc) => {
                assert_eq!(doc["rowCount"], 1);
                assert_eq!(doc["rows"][0]["hello"], 1);
                assert_eq!(doc["schema"]["fields"][0]["name"], "hello");
            }
            ResultBody::Csv(_) => panic!("expected json result body"),
        }
    }

    #[tokio::test]
    async fn submit_without_wait_returns_pending_then_poll_succeeds() {
        let base = testutil::spawn_mock().await;
        let client = StatementClient::new(&base).unwrap();
        let snap = client.submit("SELECT 1 AS hello", false, 0).await.unwrap();
        assert_eq!(snap["status"], "pending");
        let terminal = client
            .wait_terminal(snap, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(terminal["status"], "succeeded");
    }

    #[tokio::test]
    async fn csv_result_is_raw_text() {
        let base = testutil::spawn_mock().await;
        let client = StatementClient::new(&base).unwrap();
        let snap = client.submit("SELECT 1 AS hello", true, 30).await.unwrap();
        let id = snap["statementId"].as_str().unwrap();
        match client
            .get_result(id, ResultFormat::Csv, None)
            .await
            .unwrap()
        {
            ResultBody::Csv(text) => assert_eq!(text, "hello\n1\n"),
            ResultBody::Json(_) => panic!("expected csv result body"),
        }
    }

    #[tokio::test]
    async fn failed_statement_carries_server_error() {
        let base = testutil::spawn_mock().await;
        let client = StatementClient::new(&base).unwrap();
        let snap = client.submit("FAIL this query", true, 30).await.unwrap();
        assert_eq!(snap["status"], "failed");
        assert_eq!(snap["error"], "mock execution failed");
    }

    #[tokio::test]
    async fn unknown_statement_maps_404_with_error_body() {
        let base = testutil::spawn_mock().await;
        let client = StatementClient::new(&base).unwrap();
        let err = client.get_statement("no-such-id").await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("404"), "expected status in error: {msg}");
        assert!(
            msg.contains("unknown statement id"),
            "expected server error body in error: {msg}"
        );
    }

    #[tokio::test]
    async fn result_of_unfinished_statement_maps_409() {
        let base = testutil::spawn_mock().await;
        let client = StatementClient::new(&base).unwrap();
        // The mock keeps `PENDING` statements `running` forever, so the result endpoint 409s.
        let snap = client.submit("PENDING forever", false, 0).await.unwrap();
        let id = snap["statementId"].as_str().unwrap();
        let err = client
            .get_result(id, ResultFormat::Json, None)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("409"), "expected status in error: {msg}");
    }

    #[tokio::test]
    async fn wait_terminal_times_out_on_stuck_statement() {
        let base = testutil::spawn_mock().await;
        let client = StatementClient::new(&base).unwrap();
        let snap = client.submit("PENDING forever", false, 0).await.unwrap();
        let err = client
            .wait_terminal(snap, Duration::from_millis(1500))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("still `running`"), "{err}");
    }

    #[tokio::test]
    async fn cancel_pending_then_conflict_on_terminal() {
        let base = testutil::spawn_mock().await;
        let client = StatementClient::new(&base).unwrap();
        let snap = client.submit("PENDING forever", false, 0).await.unwrap();
        let id = snap["statementId"].as_str().unwrap().to_string();
        let canceled = client.cancel(&id).await.unwrap();
        assert_eq!(canceled["status"], "canceled");
        let err = client.cancel(&id).await.unwrap_err();
        assert!(err.to_string().contains("409"), "{err}");
    }

    #[tokio::test]
    async fn list_and_cluster_status() {
        let base = testutil::spawn_mock().await;
        let client = StatementClient::new(&base).unwrap();
        client.submit("SELECT 1 AS a", true, 30).await.unwrap();
        client.submit("SELECT 2 AS b", true, 30).await.unwrap();
        let list = client.list_statements().await.unwrap();
        let statements = list["statements"].as_array().unwrap();
        assert_eq!(statements.len(), 2);
        // Newest first: the second submit leads.
        assert_eq!(statements[0]["sql"], "SELECT 2 AS b");
        let status = client.cluster_status().await.unwrap();
        assert_eq!(status["mode"], "single-node");
        assert_eq!(status["workers"], json!([]));
    }

    #[tokio::test]
    async fn connection_refused_is_an_io_error() {
        let client = StatementClient::new("http://127.0.0.1:1").unwrap();
        let err = client.get_statement("x").await.unwrap_err();
        assert!(matches!(err, Error::Io(_)), "{err}");
    }

    #[test]
    fn resolve_url_prefers_flag_then_env_then_default() {
        assert_eq!(resolve_url(Some("http://h:1/".into())), "http://h:1");
        // With no flag, the value comes from OXIDANT_URL or the default; never empty.
        let resolved = resolve_url(None);
        assert!(!resolved.is_empty());
        assert!(!resolved.ends_with('/'));
    }

    #[test]
    fn terminal_state_classification() {
        assert!(is_terminal("succeeded"));
        assert!(is_terminal("failed"));
        assert!(is_terminal("canceled"));
        assert!(!is_terminal("running"));
        assert!(!is_terminal("pending"));
    }
}
