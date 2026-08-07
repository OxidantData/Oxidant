//! `oxidant mcp` — a stdio MCP server (Model Context Protocol, protocol version 2024-11-05)
//! exposing the REST statement-execution API as tools.
//!
//! Framing is newline-delimited JSON-RPC 2.0 on stdin/stdout: one message per line, one
//! response line per request, notifications (no `id`) never answered. The wire surface is
//! tiny — `initialize`, `ping`, `tools/list`, `tools/call` — so it is hand-rolled with
//! `serde_json` rather than pulling an MCP framework.
//!
//! stdout carries protocol frames ONLY; any diagnostics go to stderr (see `run_mcp`).
//! Statement failures surface as MCP *tool* errors (`isError: true` content) so an agent
//! session keeps going; transport/protocol problems are JSON-RPC errors.

use oxidant_common::{Error, Result};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};

use crate::client::{
    run_to_completion, ResultBody, ResultFormat, StatementClient, DEFAULT_STATEMENT_TIMEOUT,
};

/// MCP protocol revision this server speaks.
pub const PROTOCOL_VERSION: &str = "2024-11-05";

// JSON-RPC 2.0 error codes.
const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;

/// Tool execution outcome: `Params` maps to a JSON-RPC `-32602` protocol error (bad tool
/// name/arguments), `Execution` maps to an `isError: true` tool result (statement failed,
/// API unreachable, ...) so the MCP session survives.
enum ToolError {
    Params(String),
    Execution(String),
}

/// Serve MCP frames on stdin/stdout until EOF, answering each request against the
/// statements API at `base_url`.
pub async fn serve(base_url: &str) -> Result<()> {
    let client = StatementClient::new(base_url)?;
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut out = BufWriter::new(tokio::io::stdout());
    while let Some(line) = lines
        .next_line()
        .await
        .map_err(|e| Error::Io(format!("mcp: read stdin: {e}")))?
    {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(response) = handle_line(&client, line).await {
            let mut encoded = serde_json::to_string(&response)
                .map_err(|e| Error::Io(format!("mcp: encode response: {e}")))?;
            encoded.push('\n');
            out.write_all(encoded.as_bytes())
                .await
                .map_err(|e| Error::Io(format!("mcp: write stdout: {e}")))?;
            out.flush()
                .await
                .map_err(|e| Error::Io(format!("mcp: flush stdout: {e}")))?;
        }
    }
    out.flush()
        .await
        .map_err(|e| Error::Io(format!("mcp: flush stdout: {e}")))?;
    Ok(())
}

/// Handle one input line. Returns the response frame, or `None` for notifications and
/// stray responses (which must not be answered). Malformed JSON still produces a `-32700`
/// response (id `null`) and the caller keeps serving — one bad line never kills the server.
async fn handle_line(client: &StatementClient, line: &str) -> Option<Value> {
    let msg: Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => {
            return Some(error_response(
                Value::Null,
                PARSE_ERROR,
                &format!("parse error: {e}"),
            ));
        }
    };
    if !msg.is_object() {
        return Some(error_response(
            Value::Null,
            INVALID_REQUEST,
            "request must be a JSON object",
        ));
    }
    // A stray response (result/error, no method) is not ours to answer.
    if msg.get("method").is_none() && (msg.get("result").is_some() || msg.get("error").is_some()) {
        return None;
    }
    let id = msg.get("id").cloned();
    let method = match msg.get("method").and_then(Value::as_str) {
        Some(m) => m,
        None => {
            return id.map(|id| error_response(id, INVALID_REQUEST, "missing `method` field"));
        }
    };
    // No id → notification (`notifications/initialized`, `notifications/cancelled`, ...):
    // never answered, even when the method is unknown.
    let id = id?;
    Some(match dispatch(client, method, &msg["params"]).await {
        Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
        Err((code, message)) => error_response(id, code, &message),
    })
}

/// Route a request method to its handler. `params` may be `Null` (missing).
async fn dispatch(
    client: &StatementClient,
    method: &str,
    params: &Value,
) -> std::result::Result<Value, (i64, String)> {
    match method {
        "initialize" => Ok(json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": { "tools": { "listChanged": false } },
            "serverInfo": { "name": "oxidant", "version": env!("CARGO_PKG_VERSION") },
        })),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": tool_definitions() })),
        "tools/call" => tools_call(client, params).await,
        _ => Err((METHOD_NOT_FOUND, format!("method not found: `{method}`"))),
    }
}

/// Execute a `tools/call` request: `{name, arguments?}`.
async fn tools_call(
    client: &StatementClient,
    params: &Value,
) -> std::result::Result<Value, (i64, String)> {
    let name = params.get("name").and_then(Value::as_str).ok_or_else(|| {
        (
            INVALID_PARAMS,
            "tools/call requires a `name` string".to_string(),
        )
    })?;
    let empty = json!({});
    let args = params.get("arguments").unwrap_or(&empty);
    match call_tool(client, name, args).await {
        Ok(text) => Ok(tool_result(&text, false)),
        Err(ToolError::Execution(msg)) => Ok(tool_result(&msg, true)),
        Err(ToolError::Params(msg)) => Err((INVALID_PARAMS, msg)),
    }
}

/// `{"content":[{"type":"text","text":...}],"isError":...}` — the MCP tool-result shape.
fn tool_result(text: &str, is_error: bool) -> Value {
    json!({
        "content": [{ "type": "text", "text": text }],
        "isError": is_error,
    })
}

fn error_response(id: Value, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message },
    })
}

/// Dispatch one tool by name; the returned string is the tool's text payload (JSON-encoded
/// for structured results).
async fn call_tool(
    client: &StatementClient,
    name: &str,
    args: &Value,
) -> std::result::Result<String, ToolError> {
    match name {
        "run_sql" => {
            let sql = required_str(args, "sql")?;
            let limit = optional_limit(args)?;
            run_sql_tool(client, sql, limit).await
        }
        "submit_sql" => {
            let sql = required_str(args, "sql")?;
            to_pretty(client.submit(sql, false, 0).await)
        }
        "statement_status" => {
            let id = required_str(args, "statementId")?;
            to_pretty(client.get_statement(id).await)
        }
        "list_statements" => to_pretty(client.list_statements().await),
        "cancel_statement" => {
            let id = required_str(args, "statementId")?;
            to_pretty(client.cancel(id).await)
        }
        "cluster_status" => to_pretty(client.cluster_status().await),
        // The engine intercepts these Spark-isms natively (`Engine::sql`'s SHOW/DESCRIBE
        // arms), so both are plain `run_sql` calls.
        "list_tables" => run_sql_tool(client, "SHOW TABLES", None).await,
        "describe_table" => {
            let table = required_str(args, "table")?;
            run_sql_tool(client, &format!("DESCRIBE TABLE {table}"), None).await
        }
        other => Err(ToolError::Params(format!("unknown tool `{other}`"))),
    }
}

/// Submit `sql`, wait for a terminal state, and return the result document (schema + rows +
/// `statementId`) pretty-printed. Failed/canceled statements and timeouts are tool errors.
async fn run_sql_tool(
    client: &StatementClient,
    sql: &str,
    limit: Option<usize>,
) -> std::result::Result<String, ToolError> {
    let terminal = run_to_completion(client, sql, DEFAULT_STATEMENT_TIMEOUT)
        .await
        .map_err(|e| ToolError::Execution(e.to_string()))?;
    let id = terminal["statementId"].as_str().unwrap_or("?").to_string();
    match terminal["status"].as_str().unwrap_or("unknown") {
        "succeeded" => {}
        other => {
            let detail = terminal["error"].as_str().unwrap_or("no error detail");
            return Err(ToolError::Execution(format!(
                "statement {id} {other}: {detail}"
            )));
        }
    }
    match client.get_result(&id, ResultFormat::Json, limit).await {
        Ok(ResultBody::Json(mut doc)) => {
            doc["statementId"] = json!(id);
            Ok(serde_json::to_string_pretty(&doc).unwrap_or_else(|_| doc.to_string()))
        }
        Ok(ResultBody::Csv(_)) => Err(ToolError::Execution(
            "statements API returned CSV for a JSON result request".into(),
        )),
        Err(e) => Err(ToolError::Execution(e.to_string())),
    }
}

/// Pretty-print an API JSON response into the tool text payload.
fn to_pretty(result: Result<Value>) -> std::result::Result<String, ToolError> {
    let doc = result.map_err(|e| ToolError::Execution(e.to_string()))?;
    Ok(serde_json::to_string_pretty(&doc).unwrap_or_else(|_| doc.to_string()))
}

fn required_str<'a>(args: &'a Value, key: &str) -> std::result::Result<&'a str, ToolError> {
    args.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            ToolError::Params(format!("tool argument `{key}` must be a non-empty string"))
        })
}

/// Optional `limit` argument (positive integer row cap for the result endpoint).
fn optional_limit(args: &Value) -> std::result::Result<Option<usize>, ToolError> {
    match args.get("limit") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => match v.as_u64().filter(|n| *n > 0) {
            Some(n) => Ok(Some(n as usize)),
            None => Err(ToolError::Params(
                "tool argument `limit` must be a positive integer".into(),
            )),
        },
    }
}

/// The `tools/list` catalog: name, description, and JSON Schema `inputSchema` per tool.
fn tool_definitions() -> Value {
    let object = || json!({ "type": "object", "properties": {} });
    let string_prop = |desc: &str| json!({ "type": "string", "description": desc });
    json!([
        {
            "name": "run_sql",
            "description": "Run a SQL statement to completion and return the result rows and schema as JSON. Failed statements are reported as tool errors.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "sql": string_prop("SQL statement to execute"),
                    "limit": { "type": "integer", "description": "Optional maximum number of result rows to return" },
                },
                "required": ["sql"],
            },
        },
        {
            "name": "submit_sql",
            "description": "Submit a SQL statement for asynchronous execution; returns its statementId without waiting.",
            "inputSchema": {
                "type": "object",
                "properties": { "sql": string_prop("SQL statement to execute") },
                "required": ["sql"],
            },
        },
        {
            "name": "statement_status",
            "description": "Get the status (pending/running/succeeded/failed/canceled), error, row count, and schema of a statement.",
            "inputSchema": {
                "type": "object",
                "properties": { "statementId": string_prop("Statement id returned by submit_sql or run_sql") },
                "required": ["statementId"],
            },
        },
        {
            "name": "list_statements",
            "description": "List recent statements, newest first.",
            "inputSchema": object(),
        },
        {
            "name": "cancel_statement",
            "description": "Cancel a pending or running statement.",
            "inputSchema": {
                "type": "object",
                "properties": { "statementId": string_prop("Statement id to cancel") },
                "required": ["statementId"],
            },
        },
        {
            "name": "cluster_status",
            "description": "Get the cluster mode (single-node/local-cluster/distributed), worker endpoints, and engine version.",
            "inputSchema": object(),
        },
        {
            "name": "list_tables",
            "description": "List the tables visible to the current session (via SHOW TABLES).",
            "inputSchema": object(),
        },
        {
            "name": "describe_table",
            "description": "Describe a table's columns (via DESCRIBE TABLE).",
            "inputSchema": {
                "type": "object",
                "properties": { "table": string_prop("Table name, optionally catalog/database qualified") },
                "required": ["table"],
            },
        },
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil;

    fn dummy_client() -> StatementClient {
        // Never contacted by the pure protocol tests.
        StatementClient::new("http://127.0.0.1:1").unwrap()
    }

    #[tokio::test]
    async fn initialize_returns_protocol_version_and_capabilities() {
        let resp = handle_line(
            &dummy_client(),
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}"#,
        )
        .await
        .expect("initialize must be answered");
        assert_eq!(resp["id"], 1);
        assert_eq!(resp["result"]["protocolVersion"], PROTOCOL_VERSION);
        assert!(resp["result"]["capabilities"]["tools"].is_object());
        assert_eq!(resp["result"]["serverInfo"]["name"], "oxidant");
        assert!(!resp["result"]["serverInfo"]["version"]
            .as_str()
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn tools_list_returns_all_eight_tools_with_schemas() {
        let resp = handle_line(
            &dummy_client(),
            r#"{"jsonrpc":"2.0","id":7,"method":"tools/list"}"#,
        )
        .await
        .expect("tools/list must be answered");
        let tools = resp["result"]["tools"].as_array().unwrap();
        let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert_eq!(
            names,
            [
                "run_sql",
                "submit_sql",
                "statement_status",
                "list_statements",
                "cancel_statement",
                "cluster_status",
                "list_tables",
                "describe_table",
            ]
        );
        for tool in tools {
            assert_eq!(
                tool["inputSchema"]["type"], "object",
                "tool {} needs an object inputSchema",
                tool["name"]
            );
            assert!(!tool["description"].as_str().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn tools_call_run_sql_returns_rows_and_schema() {
        let client = StatementClient::new(&testutil::spawn_mock().await).unwrap();
        let resp = handle_line(
            &client,
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"run_sql","arguments":{"sql":"SELECT 1 AS hello"}}}"#,
        )
        .await
        .expect("tools/call must be answered");
        assert_eq!(resp["id"], 3);
        assert_eq!(resp["result"]["isError"], false);
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        let doc: Value = serde_json::from_str(text).expect("tool text is JSON");
        assert_eq!(doc["rows"][0]["hello"], 1);
        assert_eq!(doc["schema"]["fields"][0]["name"], "hello");
        assert!(doc["statementId"].as_str().unwrap().starts_with("mock-"));
    }

    #[tokio::test]
    async fn tools_call_failed_statement_is_tool_error_not_protocol_error() {
        let client = StatementClient::new(&testutil::spawn_mock().await).unwrap();
        let resp = handle_line(
            &client,
            r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"run_sql","arguments":{"sql":"FAIL me"}}}"#,
        )
        .await
        .expect("tool errors are still responses");
        assert!(resp.get("result").is_some(), "expected tool result: {resp}");
        assert_eq!(resp["result"]["isError"], true);
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("mock execution failed"), "{text}");
    }

    #[tokio::test]
    async fn malformed_json_yields_parse_error_and_server_keeps_serving() {
        let client = dummy_client();
        let resp = handle_line(&client, "this is not json")
            .await
            .expect("parse errors are answered");
        assert_eq!(resp["id"], Value::Null);
        assert_eq!(resp["error"]["code"], PARSE_ERROR);
        // The server is still alive: a well-formed request right after works.
        let resp = handle_line(&client, r#"{"jsonrpc":"2.0","id":9,"method":"ping"}"#)
            .await
            .expect("ping after garbage");
        assert_eq!(resp["result"], json!({}));
    }

    #[tokio::test]
    async fn notifications_are_never_answered() {
        let client = dummy_client();
        assert!(handle_line(
            &client,
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#
        )
        .await
        .is_none());
        assert!(handle_line(
            &client,
            r#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1}}"#
        )
        .await
        .is_none());
        // Even unknown notifications are dropped silently (JSON-RPC rule).
        assert!(
            handle_line(&client, r#"{"jsonrpc":"2.0","method":"bogus"}"#)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn unknown_method_and_unknown_tool_are_protocol_errors() {
        let client = dummy_client();
        let resp = handle_line(
            &client,
            r#"{"jsonrpc":"2.0","id":1,"method":"resources/list"}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["error"]["code"], METHOD_NOT_FOUND);
        let resp = handle_line(
            &client,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"nope","arguments":{}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["error"]["code"], INVALID_PARAMS);
    }

    #[tokio::test]
    async fn missing_tool_argument_is_invalid_params() {
        let client = dummy_client();
        let resp = handle_line(
            &client,
            r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"run_sql","arguments":{}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["error"]["code"], INVALID_PARAMS);
        assert!(resp["error"]["message"].as_str().unwrap().contains("`sql`"));
    }

    #[tokio::test]
    async fn list_tables_and_describe_table_route_through_show_and_describe() {
        let client = StatementClient::new(&testutil::spawn_mock().await).unwrap();
        for (tool, frame) in [
            ("list_tables", r#"{"name":"list_tables","arguments":{}}"#),
            (
                "describe_table",
                r#"{"name":"describe_table","arguments":{"table":"t"}}"#,
            ),
        ] {
            let line =
                format!(r#"{{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{frame}}}"#);
            let resp = handle_line(&client, &line).await.unwrap();
            assert_eq!(resp["result"]["isError"], false, "tool {tool}: {resp}");
        }
        // Both went through the statements API as the expected Spark-isms.
        let listed = client.list_statements().await.unwrap();
        let sqls: Vec<&str> = listed["statements"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|s| s["sql"].as_str())
            .collect();
        assert!(sqls.contains(&"SHOW TABLES"), "{sqls:?}");
        assert!(sqls.contains(&"DESCRIBE TABLE t"), "{sqls:?}");
    }

    #[tokio::test]
    async fn frame_round_trips_through_compact_single_line_json() {
        let client = dummy_client();
        let resp = handle_line(&client, r#"{"jsonrpc":"2.0","id":"abc","method":"ping"}"#)
            .await
            .unwrap();
        let encoded = serde_json::to_string(&resp).unwrap();
        assert!(!encoded.contains('\n'), "frames must be single-line");
        let decoded: Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded["jsonrpc"], "2.0");
        assert_eq!(decoded["id"], "abc");
        assert_eq!(decoded["result"], json!({}));
    }
}
