//! Iceberg REST / Unity Catalog-compatible catalog provider over HTTP.

use std::collections::HashMap;
use std::process::Command;

use async_trait::async_trait;
use oxidant_catalog::{CatalogProvider, TableFormat, TableMetadata};
use oxidant_common::{Error, Result};

/// REST catalog (Iceberg REST spec / Unity Catalog REST API subset).
#[derive(Debug, Clone)]
pub struct RestCatalog {
    name: String,
    uri: String,
    #[allow(dead_code)]
    warehouse: Option<String>,
    token: Option<String>,
}

impl RestCatalog {
    pub fn from_config(name: &str, options: &HashMap<String, String>) -> Result<Self> {
        let uri = options
            .get("uri")
            .cloned()
            .ok_or_else(|| Error::Unsupported(format!("catalog `{name}` needs uri")))?;
        Ok(Self {
            name: name.to_string(),
            uri,
            warehouse: options.get("warehouse").cloned(),
            token: options.get("token").cloned(),
        })
    }

    fn curl_json(&self, path: &str) -> Result<serde_json::Value> {
        let url = format!(
            "{}/{}",
            self.uri.trim_end_matches('/'),
            path.trim_start_matches('/')
        );
        let mut cmd = Command::new("curl");
        // No `-f`: the HTTP status rides along via `-w` and is classified in
        // `classify_status` — a 404 (the spec's missing-namespace/table signal) must map to a
        // not-found `Error::Plan`, not a generic `Error::Io` (KAN-83: `table_exists` depends
        // on the distinction).
        cmd.args([
            "-sS",
            "-w",
            "\n%{http_code}",
            "-H",
            "Accept: application/json",
        ]);
        if let Some(tok) = &self.token {
            cmd.args(["-H", &format!("Authorization: Bearer {tok}")]);
        }
        cmd.arg(&url);
        let out = cmd
            .output()
            .map_err(|e| Error::Io(format!("curl {url}: {e}")))?;
        if !out.status.success() {
            // curl itself failed (DNS/connect/TLS/...) — a backend failure with no HTTP
            // status to classify.
            return Err(Error::Io(format!(
                "catalog GET {url}: {}",
                String::from_utf8_lossy(&out.stderr)
            )));
        }
        let stdout = String::from_utf8_lossy(&out.stdout);
        let (body, status) = stdout
            .rsplit_once('\n')
            .and_then(|(body, code)| code.trim().parse::<u16>().ok().map(|s| (body, s)))
            .ok_or_else(|| Error::Io(format!("catalog GET {url}: malformed curl output")))?;
        classify_status(path, status, body)
    }
}

/// Classify an HTTP response from the REST catalog. 2xx parses as JSON; 404 — the Iceberg
/// REST spec's "no such namespace/table" signal — maps to [`Error::Plan`] (not-found, like
/// Glue's `EntityNotFoundException`; `CatalogProvider::table_exists`'s default impl depends
/// on it, and a 404 on a list is likewise a missing-namespace not-found). Every other non-2xx
/// is a backend failure → [`Error::Io`] with the status in the message.
fn classify_status(path: &str, status: u16, body: &str) -> Result<serde_json::Value> {
    match status {
        200..=299 => {
            serde_json::from_str(body).map_err(|e| Error::Io(format!("catalog json {path}: {e}")))
        }
        404 => Err(Error::Plan(format!("REST catalog: `{path}` not found"))),
        other => Err(Error::Io(format!(
            "catalog GET {path}: HTTP {other}: {}",
            body.trim()
        ))),
    }
}

#[async_trait]
impl CatalogProvider for RestCatalog {
    fn name(&self) -> &str {
        &self.name
    }

    async fn list_namespaces(&self, parent: &[String]) -> Result<Vec<Vec<String>>> {
        if !parent.is_empty() {
            return Ok(vec![]);
        }
        let v = self.curl_json("v1/namespaces")?;
        let mut out = Vec::new();
        if let Some(arr) = v.get("namespaces").and_then(|n| n.as_array()) {
            for item in arr {
                if let Some(name) = item
                    .as_array()
                    .and_then(|a| a.first())
                    .and_then(|s| s.as_str())
                {
                    out.push(vec![name.to_string()]);
                } else if let Some(s) = item.as_str() {
                    out.push(vec![s.to_string()]);
                }
            }
        }
        Ok(out)
    }

    async fn list_tables(&self, namespace: &[String]) -> Result<Vec<String>> {
        let db = namespace
            .first()
            .ok_or_else(|| Error::Plan("REST catalog: namespace required".into()))?;
        let path = format!("v1/namespaces/{db}/tables");
        let v = self.curl_json(&path)?;
        let mut out = Vec::new();
        if let Some(arr) = v.get("identifiers").and_then(|n| n.as_array()) {
            for item in arr {
                if let Some(name) = item.get("name").and_then(|s| s.as_str()) {
                    out.push(name.to_string());
                }
            }
        }
        Ok(out)
    }

    async fn load_table(&self, namespace: &[String], table: &str) -> Result<TableMetadata> {
        let db = namespace
            .first()
            .ok_or_else(|| Error::Plan("REST catalog: namespace required".into()))?;
        let path = format!("v1/namespaces/{db}/tables/{table}");
        let v = self.curl_json(&path)?;
        let meta = v
            .get("metadata")
            .or_else(|| v.get("table"))
            .ok_or_else(|| Error::Unsupported("REST catalog: missing table metadata".into()))?;
        let location = meta
            .get("location")
            .or_else(|| meta.pointer("/metadata/location"))
            .and_then(|l| l.as_str())
            .unwrap_or("")
            .to_string();
        let format = if location.contains("_delta_log") {
            TableFormat::Delta
        } else {
            TableFormat::Iceberg
        };
        Ok(TableMetadata::new(
            format!("{}.{}.{}", self.name, db, table),
            location,
            format,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_status_maps_http_shapes() {
        // 2xx parses the JSON body.
        let v = classify_status("v1/namespaces", 200, r#"{"namespaces":[["db1"]]}"#)
            .expect("200 parses");
        assert_eq!(v["namespaces"][0][0], "db1");
        // 404 → not-found Plan (the provider contract `table_exists` reads as `false`).
        match classify_status("v1/namespaces/db1/tables/ghost", 404, "{}") {
            Err(Error::Plan(msg)) => assert!(msg.contains("not found"), "{msg}"),
            other => panic!("expected Err(Error::Plan), got {other:?}"),
        }
        // Any other non-2xx → backend Io with the status in the message.
        match classify_status("v1/namespaces", 500, "boom") {
            Err(Error::Io(msg)) => assert!(msg.contains("HTTP 500"), "{msg}"),
            other => panic!("expected Err(Error::Io), got {other:?}"),
        }
    }

    /// A raw mini HTTP server standing in for the REST catalog: one request per connection,
    /// dispatched on the request-line path. Runs until the test process drops it.
    fn spawn_rest_stub() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind stub");
        let port = listener.local_addr().expect("local addr").port();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut sock) = stream else {
                    return;
                };
                std::thread::spawn(move || {
                    use std::io::{Read, Write};
                    let mut buf = Vec::new();
                    let mut chunk = [0_u8; 8192];
                    // A curl GET is headers-only: read to the blank line.
                    loop {
                        let n = sock.read(&mut chunk).expect("read");
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                    }
                    let request = String::from_utf8_lossy(&buf).to_string();
                    let (status, body) = if request.contains("/tables/ghost") {
                        (
                            "404 Not Found",
                            r#"{"error":{"message":"no such table","type":"NoSuchTableException"}}"#,
                        )
                    } else if request.contains("/tables/boom") {
                        (
                            "500 Internal Server Error",
                            r#"{"error":{"message":"db on fire"}}"#,
                        )
                    } else if request.contains("/tables/orders") {
                        (
                            "200 OK",
                            r#"{"metadata":{"location":"s3://bucket/db1/orders/"}}"#,
                        )
                    } else {
                        ("404 Not Found", r#"{"error":{"message":"no such"}}"#)
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = sock.write_all(response.as_bytes());
                });
            }
        });
        port
    }

    /// End-to-end through the real `curl` invocation: a missing table (the Iceberg REST
    /// spec's 404) reads as not-found, a backend 500 propagates as `Error::Io` (KAN-83:
    /// never swallowed as "not found").
    #[tokio::test]
    async fn table_exists_maps_404_to_false_and_500_to_error() {
        let port = spawn_rest_stub();
        let mut options = HashMap::new();
        options.insert("uri".to_string(), format!("http://127.0.0.1:{port}"));
        let cat = RestCatalog::from_config("rest", &options).expect("catalog");

        // 200 → the table loads and exists.
        assert!(cat
            .table_exists(&["db1".to_string()], "orders")
            .await
            .expect("200"));
        // 404 → not found, not an error.
        assert!(!cat
            .table_exists(&["db1".to_string()], "ghost")
            .await
            .expect("404 is not-found"));
        // 500 → backend failure propagates.
        match cat.table_exists(&["db1".to_string()], "boom").await {
            Err(Error::Io(msg)) => assert!(msg.contains("HTTP 500"), "{msg}"),
            other => panic!("expected Err(Error::Io), got {other:?}"),
        }
    }
}
