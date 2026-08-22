//! Iceberg REST / Unity Catalog-compatible catalog provider over HTTP.

use std::collections::HashMap;
use std::process::Command;

use async_trait::async_trait;
use oxidant_catalog::{CatalogProvider, TableFormat, TableMetadata};
use oxidant_common::{Error, Result};

/// REST catalog (Iceberg REST spec / Unity Catalog REST API subset).
#[derive(Debug)]
pub struct RestCatalog {
    name: String,
    uri: String,
    warehouse: Option<String>,
    token: Option<String>,
    /// Explicit `spark.sql.catalog.<name>.prefix` — the Iceberg REST resource path segment(s)
    /// between `v1/` and the resource (`v1/<prefix>/namespaces`). Wins over discovery.
    prefix: Option<String>,
    /// Prefix discovered from `GET /v1/config?warehouse=<warehouse>` (`overrides.prefix`),
    /// resolved once on first use. `""` when there is nothing to discover or discovery failed —
    /// the request that needed it then fails with the server's own error, which is more useful
    /// than one invented here.
    discovered_prefix: std::sync::OnceLock<String>,
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
            warehouse: non_empty(options.get("warehouse")),
            token: non_empty(options.get("token")),
            prefix: non_empty(options.get("prefix")),
            discovered_prefix: std::sync::OnceLock::new(),
        })
    }

    /// The Iceberg REST *prefix*: the server-chosen path segment(s) that sit between `v1/` and
    /// the resource, e.g. Unity Catalog's `catalogs/<catalog>` (so a namespace list is
    /// `…/iceberg/v1/catalogs/unity/namespaces`, not `…/iceberg/v1/namespaces`).
    ///
    /// The spec has the client fetch it from `GET /v1/config?warehouse=<warehouse>` and apply
    /// `overrides.prefix` to every later request, which is exactly what a UC OSS server returns
    /// for `warehouse=<catalog name>`. Discovery is skipped entirely when `prefix` was configured
    /// explicitly or no `warehouse` was given, so a plain prefix-less Iceberg REST server issues
    /// the same requests it always did.
    fn prefix(&self) -> &str {
        if let Some(explicit) = &self.prefix {
            return explicit.trim_matches('/');
        }
        let Some(warehouse) = &self.warehouse else {
            return "";
        };
        self.discovered_prefix.get_or_init(|| {
            let encoded = url_encode(warehouse);
            self.get_json(&format!("v1/config?warehouse={encoded}"))
                .ok()
                .and_then(|v| {
                    // `overrides` is what the server *requires*; `defaults` is advisory. The spec
                    // lists `prefix` under overrides, but reading both costs nothing and covers
                    // servers that put it in defaults.
                    ["overrides", "defaults"].iter().find_map(|section| {
                        v.pointer(&format!("/{section}/prefix"))
                            .and_then(|p| p.as_str())
                            .map(|p| p.trim_matches('/').to_string())
                            .filter(|p| !p.is_empty())
                    })
                })
                .unwrap_or_default()
        })
    }

    /// Fetch an Iceberg REST resource *under* the catalog prefix. `suffix` is the path after
    /// `v1/<prefix>/` — `namespaces`, `namespaces/<ns>/tables`, …
    fn curl_json(&self, suffix: &str) -> Result<serde_json::Value> {
        let path = match self.prefix() {
            "" => format!("v1/{suffix}"),
            prefix => format!("v1/{prefix}/{suffix}"),
        };
        self.get_json(&path)
    }

    fn get_json(&self, path: &str) -> Result<serde_json::Value> {
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

/// A configured option, with blank treated as unset. A `spark.sql.catalog.<name>.token=` left
/// empty in a config file must not become an `Authorization: Bearer ` header, and an empty
/// `warehouse` must not trigger prefix discovery for a warehouse named "".
fn non_empty(value: Option<&String>) -> Option<String> {
    value
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Percent-encode a query-parameter value. The catalog crate has no URL dependency and only ever
/// encodes a warehouse name, so this is the minimal unreserved-set escape rather than a general
/// URI encoder: everything outside `A-Za-z0-9-._~` becomes `%XX`.
fn url_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(*byte as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
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
        let v = self.curl_json("namespaces")?;
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
        let path = format!("namespaces/{db}/tables");
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
        let path = format!("namespaces/{db}/tables/{table}");
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
    pub(super) fn spawn_rest_stub() -> u16 {
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
                    // Unity Catalog's Iceberg REST surface: `/v1/config?warehouse=<catalog>`
                    // answers with the prefix every later request must carry, and the resource
                    // paths exist ONLY under it. A client that ignores the prefix 404s — which is
                    // exactly what oxidant did against a real UC OSS 0.6.0 server.
                    let (status, body) = if request.contains("/v1/config?warehouse=unity") {
                        (
                            "200 OK",
                            r#"{"defaults":{},"overrides":{"prefix":"catalogs/unity"}}"#,
                        )
                    } else if request
                        .contains("/v1/catalogs/unity/namespaces/default/tables/marksheet")
                    {
                        (
                            "200 OK",
                            r#"{"metadata":{"location":"file:/tmp/marksheet_uniform"}}"#,
                        )
                    } else if request.contains("/v1/catalogs/unity/namespaces") {
                        ("200 OK", r#"{"namespaces":[["default"],["commerce"]]}"#)
                    } else if request.contains("/tables/ghost") {
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

#[cfg(test)]
mod prefix_tests {
    use super::*;

    /// Unity Catalog OSS 0.6.0 answers `GET /v1/config?warehouse=unity` with
    /// `{"overrides":{"prefix":"catalogs/unity"}}` and serves every resource ONLY under that
    /// prefix. Before this was honored, `spark.sql.catalog.uc.uri=…/iceberg` produced
    /// `…/iceberg/v1/namespaces` → HTTP 404 against a real UC server.
    #[tokio::test]
    async fn a_warehouse_discovers_the_iceberg_rest_prefix() {
        let port = super::tests::spawn_rest_stub();
        let options = HashMap::from([
            ("uri".to_string(), format!("http://127.0.0.1:{port}")),
            ("warehouse".to_string(), "unity".to_string()),
        ]);
        let cat = RestCatalog::from_config("uc", &options).expect("catalog");

        let namespaces = cat.list_namespaces(&[]).await.expect("namespaces");
        assert_eq!(
            namespaces,
            vec![vec!["default".to_string()], vec!["commerce".to_string()]],
            "the listing must come from the prefixed path"
        );
        // The prefix is discovered once and reused — a second call must not re-resolve it into
        // something different.
        assert_eq!(cat.prefix(), "catalogs/unity");
    }

    /// The explicit escape hatch, for a server whose prefix cannot be discovered (or whose
    /// `warehouse` means something else): `spark.sql.catalog.<name>.prefix` is used verbatim and
    /// no `/v1/config` request is made at all.
    #[tokio::test]
    async fn an_explicit_prefix_is_used_without_discovery() {
        let port = super::tests::spawn_rest_stub();
        let options = HashMap::from([
            ("uri".to_string(), format!("http://127.0.0.1:{port}")),
            // Surrounding slashes are a natural way to write it and must not double up.
            ("prefix".to_string(), "/catalogs/unity/".to_string()),
        ]);
        let cat = RestCatalog::from_config("uc", &options).expect("catalog");
        assert_eq!(cat.prefix(), "catalogs/unity");
        assert!(!cat
            .list_namespaces(&[])
            .await
            .expect("namespaces")
            .is_empty());
    }

    /// A plain Iceberg REST server (no warehouse, no prefix) must keep issuing exactly the
    /// requests it always did — `v1/namespaces`, not `v1//namespaces`.
    #[tokio::test]
    async fn a_prefixless_catalog_is_unchanged() {
        let port = super::tests::spawn_rest_stub();
        let options = HashMap::from([("uri".to_string(), format!("http://127.0.0.1:{port}"))]);
        let cat = RestCatalog::from_config("rest", &options).expect("catalog");
        assert_eq!(cat.prefix(), "");
        // The stub's unprefixed `/v1/namespaces/db1/tables/orders` still resolves.
        assert!(cat
            .table_exists(&["db1".to_string()], "orders")
            .await
            .expect("200"));
    }

    /// Blank options are unset, not empty values: an empty `token` must not produce a bare
    /// `Authorization: Bearer` header, and an empty `warehouse` must not trigger discovery.
    #[test]
    fn blank_options_are_treated_as_unset() {
        let options = HashMap::from([
            ("uri".to_string(), "http://example.invalid".to_string()),
            ("token".to_string(), "   ".to_string()),
            ("warehouse".to_string(), "".to_string()),
            ("prefix".to_string(), "".to_string()),
        ]);
        let cat = RestCatalog::from_config("uc", &options).expect("catalog");
        assert!(cat.token.is_none());
        assert!(cat.warehouse.is_none());
        assert!(cat.prefix.is_none());
        // No warehouse → no discovery request, so this cannot hang on an unreachable host.
        assert_eq!(cat.prefix(), "");
    }

    #[test]
    fn warehouse_names_are_percent_encoded_into_the_config_query() {
        assert_eq!(url_encode("unity"), "unity");
        assert_eq!(url_encode("my catalog"), "my%20catalog");
        assert_eq!(url_encode("a/b?c=d"), "a%2Fb%3Fc%3Dd");
    }
}
