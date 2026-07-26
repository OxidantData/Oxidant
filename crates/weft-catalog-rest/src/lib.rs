//! Iceberg REST / Unity Catalog-compatible catalog provider over HTTP.

use std::collections::HashMap;
use std::process::Command;

use async_trait::async_trait;
use weft_catalog::{CatalogProvider, TableFormat, TableMetadata};
use weft_common::{Error, Result};

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
        cmd.args(["-sfS", "-H", "Accept: application/json"]);
        if let Some(tok) = &self.token {
            cmd.args(["-H", &format!("Authorization: Bearer {tok}")]);
        }
        cmd.arg(&url);
        let out = cmd
            .output()
            .map_err(|e| Error::Io(format!("curl {url}: {e}")))?;
        if !out.status.success() {
            return Err(Error::Io(format!(
                "catalog GET {url}: {}",
                String::from_utf8_lossy(&out.stderr)
            )));
        }
        serde_json::from_slice(&out.stdout)
            .map_err(|e| Error::Io(format!("catalog json {url}: {e}")))
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
