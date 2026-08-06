//! Configuration for starting a streaming query from Spark Connect.

use std::collections::HashMap;

/// How a streaming query is wired (source format/options + sink destination).
#[derive(Debug, Clone)]
pub struct StreamQueryConfig {
    pub source_format: String,
    pub source_options: HashMap<String, String>,
    pub sink_path: Option<String>,
    pub sink_format: String,
    pub output_mode: String,
    /// Optional dedup key columns (comma-separated).
    pub dedup_columns: Vec<String>,
}

impl Default for StreamQueryConfig {
    fn default() -> Self {
        Self {
            source_format: "memory".into(),
            source_options: HashMap::new(),
            sink_path: None,
            sink_format: "memory".into(),
            output_mode: "append".into(),
            dedup_columns: vec![],
        }
    }
}

impl StreamQueryConfig {
    pub fn from_spark(
        format: &str,
        options: &HashMap<String, String>,
        path: Option<String>,
    ) -> Self {
        let source_format = if format.is_empty() {
            options
                .get("source")
                .cloned()
                .unwrap_or_else(|| "memory".into())
        } else {
            format.to_string()
        };
        Self {
            source_format: source_format.clone(),
            source_options: options.clone(),
            sink_path: path.or_else(|| options.get("path").cloned()),
            sink_format: if format.is_empty() {
                "memory".into()
            } else {
                format.to_string()
            },
            output_mode: options
                .get("outputMode")
                .cloned()
                .unwrap_or_else(|| "append".into()),
            dedup_columns: options
                .get("dedupColumns")
                .map(|s| {
                    s.split(',')
                        .map(|c| c.trim().to_string())
                        .filter(|c| !c.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
        }
    }
}
