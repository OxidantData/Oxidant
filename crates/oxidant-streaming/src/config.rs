//! Configuration for starting a streaming query from Spark Connect.

use std::collections::{BTreeMap, HashMap};

/// How a streaming query is wired: where rows come from, and where they land.
#[derive(Debug, Clone)]
pub struct StreamQueryConfig {
    /// `readStream.format(...)` — `kafka`, `parquet`, `json`, `csv`, `rate`, or `memory`.
    pub source_format: String,
    /// `readStream.option(...)`. Ordered so the derived streaming-input table name
    /// ([`crate::stream_input_name`]) is stable across runs.
    pub source_options: BTreeMap<String, String>,
    /// `writeStream.format(...)` — `delta`, `parquet`, `json`, `csv`, or `memory`.
    pub sink_format: String,
    /// `writeStream.toTable("catalog.db.table")`.
    pub sink_table: Option<String>,
    /// `writeStream.start(path)`.
    pub sink_path: Option<String>,
    pub output_mode: String,
    /// Optional dedup key columns (comma-separated `dedupColumns` option).
    pub dedup_columns: Vec<String>,
}

impl Default for StreamQueryConfig {
    fn default() -> Self {
        Self {
            source_format: "memory".into(),
            source_options: BTreeMap::new(),
            sink_format: "memory".into(),
            sink_table: None,
            sink_path: None,
            output_mode: "append".into(),
            dedup_columns: vec![],
        }
    }
}

/// Where a `WriteStreamOperationStart` sends its output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkDestination {
    /// `writeStream.toTable(identifier)` — a catalog table.
    Table(String),
    /// `writeStream.start(path)` — a path with no catalog entry.
    Path(String),
    /// Neither was given (the `memory`/`console` shapes).
    None,
}

impl StreamQueryConfig {
    /// Build the config from what Spark Connect's `WriteStreamOperationStart` carries.
    ///
    /// `source_format`/`source_options` come from the streaming `Read` at the bottom of the
    /// query's relation tree, NOT from the writer — the two used to be conflated, which made a
    /// `readStream.format("kafka") … writeStream.format("delta")` pipeline read Delta and write
    /// Kafka options.
    pub fn from_spark(
        source_format: &str,
        source_options: &HashMap<String, String>,
        sink_format: &str,
        destination: SinkDestination,
        writer_options: &HashMap<String, String>,
    ) -> Self {
        // `toTable(...)` still honours an explicit `path` option — that is how Spark lets a
        // managed-looking write land at a location the catalog's warehouse convention would not
        // have chosen. Without it the option was silently dropped.
        let (sink_table, sink_path) = match destination {
            SinkDestination::Table(t) => (Some(t), writer_options.get("path").cloned()),
            SinkDestination::Path(p) => (None, Some(p)),
            SinkDestination::None => (None, writer_options.get("path").cloned()),
        };
        Self {
            source_format: if source_format.is_empty() {
                "memory".into()
            } else {
                source_format.to_string()
            },
            source_options: source_options
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            sink_format: if sink_format.is_empty() {
                // `writeStream.toTable(...)` without a format is Delta: the only sink that
                // commits atomically, so it is the right default for a live table.
                if sink_table.is_some() {
                    "delta".into()
                } else {
                    "memory".into()
                }
            } else {
                sink_format.to_string()
            },
            sink_table,
            sink_path,
            output_mode: writer_options
                .get("outputMode")
                .cloned()
                .unwrap_or_else(|| "append".into()),
            dedup_columns: writer_options
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

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn source_and_sink_formats_stay_separate() {
        let cfg = StreamQueryConfig::from_spark(
            "kafka",
            &map(&[("subscribe", "events")]),
            "delta",
            SinkDestination::Table("glue.live.events".into()),
            &map(&[]),
        );
        assert_eq!(cfg.source_format, "kafka");
        assert_eq!(cfg.sink_format, "delta");
        assert_eq!(cfg.source_options.get("subscribe").unwrap(), "events");
        assert_eq!(cfg.sink_table.as_deref(), Some("glue.live.events"));
        assert!(cfg.sink_path.is_none());
    }

    #[test]
    fn to_table_honours_an_explicit_path_option() {
        let cfg = StreamQueryConfig::from_spark(
            "kafka",
            &map(&[]),
            "delta",
            SinkDestination::Table("glue.live.events".into()),
            &map(&[("path", "s3://bucket/custom/events")]),
        );
        assert_eq!(cfg.sink_table.as_deref(), Some("glue.live.events"));
        assert_eq!(cfg.sink_path.as_deref(), Some("s3://bucket/custom/events"));
    }

    #[test]
    fn to_table_without_a_format_defaults_to_delta() {
        let cfg = StreamQueryConfig::from_spark(
            "kafka",
            &map(&[]),
            "",
            SinkDestination::Table("glue.live.events".into()),
            &map(&[]),
        );
        assert_eq!(cfg.sink_format, "delta");
    }

    #[test]
    fn a_path_option_is_honoured_when_no_destination_was_given() {
        let cfg = StreamQueryConfig::from_spark(
            "rate",
            &map(&[]),
            "parquet",
            SinkDestination::None,
            &map(&[("path", "/out")]),
        );
        assert_eq!(cfg.sink_path.as_deref(), Some("/out"));
    }
}
