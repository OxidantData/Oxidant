//! Configuration for starting a streaming query from Spark Connect.

use std::collections::{BTreeMap, HashMap};

/// How a streaming query is wired: where rows come from, and where they land.
/// What a violated expectation does to the micro-batch that violated it.
///
/// `Drop` is deliberately absent: it is a predicate on the query and never needs a count, so it
/// is composed into the SQL before the batch is ever built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectationAction {
    /// Log the violation count and write the batch anyway.
    Warn,
    /// Abort the batch, leaving the table at its last good version.
    Fail,
}

/// One counted check against a micro-batch.
#[derive(Debug, Clone)]
pub struct StreamExpectation {
    pub label: String,
    pub action: ExpectationAction,
    /// A boolean SQL expression over the batch's own columns.
    pub check: String,
}

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
    /// Counted data-quality checks evaluated against each micro-batch.
    ///
    /// Only `warn` and `fail` live here. A `drop` needs no count — it composes into the query as
    /// a predicate — so it is applied upstream, where it costs nothing extra.
    pub expectations: Vec<StreamExpectation>,
    /// `writeStream.partitionBy(...)` — Hive-style partition columns for the sink table.
    pub partition_columns: Vec<String>,
    /// Publish Iceberg metadata over the Delta table so Iceberg engines can read it. On by
    /// default: a live table is worth more when every engine in the building can query it, and
    /// the metadata costs one small Avro pair per publish, not a second copy of the data.
    pub publish_iceberg: bool,
    /// Suffix for the sibling Iceberg catalog entry (`orders` → `orders_iceberg`).
    pub iceberg_table_suffix: String,
    /// Commits between Delta checkpoints and Iceberg publishes.
    pub checkpoint_interval: u64,
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
            expectations: vec![],
            partition_columns: vec![],
            publish_iceberg: true,
            iceberg_table_suffix: DEFAULT_ICEBERG_SUFFIX.into(),
            checkpoint_interval: oxidant_datasource::delta_write::DEFAULT_CHECKPOINT_INTERVAL,
        }
    }
}

/// Default suffix for the Iceberg catalog entry that mirrors a Delta streaming table.
pub const DEFAULT_ICEBERG_SUFFIX: &str = "_iceberg";

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
    /// Build the config for a declarative pipeline table.
    ///
    /// The sibling of [`from_spark`](Self::from_spark): a streaming query started from a config
    /// file needs the same `StreamQueryConfig` a PySpark client's `writeStream` produces, just
    /// assembled from YAML instead of from a protobuf. Keeping them as two constructors over one
    /// struct is what lets the entire micro-batch engine below this point stay unaware of which
    /// front end asked for the query.
    #[allow(clippy::too_many_arguments)]
    pub fn for_pipeline(
        source_format: &str,
        source_options: BTreeMap<String, String>,
        sink_format: &str,
        sink_table: Option<String>,
        sink_path: Option<String>,
        partition_columns: Vec<String>,
        dedup_columns: Vec<String>,
        publish_iceberg: bool,
        iceberg_table_suffix: Option<String>,
        checkpoint_interval: Option<u64>,
    ) -> Self {
        let defaults = Self::default();
        Self {
            source_format: source_format.to_string(),
            source_options,
            sink_format: sink_format.to_string(),
            sink_table,
            sink_path,
            output_mode: "append".into(),
            dedup_columns,
            expectations: vec![],
            partition_columns,
            publish_iceberg,
            iceberg_table_suffix: iceberg_table_suffix
                .filter(|s| !s.trim().is_empty())
                .unwrap_or(defaults.iceberg_table_suffix),
            checkpoint_interval: checkpoint_interval
                .filter(|n| *n > 0)
                .unwrap_or(defaults.checkpoint_interval),
        }
    }

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
        partition_columns: Vec<String>,
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
            expectations: vec![],
            dedup_columns: writer_options
                .get("dedupColumns")
                .map(|s| {
                    s.split(',')
                        .map(|c| c.trim().to_string())
                        .filter(|c| !c.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            partition_columns,
            publish_iceberg: writer_options
                .get("icebergCompat")
                .or_else(|| writer_options.get("uniform"))
                .map(|v| {
                    !matches!(
                        v.trim().to_ascii_lowercase().as_str(),
                        "false" | "0" | "off"
                    )
                })
                .unwrap_or(true),
            iceberg_table_suffix: writer_options
                .get("icebergTableSuffix")
                .map(|s| s.trim())
                // An empty suffix would name the Iceberg entry exactly what the Delta entry is
                // called, so the two would be the same catalog row — the Delta table would end up
                // carrying an Iceberg `metadata_location`. Fall back rather than let a stray
                // `icebergTableSuffix=` collapse the pair.
                .filter(|s| !s.is_empty())
                .map_or_else(|| DEFAULT_ICEBERG_SUFFIX.to_string(), str::to_string),
            checkpoint_interval: writer_options
                .get("checkpointInterval")
                .map(|raw| {
                    raw.trim().parse().unwrap_or_else(|_| {
                        // Silently defaulting a value the user explicitly set means their
                        // configured cadence never takes effect and nothing says so.
                        eprintln!(
                            "[oxidant] streaming: checkpointInterval `{raw}` is not a number — \
                             using the default of {}",
                            oxidant_datasource::delta_write::DEFAULT_CHECKPOINT_INTERVAL
                        );
                        oxidant_datasource::delta_write::DEFAULT_CHECKPOINT_INTERVAL
                    })
                })
                .unwrap_or(oxidant_datasource::delta_write::DEFAULT_CHECKPOINT_INTERVAL),
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
            vec![],
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
            vec![],
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
            vec![],
        );
        assert_eq!(cfg.sink_format, "delta");
    }

    #[test]
    fn iceberg_publishing_is_on_by_default_and_can_be_turned_off() {
        let on = StreamQueryConfig::from_spark(
            "kafka",
            &map(&[]),
            "delta",
            SinkDestination::Table("glue.live.events".into()),
            &map(&[]),
            vec![],
        );
        assert!(on.publish_iceberg, "interoperability is the default");
        assert_eq!(on.iceberg_table_suffix, "_iceberg");

        let off = StreamQueryConfig::from_spark(
            "kafka",
            &map(&[]),
            "delta",
            SinkDestination::Table("glue.live.events".into()),
            &map(&[("icebergCompat", "false")]),
            vec![],
        );
        assert!(!off.publish_iceberg);
    }

    #[test]
    fn partition_columns_come_from_partition_by_not_an_option() {
        let cfg = StreamQueryConfig::from_spark(
            "kafka",
            &map(&[]),
            "delta",
            SinkDestination::Table("glue.live.events".into()),
            &map(&[]),
            vec!["event_date".into()],
        );
        assert_eq!(cfg.partition_columns, vec!["event_date".to_string()]);
    }

    #[test]
    fn a_path_option_is_honoured_when_no_destination_was_given() {
        let cfg = StreamQueryConfig::from_spark(
            "rate",
            &map(&[]),
            "parquet",
            SinkDestination::None,
            &map(&[("path", "/out")]),
            vec![],
        );
        assert_eq!(cfg.sink_path.as_deref(), Some("/out"));
    }
    /// An empty suffix would name the Iceberg entry exactly what the Delta entry is called, so the
    /// pair would collapse into one catalog row and the Delta table would carry an Iceberg
    /// `metadata_location`.
    #[test]
    fn an_empty_iceberg_suffix_falls_back_rather_than_colliding_with_the_delta_table() {
        for value in ["", "   "] {
            let cfg = StreamQueryConfig::from_spark(
                "kafka",
                &HashMap::new(),
                "delta",
                SinkDestination::None,
                &[("icebergTableSuffix".to_string(), value.to_string())]
                    .into_iter()
                    .collect(),
                vec![],
            );
            assert_eq!(
                cfg.iceberg_table_suffix, DEFAULT_ICEBERG_SUFFIX,
                "`icebergTableSuffix={value:?}` must not collapse the two catalog entries"
            );
        }
    }

    #[test]
    fn a_custom_iceberg_suffix_is_honoured() {
        let cfg = StreamQueryConfig::from_spark(
            "kafka",
            &HashMap::new(),
            "delta",
            SinkDestination::None,
            &[("icebergTableSuffix".to_string(), "_ice".to_string())]
                .into_iter()
                .collect(),
            vec![],
        );
        assert_eq!(cfg.iceberg_table_suffix, "_ice");
    }

    /// A `checkpointInterval` that does not parse must not silently become the default with the
    /// user believing their value took effect.
    #[test]
    fn a_checkpoint_interval_is_parsed_and_a_bad_one_falls_back() {
        let good = StreamQueryConfig::from_spark(
            "kafka",
            &HashMap::new(),
            "delta",
            SinkDestination::None,
            &[("checkpointInterval".to_string(), " 3 ".to_string())]
                .into_iter()
                .collect(),
            vec![],
        );
        assert_eq!(good.checkpoint_interval, 3, "a padded number still parses");

        let bad = StreamQueryConfig::from_spark(
            "kafka",
            &HashMap::new(),
            "delta",
            SinkDestination::None,
            &[("checkpointInterval".to_string(), "often".to_string())]
                .into_iter()
                .collect(),
            vec![],
        );
        assert_eq!(
            bad.checkpoint_interval,
            oxidant_datasource::delta_write::DEFAULT_CHECKPOINT_INTERVAL
        );
    }
}
