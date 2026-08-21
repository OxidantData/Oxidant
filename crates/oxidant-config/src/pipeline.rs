//! The declarative pipeline: a bronze→silver→gold table DAG the binary runs headless.
//!
//! Two kinds of table, distinguished by whether the entry declares a `source:`:
//!
//! - a **streaming table** reads a source (Kafka) and appends each micro-batch to a lakehouse
//!   table — the shape `oxidant-streaming` already implements;
//! - a **derived table** is defined purely by SQL over other declared tables, and is
//!   materialized by full recompute per update.
//!
//! Full recompute is the honest semantic for v1: it is always correct and needs no
//! cross-batch state, which the engine does not have. It is also O(full table) per update —
//! see `docs/pipelines.md` before pointing a fast trigger at a large aggregate.

use std::collections::BTreeMap;
use std::time::Duration;

use oxidant_common::{Error, Result};
use serde::{Deserialize, Serialize};

/// Pipeline-wide settings: where materialized tables land and how often the DAG updates.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct PipelineConfig {
    /// Pipeline name, used in logs and as the default state-file name.
    pub name: String,
    /// Catalog the materialized tables are registered in. Must be declared in `catalogs:`
    /// and must support write DDL (`local` or `glue` today).
    pub catalog: String,
    /// Target database. Created if missing.
    pub schema: String,
    /// Root the tables are written under. Defaults to the catalog's warehouse convention.
    #[serde(default)]
    pub storage: Option<String>,
    /// Root for per-table streaming checkpoints.
    ///
    /// This is the source of truth for replay position — not the broker — so it must be
    /// durable and must not be shared between two pipelines.
    pub checkpoints: String,
    /// How often the DAG updates.
    #[serde(default)]
    pub trigger: Trigger,
    /// Default sink format for tables that do not name one.
    #[serde(default = "default_format")]
    pub format: String,
    /// Publish Iceberg metadata over Delta tables so Iceberg engines can read them.
    #[serde(default = "default_true")]
    pub iceberg_compat: bool,
}

fn default_format() -> String {
    "delta".to_string()
}

fn default_true() -> bool {
    true
}

/// How often the pipeline runs one pass over the DAG.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Trigger {
    /// Fire every interval, on a fixed schedule.
    ProcessingTime(Duration),
    /// Drain everything available, then stop.
    Once,
    /// Drain everything available, then go inactive.
    AvailableNow,
}

impl Default for Trigger {
    fn default() -> Self {
        Trigger::ProcessingTime(Duration::from_secs(30))
    }
}

impl Trigger {
    /// Parse `once`, `available_now`, or a duration like `30 seconds` / `500ms` / `5 minutes`.
    pub fn parse(text: &str) -> Result<Self> {
        let normalized = text.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "once" => return Ok(Trigger::Once),
            "available_now" | "availablenow" | "available now" => return Ok(Trigger::AvailableNow),
            _ => {}
        }
        parse_duration(&normalized)
            .map(Trigger::ProcessingTime)
            .ok_or_else(|| {
                Error::Io(format!(
                    "unrecognized trigger `{text}` (expected `once`, `available_now`, or an \
                     interval like `30 seconds`, `5 minutes`, `500ms`)"
                ))
            })
    }
}

/// Parse `500ms` / `30 seconds` / `5 minutes` / `1 hour`, with or without a space.
///
/// Rejects a bare number rather than guessing a unit: `trigger: 30` meaning 30 milliseconds
/// when the author meant seconds is a 1000x error that would only show up as a broker
/// hammering in production.
fn parse_duration(text: &str) -> Option<Duration> {
    let split = text
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .filter(|&i| i > 0)?;
    let (value, unit) = text.split_at(split);
    let value: f64 = value.parse().ok()?;
    if !value.is_finite() || value < 0.0 {
        return None;
    }
    let seconds = match unit.trim() {
        "ms" | "millisecond" | "milliseconds" => value / 1000.0,
        "s" | "sec" | "secs" | "second" | "seconds" => value,
        "m" | "min" | "mins" | "minute" | "minutes" => value * 60.0,
        "h" | "hour" | "hours" => value * 3600.0,
        _ => return None,
    };
    Some(Duration::from_secs_f64(seconds))
}

impl Serialize for Trigger {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let text = match self {
            Trigger::Once => "once".to_string(),
            Trigger::AvailableNow => "available_now".to_string(),
            Trigger::ProcessingTime(d) => format!("{}ms", d.as_millis()),
        };
        serializer.serialize_str(&text)
    }
}

impl<'de> Deserialize<'de> for Trigger {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Trigger::parse(&text).map_err(serde::de::Error::custom)
    }
}

/// AUTO CDC merge options (SCD Type 1 only today).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct AutoCdcConfig {
    /// Pipeline-local name of the streaming CDC source table or view.
    pub source: String,
    /// Key column expressions (SQL), in declaration order.
    pub keys: Vec<String>,
    /// Expression defining event order within a key.
    pub sequence_by: String,
    /// Rows matching this expression are treated as deletes for their key.
    #[serde(default)]
    pub apply_as_deletes: Option<String>,
    /// Rows matching this expression truncate the target before applying the batch.
    #[serde(default)]
    pub apply_as_truncates: Option<String>,
    /// Explicit output columns; mutually exclusive with [`except_column_list`](Self::except_column_list).
    #[serde(default)]
    pub column_list: Option<Vec<String>>,
    /// Output is all source columns except these; mutually exclusive with [`column_list`](Self::column_list).
    #[serde(default)]
    pub except_column_list: Option<Vec<String>>,
    /// On update, do not overwrite a non-null target value with a null batch value.
    #[serde(default)]
    pub ignore_null_updates_columns: Option<Vec<String>>,
    /// Ignore-null-updates for all columns except these.
    #[serde(default)]
    pub ignore_null_updates_except: Option<Vec<String>>,
}

/// One append flow into a declared table (SDP multi-flow semantics).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct AppendFlow {
    pub sql: String,
    /// When true, map query columns to the declared output schema by name rather than position.
    #[serde(default)]
    pub by_name: bool,
}

/// One declared table in the DAG.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct TableConfig {
    /// Table name, unqualified. Materialized as `{pipeline.catalog}.{pipeline.schema}.{name}`.
    pub name: String,
    /// Streaming source. Present makes this a streaming table.
    #[serde(default)]
    pub source: Option<SourceConfig>,
    /// The transformation.
    ///
    /// For a streaming table this is optional and runs over the source, which is in scope as
    /// `stream`. For a derived table it is required and references other declared tables.
    /// When [`append_flows`](Self::append_flows) is non-empty, this is the first flow and the
    /// rest are unioned with it in declaration order.
    #[serde(default)]
    pub sql: Option<String>,
    /// Whether the primary [`sql`](Self::sql) flow uses `INSERT BY NAME` column matching.
    #[serde(default)]
    pub sql_by_name: bool,
    /// Additional flows appended into this table within the same pass (`UNION ALL`).
    #[serde(default)]
    pub append_flows: Vec<AppendFlow>,
    /// Declared output schema DDL, e.g. `(id INT, name STRING)`. Enforced at write (cast-or-fail).
    #[serde(default)]
    pub output_schema: Option<String>,
    /// Hive-style partition columns.
    ///
    /// The single biggest lever on read cost. Partition on something queries filter on that
    /// does not explode in cardinality — a date, a region, a tenant; never a raw timestamp
    /// or an id.
    #[serde(default)]
    pub partition_by: Vec<String>,
    /// Sink format. Defaults to the pipeline's.
    #[serde(default)]
    pub format: Option<String>,
    /// Publish Iceberg metadata over this table. Defaults to the pipeline's setting.
    #[serde(default)]
    pub iceberg_compat: Option<bool>,
    /// Suffix for the sibling Iceberg catalog entry (`orders` → `orders_iceberg`).
    #[serde(default)]
    pub iceberg_table_suffix: Option<String>,
    /// Commits between Delta checkpoints and Iceberg publishes.
    #[serde(default)]
    pub checkpoint_interval: Option<u64>,
    /// Deduplicate on these columns within a bounded window. Streaming tables only.
    #[serde(default)]
    pub dedup_columns: Vec<String>,
    /// When set, micro-batches from [`AutoCdcConfig::source`] are merged into this table (SCD1).
    #[serde(default)]
    pub auto_cdc: Option<AutoCdcConfig>,
    /// Data-quality constraints, keyed by name.
    #[serde(default)]
    pub expect: BTreeMap<String, Expectation>,
    /// Table comment recorded in the catalog.
    #[serde(default)]
    pub comment: Option<String>,
    /// Path-only write target for an SDP external sink: the table is written to this location
    /// and never registered in the catalog.
    ///
    /// Not a YAML key. It is set only when a Spark Connect `DefineOutput` with
    /// `output_type=SINK` is lowered to a pipeline config — a sink has no catalog identity, so
    /// exposing it in `oxidant.yaml` would be a second, unvalidated way to write an
    /// unregistered table.
    #[serde(skip)]
    pub write_path: Option<String>,
}

impl TableConfig {
    /// Whether this is a streaming, AUTO CDC, or derived table.
    pub fn kind(&self) -> TableKind {
        if self.auto_cdc.is_some() {
            TableKind::AutoCdc
        } else if self.source.is_some() {
            TableKind::Streaming
        } else {
            TableKind::Derived
        }
    }
}

/// Which table kind an entry declares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableKind {
    /// Reads a streaming source; appended per micro-batch.
    Streaming,
    /// Streaming CDC merge into a managed table (SCD Type 1).
    AutoCdc,
    /// Defined by SQL over other tables; fully recomputed per update.
    Derived,
}

/// A streaming source.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct SourceConfig {
    /// `kafka` | `json` | `csv` | `parquet` | `rate` | `memory`.
    pub format: String,
    /// Source options, passed through verbatim — the same names Spark uses
    /// (`kafka.bootstrap.servers`, `subscribe`, `startingOffsets`, `maxOffsetsPerTrigger`).
    #[serde(default)]
    pub options: BTreeMap<String, String>,
}

/// A data-quality constraint on a table's output.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct Expectation {
    /// A boolean SQL expression over the table's own output columns.
    pub check: String,
    /// What to do with rows that fail it.
    #[serde(default)]
    pub action: ExpectAction,
}

/// What a failed [`Expectation`] does.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpectAction {
    /// Count the violations and log them; write every row.
    ///
    /// The default, and deliberately not `Fail`: a constraint added to an already-running
    /// pipeline should surface the problem before it stops the ingestion someone is relying on.
    #[default]
    Warn,
    /// Filter the failing rows out before writing.
    Drop,
    /// Abort the update, leaving the table at its last good version.
    Fail,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn triggers_parse_the_documented_spellings() {
        assert_eq!(Trigger::parse("once").unwrap(), Trigger::Once);
        assert_eq!(
            Trigger::parse("available_now").unwrap(),
            Trigger::AvailableNow
        );
        assert_eq!(
            Trigger::parse("30 seconds").unwrap(),
            Trigger::ProcessingTime(Duration::from_secs(30))
        );
        assert_eq!(
            Trigger::parse("5 minutes").unwrap(),
            Trigger::ProcessingTime(Duration::from_secs(300))
        );
        assert_eq!(
            Trigger::parse("500ms").unwrap(),
            Trigger::ProcessingTime(Duration::from_millis(500))
        );
        assert_eq!(
            Trigger::parse("1 hour").unwrap(),
            Trigger::ProcessingTime(Duration::from_secs(3600))
        );
    }

    #[test]
    fn a_bare_number_is_rejected_rather_than_assigned_a_unit() {
        // Guessing milliseconds when the author meant seconds is a 1000x error that only
        // shows up as a hammered broker in production.
        assert!(Trigger::parse("30").is_err());
        assert!(Trigger::parse("").is_err());
        assert!(Trigger::parse("30 fortnights").is_err());
    }

    #[test]
    fn trigger_round_trips_through_serde() {
        for trigger in [
            Trigger::Once,
            Trigger::AvailableNow,
            Trigger::ProcessingTime(Duration::from_secs(30)),
        ] {
            let yaml = serde_norway::to_string(&trigger).expect("serializes");
            let back: Trigger = serde_norway::from_str(&yaml).expect("deserializes");
            assert_eq!(trigger, back);
        }
    }

    #[test]
    fn table_kind_follows_the_presence_of_a_source() {
        let streaming = TableConfig {
            name: "bronze".into(),
            source: Some(SourceConfig {
                format: "kafka".into(),
                options: BTreeMap::new(),
            }),
            sql: None,
            sql_by_name: false,
            append_flows: vec![],
            output_schema: None,
            partition_by: vec![],
            format: None,
            iceberg_compat: None,
            iceberg_table_suffix: None,
            checkpoint_interval: None,
            dedup_columns: vec![],
            auto_cdc: None,
            expect: BTreeMap::new(),
            comment: None,
            write_path: None,
        };
        assert_eq!(streaming.kind(), TableKind::Streaming);
        let derived = TableConfig {
            source: None,
            sql: Some("SELECT 1".into()),
            ..streaming
        };
        assert_eq!(derived.kind(), TableKind::Derived);
        let cdc = TableConfig {
            source: Some(SourceConfig {
                format: "kafka".into(),
                options: BTreeMap::new(),
            }),
            auto_cdc: Some(AutoCdcConfig {
                source: "bronze".into(),
                keys: vec!["id".into()],
                sequence_by: "seq".into(),
                apply_as_deletes: None,
                apply_as_truncates: None,
                column_list: None,
                except_column_list: None,
                ignore_null_updates_columns: None,
                ignore_null_updates_except: None,
            }),
            ..derived.clone()
        };
        assert_eq!(cdc.kind(), TableKind::AutoCdc);
    }

    #[test]
    fn an_expectation_defaults_to_warning_not_failing() {
        let expectation: Expectation = serde_norway::from_str("check: amount > 0").expect("parses");
        assert_eq!(expectation.action, ExpectAction::Warn);
    }
}
