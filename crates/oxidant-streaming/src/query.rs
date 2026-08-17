//! Streaming query identity and lifecycle state.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique streaming query identity (`id` persists across restarts; `run_id` changes per run).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamingQueryId {
    pub id: String,
    pub run_id: String,
}

impl Default for StreamingQueryId {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamingQueryId {
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            run_id: Uuid::new_v4().to_string(),
        }
    }
}

/// Runtime status of a streaming query.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryStatus {
    pub is_active: bool,
    pub is_data_available: bool,
    pub is_trigger_active: bool,
    pub message: String,
}

/// One micro-batch progress report.
///
/// Serialized into `StreamingQueryCommandResult.recent_progress_json`, which a Spark client hands
/// straight back to the user as `query.lastProgress`. So the JSON has to be *Spark's* shape —
/// `batchId`, `numInputRows`, and a `sources` array — not this struct's Rust field names, or every
/// client that reads a documented key gets `None`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryProgress {
    pub id: String,
    pub run_id: String,
    pub name: String,
    pub batch_id: u64,
    pub num_input_rows: u64,
    /// Rows read per second of wall clock. This used to carry the raw row *count*, so anything
    /// scraping the documented key charted a number that was not a rate.
    pub input_rows_per_second: f64,
    /// Rows written per second of wall clock.
    pub processed_rows_per_second: f64,
    /// How long the batch took end to end — poll, transform, and commit.
    pub duration_ms: u64,
    /// One entry per streaming source. Oxidant runs one source per query, so this always has
    /// exactly one element — but it is an array because Spark's is.
    pub sources: Vec<SourceProgress>,
    pub sink: SinkProgress,
}

/// Per-source progress inside a [`QueryProgress`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceProgress {
    /// Spark's source description — e.g. `KafkaV2[Subscribe[events]]`.
    pub description: String,
    pub num_input_rows: u64,
    pub input_rows_per_second: f64,
}

/// Where a batch landed, as `lastProgress.sink`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SinkProgress {
    /// e.g. `deltaSink[glue.streaming_live.orders]`.
    pub description: String,
    pub num_output_rows: u64,
}

/// A registered streaming query with its configuration.
#[derive(Debug, Clone)]
pub struct StreamingQuery {
    pub query_id: StreamingQueryId,
    pub name: String,
    pub source_path: Option<String>,
    pub sink_path: Option<String>,
    pub format: String,
    pub output_mode: String,
    pub checkpoint_location: String,
    pub status: QueryStatus,
    pub last_progress: Option<QueryProgress>,
    pub batch_id: u64,
}

impl StreamingQuery {
    pub fn new(name: String, checkpoint_location: String) -> Self {
        Self {
            query_id: StreamingQueryId::new(),
            name,
            source_path: None,
            sink_path: None,
            format: "parquet".into(),
            output_mode: "append".into(),
            checkpoint_location,
            status: QueryStatus {
                is_active: true,
                message: "initialized".into(),
                ..Default::default()
            },
            last_progress: None,
            batch_id: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_serializes_with_sparks_key_names() {
        // A Spark client hands this JSON back as `query.lastProgress`, so a client reading a
        // documented key like `batchId` must find it — not our snake_case field name.
        let progress = QueryProgress {
            id: "q".into(),
            run_id: "r".into(),
            name: "orders".into(),
            batch_id: 4,
            num_input_rows: 120,
            input_rows_per_second: 60.0,
            processed_rows_per_second: 60.0,
            duration_ms: 2_000,
            sources: vec![SourceProgress {
                description: "KafkaV2[Subscribe[orders]]".into(),
                num_input_rows: 120,
                input_rows_per_second: 60.0,
            }],
            sink: SinkProgress {
                description: "deltaSink[glue.live.orders]".into(),
                num_output_rows: 120,
            },
        };
        let json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&progress).unwrap()).unwrap();
        assert_eq!(json["batchId"], 4);
        assert_eq!(json["runId"], "r");
        assert_eq!(json["numInputRows"], 120);
        assert_eq!(json["processedRowsPerSecond"], 60.0);
        assert_eq!(
            json["sources"][0]["description"],
            "KafkaV2[Subscribe[orders]]"
        );
        assert_eq!(json["sources"][0]["numInputRows"], 120);
        assert_eq!(json["durationMs"], 2_000);
        assert_eq!(json["sink"]["numOutputRows"], 120);
        assert!(json.get("batch_id").is_none(), "no snake_case leakage");
    }
}
