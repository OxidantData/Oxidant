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
    /// When the batch ran, as Spark's ISO-8601 UTC instant.
    ///
    /// Required, not decorative: `StreamingQueryProgress.fromJson` reads this key unguarded, so a
    /// progress record without it raises `KeyError` inside the client the moment a program touches
    /// `query.lastProgress` — which every `while query.isActive` loop does.
    pub timestamp: String,
    pub batch_id: u64,
    /// How long the batch took end to end — poll, transform, and commit.
    ///
    /// Spelled `batchDuration` because Spark's `durationMs` is a *map* of phase to milliseconds,
    /// and the client calls `dict()` on it; a scalar under that name is a `TypeError`.
    pub batch_duration: u64,
    pub num_input_rows: u64,
    /// Rows read per second of wall clock. This used to carry the raw row *count*, so anything
    /// scraping the documented key charted a number that was not a rate.
    pub input_rows_per_second: f64,
    /// Rows written per second of wall clock.
    pub processed_rows_per_second: f64,
    /// Always empty: Oxidant's streaming has no stateful operators. Present because the client
    /// iterates this key without checking for it.
    pub state_operators: Vec<StateOperatorProgress>,
    /// One entry per streaming source. Oxidant runs one source per query, so this always has
    /// exactly one element — but it is an array because Spark's is.
    pub sources: Vec<SourceProgress>,
    pub sink: SinkProgress,
}

/// Progress of a stateful operator. Oxidant has none, so this exists only to give the `sources`-
/// style array a element type; it is never populated.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateOperatorProgress {
    pub operator_name: String,
    pub num_rows_total: u64,
    pub num_rows_updated: u64,
    pub num_rows_removed: u64,
    pub all_updates_time_ms: u64,
    pub all_removals_time_ms: u64,
    pub commit_time_ms: u64,
    pub memory_used_bytes: u64,
    pub num_rows_dropped_by_watermark: u64,
    pub num_shuffle_partitions: u64,
    pub num_state_store_instances: u64,
}

/// Per-source progress inside a [`QueryProgress`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceProgress {
    /// Spark's source description — e.g. `KafkaV2[Subscribe[events]]`.
    pub description: String,
    /// The source position this batch began at, ended at, and the furthest position the source
    /// knows about — each a JSON string, as Spark writes them. All three are required keys.
    pub start_offset: String,
    pub end_offset: String,
    pub latest_offset: String,
    pub num_input_rows: u64,
    pub input_rows_per_second: f64,
    pub processed_rows_per_second: f64,
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

    fn sample_progress() -> QueryProgress {
        QueryProgress {
            id: "8f2c1a1e-0000-4000-8000-000000000001".into(),
            run_id: "8f2c1a1e-0000-4000-8000-000000000002".into(),
            name: "orders".into(),
            timestamp: "2026-08-17T18:22:08.431Z".into(),
            batch_id: 4,
            batch_duration: 2_000,
            num_input_rows: 120,
            input_rows_per_second: 60.0,
            processed_rows_per_second: 60.0,
            state_operators: Vec::new(),
            sources: vec![SourceProgress {
                description: "KafkaV2[Subscribe[orders]]".into(),
                start_offset: "{\"orders-0\":0}".into(),
                end_offset: "{\"orders-0\":120}".into(),
                latest_offset: "{\"orders-0\":120}".into(),
                num_input_rows: 120,
                input_rows_per_second: 60.0,
                processed_rows_per_second: 60.0,
            }],
            sink: SinkProgress {
                description: "deltaSink[glue.live.orders]".into(),
                num_output_rows: 120,
            },
        }
    }

    #[test]
    fn progress_serializes_with_sparks_key_names() {
        // A Spark client hands this JSON back as `query.lastProgress`, so a client reading a
        // documented key like `batchId` must find it — not our snake_case field name.
        let json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&sample_progress()).unwrap()).unwrap();
        assert_eq!(json["batchId"], 4);
        assert_eq!(json["runId"], "8f2c1a1e-0000-4000-8000-000000000002");
        assert_eq!(json["numInputRows"], 120);
        assert_eq!(json["processedRowsPerSecond"], 60.0);
        assert_eq!(
            json["sources"][0]["description"],
            "KafkaV2[Subscribe[orders]]"
        );
        assert_eq!(json["sources"][0]["numInputRows"], 120);
        assert_eq!(json["batchDuration"], 2_000);
        assert_eq!(json["sink"]["numOutputRows"], 120);
        assert!(json.get("batch_id").is_none(), "no snake_case leakage");
    }

    /// `StreamingQueryProgress.fromJson` reads these keys **without** an `in j` guard, so a missing
    /// one is a `KeyError` raised inside the client the first time a program touches
    /// `query.lastProgress` — which is the body of every `while query.isActive` loop. Serializing
    /// successfully proves nothing; only the key list does.
    #[test]
    fn progress_carries_every_key_a_pyspark_client_reads_unguarded() {
        let json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&sample_progress()).unwrap()).unwrap();

        for key in [
            "id",
            "runId",
            "name",
            "timestamp",
            "batchId",
            "batchDuration",
            "stateOperators",
            "sources",
            "sink",
        ] {
            assert!(json.get(key).is_some(), "progress is missing `{key}`");
        }
        for key in [
            "description",
            "startOffset",
            "endOffset",
            "latestOffset",
            "numInputRows",
            "inputRowsPerSecond",
            "processedRowsPerSecond",
        ] {
            assert!(
                json["sources"][0].get(key).is_some(),
                "source progress is missing `{key}`"
            );
        }
        for key in ["description", "numOutputRows"] {
            assert!(
                json["sink"].get(key).is_some(),
                "sink progress is missing `{key}`"
            );
        }

        // `durationMs` is a *map* of phase to milliseconds in Spark and the client calls `dict()`
        // on it, so emitting a scalar under that name would be a TypeError rather than a missing
        // key. We carry the scalar as `batchDuration` and leave `durationMs` absent.
        assert!(
            json.get("durationMs").is_none_or(|d| d.is_object()),
            "durationMs must be a map if present"
        );
        // Both ids are parsed with `uuid.UUID(...)`, which rejects anything else.
        for key in ["id", "runId"] {
            assert!(
                uuid::Uuid::parse_str(json[key].as_str().expect("a string")).is_ok(),
                "`{key}` must be a UUID the client can parse"
            );
        }
    }
}
