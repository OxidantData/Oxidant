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

/// One micro-batch progress report (Spark-compatible JSON fields).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryProgress {
    pub id: String,
    pub run_id: String,
    pub name: String,
    pub num_input_rows: u64,
    pub processed_rows_per_second: f64,
    pub batch_id: u64,
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
