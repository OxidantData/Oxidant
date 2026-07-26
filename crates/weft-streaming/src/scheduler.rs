//! Micro-batch trigger scheduling and query manager.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use weft_loom::Engine;

use crate::checkpoint::CheckpointStore;
use crate::config::StreamQueryConfig;
use crate::query::{QueryProgress, QueryStatus, StreamingQuery, StreamingQueryId};
use crate::sink::{FileSink, MemorySink, Sink};
use crate::source::{FileSource, KafkaSource, MemoryRateSource, Source};
use crate::state::DedupState;
use crate::watermark::WatermarkConfig;

/// Trigger mode for micro-batch execution.
#[derive(Debug, Clone)]
pub enum Trigger {
    /// Fire every `interval` (processing-time).
    ProcessingTime(Duration),
    /// Process all available data once, then stop.
    Once,
    /// Process all currently available data, then idle.
    AvailableNow,
}

/// Manages active streaming queries.
pub struct StreamingQueryManager {
    queries: Arc<RwLock<HashMap<String, ManagedQuery>>>,
}

struct ManagedQuery {
    query: StreamingQuery,
    source: Box<dyn Source>,
    sink: Box<dyn Sink>,
    checkpoint: CheckpointStore,
    #[allow(dead_code)]
    trigger: Trigger,
    watermark: Option<WatermarkConfig>,
    dedup: Option<DedupState>,
    dedup_columns: Vec<String>,
    dedup_key_cols: Vec<usize>,
}

impl StreamingQueryManager {
    pub fn new() -> Self {
        Self {
            queries: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Start a new streaming query and return its id.
    pub async fn start(
        &self,
        name: String,
        checkpoint_location: String,
        trigger: Trigger,
    ) -> StreamingQueryId {
        self.start_with_config(
            name,
            checkpoint_location,
            trigger,
            StreamQueryConfig::default(),
        )
        .await
    }

    /// Start a streaming query with explicit source/sink configuration from Spark Connect.
    pub async fn start_with_config(
        &self,
        name: String,
        checkpoint_location: String,
        trigger: Trigger,
        config: StreamQueryConfig,
    ) -> StreamingQueryId {
        let q = StreamingQuery::new(name.clone(), checkpoint_location.clone());
        let id = q.query_id.clone();
        let checkpoint = CheckpointStore::new(&checkpoint_location);
        let _ = checkpoint.init_for_query(&id);
        let watermark = WatermarkConfig::from_options(&config.source_options);
        let source: Box<dyn Source> = build_source(&config);
        let sink: Box<dyn Sink> = build_sink(&config);
        let managed = ManagedQuery {
            query: q,
            source,
            sink,
            checkpoint,
            trigger,
            watermark,
            dedup: if config.dedup_columns.is_empty() {
                None
            } else {
                Some(DedupState::new(100_000))
            },
            dedup_columns: config.dedup_columns.clone(),
            dedup_key_cols: vec![],
        };
        self.queries.write().await.insert(id.id.clone(), managed);
        id
    }

    pub async fn status(&self, query_id: &str) -> Option<QueryStatus> {
        self.queries
            .read()
            .await
            .get(query_id)
            .map(|m| m.query.status.clone())
    }

    pub async fn last_progress(&self, query_id: &str) -> Option<QueryProgress> {
        self.queries
            .read()
            .await
            .get(query_id)
            .and_then(|m| m.query.last_progress.clone())
    }

    pub async fn stop(&self, query_id: &str) -> bool {
        if let Some(m) = self.queries.write().await.get_mut(query_id) {
            m.query.status.is_active = false;
            m.query.status.message = "stopped".into();
            true
        } else {
            false
        }
    }

    /// Run one micro-batch for `query_id` using `engine`.
    pub async fn run_batch(&self, query_id: &str, engine: &Engine) -> weft_common::Result<u64> {
        let mut guard = self.queries.write().await;
        let m = guard
            .get_mut(query_id)
            .ok_or_else(|| weft_common::Error::Execution("unknown query".into()))?;
        if !m.query.status.is_active {
            return Ok(0);
        }
        let batches = m.source.poll_batch(engine).await?;
        let mut batches = batches;
        if let Some(wm) = &m.watermark {
            let now = chrono::Utc::now().timestamp_micros();
            let watermark = wm.watermark_micros(now);
            batches = apply_watermark(batches, &wm.event_time_column, watermark);
            let mut state = m.checkpoint.load().unwrap_or_default();
            state.watermark_micros = watermark;
            let _ = m.checkpoint.save(&state);
        }
        if let Some(dedup) = &mut m.dedup {
            if m.dedup_key_cols.is_empty() && !batches.is_empty() {
                m.dedup_key_cols = resolve_dedup_cols(&batches[0], &m.dedup_columns);
            }
            batches = dedup.dedup_batches(&batches, &m.dedup_key_cols);
        }
        let rows = m.sink.write_batch(&batches)?;
        m.query.batch_id += 1;
        m.query.status.is_data_available = rows > 0;
        m.query.last_progress = Some(QueryProgress {
            id: m.query.query_id.id.clone(),
            run_id: m.query.query_id.run_id.clone(),
            name: m.query.name.clone(),
            num_input_rows: rows,
            processed_rows_per_second: rows as f64,
            batch_id: m.query.batch_id,
        });
        // Exactly-once: checkpoint only after successful sink write.
        let mut state = m.checkpoint.load().unwrap_or_default();
        state.batch_id = m.query.batch_id;
        state.committed_batch_id = m.query.batch_id;
        let _ = m.checkpoint.save(&state);
        Ok(rows)
    }

    /// Process all available data for `query_id` (for `availableNow` / `once` triggers).
    pub async fn process_all_available(
        &self,
        query_id: &str,
        engine: &Engine,
    ) -> weft_common::Result<u64> {
        let mut total = 0u64;
        loop {
            let rows = self.run_batch(query_id, engine).await?;
            if rows == 0 {
                break;
            }
            total += rows;
        }
        Ok(total)
    }

    pub async fn active_queries(&self) -> Vec<StreamingQueryId> {
        self.queries
            .read()
            .await
            .values()
            .filter(|m| m.query.status.is_active)
            .map(|m| m.query.query_id.clone())
            .collect()
    }
}

impl Default for StreamingQueryManager {
    fn default() -> Self {
        Self::new()
    }
}

fn build_source(config: &StreamQueryConfig) -> Box<dyn Source> {
    match config.source_format.to_ascii_lowercase().as_str() {
        "parquet" | "json" | "csv" => {
            let path = config
                .source_options
                .get("path")
                .cloned()
                .or_else(|| config.sink_path.clone())
                .unwrap_or_else(|| "/tmp/weft-stream-in".into());
            Box::new(FileSource::new(path, &config.source_format))
        }
        "kafka" => Box::new(KafkaSource::from_options(&config.source_options)),
        "rate" => {
            let rows = config
                .source_options
                .get("rowsPerSecond")
                .and_then(|s| s.parse().ok())
                .unwrap_or(10);
            Box::new(MemoryRateSource::new(rows, u64::MAX))
        }
        _ => Box::new(MemoryRateSource::new(10, 1)),
    }
}

fn build_sink(config: &StreamQueryConfig) -> Box<dyn Sink> {
    if let Some(path) = &config.sink_path {
        return Box::new(FileSink::new(path, &config.sink_format));
    }
    match config.sink_format.to_ascii_lowercase().as_str() {
        "parquet" | "json" | "csv" => {
            let path = config
                .source_options
                .get("checkpointLocation")
                .map(|c| format!("{c}/output"))
                .unwrap_or_else(|| "/tmp/weft-stream-out".into());
            Box::new(FileSink::new(path, &config.sink_format))
        }
        _ => Box::new(MemorySink::new()),
    }
}

fn resolve_dedup_cols(
    batch: &weft_loom::arrow::record_batch::RecordBatch,
    names: &[String],
) -> Vec<usize> {
    names
        .iter()
        .filter_map(|n| batch.schema().index_of(n).ok())
        .collect()
}

fn apply_watermark(
    batches: Vec<weft_loom::arrow::record_batch::RecordBatch>,
    event_col: &str,
    watermark_micros: i64,
) -> Vec<weft_loom::arrow::record_batch::RecordBatch> {
    use weft_loom::arrow::array::{Array, AsArray, BooleanArray};
    use weft_loom::arrow::compute::filter_record_batch;
    use weft_loom::arrow::datatypes::DataType;

    let mut out = Vec::new();
    for batch in batches {
        let Ok(col_idx) = batch.schema().index_of(event_col) else {
            out.push(batch);
            continue;
        };
        let arr = batch.column(col_idx);
        let mut keep = vec![true; batch.num_rows()];
        match arr.data_type() {
            DataType::Timestamp(_, _) => {
                let ts =
                    arr.as_primitive::<weft_loom::arrow::datatypes::TimestampMicrosecondType>();
                for (row, slot) in keep.iter_mut().enumerate() {
                    if !arr.is_null(row) && ts.value(row) < watermark_micros {
                        *slot = false;
                    }
                }
            }
            DataType::Date32 => {
                let d = arr.as_primitive::<weft_loom::arrow::datatypes::Date32Type>();
                let wm_days = (watermark_micros / 86_400_000_000) as i32;
                for (row, slot) in keep.iter_mut().enumerate() {
                    if !arr.is_null(row) && d.value(row) < wm_days {
                        *slot = false;
                    }
                }
            }
            _ => {}
        }
        let mask = BooleanArray::from(keep);
        if let Ok(filtered) = filter_record_batch(&batch, &mask) {
            if filtered.num_rows() > 0 {
                out.push(filtered);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn start_and_run_batch() {
        let mgr = StreamingQueryManager::new();
        let id = mgr
            .start("test".into(), "/tmp/weft-stream-test".into(), Trigger::Once)
            .await;
        let engine = Engine::new();
        let rows = mgr.process_all_available(&id.id, &engine).await.unwrap();
        assert!(rows > 0);
        let progress = mgr.last_progress(&id.id).await.unwrap();
        assert!(progress.batch_id >= 1);
    }
}
