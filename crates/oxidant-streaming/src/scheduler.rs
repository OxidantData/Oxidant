//! Micro-batch trigger scheduling and query manager.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion::logical_expr::LogicalPlan;
use oxidant_catalog::TableFormat;
use oxidant_common::{Error, Result};
use oxidant_loom::arrow::datatypes::SchemaRef;
use oxidant_loom::Engine;
use tokio::sync::RwLock;

use crate::checkpoint::CheckpointStore;
use crate::config::StreamQueryConfig;
use crate::input::MicroBatchInput;
use crate::kafka::KafkaSource;
use crate::lake_sink::{LakeSink, LakeTarget};
use crate::query::{QueryProgress, QueryStatus, SourceProgress, StreamingQuery, StreamingQueryId};
use crate::sink::{FileSink, MemorySink, Sink};
use crate::source::{FileSource, MemoryRateSource, Source};
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

/// The DataFrame transformation between a streaming source and its sink.
///
/// A streaming query is a batch plan re-run per micro-batch: `input` holds the rows the plan
/// reads, and `plan` is what `readStream…select…filter…` translated to. A pass-through query
/// (`readStream(...).writeStream(...)` with nothing in between) has no pipeline at all and the
/// source's batches go to the sink unchanged.
pub struct MicroBatchPipeline {
    pub input: Arc<MicroBatchInput>,
    pub plan: LogicalPlan,
}

/// Everything needed to start a query, beyond what the source/sink options carry.
#[derive(Default)]
pub struct StartOptions {
    pub pipeline: Option<MicroBatchPipeline>,
    /// Session catalog/namespace a partially-qualified `toTable(...)` resolves against.
    pub current_catalog: String,
    pub current_namespace: Vec<String>,
}

/// Manages active streaming queries.
pub struct StreamingQueryManager {
    queries: Arc<RwLock<HashMap<String, Arc<ManagedQuery>>>>,
}

/// One registered query.
///
/// The batch machinery and the observable state are behind **separate** locks on purpose. A
/// micro-batch holds `runtime` for its whole duration — a Kafka fetch plus an S3 write, which is
/// seconds — and clients poll `status()` / `lastProgress()` throughout. Sharing one lock (as this
/// used to) made every status poll wait for the batch it was asking about.
struct ManagedQuery {
    /// Serializes micro-batch execution. Never taken by a status/stop path.
    runtime: tokio::sync::Mutex<QueryRuntime>,
    /// Observable status and progress. Cheap to read at any time.
    state: RwLock<StreamingQuery>,
    checkpoint: CheckpointStore,
}

/// The moving parts of a micro-batch, held only while one runs.
struct QueryRuntime {
    source: Box<dyn Source>,
    sink: Box<dyn Sink>,
    #[allow(dead_code)]
    trigger: Trigger,
    pipeline: Option<MicroBatchPipeline>,
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
        engine: &Engine,
        name: String,
        checkpoint_location: String,
        trigger: Trigger,
    ) -> Result<StreamingQueryId> {
        self.start_with_config(
            engine,
            name,
            checkpoint_location,
            trigger,
            StreamQueryConfig::default(),
            StartOptions::default(),
        )
        .await
    }

    /// Start a streaming query with explicit source/sink configuration from Spark Connect.
    ///
    /// Resolving the sink here — not on the first batch — is deliberate: a stream pointed at a
    /// catalog that isn't registered, a database it cannot create, or a bucket it cannot write
    /// must fail the `writeStream.start()` call, where the user is looking.
    pub async fn start_with_config(
        &self,
        engine: &Engine,
        name: String,
        checkpoint_location: String,
        trigger: Trigger,
        config: StreamQueryConfig,
        options: StartOptions,
    ) -> Result<StreamingQueryId> {
        let q = StreamingQuery::new(name.clone(), checkpoint_location.clone());
        let id = q.query_id.clone();
        let checkpoint = CheckpointStore::new(&checkpoint_location);

        let mut source = build_source(&config)?;
        // Resume before the first poll: a restarted query must continue from the last committed
        // batch, not replay from `startingOffsets`.
        let restored = checkpoint.load().unwrap_or_default();
        if let Some(offsets) = &restored.source_offsets {
            source.restore_offsets(offsets);
        }

        // The sink's schema is the *plan's* output schema when there is a transformation, and the
        // source's otherwise — that is what the table gets declared with.
        let sink_schema: SchemaRef = match &options.pipeline {
            Some(p) => Arc::new(p.plan.schema().as_arrow().clone()),
            None => source.schema(),
        };
        let sink = build_sink(engine, &config, &options, sink_schema).await?;

        let _ = checkpoint.init_for_query(&id);
        let watermark = WatermarkConfig::from_options(
            &config
                .source_options
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        );
        let managed = ManagedQuery {
            runtime: tokio::sync::Mutex::new(QueryRuntime {
                source,
                sink,
                trigger,
                pipeline: options.pipeline,
                watermark,
                dedup: if config.dedup_columns.is_empty() {
                    None
                } else {
                    Some(DedupState::new(100_000))
                },
                dedup_columns: config.dedup_columns.clone(),
                dedup_key_cols: vec![],
            }),
            state: RwLock::new(q),
            checkpoint,
        };
        self.queries
            .write()
            .await
            .insert(id.id.clone(), Arc::new(managed));
        Ok(id)
    }

    pub async fn status(&self, query_id: &str) -> Option<QueryStatus> {
        let q = self.lookup(query_id).await?;
        let state = q.state.read().await;
        Some(state.status.clone())
    }

    pub async fn last_progress(&self, query_id: &str) -> Option<QueryProgress> {
        let q = self.lookup(query_id).await?;
        let state = q.state.read().await;
        state.last_progress.clone()
    }

    /// Clone the query handle out of the map, so the map lock is never held across an await
    /// that touches the query itself.
    async fn lookup(&self, query_id: &str) -> Option<Arc<ManagedQuery>> {
        self.queries.read().await.get(query_id).cloned()
    }

    /// Mark a query as terminated by an error, keeping the message on its status.
    ///
    /// A streaming query that hits an unrecoverable error (a Kafka offset aged out of retention,
    /// a sink that lost its permissions) must stop, not retry the same failure every trigger —
    /// and the reason has to survive, because `query.status()` / `awaitTermination()` is the only
    /// place a client can see it.
    pub async fn fail(&self, query_id: &str, message: &str) {
        let Some(q) = self.lookup(query_id).await else {
            return;
        };
        let mut state = q.state.write().await;
        state.status.is_active = false;
        state.status.is_data_available = false;
        state.status.message = format!("terminated with error: {message}");
    }

    pub async fn stop(&self, query_id: &str) -> bool {
        let Some(q) = self.lookup(query_id).await else {
            return false;
        };
        let mut state = q.state.write().await;
        state.status.is_active = false;
        state.status.message = "stopped".into();
        true
    }

    /// Run one micro-batch for `query_id` using `engine`.
    ///
    /// Returns rows *read* from the source, not rows written: a pipeline that filtered everything
    /// out still made progress, and `process_all_available` must keep draining rather than stop on
    /// the first fully-filtered batch.
    pub async fn run_batch(&self, query_id: &str, engine: &Engine) -> Result<u64> {
        let q = self
            .lookup(query_id)
            .await
            .ok_or_else(|| Error::Execution("unknown query".into()))?;
        if !q.state.read().await.status.is_active {
            return Ok(0);
        }
        // Held for the whole batch — a Kafka fetch plus an object-store write. Status readers
        // take `state` instead, so they are never blocked by it.
        let mut rt = q.runtime.lock().await;

        let source_batches = rt.source.poll_batch(engine).await?;
        let input_rows: u64 = source_batches.iter().map(|b| b.num_rows() as u64).sum();
        if input_rows == 0 {
            // Nothing arrived. Do not run the plan, do not touch the sink, do not advance the
            // batch id — an idle trigger must leave the table (and its version history) alone.
            q.state.write().await.status.is_data_available = false;
            return Ok(0);
        }

        // Run the user's DataFrame transformation over this batch. `execute_logical_plan`
        // collects fully, so the input can be released as soon as it returns — otherwise a
        // stopped or idle query would pin its last micro-batch in memory indefinitely.
        let mut batches = match &rt.pipeline {
            Some(p) => {
                p.input.set_batches(source_batches).await?;
                let out = engine.execute_logical_plan(p.plan.clone()).await;
                p.input.set_batches(vec![]).await?;
                out?
            }
            None => source_batches,
        };

        if let Some(wm) = &rt.watermark {
            let now = chrono::Utc::now().timestamp_micros();
            let watermark = wm.watermark_micros(now);
            batches = apply_watermark(batches, &wm.event_time_column, watermark);
            let mut state = q.checkpoint.load().unwrap_or_default();
            state.watermark_micros = watermark;
            let _ = q.checkpoint.save(&state);
        }
        if rt.dedup.is_some() {
            if rt.dedup_key_cols.is_empty() && !batches.is_empty() {
                rt.dedup_key_cols = resolve_dedup_cols(&batches[0], &rt.dedup_columns);
            }
            let keys = rt.dedup_key_cols.clone();
            let dedup = rt.dedup.as_mut().expect("checked above");
            batches = dedup.dedup_batches(&batches, &keys);
        }

        let rows = rt.sink.write_batch(&batches).await?;
        let source_description = rt.source.description();
        let committed_offsets = rt.source.committed_offsets();

        let batch_id = {
            let mut state = q.state.write().await;
            state.batch_id += 1;
            state.status.is_data_available = true;
            state.last_progress = Some(QueryProgress {
                id: state.query_id.id.clone(),
                run_id: state.query_id.run_id.clone(),
                name: state.name.clone(),
                batch_id: state.batch_id,
                num_input_rows: input_rows,
                processed_rows_per_second: rows as f64,
                sources: vec![SourceProgress {
                    description: source_description,
                    num_input_rows: input_rows,
                }],
            });
            state.batch_id
        };

        // Exactly-once-into-the-sink: the source position is committed only after the sink write
        // returns. A crash between the two replays the batch — at-least-once, never data loss.
        let mut checkpoint = q.checkpoint.load().unwrap_or_default();
        checkpoint.batch_id = batch_id;
        checkpoint.committed_batch_id = batch_id;
        checkpoint.source_offsets = committed_offsets;
        let _ = q.checkpoint.save(&checkpoint);
        Ok(input_rows)
    }

    /// Process all available data for `query_id` (for `availableNow` / `once` triggers).
    pub async fn process_all_available(&self, query_id: &str, engine: &Engine) -> Result<u64> {
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
        let queries: Vec<Arc<ManagedQuery>> = self.queries.read().await.values().cloned().collect();
        let mut out = Vec::new();
        for q in queries {
            let state = q.state.read().await;
            if state.status.is_active {
                out.push(state.query_id.clone());
            }
        }
        out
    }
}

impl Default for StreamingQueryManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Build the source for a `readStream.format(...)`.
pub fn build_source(config: &StreamQueryConfig) -> Result<Box<dyn Source>> {
    let options: HashMap<String, String> = config
        .source_options
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    Ok(match config.source_format.to_ascii_lowercase().as_str() {
        "parquet" | "json" | "csv" => {
            let path = options
                .get("path")
                .cloned()
                .unwrap_or_else(|| "/tmp/oxidant-stream-in".into());
            Box::new(FileSource::new(path, &config.source_format))
        }
        "kafka" => Box::new(KafkaSource::from_options(&options)?),
        "rate" => {
            let rows = options
                .get("rowsPerSecond")
                .and_then(|s| s.parse().ok())
                .unwrap_or(10);
            Box::new(MemoryRateSource::new(rows, u64::MAX))
        }
        "memory" => Box::new(MemoryRateSource::new(10, 1)),
        other => {
            return Err(Error::Unsupported(format!(
                "readStream.format(`{other}`) — supported: kafka, parquet, json, csv, rate"
            )))
        }
    })
}

/// The schema a source emits, known before the query runs. Used by the Connect translator to
/// plan the streaming DataFrame.
pub fn source_schema(config: &StreamQueryConfig) -> Result<SchemaRef> {
    Ok(build_source(config)?.schema())
}

async fn build_sink(
    engine: &Engine,
    config: &StreamQueryConfig,
    options: &StartOptions,
    schema: SchemaRef,
) -> Result<Box<dyn Sink>> {
    // `toTable(...)`: a catalog table, always through the lake sink.
    if let Some(identifier) = &config.sink_table {
        let format = crate::lake_sink::writable_format(&config.sink_format)?;
        let target = LakeTarget::from_table_identifier(
            identifier,
            &options.current_catalog,
            &options.current_namespace,
            format,
            config.sink_path.clone(),
        )?;
        return Ok(Box::new(LakeSink::open(engine, target, schema).await?));
    }

    // `start(path)`: Delta and Parquet go through the lake sink so `s3://` works and Delta gets a
    // real transaction log; the text formats keep the local file writer.
    if let Some(path) = &config.sink_path {
        return Ok(match config.sink_format.to_ascii_lowercase().as_str() {
            "delta" => Box::new(
                LakeSink::open(
                    engine,
                    LakeTarget::location_only(path, TableFormat::Delta),
                    schema,
                )
                .await?,
            ),
            "parquet" => Box::new(
                LakeSink::open(
                    engine,
                    LakeTarget::location_only(path, TableFormat::Parquet),
                    schema,
                )
                .await?,
            ),
            _ => Box::new(FileSink::new(path, &config.sink_format)),
        });
    }

    Ok(Box::new(MemorySink::new()))
}

fn resolve_dedup_cols(
    batch: &oxidant_loom::arrow::record_batch::RecordBatch,
    names: &[String],
) -> Vec<usize> {
    names
        .iter()
        .filter_map(|n| batch.schema().index_of(n).ok())
        .collect()
}

fn apply_watermark(
    batches: Vec<oxidant_loom::arrow::record_batch::RecordBatch>,
    event_col: &str,
    watermark_micros: i64,
) -> Vec<oxidant_loom::arrow::record_batch::RecordBatch> {
    use oxidant_loom::arrow::array::{Array, AsArray, BooleanArray};
    use oxidant_loom::arrow::compute::filter_record_batch;
    use oxidant_loom::arrow::datatypes::DataType;

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
                    arr.as_primitive::<oxidant_loom::arrow::datatypes::TimestampMicrosecondType>();
                for (row, slot) in keep.iter_mut().enumerate() {
                    if !arr.is_null(row) && ts.value(row) < watermark_micros {
                        *slot = false;
                    }
                }
            }
            DataType::Date32 => {
                let d = arr.as_primitive::<oxidant_loom::arrow::datatypes::Date32Type>();
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
    use crate::config::SinkDestination;
    use std::collections::BTreeMap;

    #[tokio::test]
    async fn start_and_run_batch() {
        let mgr = StreamingQueryManager::new();
        let engine = Engine::new();
        let id = mgr
            .start(
                &engine,
                "test".into(),
                "/tmp/oxidant-stream-test".into(),
                Trigger::Once,
            )
            .await
            .unwrap();
        let rows = mgr.process_all_available(&id.id, &engine).await.unwrap();
        assert!(rows > 0);
        let progress = mgr.last_progress(&id.id).await.unwrap();
        assert!(progress.batch_id >= 1);
    }

    #[tokio::test]
    async fn an_idle_trigger_does_not_advance_the_batch_id() {
        let dir = tempfile::TempDir::new().unwrap();
        let mgr = StreamingQueryManager::new();
        let engine = Engine::new();
        let id = mgr
            .start(
                &engine,
                "idle".into(),
                dir.path().to_string_lossy().into_owned(),
                Trigger::Once,
            )
            .await
            .unwrap();

        // The memory source yields exactly one batch, then nothing.
        assert!(mgr.run_batch(&id.id, &engine).await.unwrap() > 0);
        assert_eq!(mgr.run_batch(&id.id, &engine).await.unwrap(), 0);
        assert_eq!(mgr.last_progress(&id.id).await.unwrap().batch_id, 1);
    }

    #[tokio::test]
    async fn kafka_offsets_survive_a_restart_of_the_same_checkpoint() {
        let spool = tempfile::TempDir::new().unwrap();
        let checkpoint = tempfile::TempDir::new().unwrap();
        std::fs::write(spool.path().join("batch-0.json"), "{\"a\":1}\n{\"a\":2}\n").unwrap();

        let source_options: BTreeMap<String, String> = [
            ("subscribe".to_string(), "events".to_string()),
            (
                "oxidant.spool.dir".to_string(),
                spool.path().to_string_lossy().into_owned(),
            ),
        ]
        .into_iter()
        .collect();
        let config = StreamQueryConfig {
            source_format: "kafka".into(),
            source_options,
            ..StreamQueryConfig::from_spark(
                "kafka",
                &HashMap::new(),
                "memory",
                SinkDestination::None,
                &HashMap::new(),
            )
        };

        let engine = Engine::new();
        let mgr = StreamingQueryManager::new();
        let id = mgr
            .start_with_config(
                &engine,
                "k".into(),
                checkpoint.path().to_string_lossy().into_owned(),
                Trigger::Once,
                config.clone(),
                StartOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(mgr.process_all_available(&id.id, &engine).await.unwrap(), 2);

        let state = CheckpointStore::new(checkpoint.path()).load().unwrap();
        let offsets = state.source_offsets.expect("offsets committed");
        assert_eq!(offsets.source, "kafka");
        assert_eq!(offsets.entries.get("events-0"), Some(&2));

        // A second query on the same checkpoint resumes from the committed offset.
        let mgr2 = StreamingQueryManager::new();
        let id2 = mgr2
            .start_with_config(
                &engine,
                "k".into(),
                checkpoint.path().to_string_lossy().into_owned(),
                Trigger::Once,
                config,
                StartOptions::default(),
            )
            .await
            .unwrap();
        let state2 = CheckpointStore::new(checkpoint.path()).load().unwrap();
        assert_eq!(
            state2
                .source_offsets
                .as_ref()
                .and_then(|o| o.entries.get("events-0")),
            Some(&2),
            "a restart must keep the committed offsets, not reset them"
        );
        assert_eq!(
            state2.query_id, state.query_id,
            "the query id outlives a run"
        );
        assert_ne!(state2.run_id, state.run_id, "but the run id is new");
        let _ = id2;
    }

    /// A sink that blocks until released, standing in for a slow object-store write.
    struct BlockingSink {
        release: Arc<tokio::sync::Notify>,
        entered: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl Sink for BlockingSink {
        async fn write_batch(
            &mut self,
            batches: &[oxidant_loom::arrow::record_batch::RecordBatch],
        ) -> Result<u64> {
            self.entered.notify_one();
            self.release.notified().await;
            Ok(batches.iter().map(|b| b.num_rows() as u64).sum())
        }
    }

    #[tokio::test]
    async fn status_and_stop_are_not_blocked_by_a_running_batch() {
        // The hazard: clients poll `status()` / `lastProgress()` on a cadence while a batch is
        // mid-flight. If those shared the batch's lock, every poll would wait out a Kafka fetch
        // plus an S3 write.
        let dir = tempfile::TempDir::new().unwrap();
        let engine = Engine::new();
        let mgr = Arc::new(StreamingQueryManager::new());
        let id = mgr
            .start(
                &engine,
                "blocking".into(),
                dir.path().to_string_lossy().into_owned(),
                Trigger::Once,
            )
            .await
            .unwrap();

        let release = Arc::new(tokio::sync::Notify::new());
        let entered = Arc::new(tokio::sync::Notify::new());
        {
            let q = mgr.lookup(&id.id).await.unwrap();
            q.runtime.lock().await.sink = Box::new(BlockingSink {
                release: release.clone(),
                entered: entered.clone(),
            });
        }

        let batch_mgr = mgr.clone();
        let batch_id = id.id.clone();
        let batch_engine = engine.clone();
        let running =
            tokio::spawn(async move { batch_mgr.run_batch(&batch_id, &batch_engine).await });

        entered.notified().await; // the sink write is now in flight
        let status = tokio::time::timeout(Duration::from_secs(2), mgr.status(&id.id))
            .await
            .expect("status() blocked on the in-flight batch");
        assert!(status.expect("status").is_active);
        assert!(
            tokio::time::timeout(Duration::from_secs(2), mgr.stop(&id.id))
                .await
                .expect("stop() blocked on the in-flight batch")
        );

        release.notify_one();
        assert_eq!(running.await.unwrap().unwrap(), 10);
    }

    #[tokio::test]
    async fn an_unknown_source_format_fails_the_start_call() {
        let config = StreamQueryConfig {
            source_format: "kinesis".into(),
            ..Default::default()
        };
        let engine = Engine::new();
        let mgr = StreamingQueryManager::new();
        let err = mgr
            .start_with_config(
                &engine,
                "x".into(),
                "/tmp/oxidant-stream-unknown".into(),
                Trigger::Once,
                config,
                StartOptions::default(),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)), "{err:?}");
    }
}
