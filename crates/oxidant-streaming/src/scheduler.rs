//! Micro-batch trigger scheduling and query manager.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use datafusion::logical_expr::LogicalPlan;
use oxidant_catalog::TableFormat;
use oxidant_common::{Error, Result};
use oxidant_loom::arrow::datatypes::SchemaRef;
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;
use tokio::sync::RwLock;

use crate::checkpoint::CheckpointStore;
use crate::config::{ExpectationAction, StreamExpectation, StreamQueryConfig};
use crate::input::MicroBatchInput;
use crate::kafka::KafkaSource;
use crate::lake_sink::{LakeSink, LakeSinkOptions, LakeTarget};
use crate::query::{
    QueryProgress, QueryStatus, SinkProgress, SourceProgress, StreamingQuery, StreamingQueryId,
};
use crate::sink::{FileSink, MemorySink, Sink};
use crate::source::{FileSource, MemoryRateSource, Source, SourceOffsets};
use crate::state::DedupState;
use crate::watermark::{WatermarkConfig, WatermarkTracker};

/// Attempts a transient I/O failure gets before the query gives up on it.
const RETRY_ATTEMPTS: u32 = 4;

/// Whether an error is worth trying again.
///
/// Under load, transient failures are routine — an S3 5xx, a partition leader election during a
/// broker restart, a throttled catalog call. Terminating the query on the first one means a
/// stream cannot survive an ordinary Tuesday. A planning or schema error, by contrast, will fail
/// identically forever, so retrying it just delays the message.
fn is_retryable(e: &Error) -> bool {
    matches!(e, Error::Io(_))
}

fn retry_backoff(attempt: u32) -> Duration {
    Duration::from_millis(200u64 << attempt.min(5))
}

/// Run an I/O operation, retrying transient failures with exponential backoff.
///
/// Retrying a *sink write* is only safe because the sink stamps the batch id into its commit: a
/// second attempt after a lost acknowledgement is recognized as a replay and dropped, rather than
/// appending the rows twice.
macro_rules! with_retry {
    ($what:expr, $op:expr) => {{
        let mut attempt: u32 = 0;
        loop {
            match $op.await {
                Ok(value) => break Ok(value),
                Err(e) if is_retryable(&e) && attempt + 1 < RETRY_ATTEMPTS => {
                    attempt += 1;
                    eprintln!(
                        "[oxidant] streaming {}: {e} — retry {attempt}/{}",
                        $what,
                        RETRY_ATTEMPTS - 1
                    );
                    tokio::time::sleep(retry_backoff(attempt)).await;
                }
                Err(e) => break Err(e),
            }
        }
    }};
}

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
    /// When set, each micro-batch is cast to this schema before the sink (column-named errors).
    pub output_schema: Option<SchemaRef>,
}

/// Everything needed to start a query, beyond what the source/sink options carry.
#[derive(Default)]
pub struct StartOptions {
    pub pipeline: Option<MicroBatchPipeline>,
    /// Session catalog/namespace a partially-qualified `toTable(...)` resolves against.
    pub current_catalog: String,
    pub current_namespace: Vec<String>,
    /// When set, used instead of building a sink from [`StreamQueryConfig`].
    pub sink_override: Option<Box<dyn Sink>>,
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
    /// The moving event-time high-water mark, resumed from the checkpoint.
    tracker: WatermarkTracker,
    /// `dropDuplicates` keys, resumed from the checkpoint and expired by the watermark.
    dedup: Option<DedupState>,
    dedup_columns: Vec<String>,
    dedup_key_cols: Vec<usize>,
    /// Counted checks run against each micro-batch before it reaches the sink.
    expectations: Vec<StreamExpectation>,
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
        let checkpoint = checkpoint_store(engine, &checkpoint_location)?;

        let mut source = build_source(&config)?;
        // Resume before the first poll: a restarted query must continue from the last committed
        // batch, not replay from `startingOffsets`.
        let restored = recover_from_log(&checkpoint).await?;
        if let Some(offsets) = &restored.source_offsets {
            source.restore_offsets(offsets);
        }

        // The sink's schema is the *plan's* output schema when there is a transformation, and the
        // source's otherwise — that is what the table gets declared with.
        let sink_schema: SchemaRef = match &options.pipeline {
            Some(p) => Arc::new(p.plan.schema().as_arrow().clone()),
            None => source.schema(),
        };
        // The *persisted* query id is the sink's `appId`: it survives restarts, which is what
        // makes a replayed batch recognizable as one. A fresh uuid per run would not.
        let app_id = if restored.query_id.is_empty() {
            id.id.clone()
        } else {
            restored.query_id.clone()
        };
        let sink = if let Some(sink) = options.sink_override {
            sink
        } else {
            build_sink(engine, &config, &options, sink_schema, app_id).await?
        };

        // A checkpoint location that cannot be written is a misconfiguration the user has to see
        // at `writeStream.start()`. Discovering it later means the query has already ingested data
        // it cannot record having ingested.
        checkpoint.init_for_query(&id).await.map_err(|e| {
            Error::Io(format!(
                "streaming checkpoint `{}` is not writable: {e}",
                checkpoint.location()
            ))
        })?;
        let watermark = WatermarkConfig::from_options(
            &config
                .source_options
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        );
        // Batch ids continue where the last run stopped: they are the sink's idempotency
        // versions, so restarting at zero would make every replayed id look already-committed.
        let mut q = q;
        q.batch_id = restored.committed_batch_id;
        let managed = ManagedQuery {
            runtime: tokio::sync::Mutex::new(QueryRuntime {
                source,
                sink,
                trigger,
                pipeline: options.pipeline,
                watermark,
                tracker: WatermarkTracker::restore(restored.max_event_time_micros),
                dedup: if config.dedup_columns.is_empty() {
                    None
                } else {
                    // Resumed rather than started empty: a fresh set would re-admit every
                    // duplicate the previous run had already filtered out.
                    Some(restored.dedup_state.clone().unwrap_or_default())
                },
                dedup_columns: config.dedup_columns.clone(),
                dedup_key_cols: vec![],
                expectations: config.expectations.clone(),
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

        Self::run_batch_inner(&q, &mut rt, engine).await
    }

    /// The body of one micro-batch.
    ///
    /// The order of the three durable writes is the whole guarantee, and none of them commute:
    ///
    /// 1. **the offset log**, naming what this batch will read, *before* it reads anything;
    /// 2. **the sink**, stamped with the batch id so a replay is recognized;
    /// 3. **the commit log**, then the fast-resume record.
    ///
    /// A crash between any two is recoverable because the surviving prefix says exactly which
    /// batch was in flight and what it covered.
    async fn run_batch_inner(
        q: &Arc<ManagedQuery>,
        rt: &mut QueryRuntime,
        engine: &Engine,
    ) -> Result<u64> {
        let started = std::time::Instant::now();

        // Batch ids are not burned by failure: an attempt that never committed leaves its range
        // in the log, and the next attempt picks up the same id and the same extent. Retrying
        // under a *fresh* id instead would stop the sink recognizing a replay of a batch it had
        // in fact committed — an acknowledgement lost on the way back, say — and write the rows
        // a second time.
        let batch_id = q.state.read().await.batch_id + 1;

        let range = match q.checkpoint.load_planned(batch_id).await.map_err(|e| {
            Error::Io(format!(
                "streaming checkpoint `{}`: reading the planned range for batch {batch_id}: {e}",
                q.checkpoint.location()
            ))
        })? {
            // An attempt at this batch was already recorded. Replaying its *recorded* extent —
            // rather than asking the source what is available now — is what keeps the sink's
            // idempotency stamp sound: a wider replay would be recognized by batch id and
            // discarded whole, taking the records that arrived in between with it.
            Some(range) => range,
            None => {
                let range = with_retry!("source plan", rt.source.plan_batch(engine))?;
                if range.is_empty() {
                    // Nothing arrived. Do not plan, do not touch the sink, do not advance the
                    // batch id — an idle trigger must leave the table (and its version history)
                    // alone.
                    q.state.write().await.status.is_data_available = false;
                    return Ok(0);
                }
                q.checkpoint
                    .save_planned(batch_id, &range)
                    .await
                    .map_err(|e| {
                        Error::Io(format!(
                            "streaming checkpoint `{}`: recording batch {batch_id}: {e}",
                            q.checkpoint.location()
                        ))
                    })?;
                range
            }
        };

        let start_offset = offsets_json(Some(&SourceOffsets {
            source: range.source.clone(),
            entries: range.start.clone(),
        }));
        let source_batches = with_retry!("source poll", rt.source.poll_range(engine, &range))?;
        let input_rows: u64 = source_batches.iter().map(|b| b.num_rows() as u64).sum();

        // A recorded range that yields no records — every offset in it compacted away, or a
        // spool file of blank lines — still has to be got past, or it is replayed forever. But
        // it must not reach the sink: an empty commit per trigger fills the table's version
        // history with versions that change nothing.
        #[allow(unused_assignments)]
        let mut late_records = 0u64;
        let rows = if input_rows == 0 {
            0
        } else {
            // Run the user's DataFrame transformation over this batch. `execute_logical_plan`
            // collects fully, so the input can be released as soon as it returns — otherwise a
            // stopped or idle query would pin its last micro-batch in memory indefinitely.
            let mut batches = match &rt.pipeline {
                Some(p) => {
                    p.input.set_batches(source_batches).await?;
                    let out = engine.execute_logical_plan(p.plan.clone()).await;
                    p.input.set_batches(vec![]).await?;
                    let mut out = out?;
                    if let Some(target) = &p.output_schema {
                        out = out
                            .into_iter()
                            .map(|batch| {
                                oxidant_loom::schema_conform::conform_batch_to_schema(batch, target)
                            })
                            .collect::<oxidant_common::Result<Vec<_>>>()?;
                    }
                    out
                }
                None => source_batches,
            };

            // The watermark advances from the data, and the value this batch is judged against
            // is the one that preceded it — otherwise a single record from the future would mark
            // its own batch late and expire state the rest of the batch still needed.
            if let Some(wm) = &rt.watermark {
                let before = rt.tracker.observe(wm, &batches);
                if let Some(watermark) = before {
                    late_records = WatermarkTracker::count_late(wm, &batches, watermark);
                    if late_records > 0 {
                        eprintln!(
                            "[oxidant] streaming batch {batch_id}: {late_records} record(s) \
                             behind the watermark — kept, not dropped"
                        );
                    }
                }
            }
            if rt.dedup.is_some() {
                if rt.dedup_key_cols.is_empty() && !batches.is_empty() {
                    rt.dedup_key_cols =
                        crate::state::resolve_key_columns(&batches[0], &rt.dedup_columns)?;
                }
                let keys = rt.dedup_key_cols.clone();
                let event_time_col = rt.watermark.as_ref().and_then(|wm| {
                    batches
                        .first()
                        .and_then(|b| b.schema().index_of(&wm.event_time_column).ok())
                });
                let dedup = rt.dedup.as_mut().expect("checked above");
                batches = dedup.dedup_batches(&batches, &keys, event_time_col)?;
                // Forgetting is driven by the watermark, never by a key count: a bounded set
                // that cleared itself would start re-admitting duplicates mid-run, silently.
                if let (Some(wm), Some(max)) = (&rt.watermark, rt.tracker.max_event_time_micros()) {
                    dedup.expire(wm.watermark_for(max));
                }
            }

            // Counted checks run against the batch that is about to be written — after `drop`
            // filtering has already been composed into the query, so a row a `drop` removed is
            // not also counted here.
            //
            // A `fail` returns before the sink write, which is what "leaves the table at its
            // last good version" means: the batch's range stays in the offset log uncommitted,
            // so fixing the data and retrying replays exactly these records.
            for expectation in &rt.expectations {
                let query_id = q.state.read().await.query_id.id.clone();
                let violations =
                    count_violations(engine, &batches, &expectation.check, &query_id, batch_id)
                        .await?;
                if violations == 0 {
                    continue;
                }
                match expectation.action {
                    ExpectationAction::Warn => eprintln!(
                        "[oxidant] batch={batch_id} expectation={} failed_records={violations}",
                        expectation.label
                    ),
                    ExpectationAction::Fail => {
                        return Err(Error::Execution(format!(
                            "expectation `{}` failed on batch {batch_id}: {violations} record(s) \
                             do not satisfy `{}`; the table is unchanged",
                            expectation.label, expectation.check
                        )))
                    }
                }
            }

            with_retry!("sink write", rt.sink.write_batch(&batches, batch_id))?
        };
        // The rows are durable. Everything from here on is bookkeeping, and a failure in it must
        // not make the next attempt re-read them.
        let committed_offsets = rt.source.committed_offsets();
        q.checkpoint
            .save_commit(&crate::checkpoint::BatchCommit {
                batch_id,
                num_output_rows: rows,
                resume_position: committed_offsets.clone(),
            })
            .await
            .map_err(|e| {
                Error::Io(format!(
                    "streaming checkpoint `{}`: committing batch {batch_id}: {e}",
                    q.checkpoint.location()
                ))
            })?;
        q.state.write().await.batch_id = batch_id;
        let source_description = rt.source.description();
        let sink_description = rt.sink.description();
        let end_offset = offsets_json(committed_offsets.as_ref());

        let elapsed = started.elapsed();
        let seconds = elapsed.as_secs_f64().max(f64::EPSILON);
        {
            let mut state = q.state.write().await;
            state.status.is_data_available = true;
            state.last_progress = Some(QueryProgress {
                id: state.query_id.id.clone(),
                run_id: state.query_id.run_id.clone(),
                name: state.name.clone(),
                timestamp: now_iso8601(),
                batch_id,
                batch_duration: elapsed.as_millis() as u64,
                num_input_rows: input_rows,
                // Rows the batch actually got through, per second of wall clock — not the row
                // count, which is what this field used to report to anything watching it.
                input_rows_per_second: input_rows as f64 / seconds,
                processed_rows_per_second: rows as f64 / seconds,
                state_operators: Vec::new(),
                sources: vec![SourceProgress {
                    description: source_description,
                    start_offset: start_offset.clone(),
                    // This source reads everything it polled, so where the batch ended is also the
                    // furthest position known.
                    end_offset: end_offset.clone(),
                    latest_offset: end_offset,
                    num_input_rows: input_rows,
                    input_rows_per_second: input_rows as f64 / seconds,
                    processed_rows_per_second: rows as f64 / seconds,
                }],
                sink: SinkProgress {
                    description: sink_description,
                    num_output_rows: rows,
                },
            });
        }

        // Exactly-once-into-the-sink: the source position is committed only after the sink write
        // returns. A crash between the two replays the batch, and the sink's `txn` stamp makes
        // that replay a no-op instead of a duplicate.
        let mut checkpoint = q.checkpoint.load().await.unwrap_or_default();
        checkpoint.batch_id = batch_id;
        checkpoint.committed_batch_id = batch_id;
        checkpoint.source_offsets = committed_offsets;
        // Operator state travels with the batch that produced it, so a restart resumes the
        // watermark and the dedup keys rather than starting either over.
        checkpoint.max_event_time_micros = rt.tracker.max_event_time_micros();
        if let (Some(wm), Some(max)) = (&rt.watermark, checkpoint.max_event_time_micros) {
            checkpoint.watermark_micros = wm.watermark_for(max);
        }
        checkpoint.dedup_state = rt.dedup.clone();
        checkpoint.pruned_through = q
            .checkpoint
            .prune_log(checkpoint.pruned_through, batch_id)
            .await;
        // A checkpoint that cannot be written is not cosmetic: without it a restart re-reads from
        // wherever the last successful save left off, so the failure has to reach the user.
        q.checkpoint.save(&checkpoint).await.map_err(|e| {
            Error::Io(format!(
                "streaming checkpoint `{}`: {e}",
                q.checkpoint.location()
            ))
        })?;
        Ok(input_rows)
    }

    /// Process all available data for `query_id` (for `availableNow` / `once` triggers).
    ///
    /// "Available" means *available when the drain started*, which is the whole difference from
    /// an unbounded loop. Draining until a batch comes back empty never terminates against a
    /// source that is still being written to faster than the batches consume it — and `once` is
    /// exactly the trigger someone puts in a cron job and expects to exit. The end of the first
    /// batch's planned range is the boundary; anything produced after it is the next run's.
    pub async fn process_all_available(&self, query_id: &str, engine: &Engine) -> Result<u64> {
        /// Batches one drain will run before giving up on reaching the boundary. Generous: it is
        /// a backstop against a pathological source, not a throughput limit.
        const MAX_DRAIN_BATCHES: usize = 100_000;

        let mut total = 0u64;
        let mut boundary: Option<BTreeMap<String, i64>> = None;
        for _ in 0..MAX_DRAIN_BATCHES {
            // The boundary is fixed by the first plan of the drain: everything the source could
            // offer at that moment.
            if boundary.is_none() {
                boundary = self.available_now_boundary(query_id, engine).await?;
            }
            let rows = self.run_batch(query_id, engine).await?;
            if rows == 0 {
                break;
            }
            total += rows;
            if self.reached_boundary(query_id, boundary.as_ref()).await {
                break;
            }
        }
        Ok(total)
    }

    /// Where the source ends right now, as the drain's stopping point.
    async fn available_now_boundary(
        &self,
        query_id: &str,
        engine: &Engine,
    ) -> Result<Option<BTreeMap<String, i64>>> {
        let Some(q) = self.lookup(query_id).await else {
            return Ok(None);
        };
        let mut rt = q.runtime.lock().await;
        // The source's current end, not this batch's planned range: with a per-batch budget the
        // two differ, and stopping at the planned end would drain one batch and call the topic
        // exhausted.
        rt.source.available_end(engine).await
    }

    /// Whether the source has reached the position the drain started against.
    async fn reached_boundary(
        &self,
        query_id: &str,
        boundary: Option<&BTreeMap<String, i64>>,
    ) -> bool {
        let Some(boundary) = boundary else {
            return false;
        };
        let Some(q) = self.lookup(query_id).await else {
            return true;
        };
        let rt = q.runtime.lock().await;
        let Some(position) = rt.source.committed_offsets() else {
            return false;
        };
        boundary.iter().all(|(key, end)| {
            position
                .entries
                .get(key)
                .is_some_and(|current| current >= end)
        })
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

/// Count the rows in `batches` that do not satisfy `check`.
///
/// `IS NOT TRUE` rather than `NOT (check)`: a null column makes the comparison null, and `NOT
/// null` is null, so a row of nulls would count as neither passing nor failing — which is how a
/// column that is entirely null sails through a quality gate.
async fn count_violations(
    engine: &Engine,
    batches: &[RecordBatch],
    check: &str,
    query_id: &str,
    batch_id: u64,
) -> Result<u64> {
    use datafusion::datasource::MemTable;

    let Some(first) = batches.first() else {
        return Ok(0);
    };
    let table = MemTable::try_new(first.schema(), vec![batches.to_vec()])
        .map_err(|e| Error::Execution(format!("expectation `{check}`: {e}")))?;
    // Unique per query and batch, not a fixed name. The Connect path spawns a task per
    // streaming query and they all share one session, so a constant name means two queries
    // running a counted expectation at the same time collide: the second registration fails, or
    // one deregisters the table the other is still reading.
    let name = format!("_oxidant_expect_{query_id}_{batch_id}");
    let name = name.replace('-', "_");
    let ctx = engine.ctx();
    ctx.register_table(&name, Arc::new(table))
        .map_err(|e| Error::Execution(format!("expectation `{check}`: {e}")))?;
    let counted = engine
        .sql(&format!(
            "SELECT count(*) AS c FROM {name} WHERE ({check}) IS NOT TRUE"
        ))
        .await;
    let _ = ctx.deregister_table(&name);
    let rows = counted?;
    Ok(rows
        .first()
        .and_then(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<oxidant_loom::arrow::array::Int64Array>()
                .map(|c| c.value(0).max(0) as u64)
        })
        .unwrap_or(0))
}

/// Reconcile the fast-resume record with the offset and commit logs.
///
/// The logs are written in order — planned, sink, committed, resume record — so a crash can
/// leave the last step undone, with a batch that is durably in the table but not in the resume
/// record. The next start would then re-run that batch id: harmless for the table, because the
/// sink recognizes the replay and drops it, but the *records* would be discarded with it and the
/// resume point would jump straight past them. Reading the batch's recorded range instead
/// recovers exactly where it ended.
///
/// Bounded rather than unbounded: several batches can only be in this state if the process died
/// repeatedly in the same narrow window, and a runaway loop over a corrupt log is worse than
/// replaying one batch.
async fn recover_from_log(
    checkpoint: &CheckpointStore,
) -> Result<crate::checkpoint::CheckpointState> {
    /// Consecutive committed-but-unrecorded batches reconciled at start.
    const MAX_RECOVERED: u64 = 64;

    let mut state = checkpoint.load().await.unwrap_or_default();
    for _ in 0..MAX_RECOVERED {
        let next = state.committed_batch_id + 1;
        let commit = checkpoint.load_commit(next).await.map_err(|e| {
            Error::Io(format!(
                "streaming checkpoint `{}`: reading the commit log: {e}",
                checkpoint.location()
            ))
        })?;
        if commit.is_none() {
            break;
        }
        let Some(resumed) = commit.and_then(|c| c.resume_position) else {
            // Committed with no recorded position. Nothing can be said about where that batch
            // ended, and guessing would either duplicate or skip, so stop and let the resume
            // record stand.
            break;
        };
        eprintln!(
            "[oxidant] streaming batch {next} was committed but not recorded; \
             recovering its position from the offset log"
        );
        state.committed_batch_id = next;
        state.batch_id = next;
        state.source_offsets = Some(resumed);
        checkpoint.save(&state).await.map_err(|e| {
            Error::Io(format!(
                "streaming checkpoint `{}`: {e}",
                checkpoint.location()
            ))
        })?;
    }
    Ok(state)
}

/// Resolve `checkpointLocation` to a store and a prefix within it.
///
/// Goes through the engine's own resolver, so a checkpoint on S3 uses exactly the credentials,
/// endpoint, and assumed role the table write uses — one auth path, not two. A bare filesystem
/// path is normalized to a `file://` URL first; without that, `s3://bucket/x` and `/tmp/x` are
/// indistinguishable to the URL parser and the object store, and the S3 one silently lands in a
/// local directory named `s3:`.
fn checkpoint_store(engine: &Engine, location: &str) -> Result<CheckpointStore> {
    let url = if location.contains("://") {
        location.to_string()
    } else {
        let absolute = std::path::Path::new(location);
        let absolute = if absolute.is_absolute() {
            absolute.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|e| Error::Io(format!("resolving `{location}`: {e}")))?
                .join(absolute)
        };
        // The directory has to exist before it can be addressed as a store prefix.
        std::fs::create_dir_all(&absolute)
            .map_err(|e| Error::Io(format!("creating checkpoint `{}`: {e}", absolute.display())))?;
        format!("file://{}", absolute.display())
    };

    let store = engine.object_store_for(&url, &HashMap::new())?;
    let parsed = url::Url::parse(&url)
        .map_err(|e| Error::Plan(format!("bad checkpointLocation `{location}`: {e}")))?;
    let root = object_store::path::Path::from(parsed.path().trim_start_matches('/'));
    Ok(CheckpointStore::new(store, root, location))
}

/// A source position as the JSON string Spark's progress carries. `{}` when the source has no
/// replayable position, which is what Spark reports for a source with no offsets.
fn offsets_json(offsets: Option<&SourceOffsets>) -> String {
    offsets
        .and_then(|o| serde_json::to_string(&o.entries).ok())
        .unwrap_or_else(|| "{}".to_string())
}

/// The current instant in Spark's progress timestamp format (ISO-8601, UTC, milliseconds).
fn now_iso8601() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
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
    app_id: String,
) -> Result<Box<dyn Sink>> {
    let sink_options = LakeSinkOptions {
        app_id: Some(app_id),
        partition_columns: config.partition_columns.clone(),
        publish_iceberg: config.publish_iceberg,
        iceberg_table_suffix: config.iceberg_table_suffix.clone(),
        checkpoint_interval: config.checkpoint_interval,
    };

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
        return Ok(Box::new(
            LakeSink::open(engine, target, schema, sink_options).await?,
        ));
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
                    sink_options,
                )
                .await?,
            ),
            "parquet" => Box::new(
                LakeSink::open(
                    engine,
                    LakeTarget::location_only(path, TableFormat::Parquet),
                    schema,
                    sink_options,
                )
                .await?,
            ),
            _ => Box::new(FileSink::new(path, &config.sink_format)),
        });
    }

    Ok(Box::new(MemorySink::new()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SinkDestination;
    use oxidant_loom::arrow::record_batch::RecordBatch;
    use std::collections::BTreeMap;

    /// A sink that fails its first `fail_times` writes, then records what it was handed.
    ///
    /// Exists to drive the one path no real sink makes reachable on demand: a batch that reads
    /// its records and then cannot write them.
    struct FlakySink {
        fail_times: usize,
        writes: Arc<std::sync::Mutex<Vec<(u64, u64)>>>,
    }

    #[async_trait::async_trait]
    impl Sink for FlakySink {
        async fn write_batch(&mut self, batches: &[RecordBatch], batch_id: u64) -> Result<u64> {
            if self.fail_times > 0 {
                self.fail_times -= 1;
                return Err(Error::Execution("sink is down".into()));
            }
            let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
            self.writes.lock().expect("poisoned").push((batch_id, rows));
            Ok(rows)
        }
    }

    /// Register a query with a caller-supplied source and sink, bypassing `build_source`/
    /// `build_sink` so a test can inject failure.
    ///
    /// Mirrors the recovery and resume that `start_with_config` performs, so a test can restart
    /// a query over an existing checkpoint and exercise the same path a real restart takes.
    async fn register(
        mgr: &StreamingQueryManager,
        checkpoint_dir: &std::path::Path,
        source: Box<dyn Source>,
        sink: Box<dyn Sink>,
    ) -> String {
        let mut q = StreamingQuery::new("t".into(), checkpoint_dir.to_string_lossy().into_owned());
        let id = q.query_id.id.clone();
        let checkpoint = CheckpointStore::new(
            Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(checkpoint_dir)
                    .expect("store"),
            ),
            object_store::path::Path::from(""),
            checkpoint_dir.to_string_lossy().into_owned(),
        );
        let mut source = source;
        let restored = recover_from_log(&checkpoint).await.expect("recover");
        if let Some(offsets) = &restored.source_offsets {
            source.restore_offsets(offsets);
        }
        q.batch_id = restored.committed_batch_id;
        let managed = ManagedQuery {
            runtime: tokio::sync::Mutex::new(QueryRuntime {
                source,
                sink,
                trigger: Trigger::Once,
                pipeline: None,
                watermark: None,
                tracker: WatermarkTracker::default(),
                dedup: None,
                dedup_columns: vec![],
                dedup_key_cols: vec![],
                expectations: vec![],
            }),
            state: RwLock::new(q),
            checkpoint,
        };
        mgr.queries
            .write()
            .await
            .insert(id.clone(), Arc::new(managed));
        id
    }

    fn spool_source(dir: &std::path::Path) -> Box<dyn Source> {
        let options = [
            ("oxidant.spool.dir", dir.to_str().expect("utf-8")),
            ("subscribe", "orders"),
        ]
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
        Box::new(KafkaSource::from_options(&options).expect("source"))
    }

    #[tokio::test]
    async fn a_batch_that_cannot_write_gives_its_records_back() {
        // The failure this rules out: the source advanced past `batch-0.json` while the sink was
        // down, the next trigger polled `batch-1.json`, and the first file's rows were never
        // written and never read again — no crash, no error after the one failed batch.
        let spool = tempfile::TempDir::new().expect("tmp");
        let checkpoints = tempfile::TempDir::new().expect("tmp");
        std::fs::write(spool.path().join("batch-0.json"), "{\"n\":1}\n{\"n\":2}\n").expect("write");
        std::fs::write(spool.path().join("batch-1.json"), "{\"n\":3}\n").expect("write");

        let writes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mgr = StreamingQueryManager::new();
        let engine = Engine::new();
        let id = register(
            &mgr,
            checkpoints.path(),
            spool_source(spool.path()),
            Box::new(FlakySink {
                fail_times: 1,
                writes: writes.clone(),
            }),
        )
        .await;

        let err = mgr
            .run_batch(&id, &engine)
            .await
            .expect_err("the sink is down");
        assert!(err.to_string().contains("sink is down"), "{err}");

        // The sink recovers. The next batch must re-read the file the failed one consumed.
        let rows = mgr.run_batch(&id, &engine).await.expect("sink recovered");
        assert_eq!(rows, 2, "batch-0 is read again, not skipped");
        let rows = mgr.run_batch(&id, &engine).await.expect("next file");
        assert_eq!(rows, 1, "and batch-1 follows it");

        let written = writes.lock().expect("poisoned").clone();
        assert_eq!(
            written,
            vec![(1, 2), (2, 1)],
            "every row reaches the sink exactly once, and the failed batch did not burn id 1"
        );
    }

    #[tokio::test]
    async fn a_batch_that_failed_after_writing_does_not_re_read_its_records() {
        // The other half of the rule, and the one a naive rewind gets wrong: once the rows are
        // in the sink, re-reading them writes them twice — under the *next* batch id, so no
        // `txn` stamp catches it. A post-commit failure must leave the source where it is.
        let spool = tempfile::TempDir::new().expect("tmp");
        let checkpoints = tempfile::TempDir::new().expect("tmp");
        std::fs::write(spool.path().join("batch-0.json"), "{\"n\":1}\n").expect("write");
        std::fs::write(spool.path().join("batch-1.json"), "{\"n\":2}\n").expect("write");

        let writes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mgr = StreamingQueryManager::new();
        let engine = Engine::new();
        let id = register(
            &mgr,
            checkpoints.path(),
            spool_source(spool.path()),
            Box::new(FlakySink {
                fail_times: 0,
                writes: writes.clone(),
            }),
        )
        .await;

        // Make the checkpoint save fail *after* the sink write succeeds, by occupying the path
        // it writes with a directory: the store's final rename onto it cannot succeed.
        std::fs::create_dir_all(checkpoints.path().join("offsets.json")).expect("mkdir");

        let err = mgr
            .run_batch(&id, &engine)
            .await
            .expect_err("the checkpoint cannot be written");
        assert!(err.to_string().contains("checkpoint"), "{err}");

        assert_eq!(
            writes.lock().expect("poisoned").clone(),
            vec![(1, 1)],
            "the rows did reach the sink"
        );

        // Repair the checkpoint location and carry on. The next batch must be batch-1, not a
        // second copy of batch-0.
        std::fs::remove_dir_all(checkpoints.path().join("offsets.json")).expect("rm");
        let rows = mgr.run_batch(&id, &engine).await.expect("recovered");
        assert_eq!(rows, 1);
        assert_eq!(
            writes.lock().expect("poisoned").clone(),
            vec![(1, 1), (2, 1)],
            "batch-0 was not written a second time"
        );
    }

    #[tokio::test]
    async fn a_crash_between_the_sink_commit_and_the_resume_record_loses_nothing() {
        // The window the offset log exists to close, on a source whose replay can genuinely be
        // *wider* than the batch it replaces. (The spool cannot show this: it reads one whole
        // file per batch, so a replay is identical by construction.)
        //
        // Batch 1's rows are durably in the table and its commit marker is written, but the
        // process dies before the fast-resume record. Without a recorded range the restart plans
        // afresh — picking up the file that landed in the meantime — the sink recognizes batch
        // id 1 and discards the whole thing, and that file is never read again.
        let data = tempfile::TempDir::new().unwrap();
        let checkpoints = tempfile::TempDir::new().unwrap();
        let table = tempfile::TempDir::new().unwrap();
        std::fs::write(data.path().join("a.json"), "{\"n\":1}\n{\"n\":2}\n").unwrap();

        let engine = Engine::new();
        let schema = Arc::new(oxidant_loom::arrow::datatypes::Schema::new(vec![
            oxidant_loom::arrow::datatypes::Field::new(
                "n",
                oxidant_loom::arrow::datatypes::DataType::Int64,
                true,
            ),
        ]));
        async fn delta_sink(engine: &Engine, location: &str, schema: SchemaRef) -> Box<dyn Sink> {
            Box::new(
                LakeSink::open(
                    engine,
                    LakeTarget::location_only(location, TableFormat::Delta),
                    schema,
                    LakeSinkOptions {
                        // Stable across the restart, the way a persisted query id is: this is
                        // what makes a replayed batch recognizable at all.
                        app_id: Some("crash-test".into()),
                        checkpoint_interval: 10,
                        ..Default::default()
                    },
                )
                .await
                .expect("sink"),
            )
        }
        let location = table.path().to_str().unwrap().to_string();

        let mgr = StreamingQueryManager::new();
        let id = register(
            &mgr,
            checkpoints.path(),
            Box::new(FileSource::new(data.path(), "json")),
            delta_sink(&engine, &location, schema.clone()).await,
        )
        .await;
        assert_eq!(mgr.run_batch(&id, &engine).await.unwrap(), 2);

        // The crash: the resume record never lands. The offset and commit logs survive it —
        // they were written first, which is the entire point of the ordering.
        let store = checkpoint_store(&engine, &checkpoints.path().to_string_lossy()).unwrap();
        assert!(store.is_committed(1).await.unwrap());
        let mut crashed = store.load().await.unwrap();
        crashed.committed_batch_id = 0;
        crashed.batch_id = 0;
        crashed.source_offsets = None;
        store.save(&crashed).await.unwrap();

        // A second file arrives while the query is down. A replay that swept it up would be
        // discarded along with the batch it was attached to.
        std::fs::write(data.path().join("b.json"), "{\"n\":3}\n").unwrap();

        let restarted = StreamingQueryManager::new();
        let id2 = register(
            &restarted,
            checkpoints.path(),
            Box::new(FileSource::new(data.path(), "json")),
            delta_sink(&engine, &location, schema.clone()).await,
        )
        .await;
        restarted
            .process_all_available(&id2, &engine)
            .await
            .unwrap();

        engine
            .register_delta("recovered", table.path().to_str().unwrap())
            .await
            .unwrap();
        let batches = engine
            .sql("SELECT count(*) AS c FROM recovered")
            .await
            .unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<oxidant_loom::arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(
            count, 3,
            "two records from the committed batch, one from the file that arrived after it — \
             nothing lost to the replay, nothing written twice"
        );
    }

    #[tokio::test]
    async fn two_queries_can_evaluate_expectations_at_the_same_time() {
        // The Connect path spawns a task per streaming query and they share one session, so a
        // fixed staging-table name means two queries counting violations concurrently collide:
        // the second registration fails, or one deregisters the table the other is reading.
        let engine = Engine::new();
        let schema = Arc::new(oxidant_loom::arrow::datatypes::Schema::new(vec![
            oxidant_loom::arrow::datatypes::Field::new(
                "amount",
                oxidant_loom::arrow::datatypes::DataType::Int64,
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(oxidant_loom::arrow::array::Int64Array::from(
                vec![-1, 5, -3],
            ))],
        )
        .unwrap();

        let batches = [batch];
        let (first, second) = tokio::join!(
            count_violations(&engine, &batches, "amount > 0", "query-a", 7),
            count_violations(&engine, &batches, "amount > 0", "query-b", 7),
        );
        assert_eq!(first.unwrap(), 2);
        assert_eq!(second.unwrap(), 2);
    }

    #[tokio::test]
    async fn a_null_check_counts_as_a_violation() {
        // `NOT (check)` would miss this: a null column makes the comparison null, and `NOT null`
        // is null — so a column that is entirely null sails through the gate.
        let engine = Engine::new();
        let schema = Arc::new(oxidant_loom::arrow::datatypes::Schema::new(vec![
            oxidant_loom::arrow::datatypes::Field::new(
                "amount",
                oxidant_loom::arrow::datatypes::DataType::Int64,
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(oxidant_loom::arrow::array::Int64Array::from(
                vec![None, Some(5)],
            ))],
        )
        .unwrap();
        assert_eq!(
            count_violations(&engine, &[batch], "amount > 0", "q", 1)
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn a_range_that_yields_no_records_is_got_past_without_touching_the_sink() {
        // A spool file of blank lines stands in for the real case: every offset in the range
        // compacted away. The batch must advance — otherwise it is replayed forever — but it
        // must not reach the sink, or the table gains a version that changes nothing per trigger.
        let spool = tempfile::TempDir::new().unwrap();
        let checkpoints = tempfile::TempDir::new().unwrap();
        std::fs::write(spool.path().join("batch-0.json"), "\n\n").unwrap();
        std::fs::write(spool.path().join("batch-1.json"), "a\n").unwrap();

        let writes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mgr = StreamingQueryManager::new();
        let engine = Engine::new();
        let id = register(
            &mgr,
            checkpoints.path(),
            spool_source(spool.path()),
            Box::new(FlakySink {
                fail_times: 0,
                writes: writes.clone(),
            }),
        )
        .await;

        assert_eq!(mgr.run_batch(&id, &engine).await.unwrap(), 0);
        assert!(
            writes.lock().unwrap().is_empty(),
            "an empty batch must not be written"
        );
        assert_eq!(
            mgr.run_batch(&id, &engine).await.unwrap(),
            1,
            "and the next file is reached rather than the empty one being replayed"
        );
        assert_eq!(writes.lock().unwrap().clone(), vec![(2, 1)]);
    }

    #[tokio::test]
    async fn a_failed_batch_replays_its_recorded_range_rather_than_replanning() {
        // The in-process half of the same rule. A batch that could not be written keeps its
        // recorded extent, so the retry reads exactly what the failed attempt read — even
        // though more data has landed in the meantime.
        let spool = tempfile::TempDir::new().unwrap();
        let checkpoints = tempfile::TempDir::new().unwrap();
        std::fs::write(spool.path().join("batch-0.json"), "a\nb\n").unwrap();

        let writes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mgr = StreamingQueryManager::new();
        let engine = Engine::new();
        let id = register(
            &mgr,
            checkpoints.path(),
            spool_source(spool.path()),
            Box::new(FlakySink {
                fail_times: 1,
                writes: writes.clone(),
            }),
        )
        .await;

        mgr.run_batch(&id, &engine)
            .await
            .expect_err("the sink is down");

        // New data lands before the retry. It must not be swept into batch 1.
        std::fs::write(spool.path().join("batch-1.json"), "c\n").unwrap();

        assert_eq!(
            mgr.run_batch(&id, &engine).await.unwrap(),
            2,
            "the retry covers the recorded range, not the newcomer"
        );
        assert_eq!(
            mgr.run_batch(&id, &engine).await.unwrap(),
            1,
            "which then arrives as its own batch"
        );
        assert_eq!(
            writes.lock().unwrap().clone(),
            vec![(1, 2), (2, 1)],
            "every record written exactly once, and the failed batch did not burn id 1"
        );
    }

    #[tokio::test]
    async fn start_and_run_batch() {
        // Its own checkpoint directory: the rate source now resumes from one, so a path shared
        // between runs would make the second run of the test read nothing.
        let dir = tempfile::TempDir::new().unwrap();
        let mgr = StreamingQueryManager::new();
        let engine = Engine::new();
        let id = mgr
            .start(
                &engine,
                "test".into(),
                dir.path().to_string_lossy().into_owned(),
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

    /// A `checkpointLocation` with a URI scheme must be addressed as an object store, never as a
    /// relative filesystem path. Writing `s3://bucket/ckpt` through `std::fs` does not fail — it
    /// creates a directory literally named `s3:` under the working directory, the query runs
    /// green, and the restart-resume guarantee it advertises is silently untrue.
    #[test]
    fn a_uri_checkpoint_location_is_never_written_to_the_local_filesystem() {
        let engine = Engine::new();
        let cwd = std::env::current_dir().unwrap();

        // Resolution alone must not create anything locally; the store is remote.
        let resolved = checkpoint_store(&engine, "s3://oxidant-test-bucket/ckpt/orders");
        assert!(
            !cwd.join("s3:").exists(),
            "an s3:// checkpoint created a local `s3:` directory"
        );
        // Either it resolved to a real S3 store, or it refused — never a silent local write.
        if let Ok(store) = resolved {
            assert_eq!(store.location(), "s3://oxidant-test-bucket/ckpt/orders");
        }
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
                vec![],
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

        let state = checkpoint_store(&engine, &checkpoint.path().to_string_lossy())
            .unwrap()
            .load()
            .await
            .unwrap();
        let offsets = state.source_offsets.expect("offsets committed");
        assert_eq!(offsets.source, "kafka-spool");
        assert_eq!(offsets.entries.get("offset"), Some(&2));
        assert_eq!(offsets.entries.get("file"), Some(&1));

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
        let state2 = checkpoint_store(&engine, &checkpoint.path().to_string_lossy())
            .unwrap()
            .load()
            .await
            .unwrap();
        assert_eq!(
            state2
                .source_offsets
                .as_ref()
                .and_then(|o| o.entries.get("offset")),
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
            _batch_id: u64,
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
