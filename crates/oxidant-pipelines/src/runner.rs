//! Pipeline execution: plan the DAG, drive streaming and derived tables, persist state.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use oxidant_common::{Error, Result};
use oxidant_config::{
    ExpectAction, OxidantConfig, PipelineConfig, TableConfig, TableKind, Trigger,
};
use oxidant_loom::Engine;
use oxidant_streaming::{
    checkpoint_store, CheckpointStore, LakeSink, LakeSinkOptions, LakeTarget, MicroBatchInput,
    MicroBatchPipeline, StartOptions, StreamQueryConfig, StreamingQueryManager,
    Trigger as StreamTrigger,
};

use crate::auto_cdc::CdcMerge;
use crate::cdc_sink::CdcMergeSink;
use crate::expectations;
use crate::graph::{Graph, Node};
use crate::output_write::{
    conform_batches_to_schema, flow_queries, parse_output_schema, union_flow_sql,
};
use crate::STREAM_ALIAS;

/// Events emitted while a pipeline runs. Callers render or forward them (CLI stderr, Spark
/// PipelineEvents in later phases).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunEvent {
    pub at: SystemTime,
    pub kind: RunEventKind,
}

/// Payload for [`RunEvent`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunEventKind {
    /// The run loop is starting with the resolved subgraph.
    PipelineStarted {
        name: String,
        table_count: usize,
        order: String,
    },
    /// A table is about to be processed this pass.
    TableStarted { name: String },
    /// A table finished successfully with new or replaced contents.
    TableUpdated {
        name: String,
        rows: u64,
        elapsed: Duration,
    },
    /// A derived table was left alone because nothing it reads moved this pass.
    TableUnchanged { name: String },
    /// A table was not run because an upstream table failed this pass.
    TableSkipped { name: String },
    /// A `once` flow target was skipped because it already completed.
    OnceFlowSkipped { name: String },
    /// A `warn` expectation saw failing rows.
    ExpectationViolation {
        table: String,
        label: String,
        failed_records: u64,
    },
    /// Binding the bare table name as a temp view failed (downstream may need FQ names).
    BareNameWarning {
        table: String,
        error: String,
        /// When true, downstream tables may need fully-qualified names.
        downstream_hint: bool,
    },
    /// A table update failed.
    TableFailed {
        name: String,
        error: String,
        elapsed: Duration,
    },
    /// A sink is being written in a format with no commit protocol.
    ///
    /// Parquet writes one file per batch and has no transaction log: a reader can see a
    /// partially written *run* (some batches landed, some did not), a replayed batch is
    /// appended rather than recognized, and there is no atomic replace. Nothing in the write
    /// path can enforce this, so say it out loud at run start.
    SinkWithoutCommitProtocol {
        table: String,
        path: String,
        format: String,
    },
    /// Checkpoint state could not be written.
    StatePersistFailed { error: String },
    /// A scheduled `reconcile` ran between two passes.
    ///
    /// `report` is the rendered drift report, printed whole: a scheduled reconcile has nobody
    /// watching a terminal, and a line saying only "drift" would send that person back to run the
    /// command by hand to learn what drifted.
    ReconcileFinished {
        cron: String,
        drifted: usize,
        /// Tables whose comparison could not be run at all — not drift, and not silence either.
        errored: usize,
        tables: usize,
        report: String,
    },
    /// A scheduled `reconcile` could not run. The pipeline keeps going.
    ReconcileFailed { cron: String, error: String },
    /// One pass over the subgraph finished.
    PassComplete { outcomes: Vec<TableOutcome> },
}

fn emit<F>(on_event: &mut F, kind: RunEventKind)
where
    F: FnMut(RunEvent),
{
    on_event(RunEvent {
        at: SystemTime::now(),
        kind,
    });
}

/// One table's outcome in a single pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableOutcome {
    pub name: String,
    pub rows: u64,
    pub elapsed: Duration,
    /// True when the table was left alone because nothing it reads moved this pass.
    pub unchanged: bool,
    /// `Some` when this table failed; its descendants are skipped for the pass.
    pub error: Option<String>,
    pub skipped: bool,
}

/// Status distilled from [`TableOutcome`] for event consumers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableStatus {
    Updated,
    Unchanged,
    Skipped,
    Failed { error: String },
}

impl TableOutcome {
    pub fn status(&self) -> TableStatus {
        if self.skipped {
            TableStatus::Skipped
        } else if self.unchanged {
            TableStatus::Unchanged
        } else if let Some(error) = &self.error {
            TableStatus::Failed {
                error: error.clone(),
            }
        } else {
            TableStatus::Updated
        }
    }
}

/// The config a pipeline run needs, with the sections it requires already checked.
pub struct Plan<'a> {
    pub config: &'a OxidantConfig,
    pub pipeline: &'a PipelineConfig,
    pub graph: Graph,
}

impl<'a> Plan<'a> {
    pub fn build(config: &'a OxidantConfig) -> Result<Self> {
        let pipeline = config.pipeline.as_ref().ok_or_else(|| {
            Error::Io(
                "this config declares no `pipeline:` section, so there is nothing to run — see \
                 docs/pipelines.md"
                    .into(),
            )
        })?;
        let graph = Graph::build(&config.tables)?;
        Ok(Self {
            config,
            pipeline,
            graph,
        })
    }

    pub fn table(&self, name: &str) -> Option<&'a TableConfig> {
        self.config.tables.iter().find(|t| t.name.trim() == name)
    }

    /// Fully-qualified target for a table: `{catalog}.{schema}.{name}`.
    pub fn target_of(&self, name: &str) -> String {
        format!("{}.{}.{name}", self.pipeline.catalog, self.pipeline.schema)
    }

    /// Sink format for a table, falling back to the pipeline default.
    pub fn format_of(&self, table: &TableConfig) -> String {
        table
            .format
            .clone()
            .unwrap_or_else(|| self.pipeline.format.clone())
    }

    /// Where a table's files live, when the pipeline pins a storage root.
    pub fn location_of(&self, name: &str) -> Option<String> {
        if let Some(table) = self.table(name) {
            if let Some(path) = &table.write_path {
                return Some(path.trim_end_matches('/').to_string());
            }
        }
        self.pipeline
            .storage
            .as_ref()
            .map(|root| format!("{}/{name}/", root.trim_end_matches('/')))
    }

    /// Catalog sink target, or `None` for a path-only external sink.
    ///
    /// An SDP sink is a write target with no catalog identity, so the streaming writer must be
    /// pointed at a location instead of a table name — see [`location_of`](Self::location_of).
    pub fn sink_table_of(&self, table: &TableConfig) -> Option<String> {
        if table.write_path.is_some() {
            None
        } else {
            Some(self.target_of(&table.name))
        }
    }
}

/// Drop persisted pipeline-state entries so the next pass recomputes affected tables.
///
/// `engine` resolves the checkpoint root, which may be an `s3://` URL — see
/// [`oxidant_streaming::checkpoint_store`].
pub async fn clear_pipeline_state(
    engine: &Engine,
    checkpoints: &str,
    tables: &[String],
) -> Result<()> {
    let store = checkpoint_store(engine, checkpoints)?;
    let mut state = PipelineState::load(&store).await;
    if tables.is_empty() {
        state.tables.clear();
        state.once_completed.clear();
    } else {
        for name in tables {
            state.tables.remove(name);
            state.once_completed.remove(name);
        }
    }
    state.save(&store).await
}

/// Run the pipeline subgraph, emitting events through `on_event`.
pub async fn run_pipeline<F>(
    engine: &Engine,
    plan: &Plan<'_>,
    wanted: &[String],
    force_once: bool,
    once_tables: &HashSet<String>,
    on_event: &mut F,
) -> Result<()>
where
    F: FnMut(RunEvent) + Send,
{
    let nodes = plan.graph.subgraph(wanted)?;
    if nodes.is_empty() {
        return Err(Error::Io("nothing to run".into()));
    }

    ensure_database(engine, plan).await?;

    engine.set_current_catalog(&plan.pipeline.catalog).await?;
    engine.set_current_namespace(&plan.pipeline.schema).await?;

    let trigger = if force_once {
        Trigger::Once
    } else {
        plan.pipeline.trigger.clone()
    };

    // Resolved and probed before a single table starts. The checkpoint root is where every
    // table's replay position lives; a root that is not writable is a pipeline that re-snapshots
    // on every restart, and an operator has to learn that here rather than an hour in.
    let checkpoints = checkpoint_store(engine, &plan.pipeline.checkpoints)?;
    checkpoints.probe().await?;

    let mut streams = StreamState::start(engine, plan, &nodes).await?;
    let mut state = PipelineState::load(&checkpoints).await;
    emit(
        on_event,
        RunEventKind::PipelineStarted {
            name: plan.pipeline.name.clone(),
            table_count: nodes.len(),
            order: nodes
                .iter()
                .map(|n| n.name.as_str())
                .collect::<Vec<_>>()
                .join(" -> "),
        },
    );
    for node in &nodes {
        let Some(table) = plan.table(&node.name) else {
            continue;
        };
        let Some(path) = table.write_path.as_deref() else {
            continue;
        };
        let format = plan.format_of(table);
        if !format.eq_ignore_ascii_case("delta") {
            emit(
                on_event,
                RunEventKind::SinkWithoutCommitProtocol {
                    table: node.name.clone(),
                    path: path.to_string(),
                    format,
                },
            );
        }
    }

    match trigger {
        Trigger::Once | Trigger::AvailableNow => {
            let outcomes = one_pass(
                engine,
                plan,
                &nodes,
                &mut streams,
                &mut state,
                true,
                once_tables,
                on_event,
            )
            .await;
            if let Err(e) = state.save(&checkpoints).await {
                emit(
                    on_event,
                    RunEventKind::StatePersistFailed {
                        error: e.to_string(),
                    },
                );
            }
            emit_pass_outcomes(&outcomes, on_event);
            if outcomes.iter().any(|o| o.error.is_some()) {
                return Err(Error::Execution(
                    "the pipeline finished with failed tables".into(),
                ));
            }
            Ok(())
        }
        Trigger::ProcessingTime(interval) => {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // This process' memory of when the reconcile schedule last ran, which is what keeps a
            // schedule whose file could not be written from firing on every single pass. See
            // `ReconcileSchedule::is_due_since`.
            let mut reconciled_at: Option<chrono::DateTime<chrono::Utc>> = None;
            loop {
                ticker.tick().await;
                let outcomes = one_pass(
                    engine,
                    plan,
                    &nodes,
                    &mut streams,
                    &mut state,
                    false,
                    once_tables,
                    on_event,
                )
                .await;
                if let Err(e) = state.save(&checkpoints).await {
                    emit(
                        on_event,
                        RunEventKind::StatePersistFailed {
                            error: e.to_string(),
                        },
                    );
                }
                emit_pass_outcomes(&outcomes, on_event);
                // Between triggers, never inside one: a reconcile that interleaved a micro-batch
                // would compare a target mid-write and report drift that the next commit closes.
                reconcile_tick(engine, plan, &checkpoints, &mut reconciled_at, on_event).await;
            }
        }
    }
}

/// Run the persisted `reconcile.json` schedule if it is due.
///
/// The standalone `oxidant pipeline reconcile` is the operational path; this exists so a schedule
/// registered with `--cron` fires without a second process to run it. It is deliberately the
/// simplest thing that works: the schedule is re-read each pass (so registering one does not need
/// a restart), it only ticks on the `ProcessingTime` trigger — a `once` run does one pass and
/// exits, and there is no "between triggers" there — and a reconcile that fails is reported and
/// then dropped. A drift report is not a reason to stop replicating; it is a reason to look.
async fn reconcile_tick<F>(
    engine: &Engine,
    plan: &Plan<'_>,
    checkpoints: &CheckpointStore,
    reconciled_at: &mut Option<chrono::DateTime<chrono::Utc>>,
    on_event: &mut F,
) where
    F: FnMut(RunEvent) + Send,
{
    let Some(mut schedule) = crate::reconcile::ReconcileSchedule::load(checkpoints).await else {
        return;
    };
    let started = chrono::Utc::now();
    if !schedule.is_due_since(*reconciled_at, started) {
        return;
    }
    // Claimed before the run rather than after it: whatever happens next — an error, a walk that
    // outlives the trigger interval, a `save` that cannot write — this pass has fired the
    // schedule, and the next one must not fire it again.
    *reconciled_at = Some(started);
    let options = crate::reconcile::ReconcileOptions {
        tables: schedule.tables.clone(),
        sample: schedule.sample,
    };
    let cron = schedule.cron.clone();
    let outcome = crate::reconcile::reconcile(engine, plan, &options).await;
    // The instant it *finished*. Anchoring on `started` would make a reconcile slower than its
    // own cron period due again the moment it lands, and the tick is awaited inline in the
    // trigger loop — so the pipeline would stop replicating and look hung.
    let finished = chrono::Utc::now();
    *reconciled_at = Some(finished);
    match outcome {
        Ok(report) => {
            schedule.record(
                finished,
                crate::reconcile::ReconcileSchedule::result_of(&report),
            );
            emit(
                on_event,
                RunEventKind::ReconcileFinished {
                    cron,
                    drifted: report.drifted(),
                    errored: report.errored(),
                    tables: report.tables.len(),
                    report: report.render(),
                },
            );
        }
        Err(e) => {
            // Stamped as a run even though it failed, so a source that is unreachable every
            // morning produces one report per schedule rather than one per micro-batch.
            schedule.record(finished, format!("failed: {e}"));
            emit(
                on_event,
                RunEventKind::ReconcileFailed {
                    cron,
                    error: e.to_string(),
                },
            );
        }
    }
    if let Err(e) = schedule.save(checkpoints).await {
        // Reported, not fatal — and not a reason to re-run: `reconciled_at` above is what the
        // next tick measures from when this file could not take the stamp.
        emit(
            on_event,
            RunEventKind::StatePersistFailed {
                error: format!("reconcile schedule: {e}"),
            },
        );
    }
}

fn emit_pass_outcomes<F>(outcomes: &[TableOutcome], on_event: &mut F)
where
    F: FnMut(RunEvent),
{
    for outcome in outcomes {
        let event = match outcome.status() {
            TableStatus::Updated => RunEventKind::TableUpdated {
                name: outcome.name.clone(),
                rows: outcome.rows,
                elapsed: outcome.elapsed,
            },
            TableStatus::Unchanged => RunEventKind::TableUnchanged {
                name: outcome.name.clone(),
            },
            TableStatus::Skipped => RunEventKind::TableSkipped {
                name: outcome.name.clone(),
            },
            TableStatus::Failed { error } => RunEventKind::TableFailed {
                name: outcome.name.clone(),
                error,
                elapsed: outcome.elapsed,
            },
        };
        emit(on_event, event);
    }
    emit(
        on_event,
        RunEventKind::PassComplete {
            outcomes: outcomes.to_vec(),
        },
    );
}

async fn ensure_database(engine: &Engine, plan: &Plan<'_>) -> Result<()> {
    let catalog = engine
        .external_catalog(&plan.pipeline.catalog)
        .ok_or_else(|| {
            Error::Plan(format!(
                "pipeline catalog `{}` is not registered — is it declared under `catalogs:`?",
                plan.pipeline.catalog
            ))
        })?;
    catalog
        .create_database(
            &plan.pipeline.schema,
            true,
            Some(format!("Oxidant pipeline `{}`", plan.pipeline.name)),
            None,
        )
        .await
}

struct StreamState {
    manager: Arc<StreamingQueryManager>,
    queries: BTreeMap<String, String>,
}

impl StreamState {
    async fn start(engine: &Engine, plan: &Plan<'_>, nodes: &[Node]) -> Result<Self> {
        let manager = Arc::new(StreamingQueryManager::new());
        let mut queries = BTreeMap::new();
        for node in nodes {
            let Some(table) = plan.table(&node.name) else {
                continue;
            };
            if !matches!(table.kind(), TableKind::Streaming | TableKind::AutoCdc) {
                continue;
            }
            let id = start_stream(engine, plan, table, &manager).await?;
            queries.insert(node.name.clone(), id);
        }
        Ok(Self { manager, queries })
    }
}

async fn start_stream(
    engine: &Engine,
    plan: &Plan<'_>,
    table: &TableConfig,
    manager: &StreamingQueryManager,
) -> Result<String> {
    let source = table
        .source
        .as_ref()
        .expect("a streaming table has a source");
    let name = table.name.trim();

    // A connector's operator log lives beside the pipeline's checkpoints, under a file named for
    // the table it feeds. Both are derived here rather than read from `options:` so one
    // connector's log can never be pointed at another's file.
    let mut source_options: BTreeMap<String, String> = source.options.clone();
    if source.format.trim().eq_ignore_ascii_case("postgres_cdc") {
        oxidant_streaming::postgres_cdc_pipeline_options(
            &mut source_options,
            &plan.pipeline.checkpoints,
            name,
        );
    }

    let config = StreamQueryConfig::for_pipeline(
        &source.format,
        source_options,
        &plan.format_of(table),
        plan.sink_table_of(table),
        plan.location_of(name),
        table.partition_by.clone(),
        table.dedup_columns.clone(),
        table.iceberg_compat.unwrap_or(plan.pipeline.iceberg_compat),
        table.iceberg_table_suffix.clone(),
        table.checkpoint_interval,
    );
    let mut config = config;
    config.expectations = expectations::counted(&table.expect)
        .into_iter()
        .map(
            |(label, expectation)| oxidant_streaming::StreamExpectation {
                label: label.to_string(),
                action: match expectation.action {
                    ExpectAction::Fail => oxidant_streaming::ExpectationAction::Fail,
                    _ => oxidant_streaming::ExpectationAction::Warn,
                },
                check: expectation.check.clone(),
            },
        )
        .collect();

    let declared_sql = table
        .sql
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let has_flows = declared_sql.is_some() || !table.append_flows.is_empty();
    let synthesized_sql = if table.auto_cdc.is_some() && !has_flows {
        Some(format!("SELECT * FROM {STREAM_ALIAS}"))
    } else {
        (!has_flows)
            .then(|| {
                expectations::has_drops(&table.expect)
                    .then(|| format!("SELECT * FROM {STREAM_ALIAS}"))
            })
            .flatten()
    };
    let needs_stream_plan = has_flows || synthesized_sql.is_some();
    let stream_input = if needs_stream_plan {
        let schema = oxidant_streaming::source_schema(Some(engine), &config)?;
        let input = Arc::new(MicroBatchInput::new(STREAM_ALIAS, schema)?);
        engine
            .ctx()
            .register_table(STREAM_ALIAS, input.provider())
            .map_err(|e| Error::Plan(format!("register streaming input: {e}")))?;
        Some(input)
    } else {
        None
    };
    let output_schema = table
        .output_schema
        .as_deref()
        .map(parse_output_schema)
        .transpose()?;
    let unioned = if has_flows {
        let flows = flow_queries(declared_sql, table.sql_by_name, &table.append_flows);
        Some(union_flow_sql(engine, &flows, table.output_schema.as_deref()).await?)
    } else {
        None
    };
    let pipeline = match unioned.or(synthesized_sql) {
        Some(sql) => {
            let input = stream_input.ok_or_else(|| {
                Error::Plan("streaming table pipeline is missing registered input".into())
            })?;
            let sql = expectations::apply_drops(&sql, &table.expect);
            let plan_result = engine.logical_plan(&sql).await;
            engine.deregister_table(STREAM_ALIAS);
            Some(MicroBatchPipeline {
                input,
                plan: plan_result?,
                output_schema,
            })
        }
        None => None,
    };

    let checkpoint = format!("{}/{name}", plan.pipeline.checkpoints.trim_end_matches('/'));

    let mut start_options = StartOptions {
        pipeline,
        current_catalog: plan.pipeline.catalog.clone(),
        current_namespace: vec![plan.pipeline.schema.clone()],
        sink_override: None,
    };
    if let Some(cdc) = table.auto_cdc.as_ref() {
        let target_fqn = plan.target_of(name);
        let format = oxidant_streaming::writable_format(&plan.format_of(table))?;
        let lake_target = LakeTarget::from_table_identifier(
            &target_fqn,
            &plan.pipeline.catalog,
            std::slice::from_ref(&plan.pipeline.schema),
            format,
            plan.location_of(name),
        )?;
        // The micro-batch schema is what the source's transformation produces; the *target*
        // schema is that projected through COLUMNS / COLUMNS * EXCEPT, so the sink has to be
        // opened on the merge's schema rather than the stream's.
        let batch_schema = if let Some(p) = &start_options.pipeline {
            Arc::new(p.plan.schema().as_arrow().clone())
        } else {
            oxidant_streaming::source_schema(Some(engine), &config)?
        };
        let merge = CdcMerge::new(cdc, &batch_schema, name)?;
        let sink_schema = merge.schema();
        let inner = LakeSink::open(
            engine,
            lake_target,
            sink_schema,
            LakeSinkOptions {
                app_id: Some(format!("{}::{name}", plan.pipeline.name)),
                partition_columns: table.partition_by.clone(),
                publish_iceberg: table.iceberg_compat.unwrap_or(plan.pipeline.iceberg_compat),
                iceberg_table_suffix: table
                    .iceberg_table_suffix
                    .clone()
                    .unwrap_or_else(|| oxidant_streaming::DEFAULT_ICEBERG_SUFFIX.to_string()),
                checkpoint_interval: table
                    .checkpoint_interval
                    .unwrap_or(oxidant_streaming::DEFAULT_CHECKPOINT_INTERVAL),
            },
        )
        .await?;
        start_options.sink_override = Some(Box::new(CdcMergeSink::new(
            engine.clone(),
            merge,
            target_fqn,
            inner,
        )));
    }

    let id = manager
        .start_with_config(
            engine,
            name.to_string(),
            checkpoint,
            StreamTrigger::AvailableNow,
            config,
            start_options,
        )
        .await?;
    Ok(id.id)
}

#[allow(clippy::too_many_arguments)]
async fn one_pass<F>(
    engine: &Engine,
    plan: &Plan<'_>,
    nodes: &[Node],
    streams: &mut StreamState,
    state: &mut PipelineState,
    drain: bool,
    once_tables: &HashSet<String>,
    on_event: &mut F,
) -> Vec<TableOutcome>
where
    F: FnMut(RunEvent) + Send,
{
    let mut outcomes: Vec<TableOutcome> = Vec::new();
    let mut failed: Vec<String> = Vec::new();
    let mut changed: Vec<String> = Vec::new();

    for node in nodes {
        emit(
            on_event,
            RunEventKind::TableStarted {
                name: node.name.clone(),
            },
        );

        if node.depends_on.iter().any(|d| failed.contains(d)) {
            outcomes.push(TableOutcome {
                name: node.name.clone(),
                rows: 0,
                elapsed: Duration::ZERO,
                error: None,
                skipped: true,
                unchanged: false,
            });
            failed.push(node.name.clone());
            continue;
        }
        let Some(table) = plan.table(&node.name) else {
            continue;
        };
        if once_tables.contains(&node.name) && state.once_completed(&node.name) {
            emit(
                on_event,
                RunEventKind::OnceFlowSkipped {
                    name: node.name.clone(),
                },
            );
            outcomes.push(TableOutcome {
                name: node.name.clone(),
                rows: 0,
                elapsed: Duration::ZERO,
                error: None,
                skipped: false,
                unchanged: true,
            });
            continue;
        }
        let definition = definition_fingerprint(table);
        if table.kind() == TableKind::Derived
            && state.built_as(&node.name, &definition)
            && !node.reads_outside_pipeline
            && !node.depends_on.iter().any(|d| changed.contains(d))
        {
            if let Err(e) = bind_bare_name(engine, plan, &node.name).await {
                emit(
                    on_event,
                    RunEventKind::BareNameWarning {
                        table: node.name.clone(),
                        error: e.to_string(),
                        downstream_hint: false,
                    },
                );
            }
            outcomes.push(TableOutcome {
                name: node.name.clone(),
                rows: 0,
                elapsed: Duration::ZERO,
                error: None,
                skipped: false,
                unchanged: true,
            });
            continue;
        }

        let started = Instant::now();
        let result = match table.kind() {
            TableKind::Streaming | TableKind::AutoCdc => {
                advance_stream(engine, streams, &node.name, drain).await
            }
            TableKind::Derived => recompute(engine, plan, table, state, on_event).await,
        };
        match result {
            Ok(rows) => {
                if rows > 0 || table.kind() == TableKind::Derived {
                    changed.push(node.name.clone());
                }
                if table.kind() == TableKind::Derived {
                    state.mark_built(&node.name, &definition);
                }
                if once_tables.contains(&node.name) {
                    state.mark_once_completed(&node.name);
                }
                if let Err(e) = bind_bare_name(engine, plan, &node.name).await {
                    emit(
                        on_event,
                        RunEventKind::BareNameWarning {
                            table: node.name.clone(),
                            error: e.to_string(),
                            downstream_hint: true,
                        },
                    );
                }
                outcomes.push(TableOutcome {
                    name: node.name.clone(),
                    rows,
                    elapsed: started.elapsed(),
                    error: None,
                    skipped: false,
                    unchanged: false,
                });
            }
            Err(e) => {
                failed.push(node.name.clone());
                outcomes.push(TableOutcome {
                    name: node.name.clone(),
                    rows: 0,
                    elapsed: started.elapsed(),
                    error: Some(e.to_string()),
                    skipped: false,
                    unchanged: false,
                });
            }
        }
    }
    outcomes
}

async fn bind_bare_name(engine: &Engine, plan: &Plan<'_>, name: &str) -> Result<()> {
    let Some(table) = plan.table(name) else {
        return Ok(());
    };
    if table.write_path.is_some() {
        return Ok(());
    }
    let target = plan.target_of(name);
    let _ = engine.refresh_table(&target).await;
    engine
        .sql(&format!(
            "CREATE OR REPLACE TEMPORARY VIEW {name} AS SELECT * FROM {target}"
        ))
        .await
        .map(|_| ())
}

async fn advance_stream(
    engine: &Engine,
    streams: &StreamState,
    name: &str,
    drain: bool,
) -> Result<u64> {
    let Some(id) = streams.queries.get(name) else {
        return Ok(0);
    };
    if drain {
        streams.manager.process_all_available(id, engine).await
    } else {
        streams.manager.run_batch(id, engine).await
    }
}

async fn recompute<F>(
    engine: &Engine,
    plan: &Plan<'_>,
    table: &TableConfig,
    state: &mut PipelineState,
    on_event: &mut F,
) -> Result<u64>
where
    F: FnMut(RunEvent) + Send,
{
    let name = table.name.trim();
    let flows = flow_queries(table.sql.as_deref(), table.sql_by_name, &table.append_flows);
    let sql = union_flow_sql(engine, &flows, table.output_schema.as_deref()).await?;
    let sql = sql.trim();
    if sql.is_empty() {
        return Err(Error::Plan(format!("derived table `{name}` has no `sql:`")));
    }

    for (label, expectation) in expectations::counted(&table.expect) {
        let count_sql = expectations::violation_count_sql(sql, &expectation.check);
        let violations = scalar_count(engine, &count_sql).await?;
        if violations == 0 {
            continue;
        }
        match expectation.action {
            ExpectAction::Fail => {
                return Err(Error::Execution(format!(
                    "table `{name}` expectation `{label}` failed: {violations} record(s) do not \
                     satisfy `{}`; the table is unchanged",
                    expectation.check
                )))
            }
            ExpectAction::Warn => {
                emit(
                    on_event,
                    RunEventKind::ExpectationViolation {
                        table: name.to_string(),
                        label: label.to_string(),
                        failed_records: violations,
                    },
                );
            }
            ExpectAction::Drop => {}
        }
    }

    let effective_sql = expectations::apply_drops(sql, &table.expect);
    let mut batches = engine.sql(&effective_sql).await?;
    let declared_schema = table
        .output_schema
        .as_deref()
        .map(parse_output_schema)
        .transpose()?;
    if let Some(target) = &declared_schema {
        batches = conform_batches_to_schema(batches, target)?;
    }
    let schema = if let Some(target) = &declared_schema {
        target.clone()
    } else if let Some(batch) = batches.first() {
        batch.schema()
    } else {
        engine.schema(&effective_sql).await?
    };

    let format = oxidant_streaming::writable_format(&plan.format_of(table))?;
    let target = if let Some(path) = &table.write_path {
        LakeTarget::location_only(path, format)
    } else {
        LakeTarget::from_table_identifier(
            &plan.target_of(name),
            &plan.pipeline.catalog,
            std::slice::from_ref(&plan.pipeline.schema),
            format,
            plan.location_of(name),
        )?
    };
    let mut sink = LakeSink::open(
        engine,
        target,
        schema,
        LakeSinkOptions {
            app_id: Some(format!("{}::{name}", plan.pipeline.name)),
            partition_columns: table.partition_by.clone(),
            publish_iceberg: table.iceberg_compat.unwrap_or(plan.pipeline.iceberg_compat),
            iceberg_table_suffix: table
                .iceberg_table_suffix
                .clone()
                .unwrap_or_else(|| oxidant_streaming::DEFAULT_ICEBERG_SUFFIX.to_string()),
            checkpoint_interval: table
                .checkpoint_interval
                .unwrap_or(oxidant_streaming::DEFAULT_CHECKPOINT_INTERVAL),
        },
    )
    .await?;

    let version = state
        .next_epoch(name)
        .max(sink.committed_txn_version().max(0) as u64 + 1);
    state.tables.entry(name.to_string()).or_default().epoch = version;
    sink.replace_batch(&batches, version).await
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct PipelineState {
    tables: BTreeMap<String, TableState>,
    #[serde(default)]
    once_completed: BTreeSet<String>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct TableState {
    epoch: u64,
    built: bool,
    #[serde(default)]
    definition: String,
}

impl PipelineState {
    /// The per-table epochs, relative to the checkpoint root.
    const KEY: &'static str = "_pipeline-state.json";

    /// The state, or a fresh one — an unreadable or corrupt object reads as fresh, which is what
    /// makes a first run and a torn write behave the same way: rebuild.
    async fn load(checkpoints: &CheckpointStore) -> Self {
        checkpoints
            .read(Self::KEY)
            .await
            .ok()
            .flatten()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default()
    }

    /// Replace the state.
    ///
    /// No staged temporary file: this goes through the checkpoint's object store, where a `PUT`
    /// is atomic — a reader sees the whole old object or the whole new one. That is the same
    /// reason `CheckpointStore::save` dropped its own staging file.
    async fn save(&self, checkpoints: &CheckpointStore) -> Result<()> {
        let text = serde_json::to_vec_pretty(self)
            .map_err(|e| Error::Io(format!("serializing pipeline state: {e}")))?;
        checkpoints
            .write(Self::KEY, text)
            .await
            .map_err(|e| Error::Io(format!("writing `{}`: {e}", checkpoints.uri(Self::KEY))))?;
        Ok(())
    }

    fn next_epoch(&mut self, table: &str) -> u64 {
        let slot = self.tables.entry(table.to_string()).or_default();
        slot.epoch += 1;
        slot.epoch
    }

    fn built_as(&self, table: &str, definition: &str) -> bool {
        self.tables
            .get(table)
            .is_some_and(|t| t.built && t.definition == definition)
    }

    fn mark_built(&mut self, table: &str, definition: &str) {
        let slot = self.tables.entry(table.to_string()).or_default();
        slot.built = true;
        slot.definition = definition.to_string();
    }

    fn once_completed(&self, table: &str) -> bool {
        self.once_completed.contains(table)
    }

    fn mark_once_completed(&mut self, table: &str) {
        self.once_completed.insert(table.to_string());
    }
}

/// Hash of everything that decides what a table's contents are and where they land.
///
/// `write_path` is `#[serde(skip)]` — it is not an `oxidant.yaml` key, only a lowering of an SDP
/// sink — so it is absent from the serialized form and has to be folded in by hand. Without it a
/// sink whose path changed but whose SQL did not would fingerprint identically, be reported
/// `unchanged`, and write nothing to the new location.
fn definition_fingerprint(table: &TableConfig) -> String {
    use std::hash::{Hash, Hasher};
    let text = serde_json::to_string(table).unwrap_or_default();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    text.hash(&mut hasher);
    table.write_path.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

async fn scalar_count(engine: &Engine, sql: &str) -> Result<u64> {
    let batches = engine.sql(sql).await?;
    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        if let Some(values) = batch
            .column(0)
            .as_any()
            .downcast_ref::<oxidant_loom::arrow::array::Int64Array>()
        {
            return Ok(values.value(0).max(0) as u64);
        }
    }
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_two_postgres_cdc_option_lists_are_the_same_list() {
        // `oxidant-config` validates a file's `options:` without a database, and
        // `oxidant-streaming` parses them at pipeline start. Neither crate can see the other —
        // this one depends on both, which is why the check lives here. Drift either way is
        // silent until someone hits it: an option the source accepts but the validator does not
        // makes a valid config fail `oxidant config validate`, and the reverse makes a config
        // that validates fail at start.
        assert_eq!(
            oxidant_config::POSTGRES_CDC_OPTIONS,
            oxidant_streaming::KNOWN_OPTIONS,
            "add the option to both lists, or to neither"
        );
    }

    /// A pipeline whose `postgres_cdc` source points at a closed port, so a reconcile fails
    /// immediately and without a database.
    fn unreachable_cdc_config(checkpoints: &str) -> oxidant_config::OxidantConfig {
        oxidant_config::OxidantConfig::parse(&format!(
            "catalogs:
  local:
    type: local
    warehouse: {checkpoints}/warehouse
pipeline:
  name: sales-cdc
  catalog: local
  schema: live
  checkpoints: {checkpoints}
tables:
  - name: sales_suppliers
    source:
      format: postgres_cdc
      options:
        host: 127.0.0.1
        port: \"1\"
        database: sales
        user: oxidant_cdc
        tls: disable
        publication: oxidant_sales
        slot: oxidant_sales_suppliers
        tables: public.sales_suppliers
    auto_cdc:
      source: sales_suppliers_changes
      keys: [supplierid]
      sequence_by: __oxidant_lsn
"
        ))
        .expect("the fixture config parses")
    }

    #[tokio::test]
    async fn a_tick_whose_schedule_cannot_be_written_does_not_re_run_on_the_next_pass() {
        // The schedule's anchor lives in `reconcile.json`. When that file cannot be written — a
        // read-only checkpoint volume, a permissions change, an NFS blip — the anchor never
        // advances, and with the file as the only anchor the schedule is due again on the very
        // next pass: at a 200ms trigger that is a `count(*)` plus a sampled ordered scan against
        // the publisher five times a second, indefinitely, with one `StatePersistFailed` line per
        // round as the only symptom.
        let dir = tempfile::TempDir::new().unwrap();
        let checkpoints = dir.path().to_string_lossy().into_owned();
        let config = unreachable_cdc_config(&checkpoints);
        let plan = Plan::build(&config).expect("plans");

        let schedule = crate::reconcile::ReconcileSchedule {
            path: None,
            cron: "* * * * *".into(),
            tables: vec![],
            sample: 10,
            // Backdated, so the first tick is due rather than due in a minute.
            created: "2020-01-01T00:00:00Z".into(),
            last_run: None,
            last_result: None,
        };
        let engine = Engine::new();
        let store = checkpoint_store(&engine, &checkpoints).expect("resolves");
        schedule.save(&store).await.expect("writes the schedule");
        let path = dir.path().join(crate::reconcile::ReconcileSchedule::KEY);
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(&path, permissions).expect("makes the anchor unwritable");

        let mut reconciled_at = None;
        let (mut ran, mut persist_failed) = (0, 0);
        let mut on_event = |event: RunEvent| match event.kind {
            RunEventKind::ReconcileFinished { .. } | RunEventKind::ReconcileFailed { .. } => {
                ran += 1
            }
            RunEventKind::StatePersistFailed { .. } => persist_failed += 1,
            _ => {}
        };
        for _ in 0..3 {
            reconcile_tick(&engine, &plan, &store, &mut reconciled_at, &mut on_event).await;
        }

        assert_eq!(
            persist_failed, 1,
            "the anchor really was unwritable — without that this test proves nothing"
        );
        assert_eq!(
            ran, 1,
            "the schedule fired once; the passes after it are not due until the next minute"
        );
        assert!(
            reconciled_at.is_some(),
            "and the run this process remembers is what says so"
        );
    }

    #[test]
    fn pass_outcomes_map_to_run_events() {
        let outcomes = vec![
            TableOutcome {
                name: "a".into(),
                rows: 3,
                elapsed: Duration::from_millis(1500),
                unchanged: false,
                skipped: false,
                error: None,
            },
            TableOutcome {
                name: "b".into(),
                rows: 0,
                elapsed: Duration::ZERO,
                unchanged: true,
                skipped: false,
                error: None,
            },
        ];
        let mut events = Vec::new();
        emit_pass_outcomes(&outcomes, &mut |event| events.push(event));
        assert_eq!(events.len(), 3);
        assert!(matches!(
            events[0].kind,
            RunEventKind::TableUpdated { ref name, rows: 3, .. } if name == "a"
        ));
        assert!(matches!(
            events[1].kind,
            RunEventKind::TableUnchanged { ref name } if name == "b"
        ));
        assert!(matches!(events[2].kind, RunEventKind::PassComplete { .. }));
    }

    #[test]
    fn table_outcome_status_classifies_skipped_and_failed() {
        let skipped = TableOutcome {
            name: "x".into(),
            rows: 0,
            elapsed: Duration::ZERO,
            unchanged: false,
            skipped: true,
            error: None,
        };
        assert_eq!(skipped.status(), TableStatus::Skipped);

        let failed = TableOutcome {
            name: "y".into(),
            rows: 0,
            elapsed: Duration::from_secs(1),
            unchanged: false,
            skipped: false,
            error: Some("boom".into()),
        };
        assert!(matches!(
            failed.status(),
            TableStatus::Failed { error } if error == "boom"
        ));
    }
}
