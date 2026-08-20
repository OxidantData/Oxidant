//! Pipeline execution: plan the DAG, drive streaming and derived tables, persist state.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use oxidant_common::{Error, Result};
use oxidant_config::{
    ExpectAction, OxidantConfig, PipelineConfig, TableConfig, TableKind, Trigger,
};
use oxidant_loom::Engine;
use oxidant_streaming::{
    LakeSink, LakeSinkOptions, LakeTarget, MicroBatchInput, MicroBatchPipeline, StartOptions,
    StreamQueryConfig, StreamingQueryManager, Trigger as StreamTrigger,
};

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
    /// Checkpoint state could not be written.
    StatePersistFailed { error: String },
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
        self.pipeline
            .storage
            .as_ref()
            .map(|root| format!("{}/{name}/", root.trim_end_matches('/')))
    }
}

/// Drop persisted pipeline-state entries so the next pass recomputes affected tables.
pub fn clear_pipeline_state(checkpoints: &str, tables: &[String]) -> Result<()> {
    let mut state = PipelineState::load(checkpoints);
    if tables.is_empty() {
        state.tables.clear();
        state.once_completed.clear();
    } else {
        for name in tables {
            state.tables.remove(name);
            state.once_completed.remove(name);
        }
    }
    state.save(checkpoints)
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

    let mut streams = StreamState::start(engine, plan, &nodes).await?;
    let mut state = PipelineState::load(&plan.pipeline.checkpoints);
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
            if let Err(e) = state.save(&plan.pipeline.checkpoints) {
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
                if let Err(e) = state.save(&plan.pipeline.checkpoints) {
                    emit(
                        on_event,
                        RunEventKind::StatePersistFailed {
                            error: e.to_string(),
                        },
                    );
                }
                emit_pass_outcomes(&outcomes, on_event);
            }
        }
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
            if table.kind() != TableKind::Streaming {
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

    let config = StreamQueryConfig::for_pipeline(
        &source.format,
        source.options.clone().into_iter().collect(),
        &plan.format_of(table),
        plan.target_of(name),
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
    let synthesized_sql = (!has_flows)
        .then(|| {
            expectations::has_drops(&table.expect).then(|| format!("SELECT * FROM {STREAM_ALIAS}"))
        })
        .flatten();
    let needs_stream_plan = has_flows || synthesized_sql.is_some();
    let stream_input = if needs_stream_plan {
        let schema = oxidant_streaming::source_schema(&config)?;
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
    let id = manager
        .start_with_config(
            engine,
            name.to_string(),
            checkpoint,
            StreamTrigger::AvailableNow,
            config,
            StartOptions {
                pipeline,
                current_catalog: plan.pipeline.catalog.clone(),
                current_namespace: vec![plan.pipeline.schema.clone()],
            },
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
            TableKind::Streaming => advance_stream(engine, streams, &node.name, drain).await,
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
    let target = LakeTarget::from_table_identifier(
        &plan.target_of(name),
        &plan.pipeline.catalog,
        std::slice::from_ref(&plan.pipeline.schema),
        format,
        plan.location_of(name),
    )?;
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
    fn path(checkpoints: &str) -> PathBuf {
        Path::new(checkpoints).join("_pipeline-state.json")
    }

    fn load(checkpoints: &str) -> Self {
        std::fs::read(Self::path(checkpoints))
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default()
    }

    fn save(&self, checkpoints: &str) -> Result<()> {
        let path = Self::path(checkpoints);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| Error::Io(format!("creating `{}`: {e}", parent.display())))?;
        }
        let text = serde_json::to_vec_pretty(self)
            .map_err(|e| Error::Io(format!("serializing pipeline state: {e}")))?;
        let staging = path.with_extension("json.tmp");
        std::fs::write(&staging, text)
            .map_err(|e| Error::Io(format!("writing `{}`: {e}", staging.display())))?;
        std::fs::rename(&staging, &path)
            .map_err(|e| Error::Io(format!("writing `{}`: {e}", path.display())))?;
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

fn definition_fingerprint(table: &TableConfig) -> String {
    use std::hash::{Hash, Hasher};
    let text = serde_json::to_string(table).unwrap_or_default();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    text.hash(&mut hasher);
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
