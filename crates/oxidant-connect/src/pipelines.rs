//! Spark Declarative Pipelines (`PipelineCommand`) handlers (SDP Phase 1A–2).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::SystemTime;

use datafusion::sql::unparser::Unparser;
use oxidant_config::{
    AutoCdcConfig, OxidantConfig, PipelineConfig, SourceConfig, TableConfig, Trigger,
};
use oxidant_loom::Engine;
use oxidant_pipelines::{
    clear_pipeline_state, parse, run_pipeline, split_table_properties, validate_auto_cdc,
    validate_output_format, OutputKind, Plan, RunEvent, RunEventKind,
};
use oxidant_proto::spark::connect as sc;
use prost_types::Timestamp;
use tonic::Status;
use uuid::Uuid;

use crate::translate;
use crate::OxidantService;

/// Outcome of a [`PipelineCommand`] handler. [`Failed`](Self::Failed) streams buffered
/// `PipelineEvent` responses before the terminal gRPC error (StartRun only).
pub(crate) enum PipelineCommandOutput {
    Complete(Vec<sc::ExecutePlanResponse>),
    Failed {
        responses: Vec<sc::ExecutePlanResponse>,
        status: Status,
    },
}

/// Session-scoped registry of in-memory dataflow graphs keyed by graph id.
#[derive(Default)]
pub struct DataflowGraphRegistry {
    graphs: std::sync::Mutex<HashMap<String, GraphEntry>>,
}

struct GraphEntry {
    session_id: String,
    graph: DataflowGraph,
}

/// In-memory graph state between `CreateDataflowGraph` and `StartRun`.
#[derive(Debug, Clone)]
pub struct DataflowGraph {
    pub default_catalog: Option<String>,
    pub default_database: Option<String>,
    #[allow(dead_code)]
    pub sql_conf: HashMap<String, String>,
    pub outputs: Vec<OutputDef>,
    pub flows: Vec<FlowDef>,
    /// `REFRESH MATERIALIZED VIEW` / `OR REFRESH` requests applied at the next `StartRun`.
    pub refreshes: Vec<String>,
    #[allow(dead_code)]
    pub created_at: SystemTime,
}

/// Mirrors `PipelineCommand.DefineOutput`.
#[derive(Debug, Clone)]
pub struct OutputDef {
    pub output_name: String,
    pub resolved: sc::ResolvedIdentifier,
    pub output_type: i32,
    pub comment: Option<String>,
    pub table_details: Option<sc::pipeline_command::define_output::TableDetails>,
    #[allow(dead_code)]
    pub sink_details: Option<sc::pipeline_command::define_output::SinkDetails>,
    pub source_code_location: Option<sc::SourceCodeLocation>,
}

/// Mirrors `PipelineCommand.DefineFlow`; relation may stay unresolved until
/// `DefineFlowQueryFunctionResult` or `StartRun`.
#[derive(Debug, Clone)]
pub struct FlowDef {
    pub flow_name: String,
    #[allow(dead_code)]
    pub resolved: sc::ResolvedIdentifier,
    pub target: sc::ResolvedIdentifier,
    pub sql_conf: HashMap<String, String>,
    /// Routes query-function evaluation signals to the client's signal stream.
    pub client_id: Option<String>,
    pub relation: Option<sc::Relation>,
    /// Populated by `DefineSqlGraphElements` when the flow comes from SDP SQL text.
    pub query_sql: Option<String>,
    pub auto_cdc: Option<AutoCdcConfig>,
    pub once: bool,
    pub by_name: bool,
    pub source_code_location: Option<sc::SourceCodeLocation>,
}

impl DataflowGraphRegistry {
    pub fn insert(&self, session_id: &str, graph_id: String, graph: DataflowGraph) {
        self.graphs
            .lock()
            .expect("dataflow graphs poisoned")
            .insert(
                graph_id,
                GraphEntry {
                    session_id: session_id.to_string(),
                    graph,
                },
            );
    }

    pub fn remove(&self, graph_id: &str) -> bool {
        self.graphs
            .lock()
            .expect("dataflow graphs poisoned")
            .remove(graph_id)
            .is_some()
    }

    pub fn drop_session(&self, session_id: &str) {
        let mut graphs = self.graphs.lock().expect("dataflow graphs poisoned");
        graphs.retain(|_, entry| entry.session_id != session_id);
    }

    fn with_graph<F, T>(&self, graph_id: &str, session_id: &str, f: F) -> Result<T, Status>
    where
        F: FnOnce(&mut DataflowGraph) -> Result<T, Status>,
    {
        let mut graphs = self.graphs.lock().expect("dataflow graphs poisoned");
        let entry = graphs.get_mut(graph_id).ok_or_else(|| {
            Status::invalid_argument(format!("unknown dataflow graph `{graph_id}`"))
        })?;
        if entry.session_id != session_id {
            return Err(Status::invalid_argument(format!(
                "dataflow graph `{graph_id}` belongs to another session"
            )));
        }
        f(&mut entry.graph)
    }

    fn get_graph(&self, graph_id: &str, session_id: &str) -> Result<DataflowGraph, Status> {
        let graphs = self.graphs.lock().expect("dataflow graphs poisoned");
        let entry = graphs.get(graph_id).ok_or_else(|| {
            Status::invalid_argument(format!("unknown dataflow graph `{graph_id}`"))
        })?;
        if entry.session_id != session_id {
            return Err(Status::invalid_argument(format!(
                "dataflow graph `{graph_id}` belongs to another session"
            )));
        }
        Ok(entry.graph.clone())
    }
}

/// Resolve a partially- or fully-qualified name against graph defaults into a multipart identifier.
pub fn resolve_identifier(
    name: &str,
    default_catalog: Option<&str>,
    default_database: Option<&str>,
) -> sc::ResolvedIdentifier {
    let trimmed = name.trim();
    let parts: Vec<&str> = trimmed.split('.').filter(|p| !p.is_empty()).collect();
    match parts.len() {
        0 => sc::ResolvedIdentifier {
            catalog_name: default_catalog.unwrap_or_default().to_string(),
            namespace: default_database
                .map(|d| vec![d.to_string()])
                .unwrap_or_default(),
            table_name: String::new(),
        },
        1 => sc::ResolvedIdentifier {
            catalog_name: default_catalog.unwrap_or_default().to_string(),
            namespace: default_database
                .map(|d| vec![d.to_string()])
                .unwrap_or_default(),
            table_name: parts[0].to_string(),
        },
        2 => sc::ResolvedIdentifier {
            catalog_name: default_catalog.unwrap_or_default().to_string(),
            namespace: vec![parts[0].to_string()],
            table_name: parts[1].to_string(),
        },
        n => {
            let catalog = parts[0].to_string();
            let table = parts[n - 1].to_string();
            let namespace = parts[1..n - 1].iter().map(|s| (*s).to_string()).collect();
            sc::ResolvedIdentifier {
                catalog_name: catalog,
                namespace,
                table_name: table,
            }
        }
    }
}

fn identifiers_match(a: &sc::ResolvedIdentifier, b: &sc::ResolvedIdentifier) -> bool {
    a.catalog_name == b.catalog_name && a.namespace == b.namespace && a.table_name == b.table_name
}

fn flow_needs_query_function_evaluation(flow: &FlowDef) -> bool {
    // An AUTO CDC flow carries neither a relation nor SDP SQL text, but it is fully defined —
    // reporting it as pending would ask the client for a query function it never registered.
    flow.relation.is_none() && flow.query_sql.is_none() && flow.auto_cdc.is_none()
}

fn flow_matches_client_id(flow: &FlowDef, request_client_id: Option<&str>) -> bool {
    match (flow.client_id.as_deref(), request_client_id) {
        (None, _) => true,
        (Some(flow_id), Some(req_id)) => flow_id == req_id,
        (Some(_), None) => false,
    }
}

fn pending_query_function_flows(
    graph: &DataflowGraph,
    request_client_id: Option<&str>,
) -> Vec<sc::ResolvedIdentifier> {
    graph
        .flows
        .iter()
        .filter(|flow| {
            flow_needs_query_function_evaluation(flow)
                && flow_matches_client_id(flow, request_client_id)
        })
        .map(|flow| flow.resolved.clone())
        .collect()
}

/// The flow a `DefineFlowQueryFunctionResult` addresses, and whether it may be backfilled.
enum BackfillTarget {
    /// Awaiting its query-function result: neither a relation nor SDP SQL text, i.e. exactly what
    /// the signal stream reported as pending.
    Pending(usize),
    /// The flow exists but is already defined — by an earlier backfill, or by SDP SQL from
    /// `DefineSqlGraphElements`. Backfilling it would silently replace that definition, because
    /// `graph_to_config` prefers `relation` over `query_sql`, so it is rejected instead.
    AlreadyDefined(usize),
    NotFound,
}

/// Resolve the addressed flow. Matching is by identifier or name, but the result is *also* gated
/// on the flow still being pending: backfill is accepted **once**, and only against a flow the
/// signal stream actually signalled. Without that gate a stale or duplicated result silently
/// overwrites a resolved relation, and — worse — one naming an SDP-SQL flow replaces the query
/// its `.sql` file defined, with no diagnostic and no source location.
fn find_backfill_target(
    graph: &DataflowGraph,
    cmd: &sc::pipeline_command::DefineFlowQueryFunctionResult,
) -> BackfillTarget {
    let index = if let Some(id) = cmd.flow_identifier.as_ref() {
        graph
            .flows
            .iter()
            .position(|flow| identifiers_match(&flow.resolved, id))
    } else {
        #[allow(deprecated)]
        let name = cmd.flow_name.as_deref().filter(|s| !s.is_empty());
        // Pre-4.2 clients echo back whatever we put in the deprecated `flow_names` signal field,
        // which is fully qualified — accept both that and the short `DefineFlow.flow_name`.
        name.and_then(|name| {
            graph.flows.iter().position(|flow| {
                flow.flow_name == name || resolved_identifier_key(&flow.resolved) == name
            })
        })
    };
    match index {
        None => BackfillTarget::NotFound,
        Some(idx) if flow_needs_query_function_evaluation(&graph.flows[idx]) => {
            BackfillTarget::Pending(idx)
        }
        Some(idx) => BackfillTarget::AlreadyDefined(idx),
    }
}

/// Human-readable name of the flow a `DefineFlowQueryFunctionResult` addressed, for errors.
fn requested_flow_label(cmd: &sc::pipeline_command::DefineFlowQueryFunctionResult) -> String {
    if let Some(id) = cmd.flow_identifier.as_ref() {
        return resolved_identifier_key(id);
    }
    #[allow(deprecated)]
    cmd.flow_name.clone().unwrap_or_default()
}

fn is_temporary_view_output(output: &OutputDef) -> bool {
    output.output_type == sc::OutputType::TemporaryView as i32
}

fn flow_targets_temporary_view(graph: &DataflowGraph, flow: &FlowDef) -> bool {
    graph
        .outputs
        .iter()
        .any(|o| is_temporary_view_output(o) && identifiers_match(&o.resolved, &flow.target))
}

fn output_dedup_key(graph: &DataflowGraph, name: &str) -> String {
    resolved_identifier_key(&resolve_identifier(
        name,
        graph.default_catalog.as_deref(),
        graph.default_database.as_deref(),
    ))
}

/// Fully-qualified map key for a resolved identifier (catalog.namespace.table).
fn resolved_identifier_key(id: &sc::ResolvedIdentifier) -> String {
    let mut parts = Vec::new();
    if !id.catalog_name.is_empty() {
        parts.push(id.catalog_name.as_str());
    }
    for ns in &id.namespace {
        parts.push(ns.as_str());
    }
    if !id.table_name.is_empty() {
        parts.push(id.table_name.as_str());
    }
    parts.join(".")
}

/// Whether `sql_conf` is applied at graph scope (with catalog sync) or per-flow scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlConfScope {
    Graph,
    Flow,
}

/// Keys accepted into the session-local Connect config store during SDP `sql_conf`
/// application. Anything else is ignored with a `PipelineEvent`.
///
/// Catalog keys (`spark.sql.catalog.*`, `spark.sql.defaultCatalog`) are graph-level only:
/// flow-level entries are ignored because `sync_catalogs()` runs once per `StartRun`, not per flow.
fn is_known_sql_conf_key(key: &str, scope: SqlConfScope) -> bool {
    if scope == SqlConfScope::Flow
        && (key.starts_with("spark.sql.catalog.") || key == "spark.sql.defaultCatalog")
    {
        return false;
    }
    key.starts_with("spark.sql.catalog.")
        || key == "spark.sql.defaultCatalog"
        || key.starts_with("spark.sql.session.")
        || key.starts_with("spark.oxidant.")
        || key.starts_with("engine.")
}

fn format_source_location(loc: &sc::SourceCodeLocation) -> Option<String> {
    let file = loc.file_name.as_deref().filter(|s| !s.is_empty())?;
    let line = loc.line_number.filter(|n| *n > 0)?;
    Some(format!("{file}:{line}"))
}

fn source_location_label(loc: &Option<sc::SourceCodeLocation>) -> Option<String> {
    loc.as_ref().and_then(format_source_location)
}

fn table_source_locations(graph: &DataflowGraph) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for output in &graph.outputs {
        if let Some(label) = source_location_label(&output.source_code_location) {
            out.insert(resolved_identifier_key(&output.resolved), label);
        }
    }
    for flow in &graph.flows {
        if let Some(label) = source_location_label(&flow.source_code_location) {
            out.insert(resolved_identifier_key(&flow.target), label);
        }
    }
    out
}

fn source_location_for_table<'a>(
    locations: &'a HashMap<String, String>,
    table: &str,
) -> Option<&'a String> {
    locations.get(table).or_else(|| {
        locations
            .iter()
            .find_map(|(key, label)| key.ends_with(&format!(".{table}")).then_some(label))
    })
}

/// Prior values for keys touched by a scoped `sql_conf` application.
struct SqlConfSnapshot {
    previous: HashMap<String, Option<String>>,
}

/// Restores graph-scoped `sql_conf` and re-syncs the observability environment on every exit.
struct SqlConfGuard<'a> {
    svc: &'a OxidantService,
    snap: Option<SqlConfSnapshot>,
}

impl<'a> SqlConfGuard<'a> {
    fn new(svc: &'a OxidantService, snap: SqlConfSnapshot) -> Self {
        Self {
            svc,
            snap: Some(snap),
        }
    }
}

impl Drop for SqlConfGuard<'_> {
    fn drop(&mut self) {
        if let Some(snap) = self.snap.take() {
            self.svc.restore_sql_conf(snap);
            self.svc.sync_observability_env();
        }
    }
}

/// Planning failure with the target table name threaded structurally (not re-parsed from status text).
#[derive(Debug)]
struct TablePlanningFailure {
    table: String,
    /// Raw planner error (for `PipelineEvent` text — avoid double-wrapping `format_table_failed`).
    inner: String,
    status: Status,
}

/// Drops temp views registered during a dry `StartRun` on every exit path.
struct DryRunTempViewGuard<'a> {
    engine: &'a Engine,
    registered: Vec<String>,
}

impl<'a> DryRunTempViewGuard<'a> {
    fn new(engine: &'a Engine) -> Self {
        Self {
            engine,
            registered: Vec::new(),
        }
    }

    async fn cleanup(&mut self) {
        for name in self.registered.drain(..) {
            let _ = self
                .engine
                .sql(&format!("DROP VIEW IF EXISTS {name}"))
                .await;
        }
    }
}

impl OxidantService {
    fn apply_sql_conf(
        &self,
        conf: &HashMap<String, String>,
        scope: SqlConfScope,
    ) -> (Vec<String>, SqlConfSnapshot) {
        let mut ignored = Vec::new();
        let mut previous = HashMap::new();
        let mut store = self.config.lock().expect("config poisoned");
        for (key, value) in conf {
            if is_known_sql_conf_key(key, scope) {
                previous
                    .entry(key.clone())
                    .or_insert_with(|| store.get(key).cloned());
                store.insert(key.clone(), value.clone());
            } else {
                ignored.push(key.clone());
            }
        }
        ignored.sort();
        (ignored, SqlConfSnapshot { previous })
    }

    fn restore_sql_conf(&self, snapshot: SqlConfSnapshot) {
        let mut store = self.config.lock().expect("config poisoned");
        for (key, old) in snapshot.previous {
            match old {
                Some(v) => {
                    store.insert(key, v);
                }
                None => {
                    store.remove(&key);
                }
            }
        }
    }

    fn sql_conf_ignored_events(
        &self,
        session_id: &str,
        operation_id: &str,
        ignored: &[String],
    ) -> Vec<sc::ExecutePlanResponse> {
        ignored
            .iter()
            .map(|key| {
                self.pipeline_event(
                    session_id,
                    operation_id,
                    &format!("[oxidant] ignored sql_conf key `{key}` (not supported)"),
                    None,
                )
            })
            .collect()
    }
}

impl OxidantService {
    pub(crate) async fn handle_pipeline_command(
        &self,
        engine: &Engine,
        session_id: &str,
        operation_id: &str,
        cmd: &sc::PipelineCommand,
    ) -> Result<PipelineCommandOutput, Status> {
        match cmd.command_type.as_ref() {
            Some(sc::pipeline_command::CommandType::CreateDataflowGraph(c)) => self
                .create_dataflow_graph(session_id, operation_id, c)
                .map(PipelineCommandOutput::Complete),
            Some(sc::pipeline_command::CommandType::DropDataflowGraph(c)) => self
                .drop_dataflow_graph(session_id, operation_id, c)
                .map(PipelineCommandOutput::Complete),
            Some(sc::pipeline_command::CommandType::DefineOutput(c)) => self
                .define_output(session_id, operation_id, c)
                .map(PipelineCommandOutput::Complete),
            Some(sc::pipeline_command::CommandType::DefineFlow(c)) => self
                .define_flow(session_id, operation_id, c)
                .map(PipelineCommandOutput::Complete),
            Some(sc::pipeline_command::CommandType::StartRun(c)) => {
                self.start_run(engine, session_id, operation_id, c).await
            }
            Some(sc::pipeline_command::CommandType::DefineSqlGraphElements(c)) => self
                .define_sql_graph_elements(session_id, operation_id, c)
                .map(PipelineCommandOutput::Complete),
            Some(sc::pipeline_command::CommandType::GetQueryFunctionExecutionSignalStream(c)) => {
                self.get_query_function_execution_signal_stream(session_id, operation_id, c)
                    .map(PipelineCommandOutput::Complete)
            }
            Some(sc::pipeline_command::CommandType::DefineFlowQueryFunctionResult(c)) => self
                .define_flow_query_function_result(session_id, operation_id, c)
                .map(PipelineCommandOutput::Complete),
            _ => Err(Status::unimplemented("unsupported PipelineCommand")),
        }
    }

    fn create_dataflow_graph(
        &self,
        session_id: &str,
        operation_id: &str,
        cmd: &sc::pipeline_command::CreateDataflowGraph,
    ) -> Result<Vec<sc::ExecutePlanResponse>, Status> {
        let graph_id = Uuid::new_v4().to_string();
        let graph = DataflowGraph {
            default_catalog: cmd.default_catalog.clone(),
            default_database: cmd.default_database.clone(),
            sql_conf: cmd.sql_conf.clone(),
            outputs: Vec::new(),
            flows: Vec::new(),
            refreshes: Vec::new(),
            created_at: SystemTime::now(),
        };
        self.dataflow_graphs
            .insert(session_id, graph_id.clone(), graph);
        Ok(vec![
            self.pipeline_result(
                session_id,
                operation_id,
                sc::PipelineCommandResult {
                    result_type: Some(
                        sc::pipeline_command_result::ResultType::CreateDataflowGraphResult(
                            sc::pipeline_command_result::CreateDataflowGraphResult {
                                dataflow_graph_id: Some(graph_id),
                            },
                        ),
                    ),
                },
            ),
            self.result_complete(session_id, operation_id),
        ])
    }

    fn drop_dataflow_graph(
        &self,
        session_id: &str,
        operation_id: &str,
        cmd: &sc::pipeline_command::DropDataflowGraph,
    ) -> Result<Vec<sc::ExecutePlanResponse>, Status> {
        let graph_id = cmd
            .dataflow_graph_id
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                Status::invalid_argument("DropDataflowGraph.dataflow_graph_id is required")
            })?;
        self.dataflow_graphs
            .with_graph(graph_id, session_id, |_| Ok(()))?;
        self.dataflow_graphs.remove(graph_id);
        Ok(vec![self.result_complete(session_id, operation_id)])
    }

    fn define_output(
        &self,
        session_id: &str,
        operation_id: &str,
        cmd: &sc::pipeline_command::DefineOutput,
    ) -> Result<Vec<sc::ExecutePlanResponse>, Status> {
        let graph_id = cmd
            .dataflow_graph_id
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                Status::invalid_argument("DefineOutput.dataflow_graph_id is required")
            })?;
        let output_name = cmd
            .output_name
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Status::invalid_argument("DefineOutput.output_name is required"))?;
        let output_type = cmd.output_type.unwrap_or(0);

        let (table_details, sink_details) = match &cmd.details {
            Some(sc::pipeline_command::define_output::Details::TableDetails(t)) => {
                (Some(t.clone()), None)
            }
            Some(sc::pipeline_command::define_output::Details::SinkDetails(s)) => {
                (None, Some(s.clone()))
            }
            _ => (None, None),
        };

        let resolved = self
            .dataflow_graphs
            .with_graph(graph_id, session_id, |graph| {
                if output_type == sc::OutputType::Sink as i32 {
                    return Err(Status::unimplemented(
                        "DefineOutput SINK is not supported yet (SDP Phase 4c)",
                    ));
                }
                let resolved = resolve_identifier(
                    output_name,
                    graph.default_catalog.as_deref(),
                    graph.default_database.as_deref(),
                );
                replace_output(
                    graph,
                    OutputDef {
                        output_name: output_name.to_string(),
                        resolved: resolved.clone(),
                        output_type,
                        comment: cmd.comment.clone(),
                        table_details,
                        sink_details,
                        source_code_location: cmd.source_code_location.clone(),
                    },
                );
                Ok(resolved)
            })?;

        Ok(vec![
            self.pipeline_result(
                session_id,
                operation_id,
                sc::PipelineCommandResult {
                    result_type: Some(sc::pipeline_command_result::ResultType::DefineOutputResult(
                        sc::pipeline_command_result::DefineOutputResult {
                            resolved_identifier: Some(resolved),
                        },
                    )),
                },
            ),
            self.result_complete(session_id, operation_id),
        ])
    }

    fn define_flow(
        &self,
        session_id: &str,
        operation_id: &str,
        cmd: &sc::pipeline_command::DefineFlow,
    ) -> Result<Vec<sc::ExecutePlanResponse>, Status> {
        let graph_id = cmd
            .dataflow_graph_id
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Status::invalid_argument("DefineFlow.dataflow_graph_id is required"))?;
        let flow_name = cmd
            .flow_name
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Status::invalid_argument("DefineFlow.flow_name is required"))?;
        let target_name = cmd
            .target_dataset_name
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                Status::invalid_argument("DefineFlow.target_dataset_name is required")
            })?;

        let engine = self.engine();
        let resolved = self
            .dataflow_graphs
            .with_graph(graph_id, session_id, |graph| {
                let (relation, query_sql, auto_cdc, once) = match &cmd.details {
                    Some(sc::pipeline_command::define_flow::Details::RelationFlowDetails(d)) => {
                        let rel = d.relation.as_ref().and_then(|r| {
                            if r.rel_type.is_some() {
                                Some(r.clone())
                            } else {
                                None
                            }
                        });
                        (rel, None, None, cmd.once.unwrap_or(false))
                    }
                    Some(sc::pipeline_command::define_flow::Details::AutoCdcFlowDetails(d)) => {
                        let cdc = auto_cdc_from_proto(engine.ctx(), d).map_err(|e| {
                            Status::invalid_argument(format!("DefineFlow AUTO CDC: {e}"))
                        })?;
                        validate_auto_cdc(&cdc, flow_name)
                            .map_err(|e| Status::invalid_argument(e.to_string()))?;
                        (None, None, Some(cdc), cmd.once.unwrap_or(false))
                    }
                    _ => {
                        return Err(Status::invalid_argument(
                            "DefineFlow requires relation_flow_details or auto_cdc_flow_details",
                        ));
                    }
                };
                let target = resolve_identifier(
                    target_name,
                    graph.default_catalog.as_deref(),
                    graph.default_database.as_deref(),
                );
                let resolved = resolve_identifier(
                    flow_name,
                    graph.default_catalog.as_deref(),
                    graph.default_database.as_deref(),
                );
                replace_flow(
                    graph,
                    FlowDef {
                        flow_name: flow_name.to_string(),
                        resolved: resolved.clone(),
                        target,
                        sql_conf: cmd.sql_conf.clone(),
                        client_id: cmd.client_id.clone(),
                        relation,
                        query_sql,
                        auto_cdc,
                        once,
                        by_name: false,
                        source_code_location: cmd.source_code_location.clone(),
                    },
                );
                Ok(resolved)
            })?;

        Ok(vec![
            self.pipeline_result(
                session_id,
                operation_id,
                sc::PipelineCommandResult {
                    result_type: Some(sc::pipeline_command_result::ResultType::DefineFlowResult(
                        sc::pipeline_command_result::DefineFlowResult {
                            resolved_identifier: Some(resolved),
                        },
                    )),
                },
            ),
            self.result_complete(session_id, operation_id),
        ])
    }

    fn get_query_function_execution_signal_stream(
        &self,
        session_id: &str,
        operation_id: &str,
        cmd: &sc::pipeline_command::GetQueryFunctionExecutionSignalStream,
    ) -> Result<Vec<sc::ExecutePlanResponse>, Status> {
        let graph_id = cmd
            .dataflow_graph_id
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                Status::invalid_argument(
                    "GetQueryFunctionExecutionSignalStream.dataflow_graph_id is required",
                )
            })?;
        let request_client_id = cmd.client_id.as_deref().filter(|s| !s.is_empty());

        let pending = self
            .dataflow_graphs
            .with_graph(graph_id, session_id, |graph| {
                Ok(pending_query_function_flows(graph, request_client_id))
            })?;

        // `execute_plan` buffers the full response list before streaming, so we emit all
        // currently-pending signals in one shot and complete — late DefineFlow arrivals need
        // another GetQueryFunctionExecutionSignalStream call.
        let mut responses = Vec::new();
        if !pending.is_empty() {
            responses.push(self.pipeline_query_function_signal(session_id, operation_id, pending));
        }
        responses.push(self.result_complete(session_id, operation_id));
        Ok(responses)
    }

    fn define_flow_query_function_result(
        &self,
        session_id: &str,
        operation_id: &str,
        cmd: &sc::pipeline_command::DefineFlowQueryFunctionResult,
    ) -> Result<Vec<sc::ExecutePlanResponse>, Status> {
        let graph_id = cmd
            .dataflow_graph_id
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                Status::invalid_argument(
                    "DefineFlowQueryFunctionResult.dataflow_graph_id is required",
                )
            })?;
        let label = requested_flow_label(cmd);
        if label.is_empty() {
            return Err(Status::invalid_argument(
                "DefineFlowQueryFunctionResult.flow_identifier is required (the deprecated \
                 `flow_name` is accepted too)",
            ));
        }
        let relation = cmd.relation.as_ref().ok_or_else(|| {
            Status::invalid_argument("DefineFlowQueryFunctionResult.relation is required")
        })?;
        if relation.rel_type.is_none() {
            return Err(Status::invalid_argument(
                "DefineFlowQueryFunctionResult.relation is empty",
            ));
        }

        self.dataflow_graphs.with_graph(
            graph_id,
            session_id,
            |graph| match find_backfill_target(graph, cmd) {
                BackfillTarget::Pending(idx) => {
                    graph.flows[idx].relation = Some(relation.clone());
                    Ok(())
                }
                BackfillTarget::AlreadyDefined(idx) => {
                    let defined_by = if graph.flows[idx].query_sql.is_some() {
                        "SDP SQL text"
                    } else {
                        "an earlier DefineFlowQueryFunctionResult"
                    };
                    Err(Status::failed_precondition(format!(
                        "DefineFlowQueryFunctionResult: flow `{label}` in dataflow graph \
                             `{graph_id}` is not awaiting a query-function result — it is \
                             already defined by {defined_by}. Backfill is accepted once, and \
                             only for a flow the signal stream reported as pending."
                    )))
                }
                BackfillTarget::NotFound => Err(Status::invalid_argument(format!(
                    "DefineFlowQueryFunctionResult: unknown flow `{label}` in dataflow \
                         graph `{graph_id}`"
                ))),
            },
        )?;

        Ok(vec![self.result_complete(session_id, operation_id)])
    }

    fn define_sql_graph_elements(
        &self,
        session_id: &str,
        operation_id: &str,
        cmd: &sc::pipeline_command::DefineSqlGraphElements,
    ) -> Result<Vec<sc::ExecutePlanResponse>, Status> {
        let graph_id = cmd
            .dataflow_graph_id
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                Status::invalid_argument("DefineSqlGraphElements.dataflow_graph_id is required")
            })?;
        let path = cmd.sql_file_path.as_deref();
        let text = cmd.sql_text.as_deref().ok_or_else(|| {
            Status::invalid_argument("DefineSqlGraphElements.sql_text is required")
        })?;
        let elements = parse(text, path).map_err(crate::err_to_status)?;
        self.dataflow_graphs
            .with_graph(graph_id, session_id, |graph| {
                merge_sql_elements(graph, elements);
                Ok(())
            })?;
        Ok(vec![self.result_complete(session_id, operation_id)])
    }

    async fn start_run(
        &self,
        engine: &Engine,
        session_id: &str,
        operation_id: &str,
        cmd: &sc::pipeline_command::StartRun,
    ) -> Result<PipelineCommandOutput, Status> {
        let graph_id = cmd
            .dataflow_graph_id
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Status::invalid_argument("StartRun.dataflow_graph_id is required"))?;
        let dry = cmd.dry.unwrap_or(false);

        let graph_refreshes = if dry {
            Vec::new()
        } else {
            self.dataflow_graphs
                .with_graph(graph_id, session_id, |graph| {
                    Ok(std::mem::take(&mut graph.refreshes))
                })?
        };

        let graph = self.dataflow_graphs.get_graph(graph_id, session_id)?;
        let locations = table_source_locations(&graph);

        let (ignored_graph, conf_snap) = self.apply_sql_conf(&graph.sql_conf, SqlConfScope::Graph);
        if !dry {
            self.sync_catalogs().await;
            self.sync_observability_env();
        }
        let _conf_guard = SqlConfGuard::new(self, conf_snap);
        let mut responses = self.sql_conf_ignored_events(session_id, operation_id, &ignored_graph);

        let storage = cmd
            .storage
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| {
                std::env::temp_dir()
                    .join(format!("oxidant-sdp-{graph_id}"))
                    .display()
                    .to_string()
            });

        let mut refresh_selection: Vec<String> = cmd
            .refresh_selection
            .iter()
            .filter(|s| !s.is_empty())
            .cloned()
            .collect();
        refresh_selection.extend(graph_refreshes.iter().cloned());

        let full_refresh = cmd.full_refresh_all.unwrap_or(false);
        let full_refresh_tables: Vec<String> = if full_refresh {
            Vec::new()
        } else {
            cmd.full_refresh_selection
                .iter()
                .filter(|s| !s.is_empty())
                .cloned()
                .collect()
        };

        if !dry {
            if full_refresh {
                clear_pipeline_state(&storage, &[]).map_err(crate::err_to_status)?;
            } else if !full_refresh_tables.is_empty() {
                clear_pipeline_state(&storage, &full_refresh_tables)
                    .map_err(crate::err_to_status)?;
            } else if !graph_refreshes.is_empty() {
                let tables: Vec<String> = graph_refreshes
                    .iter()
                    .map(|name| {
                        resolve_identifier(
                            name,
                            graph.default_catalog.as_deref(),
                            graph.default_database.as_deref(),
                        )
                        .table_name
                    })
                    .collect();
                clear_pipeline_state(&storage, &tables).map_err(crate::err_to_status)?;
            }
        }

        let mut dry_temp_views = if dry {
            Some(DryRunTempViewGuard::new(engine))
        } else {
            None
        };
        if let Err(status) = register_temp_views(
            engine,
            &graph,
            dry,
            dry_temp_views.as_mut().map(|g| &mut g.registered),
        )
        .await
        {
            if let Some(guard) = dry_temp_views.as_mut() {
                guard.cleanup().await;
            }
            responses.push(
                self.pipeline_table_failed_event(
                    session_id,
                    operation_id,
                    graph
                        .outputs
                        .iter()
                        .find(|o| is_temporary_view_output(o))
                        .map(|o| o.resolved.table_name.as_str())
                        .unwrap_or("temporary_view"),
                    status.message(),
                    &locations,
                ),
            );
            return Ok(PipelineCommandOutput::Failed {
                responses,
                status: status_with_location(&status, &locations, None),
            });
        }

        let mut flow_ignored = Vec::new();
        let config = match graph_to_config(
            self,
            &graph,
            graph_id,
            &storage,
            engine,
            &mut flow_ignored,
        )
        .await
        {
            Ok(config) => config,
            Err(failure) => {
                if let Some(guard) = dry_temp_views.as_mut() {
                    guard.cleanup().await;
                }
                responses.extend(self.sql_conf_ignored_events(
                    session_id,
                    operation_id,
                    &flow_ignored,
                ));
                responses.push(self.pipeline_table_failed_event(
                    session_id,
                    operation_id,
                    &failure.table,
                    &failure.inner,
                    &locations,
                ));
                return Ok(PipelineCommandOutput::Failed {
                    responses,
                    status: status_with_location(&failure.status, &locations, Some(&failure.table)),
                });
            }
        };
        responses.extend(self.sql_conf_ignored_events(session_id, operation_id, &flow_ignored));

        let plan = match Plan::build(&config) {
            Ok(plan) => plan,
            Err(e) => {
                if let Some(guard) = dry_temp_views.as_mut() {
                    guard.cleanup().await;
                }
                let status = crate::err_to_status(e);
                return Ok(PipelineCommandOutput::Failed {
                    responses,
                    status: status_with_location(&status, &locations, None),
                });
            }
        };

        if dry {
            if let Some(guard) = dry_temp_views.as_mut() {
                guard.cleanup().await;
            }
            let order = plan
                .graph
                .order
                .iter()
                .map(|n| n.name.as_str())
                .collect::<Vec<_>>()
                .join(" -> ");
            let message = format!(
                "pipeline `{}` is valid: {} table(s), update order: {order}",
                plan.pipeline.name,
                plan.graph.order.len()
            );
            responses.push(self.pipeline_event(session_id, operation_id, &message, None));
            responses.push(self.result_complete(session_id, operation_id));
            return Ok(PipelineCommandOutput::Complete(responses));
        }

        let wanted = resolve_wanted_tables(&graph, &refresh_selection);
        let once_tables = once_table_targets(&graph);
        let mut events = Vec::new();
        let run_result = run_pipeline(engine, &plan, &wanted, true, &once_tables, &mut |event| {
            events.push(event)
        })
        .await;

        let mut run_responses: Vec<sc::ExecutePlanResponse> = events
            .into_iter()
            .map(|event| self.pipeline_run_event(session_id, operation_id, &locations, event))
            .collect();
        responses.append(&mut run_responses);
        if let Err(e) = run_result {
            return Ok(PipelineCommandOutput::Failed {
                responses,
                status: status_with_location(&crate::err_to_status(e), &locations, None),
            });
        }
        responses.push(self.result_complete(session_id, operation_id));
        Ok(PipelineCommandOutput::Complete(responses))
    }

    fn pipeline_table_failed_event(
        &self,
        session_id: &str,
        operation_id: &str,
        table: &str,
        error: &str,
        locations: &HashMap<String, String>,
    ) -> sc::ExecutePlanResponse {
        self.pipeline_event(
            session_id,
            operation_id,
            &format_table_failed(table, error, source_location_for_table(locations, table)),
            None,
        )
    }

    fn pipeline_result(
        &self,
        session_id: &str,
        operation_id: &str,
        result: sc::PipelineCommandResult,
    ) -> sc::ExecutePlanResponse {
        self.response(
            session_id,
            operation_id,
            sc::execute_plan_response::ResponseType::PipelineCommandResult(result),
        )
    }

    fn pipeline_event(
        &self,
        session_id: &str,
        operation_id: &str,
        message: &str,
        at: Option<SystemTime>,
    ) -> sc::ExecutePlanResponse {
        let now = at.unwrap_or_else(SystemTime::now);
        let duration = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        self.response(
            session_id,
            operation_id,
            sc::execute_plan_response::ResponseType::PipelineEventResult(sc::PipelineEventResult {
                event: Some(sc::PipelineEvent {
                    timestamp: Some(Timestamp {
                        seconds: duration.as_secs() as i64,
                        nanos: duration.subsec_nanos() as i32,
                    }),
                    message: Some(message.to_string()),
                }),
            }),
        )
    }

    fn pipeline_run_event(
        &self,
        session_id: &str,
        operation_id: &str,
        locations: &HashMap<String, String>,
        event: RunEvent,
    ) -> sc::ExecutePlanResponse {
        self.pipeline_event(
            session_id,
            operation_id,
            &format_run_event(&event, locations),
            Some(event.at),
        )
    }

    fn pipeline_query_function_signal(
        &self,
        session_id: &str,
        operation_id: &str,
        flow_identifiers: Vec<sc::ResolvedIdentifier>,
    ) -> sc::ExecutePlanResponse {
        // `flow_names` is deprecated since Spark 4.2 but is still the only field a 4.0/4.1
        // client reads, so populate both with the same fully-qualified names.
        let flow_names = flow_identifiers
            .iter()
            .map(resolved_identifier_key)
            .collect();
        #[allow(deprecated)]
        let signal = sc::PipelineQueryFunctionExecutionSignal {
            flow_names,
            flow_identifiers,
        };
        self.response(
            session_id,
            operation_id,
            sc::execute_plan_response::ResponseType::PipelineQueryFunctionExecutionSignal(signal),
        )
    }
}

async fn graph_to_config(
    svc: &OxidantService,
    graph: &DataflowGraph,
    graph_id: &str,
    storage: &str,
    engine: &Engine,
    flow_ignored: &mut Vec<String>,
) -> Result<OxidantConfig, TablePlanningFailure> {
    let catalog = graph
        .default_catalog
        .clone()
        .unwrap_or_else(|| "spark_catalog".to_string());
    let schema = graph
        .default_database
        .clone()
        .unwrap_or_else(|| "default".to_string());

    let table_storage = std::path::Path::new(storage)
        .parent()
        .map(|p| format!("{}/{schema}", p.display()))
        .unwrap_or_else(|| format!("{storage}/{schema}"));

    let mut tables: HashMap<String, TableConfig> = HashMap::new();
    let mut pending_cdc: Vec<PendingCdc> = Vec::new();
    for output in &graph.outputs {
        if output.output_type == sc::OutputType::TemporaryView as i32 {
            continue;
        }
        let key = resolved_identifier_key(&output.resolved);
        let name = output.resolved.table_name.clone();
        let table_details = output.table_details.as_ref();
        let table_properties: BTreeMap<String, String> = table_details
            .map(|t| t.table_properties.clone().into_iter().collect())
            .unwrap_or_default();
        let format = table_details.and_then(|t| t.format.clone());
        if let Some(fmt) = format.as_deref() {
            validate_output_format(fmt, &format!("table `{name}`")).map_err(|e| {
                table_planning_failure(&name, crate::err_to_status(e), &output.source_code_location)
            })?;
        }
        let (source_opts, sink_opts) = split_table_properties(&table_properties).map_err(|e| {
            table_planning_failure(&name, crate::err_to_status(e), &output.source_code_location)
        })?;
        let source = kafka_source_from_properties(&source_opts);
        let iceberg_compat = sink_opts
            .get("icebergCompat")
            .map(|v| v.eq_ignore_ascii_case("true"));
        let output_schema = if let Some(details) = table_details {
            table_schema_from_details(details).map_err(|e| {
                table_planning_failure(&name, crate::err_to_status(e), &output.source_code_location)
            })?
        } else {
            None
        };
        let table = TableConfig {
            name: name.clone(),
            source,
            sql: None,
            sql_by_name: false,
            append_flows: Vec::new(),
            output_schema,
            partition_by: table_details
                .map(|t| t.partition_cols.clone())
                .unwrap_or_default(),
            format,
            iceberg_compat,
            iceberg_table_suffix: None,
            checkpoint_interval: None,
            dedup_columns: Vec::new(),
            auto_cdc: None,
            expect: Default::default(),
            comment: output.comment.clone(),
        };
        tables.insert(key, table);
    }

    for flow in &graph.flows {
        if flow_targets_temporary_view(graph, flow) {
            if flow.auto_cdc.is_some() {
                return Err(table_planning_failure(
                    &flow.target.table_name,
                    Status::invalid_argument(format!(
                        "AUTO CDC flow `{}` cannot target temporary view `{}` — AUTO CDC \
                         writes into a persisted table",
                        flow.flow_name, flow.target.table_name
                    )),
                    &flow.source_code_location,
                ));
            }
            continue;
        }
        let target_key = resolved_identifier_key(&flow.target);
        let target_name = flow.target.table_name.clone();
        let table = tables.get_mut(&target_key).ok_or_else(|| {
            table_planning_failure(
                &target_name,
                Status::invalid_argument(format!(
                    "flow `{}` targets undefined table `{target_name}`",
                    flow.flow_name
                )),
                &flow.source_code_location,
            )
        })?;
        let (ignored, flow_snap) = svc.apply_sql_conf(&flow.sql_conf, SqlConfScope::Flow);
        flow_ignored.extend(ignored);
        if let Some(cdc) = &flow.auto_cdc {
            // AUTO CDC flows are resolved after every append flow has been folded in, so the
            // source table's own transformation is already known no matter what order the
            // statements arrived in.
            pending_cdc.push(PendingCdc {
                target_key: target_key.clone(),
                target_name: target_name.clone(),
                flow_name: flow.flow_name.clone(),
                cdc: cdc.clone(),
                source_code_location: flow.source_code_location.clone(),
            });
            svc.restore_sql_conf(flow_snap);
            continue;
        }
        let sql_result: Result<(), TablePlanningFailure> = {
            let flow_sql = if let Some(relation) = &flow.relation {
                if table.source.is_none() {
                    if let Some(source) = extract_streaming_source(relation) {
                        table.source = Some(source);
                    }
                }
                sql_from_relation(engine, relation, graph)
                    .await
                    .map_err(|status| {
                        table_planning_failure(&target_name, status, &flow.source_code_location)
                    })?
            } else if let Some(sql) = flow.query_sql.as_deref() {
                if table.source.is_none() {
                    if let Some(output) = graph
                        .outputs
                        .iter()
                        .find(|o| identifiers_match(&o.resolved, &flow.target))
                    {
                        if let Some(source) = kafka_source_from_output(output) {
                            table.source = Some(source);
                        }
                    }
                }
                sql.to_string()
            } else {
                return Err(table_planning_failure(
                    &target_name,
                    Status::failed_precondition(format!(
                        "flow `{}` has no relation or SQL at StartRun",
                        flow.flow_name
                    )),
                    &flow.source_code_location,
                ));
            };
            if table.sql.is_none() {
                table.sql = Some(flow_sql);
                table.sql_by_name = flow.by_name;
            } else {
                table.append_flows.push(oxidant_config::AppendFlow {
                    sql: flow_sql,
                    by_name: flow.by_name,
                });
            }
            Ok(())
        };
        svc.restore_sql_conf(flow_snap);
        sql_result?;
    }

    resolve_auto_cdc_flows(graph, &mut tables, pending_cdc)?;

    let pipeline = PipelineConfig {
        name: graph_id.to_string(),
        catalog,
        schema,
        storage: Some(table_storage),
        checkpoints: storage.to_string(),
        trigger: Trigger::Once,
        format: "delta".to_string(),
        iceberg_compat: true,
    };

    Ok(OxidantConfig {
        pipeline: Some(pipeline),
        tables: tables.into_values().collect(),
        ..Default::default()
    })
}

/// An AUTO CDC flow held back until every append flow has been folded into its target.
#[derive(Clone)]
struct PendingCdc {
    target_key: String,
    target_name: String,
    flow_name: String,
    cdc: AutoCdcConfig,
    source_code_location: Option<sc::SourceCodeLocation>,
}

/// Fold AUTO CDC flows into their target tables.
///
/// The target itself declares no stream: it inherits the *source* table's streaming source and
/// per-batch transformation, and every micro-batch of that stream is merged into the target by
/// key instead of appended. That is why this runs after the append-flow loop — the source
/// table's `sql` is only known once its own flows have been folded in.
fn resolve_auto_cdc_flows(
    graph: &DataflowGraph,
    tables: &mut HashMap<String, TableConfig>,
    pending: Vec<PendingCdc>,
) -> Result<(), TablePlanningFailure> {
    for p in pending {
        let reject = |msg: String| {
            table_planning_failure(
                &p.target_name,
                Status::invalid_argument(msg),
                &p.source_code_location,
            )
        };
        let mut cdc = p.cdc.clone();
        validate_auto_cdc(&cdc, &p.target_name)
            .map_err(|e| reject(format!("AUTO CDC flow `{}`: {e}", p.flow_name)))?;

        let source_key = output_dedup_key(graph, &cdc.source);
        let source_table = tables.get(&source_key).ok_or_else(|| {
            reject(format!(
                "AUTO CDC flow `{}` reads `{}`, which is not a table declared in this pipeline",
                p.flow_name, cdc.source
            ))
        })?;
        let Some(source_config) = source_table.source.clone() else {
            return Err(reject(format!(
                "AUTO CDC flow `{}` reads `{}`, which is not a streaming table — an AUTO CDC \
                 source must declare a streaming source",
                p.flow_name, cdc.source
            )));
        };
        let source_sql = source_table.sql.clone();
        let source_sql_by_name = source_table.sql_by_name;
        let source_flows = source_table.append_flows.clone();
        // In Databricks an AUTO CDC flow reads the source *dataset* — i.e. what the source table
        // actually holds, after its expectations and deduplication. Since the target re-ingests
        // the raw stream rather than reading the table, those have to come along: a bronze table
        // with `ON VIOLATION DROP ROW` that dropped rows from itself while the CDC target merged
        // them would leave the two tables disagreeing about the same events.
        let source_expect = source_table.expect.clone();
        let source_dedup = source_table.dedup_columns.clone();
        // Normalize to the declared table name so the dependency edge resolves in `Graph::build`.
        cdc.source = source_table.name.clone();

        let table = tables.get_mut(&p.target_key).ok_or_else(|| {
            reject(format!(
                "AUTO CDC flow `{}` targets undefined table `{}`",
                p.flow_name, p.target_name
            ))
        })?;
        if table.auto_cdc.is_some() {
            return Err(reject(format!(
                "table `{}` is the target of more than one AUTO CDC flow",
                p.target_name
            )));
        }
        if table.source.is_some() {
            return Err(reject(format!(
                "AUTO CDC flow `{}` cannot target streaming table `{}` — the target already \
                 ingests its own stream; declare it as a plain `CREATE STREAMING TABLE {}` with \
                 no query or TBLPROPERTIES",
                p.flow_name, p.target_name, p.target_name
            )));
        }
        if table.sql.is_some() || !table.append_flows.is_empty() {
            return Err(reject(format!(
                "AUTO CDC flow `{}` cannot target `{}` — the target already has a query or \
                 append flow; an AUTO CDC target is defined only by its CDC flow",
                p.flow_name, p.target_name
            )));
        }
        if !table.expect.is_empty() {
            return Err(reject(format!(
                "AUTO CDC flow `{}` targets `{}`, which declares its own expectations — an AUTO \
                 CDC target inherits the source table's, so declare them on `{}` instead",
                p.flow_name, p.target_name, cdc.source
            )));
        }
        table.source = Some(source_config);
        table.sql = source_sql;
        table.sql_by_name = source_sql_by_name;
        table.append_flows = source_flows;
        table.expect = source_expect;
        table.dedup_columns = source_dedup;
        table.auto_cdc = Some(cdc);
    }
    Ok(())
}

fn merge_sql_elements(graph: &mut DataflowGraph, elements: oxidant_pipelines::SqlGraphElements) {
    for output in elements.outputs {
        let refresh = output.or_refresh;
        let def = parsed_output_to_def(&output, graph);
        replace_output(graph, def);
        if refresh {
            graph.refreshes.push(output.name);
        }
    }
    for flow in elements.flows {
        let def = parsed_flow_to_def(&flow, graph);
        replace_flow(graph, def);
    }
    graph.refreshes.extend(elements.refreshes);
}

fn replace_output(graph: &mut DataflowGraph, output: OutputDef) {
    let key = output_dedup_key(graph, &output.output_name);
    if let Some(idx) = graph
        .outputs
        .iter()
        .position(|o| output_dedup_key(graph, &o.output_name) == key)
    {
        graph.outputs[idx] = output;
    } else {
        graph.outputs.push(output);
    }
}

fn replace_flow(graph: &mut DataflowGraph, flow: FlowDef) {
    let key = flow.flow_name.clone();
    if let Some(idx) = graph.flows.iter().position(|f| f.flow_name == key) {
        graph.flows[idx] = flow;
    } else {
        graph.flows.push(flow);
    }
}

fn parsed_output_to_def(
    parsed: &oxidant_pipelines::ParsedOutput,
    graph: &DataflowGraph,
) -> OutputDef {
    let output_type = match parsed.kind {
        OutputKind::Table => sc::OutputType::Table as i32,
        OutputKind::MaterializedView => sc::OutputType::MaterializedView as i32,
        OutputKind::TemporaryView => sc::OutputType::TemporaryView as i32,
    };
    let resolved = resolve_identifier(
        &parsed.name,
        graph.default_catalog.as_deref(),
        graph.default_database.as_deref(),
    );
    OutputDef {
        output_name: parsed.name.clone(),
        resolved,
        output_type,
        comment: parsed.comment.clone(),
        table_details: Some(sc::pipeline_command::define_output::TableDetails {
            table_properties: parsed.table_properties.clone().into_iter().collect(),
            partition_cols: parsed.partition_cols.clone(),
            format: parsed.format.clone(),
            schema: parsed.schema.as_ref().map(|ddl| {
                sc::pipeline_command::define_output::table_details::Schema::SchemaString(
                    ddl.clone(),
                )
            }),
            clustering_columns: vec![],
        }),
        sink_details: None,
        source_code_location: None,
    }
}

fn parsed_flow_to_def(parsed: &oxidant_pipelines::ParsedFlow, graph: &DataflowGraph) -> FlowDef {
    let flow_name = parsed
        .name
        .clone()
        .unwrap_or_else(|| format!("flow_to_{}", parsed.target));
    let resolved = resolve_identifier(
        &flow_name,
        graph.default_catalog.as_deref(),
        graph.default_database.as_deref(),
    );
    let target = resolve_identifier(
        &parsed.target,
        graph.default_catalog.as_deref(),
        graph.default_database.as_deref(),
    );
    FlowDef {
        flow_name,
        resolved,
        target,
        sql_conf: HashMap::new(),
        client_id: None,
        relation: None,
        query_sql: if parsed.auto_cdc.is_some() {
            None
        } else {
            Some(parsed.query_sql.clone())
        },
        auto_cdc: parsed.auto_cdc.as_ref().map(parsed_auto_cdc_to_config),
        once: parsed.once,
        by_name: parsed.by_name,
        source_code_location: None,
    }
}

fn kafka_source_from_output(output: &OutputDef) -> Option<SourceConfig> {
    let props: BTreeMap<String, String> = output
        .table_details
        .as_ref()
        .map(|t| t.table_properties.clone().into_iter().collect())
        .filter(|p: &BTreeMap<String, String>| !p.is_empty())?;
    kafka_source_from_properties(&props)
}

fn kafka_source_from_properties(props: &BTreeMap<String, String>) -> Option<SourceConfig> {
    if props.contains_key("subscribe") || props.contains_key("oxidant.spool.dir") {
        Some(SourceConfig {
            format: "kafka".to_string(),
            options: props.clone().into_iter().collect(),
        })
    } else {
        None
    }
}

fn table_schema_from_details(
    details: &sc::pipeline_command::define_output::TableDetails,
) -> Result<Option<String>, oxidant_common::Error> {
    match &details.schema {
        Some(sc::pipeline_command::define_output::table_details::Schema::SchemaString(s))
            if !s.trim().is_empty() =>
        {
            Ok(Some(s.clone()))
        }
        Some(sc::pipeline_command::define_output::table_details::Schema::SchemaDataType(dt)) => {
            Ok(Some(proto_schema_to_ddl(dt)?))
        }
        _ => Ok(None),
    }
}

fn proto_schema_to_ddl(dt: &sc::DataType) -> Result<String, oxidant_common::Error> {
    use oxidant_common::Error;
    use sc::data_type::Kind;
    let kind = dt.kind.as_ref().ok_or_else(|| {
        Error::Plan("invalid schema_data_type proto for output schema: empty DataType".into())
    })?;
    match kind {
        Kind::Struct(s) => {
            let cols = s
                .fields
                .iter()
                .map(|f| {
                    let ty = f.data_type.as_ref().ok_or_else(|| {
                        Error::Plan(format!(
                            "invalid schema_data_type proto for output schema: field `{}` is missing a type",
                            f.name
                        ))
                    })?;
                    Ok(format!("{} {}", f.name, proto_spark_type_name(ty)?))
                })
                .collect::<Result<Vec<_>, Error>>()?
                .join(", ");
            Ok(format!("({cols})"))
        }
        other => Err(Error::Plan(format!(
            "output schema must be a struct of columns, got {other:?}"
        ))),
    }
}

fn proto_spark_type_name(dt: &sc::DataType) -> Result<String, oxidant_common::Error> {
    use oxidant_common::Error;
    use sc::data_type::Kind;
    let kind = dt.kind.as_ref().ok_or_else(|| {
        Error::Plan("invalid schema_data_type proto for output schema: empty field type".into())
    })?;
    Ok(match kind {
        Kind::Boolean(_) => "BOOLEAN".to_string(),
        Kind::Byte(_) => "TINYINT".to_string(),
        Kind::Short(_) => "SMALLINT".to_string(),
        Kind::Integer(_) => "INT".to_string(),
        Kind::Long(_) => "BIGINT".to_string(),
        Kind::Float(_) => "FLOAT".to_string(),
        Kind::Double(_) => "DOUBLE".to_string(),
        Kind::String(_) => "STRING".to_string(),
        Kind::Binary(_) => "BINARY".to_string(),
        Kind::Date(_) => "DATE".to_string(),
        Kind::Timestamp(_) | Kind::TimestampNtz(_) => "TIMESTAMP".to_string(),
        Kind::Decimal(d) => format!(
            "DECIMAL({},{})",
            d.precision.unwrap_or(38),
            d.scale.unwrap_or(0)
        ),
        Kind::Array(a) => {
            let inner = a.element_type.as_ref().ok_or_else(|| {
                Error::Plan(
                    "invalid schema_data_type proto for output schema: array element_type is missing"
                        .into(),
                )
            })?;
            format!("ARRAY<{}>", proto_spark_type_name(inner)?)
        }
        Kind::Struct(s) => {
            let inner = s
                .fields
                .iter()
                .map(|f| {
                    let ty = f.data_type.as_ref().ok_or_else(|| {
                        Error::Plan(format!(
                            "invalid schema_data_type proto for output schema: struct field `{}` is missing a type",
                            f.name
                        ))
                    })?;
                    Ok(format!("{}:{}", f.name, proto_spark_type_name(ty)?))
                })
                .collect::<Result<Vec<_>, Error>>()?
                .join(",");
            format!("STRUCT<{inner}>")
        }
        Kind::Map(m) => {
            let key = m.key_type.as_ref().ok_or_else(|| {
                Error::Plan(
                    "invalid schema_data_type proto for output schema: map key_type is missing"
                        .into(),
                )
            })?;
            let value = m.value_type.as_ref().ok_or_else(|| {
                Error::Plan(
                    "invalid schema_data_type proto for output schema: map value_type is missing"
                        .into(),
                )
            })?;
            format!(
                "MAP<{},{}>",
                proto_spark_type_name(key)?,
                proto_spark_type_name(value)?
            )
        }
        other => {
            return Err(Error::Plan(format!(
                "invalid schema_data_type proto for output schema: unsupported type {other:?}"
            )));
        }
    })
}

fn resolve_wanted_tables(graph: &DataflowGraph, refresh_selection: &[String]) -> Vec<String> {
    if refresh_selection.is_empty() {
        return Vec::new();
    }
    refresh_selection
        .iter()
        .map(|name| {
            resolve_identifier(
                name,
                graph.default_catalog.as_deref(),
                graph.default_database.as_deref(),
            )
            .table_name
        })
        .collect()
}

fn once_table_targets(graph: &DataflowGraph) -> HashSet<String> {
    graph
        .flows
        .iter()
        .filter(|f| f.once)
        .map(|f| f.target.table_name.clone())
        .collect()
}

async fn register_temp_views(
    engine: &Engine,
    graph: &DataflowGraph,
    dry: bool,
    mut registered: Option<&mut Vec<String>>,
) -> Result<(), Status> {
    for output in &graph.outputs {
        if output.output_type != sc::OutputType::TemporaryView as i32 {
            continue;
        }
        let name = output.resolved.table_name.clone();
        let sql = flow_sql_for_target(graph, &output.resolved).await?;
        if dry {
            if temp_view_exists(engine, &name).await? {
                return Err(Status::failed_precondition(format!(
                    "dry run cannot register temporary view `{name}`: a session view with the \
                     same name already exists"
                )));
            }
            match engine
                .sql(&format!("CREATE TEMPORARY VIEW {name} AS {sql}"))
                .await
            {
                Ok(_) => {
                    if let Some(registered) = registered.as_deref_mut() {
                        registered.push(name);
                    }
                }
                Err(e) if view_already_exists_error(&e) => {
                    return Err(Status::failed_precondition(format!(
                        "dry run cannot register temporary view `{name}`: a session view with the \
                         same name already exists"
                    )));
                }
                Err(e) => return Err(crate::err_to_status(e)),
            }
        } else {
            engine
                .sql(&format!("CREATE OR REPLACE TEMPORARY VIEW {name} AS {sql}"))
                .await
                .map_err(crate::err_to_status)?;
        }
    }
    Ok(())
}

async fn temp_view_exists(engine: &Engine, name: &str) -> Result<bool, Status> {
    Ok(engine.sql(&format!("DESCRIBE {name}")).await.is_ok())
}

fn view_already_exists_error(e: &oxidant_common::Error) -> bool {
    let msg = e.to_string().to_ascii_lowercase();
    msg.contains("already exists")
}

async fn flow_sql_for_target(
    graph: &DataflowGraph,
    target: &sc::ResolvedIdentifier,
) -> Result<String, Status> {
    let flow = graph
        .flows
        .iter()
        .find(|f| identifiers_match(&f.target, target))
        .ok_or_else(|| {
            Status::invalid_argument(format!(
                "temporary view `{}` has no defining flow",
                target.table_name
            ))
        })?;
    if let Some(sql) = flow.query_sql.as_deref() {
        return Ok(sql.to_string());
    }
    let relation = flow.relation.as_ref().ok_or_else(|| {
        Status::failed_precondition(format!("flow `{}` has no SQL at StartRun", flow.flow_name))
    })?;
    let _ = relation;
    Err(Status::failed_precondition(format!(
        "flow `{}` uses a Connect relation; temporary views from relations are not supported \
         yet",
        flow.flow_name
    )))
}

fn format_table_failed(name: &str, error: &str, location: Option<&String>) -> String {
    let at = location.map(|loc| format!(" ({loc})")).unwrap_or_default();
    format!("[oxidant] {name:<24} FAILED{at}: {error}")
}

fn table_planning_failure(
    table: &str,
    status: Status,
    location: &Option<sc::SourceCodeLocation>,
) -> TablePlanningFailure {
    let inner = status.message().to_string();
    let at = source_location_suffix(location);
    TablePlanningFailure {
        table: table.to_string(),
        inner: inner.clone(),
        status: Status::new(
            status.code(),
            format!("[oxidant] {table:<24} FAILED{at}: {inner}"),
        ),
    }
}

fn source_location_suffix(loc: &Option<sc::SourceCodeLocation>) -> String {
    source_location_label(loc)
        .map(|loc| format!(" (at {loc})"))
        .unwrap_or_default()
}

fn status_with_location(
    status: &Status,
    locations: &HashMap<String, String>,
    table: Option<&str>,
) -> Status {
    let msg = status.message();
    if locations.values().any(|loc| msg.contains(loc)) {
        return status.clone();
    }
    if let Some(table) = table {
        if let Some(loc) = source_location_for_table(locations, table) {
            return Status::new(status.code(), format!("{msg} (at {loc})"));
        }
    }
    status.clone()
}

fn format_run_event(event: &RunEvent, locations: &HashMap<String, String>) -> String {
    match &event.kind {
        RunEventKind::PipelineStarted {
            name,
            table_count,
            order,
        } => format!("[oxidant] pipeline `{name}`: {table_count} table(s), order: {order}"),
        RunEventKind::TableStarted { name } => format!("[oxidant] {name}: starting"),
        RunEventKind::TableUpdated {
            name,
            rows,
            elapsed,
        } => format!(
            "[oxidant] {name:<24} {rows} row(s) in {:.2}s",
            elapsed.as_secs_f64()
        ),
        RunEventKind::TableUnchanged { name } => {
            format!("[oxidant] {name:<24} unchanged (nothing it reads moved this pass)")
        }
        RunEventKind::TableSkipped { name } => {
            format!("[oxidant] {name:<24} skipped (an upstream table failed this pass)")
        }
        RunEventKind::OnceFlowSkipped { name } => {
            format!("[oxidant] {name:<24} skipped (once flow already completed)")
        }
        RunEventKind::ExpectationViolation {
            table,
            label,
            failed_records,
        } => format!("table={table} expectation={label} failed_records={failed_records}"),
        RunEventKind::BareNameWarning { table, error, .. } => {
            format!("[oxidant] {table:<24} warning: could not alias bare name ({error})")
        }
        RunEventKind::TableFailed { name, error, .. } => {
            format_table_failed(name, error, source_location_for_table(locations, name))
        }
        RunEventKind::StatePersistFailed { error } => {
            format!("[oxidant] could not persist pipeline state: {error}")
        }
        RunEventKind::PassComplete { outcomes } => {
            let failed = outcomes.iter().filter(|o| o.error.is_some()).count();
            format!(
                "[oxidant] pass complete: {} table(s), {failed} failed",
                outcomes.len()
            )
        }
    }
}

/// Flow relations are planned through DataFusion and unparsed, so a bad flow fails at `StartRun`
/// with its source location attached. A plain `SQL` relation that reads another table this graph
/// builds cannot be planned yet — that table only exists once the run creates it — so its text is
/// forwarded verbatim, exactly like an SDP-SQL flow's `query_sql`. This is what lets a Python
/// query function backfilled by `DefineFlowQueryFunctionResult` read an upstream pipeline table.
async fn sql_from_relation(
    engine: &Engine,
    relation: &sc::Relation,
    graph: &DataflowGraph,
) -> Result<String, Status> {
    if let Some(sc::relation::RelType::Sql(sql)) = relation.rel_type.as_ref() {
        if !sql.query.is_empty() {
            if let Some(external) = deferred_sql_references(&sql.query, graph) {
                reject_unknown_references(engine, &external).await?;
                return Ok(sql.query.clone());
            }
        }
    }
    relation_to_sql(engine, relation).await
}

/// Whether `output` is the table `full` (fully qualified) / `short` (leaf name) refers to.
///
/// Leaf-name matching is deliberately loose — `other_catalog.other_db.bronze` matches a graph
/// output named `bronze` — because that is exactly how `oxidant_pipelines::Graph::build` derives
/// its dependency edges, and the two must agree about what a flow depends on.
fn reference_matches_output(full: &str, short: &str, output: &OutputDef) -> bool {
    output.resolved.table_name == short || resolved_identifier_key(&output.resolved) == full
}

/// Classify the tables `sql` reads.
///
/// `None` means nothing it reads is built by this graph, so it can be planned normally.
/// `Some(external)` means it must be deferred, and carries the references this graph does *not*
/// build.
///
/// Note the asymmetry this creates: deferring is all-or-nothing per query. A single graph-built
/// reference sends the whole statement to the runner unplanned, so the rest of its references
/// never reach the analyzer — that is why the caller catalog-checks `external` separately.
/// Graph-built references genuinely cannot be checked here: those tables only exist once the run
/// creates them.
///
/// Temporary views are not "built" for this purpose: they are registered on the session before
/// planning, so they resolve normally — and, for the same reason, are not reported as external.
fn deferred_sql_references(sql: &str, graph: &DataflowGraph) -> Option<Vec<String>> {
    use datafusion::sql::parser::DFParser;
    use datafusion::sql::resolve::resolve_table_references;

    // Unparseable text falls through to the planner, which reports the real error.
    let statements = DFParser::parse_sql(sql).ok()?;
    let mut reads_graph_output = false;
    let mut external = Vec::new();
    for statement in &statements {
        let (references, ctes) = resolve_table_references(statement, true).ok()?;
        for reference in &references {
            let full = reference.to_string();
            let short = full.rsplit('.').next().unwrap_or(full.as_str());
            if graph
                .outputs
                .iter()
                .filter(|output| !is_temporary_view_output(output))
                .any(|output| reference_matches_output(&full, short, output))
            {
                reads_graph_output = true;
            } else if !graph
                .outputs
                .iter()
                .any(|output| reference_matches_output(&full, short, output))
                // A name can be both a CTE and a table reference in one statement
                // (`WITH t AS (SELECT * FROM t) …`); never report those as external.
                && !ctes.iter().any(|cte| cte.to_string() == full)
            {
                external.push(full);
            }
        }
    }
    reads_graph_output.then_some(external)
}

/// Reject a deferred query's references to tables that neither this graph builds nor the catalog
/// already holds. Without this, a typo sitting next to a legitimate pipeline table
/// (`FROM orders_bronze JOIN typo_tabel`) rides through unplanned and fails from deep inside the
/// runner, losing the `SourceCodeLocation` that `table_planning_failure` attaches here.
async fn reject_unknown_references(engine: &Engine, references: &[String]) -> Result<(), Status> {
    for reference in references {
        if engine
            .ctx()
            .table_provider(reference.as_str())
            .await
            .is_err()
        {
            return Err(Status::invalid_argument(format!(
                "table `{reference}` not found: this flow's SQL reads a table the pipeline \
                 builds, so it is forwarded to the run unplanned — every other table it names \
                 must already exist"
            )));
        }
    }
    Ok(())
}

async fn relation_to_sql(engine: &Engine, relation: &sc::Relation) -> Result<String, Status> {
    let plan = translate::to_plan(engine.ctx(), relation)
        .await
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
    Unparser::default()
        .plan_to_sql(&plan)
        .map(|stmt| stmt.to_string())
        .map_err(|e| Status::invalid_argument(format!("relation to sql: {e}")))
}

fn auto_cdc_from_proto(
    ctx: &datafusion::prelude::SessionContext,
    details: &sc::pipeline_command::define_flow::AutoCdcFlowDetails,
) -> Result<AutoCdcConfig, String> {
    let source = details
        .source
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "source is required".to_string())?
        .to_string();
    if details.keys.is_empty() {
        return Err("keys must not be empty".into());
    }
    let keys = details
        .keys
        .iter()
        .map(|e| expression_to_sql(ctx, e))
        .collect::<Result<Vec<_>, _>>()?;
    let sequence_by = details
        .sequence_by
        .as_ref()
        .map(|e| expression_to_sql(ctx, e))
        .transpose()?
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "sequence_by is required".to_string())?;
    match details.stored_as_scd_type {
        0 | 1 => {}
        other => {
            return Err(format!(
                "stored_as_scd_type {other} is not supported (only SCD_TYPE_1)"
            ));
        }
    }
    Ok(AutoCdcConfig {
        source,
        keys,
        sequence_by,
        apply_as_deletes: optional_expression_to_sql(ctx, details.apply_as_deletes.as_ref())?,
        apply_as_truncates: optional_expression_to_sql(ctx, details.apply_as_truncates.as_ref())?,
        column_list: expression_list_to_sql(ctx, &details.column_list)?,
        except_column_list: expression_list_to_sql(ctx, &details.except_column_list)?,
        ignore_null_updates_columns: expression_list_to_sql(
            ctx,
            &details.ignore_null_updates_column_list,
        )?,
        ignore_null_updates_except: expression_list_to_sql(
            ctx,
            &details.ignore_null_updates_except_column_list,
        )?,
    })
}

fn parsed_auto_cdc_to_config(parsed: &oxidant_pipelines::ParsedAutoCdcFlow) -> AutoCdcConfig {
    AutoCdcConfig {
        source: parsed.source.clone(),
        keys: parsed.keys.clone(),
        sequence_by: parsed.sequence_by.clone(),
        apply_as_deletes: parsed.apply_as_deletes.clone(),
        apply_as_truncates: parsed.apply_as_truncates.clone(),
        column_list: parsed.column_list.clone(),
        except_column_list: parsed.except_column_list.clone(),
        ignore_null_updates_columns: parsed.ignore_null_updates_columns.clone(),
        ignore_null_updates_except: parsed.ignore_null_updates_except.clone(),
    }
}

fn optional_expression_to_sql(
    ctx: &datafusion::prelude::SessionContext,
    expr: Option<&sc::Expression>,
) -> Result<Option<String>, String> {
    expr.map(|e| expression_to_sql(ctx, e)).transpose()
}

fn expression_list_to_sql(
    ctx: &datafusion::prelude::SessionContext,
    exprs: &[sc::Expression],
) -> Result<Option<Vec<String>>, String> {
    if exprs.is_empty() {
        return Ok(None);
    }
    exprs
        .iter()
        .map(|e| expression_to_sql(ctx, e))
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

/// Render one AUTO CDC option expression as SQL text.
///
/// Column references and `expr("…")` strings are taken verbatim — round-tripping them through
/// the planner would quote them in a dialect the merge statement is not parsed with. Anything
/// else (`col("op") == "D"`, say) is lowered to a DataFusion expression and unparsed.
fn expression_to_sql(
    ctx: &datafusion::prelude::SessionContext,
    expr: &sc::Expression,
) -> Result<String, String> {
    use sc::expression::ExprType;
    match expr.expr_type.as_ref() {
        Some(ExprType::UnresolvedAttribute(a)) => Ok(a.unparsed_identifier.clone()),
        Some(ExprType::ExpressionString(s)) => Ok(s.expression.clone()),
        Some(ExprType::Literal(l)) => literal_to_sql(l),
        Some(_) => {
            let lowered = crate::translate::expr::to_expr(ctx, expr, None)
                .map_err(|e| format!("unsupported AUTO CDC expression: {}", e.message()))?;
            Unparser::default()
                .expr_to_sql(&lowered)
                .map(|e| e.to_string())
                .map_err(|e| format!("AUTO CDC expression to sql: {e}"))
        }
        None => Err("empty AUTO CDC expression".into()),
    }
}

fn literal_to_sql(l: &sc::expression::Literal) -> Result<String, String> {
    use sc::expression::literal::LiteralType;
    match l.literal_type.as_ref() {
        Some(LiteralType::String(s)) => Ok(format!("'{}'", s.replace('\'', "''"))),
        Some(LiteralType::Integer(i)) => Ok(i.to_string()),
        Some(LiteralType::Long(i)) => Ok(i.to_string()),
        Some(LiteralType::Double(d)) => Ok(d.to_string()),
        Some(LiteralType::Boolean(b)) => Ok(if *b { "true" } else { "false" }.to_string()),
        other => Err(format!("unsupported AUTO CDC literal: {other:?}")),
    }
}

fn extract_streaming_source(relation: &sc::Relation) -> Option<SourceConfig> {
    match relation.rel_type.as_ref()? {
        sc::relation::RelType::Read(r) if r.is_streaming => match r.read_type.as_ref()? {
            sc::read::ReadType::DataSource(ds) => Some(SourceConfig {
                format: ds
                    .format
                    .clone()
                    .filter(|f| !f.is_empty())
                    .unwrap_or_else(|| "kafka".to_string()),
                options: ds.options.clone().into_iter().collect(),
            }),
            _ => None,
        },
        sc::relation::RelType::Project(p) => extract_streaming_source(p.input.as_deref()?),
        sc::relation::RelType::Filter(f) => extract_streaming_source(f.input.as_deref()?),
        sc::relation::RelType::SubqueryAlias(s) => extract_streaming_source(s.input.as_deref()?),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_single_part_uses_defaults() {
        let id = resolve_identifier("orders", Some("prod"), Some("live"));
        assert_eq!(id.catalog_name, "prod");
        assert_eq!(id.namespace, vec!["live"]);
        assert_eq!(id.table_name, "orders");
    }

    #[test]
    fn resolve_two_part_uses_default_catalog() {
        let id = resolve_identifier("silver.orders", Some("prod"), Some("live"));
        assert_eq!(id.catalog_name, "prod");
        assert_eq!(id.namespace, vec!["silver"]);
        assert_eq!(id.table_name, "orders");
    }

    #[test]
    fn resolve_three_part_is_fully_qualified() {
        let id = resolve_identifier("prod.silver.orders", None, None);
        assert_eq!(id.catalog_name, "prod");
        assert_eq!(id.namespace, vec!["silver"]);
        assert_eq!(id.table_name, "orders");
    }

    #[test]
    fn resolve_four_part_multilevel_namespace() {
        let id = resolve_identifier("prod.a.b.orders", None, None);
        assert_eq!(id.catalog_name, "prod");
        assert_eq!(id.namespace, vec!["a", "b"]);
        assert_eq!(id.table_name, "orders");
    }

    #[test]
    fn resolved_identifier_key_is_fully_qualified() {
        let id = resolve_identifier("prod.silver.orders", None, None);
        assert_eq!(
            resolved_identifier_key(&id),
            "prod.silver.orders".to_string()
        );
    }

    fn cdc_graph() -> DataflowGraph {
        DataflowGraph {
            default_catalog: Some("local".into()),
            default_database: Some("live".into()),
            sql_conf: HashMap::new(),
            outputs: Vec::new(),
            flows: Vec::new(),
            refreshes: Vec::new(),
            created_at: SystemTime::now(),
        }
    }

    fn cdc_table(name: &str) -> TableConfig {
        TableConfig {
            name: name.to_string(),
            source: None,
            sql: None,
            sql_by_name: false,
            append_flows: Vec::new(),
            output_schema: None,
            partition_by: Vec::new(),
            format: None,
            iceberg_compat: None,
            iceberg_table_suffix: None,
            checkpoint_interval: None,
            dedup_columns: Vec::new(),
            auto_cdc: None,
            expect: Default::default(),
            comment: None,
        }
    }

    fn cdc_options() -> AutoCdcConfig {
        AutoCdcConfig {
            source: "bronze".into(),
            keys: vec!["id".into()],
            sequence_by: "seq".into(),
            apply_as_deletes: None,
            apply_as_truncates: None,
            column_list: None,
            except_column_list: None,
            ignore_null_updates_columns: None,
            ignore_null_updates_except: None,
        }
    }

    fn pending(cdc: AutoCdcConfig) -> Vec<PendingCdc> {
        vec![PendingCdc {
            target_key: "local.live.silver".into(),
            target_name: "silver".into(),
            flow_name: "cdc_flow".into(),
            cdc,
            source_code_location: None,
        }]
    }

    /// A streaming `bronze` and an empty `silver`, the shape an AUTO CDC flow expects.
    fn cdc_tables() -> HashMap<String, TableConfig> {
        let mut bronze = cdc_table("bronze");
        bronze.source = Some(SourceConfig {
            format: "kafka".into(),
            options: BTreeMap::from([("subscribe".to_string(), "cdc".to_string())]),
        });
        bronze.sql = Some("SELECT * FROM stream".into());
        HashMap::from([
            ("local.live.bronze".to_string(), bronze),
            ("local.live.silver".to_string(), cdc_table("silver")),
        ])
    }

    #[test]
    fn auto_cdc_target_inherits_the_source_stream() {
        let mut tables = cdc_tables();
        resolve_auto_cdc_flows(&cdc_graph(), &mut tables, pending(cdc_options()))
            .expect("resolves");
        let target = &tables["local.live.silver"];
        assert_eq!(
            target.source.as_ref().map(|s| s.format.as_str()),
            Some("kafka")
        );
        assert_eq!(target.sql.as_deref(), Some("SELECT * FROM stream"));
        assert_eq!(target.kind(), oxidant_config::TableKind::AutoCdc);
        // Normalized to the declared table name so the graph edge resolves.
        assert_eq!(target.auto_cdc.as_ref().unwrap().source, "bronze");
    }

    #[test]
    fn auto_cdc_target_inherits_the_source_expectations_and_dedup() {
        // Databricks' AUTO CDC reads the source *dataset*, i.e. post-expectation. Since the
        // target re-ingests the raw stream instead of reading the table, the source's quality
        // rules have to be copied — otherwise a bronze `ON VIOLATION DROP ROW` drops rows from
        // bronze while the CDC target happily merges them.
        let mut tables = cdc_tables();
        {
            let bronze = tables.get_mut("local.live.bronze").unwrap();
            bronze.expect.insert(
                "positive_id".into(),
                oxidant_config::Expectation {
                    check: "id > 0".into(),
                    action: oxidant_config::ExpectAction::Drop,
                },
            );
            bronze.dedup_columns = vec!["id".into()];
        }
        resolve_auto_cdc_flows(&cdc_graph(), &mut tables, pending(cdc_options()))
            .expect("resolves");
        let target = &tables["local.live.silver"];
        assert_eq!(
            target.expect.get("positive_id").map(|e| e.check.as_str()),
            Some("id > 0")
        );
        assert_eq!(target.dedup_columns, vec!["id".to_string()]);
    }

    #[test]
    fn auto_cdc_rejects_a_target_with_its_own_expectations() {
        // Two sets of rules over the same stream is a silent disagreement, not a merge.
        let mut tables = cdc_tables();
        tables.get_mut("local.live.silver").unwrap().expect.insert(
            "own".into(),
            oxidant_config::Expectation {
                check: "id > 0".into(),
                action: oxidant_config::ExpectAction::Drop,
            },
        );
        let err = resolve_auto_cdc_flows(&cdc_graph(), &mut tables, pending(cdc_options()))
            .expect_err("target expectations");
        assert!(
            err.status.message().contains("its own expectations"),
            "{}",
            err.status.message()
        );
    }

    #[test]
    fn a_once_auto_cdc_flow_marks_its_target_as_once() {
        // `ONCE` is carried by the flow, not by the CDC options, so it has to survive the fold
        // into the target table — a `CREATE FLOW ... AS ONCE AUTO CDC` that ran on every trigger
        // would re-merge the whole source stream forever.
        let mut graph = cdc_graph();
        graph.flows.push(FlowDef {
            flow_name: "cdc_flow".into(),
            resolved: resolve_identifier("cdc_flow", Some("local"), Some("live")),
            target: resolve_identifier("silver", Some("local"), Some("live")),
            sql_conf: HashMap::new(),
            client_id: None,
            relation: None,
            query_sql: None,
            auto_cdc: Some(cdc_options()),
            once: true,
            by_name: false,
            source_code_location: None,
        });
        assert!(once_table_targets(&graph).contains("silver"));
    }

    #[test]
    fn auto_cdc_rejects_a_target_that_ingests_its_own_stream() {
        let mut tables = cdc_tables();
        tables.get_mut("local.live.silver").unwrap().source = Some(SourceConfig {
            format: "kafka".into(),
            options: BTreeMap::new(),
        });
        let err = resolve_auto_cdc_flows(&cdc_graph(), &mut tables, pending(cdc_options()))
            .expect_err("streaming target");
        assert!(
            err.status.message().contains("ingests its own stream"),
            "{}",
            err.status.message()
        );
    }

    #[test]
    fn auto_cdc_rejects_a_target_that_already_has_a_query() {
        let mut tables = cdc_tables();
        tables.get_mut("local.live.silver").unwrap().sql = Some("SELECT 1".into());
        let err = resolve_auto_cdc_flows(&cdc_graph(), &mut tables, pending(cdc_options()))
            .expect_err("query target");
        assert!(
            err.status.message().contains("already has a query"),
            "{}",
            err.status.message()
        );
    }

    #[test]
    fn auto_cdc_rejects_an_unknown_or_non_streaming_source() {
        let mut tables = cdc_tables();
        let mut unknown = cdc_options();
        unknown.source = "nope".into();
        let err = resolve_auto_cdc_flows(&cdc_graph(), &mut tables, pending(unknown))
            .expect_err("unknown source");
        assert!(
            err.status.message().contains("not a table declared"),
            "{}",
            err.status.message()
        );

        let mut tables = cdc_tables();
        tables.get_mut("local.live.bronze").unwrap().source = None;
        let err = resolve_auto_cdc_flows(&cdc_graph(), &mut tables, pending(cdc_options()))
            .expect_err("batch source");
        assert!(
            err.status.message().contains("not a streaming table"),
            "{}",
            err.status.message()
        );
    }

    #[test]
    fn auto_cdc_rejects_missing_keys() {
        let mut tables = cdc_tables();
        let mut no_keys = cdc_options();
        no_keys.keys.clear();
        let err = resolve_auto_cdc_flows(&cdc_graph(), &mut tables, pending(no_keys))
            .expect_err("keys required");
        assert!(
            err.status.message().contains("key column"),
            "{}",
            err.status.message()
        );
    }

    #[test]
    fn auto_cdc_rejects_two_flows_into_one_target() {
        let mut tables = cdc_tables();
        let mut two = pending(cdc_options());
        two.push(two[0].clone());
        let err = resolve_auto_cdc_flows(&cdc_graph(), &mut tables, two)
            .expect_err("one cdc flow per target");
        assert!(
            err.status.message().contains("more than one AUTO CDC flow"),
            "{}",
            err.status.message()
        );
    }

    fn attribute(name: &str) -> sc::Expression {
        sc::Expression {
            expr_type: Some(sc::expression::ExprType::UnresolvedAttribute(
                sc::expression::UnresolvedAttribute {
                    unparsed_identifier: name.to_string(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    }

    fn expr_string(text: &str) -> sc::Expression {
        sc::Expression {
            expr_type: Some(sc::expression::ExprType::ExpressionString(
                sc::expression::ExpressionString {
                    expression: text.to_string(),
                },
            )),
            ..Default::default()
        }
    }

    #[test]
    fn auto_cdc_proto_maps_columns_and_conditions() {
        let ctx = datafusion::prelude::SessionContext::new();
        let details = sc::pipeline_command::define_flow::AutoCdcFlowDetails {
            source: Some("bronze".into()),
            keys: vec![attribute("id"), attribute("tenant")],
            sequence_by: Some(attribute("seq")),
            apply_as_deletes: Some(expr_string("op = 'D'")),
            except_column_list: vec![attribute("op")],
            ignore_null_updates_column_list: vec![attribute("name")],
            stored_as_scd_type: sc::pipeline_command::define_flow::ScdType::ScdType1 as i32,
            ..Default::default()
        };
        let cdc = auto_cdc_from_proto(&ctx, &details).expect("maps");
        assert_eq!(cdc.keys, vec!["id", "tenant"]);
        assert_eq!(cdc.sequence_by, "seq");
        assert_eq!(cdc.apply_as_deletes.as_deref(), Some("op = 'D'"));
        assert_eq!(cdc.except_column_list, Some(vec!["op".to_string()]));
        assert_eq!(
            cdc.ignore_null_updates_columns,
            Some(vec!["name".to_string()])
        );
    }

    #[test]
    fn auto_cdc_proto_rejects_scd_type_2_and_missing_options() {
        let ctx = datafusion::prelude::SessionContext::new();
        let base = sc::pipeline_command::define_flow::AutoCdcFlowDetails {
            source: Some("bronze".into()),
            keys: vec![attribute("id")],
            sequence_by: Some(attribute("seq")),
            ..Default::default()
        };

        // The vendored proto only knows SCD_TYPE_1, so anything else arrives as a raw enum value.
        let scd2 = sc::pipeline_command::define_flow::AutoCdcFlowDetails {
            stored_as_scd_type: 2,
            ..base.clone()
        };
        let err = auto_cdc_from_proto(&ctx, &scd2).expect_err("scd type 2");
        assert!(err.contains("SCD_TYPE_1"), "{err}");

        let no_keys = sc::pipeline_command::define_flow::AutoCdcFlowDetails {
            keys: Vec::new(),
            ..base.clone()
        };
        assert!(auto_cdc_from_proto(&ctx, &no_keys)
            .expect_err("keys")
            .contains("keys"));

        let no_seq = sc::pipeline_command::define_flow::AutoCdcFlowDetails {
            sequence_by: None,
            ..base
        };
        assert!(auto_cdc_from_proto(&ctx, &no_seq)
            .expect_err("sequence_by")
            .contains("sequence_by"));
    }

    #[test]
    fn registry_drop_session_removes_graphs() {
        let registry = DataflowGraphRegistry::default();
        let graph = DataflowGraph {
            default_catalog: None,
            default_database: None,
            sql_conf: HashMap::new(),
            outputs: Vec::new(),
            flows: Vec::new(),
            refreshes: Vec::new(),
            created_at: SystemTime::now(),
        };
        registry.insert("sess-a", "g1".into(), graph);
        assert!(registry.remove("g1"));
        registry.insert(
            "sess-a",
            "g2".into(),
            DataflowGraph {
                default_catalog: None,
                default_database: None,
                sql_conf: HashMap::new(),
                outputs: Vec::new(),
                flows: Vec::new(),
                refreshes: Vec::new(),
                created_at: SystemTime::now(),
            },
        );
        registry.drop_session("sess-a");
        assert!(!registry.remove("g2"));
    }

    #[test]
    fn registry_rejects_cross_session_access() {
        let registry = DataflowGraphRegistry::default();
        registry.insert(
            "owner",
            "g1".into(),
            DataflowGraph {
                default_catalog: None,
                default_database: None,
                sql_conf: HashMap::new(),
                outputs: Vec::new(),
                flows: Vec::new(),
                refreshes: Vec::new(),
                created_at: SystemTime::now(),
            },
        );
        let err = registry
            .with_graph("g1", "intruder", |_| Ok(()))
            .expect_err("cross-session access");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("another session"));
    }

    #[test]
    fn apply_sql_conf_stores_known_keys_and_reports_unknown() {
        let svc = OxidantService::new();
        let (ignored, snap) = svc.apply_sql_conf(
            &HashMap::from([
                (
                    "spark.sql.session.timeZone".to_string(),
                    "Europe/Berlin".to_string(),
                ),
                ("not.supported.key".to_string(), "x".to_string()),
            ]),
            SqlConfScope::Graph,
        );
        assert_eq!(ignored, vec!["not.supported.key".to_string()]);
        assert_eq!(
            svc.config
                .lock()
                .expect("config")
                .get("spark.sql.session.timeZone")
                .map(String::as_str),
            Some("Europe/Berlin")
        );
        svc.restore_sql_conf(snap);
        assert_eq!(
            svc.config
                .lock()
                .expect("config")
                .get("spark.sql.session.timeZone")
                .map(String::as_str),
            Some("UTC")
        );
    }

    #[test]
    fn table_source_locations_prefers_flow_target() {
        let mut graph = DataflowGraph {
            default_catalog: Some("cat".into()),
            default_database: Some("db".into()),
            sql_conf: HashMap::new(),
            outputs: vec![OutputDef {
                output_name: "metrics".into(),
                resolved: resolve_identifier("metrics", Some("cat"), Some("db")),
                output_type: sc::OutputType::Table as i32,
                comment: None,
                table_details: None,
                sink_details: None,
                source_code_location: Some(sc::SourceCodeLocation {
                    file_name: Some("out.py".into()),
                    line_number: Some(1),
                    definition_path: None,
                    extension: vec![],
                }),
            }],
            flows: vec![FlowDef {
                flow_name: "f".into(),
                resolved: resolve_identifier("f", Some("cat"), Some("db")),
                target: resolve_identifier("metrics", Some("cat"), Some("db")),
                sql_conf: HashMap::new(),
                client_id: None,
                relation: None,
                query_sql: Some("SELECT 1".into()),
                auto_cdc: None,
                once: false,
                by_name: false,
                source_code_location: Some(sc::SourceCodeLocation {
                    file_name: Some("flow.sql".into()),
                    line_number: Some(9),
                    definition_path: None,
                    extension: vec![],
                }),
            }],
            refreshes: Vec::new(),
            created_at: SystemTime::now(),
        };
        let locs = table_source_locations(&graph);
        assert_eq!(
            locs.get("cat.db.metrics").map(String::as_str),
            Some("flow.sql:9")
        );
        graph.flows.clear();
        let locs = table_source_locations(&graph);
        assert_eq!(
            locs.get("cat.db.metrics").map(String::as_str),
            Some("out.py:1")
        );
    }

    #[test]
    fn apply_sql_conf_ignored_keys_are_sorted() {
        let svc = OxidantService::new();
        let (ignored, snap) = svc.apply_sql_conf(
            &HashMap::from([
                ("z.last".to_string(), "1".to_string()),
                ("a.first".to_string(), "2".to_string()),
                ("m.middle".to_string(), "3".to_string()),
            ]),
            SqlConfScope::Graph,
        );
        assert_eq!(
            ignored,
            vec![
                "a.first".to_string(),
                "m.middle".to_string(),
                "z.last".to_string()
            ]
        );
        svc.restore_sql_conf(snap);
    }

    #[test]
    fn shuffle_partitions_sql_conf_key_is_not_allowlisted() {
        let svc = OxidantService::new();
        let (ignored, snap) = svc.apply_sql_conf(
            &HashMap::from([("spark.sql.shuffle.partitions".to_string(), "8".to_string())]),
            SqlConfScope::Graph,
        );
        assert_eq!(ignored, vec!["spark.sql.shuffle.partitions".to_string()]);
        assert!(svc
            .config
            .lock()
            .expect("config")
            .get("spark.sql.shuffle.partitions")
            .is_none());
        svc.restore_sql_conf(snap);
    }

    #[test]
    fn sql_conf_guard_restores_on_drop() {
        let svc = OxidantService::new();
        let (_, snap) = svc.apply_sql_conf(
            &HashMap::from([(
                "spark.sql.session.timeZone".to_string(),
                "Europe/Berlin".to_string(),
            )]),
            SqlConfScope::Graph,
        );
        assert_eq!(
            svc.config
                .lock()
                .expect("config")
                .get("spark.sql.session.timeZone")
                .map(String::as_str),
            Some("Europe/Berlin")
        );
        drop(SqlConfGuard::new(&svc, snap));
        assert_eq!(
            svc.config
                .lock()
                .expect("config")
                .get("spark.sql.session.timeZone")
                .map(String::as_str),
            Some("UTC")
        );
    }

    #[test]
    fn flow_level_catalog_sql_conf_keys_are_ignored() {
        let svc = OxidantService::new();
        let (ignored, snap) = svc.apply_sql_conf(
            &HashMap::from([
                (
                    "spark.sql.catalog.flowonly.type".to_string(),
                    "local".to_string(),
                ),
                (
                    "spark.sql.defaultCatalog".to_string(),
                    "flowonly".to_string(),
                ),
                (
                    "spark.sql.session.timeZone".to_string(),
                    "Europe/Berlin".to_string(),
                ),
            ]),
            SqlConfScope::Flow,
        );
        assert_eq!(
            ignored,
            vec![
                "spark.sql.catalog.flowonly.type".to_string(),
                "spark.sql.defaultCatalog".to_string(),
            ]
        );
        assert_eq!(
            svc.config
                .lock()
                .expect("config")
                .get("spark.sql.session.timeZone")
                .map(String::as_str),
            Some("Europe/Berlin")
        );
        assert!(svc
            .config
            .lock()
            .expect("config")
            .get("spark.sql.catalog.flowonly.type")
            .is_none());
        svc.restore_sql_conf(snap);
    }

    #[test]
    fn table_planning_failure_event_uses_inner_message() {
        let failure = table_planning_failure(
            "bad",
            Status::invalid_argument("table not found: missing_xyz"),
            &Some(sc::SourceCodeLocation {
                file_name: Some("pipeline.sql".into()),
                line_number: Some(17),
                definition_path: None,
                extension: vec![],
            }),
        );
        let event = format_table_failed(
            &failure.table,
            &failure.inner,
            Some(&"pipeline.sql:17".to_string()),
        );
        assert_eq!(
            event,
            "[oxidant] bad                      FAILED (pipeline.sql:17): table not found: missing_xyz"
        );
        assert!(
            !event.contains("FAILED (at "),
            "event must not double-wrap the terminal status message: {event}"
        );
    }

    #[test]
    fn replace_output_replaces_by_resolved_name() {
        let mut graph = DataflowGraph {
            default_catalog: Some("cat".into()),
            default_database: Some("db".into()),
            sql_conf: HashMap::new(),
            outputs: Vec::new(),
            flows: Vec::new(),
            refreshes: Vec::new(),
            created_at: SystemTime::now(),
        };
        replace_output(
            &mut graph,
            OutputDef {
                output_name: "metrics".into(),
                resolved: resolve_identifier("metrics", Some("cat"), Some("db")),
                output_type: sc::OutputType::Table as i32,
                comment: Some("v1".into()),
                table_details: None,
                sink_details: None,
                source_code_location: None,
            },
        );
        replace_output(
            &mut graph,
            OutputDef {
                output_name: "cat.db.metrics".into(),
                resolved: resolve_identifier("cat.db.metrics", Some("cat"), Some("db")),
                output_type: sc::OutputType::MaterializedView as i32,
                comment: Some("v2".into()),
                table_details: None,
                sink_details: None,
                source_code_location: None,
            },
        );
        assert_eq!(graph.outputs.len(), 1);
        assert_eq!(
            graph.outputs[0].output_type,
            sc::OutputType::MaterializedView as i32
        );
        assert_eq!(graph.outputs[0].comment.as_deref(), Some("v2"));
    }

    #[test]
    fn proto_schema_to_ddl_renders_timestamp_for_planner() {
        use sc::data_type::{Kind, Struct, StructField, TimestampNtz};
        let dt = sc::DataType {
            kind: Some(Kind::Struct(Struct {
                fields: vec![StructField {
                    name: "ts".into(),
                    data_type: Some(sc::DataType {
                        kind: Some(Kind::TimestampNtz(TimestampNtz {
                            type_variation_reference: 0,
                        })),
                    }),
                    nullable: true,
                    metadata: None,
                }],
                type_variation_reference: 0,
            })),
        };
        let ddl = proto_schema_to_ddl(&dt).expect("timestamp ddl");
        assert!(ddl.contains("TIMESTAMP"), "ddl={ddl}");
        assert!(!ddl.contains("TIMESTAMP_NTZ"), "ddl={ddl}");
    }

    #[test]
    fn proto_schema_to_ddl_surfaces_conversion_errors() {
        let dt = sc::DataType { kind: None };
        let err = proto_schema_to_ddl(&dt).expect_err("invalid proto");
        assert!(err.to_string().contains("schema_data_type"), "{err}");
    }

    fn graph_with_outputs(outputs: &[(&str, i32)]) -> DataflowGraph {
        DataflowGraph {
            default_catalog: Some("local".into()),
            default_database: Some("live".into()),
            sql_conf: HashMap::new(),
            outputs: outputs
                .iter()
                .map(|(name, output_type)| OutputDef {
                    output_name: (*name).to_string(),
                    resolved: resolve_identifier(name, Some("local"), Some("live")),
                    output_type: *output_type,
                    comment: None,
                    table_details: None,
                    sink_details: None,
                    source_code_location: None,
                })
                .collect(),
            flows: Vec::new(),
            refreshes: Vec::new(),
            created_at: SystemTime::now(),
        }
    }

    #[test]
    fn deferred_sql_references_defers_only_pipeline_built_tables() {
        let graph = graph_with_outputs(&[
            ("orders_bronze", sc::OutputType::Table as i32),
            ("scratch", sc::OutputType::TemporaryView as i32),
        ]);

        // Reads a table this run builds: cannot be planned before the run.
        assert_eq!(
            deferred_sql_references("SELECT customer FROM orders_bronze", &graph),
            Some(vec![])
        );
        assert_eq!(
            deferred_sql_references("SELECT customer FROM local.live.orders_bronze", &graph),
            Some(vec![])
        );
        // Reads only catalog tables, so it must still be validated at StartRun.
        assert_eq!(
            deferred_sql_references("SELECT * FROM definitely_missing_table_xyz", &graph),
            None
        );
        // Temporary views are registered before planning and resolve normally.
        assert_eq!(
            deferred_sql_references("SELECT * FROM scratch", &graph),
            None
        );
        // Unparseable text falls through to the planner, which reports the real error.
        assert_eq!(deferred_sql_references("NOT SQL AT ALL (", &graph), None);
    }

    #[test]
    fn deferred_sql_references_reports_non_graph_tables() {
        let graph = graph_with_outputs(&[
            ("orders_bronze", sc::OutputType::Table as i32),
            ("scratch", sc::OutputType::TemporaryView as i32),
        ]);

        // The whole statement is deferred because of `orders_bronze`, so `typo_tabel` never
        // reaches the analyzer — it is handed back for a catalog check instead.
        assert_eq!(
            deferred_sql_references(
                "SELECT * FROM orders_bronze JOIN typo_tabel ON orders_bronze.id = typo_tabel.id",
                &graph
            ),
            Some(vec!["typo_tabel".to_string()])
        );
        // Graph temporary views resolve normally, so they are not reported as external.
        assert_eq!(
            deferred_sql_references(
                "SELECT * FROM orders_bronze JOIN scratch USING (id)",
                &graph
            ),
            Some(vec![])
        );
        // CTEs are not tables, even when a CTE shadows a name used as a table reference.
        assert_eq!(
            deferred_sql_references(
                "WITH t AS (SELECT * FROM orders_bronze) SELECT * FROM t",
                &graph
            ),
            Some(vec![])
        );
    }

    #[test]
    fn pending_query_function_flows_respects_client_id() {
        let graph = DataflowGraph {
            default_catalog: Some("local".into()),
            default_database: Some("live".into()),
            sql_conf: HashMap::new(),
            outputs: Vec::new(),
            flows: vec![
                FlowDef {
                    flow_name: "pending_a".into(),
                    resolved: resolve_identifier("pending_a", Some("local"), Some("live")),
                    target: resolve_identifier("out_a", Some("local"), Some("live")),
                    sql_conf: HashMap::new(),
                    client_id: Some("client-a".into()),
                    relation: None,
                    query_sql: None,
                    auto_cdc: None,
                    once: false,
                    by_name: false,
                    source_code_location: None,
                },
                FlowDef {
                    flow_name: "resolved".into(),
                    resolved: resolve_identifier("resolved", Some("local"), Some("live")),
                    target: resolve_identifier("out_b", Some("local"), Some("live")),
                    sql_conf: HashMap::new(),
                    client_id: Some("client-a".into()),
                    relation: Some(sc::Relation {
                        rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                            query: "SELECT 1".into(),
                            ..Default::default()
                        })),
                        ..Default::default()
                    }),
                    query_sql: None,
                    auto_cdc: None,
                    once: false,
                    by_name: false,
                    source_code_location: None,
                },
            ],
            refreshes: Vec::new(),
            created_at: SystemTime::now(),
        };
        let pending = pending_query_function_flows(&graph, Some("client-a"));
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].table_name, "pending_a");
        assert!(pending_query_function_flows(&graph, Some("client-b")).is_empty());
    }
}
