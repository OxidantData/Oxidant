//! Spark Declarative Pipelines (`PipelineCommand`) handlers (SDP Phase 1A–2).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::SystemTime;

use datafusion::sql::unparser::Unparser;
use oxidant_config::{OxidantConfig, PipelineConfig, SourceConfig, TableConfig, Trigger};
use oxidant_loom::Engine;
use oxidant_pipelines::{
    clear_pipeline_state, parse, run_pipeline, split_table_properties, validate_output_format,
    OutputKind, Plan, RunEvent, RunEventKind,
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
}

/// Mirrors `PipelineCommand.DefineFlow`; relation stays unresolved until `StartRun`.
#[derive(Debug, Clone)]
pub struct FlowDef {
    pub flow_name: String,
    #[allow(dead_code)]
    pub resolved: sc::ResolvedIdentifier,
    pub target: sc::ResolvedIdentifier,
    #[allow(dead_code)]
    pub sql_conf: HashMap<String, String>,
    pub relation: Option<sc::Relation>,
    /// Populated by `DefineSqlGraphElements` when the flow comes from SDP SQL text.
    pub query_sql: Option<String>,
    pub once: bool,
    pub by_name: bool,
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

        let resolved = self
            .dataflow_graphs
            .with_graph(graph_id, session_id, |graph| {
                let (relation, once) = match &cmd.details {
                    Some(sc::pipeline_command::define_flow::Details::RelationFlowDetails(d)) => {
                        let rel = d.relation.as_ref();
                        if rel.map(|r| r.rel_type.is_none()).unwrap_or(true) {
                            return Err(Status::failed_precondition(
                                "DefineFlow relation is empty: Python query-function signal \
                                 stream is not supported yet (SDP Phase 4a)",
                            ));
                        }
                        (rel.cloned(), cmd.once.unwrap_or(false))
                    }
                    Some(sc::pipeline_command::define_flow::Details::AutoCdcFlowDetails(_)) => {
                        return Err(Status::unimplemented(
                            "DefineFlow AUTO CDC is not supported yet (SDP Phase 4b)",
                        ));
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
                        relation,
                        query_sql: None,
                        once,
                        by_name: false,
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

        let graph_refreshes = self
            .dataflow_graphs
            .with_graph(graph_id, session_id, |graph| {
                Ok(std::mem::take(&mut graph.refreshes))
            })?;

        let graph = self.dataflow_graphs.get_graph(graph_id, session_id)?;
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

        if full_refresh {
            clear_pipeline_state(&storage, &[]).map_err(crate::err_to_status)?;
        } else if !full_refresh_tables.is_empty() {
            clear_pipeline_state(&storage, &full_refresh_tables).map_err(crate::err_to_status)?;
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

        register_temp_views(engine, &graph).await?;

        let config = graph_to_config(&graph, graph_id, &storage, engine, session_id).await?;
        let plan = Plan::build(&config).map_err(crate::err_to_status)?;

        if dry {
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
            return Ok(PipelineCommandOutput::Complete(vec![
                self.pipeline_event(session_id, operation_id, &message, None),
                self.result_complete(session_id, operation_id),
            ]));
        }

        let wanted = resolve_wanted_tables(&graph, &refresh_selection);
        let once_tables = once_table_targets(&graph);
        let mut events = Vec::new();
        let run_result = run_pipeline(engine, &plan, &wanted, true, &once_tables, &mut |event| {
            events.push(event)
        })
        .await;

        let responses: Vec<sc::ExecutePlanResponse> = events
            .into_iter()
            .map(|event| self.pipeline_run_event(session_id, operation_id, event))
            .collect();
        if let Err(e) = run_result {
            return Ok(PipelineCommandOutput::Failed {
                responses,
                status: crate::err_to_status(e),
            });
        }
        let mut responses = responses;
        responses.push(self.result_complete(session_id, operation_id));
        Ok(PipelineCommandOutput::Complete(responses))
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
        event: RunEvent,
    ) -> sc::ExecutePlanResponse {
        self.pipeline_event(
            session_id,
            operation_id,
            &format_run_event(&event),
            Some(event.at),
        )
    }
}

async fn graph_to_config(
    graph: &DataflowGraph,
    graph_id: &str,
    storage: &str,
    engine: &Engine,
    _session_id: &str,
) -> Result<OxidantConfig, Status> {
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
            validate_output_format(fmt, &format!("table `{name}`"))
                .map_err(crate::err_to_status)?;
        }
        let (source_opts, sink_opts) =
            split_table_properties(&table_properties).map_err(crate::err_to_status)?;
        let source = kafka_source_from_properties(&source_opts);
        let iceberg_compat = sink_opts
            .get("icebergCompat")
            .map(|v| v.eq_ignore_ascii_case("true"));
        let output_schema = if let Some(details) = table_details {
            table_schema_from_details(details).map_err(crate::err_to_status)?
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
            expect: Default::default(),
            comment: output.comment.clone(),
        };
        tables.insert(key, table);
    }

    for flow in &graph.flows {
        if flow_targets_temporary_view(graph, flow) {
            continue;
        }
        let target_key = resolved_identifier_key(&flow.target);
        let target_name = flow.target.table_name.clone();
        let table = tables.get_mut(&target_key).ok_or_else(|| {
            Status::invalid_argument(format!(
                "flow `{}` targets undefined table `{target_name}`",
                flow.flow_name
            ))
        })?;
        let flow_sql = if let Some(relation) = &flow.relation {
            if table.source.is_none() {
                if let Some(source) = extract_streaming_source(relation) {
                    table.source = Some(source);
                }
            }
            relation_to_sql(engine, relation).await?
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
            return Err(Status::failed_precondition(format!(
                "flow `{}` has no relation or SQL at StartRun",
                flow.flow_name
            )));
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
    }

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
        relation: None,
        query_sql: Some(parsed.query_sql.clone()),
        once: parsed.once,
        by_name: parsed.by_name,
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

async fn register_temp_views(engine: &Engine, graph: &DataflowGraph) -> Result<(), Status> {
    for output in &graph.outputs {
        if output.output_type != sc::OutputType::TemporaryView as i32 {
            continue;
        }
        let name = output.resolved.table_name.clone();
        let sql = flow_sql_for_target(graph, &output.resolved).await?;
        engine
            .sql(&format!("CREATE OR REPLACE TEMPORARY VIEW {name} AS {sql}"))
            .await
            .map_err(crate::err_to_status)?;
    }
    Ok(())
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

fn format_run_event(event: &RunEvent) -> String {
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
            format!("[oxidant] {name:<24} FAILED: {error}")
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

async fn relation_to_sql(engine: &Engine, relation: &sc::Relation) -> Result<String, Status> {
    let plan = translate::to_plan(engine.ctx(), relation)
        .await
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
    Unparser::default()
        .plan_to_sql(&plan)
        .map(|stmt| stmt.to_string())
        .map_err(|e| Status::invalid_argument(format!("relation to sql: {e}")))
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
}
