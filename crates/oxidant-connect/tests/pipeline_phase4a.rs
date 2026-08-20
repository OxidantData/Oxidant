//! SDP Phase 4a: Python query-function execution signal stream.

use std::collections::HashMap;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::time::Duration;

use oxidant_connect::{serve, ServerConfig};
use oxidant_loom::arrow::array::Int64Array;
use oxidant_loom::arrow::ipc::reader::StreamReader;
use oxidant_proto::spark::connect as sc;
use sc::spark_connect_service_client::SparkConnectServiceClient;
use tokio_stream::StreamExt;
use tonic::{Code, Request, Status};

const SESSION: &str = "sdp-phase4a";
const CLIENT_ID: &str = "pyspark-client-001";
/// Source location declared on the empty `DefineFlow` in [`setup_spool_graph`], so tests can
/// assert that flow failures at `StartRun` carry it.
const FLOW_FILE: &str = "pipeline.py";
const FLOW_LINE: i32 = 42;

/// The flow `DefineSqlGraphElements` synthesizes for `CREATE STREAMING TABLE orders_bronze`.
/// It carries `query_sql`, never a relation, so it must never be a backfill target.
const SQL_FLOW_NAME: &str = "flow_to_orders_bronze";

/// The query function the client evaluates for `revenue_gold`; reads a table the run builds, so
/// it is forwarded to the runner unplanned.
const GOLD_QUERY: &str = "SELECT customer, sum(amount) AS revenue, count(*) AS orders \
                          FROM orders_bronze WHERE amount > 0 GROUP BY customer";

fn pick_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local_addr")
        .port()
}

fn local_catalog_conf(warehouse: &Path) -> HashMap<String, String> {
    HashMap::from([
        (
            "spark.sql.catalog.local.type".to_string(),
            "local".to_string(),
        ),
        (
            "spark.sql.catalog.local.warehouse".to_string(),
            warehouse.to_string_lossy().to_string(),
        ),
        ("spark.sql.defaultCatalog".to_string(), "local".to_string()),
    ])
}

async fn boot(
    port: u16,
    catalogs: HashMap<String, String>,
) -> SparkConnectServiceClient<tonic::transport::Channel> {
    tokio::spawn(async move {
        let _ = serve(ServerConfig {
            port,
            ui_port: None,
            catalogs,
            ..Default::default()
        })
        .await;
    });
    let endpoint = format!("http://127.0.0.1:{port}");
    for _ in 0..50 {
        if let Ok(c) = SparkConnectServiceClient::connect(endpoint.clone()).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server did not become ready on {port}");
}

fn pipeline_plan(cmd: sc::PipelineCommand) -> sc::Plan {
    sc::Plan {
        op_type: Some(sc::plan::OpType::Command(sc::Command {
            command_type: Some(sc::command::CommandType::PipelineCommand(cmd)),
        })),
    }
}

async fn execute_pipeline(
    client: &mut SparkConnectServiceClient<tonic::transport::Channel>,
    cmd: sc::PipelineCommand,
) -> Result<Vec<sc::ExecutePlanResponse>, Status> {
    let mut stream = client
        .execute_plan(Request::new(sc::ExecutePlanRequest {
            session_id: SESSION.to_string(),
            plan: Some(pipeline_plan(cmd)),
            ..Default::default()
        }))
        .await?
        .into_inner();
    let mut responses = Vec::new();
    while let Some(item) = stream.next().await {
        responses.push(item?);
    }
    Ok(responses)
}

async fn execute_pipeline_expect_error(
    client: &mut SparkConnectServiceClient<tonic::transport::Channel>,
    cmd: sc::PipelineCommand,
) -> (Vec<sc::ExecutePlanResponse>, Status) {
    let mut stream = client
        .execute_plan(Request::new(sc::ExecutePlanRequest {
            session_id: SESSION.to_string(),
            plan: Some(pipeline_plan(cmd)),
            ..Default::default()
        }))
        .await
        .expect("execute_plan rpc")
        .into_inner();
    let mut responses = Vec::new();
    while let Some(item) = stream.next().await {
        match item {
            Ok(resp) => responses.push(resp),
            Err(status) => return (responses, status),
        }
    }
    panic!("expected terminal error status, got only success responses");
}

fn graph_id_from_create(resps: &[sc::ExecutePlanResponse]) -> String {
    resps
        .iter()
        .find_map(|r| match &r.response_type {
            Some(sc::execute_plan_response::ResponseType::PipelineCommandResult(res)) => {
                match &res.result_type {
                    Some(sc::pipeline_command_result::ResultType::CreateDataflowGraphResult(r)) => {
                        r.dataflow_graph_id.clone()
                    }
                    _ => None,
                }
            }
            _ => None,
        })
        .expect("CreateDataflowGraphResult")
}

fn flow_id_from_define(resps: &[sc::ExecutePlanResponse]) -> sc::ResolvedIdentifier {
    resps
        .iter()
        .find_map(|r| match &r.response_type {
            Some(sc::execute_plan_response::ResponseType::PipelineCommandResult(res)) => {
                match &res.result_type {
                    Some(sc::pipeline_command_result::ResultType::DefineFlowResult(r)) => {
                        r.resolved_identifier.clone()
                    }
                    _ => None,
                }
            }
            _ => None,
        })
        .expect("DefineFlowResult")
}

fn signal_flow_identifiers(resps: &[sc::ExecutePlanResponse]) -> Vec<sc::ResolvedIdentifier> {
    resps
        .iter()
        .filter_map(|r| match &r.response_type {
            Some(
                sc::execute_plan_response::ResponseType::PipelineQueryFunctionExecutionSignal(sig),
            ) => Some(sig.flow_identifiers.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}

/// The deprecated pre-4.2 `flow_names` field, which oxidant populates alongside identifiers.
fn signal_flow_names(resps: &[sc::ExecutePlanResponse]) -> Vec<String> {
    resps
        .iter()
        .filter_map(|r| match &r.response_type {
            Some(
                sc::execute_plan_response::ResponseType::PipelineQueryFunctionExecutionSignal(sig),
            ) =>
            {
                #[allow(deprecated)]
                Some(sig.flow_names.clone())
            }
            _ => None,
        })
        .flatten()
        .collect()
}

fn spool_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/spool/orders")
}

fn bronze_sql(spool: &Path) -> String {
    format!(
        "CREATE STREAMING TABLE orders_bronze \
         TBLPROPERTIES ('subscribe' = 'orders', 'oxidant.spool.dir' = '{}', 'startingOffsets' = 'earliest') \
         USING DELTA \
         AS SELECT \
           CAST(get_json_object(CAST(value AS STRING), '$.order_id') AS BIGINT) AS order_id, \
           get_json_object(CAST(value AS STRING), '$.customer') AS customer, \
           CAST(get_json_object(CAST(value AS STRING), '$.amount') AS BIGINT) AS amount \
         FROM stream",
        spool.display()
    )
}

async fn sql_scalar_i64(
    client: &mut SparkConnectServiceClient<tonic::transport::Channel>,
    sql: &str,
) -> i64 {
    let mut stream = client
        .execute_plan(Request::new(sc::ExecutePlanRequest {
            session_id: SESSION.to_string(),
            plan: Some(sc::Plan {
                op_type: Some(sc::plan::OpType::Root(sc::Relation {
                    rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                        query: sql.to_string(),
                        ..Default::default()
                    })),
                    ..Default::default()
                })),
            }),
            ..Default::default()
        }))
        .await
        .expect("sql query")
        .into_inner();

    let mut value = None;
    while let Some(msg) = stream.next().await {
        let msg = msg.expect("response");
        if let Some(sc::execute_plan_response::ResponseType::ArrowBatch(batch)) = msg.response_type
        {
            let reader =
                StreamReader::try_new(std::io::Cursor::new(batch.data), None).expect("ipc");
            for rb in reader {
                let rb = rb.expect("batch");
                let col = rb
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("i64 column");
                value = Some(col.value(0));
            }
        }
    }
    value.expect("scalar result")
}

async fn setup_spool_graph(
    client: &mut SparkConnectServiceClient<tonic::transport::Channel>,
) -> (String, sc::ResolvedIdentifier) {
    let create = execute_pipeline(
        client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::CreateDataflowGraph(
                sc::pipeline_command::CreateDataflowGraph {
                    default_catalog: Some("local".into()),
                    default_database: Some("live".into()),
                    sql_conf: Default::default(),
                },
            )),
        },
    )
    .await
    .expect("create graph");
    let graph_id = graph_id_from_create(&create);

    execute_pipeline(
        client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineSqlGraphElements(
                sc::pipeline_command::DefineSqlGraphElements {
                    dataflow_graph_id: Some(graph_id.clone()),
                    sql_file_path: Some("bronze.sql".into()),
                    sql_text: Some(bronze_sql(&spool_dir().canonicalize().expect("spool"))),
                },
            )),
        },
    )
    .await
    .expect("define bronze sql");

    execute_pipeline(
        client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineOutput(
                sc::pipeline_command::DefineOutput {
                    dataflow_graph_id: Some(graph_id.clone()),
                    output_name: Some("revenue_gold".into()),
                    output_type: Some(sc::OutputType::MaterializedView as i32),
                    comment: None,
                    source_code_location: None,
                    details: Some(sc::pipeline_command::define_output::Details::TableDetails(
                        sc::pipeline_command::define_output::TableDetails {
                            table_properties: Default::default(),
                            partition_cols: vec![],
                            format: Some("delta".into()),
                            schema: None,
                            clustering_columns: vec![],
                        },
                    )),
                },
            )),
        },
    )
    .await
    .expect("define output");

    let define_flow = execute_pipeline(
        client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineFlow(
                sc::pipeline_command::DefineFlow {
                    dataflow_graph_id: Some(graph_id.clone()),
                    flow_name: Some("to_revenue_gold".into()),
                    target_dataset_name: Some("revenue_gold".into()),
                    sql_conf: Default::default(),
                    client_id: Some(CLIENT_ID.into()),
                    source_code_location: Some(sc::SourceCodeLocation {
                        file_name: Some(FLOW_FILE.into()),
                        line_number: Some(FLOW_LINE),
                        ..Default::default()
                    }),
                    details: Some(
                        sc::pipeline_command::define_flow::Details::RelationFlowDetails(
                            sc::pipeline_command::define_flow::WriteRelationFlowDetails {
                                relation: Some(sc::Relation {
                                    rel_type: None,
                                    ..Default::default()
                                }),
                            },
                        ),
                    ),
                    once: None,
                },
            )),
        },
    )
    .await
    .expect("define empty flow");
    let flow_id = flow_id_from_define(&define_flow);

    (graph_id, flow_id)
}

#[tokio::test]
async fn query_function_signal_backfill_and_start_run() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_checkpoints");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints dir");

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;
    let (graph_id, flow_id) = setup_spool_graph(&mut client).await;

    let signals = execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(
                sc::pipeline_command::CommandType::GetQueryFunctionExecutionSignalStream(
                    sc::pipeline_command::GetQueryFunctionExecutionSignalStream {
                        dataflow_graph_id: Some(graph_id.clone()),
                        client_id: Some(CLIENT_ID.into()),
                    },
                ),
            ),
        },
    )
    .await
    .expect("signal stream");
    let pending = signal_flow_identifiers(&signals);
    assert_eq!(pending.len(), 1, "expected one pending flow signal");
    assert_eq!(pending[0].table_name, flow_id.table_name);
    assert!(
        signals.iter().any(|r| matches!(
            r.response_type,
            Some(sc::execute_plan_response::ResponseType::ResultComplete(_))
        )),
        "signal stream must complete"
    );

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(
                sc::pipeline_command::CommandType::DefineFlowQueryFunctionResult(
                    sc::pipeline_command::DefineFlowQueryFunctionResult {
                        dataflow_graph_id: Some(graph_id.clone()),
                        flow_identifier: Some(flow_id.clone()),
                        relation: Some(sc::Relation {
                            rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                                query:
                                    "SELECT customer, sum(amount) AS revenue, count(*) AS orders \
                                         FROM orders_bronze WHERE amount > 0 GROUP BY customer"
                                        .into(),
                                ..Default::default()
                            })),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                ),
            ),
        },
    )
    .await
    .expect("backfill relation");

    let resync = execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(
                sc::pipeline_command::CommandType::GetQueryFunctionExecutionSignalStream(
                    sc::pipeline_command::GetQueryFunctionExecutionSignalStream {
                        dataflow_graph_id: Some(graph_id.clone()),
                        client_id: Some(CLIENT_ID.into()),
                    },
                ),
            ),
        },
    )
    .await
    .expect("resync signal stream");
    assert!(
        signal_flow_identifiers(&resync).is_empty(),
        "backfilled flow must no longer be pending"
    );

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::StartRun(
                sc::pipeline_command::StartRun {
                    dataflow_graph_id: Some(graph_id),
                    dry: Some(false),
                    storage: Some(checkpoints.to_string_lossy().into_owned()),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("start run");

    let total = sql_scalar_i64(
        &mut client,
        "SELECT sum(revenue) FROM local.live.revenue_gold",
    )
    .await;
    assert_eq!(total, 725, "MV contents must match spool-derived revenue");
}

#[tokio::test]
async fn query_function_rejection_paths() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_checkpoints");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints dir");

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;
    let (graph_id, flow_id) = setup_spool_graph(&mut client).await;

    let err = execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(
                sc::pipeline_command::CommandType::DefineFlowQueryFunctionResult(
                    sc::pipeline_command::DefineFlowQueryFunctionResult {
                        dataflow_graph_id: Some(graph_id.clone()),
                        flow_identifier: Some(sc::ResolvedIdentifier {
                            catalog_name: "local".into(),
                            namespace: vec!["live".into()],
                            table_name: "missing_flow".into(),
                        }),
                        relation: Some(sc::Relation {
                            rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                                query: "SELECT 1".into(),
                                ..Default::default()
                            })),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                ),
            ),
        },
    )
    .await
    .expect_err("unknown flow backfill");
    assert_eq!(err.code(), Code::InvalidArgument);

    let other_graph = execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::CreateDataflowGraph(
                sc::pipeline_command::CreateDataflowGraph {
                    default_catalog: Some("local".into()),
                    default_database: Some("live".into()),
                    sql_conf: Default::default(),
                },
            )),
        },
    )
    .await
    .expect("other graph");
    let other_graph_id = graph_id_from_create(&other_graph);

    let err = execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(
                sc::pipeline_command::CommandType::DefineFlowQueryFunctionResult(
                    sc::pipeline_command::DefineFlowQueryFunctionResult {
                        dataflow_graph_id: Some(other_graph_id),
                        flow_identifier: Some(flow_id),
                        relation: Some(sc::Relation {
                            rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                                query: "SELECT 1".into(),
                                ..Default::default()
                            })),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                ),
            ),
        },
    )
    .await
    .expect_err("wrong graph backfill");
    assert_eq!(err.code(), Code::InvalidArgument);

    let (_, status) = execute_pipeline_expect_error(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::StartRun(
                sc::pipeline_command::StartRun {
                    dataflow_graph_id: Some(graph_id.clone()),
                    dry: Some(false),
                    storage: Some(checkpoints.to_string_lossy().into_owned()),
                    ..Default::default()
                },
            )),
        },
    )
    .await;
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(
        status.message().contains("to_revenue_gold"),
        "StartRun must name unresolved flow: {}",
        status.message()
    );
}

#[tokio::test]
async fn release_session_clears_pending_query_function_state() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;
    let (graph_id, _) = setup_spool_graph(&mut client).await;

    client
        .release_session(Request::new(sc::ReleaseSessionRequest {
            session_id: SESSION.to_string(),
            ..Default::default()
        }))
        .await
        .expect("release session");

    let err = execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(
                sc::pipeline_command::CommandType::GetQueryFunctionExecutionSignalStream(
                    sc::pipeline_command::GetQueryFunctionExecutionSignalStream {
                        dataflow_graph_id: Some(graph_id),
                        client_id: Some(CLIENT_ID.into()),
                    },
                ),
            ),
        },
    )
    .await
    .expect_err("graph must be gone after session release");
    assert_eq!(err.code(), Code::InvalidArgument);
}

/// The stock client packs `PipelineAnalysisContext` into `user_context.extensions` while it
/// evaluates a query function (`pyspark.pipelines.add_pipeline_analysis_context`); the server must
/// accept requests carrying it even though it does not act on it yet.
fn analysis_context_extension(graph_id: &str, flow_name: &str) -> prost_types::Any {
    let ctx = {
        #[allow(deprecated)]
        sc::PipelineAnalysisContext {
            dataflow_graph_id: Some(graph_id.to_string()),
            definition_path: Some("pipeline.py".into()),
            flow_name: Some(flow_name.to_string()),
            flow_identifier: None,
            extension: vec![],
        }
    };
    prost_types::Any {
        type_url: "type.googleapis.com/spark.connect.PipelineAnalysisContext".to_string(),
        value: prost::Message::encode_to_vec(&ctx),
    }
}

async fn execute_pipeline_with_user_context(
    client: &mut SparkConnectServiceClient<tonic::transport::Channel>,
    cmd: sc::PipelineCommand,
    user_context: sc::UserContext,
) -> Result<Vec<sc::ExecutePlanResponse>, Status> {
    let mut stream = client
        .execute_plan(Request::new(sc::ExecutePlanRequest {
            session_id: SESSION.to_string(),
            plan: Some(pipeline_plan(cmd)),
            user_context: Some(user_context),
            ..Default::default()
        }))
        .await?
        .into_inner();
    let mut responses = Vec::new();
    while let Some(item) = stream.next().await {
        responses.push(item?);
    }
    Ok(responses)
}

#[tokio::test]
async fn pipeline_analysis_context_extension_is_accepted() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;
    let (graph_id, flow_id) = setup_spool_graph(&mut client).await;

    let user_context = sc::UserContext {
        user_id: "oxidant".into(),
        user_name: "oxidant".into(),
        extensions: vec![analysis_context_extension(&graph_id, "to_revenue_gold")],
    };

    // 1. A plain query issued while the analysis context is active.
    let mut stream = client
        .execute_plan(Request::new(sc::ExecutePlanRequest {
            session_id: SESSION.to_string(),
            user_context: Some(user_context.clone()),
            plan: Some(sc::Plan {
                op_type: Some(sc::plan::OpType::Root(sc::Relation {
                    rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                        query: "SELECT 1 AS one".into(),
                        ..Default::default()
                    })),
                    ..Default::default()
                })),
            }),
            ..Default::default()
        }))
        .await
        .expect("execute_plan with analysis context")
        .into_inner();
    while let Some(item) = stream.next().await {
        item.expect("query with analysis context must succeed");
    }

    // 2. The backfill itself, which the client sends from the same context.
    execute_pipeline_with_user_context(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(
                sc::pipeline_command::CommandType::DefineFlowQueryFunctionResult(
                    sc::pipeline_command::DefineFlowQueryFunctionResult {
                        dataflow_graph_id: Some(graph_id.clone()),
                        flow_identifier: Some(flow_id),
                        relation: Some(sc::Relation {
                            rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                                query: "SELECT customer, sum(amount) AS revenue, count(*) AS \
                                        orders FROM orders_bronze GROUP BY customer"
                                    .into(),
                                ..Default::default()
                            })),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                ),
            ),
        },
        user_context,
    )
    .await
    .expect("backfill with analysis context");
}

#[tokio::test]
async fn signal_stream_is_scoped_to_client_id() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;
    let (graph_id, flow_id) = setup_spool_graph(&mut client).await;

    let other = execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(
                sc::pipeline_command::CommandType::GetQueryFunctionExecutionSignalStream(
                    sc::pipeline_command::GetQueryFunctionExecutionSignalStream {
                        dataflow_graph_id: Some(graph_id.clone()),
                        client_id: Some("some-other-client".into()),
                    },
                ),
            ),
        },
    )
    .await
    .expect("signal stream for other client");
    assert!(
        signal_flow_identifiers(&other).is_empty(),
        "a flow registered by another client_id must not be signalled"
    );
    assert!(
        other.iter().any(|r| matches!(
            r.response_type,
            Some(sc::execute_plan_response::ResponseType::ResultComplete(_))
        )),
        "signal stream must complete even with nothing pending"
    );

    // The owning client still sees it, and gets the deprecated `flow_names` field too.
    let mine = execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(
                sc::pipeline_command::CommandType::GetQueryFunctionExecutionSignalStream(
                    sc::pipeline_command::GetQueryFunctionExecutionSignalStream {
                        dataflow_graph_id: Some(graph_id.clone()),
                        client_id: Some(CLIENT_ID.into()),
                    },
                ),
            ),
        },
    )
    .await
    .expect("signal stream for owning client");
    assert_eq!(signal_flow_identifiers(&mine).len(), 1);
    let names = signal_flow_names(&mine);
    assert_eq!(names, vec!["local.live.to_revenue_gold".to_string()]);

    // A pre-4.2 client echoes that deprecated name back; the backfill must still land.
    #[allow(deprecated)]
    let by_name = sc::pipeline_command::DefineFlowQueryFunctionResult {
        dataflow_graph_id: Some(graph_id.clone()),
        flow_name: Some(names[0].clone()),
        flow_identifier: None,
        relation: Some(sc::Relation {
            rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                query: "SELECT customer, sum(amount) AS revenue, count(*) AS orders FROM \
                        orders_bronze GROUP BY customer"
                    .into(),
                ..Default::default()
            })),
            ..Default::default()
        }),
    };
    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(
                sc::pipeline_command::CommandType::DefineFlowQueryFunctionResult(by_name),
            ),
        },
    )
    .await
    .expect("backfill by deprecated flow_name");

    let resync = execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(
                sc::pipeline_command::CommandType::GetQueryFunctionExecutionSignalStream(
                    sc::pipeline_command::GetQueryFunctionExecutionSignalStream {
                        dataflow_graph_id: Some(graph_id),
                        client_id: Some(CLIENT_ID.into()),
                    },
                ),
            ),
        },
    )
    .await
    .expect("resync signal stream");
    assert!(
        signal_flow_identifiers(&resync).is_empty(),
        "flow backfilled by deprecated name must clear"
    );
    assert_eq!(flow_id.table_name, "to_revenue_gold");
}

#[tokio::test]
async fn backfill_requires_flow_and_relation() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;
    let (graph_id, flow_id) = setup_spool_graph(&mut client).await;

    let err = execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(
                sc::pipeline_command::CommandType::DefineFlowQueryFunctionResult(
                    sc::pipeline_command::DefineFlowQueryFunctionResult {
                        dataflow_graph_id: Some(graph_id.clone()),
                        relation: Some(sc::Relation {
                            rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                                query: "SELECT 1".into(),
                                ..Default::default()
                            })),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                ),
            ),
        },
    )
    .await
    .expect_err("backfill without a flow identifier");
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(err.message().contains("flow_identifier"), "{err}");

    let err = execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(
                sc::pipeline_command::CommandType::DefineFlowQueryFunctionResult(
                    sc::pipeline_command::DefineFlowQueryFunctionResult {
                        dataflow_graph_id: Some(graph_id),
                        flow_identifier: Some(flow_id),
                        relation: Some(sc::Relation {
                            rel_type: None,
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                ),
            ),
        },
    )
    .await
    .expect_err("backfill with an empty relation");
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(err.message().contains("relation is empty"), "{err}");
}

/// Build a `DefineFlowQueryFunctionResult` addressing a flow by resolved identifier.
fn backfill_by_id(
    graph_id: &str,
    flow_id: &sc::ResolvedIdentifier,
    query: &str,
) -> sc::PipelineCommand {
    sc::PipelineCommand {
        command_type: Some(
            sc::pipeline_command::CommandType::DefineFlowQueryFunctionResult(
                sc::pipeline_command::DefineFlowQueryFunctionResult {
                    dataflow_graph_id: Some(graph_id.to_string()),
                    flow_identifier: Some(flow_id.clone()),
                    relation: Some(sc::Relation {
                        rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                            query: query.to_string(),
                            ..Default::default()
                        })),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ),
        ),
    }
}

fn start_run(graph_id: &str, checkpoints: &Path) -> sc::PipelineCommand {
    sc::PipelineCommand {
        command_type: Some(sc::pipeline_command::CommandType::StartRun(
            sc::pipeline_command::StartRun {
                dataflow_graph_id: Some(graph_id.to_string()),
                dry: Some(false),
                storage: Some(checkpoints.to_string_lossy().into_owned()),
                ..Default::default()
            },
        )),
    }
}

/// Backfill is accepted **once**. A second `DefineFlowQueryFunctionResult` for the same flow is
/// rejected rather than silently overwriting the relation the first one stored.
#[tokio::test]
async fn double_backfill_is_rejected_and_keeps_the_first_relation() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_checkpoints");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints dir");

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;
    let (graph_id, flow_id) = setup_spool_graph(&mut client).await;

    execute_pipeline(&mut client, backfill_by_id(&graph_id, &flow_id, GOLD_QUERY))
        .await
        .expect("first backfill");

    // A second result — here one that would produce visibly different contents.
    let err = execute_pipeline(
        &mut client,
        backfill_by_id(
            &graph_id,
            &flow_id,
            "SELECT customer, CAST(1 AS BIGINT) AS revenue, CAST(1 AS BIGINT) AS orders \
             FROM orders_bronze GROUP BY customer",
        ),
    )
    .await
    .expect_err("second backfill must be rejected");
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(
        err.message().contains("already defined by an earlier"),
        "{err}"
    );

    execute_pipeline(&mut client, start_run(&graph_id, &checkpoints))
        .await
        .expect("start run");
    let total = sql_scalar_i64(
        &mut client,
        "SELECT sum(revenue) FROM local.live.revenue_gold",
    )
    .await;
    assert_eq!(
        total, 725,
        "the first backfilled relation must be the one that ran"
    );
}

/// A stray result naming an SDP-SQL flow must not replace the query its `.sql` file defined —
/// `graph_to_config` prefers `relation` over `query_sql`, so accepting it would silently swap the
/// table's contents.
#[tokio::test]
async fn backfill_targeting_a_sql_defined_flow_is_rejected() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_checkpoints");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints dir");

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;
    let (graph_id, flow_id) = setup_spool_graph(&mut client).await;

    let sql_flow_id = sc::ResolvedIdentifier {
        catalog_name: "local".into(),
        namespace: vec!["live".into()],
        table_name: SQL_FLOW_NAME.into(),
    };
    let err = execute_pipeline(
        &mut client,
        backfill_by_id(
            &graph_id,
            &sql_flow_id,
            "SELECT CAST(0 AS BIGINT) AS order_id, 'nobody' AS customer, \
             CAST(0 AS BIGINT) AS amount",
        ),
    )
    .await
    .expect_err("backfill of an SDP-SQL flow must be rejected");
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(
        err.message().contains("already defined by SDP SQL"),
        "{err}"
    );
    assert!(err.message().contains(SQL_FLOW_NAME), "{err}");

    // The SQL definition is intact: the run still reads the spool fixture.
    execute_pipeline(&mut client, backfill_by_id(&graph_id, &flow_id, GOLD_QUERY))
        .await
        .expect("backfill the pending flow");
    execute_pipeline(&mut client, start_run(&graph_id, &checkpoints))
        .await
        .expect("start run");
    let total = sql_scalar_i64(
        &mut client,
        "SELECT sum(revenue) FROM local.live.revenue_gold",
    )
    .await;
    assert_eq!(
        total, 725,
        "the SQL-defined bronze flow must still feed the run"
    );
}

/// Deferring is all-or-nothing per query: one reference to a table the run builds forwards the
/// whole statement unplanned. The references the graph does *not* build are still catalog-checked
/// at `StartRun`, so a typo fails through `table_planning_failure` with the flow's source
/// location instead of from deep inside the runner.
#[tokio::test]
async fn deferred_query_with_unknown_table_fails_at_start_run() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_checkpoints");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints dir");

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;
    let (graph_id, flow_id) = setup_spool_graph(&mut client).await;

    execute_pipeline(
        &mut client,
        backfill_by_id(
            &graph_id,
            &flow_id,
            "SELECT b.customer, sum(b.amount) AS revenue, count(*) AS orders \
             FROM orders_bronze b JOIN typo_tabel t ON b.customer = t.customer \
             GROUP BY b.customer",
        ),
    )
    .await
    .expect("backfill with a typo'd non-graph table");

    let (_, status) =
        execute_pipeline_expect_error(&mut client, start_run(&graph_id, &checkpoints)).await;
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(
        status.message().contains("typo_tabel"),
        "StartRun must name the missing table: {}",
        status.message()
    );
    assert!(
        status.message().contains("revenue_gold"),
        "table_planning_failure must name the target table: {}",
        status.message()
    );
    assert!(
        status
            .message()
            .contains(&format!("(at {FLOW_FILE}:{FLOW_LINE})")),
        "table_planning_failure must carry the flow's source location: {}",
        status.message()
    );
}
