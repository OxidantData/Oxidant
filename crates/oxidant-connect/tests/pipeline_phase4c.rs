//! SDP Phase 4c: external sinks and `ExecuteOutputFlows`.

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

const SESSION: &str = "sdp-phase4c";

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
        ("spark.sql.defaultDatabase".to_string(), "live".to_string()),
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
    execute_pipeline_in_session(client, SESSION, cmd).await
}

async fn execute_pipeline_in_session(
    client: &mut SparkConnectServiceClient<tonic::transport::Channel>,
    session_id: &str,
    cmd: sc::PipelineCommand,
) -> Result<Vec<sc::ExecutePlanResponse>, Status> {
    let mut stream = client
        .execute_plan(Request::new(sc::ExecutePlanRequest {
            session_id: session_id.to_string(),
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

fn pipeline_event_messages(resps: &[sc::ExecutePlanResponse]) -> Vec<String> {
    resps
        .iter()
        .filter_map(|r| match &r.response_type {
            Some(sc::execute_plan_response::ResponseType::PipelineEventResult(ev)) => {
                ev.event.as_ref()?.message.clone()
            }
            _ => None,
        })
        .collect()
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

fn spool_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/spool/orders")
}

fn spool_table_properties(spool: &Path) -> HashMap<String, String> {
    HashMap::from([
        ("subscribe".into(), "orders".into()),
        ("oxidant.spool.dir".into(), spool.display().to_string()),
        ("startingOffsets".into(), "earliest".into()),
    ])
}

fn bronze_flow_sql() -> String {
    "SELECT \
       CAST(get_json_object(CAST(value AS STRING), '$.order_id') AS BIGINT) AS order_id, \
       get_json_object(CAST(value AS STRING), '$.customer') AS customer, \
       CAST(get_json_object(CAST(value AS STRING), '$.amount') AS BIGINT) AS amount \
     FROM stream"
        .to_string()
}

fn revenue_flow_sql() -> String {
    format!(
        "SELECT customer, sum(amount) AS revenue, count(*) AS orders \
         FROM ({}) t WHERE amount > 0 GROUP BY customer",
        bronze_flow_sql()
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

async fn delta_scalar_i64(table_path: &Path, sql_expr: &str) -> i64 {
    let engine = oxidant_loom::Engine::new();
    engine
        .register_delta("streamed", &table_path.to_string_lossy())
        .await
        .expect("register delta table");
    let batches = engine
        .sql(&format!("SELECT {sql_expr} FROM streamed"))
        .await
        .expect("query delta table");
    let col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("i64 column");
    col.value(0)
}

async fn parquet_scalar_i64(dir: &Path, sql_expr: &str) -> i64 {
    let engine = oxidant_loom::Engine::new();
    engine
        .register_parquet("streamed", &dir.to_string_lossy())
        .await
        .expect("register parquet directory");
    let batches = engine
        .sql(&format!("SELECT {sql_expr} FROM streamed"))
        .await
        .expect("query parquet files");
    let col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("i64 column");
    col.value(0)
}

/// `CreateDataflowGraph` + `DefineOutput` for a sink, returning the graph id.
async fn define_sink(
    client: &mut SparkConnectServiceClient<tonic::transport::Channel>,
    name: &str,
    format: &str,
    options: HashMap<String, String>,
) -> Result<String, Status> {
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

    define_sink_on(client, &graph_id, name, format, options).await?;
    Ok(graph_id)
}

/// `DefineOutput` for a sink on an existing graph. Re-issuing it replaces the definition, which
/// is how a user edits a sink's path without touching its SQL.
async fn define_sink_on(
    client: &mut SparkConnectServiceClient<tonic::transport::Channel>,
    graph_id: &str,
    name: &str,
    format: &str,
    options: HashMap<String, String>,
) -> Result<(), Status> {
    execute_pipeline(
        client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineOutput(
                sc::pipeline_command::DefineOutput {
                    dataflow_graph_id: Some(graph_id.to_string()),
                    output_name: Some(name.into()),
                    output_type: Some(sc::OutputType::Sink as i32),
                    details: Some(sc::pipeline_command::define_output::Details::SinkDetails(
                        sc::pipeline_command::define_output::SinkDetails {
                            format: Some(format.into()),
                            options,
                        },
                    )),
                    ..Default::default()
                },
            )),
        },
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn define_output_kafka_sink_is_refused() {
    let port = pick_port();
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;

    let create = execute_pipeline(
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
    .expect("create graph");
    let graph_id = graph_id_from_create(&create);

    let err = execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineOutput(
                sc::pipeline_command::DefineOutput {
                    dataflow_graph_id: Some(graph_id),
                    output_name: Some("events_out".into()),
                    output_type: Some(sc::OutputType::Sink as i32),
                    details: Some(sc::pipeline_command::define_output::Details::SinkDetails(
                        sc::pipeline_command::define_output::SinkDetails {
                            format: Some("kafka".into()),
                            options: HashMap::from([
                                ("path".into(), "/tmp/out".into()),
                                ("topic".into(), "events".into()),
                            ]),
                        },
                    )),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect_err("kafka sink must be refused");
    assert_eq!(err.code(), Code::Unimplemented, "{err}");
    assert!(
        err.message().contains("Kafka sink is not supported"),
        "{err}"
    );
    assert!(err.message().contains("TODOS"), "{err}");
}

#[tokio::test]
async fn start_run_delta_sink_streams_from_spool_without_catalog_registration() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_checkpoints");
    let sink_path = warehouse.path().join("external_sink");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints dir");

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;

    let create = execute_pipeline(
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
    .expect("create graph");
    let graph_id = graph_id_from_create(&create);

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineOutput(
                sc::pipeline_command::DefineOutput {
                    dataflow_graph_id: Some(graph_id.clone()),
                    output_name: Some("orders_sink".into()),
                    output_type: Some(sc::OutputType::Sink as i32),
                    details: Some(sc::pipeline_command::define_output::Details::SinkDetails(
                        sc::pipeline_command::define_output::SinkDetails {
                            format: Some("delta".into()),
                            options: {
                                let mut opts = spool_table_properties(
                                    &spool_dir().canonicalize().expect("spool"),
                                );
                                opts.insert(
                                    "path".into(),
                                    sink_path.to_string_lossy().into_owned(),
                                );
                                opts
                            },
                        },
                    )),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("define sink");

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineFlow(
                sc::pipeline_command::DefineFlow {
                    dataflow_graph_id: Some(graph_id.clone()),
                    flow_name: Some("flow_to_orders_sink".into()),
                    target_dataset_name: Some("orders_sink".into()),
                    details: Some(
                        sc::pipeline_command::define_flow::Details::RelationFlowDetails(
                            sc::pipeline_command::define_flow::WriteRelationFlowDetails {
                                relation: Some(sc::Relation {
                                    rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                                        query: bronze_flow_sql(),
                                        ..Default::default()
                                    })),
                                    ..Default::default()
                                }),
                            },
                        ),
                    ),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("define sink flow");

    let run = execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::StartRun(
                sc::pipeline_command::StartRun {
                    dataflow_graph_id: Some(graph_id),
                    dry: Some(false),
                    full_refresh_all: Some(true),
                    storage: Some(checkpoints.to_string_lossy().into_owned()),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("start run");

    let events = pipeline_event_messages(&run);
    assert!(
        events.iter().any(|m| m.contains("orders_sink")),
        "expected sink table event, got {events:?}"
    );
    assert!(
        events.iter().any(|m| m.contains("pass complete")),
        "expected pass complete, got {events:?}"
    );
    assert!(
        sink_path.join("_delta_log").is_dir(),
        "delta sink must write a transaction log at {}",
        sink_path.display()
    );
    assert_eq!(
        delta_scalar_i64(&sink_path, "count(*)").await,
        5,
        "all spool records must land in the external delta sink"
    );

    let describe = client
        .execute_plan(Request::new(sc::ExecutePlanRequest {
            session_id: SESSION.to_string(),
            plan: Some(sc::Plan {
                op_type: Some(sc::plan::OpType::Root(sc::Relation {
                    rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                        query: "DESCRIBE TABLE local.live.orders_sink".to_string(),
                        ..Default::default()
                    })),
                    ..Default::default()
                })),
            }),
            ..Default::default()
        }))
        .await;
    assert!(
        describe.is_err(),
        "external sink must not be registered in the catalog"
    );
}

#[tokio::test]
async fn start_run_rejects_flow_that_reads_a_sink() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_checkpoints");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints dir");

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;

    let create = execute_pipeline(
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
    .expect("create graph");
    let graph_id = graph_id_from_create(&create);

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineOutput(
                sc::pipeline_command::DefineOutput {
                    dataflow_graph_id: Some(graph_id.clone()),
                    output_name: Some("orders_sink".into()),
                    output_type: Some(sc::OutputType::Sink as i32),
                    details: Some(sc::pipeline_command::define_output::Details::SinkDetails(
                        sc::pipeline_command::define_output::SinkDetails {
                            format: Some("delta".into()),
                            options: HashMap::from([(
                                "path".into(),
                                warehouse.path().join("sink").to_string_lossy().into_owned(),
                            )]),
                        },
                    )),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("define sink");

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineOutput(
                sc::pipeline_command::DefineOutput {
                    dataflow_graph_id: Some(graph_id.clone()),
                    output_name: Some("downstream".into()),
                    output_type: Some(sc::OutputType::MaterializedView as i32),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("define mv");

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineFlow(
                sc::pipeline_command::DefineFlow {
                    dataflow_graph_id: Some(graph_id.clone()),
                    flow_name: Some("bad_flow".into()),
                    target_dataset_name: Some("downstream".into()),
                    details: Some(
                        sc::pipeline_command::define_flow::Details::RelationFlowDetails(
                            sc::pipeline_command::define_flow::WriteRelationFlowDetails {
                                relation: Some(sc::Relation {
                                    rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                                        query: "SELECT * FROM orders_sink".to_string(),
                                        ..Default::default()
                                    })),
                                    ..Default::default()
                                }),
                            },
                        ),
                    ),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("define bad flow");

    let err = execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::StartRun(
                sc::pipeline_command::StartRun {
                    dataflow_graph_id: Some(graph_id),
                    dry: Some(true),
                    storage: Some(checkpoints.to_string_lossy().into_owned()),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect_err("reading a sink must fail planning");
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert!(err.message().contains("cannot read sink"), "{err}");
}

#[tokio::test]
async fn execute_output_flows_materializes_mv_from_spool_without_registered_graph() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_checkpoints");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints dir");
    let spool = spool_dir().canonicalize().expect("spool");

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;

    let run = execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::ExecuteOutputFlows(
                sc::pipeline_command::ExecuteOutputFlows {
                    define_output: Some(sc::pipeline_command::DefineOutput {
                        output_name: Some("revenue_gold".into()),
                        output_type: Some(sc::OutputType::MaterializedView as i32),
                        details: Some(sc::pipeline_command::define_output::Details::TableDetails(
                            sc::pipeline_command::define_output::TableDetails {
                                table_properties: spool_table_properties(&spool),
                                format: Some("delta".into()),
                                ..Default::default()
                            },
                        )),
                        ..Default::default()
                    }),
                    define_flows: vec![sc::pipeline_command::DefineFlow {
                        flow_name: Some("flow_to_revenue_gold".into()),
                        target_dataset_name: Some("revenue_gold".into()),
                        details: Some(
                            sc::pipeline_command::define_flow::Details::RelationFlowDetails(
                                sc::pipeline_command::define_flow::WriteRelationFlowDetails {
                                    relation: Some(sc::Relation {
                                        rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                                            query: revenue_flow_sql(),
                                            ..Default::default()
                                        })),
                                        ..Default::default()
                                    }),
                                },
                            ),
                        ),
                        ..Default::default()
                    }],
                    full_refresh: Some(true),
                    storage: Some(checkpoints.to_string_lossy().into_owned()),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("execute output flows");

    let events = pipeline_event_messages(&run);
    assert!(
        events.iter().any(|m| m.contains("revenue_gold")),
        "expected MV event, got {events:?}"
    );
    assert!(
        events.iter().any(|m| m.contains("pass complete")),
        "expected pass complete, got {events:?}"
    );

    let total = sql_scalar_i64(
        &mut client,
        "SELECT sum(revenue) FROM local.live.revenue_gold",
    )
    .await;
    assert_eq!(total, 725, "spool fixture revenue total");
}

#[tokio::test]
async fn define_output_json_sink_is_refused_for_lack_of_a_writer() {
    let port = pick_port();
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;

    // `json` is a writable *table* format, but the streaming sink writer implements only Delta
    // and Parquet — so the refusal has to happen here, not on the first micro-batch.
    let err = define_sink(
        &mut client,
        "events_out",
        "json",
        HashMap::from([("path".into(), "/tmp/out".into())]),
    )
    .await
    .expect_err("json sink must be refused");
    assert_eq!(err.code(), Code::Unimplemented, "{err}");
    assert!(err.message().contains("has no streaming writer"), "{err}");
    assert!(err.message().contains("delta"), "{err}");
}

#[tokio::test]
async fn define_output_sink_requires_a_path() {
    let port = pick_port();
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;

    let err = define_sink(&mut client, "events_out", "delta", HashMap::new())
        .await
        .expect_err("sink without options.path must be refused");
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert!(err.message().contains("options.path"), "{err}");
}

#[tokio::test]
async fn start_run_parquet_sink_writes_files_but_no_commit_log() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_checkpoints");
    let sink_path = warehouse.path().join("parquet_sink");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints dir");

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;

    let mut options = spool_table_properties(&spool_dir().canonicalize().expect("spool"));
    options.insert("path".into(), sink_path.to_string_lossy().into_owned());
    let graph_id = define_sink(&mut client, "orders_parquet_sink", "parquet", options)
        .await
        .expect("define parquet sink");

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineFlow(
                sc::pipeline_command::DefineFlow {
                    dataflow_graph_id: Some(graph_id.clone()),
                    flow_name: Some("flow_to_orders_parquet_sink".into()),
                    target_dataset_name: Some("orders_parquet_sink".into()),
                    details: Some(
                        sc::pipeline_command::define_flow::Details::RelationFlowDetails(
                            sc::pipeline_command::define_flow::WriteRelationFlowDetails {
                                relation: Some(sc::Relation {
                                    rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                                        query: bronze_flow_sql(),
                                        ..Default::default()
                                    })),
                                    ..Default::default()
                                }),
                            },
                        ),
                    ),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("define sink flow");

    let run = execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::StartRun(
                sc::pipeline_command::StartRun {
                    dataflow_graph_id: Some(graph_id),
                    dry: Some(false),
                    full_refresh_all: Some(true),
                    storage: Some(checkpoints.to_string_lossy().into_owned()),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("start run");

    let events = pipeline_event_messages(&run);
    assert!(
        events.iter().any(|m| m.contains("pass complete")),
        "expected pass complete, got {events:?}"
    );
    assert_eq!(
        parquet_scalar_i64(&sink_path, "count(*)").await,
        5,
        "all spool records must land in the external parquet sink"
    );
    // Documented limit: a bare Parquet sink has no commit protocol — no transaction log, so a
    // reader can observe a partially written batch. Delta is the sink with atomic commits.
    assert!(
        !sink_path.join("_delta_log").exists(),
        "a parquet sink must not fabricate a transaction log"
    );
}

#[tokio::test]
async fn start_run_rejects_a_relation_flow_that_reads_a_sink() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_checkpoints");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints dir");

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;

    let graph_id = define_sink(
        &mut client,
        "orders_sink",
        "delta",
        HashMap::from([(
            "path".into(),
            warehouse.path().join("sink").to_string_lossy().into_owned(),
        )]),
    )
    .await
    .expect("define sink");

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineOutput(
                sc::pipeline_command::DefineOutput {
                    dataflow_graph_id: Some(graph_id.clone()),
                    output_name: Some("downstream".into()),
                    output_type: Some(sc::OutputType::MaterializedView as i32),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("define mv");

    // `spark.readStream.table("orders_sink")` shape: a named-table read, not SQL text. Without
    // the relation walk this would surface as "table not found" — the sink has no catalog entry.
    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineFlow(
                sc::pipeline_command::DefineFlow {
                    dataflow_graph_id: Some(graph_id.clone()),
                    flow_name: Some("bad_relation_flow".into()),
                    target_dataset_name: Some("downstream".into()),
                    details: Some(
                        sc::pipeline_command::define_flow::Details::RelationFlowDetails(
                            sc::pipeline_command::define_flow::WriteRelationFlowDetails {
                                relation: Some(sc::Relation {
                                    rel_type: Some(sc::relation::RelType::Read(sc::Read {
                                        is_streaming: true,
                                        read_type: Some(sc::read::ReadType::NamedTable(
                                            sc::read::NamedTable {
                                                unparsed_identifier: "orders_sink".into(),
                                                options: Default::default(),
                                            },
                                        )),
                                    })),
                                    ..Default::default()
                                }),
                            },
                        ),
                    ),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("define bad flow");

    let err = execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::StartRun(
                sc::pipeline_command::StartRun {
                    dataflow_graph_id: Some(graph_id),
                    dry: Some(true),
                    storage: Some(checkpoints.to_string_lossy().into_owned()),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect_err("reading a sink must fail planning");
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert!(err.message().contains("cannot read sink"), "{err}");
}

#[tokio::test]
async fn execute_output_flows_refuses_a_graph_id() {
    let port = pick_port();
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;

    // The command carries its own definitions; a graph id means the client conflated it with the
    // registered-graph path, where the definitions would already have been merged.
    let err = execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::ExecuteOutputFlows(
                sc::pipeline_command::ExecuteOutputFlows {
                    define_output: Some(sc::pipeline_command::DefineOutput {
                        dataflow_graph_id: Some("some-graph".into()),
                        output_name: Some("mv".into()),
                        output_type: Some(sc::OutputType::MaterializedView as i32),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect_err("a graph id must be refused");
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert!(err.message().contains("dataflow_graph_id"), "{err}");
}

#[tokio::test]
async fn execute_output_flows_writes_a_delta_sink() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_checkpoints");
    let sink_path = warehouse.path().join("one_shot_sink");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints dir");
    let spool = spool_dir().canonicalize().expect("spool");

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;

    let mut options = spool_table_properties(&spool);
    options.insert("path".into(), sink_path.to_string_lossy().into_owned());

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::ExecuteOutputFlows(
                sc::pipeline_command::ExecuteOutputFlows {
                    define_output: Some(sc::pipeline_command::DefineOutput {
                        output_name: Some("one_shot_sink".into()),
                        output_type: Some(sc::OutputType::Sink as i32),
                        details: Some(sc::pipeline_command::define_output::Details::SinkDetails(
                            sc::pipeline_command::define_output::SinkDetails {
                                format: Some("delta".into()),
                                options,
                            },
                        )),
                        ..Default::default()
                    }),
                    define_flows: vec![sc::pipeline_command::DefineFlow {
                        flow_name: Some("flow_to_one_shot_sink".into()),
                        target_dataset_name: Some("one_shot_sink".into()),
                        details: Some(
                            sc::pipeline_command::define_flow::Details::RelationFlowDetails(
                                sc::pipeline_command::define_flow::WriteRelationFlowDetails {
                                    relation: Some(sc::Relation {
                                        rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                                            query: bronze_flow_sql(),
                                            ..Default::default()
                                        })),
                                        ..Default::default()
                                    }),
                                },
                            ),
                        ),
                        ..Default::default()
                    }],
                    full_refresh: Some(true),
                    storage: Some(checkpoints.to_string_lossy().into_owned()),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("execute output flows to a sink");

    assert_eq!(
        delta_scalar_i64(&sink_path, "count(*)").await,
        5,
        "ExecuteOutputFlows must drive the same streaming write as StartRun"
    );
}

#[tokio::test]
async fn a_sink_fed_by_a_pipeline_table_is_recomputed_and_replaced() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_checkpoints");
    let sink_path = warehouse.path().join("derived_sink");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints dir");
    let spool = spool_dir().canonicalize().expect("spool");

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;

    let graph_id = define_sink(
        &mut client,
        "revenue_sink",
        "delta",
        HashMap::from([("path".into(), sink_path.to_string_lossy().into_owned())]),
    )
    .await
    .expect("define sink");

    // A streaming MV off the spool, then a sink flow that reads *it* rather than Kafka. This is
    // the derived path: no source of its own, so each pass recomputes and replaces the location.
    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineSqlGraphElements(
                sc::pipeline_command::DefineSqlGraphElements {
                    dataflow_graph_id: Some(graph_id.clone()),
                    sql_text: Some(format!(
                        "CREATE STREAMING TABLE orders_bronze \
                           TBLPROPERTIES ('subscribe' = 'orders', \
                             'oxidant.spool.dir' = '{}', 'startingOffsets' = 'earliest') \
                           AS {};",
                        spool.display(),
                        bronze_flow_sql()
                    )),
                    sql_file_path: Some("sink_pipeline.sql".into()),
                },
            )),
        },
    )
    .await
    .expect("define bronze table");

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineFlow(
                sc::pipeline_command::DefineFlow {
                    dataflow_graph_id: Some(graph_id.clone()),
                    flow_name: Some("flow_to_revenue_sink".into()),
                    target_dataset_name: Some("revenue_sink".into()),
                    details: Some(
                        sc::pipeline_command::define_flow::Details::RelationFlowDetails(
                            sc::pipeline_command::define_flow::WriteRelationFlowDetails {
                                relation: Some(sc::Relation {
                                    rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                                        query: "SELECT customer, sum(amount) AS revenue \
                                                FROM orders_bronze WHERE amount > 0 \
                                                GROUP BY customer"
                                            .into(),
                                        ..Default::default()
                                    })),
                                    ..Default::default()
                                }),
                            },
                        ),
                    ),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("define sink flow");

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::StartRun(
                sc::pipeline_command::StartRun {
                    dataflow_graph_id: Some(graph_id),
                    dry: Some(false),
                    full_refresh_all: Some(true),
                    storage: Some(checkpoints.to_string_lossy().into_owned()),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("start run");

    assert_eq!(
        delta_scalar_i64(&sink_path, "sum(revenue)").await,
        725,
        "the sink must receive the aggregate of the upstream pipeline table"
    );
    // The upstream table is a real catalog dataset; the sink it feeds is not.
    assert_eq!(
        sql_scalar_i64(&mut client, "SELECT count(*) FROM local.live.orders_bronze").await,
        5
    );
    let describe = client
        .execute_plan(Request::new(sc::ExecutePlanRequest {
            session_id: SESSION.to_string(),
            plan: Some(sc::Plan {
                op_type: Some(sc::plan::OpType::Root(sc::Relation {
                    rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                        query: "DESCRIBE TABLE local.live.revenue_sink".to_string(),
                        ..Default::default()
                    })),
                    ..Default::default()
                })),
            }),
            ..Default::default()
        }))
        .await;
    assert!(
        describe.is_err(),
        "a sink fed by a pipeline table must still stay out of the catalog"
    );
}

/// `CreateDataflowGraph` + a spool-fed streaming table + a sink flow that reads *it*.
///
/// The sink has no source of its own, which makes it a derived output: every pass recomputes the
/// aggregate and replaces the location.
async fn define_derived_sink_graph(
    client: &mut SparkConnectServiceClient<tonic::transport::Channel>,
    sink_name: &str,
    format: &str,
    sink_path: &Path,
) -> String {
    let spool = spool_dir().canonicalize().expect("spool");
    let graph_id = define_sink(
        client,
        sink_name,
        format,
        HashMap::from([("path".into(), sink_path.to_string_lossy().into_owned())]),
    )
    .await
    .expect("define sink");

    execute_pipeline(
        client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineSqlGraphElements(
                sc::pipeline_command::DefineSqlGraphElements {
                    dataflow_graph_id: Some(graph_id.clone()),
                    sql_text: Some(format!(
                        "CREATE STREAMING TABLE orders_bronze \
                           TBLPROPERTIES ('subscribe' = 'orders', \
                             'oxidant.spool.dir' = '{}', 'startingOffsets' = 'earliest') \
                           AS {};",
                        spool.display(),
                        bronze_flow_sql()
                    )),
                    sql_file_path: Some("sink_pipeline.sql".into()),
                },
            )),
        },
    )
    .await
    .expect("define bronze table");

    execute_pipeline(
        client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineFlow(
                sc::pipeline_command::DefineFlow {
                    dataflow_graph_id: Some(graph_id.clone()),
                    flow_name: Some(format!("flow_to_{sink_name}")),
                    target_dataset_name: Some(sink_name.into()),
                    details: Some(
                        sc::pipeline_command::define_flow::Details::RelationFlowDetails(
                            sc::pipeline_command::define_flow::WriteRelationFlowDetails {
                                relation: Some(sc::Relation {
                                    rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                                        query: "SELECT customer, sum(amount) AS revenue \
                                                FROM orders_bronze WHERE amount > 0 \
                                                GROUP BY customer"
                                            .into(),
                                        ..Default::default()
                                    })),
                                    ..Default::default()
                                }),
                            },
                        ),
                    ),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("define sink flow");
    graph_id
}

async fn start_run(
    client: &mut SparkConnectServiceClient<tonic::transport::Channel>,
    graph_id: &str,
    checkpoints: &Path,
    full_refresh: bool,
) -> Result<Vec<sc::ExecutePlanResponse>, Status> {
    execute_pipeline(
        client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::StartRun(
                sc::pipeline_command::StartRun {
                    dataflow_graph_id: Some(graph_id.to_string()),
                    dry: Some(false),
                    full_refresh_all: Some(full_refresh),
                    storage: Some(checkpoints.to_string_lossy().into_owned()),
                    ..Default::default()
                },
            )),
        },
    )
    .await
}

/// Changing only the sink's path must re-write, not report `unchanged`.
///
/// `write_path` is `#[serde(skip)]`, so it is invisible to `serde_json::to_string(table)`. If the
/// definition fingerprint is taken over the serialized form alone, a derived sink whose path moved
/// — same SQL, same storage, no new upstream rows — fingerprints identically, is short-circuited
/// as `unchanged`, and the new location stays empty while the run reports success.
#[tokio::test]
async fn a_derived_sink_whose_path_changes_is_rewritten_not_skipped() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_checkpoints");
    let first_path = warehouse.path().join("sink_a");
    let second_path = warehouse.path().join("sink_b");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints dir");

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;

    let graph_id =
        define_derived_sink_graph(&mut client, "revenue_sink", "delta", &first_path).await;
    start_run(&mut client, &graph_id, &checkpoints, true)
        .await
        .expect("first run");
    assert_eq!(
        delta_scalar_i64(&first_path, "sum(revenue)").await,
        725,
        "the first run must fill the original sink path"
    );

    // The user edits only `SinkDetails.options.path` and re-runs against the same storage. The
    // upstream stream has nothing new, so nothing is in `changed` — only the path moved.
    define_sink_on(
        &mut client,
        &graph_id,
        "revenue_sink",
        "delta",
        HashMap::from([("path".into(), second_path.to_string_lossy().into_owned())]),
    )
    .await
    .expect("redefine sink at a new path");

    let run = start_run(&mut client, &graph_id, &checkpoints, false)
        .await
        .expect("second run");
    let events = pipeline_event_messages(&run);
    assert!(
        second_path.join("_delta_log").is_dir(),
        "a sink whose path changed must be re-written to the new location, got events {events:?}"
    );
    assert_eq!(
        delta_scalar_i64(&second_path, "sum(revenue)").await,
        725,
        "the new sink path must hold the full recomputed result"
    );
}

/// A parquet sink fed only by pipeline tables is refused when the graph is lowered.
///
/// It would otherwise reach `LakeSink::replace_batch`, whose parquet arm is a hard refusal
/// phrased for catalog tables — the user would be told to "declare `/tmp/...` as delta".
#[tokio::test]
async fn a_derived_parquet_sink_is_refused_before_the_run() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_checkpoints");
    let sink_path = warehouse.path().join("derived_parquet_sink");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints dir");

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;

    let graph_id =
        define_derived_sink_graph(&mut client, "revenue_parquet_sink", "parquet", &sink_path).await;

    let err = start_run(&mut client, &graph_id, &checkpoints, true)
        .await
        .expect_err("a derived parquet sink cannot be replaced atomically");
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert!(
        err.message().contains("revenue_parquet_sink"),
        "the error must name the sink the user declared: {err}"
    );
    assert!(
        err.message().contains("own streaming source"),
        "the error must say what to do instead: {err}"
    );
    assert!(err.message().contains("delta"), "{err}");
    assert!(
        !sink_path.exists(),
        "nothing may be written when the sink is refused"
    );
}

/// A parquet sink with its own source is still allowed, and warns about the missing protocol.
#[tokio::test]
async fn a_parquet_sink_with_its_own_source_warns_but_runs() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_checkpoints");
    let sink_path = warehouse.path().join("sourced_parquet_sink");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints dir");

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;

    let mut options = spool_table_properties(&spool_dir().canonicalize().expect("spool"));
    options.insert("path".into(), sink_path.to_string_lossy().into_owned());
    let graph_id = define_sink(&mut client, "sourced_parquet_sink", "parquet", options)
        .await
        .expect("define parquet sink");

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineFlow(
                sc::pipeline_command::DefineFlow {
                    dataflow_graph_id: Some(graph_id.clone()),
                    flow_name: Some("flow_to_sourced_parquet_sink".into()),
                    target_dataset_name: Some("sourced_parquet_sink".into()),
                    details: Some(
                        sc::pipeline_command::define_flow::Details::RelationFlowDetails(
                            sc::pipeline_command::define_flow::WriteRelationFlowDetails {
                                relation: Some(sc::Relation {
                                    rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                                        query: bronze_flow_sql(),
                                        ..Default::default()
                                    })),
                                    ..Default::default()
                                }),
                            },
                        ),
                    ),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("define sink flow");

    let run = start_run(&mut client, &graph_id, &checkpoints, true)
        .await
        .expect("a sourced parquet sink is supported");
    let events = pipeline_event_messages(&run);
    assert_eq!(parquet_scalar_i64(&sink_path, "count(*)").await, 5);
    assert!(
        events.iter().any(|m| m.contains("no commit protocol")),
        "the operator must be told the sink has no commit protocol, got {events:?}"
    );
}

/// Two identical one-shot runs must not double-write — including from *different sessions*.
///
/// This is what pins the deterministic graph id and the default checkpoint root derived from it.
/// The id becomes the pipeline name, hence the Delta `appId`; the root holds the offsets. A fresh
/// UUID per call, or a root scoped by anything the caller varies between runs (a session id),
/// would make the second run a different writer starting from an empty checkpoint — and since the
/// sink's `write_path` is *not* caller-scoped, the spool would be replayed and appended into the
/// same location. No call passes `storage`, so the default root is what is under test. The third
/// run carries a different `session_id`, which is the arrangement a session-scoped root breaks.
#[tokio::test]
async fn two_identical_execute_output_flows_calls_do_not_double_write() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let sink_path = warehouse.path().join("repeat_sink");
    let spool = spool_dir().canonicalize().expect("spool");

    // Unique per test process: the default checkpoint root is derived from the output name, so a
    // fixed name would inherit a previous run's state from `$TMPDIR`.
    let output_name = format!(
        "repeat_sink_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    );

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;

    let mut options = spool_table_properties(&spool);
    options.insert("path".into(), sink_path.to_string_lossy().into_owned());

    let one_shot = sc::PipelineCommand {
        command_type: Some(sc::pipeline_command::CommandType::ExecuteOutputFlows(
            sc::pipeline_command::ExecuteOutputFlows {
                define_output: Some(sc::pipeline_command::DefineOutput {
                    output_name: Some(output_name.clone()),
                    output_type: Some(sc::OutputType::Sink as i32),
                    details: Some(sc::pipeline_command::define_output::Details::SinkDetails(
                        sc::pipeline_command::define_output::SinkDetails {
                            format: Some("delta".into()),
                            options,
                        },
                    )),
                    ..Default::default()
                }),
                define_flows: vec![sc::pipeline_command::DefineFlow {
                    flow_name: Some("flow_to_repeat_sink".into()),
                    target_dataset_name: Some(output_name.clone()),
                    details: Some(
                        sc::pipeline_command::define_flow::Details::RelationFlowDetails(
                            sc::pipeline_command::define_flow::WriteRelationFlowDetails {
                                relation: Some(sc::Relation {
                                    rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                                        query: bronze_flow_sql(),
                                        ..Default::default()
                                    })),
                                    ..Default::default()
                                }),
                            },
                        ),
                    ),
                    ..Default::default()
                }],
                // No `full_refresh`, no `storage`: the incremental path, on the default root.
                ..Default::default()
            },
        )),
    };

    execute_pipeline(&mut client, one_shot.clone())
        .await
        .expect("first one-shot run");
    assert_eq!(delta_scalar_i64(&sink_path, "count(*)").await, 5);

    execute_pipeline(&mut client, one_shot.clone())
        .await
        .expect("second one-shot run");
    assert_eq!(
        delta_scalar_i64(&sink_path, "count(*)").await,
        5,
        "a re-run must resume from the same checkpoint rather than replay the spool"
    );

    // Same output, same server, a session that has never seen it: still the same writer.
    execute_pipeline_in_session(&mut client, "sdp-phase4c-other-session", one_shot)
        .await
        .expect("one-shot run from a second session");
    assert_eq!(
        delta_scalar_i64(&sink_path, "count(*)").await,
        5,
        "the default checkpoint root must be stable across sessions — a session-scoped root \
         hands the new session an empty checkpoint and it replays the spool into the same sink"
    );
}

/// `ExecuteOutputFlows` must not silently accept an output type it cannot write.
#[tokio::test]
async fn execute_output_flows_rejects_output_types_it_cannot_write() {
    let port = pick_port();
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;

    let with_output_type = |output_type: Option<i32>| sc::PipelineCommand {
        command_type: Some(sc::pipeline_command::CommandType::ExecuteOutputFlows(
            sc::pipeline_command::ExecuteOutputFlows {
                define_output: Some(sc::pipeline_command::DefineOutput {
                    output_name: Some("one_shot".into()),
                    output_type,
                    ..Default::default()
                }),
                define_flows: vec![sc::pipeline_command::DefineFlow {
                    flow_name: Some("f".into()),
                    target_dataset_name: Some("one_shot".into()),
                    details: Some(
                        sc::pipeline_command::define_flow::Details::RelationFlowDetails(
                            sc::pipeline_command::define_flow::WriteRelationFlowDetails {
                                relation: Some(sc::Relation {
                                    rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                                        query: "SELECT 1 AS a".into(),
                                        ..Default::default()
                                    })),
                                    ..Default::default()
                                }),
                            },
                        ),
                    ),
                    ..Default::default()
                }],
                ..Default::default()
            },
        )),
    };

    // Unset is OUTPUT_TYPE_UNSPECIFIED, which lowering would silently treat as a table.
    let err = execute_pipeline(&mut client, with_output_type(None))
        .await
        .expect_err("an unset output_type must be refused");
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert!(err.message().contains("output_type"), "{err}");

    // A temporary view is registered and then skipped by the run: success, nothing written.
    let err = execute_pipeline(
        &mut client,
        with_output_type(Some(sc::OutputType::TemporaryView as i32)),
    )
    .await
    .expect_err("a temporary view writes nothing");
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert!(err.message().contains("TEMPORARY_VIEW"), "{err}");
}
