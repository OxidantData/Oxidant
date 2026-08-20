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

    execute_pipeline(
        client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineOutput(
                sc::pipeline_command::DefineOutput {
                    dataflow_graph_id: Some(graph_id.clone()),
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
    Ok(graph_id)
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
