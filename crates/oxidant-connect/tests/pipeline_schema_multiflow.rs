//! SDP Phase 3 track B: schema enforcement and multi-flow append.

use std::collections::HashMap;
use std::net::TcpListener;
use std::path::Path;
use std::time::Duration;

use oxidant_connect::{serve, ServerConfig};
use oxidant_loom::arrow::array::Int64Array;
use oxidant_loom::arrow::ipc::reader::StreamReader;
use oxidant_proto::spark::connect as sc;
use sc::spark_connect_service_client::SparkConnectServiceClient;
use tokio_stream::StreamExt;
use tonic::{Request, Status};

const SESSION: &str = "sdp-p3b";

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

async fn create_graph(client: &mut SparkConnectServiceClient<tonic::transport::Channel>) -> String {
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
    graph_id_from_create(&create)
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

#[tokio::test]
async fn schema_mismatch_fails_with_clear_event() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_ckpt");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints");

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;
    let graph_id = create_graph(&mut client).await;

    let sql = "\
CREATE MATERIALIZED VIEW typed_mv (id BIGINT) AS SELECT 1 AS id;
CREATE FLOW widen AS INSERT INTO typed_mv SELECT 'not-a-number' AS id";
    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineSqlGraphElements(
                sc::pipeline_command::DefineSqlGraphElements {
                    dataflow_graph_id: Some(graph_id.clone()),
                    sql_file_path: None,
                    sql_text: Some(sql.into()),
                },
            )),
        },
    )
    .await
    .expect("define sql");

    let (responses, status) = execute_pipeline_expect_error(
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
    .await;
    let events = pipeline_event_messages(&responses);
    assert!(
        events
            .iter()
            .any(|m| m.contains("FAILED") && m.contains("typed_mv")),
        "expected TableFailed event for typed_mv, got {events:?}; status={status}"
    );
    assert!(
        events
            .iter()
            .any(|m| { m.contains("Cast") || m.contains("Int64") || m.contains("not-a-number") }),
        "failure should mention the incompatible cast, got {events:?}"
    );
}

#[tokio::test]
async fn unsupported_tblproperty_is_refused_at_start_run() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_ckpt");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints");

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;
    let graph_id = create_graph(&mut client).await;

    let sql = "CREATE MATERIALIZED VIEW bad_props \
        TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true') \
        AS SELECT 1 AS id";
    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineSqlGraphElements(
                sc::pipeline_command::DefineSqlGraphElements {
                    dataflow_graph_id: Some(graph_id.clone()),
                    sql_file_path: None,
                    sql_text: Some(sql.into()),
                },
            )),
        },
    )
    .await
    .expect("define sql");

    let err = execute_pipeline(
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
    .expect_err("unsupported property");
    assert!(
        err.message().contains("delta.autoOptimize.optimizeWrite"),
        "must name the property: {}",
        err.message()
    );
}

#[tokio::test]
async fn multi_flow_mv_unions_both_sources() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_ckpt");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints");

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;
    let graph_id = create_graph(&mut client).await;

    let sql = "\
CREATE MATERIALIZED VIEW union_mv (n BIGINT) AS SELECT 1 AS n;
CREATE FLOW extra AS INSERT INTO union_mv SELECT 2 AS n";
    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineSqlGraphElements(
                sc::pipeline_command::DefineSqlGraphElements {
                    dataflow_graph_id: Some(graph_id.clone()),
                    sql_file_path: None,
                    sql_text: Some(sql.into()),
                },
            )),
        },
    )
    .await
    .expect("define sql");

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

    let total = sql_scalar_i64(&mut client, "SELECT sum(n) FROM local.live.union_mv").await;
    assert_eq!(total, 3, "both flows should land in the MV");
}

#[tokio::test]
async fn by_name_flow_maps_columns_by_name() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_ckpt");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints");

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;
    let graph_id = create_graph(&mut client).await;

    let sql = "\
CREATE MATERIALIZED VIEW by_name_mv (a BIGINT, b BIGINT) AS SELECT 1 AS a, 10 AS b;
CREATE FLOW swapped AS INSERT INTO by_name_mv BY NAME SELECT 20 AS b, 2 AS a";
    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineSqlGraphElements(
                sc::pipeline_command::DefineSqlGraphElements {
                    dataflow_graph_id: Some(graph_id.clone()),
                    sql_file_path: None,
                    sql_text: Some(sql.into()),
                },
            )),
        },
    )
    .await
    .expect("define sql");

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

    let sum_a = sql_scalar_i64(&mut client, "SELECT sum(a) FROM local.live.by_name_mv").await;
    let sum_b = sql_scalar_i64(&mut client, "SELECT sum(b) FROM local.live.by_name_mv").await;
    assert_eq!(sum_a, 3, "column a should be 1 and 2");
    assert_eq!(sum_b, 30, "column b should be 10 and 20");
}
