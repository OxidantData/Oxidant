//! Regression tests for PR #98 review findings (temp views, refresh drain, failure events).

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
use tonic::{Code, Request, Status};

const SESSION: &str = "sdp-review-fixes";

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
    create
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
        .expect("graph id")
}

async fn define_sql(
    client: &mut SparkConnectServiceClient<tonic::transport::Channel>,
    graph_id: &str,
    sql: &str,
) {
    execute_pipeline(
        client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineSqlGraphElements(
                sc::pipeline_command::DefineSqlGraphElements {
                    dataflow_graph_id: Some(graph_id.to_string()),
                    sql_file_path: Some("pipeline.sql".into()),
                    sql_text: Some(sql.to_string()),
                },
            )),
        },
    )
    .await
    .expect("define sql");
}

async fn start_run(
    client: &mut SparkConnectServiceClient<tonic::transport::Channel>,
    graph_id: &str,
    storage: &str,
) -> Vec<sc::ExecutePlanResponse> {
    execute_pipeline(
        client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::StartRun(
                sc::pipeline_command::StartRun {
                    dataflow_graph_id: Some(graph_id.to_string()),
                    dry: Some(false),
                    storage: Some(storage.to_string()),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("start run")
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
async fn start_run_materialized_view_reads_temporary_view() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_checkpoints");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints dir");

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;
    let graph_id = create_graph(&mut client).await;

    define_sql(
        &mut client,
        &graph_id,
        "CREATE TEMPORARY VIEW tv AS SELECT 1 AS id; \
         CREATE MATERIALIZED VIEW mv AS SELECT * FROM tv",
    )
    .await;

    let run = start_run(&mut client, &graph_id, &checkpoints.to_string_lossy()).await;
    let events = pipeline_event_messages(&run);
    assert!(
        events.iter().any(|m| m.contains("mv")),
        "expected MV update event, got {events:?}"
    );

    let count = sql_scalar_i64(&mut client, "SELECT count(*) FROM local.live.mv").await;
    assert_eq!(count, 1, "MV must see rows from the temporary view");
}

#[tokio::test]
async fn graph_refresh_requests_apply_to_single_start_run() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_checkpoints");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints dir");
    let storage = checkpoints.to_string_lossy().to_string();

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;
    let graph_id = create_graph(&mut client).await;

    define_sql(
        &mut client,
        &graph_id,
        "CREATE MATERIALIZED VIEW mv_a AS SELECT 1 AS x; \
         CREATE MATERIALIZED VIEW mv_b AS SELECT 2 AS y; \
         REFRESH MATERIALIZED VIEW mv_a",
    )
    .await;

    let first = start_run(&mut client, &graph_id, &storage).await;
    let first_events = pipeline_event_messages(&first);
    assert!(
        first_events.iter().any(|m| m.contains("mv_a")),
        "refresh run should update mv_a, got {first_events:?}"
    );
    assert!(
        !first_events.iter().any(|m| m.contains("mv_b")),
        "refresh run should not update mv_b, got {first_events:?}"
    );

    let second = start_run(&mut client, &graph_id, &storage).await;
    let second_events = pipeline_event_messages(&second);
    assert!(
        second_events.iter().any(|m| m.contains("mv_a")),
        "full second run should include mv_a, got {second_events:?}"
    );
    assert!(
        second_events.iter().any(|m| m.contains("mv_b")),
        "full second run should include mv_b after refresh was drained, got {second_events:?}"
    );
}

#[tokio::test]
async fn failed_start_run_streams_table_failed_before_error_status() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_checkpoints");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints dir");

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;
    let graph_id = create_graph(&mut client).await;

    define_sql(
        &mut client,
        &graph_id,
        "CREATE MATERIALIZED VIEW bad AS SELECT * FROM definitely_missing_table_xyz",
    )
    .await;

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
            .any(|m| m.contains("bad") && m.contains("FAILED")),
        "client must receive TableFailed event before error status, got {events:?}"
    );
    assert_ne!(
        status.code(),
        Code::Ok,
        "failed pipeline run must end with error status"
    );
    assert!(
        !responses.iter().any(|r| matches!(
            r.response_type,
            Some(sc::execute_plan_response::ResponseType::ResultComplete(_))
        )),
        "failed run must not emit ResultComplete before the error status"
    );
}
