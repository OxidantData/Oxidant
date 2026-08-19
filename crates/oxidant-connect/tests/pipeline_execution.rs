//! SDP Phase 2 acceptance: `DefineSqlGraphElements` + non-dry `StartRun` over a Kafka spool.

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
use tonic::{Request, Status};

const SESSION: &str = "sdp-execution";

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

fn pipeline_sql(spool: &Path) -> String {
    let spool = spool.canonicalize().expect("spool path");
    format!(
        "CREATE STREAMING TABLE orders_bronze \
         TBLPROPERTIES ('subscribe' = 'orders', 'oxidant.spool.dir' = '{}', 'startingOffsets' = 'earliest') \
         USING DELTA \
         AS SELECT \
           CAST(get_json_object(CAST(value AS STRING), '$.order_id') AS BIGINT) AS order_id, \
           get_json_object(CAST(value AS STRING), '$.customer') AS customer, \
           CAST(get_json_object(CAST(value AS STRING), '$.amount') AS BIGINT) AS amount \
         FROM stream; \
         CREATE MATERIALIZED VIEW revenue_gold AS \
         SELECT customer, sum(amount) AS revenue, count(*) AS orders \
         FROM orders_bronze WHERE amount > 0 GROUP BY customer",
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

#[tokio::test]
async fn sdp_sql_graph_start_run_streams_events_and_materializes() {
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
            command_type: Some(sc::pipeline_command::CommandType::DefineSqlGraphElements(
                sc::pipeline_command::DefineSqlGraphElements {
                    dataflow_graph_id: Some(graph_id.clone()),
                    sql_file_path: Some("pipeline.sql".into()),
                    sql_text: Some(pipeline_sql(&spool_dir())),
                },
            )),
        },
    )
    .await
    .expect("define sql");

    let run = execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::StartRun(
                sc::pipeline_command::StartRun {
                    dataflow_graph_id: Some(graph_id.clone()),
                    full_refresh_selection: vec![],
                    full_refresh_all: None,
                    refresh_selection: vec![],
                    dry: Some(false),
                    storage: Some(checkpoints.to_string_lossy().into_owned()),
                },
            )),
        },
    )
    .await
    .expect("start run");

    let events = pipeline_event_messages(&run);
    assert!(
        events.iter().any(|m| m.contains("pipeline `")),
        "expected PipelineStarted-style event, got {events:?}"
    );
    assert!(
        events.iter().any(|m| m.contains("orders_bronze")),
        "expected streaming table event, got {events:?}"
    );
    assert!(
        events.iter().any(|m| m.contains("revenue_gold")),
        "expected MV event, got {events:?}"
    );
    assert!(
        events.iter().any(|m| m.contains("pass complete")),
        "expected PassComplete event, got {events:?}"
    );
    assert!(
        run.iter().any(|r| matches!(
            r.response_type,
            Some(sc::execute_plan_response::ResponseType::ResultComplete(_))
        )),
        "non-dry StartRun must end with ResultComplete"
    );

    let dry = execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::StartRun(
                sc::pipeline_command::StartRun {
                    dataflow_graph_id: Some(graph_id.clone()),
                    dry: Some(true),
                    storage: Some(checkpoints.to_string_lossy().into_owned()),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("dry run");
    let dry_msg = pipeline_event_messages(&dry)
        .into_iter()
        .find(|m| m.contains("is valid"))
        .expect("dry validation event");
    assert!(dry_msg.contains("revenue_gold") || dry_msg.contains("2 table"));

    let refresh = execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::StartRun(
                sc::pipeline_command::StartRun {
                    dataflow_graph_id: Some(graph_id),
                    full_refresh_all: Some(true),
                    dry: Some(false),
                    storage: Some(checkpoints.to_string_lossy().into_owned()),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("full refresh run");
    assert!(
        pipeline_event_messages(&refresh)
            .iter()
            .any(|m| m.contains("revenue_gold")),
        "full refresh must recompute the MV"
    );

    // ada: 100 + 300, bob: 250, cy: 75 → total revenue 725
    let total = sql_scalar_i64(
        &mut client,
        "SELECT sum(revenue) FROM local.live.revenue_gold",
    )
    .await;
    assert_eq!(total, 725, "MV contents must match spool-derived revenue");
}
