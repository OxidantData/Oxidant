//! AUTO CDC (SCD Type 1) integration over SDP SQL + StartRun.

use std::collections::HashMap;
use std::net::TcpListener;
use std::path::Path;
use std::time::Duration;

use oxidant_connect::{serve, ServerConfig};
use oxidant_loom::arrow::array::{Array, Int64Array, StringArray};
use oxidant_loom::arrow::ipc::reader::StreamReader;
use oxidant_proto::spark::connect as sc;
use sc::spark_connect_service_client::SparkConnectServiceClient;
use tokio_stream::StreamExt;
use tonic::{Request, Status};

const SESSION: &str = "sdp-auto-cdc";

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

fn write_spool(dir: &Path, batches: &[&str]) {
    std::fs::create_dir_all(dir).expect("spool dir");
    for (i, body) in batches.iter().enumerate() {
        std::fs::write(dir.join(format!("batch-{i}.json")), body).expect("batch file");
    }
}

fn cdc_pipeline_sql(spool: &Path) -> String {
    format!(
        "CREATE STREAMING TABLE cdc_source \
         TBLPROPERTIES ('subscribe' = 'cdc', 'oxidant.spool.dir' = '{}', 'startingOffsets' = 'earliest') \
         USING DELTA AS \
         SELECT \
           CAST(get_json_object(CAST(value AS STRING), '$.id') AS BIGINT) AS id, \
           get_json_object(CAST(value AS STRING), '$.name') AS name, \
           CAST(get_json_object(CAST(value AS STRING), '$.seq') AS BIGINT) AS seq, \
           get_json_object(CAST(value AS STRING), '$.op') AS op \
         FROM stream; \
         CREATE STREAMING TABLE cdc_target; \
         CREATE FLOW cdc_flow AS AUTO CDC INTO cdc_target FROM cdc_source \
         KEYS (id) APPLY AS DELETE WHEN op = 'D' SEQUENCE BY seq \
         COLUMNS * EXCEPT (op) IGNORE NULL UPDATES ON (name) STORED AS SCD TYPE 1",
        spool.display()
    )
}

async fn sql_rows(
    client: &mut SparkConnectServiceClient<tonic::transport::Channel>,
    sql: &str,
) -> Vec<(i64, Option<String>, i64)> {
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

    let mut rows = Vec::new();
    while let Some(msg) = stream.next().await {
        let msg = msg.expect("response");
        if let Some(sc::execute_plan_response::ResponseType::ArrowBatch(batch)) = msg.response_type
        {
            let reader =
                StreamReader::try_new(std::io::Cursor::new(batch.data), None).expect("ipc");
            for rb in reader {
                let rb = rb.expect("batch");
                let ids = rb
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id");
                let names = rb
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("name");
                let seqs = rb
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("seq");
                for i in 0..rb.num_rows() {
                    rows.push((
                        ids.value(i),
                        if names.is_null(i) {
                            None
                        } else {
                            Some(names.value(i).to_string())
                        },
                        seqs.value(i),
                    ));
                }
            }
        }
    }
    rows
}

#[tokio::test]
async fn sdp_auto_cdc_flow_merges_scd1_target() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_checkpoints");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints dir");
    let spool = warehouse.path().join("spool");
    write_spool(
        &spool,
        &[
            "{\"id\":1,\"name\":\"alice\",\"seq\":1,\"op\":\"I\"}\n{\"id\":2,\"name\":\"bob\",\"seq\":1,\"op\":\"I\"}",
            "{\"id\":1,\"name\":\"stale\",\"seq\":0,\"op\":\"U\"}\n{\"id\":1,\"name\":\"alice_new\",\"seq\":2,\"op\":\"U\"}\n{\"id\":2,\"name\":null,\"seq\":2,\"op\":\"U\"}\n{\"id\":3,\"name\":\"cy\",\"seq\":1,\"op\":\"I\"}",
            "{\"id\":2,\"seq\":3,\"op\":\"D\"}",
        ],
    );

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
                    sql_file_path: Some("cdc.sql".into()),
                    sql_text: Some(cdc_pipeline_sql(&spool)),
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

    let rows = sql_rows(
        &mut client,
        "SELECT id, name, seq FROM local.live.cdc_target ORDER BY id",
    )
    .await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, Some("alice_new".into()), 2));
    assert_eq!(rows[1], (3, Some("cy".into()), 1));
}

#[tokio::test]
async fn sdp_auto_cdc_rejects_a_target_with_its_own_stream() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let spool = warehouse.path().join("spool");
    write_spool(
        &spool,
        &["{\"id\":1,\"name\":\"a\",\"seq\":1,\"op\":\"I\"}"],
    );

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

    let sql = format!(
        "CREATE STREAMING TABLE cdc_target \
         TBLPROPERTIES ('subscribe' = 'cdc', 'oxidant.spool.dir' = '{}') \
         USING DELTA AS SELECT 1 AS id FROM stream; \
         CREATE STREAMING TABLE cdc_source \
         TBLPROPERTIES ('subscribe' = 'cdc', 'oxidant.spool.dir' = '{}') \
         USING DELTA AS SELECT CAST(get_json_object(CAST(value AS STRING), '$.id') AS BIGINT) AS id, \
           CAST(get_json_object(CAST(value AS STRING), '$.seq') AS BIGINT) AS seq, \
           get_json_object(CAST(value AS STRING), '$.op') AS op FROM stream; \
         CREATE FLOW bad AS AUTO CDC INTO cdc_target FROM cdc_source KEYS (id) SEQUENCE BY seq",
        spool.display(),
        spool.display()
    );

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineSqlGraphElements(
                sc::pipeline_command::DefineSqlGraphElements {
                    dataflow_graph_id: Some(graph_id.clone()),
                    sql_text: Some(sql),
                    ..Default::default()
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
                    dry: Some(true),
                    storage: Some(
                        warehouse
                            .path()
                            .join("_ckpt")
                            .to_string_lossy()
                            .into_owned(),
                    ),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect_err("dry run must fail");
    assert!(
        err.message().contains("cannot target streaming table"),
        "{err}"
    );
}
