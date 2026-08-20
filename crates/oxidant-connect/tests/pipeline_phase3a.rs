//! SDP Phase 3 track A: sql_conf, source-code locations, dry-run purity.

use std::collections::HashMap;
use std::net::TcpListener;
use std::path::Path;
use std::time::Duration;

use oxidant_connect::{serve, ServerConfig};
use oxidant_proto::spark::connect as sc;
use sc::spark_connect_service_client::SparkConnectServiceClient;
use tokio_stream::StreamExt;
use tonic::{Code, Request, Status};

const SESSION: &str = "sdp-phase3a";

fn pick_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local_addr")
        .port()
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

async fn boot_default(port: u16) -> SparkConnectServiceClient<tonic::transport::Channel> {
    boot(port, HashMap::new()).await
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

async fn create_graph(
    client: &mut SparkConnectServiceClient<tonic::transport::Channel>,
    sql_conf: HashMap<String, String>,
) -> String {
    create_graph_with_defaults(
        client,
        sql_conf,
        Some("spark_catalog".into()),
        Some("default".into()),
    )
    .await
}

async fn create_graph_with_defaults(
    client: &mut SparkConnectServiceClient<tonic::transport::Channel>,
    sql_conf: HashMap<String, String>,
    default_catalog: Option<String>,
    default_database: Option<String>,
) -> String {
    let create = execute_pipeline(
        client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::CreateDataflowGraph(
                sc::pipeline_command::CreateDataflowGraph {
                    default_catalog,
                    default_database,
                    sql_conf,
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

#[tokio::test]
async fn graph_sql_conf_applies_known_key_and_ignores_unknown() {
    let port = pick_port();
    let mut client = boot_default(port).await;

    let graph_id = create_graph(
        &mut client,
        HashMap::from([
            (
                "spark.sql.session.timeZone".to_string(),
                "Asia/Tokyo".to_string(),
            ),
            (
                "spark.sql.not.a.real.oxidant.key".to_string(),
                "nope".to_string(),
            ),
        ]),
    )
    .await;

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineOutput(
                sc::pipeline_command::DefineOutput {
                    dataflow_graph_id: Some(graph_id.clone()),
                    output_name: Some("metrics".into()),
                    output_type: Some(sc::OutputType::Table as i32),
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

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineFlow(
                sc::pipeline_command::DefineFlow {
                    dataflow_graph_id: Some(graph_id.clone()),
                    flow_name: Some("fill_metrics".into()),
                    target_dataset_name: Some("metrics".into()),
                    sql_conf: Default::default(),
                    client_id: None,
                    source_code_location: None,
                    details: Some(
                        sc::pipeline_command::define_flow::Details::RelationFlowDetails(
                            sc::pipeline_command::define_flow::WriteRelationFlowDetails {
                                relation: Some(sc::Relation {
                                    rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                                        query: "SELECT 1 AS id".into(),
                                        ..Default::default()
                                    })),
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
    .expect("define flow");

    let dry = execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::StartRun(
                sc::pipeline_command::StartRun {
                    dataflow_graph_id: Some(graph_id),
                    dry: Some(true),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("dry start run");

    let events = pipeline_event_messages(&dry);
    assert!(
        events
            .iter()
            .any(|m| m.contains("ignored sql_conf key `spark.sql.not.a.real.oxidant.key`")),
        "unknown sql_conf should emit a PipelineEvent, got {events:?}"
    );
    assert!(
        events.iter().any(|m| m.contains("is valid")),
        "dry run should succeed with known sql_conf applied for validation"
    );
}

#[tokio::test]
async fn dry_start_run_does_not_register_temporary_views() {
    let port = pick_port();
    let mut client = boot_default(port).await;
    let graph_id = create_graph(&mut client, HashMap::new()).await;

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineSqlGraphElements(
                sc::pipeline_command::DefineSqlGraphElements {
                    dataflow_graph_id: Some(graph_id.clone()),
                    sql_file_path: Some("pipeline.sql".into()),
                    sql_text: Some(
                        "CREATE TEMPORARY VIEW tv AS SELECT 42 AS n; \
                         CREATE MATERIALIZED VIEW mv AS SELECT * FROM tv"
                            .into(),
                    ),
                },
            )),
        },
    )
    .await
    .expect("define sql");

    let dry = execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::StartRun(
                sc::pipeline_command::StartRun {
                    dataflow_graph_id: Some(graph_id),
                    dry: Some(true),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("dry start run");
    assert!(
        pipeline_event_messages(&dry)
            .iter()
            .any(|m| m.contains("is valid")),
        "dry run should validate successfully"
    );

    let err = client
        .execute_plan(Request::new(sc::ExecutePlanRequest {
            session_id: SESSION.to_string(),
            plan: Some(sc::Plan {
                op_type: Some(sc::plan::OpType::Root(sc::Relation {
                    rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                        query: "SELECT * FROM tv".into(),
                        ..Default::default()
                    })),
                    ..Default::default()
                })),
            }),
            ..Default::default()
        }))
        .await
        .expect_err("temp view must not exist after dry run");
    assert_ne!(err.code(), Code::Ok);
}

#[tokio::test]
async fn failed_flow_error_event_includes_source_code_location() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_checkpoints");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints dir");

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;
    let graph_id = create_graph(&mut client, HashMap::new()).await;

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineOutput(
                sc::pipeline_command::DefineOutput {
                    dataflow_graph_id: Some(graph_id.clone()),
                    output_name: Some("bad".into()),
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

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineFlow(
                sc::pipeline_command::DefineFlow {
                    dataflow_graph_id: Some(graph_id.clone()),
                    flow_name: Some("to_bad".into()),
                    target_dataset_name: Some("bad".into()),
                    sql_conf: Default::default(),
                    client_id: None,
                    source_code_location: Some(sc::SourceCodeLocation {
                        file_name: Some("pipeline.sql".into()),
                        line_number: Some(17),
                        definition_path: None,
                        extension: vec![],
                    }),
                    details: Some(
                        sc::pipeline_command::define_flow::Details::RelationFlowDetails(
                            sc::pipeline_command::define_flow::WriteRelationFlowDetails {
                                relation: Some(sc::Relation {
                                    rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                                        query: "SELECT * FROM definitely_missing_table_xyz".into(),
                                        ..Default::default()
                                    })),
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
    .expect("define flow");

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
        events.iter().any(|m| {
            m.contains("bad") && m.contains("FAILED") && m.contains("pipeline.sql:17")
        }),
        "failure event must echo SourceCodeLocation file:line, got {events:?}"
    );
    assert!(
        status.message().contains("pipeline.sql:17"),
        "terminal error should include source location, got {}",
        status.message()
    );
}

async fn config_get(
    client: &mut SparkConnectServiceClient<tonic::transport::Channel>,
    keys: &[&str],
) -> HashMap<String, Option<String>> {
    let resp = client
        .config(Request::new(sc::ConfigRequest {
            session_id: SESSION.to_string(),
            operation: Some(sc::config_request::Operation {
                op_type: Some(sc::config_request::operation::OpType::Get(
                    sc::config_request::Get {
                        keys: keys.iter().map(|k| (*k).to_string()).collect(),
                    },
                )),
            }),
            ..Default::default()
        }))
        .await
        .expect("config get")
        .into_inner();
    keys.iter()
        .map(|key| {
            let value = resp
                .pairs
                .iter()
                .find(|p| p.key == *key)
                .and_then(|p| p.value.clone());
            ((*key).to_string(), value)
        })
        .collect()
}

async fn sql_query(
    client: &mut SparkConnectServiceClient<tonic::transport::Channel>,
    query: &str,
) -> Result<(), Status> {
    let mut stream = client
        .execute_plan(Request::new(sc::ExecutePlanRequest {
            session_id: SESSION.to_string(),
            plan: Some(sc::Plan {
                op_type: Some(sc::plan::OpType::Root(sc::Relation {
                    rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                        query: query.to_string(),
                        ..Default::default()
                    })),
                    ..Default::default()
                })),
            }),
            ..Default::default()
        }))
        .await?
        .into_inner();
    while let Some(item) = stream.next().await {
        item?;
    }
    Ok(())
}

#[tokio::test]
async fn dry_start_run_skips_catalog_sync_and_restores_session_conf() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let port = pick_port();
    let mut client = boot_default(port).await;

    let graph_id = create_graph(
        &mut client,
        HashMap::from([
            (
                "spark.sql.session.timeZone".to_string(),
                "Asia/Tokyo".to_string(),
            ),
            (
                "spark.sql.catalog.dryonly.type".to_string(),
                "local".to_string(),
            ),
            (
                "spark.sql.catalog.dryonly.warehouse".to_string(),
                warehouse.path().to_string_lossy().to_string(),
            ),
            (
                "spark.sql.defaultCatalog".to_string(),
                "dryonly".to_string(),
            ),
        ]),
    )
    .await;

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineOutput(
                sc::pipeline_command::DefineOutput {
                    dataflow_graph_id: Some(graph_id.clone()),
                    output_name: Some("metrics".into()),
                    output_type: Some(sc::OutputType::Table as i32),
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

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineFlow(
                sc::pipeline_command::DefineFlow {
                    dataflow_graph_id: Some(graph_id.clone()),
                    flow_name: Some("fill_metrics".into()),
                    target_dataset_name: Some("metrics".into()),
                    sql_conf: Default::default(),
                    client_id: None,
                    source_code_location: None,
                    details: Some(
                        sc::pipeline_command::define_flow::Details::RelationFlowDetails(
                            sc::pipeline_command::define_flow::WriteRelationFlowDetails {
                                relation: Some(sc::Relation {
                                    rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                                        query: "SELECT 1 AS id".into(),
                                        ..Default::default()
                                    })),
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
    .expect("define flow");

    let dry = execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::StartRun(
                sc::pipeline_command::StartRun {
                    dataflow_graph_id: Some(graph_id),
                    dry: Some(true),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("dry start run");
    assert!(
        pipeline_event_messages(&dry)
            .iter()
            .any(|m| m.contains("is valid")),
        "dry run should validate successfully"
    );

    let conf = config_get(&mut client, &["spark.sql.session.timeZone"]).await;
    assert_eq!(
        conf.get("spark.sql.session.timeZone")
            .and_then(|v| v.as_deref()),
        Some("UTC"),
        "graph sql_conf must not leak into the session after a dry run"
    );

    let err = sql_query(&mut client, "SELECT * FROM dryonly.default.metrics")
        .await
        .expect_err("dry-only catalog must not be registered");
    assert_ne!(err.code(), Code::Ok);
}

#[tokio::test]
async fn start_run_restores_sql_conf_when_checkpoint_clear_fails() {
    let port = pick_port();
    let mut client = boot_default(port).await;
    let graph_id = create_graph(
        &mut client,
        HashMap::from([(
            "spark.sql.session.timeZone".to_string(),
            "Europe/Berlin".to_string(),
        )]),
    )
    .await;

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineOutput(
                sc::pipeline_command::DefineOutput {
                    dataflow_graph_id: Some(graph_id.clone()),
                    output_name: Some("metrics".into()),
                    output_type: Some(sc::OutputType::Table as i32),
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

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineFlow(
                sc::pipeline_command::DefineFlow {
                    dataflow_graph_id: Some(graph_id.clone()),
                    flow_name: Some("fill_metrics".into()),
                    target_dataset_name: Some("metrics".into()),
                    sql_conf: Default::default(),
                    client_id: None,
                    source_code_location: None,
                    details: Some(
                        sc::pipeline_command::define_flow::Details::RelationFlowDetails(
                            sc::pipeline_command::define_flow::WriteRelationFlowDetails {
                                relation: Some(sc::Relation {
                                    rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                                        query: "SELECT 1 AS id".into(),
                                        ..Default::default()
                                    })),
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
    .expect("define flow");

    let storage_file = tempfile::NamedTempFile::new().expect("storage file");
    let run = client
        .execute_plan(Request::new(sc::ExecutePlanRequest {
            session_id: SESSION.to_string(),
            plan: Some(pipeline_plan(sc::PipelineCommand {
                command_type: Some(sc::pipeline_command::CommandType::StartRun(
                    sc::pipeline_command::StartRun {
                        dataflow_graph_id: Some(graph_id),
                        dry: Some(false),
                        storage: Some(storage_file.path().to_string_lossy().into_owned()),
                        full_refresh_all: Some(true),
                        ..Default::default()
                    },
                )),
            })),
            ..Default::default()
        }))
        .await;
    match run {
        Err(status) => assert_ne!(status.code(), Code::Ok, "checkpoint clear should fail"),
        Ok(response) => {
            let mut stream = response.into_inner();
            let mut saw_error = false;
            while let Some(item) = stream.next().await {
                if item.is_err() {
                    saw_error = true;
                }
            }
            assert!(
                saw_error,
                "checkpoint clear failure should surface as stream error"
            );
        }
    }

    let conf = config_get(&mut client, &["spark.sql.session.timeZone"]).await;
    assert_eq!(
        conf.get("spark.sql.session.timeZone")
            .and_then(|v| v.as_deref()),
        Some("UTC"),
        "graph sql_conf must be restored when checkpoint clearing fails, got {conf:?}"
    );
}

#[tokio::test]
async fn relation_flow_reads_temporary_view_at_start_run() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let checkpoints = warehouse.path().join("_checkpoints");
    std::fs::create_dir_all(&checkpoints).expect("checkpoints dir");

    let port = pick_port();
    let mut client = boot(port, local_catalog_conf(warehouse.path())).await;
    let graph_id = create_graph_with_defaults(
        &mut client,
        HashMap::new(),
        Some("local".into()),
        Some("live".into()),
    )
    .await;

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineSqlGraphElements(
                sc::pipeline_command::DefineSqlGraphElements {
                    dataflow_graph_id: Some(graph_id.clone()),
                    sql_file_path: Some("pipeline.sql".into()),
                    sql_text: Some("CREATE TEMPORARY VIEW tv AS SELECT 42 AS n".into()),
                },
            )),
        },
    )
    .await
    .expect("define temp view via sql");

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineOutput(
                sc::pipeline_command::DefineOutput {
                    dataflow_graph_id: Some(graph_id.clone()),
                    output_name: Some("mv".into()),
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
    .expect("define mv output");

    execute_pipeline(
        &mut client,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineFlow(
                sc::pipeline_command::DefineFlow {
                    dataflow_graph_id: Some(graph_id.clone()),
                    flow_name: Some("fill_mv".into()),
                    target_dataset_name: Some("mv".into()),
                    sql_conf: Default::default(),
                    client_id: None,
                    source_code_location: None,
                    details: Some(
                        sc::pipeline_command::define_flow::Details::RelationFlowDetails(
                            sc::pipeline_command::define_flow::WriteRelationFlowDetails {
                                relation: Some(sc::Relation {
                                    rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                                        query: "SELECT n FROM tv".into(),
                                        ..Default::default()
                                    })),
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
    .expect("define mv flow");

    let run = execute_pipeline(
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
    .expect("start run with relation flow over temp view");
    assert!(
        pipeline_event_messages(&run)
            .iter()
            .any(|m| m.contains("mv")),
        "MV should update when a relation flow reads a temp view, got {:?}",
        pipeline_event_messages(&run)
    );
}
