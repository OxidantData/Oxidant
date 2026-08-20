//! Spark Declarative Pipelines (`PipelineCommand`) integration tests.

use std::net::TcpListener;
use std::time::Duration;

use oxidant_connect::{serve, ServerConfig};
use oxidant_proto::spark::connect as sc;
use sc::spark_connect_service_client::SparkConnectServiceClient;
use tokio_stream::StreamExt;
use tonic::{Code, Request, Status};

fn pick_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local_addr")
        .port()
}

async fn boot(port: u16) -> SparkConnectServiceClient<tonic::transport::Channel> {
    tokio::spawn(async move {
        let _ = serve(ServerConfig {
            port,
            ui_port: None,
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

async fn expect_status(
    client: &mut SparkConnectServiceClient<tonic::transport::Channel>,
    session_id: &str,
    cmd: sc::PipelineCommand,
    code: Code,
) {
    let err = execute_pipeline(client, session_id, cmd)
        .await
        .expect_err("expected pipeline command to fail");
    assert_eq!(err.code(), code, "unexpected error: {err}");
}

#[tokio::test]
async fn pipeline_create_define_dry_run_and_drop() {
    let port = pick_port();
    let mut client = boot(port).await;
    let session = "sdp-test";

    let create_resps = execute_pipeline(
        &mut client,
        session,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::CreateDataflowGraph(
                sc::pipeline_command::CreateDataflowGraph {
                    default_catalog: Some("spark_catalog".into()),
                    default_database: Some("default".into()),
                    sql_conf: Default::default(),
                },
            )),
        },
    )
    .await
    .expect("CreateDataflowGraph");

    let graph_id = create_resps
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
        .expect("CreateDataflowGraphResult");
    assert!(
        create_resps.iter().any(|r| matches!(
            r.response_type,
            Some(sc::execute_plan_response::ResponseType::ResultComplete(_))
        )),
        "CreateDataflowGraph must end with ResultComplete"
    );

    let define_out = execute_pipeline(
        &mut client,
        session,
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
    .expect("DefineOutput");
    assert!(define_out.iter().any(|r| matches!(
        &r.response_type,
        Some(sc::execute_plan_response::ResponseType::PipelineCommandResult(_))
    )));

    let define_flow = execute_pipeline(
        &mut client,
        session,
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
    .expect("DefineFlow");
    assert!(define_flow.iter().any(|r| matches!(
        &r.response_type,
        Some(sc::execute_plan_response::ResponseType::PipelineCommandResult(_))
    )));

    let dry_run = execute_pipeline(
        &mut client,
        session,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::StartRun(
                sc::pipeline_command::StartRun {
                    dataflow_graph_id: Some(graph_id.clone()),
                    full_refresh_selection: vec![],
                    full_refresh_all: None,
                    refresh_selection: vec![],
                    dry: Some(true),
                    storage: None,
                },
            )),
        },
    )
    .await
    .expect("StartRun dry");
    let event = dry_run
        .iter()
        .find_map(|r| match &r.response_type {
            Some(sc::execute_plan_response::ResponseType::PipelineEventResult(ev)) => {
                ev.event.as_ref()?.message.clone()
            }
            _ => None,
        })
        .expect("PipelineEventResult");
    assert!(
        event.contains("is valid"),
        "dry run summary should mention validation: {event}"
    );
    assert!(dry_run.iter().any(|r| matches!(
        r.response_type,
        Some(sc::execute_plan_response::ResponseType::ResultComplete(_))
    )));

    let drop_resps = execute_pipeline(
        &mut client,
        session,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DropDataflowGraph(
                sc::pipeline_command::DropDataflowGraph {
                    dataflow_graph_id: Some(graph_id),
                },
            )),
        },
    )
    .await
    .expect("DropDataflowGraph");
    assert_eq!(drop_resps.len(), 1);
    assert!(matches!(
        drop_resps[0].response_type,
        Some(sc::execute_plan_response::ResponseType::ResultComplete(_))
    ));
}

#[tokio::test]
async fn pipeline_rejection_paths() {
    let port = pick_port();
    let mut client = boot(port).await;
    let session = "sdp-reject";

    let create = execute_pipeline(
        &mut client,
        session,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::CreateDataflowGraph(
                sc::pipeline_command::CreateDataflowGraph {
                    default_catalog: Some("spark_catalog".into()),
                    default_database: Some("default".into()),
                    sql_conf: Default::default(),
                },
            )),
        },
    )
    .await
    .expect("create");
    let graph_id = create
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
        .expect("graph id");

    expect_status(
        &mut client,
        session,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineOutput(
                sc::pipeline_command::DefineOutput {
                    dataflow_graph_id: Some("missing-graph".into()),
                    output_name: Some("t".into()),
                    output_type: Some(sc::OutputType::Table as i32),
                    ..Default::default()
                },
            )),
        },
        Code::InvalidArgument,
    )
    .await;

    expect_status(
        &mut client,
        session,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineOutput(
                sc::pipeline_command::DefineOutput {
                    dataflow_graph_id: Some(graph_id.clone()),
                    output_name: Some("sink_out".into()),
                    output_type: Some(sc::OutputType::Sink as i32),
                    ..Default::default()
                },
            )),
        },
        Code::InvalidArgument,
    )
    .await;

    execute_pipeline(
        &mut client,
        session,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineOutput(
                sc::pipeline_command::DefineOutput {
                    dataflow_graph_id: Some(graph_id.clone()),
                    output_name: Some("t".into()),
                    output_type: Some(sc::OutputType::Table as i32),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("define table");

    let empty_flow = execute_pipeline(
        &mut client,
        session,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineFlow(
                sc::pipeline_command::DefineFlow {
                    dataflow_graph_id: Some(graph_id.clone()),
                    flow_name: Some("empty".into()),
                    target_dataset_name: Some("t".into()),
                    details: Some(
                        sc::pipeline_command::define_flow::Details::RelationFlowDetails(
                            sc::pipeline_command::define_flow::WriteRelationFlowDetails {
                                relation: None,
                            },
                        ),
                    ),
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("empty relation DefineFlow is accepted for query-function backfill");
    assert!(empty_flow.iter().any(|r| matches!(
        &r.response_type,
        Some(sc::execute_plan_response::ResponseType::PipelineCommandResult(_))
    )));

    expect_status(
        &mut client,
        session,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::StartRun(
                sc::pipeline_command::StartRun {
                    dataflow_graph_id: Some(graph_id.clone()),
                    dry: Some(true),
                    ..Default::default()
                },
            )),
        },
        Code::FailedPrecondition,
    )
    .await;

    expect_status(
        &mut client,
        session,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineFlow(
                sc::pipeline_command::DefineFlow {
                    dataflow_graph_id: Some(graph_id.clone()),
                    flow_name: Some("cdc".into()),
                    target_dataset_name: Some("t".into()),
                    details: Some(
                        sc::pipeline_command::define_flow::Details::AutoCdcFlowDetails(
                            sc::pipeline_command::define_flow::AutoCdcFlowDetails::default(),
                        ),
                    ),
                    ..Default::default()
                },
            )),
        },
        Code::InvalidArgument,
    )
    .await;

    expect_status(
        &mut client,
        session,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineOutput(
                sc::pipeline_command::DefineOutput {
                    dataflow_graph_id: Some("missing-graph".into()),
                    output_name: Some("sink_out".into()),
                    output_type: Some(sc::OutputType::Sink as i32),
                    ..Default::default()
                },
            )),
        },
        Code::InvalidArgument,
    )
    .await;

    expect_status(
        &mut client,
        session,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineFlow(
                sc::pipeline_command::DefineFlow {
                    dataflow_graph_id: Some("missing-graph".into()),
                    flow_name: Some("empty".into()),
                    target_dataset_name: Some("t".into()),
                    details: Some(
                        sc::pipeline_command::define_flow::Details::RelationFlowDetails(
                            sc::pipeline_command::define_flow::WriteRelationFlowDetails {
                                relation: None,
                            },
                        ),
                    ),
                    ..Default::default()
                },
            )),
        },
        Code::InvalidArgument,
    )
    .await;

    expect_status(
        &mut client,
        session,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineSqlGraphElements(
                sc::pipeline_command::DefineSqlGraphElements {
                    dataflow_graph_id: Some(graph_id),
                    sql_file_path: Some("pipeline.sql".into()),
                    sql_text: Some("SELECT 1".into()),
                },
            )),
        },
        Code::Unimplemented,
    )
    .await;
}

async fn create_graph(
    client: &mut SparkConnectServiceClient<tonic::transport::Channel>,
    session: &str,
) -> String {
    let create = execute_pipeline(
        client,
        session,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::CreateDataflowGraph(
                sc::pipeline_command::CreateDataflowGraph {
                    default_catalog: Some("spark_catalog".into()),
                    default_database: Some("default".into()),
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

#[tokio::test]
async fn pipeline_release_session_drops_graphs() {
    let port = pick_port();
    let mut client = boot(port).await;
    let session = "sdp-release";
    let graph_id = create_graph(&mut client, session).await;

    client
        .release_session(Request::new(sc::ReleaseSessionRequest {
            session_id: session.to_string(),
            ..Default::default()
        }))
        .await
        .expect("release session");

    expect_status(
        &mut client,
        session,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::StartRun(
                sc::pipeline_command::StartRun {
                    dataflow_graph_id: Some(graph_id),
                    dry: Some(true),
                    ..Default::default()
                },
            )),
        },
        Code::InvalidArgument,
    )
    .await;
}

#[tokio::test]
async fn pipeline_cross_session_graph_access_rejected() {
    let port = pick_port();
    let mut client = boot(port).await;
    let owner = "sdp-owner";
    let intruder = "sdp-intruder";
    let graph_id = create_graph(&mut client, owner).await;

    expect_status(
        &mut client,
        intruder,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineOutput(
                sc::pipeline_command::DefineOutput {
                    dataflow_graph_id: Some(graph_id.clone()),
                    output_name: Some("t".into()),
                    output_type: Some(sc::OutputType::Table as i32),
                    ..Default::default()
                },
            )),
        },
        Code::InvalidArgument,
    )
    .await;

    expect_status(
        &mut client,
        intruder,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DropDataflowGraph(
                sc::pipeline_command::DropDataflowGraph {
                    dataflow_graph_id: Some(graph_id),
                },
            )),
        },
        Code::InvalidArgument,
    )
    .await;
}

#[tokio::test]
async fn pipeline_define_output_replaces_by_name() {
    let port = pick_port();
    let mut client = boot(port).await;
    let session = "sdp-replace";
    let graph_id = create_graph(&mut client, session).await;

    for comment in ["v1", "v2"] {
        execute_pipeline(
            &mut client,
            session,
            sc::PipelineCommand {
                command_type: Some(sc::pipeline_command::CommandType::DefineOutput(
                    sc::pipeline_command::DefineOutput {
                        dataflow_graph_id: Some(graph_id.clone()),
                        output_name: Some("metrics".into()),
                        output_type: Some(sc::OutputType::Table as i32),
                        comment: Some(comment.into()),
                        ..Default::default()
                    },
                )),
            },
        )
        .await
        .expect("DefineOutput");
    }

    execute_pipeline(
        &mut client,
        session,
        sc::PipelineCommand {
            command_type: Some(sc::pipeline_command::CommandType::DefineFlow(
                sc::pipeline_command::DefineFlow {
                    dataflow_graph_id: Some(graph_id.clone()),
                    flow_name: Some("fill".into()),
                    target_dataset_name: Some("metrics".into()),
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
                    ..Default::default()
                },
            )),
        },
    )
    .await
    .expect("DefineFlow without pre-defined target output");

    let dry_run = execute_pipeline(
        &mut client,
        session,
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
    .expect("StartRun dry");
    let event = dry_run
        .iter()
        .find_map(|r| match &r.response_type {
            Some(sc::execute_plan_response::ResponseType::PipelineEventResult(ev)) => {
                ev.event.as_ref()?.message.clone()
            }
            _ => None,
        })
        .expect("dry run event");
    assert!(
        event.contains("is valid"),
        "flow to output defined only at DefineFlow time should validate at StartRun: {event}"
    );
}
