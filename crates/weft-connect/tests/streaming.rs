//! Streaming Connect integration tests.

use std::net::TcpListener;
use std::time::Duration;

use sc::spark_connect_service_client::SparkConnectServiceClient;
use sc::spark_connect_service_server::SparkConnectService;
use tonic::Request;
use weft_connect::{serve, ServerConfig, WeftService};
use weft_proto::spark::connect as sc;

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

#[tokio::test]
async fn write_stream_start_returns_query_id() {
    let port = pick_port();
    let mut client = boot(port).await;

    let req = sc::ExecutePlanRequest {
        session_id: "sess".into(),
        plan: Some(sc::Plan {
            op_type: Some(sc::plan::OpType::Command(sc::Command {
                command_type: Some(sc::command::CommandType::WriteStreamOperationStart(
                    sc::WriteStreamOperationStart {
                        input: None,
                        format: "memory".into(),
                        options: Default::default(),
                        partitioning_column_names: vec![],
                        trigger: Some(sc::write_stream_operation_start::Trigger::Once(true)),
                        output_mode: "append".into(),
                        query_name: "stream_test".into(),
                        ..Default::default()
                    },
                )),
            })),
        }),
        ..Default::default()
    };

    let mut stream = client
        .execute_plan(Request::new(req))
        .await
        .expect("execute_plan")
        .into_inner();
    use tokio_stream::StreamExt;
    let mut got_id = false;
    while let Some(resp) = stream.next().await {
        let resp = resp.expect("stream item");
        if let Some(sc::execute_plan_response::ResponseType::WriteStreamOperationStartResult(r)) =
            resp.response_type
        {
            let qid = r.query_id.expect("query_id");
            assert!(!qid.id.is_empty());
            assert!(!qid.run_id.is_empty());
            got_id = true;
        }
    }
    assert!(got_id, "expected WriteStreamOperationStartResult");
}

#[tokio::test]
#[allow(clippy::needless_update)]
async fn analyze_is_streaming_true_for_streaming_read() {
    let service = WeftService::new();
    let rel = sc::Relation {
        rel_type: Some(sc::relation::RelType::Read(sc::Read {
            is_streaming: true,
            read_type: Some(sc::read::ReadType::DataSource(sc::read::DataSource {
                format: Some("parquet".into()),
                schema: None,
                options: Default::default(),
                paths: vec!["/tmp/stream".into()],
                predicates: vec![],
                ..Default::default()
            })),
            ..Default::default()
        })),
        ..Default::default()
    };
    let req = sc::AnalyzePlanRequest {
        analyze: Some(sc::analyze_plan_request::Analyze::IsStreaming(
            sc::analyze_plan_request::IsStreaming {
                plan: Some(sc::Plan {
                    op_type: Some(sc::plan::OpType::Root(rel)),
                }),
            },
        )),
        ..Default::default()
    };
    let resp = service
        .analyze_plan(Request::new(req))
        .await
        .expect("analyze")
        .into_inner();
    match resp.result {
        Some(sc::analyze_plan_response::Result::IsStreaming(s)) => {
            assert!(s.is_streaming);
        }
        other => panic!("unexpected analyze result: {other:?}"),
    }
}

#[tokio::test]
async fn streaming_query_status_and_stop() {
    let port = pick_port();
    let mut client = boot(port).await;

    let start_req = sc::ExecutePlanRequest {
        session_id: "sess".into(),
        plan: Some(sc::Plan {
            op_type: Some(sc::plan::OpType::Command(sc::Command {
                command_type: Some(sc::command::CommandType::WriteStreamOperationStart(
                    sc::WriteStreamOperationStart {
                        format: "memory".into(),
                        trigger: Some(sc::write_stream_operation_start::Trigger::Once(true)),
                        output_mode: "append".into(),
                        query_name: "status_test".into(),
                        ..Default::default()
                    },
                )),
            })),
        }),
        ..Default::default()
    };

    let mut stream = client
        .execute_plan(Request::new(start_req))
        .await
        .expect("execute_plan")
        .into_inner();
    use tokio_stream::StreamExt;
    let mut query_id = None;
    while let Some(resp) = stream.next().await {
        let resp = resp.expect("stream item");
        if let Some(sc::execute_plan_response::ResponseType::WriteStreamOperationStartResult(r)) =
            resp.response_type
        {
            query_id = r.query_id;
        }
    }
    let qid = query_id.expect("query_id from start");

    let status_req = sc::ExecutePlanRequest {
        session_id: "sess".into(),
        plan: Some(sc::Plan {
            op_type: Some(sc::plan::OpType::Command(sc::Command {
                command_type: Some(sc::command::CommandType::StreamingQueryCommand(
                    sc::StreamingQueryCommand {
                        query_id: Some(qid.clone()),
                        command: Some(sc::streaming_query_command::Command::Status(true)),
                    },
                )),
            })),
        }),
        ..Default::default()
    };
    let mut status_stream = client
        .execute_plan(Request::new(status_req))
        .await
        .expect("status")
        .into_inner();
    let mut saw_active = false;
    while let Some(resp) = status_stream.next().await {
        let resp = resp.expect("status item");
        if let Some(sc::execute_plan_response::ResponseType::StreamingQueryCommandResult(r)) =
            resp.response_type
        {
            if let Some(sc::streaming_query_command_result::ResultType::Status(s)) = r.result_type {
                saw_active = s.is_active;
            }
        }
    }
    assert!(saw_active, "query should be active after start");

    let stop_req = sc::ExecutePlanRequest {
        session_id: "sess".into(),
        plan: Some(sc::Plan {
            op_type: Some(sc::plan::OpType::Command(sc::Command {
                command_type: Some(sc::command::CommandType::StreamingQueryCommand(
                    sc::StreamingQueryCommand {
                        query_id: Some(qid),
                        command: Some(sc::streaming_query_command::Command::Stop(true)),
                    },
                )),
            })),
        }),
        ..Default::default()
    };
    let mut stop_stream = client
        .execute_plan(Request::new(stop_req))
        .await
        .expect("stop")
        .into_inner();
    let mut stopped = false;
    while let Some(resp) = stop_stream.next().await {
        let resp = resp.expect("stop item");
        if let Some(sc::execute_plan_response::ResponseType::StreamingQueryCommandResult(r)) =
            resp.response_type
        {
            if let Some(sc::streaming_query_command_result::ResultType::Status(s)) = r.result_type {
                stopped = !s.is_active;
            }
        }
    }
    assert!(stopped, "query should be inactive after stop");
}
