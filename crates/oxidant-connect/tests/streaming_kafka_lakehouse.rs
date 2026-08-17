//! End-to-end Structured Streaming: Kafka source → DataFrame transformation → live Delta table.
//!
//! This is the shape a live-dashboard pipeline actually has —
//! `readStream.format("kafka")…select(...)…writeStream.format("delta").start(path)` — driven
//! through the real Spark Connect service, not through the streaming crate's internals.
//!
//! Kafka runs in the source's offline spool mode (`oxidant.spool.dir`), so the test exercises
//! every layer above the broker socket without needing a broker in CI. The Glue leg is covered by
//! `scripts/validate-streaming-glue.sh`, which needs real AWS.

use std::collections::HashMap;
use std::net::TcpListener;
use std::time::Duration;

use oxidant_connect::{serve, ServerConfig};
use oxidant_proto::spark::connect as sc;
use sc::spark_connect_service_client::SparkConnectServiceClient;
use tonic::transport::Channel;
use tonic::Request;

const SESSION: &str = "streaming-lakehouse";

fn pick_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local_addr")
        .port()
}

async fn boot(port: u16) -> SparkConnectServiceClient<Channel> {
    tokio::spawn(async move {
        let _ = serve(ServerConfig {
            port,
            ui_port: None,
            ..Default::default()
        })
        .await;
    });
    let endpoint = format!("http://127.0.0.1:{port}");
    for _ in 0..100 {
        if let Ok(c) = SparkConnectServiceClient::connect(endpoint.clone()).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("server did not become ready on {port}");
}

fn expr(t: sc::expression::ExprType) -> sc::Expression {
    sc::Expression {
        expr_type: Some(t),
        ..Default::default()
    }
}

fn attr(name: &str) -> sc::Expression {
    expr(sc::expression::ExprType::UnresolvedAttribute(
        sc::expression::UnresolvedAttribute {
            unparsed_identifier: name.to_string(),
            ..Default::default()
        },
    ))
}

/// `readStream.format("kafka").option(...).load()`
#[allow(clippy::needless_update)]
fn kafka_read(options: HashMap<String, String>) -> sc::Relation {
    sc::Relation {
        rel_type: Some(sc::relation::RelType::Read(sc::Read {
            is_streaming: true,
            read_type: Some(sc::read::ReadType::DataSource(sc::read::DataSource {
                format: Some("kafka".into()),
                options,
                ..Default::default()
            })),
            ..Default::default()
        })),
        ..Default::default()
    }
}

async fn run_command(
    client: &mut SparkConnectServiceClient<Channel>,
    command: sc::command::CommandType,
) -> Vec<sc::ExecutePlanResponse> {
    use futures::StreamExt;

    let req = sc::ExecutePlanRequest {
        session_id: SESSION.into(),
        plan: Some(sc::Plan {
            op_type: Some(sc::plan::OpType::Command(sc::Command {
                command_type: Some(command),
            })),
        }),
        ..Default::default()
    };
    let mut stream = client
        .execute_plan(Request::new(req))
        .await
        .expect("execute_plan")
        .into_inner();
    let mut out = Vec::new();
    while let Some(msg) = stream.next().await {
        out.push(msg.expect("response"));
    }
    out
}

/// Query the streamed Delta table the way a dashboard would: register it and run SQL. Uses a
/// fresh engine so the assertion proves the *committed table* is readable, not that some
/// in-memory state happened to survive inside the server.
async fn delta_scalar_i64(table_path: &std::path::Path, sql_expr: &str) -> i64 {
    use oxidant_loom::arrow::array::{Array, Int64Array};

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
        .unwrap_or_else(|| panic!("expected i64, got schema {:?}", batches[0].schema()));
    assert!(!col.is_null(0));
    col.value(0)
}

#[tokio::test]
async fn kafka_micro_batches_land_in_a_queryable_delta_table() {
    let spool = tempfile::TempDir::new().expect("spool dir");
    let out = tempfile::TempDir::new().expect("out dir");
    let checkpoint = tempfile::TempDir::new().expect("checkpoint dir");
    // Two micro-batches of records on the topic.
    std::fs::write(
        spool.path().join("batch-0.json"),
        "{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n",
    )
    .expect("write batch 0");
    std::fs::write(
        spool.path().join("batch-1.json"),
        "{\"id\":4}\n{\"id\":5}\n",
    )
    .expect("write batch 1");

    let mut client = boot(pick_port()).await;

    let source_options: HashMap<String, String> = [
        ("subscribe".to_string(), "events".to_string()),
        (
            "oxidant.spool.dir".to_string(),
            spool.path().to_string_lossy().into_owned(),
        ),
    ]
    .into_iter()
    .collect();

    // readStream(kafka).select(value, topic, offset) — a projection, so the sink's schema is the
    // *plan's*, not the source's seven-column Kafka schema.
    let projected = sc::Relation {
        rel_type: Some(sc::relation::RelType::Project(Box::new(sc::Project {
            input: Some(Box::new(kafka_read(source_options))),
            expressions: vec![attr("value"), attr("topic"), attr("offset")],
        }))),
        ..Default::default()
    };

    let table_path = out.path().join("events_delta");
    let writer_options: HashMap<String, String> = [(
        "checkpointLocation".to_string(),
        checkpoint.path().to_string_lossy().into_owned(),
    )]
    .into_iter()
    .collect();

    let responses = run_command(
        &mut client,
        sc::command::CommandType::WriteStreamOperationStart(sc::WriteStreamOperationStart {
            input: Some(projected),
            format: "delta".into(),
            options: writer_options,
            trigger: Some(sc::write_stream_operation_start::Trigger::Once(true)),
            output_mode: "append".into(),
            query_name: "kafka_to_delta".into(),
            sink_destination: Some(sc::write_stream_operation_start::SinkDestination::Path(
                table_path.to_string_lossy().into_owned(),
            )),
            ..Default::default()
        }),
    )
    .await;

    let query_id = responses
        .iter()
        .find_map(|r| match &r.response_type {
            Some(sc::execute_plan_response::ResponseType::WriteStreamOperationStartResult(res)) => {
                res.query_id.clone()
            }
            _ => None,
        })
        .expect("WriteStreamOperationStartResult");

    // `Trigger.Once` drains on a spawned task; drive it deterministically instead of sleeping.
    run_command(
        &mut client,
        sc::command::CommandType::StreamingQueryCommand(sc::StreamingQueryCommand {
            query_id: Some(query_id.clone()),
            command: Some(sc::streaming_query_command::Command::ProcessAllAvailable(
                true,
            )),
        }),
    )
    .await;

    // The transaction log is what makes this a live table rather than a pile of files.
    let log = table_path.join("_delta_log");
    assert!(log.is_dir(), "no _delta_log at {}", table_path.display());
    let commits = std::fs::read_dir(&log)
        .expect("read log")
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|x| x == "json"))
        .count();
    assert!(commits >= 1, "expected at least one Delta commit");

    // And the point of it all: a query reads the streamed rows back out of the table.
    assert_eq!(
        delta_scalar_i64(&table_path, "count(*)").await,
        5,
        "all five Kafka records must be queryable in the Delta table"
    );

    // Every record's offset survived the projection, so the table carries Kafka's own ordering.
    assert_eq!(delta_scalar_i64(&table_path, "max(`offset`)").await, 4);
}

#[tokio::test]
async fn a_kafka_stream_with_no_broker_and_no_spool_fails_the_start_call() {
    let mut client = boot(pick_port()).await;

    let read = kafka_read(
        [("subscribe".to_string(), "events".to_string())]
            .into_iter()
            .collect(),
    );
    let req = sc::ExecutePlanRequest {
        session_id: SESSION.into(),
        plan: Some(sc::Plan {
            op_type: Some(sc::plan::OpType::Command(sc::Command {
                command_type: Some(sc::command::CommandType::WriteStreamOperationStart(
                    sc::WriteStreamOperationStart {
                        input: Some(read),
                        format: "memory".into(),
                        trigger: Some(sc::write_stream_operation_start::Trigger::Once(true)),
                        query_name: "no_broker".into(),
                        ..Default::default()
                    },
                )),
            })),
        }),
        ..Default::default()
    };

    // Misconfiguration must surface on `writeStream.start()`, not minutes later on a batch.
    let err = match client.execute_plan(Request::new(req)).await {
        Err(status) => status,
        Ok(resp) => {
            use futures::StreamExt;
            let mut stream = resp.into_inner();
            let mut found = None;
            while let Some(msg) = stream.next().await {
                if let Err(status) = msg {
                    found = Some(status);
                    break;
                }
            }
            found.expect("expected a failure for a broker-less kafka stream")
        }
    };
    assert!(
        err.message().contains("bootstrap.servers"),
        "unhelpful error: {}",
        err.message()
    );
}
