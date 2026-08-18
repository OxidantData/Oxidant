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
    // Every operation must end with `ResultComplete`. A PySpark 3.5+ client treats an
    // `ExecutePlan` stream as reattachable and keeps reattaching until it arrives, so a command
    // that omits it hangs the client on a call the server already finished. Asserting it here
    // covers every command these tests issue, because a stream that ends without it looks
    // perfectly healthy from inside the server.
    assert!(
        matches!(
            out.last().and_then(|r| r.response_type.as_ref()),
            Some(sc::execute_plan_response::ResponseType::ResultComplete(_))
        ),
        "the response stream must end with ResultComplete, got {:?}",
        out.last().and_then(|r| r.response_type.as_ref())
    );
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

/// Read the same table back through the *Iceberg* resolver instead of the Delta one.
async fn iceberg_scalar_i64(table_path: &std::path::Path, sql_expr: &str) -> i64 {
    use oxidant_loom::arrow::array::{Array, Int64Array};

    let engine = oxidant_loom::Engine::new();
    engine
        .register_iceberg("streamed_iceberg", &table_path.to_string_lossy())
        .await
        .expect("register iceberg view of the delta table");
    let batches = engine
        .sql(&format!("SELECT {sql_expr} FROM streamed_iceberg"))
        .await
        .expect("query iceberg view");
    let col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("expected i64, got schema {:?}", batches[0].schema()));
    assert!(!col.is_null(0));
    col.value(0)
}

#[tokio::test]
async fn a_streamed_delta_table_is_also_readable_as_iceberg() {
    // Interoperability, end to end and through the real service: one stream, one copy of the
    // Parquet, and both a Delta reader and an Iceberg reader see the same rows. This is what lets
    // a team point Athena, Trino, or DuckDB at a live table without standing up a second pipeline
    // to maintain a second copy.
    let spool = tempfile::TempDir::new().expect("spool dir");
    let out = tempfile::TempDir::new().expect("out dir");
    let checkpoint = tempfile::TempDir::new().expect("checkpoint dir");
    std::fs::write(
        spool.path().join("batch-0.json"),
        "{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n",
    )
    .expect("write batch 0");

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

    let projected = sc::Relation {
        rel_type: Some(sc::relation::RelType::Project(Box::new(sc::Project {
            input: Some(Box::new(kafka_read(source_options))),
            expressions: vec![attr("value"), attr("topic"), attr("offset")],
        }))),
        ..Default::default()
    };

    let table_path = out.path().join("events_uniform");
    let writer_options: HashMap<String, String> = [
        (
            "checkpointLocation".to_string(),
            checkpoint.path().to_string_lossy().into_owned(),
        ),
        // Publish the Iceberg tree on every commit so one micro-batch is enough to assert on;
        // the default of 10 amortizes the metadata write over more batches.
        ("checkpointInterval".to_string(), "1".to_string()),
    ]
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
            query_name: "kafka_to_uniform".into(),
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

    run_command(
        &mut client,
        sc::command::CommandType::StreamingQueryCommand(sc::StreamingQueryCommand {
            query_id: Some(query_id),
            command: Some(sc::streaming_query_command::Command::ProcessAllAvailable(
                true,
            )),
        }),
    )
    .await;

    // Both metadata trees exist over one set of data files.
    assert!(table_path.join("_delta_log").is_dir(), "no Delta log");
    let metadata = table_path.join("metadata");
    assert!(metadata.is_dir(), "no Iceberg metadata directory");
    assert!(
        metadata.join("version-hint.text").exists(),
        "catalog-less Iceberg readers need version-hint.text"
    );

    // The name mapping is the detail that makes field-id-less Parquet readable as Iceberg —
    // without it the query below returns rows of nulls rather than failing, so assert on it.
    let current: String = std::fs::read_to_string(metadata.join("version-hint.text"))
        .expect("version hint")
        .trim()
        .to_string();
    let table_metadata =
        std::fs::read_to_string(metadata.join(format!("v{current}.metadata.json")))
            .expect("iceberg metadata json");
    assert!(
        table_metadata.contains("schema.name-mapping.default"),
        "published Iceberg metadata must carry a name mapping"
    );

    assert_eq!(delta_scalar_i64(&table_path, "count(*)").await, 3);
    assert_eq!(
        iceberg_scalar_i64(&table_path, "count(*)").await,
        3,
        "the Iceberg view of the table must see the same rows as the Delta view"
    );
    assert_eq!(iceberg_scalar_i64(&table_path, "max(`offset`)").await, 2);
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

#[tokio::test]
async fn an_available_now_query_terminates_once_it_has_drained_the_source() {
    // `Trigger.AvailableNow` means "process what is there, then stop" — a batch job spelled as a
    // stream. `awaitTermination()` and the `while query.isActive` loop every such job is built
    // around only return when the query goes inactive *by itself*; a query left active after it
    // drained hangs the job forever on work that is already finished. The other tests here drive
    // `ProcessAllAvailable` by hand, which masks this entirely.
    let spool = tempfile::TempDir::new().expect("spool dir");
    let out = tempfile::TempDir::new().expect("out dir");
    let checkpoint = tempfile::TempDir::new().expect("checkpoint dir");
    std::fs::write(
        spool.path().join("batch-0.json"),
        "{\"id\":1}\n{\"id\":2}\n",
    )
    .expect("write batch 0");

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

    let projected = sc::Relation {
        rel_type: Some(sc::relation::RelType::Project(Box::new(sc::Project {
            input: Some(Box::new(kafka_read(source_options))),
            expressions: vec![attr("value"), attr("offset")],
        }))),
        ..Default::default()
    };

    let table_path = out.path().join("events_available_now");
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
            trigger: Some(sc::write_stream_operation_start::Trigger::AvailableNow(
                true,
            )),
            output_mode: "append".into(),
            query_name: "available_now".into(),
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

    // Poll the way a client does. Deliberately no `ProcessAllAvailable`: the trigger has to drain
    // and terminate on its own.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut active = true;
    while active && std::time::Instant::now() < deadline {
        let responses = run_command(
            &mut client,
            sc::command::CommandType::StreamingQueryCommand(sc::StreamingQueryCommand {
                query_id: Some(query_id.clone()),
                command: Some(sc::streaming_query_command::Command::Status(true)),
            }),
        )
        .await;
        active = responses
            .iter()
            .find_map(|r| match &r.response_type {
                Some(sc::execute_plan_response::ResponseType::StreamingQueryCommandResult(res)) => {
                    match &res.result_type {
                        Some(sc::streaming_query_command_result::ResultType::Status(s)) => {
                            Some(s.is_active)
                        }
                        _ => None,
                    }
                }
                _ => None,
            })
            .expect("a status result");
        if active {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    assert!(
        !active,
        "an availableNow query is still active after draining its source"
    );
    // Terminated because the work is done, not because it gave up before writing.
    assert_eq!(delta_scalar_i64(&table_path, "count(*)").await, 2);
}
