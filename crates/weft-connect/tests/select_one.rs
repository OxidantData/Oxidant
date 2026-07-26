//! End-to-end test for issue #1: boot the Spark Connect server, run `SELECT 1` over gRPC
//! via the generated client, decode the Arrow IPC result, and assert the value is 1.

use std::time::Duration;

use sc::spark_connect_service_client::SparkConnectServiceClient;
use weft_connect::{serve, ServerConfig};
use weft_loom::arrow::array::Int32Array;
use weft_loom::arrow::ipc::reader::StreamReader;
use weft_proto::spark::connect as sc;

#[tokio::test]
async fn select_one_over_grpc_returns_1() {
    let port = 50971;

    // Start the server in the background.
    tokio::spawn(async move {
        let _ = serve(ServerConfig {
            port,
            ..Default::default()
        })
        .await;
    });

    // Wait for readiness (retry connect).
    let endpoint = format!("http://127.0.0.1:{port}");
    let mut client = None;
    for _ in 0..50 {
        match SparkConnectServiceClient::connect(endpoint.clone()).await {
            Ok(c) => {
                client = Some(c.max_decoding_message_size(256 * 1024 * 1024));
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    let mut client = client.expect("server did not become ready");

    // Build ExecutePlan(SELECT 1 AS x) as a SQL relation.
    let request = sc::ExecutePlanRequest {
        session_id: uuid_like(),
        plan: Some(sc::Plan {
            op_type: Some(sc::plan::OpType::Root(sc::Relation {
                common: None,
                rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                    query: "SELECT 1 AS x".to_string(),
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    };

    let mut stream = client
        .execute_plan(request)
        .await
        .expect("execute_plan failed")
        .into_inner();

    let mut saw_value = false;
    let mut saw_complete = false;
    while let Some(msg) = stream.message().await.expect("stream error") {
        match msg.response_type {
            Some(sc::execute_plan_response::ResponseType::ArrowBatch(batch)) => {
                assert_eq!(batch.row_count, 1);
                let reader = StreamReader::try_new(std::io::Cursor::new(batch.data), None)
                    .expect("ipc reader");
                for rb in reader {
                    let rb = rb.expect("decode batch");
                    // `SELECT 1` is Spark `IntegerType` (Int32) — weft types integer literals as
                    // Int32 to match Spark (real PySpark `SELECT 1` → IntegerType), not i64.
                    let col = rb
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("int32 column");
                    assert_eq!(col.value(0), 1);
                    saw_value = true;
                }
            }
            Some(sc::execute_plan_response::ResponseType::ResultComplete(_)) => {
                saw_complete = true;
            }
            _ => {}
        }
    }

    assert!(saw_value, "no Arrow batch with the value");
    assert!(saw_complete, "stream did not terminate with ResultComplete");
}

fn uuid_like() -> String {
    "00112233-4455-6677-8899-aabbccddeeff".to_string()
}
