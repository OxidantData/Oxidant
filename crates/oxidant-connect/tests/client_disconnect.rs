//! KAN-46: a Spark Connect client that drops its `ExecutePlan` stream mid-query (disconnect,
//! or a client-side timeout that resets the call) must not leave the query running on the
//! workers — the driver cancels the plan's stages, worker task slots free, and cached stage
//! output is evicted. A slow client that stays connected is never killed.
//!
//! Own integration binary: the stage-delay env knobs are process-global, so the tests
//! serialize through a lock.

#![allow(clippy::await_holding_lock)] // ENV_LOCK serializes process-global env across async tests

use std::sync::{Arc, Mutex};
use std::time::Duration;

use oxidant_connect::OxidantService;
use oxidant_execution::flight::{health_check_worker, heartbeat_worker, pull_bucket, serve_worker};
use oxidant_loom::arrow::array::Int64Array;
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;
use oxidant_proto::spark::connect as sc;
use sc::spark_connect_service_client::SparkConnectServiceClient;
use tonic::transport::Channel;

/// `OXIDANT_TEST_STAGE_DELAY_MS` / `OXIDANT_STAGE_TIMEOUT_MS` are process-global; serialize tests.
static ENV_LOCK: Mutex<()> = Mutex::new(());

fn ephemeral_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn make_batch(start: i64, end: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let ks: Vec<i64> = (start..end).map(|i| i % 5).collect();
    let vs: Vec<i64> = (start..end).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ks)),
            Arc::new(Int64Array::from(vs)),
        ],
    )
    .unwrap()
}

/// One Flight worker holding table `t`, plus a Spark Connect server routing to it.
async fn start_connect_with_worker() -> (String, u16) {
    const N: i64 = 100;
    let wport = ephemeral_port();
    let worker_engine = Arc::new(Engine::new());
    worker_engine
        .register_batches("t", vec![make_batch(0, N)])
        .unwrap();
    tokio::spawn(async move {
        let _ = serve_worker(wport, worker_engine).await;
    });
    let worker_ep = format!("http://127.0.0.1:{wport}");
    for _ in 0..50 {
        if health_check_worker(worker_ep.clone()).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let driver_engine = Arc::new(Engine::new());
    driver_engine
        .register_batches("t", vec![make_batch(0, N)])
        .unwrap();
    let mut service = OxidantService::with_engine(driver_engine);
    service.workers = vec![worker_ep.clone()];
    let port = ephemeral_port();
    tokio::spawn(async move {
        let _ = oxidant_connect::serve_instance(service, port).await;
    });
    (worker_ep, port)
}

async fn connect(endpoint: &str) -> SparkConnectServiceClient<Channel> {
    for _ in 0..50 {
        if let Ok(c) = SparkConnectServiceClient::connect(endpoint.to_string()).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server not ready at {endpoint}");
}

fn sql_request(sql: &str) -> sc::ExecutePlanRequest {
    sc::ExecutePlanRequest {
        session_id: "00112233-4455-6677-8899-aabbccddeeff".into(),
        plan: Some(sc::Plan {
            op_type: Some(sc::plan::OpType::Root(sc::Relation {
                common: None,
                rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                    query: sql.into(),
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    }
}

async fn wait_slot_held(worker_ep: &str) {
    for _ in 0..100 {
        let hb = heartbeat_worker(worker_ep.to_string()).await.unwrap();
        if hb.slots_used.is_some_and(|used| used >= 1) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no worker slot ever held at {worker_ep}");
}

async fn wait_slot_free(worker_ep: &str) {
    for _ in 0..100 {
        let hb = heartbeat_worker(worker_ep.to_string()).await.unwrap();
        if hb.slots_used == Some(0) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("worker slot still held after the client went away at {worker_ep}");
}

/// The producer stage's cached buckets serve typed batches; after eviction the read
/// round-trips a schema-less placeholder.
async fn wait_stage_cache_typed(worker_ep: &str, stage_id: u32) {
    for _ in 0..150 {
        let pulled = pull_bucket(worker_ep.to_string(), stage_id, 0)
            .await
            .unwrap();
        if pulled.iter().any(|b| !b.schema().fields().is_empty()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("stage {stage_id} never cached typed output at {worker_ep}");
}

async fn wait_stage_cache_cleared(worker_ep: &str, stage_id: u32) {
    for _ in 0..100 {
        let pulled = pull_bucket(worker_ep.to_string(), stage_id, 0)
            .await
            .unwrap();
        if pulled.iter().all(|b| b.schema().fields().is_empty()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("stage {stage_id} output still cached after the client went away at {worker_ep}");
}

/// The client abandons the ExecutePlan call while the output stage is running on the worker
/// (what a client-side timeout or process kill does: the RPC resets, and tonic cancels the
/// server-side handler future). The driver must cancel the plan's stages and evict the
/// producer's cached buckets instead of leaving the query to the stage timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disconnect_mid_query_cancels_stages_and_evicts_cache() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    std::env::set_var("OXIDANT_STAGE_TIMEOUT_MS", "60000");
    std::env::set_var("OXIDANT_TEST_STAGE_DELAY_MS", "3000");

    let (worker_ep, port) = start_connect_with_worker().await;
    let mut client = connect(&format!("http://127.0.0.1:{port}")).await;

    // The server runs the whole query inside the `execute_plan` call before any response
    // streams, so the mid-query disconnect signal is the *call future* going away. Drive the
    // RPC on its own task so the test can drop it mid-stage (RST_STREAM + channel close).
    let rpc = tokio::spawn(async move {
        client
            .execute_plan(sql_request("SELECT k, SUM(v) AS s FROM t GROUP BY k"))
            .await
    });

    // Stage 0 (partial agg) produced and cached its buckets; stage 1 (final agg) is holding
    // a worker slot inside the stage-delay hook.
    wait_stage_cache_typed(&worker_ep, 0).await;
    wait_slot_held(&worker_ep).await;

    // The client goes away mid-query.
    rpc.abort();
    let _ = rpc.await;

    wait_slot_free(&worker_ep).await;
    wait_stage_cache_cleared(&worker_ep, 0).await;

    std::env::remove_var("OXIDANT_STAGE_TIMEOUT_MS");
    std::env::remove_var("OXIDANT_TEST_STAGE_DELAY_MS");
}

/// A client that stays connected while its (slow) query runs must not be killed: the query
/// completes, returns the right rows, and the normal end-of-query cleanup still runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_connected_client_is_not_killed() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    std::env::set_var("OXIDANT_STAGE_TIMEOUT_MS", "60000");
    std::env::set_var("OXIDANT_TEST_STAGE_DELAY_MS", "3000");

    let (worker_ep, port) = start_connect_with_worker().await;
    let mut client = connect(&format!("http://127.0.0.1:{port}")).await;

    let mut stream = client
        .execute_plan(sql_request("SELECT k, SUM(v) AS s FROM t GROUP BY k"))
        .await
        .unwrap()
        .into_inner();

    // Stay connected and read the stream to the end: two stages × the 3 s delay hook, and
    // the query must still succeed.
    let mut rows = 0usize;
    let mut completed = false;
    while let Some(msg) = stream.message().await.unwrap() {
        match msg.response_type {
            Some(sc::execute_plan_response::ResponseType::ArrowBatch(b)) => {
                rows += b.row_count as usize;
            }
            Some(sc::execute_plan_response::ResponseType::ResultComplete(_)) => {
                completed = true;
            }
            _ => {}
        }
    }
    assert!(
        completed,
        "connected client's query must run to ResultComplete"
    );
    assert_eq!(rows, 5, "five k-groups (v % 5 over 100 rows)");

    // The normal exit path still evicts stage caches and frees the slot.
    wait_slot_free(&worker_ep).await;
    wait_stage_cache_cleared(&worker_ep, 0).await;

    std::env::remove_var("OXIDANT_STAGE_TIMEOUT_MS");
    std::env::remove_var("OXIDANT_TEST_STAGE_DELAY_MS");
}
