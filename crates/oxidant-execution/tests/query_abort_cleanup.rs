//! KAN-46 (+KAN-31): a dropped or aborted query must not pin worker resources.
//!
//! - Dropping the driver future mid-stage (what tonic does to the Spark Connect handler
//!   when the client disconnects) must still cancel the plan's stages on the workers and
//!   evict their cached stage output — the cleanup used to live only *after* the driver's
//!   inner await, so it never ran on that path.
//! - A producer stage task that exits without committing its output (driver cancel, or the
//!   do_get future dropped when the Flight client went away) must reap its own spill scope;
//!   `BucketCache` has no `Drop` and an uncommitted stage id never reaches the stage cache,
//!   so `clear_stages` could never find those files (SF10: 25–38 GB orphaned spill segments).
//!
//! Own integration binary: the stage-delay env knobs are process-global, so the tests
//! serialize through a lock.

#![allow(clippy::await_holding_lock)] // ENV_LOCK serializes process-global env across async tests

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use oxidant_execution::driver::{run_stages, Cluster, StageDef};
use oxidant_execution::flight::{
    cancel_stage_on_worker, clear_worker_stages, health_check_worker, heartbeat_worker,
    pull_bucket, run_stage_on_worker, serve_worker, serve_worker_with_spill,
};
use oxidant_execution::shuffle::protocol::StageTicket;
use oxidant_execution::shuffle::spill::SpillStore;
use oxidant_loom::arrow::array::Int64Array;
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;

/// `OXIDANT_TEST_STAGE_DELAY_MS` / `OXIDANT_STAGE_TIMEOUT_MS` are process-global; serialize tests.
static ENV_LOCK: Mutex<()> = Mutex::new(());

fn ephemeral_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

async fn wait_worker_up(endpoint: &str) {
    for _ in 0..50 {
        if health_check_worker(endpoint.to_string()).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("worker did not become ready at {endpoint}");
}

async fn start_worker() -> String {
    let port = ephemeral_port();
    let engine = Arc::new(Engine::new());
    tokio::spawn(async move {
        let _ = serve_worker(port, engine).await;
    });
    let endpoint = format!("http://127.0.0.1:{port}");
    wait_worker_up(&endpoint).await;
    endpoint
}

/// A worker that spills every non-empty shuffle bucket to a temp dir. The returned store
/// clones the worker's, so the test can seed and inspect spill files directly.
async fn start_spill_worker(root: PathBuf) -> (String, SpillStore) {
    let store = SpillStore::with_memory_limit(root, 1).expect("spill store");
    let port = ephemeral_port();
    let engine = Arc::new(Engine::new());
    let spill = store.clone();
    tokio::spawn(async move {
        let _ = serve_worker_with_spill(port, engine, spill, false).await;
    });
    let endpoint = format!("http://127.0.0.1:{port}");
    wait_worker_up(&endpoint).await;
    (endpoint, store)
}

fn spill_root(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("oxidant-kan46-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Spill files a stage owns anywhere under the store root (base files + `.segN` segments).
fn spill_files(store: &SpillStore, stage_id: u32) -> usize {
    let prefix = format!("stage_{stage_id}_");
    std::fs::read_dir(store.root())
        .map(|entries| {
            entries
                .flatten()
                .filter(|e| e.file_name().to_string_lossy().starts_with(&prefix))
                .count()
        })
        .unwrap_or(0)
}

fn seed_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap()
}

fn producer_ticket(stage_id: u32) -> StageTicket {
    StageTicket {
        stage_id,
        partition_id: 0,
        num_partitions: 1,
        upstream_endpoints: vec![],
        stage_sql: "SELECT 1 AS k, 2 AS v".into(),
        plan_fragment: vec![],
        hash_key_cols: vec![0],
        upstream_stage_ids: vec![],
        produce: true,
        lakehouse_snapshot_pins: String::new(),
        replicated_tables: String::new(),
        coalesce_read_modulus: 0,
        forward_upstream_stage_ids: vec![],
        upstream_bucket_rows: vec![],
        lakeformation_required: false,
        lakeformation_principal: String::new(),
    }
}

/// Simulate a producer's partial output written before the wedge: a base file plus one
/// `.seg0` segment, exactly the shapes KAN-31 found orphaned on workers.
fn seed_partial_spill(store: &SpillStore, stage_id: u32) {
    let batch = seed_batch();
    store
        .write_bucket(stage_id, 0, 0, batch.schema(), std::slice::from_ref(&batch))
        .unwrap();
    store
        .append_batches_to_bucket(stage_id, 0, 0, batch.schema(), std::slice::from_ref(&batch))
        .unwrap();
    assert!(
        spill_files(store, stage_id) >= 2,
        "seed should create a base file and a segment"
    );
}

async fn wait_slot_held(endpoint: &str) {
    for _ in 0..100 {
        let hb = heartbeat_worker(endpoint.to_string()).await.unwrap();
        if hb.slots_used.is_some_and(|used| used >= 1) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("stage task never held a worker slot at {endpoint}");
}

async fn wait_slot_free(endpoint: &str) {
    for _ in 0..100 {
        let hb = heartbeat_worker(endpoint.to_string()).await.unwrap();
        if hb.slots_used == Some(0) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("worker slot still held after the query went away at {endpoint}");
}

/// A live cache entry serves typed batches (the producer schema); a cleared one round-trips
/// as a schema-less placeholder (see `Worker::read_shuffle` / `do_get_batches_once`).
async fn wait_stage_cache_typed(endpoint: &str, stage_id: u32) {
    for _ in 0..150 {
        let pulled = pull_bucket(endpoint.to_string(), stage_id, 0)
            .await
            .unwrap();
        if pulled.iter().any(|b| !b.schema().fields().is_empty()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("stage {stage_id} never cached typed output at {endpoint}");
}

async fn wait_stage_cache_cleared(endpoint: &str, stage_id: u32) {
    for _ in 0..100 {
        let pulled = pull_bucket(endpoint.to_string(), stage_id, 0)
            .await
            .unwrap();
        if pulled.iter().all(|b| b.schema().fields().is_empty()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("stage {stage_id} output still cached after the query went away at {endpoint}");
}

/// Dropping the driver future mid-stage (client disconnect cancels the Spark Connect handler
/// future exactly this way) must still cancel the in-flight stage on the worker and evict the
/// completed producer's cached output.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_driver_future_cancels_stage_and_clears_cache() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    std::env::set_var("OXIDANT_STAGE_TIMEOUT_MS", "60000");
    std::env::set_var("OXIDANT_TEST_STAGE_DELAY_MS", "3000");

    let endpoint = start_worker().await;
    let cluster = Cluster::new(vec![endpoint.clone()]);
    let stages = vec![
        StageDef::new(0, "SELECT 1 AS k, 2 AS v", vec![], vec![0]),
        StageDef::new(
            1,
            "SELECT k, SUM(v) AS s FROM shuffle_input GROUP BY k",
            vec![0],
            vec![],
        ),
    ];
    let driver = tokio::spawn({
        let cluster = cluster.clone();
        async move { run_stages(&cluster, &stages).await }
    });

    // Stage 0 produced and cached its buckets; stage 1 is holding a worker slot.
    wait_stage_cache_typed(&endpoint, 0).await;
    wait_slot_held(&endpoint).await;

    // The client is gone: the driver future is dropped mid-stage (tonic cancels the handler
    // future the same way). Cleanup must not depend on reaching the code after the await.
    driver.abort();
    let _ = driver.await;

    wait_slot_free(&endpoint).await;
    wait_stage_cache_cleared(&endpoint, 0).await;

    std::env::remove_var("OXIDANT_STAGE_TIMEOUT_MS");
    std::env::remove_var("OXIDANT_TEST_STAGE_DELAY_MS");
}

/// A driver-cancelled producer stage task reaps its own spill scope (the files it wrote
/// before the cancel); the slot frees through the existing KAN-17 path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_stage_reaps_its_spill_scope() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    std::env::set_var("OXIDANT_STAGE_TIMEOUT_MS", "60000");
    std::env::set_var("OXIDANT_TEST_STAGE_DELAY_MS", "30000");

    let root = spill_root("cancel");
    let (endpoint, store) = start_spill_worker(root.clone()).await;
    seed_partial_spill(&store, 903);

    let stage = tokio::spawn(run_stage_on_worker(endpoint.clone(), producer_ticket(903)));
    wait_slot_held(&endpoint).await;

    cancel_stage_on_worker(endpoint.clone(), 903).await.unwrap();
    let err = stage.await.unwrap().unwrap_err().to_string();
    assert!(
        err.contains("cancelled"),
        "expected stage-cancelled error, got: {err}"
    );

    assert_eq!(
        spill_files(&store, 903),
        0,
        "cancelled producer stage left orphaned spill segments"
    );
    wait_slot_free(&endpoint).await;

    std::env::remove_var("OXIDANT_STAGE_TIMEOUT_MS");
    std::env::remove_var("OXIDANT_TEST_STAGE_DELAY_MS");
    let _ = std::fs::remove_dir_all(root);
}

/// When the Flight client (driver) goes away mid-stage, tonic drops the worker's do_get
/// future; the task must unwind — slot freed and partial spill reaped — without waiting for
/// the stage timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_do_get_reaps_spill_and_frees_slot() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    std::env::set_var("OXIDANT_STAGE_TIMEOUT_MS", "60000");
    std::env::set_var("OXIDANT_TEST_STAGE_DELAY_MS", "30000");

    let root = spill_root("drop");
    let (endpoint, store) = start_spill_worker(root.clone()).await;
    seed_partial_spill(&store, 904);

    let stage = tokio::spawn(run_stage_on_worker(endpoint.clone(), producer_ticket(904)));
    wait_slot_held(&endpoint).await;

    // The driver vanished mid-stage: the do_get client stream resets and the worker's
    // handler future is dropped.
    stage.abort();
    let _ = stage.await;

    wait_slot_free(&endpoint).await;
    for _ in 0..100 {
        if spill_files(&store, 904) == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        spill_files(&store, 904),
        0,
        "dropped do_get left orphaned spill segments"
    );

    std::env::remove_var("OXIDANT_STAGE_TIMEOUT_MS");
    std::env::remove_var("OXIDANT_TEST_STAGE_DELAY_MS");
    let _ = std::fs::remove_dir_all(root);
}

/// Positive control: a producer that *commits* its output keeps its spill files (the cache
/// entry owns them) until the driver's `clear_stages` — the reaper must not eat live data.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_producer_keeps_spill_until_clear_stages() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    std::env::remove_var("OXIDANT_TEST_STAGE_DELAY_MS");

    let root = spill_root("commit");
    let (endpoint, store) = start_spill_worker(root.clone()).await;

    run_stage_on_worker(endpoint.clone(), producer_ticket(905))
        .await
        .expect("producer stage");
    assert!(
        spill_files(&store, 905) > 0,
        "committed producer output must stay readable from spill"
    );

    clear_worker_stages(endpoint).await.unwrap();
    assert_eq!(
        spill_files(&store, 905),
        0,
        "clear_stages should still reap committed spill"
    );

    let _ = std::fs::remove_dir_all(root);
}
