//! KAN-145: StageFinished events carry real counters instead of zeros. After each
//! producer stage the driver's barrier sample (the cheap KAN-32 `bucket_row_counts`
//! probe, extended with per-bucket byte counts) feeds the event:
//!
//! - a producer stage reports its exact output rows (bucket rows summed over every
//!   producing worker) and shuffle-write bytes (estimated in-memory footprint, real
//!   on-disk bytes once spilled);
//! - a consumer / output stage reports input rows and shuffle-read bytes as the summed
//!   outputs of its upstreams (a coalesced AQE read still pulls every bucket once);
//! - the sample runs whenever telemetry is wired — even with `OXIDANT_AQE=0` AND
//!   `OXIDANT_STAGE_INPUT_STATS=0` — so the counters never regress to zeros.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use oxidant_execution::driver::{run_stages_obs, Cluster, StageDef};
use oxidant_execution::flight::serve_worker;
use oxidant_loom::arrow::array::Int64Array;
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;
use oxidant_observability::{AppStateStore, ExecutionEvent};

/// `OXIDANT_AQE` / `OXIDANT_STAGE_INPUT_STATS` / `OXIDANT_SHUFFLE_PARTITIONS` are
/// process-global; serialize these tests.
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn orders(start: i64, end: i64, custs: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
    ]));
    let ok: Vec<i64> = (start..end).collect();
    let ck: Vec<i64> = (start..end).map(|i| i % custs).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ok)),
            Arc::new(Int64Array::from(ck)),
        ],
    )
    .unwrap()
}

fn customer(start: i64, end: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_val", DataType::Int64, false),
    ]));
    let ck: Vec<i64> = (start..end).collect();
    let cv: Vec<i64> = (start..end).map(|i| i * 10).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ck)),
            Arc::new(Int64Array::from(cv)),
        ],
    )
    .unwrap()
}

/// The three-stage shuffle join: stages 0/1 hash-shuffle orders/customer onto the join
/// key, stage 2 joins the co-located buckets and aggregates (it is the output stage).
fn join_stages() -> Vec<StageDef> {
    vec![
        StageDef {
            stage_id: 0,
            sql: "SELECT o_orderkey, o_custkey FROM orders".into(),
            upstream_stage_ids: vec![],
            hash_key_cols: vec![1],
            ..StageDef::default()
        },
        StageDef {
            stage_id: 1,
            sql: "SELECT c_custkey, c_val FROM customer".into(),
            upstream_stage_ids: vec![],
            hash_key_cols: vec![0],
            ..StageDef::default()
        },
        StageDef {
            stage_id: 2,
            sql: "SELECT o.o_custkey AS k, COUNT(*) AS n, SUM(c.c_val) AS s \
                  FROM shuffle_input_0 o JOIN shuffle_input_1 c \
                  ON o.o_custkey = c.c_custkey GROUP BY o.o_custkey"
                .into(),
            upstream_stage_ids: vec![0, 1],
            hash_key_cols: vec![],
            ..StageDef::default()
        },
    ]
}

async fn start_worker(orders_batch: RecordBatch, customer_batch: RecordBatch) -> String {
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let engine = Arc::new(Engine::new());
    engine
        .register_batches("orders", vec![orders_batch])
        .unwrap();
    engine
        .register_batches("customer", vec![customer_batch])
        .unwrap();
    let worker = engine.clone();
    tokio::spawn(async move {
        let _ = serve_worker(port, worker).await;
    });
    format!("http://127.0.0.1:{port}")
}

/// Subscribe a draining collector to `store`'s event stream (same pattern as
/// `tests/concurrent_stages.rs::collect_events`).
fn collect_events(store: &Arc<AppStateStore>) -> Arc<Mutex<Vec<ExecutionEvent>>> {
    let mut rx = store.subscribe();
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_c = events.clone();
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(ev) => events_c.lock().expect("events poisoned").push(ev),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    panic!("event collector lagged by {n}: assertion would be unsound")
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });
    events
}

#[derive(Debug, Default, Clone, Copy)]
struct Finished {
    shuffle_read_bytes: i64,
    shuffle_write_bytes: i64,
    input_rows: i64,
    output_rows: i64,
}

/// Run the shuffle join on two workers with telemetry wired and return the LAST
/// StageFinished counters per stage (a startup-race retry re-runs the whole query and
/// re-emits; the successful run's events come last).
async fn run_join_observed(total_rows: i64, custs: i64) -> HashMap<i32, Finished> {
    let ep0 = start_worker(orders(0, total_rows / 2, custs), customer(0, custs / 2)).await;
    let ep1 = start_worker(
        orders(total_rows / 2, total_rows, custs),
        customer(custs / 2, custs),
    )
    .await;
    let cluster = Cluster::new(vec![ep0, ep1]);
    let store = Arc::new(AppStateStore::new());
    let events = collect_events(&store);
    let stages = join_stages();
    let mut done = false;
    for _ in 0..50 {
        match run_stages_obs(
            &cluster,
            &stages,
            Some(store.clone()),
            Some("stage-finished-counters".into()),
            None,
        )
        .await
        {
            Ok(_) => {
                done = true;
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    }
    assert!(done, "distributed join never succeeded");
    // Let the collector drain the broadcast channel before reading.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let mut finished: HashMap<i32, Finished> = HashMap::new();
    for ev in events.lock().expect("events poisoned").iter() {
        if let ExecutionEvent::StageFinished {
            stage_id,
            shuffle_read_bytes,
            shuffle_write_bytes,
            input_rows,
            output_rows,
            ..
        } = ev
        {
            finished.insert(
                *stage_id,
                Finished {
                    shuffle_read_bytes: *shuffle_read_bytes,
                    shuffle_write_bytes: *shuffle_write_bytes,
                    input_rows: *input_rows,
                    output_rows: *output_rows,
                },
            );
        }
    }
    finished
}

/// Assert the full counter contract over one observed run.
fn assert_real_counters(finished: &HashMap<i32, Finished>, total_rows: i64, custs: i64) {
    for stage_id in [0, 1, 2] {
        assert!(
            finished.contains_key(&stage_id),
            "stage {stage_id} must emit StageFinished"
        );
    }
    let s0 = finished[&0];
    let s1 = finished[&1];
    let s2 = finished[&2];
    assert_eq!(
        s0.output_rows, total_rows,
        "stage 0 shuffles all orders rows"
    );
    assert_eq!(s1.output_rows, custs, "stage 1 shuffles all customer rows");
    assert_eq!(
        (s0.input_rows, s1.input_rows),
        (0, 0),
        "leaf stages read base tables, not shuffle input"
    );
    assert!(
        s0.shuffle_write_bytes > 0 && s1.shuffle_write_bytes > 0,
        "producers must report real shuffle-write bytes: {} / {}",
        s0.shuffle_write_bytes,
        s1.shuffle_write_bytes
    );
    assert_eq!(
        s2.input_rows,
        total_rows + custs,
        "the join stage's input is both upstreams' measured output"
    );
    assert_eq!(
        s2.shuffle_read_bytes,
        s0.shuffle_write_bytes + s1.shuffle_write_bytes,
        "the join stage reads exactly what its upstreams wrote"
    );
    assert_eq!(
        s2.output_rows, custs,
        "every custkey joins — the group-by emits one row per key"
    );
}

/// Default gates (AQE + stage-input stats on): every StageFinished counter is real.
#[tokio::test]
async fn stage_finished_events_carry_real_counters() {
    let _guard = ENV_LOCK.lock().await;
    std::env::remove_var("OXIDANT_AQE");
    std::env::remove_var("OXIDANT_STAGE_INPUT_STATS");
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "2");
    const CUSTS: i64 = 200;
    const ORDERS: i64 = 2_000;
    let finished = run_join_observed(ORDERS, CUSTS).await;
    std::env::remove_var("OXIDANT_SHUFFLE_PARTITIONS");
    assert_real_counters(&finished, ORDERS, CUSTS);
}

/// `OXIDANT_AQE=0` turns off the coalesce decision but NOT the barrier sample — the
/// StageFinished counters stay real.
#[tokio::test]
async fn stage_finished_counters_still_real_with_aqe_disabled() {
    let _guard = ENV_LOCK.lock().await;
    std::env::set_var("OXIDANT_AQE", "0");
    std::env::remove_var("OXIDANT_STAGE_INPUT_STATS");
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "2");
    const CUSTS: i64 = 200;
    const ORDERS: i64 = 2_000;
    let finished = run_join_observed(ORDERS, CUSTS).await;
    std::env::remove_var("OXIDANT_AQE");
    std::env::remove_var("OXIDANT_SHUFFLE_PARTITIONS");
    assert_real_counters(&finished, ORDERS, CUSTS);
}

/// Both sampling consumers off (`OXIDANT_AQE=0` + `OXIDANT_STAGE_INPUT_STATS=0`): the
/// barrier still samples for telemetry alone, so counters never regress to the
/// pre-KAN-145 zeros.
#[tokio::test]
async fn stage_finished_counters_survive_all_sampling_gates_off() {
    let _guard = ENV_LOCK.lock().await;
    std::env::set_var("OXIDANT_AQE", "0");
    std::env::set_var("OXIDANT_STAGE_INPUT_STATS", "0");
    std::env::set_var("OXIDANT_SHUFFLE_PARTITIONS", "2");
    const CUSTS: i64 = 200;
    const ORDERS: i64 = 2_000;
    let finished = run_join_observed(ORDERS, CUSTS).await;
    std::env::remove_var("OXIDANT_AQE");
    std::env::remove_var("OXIDANT_STAGE_INPUT_STATS");
    std::env::remove_var("OXIDANT_SHUFFLE_PARTITIONS");
    assert_real_counters(&finished, ORDERS, CUSTS);
}
