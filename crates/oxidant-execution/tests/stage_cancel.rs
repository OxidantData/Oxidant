//! KAN-17: the `cancel_stage` Flight action aborts a running stage and frees its worker task
//! slot, so a cancelled query can't pin slots forever.
//!
//! Own integration binary so the env knobs don't race other tests (process-global env).

use std::sync::Arc;

use oxidant_execution::flight::{
    cancel_stage_on_worker, health_check_worker, heartbeat_worker, run_stage_on_worker,
    serve_worker,
};
use oxidant_execution::shuffle::protocol::StageTicket;
use oxidant_loom::Engine;

fn slow_stage_ticket(stage_id: u32) -> StageTicket {
    StageTicket {
        stage_id,
        partition_id: 0,
        num_partitions: 1,
        upstream_endpoints: vec![],
        stage_sql: "SELECT 1 AS v".into(),
        plan_fragment: vec![],
        hash_key_cols: vec![],
        upstream_stage_ids: vec![],
        produce: false,
        lakehouse_snapshot_pins: String::new(),
        replicated_tables: String::new(),
        coalesce_read_modulus: 0,
        forward_upstream_stage_ids: vec![],
        upstream_bucket_rows: vec![],
    }
}

async fn wait_worker_up(endpoint: &str) {
    for _ in 0..50 {
        if health_check_worker(endpoint.to_string()).await.is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("worker did not become ready at {endpoint}");
}

#[tokio::test]
async fn cancel_action_aborts_stage_and_frees_slot() {
    // The stage body sleeps 30 s via the test hook; the stage timeout stays out of the way so
    // only the cancel can cut it off.
    std::env::set_var("OXIDANT_STAGE_TIMEOUT_MS", "60000");
    std::env::set_var("OXIDANT_TEST_STAGE_DELAY_MS", "30000");

    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let engine = Arc::new(Engine::new());
    tokio::spawn(async move {
        let _ = serve_worker(port, engine).await;
    });
    let endpoint = format!("http://127.0.0.1:{port}");
    wait_worker_up(&endpoint).await;

    let stage = tokio::spawn(run_stage_on_worker(
        endpoint.clone(),
        slow_stage_ticket(901),
    ));

    // Wait until the worker is actually holding the slot for the stage.
    let mut held = false;
    for _ in 0..50 {
        let hb = heartbeat_worker(endpoint.clone()).await.unwrap();
        if hb.slots_used == Some(1) {
            held = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(held, "stage task never held a worker slot");

    cancel_stage_on_worker(endpoint.clone(), 901).await.unwrap();

    let err = stage.await.unwrap().unwrap_err().to_string();
    assert!(
        err.contains("cancelled"),
        "expected stage-cancelled error, got: {err}"
    );

    let hb = heartbeat_worker(endpoint).await.unwrap();
    assert_eq!(hb.slots_used, Some(0));

    std::env::remove_var("OXIDANT_STAGE_TIMEOUT_MS");
    std::env::remove_var("OXIDANT_TEST_STAGE_DELAY_MS");
}
