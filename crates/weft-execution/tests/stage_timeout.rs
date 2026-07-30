//! KAN-17: a stage that exceeds `WEFT_STAGE_TIMEOUT_MS` errors out non-retryably and its
//! worker task slot frees, instead of running forever.
//!
//! Own integration binary so the env knobs don't race other tests (process-global env).

use std::sync::Arc;

use weft_execution::flight::{
    health_check_worker, heartbeat_worker, run_stage_on_worker, serve_worker,
};
use weft_execution::shuffle::protocol::StageTicket;
use weft_loom::Engine;

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
async fn stage_timeout_errors_and_frees_slot() {
    // The stage body sleeps 30 s via the test hook; the 500 ms server-side stage timeout
    // must cut it off.
    std::env::set_var("WEFT_STAGE_TIMEOUT_MS", "500");
    std::env::set_var("WEFT_TEST_STAGE_DELAY_MS", "30000");

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

    let start = std::time::Instant::now();
    let err = run_stage_on_worker(endpoint.clone(), slow_stage_ticket(900))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("timed out"),
        "expected stage-timeout error, got: {err}"
    );
    assert!(
        start.elapsed() < std::time::Duration::from_secs(10),
        "stage timeout should cut off the 30 s stage promptly"
    );

    // The timed-out task dropped its slot guard: the worker is back to 0 used slots.
    let hb = heartbeat_worker(endpoint).await.unwrap();
    assert_eq!(hb.slots_used, Some(0));

    std::env::remove_var("WEFT_STAGE_TIMEOUT_MS");
    std::env::remove_var("WEFT_TEST_STAGE_DELAY_MS");
}
