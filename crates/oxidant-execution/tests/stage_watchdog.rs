//! KAN-47: a stage that makes no progress (batch heartbeat, memory-pool activity, and
//! spill bytes all frozen) is aborted by the worker-side watchdog well before the
//! wall-clock stage timeout, with an actionable error naming the deadlock class.
//!
//! Own integration binary so the env knobs don't race other tests (process-global env).

use std::sync::Arc;

use oxidant_execution::flight::{
    health_check_worker, heartbeat_worker, run_stage_on_worker, serve_worker,
};
use oxidant_execution::shuffle::protocol::StageTicket;
use oxidant_loom::Engine;

fn stalled_stage_ticket(stage_id: u32) -> StageTicket {
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
        lakeformation_required: false,
        lakeformation_principal: String::new(),
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
async fn no_progress_watchdog_aborts_stalled_stage() {
    // Phase 1: the stage body sleeps 30 s via the test hook (zero progress signals); the
    // 2 s no-progress budget must cut it off long before the 60 s wall-clock timeout.
    std::env::set_var("OXIDANT_STAGE_TIMEOUT_MS", "60000");
    std::env::set_var("OXIDANT_STAGE_NO_PROGRESS_SECS", "2");
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

    let start = std::time::Instant::now();
    let err = run_stage_on_worker(endpoint.clone(), stalled_stage_ticket(902))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("no progress"),
        "expected no-progress watchdog abort, got: {err}"
    );
    assert!(
        err.contains("KAN-47"),
        "error should name the deadlock class, got: {err}"
    );
    assert!(
        start.elapsed() < std::time::Duration::from_secs(15),
        "watchdog should cut off the 30 s stall well before the 60 s timeout"
    );

    // The aborted task dropped its slot guard: the worker is back to 0 used slots.
    let hb = heartbeat_worker(endpoint.clone()).await.unwrap();
    assert_eq!(hb.slots_used, Some(0));

    // Phase 2 (fail-before analog / guard): with the watchdog budget far above the
    // wall-clock timeout, the same stalled stage still exits via the KAN-17 timeout path —
    // the "no progress" error comes only from the watchdog, never shadowing the timeout.
    std::env::set_var("OXIDANT_STAGE_TIMEOUT_MS", "1500");
    std::env::set_var("OXIDANT_STAGE_NO_PROGRESS_SECS", "300");
    let err = run_stage_on_worker(endpoint, stalled_stage_ticket(904))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("timed out"),
        "expected stage-timeout error, got: {err}"
    );
    assert!(
        !err.contains("no progress"),
        "timeout path must stay distinct from the watchdog, got: {err}"
    );

    std::env::remove_var("OXIDANT_STAGE_TIMEOUT_MS");
    std::env::remove_var("OXIDANT_STAGE_NO_PROGRESS_SECS");
    std::env::remove_var("OXIDANT_TEST_STAGE_DELAY_MS");
}
