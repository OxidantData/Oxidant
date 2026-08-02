//! KAN-53: a stage task aborted by the KAN-47 no-progress watchdog is retried ONCE on the
//! worker with the flipped join strategy (`weft_loom::with_join_strategy_flipped`) before
//! the query is failed — the wedge class the watchdog catches is strategy-dependent
//! (hash-build memory pressure vs. sort-merge spill).
//!
//! Own integration binary so the env knobs don't race other tests (process-global env).

use std::sync::Arc;

use weft_execution::flight::{
    health_check_worker, heartbeat_worker, run_stage_on_worker, serve_worker,
};
use weft_execution::shuffle::protocol::StageTicket;
use weft_loom::Engine;

fn stage_ticket(stage_id: u32, sql: &str) -> StageTicket {
    StageTicket {
        stage_id,
        partition_id: 0,
        num_partitions: 1,
        upstream_endpoints: vec![],
        stage_sql: sql.into(),
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
async fn watchdog_abort_retries_stage_once_with_flipped_join_strategy() {
    std::env::set_var("WEFT_STAGE_TIMEOUT_MS", "60000");
    std::env::set_var("WEFT_STAGE_NO_PROGRESS_SECS", "2");

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

    // Phase 1 (fail-before/pass-after): the first attempt stalls (zero progress signals)
    // via the once-only test hook; the 2 s watchdog aborts it, and the KAN-53 retry —
    // running with the flipped join strategy — runs the real stage body and completes.
    // Before KAN-53 the abort surfaced directly and this call errored.
    std::env::set_var("WEFT_TEST_STAGE_STALL_ONCE_MS", "30000");
    let start = std::time::Instant::now();
    let batches = run_stage_on_worker(endpoint.clone(), stage_ticket(910, "SELECT 1 AS v"))
        .await
        .expect("watchdog-aborted stage must be retried once and complete");
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert!(
        start.elapsed() < std::time::Duration::from_secs(30),
        "the retry must complete without waiting out the 30 s stall"
    );

    // The retried task dropped its slot guard: the worker is back to 0 used slots.
    let hb = heartbeat_worker(endpoint.clone()).await.unwrap();
    assert_eq!(hb.slots_used, Some(0));

    // Phase 2: a stage that stalls under BOTH strategies (the always-on delay hook)
    // fails after exactly one retry — the watchdog aborts each attempt in ~2 s, so two
    // attempts finish well under the 60 s wall-clock timeout, and the final error is
    // still the actionable no-progress abort (never a silent hang or a retry loop).
    std::env::set_var("WEFT_TEST_STAGE_DELAY_MS", "30000");
    let start = std::time::Instant::now();
    let err = run_stage_on_worker(endpoint.clone(), stage_ticket(911, "SELECT 2 AS v"))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("no progress"),
        "a twice-stalled stage must end in the no-progress abort, got: {err}"
    );
    assert!(
        start.elapsed() < std::time::Duration::from_secs(15),
        "exactly one retry (two ~2 s attempts) must bound the failure"
    );

    let hb = heartbeat_worker(endpoint.clone()).await.unwrap();
    assert_eq!(hb.slots_used, Some(0));

    std::env::remove_var("WEFT_STAGE_TIMEOUT_MS");
    std::env::remove_var("WEFT_STAGE_NO_PROGRESS_SECS");
    std::env::remove_var("WEFT_TEST_STAGE_DELAY_MS");
    std::env::remove_var("WEFT_TEST_STAGE_STALL_ONCE_MS");
}
