//! KAN-47: a slow-but-progressing stage — every progress gap far below the no-progress
//! budget, but a total runtime of several budget lengths — is NOT aborted by the
//! worker-side watchdog.
//!
//! Own integration binary so the env knobs don't race other tests (process-global env).

use std::sync::Arc;

use oxidant_execution::flight::{health_check_worker, run_stage_on_worker, serve_worker};
use oxidant_execution::shuffle::protocol::StageTicket;
use oxidant_loom::Engine;

fn slow_stage_ticket(stage_id: u32) -> StageTicket {
    StageTicket {
        stage_id,
        partition_id: 0,
        num_partitions: 1,
        upstream_endpoints: vec![],
        stage_sql: "SELECT * FROM range(20)".into(),
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
async fn slow_but_progressing_stage_is_not_aborted() {
    // `range(20)` with batch size 1 yields 20 one-row batches; the 150 ms per-batch test
    // delay keeps every progress gap far below the 1 s no-progress budget while the whole
    // stage runs ~3 s — three budget lengths. The watchdog must stay quiet.
    std::env::set_var("OXIDANT_STAGE_TIMEOUT_MS", "30000");
    std::env::set_var("OXIDANT_STAGE_NO_PROGRESS_SECS", "1");
    std::env::set_var("OXIDANT_BATCH_SIZE", "1");
    std::env::set_var("OXIDANT_TEST_STAGE_BATCH_DELAY_MS", "150");

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
    let batches = run_stage_on_worker(endpoint, slow_stage_ticket(903))
        .await
        .expect("a progressing stage must not be aborted by the watchdog");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 20, "the stage must return all of range(20)");
    assert!(
        start.elapsed() >= std::time::Duration::from_millis(2500),
        "the per-batch delays should make the stage outlast several no-progress budgets"
    );

    std::env::remove_var("OXIDANT_STAGE_TIMEOUT_MS");
    std::env::remove_var("OXIDANT_STAGE_NO_PROGRESS_SECS");
    std::env::remove_var("OXIDANT_BATCH_SIZE");
    std::env::remove_var("OXIDANT_TEST_STAGE_BATCH_DELAY_MS");
}
