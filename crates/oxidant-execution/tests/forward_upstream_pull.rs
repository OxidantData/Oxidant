//! R5-11: a `Forward`-mode upstream runs exactly once on one worker (the first entry of the
//! consumer's `upstream_endpoints`), yet consumers historically pulled its bucket from EVERY
//! endpoint and tolerated the `(workers - 1)` schema-less placeholder replies. A ticket
//! marking the upstream in `forward_upstream_stage_ids` must pull it from the producer
//! endpoint only — proven here by listing a dead endpoint second: the marked read never
//! dials it, while the legacy unmarked read fails on it.
//!
//! Own integration binary so ports / engine state don't race other tests.

use std::sync::Arc;

use oxidant_execution::flight::{health_check_worker, run_stage_on_worker, serve_worker};
use oxidant_execution::shuffle::protocol::StageTicket;
use oxidant_loom::arrow::array::Int64Array;
use oxidant_loom::Engine;

fn consumer_ticket(partition_id: u32, endpoints: Vec<String>, forward: Vec<u32>) -> StageTicket {
    StageTicket {
        stage_id: 1,
        partition_id,
        num_partitions: 2,
        upstream_endpoints: endpoints,
        stage_sql: "SELECT k, v FROM shuffle_input".into(),
        plan_fragment: vec![],
        hash_key_cols: vec![],
        upstream_stage_ids: vec![0],
        produce: false,
        lakehouse_snapshot_pins: String::new(),
        replicated_tables: String::new(),
        coalesce_read_modulus: 0,
        forward_upstream_stage_ids: forward,
        upstream_bucket_rows: vec![],
    }
}

#[tokio::test]
async fn forward_upstream_pulls_only_from_producer_endpoint() {
    // The producer endpoint: live worker running the "forward" stage exactly once.
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let engine = Arc::new(Engine::new());
    tokio::spawn(async move {
        let _ = serve_worker(port, engine).await;
    });
    let live = format!("http://127.0.0.1:{port}");
    let mut up = false;
    for _ in 0..50 {
        if health_check_worker(live.clone()).await.is_ok() {
            up = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(up, "worker did not become ready at {live}");

    // A second "worker" that never produced the stage — here, nothing listens at all, so any
    // pull attempt against it fails loudly instead of serving a placeholder.
    let dead = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        format!("http://127.0.0.1:{}", l.local_addr().unwrap().port())
    };

    // Stage 0 as a Forward producer: one invocation on the first endpoint, hash-partitioned
    // into 2 buckets.
    let producer = StageTicket {
        stage_id: 0,
        partition_id: 0,
        num_partitions: 2,
        upstream_endpoints: vec![],
        stage_sql: "SELECT CAST(k AS BIGINT) AS k, CAST(v AS BIGINT) AS v \
                    FROM (VALUES (1, 10), (2, 20), (3, 30)) AS t(k, v)"
            .into(),
        plan_fragment: vec![],
        hash_key_cols: vec![0],
        upstream_stage_ids: vec![],
        produce: true,
        lakehouse_snapshot_pins: String::new(),
        replicated_tables: String::new(),
        coalesce_read_modulus: 0,
        forward_upstream_stage_ids: vec![],
        upstream_bucket_rows: vec![],
    };
    run_stage_on_worker(live.clone(), producer)
        .await
        .expect("forward producer stage");

    // Marked consumers pull every bucket from the producer endpoint only: all rows exactly
    // once across the two read partitions, and the dead endpoint is never dialed.
    let mut seen = Vec::new();
    for p in 0..2 {
        let out = run_stage_on_worker(
            live.clone(),
            consumer_ticket(p, vec![live.clone(), dead.clone()], vec![0]),
        )
        .await
        .expect("marked forward read must not dial the dead endpoint");
        for b in &out {
            let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            seen.extend((0..k.len()).map(|i| k.value(i)));
        }
    }
    seen.sort_unstable();
    assert_eq!(seen, vec![1, 2, 3], "every bucket read exactly once");

    // Control: without the mark the consumer pulls EVERY endpoint and the dead one fails it.
    let err = run_stage_on_worker(
        live.clone(),
        consumer_ticket(0, vec![live.clone(), dead.clone()], vec![]),
    )
    .await
    .expect_err("legacy unmarked read must dial (and fail on) the dead endpoint");
    assert!(
        err.to_string().contains("connect worker"),
        "unexpected error: {err}"
    );
}
