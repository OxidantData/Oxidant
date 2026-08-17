//! KAN-18 / KAN-19: finished or failed distributed queries must not leak worker-side state —
//! cached stage outputs (KAN-18) or `shuffle_input` MemTables in the shared worker session
//! (KAN-19).
//!
//! Own integration binary so ports / engine state don't race other tests.

use std::sync::Arc;

use oxidant_execution::driver::{run_stages, Cluster, StageDef};
use oxidant_execution::flight::{
    health_check_worker, pull_bucket, run_stage_on_worker, serve_worker,
};
use oxidant_execution::shuffle::protocol::StageTicket;
use oxidant_loom::Engine;

async fn start_worker() -> (String, Arc<Engine>) {
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let engine = Arc::new(Engine::new());
    let worker_engine = engine.clone();
    tokio::spawn(async move {
        let _ = serve_worker(port, worker_engine).await;
    });
    let endpoint = format!("http://127.0.0.1:{port}");
    for _ in 0..50 {
        if health_check_worker(endpoint.clone()).await.is_ok() {
            return (endpoint, engine);
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("worker did not become ready at {endpoint}");
}

/// A live cache entry serves typed batches (the producer schema); a cleared entry round-trips
/// as a schema-less placeholder batch (see `Worker::read_shuffle` / `do_get_batches_once`).
async fn assert_stage_cache_cleared(endpoint: &str, stage_id: u32, num_partitions: u32) {
    for p in 0..num_partitions {
        let pulled = pull_bucket(endpoint.to_string(), stage_id, p)
            .await
            .unwrap();
        assert!(
            pulled.iter().all(|b| b.schema().fields().is_empty()),
            "stage {stage_id} partition {p} still cached after query exit"
        );
    }
}

#[tokio::test]
async fn driver_clears_stage_cache_on_stage_error() {
    let (endpoint, _engine) = start_worker().await;
    let cluster = Cluster::new(vec![endpoint.clone()]);
    let stages = vec![
        StageDef::new(0, "SELECT 1 AS k, 2 AS v", vec![], vec![0]),
        // Non-retryable planning error (missing table) in the output stage.
        StageDef::new(1, "SELECT * FROM missing_table", vec![0], vec![]),
    ];
    let err = run_stages(&cluster, &stages)
        .await
        .expect_err("output stage references a missing table")
        .to_string();
    assert!(err.contains("missing_table"), "unexpected error: {err}");
    // KAN-18: the error exit path must still evict the producer's cached buckets.
    assert_stage_cache_cleared(&endpoint, 0, cluster.num_partitions).await;
}

#[tokio::test]
async fn driver_clears_stage_cache_on_success() {
    let (endpoint, _engine) = start_worker().await;
    let cluster = Cluster::new(vec![endpoint.clone()]);
    let stages = vec![
        StageDef::new(0, "SELECT 1 AS k, 2 AS v", vec![], vec![0]),
        StageDef::new(
            1,
            "SELECT k, sum(v) AS s FROM shuffle_input GROUP BY k",
            vec![0],
            vec![],
        ),
    ];
    run_stages(&cluster, &stages)
        .await
        .expect("two-stage query");
    // KAN-18 regression: the success path still evicts stage caches (moved into the
    // `run_stages_obs` wrapper so every exit path shares it).
    assert_stage_cache_cleared(&endpoint, 0, cluster.num_partitions).await;
}

/// The end-of-query sweep must reach EVERY worker, not just the first: with two workers the
/// producer caches buckets on both, and both must be evicted on the success path.
#[tokio::test]
async fn driver_clears_stage_cache_on_every_worker() {
    let (ep0, _e0) = start_worker().await;
    let (ep1, _e1) = start_worker().await;
    let cluster = Cluster::new(vec![ep0.clone(), ep1.clone()]);
    let stages = vec![
        StageDef::new(0, "SELECT 1 AS k, 2 AS v", vec![], vec![0]),
        StageDef::new(
            1,
            "SELECT k, sum(v) AS s FROM shuffle_input GROUP BY k",
            vec![0],
            vec![],
        ),
    ];
    run_stages(&cluster, &stages)
        .await
        .expect("two-stage query over two workers");
    for ep in [&ep0, &ep1] {
        assert_stage_cache_cleared(ep, 0, cluster.num_partitions).await;
    }
}

#[tokio::test]
async fn shuffle_input_deregistered_after_stage_exit() {
    let (endpoint, engine) = start_worker().await;
    let producer = StageTicket {
        stage_id: 0,
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
    };
    run_stage_on_worker(endpoint.clone(), producer)
        .await
        .unwrap();

    let consumer = |stage_id: u32, sql: &str| StageTicket {
        stage_id,
        partition_id: 0,
        num_partitions: 1,
        upstream_endpoints: vec![endpoint.clone()],
        stage_sql: sql.into(),
        plan_fragment: vec![],
        hash_key_cols: vec![],
        upstream_stage_ids: vec![0],
        produce: false,
        lakehouse_snapshot_pins: String::new(),
        replicated_tables: String::new(),
        coalesce_read_modulus: 0,
        forward_upstream_stage_ids: vec![],
        upstream_bucket_rows: vec![],
        lakeformation_required: false,
        lakeformation_principal: String::new(),
    };

    // Success path: the table is dropped when the stage task returns.
    let out = run_stage_on_worker(
        endpoint.clone(),
        consumer(1, "SELECT k, v FROM shuffle_input"),
    )
    .await
    .unwrap();
    assert_eq!(out.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert!(
        engine.sql("SELECT * FROM shuffle_input").await.is_err(),
        "shuffle_input left registered after a successful stage"
    );

    // Error path (bad column): the registration is still rolled back.
    run_stage_on_worker(
        endpoint.clone(),
        consumer(2, "SELECT nope FROM shuffle_input"),
    )
    .await
    .expect_err("unknown column must fail the stage");
    assert!(
        engine.sql("SELECT * FROM shuffle_input").await.is_err(),
        "shuffle_input left registered after a failed stage"
    );
}
