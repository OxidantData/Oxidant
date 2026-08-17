//! KAN-39: a cached shuffle entry left behind by a previous (timed-out/cancelled) query must
//! never be unioned into a later query's same-id stage read, and benign schema drift between
//! shuffle-input batches must not fail `shuffle_input` MemTable registration.
//!
//! Stage ids repeat across queries (the planner numbers each plan from 0), and a producer of
//! a query that hit a client-side timeout can insert its cache entry after the driver's
//! best-effort stage cleanup raced past it. `Worker::read_shuffle` used to union *every*
//! entry matching the stage id; when the leftover's schema differed, the consumer's
//! `register_batches` built a MemTable over mixed-schema batches and failed with "Mismatch
//! between schema and batches" (observed on a TPC-H Q4 re-run right after Q18/Q22 timeouts).
//! Own integration binary so ports / engine state don't race other tests.

use std::sync::Arc;

use oxidant_execution::flight::{
    health_check_worker, pull_bucket, run_stage_on_worker, serve_worker,
};
use oxidant_execution::shuffle::protocol::StageTicket;
use oxidant_loom::arrow::array::Int64Array;
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;

async fn start_worker(engine: Arc<Engine>) -> String {
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    tokio::spawn(async move {
        let _ = serve_worker(port, engine).await;
    });
    let endpoint = format!("http://127.0.0.1:{port}");
    for _ in 0..50 {
        if health_check_worker(endpoint.clone()).await.is_ok() {
            return endpoint;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("worker did not become ready at {endpoint}");
}

fn producer(stage_id: u32, partition_id: u32, sql: &str) -> StageTicket {
    StageTicket {
        stage_id,
        partition_id,
        num_partitions: 1,
        upstream_endpoints: vec![],
        stage_sql: sql.into(),
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

/// The cache state the KAN-39 race produces: a previous query's stage-1 entry under producer
/// scope 0 (schema `a: Int64`) lingers, and the current query's stage 1 inserts under scope 1
/// (schema `b: Utf8`). The pull must serve only the freshest entry's batches; before the fix
/// the read unioned both, handing the consumer a mixed-schema bucket set.
#[tokio::test]
async fn stale_entry_from_prior_query_is_not_served() {
    let endpoint = start_worker(Arc::new(Engine::new())).await;
    run_stage_on_worker(endpoint.clone(), producer(1, 0, "SELECT 1 AS a"))
        .await
        .unwrap();
    run_stage_on_worker(endpoint.clone(), producer(1, 1, "SELECT 'x' AS b"))
        .await
        .unwrap();

    let pulled = pull_bucket(endpoint, 1, 0).await.unwrap();
    assert!(!pulled.is_empty(), "fresh bucket must be served");
    for b in &pulled {
        let schema = b.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec!["b"],
            "stale schema leaked into the read: {names:?}"
        );
    }
    let rows: usize = pulled.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1, "exactly the fresh row must be served");
}

/// A typed zero-row bucket (KAN-28) must flow through shuffle-input assembly: the consumer
/// registers `shuffle_input` from the schema-carrying empty batch and runs its SQL.
#[tokio::test]
async fn empty_typed_bucket_flows_through_shuffle_input() {
    let engine = Arc::new(Engine::new());
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let vals = Int64Array::from(vec![1, 2, 3]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(vals)]).unwrap();
    engine.register_batches("t", vec![batch]).unwrap();
    let endpoint = start_worker(engine).await;
    run_stage_on_worker(
        endpoint.clone(),
        producer(0, 0, "SELECT v FROM t WHERE v > 100"),
    )
    .await
    .unwrap();

    let consumer = StageTicket {
        stage_id: 1,
        partition_id: 0,
        num_partitions: 1,
        upstream_endpoints: vec![endpoint.clone()],
        stage_sql: "SELECT count(*) AS c FROM shuffle_input".into(),
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
    let out = run_stage_on_worker(endpoint, consumer)
        .await
        .expect("typed empty shuffle input must register and run");
    let c: i64 = out
        .iter()
        .map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        })
        .sum();
    assert_eq!(c, 0);
}
