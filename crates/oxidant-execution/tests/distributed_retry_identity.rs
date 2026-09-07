//! OxidantData/Oxidant#182: a leaf scatter ticket that fails on its assigned worker
//! must not succeed by running the same SQL on an alternate worker's local table.

use std::sync::Arc;

use oxidant_execution::driver::{run_stages, Cluster, StageDef};
use oxidant_execution::flight::{health_check_worker, serve_worker};
use oxidant_loom::arrow::array::Int64Array;
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;

fn partition_batch(ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .unwrap()
}

fn collected_ids(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id Int64");
        for i in 0..b.num_rows() {
            out.push(col.value(i));
        }
    }
    out.sort_unstable();
    out
}

async fn wait_ready(endpoint: &str) {
    let mut up = false;
    for _ in 0..50 {
        if health_check_worker(endpoint.to_string()).await.is_ok() {
            up = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(up, "worker did not become ready at {endpoint}");
}

#[tokio::test]
async fn absent_worker_local_partition_does_not_duplicate_the_survivor() {
    std::env::set_var("OXIDANT_WORKER_COUNT", "2");
    std::env::set_var("OXIDANT_TASK_MAX_RETRIES", "1");
    std::env::set_var("OXIDANT_SPECULATIVE", "false");
    std::env::set_var("OXIDANT_DISTRIBUTED_STRICT", "1");
    std::env::set_var("OXIDANT_STAGE_TIMEOUT_MS", "5000");

    let p0 = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let p1 = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let dead = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };

    let e0 = Arc::new(Engine::new());
    e0.register_batches("partition_rows", vec![partition_batch(&[1, 2, 3])])
        .unwrap();
    let e1 = Arc::new(Engine::new());
    e1.register_batches("partition_rows", vec![partition_batch(&[101, 102, 103])])
        .unwrap();

    tokio::spawn(async move {
        let _ = serve_worker(p0, e0).await;
    });
    tokio::spawn(async move {
        let _ = serve_worker(p1, e1).await;
    });

    let ep0 = format!("http://127.0.0.1:{p0}");
    let ep1 = format!("http://127.0.0.1:{p1}");
    wait_ready(&ep0).await;
    wait_ready(&ep1).await;

    let healthy = Cluster::new(vec![ep0.clone(), ep1.clone()]);
    let healthy_out = run_stages(
        &healthy,
        &[StageDef::new(
            71001,
            "SELECT id FROM partition_rows",
            vec![],
            vec![],
        )],
    )
    .await
    .expect("two healthy workers");
    let expected = vec![1, 2, 3, 101, 102, 103];
    assert_eq!(
        collected_ids(&healthy_out),
        expected,
        "healthy scatter must return each partition exactly once"
    );

    let broken = Cluster::new(vec![format!("http://127.0.0.1:{dead}"), ep1]);
    let result = run_stages(
        &broken,
        &[StageDef::new(
            71002,
            "SELECT id FROM partition_rows",
            vec![],
            vec![],
        )],
    )
    .await;

    match result {
        Ok(batches) => {
            let got = collected_ids(&batches);
            assert_ne!(
                got,
                vec![101, 101, 102, 102, 103, 103],
                "relocating the missing partition onto the survivor duplicated its rows"
            );
            assert_eq!(
                got, expected,
                "success is only allowed with the complete reference, never a partial duplicate"
            );
        }
        Err(e) => {
            let msg = e.to_string();
            assert!(!msg.is_empty(), "refusal must be an explicit error");
        }
    }

    std::env::remove_var("OXIDANT_WORKER_COUNT");
    std::env::remove_var("OXIDANT_TASK_MAX_RETRIES");
    std::env::remove_var("OXIDANT_SPECULATIVE");
    std::env::remove_var("OXIDANT_DISTRIBUTED_STRICT");
    std::env::remove_var("OXIDANT_STAGE_TIMEOUT_MS");
}
