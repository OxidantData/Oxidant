//! Non-aggregate scan queries: scatter per worker + global ORDER BY / LIMIT.

use std::sync::Arc;

use oxidant_execution::driver::{run_stages, Cluster};
use oxidant_execution::flight::serve_worker;
use oxidant_execution::plan::plan_distributed;
use oxidant_loom::arrow::array::Int64Array;
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;

fn make_batch(start: i64, end: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let ks: Vec<i64> = (start..end).map(|i| i % 5).collect();
    let vs: Vec<i64> = (start..end).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ks)),
            Arc::new(Int64Array::from(vs)),
        ],
    )
    .unwrap()
}

fn rows(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let v = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((k.value(i), v.value(i)));
        }
    }
    out
}

#[tokio::test]
async fn two_worker_scan_with_limit_matches_single_node() {
    const N: i64 = 100;
    let query = "SELECT k, v FROM t WHERE v > 10 ORDER BY v LIMIT 5";

    let single = Engine::new();
    single
        .register_batches("t", vec![make_batch(0, N)])
        .unwrap();
    let expected = rows(&single.sql(query).await.unwrap());

    let p0 = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let p1 = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let e0 = Arc::new(Engine::new());
    e0.register_batches("t", vec![make_batch(0, N / 2)])
        .unwrap();
    let e1 = Arc::new(Engine::new());
    e1.register_batches("t", vec![make_batch(N / 2, N)])
        .unwrap();
    tokio::spawn(async move {
        let _ = serve_worker(p0, e0).await;
    });
    tokio::spawn(async move {
        let _ = serve_worker(p1, e1).await;
    });

    let cluster = Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ]);

    let dq = plan_distributed(&single, query, &[])
        .await
        .expect("scan should plan");

    let mut gathered = None;
    for _ in 0..50 {
        match run_stages(&cluster, &dq.stages).await {
            Ok(b) => {
                gathered = Some(b);
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    }
    let gathered = gathered.expect("distributed scan never succeeded");
    let actual = match &dq.finalize_sql {
        None => gathered,
        Some(fsql) => {
            let fin = Engine::new();
            fin.register_batches("result", gathered).unwrap();
            fin.sql(fsql).await.expect("finalize")
        }
    };

    assert_eq!(
        rows(&actual),
        expected,
        "scan + global LIMIT must match single-node"
    );
}
