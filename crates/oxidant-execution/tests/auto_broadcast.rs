//! Auto-broadcast dims from Parquet sizes (KAN-1 / E-DIST-BCAST): no `OXIDANT_REPLICATED_TABLES`.

use std::sync::Arc;

use datafusion::parquet::arrow::ArrowWriter;
use oxidant_execution::driver::{run_stages, Cluster};
use oxidant_execution::flight::serve_worker;
use oxidant_execution::plan::{plan_distributed, resolve_replicated_tables};
use oxidant_loom::arrow::array::Int64Array;
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;

fn ephemeral_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn write_parquet(path: &std::path::Path, batch: &RecordBatch) {
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

fn fact_batch(n: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values((0..n).map(|i| i % 8))),
            Arc::new(Int64Array::from_iter_values(0..n)),
        ],
    )
    .unwrap()
}

fn dim_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("d_key", DataType::Int64, false),
        Field::new("d_name", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(0..8)),
            Arc::new(Int64Array::from_iter_values((0..8).map(|i| i * 10))),
        ],
    )
    .unwrap()
}

fn sorted_lines(batches: &[RecordBatch]) -> Vec<String> {
    let mut lines = Vec::new();
    for b in batches {
        let name = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let sv = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            lines.push(format!("{},{}", name.value(i), sv.value(i)));
        }
    }
    lines.sort();
    lines
}

#[tokio::test]
async fn resolve_replicated_from_parquet_sizes_without_env() {
    std::env::remove_var("OXIDANT_REPLICATED_TABLES");
    let dir = tempfile::tempdir().unwrap();
    let fact_path = dir.path().join("fact.parquet");
    let dim_path = dir.path().join("dim.parquet");
    // Fact must be clearly larger so it is the unique max.
    write_parquet(&fact_path, &fact_batch(2_000));
    write_parquet(&dim_path, &dim_batch());

    let engine = Engine::new();
    engine
        .register_parquet("fact", fact_path.to_str().unwrap())
        .await
        .unwrap();
    engine
        .register_parquet("dim", dim_path.to_str().unwrap())
        .await
        .unwrap();

    let sql = "SELECT d.d_name AS name, SUM(f.v) AS sv \
               FROM fact f JOIN dim d ON f.k = d.d_key GROUP BY d.d_name";
    let lp = engine.logical_plan(sql).await.unwrap();
    let replicated = resolve_replicated_tables(&engine, &lp).await;
    assert!(
        replicated.iter().any(|t| t == "dim"),
        "dim should auto-replicate: {replicated:?}"
    );
    assert!(
        !replicated.iter().any(|t| t == "fact"),
        "fact (largest) must stay sharded: {replicated:?}"
    );

    let dq = plan_distributed(
        &engine,
        sql,
        &replicated.iter().map(String::as_str).collect::<Vec<_>>(),
    )
    .await
    .expect("auto-broadcast star should plan");
    assert!(
        dq.stages
            .iter()
            .all(|s| s.replicated_tables.contains("dim")),
        "stages must carry classified replicate set"
    );
}

#[tokio::test]
async fn auto_broadcast_parquet_distributed_matches_single_node() {
    std::env::remove_var("OXIDANT_REPLICATED_TABLES");
    let dir = tempfile::tempdir().unwrap();
    let fact_path = dir.path().join("fact.parquet");
    let dim_path = dir.path().join("dim.parquet");
    write_parquet(&fact_path, &fact_batch(400));
    write_parquet(&dim_path, &dim_batch());

    let sql = "SELECT d.d_name AS name, SUM(f.v) AS sv \
               FROM fact f JOIN dim d ON f.k = d.d_key GROUP BY d.d_name";

    let single = Engine::new();
    single
        .register_parquet("fact", fact_path.to_str().unwrap())
        .await
        .unwrap();
    single
        .register_parquet("dim", dim_path.to_str().unwrap())
        .await
        .unwrap();
    let expected = single.sql(sql).await.unwrap();

    let lp = single.logical_plan(sql).await.unwrap();
    let replicated = resolve_replicated_tables(&single, &lp).await;
    assert!(replicated.iter().any(|t| t == "dim"));

    // Manual shard of fact batches + full dim on each worker (same layout as existing
    // broadcast tests); planning uses auto-resolved replicate list, not a hard-coded env.
    let full_fact = fact_batch(400);
    let mid = 200;
    let schema = full_fact.schema();
    let k = full_fact
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let v = full_fact
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let shard = |start: i64, end: i64| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    (start..end).map(|i| k.value(i as usize)),
                )),
                Arc::new(Int64Array::from_iter_values(
                    (start..end).map(|i| v.value(i as usize)),
                )),
            ],
        )
        .unwrap()
    };

    let p0 = ephemeral_port();
    let p1 = ephemeral_port();
    let e0 = Arc::new(Engine::new());
    e0.register_batches("fact", vec![shard(0, mid)]).unwrap();
    e0.register_batches("dim", vec![dim_batch()]).unwrap();
    let e1 = Arc::new(Engine::new());
    e1.register_batches("fact", vec![shard(mid, 400)]).unwrap();
    e1.register_batches("dim", vec![dim_batch()]).unwrap();
    tokio::spawn(async move {
        let _ = serve_worker(p0, e0).await;
    });
    tokio::spawn(async move {
        let _ = serve_worker(p1, e1).await;
    });
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(("127.0.0.1", p0))
            .await
            .is_ok()
            && tokio::net::TcpStream::connect(("127.0.0.1", p1))
                .await
                .is_ok()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let cluster = Cluster::new(vec![
        format!("http://127.0.0.1:{p0}"),
        format!("http://127.0.0.1:{p1}"),
    ]);
    let refs: Vec<&str> = replicated.iter().map(String::as_str).collect();
    let dq = plan_distributed(&single, sql, &refs)
        .await
        .expect("auto-broadcast plan");

    let mut gathered = None;
    for _ in 0..150 {
        if let Ok(b) = run_stages(&cluster, &dq.stages).await {
            gathered = Some(b);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let actual = gathered.expect("distributed auto-broadcast never succeeded");
    assert_eq!(
        sorted_lines(&actual),
        sorted_lines(&expected),
        "auto-broadcast (no OXIDANT_REPLICATED_TABLES) must match single-node"
    );
}
