//! KAN-48: distributed stages over lakehouse (Delta/Iceberg) tables must carry the
//! driver-pinned snapshot identity, or the worker-side guard rejects the scan with
//! "distributed stage omitted the snapshot pin" (SF10 Glue `tpch_sf10_delta` /
//! `tpch_sf10_iceberg`: every TPC-H query failed while Parquet passed 22/22).
//!
//! These tests stand up two in-process Flight workers whose catalogs each resolve the
//! same logical Delta table to a *disjoint local shard directory* (mirroring the
//! per-worker file shards of a real cluster, where `WEFT_WORKER_COUNT`/`WEFT_SHARD_INDEX`
//! — process-global env — cannot differ across in-process workers), then run a
//! distributed GROUP BY through the full Spark Connect gRPC path.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sc::spark_connect_service_client::SparkConnectServiceClient;
use tonic::transport::Channel;
use weft_catalog::{CatalogProvider, Error, Result, TableFormat, TableMetadata};
use weft_connect::WeftService;
use weft_execution::flight::serve_worker;
use weft_loom::arrow::array::Int64Array;
use weft_loom::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::Engine;
use weft_proto::spark::connect as sc;

// Keep clear of distributed_pyspark.rs (50870–50878).
const PORT: u16 = 50890;
const W0: u16 = 50891;
const W1: u16 = 50892;
const PORT_DF: u16 = 50893;
const W0_DF: u16 = 50894;
const W1_DF: u16 = 50895;

const N: i64 = 100;

/// Minimal external catalog exposing one Delta table `glue.ns.t` at `location`.
#[derive(Debug)]
struct ShardCatalog {
    location: String,
    schema: SchemaRef,
}

#[async_trait]
impl CatalogProvider for ShardCatalog {
    fn name(&self) -> &str {
        "glue"
    }

    async fn list_namespaces(&self, _parent: &[String]) -> Result<Vec<Vec<String>>> {
        Ok(vec![vec!["ns".into()]])
    }

    async fn list_tables(&self, namespace: &[String]) -> Result<Vec<String>> {
        if namespace == ["ns"] {
            Ok(vec!["t".into()])
        } else {
            Ok(Vec::new())
        }
    }

    async fn load_table(&self, namespace: &[String], table: &str) -> Result<TableMetadata> {
        if namespace != ["ns"] || table != "t" {
            return Err(Error::Plan(format!("unknown fixture table `{table}`")));
        }
        Ok(
            TableMetadata::new("glue.ns.t", &self.location, TableFormat::Delta)
                .with_schema(self.schema.clone()),
        )
    }
}

fn kv_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

/// Write a minimal one-commit Delta table at `dir`: one Parquet data file plus a single
/// `_delta_log` JSON commit (`protocol` + `metaData` + `add`) — the smallest table
/// delta-kernel-rs will resolve, same shape as weft-loom's `reads_a_delta_table` fixture.
fn write_delta_table(dir: &Path, rows: &[(i64, i64)]) {
    std::fs::create_dir_all(dir.join("_delta_log")).unwrap();

    let batch = RecordBatch::try_new(
        kv_schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    {
        let file = std::fs::File::create(dir.join("part-0.parquet")).unwrap();
        let mut writer =
            datafusion::parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    let file_size = std::fs::metadata(dir.join("part-0.parquet")).unwrap().len();

    let schema_string = serde_json::json!({
        "type": "struct",
        "fields": [
            {"name": "k", "type": "long", "nullable": false, "metadata": {}},
            {"name": "v", "type": "long", "nullable": false, "metadata": {}}
        ]
    })
    .to_string();
    let commit = [
        serde_json::json!({
            "protocol": {"minReaderVersion": 1, "minWriterVersion": 2}
        })
        .to_string(),
        serde_json::json!({
            "metaData": {
                "id": "00000000-0000-0000-0000-0000000000aa",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema_string,
                "partitionColumns": [],
                "configuration": {}
            }
        })
        .to_string(),
        serde_json::json!({
            "add": {
                "path": "part-0.parquet",
                "partitionValues": {},
                "size": file_size,
                "modificationTime": 0,
                "dataChange": true
            }
        })
        .to_string(),
    ]
    .join("\n");
    std::fs::write(dir.join("_delta_log/00000000000000000000.json"), commit).unwrap();
}

fn rows(start: i64, end: i64) -> Vec<(i64, i64)> {
    (start..end).map(|i| (i % 5, i)).collect()
}

fn unique_temp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "weft-kan48-{tag}-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn engine_with_catalog(location: &std::path::Path) -> Arc<Engine> {
    let engine = Arc::new(Engine::new());
    engine.register_catalog(
        "glue",
        Arc::new(ShardCatalog {
            location: location.to_string_lossy().to_string(),
            schema: kv_schema(),
        }),
    );
    engine
}

fn group_rows(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let s = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((k.value(i), s.value(i)));
        }
    }
    out
}

/// The KAN-48 repro: a distributed aggregate over a catalog-backed Delta table must
/// execute on the workers (not die on the snapshot-pin guard) and match single-node.
#[tokio::test]
async fn distributed_groupby_over_delta_table_matches_single_node() {
    const SQL: &str = "SELECT k, SUM(v) AS s FROM glue.ns.t GROUP BY k ORDER BY k";

    let full_dir = unique_temp_dir("full");
    let shard0_dir = unique_temp_dir("shard0");
    let shard1_dir = unique_temp_dir("shard1");
    write_delta_table(&full_dir, &rows(0, N));
    write_delta_table(&shard0_dir, &rows(0, N / 2));
    write_delta_table(&shard1_dir, &rows(N / 2, N));

    // Ground truth: single-node over the full table.
    let single = Engine::new();
    single.register_catalog(
        "glue",
        Arc::new(ShardCatalog {
            location: full_dir.to_string_lossy().to_string(),
            schema: kv_schema(),
        }),
    );
    let expected = group_rows(&single.sql(SQL).await.expect("single-node baseline"));

    // Two workers, each resolving `glue.ns.t` to its own disjoint shard directory.
    for (port, dir) in [(W0, &shard0_dir), (W1, &shard1_dir)] {
        let engine = engine_with_catalog(dir);
        tokio::spawn(async move {
            let _ = serve_worker(port, engine).await;
        });
    }

    let driver = engine_with_catalog(&full_dir);
    let mut service = WeftService::with_engine(driver);
    service.workers = vec![
        format!("http://127.0.0.1:{W0}"),
        format!("http://127.0.0.1:{W1}"),
    ];
    tokio::spawn(async move {
        let _ = weft_connect::serve_instance(service, PORT).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;

    let mut client = connect(&format!("http://127.0.0.1:{PORT}")).await;
    let batches = exec_sql(&mut client, SQL).await;
    assert_eq!(
        group_rows(&batches),
        expected,
        "distributed lakehouse aggregate must match single-node"
    );

    let _ = std::fs::remove_dir_all(&full_dir);
    let _ = std::fs::remove_dir_all(&shard0_dir);
    let _ = std::fs::remove_dir_all(&shard1_dir);
}

/// Same KAN-48 coverage for the DataFrame relation-tree path (`spark.read.table(...).groupBy`),
/// which builds its logical plan without SQL and must capture the same pins.
#[tokio::test]
async fn distributed_dataframe_groupby_over_delta_table_matches_single_node() {
    const SQL: &str = "SELECT k, SUM(v) AS s FROM glue.ns.t GROUP BY k ORDER BY k";

    let full_dir = unique_temp_dir("df-full");
    let shard0_dir = unique_temp_dir("df-shard0");
    let shard1_dir = unique_temp_dir("df-shard1");
    write_delta_table(&full_dir, &rows(0, N));
    write_delta_table(&shard0_dir, &rows(0, N / 2));
    write_delta_table(&shard1_dir, &rows(N / 2, N));

    let single = Engine::new();
    single.register_catalog(
        "glue",
        Arc::new(ShardCatalog {
            location: full_dir.to_string_lossy().to_string(),
            schema: kv_schema(),
        }),
    );
    let expected = group_rows(&single.sql(SQL).await.expect("single-node baseline"));

    for (port, dir) in [(W0_DF, &shard0_dir), (W1_DF, &shard1_dir)] {
        let engine = engine_with_catalog(dir);
        tokio::spawn(async move {
            let _ = serve_worker(port, engine).await;
        });
    }

    let driver = engine_with_catalog(&full_dir);
    let mut service = WeftService::with_engine(driver);
    service.workers = vec![
        format!("http://127.0.0.1:{W0_DF}"),
        format!("http://127.0.0.1:{W1_DF}"),
    ];
    tokio::spawn(async move {
        let _ = weft_connect::serve_instance(service, PORT_DF).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;

    let mut client = connect(&format!("http://127.0.0.1:{PORT_DF}")).await;
    let batches = exec_relation(&mut client, dataframe_groupby_sum("glue.ns.t")).await;
    let mut got = group_rows(&batches);
    got.sort();
    assert_eq!(
        got, expected,
        "distributed lakehouse DataFrame aggregate must match single-node"
    );

    let _ = std::fs::remove_dir_all(&full_dir);
    let _ = std::fs::remove_dir_all(&shard0_dir);
    let _ = std::fs::remove_dir_all(&shard1_dir);
}

/// `spark.read.table(name).groupBy("k").agg(sum("v"))` as a relation tree (no SQL).
fn dataframe_groupby_sum(table: &str) -> sc::Relation {
    fn expr(t: sc::expression::ExprType) -> sc::Expression {
        sc::Expression {
            common: None,
            expr_type: Some(t),
        }
    }
    fn attr(name: &str) -> sc::Expression {
        expr(sc::expression::ExprType::UnresolvedAttribute(
            sc::expression::UnresolvedAttribute {
                unparsed_identifier: name.to_string(),
                ..Default::default()
            },
        ))
    }
    sc::Relation {
        common: None,
        rel_type: Some(sc::relation::RelType::Aggregate(Box::new(sc::Aggregate {
            input: Some(Box::new(sc::Relation {
                common: None,
                rel_type: Some(sc::relation::RelType::Read(sc::Read {
                    read_type: Some(sc::read::ReadType::NamedTable(sc::read::NamedTable {
                        unparsed_identifier: table.to_string(),
                        ..Default::default()
                    })),
                    ..Default::default()
                })),
            })),
            group_type: sc::aggregate::GroupType::Groupby as i32,
            grouping_expressions: vec![attr("k")],
            aggregate_expressions: vec![expr(sc::expression::ExprType::UnresolvedFunction(
                sc::expression::UnresolvedFunction {
                    function_name: "sum".to_string(),
                    arguments: vec![attr("v")],
                    ..Default::default()
                },
            ))],
            ..Default::default()
        }))),
    }
}

async fn exec_relation(
    client: &mut SparkConnectServiceClient<Channel>,
    relation: sc::Relation,
) -> Vec<RecordBatch> {
    use std::io::Cursor;
    use weft_loom::arrow::ipc::reader::StreamReader;

    let req = sc::ExecutePlanRequest {
        session_id: "00112233-4455-6677-8899-aabbccddeeff".into(),
        plan: Some(sc::Plan {
            op_type: Some(sc::plan::OpType::Root(relation)),
        }),
        ..Default::default()
    };
    let mut stream = client.execute_plan(req).await.unwrap().into_inner();
    let mut out = Vec::new();
    while let Some(msg) = stream.message().await.unwrap() {
        if let Some(sc::execute_plan_response::ResponseType::ArrowBatch(b)) = msg.response_type {
            if b.data.is_empty() {
                continue;
            }
            let reader = StreamReader::try_new(Cursor::new(b.data), None).unwrap();
            for rb in reader {
                out.push(rb.unwrap());
            }
        }
    }
    out
}

/// The worker-side guard must stay armed: a lakehouse scan requested WITHOUT a snapshot
/// pin (an empty pin map) is rejected, never silently resolved to the latest snapshot.
#[tokio::test]
async fn worker_rejects_lakehouse_scan_without_snapshot_pin() {
    let dir = unique_temp_dir("guard");
    write_delta_table(&dir, &rows(0, 10));

    let engine = engine_with_catalog(&dir);
    engine.require_lakehouse_snapshot_pins();
    let err = engine
        .sql_with_lakehouse_snapshots("SELECT COUNT(*) FROM glue.ns.t", "")
        .await
        .expect_err("an unpinned lakehouse scan must be rejected");
    assert!(
        err.to_string().contains("omitted the snapshot pin"),
        "unexpected error: {err}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

async fn connect(endpoint: &str) -> SparkConnectServiceClient<Channel> {
    for _ in 0..50 {
        if let Ok(c) = SparkConnectServiceClient::connect(endpoint.to_string()).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server not ready at {endpoint}");
}

async fn exec_sql(client: &mut SparkConnectServiceClient<Channel>, sql: &str) -> Vec<RecordBatch> {
    use std::io::Cursor;
    use weft_loom::arrow::ipc::reader::StreamReader;

    let req = sc::ExecutePlanRequest {
        session_id: "00112233-4455-6677-8899-aabbccddeeff".into(),
        plan: Some(sc::Plan {
            op_type: Some(sc::plan::OpType::Root(sc::Relation {
                common: None,
                rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                    query: sql.into(),
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    };
    let mut stream = client.execute_plan(req).await.unwrap().into_inner();
    let mut out = Vec::new();
    while let Some(msg) = stream.message().await.unwrap() {
        if let Some(sc::execute_plan_response::ResponseType::ArrowBatch(b)) = msg.response_type {
            if b.data.is_empty() {
                continue;
            }
            let reader = StreamReader::try_new(Cursor::new(b.data), None).unwrap();
            for rb in reader {
                out.push(rb.unwrap());
            }
        }
    }
    out
}
