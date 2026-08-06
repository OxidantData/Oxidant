//! Distributed GROUP BY over Spark Connect when workers are configured.

use std::sync::Arc;
use std::time::Duration;

use oxidant_connect::OxidantService;
use oxidant_execution::flight::serve_worker;
use oxidant_loom::arrow::array::Int64Array;
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;
use oxidant_observability::{AppStateStore, ExecutionEvent};
use oxidant_proto::spark::connect as sc;
use sc::spark_connect_service_client::SparkConnectServiceClient;
use tonic::transport::Channel;

// Keep clear of oxidant-execution distributed_* tests (50571–50634 range).
const PORT: u16 = 50870;
const PORT_DF: u16 = 50873;
const W0: u16 = 50871;
const W1: u16 = 50872;
const W0_DF: u16 = 50874;
const W1_DF: u16 = 50875;
const PORT_Z: u16 = 50876;
const W0_Z: u16 = 50877;
const W1_Z: u16 = 50878;

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

#[tokio::test]
async fn distributed_groupby_via_connect() {
    const N: i64 = 100;
    for (port, start, end) in [(W0, 0, N / 2), (W1, N / 2, N)] {
        let e = Arc::new(Engine::new());
        e.register_batches("t", vec![make_batch(start, end)])
            .unwrap();
        e.register_batches("full_t", vec![make_batch(0, N)])
            .unwrap();
        let ee = e.clone();
        tokio::spawn(async move {
            let _ = serve_worker(port, ee).await;
        });
    }

    let driver_engine = Arc::new(Engine::new());
    driver_engine
        .register_batches("t", vec![make_batch(0, N)])
        .unwrap();
    driver_engine
        .register_batches("full_t", vec![make_batch(0, N)])
        .unwrap();

    let mut service = OxidantService::with_engine(driver_engine);
    service.workers = vec![
        format!("http://127.0.0.1:{W0}"),
        format!("http://127.0.0.1:{W1}"),
    ];

    tokio::spawn(async move {
        let _ = oxidant_connect::serve_instance(service, PORT).await;
    });

    tokio::time::sleep(Duration::from_millis(400)).await;

    let single = Engine::new();
    single
        .register_batches("t", vec![make_batch(0, N)])
        .unwrap();
    single
        .register_batches("full_t", vec![make_batch(0, N)])
        .unwrap();
    let expected_rows: usize = single
        .sql("SELECT k, SUM(v) AS s FROM t GROUP BY k")
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();

    let mut client = connect(&format!("http://127.0.0.1:{PORT}")).await;
    let batches = exec_sql(&mut client, "SELECT k, SUM(v) AS s FROM t GROUP BY k").await;
    let got_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(got_rows, expected_rows);

    // Non-aggregate path over the sharded table (workers hold disjoint slices of `t`).
    let forward_sql = "SELECT k, v FROM t WHERE v < 3 ORDER BY v";
    let expected_rows: usize = single
        .sql(forward_sql)
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    let batches = exec_sql(&mut client, forward_sql).await;
    let got_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(got_rows, expected_rows);
}

#[tokio::test]
async fn distributed_groupby_via_dataframe_relation_tree() {
    const N: i64 = 100;
    for (port, start, end) in [(W0_DF, 0, N / 2), (W1_DF, N / 2, N)] {
        let e = Arc::new(Engine::new());
        e.register_batches("t", vec![make_batch(start, end)])
            .unwrap();
        let ee = e.clone();
        tokio::spawn(async move {
            let _ = serve_worker(port, ee).await;
        });
    }

    let store = Arc::new(AppStateStore::new());
    let mut rx = store.subscribe();
    let mut service = OxidantService::with_store(store.clone());
    service
        .engine()
        .register_batches("t", vec![make_batch(0, N)])
        .unwrap();
    service.workers = vec![
        format!("http://127.0.0.1:{W0_DF}"),
        format!("http://127.0.0.1:{W1_DF}"),
    ];

    tokio::spawn(async move {
        let _ = oxidant_connect::serve_instance(service, PORT_DF).await;
    });

    tokio::time::sleep(Duration::from_millis(400)).await;

    let single = Engine::new();
    single
        .register_batches("t", vec![make_batch(0, N)])
        .unwrap();
    let expected_rows: usize = single
        .sql("SELECT k, SUM(v) AS s FROM t GROUP BY k")
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();

    let plan = dataframe_groupby_sum("t");
    let mut client = connect(&format!("http://127.0.0.1:{PORT_DF}")).await;
    let batches = exec_relation(&mut client, plan, "op-df-groupby").await;
    let got_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(got_rows, expected_rows);

    let mut worker_stage_tasks = 0i32;
    while let Ok(event) = rx.try_recv() {
        if let ExecutionEvent::StageStarted {
            num_tasks,
            operation_id,
            ..
        } = event
        {
            assert_eq!(operation_id, "op-df-groupby");
            if num_tasks >= 2 {
                worker_stage_tasks = num_tasks;
            }
        }
    }
    assert!(
        worker_stage_tasks >= 2,
        "DataFrame groupBy should run multi-task worker stages, saw {worker_stage_tasks}"
    );
}

#[tokio::test]
async fn distributed_zero_row_result_still_streams_typed_empty_batch() {
    // KAN-42: a distributed query whose result is empty must still stream one typed (empty)
    // ArrowBatch — PySpark's `collect()` asserts `table is not None`, so a bare
    // ResultComplete-only stream kills the client (TPC-DS Q17 at SF10).
    const N: i64 = 100;
    for (port, start, end) in [(W0_Z, 0, N / 2), (W1_Z, N / 2, N)] {
        let e = Arc::new(Engine::new());
        e.register_batches("t", vec![make_batch(start, end)])
            .unwrap();
        let ee = e.clone();
        tokio::spawn(async move {
            let _ = serve_worker(port, ee).await;
        });
    }

    let driver_engine = Arc::new(Engine::new());
    driver_engine
        .register_batches("t", vec![make_batch(0, N)])
        .unwrap();

    let mut service = OxidantService::with_engine(driver_engine);
    service.workers = vec![
        format!("http://127.0.0.1:{W0_Z}"),
        format!("http://127.0.0.1:{W1_Z}"),
    ];

    tokio::spawn(async move {
        let _ = oxidant_connect::serve_instance(service, PORT_Z).await;
    });

    tokio::time::sleep(Duration::from_millis(400)).await;

    let mut client = connect(&format!("http://127.0.0.1:{PORT_Z}")).await;
    // Distributable shape (filter + two-stage aggregate + driver-side ORDER BY/LIMIT finalize)
    // with an empty result set.
    let (batches, arrow_responses) = exec_sql_full(
        &mut client,
        "SELECT k, SUM(v) AS s FROM t WHERE v < 0 GROUP BY k ORDER BY k LIMIT 100",
    )
    .await;
    assert!(
        arrow_responses > 0,
        "zero-row distributed result must still stream one typed empty ArrowBatch \
         (PySpark asserts `table is not None`)"
    );
    let got_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(got_rows, 0);
    let schema = batches[0].schema();
    assert_eq!(schema.field(0).name(), "k");
    assert_eq!(schema.field(1).name(), "s");
}

const PORT_R: u16 = 50879;
const W0_R: u16 = 50880;
const W1_R: u16 = 50881;

#[tokio::test]
async fn distributed_sql_rank_window_result_is_signed_for_pyspark() {
    // KAN-49 (SF10 Q36/44/49/67/70/86): DataFusion ranking windows produce UInt64, which
    // Spark has no type for — PySpark aborts the Arrow stream with
    // UNSUPPORTED_DATA_TYPE_FOR_ARROW_CONVERSION. The SQL distributed path must normalize
    // unsigned columns to signed exactly like the DataFrame path does.
    const N: i64 = 100;
    for (port, start, end) in [(W0_R, 0, N / 2), (W1_R, N / 2, N)] {
        let e = Arc::new(Engine::new());
        e.register_batches("t", vec![make_batch(start, end)])
            .unwrap();
        let ee = e.clone();
        tokio::spawn(async move {
            let _ = serve_worker(port, ee).await;
        });
    }

    let store = Arc::new(AppStateStore::new());
    let mut rx = store.subscribe();
    let mut service = OxidantService::with_store(store.clone());
    service
        .engine()
        .register_batches("t", vec![make_batch(0, N)])
        .unwrap();
    service.workers = vec![
        format!("http://127.0.0.1:{W0_R}"),
        format!("http://127.0.0.1:{W1_R}"),
    ];

    tokio::spawn(async move {
        let _ = oxidant_connect::serve_instance(service, PORT_R).await;
    });

    tokio::time::sleep(Duration::from_millis(400)).await;

    // Window over an aggregate: rank emits UInt64 under DataFusion.
    let sql = "SELECT k, s, RANK() OVER (ORDER BY s DESC) AS r \
               FROM (SELECT k, SUM(v) AS s FROM t GROUP BY k) ORDER BY r";
    let single = Engine::new();
    single
        .register_batches("t", vec![make_batch(0, N)])
        .unwrap();
    let expected = single.sql(sql).await.unwrap();
    let expected_rows: usize = expected.iter().map(|b| b.num_rows()).sum();

    let mut client = connect(&format!("http://127.0.0.1:{PORT_R}")).await;
    let batches = exec_sql(&mut client, sql).await;
    let got_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(got_rows, expected_rows);

    // The query must actually have gone distributed — a local fallback would be
    // signed already and make this test vacuous.
    let mut went_distributed = false;
    while let Ok(event) = rx.try_recv() {
        match event {
            ExecutionEvent::DistributedFallback { .. } => {
                panic!("query fell back to local execution; test is vacuous")
            }
            ExecutionEvent::StageStarted { num_tasks, .. } if num_tasks >= 2 => {
                went_distributed = true;
            }
            _ => {}
        }
    }
    assert!(went_distributed, "expected multi-task worker stages");

    for b in &batches {
        for f in b.schema().fields() {
            assert!(
                !matches!(
                    f.data_type(),
                    DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
                ),
                "unsigned column `{}` ({:?}) reached the client; PySpark rejects it",
                f.name(),
                f.data_type()
            );
        }
    }
    let schema0 = batches[0].schema();
    let r = &schema0.fields()[2];
    assert_eq!(r.name(), "r");
    assert_eq!(r.data_type(), &DataType::Int64, "rank maps to Spark long");
}

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

fn func(name: &str, args: Vec<sc::Expression>) -> sc::Expression {
    expr(sc::expression::ExprType::UnresolvedFunction(
        sc::expression::UnresolvedFunction {
            function_name: name.to_string(),
            arguments: args,
            ..Default::default()
        },
    ))
}

fn read_table(name: &str) -> sc::Relation {
    sc::Relation {
        common: None,
        rel_type: Some(sc::relation::RelType::Read(sc::Read {
            read_type: Some(sc::read::ReadType::NamedTable(sc::read::NamedTable {
                unparsed_identifier: name.to_string(),
                ..Default::default()
            })),
            ..Default::default()
        })),
    }
}

/// `spark.read.table(t).groupBy("k").agg(sum("v"))` without RelType::Sql.
fn dataframe_groupby_sum(table: &str) -> sc::Relation {
    sc::Relation {
        common: None,
        rel_type: Some(sc::relation::RelType::Aggregate(Box::new(sc::Aggregate {
            input: Some(Box::new(read_table(table))),
            group_type: sc::aggregate::GroupType::Groupby as i32,
            grouping_expressions: vec![attr("k")],
            aggregate_expressions: vec![func("sum", vec![attr("v")])],
            ..Default::default()
        }))),
    }
}

async fn exec_relation(
    client: &mut SparkConnectServiceClient<Channel>,
    relation: sc::Relation,
    operation_id: &str,
) -> Vec<RecordBatch> {
    use oxidant_loom::arrow::ipc::reader::StreamReader;
    use std::io::Cursor;

    let req = sc::ExecutePlanRequest {
        session_id: "00112233-4455-6677-8899-aabbccddeeff".into(),
        operation_id: Some(operation_id.to_string()),
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
    exec_sql_full(client, sql).await.0
}

/// Like [`exec_sql`], but also reports how many `ArrowBatch` responses the stream carried —
/// the client-visible contract a zero-row result must still honor (KAN-42).
async fn exec_sql_full(
    client: &mut SparkConnectServiceClient<Channel>,
    sql: &str,
) -> (Vec<RecordBatch>, usize) {
    use oxidant_loom::arrow::ipc::reader::StreamReader;
    use oxidant_proto::spark::connect as sc;
    use std::io::Cursor;

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
    let mut arrow_responses = 0;
    while let Some(msg) = stream.message().await.unwrap() {
        if let Some(sc::execute_plan_response::ResponseType::ArrowBatch(b)) = msg.response_type {
            arrow_responses += 1;
            if b.data.is_empty() {
                continue;
            }
            let reader = StreamReader::try_new(Cursor::new(b.data), None).unwrap();
            for rb in reader {
                out.push(rb.unwrap());
            }
        }
    }
    (out, arrow_responses)
}
