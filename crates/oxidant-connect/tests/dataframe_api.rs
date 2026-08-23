//! The DataFrame-API path: Spark Connect relation/expression trees (Project/Filter/Aggregate over
//! a LocalRelation) lowered to DataFusion logical plans. Mirrors what stock PySpark sends for
//! `df.filter(...).select(...)` / `df.groupBy(...).agg(...)` without going through SQL.

use std::sync::Arc;
use std::time::Duration;

use oxidant_connect::{serve, ServerConfig};
use oxidant_loom::arrow::array::{Int64Array, RecordBatch};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::ipc::writer::StreamWriter;
use oxidant_proto::spark::connect as sc;
use sc::spark_connect_service_client::SparkConnectServiceClient;
use tonic::transport::Channel;

const SESSION: &str = "00112233-4455-6677-8899-aabbccddeeff";

/// A port nobody on this machine is already listening on. Hard-coded ports collide with
/// real daemons in the wild (macOS `rapportd` holds 50603/50604 here, which made the window
/// test fail with `server not ready` on a clean checkout), so tests bind an ephemeral port
/// instead of reserving a number.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn boot(port: u16) -> SparkConnectServiceClient<Channel> {
    tokio::spawn(async move {
        let _ = serve(ServerConfig {
            port,
            ui_port: None,
            ..Default::default()
        })
        .await;
    });
    let endpoint = format!("http://127.0.0.1:{port}");
    for _ in 0..50 {
        if let Ok(c) = SparkConnectServiceClient::connect(endpoint.clone()).await {
            return c.max_decoding_message_size(256 * 1024 * 1024);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server not ready on {port}");
}

/// A two-column (`id`, `v`) inline LocalRelation relation.
fn local_relation(ids: Vec<i64>, vs: Vec<i64>) -> sc::Relation {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(vs)),
        ],
    )
    .unwrap();
    let mut data = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut data, schema.as_ref()).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
    }
    sc::Relation {
        common: None,
        rel_type: Some(sc::relation::RelType::LocalRelation(sc::LocalRelation {
            data: Some(data),
            schema: None,
        })),
    }
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
fn lit_i64(v: i64) -> sc::Expression {
    expr(sc::expression::ExprType::Literal(sc::expression::Literal {
        literal_type: Some(sc::expression::literal::LiteralType::Long(v)),
        ..Default::default()
    }))
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
fn boxed(r: sc::Relation) -> Option<Box<sc::Relation>> {
    Some(Box::new(r))
}
fn rel(t: sc::relation::RelType) -> sc::Relation {
    sc::Relation {
        common: None,
        rel_type: Some(t),
    }
}

async fn count_rows(client: &mut SparkConnectServiceClient<Channel>, plan: sc::Relation) -> i64 {
    use oxidant_loom::arrow::ipc::reader::StreamReader;
    let req = sc::ExecutePlanRequest {
        session_id: SESSION.to_string(),
        plan: Some(sc::Plan {
            op_type: Some(sc::plan::OpType::Root(plan)),
        }),
        ..Default::default()
    };
    let mut stream = client
        .execute_plan(req)
        .await
        .expect("execute")
        .into_inner();
    let mut rows = 0i64;
    while let Some(msg) = stream.message().await.expect("stream") {
        if let Some(sc::execute_plan_response::ResponseType::ArrowBatch(b)) = msg.response_type {
            let reader = StreamReader::try_new(std::io::Cursor::new(b.data), None).unwrap();
            for rb in reader {
                rows += rb.unwrap().num_rows() as i64;
            }
        }
    }
    rows
}

#[tokio::test]
async fn filter_lowers_and_executes() {
    let mut client = boot(free_port()).await;
    // df(id,v) of 3 rows, filter v > 1  →  2 rows.
    let src = local_relation(vec![1, 2, 3], vec![0, 5, 9]);
    let plan = rel(sc::relation::RelType::Filter(Box::new(sc::Filter {
        input: boxed(src),
        condition: Some(func(">", vec![attr("v"), lit_i64(1)])),
    })));
    assert_eq!(count_rows(&mut client, plan).await, 2);
}

#[tokio::test]
async fn window_function_lowers_and_executes() {
    let mut client = boot(free_port()).await;
    // row_number() OVER (ORDER BY v) over 3 rows → 3 rows, and the result is signed (UInt64 from
    // row_number would be unrepresentable in Spark and error at the client).
    let src = local_relation(vec![1, 2, 3], vec![30, 10, 20]);
    let order = sc::expression::SortOrder {
        child: Some(Box::new(attr("v"))),
        direction: sc::expression::sort_order::SortDirection::Ascending as i32,
        null_ordering: 0,
    };
    let win = expr(sc::expression::ExprType::Window(Box::new(
        sc::expression::Window {
            window_function: Some(Box::new(func("row_number", vec![]))),
            partition_spec: vec![],
            order_spec: vec![order],
            frame_spec: None,
        },
    )));
    let plan = rel(sc::relation::RelType::Project(Box::new(sc::Project {
        input: boxed(src),
        expressions: vec![win],
    })));
    assert_eq!(count_rows(&mut client, plan).await, 3);
}

#[tokio::test]
async fn project_and_aggregate_lower_and_execute() {
    let mut client = boot(free_port()).await;
    // df(id,v): rows (1,0),(2,0),(3,9) grouped by v → 2 groups.
    let src = local_relation(vec![1, 2, 3], vec![0, 0, 9]);
    let agg = rel(sc::relation::RelType::Aggregate(Box::new(sc::Aggregate {
        input: boxed(src),
        group_type: sc::aggregate::GroupType::Groupby as i32,
        grouping_expressions: vec![attr("v")],
        aggregate_expressions: vec![func("count", vec![attr("id")])],
        ..Default::default()
    })));
    assert_eq!(count_rows(&mut client, agg).await, 2);
}
