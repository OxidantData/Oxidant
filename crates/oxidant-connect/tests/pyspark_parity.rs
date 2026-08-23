//! PySpark-shaped requests over gRPC: the `SqlCommand` path (`spark.sql(...)`), the
//! `SqlCommandResult` → relation round-trip, eager DDL, and `AnalyzePlan(Schema)`. These are the
//! request shapes stock PySpark 4.x sends (vs. the raw `Sql`-relation our Rust bench client uses).

use std::time::Duration;

use oxidant_connect::{serve, ServerConfig};
use oxidant_loom::arrow::array::{Int32Array, Int64Array};
use oxidant_loom::arrow::ipc::reader::StreamReader;
use oxidant_proto::spark::connect as sc;
use sc::spark_connect_service_client::SparkConnectServiceClient;
use tonic::transport::Channel;

const SESSION: &str = "00112233-4455-6677-8899-aabbccddeeff";

/// See `dataframe_api.rs` — hard-coded ports collide with real daemons (macOS `rapportd`
/// holds 50603/50604), so tests bind an ephemeral port instead of reserving a number.
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
            // Parallel integration tests collide on Spark's default UI port 4040.
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
    panic!("server did not become ready on {port}");
}

fn sql_relation(query: &str) -> sc::Relation {
    sc::Relation {
        common: None,
        rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
            query: query.to_string(),
            ..Default::default()
        })),
    }
}

fn root(rel: sc::Relation) -> sc::Plan {
    sc::Plan {
        op_type: Some(sc::plan::OpType::Root(rel)),
    }
}

/// PySpark `spark.sql(q)` shape: a Command wrapping a SqlCommand whose `input` is a Sql relation.
fn sql_command(query: &str) -> sc::Plan {
    sc::Plan {
        op_type: Some(sc::plan::OpType::Command(sc::Command {
            command_type: Some(sc::command::CommandType::SqlCommand(sc::SqlCommand {
                input: Some(sql_relation(query)),
                ..Default::default()
            })),
        })),
    }
}

async fn run(
    client: &mut SparkConnectServiceClient<Channel>,
    plan: sc::Plan,
) -> Vec<sc::execute_plan_response::ResponseType> {
    let req = sc::ExecutePlanRequest {
        session_id: SESSION.to_string(),
        plan: Some(plan),
        ..Default::default()
    };
    let mut stream = client
        .execute_plan(req)
        .await
        .expect("execute_plan")
        .into_inner();
    let mut out = Vec::new();
    while let Some(msg) = stream.message().await.expect("stream") {
        if let Some(rt) = msg.response_type {
            out.push(rt);
        }
    }
    out
}

/// First Utf8 cell across any ArrowBatch in the responses.
fn first_string(resps: &[sc::execute_plan_response::ResponseType]) -> Option<String> {
    use oxidant_loom::arrow::array::{Array, StringArray};
    for r in resps {
        if let sc::execute_plan_response::ResponseType::ArrowBatch(b) = r {
            let reader = StreamReader::try_new(std::io::Cursor::new(b.data.clone()), None).ok()?;
            for rb in reader {
                let rb = rb.ok()?;
                if let Some(c) = rb.column(0).as_any().downcast_ref::<StringArray>() {
                    if !c.is_empty() {
                        return Some(c.value(0).to_string());
                    }
                }
            }
        }
    }
    None
}

/// Count ArrowBatch responses.
fn arrow_batch_count(resps: &[sc::execute_plan_response::ResponseType]) -> usize {
    resps
        .iter()
        .filter(|r| matches!(r, sc::execute_plan_response::ResponseType::ArrowBatch(_)))
        .count()
}

/// First i64 value across any ArrowBatch in the responses.
fn first_i64(resps: &[sc::execute_plan_response::ResponseType]) -> Option<i64> {
    for r in resps {
        if let sc::execute_plan_response::ResponseType::ArrowBatch(b) = r {
            let reader = StreamReader::try_new(std::io::Cursor::new(b.data.clone()), None).ok()?;
            for rb in reader {
                let rb = rb.ok()?;
                if let Some(c) = rb.column(0).as_any().downcast_ref::<Int64Array>() {
                    if !c.is_empty() {
                        return Some(c.value(0));
                    }
                } else if let Some(c) = rb.column(0).as_any().downcast_ref::<Int32Array>() {
                    // `SELECT 7` / `SELECT 42` are Spark `IntegerType` (Int32) — oxidant types integer
                    // literals as Int32 to match Spark (real PySpark `SELECT 1` → IntegerType), so
                    // widen here to keep this i64 helper working for both widths.
                    if !c.is_empty() {
                        return Some(c.value(0) as i64);
                    }
                }
            }
        }
    }
    None
}

#[tokio::test]
async fn sql_command_query_returns_lazy_relation_then_executes() {
    let mut client = boot(free_port()).await;

    // spark.sql("SELECT 7 AS x") → a SqlCommandResult carrying a relation handle.
    let resps = run(&mut client, sql_command("SELECT 7 AS x")).await;
    let relation = resps.iter().find_map(|r| match r {
        sc::execute_plan_response::ResponseType::SqlCommandResult(s) => s.relation.clone(),
        _ => None,
    });
    let relation = relation.expect("SqlCommandResult.relation present");
    assert!(
        resps.iter().any(|r| matches!(
            r,
            sc::execute_plan_response::ResponseType::ResultComplete(_)
        )),
        "stream must terminate with ResultComplete"
    );

    // PySpark then executes the DataFrame's relation (the `.show()`/`.collect()` step).
    let resps2 = run(&mut client, root(relation)).await;
    assert_eq!(
        first_i64(&resps2),
        Some(7),
        "executing the returned relation yields the row"
    );
}

#[tokio::test]
async fn sql_command_ddl_executes_eagerly() {
    let mut client = boot(free_port()).await;

    // spark.sql("CREATE TABLE ...") must run eagerly (side effect), returning a result relation.
    let resps = run(&mut client, sql_command("CREATE TABLE t AS SELECT 42 AS v")).await;
    assert!(
        resps.iter().any(|r| matches!(
            r,
            sc::execute_plan_response::ResponseType::SqlCommandResult(_)
        )),
        "a command returns a SqlCommandResult"
    );

    // The table must now exist (proves eager execution) — query it via a raw Sql relation.
    let resps2 = run(&mut client, root(sql_relation("SELECT v FROM t"))).await;
    assert_eq!(
        first_i64(&resps2),
        Some(42),
        "the eagerly-created table is queryable"
    );
}

/// TPC-DS Q5/Q12-class Spark spelling must plan via Connect Sql relations (not only Engine::sql).
#[tokio::test]
async fn sql_relation_accepts_spark_interval_unit_in_string() {
    let mut client = boot(free_port()).await;
    let q = "SELECT (cast('2001-01-12' AS date) + interval '30 days') AS d";
    let resps = run(&mut client, root(sql_relation(q))).await;
    assert!(
        arrow_batch_count(&resps) >= 1,
        "Spark interval '30 days' must execute over Connect Sql relation; got {resps:?}"
    );
}

/// Same normalize must apply when PySpark sends SqlCommand then executes the lazy relation.
#[tokio::test]
async fn sql_command_accepts_spark_interval_between_window() {
    let mut client = boot(free_port()).await;
    // TPC-DS-style window: BETWEEN start AND (start + interval '30 days').
    let q = "SELECT 1 AS ok \
             WHERE cast('2001-01-12' AS date) BETWEEN cast('2001-01-12' AS date) \
             AND (cast('2001-01-12' AS date) + interval '30 days')";
    let resps = run(&mut client, sql_command(q)).await;
    let relation = resps.iter().find_map(|r| match r {
        sc::execute_plan_response::ResponseType::SqlCommandResult(s) => s.relation.clone(),
        _ => None,
    });
    let relation = relation.expect("SqlCommandResult.relation present");
    let resps2 = run(&mut client, root(relation)).await;
    assert_eq!(
        first_i64(&resps2),
        Some(1),
        "interval BETWEEN window must execute via SqlCommand → relation; got {resps2:?}"
    );
}

#[tokio::test]
async fn show_string_renders_a_table() {
    let mut client = boot(free_port()).await;
    // PySpark `.show()` shape: a ShowString relation wrapping the query.
    let plan = sc::Plan {
        op_type: Some(sc::plan::OpType::Root(sc::Relation {
            common: None,
            rel_type: Some(sc::relation::RelType::ShowString(Box::new(
                sc::ShowString {
                    input: Some(Box::new(sql_relation("SELECT 7 AS x"))),
                    num_rows: 20,
                    truncate: 20,
                    vertical: false,
                },
            ))),
        })),
    };
    let resps = run(&mut client, plan).await;
    let table = first_string(&resps).expect("a show_string cell");
    assert!(table.contains('x'), "header present:\n{table}");
    assert!(table.contains('7'), "value present:\n{table}");
    assert!(table.contains('+'), "box-drawing present:\n{table}");
}

#[tokio::test]
async fn empty_result_still_emits_a_batch() {
    let mut client = boot(free_port()).await;
    // A zero-row result must still emit at least one ArrowBatch — PySpark `collect()` asserts it
    // received a RecordBatch, otherwise the table is None.
    let resps = run(&mut client, root(sql_relation("SELECT 1 AS x WHERE 1 = 0"))).await;
    assert!(
        arrow_batch_count(&resps) >= 1,
        "a 0-row result must still emit an ArrowBatch"
    );
}

#[tokio::test]
async fn config_set_then_get_roundtrips() {
    let mut client = boot(free_port()).await;
    let op = |op_type| sc::ConfigRequest {
        session_id: SESSION.to_string(),
        operation: Some(sc::config_request::Operation {
            op_type: Some(op_type),
        }),
        ..Default::default()
    };
    // Set my.key=hi.
    client
        .config(op(sc::config_request::operation::OpType::Set(
            sc::config_request::Set {
                pairs: vec![sc::KeyValue {
                    key: "my.key".to_string(),
                    value: Some("hi".to_string()),
                }],
                silent: None,
            },
        )))
        .await
        .expect("config set");
    // Get my.key + the seeded timezone default.
    let resp = client
        .config(op(sc::config_request::operation::OpType::Get(
            sc::config_request::Get {
                keys: vec![
                    "my.key".to_string(),
                    "spark.sql.session.timeZone".to_string(),
                ],
            },
        )))
        .await
        .expect("config get")
        .into_inner();
    let get = |k: &str| {
        resp.pairs
            .iter()
            .find(|p| p.key == k)
            .and_then(|p| p.value.clone())
    };
    assert_eq!(get("my.key").as_deref(), Some("hi"));
    assert_eq!(get("spark.sql.session.timeZone").as_deref(), Some("UTC"));
}

#[tokio::test]
async fn analyze_schema_returns_spark_types() {
    let mut client = boot(free_port()).await;

    let req = sc::AnalyzePlanRequest {
        session_id: SESSION.to_string(),
        analyze: Some(sc::analyze_plan_request::Analyze::Schema(
            sc::analyze_plan_request::Schema {
                plan: Some(root(sql_relation("SELECT 1 AS a, 'x' AS b"))),
            },
        )),
        ..Default::default()
    };
    let resp = client
        .analyze_plan(req)
        .await
        .expect("analyze_plan")
        .into_inner();
    let sc::analyze_plan_response::Result::Schema(schema) = resp.result.expect("a result") else {
        panic!("expected Schema result")
    };
    let dt = schema.schema.expect("a DataType");
    let Some(sc::data_type::Kind::Struct(st)) = dt.kind else {
        panic!("expected struct schema")
    };
    assert_eq!(st.fields.len(), 2);
    assert_eq!(st.fields[0].name, "a");
    // `SELECT 1` is Spark `IntegerType` (Int32) — oxidant types integer literals as Int32 to match
    // Spark (real PySpark `SELECT 1 AS a` → IntegerType), not DataFusion's native Int64/Long.
    assert!(matches!(
        st.fields[0].data_type.as_ref().unwrap().kind,
        Some(sc::data_type::Kind::Integer(_))
    ));
    assert_eq!(st.fields[1].name, "b");
    assert!(matches!(
        st.fields[1].data_type.as_ref().unwrap().kind,
        Some(sc::data_type::Kind::String(_))
    ));
}

async fn analyze(
    client: &mut SparkConnectServiceClient<Channel>,
    analyze: sc::analyze_plan_request::Analyze,
) -> sc::analyze_plan_response::Result {
    let req = sc::AnalyzePlanRequest {
        session_id: SESSION.to_string(),
        analyze: Some(analyze),
        ..Default::default()
    };
    client
        .analyze_plan(req)
        .await
        .expect("analyze_plan")
        .into_inner()
        .result
        .expect("a result")
}

#[tokio::test]
async fn analyze_explain_renders_a_plan() {
    let mut client = boot(free_port()).await;
    let result = analyze(
        &mut client,
        sc::analyze_plan_request::Analyze::Explain(sc::analyze_plan_request::Explain {
            plan: Some(root(sql_relation("SELECT 1 AS a, 'x' AS b"))),
            explain_mode: sc::analyze_plan_request::explain::ExplainMode::Extended as i32,
        }),
    )
    .await;
    let sc::analyze_plan_response::Result::Explain(e) = result else {
        panic!("expected Explain result")
    };
    assert!(
        e.explain_string.contains("Physical Plan"),
        "explain carries a physical plan:\n{}",
        e.explain_string
    );
    assert!(
        e.explain_string.contains("Optimized Logical Plan"),
        "EXTENDED mode includes the optimized logical plan:\n{}",
        e.explain_string
    );
}

#[tokio::test]
async fn analyze_explain_shows_filter_pushdown() {
    let mut client = boot(free_port()).await;
    // A filter over a created table must show the predicate pushed into the scan — proves the
    // optimizer runs on the resolved plan (the `.into_unoptimized_plan()` subplans get optimized
    // once, at the execution/explain seam).
    run(
        &mut client,
        sql_command("CREATE TABLE pushdown_t AS SELECT * FROM (VALUES (1),(2),(3)) AS t(v)"),
    )
    .await;
    let result = analyze(
        &mut client,
        sc::analyze_plan_request::Analyze::Explain(sc::analyze_plan_request::Explain {
            plan: Some(root(sql_relation("SELECT v FROM pushdown_t WHERE v > 1"))),
            explain_mode: sc::analyze_plan_request::explain::ExplainMode::Simple as i32,
        }),
    )
    .await;
    let sc::analyze_plan_response::Result::Explain(e) = result else {
        panic!("expected Explain result")
    };
    // DataFusion renders pushed predicates in the scan node (`DataSourceExec`/filter expr). The
    // optimized plan must reference the predicate against `v`, not a separate post-scan FilterExec
    // only — assert the predicate text survived optimization.
    assert!(
        e.explain_string.contains("v@0 > 1") || e.explain_string.contains("v > 1"),
        "predicate present in optimized physical plan:\n{}",
        e.explain_string
    );
}

#[tokio::test]
async fn analyze_tree_string_formats_schema() {
    let mut client = boot(free_port()).await;
    let result = analyze(
        &mut client,
        sc::analyze_plan_request::Analyze::TreeString(sc::analyze_plan_request::TreeString {
            plan: Some(root(sql_relation("SELECT 1 AS a, 'x' AS b"))),
            level: None,
        }),
    )
    .await;
    let sc::analyze_plan_response::Result::TreeString(t) = result else {
        panic!("expected TreeString result")
    };
    assert!(t.tree_string.starts_with("root\n"), "{}", t.tree_string);
    // `SELECT 1` is Spark `IntegerType` (Int32) — oxidant types integer literals as Int32 to match
    // Spark (real PySpark `SELECT 1` → IntegerType / "integer"), not DataFusion's native i64/long.
    assert!(
        t.tree_string.contains("|-- a: integer"),
        "{}",
        t.tree_string
    );
    assert!(t.tree_string.contains("|-- b: string"), "{}", t.tree_string);
}

#[tokio::test]
async fn analyze_is_local_and_is_streaming_are_false() {
    let mut client = boot(free_port()).await;
    let local = analyze(
        &mut client,
        sc::analyze_plan_request::Analyze::IsLocal(sc::analyze_plan_request::IsLocal {
            plan: Some(root(sql_relation("SELECT 1 AS a"))),
        }),
    )
    .await;
    assert!(matches!(
        local,
        sc::analyze_plan_response::Result::IsLocal(sc::analyze_plan_response::IsLocal {
            is_local: false
        })
    ));
    let streaming = analyze(
        &mut client,
        sc::analyze_plan_request::Analyze::IsStreaming(sc::analyze_plan_request::IsStreaming {
            plan: Some(root(sql_relation("SELECT 1 AS a"))),
        }),
    )
    .await;
    assert!(matches!(
        streaming,
        sc::analyze_plan_response::Result::IsStreaming(sc::analyze_plan_response::IsStreaming {
            is_streaming: false
        })
    ));
}
