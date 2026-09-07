//! OxidantData/Oxidant#177: a Python UDF registered in session A must not break
//! SQL `CREATE FUNCTION` in a distinct session B.

use std::time::Duration;

use oxidant_connect::{serve, ServerConfig};
use oxidant_loom::arrow::array::Int32Array;
use oxidant_loom::arrow::ipc::reader::StreamReader;
use oxidant_proto::spark::connect as sc;
use sc::spark_connect_service_client::SparkConnectServiceClient;
use tonic::transport::Channel;

const SESSION_A: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const SESSION_B: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";

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

fn sql_plan(query: &str) -> sc::Plan {
    sc::Plan {
        op_type: Some(sc::plan::OpType::Root(sc::Relation {
            common: None,
            rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                query: query.to_string(),
                ..Default::default()
            })),
        })),
    }
}

fn python_register_plan(name: &str) -> sc::Plan {
    sc::Plan {
        op_type: Some(sc::plan::OpType::Command(sc::Command {
            command_type: Some(sc::command::CommandType::RegisterFunction(
                sc::CommonInlineUserDefinedFunction {
                    function_name: name.to_string(),
                    deterministic: true,
                    arguments: vec![],
                    function: Some(
                        sc::common_inline_user_defined_function::Function::PythonUdf(
                            sc::PythonUdf {
                                output_type: None,
                                eval_type: 0,
                                command: b"not-executed".to_vec(),
                                python_ver: "3.11".into(),
                                additional_includes: vec![],
                            },
                        ),
                    ),
                    is_distinct: false,
                },
            )),
        })),
    }
}

async fn execute(
    client: &mut SparkConnectServiceClient<Channel>,
    session_id: &str,
    plan: sc::Plan,
) -> Result<Vec<sc::ExecutePlanResponse>, tonic::Status> {
    let req = sc::ExecutePlanRequest {
        session_id: session_id.to_string(),
        plan: Some(plan),
        ..Default::default()
    };
    let mut stream = client.execute_plan(req).await?.into_inner();
    let mut out = Vec::new();
    while let Some(msg) = stream.message().await? {
        out.push(msg);
    }
    Ok(out)
}

async fn collect_i32(
    client: &mut SparkConnectServiceClient<Channel>,
    session_id: &str,
    query: &str,
) -> Result<Vec<i32>, tonic::Status> {
    let responses = execute(client, session_id, sql_plan(query)).await?;
    let mut out = Vec::new();
    for msg in responses {
        if let Some(sc::execute_plan_response::ResponseType::ArrowBatch(b)) = msg.response_type {
            let reader = StreamReader::try_new(std::io::Cursor::new(b.data), None).unwrap();
            for rb in reader {
                let rb = rb.unwrap();
                let col = rb
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap_or_else(|| panic!("col0 {:?}", rb.schema()));
                out.extend(col.iter().map(|v| v.unwrap()));
            }
        }
    }
    Ok(out)
}

#[tokio::test]
async fn python_udf_in_session_a_does_not_break_sql_create_in_session_b() {
    std::env::set_var("OXIDANT_ALLOW_PYTHON_UDF", "false");
    let mut client = boot(free_port()).await;
    assert_ne!(SESSION_A, SESSION_B);

    execute(
        &mut client,
        SESSION_B,
        sql_plan("CREATE FUNCTION registry_before() RETURNS INT RETURN 7"),
    )
    .await
    .unwrap_or_else(|e| panic!("before: {e}"));
    assert_eq!(
        collect_i32(&mut client, SESSION_B, "SELECT registry_before() AS v")
            .await
            .unwrap_or_else(|e| panic!("select before: {e}")),
        vec![7]
    );

    execute(
        &mut client,
        SESSION_A,
        python_register_plan("registry_python"),
    )
    .await
    .unwrap_or_else(|e| panic!("python register must be accepted: {e}"));

    execute(
        &mut client,
        SESSION_B,
        sql_plan("CREATE FUNCTION registry_after() RETURNS INT RETURN 7"),
    )
    .await
    .unwrap_or_else(|e| panic!("after (must not be udf registry_python has no body): {e}"));
    assert_eq!(
        collect_i32(&mut client, SESSION_B, "SELECT registry_after() AS v")
            .await
            .unwrap_or_else(|e| panic!("select after: {e}")),
        vec![7]
    );
}
