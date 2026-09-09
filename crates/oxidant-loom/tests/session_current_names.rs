use std::sync::Arc;

use async_trait::async_trait;
use oxidant_catalog::{CatalogProvider, TableMetadata};
use oxidant_common::{Error, Result};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::arrow::util::display::array_value_to_string;
use oxidant_loom::Engine;

struct EmptyCatalog;

#[async_trait]
impl CatalogProvider for EmptyCatalog {
    fn name(&self) -> &str {
        "prod"
    }

    async fn list_namespaces(&self, parent: &[String]) -> Result<Vec<Vec<String>>> {
        Ok(if parent.is_empty() {
            vec![
                vec!["sales".into()],
                vec!["finance".into()],
                vec!["division".into()],
                vec!["sales.eu".into()],
            ]
        } else if parent == ["division"] {
            vec![vec!["division".into(), "sales".into()]]
        } else {
            vec![]
        })
    }

    async fn list_tables(&self, _: &[String]) -> Result<Vec<String>> {
        Ok(vec![])
    }

    async fn load_table(&self, _: &[String], _: &str) -> Result<TableMetadata> {
        Err(Error::Plan("no tables in test catalog".into()))
    }
}

fn rows(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(move |row| {
                (0..batch.num_columns())
                    .map(|col| array_value_to_string(batch.column(col), row).unwrap())
                    .collect()
            })
        })
        .collect()
}

async fn session() -> Engine {
    let engine = Engine::new();
    engine.register_catalog("prod", Arc::new(EmptyCatalog));
    let session = engine.for_session("names");
    session.set_current_catalog("prod").await.unwrap();
    session.set_current_namespace("sales").await.unwrap();
    session
}

// UNION ALL makes local collection use CoalescePartitionsExec's spawned tasks.
// A single SELECT runs on the caller and cannot detect lost Tokio task-locals.
const UNION: &str =
    "SELECT current_catalog() AS c, current_database() AS d, current_schema() AS s \
                    UNION ALL \
                    SELECT current_catalog() AS c, current_database() AS d, current_schema() AS s";

fn expected(catalog: &str, namespace: &str, count: usize) -> Vec<Vec<String>> {
    vec![vec![catalog.into(), namespace.into(), namespace.into()]; count]
}

#[tokio::test]
async fn sql_local_partitions_retain_session_names() {
    let session = session().await;
    let actual = rows(&session.sql(UNION).await.unwrap());
    assert_eq!(actual, expected("prod", "sales", 2));
}

#[tokio::test]
async fn sql_with_stats_local_partitions_retain_session_names() {
    let session = session().await;
    let (batches, stats) = session.sql_with_stats(UNION).await.unwrap();
    assert_eq!(rows(&batches), expected("prod", "sales", 2));
    assert_eq!(stats.output_rows, 2);
}

#[derive(Clone, Copy)]
enum QueryApi {
    Sql,
    WithStats,
}

impl QueryApi {
    async fn run(self, engine: &Engine, query: &str) -> Vec<Vec<String>> {
        match self {
            Self::Sql => rows(&engine.sql(query).await.unwrap()),
            Self::WithStats => {
                let (batches, stats) = engine.sql_with_stats(query).await.unwrap();
                let actual = rows(&batches);
                assert_eq!(stats.output_rows, actual.len() as u64);
                actual
            }
        }
    }
}

async fn assert_session_reuse(api: QueryApi) {
    let session = session().await;
    let builtin = session.for_session("builtin");
    assert_eq!(
        api.run(&builtin, UNION).await,
        expected("spark_catalog", "default", 2)
    );
    // Exact SQL is reused across handles and USE switches: neither a folded plan nor
    // a result from an earlier session may be reused with stale current-name values.
    for (statement, catalog, namespace) in [
        ("USE prod.finance", "prod", "finance"),
        ("USE prod.sales", "prod", "sales"),
        ("USE prod.finance", "prod", "finance"),
        // Selecting the SAME catalog is a no-op; switching away and back resets it.
        ("USE CATALOG prod", "prod", "finance"),
        ("USE CATALOG spark_catalog", "spark_catalog", "default"),
        ("USE CATALOG prod", "prod", ""),
        ("USE prod.division.sales", "prod", "sales"),
        ("USE prod.`sales.eu`", "prod", "sales.eu"),
        ("USE CATALOG spark_catalog", "spark_catalog", "default"),
        ("USE prod.sales", "prod", "sales"),
    ] {
        session.sql(statement).await.unwrap();
        assert_eq!(
            api.run(&session, UNION).await,
            expected(catalog, namespace, 2)
        );
        // A new handle of the SAME session sees the switch; a different one does not.
        assert_eq!(
            api.run(&session.for_session("names"), UNION).await,
            expected(catalog, namespace, 2)
        );
        assert_eq!(
            api.run(&builtin, UNION).await,
            expected("spark_catalog", "default", 2)
        );
    }
}

#[tokio::test]
async fn sql_repeated_sql_use_and_scoped_names_are_session_isolated() {
    assert_session_reuse(QueryApi::Sql).await;
}

#[tokio::test]
async fn sql_with_stats_repeated_sql_use_and_scoped_names_are_session_isolated() {
    assert_session_reuse(QueryApi::WithStats).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_public_entries_keep_partition_names_isolated() {
    let sales = session().await;
    let finance = sales.for_session("finance");
    finance.sql("USE prod.finance").await.unwrap();
    let builtin = sales.for_session("builtin");
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let mut tasks = Vec::new();
    for (session, catalog, namespace) in [
        (sales, "prod", "sales"),
        (finance, "prod", "finance"),
        (builtin, "spark_catalog", "default"),
    ] {
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            for _ in 0..8 {
                for api in [QueryApi::Sql, QueryApi::WithStats] {
                    assert_eq!(
                        api.run(&session, UNION).await,
                        expected(catalog, namespace, 2)
                    );
                }
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
}

#[tokio::test]
async fn stored_view_plan_does_not_capture_creator_session_names() {
    let sales = session().await;
    // DataFusion retains this logical plan in a shared ViewTable. It must stay
    // unbound until EACH consuming query creates its physical expressions.
    sales
        .sql(&format!("CREATE TEMP VIEW stored_names AS {UNION}"))
        .await
        .unwrap();
    let builtin = sales.for_session("builtin");
    const QUERY: &str = "SELECT * FROM spark_catalog.default.stored_names";
    for api in [QueryApi::Sql, QueryApi::WithStats] {
        for (statement, namespace) in [("USE prod.sales", "sales"), ("USE prod.finance", "finance")]
        {
            sales.sql(statement).await.unwrap();
            assert_eq!(api.run(&sales, QUERY).await, expected("prod", namespace, 2));
            assert_eq!(
                api.run(&builtin, QUERY).await,
                expected("spark_catalog", "default", 2)
            );
        }
    }
}

#[tokio::test]
async fn local_join_replanning_retains_session_names() {
    let session = session().await;
    // This public wrapper forces the local collect guard's replan branch, without
    // setting process-global join options or requiring a worker/provider.
    let actual = oxidant_loom::with_join_strategy_flipped(session.sql(UNION))
        .await
        .unwrap();
    assert_eq!(rows(&actual), expected("prod", "sales", 2));
    assert_eq!(
        rows(&session.sql(UNION).await.unwrap()),
        expected("prod", "sales", 2)
    );
}
