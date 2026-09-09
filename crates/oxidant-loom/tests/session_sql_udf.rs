//! Combined local execution contract for session-name snapshots and integer SQL UDFs.
//! These are engine-entry tests, not Connect-client or distributed-worker acceptance.

use std::sync::Arc;

use async_trait::async_trait;
use oxidant_catalog::{CatalogProvider, TableMetadata};
use oxidant_common::{Error, Result};
use oxidant_loom::arrow::array::{Array, Int32Array, StringArray};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;

struct NamesCatalog;

#[async_trait]
impl CatalogProvider for NamesCatalog {
    fn name(&self) -> &str {
        "prod"
    }

    async fn list_namespaces(&self, parent: &[String]) -> Result<Vec<Vec<String>>> {
        Ok(if parent.is_empty() {
            vec![vec!["sales".into()], vec!["finance".into()]]
        } else {
            vec![]
        })
    }

    async fn list_tables(&self, _: &[String]) -> Result<Vec<String>> {
        Ok(vec![])
    }

    async fn load_table(&self, _: &[String], _: &str) -> Result<TableMetadata> {
        Err(Error::Plan("test catalog contains only namespaces".into()))
    }
}

// UNION ALL forces independent partition tasks; the UDF consumes both a varying
// nullable column and the session-name expression, rather than testing them apart.
const QUERY: &str = "SELECT current_catalog() AS c, current_database() AS d, \
    current_schema() AS s, combined_adjust(v, length(current_schema())) AS adjusted \
    FROM spark_catalog.default.combined_input UNION ALL \
    SELECT current_catalog() AS c, current_database() AS d, \
    current_schema() AS s, combined_adjust(v, length(current_schema())) AS adjusted \
    FROM spark_catalog.default.combined_input";

async fn engine() -> Engine {
    let engine = Engine::new();
    engine.register_catalog("prod", Arc::new(NamesCatalog));
    engine
        .register_batches(
            "combined_input",
            vec![RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)])),
                vec![Arc::new(Int32Array::from(vec![Some(2), Some(5), None]))],
            )
            .unwrap()],
        )
        .unwrap();
    engine
        .sql("CREATE FUNCTION combined_adjust(a INT, b INT) RETURNS INT RETURN a * 2 -- comment\n + b")
        .await
        .unwrap();
    engine
}

fn assert_rows(batches: &[RecordBatch], catalog: &str, namespace: &str, factor: i32) {
    let mut actual = Vec::new();
    for batch in batches {
        assert_eq!(batch.num_columns(), 4);
        let names: Vec<_> = (0..3)
            .map(|i| {
                assert_eq!(batch.column(i).data_type(), &DataType::Utf8);
                batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
            })
            .collect();
        assert_eq!(batch.column(3).data_type(), &DataType::Int32);
        let adjusted = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            for (column, expected) in names.iter().zip([catalog, namespace, namespace]) {
                assert!(!column.is_null(row));
                assert_eq!(column.value(row), expected);
            }
            actual.push((!adjusted.is_null(row)).then(|| adjusted.value(row)));
        }
    }
    let mut expected: Vec<_> = [Some(2), Some(5), None]
        .into_iter()
        .cycle()
        .take(6)
        .map(|v| v.map(|v| v * factor + namespace.len() as i32))
        .collect();
    actual.sort();
    expected.sort();
    assert_eq!(
        actual, expected,
        "typed values, nulls and multiplicity must all match"
    );
}

async fn assert_both_entries(engine: &Engine, catalog: &str, namespace: &str, factor: i32) {
    assert_rows(
        &engine.sql(QUERY).await.unwrap(),
        catalog,
        namespace,
        factor,
    );
    let (batches, stats) = engine.sql_with_stats(QUERY).await.unwrap();
    assert_rows(&batches, catalog, namespace, factor);
    assert_eq!(stats.output_rows, 6);
}

#[tokio::test]
async fn udf_replacement_preserves_validation_and_refreshes_the_same_query() {
    let engine = engine().await;
    let session = engine.for_session("replacement");
    session.sql("USE prod.sales").await.unwrap();
    assert_both_entries(&session, "prod", "sales", 2).await;
    let before = engine.export_udfs_json();
    for definition in [
        "CREATE OR REPLACE FUNCTION combined_adjust(a INT, b INT) RETURNS INT RETURN a -- comment\n + length(a)",
        "CREATE OR REPLACE FUNCTION combined_adjust(a INT, b INT) RETURNS DOUBLE RETURN a + b",
    ] {
        let err = session.sql(definition).await.unwrap_err();
        assert!(matches!(err, Error::Plan(ref message) if message.contains("unsupported")), "{err}");
        assert_eq!(engine.export_udfs_json(), before);
        assert_both_entries(&session, "prod", "sales", 2).await;
    }
    session.set_current_namespace("finance").await.unwrap();
    session
        .sql("CREATE OR REPLACE FUNCTION combined_adjust(a INT, b INT) RETURNS INT RETURN a * 3 -- comment\r\n + b")
        .await
        .unwrap();
    assert_both_entries(&session, "prod", "finance", 3).await;
    // UDF registration remains shared; only current-name state is session-specific.
    // This is not a claim of the separate session-registry isolation contract.
    assert_both_entries(
        &engine.for_session("builtin"),
        "spark_catalog",
        "default",
        3,
    )
    .await;
}

#[tokio::test]
async fn local_join_replanning_preserves_names_and_integer_udf_values() {
    let engine = engine().await;
    let session = engine.for_session("join");
    session.sql("USE prod.sales").await.unwrap();
    let query = QUERY.replace(
        "FROM spark_catalog.default.combined_input",
        "FROM spark_catalog.default.combined_input \
         LEFT JOIN (VALUES (2), (5)) AS matches(k) ON v = k",
    );
    let ordinary = session.sql(&query).await.unwrap();
    assert_rows(&ordinary, "prod", "sales", 2);
    let replanned = oxidant_loom::with_join_strategy_flipped(session.sql(&query))
        .await
        .unwrap();
    assert_rows(&replanned, "prod", "sales", 2);
    let (batches, stats) = session.sql_with_stats(&query).await.unwrap();
    assert_rows(&batches, "prod", "sales", 2);
    assert_eq!(stats.output_rows, 6);
    // A replan must not erase the shared input or UDF registration for later queries.
    assert_both_entries(&session, "prod", "sales", 2).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_partitioned_udfs_consume_each_sessions_current_names() {
    let engine = engine().await;
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let mut tasks = Vec::new();
    for (id, catalog, namespaces) in [
        ("sales", "prod", ["sales", "finance"]),
        ("finance", "prod", ["finance", "sales"]),
        ("builtin", "spark_catalog", ["default", "default"]),
    ] {
        let session = engine.for_session(id);
        session.set_current_catalog(catalog).await.unwrap();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            for namespace in namespaces.into_iter().cycle().take(8) {
                session
                    .sql(&format!("USE {catalog}.{namespace}"))
                    .await
                    .unwrap();
                assert_eq!(
                    session.current_catalog_and_namespace(),
                    (catalog.into(), vec![namespace.into()])
                );
                assert_both_entries(&session, catalog, namespace, 2).await;
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
}
