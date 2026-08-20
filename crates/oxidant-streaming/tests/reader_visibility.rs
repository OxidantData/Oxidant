//! What a sink commits must be visible to the next reader of the table's *name*.
//!
//! A resolved `TableProvider` embeds the file list it was built from, and the catalog bridge
//! caches lakehouse entries with no TTL revalidation — so the first query to touch a table pins
//! the snapshot it saw. Every later commit by this process is then invisible to it, silently:
//! a `SELECT` returns the old rows, and a *derived* table recomputed from that read is committed
//! missing everything its upstream has written since. `LakeSink` closes that by invalidating the
//! name after each landed commit; these tests are what says so.
//!
//! Real commits over a real local catalog — the caching only exists on the catalog-resolved path,
//! so an in-memory fake would prove nothing.

use std::collections::HashMap;
use std::sync::Arc;

use oxidant_catalog::TableFormat;
use oxidant_loom::arrow::array::Int64Array;
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;
use oxidant_streaming::{LakeSink, LakeSinkOptions, LakeTarget, Sink};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn batch(ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(ids.to_vec()))]).expect("batch")
}

/// An engine with one registered local catalog, and a temp dir kept alive alongside it.
async fn engine_with_catalog() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let warehouse = dir.path().join("wh").to_string_lossy().to_string();
    let engine = Engine::new();
    let catalog = oxidant_catalog_local::LocalCatalog::new(
        "local",
        warehouse,
        HashMap::new(),
        Vec::new(),
        Vec::new(),
    )
    .await
    .expect("local catalog");
    engine.register_catalog("local", Arc::new(catalog));
    (engine, dir)
}

async fn open_sink(engine: &Engine, table: &str) -> LakeSink {
    open_sink_with(engine, table, schema()).await
}

async fn open_sink_with(engine: &Engine, table: &str, schema: SchemaRef) -> LakeSink {
    LakeSink::open(
        engine,
        LakeTarget {
            catalog: Some("local".into()),
            namespace: vec!["live".into()],
            table: table.into(),
            format: TableFormat::Delta,
            location: None,
        },
        schema,
        LakeSinkOptions {
            app_id: Some(format!("visibility-{table}")),
            partition_columns: vec![],
            publish_iceberg: false,
            iceberg_table_suffix: "_iceberg".into(),
            checkpoint_interval: 0,
        },
    )
    .await
    .expect("open sink")
}

/// Read a table by NAME — the path that caches.
async fn ids(engine: &Engine, name: &str) -> Vec<i64> {
    let out = engine
        .sql(&format!("SELECT id FROM {name} ORDER BY id"))
        .await
        .expect("read");
    let mut ids = Vec::new();
    for b in out {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id");
        for i in 0..b.num_rows() {
            ids.push(col.value(i));
        }
    }
    ids
}

#[tokio::test]
async fn a_streaming_append_is_visible_to_the_next_read_of_the_table_name() {
    let (engine, _dir) = engine_with_catalog().await;
    let mut sink = open_sink(&engine, "events").await;

    sink.write_batch(&[batch(&[1])], 0).await.expect("batch 0");
    // This read is what resolves — and caches — the provider.
    assert_eq!(ids(&engine, "local.live.events").await, vec![1]);

    sink.write_batch(&[batch(&[2])], 1).await.expect("batch 1");
    assert_eq!(
        ids(&engine, "local.live.events").await,
        vec![1, 2],
        "a committed micro-batch must not be invisible to a reader that resolved the name earlier"
    );
}

#[tokio::test]
async fn a_derived_table_recomputes_from_the_upstream_it_was_just_given() {
    let (engine, _dir) = engine_with_catalog().await;
    let mut upstream = open_sink(&engine, "bronze").await;
    upstream.write_batch(&[batch(&[1])], 0).await.expect("b0");

    // A derived table: read the upstream by name, replace this table's whole contents with the
    // result. That read caches the upstream's provider.
    let derive = |engine: Engine| async move {
        let rows = engine
            .sql("SELECT id FROM local.live.bronze")
            .await
            .expect("derive");
        // A query's output columns are nullable; the target has to be declared that way.
        let derived_schema = rows.first().map(|b| b.schema()).expect("derived rows");
        let mut silver = open_sink_with(&engine, "silver", derived_schema).await;
        let version = silver.committed_txn_version().max(0) as u64 + 1;
        silver.replace_batch(&rows, version).await.expect("replace");
    };
    derive(engine.clone()).await;
    assert_eq!(ids(&engine, "local.live.silver").await, vec![1]);

    // The upstream moves, and the derived table is recomputed in the same process. Before the
    // sink invalidated its name, this read served the one-row snapshot and `silver` was
    // committed missing row 2 — no error anywhere.
    upstream.write_batch(&[batch(&[2])], 1).await.expect("b1");
    derive(engine.clone()).await;
    assert_eq!(
        ids(&engine, "local.live.silver").await,
        vec![1, 2],
        "a derived table must recompute from the upstream's committed contents"
    );
}

/// A table emptied by a `replace` is still a table: both ways of reading it return no rows.
///
/// A recompute whose result is empty — an `INSERT OVERWRITE ... WHERE false`, a CDC target
/// drained by deletes — retires every live file, leaving a log with a schema and nothing to
/// infer one from. The by-name path has always been served by the catalog's declared schema;
/// the by-location path has to ask the Delta log for the same thing, and when it did not, the
/// two disagreed about the same table at the same instant: `SELECT` said empty and `read_delta`
/// said the columns could not be determined.
#[tokio::test]
async fn an_emptied_table_reads_as_empty_by_name_and_by_location() {
    let (engine, _dir) = engine_with_catalog().await;
    let mut sink = open_sink(&engine, "drained").await;
    sink.write_batch(&[batch(&[1])], 0).await.expect("batch 0");
    let location = sink.location().to_string();
    assert_eq!(ids(&engine, "local.live.drained").await, vec![1]);

    sink.replace_batch(&[batch(&[])], 1)
        .await
        .expect("replace with nothing");

    assert_eq!(
        ids(&engine, "local.live.drained").await,
        Vec::<i64>::new(),
        "an emptied table must read as empty, not fail"
    );
    let rows = engine
        .read_delta("local.live.drained", &location)
        .await
        .expect("an emptied table must be readable by location too");
    assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
}
