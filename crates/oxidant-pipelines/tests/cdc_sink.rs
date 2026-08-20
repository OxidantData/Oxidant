//! `CdcMergeSink` against a real Delta table: the state cache, the empty-target classifier, and
//! the replay path.
//!
//! These are the parts exactly-once rests on and they are not reachable from the merge's own
//! unit tests, which never touch a sink. Everything here goes through `LakeSink` over a temp
//! directory, so a commit is a real Delta commit and a replay is a real `txn` dedup.

use std::sync::Arc;

use oxidant_config::AutoCdcConfig;
use oxidant_loom::arrow::array::{Int64Array, StringArray};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;
use oxidant_pipelines::{CdcMerge, CdcMergeSink};
use oxidant_streaming::{writable_format, LakeSink, LakeSinkOptions, LakeTarget, Sink};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("seq", DataType::Int64, false),
        Field::new("op", DataType::Utf8, true),
    ]))
}

fn cfg() -> AutoCdcConfig {
    AutoCdcConfig {
        source: "src".into(),
        keys: vec!["id".into()],
        sequence_by: "seq".into(),
        apply_as_deletes: Some("op = 'D'".into()),
        apply_as_truncates: None,
        column_list: None,
        except_column_list: Some(vec!["op".into()]),
        ignore_null_updates_columns: None,
        ignore_null_updates_except: None,
    }
}

fn batch(rows: &[(i64, &str, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| Some(r.1)).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| Some(r.3)).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch")
}

/// A fresh sink over `location`, as a restart would build one: no cached state, everything it
/// knows about the target comes from the Delta log.
async fn open_sink(engine: &Engine, location: &str, target_table: &str) -> CdcMergeSink {
    let merge = CdcMerge::new(&cfg(), &schema(), "sink_test").expect("plan merge");
    let inner = LakeSink::open(
        engine,
        LakeTarget::location_only(location, writable_format("delta").expect("delta")),
        merge.schema(),
        LakeSinkOptions {
            app_id: Some("cdc-sink-test".into()),
            partition_columns: vec![],
            publish_iceberg: false,
            iceberg_table_suffix: "_iceberg".into(),
            checkpoint_interval: 0,
        },
    )
    .await
    .expect("open lake sink");
    CdcMergeSink::new(engine.clone(), merge, target_table.to_string(), inner)
}

/// Read the committed table, not the sink's cache.
async fn committed(engine: &Engine, location: &str) -> Vec<(i64, String, i64)> {
    engine
        .register_delta("__probe", location)
        .await
        .expect("register delta");
    let out = engine
        .sql("SELECT id, name, seq FROM __probe ORDER BY id")
        .await
        .expect("read target");
    engine.deregister_table("__probe");
    let mut rows = Vec::new();
    for b in out {
        let ids = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id");
        let names = b
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name");
        let seqs = b
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("seq");
        for i in 0..b.num_rows() {
            rows.push((ids.value(i), names.value(i).to_string(), seqs.value(i)));
        }
    }
    rows
}

#[tokio::test]
async fn a_restarted_sink_merges_against_the_committed_table() {
    let dir = tempfile::tempdir().expect("tempdir");
    let location = dir.path().join("scd1").to_string_lossy().to_string();
    let engine = Engine::new();

    // First batch: the target has never been committed to, so there is nothing to read.
    let mut sink = open_sink(&engine, &location, "cdc_target").await;
    sink.write_batch(&[batch(&[(1, "ada", 1, "I"), (2, "bob", 1, "I")])], 0)
        .await
        .expect("batch 0");
    assert_eq!(
        committed(&engine, &location).await,
        vec![(1, "ada".into(), 1), (2, "bob".into(), 1)]
    );

    // Restart: a brand new sink, whose cache is empty, must reconstruct the target from Delta
    // rather than treating it as absent and overwriting it with just this batch.
    engine
        .register_delta("cdc_target", &location)
        .await
        .expect("register target");
    let mut sink = open_sink(&engine, &location, "cdc_target").await;
    sink.write_batch(&[batch(&[(1, "ada2", 2, "U"), (3, "cy", 1, "I")])], 1)
        .await
        .expect("batch 1");
    assert_eq!(
        committed(&engine, &location).await,
        vec![
            (1, "ada2".into(), 2),
            (2, "bob".into(), 1),
            (3, "cy".into(), 1)
        ],
        "a restart must merge into the existing table, not replace it"
    );
}

#[tokio::test]
async fn a_replayed_batch_leaves_the_table_alone_and_drops_the_cache() {
    let dir = tempfile::tempdir().expect("tempdir");
    let location = dir.path().join("scd1").to_string_lossy().to_string();
    let engine = Engine::new();

    let mut sink = open_sink(&engine, &location, "cdc_target").await;
    sink.write_batch(&[batch(&[(1, "ada", 1, "I")])], 0)
        .await
        .expect("batch 0");
    sink.write_batch(&[batch(&[(2, "bob", 1, "I")])], 1)
        .await
        .expect("batch 1");
    let after_two = committed(&engine, &location).await;
    assert_eq!(after_two, vec![(1, "ada".into(), 1), (2, "bob".into(), 1)]);

    // A restart that resumes from a checkpoint saved *before* the last commit replays batch 1.
    // The Delta `txn` dedup drops it, so the table must not change. The replayed batch here
    // carries an extra row, which is the case that matters: whatever the sink recomputes was
    // *not* committed, so caching it would leave every later batch merging against a base the
    // table does not have — and the next commit would then persist the phantom row.
    engine
        .register_delta("cdc_target", &location)
        .await
        .expect("register target");
    let mut sink = open_sink(&engine, &location, "cdc_target").await;
    let rows = sink
        .write_batch(&[batch(&[(2, "bob", 1, "I"), (99, "phantom", 1, "I")])], 1)
        .await
        .expect("replay of batch 1");
    assert_eq!(rows, 0, "a replayed batch commits nothing");
    assert_eq!(
        committed(&engine, &location).await,
        after_two,
        "a replayed batch must not change the table"
    );

    // The batch after the replay lands on the real contents: the sink dropped its cache on the
    // dedup, so it re-reads rather than trusting a merge that was never committed.
    engine.deregister_table("cdc_target");
    engine
        .register_delta("cdc_target", &location)
        .await
        .expect("re-register target");
    sink.write_batch(&[batch(&[(3, "cy", 1, "I")])], 2)
        .await
        .expect("batch 2");
    assert_eq!(
        committed(&engine, &location).await,
        vec![
            (1, "ada".into(), 1),
            (2, "bob".into(), 1),
            (3, "cy".into(), 1)
        ],
        "the deduplicated batch's rows must not reappear through the cache"
    );
}

#[tokio::test]
async fn a_read_failure_over_a_live_table_is_an_error_not_an_empty_target() {
    let dir = tempfile::tempdir().expect("tempdir");
    let location = dir.path().join("scd1").to_string_lossy().to_string();
    let engine = Engine::new();

    let mut sink = open_sink(&engine, &location, "cdc_target").await;
    sink.write_batch(&[batch(&[(1, "ada", 1, "I"), (2, "bob", 1, "I")])], 0)
        .await
        .expect("batch 0");
    let live = committed(&engine, &location).await;
    assert_eq!(live.len(), 2);

    // The target *has* been committed to, but the name does not resolve — a catalog blip, or a
    // 404 on a log file. The resulting error says "not found", which a substring classifier
    // read as "empty target", making the next commit overwrite the table with just this batch.
    let mut sink = open_sink(&engine, &location, "no_such_table").await;
    let err = sink
        .write_batch(&[batch(&[(3, "cy", 1, "I")])], 1)
        .await
        .expect_err("an unreadable live target must fail the batch");
    assert!(err.to_string().contains("no_such_table"), "{err}");
    assert_eq!(
        committed(&engine, &location).await,
        live,
        "a failed read must not overwrite the table"
    );
}
