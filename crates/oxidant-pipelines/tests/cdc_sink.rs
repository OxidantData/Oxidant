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
        // Nullable: a change feed can carry a row with no sequencing value, and rejecting that
        // batch is a behaviour under test. The merge's own output schema is all-nullable either
        // way, so this does not change what the target is declared as.
        Field::new("seq", DataType::Int64, true),
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
    rows_with_seq(
        rows,
        rows.iter()
            .map(|r| Some(r.2))
            .collect::<Vec<_>>()
            .as_slice(),
    )
}

/// The same batch with `seq` supplied separately, so a row can carry a NULL one — the case the
/// merge rejects outright.
fn rows_with_seq(rows: &[(i64, &str, i64, &str)], seq: &[Option<i64>]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| Some(r.1)).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(seq.to_vec())),
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
    open_sink_with(engine, location, target_table, cfg(), Vec::new()).await
}

/// [`open_sink`] with the flow's config and the target's partitioning spelled out.
async fn open_sink_with(
    engine: &Engine,
    location: &str,
    target_table: &str,
    config: AutoCdcConfig,
    partition_columns: Vec<String>,
) -> CdcMergeSink {
    let merge = CdcMerge::new(&config, &schema(), "sink_test").expect("plan merge");
    let inner = LakeSink::open(
        engine,
        LakeTarget::location_only(location, writable_format("delta").expect("delta")),
        merge.schema(),
        LakeSinkOptions {
            app_id: Some("cdc-sink-test".into()),
            partition_columns,
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
    // rather than treating it as absent and overwriting it with just this batch. The target is
    // deliberately never registered under its name — the sink reads its own location.
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
    // dedup, so it re-reads rather than trusting a merge that was never committed. Nothing
    // re-registers the target here on purpose — the re-read has to be fresh on its own.
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

    // The target *has* been committed to, but its data cannot be read — an object-store 404 on a
    // file the log still lists, or a credentials blip. Classifying that as "empty target" is what
    // would make the next commit overwrite the whole table with just this batch. The log is left
    // intact, so the sink still knows it has committed; only the data goes missing.
    let stash = dir.path().join("stash");
    std::fs::create_dir_all(&stash).expect("mkdir stash");
    let mut moved = Vec::new();
    for entry in std::fs::read_dir(&location).expect("read location") {
        let path = entry.expect("dir entry").path();
        if path.extension().is_some_and(|e| e == "parquet") {
            let to = stash.join(path.file_name().expect("file name"));
            std::fs::rename(&path, &to).expect("stash data file");
            moved.push((path, to));
        }
    }
    assert!(!moved.is_empty(), "the commit wrote no data files to stash");

    let mut sink = open_sink(&engine, &location, "cdc_target").await;
    let err = sink
        .write_batch(&[batch(&[(3, "cy", 1, "I")])], 1)
        .await
        .expect_err("an unreadable live target must fail the batch");
    assert!(err.to_string().contains("cdc_target"), "{err}");

    for (path, to) in moved {
        std::fs::rename(&to, &path).expect("restore data file");
    }
    assert_eq!(
        committed(&engine, &location).await,
        live,
        "a failed read must not overwrite the table"
    );
}

/// A failed batch must not cost the rows committed before it. One sink, one process, no restart.
///
/// Every path that clears the state cache — a failed batch, a deduplicated replay — makes the
/// next batch re-read the target. If that read is served from a snapshot taken before the sink's
/// own commits, the merge recomputes the table without the rows it cannot see and the next
/// `replace_batch` persists that. No error, no event: an ordinary-looking Delta commit that
/// destroys committed rows.
#[tokio::test]
async fn a_failed_batch_does_not_destroy_the_rows_committed_before_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let location = dir.path().join("scd1").to_string_lossy().to_string();
    let engine = Engine::new();

    let mut sink = open_sink(&engine, &location, "cdc_target").await;
    sink.write_batch(&[batch(&[(1, "ada", 1, "I")])], 0)
        .await
        .expect("batch 0");

    // The target is registered under its name here and never again — which is the state a
    // long-running pipeline leaves the catalog in. The provider behind that name pins the
    // one-row version, so a sink that re-reads *by name* is reading its own past.
    engine
        .register_delta("cdc_target", &location)
        .await
        .expect("register target");

    sink.write_batch(&[batch(&[(2, "bob", 1, "I")])], 1)
        .await
        .expect("batch 1");
    assert_eq!(
        committed(&engine, &location).await,
        vec![(1, "ada".into(), 1), (2, "bob".into(), 1)]
    );

    // A NULL `sequence_by` fails the batch, which drops the state cache — the likeliest way for
    // a running sink to reach the re-read path.
    let err = sink
        // The tuple's `seq` is ignored — the separate slice is what the column carries.
        .write_batch(&[rows_with_seq(&[(3, "cy", 0, "I")], &[None])], 2)
        .await
        .expect_err("a NULL sequence value must fail the batch");
    assert!(err.to_string().contains("NULL"), "{err}");
    assert_eq!(
        committed(&engine, &location).await,
        vec![(1, "ada".into(), 1), (2, "bob".into(), 1)],
        "a failed batch must not change the table"
    );

    // The next batch re-reads the target. `bob` was committed *after* the first read of this
    // table in this process, and it must still be there.
    sink.write_batch(&[batch(&[(4, "dan", 1, "I")])], 3)
        .await
        .expect("batch 3");
    assert_eq!(
        committed(&engine, &location).await,
        vec![
            (1, "ada".into(), 1),
            (2, "bob".into(), 1),
            (4, "dan".into(), 1)
        ],
        "the batch after a failure must merge against everything committed, not a stale snapshot"
    );
}

/// A target that legitimately goes **empty** must stay readable, or the flow is over.
///
/// Draining the last key leaves a Delta table with a committed log and no live files. There is
/// no data file left to infer a schema from, so a reader that does not ask the log's `metaData`
/// cannot name the table's columns and fails outright — and this sink re-reads its target
/// whenever it has no cached state (the first batch of a run, a failed batch, a deduplicated
/// replay), so that failure is not one bad query: the target can never be written to again.
#[tokio::test]
async fn a_target_emptied_by_deletes_still_takes_the_next_batch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let location = dir.path().join("scd1").to_string_lossy().to_string();
    let engine = Engine::new();

    let mut sink = open_sink(&engine, &location, "cdc_target").await;
    sink.write_batch(&[batch(&[(1, "ada", 1, "I"), (2, "bob", 1, "I")])], 0)
        .await
        .expect("batch 0");
    sink.write_batch(&[batch(&[(1, "ada", 2, "D"), (2, "bob", 2, "D")])], 1)
        .await
        .expect("batch 1 (drains the table)");
    // The plain read path too: an emptied table reads as empty, not as an error.
    assert_eq!(committed(&engine, &location).await, vec![]);

    // The next run: a fresh sink with no cached state, so this batch goes through `read_target`.
    let mut sink = open_sink(&engine, &location, "cdc_target").await;
    sink.write_batch(&[batch(&[(3, "cy", 3, "I")])], 2)
        .await
        .expect("a batch after an emptied target must still merge");
    assert_eq!(
        committed(&engine, &location).await,
        vec![(3, "cy".into(), 3)],
        "the re-inserted row is the whole table"
    );
}

/// The same, reached by `APPLY AS TRUNCATE` rather than by deletes — the other way a target
/// legitimately empties, and the one a source can trigger with a single row.
#[tokio::test]
async fn a_target_emptied_by_a_truncate_still_takes_the_next_batch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let location = dir.path().join("scd1").to_string_lossy().to_string();
    let engine = Engine::new();
    let mut config = cfg();
    config.apply_as_truncates = Some("op = 'T'".into());

    let mut sink =
        open_sink_with(&engine, &location, "cdc_target", config.clone(), Vec::new()).await;
    sink.write_batch(&[batch(&[(1, "ada", 1, "I"), (2, "bob", 1, "I")])], 0)
        .await
        .expect("batch 0");
    sink.write_batch(&[batch(&[(9, "-", 5, "T")])], 1)
        .await
        .expect("truncate batch");
    assert_eq!(committed(&engine, &location).await, vec![]);

    let mut sink = open_sink_with(&engine, &location, "cdc_target", config, Vec::new()).await;
    sink.write_batch(&[batch(&[(3, "cy", 6, "I")])], 2)
        .await
        .expect("a batch after a truncate must still merge");
    assert_eq!(
        committed(&engine, &location).await,
        vec![(3, "cy".into(), 6)]
    );
}

/// A partitioned target has always survived being emptied — its declared schema was supplied
/// because the partition columns had to be. Pinned so the unpartitioned case cannot regress back
/// to being the only one that is handled.
///
/// Asserted by row count rather than through `committed`: a bare-path read of a partitioned Delta
/// table serves the partition column in the position the *scan* puts it (last), not the position
/// the declared schema gives it, so `id, name, seq` comes back mis-ordered here. That is a
/// separate, pre-existing defect of the partitioned read path — this test is about the empty one.
#[tokio::test]
async fn a_partitioned_target_emptied_by_deletes_still_takes_the_next_batch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let location = dir.path().join("scd1").to_string_lossy().to_string();
    let engine = Engine::new();
    let partitioned = || vec!["name".to_string()];

    let mut sink = open_sink_with(&engine, &location, "cdc_target", cfg(), partitioned()).await;
    sink.write_batch(&[batch(&[(1, "ada", 1, "I")])], 0)
        .await
        .expect("batch 0");
    sink.write_batch(&[batch(&[(1, "ada", 2, "D")])], 1)
        .await
        .expect("batch 1 (drains the table)");
    assert_eq!(committed_rows(&engine, &location).await, 0);

    let mut sink = open_sink_with(&engine, &location, "cdc_target", cfg(), partitioned()).await;
    sink.write_batch(&[batch(&[(3, "cy", 3, "I")])], 2)
        .await
        .expect("a batch after an emptied partitioned target must still merge");
    assert_eq!(committed_rows(&engine, &location).await, 1);
}

/// How many rows the committed table holds — [`committed`] without reading any column of it.
async fn committed_rows(engine: &Engine, location: &str) -> i64 {
    engine
        .register_delta("__count_probe", location)
        .await
        .expect("register delta");
    let out = engine
        .sql("SELECT COUNT(*) AS c FROM __count_probe")
        .await
        .expect("count target");
    engine.deregister_table("__count_probe");
    out[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count")
        .value(0)
}
