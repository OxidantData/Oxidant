//! Checkpoints on a real object store, against the MinIO the CI job runs.
//!
//! The claim under test is the only one that matters for an `s3://` checkpoint root: **the
//! replay position outlives the process that wrote it**. Everything else about checkpointing is
//! covered by the unit tests on a temp directory, and none of it distinguishes a checkpoint that
//! reached the bucket from one that landed in a local directory named `s3:` — which is exactly
//! what a filesystem write of `s3://bucket/...` produces, silently, while the query runs
//! perfectly and its restart story is fiction.
//!
//! Skipped unless `OXIDANT_MINIO_TEST=1`. `.github/workflows/ci.yml` starts MinIO with the
//! `oxidant-test` bucket and sets that variable plus `OXIDANT_MINIO_ENDPOINT` for the workspace
//! test job; locally,
//!
//! ```text
//! docker run -d --name oxidant-minio -p 9000:9000 \
//!   -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin123 \
//!   quay.io/minio/minio:latest server /data
//! docker run --rm --network host -e MC_HOST_local=http://minioadmin:minioadmin123@127.0.0.1:9000 \
//!   minio/mc mb --ignore-existing local/oxidant-test
//! OXIDANT_MINIO_TEST=1 cargo test -p oxidant-streaming --test minio_checkpoints
//! ```

use std::collections::{BTreeMap, HashMap};

use oxidant_streaming::{
    checkpoint_store, ConnectorLog, Engine, SinkDestination, StartOptions, StreamQueryConfig,
    StreamingQueryManager, Trigger,
};

const BUCKET: &str = "oxidant-test";

fn minio_enabled() -> bool {
    std::env::var("OXIDANT_MINIO_TEST")
        .ok()
        .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
}

fn endpoint() -> String {
    std::env::var("OXIDANT_MINIO_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".into())
}

/// Point the *ambient* AWS environment at MinIO.
///
/// Deliberately the environment and not a `storage_options` map: the engine resolver these
/// checkpoints go through takes no options for a checkpoint root — it is the same auth path the
/// table writes use — so configuring it any other way would be testing a path production does
/// not take.
fn use_minio() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        std::env::set_var("AWS_ENDPOINT", endpoint());
        std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin123");
        std::env::set_var("AWS_REGION", "us-east-1");
        std::env::set_var("AWS_ALLOW_HTTP", "true");
    });
}

/// A prefix nothing else in this binary — or in a previous run — is using.
fn unique_root(what: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("a clock after 1970")
        .as_nanos();
    format!(
        "s3://{BUCKET}/oxidant-ckpt-test/{what}-{}-{nanos}",
        std::process::id()
    )
}

/// A Kafka query reading the offline spool in `dir` and writing to memory.
///
/// The spool source rather than a broker because what is under test is the *checkpoint*, and the
/// spool gives a source with real, replayable offsets and no service to stand up.
fn spool_query(dir: &std::path::Path) -> StreamQueryConfig {
    let source_options: BTreeMap<String, String> = [
        ("subscribe".to_string(), "events".to_string()),
        (
            "oxidant.spool.dir".to_string(),
            dir.to_string_lossy().into_owned(),
        ),
    ]
    .into_iter()
    .collect();
    StreamQueryConfig {
        source_format: "kafka".into(),
        source_options,
        ..StreamQueryConfig::from_spark(
            "kafka",
            &HashMap::new(),
            "memory",
            SinkDestination::None,
            &HashMap::new(),
            vec![],
        )
    }
}

/// A checkpoint on S3 is still there for a process that did not write it.
///
/// The restart is a whole new [`Engine`] and a whole new [`StreamingQueryManager`] — nothing of
/// the first run is in memory — which is the closest thing to a driver replacement a test can
/// stage. If the checkpoint had been ephemeral, the second run would replay the first run's rows
/// and this asserts it does not.
#[tokio::test]
async fn a_checkpoint_on_s3_outlives_the_process_that_wrote_it() {
    if !minio_enabled() {
        eprintln!("skipping: set OXIDANT_MINIO_TEST=1 (and start MinIO) to run this");
        return;
    }
    use_minio();
    let root = unique_root("resume");
    let spool = tempfile::TempDir::new().expect("a spool directory");
    std::fs::write(spool.path().join("batch-0.json"), "{\"a\":1}\n{\"a\":2}\n").unwrap();

    // --- first process ---------------------------------------------------------------------
    let first_state = {
        let engine = Engine::new();
        let manager = StreamingQueryManager::new();
        let id = manager
            .start_with_config(
                &engine,
                "orders".into(),
                root.clone(),
                Trigger::Once,
                spool_query(spool.path()),
                StartOptions::default(),
            )
            .await
            .expect("the query starts against the bucket");
        assert_eq!(
            manager
                .process_all_available(&id.id, &engine)
                .await
                .expect("a batch runs"),
            2,
            "the first run reads both rows"
        );

        let state = checkpoint_store(&engine, &root)
            .expect("resolves")
            .load()
            .await
            .expect("the checkpoint reads back out of the bucket");
        assert_eq!(
            state
                .source_offsets
                .as_ref()
                .and_then(|o| o.entries.get("offset")),
            Some(&2),
            "the committed offset is in the bucket, not in this process"
        );
        state
    };

    // The trap this whole feature exists to close: a filesystem write of an `s3://` location
    // creates a *relative* directory whose first component is `s3:`.
    assert!(
        !std::env::current_dir().unwrap().join("s3:").exists(),
        "an s3:// checkpoint root must never become a local `s3:` directory"
    );

    // --- new rows arrive, then a different process picks the pipeline up --------------------
    std::fs::write(spool.path().join("batch-1.json"), "{\"a\":3}\n{\"a\":4}\n").unwrap();

    let engine = Engine::new();
    let manager = StreamingQueryManager::new();
    let id = manager
        .start_with_config(
            &engine,
            "orders".into(),
            root.clone(),
            Trigger::Once,
            spool_query(spool.path()),
            StartOptions::default(),
        )
        .await
        .expect("the replacement query starts on the same checkpoint");
    assert_eq!(
        manager
            .process_all_available(&id.id, &engine)
            .await
            .expect("a batch runs"),
        2,
        "the replacement reads only the two new rows — a lost checkpoint would replay all four"
    );

    let resumed = checkpoint_store(&engine, &root)
        .expect("resolves")
        .load()
        .await
        .expect("reads back");
    assert_eq!(
        resumed
            .source_offsets
            .as_ref()
            .and_then(|o| o.entries.get("offset")),
        Some(&4),
        "and it committed from where the first run left off"
    );
    assert_eq!(
        resumed.query_id, first_state.query_id,
        "the query's identity outlives the process"
    );
    assert_ne!(resumed.run_id, first_state.run_id, "but the run is new");
}

/// A bucket that is not there stops the query at `start`, naming the root.
///
/// Without the probe, the first thing a bogus root produces is a failed checkpoint write inside
/// whatever the connector was doing — and for a `postgres_cdc` pipeline that happens *after* the
/// replication slot is open, with the complaint written to a connector log in the same bucket
/// that is not there.
#[tokio::test]
async fn a_bogus_bucket_is_refused_at_start_and_says_which_one() {
    if !minio_enabled() {
        eprintln!("skipping: set OXIDANT_MINIO_TEST=1 (and start MinIO) to run this");
        return;
    }
    use_minio();
    let root = format!(
        "s3://oxidant-no-such-bucket-{}/checkpoints/orders",
        std::process::id()
    );
    let spool = tempfile::TempDir::new().expect("a spool directory");
    std::fs::write(spool.path().join("batch-0.json"), "{\"a\":1}\n").unwrap();

    let engine = Engine::new();
    let manager = StreamingQueryManager::new();
    let err = manager
        .start_with_config(
            &engine,
            "orders".into(),
            root.clone(),
            Trigger::Once,
            spool_query(spool.path()),
            StartOptions::default(),
        )
        .await
        .expect_err("a missing bucket cannot host a checkpoint");

    let message = err.to_string();
    assert!(
        message.contains(&root),
        "the error names the root an operator has to fix: {message}"
    );
    assert!(
        message.contains("not reachable"),
        "the error says what is wrong with it: {message}"
    );
}

/// The connector log lands in the bucket, and a reader finds it under `logs/<name>.jsonl`.
///
/// The path the platform console reads. It used to be written with `std::fs` regardless of where
/// the checkpoints went, so on an `s3://` root the record of why a pipeline stopped lived on the
/// driver's disk and went with the instance.
#[tokio::test]
async fn a_connector_log_lands_in_the_bucket_beside_the_checkpoints() {
    if !minio_enabled() {
        eprintln!("skipping: set OXIDANT_MINIO_TEST=1 (and start MinIO) to run this");
        return;
    }
    use_minio();
    let root = unique_root("logs");
    let engine = Engine::new();

    let log = ConnectorLog::open(
        Some(&engine),
        Some(&format!("{root}/logs")),
        "public.sales_suppliers",
    );
    assert!(
        log.path().is_none(),
        "a log on an object store has no filesystem path to name"
    );
    log.event(
        "snapshot_start",
        serde_json::json!({ "table": "public.sales_suppliers" }),
    );
    log.error("slot is gone", true);
    log.flush().await;

    let store = checkpoint_store(&engine, &root).expect("resolves");
    let listed = store.list("logs").await.expect("lists the prefix");
    assert_eq!(
        listed.iter().map(|o| o.name.as_str()).collect::<Vec<_>>(),
        vec!["public.sales_suppliers.jsonl"],
        "the file name is sanitized and sits directly under `logs/`"
    );

    let bytes = store
        .read("logs/public.sales_suppliers.jsonl")
        .await
        .expect("reads")
        .expect("the object is there");
    let events: Vec<serde_json::Value> = String::from_utf8(bytes)
        .expect("utf-8")
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("each line is one JSON object"))
        .collect();
    assert_eq!(
        events.len(),
        2,
        "both events reached the bucket: {events:?}"
    );
    assert_eq!(events[0]["event"], "snapshot_start");
    assert_eq!(events[1]["event"], "error");
    assert_eq!(events[1]["will_retry"], true);
}
