//! The pipeline's own state on a real object store.
//!
//! `_pipeline-state.json` and `reconcile.json` sit beside the offsets and are just as much part
//! of "the pipeline's replay position": the first says which tables are built and at what epoch,
//! the second is the only record that a pipeline reconciles itself at all. Both used to be
//! written with `std::fs`, so on an `s3://` checkpoint root they landed under the driver's
//! working directory in a folder named `s3:` — a full refresh on every driver replacement, and a
//! reconcile schedule that silently stopped existing.
//!
//! Skipped unless `OXIDANT_MINIO_TEST=1`; see `crates/oxidant-streaming/tests/minio_checkpoints.rs`
//! for how to bring MinIO up locally.

use oxidant_pipelines::{
    checkpoint_store, clear_pipeline_state, ReconcileSchedule, DEFAULT_SAMPLE,
};
use oxidant_streaming::Engine;

const BUCKET: &str = "oxidant-test";

fn minio_enabled() -> bool {
    std::env::var("OXIDANT_MINIO_TEST")
        .ok()
        .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
}

fn use_minio() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let endpoint = std::env::var("OXIDANT_MINIO_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9000".into());
        std::env::set_var("AWS_ENDPOINT", endpoint);
        std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin123");
        std::env::set_var("AWS_REGION", "us-east-1");
        std::env::set_var("AWS_ALLOW_HTTP", "true");
    });
}

fn unique_root(what: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("a clock after 1970")
        .as_nanos();
    format!(
        "s3://{BUCKET}/oxidant-pipeline-test/{what}-{}-{nanos}",
        std::process::id()
    )
}

/// The state a full refresh clears is in the bucket, not beside the shell that ran it.
#[tokio::test]
async fn pipeline_state_is_written_into_the_bucket() {
    if !minio_enabled() {
        eprintln!("skipping: set OXIDANT_MINIO_TEST=1 (and start MinIO) to run this");
        return;
    }
    use_minio();
    let root = unique_root("state");
    let engine = Engine::new();

    clear_pipeline_state(&engine, &root, &[])
        .await
        .expect("writes the state into the bucket");

    let store = checkpoint_store(&engine, &root).expect("resolves");
    let bytes = store
        .read("_pipeline-state.json")
        .await
        .expect("reads")
        .expect("the object is there");
    let state: serde_json::Value = serde_json::from_slice(&bytes).expect("valid JSON");
    assert!(state.get("tables").is_some(), "got: {state}");
    assert!(
        !std::env::current_dir().unwrap().join("s3:").exists(),
        "an s3:// checkpoint root must never become a local `s3:` directory"
    );
}

/// A reconcile schedule registered against an `s3://` root is there for the next driver.
#[tokio::test]
async fn a_reconcile_schedule_round_trips_through_the_bucket() {
    if !minio_enabled() {
        eprintln!("skipping: set OXIDANT_MINIO_TEST=1 (and start MinIO) to run this");
        return;
    }
    use_minio();
    let root = unique_root("schedule");
    let engine = Engine::new();
    let store = checkpoint_store(&engine, &root).expect("resolves");

    assert!(ReconcileSchedule::load(&store).await.is_none());
    let schedule = ReconcileSchedule {
        path: Some("/srv/oxidant.yaml".into()),
        cron: "0 6 * * *".into(),
        tables: vec!["public.sales_suppliers".into()],
        sample: DEFAULT_SAMPLE,
        created: "2026-08-23T05:00:00Z".into(),
        last_run: None,
        last_result: None,
    };
    schedule.save(&store).await.expect("writes");

    // The path an operator is told to look at is the bucket, not a local file they will not find.
    assert_eq!(
        ReconcileSchedule::path_in(&store),
        format!("{root}/reconcile.json")
    );

    // A different resolution of the same root — a different process, in production — reads it.
    let reread = checkpoint_store(&Engine::new(), &root).expect("resolves");
    assert_eq!(
        ReconcileSchedule::load(&reread).await.expect("reads back"),
        schedule
    );

    assert!(ReconcileSchedule::remove(&reread).await.expect("removes"));
    assert!(!ReconcileSchedule::remove(&reread)
        .await
        .expect("idempotent"));
    assert!(ReconcileSchedule::load(&reread).await.is_none());
}
