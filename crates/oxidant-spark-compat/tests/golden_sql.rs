//! Integration tests for the Spark golden SQL parity harness.
//!
//! Two fast tests guard the harness wiring on every `cargo test`. The full-corpus **ratchet**
//! (`full_corpus_does_not_regress`) is `#[ignore]`d here because it replays ~12k queries; it is
//! the same check CI runs via the `oxidant-parity` binary against `parity/baseline.json`, and can
//! be run locally with `cargo test -p oxidant-spark-compat -- --ignored`.

use std::path::PathBuf;

use oxidant_spark_compat::runner;
use oxidant_spark_compat::Corpus;

/// The vendored corpus must be present and substantial — guards an accidental wipe.
#[test]
fn corpus_is_vendored() {
    let results = PathBuf::from(oxidant_spark_compat::CORPUS_DIR).join("results");
    let count = std::fs::read_dir(&results)
        .expect("spark-tests/results must exist")
        .count();
    assert!(
        count > 50,
        "expected a populated results dir, found {count} entries"
    );
}

/// The authored Databricks corpus is independently selectable and its explicit skips remain
/// visible in the report instead of being silently omitted.
#[tokio::test(flavor = "multi_thread")]
async fn databricks_corpus_reports_explicit_skips() {
    let report = runner::run_corpus(Corpus::Databricks, Some("copy-into")).await;
    assert_eq!(report.files_total, 1);
    assert_eq!(report.files_skipped, 1);
    assert_eq!(report.blocks_total, 0);
    assert_eq!(
        report.files[0].skipped.as_deref(),
        Some("requires-delta-storage")
    );
    assert!(report.to_markdown().contains("requires-delta-storage"));
}

/// A representative file replays end-to-end: blocks parse, run, and classify into buckets.
/// `group-by.sql` is a good canary — temp-view setup + aggregates + analysis errors.
#[tokio::test(flavor = "multi_thread")]
async fn group_by_file_replays() {
    let report = runner::run_file(Corpus::Spark, "group-by.sql.out").await;
    assert!(report.skipped.is_none(), "group-by should not be skipped");
    assert!(
        report.total > 10,
        "expected many blocks, got {}",
        report.total
    );
    // Every block lands in exactly one bucket, so bucket counts sum to total.
    let summed: usize = report.buckets.values().sum();
    assert_eq!(summed, report.total, "every block must be classified");
}

/// Ratchet: the live corpus pass-counts must not drop below the committed baseline. This is the
/// regression gate — improving oxidant raises the baseline (see `oxidant-parity` + CI), a regression
/// trips this. Ignored by default because it replays the whole corpus.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "full corpus replay; run with --ignored or in CI"]
async fn full_corpus_does_not_regress() {
    #[derive(serde::Deserialize)]
    struct Baseline {
        strict_pass: usize,
        semantic_pass: usize,
        blocks_total: usize,
    }
    let baseline_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("parity")
        .join("baseline.json");
    let baseline: Baseline =
        serde_json::from_str(&std::fs::read_to_string(&baseline_path).expect("baseline.json"))
            .expect("parse baseline.json");

    let report = runner::run_corpus(Corpus::Spark, None).await;
    // Corpus size is stable for a pinned Spark tag; a big swing means the corpus changed.
    assert_eq!(
        report.blocks_total, baseline.blocks_total,
        "corpus size changed — re-baseline if the Spark tag was bumped"
    );
    assert!(
        report.strict_pass >= baseline.strict_pass,
        "strict parity regressed: {} < baseline {}",
        report.strict_pass,
        baseline.strict_pass
    );
    assert!(
        report.semantic_pass >= baseline.semantic_pass,
        "semantic parity regressed: {} < baseline {}",
        report.semantic_pass,
        baseline.semantic_pass
    );
}
