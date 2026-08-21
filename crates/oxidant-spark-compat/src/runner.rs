//! Replay the vendored golden corpus through oxidant and classify every block.
//!
//! Per file we spin up **one** [`oxidant_loom::Engine`] and replay the golden blocks in order, so
//! `CREATE TEMPORARY VIEW` / `CREATE TABLE` setup persists for the queries that follow (exactly
//! how Spark's `SQLQueryTestSuite` runs a file). The golden `.sql.out` is the source of truth
//! for *what* to run — we never re-derive the statement list.

use std::path::Path;

use oxidant_loom::Engine;

use crate::classify::{classify, Verdict};
use crate::format::format_result;
use crate::report::{CorpusReport, FileReport};
use crate::{golden, splitter, Corpus, GoldenBlock, Outcome};

/// The corpus tag the run was scored against (from `<corpus>/VERSION`).
pub fn corpus_version(corpus: Corpus) -> String {
    std::fs::read_to_string(corpus.root().join("VERSION"))
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("Tag:").map(|v| v.trim().to_string()))
        })
        .unwrap_or_else(|| "unknown".to_string())
}

/// The Spark tag the vendored corpus was vendored at (from `spark-tests/VERSION`).
pub fn spark_version() -> String {
    corpus_version(Corpus::Spark)
}

/// Replay one golden file (path relative to `results/`, e.g. `group-by.sql.out` or
/// `subquery/in-subquery/x.sql.out`) and return its report.
pub async fn run_file(corpus: Corpus, rel_out: &str) -> FileReport {
    let root = corpus.root();
    let out_path = root.join("results").join(rel_out);
    // results/<rel>.sql.out  ->  inputs/<rel>.sql
    let rel_input = rel_out.strip_suffix(".out").unwrap_or(rel_out);
    let in_path = root.join("inputs").join(rel_input);

    // Skip whole files that need machinery we don't have yet — but record *why*.
    if let Ok(input_sql) = std::fs::read_to_string(&in_path) {
        if let Some(reason) = splitter::skip_reason(&input_sql) {
            return FileReport::skipped(rel_input, reason.as_str());
        }
    }

    let text = match std::fs::read_to_string(&out_path) {
        Ok(t) => t,
        Err(_) => return FileReport::skipped(rel_input, "missing-golden-file"),
    };
    let blocks = golden::parse(&text);

    // One engine per file so `CREATE … VIEW` setup persists across the file's blocks. We run
    // each block in its own task and catch panics: DataFusion still `panic!`s on some inputs
    // (e.g. multi-arg `COUNT(DISTINCT a, b)`), and a single panicking query must not abort the
    // whole corpus run — it becomes an `engine-panic` verdict instead.
    let engine = std::sync::Arc::new(Engine::new());

    // Execute `--IMPORT` setup statements (CREATE VIEW, SET, etc.) before golden replay.
    let inputs_dir = root.join("inputs");
    for setup_sql in splitter::setup_statements(&inputs_dir, rel_input) {
        let eng = engine.clone();
        let sql = setup_sql.clone();
        if let Err(join_err) = tokio::spawn(async move { eng.sql(&sql).await }).await {
            let _ = panic_message(join_err);
        }
    }

    let mut verdicts: Vec<(String, Verdict)> = Vec::with_capacity(blocks.len());
    for b in &blocks {
        let eng = engine.clone();
        let block = b.clone();
        let outcome = match tokio::spawn(async move { replay(&eng, &block).await }).await {
            Ok(o) => o,
            Err(join_err) => Outcome::Err {
                message: panic_message(join_err),
            },
        };
        let verdict = classify(b, &outcome);
        verdicts.push((b.sql.clone(), verdict));
    }
    FileReport::from_verdicts(rel_input, &verdicts)
}

/// Run one block's SQL through the engine and capture a Spark-formatted [`Outcome`].
async fn replay(engine: &Engine, b: &GoldenBlock) -> Outcome {
    match engine.sql(&b.sql).await {
        Err(e) => Outcome::Err {
            message: e.to_string(),
        },
        Ok(batches) => {
            if let Some(first) = batches.first() {
                let f = format_result(&first.schema(), &batches);
                let output = f.output();
                Outcome::Ok {
                    schema: f.schema,
                    output,
                }
            } else if is_read_only(&b.sql) {
                // Zero-row read: recover the schema without side effects.
                match engine.schema(&b.sql).await {
                    Ok(schema) => {
                        let f = format_result(&schema, &[]);
                        Outcome::Ok {
                            schema: f.schema,
                            output: String::new(),
                        }
                    }
                    Err(e) => Outcome::Err {
                        message: e.to_string(),
                    },
                }
            } else {
                // DDL / DML / SET with no result set — Spark renders this as `struct<>` + "".
                Outcome::Ok {
                    schema: "struct<>".into(),
                    output: String::new(),
                }
            }
        }
    }
}

/// Turn a panicked task's `JoinError` into a one-line `engine panicked: …` message.
fn panic_message(e: tokio::task::JoinError) -> String {
    if e.is_panic() {
        let p = e.into_panic();
        let msg = p
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| p.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "unknown".into());
        format!("engine panicked: {}", msg.lines().next().unwrap_or(""))
    } else {
        format!("engine task cancelled: {e}")
    }
}

/// Read-only statements have no side effects, so re-planning them for schema recovery is safe.
fn is_read_only(sql: &str) -> bool {
    let s = sql.trim_start().to_lowercase();
    [
        "select", "with", "values", "show", "describe", "desc", "explain", "table", "(",
    ]
    .iter()
    .any(|kw| s.starts_with(kw))
}

/// Replay the whole corpus (optionally filtering files whose relative path contains `filter`).
pub async fn run_corpus(corpus: Corpus, filter: Option<&str>) -> CorpusReport {
    // DataFusion panics on a handful of inputs; we catch each per block, but the default panic
    // hook would still print ~hundreds of backtraces. Silence it for the duration of the sweep.
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    let results_dir = corpus.root().join("results");
    let mut rels = Vec::new();
    collect_outputs(&results_dir, &results_dir, &mut rels);
    rels.sort();

    let mut files = Vec::new();
    for rel in rels {
        if let Some(f) = filter {
            if !rel.contains(f) {
                continue;
            }
        }
        files.push(run_file(corpus, &rel).await);
    }
    std::panic::set_hook(prev_hook);
    CorpusReport::build(corpus_version(corpus), files)
}

/// Recursively collect `*.sql.out` paths relative to `base`.
fn collect_outputs(base: &Path, dir: &Path, out: &mut Vec<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_outputs(base, &path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("out") {
            if let Ok(rel) = path.strip_prefix(base) {
                out.push(rel.to_string_lossy().replace('\\', "/"));
            }
        }
    }
}
