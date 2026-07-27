//! TPC-DS distributed planner coverage + optional execute correctness.
//!
//! Planner mode (`run_coverage`) runs [`plan_distributed`] over Q1–Q99 and ratchets against
//! `bench/distributed/tpcds-planner-baseline.json`. Execute mode compares distributed vs
//! single-node for the supported subset at small SF.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use weft_execution::driver::{run_stages, Cluster, ExchangeMode, StageDef};
use weft_execution::flight::serve_worker;
use weft_execution::plan::plan_distributed;
use weft_loom::Engine;

use crate::distributed_coverage::{
    check_ratchet, plan_coverage, print_report, try_plan_with_facts, write_report,
};
use crate::tpcds::{normalize_batches, queries};
use crate::tpcds_data;
use crate::tpch_dist::shard;

const BASELINE_JSON: &str = include_str!("../../../bench/distributed/tpcds-planner-baseline.json");

/// Large fact tables that may be sharded (one per query).
pub const FACT_TABLES: [&str; 7] = [
    "store_sales",
    "catalog_sales",
    "web_sales",
    "store_returns",
    "catalog_returns",
    "web_returns",
    "inventory",
];

pub struct CoverageOpts<'a> {
    pub sf: f64,
    pub data: &'a Path,
    pub baseline: &'a Path,
    pub out_json: Option<&'a Path>,
    pub skip_ratchet: bool,
}

/// Queries verified to pass distributed execute vs single-node at sf=0.01 (subset of planner-supported).
pub const CORRECTNESS_CI_QUERIES: &[&str] = &["Q3", "Q6", "Q13", "Q19", "Q96"];

pub struct ExecuteOpts<'a> {
    pub sf: f64,
    pub data: &'a Path,
    pub workers: usize,
    /// Max queries to execute (0 = all in filter / supported list).
    pub sample: usize,
    /// When set, only run these query names (must also be planner-supported).
    pub query_filter: Option<&'a [&'a str]>,
    /// Ratchet the execute-verified set against this baseline. Only meaningful for a full sweep;
    /// skipped when the run is sampled or filtered, since a subset cannot prove the set held.
    pub baseline: Option<&'a Path>,
}

/// The execute-verified set: queries whose distributed result matched single-node exactly.
///
/// This is the number that says something about the engine. The planner ratchet counts queries a
/// distributed plan could be *built* for, which is a strictly weaker claim — before this gate
/// existed, 24 of 67 "supported" queries were returning a wrong answer or failing on the worker.
#[derive(Debug, Deserialize, Serialize)]
pub struct ExecuteBaseline {
    #[serde(default)]
    pub notes: String,
    pub suite: String,
    pub verified: usize,
    pub verified_queries: Vec<String>,
}

/// Outcome of one full execute sweep.
pub struct ExecuteReport {
    pub verified: Vec<String>,
    pub mismatched: Vec<String>,
    pub errored: Vec<String>,
}

/// Register all 24 TPC-DS Parquet tables on `engine`.
async fn register_tpcds(engine: &Engine, dir: &Path) {
    for t in tpcds_data::TABLES {
        let path = dir.join(format!("{t}.parquet"));
        engine
            .register_parquet(t, path.to_str().expect("utf8 path"))
            .await
            .unwrap_or_else(|e| panic!("register {t}: {e}"));
    }
}

/// Planner-only coverage over Q1–Q99 with ratchet.
pub async fn run_coverage(opts: CoverageOpts<'_>) {
    eprintln!(
        "[tpcds-dist] planner coverage sf{} data={} …",
        opts.sf,
        opts.data.display()
    );
    if let Err(e) = tpcds_data::generate(opts.sf, opts.data) {
        eprintln!("[tpcds-dist] data generation failed: {e}");
        std::process::exit(1);
    }

    let engine = Engine::new();
    register_tpcds(&engine, opts.data).await;

    let qs = queries();
    let all = tpcds_data::TABLES.to_vec();
    let facts = FACT_TABLES.to_vec();

    let only = std::env::var("WEFT_TPCDS_ONLY").ok();
    let report = if let Some(ref only) = only {
        let filtered: Vec<_> = qs
            .into_iter()
            .filter(|(n, _)| n.eq_ignore_ascii_case(only))
            .collect();
        plan_coverage("tpcds", &engine, &filtered, &all, &facts).await
    } else {
        plan_coverage("tpcds", &engine, &qs, &all, &facts).await
    };

    for q in &report.per_query {
        if q.supported {
            eprintln!(
                "{:<4} PLAN ok   sharded={}",
                q.name,
                q.sharded_fact.as_deref().unwrap_or("?")
            );
        } else {
            eprintln!(
                "{:<4} PLAN skip {}",
                q.name,
                q.reason.as_deref().unwrap_or("?")
            );
        }
    }

    print_report(&report);
    if let Some(out) = opts.out_json {
        write_report(out, &report);
    }

    if !opts.skip_ratchet && only.is_none() && !check_ratchet(&report, opts.baseline) {
        std::process::exit(1);
    }
}

/// Execute supported queries on in-process workers; assert row match vs single-node.
pub async fn run_execute(opts: ExecuteOpts<'_>) {
    eprintln!(
        "[tpcds-dist] execute sf{} data={} workers={} …",
        opts.sf,
        opts.data.display(),
        opts.workers
    );
    if let Err(e) = tpcds_data::generate(opts.sf, opts.data) {
        eprintln!("[tpcds-dist] data generation failed: {e}");
        std::process::exit(1);
    }

    let single = Engine::new();
    register_tpcds(&single, opts.data).await;

    let qs = queries();
    let all = tpcds_data::TABLES.to_vec();
    let facts = FACT_TABLES.to_vec();

    // Plan pass: collect supported queries + sharded fact.
    let mut supported: Vec<(String, String, String)> = Vec::new(); // name, sql, fact
    for (name, raw) in &qs {
        let sql = raw.trim().trim_end_matches(';').trim();
        if let Ok((_dq, fact)) = try_plan_with_facts(&single, sql, &all, &facts).await {
            supported.push((name.to_string(), sql.to_string(), fact));
        }
    }

    if let Some(filter) = opts.query_filter {
        supported.retain(|(n, _, _)| filter.iter().any(|q| q.eq_ignore_ascii_case(n)));
    }

    let sample = if opts.sample == 0 {
        supported.len()
    } else {
        opts.sample.min(supported.len())
    };
    eprintln!(
        "[tpcds-dist] {}/{} distributable; executing sample of {sample}\n",
        supported.len(),
        qs.len()
    );

    // Load full table data once.
    let mut full: Vec<(&str, Vec<weft_loom::arrow::record_batch::RecordBatch>)> = Vec::new();
    for t in tpcds_data::TABLES {
        let b = single.sql(&format!("SELECT * FROM {t}")).await.unwrap();
        full.push((t, b));
    }

    let clusters = build_clusters(&full, opts.workers).await;

    let only = std::env::var("WEFT_TPCDS_ONLY").ok();
    let mut to_run: Vec<&(String, String, String)> = supported.iter().collect();
    if let Some(ref o) = only {
        to_run.retain(|(n, _, _)| n.eq_ignore_ascii_case(o));
    }
    let run_count = if opts.sample == 0 {
        to_run.len()
    } else {
        opts.sample.min(to_run.len())
    };

    let mut report = ExecuteReport {
        verified: Vec::new(),
        mismatched: Vec::new(),
        errored: Vec::new(),
    };
    let debug = std::env::var("WEFT_TPCDS_DEBUG").is_ok();

    for (i, (name, sql, fact)) in to_run.iter().take(run_count).enumerate() {
        let replicated: Vec<&str> = all
            .iter()
            .copied()
            .filter(|t| *t != fact.as_str())
            .collect();
        let dq = match plan_distributed(&single, sql, &replicated).await {
            Ok(d) => d,
            Err(e) => {
                report.errored.push(name.to_string());
                eprintln!("{name:<4} replan ERROR: {e}");
                continue;
            }
        };

        let forward = dq
            .stages
            .iter()
            .any(|s| s.exchange == ExchangeMode::Forward);
        let cluster = if forward {
            &clusters.full
        } else {
            clusters
                .by_fact
                .get(fact)
                .unwrap_or_else(|| panic!("no cluster for sharded fact {fact}"))
        };
        let mode = if forward { "forward" } else { "shuffle" };

        if std::env::var("WEFT_TPCDS_DEBUG").as_deref() == Ok("plan") {
            let lp = single.logical_plan(sql).await.unwrap();
            eprintln!("  {name} logical plan:\n{}", lp.display_indent_schema());
        }
        if debug {
            for s in &dq.stages {
                eprintln!(
                    "  {name} [{mode}] stage{} keys{:?} exch={:?}: {}",
                    s.stage_id, s.hash_key_cols, s.exchange, s.sql
                );
            }
            if let Some(f) = &dq.finalize_sql {
                eprintln!("  {name} finalize: {f}");
            }
        }

        let base = (i as u32 + 1) * 1000;
        let stages: Vec<StageDef> = dq
            .stages
            .iter()
            .map(|s| StageDef {
                stage_id: s.stage_id + base,
                upstream_stage_ids: s.upstream_stage_ids.iter().map(|u| u + base).collect(),
                sql: s.sql.clone(),
                hash_key_cols: s.hash_key_cols.clone(),
                exchange: s.exchange,
                plan_fragment: s.plan_fragment.clone(),
                lakehouse_snapshot_pins: s.lakehouse_snapshot_pins.clone(),
            })
            .collect();

        let gathered = match run_stages_with_retry(cluster, &stages).await {
            Ok(b) => b,
            Err(e) => {
                report.errored.push(name.to_string());
                eprintln!("{name:<4} distributed ERROR: {e}");
                continue;
            }
        };

        let result = match &dq.finalize_sql {
            None => gathered,
            Some(_f) if gathered.is_empty() => {
                // Zero-row partial output: skip finalize registration (register_batches needs schema).
                Vec::new()
            }
            Some(f) => {
                let fin = Engine::new();
                fin.register_batches("result", gathered)
                    .unwrap_or_else(|e| panic!("{name} finalize register: {e}"));
                fin.sql(f)
                    .await
                    .unwrap_or_else(|e| panic!("{name} finalize sql: {e}"))
            }
        };

        let expected = single.sql(sql).await.unwrap();
        if normalize_batches(&result) == normalize_batches(&expected) {
            report.verified.push(name.to_string());
            eprintln!("{name:<4} distributed ok [{mode}] ({})", dq.stages.len());
        } else {
            report.mismatched.push(name.to_string());
            eprintln!("{name:<4} distributed MISMATCH [{mode}]");
            if debug {
                let exp = normalize_batches(&expected);
                let got = normalize_batches(&result);
                eprintln!(
                    "  expected {} rows / got {} rows\n  first expected: {:?}\n  first got:      {:?}",
                    exp.len(),
                    got.len(),
                    exp.first(),
                    got.first()
                );
            }
        }
    }

    eprintln!(
        "\n=== TPC-DS distributed execute sf{}: {} execute-verified, {} mismatch, {} error \
         (ran {run_count}) ===",
        opts.sf,
        report.verified.len(),
        report.mismatched.len(),
        report.errored.len()
    );
    eprintln!(
        "verified_json={}",
        serde_json::to_string(&report.verified).unwrap_or_default()
    );

    // A sampled or filtered run only ever sees a subset, so it can confirm the queries it ran but
    // cannot prove the baseline set still holds. Ratchet the full sweep only.
    let full_sweep = opts.sample == 0 && opts.query_filter.is_none() && only.is_none();
    let baseline = opts.baseline.filter(|_| full_sweep);
    if !check_execute_ratchet(&report, baseline) {
        std::process::exit(1);
    }
}

/// Enforce the execute gate: no mismatches, no errors, and no query dropping out of the
/// execute-verified baseline.
///
/// Mismatches are never tolerated and are not ratcheted to a count — a wrong answer is a wrong
/// answer regardless of how many of them there are.
fn check_execute_ratchet(report: &ExecuteReport, baseline: Option<&Path>) -> bool {
    let mut ok = true;
    if !report.mismatched.is_empty() {
        eprintln!(
            "[tpcds-execute] WRONG ANSWERS: {} — a distributed plan must never return a result \
             that differs from single-node; make the planner decline the shape instead",
            report.mismatched.join(", ")
        );
        ok = false;
    }
    if !report.errored.is_empty() {
        eprintln!(
            "[tpcds-execute] EXECUTION ERRORS: {} — the planner accepted a shape it cannot run",
            report.errored.join(", ")
        );
        ok = false;
    }

    let Some(path) = baseline else {
        return ok;
    };
    let base: ExecuteBaseline = serde_json::from_str(
        &std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("read execute baseline {}: {e}", path.display())),
    )
    .unwrap_or_else(|e| panic!("parse execute baseline {}: {e}", path.display()));

    let lost: Vec<&String> = base
        .verified_queries
        .iter()
        .filter(|q| !report.verified.contains(q))
        .collect();
    if lost.is_empty() {
        let gained: Vec<&String> = report
            .verified
            .iter()
            .filter(|q| !base.verified_queries.contains(q))
            .collect();
        if gained.is_empty() {
            eprintln!(
                "[tpcds-execute] ratchet OK: {} execute-verified held (baseline {})",
                report.verified.len(),
                base.verified
            );
        } else {
            eprintln!(
                "[tpcds-execute] ratchet gain: +{} execute-verified — re-baseline {}: {}",
                gained.len(),
                path.display(),
                gained
                    .iter()
                    .map(|q| q.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    } else {
        eprintln!(
            "[tpcds-execute] RATCHET REGRESSION: no longer execute-verified: {}",
            lost.iter()
                .map(|q| q.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        ok = false;
    }
    ok
}

struct ClusterSet {
    by_fact: HashMap<String, Cluster>,
    full: Cluster,
}

async fn build_clusters(
    full: &[(&str, Vec<weft_loom::arrow::record_batch::RecordBatch>)],
    num_workers: usize,
) -> ClusterSet {
    let mut by_fact = HashMap::new();
    for (fi, fact) in FACT_TABLES.iter().enumerate() {
        let fact_batches = full.iter().find(|(t, _)| *t == *fact).unwrap().1.clone();
        let shards = shard(&fact_batches, num_workers);
        let base_port = 50800u16 + (fi as u16) * 10;
        let mut endpoints = Vec::new();
        for (i, shard_batches) in shards.into_iter().enumerate() {
            let e = Arc::new(Engine::new());
            for (t, batches) in full {
                let data = if *t == *fact {
                    shard_batches.clone()
                } else {
                    batches.clone()
                };
                e.register_batches(t, data).unwrap();
            }
            let port = base_port + i as u16;
            let ee = e.clone();
            tokio::spawn(async move {
                let _ = serve_worker(port, ee).await;
            });
            endpoints.push(format!("http://127.0.0.1:{port}"));
        }
        by_fact.insert(fact.to_string(), Cluster::new(endpoints));
    }

    let mut full_endpoints = Vec::new();
    for i in 0..num_workers {
        let e = Arc::new(Engine::new());
        for (t, batches) in full {
            e.register_batches(t, batches.clone()).unwrap();
        }
        let port = 50900 + i as u16;
        let ee = e.clone();
        tokio::spawn(async move {
            let _ = serve_worker(port, ee).await;
        });
        full_endpoints.push(format!("http://127.0.0.1:{port}"));
    }
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    ClusterSet {
        by_fact,
        full: Cluster::new(full_endpoints),
    }
}

async fn run_stages_with_retry(
    cluster: &Cluster,
    stages: &[StageDef],
) -> Result<Vec<weft_loom::arrow::record_batch::RecordBatch>, String> {
    let mut last_err = None;
    for _ in 0..30 {
        match run_stages(cluster, stages).await {
            Ok(b) => return Ok(b),
            Err(e) => {
                let transient = e.to_string().contains("connect");
                last_err = Some(e.to_string());
                if !transient {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| "run_stages failed".into()))
}

/// Run a CI-sized correctness sample (curated queries that pass distributed execute).
pub async fn run_correctness_sample(sf: f64, data: &Path, workers: usize, sample: usize) {
    run_execute(ExecuteOpts {
        sf,
        data,
        workers,
        sample,
        query_filter: Some(CORRECTNESS_CI_QUERIES),
        baseline: None,
    })
    .await;
}

pub fn default_baseline_path() -> PathBuf {
    PathBuf::from("bench/distributed/tpcds-planner-baseline.json")
}

pub fn default_execute_baseline_path() -> PathBuf {
    PathBuf::from("bench/distributed/tpcds-execute-baseline.json")
}

/// Load embedded baseline for tests / dry runs before the file exists on disk.
#[allow(dead_code)]
pub fn embedded_baseline_supported() -> usize {
    let v: serde_json::Value = serde_json::from_str(BASELINE_JSON).expect("embedded baseline");
    v.get("supported").and_then(|s| s.as_u64()).unwrap_or(0) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(verified: &[&str], mismatched: &[&str], errored: &[&str]) -> ExecuteReport {
        let own = |xs: &[&str]| xs.iter().map(|s| s.to_string()).collect();
        ExecuteReport {
            verified: own(verified),
            mismatched: own(mismatched),
            errored: own(errored),
        }
    }

    /// Write a throwaway baseline under a caller-supplied name, so tests running in parallel
    /// never write and read the same path.
    fn baseline_file(label: &str, queries: &[&str]) -> PathBuf {
        let base = ExecuteBaseline {
            notes: String::new(),
            suite: "tpcds".into(),
            verified: queries.len(),
            verified_queries: queries.iter().map(|s| s.to_string()).collect(),
        };
        let path = std::env::temp_dir().join(format!("weft-tpcds-execute-baseline-{label}.json"));
        std::fs::write(&path, serde_json::to_string(&base).unwrap()).unwrap();
        path
    }

    #[test]
    fn clean_sweep_holding_the_baseline_passes() {
        let f = baseline_file("held", &["Q3", "Q6"]);
        assert!(check_execute_ratchet(
            &report(&["Q3", "Q6"], &[], &[]),
            Some(&f)
        ));
    }

    #[test]
    fn a_mismatch_fails_even_when_the_baseline_set_is_intact() {
        // The whole point of the gate: a wrong answer is never ratcheted or tolerated.
        let f = baseline_file("mismatch", &["Q3"]);
        assert!(!check_execute_ratchet(
            &report(&["Q3"], &["Q66"], &[]),
            Some(&f)
        ));
    }

    #[test]
    fn a_mismatch_fails_with_no_baseline_at_all() {
        assert!(!check_execute_ratchet(
            &report(&["Q3"], &["Q66"], &[]),
            None
        ));
    }

    #[test]
    fn an_execution_error_fails() {
        assert!(!check_execute_ratchet(&report(&["Q3"], &[], &["Q5"]), None));
    }

    #[test]
    fn losing_a_baseline_query_fails() {
        let f = baseline_file("lost", &["Q3", "Q6"]);
        assert!(!check_execute_ratchet(&report(&["Q3"], &[], &[]), Some(&f)));
    }

    #[test]
    fn gaining_a_query_passes_and_asks_for_a_re_baseline() {
        let f = baseline_file("gained", &["Q3"]);
        assert!(check_execute_ratchet(
            &report(&["Q3", "Q6"], &[], &[]),
            Some(&f)
        ));
    }

    #[test]
    fn committed_execute_baseline_is_a_subset_of_the_planner_baseline() {
        // A query can only be execute-verified if the planner can distribute it, so the execute
        // set drifting outside the planner set means one of the two files is stale.
        let exec: ExecuteBaseline = serde_json::from_str(
            &std::fs::read_to_string("../../bench/distributed/tpcds-execute-baseline.json")
                .expect("execute baseline"),
        )
        .expect("parse execute baseline");
        let planner: serde_json::Value =
            serde_json::from_str(BASELINE_JSON).expect("planner baseline");
        let supported: Vec<String> = planner["supported_queries"]
            .as_array()
            .expect("supported_queries")
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert_eq!(exec.verified, exec.verified_queries.len());
        let outside: Vec<&String> = exec
            .verified_queries
            .iter()
            .filter(|q| !supported.contains(q))
            .collect();
        assert!(outside.is_empty(), "not planner-supported: {outside:?}");
    }
}
