//! TPC-DS distributed planner coverage + optional execute correctness.
//!
//! Planner mode (`run_coverage`) runs [`plan_distributed`] over Q1–Q99 and ratchets against
//! `bench/distributed/tpcds-planner-baseline.json`. Execute mode compares distributed vs
//! single-node for the supported subset at small SF.

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

    let (mut ok, mut mismatch, mut error) = (0usize, 0usize, 0usize);

    for (i, (name, sql, fact)) in to_run.iter().take(run_count).enumerate() {
        let replicated: Vec<&str> = all
            .iter()
            .copied()
            .filter(|t| *t != fact.as_str())
            .collect();
        let dq = match plan_distributed(&single, sql, &replicated).await {
            Ok(d) => d,
            Err(e) => {
                error += 1;
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
                error += 1;
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
            ok += 1;
            eprintln!("{name:<4} distributed ok [{mode}] ({})", dq.stages.len());
        } else {
            mismatch += 1;
            eprintln!("{name:<4} distributed MISMATCH [{mode}]");
        }
    }

    eprintln!(
        "\n=== TPC-DS distributed execute sf{}: {ok} ok, {mismatch} mismatch, {error} error \
         (ran {run_count}) ===",
        opts.sf
    );
    if mismatch > 0 || error > 0 {
        std::process::exit(1);
    }
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
    })
    .await;
}

pub fn default_baseline_path() -> PathBuf {
    PathBuf::from("bench/distributed/tpcds-planner-baseline.json")
}

/// Load embedded baseline for tests / dry runs before the file exists on disk.
#[allow(dead_code)]
pub fn embedded_baseline_supported() -> usize {
    let v: serde_json::Value = serde_json::from_str(BASELINE_JSON).expect("embedded baseline");
    v.get("supported").and_then(|s| s.as_u64()).unwrap_or(0) as usize
}
