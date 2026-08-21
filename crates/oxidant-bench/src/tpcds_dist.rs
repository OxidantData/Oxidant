//! TPC-DS distributed planner coverage + optional execute correctness.
//!
//! Planner mode (`run_coverage`) runs [`plan_distributed`] over Q1–Q99 and ratchets against
//! `bench/distributed/tpcds-planner-baseline.json`. Execute mode compares distributed vs
//! single-node for the supported subset at small SF.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use oxidant_execution::driver::{run_stages, Cluster, ExchangeMode, StageDef};
use oxidant_execution::flight::serve_worker;
use oxidant_execution::plan::plan_distributed;
use oxidant_loom::Engine;

use crate::distributed_coverage::{
    check_ratchet, plan_coverage, print_report, try_plan_with_facts, write_report,
};
use crate::tpcds::{batches_equal_ordered, normalize_batches, queries};
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
    /// The subset of `mismatched` whose row *multiset* matched single-node exactly — only the
    /// sequence differed, which an `ORDER BY` with ties across the `LIMIT` boundary permits.
    pub order_only: Vec<String>,
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

    let only = std::env::var("OXIDANT_TPCDS_ONLY").ok();
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

    // Apply OXIDANT_TPCDS_ONLY / query_filter *before* the plan pass and cluster build so a
    // single-query iteration doesn't pay for 99 plans + 16 in-process engines.
    let only = std::env::var("OXIDANT_TPCDS_ONLY").ok();
    let qs_filtered: Vec<(&str, &str)> = qs
        .iter()
        .copied()
        .filter(|(name, _)| {
            if let Some(ref o) = only {
                if !name.eq_ignore_ascii_case(o) {
                    return false;
                }
            }
            if let Some(filter) = opts.query_filter {
                if !filter.iter().any(|q| q.eq_ignore_ascii_case(name)) {
                    return false;
                }
            }
            true
        })
        .collect();

    // Ad-hoc SQL against the fully-registered single-node engine. Lets a generated stage SQL (or a
    // hand-reduced variant of one) be checked against an external oracle without a distributed run.
    if let Ok(path) = std::env::var("OXIDANT_TPCDS_SQL_FILE") {
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
        for (i, stmt) in text
            .split(";\n")
            .filter(|s| !s.trim().is_empty())
            .enumerate()
        {
            match single.sql(stmt.trim()).await {
                Ok(b) => {
                    let rows = normalize_batches(&b);
                    let last: Vec<i128> = rows
                        .iter()
                        .filter_map(|r| r.last())
                        .filter_map(|v| v.parse::<i128>().ok())
                        .collect();
                    eprintln!(
                        "[sql-file {i}] n={} last_col_max={:?} last_col_sum={}",
                        rows.len(),
                        last.iter().max(),
                        last.iter().sum::<i128>()
                    );
                    if rows.len() <= 4 {
                        for r in &rows {
                            eprintln!("[sql-file {i}] {}", r.join(" | "));
                        }
                    }
                }
                Err(e) => eprintln!("[sql-file {i}] FAILED: {e}"),
            }
        }
        return;
    }

    // Plan pass: collect supported queries + sharded fact.
    let mut supported: Vec<(String, String, String)> = Vec::new(); // name, sql, fact
    for (name, raw) in &qs_filtered {
        let sql = raw.trim().trim_end_matches(';').trim();
        if let Ok((_dq, fact)) = try_plan_with_facts(&single, sql, &all, &facts).await {
            supported.push((name.to_string(), sql.to_string(), fact));
        }
    }

    let sample = if opts.sample == 0 {
        supported.len()
    } else {
        opts.sample.min(supported.len())
    };
    eprintln!(
        "[tpcds-dist] {}/{} distributable (of {} considered); executing sample of {sample}\n",
        supported.len(),
        qs.len(),
        qs_filtered.len()
    );

    let to_run: Vec<&(String, String, String)> = supported.iter().take(sample).collect();
    let run_count = to_run.len();

    // Build only the fact clusters this run will touch, plus the unreplicated `full`
    // cluster (Forward plans and cheap to add — 2 workers vs 14 for all facts).
    let needed_facts: Vec<&str> = {
        let mut seen = std::collections::BTreeSet::new();
        for (_, _, fact) in &to_run {
            seen.insert(fact.as_str());
        }
        seen.into_iter().collect()
    };

    // Load the sharded facts once — only needed to write per-worker parquet shards.
    // Replicated tables are registered on workers straight from the generated
    // parquet fixtures (real ListingTables with statistics), so their batches are
    // never materialized here (KAN-51).
    let mut fact_data: Vec<(&str, Vec<oxidant_loom::arrow::record_batch::RecordBatch>)> =
        Vec::new();
    if run_count > 0 {
        for fact in &needed_facts {
            let b = single.sql(&format!("SELECT * FROM {fact}")).await.unwrap();
            fact_data.push((fact, b));
        }
    }

    let clusters = if run_count == 0 {
        ClusterSet {
            by_fact: HashMap::new(),
            full: Cluster::new(Vec::new()),
        }
    } else {
        build_clusters(opts.data, &fact_data, opts.workers, &needed_facts, true).await
    };

    let mut report = ExecuteReport {
        verified: Vec::new(),
        mismatched: Vec::new(),
        order_only: Vec::new(),
        errored: Vec::new(),
    };
    let debug = std::env::var("OXIDANT_TPCDS_DEBUG").is_ok();

    for (i, (name, sql, fact)) in to_run.iter().enumerate() {
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

        // A plan is "whole-query forward" only if its *output* stage (the one nothing else
        // consumes — see `run_stages_obs`) runs with `Forward` exchange, i.e. the entire query
        // executes on a single worker against a fully-replicated dataset (no fact table needs
        // sharding). A plan that mixes a `Forward` producer (a replicated-only UNION arm — see
        // `stage_planner::try_split_broadcast_union`) with an ordinary hash-shuffled combine
        // output still needs `fact` genuinely sharded across workers, so it must use the
        // `by_fact` cluster like any other shuffle plan — using the fully-replicated `full`
        // cluster here would run the sharded arm's aggregation on the *whole* table on every
        // worker and multiply its contribution by the worker count.
        let consumed: std::collections::HashSet<u32> = dq
            .stages
            .iter()
            .flat_map(|s| s.upstream_stage_ids.iter().copied())
            .collect();
        let forward = dq
            .stages
            .iter()
            .filter(|s| !consumed.contains(&s.stage_id))
            .all(|s| s.exchange == ExchangeMode::Forward);
        let cluster = if forward {
            &clusters.full
        } else {
            clusters
                .by_fact
                .get(fact)
                .unwrap_or_else(|| panic!("no cluster for sharded fact {fact}"))
        };
        let mode = if forward { "forward" } else { "shuffle" };

        if std::env::var("OXIDANT_TPCDS_DEBUG").as_deref() == Ok("plan") {
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
                replicated_tables: s.replicated_tables.clone(),
                lakeformation_required: false,
                lakeformation_principal: String::new(),
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
        if batches_equal_ordered(&result, &expected) {
            report.verified.push(name.to_string());
            eprintln!("{name:<4} distributed ok [{mode}] ({})", dq.stages.len());
        } else {
            report.mismatched.push(name.to_string());
            // Distinguish "returned different data" from "returned the same rows in a different
            // order". A query whose ORDER BY keys tie across the LIMIT boundary has no single
            // correct row order, so distributed and single-node can disagree on sequence while
            // both are right — TPC-DS Q18 and Q65 do exactly this at sf10. Both still fail the
            // gate (an order-only difference can also be a real ordering bug), but labelling them
            // apart keeps a genuine wrong-answer from being dismissed as "just the tie thing".
            let exp_rows = normalize_batches(&expected);
            let got_rows = normalize_batches(&result);
            let (mut se, mut sg) = (exp_rows.clone(), got_rows.clone());
            se.sort();
            sg.sort();
            if se == sg {
                report.order_only.push(name.to_string());
                eprintln!(
                    "{name:<4} distributed MISMATCH [{mode}] — ORDER ONLY: identical {} row \
                     multiset, different sequence (ORDER BY ties)",
                    se.len()
                );
            } else {
                eprintln!("{name:<4} distributed MISMATCH [{mode}] — DIFFERENT ROWS");
            }
            if debug {
                let exp = exp_rows;
                let got = got_rows;
                eprintln!(
                    "  expected {} rows / got {} rows\n  first expected: {:?}\n  first got:      {:?}",
                    exp.len(),
                    got.len(),
                    exp.first(),
                    got.first()
                );
                if std::env::var("OXIDANT_TPCDS_DEBUG_FULL").is_ok() {
                    for (i, (e, g)) in exp.iter().zip(got.iter()).enumerate() {
                        let mark = if e == g { "==" } else { "!=" };
                        eprintln!("    [{i}] {mark} exp={e:?}\n           got={g:?}");
                    }
                }
                // Re-run each leaf stage's generated SQL on the single-node engine (which has
                // every table registered unsharded). A leaf whose result differs from what the
                // same SQL means elsewhere localizes the defect to planning that generated text,
                // rather than to sharding, placement, or the exchange.
                if std::env::var("OXIDANT_TPCDS_RUN_LEAF_SQL").is_ok() {
                    for st in dq.stages.iter().filter(|s| s.upstream_stage_ids.is_empty()) {
                        match single.sql(&st.sql).await {
                            Ok(b) => {
                                let rows = normalize_batches(&b);
                                // Fingerprint the last column (the count partial) so the result
                                // can be matched against an independent oracle's numbers.
                                let last: Vec<i128> = rows
                                    .iter()
                                    .filter_map(|r| r.last())
                                    .filter_map(|v| v.parse::<i128>().ok())
                                    .collect();
                                eprintln!(
                                    "  [leaf-sql] stage {} -> {} rows; last_col n={} max={:?} sum={}",
                                    st.stage_id,
                                    rows.len(),
                                    last.len(),
                                    last.iter().max(),
                                    last.iter().sum::<i128>()
                                );
                            }
                            Err(e) => {
                                eprintln!("  [leaf-sql] stage {} FAILED: {e}", st.stage_id)
                            }
                        }
                    }
                }
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
    if !report.order_only.is_empty() {
        eprintln!(
            "  of those, {} were ORDER-ONLY (identical row multiset, ORDER BY ties): {}",
            report.order_only.len(),
            report.order_only.join(", ")
        );
    }
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
    let different_rows: Vec<&String> = report
        .mismatched
        .iter()
        .filter(|q| !report.order_only.contains(q))
        .collect();
    if !different_rows.is_empty() {
        eprintln!(
            "[tpcds-execute] WRONG ANSWERS: {} — a distributed plan must never return a result \
             that differs from single-node; make the planner decline the shape instead",
            different_rows
                .iter()
                .map(|q| q.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        ok = false;
    }
    if !report.order_only.is_empty() {
        // Still a gate failure — an order-only difference can be a genuine ordering bug — but it
        // is a materially different finding from returning different data, so say which it is.
        eprintln!(
            "[tpcds-execute] ORDER-ONLY MISMATCH: {} — row multiset matches single-node exactly, \
             only the sequence differs (ORDER BY keys tie across the LIMIT). Not a wrong answer, \
             but the distributed plan must still reproduce single-node's order",
            report.order_only.join(", ")
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
    dir: &Path,
    fact_data: &[(&str, Vec<oxidant_loom::arrow::record_batch::RecordBatch>)],
    num_workers: usize,
    needed_facts: &[&str],
    need_full: bool,
) -> ClusterSet {
    let mut by_fact = HashMap::new();
    for (fi, fact) in FACT_TABLES.iter().enumerate() {
        if !needed_facts.contains(fact) {
            continue;
        }
        let fact_batches = &fact_data.iter().find(|(t, _)| *t == *fact).unwrap().1;
        let shards = shard(fact_batches, num_workers);
        let shard_paths = write_fact_shards(dir, fact, &shards);
        let base_port = 50800u16 + (fi as u16) * 10;
        let mut endpoints = Vec::new();
        for (i, shard_path) in shard_paths.into_iter().enumerate() {
            let e = Arc::new(Engine::new());
            register_worker_tables(&e, dir, Some((fact, shard_path.as_path()))).await;
            let port = base_port + i as u16;
            let ee = e.clone();
            tokio::spawn(async move {
                let _ = serve_worker(port, ee).await;
            });
            endpoints.push(format!("http://127.0.0.1:{port}"));
        }
        by_fact.insert(fact.to_string(), Cluster::new(endpoints));
    }

    let full_cluster = if need_full {
        let mut full_endpoints = Vec::new();
        for i in 0..num_workers {
            let e = Arc::new(Engine::new());
            register_worker_tables(&e, dir, None).await;
            let port = 50900 + i as u16;
            let ee = e.clone();
            tokio::spawn(async move {
                let _ = serve_worker(port, ee).await;
            });
            full_endpoints.push(format!("http://127.0.0.1:{port}"));
        }
        Cluster::new(full_endpoints)
    } else {
        Cluster::new(Vec::new())
    };
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    ClusterSet {
        by_fact,
        full: full_cluster,
    }
}

/// Register every TPC-DS table on a worker engine as a parquet **ListingTable** —
/// the same provider shape live workers get when `catalog_bridge` resolves the
/// Glue table (KAN-51): real file statistics, dynamic-filter pushdown, and
/// partitioned joins, instead of the old stat-less in-memory `MemTable`s that
/// made workers plan single-partition `CollectLeft` joins (the false local Q72
/// timeout during KAN-41). `fact_shard` maps the sharded fact to this worker's
/// parquet shard; every other (replicated) table resolves to the generated
/// `<dir>/<table>.parquet` fixture.
async fn register_worker_tables(engine: &Engine, dir: &Path, fact_shard: Option<(&str, &Path)>) {
    for t in tpcds_data::TABLES {
        let path = match fact_shard {
            Some((fact, shard)) if t == fact => shard.to_path_buf(),
            _ => dir.join(format!("{t}.parquet")),
        };
        engine
            .register_parquet(t, path.to_str().expect("utf8 path"))
            .await
            .unwrap_or_else(|e| panic!("worker register {t}: {e}"));
    }
}

/// Write each worker's fact shard as its own parquet file under
/// `<dir>/dist-shards/<fact>/worker-<i>.parquet` and return the per-worker paths.
/// A zero-row shard still gets a schema-only file so every worker resolves the
/// fact — the old `register_batches` path panicked on empty batches.
fn write_fact_shards(
    dir: &Path,
    fact: &str,
    shards: &[Vec<oxidant_loom::arrow::record_batch::RecordBatch>],
) -> Vec<PathBuf> {
    let shard_dir = dir.join("dist-shards").join(fact);
    if shard_dir.exists() {
        std::fs::remove_dir_all(&shard_dir).expect("clear stale fact shards");
    }
    std::fs::create_dir_all(&shard_dir).expect("create shard dir");
    let schema = shards
        .iter()
        .find_map(|s| s.first())
        .map(|b| b.schema())
        .unwrap_or_else(|| panic!("fact table {fact} produced no batches"));
    shards
        .iter()
        .enumerate()
        .map(|(i, batches)| {
            let path = shard_dir.join(format!("worker-{i}.parquet"));
            write_parquet(&path, schema.clone(), batches);
            path
        })
        .collect()
}

/// Write record batches as a single parquet file (zero batches → schema-only file).
fn write_parquet(
    path: &Path,
    schema: oxidant_loom::arrow::datatypes::SchemaRef,
    batches: &[oxidant_loom::arrow::record_batch::RecordBatch],
) {
    use datafusion::parquet::arrow::ArrowWriter;

    let file =
        std::fs::File::create(path).unwrap_or_else(|e| panic!("create {}: {e}", path.display()));
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("parquet writer");
    for b in batches {
        writer.write(b).expect("write shard batch");
    }
    writer.close().expect("close shard parquet");
}

async fn run_stages_with_retry(
    cluster: &Cluster,
    stages: &[StageDef],
) -> Result<Vec<oxidant_loom::arrow::record_batch::RecordBatch>, String> {
    let mut last_err = None;
    // 30s ceiling: on the 4-vCPU CI runner a worker can be unreachable for several seconds
    // under load (FD pressure, accept-queue backlog) and then recover — the old 30×100ms=3s
    // window gave up mid-hiccup and failed the whole execute gate (Q30/Q81/Q91/Q80 flakes).
    // A genuinely dead worker still fails inside 30s, and only connection-layer errors retry;
    // plan/SQL errors never match the markers.
    for _ in 0..60 {
        match run_stages(cluster, stages).await {
            Ok(b) => return Ok(b),
            Err(e) => {
                let msg = e.to_string();
                let transient = msg.contains("connect") || msg.contains("transport");
                last_err = Some(msg);
                if !transient {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
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

    /// KAN-2 throughput residual audit: run every TPC-DS query through the production
    /// pre-split path (`logical_plan` → `Engine::optimize_logical_plan` → shape split).
    /// The pre-split optimizer must never cost a query its distributed plan — a query
    /// whose *unoptimized* plan splits must also split after optimization (the driver
    /// falls back to the original plan on `Unsupported`, but for TPC-DS the rewritten
    /// shapes are expected to stay inside the splitter's vocabulary) — and the rewrite
    /// must not multiply the stage DAG past the driver's explosion guard (the v12 Q4
    /// 66-stage do_get failure: ~15 stages became 66 tiny ones).
    #[tokio::test]
    async fn pre_split_optimizer_keeps_tpcds_distributed_coverage() {
        use datafusion::logical_expr::LogicalPlan;
        use oxidant_execution::plan::plan_distributed_logical;

        let dir = std::env::temp_dir().join("oxidant-tpcds-sf1");
        // Data generation shells out to official dsdgen (integer SCALE ≥ 1). The CI
        // `clippy + test` job runs the workspace suite without the kits (query-gates
        // installs them and exercises the same path via `tpcds-distributed`). Skip —
        // rather than fail — only when generation is impossible; any real generation
        // error still panics.
        if let Err(e) = tpcds_data::generate(1.0, &dir) {
            if e.kind() == std::io::ErrorKind::NotFound {
                eprintln!("[pre-split-audit] skipping: {e}");
                return;
            }
            panic!("data generation failed: {e}");
        }
        let engine = Engine::new();
        register_tpcds(&engine, &dir).await;

        // Mirror of `try_plan_with_facts` over an already-built plan: first sharded-fact
        // candidate that splits wins.
        let split = |lp: &LogicalPlan| -> Option<usize> {
            for fact in FACT_TABLES {
                let replicated: Vec<&str> = tpcds_data::TABLES
                    .iter()
                    .copied()
                    .filter(|t| *t != fact)
                    .collect();
                if let Ok(dq) = plan_distributed_logical(lp, &replicated) {
                    return Some(dq.stages.len());
                }
            }
            None
        };

        let mut lost_distribution = Vec::new();
        for (name, sql) in queries() {
            let lp = engine.logical_plan(sql).await.unwrap();
            let before = split(&lp);
            let optimized = engine.optimize_logical_plan(lp.clone()).unwrap();
            if format!("{}", optimized.display_indent()) == format!("{}", lp.display_indent()) {
                continue; // Skip class or no-op rewrite: identical plan, identical split.
            }
            let after = split(&optimized);
            match (before, after) {
                (Some(n_before), Some(n_after)) => {
                    eprintln!(
                        "[pre-split-audit] {name}: rewritten, stages {n_before} -> {n_after}"
                    );
                    assert!(
                        n_after <= 40 || n_after <= n_before,
                        "{name}: rewrite exploded the stage DAG ({n_before} -> {n_after}) — \
                         the v12 Q4 failure signature"
                    );
                }
                (Some(_), None) => lost_distribution.push(name),
                (None, _) => {}
            }
        }
        assert!(
            lost_distribution.is_empty(),
            "queries whose distributed plan the pre-split rewrite breaks: {lost_distribution:?}"
        );
    }

    fn report(verified: &[&str], mismatched: &[&str], errored: &[&str]) -> ExecuteReport {
        let own = |xs: &[&str]| xs.iter().map(|s| s.to_string()).collect();
        ExecuteReport {
            verified: own(verified),
            mismatched: own(mismatched),
            // These fixtures assert the ratchet's wrong-answer path, so treat every mismatch as a
            // genuine data difference; `order_only` is covered by its own case below.
            order_only: Vec::new(),
            errored: own(errored),
        }
    }

    /// A mismatch whose row multiset matched single-node still fails the gate — an order-only
    /// difference can be a real ordering bug — but it must be reported as ORDER-ONLY rather than
    /// as a wrong answer, or a genuine wrong answer gets dismissed as "just the tie thing".
    #[test]
    fn order_only_mismatch_fails_the_gate_but_is_not_a_wrong_answer() {
        let mut r = report(&["Q1"], &["Q18"], &[]);
        r.order_only = vec!["Q18".to_string()];
        assert!(
            !check_execute_ratchet(&r, None),
            "an order-only mismatch must still fail the execute gate"
        );
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
        let path =
            std::env::temp_dir().join(format!("oxidant-tpcds-execute-baseline-{label}.json"));
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

    fn tiny_batch() -> oxidant_loom::arrow::record_batch::RecordBatch {
        use oxidant_loom::arrow::array::Int64Array;
        use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
        use oxidant_loom::arrow::record_batch::RecordBatch;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap()
    }

    /// KAN-51: worker tables must be parquet ListingTables (like live workers via
    /// `catalog_bridge`), not stat-less in-memory `MemTable`s — a scan plans as a
    /// parquet `DataSourceExec`, never `MemoryExec`.
    #[tokio::test]
    async fn worker_tables_are_parquet_listing_tables() {
        let dir =
            std::env::temp_dir().join(format!("oxidant-tpcds-dist-tbl-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let batch = tiny_batch();
        for t in tpcds_data::TABLES {
            write_parquet(
                &dir.join(format!("{t}.parquet")),
                batch.schema(),
                std::slice::from_ref(&batch),
            );
        }
        let engine = Engine::new();
        register_worker_tables(&engine, &dir, None).await;
        let plan = engine
            .sql("EXPLAIN SELECT COUNT(*) FROM store_sales")
            .await
            .unwrap()
            .into_iter()
            .map(|b| format!("{b:?}"))
            .collect::<String>();
        assert!(
            plan.contains("DataSourceExec") && plan.contains("file_type=parquet"),
            "expected a parquet ListingTable scan: {plan}"
        );
        assert!(
            !plan.contains("MemoryExec"),
            "unexpected MemTable scan: {plan}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Fact shards land as per-worker parquet files; zero-row shards still read
    /// back as a valid, empty table (the old MemTable path panicked on them).
    #[tokio::test]
    async fn fact_shards_round_trip_including_empty_shards() {
        let dir =
            std::env::temp_dir().join(format!("oxidant-tpcds-dist-shard-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let batch = tiny_batch();
        let shards = shard(std::slice::from_ref(&batch), 4); // 3 rows over 4 workers → some empty shards
        assert_eq!(shards.len(), 4);
        assert!(shards.iter().any(|s| s.is_empty()));
        let paths = write_fact_shards(&dir, "store_sales", &shards);
        let mut total = 0i64;
        for p in &paths {
            let engine = Engine::new();
            engine
                .register_parquet("shard", p.to_str().unwrap())
                .await
                .unwrap();
            let b = engine.sql("SELECT COUNT(*) FROM shard").await.unwrap();
            let counts = b[0]
                .column(0)
                .as_any()
                .downcast_ref::<oxidant_loom::arrow::array::Int64Array>()
                .unwrap();
            total += counts.value(0);
        }
        assert_eq!(total, 3);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
