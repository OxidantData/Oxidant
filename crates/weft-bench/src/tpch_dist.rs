//! Distributed TPC-H: run every query through [`plan_distributed`] and compare row-for-row
//! against single-node.
//!
//! Candidate facts (KAN-9): `lineitem`, `orders`, `partsupp`, `customer` — each query is planned
//! with the first fact that yields a distributable shape (others replicated). Worker pools shard
//! that chosen fact; Forward plans use a fully-replicated pool.
//!
//! The CI gate requires **0 single-node fallback** and **0 mismatch** across Q1–Q22.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;

use datafusion::prelude::CsvReadOptions;
use weft_execution::driver::{run_stages, Cluster, ExchangeMode, StageDef};
use weft_execution::flight::serve_worker;
use weft_execution::plan::plan_distributed;
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::Engine;

use crate::distributed_coverage::{
    check_ratchet, plan_coverage, print_report, try_plan_with_facts, write_report,
};
use crate::tpch::{normalize_batches, queries_for_sf};
use crate::tpch_data;

/// Candidate sharded facts for TPC-H (KAN-9). Order is try-order; first successful plan wins.
pub const FACT_TABLES: [&str; 4] = ["lineitem", "orders", "partsupp", "customer"];

/// Planner-only coverage: `plan_distributed` over Q1–Q22 with multi-fact candidates.
pub async fn run_planner_coverage(sf: f64, dir: &Path, skip_ratchet: bool) {
    eprintln!(
        "[tpch-dist] planner coverage sf{sf} data={} …",
        dir.display()
    );
    if let Err(e) = tpch_data::generate(sf, dir) {
        eprintln!("[tpch-dist] data generation failed: {e}");
        std::process::exit(1);
    }

    let engine = Engine::new();
    register_csv(&engine, dir).await;

    let owned = queries_for_sf(sf);
    let qs: Vec<(&str, &str)> = owned.iter().map(|(n, s)| (*n, s.as_str())).collect();
    let all = tpch_data::TABLES.to_vec();
    let facts = FACT_TABLES.to_vec();
    let only = std::env::var("WEFT_TPCH_ONLY").ok();
    let report = if let Some(ref only) = only {
        let filtered: Vec<_> = qs
            .into_iter()
            .filter(|(n, _)| n.eq_ignore_ascii_case(only))
            .collect();
        plan_coverage("tpch", &engine, &filtered, &all, &facts).await
    } else {
        plan_coverage("tpch", &engine, &qs, &all, &facts).await
    };

    for q in &report.per_query {
        if q.supported {
            eprintln!(
                "{:<4} PLAN ok (fact={})",
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
    write_report(
        Path::new("bench/distributed/tpch-planner-latest.json"),
        &report,
    );

    if !skip_ratchet
        && only.is_none()
        && !check_ratchet(
            &report,
            Path::new("bench/distributed/tpch-planner-baseline.json"),
        )
    {
        std::process::exit(1);
    }
}

/// Generate data, build worker clusters, and run all 22 queries through the distributed engine.
pub async fn run(sf: f64, dir: &Path, num_workers: usize) {
    eprintln!("[tpch-dist] generating sf{sf} into {} …", dir.display());
    if let Err(e) = tpch_data::generate(sf, dir) {
        eprintln!("[tpch-dist] data generation failed: {e}");
        std::process::exit(1);
    }

    let single = Engine::new();
    register_csv(&single, dir).await;

    let mut full: Vec<(&str, Vec<RecordBatch>)> = Vec::new();
    for t in tpch_data::TABLES {
        let b = single.sql(&format!("SELECT * FROM {t}")).await.unwrap();
        full.push((t, b));
    }

    let all = tpch_data::TABLES.to_vec();
    let facts = FACT_TABLES.to_vec();
    let only = std::env::var("WEFT_TPCH_ONLY").ok();
    let fail_on_fallback = std::env::var("WEFT_TPCH_DIST_REQUIRE_ALL")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(true);

    let owned = queries_for_sf(sf);
    let qs: Vec<(&str, &str)> = owned.iter().map(|(n, s)| (*n, s.as_str())).collect();

    let mut planned: Vec<(String, String, String)> = Vec::new(); // name, sql, fact
    for (name, raw) in &qs {
        if let Some(ref o) = only {
            if *name != o.as_str() {
                continue;
            }
        }
        let sql = raw.trim().trim_end_matches(';').trim().to_string();
        match try_plan_with_facts(&single, &sql, &all, &facts).await {
            Ok((_dq, fact)) => planned.push((name.to_string(), sql, fact)),
            Err(e) => {
                eprintln!("{name:<4} PLAN ERROR: {e}");
                if fail_on_fallback {
                    // Counted below via dist_ok < 22
                }
            }
        }
    }

    let needed_facts: Vec<&str> = {
        let mut seen = BTreeSet::new();
        for (_, _, fact) in &planned {
            seen.insert(fact.as_str());
        }
        seen.into_iter().collect()
    };
    let clusters = build_clusters(&full, num_workers, &needed_facts).await;
    eprintln!(
        "[tpch-dist] {num_workers} workers × facts {:?} + full-replicate pool\n",
        needed_facts
    );

    let (mut dist_ok, mut fallback, mut mismatch) = (0usize, 0usize, 0usize);
    // Track plan failures for queries that never entered `planned`.
    let planned_names: BTreeSet<&str> = planned.iter().map(|(n, _, _)| n.as_str()).collect();
    for (name, _) in &qs {
        if let Some(ref o) = only {
            if *name != o.as_str() {
                continue;
            }
        }
        if !planned_names.contains(name) {
            fallback += 1;
        }
    }

    for (qi, (name, sql, fact)) in planned.iter().enumerate() {
        let replicated: Vec<&str> = all
            .iter()
            .copied()
            .filter(|t| *t != fact.as_str())
            .collect();
        let dq = match plan_distributed(&single, sql, &replicated).await {
            Ok(dq) => dq,
            Err(e) => {
                fallback += 1;
                eprintln!("{name:<4} PLAN ERROR (unexpected replan): {e}");
                continue;
            }
        };

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
                .unwrap_or_else(|| panic!("missing cluster for fact {fact}"))
        };
        let mode = if forward { "forward" } else { "shuffle" };

        if std::env::var("WEFT_TPCH_DEBUG").is_ok() {
            for s in &dq.stages {
                eprintln!(
                    "  {name} [{mode} fact={fact}] stage{} keys{:?} exch={:?}: {}",
                    s.stage_id, s.hash_key_cols, s.exchange, s.sql
                );
            }
        }

        let base = (qi as u32 + 1) * 1000;
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
            })
            .collect();

        let mut gathered = None;
        let mut last_err = None;
        for _ in 0..30 {
            match run_stages(cluster, &stages).await {
                Ok(b) => {
                    gathered = Some(b);
                    break;
                }
                Err(e) => {
                    let transient = e.to_string().contains("connect");
                    last_err = Some(e);
                    if !transient {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
        let gathered = match gathered {
            Some(b) => b,
            None => {
                mismatch += 1;
                eprintln!(
                    "{name:<4} distributed ERROR: {}",
                    last_err.map(|e| e.to_string()).unwrap_or_default()
                );
                continue;
            }
        };
        let result = match &dq.finalize_sql {
            None => gathered,
            Some(f) => {
                let fin = Engine::new();
                fin.register_batches("result", gathered).unwrap();
                fin.sql(f).await.unwrap()
            }
        };

        let expected = single.sql(sql).await.unwrap();
        if normalize_batches(&result) == normalize_batches(&expected) {
            dist_ok += 1;
            eprintln!(
                "{name:<4} distributed ok [{mode} fact={fact}] ({} stages)",
                dq.stages.len()
            );
        } else {
            mismatch += 1;
            eprintln!("{name:<4} distributed MISMATCH vs single-node [{mode} fact={fact}]");
        }
    }

    let considered = if only.is_some() {
        planned.len() + fallback
    } else {
        22
    };
    eprintln!(
        "\n=== TPC-H distributed sf{sf}: {dist_ok} distributed-ok, {fallback} plan-error, \
         {mismatch} mismatch (of {considered}) ==="
    );
    if mismatch > 0 || (fail_on_fallback && fallback > 0) || (only.is_none() && dist_ok < 22) {
        if only.is_none() && dist_ok < 22 {
            eprintln!(
                "[tpch-dist] required 22 distributed-ok, got {dist_ok} (fallback={fallback}, \
                 mismatch={mismatch})"
            );
        }
        std::process::exit(1);
    }
}

struct ClusterSet {
    by_fact: HashMap<String, Cluster>,
    full: Cluster,
}

async fn build_clusters(
    full: &[(&str, Vec<RecordBatch>)],
    num_workers: usize,
    needed_facts: &[&str],
) -> ClusterSet {
    let mut by_fact = HashMap::new();
    for (fi, fact) in FACT_TABLES.iter().enumerate() {
        if !needed_facts.contains(fact) {
            continue;
        }
        let fact_batches = full.iter().find(|(t, _)| *t == *fact).unwrap().1.clone();
        let shards = shard(&fact_batches, num_workers);
        let base_port = 50670u16 + (fi as u16) * 20;
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
        let port = 50770 + i as u16;
        let ee = e.clone();
        tokio::spawn(async move {
            let _ = serve_worker(port, ee).await;
        });
        full_endpoints.push(format!("http://127.0.0.1:{port}"));
    }

    ClusterSet {
        by_fact,
        full: Cluster::new(full_endpoints),
    }
}

/// Register all eight TPC-H CSVs on `engine` with their explicit schemas.
async fn register_csv(engine: &Engine, dir: &Path) {
    for t in tpch_data::TABLES {
        let path = dir.join(format!("{t}.csv"));
        let sch = tpch_data::schema(t);
        let opts = CsvReadOptions::new().has_header(true).schema(sch.as_ref());
        engine
            .ctx()
            .register_csv(t, path.to_str().unwrap(), opts)
            .await
            .unwrap_or_else(|e| panic!("register {t}: {e}"));
    }
}

/// Split `batches` row-wise into `n` shards (each batch sliced into n contiguous ranges), so every
/// worker gets a portion of the fact even when the table is a single batch.
pub(crate) fn shard(batches: &[RecordBatch], n: usize) -> Vec<Vec<RecordBatch>> {
    let mut out: Vec<Vec<RecordBatch>> = (0..n).map(|_| Vec::new()).collect();
    for b in batches {
        let rows = b.num_rows();
        let chunk = (rows + n - 1) / n;
        for (i, shard) in out.iter_mut().enumerate() {
            let start = (i * chunk).min(rows);
            let len = chunk.min(rows - start);
            if len > 0 {
                shard.push(b.slice(start, len));
            }
        }
    }
    out
}
