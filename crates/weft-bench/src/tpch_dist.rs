//! Distributed TPC-H: run every query through [`plan_distributed`] and compare row-for-row
//! against single-node.
//!
//! Two worker pools:
//! - **Sharded** — `lineitem` partitioned across workers, dimensions replicated. Used when the
//!   shape-based splitter emits a multi-stage Hash plan.
//! - **Full-replicate** — every table present in full on every worker. Used for
//!   [`ExchangeMode::Forward`](weft_execution::driver::ExchangeMode) single-stage plans (Sail
//!   shared-storage coverage path) so correlated / nested queries stay correct.
//!
//! The CI gate requires **0 single-node fallback** and **0 mismatch** (22 distributed-ok).

use std::path::Path;
use std::sync::Arc;

use datafusion::prelude::CsvReadOptions;
use weft_execution::driver::{run_stages, Cluster, ExchangeMode, StageDef};
use weft_execution::flight::serve_worker;
use weft_execution::plan::plan_distributed;
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::Engine;

use crate::tpch::{normalize_batches, queries};
use crate::tpch_data;

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
    let lineitem = &full.iter().find(|(t, _)| *t == "lineitem").unwrap().1;
    let shards = shard(lineitem, num_workers);

    // Sharded pool: lineitem partitioned, dims replicated.
    let mut shard_endpoints = Vec::new();
    for (i, shard_batches) in shards.into_iter().enumerate() {
        let e = Arc::new(Engine::new());
        for (t, batches) in &full {
            let data = if *t == "lineitem" {
                shard_batches.clone()
            } else {
                batches.clone()
            };
            e.register_batches(t, data).unwrap();
        }
        let port = 50670 + i as u16;
        let ee = e.clone();
        tokio::spawn(async move {
            let _ = serve_worker(port, ee).await;
        });
        shard_endpoints.push(format!("http://127.0.0.1:{port}"));
    }
    let shard_cluster = Cluster::new(shard_endpoints);

    // Full-replicate pool: every table complete on every worker (Forward path).
    let mut full_endpoints = Vec::new();
    for i in 0..num_workers {
        let e = Arc::new(Engine::new());
        for (t, batches) in &full {
            e.register_batches(t, batches.clone()).unwrap();
        }
        let port = 50770 + i as u16;
        let ee = e.clone();
        tokio::spawn(async move {
            let _ = serve_worker(port, ee).await;
        });
        full_endpoints.push(format!("http://127.0.0.1:{port}"));
    }
    let full_cluster = Cluster::new(full_endpoints);

    let replicated: Vec<&str> = tpch_data::TABLES
        .iter()
        .copied()
        .filter(|t| *t != "lineitem")
        .collect();
    eprintln!(
        "[tpch-dist] {num_workers} sharded workers + {num_workers} full-replicate workers \
         ({} dims replicated on shard pool)\n",
        replicated.len()
    );

    let only = std::env::var("WEFT_TPCH_ONLY").ok();
    let fail_on_fallback = std::env::var("WEFT_TPCH_DIST_REQUIRE_ALL")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(true);
    let (mut dist_ok, mut fallback, mut mismatch) = (0usize, 0usize, 0usize);
    for (qi, (name, raw)) in queries().into_iter().enumerate() {
        if let Some(o) = &only {
            if name != o {
                continue;
            }
        }
        let sql = raw.trim().trim_end_matches(';').trim();
        let dq = match plan_distributed(&single, sql, &replicated).await {
            Ok(dq) => dq,
            Err(e) => {
                fallback += 1;
                eprintln!("{name:<4} PLAN ERROR (unexpected): {e}");
                continue;
            }
        };

        let forward = dq
            .stages
            .iter()
            .any(|s| s.exchange == ExchangeMode::Forward);
        let cluster = if forward {
            &full_cluster
        } else {
            &shard_cluster
        };
        let mode = if forward { "forward" } else { "shuffle" };

        if std::env::var("WEFT_TPCH_DEBUG").is_ok() {
            for s in &dq.stages {
                eprintln!(
                    "  {name} [{mode}] stage{} keys{:?} exch={:?}: {}",
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
                "{name:<4} distributed ok [{mode}] ({} stages)",
                dq.stages.len()
            );
        } else {
            mismatch += 1;
            eprintln!("{name:<4} distributed MISMATCH vs single-node [{mode}]");
        }
    }

    eprintln!(
        "\n=== TPC-H distributed sf{sf}: {dist_ok} distributed-ok, {fallback} plan-error, \
         {mismatch} mismatch (of 22) ==="
    );
    if mismatch > 0 || (fail_on_fallback && fallback > 0) || dist_ok < 22 {
        if dist_ok < 22 {
            eprintln!(
                "[tpch-dist] required 22 distributed-ok, got {dist_ok} (fallback={fallback}, \
                 mismatch={mismatch})"
            );
        }
        std::process::exit(1);
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
/// worker gets a portion of lineitem even when the table is a single batch.
fn shard(batches: &[RecordBatch], n: usize) -> Vec<Vec<RecordBatch>> {
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
