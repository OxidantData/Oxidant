//! Shared timing + JSON result writing for TPC-H / TPC-DS (and future) suites.
//!
//! Contract matches ClickBench / the site: 3 tries per query, hot = min(try2, try3),
//! failed queries recorded as null gaps (never silently dropped).

use std::path::Path;
use std::process::Command;
use std::time::Instant;

use serde_json::{json, Value};
use weft_loom::Engine;

#[derive(Clone, Debug)]
pub struct Query<'a> {
    pub name: &'a str,
    pub sql: &'a str,
}

#[derive(Clone, Debug)]
pub struct EngineResult {
    pub key: String,
    pub name: String,
    pub highlight: bool,
    pub per_query: Vec<Option<f64>>,
    pub failures: usize,
    pub failed_queries: Vec<usize>,
}

impl EngineResult {
    pub fn total(&self) -> Option<f64> {
        if self.per_query.iter().any(|t| t.is_none()) && self.failures > 0 {
            // Fair total over completed queries only; still surface failures separately.
        }
        let mut sum = 0.0;
        let mut any = false;
        for t in &self.per_query {
            if let Some(v) = t {
                sum += v;
                any = true;
            }
        }
        any.then_some(sum)
    }

    pub fn total_all(&self) -> Option<f64> {
        self.total()
    }
}

/// Run `queries` through Weft (engine-direct). Returns per-query hot times.
pub async fn run_weft(engine: &Engine, queries: &[Query<'_>]) -> EngineResult {
    let mut per_query = Vec::with_capacity(queries.len());
    let mut failures = 0usize;
    let mut failed_queries = Vec::new();

    for (idx, q) in queries.iter().enumerate() {
        let sql = q.sql.trim().trim_end_matches(';').trim();
        let mut times = Vec::new();
        for _ in 0..3 {
            let t = Instant::now();
            match engine.sql(sql).await {
                Ok(_batches) => times.push(t.elapsed().as_secs_f64()),
                Err(e) => {
                    failures += 1;
                    failed_queries.push(idx + 1);
                    eprintln!(
                        "{:<5} FAIL  {}",
                        q.name,
                        e.to_string().lines().next().unwrap_or("")
                    );
                    times.clear();
                    break;
                }
            }
        }
        if times.len() < 3 {
            per_query.push(None);
            continue;
        }
        let hot = times[1].min(times[2]);
        per_query.push(Some(hot));
        eprintln!("{:<5} {hot:>8.4}s", q.name);
    }

    EngineResult {
        key: "weft".into(),
        name: "Weft".into(),
        highlight: true,
        per_query,
        failures,
        failed_queries,
    }
}

/// Run the same queries through DuckDB CLI over a DuckDB database file (`.db`) or a directory of
/// Parquet exported by DuckDB (`EXPORT DATABASE`). Used as an independent CPU baseline on the
/// same box.
pub fn run_duckdb(duckdb: &str, data: &Path, queries: &[Query<'_>]) -> EngineResult {
    let mut per_query = Vec::with_capacity(queries.len());
    let mut failures = 0usize;
    let mut failed_queries = Vec::new();

    let setup = duckdb_setup_sql(data);

    for (idx, q) in queries.iter().enumerate() {
        let sql = q.sql.trim().trim_end_matches(';').trim();
        let mut times = Vec::new();
        for _ in 0..3 {
            let script = format!("{setup}\n.timer on\n{sql};");
            let t = Instant::now();
            let out = Command::new(duckdb)
                .args(["-c", &script])
                .output();
            match out {
                Ok(o) if o.status.success() => times.push(t.elapsed().as_secs_f64()),
                Ok(o) => {
                    failures += 1;
                    failed_queries.push(idx + 1);
                    let err = String::from_utf8_lossy(&o.stderr);
                    eprintln!(
                        "{:<5} FAIL  {}",
                        q.name,
                        err.lines().next().unwrap_or("duckdb error")
                    );
                    times.clear();
                    break;
                }
                Err(e) => {
                    failures += 1;
                    failed_queries.push(idx + 1);
                    eprintln!("{:<5} FAIL  {e}", q.name);
                    times.clear();
                    break;
                }
            }
        }
        if times.len() < 3 {
            per_query.push(None);
            continue;
        }
        let hot = times[1].min(times[2]);
        per_query.push(Some(hot));
        eprintln!("{:<5} {hot:>8.4}s  (duckdb)", q.name);
    }

    EngineResult {
        key: "duckdb".into(),
        name: "DuckDB".into(),
        highlight: false,
        per_query,
        failures,
        failed_queries,
    }
}

fn duckdb_setup_sql(data: &Path) -> String {
    if data.is_file() {
        // Attach a DuckDB database file containing the TPC tables.
        format!(
            "ATTACH '{}' AS bench (READ_ONLY); USE bench;",
            data.display()
        )
    } else {
        // Parquet export directory: one `<table>.parquet` (or nested) per table.
        let mut s = String::new();
        if let Ok(rd) = std::fs::read_dir(data) {
            for ent in rd.flatten() {
                let p = ent.path();
                let name = p.file_stem().and_then(|n| n.to_str()).unwrap_or("");
                if p.extension().and_then(|e| e.to_str()) == Some("parquet") && !name.is_empty() {
                    s.push_str(&format!(
                        "CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{}');\n",
                        p.display()
                    ));
                }
            }
        }
        s
    }
}

pub fn write_site_json(
    path: &Path,
    dataset: &str,
    machine: &str,
    run_date: &str,
    method: &str,
    query_count: usize,
    engines: &[EngineResult],
) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Fair common-set: queries every measured engine completed.
    let mut common_mask = vec![true; query_count];
    for e in engines {
        for (i, t) in e.per_query.iter().enumerate() {
            if t.is_none() {
                common_mask[i] = false;
            }
        }
    }
    let common_count = common_mask.iter().filter(|&&b| b).count();

    let engines_json: Vec<Value> = engines
        .iter()
        .map(|e| {
            let common_total: Option<f64> = {
                let mut sum = 0.0;
                let mut ok = true;
                let mut any = false;
                for (i, t) in e.per_query.iter().enumerate() {
                    if !common_mask[i] {
                        continue;
                    }
                    match t {
                        Some(v) => {
                            sum += v;
                            any = true;
                        }
                        None => ok = false,
                    }
                }
                (ok && any).then_some(sum)
            };
            let total_all = e.total_all();
            json!({
                "key": e.key,
                "name": e.name,
                "highlight": e.highlight,
                "total": common_total,
                "totalAll": total_all,
                "source": format!("measured (EC2 {machine} {run_date})"),
                "perQuery": e.per_query,
                "failures": e.failures,
                "failedQueries": e.failed_queries,
            })
        })
        .collect();

    let doc = json!({
        "dataset": dataset,
        "machine": machine,
        "runDate": run_date,
        "queryCount": query_count,
        "commonCount": common_count,
        "method": method,
        "engines": engines_json,
    });

    std::fs::write(path, serde_json::to_vec_pretty(&doc)?)?;
    eprintln!("[suite] wrote {}", path.display());
    Ok(())
}

/// Locate a `duckdb` binary on PATH / common install locations.
pub fn duckdb_path() -> Option<String> {
    for cand in [
        "duckdb",
        "/usr/local/bin/duckdb",
        "/opt/homebrew/opt/duckdb/bin/duckdb",
        "/tmp/duckdb",
    ] {
        if Command::new(cand).arg("--version").output().is_ok() {
            return Some(cand.to_string());
        }
    }
    None
}
