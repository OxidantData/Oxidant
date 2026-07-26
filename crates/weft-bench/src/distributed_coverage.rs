//! Shared planner-coverage harness for distributed auto-splitting.
//!
//! Runs [`plan_distributed`] over a query suite, buckets reject reasons, and enforces a
//! committed baseline (supported count must not drop).

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use weft_execution::plan::{plan_distributed_logical, DistributedQuery};
use weft_loom::Engine;

/// Outcome of attempting to plan one query for distribution.
#[derive(Debug, Clone, Serialize)]
pub struct QueryPlanOutcome {
    pub name: String,
    pub supported: bool,
    /// Histogram bucket when unsupported.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Which fact table was sharded when planning succeeded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sharded_fact: Option<String>,
}

/// Aggregated planner coverage for a suite.
#[derive(Debug, Clone, Serialize)]
pub struct CoverageReport {
    pub suite: String,
    pub query_count: usize,
    pub supported: usize,
    pub supported_queries: Vec<String>,
    pub reason_histogram: BTreeMap<String, usize>,
    pub per_query: Vec<QueryPlanOutcome>,
}

#[derive(Debug, Deserialize)]
struct Baseline {
    suite: String,
    query_count: usize,
    supported: usize,
    #[serde(default)]
    supported_queries: Vec<String>,
    #[allow(dead_code)]
    reason_histogram: BTreeMap<String, usize>,
}

/// Normalize an `Unsupported` / planner error into a stable histogram bucket.
pub fn bucket_reason(msg: &str) -> String {
    let s = msg
        .strip_prefix("unsupported: ")
        .or_else(|| msg.strip_prefix("Unsupported: "))
        .unwrap_or(msg);
    let s = s.strip_prefix("auto-distribute: ").unwrap_or(s);
    let s = s.split(" — ").next().unwrap_or(s).trim();
    let s = s.split(';').next().unwrap_or(s).trim();
    if s.len() > 96 {
        format!("{}…", &s[..93])
    } else {
        s.to_string()
    }
}

/// Try shape-based [`plan_distributed_logical`] with each candidate sharded fact (all other
/// tables replicated). Uses the logical planner (no Forward fallback) so the coverage ratchet
/// measures real distribution, not single-worker Forward.
pub async fn try_plan_with_facts(
    engine: &Engine,
    sql: &str,
    all_tables: &[&str],
    fact_tables: &[&str],
) -> Result<(DistributedQuery, String), String> {
    let lp = engine
        .logical_plan(sql)
        .await
        .map_err(|e| bucket_reason(&e.to_string()))?;
    let mut errors = Vec::new();
    for fact in fact_tables {
        let replicated: Vec<&str> = all_tables.iter().copied().filter(|t| *t != *fact).collect();
        match plan_distributed_logical(&lp, &replicated) {
            Ok(dq) => return Ok((dq, fact.to_string())),
            Err(e) => errors.push(bucket_reason(&e.to_string())),
        }
    }
    Err(mode_error(&errors))
}

fn mode_error(errors: &[String]) -> String {
    if errors.is_empty() {
        return "plan failed".into();
    }
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for e in errors {
        *counts.entry(e.as_str()).or_default() += 1;
    }
    counts
        .into_iter()
        .max_by_key(|(_, n)| *n)
        .map(|(k, _)| k.to_string())
        .unwrap_or_else(|| errors[0].clone())
}

/// Build a coverage report over `queries` (name, sql pairs).
pub async fn plan_coverage(
    suite: &str,
    engine: &Engine,
    queries: &[(&str, &str)],
    all_tables: &[&str],
    fact_tables: &[&str],
) -> CoverageReport {
    let mut per_query = Vec::with_capacity(queries.len());
    let mut reason_histogram: BTreeMap<String, usize> = BTreeMap::new();
    let mut supported_queries = Vec::new();

    for (name, raw) in queries {
        let sql = raw.trim().trim_end_matches(';').trim();
        match try_plan_with_facts(engine, sql, all_tables, fact_tables).await {
            Ok((_dq, fact)) => {
                supported_queries.push(name.to_string());
                per_query.push(QueryPlanOutcome {
                    name: name.to_string(),
                    supported: true,
                    reason: None,
                    sharded_fact: Some(fact),
                });
            }
            Err(reason) => {
                *reason_histogram.entry(reason.clone()).or_default() += 1;
                per_query.push(QueryPlanOutcome {
                    name: name.to_string(),
                    supported: false,
                    reason: Some(reason),
                    sharded_fact: None,
                });
            }
        }
    }

    CoverageReport {
        suite: suite.to_string(),
        query_count: queries.len(),
        supported: supported_queries.len(),
        supported_queries,
        reason_histogram,
        per_query,
    }
}

/// Single-sharded-table coverage (TPC-H: `lineitem` sharded, all else replicated).
pub async fn plan_coverage_single_shard(
    suite: &str,
    engine: &Engine,
    queries: &[(&str, &str)],
    all_tables: &[&str],
    sharded: &str,
) -> CoverageReport {
    let replicated: Vec<&str> = all_tables
        .iter()
        .copied()
        .filter(|t| *t != sharded)
        .collect();
    let mut per_query = Vec::with_capacity(queries.len());
    let mut reason_histogram: BTreeMap<String, usize> = BTreeMap::new();
    let mut supported_queries = Vec::new();

    for (name, raw) in queries {
        let sql = raw.trim().trim_end_matches(';').trim();
        let outcome = match engine.logical_plan(sql).await {
            Ok(lp) => plan_distributed_logical(&lp, &replicated).map_err(|e| e.to_string()),
            Err(e) => Err(e.to_string()),
        };
        match outcome {
            Ok(_dq) => {
                supported_queries.push(name.to_string());
                per_query.push(QueryPlanOutcome {
                    name: name.to_string(),
                    supported: true,
                    reason: None,
                    sharded_fact: Some(sharded.to_string()),
                });
            }
            Err(e) => {
                let reason = bucket_reason(&e);
                *reason_histogram.entry(reason.clone()).or_default() += 1;
                per_query.push(QueryPlanOutcome {
                    name: name.to_string(),
                    supported: false,
                    reason: Some(reason),
                    sharded_fact: None,
                });
            }
        }
    }

    CoverageReport {
        suite: suite.to_string(),
        query_count: queries.len(),
        supported: supported_queries.len(),
        supported_queries,
        reason_histogram,
        per_query,
    }
}

pub fn print_report(report: &CoverageReport) {
    eprintln!(
        "\n=== {} planner coverage: {}/{} distributable ===",
        report.suite, report.supported, report.query_count
    );
    if !report.reason_histogram.is_empty() {
        eprintln!("reject reasons:");
        for (reason, count) in &report.reason_histogram {
            eprintln!("  {count:>3}  {reason}");
        }
    }
    eprintln!(
        "supported_json={}",
        serde_json::to_string(&report.supported_queries).unwrap_or_default()
    );
}

/// Write the report JSON to `out_path`.
pub fn write_report(path: &Path, report: &CoverageReport) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    std::fs::write(path, serde_json::to_string_pretty(report).unwrap())
        .unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
    eprintln!("[coverage] wrote {}", path.display());
}

/// Fail if supported count dropped below the committed baseline.
pub fn check_ratchet(report: &CoverageReport, baseline_path: &Path) -> bool {
    let base: Baseline = serde_json::from_str(
        &std::fs::read_to_string(baseline_path)
            .unwrap_or_else(|e| panic!("read baseline {}: {e}", baseline_path.display())),
    )
    .unwrap_or_else(|e| panic!("parse baseline {}: {e}", baseline_path.display()));

    if base.suite != report.suite {
        eprintln!(
            "[coverage] baseline suite mismatch: {} vs {}",
            base.suite, report.suite
        );
        return false;
    }
    if base.query_count != report.query_count {
        eprintln!(
            "[coverage] query count changed ({} vs baseline {}) — re-baseline if the corpus moved",
            report.query_count, base.query_count
        );
        return false;
    }

    let mut ok = true;
    if report.supported < base.supported {
        eprintln!(
            "[coverage] RATCHET REGRESSION: supported {} < baseline {}",
            report.supported, base.supported
        );
        let missing: Vec<_> = base
            .supported_queries
            .iter()
            .filter(|q| !report.supported_queries.contains(q))
            .cloned()
            .collect();
        if !missing.is_empty() {
            eprintln!("[coverage] lost queries: {}", missing.join(", "));
        }
        ok = false;
    }

    if report.supported > base.supported {
        let gained: Vec<_> = report
            .supported_queries
            .iter()
            .filter(|q| !base.supported_queries.contains(q))
            .cloned()
            .collect();
        eprintln!(
            "[coverage] ratchet gain: +{} distributable — re-baseline {}: {}",
            gained.len(),
            baseline_path.display(),
            gained.join(", ")
        );
    }

    if ok {
        eprintln!(
            "[coverage] ratchet OK: {}/{} distributable held (baseline {}/{})",
            report.supported, report.query_count, base.supported, base.query_count
        );
    }
    ok
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_strips_prefix_and_truncates() {
        let msg = "unsupported: auto-distribute: window functions are not supported \
                   (no PARTITION BY shuffle path matched) — falling back to local execution";
        assert_eq!(
            bucket_reason(msg),
            "window functions are not supported (no PARTITION BY shuffle path matched)"
        );
    }

    #[test]
    fn mode_error_picks_most_common() {
        assert_eq!(mode_error(&["a".into(), "b".into(), "a".into(),]), "a");
    }
}
