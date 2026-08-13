//! Classification guard: baseline-pinned plan/decline matrix for all 99 TPC-DS
//! queries under two simulated SF100 auto-broadcast classifications.
//!
//! This test ports the scratch q2diag harness (which predicted the v0.1.11
//! auto-broadcast regression blast radius 20/20) into the repo as a permanent
//! guard. It builds an Engine over empty MemTables with the SF100 TPC-DS
//! schemas, then runs `plan_distributed_logical` for every query under two
//! explicit replicated-table lists mirroring historical classifications:
//!
//!   * `bcast_32gib`: v0.1.10-equivalent — only the query's byte-max sales fact
//!     stays sharded (store_sales ~29 GB > catalog_sales ~14.5 GB >
//!     web_sales ~7.2 GB at SF100 against a 32 GiB threshold).
//!   * `bcast_4gib`: v0.1.11-equivalent — all three sales facts exceed 4 GiB,
//!     so all stay sharded; everything else is replicated.
//!
//! The expected outcome per (query, classification) — `ok(<stage count>)` or
//! `fail` — is pinned in `fixtures/classification_guard_baseline.tsv`. The
//! test FAILS when any query's plan/decline status (or stage count) changes
//! vs the baseline. Intentional behavior changes must update the baseline in
//! the same PR: regenerate with
//!
//!   OXIDANT_CLASSIFICATION_GUARD_BLESS=1 cargo test -p oxidant-execution \
//!       --test classification_guard
//!
//! and review the diff carefully (a query flipping `ok` -> `fail` is a
//! regression of exactly the class this guard exists to catch).
//!
//! Planning-only (no data, MemTables); runs in seconds and is part of the
//! normal `cargo test -p oxidant-execution` suite.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use oxidant_execution::plan::plan_distributed_logical;
use oxidant_loom::Engine;

// SF100 byte order among the sales facts (see shard.rs KAN-161 comment):
// store_sales (~29 GB) > catalog_sales (~14.5 GB) > web_sales (~7.2 GB).
const SALES: [&str; 3] = ["store_sales", "catalog_sales", "web_sales"];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

fn baseline_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/classification_guard_baseline.tsv")
}

fn tpcds_schemas() -> HashMap<String, Schema> {
    let text = std::fs::read_to_string(repo_root().join("bench/tpc/tpcds_types.tsv")).unwrap();
    let mut m = HashMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (table, cols) = line.split_once('\t').unwrap();
        let mut fields = Vec::new();
        for col in cols.split('|') {
            let Some((name, typ)) = col.rsplit_once(':') else {
                continue;
            };
            let t = typ.trim().to_ascii_lowercase();
            let dt = if t == "integer" {
                DataType::Int32
            } else if t == "date" {
                DataType::Date32
            } else if let Some(ps) = t.strip_prefix("decimal(").and_then(|s| s.strip_suffix(')')) {
                let (p, s) = ps.split_once(',').unwrap();
                DataType::Decimal128(p.trim().parse().unwrap(), s.trim().parse().unwrap())
            } else if t.starts_with("char") || t.starts_with("varchar") || t == "time" {
                DataType::Utf8
            } else {
                panic!("unhandled type {t}");
            };
            fields.push(Field::new(name, dt, true));
        }
        m.insert(table.to_string(), Schema::new(fields));
    }
    m
}

/// Crude scan for sales-fact references; classification only cares about the
/// three sales facts (substring match is how the scratch harness did it too).
fn sales_facts_of(sql: &str) -> Vec<String> {
    let lower = sql.to_ascii_lowercase();
    SALES
        .iter()
        .filter(|t| lower.contains(**t))
        .map(|s| s.to_string())
        .collect()
}

/// 4 GiB threshold (v0.1.11-equivalent): all three sales facts > 4 GiB stay
/// sharded; every other table is replicated.
fn replicated_4gib(all: &[String]) -> Vec<String> {
    all.iter()
        .filter(|t| !SALES.contains(&t.as_str()))
        .cloned()
        .collect()
}

/// 32 GiB threshold (v0.1.10-equivalent): only the query's byte-max sales fact
/// stays sharded; everything else (including smaller sales facts) replicates.
fn replicated_32gib(all: &[String], sql: &str) -> Vec<String> {
    let scanned = sales_facts_of(sql);
    let sharded = SALES.iter().find(|t| scanned.contains(&t.to_string()));
    all.iter()
        .filter(|t| Some(&t.as_str()) != sharded)
        .cloned()
        .collect()
}

/// Mirror the driver's optimized-then-original split retry: plan the optimized
/// plan; on failure, if optimization changed the plan, retry the original.
async fn plan_stage_count(
    engine: &Engine,
    sql: &str,
    replicated: &[String],
) -> Result<usize, String> {
    let refs: Vec<&str> = replicated.iter().map(String::as_str).collect();
    let lp = engine
        .logical_plan(sql)
        .await
        .map_err(|e| format!("logical_plan: {}", first_line(&e.to_string())))?;
    let optimized = engine
        .optimize_logical_plan(lp.clone())
        .unwrap_or_else(|_| lp.clone());
    match plan_distributed_logical(&optimized, &refs) {
        Ok(dq) => Ok(dq.stages.len()),
        Err(e) => {
            if format!("{}", optimized.display_indent()) != format!("{}", lp.display_indent()) {
                match plan_distributed_logical(&lp, &refs) {
                    Ok(dq) => Ok(dq.stages.len()),
                    Err(e2) => Err(first_line(&e2.to_string())),
                }
            } else {
                Err(first_line(&e.to_string()))
            }
        }
    }
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or(s).trim().to_string()
}

fn split_statements(sql: &str) -> Vec<String> {
    sql.split(';')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// `(name, sql)` for every statement of every TPC-DS query, q1..q99 in order.
fn all_statements() -> Vec<(String, String)> {
    let qdir = repo_root().join("bench/tpcds/queries");
    let mut files: Vec<_> = std::fs::read_dir(&qdir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".sql"))
        .collect();
    files.sort_by_key(|n| {
        n.trim_start_matches('q')
            .trim_end_matches(".sql")
            .parse::<u32>()
            .unwrap()
    });
    assert_eq!(files.len(), 99, "expected all 99 TPC-DS query files");
    let mut out = Vec::new();
    for f in &files {
        let sql = std::fs::read_to_string(qdir.join(f)).unwrap();
        let stmts = split_statements(&sql);
        for (i, stmt) in stmts.iter().enumerate() {
            let name = if stmts.len() > 1 {
                format!("{}{}", f.trim_end_matches(".sql"), (b'a' + i as u8) as char)
            } else {
                f.trim_end_matches(".sql").to_string()
            };
            out.push((name, stmt.clone()));
        }
    }
    out
}

fn outcome_s(r: &Result<usize, String>) -> String {
    match r {
        Ok(n) => format!("ok({n})"),
        Err(_) => "fail".to_string(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn classification_guard_baseline() {
    let engine = Engine::new();
    let schemas = tpcds_schemas();
    let mut all: Vec<String> = schemas.keys().cloned().collect();
    all.sort();
    for (name, schema) in &schemas {
        let table = MemTable::try_new(Arc::new(schema.clone()), vec![vec![]]).unwrap();
        engine.ctx().register_table(name, Arc::new(table)).unwrap();
    }

    // header + one row per statement: `name<TAB>bcast_32gib<TAB>bcast_4gib`
    let mut rows: Vec<String> = Vec::new();
    let mut errors: Vec<(String, String, String)> = Vec::new();
    for (name, sql) in all_statements() {
        let old = plan_stage_count(&engine, &sql, &replicated_32gib(&all, &sql)).await;
        let new = plan_stage_count(&engine, &sql, &replicated_4gib(&all)).await;
        if let Err(e) = &old {
            errors.push((name.clone(), "bcast_32gib".into(), e.clone()));
        }
        if let Err(e) = &new {
            errors.push((name.clone(), "bcast_4gib".into(), e.clone()));
        }
        rows.push(format!("{name}\t{}\t{}", outcome_s(&old), outcome_s(&new)));
    }

    let actual = format!("# query\tbcast_32gib\tbcast_4gib\n{}\n", rows.join("\n"));

    let path = baseline_path();
    if std::env::var("OXIDANT_CLASSIFICATION_GUARD_BLESS").is_ok() {
        std::fs::write(&path, &actual).unwrap();
        println!("blessed baseline written to {}", path.display());
        return;
    }

    let expected = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read baseline {}: {e}", path.display()));
    if actual != expected {
        let exp_lines: Vec<&str> = expected.lines().collect();
        let act_lines: Vec<&str> = actual.lines().collect();
        let mut diff = String::from("classification guard baseline mismatch:\n");
        for (e, a) in exp_lines.iter().zip(act_lines.iter()) {
            if e != a {
                diff.push_str(&format!("  expected: {e}\n  actual:   {a}\n"));
            }
        }
        if exp_lines.len() != act_lines.len() {
            diff.push_str(&format!(
                "  line count changed: expected {} actual {}\n",
                exp_lines.len(),
                act_lines.len()
            ));
        }
        diff.push_str(
            "If this change is intentional, regenerate the baseline with \
             OXIDANT_CLASSIFICATION_GUARD_BLESS=1 and review the diff in the same PR.",
        );
        panic!("{diff}");
    }

    // Sanity summary (visible with --nocapture): which queries decline today.
    let fail_32: Vec<&str> = rows
        .iter()
        .filter_map(|r| {
            let mut c = r.split('\t');
            match (c.next(), c.next()) {
                (Some(n), Some("fail")) => Some(n),
                _ => None,
            }
        })
        .collect();
    let fail_4: Vec<&str> = rows
        .iter()
        .filter_map(|r| {
            let mut c = r.split('\t');
            match (c.next(), c.nth(1)) {
                (Some(n), Some("fail")) => Some(n),
                _ => None,
            }
        })
        .collect();
    println!("bcast_32gib declines: {fail_32:?}");
    println!("bcast_4gib declines: {fail_4:?}");
    for (name, cls, err) in &errors {
        println!("{name}\t{cls}\t{err}");
    }
}
