//! Classification guard: baseline-pinned plan/decline matrix for all 99 TPC-DS
//! queries under two simulated SF100 auto-broadcast classifications — each run
//! once against bare MemTable scans and once against catalog-style wrapped
//! scans (`SubqueryAlias → passthrough Projection → TableScan` with a qualified
//! `glue.tpcds_sf100.*` scan name). Every table is wrapped, matching a real Glue
//! catalog, so a SQL-aliased table nests two `SubqueryAlias`es exactly as it does
//! on the cluster.
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
//! The bare-MemTable matrix missed Glue/Hive view expansion: real catalog scans
//! arrive wrapped, and TPC-DS q5's co-located LEFT JOIN admission previously
//! required a bare `TableScan`. The wrapped columns exercise that shape.
//!
//! The expected outcome per (query, classification, scan mode) — `ok(<stage
//! count>)` or `fail` — is pinned in `fixtures/classification_guard_baseline.tsv`.
//! The test FAILS when any cell's plan/decline status (or stage count) changes
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
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, TableReference};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::logical_plan::builder::LogicalTableSource;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use oxidant_execution::plan::plan_distributed_logical;
use oxidant_loom::Engine;

// SF100 byte order among the sales facts (see shard.rs KAN-161 comment):
// store_sales (~29 GB) > catalog_sales (~14.5 GB) > web_sales (~7.2 GB).
const SALES: [&str; 3] = ["store_sales", "catalog_sales", "web_sales"];

// Historical note: this used to wrap only the six fact tables, which is what exposed the q5
// bare-scan admission gap. It now wraps EVERY table, because a real Glue/Hive catalog expands
// all of them — and an *aliased* table then nests two `SubqueryAlias`es
// (`SubqueryAlias(d1) → SubqueryAlias(date_dim) → Projection → TableScan`), which facts-only
// wrapping never produced. Verified against the live catalog: real q64 declined on exactly that
// shape while the facts-only matrix read green, so KAN-162's first fix looked complete and was
// not. Wrapping everything reproduces the decline in CI.

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

/// Rewrite fact `TableScan`s into the Glue/Hive view-expansion shape:
/// `SubqueryAlias(bare) → passthrough Projection → TableScan(glue.tpcds_sf100.bare)`.
/// Planning-only — the qualified scan uses a `LogicalTableSource` so stage SQL is
/// never executed against it.
fn wrap_fact_scans_catalog_style(lp: LogicalPlan) -> LogicalPlan {
    lp.transform(|node| {
        let LogicalPlan::TableScan(scan) = &node else {
            return Ok(Transformed::no(node));
        };
        let bare = scan.table_name.table().to_string();
        // Already catalog-qualified (or previously wrapped): leave alone.
        if !matches!(scan.table_name, TableReference::Bare { .. }) {
            return Ok(Transformed::no(node));
        }
        let source = Arc::new(LogicalTableSource::new(Arc::clone(
            scan.projected_schema.inner(),
        )));
        let scan_plan = LogicalPlanBuilder::scan(
            TableReference::full("glue", "tpcds_sf100", bare.as_str()),
            source,
            None,
        )
        .expect("catalog-style fact scan")
        .build()
        .expect("catalog-style fact scan build");
        let exprs: Vec<Expr> = scan_plan
            .schema()
            .iter()
            .map(|(q, f)| Expr::Column(Column::new(q.cloned(), f.name())))
            .collect();
        let wrapped = LogicalPlanBuilder::from(scan_plan)
            .project(exprs)
            .expect("passthrough projection")
            .alias(bare.as_str())
            .expect("subquery alias")
            .build()
            .expect("wrapped fact build");
        Ok(Transformed::yes(wrapped))
    })
    .expect("wrap fact scans")
    .data
}

/// Mirror the driver's optimized-then-original split retry: plan the optimized
/// plan; on failure, if optimization changed the plan, retry the original.
/// When `wrap_facts` is set, both attempts see catalog-style wrapped fact scans.
async fn plan_stage_count(
    engine: &Engine,
    sql: &str,
    replicated: &[String],
    wrap_facts: bool,
) -> Result<usize, String> {
    let refs: Vec<&str> = replicated.iter().map(String::as_str).collect();
    let lp = engine
        .logical_plan(sql)
        .await
        .map_err(|e| format!("logical_plan: {}", first_line(&e.to_string())))?;
    let lp = if wrap_facts {
        wrap_fact_scans_catalog_style(lp)
    } else {
        lp
    };
    let optimized = engine
        .optimize_logical_plan(lp.clone())
        .unwrap_or_else(|_| lp.clone());
    let optimized = if wrap_facts {
        // Optimization can re-introduce bare scans from view folding; re-wrap.
        wrap_fact_scans_catalog_style(optimized)
    } else {
        optimized
    };
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

    // header + one row per statement:
    // `name<TAB>bcast_32gib<TAB>bcast_4gib<TAB>wrapped_32gib<TAB>wrapped_4gib`
    let mut rows: Vec<String> = Vec::new();
    let mut errors: Vec<(String, String, String)> = Vec::new();
    for (name, sql) in all_statements() {
        let repl_32 = replicated_32gib(&all, &sql);
        let repl_4 = replicated_4gib(&all);
        let bare_32 = plan_stage_count(&engine, &sql, &repl_32, false).await;
        let bare_4 = plan_stage_count(&engine, &sql, &repl_4, false).await;
        let wrap_32 = plan_stage_count(&engine, &sql, &repl_32, true).await;
        let wrap_4 = plan_stage_count(&engine, &sql, &repl_4, true).await;
        for (label, r) in [
            ("bcast_32gib", &bare_32),
            ("bcast_4gib", &bare_4),
            ("wrapped_32gib", &wrap_32),
            ("wrapped_4gib", &wrap_4),
        ] {
            if let Err(e) = r {
                errors.push((name.clone(), label.into(), e.clone()));
            }
        }
        rows.push(format!(
            "{name}\t{}\t{}\t{}\t{}",
            outcome_s(&bare_32),
            outcome_s(&bare_4),
            outcome_s(&wrap_32),
            outcome_s(&wrap_4)
        ));
    }

    let actual = format!(
        "# query\tbcast_32gib\tbcast_4gib\twrapped_32gib\twrapped_4gib\n{}\n",
        rows.join("\n")
    );

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
    let fail_w4: Vec<&str> = rows
        .iter()
        .filter_map(|r| {
            let mut c = r.split('\t');
            match (c.next(), c.nth(3)) {
                (Some(n), Some("fail")) => Some(n),
                _ => None,
            }
        })
        .collect();
    println!("bcast_32gib declines: {fail_32:?}");
    println!("bcast_4gib declines: {fail_4:?}");
    println!("wrapped_4gib declines: {fail_w4:?}");
    for (name, cls, err) in &errors {
        println!("{name}\t{cls}\t{err}");
    }
}
