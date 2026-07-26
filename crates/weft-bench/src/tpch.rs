//! TPC-H harness on **real** generated data: [`tpch_data`] writes scale-factor CSVs, weft
//! (DataFusion) registers and runs the queries with ClickBench-style hot timing, and — when a
//! `duckdb` CLI is found — every result is cross-checked against DuckDB over the same data (an
//! independent oracle). Runs the full official **TPC-H Q1–Q22** (from `bench/tpch/queries/`).
//!
//! For published SF100 timings use [`run_bench`]: Parquet from DuckDB's pre-built
//! `tpch-sf100.db`, JSON output for the site, optional DuckDB baseline (no row-level oracle).

use std::path::Path;
use std::process::Command;
use std::time::Instant;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::prelude::CsvReadOptions;
use weft_loom::Engine;

use crate::suite::{self, Query};
use crate::tpch_data;

/// The 22 official TPC-H queries, loaded from `bench/tpch/queries/q{N}.sql` at compile time. The
/// trailing `;` is stripped before execution (DataFusion runs a single statement). Standard SQL —
/// `date '…'` literals, `INTERVAL` day/month/year arithmetic (incl. ANSI `day (3)` precision on
/// Q1), EXISTS/correlated subqueries, CTEs.
pub(crate) fn queries() -> Vec<(&'static str, &'static str)> {
    macro_rules! q {
        ($n:literal) => {
            (
                concat!("Q", $n),
                include_str!(concat!("../../../bench/tpch/queries/q", $n, ".sql")),
            )
        };
    }
    vec![
        q!("1"),
        q!("2"),
        q!("3"),
        q!("4"),
        q!("5"),
        q!("6"),
        q!("7"),
        q!("8"),
        q!("9"),
        q!("10"),
        q!("11"),
        q!("12"),
        q!("13"),
        q!("14"),
        q!("15"),
        q!("16"),
        q!("17"),
        q!("18"),
        q!("19"),
        q!("20"),
        q!("21"),
        q!("22"),
    ]
}

pub struct BenchOpts<'a> {
    pub sf: f64,
    pub data: &'a Path,
    pub duckdb_db: Option<&'a Path>,
    pub out_json: &'a Path,
    pub machine: &'a str,
    pub run_date: &'a str,
    pub with_duckdb: bool,
}

/// Published-benchmark path: Parquet tables + JSON results (+ optional DuckDB baseline).
pub async fn run_bench(opts: BenchOpts<'_>) {
    eprintln!(
        "[tpch-bench] SF{} data={} → {}",
        opts.sf,
        opts.data.display(),
        opts.out_json.display()
    );

    let engine = Engine::new();
    for t in tpch_data::TABLES {
        let path = opts.data.join(format!("{t}.parquet"));
        if path.exists() {
            engine
                .register_parquet(t, path.to_str().unwrap())
                .await
                .unwrap_or_else(|e| panic!("register {t}: {e}"));
            continue;
        }
        let alt = opts.data.join(t);
        if alt.is_dir() {
            engine
                .register_parquet(t, alt.to_str().unwrap())
                .await
                .unwrap_or_else(|e| panic!("register {t}: {e}"));
            continue;
        }
        panic!(
            "missing TPC-H table `{t}` under {} — run bench/tpch/prepare.sh first",
            opts.data.display()
        );
    }

    let qs: Vec<Query<'_>> = queries()
        .iter()
        .map(|(n, s)| Query { name: n, sql: s })
        .collect();

    eprintln!("[tpch-bench] running Weft ({} queries × 3 tries) …", qs.len());
    let weft = suite::run_weft(&engine, &qs).await;

    let mut engines = vec![weft];
    if opts.with_duckdb {
        if let Some(duck) = suite::duckdb_path() {
            let duck_data = opts.duckdb_db.unwrap_or(opts.data);
            eprintln!(
                "[tpch-bench] running DuckDB baseline on {} …",
                duck_data.display()
            );
            engines.push(suite::run_duckdb(&duck, duck_data, &qs));
        } else {
            eprintln!("[tpch-bench] DuckDB not on PATH — skipping baseline");
        }
    }

    let dataset = format!("TPC-H SF{} (DuckDB dbgen / blobs.duckdb.org)", opts.sf);
    suite::write_site_json(
        opts.out_json,
        &dataset,
        opts.machine,
        opts.run_date,
        "engine-direct Parquet, 3 tries/query, hot = min(try2, try3)",
        qs.len(),
        &engines,
    )
    .expect("write tpch results json");

    let failed = engines[0].failures;
    let hot = engines[0].total().unwrap_or(0.0);
    eprintln!(
        "\n=== TPC-H sf{}: weft hot total {hot:.4}s, {failed} failure(s) ===",
        opts.sf
    );
}

/// Generate data, register tables, run the queries (hot timing), and cross-check vs DuckDB.
pub async fn run(sf: f64, dir: &Path) {
    eprintln!(
        "[tpch] generating scale factor {sf} data into {} …",
        dir.display()
    );
    if let Err(e) = tpch_data::generate(sf, dir) {
        eprintln!("[tpch] data generation failed: {e}");
        std::process::exit(1);
    }

    let engine = Engine::new();
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

    let oracle = duckdb_path();
    match &oracle {
        Some(p) => eprintln!("[tpch] oracle: DuckDB at {p}\n"),
        None => {
            eprintln!("[tpch] oracle: DuckDB not found on PATH — running without cross-check\n")
        }
    }

    let mut failed = 0usize;
    let mut hot_total = 0.0f64;
    for (name, raw) in queries() {
        let sql = raw.trim().trim_end_matches(';').trim();
        // Three tries; hot = min of try 2 & 3 (ClickBench contract).
        let mut times = Vec::new();
        let mut result = Vec::new();
        for _ in 0..3 {
            let t = Instant::now();
            match engine.sql(sql).await {
                Ok(b) => {
                    times.push(t.elapsed().as_secs_f64());
                    result = b;
                }
                Err(e) => {
                    failed += 1;
                    eprintln!(
                        "{name:<4} FAIL  {}",
                        e.to_string().lines().next().unwrap_or("")
                    );
                    times.clear();
                    break;
                }
            }
        }
        if times.is_empty() {
            continue;
        }
        let hot = times[1].min(times[2]);
        hot_total += hot;
        let rows: usize = result.iter().map(|b| b.num_rows()).sum();

        // Oracle cross-check.
        let verdict = match &oracle {
            None => "(no oracle)".to_string(),
            Some(p) => match duckdb_result(p, dir, sql) {
                None => "oracle-err".to_string(),
                Some(expected) => {
                    let got = normalize_batches(&result);
                    let want = normalize_text(&expected);
                    if got == want {
                        "ok".to_string()
                    } else {
                        failed += 1;
                        if std::env::var("WEFT_TPCH_DEBUG").is_ok() {
                            for w in &got {
                                if !want.contains(w) {
                                    eprintln!("  only-weft:   {w:?}");
                                }
                            }
                            for w in &want {
                                if !got.contains(w) {
                                    eprintln!("  only-duckdb: {w:?}");
                                }
                            }
                        }
                        "MISMATCH".to_string()
                    }
                }
            },
        };
        eprintln!("{name:<4} {hot:>7.4}s  {rows:>6} rows  vs duckdb: {verdict}");
    }

    eprintln!("\n=== TPC-H sf{sf}: hot total {hot_total:.4}s, {failed} failure(s) ===");
    if failed > 0 {
        std::process::exit(1);
    }
}

/// Locate a `duckdb` binary: PATH, else the common Homebrew keg location.
fn duckdb_path() -> Option<String> {
    for cand in [
        "duckdb",
        "/opt/homebrew/opt/duckdb/bin/duckdb",
        "/usr/local/bin/duckdb",
    ] {
        if Command::new(cand).arg("--version").output().is_ok() {
            return Some(cand.to_string());
        }
    }
    None
}

/// Run `sql` in DuckDB over the same CSV data and return its CSV output (no header), or `None` on
/// failure. DuckDB reads each table directly from its CSV via a view.
fn duckdb_result(duckdb: &str, dir: &Path, sql: &str) -> Option<String> {
    let mut script = String::new();
    for t in tpch_data::TABLES {
        let path = dir.join(format!("{t}.csv"));
        script.push_str(&format!(
            "CREATE VIEW {t} AS SELECT * FROM read_csv_auto('{}', header=true);\n",
            path.display()
        ));
    }
    script.push_str(sql);
    script.push(';');
    let out = Command::new(duckdb)
        .args(["-csv", "-noheader", "-c", &script])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Normalize weft result batches to sorted rows of cells (numbers rounded) for oracle comparison.
pub(crate) fn normalize_batches(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    // Render NULL as the literal `NULL` to match DuckDB's `-csv` output (weft's default is empty).
    let opts = FormatOptions::default().with_null("NULL");
    let mut rows = Vec::new();
    for b in batches {
        let fmts: Vec<_> = b
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c, &opts).unwrap())
            .collect();
        for r in 0..b.num_rows() {
            rows.push(
                fmts.iter()
                    .map(|f| round_cell(&f.value(r).to_string()))
                    .collect(),
            );
        }
    }
    rows.sort();
    rows
}

/// Normalize DuckDB CSV text to sorted rows of cells (quote-aware split, numbers rounded).
fn normalize_text(text: &str) -> Vec<Vec<String>> {
    let mut rows: Vec<Vec<String>> = text
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| parse_csv_line(l).iter().map(|c| round_cell(c)).collect())
        .collect();
    rows.sort();
    rows
}

/// Split one CSV line into cells, honoring `"`-quoted fields (with `""` escapes) so a comma inside a
/// quoted string field isn't treated as a separator.
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut cells = Vec::new();
    let mut cur = String::new();
    let mut chars = line.chars().peekable();
    let mut in_quotes = false;
    while let Some(c) = chars.next() {
        match c {
            '"' if in_quotes && chars.peek() == Some(&'"') => {
                cur.push('"');
                chars.next();
            }
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                cells.push(std::mem::take(&mut cur));
            }
            _ => cur.push(c),
        }
    }
    cells.push(cur);
    cells
}

/// Normalize one cell: any numeric value renders as 4-significant-figure scientific (so
/// `380456.0` == `380456` and an exact `1462293.00` == DuckDB's `1462292.97`), strings pass through
/// verbatim. Uniform scientific (no integer special-case) avoids an integer-vs-scientific formatting
/// split on values that are integral in one engine but not the other. 4 sig figs (0.01% relative)
/// absorbs benign decimal `avg`/`sum` rounding differences (DataFusion truncates where DuckDB rounds)
/// — far tighter than TPC-H's own ±0.01 absolute tolerance — while still catching structural errors.
/// (At the small scale factors used for correctness, all keys are ≤4 digits, so exact.)
fn round_cell(s: &str) -> String {
    match s.trim().parse::<f64>() {
        Ok(v) => format!("{v:.3e}"),
        Err(_) => s.to_string(),
    }
}
