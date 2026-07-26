//! TPC-DS harness on **real** generated Parquet: [`tpcds_data`] invokes DuckDB `dsdgen`, weft
//! registers the 24 tables and runs Q1–Q99 with ClickBench-style hot timing, then cross-checks
//! against DuckDB over the same files. CI gates a tiny SF (`0.01`) via a pass-set ratchet in
//! `bench/tpcds/baseline.json` so coverage can only hold or rise toward 99/99.
//!
//! DuckDB is both the data generator and the oracle (engineering harness — not an independent
//! ground truth). Set `WEFT_TPCDS_ALLOW_NO_ORACLE=1` only for execute-only smoke without DuckDB.

use std::collections::BTreeSet;
use std::path::Path;
use std::process::Command;
use std::time::Instant;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use serde_json::Value;
use weft_loom::Engine;

use crate::tpcds_data;

/// Baseline floor committed under `bench/tpcds/baseline.json`.
const BASELINE_JSON: &str = include_str!("../../../bench/tpcds/baseline.json");

/// Relative tolerance for non-integral numeric cells (covers Q66-style ratio FP drift ~0.03%
/// without collapsing distinct integer keys the way 3-sig-fig rounding does).
const FLOAT_REL_EPS: f64 = 1e-3;

/// The 99 official TPC-DS queries (DuckDB `tpcds_queries()` fixed substitution parameters),
/// loaded from `bench/tpcds/queries/q{N}.sql` at compile time.
pub(crate) fn queries() -> Vec<(&'static str, &'static str)> {
    macro_rules! q {
        ($n:literal) => {
            (
                concat!("Q", $n),
                include_str!(concat!("../../../bench/tpcds/queries/q", $n, ".sql")),
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
        q!("23"),
        q!("24"),
        q!("25"),
        q!("26"),
        q!("27"),
        q!("28"),
        q!("29"),
        q!("30"),
        q!("31"),
        q!("32"),
        q!("33"),
        q!("34"),
        q!("35"),
        q!("36"),
        q!("37"),
        q!("38"),
        q!("39"),
        q!("40"),
        q!("41"),
        q!("42"),
        q!("43"),
        q!("44"),
        q!("45"),
        q!("46"),
        q!("47"),
        q!("48"),
        q!("49"),
        q!("50"),
        q!("51"),
        q!("52"),
        q!("53"),
        q!("54"),
        q!("55"),
        q!("56"),
        q!("57"),
        q!("58"),
        q!("59"),
        q!("60"),
        q!("61"),
        q!("62"),
        q!("63"),
        q!("64"),
        q!("65"),
        q!("66"),
        q!("67"),
        q!("68"),
        q!("69"),
        q!("70"),
        q!("71"),
        q!("72"),
        q!("73"),
        q!("74"),
        q!("75"),
        q!("76"),
        q!("77"),
        q!("78"),
        q!("79"),
        q!("80"),
        q!("81"),
        q!("82"),
        q!("83"),
        q!("84"),
        q!("85"),
        q!("86"),
        q!("87"),
        q!("88"),
        q!("89"),
        q!("90"),
        q!("91"),
        q!("92"),
        q!("93"),
        q!("94"),
        q!("95"),
        q!("96"),
        q!("97"),
        q!("98"),
        q!("99"),
    ]
}

/// Generate data, register tables, run queries (hot timing), oracle-check, and enforce the ratchet.
pub async fn run(sf: f64, dir: &Path) {
    eprintln!(
        "[tpcds] generating scale factor {sf} data into {} …",
        dir.display()
    );
    if let Err(e) = tpcds_data::generate(sf, dir) {
        eprintln!("[tpcds] data generation failed: {e}");
        std::process::exit(1);
    }

    let engine = Engine::new();
    for t in tpcds_data::TABLES {
        let path = dir.join(format!("{t}.parquet"));
        let path_str = path.to_str().unwrap_or_else(|| {
            eprintln!("[tpcds] non-UTF8 path for table {t}: {}", path.display());
            std::process::exit(1);
        });
        engine
            .register_parquet(t, path_str)
            .await
            .unwrap_or_else(|e| panic!("register {t}: {e}"));
    }

    let allow_no_oracle = std::env::var("WEFT_TPCDS_ALLOW_NO_ORACLE").is_ok();
    let oracle = tpcds_data::duckdb_path();
    match &oracle {
        Some(p) => eprintln!("[tpcds] oracle: DuckDB at {p}\n"),
        None if allow_no_oracle => {
            eprintln!(
                "[tpcds] WARNING: DuckDB not found — WEFT_TPCDS_ALLOW_NO_ORACLE set; execute-only (no result check)\n"
            );
        }
        None => {
            eprintln!(
                "[tpcds] DuckDB not found on PATH — required for oracle cross-check \
                 (set WEFT_TPCDS_ALLOW_NO_ORACLE=1 to allow execute-only smoke)"
            );
            std::process::exit(1);
        }
    }

    let only = std::env::var("WEFT_TPCDS_ONLY").ok();
    let baseline = load_baseline();
    let mut passed: BTreeSet<String> = BTreeSet::new();
    let mut failed = 0usize;
    let mut hot_total = 0.0f64;

    for (name, raw) in queries() {
        if let Some(ref only) = only {
            if !name.eq_ignore_ascii_case(only) {
                continue;
            }
        }
        let sql = raw.trim().trim_end_matches(';').trim();
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
                    if std::env::var("WEFT_TPCDS_DEBUG").is_ok() {
                        eprintln!("  full: {e}");
                    }
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

        let verdict = match &oracle {
            None => {
                // Execute-only smoke (explicit opt-in): count as pass for local debugging only.
                passed.insert(name.to_string());
                "(no oracle)".to_string()
            }
            Some(p) => match duckdb_result(p, dir, sql) {
                None => {
                    failed += 1;
                    "oracle-err".to_string()
                }
                Some(expected) => {
                    let got = normalize_batches(&result);
                    let want = normalize_text(&expected);
                    if rows_equal(&got, &want) {
                        passed.insert(name.to_string());
                        "ok".to_string()
                    } else {
                        failed += 1;
                        if std::env::var("WEFT_TPCDS_DEBUG").is_ok() {
                            eprintln!(
                                "  mismatch: weft {} rows, duckdb {} rows",
                                got.len(),
                                want.len()
                            );
                            for w in got.iter().take(3) {
                                eprintln!("  sample-weft:   {w:?}");
                            }
                            for w in want.iter().take(3) {
                                eprintln!("  sample-duckdb: {w:?}");
                            }
                        }
                        "MISMATCH".to_string()
                    }
                }
            },
        };
        eprintln!("{name:<4} {hot:>7.4}s  {rows:>6} rows  vs duckdb: {verdict}");
    }

    eprintln!(
        "\n=== TPC-DS sf{sf}: hot total {hot_total:.4}s, {}/99 pass, {failed} failure(s) ===",
        passed.len()
    );

    // Any query failure fails the process (including WEFT_TPCDS_ONLY single-query debug).
    if failed > 0 {
        eprintln!("[tpcds] {failed} quer(ies) failed — exiting non-zero");
        std::process::exit(1);
    }

    // Ratchet: every query listed in the baseline must still pass; pass count must not drop.
    if only.is_none() {
        let missing: Vec<_> = baseline
            .iter()
            .filter(|q| !passed.contains(q.as_str()))
            .cloned()
            .collect();
        if !missing.is_empty() {
            eprintln!(
                "[tpcds] RATCHET REGRESSION: {} baseline quer(ies) no longer pass: {}",
                missing.len(),
                missing.join(", ")
            );
            std::process::exit(1);
        }
        if passed.len() > baseline.len() {
            let newly: Vec<_> = passed.difference(&baseline).cloned().collect();
            eprintln!(
                "[tpcds] ratchet gain: {} new pass(es) — re-baseline bench/tpcds/baseline.json: {}",
                newly.len(),
                newly.join(", ")
            );
        }
        eprintln!(
            "[tpcds] ratchet OK: {}/{} baseline held (now {}/99)",
            baseline.len(),
            baseline.len(),
            passed.len()
        );
        let list: Vec<&str> = passed.iter().map(String::as_str).collect();
        eprintln!(
            "[tpcds] passed_json={}",
            serde_json::to_string(&list).unwrap_or_default()
        );
    }
}

fn load_baseline() -> BTreeSet<String> {
    let v: Value = serde_json::from_str(BASELINE_JSON).expect("baseline.json parse");
    let arr = v
        .get("passed")
        .and_then(|p| p.as_array())
        .expect("baseline.json missing passed[]");
    arr.iter()
        .filter_map(|x| x.as_str().map(|s| s.to_string()))
        .collect()
}

/// Run `sql` in DuckDB over the same Parquet data and return CSV output (no header).
fn duckdb_result(duckdb: &str, dir: &Path, sql: &str) -> Option<String> {
    let mut script = String::new();
    for t in tpcds_data::TABLES {
        let path = dir.join(format!("{t}.parquet"));
        let lit = tpcds_data::duckdb_quote_path(&path).ok()?;
        script.push_str(&format!(
            "CREATE VIEW {t} AS SELECT * FROM read_parquet('{lit}');\n"
        ));
    }
    script.push_str(sql);
    script.push(';');
    let out = Command::new(duckdb)
        .args(["-csv", "-noheader", "-c", &script])
        .output()
        .ok()?;
    if !out.status.success() {
        if std::env::var("WEFT_TPCDS_DEBUG").is_ok() {
            eprintln!("  duckdb stderr: {}", String::from_utf8_lossy(&out.stderr));
        }
        return None;
    }
    Some(String::from_utf8_lossy(&out.stdout).into_owned())
}

fn normalize_batches(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let opts = FormatOptions::default().with_null("NULL");
    let mut rows = Vec::new();
    for b in batches {
        let fmts: Vec<_> = b
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c, &opts).unwrap())
            .collect();
        for r in 0..b.num_rows() {
            rows.push(fmts.iter().map(|f| f.value(r).to_string()).collect());
        }
    }
    rows
}

fn normalize_text(text: &str) -> Vec<Vec<String>> {
    text.lines()
        .filter(|l| !l.is_empty())
        .map(parse_csv_line)
        .collect()
}

/// Multiset equality: sort rows by a total order on parsed cells, then pairwise
/// [`cells_equal`] (exact ints / relative floats / string eq).
fn rows_equal(a: &[Vec<String>], b: &[Vec<String>]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut aa: Vec<Vec<NormCell>> = a
        .iter()
        .map(|r| r.iter().map(|c| parse_cell(c)).collect())
        .collect();
    let mut bb: Vec<Vec<NormCell>> = b
        .iter()
        .map(|r| r.iter().map(|c| parse_cell(c)).collect())
        .collect();
    aa.sort();
    bb.sort();
    aa.iter().zip(bb.iter()).all(|(ra, rb)| {
        ra.len() == rb.len() && ra.iter().zip(rb.iter()).all(|(x, y)| cells_equal(x, y))
    })
}

#[derive(Clone, Debug, PartialEq)]
enum NormCell {
    Text(String),
    Int(i64),
    Float(f64),
}

impl Eq for NormCell {}

impl PartialOrd for NormCell {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NormCell {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        use NormCell::*;
        match (self, other) {
            (Text(a), Text(b)) => a.cmp(b),
            (Int(a), Int(b)) => a.cmp(b),
            (Float(a), Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Int(a), Float(b)) => (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal),
            (Float(a), Int(b)) => a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal),
            (Text(_), _) => Ordering::Less,
            (_, Text(_)) => Ordering::Greater,
        }
    }
}

fn parse_cell(s: &str) -> NormCell {
    let t = s.trim();
    match t.parse::<f64>() {
        Ok(v) if v.is_finite() => {
            let r = v.round();
            // Treat near-integrals as ints so keys like 1001/1002 never collapse under float eps.
            if (v - r).abs() <= 1e-9 * v.abs().max(1.0) && r.abs() <= i64::MAX as f64 {
                NormCell::Int(r as i64)
            } else {
                NormCell::Float(v)
            }
        }
        _ => NormCell::Text(t.to_string()),
    }
}

fn cells_equal(a: &NormCell, b: &NormCell) -> bool {
    use NormCell::*;
    match (a, b) {
        (Text(x), Text(y)) => x == y,
        (Int(x), Int(y)) => x == y,
        (Float(x), Float(y)) => floats_close(*x, *y),
        (Int(x), Float(y)) | (Float(y), Int(x)) => floats_close(*x as f64, *y),
        _ => false,
    }
}

fn floats_close(x: f64, y: f64) -> bool {
    if x == y {
        return true;
    }
    let scale = x.abs().max(y.abs()).max(1e-12);
    (x - y).abs() <= FLOAT_REL_EPS * scale
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_csv_line_honors_quotes() {
        assert_eq!(parse_csv_line(r#"a,"b,c",d"#), vec!["a", "b,c", "d"]);
        assert_eq!(
            parse_csv_line(r#""say ""hi""",x"#),
            vec![r#"say "hi""#, "x"]
        );
    }

    #[test]
    fn integer_keys_are_exact() {
        let a = vec![vec!["1001".into(), "Midway".into()]];
        let b = vec![vec!["1002".into(), "Midway".into()]];
        assert!(!rows_equal(&a, &b), "distinct int keys must not match");
        assert!(rows_equal(&a, &a));
    }

    #[test]
    fn float_ratio_within_rel_eps_matches() {
        // Q66-style: 2.934e-3 vs 2.935e-3 ≈ 0.034% relative.
        let a = vec![vec!["2.934e-3".into()]];
        let b = vec![vec!["2.935e-3".into()]];
        assert!(rows_equal(&a, &b));
    }

    #[test]
    fn float_far_apart_mismatches() {
        let a = vec![vec!["1.0".into()]];
        let b = vec![vec!["1.01".into()]]; // 1% > 0.1%
        assert!(!rows_equal(&a, &b));
    }

    #[test]
    fn row_order_does_not_matter() {
        let a = vec![vec!["2".into()], vec!["1".into()]];
        let b = vec![vec!["1".into()], vec!["2".into()]];
        assert!(rows_equal(&a, &b));
    }

    #[test]
    fn parse_cell_near_integral_is_int() {
        assert_eq!(parse_cell("1001"), NormCell::Int(1001));
        assert_eq!(parse_cell("1001.0"), NormCell::Int(1001));
        assert!(matches!(parse_cell("2.934e-3"), NormCell::Float(_)));
    }
}
