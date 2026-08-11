//! TPC-DS data via official TPC `dsdgen` (not DuckDB).
//!
//! Emits Snappy Parquet under `dir`. Official `dsdgen` requires an integer scale factor ≥ 1
//! (SCALE is GB). Idempotent when `store_sales.parquet` exists and `scale_factor.txt` matches.
//!
//! Column names come from [`TPCDS_COLUMNS`] (`bench/tpc/tpcds_columns.tsv`) — the public SQL
//! schema matching official `.dat` column order.

use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::Path;
use std::sync::OnceLock;

use datafusion::prelude::{CsvReadOptions, SessionContext};

use crate::tpc_kits;

/// Vendored public column lists (table → ordered columns), matching official `dsdgen` `.dat` order.
const TPCDS_COLUMNS: &str = include_str!("../../../bench/tpc/tpcds_columns.tsv");

/// The 24 TPC-DS tables (official kit).
pub const TABLES: [&str; 24] = [
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
];

const SENTINEL: &str = "store_sales.parquet";
pub(crate) const SF_MARKER: &str = "scale_factor.txt";
const RAW_DIR: &str = ".dsdgen-raw";

fn column_map() -> &'static HashMap<String, Vec<String>> {
    static MAP: OnceLock<HashMap<String, Vec<String>>> = OnceLock::new();
    MAP.get_or_init(|| {
        let mut m = HashMap::new();
        for line in TPCDS_COLUMNS.lines() {
            let line = line.trim().trim_matches('"');
            if line.is_empty() {
                continue;
            }
            let (table, cols) = line
                .split_once('\t')
                .unwrap_or_else(|| panic!("bad tpcds_columns.tsv line: {line}"));
            m.insert(
                table.to_string(),
                cols.split(',').map(|s| s.trim().to_string()).collect(),
            );
        }
        m
    })
}

/// Locate a `duckdb` binary (optional **oracle only** — not used for generation).
pub fn duckdb_path() -> Option<String> {
    for cand in [
        "duckdb",
        "/opt/homebrew/opt/duckdb/bin/duckdb",
        "/usr/local/bin/duckdb",
    ] {
        if std::process::Command::new(cand)
            .arg("--version")
            .output()
            .is_ok()
        {
            return Some(cand.to_string());
        }
    }
    None
}

/// Escape a filesystem path for embedding in a single-quoted DuckDB string literal.
pub(crate) fn duckdb_quote_path(path: &Path) -> io::Result<String> {
    let s = path
        .to_str()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "non-UTF8 data path"))?;
    Ok(s.replace('\'', "''"))
}

/// Generate scale-factor `sf` TPC-DS data as Parquet under `dir`.
pub fn generate(sf: f64, dir: &Path) -> io::Result<()> {
    let sf_int = validate_sf(sf)?;
    fs::create_dir_all(dir)?;
    let sentinel = dir.join(SENTINEL);
    let marker = dir.join(SF_MARKER);
    if sentinel.exists() {
        if sf_marker_matches(&marker, sf)? {
            return Ok(());
        }
        clear_harness_artifacts(dir)?;
    }

    let raw = dir.join(RAW_DIR);
    if raw.exists() {
        fs::remove_dir_all(&raw)?;
    }
    fs::create_dir_all(&raw)?;
    eprintln!(
        "[tpcds-data] official dsdgen SCALE={sf_int} → {} (then Parquet)",
        raw.display()
    );
    tpc_kits::run_dsdgen(sf_int, &raw)?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("tokio rt: {e}")))?;
    rt.block_on(convert_dat_to_parquet(&raw, dir))?;

    let _ = fs::remove_dir_all(&raw);
    let mut f = fs::File::create(&marker)?;
    writeln!(f, "{sf:.10}")?;
    Ok(())
}

fn validate_sf(sf: f64) -> io::Result<u32> {
    if sf < 1.0 || (sf - sf.round()).abs() > 1e-9 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "official TPC-DS dsdgen requires integer SCALE ≥ 1 (got {sf}); use --sf 1 (or 10/100/…)"
            ),
        ));
    }
    Ok(sf.round() as u32)
}

async fn convert_dat_to_parquet(raw: &Path, out: &Path) -> io::Result<()> {
    let cols_map = column_map();
    let ctx = SessionContext::new();
    for t in TABLES {
        let src = raw.join(format!("{t}.dat"));
        if !src.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("dsdgen did not produce {}", src.display()),
            ));
        }
        let headers = cols_map.get(t).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("no column list for `{t}` in tpcds_columns.tsv"),
            )
        })?;
        let header_refs: Vec<&str> = headers.iter().map(String::as_str).collect();
        let staged = raw.join(format!("{t}.csv"));
        tpc_kits::pipe_tbl_to_csv(&src, &staged, &header_refs)?;

        let path_str = staged
            .to_str()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "non-UTF8 path"))?;
        // Headered CSV + type inference (matches prior DuckDB parquet usability for SQL).
        let opts = CsvReadOptions::new().has_header(true);
        ctx.register_csv(t, path_str, opts)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("register {t}: {e}")))?;
        let dest = out.join(format!("{t}.parquet"));
        let dest_str = dest
            .to_str()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "non-UTF8 path"))?
            .replace('\'', "''");
        ctx.sql(&format!(
            "COPY (SELECT * FROM \"{t}\") TO '{dest_str}' (FORMAT PARQUET)"
        ))
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("copy plan {t}: {e}")))?
        .collect()
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("copy exec {t}: {e}")))?;
        let _ = fs::remove_file(&staged);
    }
    Ok(())
}

fn sf_marker_matches(marker: &Path, sf: f64) -> io::Result<bool> {
    if !marker.exists() {
        return Ok(false);
    }
    let prev = fs::read_to_string(marker)?;
    Ok(prev
        .trim()
        .parse::<f64>()
        .ok()
        .is_some_and(|p| (p - sf).abs() < 1e-9))
}

fn clear_harness_artifacts(dir: &Path) -> io::Result<()> {
    for t in TABLES {
        let p = dir.join(format!("{t}.parquet"));
        if p.exists() {
            fs::remove_file(&p)?;
        }
    }
    for name in [SF_MARKER, "schema.sql", "load.sql"] {
        let p = dir.join(name);
        if p.exists() {
            fs::remove_file(&p)?;
        }
    }
    let raw = dir.join(RAW_DIR);
    if raw.exists() {
        fs::remove_dir_all(&raw)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn tmp_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("oxidant-tpcds-test-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn column_map_has_all_tables() {
        let m = column_map();
        for t in TABLES {
            assert!(m.contains_key(t), "missing {t}");
            assert!(!m[t].is_empty());
        }
        assert_eq!(m["store_sales"].len(), 23);
    }

    #[test]
    fn duckdb_quote_path_escapes_single_quotes() {
        let p = Path::new("/tmp/oxidant's-data");
        assert_eq!(duckdb_quote_path(p).unwrap(), "/tmp/oxidant''s-data");
    }

    #[test]
    fn validate_sf_rejects_fractional() {
        assert!(validate_sf(0.01).is_err());
        assert_eq!(validate_sf(1.0).unwrap(), 1);
    }

    #[test]
    fn clear_harness_artifacts_preserves_unrelated_files() {
        let dir = tmp_dir("wipe");
        fs::write(dir.join("store_sales.parquet"), b"ss").unwrap();
        fs::write(dir.join(SF_MARKER), b"1\n").unwrap();
        fs::write(dir.join("keep_me.txt"), b"important").unwrap();
        clear_harness_artifacts(&dir).unwrap();
        assert!(!dir.join("store_sales.parquet").exists());
        assert_eq!(
            fs::read_to_string(dir.join("keep_me.txt")).unwrap(),
            "important"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
