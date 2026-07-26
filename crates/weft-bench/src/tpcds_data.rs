//! TPC-DS data generation via the DuckDB `tpcds` extension (`dsdgen`) exported as Parquet.
//!
//! Parquet (not CSV) is required so the same harness can target SF100/500/1000 on larger hardware
//! without materializing everything as text. Generation requires a `duckdb` CLI on PATH (or a
//! common install location). Idempotent: skipped when the sentinel + recorded scale factor match.
//!
//! First-time `INSTALL tpcds` needs network egress to DuckDB's extension repository; subsequent
//! runs use the local extension cache.

use std::fs;
use std::io::{self, Write};
use std::path::Path;
use std::process::Command;

/// The 24 TPC-DS tables (DuckDB `dsdgen` / official kit).
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
const EXPORT_DIR: &str = ".export";

/// Locate a `duckdb` binary: PATH, else common install locations.
pub fn duckdb_path() -> Option<String> {
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

/// Escape a filesystem path for embedding in a single-quoted DuckDB string literal.
pub(crate) fn duckdb_quote_path(path: &Path) -> io::Result<String> {
    let s = path
        .to_str()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "non-UTF8 data path"))?;
    Ok(s.replace('\'', "''"))
}

/// Generate scale-factor `sf` TPC-DS data as Parquet under `dir`. Idempotent when
/// `store_sales.parquet` exists and `scale_factor.txt` matches `sf`.
///
/// On SF mismatch, only known harness artifacts are removed (table Parquets, SF marker,
/// DuckDB export leftovers) — never unrelated files in `--data`.
pub fn generate(sf: f64, dir: &Path) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    let sentinel = dir.join(SENTINEL);
    let marker = dir.join(SF_MARKER);
    if sentinel.exists() {
        if sf_marker_matches(&marker, sf)? {
            return Ok(());
        }
        clear_harness_artifacts(dir)?;
    }

    let duckdb = duckdb_path().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "duckdb CLI not found on PATH (required for TPC-DS data generation via dsdgen)",
        )
    })?;

    let stage = dir.join(EXPORT_DIR);
    if stage.exists() {
        fs::remove_dir_all(&stage)?;
    }
    fs::create_dir_all(&stage)?;

    let stage_lit = duckdb_quote_path(&stage)?;
    // INSTALL needs network the first time; LOAD uses the local cache afterward.
    let script = format!(
        "INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf = {sf}); EXPORT DATABASE '{stage_lit}' (FORMAT PARQUET);"
    );
    let out = Command::new(&duckdb)
        .args(["-c", &script])
        .output()
        .map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("failed to spawn duckdb: {e}"))
        })?;
    if !out.status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "duckdb dsdgen/export failed (INSTALL tpcds needs network on first use): {}",
                String::from_utf8_lossy(&out.stderr)
            ),
        ));
    }

    for t in TABLES {
        let src = stage.join(format!("{t}.parquet"));
        if !src.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("expected {} after EXPORT", src.display()),
            ));
        }
        fs::rename(&src, dir.join(format!("{t}.parquet")))?;
    }
    let _ = fs::remove_dir_all(&stage);

    let mut f = fs::File::create(&marker)?;
    // Fixed decimal so marker round-trips with CLI-parsed f64 (avoids 0.01 vs 0.010000000000000002).
    writeln!(f, "{sf:.10}")?;
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

/// Remove only harness-owned files so a wrong `--sf` cannot wipe an unrelated `--data` tree.
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
    let export = dir.join(EXPORT_DIR);
    if export.exists() {
        fs::remove_dir_all(&export)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn tmp_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("weft-tpcds-test-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn duckdb_quote_path_escapes_single_quotes() {
        let p = Path::new("/tmp/weft's-data");
        assert_eq!(duckdb_quote_path(p).unwrap(), "/tmp/weft''s-data");
    }

    #[test]
    fn clear_harness_artifacts_preserves_unrelated_files() {
        let dir = tmp_dir("wipe");
        fs::write(dir.join("store_sales.parquet"), b"ss").unwrap();
        fs::write(dir.join("customer.parquet"), b"c").unwrap();
        fs::write(dir.join(SF_MARKER), b"0.01\n").unwrap();
        fs::write(dir.join("keep_me.txt"), b"important").unwrap();
        fs::create_dir_all(dir.join("keep_subdir")).unwrap();
        fs::write(dir.join("keep_subdir/x"), b"x").unwrap();

        clear_harness_artifacts(&dir).unwrap();

        assert!(!dir.join("store_sales.parquet").exists());
        assert!(!dir.join("customer.parquet").exists());
        assert!(!dir.join(SF_MARKER).exists());
        assert_eq!(
            fs::read_to_string(dir.join("keep_me.txt")).unwrap(),
            "important"
        );
        assert!(dir.join("keep_subdir/x").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn sf_marker_matches_uses_epsilon() {
        let dir = tmp_dir("sf");
        let marker = dir.join(SF_MARKER);
        fs::write(&marker, "0.0100000000\n").unwrap();
        assert!(sf_marker_matches(&marker, 0.01).unwrap());
        assert!(!sf_marker_matches(&marker, 0.02).unwrap());
        let _ = fs::remove_dir_all(&dir);
    }
}
