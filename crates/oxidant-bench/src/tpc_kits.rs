//! Locate and invoke official TPC.org toolkits (`dbgen` / `dsdgen` / `qgen` / `dsqgen`).
//!
//! Resolution order for the kits root:
//! 1. `OXIDANT_TPC_KITS`
//! 2. `$DATA_ROOT/kits` when `OXIDANT_TPC_DATA_ROOT` / `DATA_ROOT` is set
//! 3. `$HOME/.cache/oxidant/tpc-kits`
//! 4. `/tmp/oxidant-tpc-kits/kits` (local smoke convenience)
//!
//! Build with `./bench/tpc/fetch-kits.sh && ./bench/tpc/build-kits.sh` (or the CI step that
//! wraps them). This module never downloads kits itself.

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Prefer an explicit override, then common cache locations.
pub fn kits_dir() -> PathBuf {
    if let Ok(p) = std::env::var("OXIDANT_TPC_KITS") {
        return PathBuf::from(p);
    }
    for key in ["OXIDANT_TPC_DATA_ROOT", "DATA_ROOT"] {
        if let Ok(root) = std::env::var(key) {
            return PathBuf::from(root).join("kits");
        }
    }
    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home).join(".cache/oxidant/tpc-kits");
    }
    PathBuf::from("/tmp/oxidant-tpc-kits/kits")
}

pub fn find_dbgen() -> io::Result<PathBuf> {
    let kits = kits_dir();
    let candidates = [
        kits.join("tpch-kit/dbgen/dbgen"),
        // Official TPC zip layout: TPC-H_Tools_v*/dbgen/dbgen
    ];
    for c in &candidates {
        if c.is_file() {
            return Ok(c.clone());
        }
    }
    if let Ok(rd) = std::fs::read_dir(kits.join("tpch-kit")) {
        for ent in rd.flatten() {
            let p = ent.path().join("dbgen/dbgen");
            if p.is_file() {
                return Ok(p);
            }
        }
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!(
            "TPC-H dbgen not found under {} — run: DATA_ROOT=… ./bench/tpc/fetch-kits.sh && ./bench/tpc/build-kits.sh (or set OXIDANT_TPC_KITS)",
            kits.display()
        ),
    ))
}

pub fn find_dsdgen() -> io::Result<PathBuf> {
    let p = kits_dir().join("tpcds-kit/tools/dsdgen");
    if p.is_file() {
        return Ok(p);
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!(
            "TPC-DS dsdgen not found at {} — run: DATA_ROOT=… ./bench/tpc/fetch-kits.sh && ./bench/tpc/build-kits.sh (or set OXIDANT_TPC_KITS)",
            p.display()
        ),
    ))
}

pub fn find_tpcds_idx() -> io::Result<PathBuf> {
    let p = kits_dir().join("tpcds-kit/tools/tpcds.idx");
    if p.is_file() {
        Ok(p)
    } else {
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("tpcds.idx missing at {}", p.display()),
        ))
    }
}

/// Run `dbgen -s <sf> -f` with cwd = dbgen dir (needs `dists.dss`), move `*.tbl` into `raw_dir`.
pub fn run_dbgen(sf: f64, raw_dir: &Path) -> io::Result<()> {
    std::fs::create_dir_all(raw_dir)?;
    let dbgen = find_dbgen()?;
    let dbgen_dir = dbgen.parent().unwrap();
    let status = Command::new(&dbgen)
        .current_dir(dbgen_dir)
        .args(["-s", &format_sf(sf), "-f"])
        .status()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("spawn dbgen: {e}")))?;
    if !status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("dbgen failed with status {status}"),
        ));
    }
    for ent in std::fs::read_dir(dbgen_dir)? {
        let ent = ent?;
        let name = ent.file_name();
        let name = name.to_string_lossy();
        if name.ends_with(".tbl") {
            let dest = raw_dir.join(name.as_ref());
            std::fs::rename(ent.path(), &dest).or_else(|_| {
                std::fs::copy(ent.path(), &dest)?;
                std::fs::remove_file(ent.path())
            })?;
        }
    }
    Ok(())
}

/// Run official `dsdgen -SCALE <sf>`. `sf` must be an integer ≥ 1 (TPC-DS kit rule).
pub fn run_dsdgen(sf: u32, raw_dir: &Path) -> io::Result<()> {
    if sf < 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "official TPC-DS dsdgen requires integer SCALE ≥ 1 (no fractional SF)",
        ));
    }
    std::fs::create_dir_all(raw_dir)?;
    let dsdgen = find_dsdgen()?;
    let tools = dsdgen.parent().unwrap();
    let idx = find_tpcds_idx()?;
    let status = Command::new(&dsdgen)
        .current_dir(tools)
        .args([
            "-DIR",
            raw_dir
                .to_str()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "non-UTF8 raw dir"))?,
            "-SCALE",
            &sf.to_string(),
            "-FORCE",
            "-VERBOSE",
            "N",
            "-DISTRIBUTIONS",
            idx.to_str().unwrap(),
        ])
        .status()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("spawn dsdgen: {e}")))?;
    if !status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("dsdgen failed with status {status}"),
        ));
    }
    Ok(())
}

fn format_sf(sf: f64) -> String {
    // Avoid scientific notation; dbgen accepts 0.01.
    if (sf - sf.round()).abs() < 1e-12 {
        format!("{}", sf.round() as i64)
    } else {
        format!("{sf}")
    }
}

/// Convert a pipe-delimited TPC flat file (optional trailing `|`) into headered CSV.
/// Bytes are decoded as Latin-1 (TPC kits are not always UTF-8).
pub fn pipe_tbl_to_csv(src: &Path, dest: &Path, headers: &[&str]) -> io::Result<()> {
    use std::io::Read;
    let mut bytes = Vec::new();
    std::fs::File::open(src)?.read_to_end(&mut bytes)?;
    let text: String = bytes.iter().map(|&b| b as char).collect();
    let mut out = std::io::BufWriter::new(std::fs::File::create(dest)?);
    writeln!(out, "{}", headers.join(","))?;
    for line in text.lines() {
        let mut line = line.to_string();
        if line.ends_with('|') {
            line.pop();
        }
        if line.is_empty() {
            continue;
        }
        let cols: Vec<&str> = line.split('|').collect();
        if cols.len() != headers.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{}: expected {} columns, got {} in row",
                    src.display(),
                    headers.len(),
                    cols.len()
                ),
            ));
        }
        let mut first = true;
        for c in cols {
            if !first {
                write!(out, ",")?;
            }
            first = false;
            if needs_csv_quote(c) {
                write!(out, "\"{}\"", c.replace('"', "\"\""))?;
            } else {
                write!(out, "{c}")?;
            }
        }
        writeln!(out)?;
    }
    out.flush()
}

fn needs_csv_quote(s: &str) -> bool {
    s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_sf_preserves_fraction() {
        assert_eq!(format_sf(0.01), "0.01");
        assert_eq!(format_sf(100.0), "100");
    }
}
