//! Format sniffing for `discover:` — turning a directory tree into catalog tables.
//!
//! The rules mirror how each format actually announces itself on disk, so a directory written
//! by Spark, Databricks, or Oxidant itself is recognized without being told what it is:
//!
//! | Signal | Format |
//! |---|---|
//! | a `_delta_log/` child | Delta |
//! | a `metadata/` child, or a `*.metadata.json` | Iceberg |
//! | `*.parquet` files | Parquet |
//! | `*.csv` / `*.tsv` files | CSV |
//! | `*.json` / `*.ndjson` / `*.jsonl` files | JSON |
//!
//! Delta and Iceberg are checked first and in that order. A Delta table with Iceberg metadata
//! published over it (Oxidant's own `icebergCompat` output) has **both** markers, and it must
//! register as Delta: the Delta log is the authoritative, always-current file list, while the
//! published Iceberg tree trails it by up to `checkpointInterval` commits. Sniffing it as
//! Iceberg would silently serve stale data.
//!
//! Two directory layouts are recognized, because both are common and the committed
//! `sample-data/` tree uses each of them:
//!
//! - **table-per-subdirectory** — `bronze/orders/_delta_log/…`, the layout every lakehouse
//!   table uses;
//! - **table-per-file** — `parquet/tpch_nation.parquet`, where each file at the root of the
//!   scanned directory is its own table, named after the file stem.

use oxidant_catalog::TableFormat;

/// One directory entry the sniffer is shown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirEntry {
    /// Final path segment.
    pub name: String,
    /// Whether it is a directory.
    pub is_dir: bool,
}

impl DirEntry {
    pub fn dir(name: &str) -> Self {
        Self {
            name: name.to_string(),
            is_dir: true,
        }
    }

    pub fn file(name: &str) -> Self {
        Self {
            name: name.to_string(),
            is_dir: false,
        }
    }
}

/// Infer a table's format from what its directory contains, or `None` if nothing identifies it.
///
/// Pure so the precedence rules are testable without a filesystem.
/// The lowercased extension of a file name, if it has one.
pub(crate) fn extension_of(name: &str) -> Option<String> {
    let (_, extension) = name.rsplit_once('.')?;
    (!extension.is_empty() && extension.chars().all(|c| c.is_ascii_alphanumeric()))
        .then(|| extension.to_ascii_lowercase())
}

pub(crate) fn sniff_format(entries: &[DirEntry]) -> Option<TableFormat> {
    let has_dir = |name: &str| {
        entries
            .iter()
            .any(|e| e.is_dir && e.name.eq_ignore_ascii_case(name))
    };
    // Delta before Iceberg: a UniForm table carries both trees, and the Delta log is the one
    // that is always current.
    if has_dir("_delta_log") {
        return Some(TableFormat::Delta);
    }
    if has_dir("metadata")
        || entries
            .iter()
            .any(|e| !e.is_dir && e.name.ends_with(".metadata.json"))
    {
        return Some(TableFormat::Iceberg);
    }
    let has_ext = |exts: &[&str]| {
        entries.iter().any(|e| {
            !e.is_dir
                && exts.iter().any(|ext| {
                    e.name.len() > ext.len()
                        && e.name[e.name.len() - ext.len()..].eq_ignore_ascii_case(ext)
                })
        })
    };
    if has_ext(&[".parquet", ".parq"]) {
        return Some(TableFormat::Parquet);
    }
    if has_ext(&[".csv", ".tsv"]) {
        return Some(TableFormat::Csv);
    }
    if has_ext(&[".json", ".ndjson", ".jsonl"]) {
        return Some(TableFormat::Json);
    }
    None
}

/// Infer a table's format from a bare data file's name, for the table-per-file layout.
pub(crate) fn sniff_file_format(name: &str) -> Option<TableFormat> {
    let lower = name.to_ascii_lowercase();
    let (_, ext) = lower.rsplit_once('.')?;
    match ext {
        "parquet" | "parq" => Some(TableFormat::Parquet),
        "csv" | "tsv" => Some(TableFormat::Csv),
        "json" | "ndjson" | "jsonl" => Some(TableFormat::Json),
        _ => None,
    }
}

/// The table name for a bare data file: its stem, with the extension removed.
pub(crate) fn file_stem(name: &str) -> &str {
    name.rsplit_once('.').map_or(name, |(stem, _)| stem)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_data_files_are_tables_named_after_their_stem() {
        // The `sample-data/parquet/tpch_nation.parquet` layout.
        assert_eq!(
            sniff_file_format("tpch_nation.parquet"),
            Some(TableFormat::Parquet)
        );
        assert_eq!(file_stem("tpch_nation.parquet"), "tpch_nation");
        assert_eq!(sniff_file_format("data.CSV"), Some(TableFormat::Csv));
        assert_eq!(sniff_file_format("events.ndjson"), Some(TableFormat::Json));
        // Not data — must not become a table.
        assert_eq!(sniff_file_format("README.md"), None);
        assert_eq!(sniff_file_format("no-extension"), None);
    }

    #[test]
    fn delta_is_recognized_by_its_log() {
        let entries = [
            DirEntry::dir("_delta_log"),
            DirEntry::file("part-0.parquet"),
        ];
        assert_eq!(sniff_format(&entries), Some(TableFormat::Delta));
    }

    #[test]
    fn iceberg_is_recognized_by_metadata() {
        assert_eq!(
            sniff_format(&[DirEntry::dir("metadata"), DirEntry::file("part-0.parquet")]),
            Some(TableFormat::Iceberg)
        );
        assert_eq!(
            sniff_format(&[DirEntry::file("v1.metadata.json")]),
            Some(TableFormat::Iceberg)
        );
    }

    #[test]
    fn a_uniform_table_carrying_both_trees_registers_as_delta() {
        // This is Oxidant's own `icebergCompat` output. Sniffing it as Iceberg would serve a
        // snapshot that trails the Delta log by up to `checkpointInterval` commits — stale
        // data, with nothing to indicate it.
        let entries = [
            DirEntry::dir("_delta_log"),
            DirEntry::dir("metadata"),
            DirEntry::file("part-0.parquet"),
        ];
        assert_eq!(sniff_format(&entries), Some(TableFormat::Delta));
    }

    #[test]
    fn plain_file_formats_fall_through_to_their_extension() {
        assert_eq!(
            sniff_format(&[DirEntry::file("part-0.parquet")]),
            Some(TableFormat::Parquet)
        );
        assert_eq!(
            sniff_format(&[DirEntry::file("data.csv")]),
            Some(TableFormat::Csv)
        );
        assert_eq!(
            sniff_format(&[DirEntry::file("events.ndjson")]),
            Some(TableFormat::Json)
        );
    }

    #[test]
    fn extensions_match_case_insensitively() {
        assert_eq!(
            sniff_format(&[DirEntry::file("PART-0.PARQUET")]),
            Some(TableFormat::Parquet)
        );
    }

    #[test]
    fn an_unidentifiable_directory_is_skipped_rather_than_guessed() {
        // Registering a table Oxidant cannot read would turn a harmless stray directory into
        // a query-time error, so discovery skips it instead.
        assert_eq!(sniff_format(&[DirEntry::file("README.md")]), None);
        assert_eq!(sniff_format(&[]), None);
    }

    #[test]
    fn a_directory_named_like_a_data_file_does_not_count() {
        // `is_dir` is checked: a directory called `x.parquet` is not a Parquet file.
        assert_eq!(sniff_format(&[DirEntry::dir("x.parquet")]), None);
    }
}
