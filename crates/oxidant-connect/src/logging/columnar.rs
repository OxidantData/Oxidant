//! Parquet-on-roll: the derived, compressed form of a rolled text log (§6).
//!
//! **Text is authoritative; Parquet is a derived form.** Conversion happens after the roll has
//! closed, fsynced and renamed the text file — never during — and it runs in this exact order:
//!
//! 1. write `oxidant-<period>[.N].parquet.tmp`, `fsync` it;
//! 2. `rename` to `.parquet`, `fsync` `logs/`;
//! 3. **read the footer back**;
//! 4. only then unlink the text file, and `fsync` `logs/` again.
//!
//! Parquet's footer-at-the-end means a half-written Parquet is not partially readable *at all*,
//! which is why the text file is never removed before step 3, and why a `.parquet.tmp` found at
//! boot is deleted and the conversion redone rather than trusted.
//!
//! **The cost, stated:** once converted, an operator can no longer `tail`/`grep` yesterday's log
//! with shell tools. That is a real loss and it is the price of ~10× compression plus the
//! predicate-pushdown browsing PR4 builds on `ts`/`level`/`target`.
//! `OXIDANT_LOG_PARQUET=off` keeps rolled files as plain text, subject to the same budget and
//! roughly 10× larger.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::{Compression, ZstdLevel};
use datafusion::parquet::file::properties::WriterProperties;
use oxidant_loom::arrow::array::{Array, ArrayRef, StringArray, TimestampMillisecondArray};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use oxidant_loom::arrow::record_batch::RecordBatch;

use super::line::{parse_line, ParsedLine, TS_FORMAT};
use crate::history::fs_util;

/// Rows per Parquet row group written by the converter.
const ROWS_PER_BATCH: usize = 8192;

/// `(ts, level, target, message, fields_json)` — §6's schema, verbatim.
///
/// `ts` is nullable because a line that carried no parseable timestamp (a `tracing` field value
/// with a newline in it, already split across two file lines by `writeln!`) is preserved whole
/// rather than dropped.
pub(crate) fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
        Field::new("level", DataType::Utf8, true),
        Field::new("target", DataType::Utf8, true),
        Field::new("message", DataType::Utf8, true),
        Field::new("fields_json", DataType::Utf8, true),
    ]))
}

fn batch(rows: &[ParsedLine]) -> Result<RecordBatch, String> {
    let ts: ArrayRef = Arc::new(
        rows.iter()
            .map(|r| r.ts_ms)
            .collect::<TimestampMillisecondArray>()
            .with_timezone("UTC"),
    );
    let col = |f: fn(&ParsedLine) -> Option<&str>| -> ArrayRef {
        Arc::new(rows.iter().map(f).collect::<StringArray>())
    };
    RecordBatch::try_new(
        schema(),
        vec![
            ts,
            col(|r| r.level.as_deref()),
            col(|r| r.target.as_deref()),
            col(|r| r.message.as_deref()),
            col(|r| r.fields_json.as_deref()),
        ],
    )
    .map_err(|e| format!("building a log batch: {e}"))
}

/// Convert `text` (a rolled `oxidant-<period>[.N].log`) to its `.parquet` sibling.
///
/// On success the text file is gone and the returned path exists. On failure **nothing is
/// removed**: the caller retries at the next sweep, and after the second failure leaves the
/// `.log` in place permanently — `?file=` still serves it, as text.
pub(crate) fn convert(text: &Path) -> Result<PathBuf, String> {
    let dir = text
        .parent()
        .ok_or_else(|| "a rolled log with no parent directory".to_string())?;
    let stem = text
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| "a rolled log with no stem".to_string())?;
    let target = dir.join(format!("{stem}.parquet"));
    let tmp = dir.join(format!("{stem}.parquet.tmp"));
    // One cleanup site, because there are a dozen ways to fail in here and every one of them
    // must leave the directory as it found it. An earlier spelling scattered `drop_tmp` calls
    // through the body and missed the one that matters most — a read error partway through the
    // source file, which is what a truncated or unreadable rolled log actually produces.
    convert_inner(text, dir, &tmp, &target).inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })
}

fn convert_inner(
    text: &Path,
    dir: &Path,
    tmp: &Path,
    target: &Path,
) -> Result<PathBuf, String> {
    let source =
        std::fs::File::open(text).map_err(|e| format!("opening {}: {e}", text.display()))?;
    let out = fs_util::create_secure(tmp).map_err(|e| format!("creating {}: {e}", tmp.display()))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut writer = ArrowWriter::try_new(out, schema(), Some(props))
        .map_err(|e| format!("opening a parquet writer on {}: {e}", tmp.display()))?;

    let mut rows: Vec<ParsedLine> = Vec::with_capacity(ROWS_PER_BATCH);
    let mut written = 0usize;
    let flush = |writer: &mut ArrowWriter<std::fs::File>,
                 rows: &mut Vec<ParsedLine>|
     -> Result<(), String> {
        if rows.is_empty() {
            return Ok(());
        }
        writer
            .write(&batch(rows)?)
            .map_err(|e| format!("writing a log row group: {e}"))?;
        rows.clear();
        Ok(())
    };
    for line in BufReader::new(source).lines() {
        let line = line.map_err(|e| format!("reading {}: {e}", text.display()))?;
        rows.push(parse_line(&line));
        written += 1;
        if rows.len() >= ROWS_PER_BATCH {
            flush(&mut writer, &mut rows)?;
        }
    }
    flush(&mut writer, &mut rows)?;
    // An empty rolled file still becomes an empty Parquet with a footer rather than being left
    // as text forever: `?file=` must answer the same shape either way.
    let file = writer
        .into_inner()
        .map_err(|e| format!("closing the parquet writer on {}: {e}", tmp.display()))?;
    file.sync_all()
        .map_err(|e| format!("fsync {}: {e}", tmp.display()))?;
    drop(file);
    fs_util::rename_durable(tmp, target, dir)
        .map_err(|e| format!("rename {} -> {}: {e}", tmp.display(), target.display()))?;
    // Step 3: the footer must read back before the text file — the only complete copy — goes.
    if let Err(e) = footer_rows(target) {
        let _ = std::fs::remove_file(target);
        fs_util::fsync_dir(dir);
        return Err(format!(
            "the parquet footer of {} did not read back ({e}); keeping the text file",
            target.display()
        ));
    }
    std::fs::remove_file(text).map_err(|e| format!("unlinking {}: {e}", text.display()))?;
    fs_util::fsync_dir(dir);
    tracing::debug!(
        file = %target.display(),
        rows = written,
        "rolled exec log converted to parquet"
    );
    Ok(target.to_path_buf())
}

/// Read a Parquet file's footer, answering its row count. This is the read-back that licenses
/// deleting the text file.
fn footer_rows(path: &Path) -> Result<i64, String> {
    let file = std::fs::File::open(path).map_err(|e| format!("{e}"))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| format!("{e}"))?;
    Ok(builder.metadata().file_metadata().num_rows())
}

/// Read a rolled Parquet log back into the rendered lines `GET /api/v1/logs?file=` serves.
///
/// The rendered form is reconstructed from the columns, not stored twice: what a caller reads
/// out of a converted file is what the text file held, modulo the best-effort field parse
/// documented in [`super::line`].
pub(crate) fn read_lines(path: &Path) -> Result<Vec<String>, String> {
    let file = std::fs::File::open(path).map_err(|e| format!("opening {}: {e}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("reading {}: {e}", path.display()))?
        .build()
        .map_err(|e| format!("reading {}: {e}", path.display()))?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| format!("reading {}: {e}", path.display()))?;
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| "log parquet: ts is not a timestamp column".to_string())?;
        let text = |idx: usize| -> Result<&StringArray, String> {
            batch
                .column(idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| format!("log parquet: column {idx} is not a string column"))
        };
        let (level, target, message, fields) = (text(1)?, text(2)?, text(3)?, text(4)?);
        for row in 0..batch.num_rows() {
            let mut line = String::new();
            if ts.is_valid(row) {
                if let Some(t) = chrono::DateTime::from_timestamp_millis(ts.value(row)) {
                    line.push_str(&t.format(TS_FORMAT).to_string());
                    line.push(' ');
                }
            }
            if level.is_valid(row) {
                line.push('[');
                line.push_str(level.value(row));
                line.push_str("] ");
            }
            if target.is_valid(row) {
                line.push_str(target.value(row));
            }
            let mut fields_out = Vec::new();
            if message.is_valid(row) {
                let msg = message.value(row);
                if level.is_valid(row) {
                    fields_out.push(format!("message={msg}"));
                } else {
                    // A line that never decomposed: it was preserved whole, so serve it whole.
                    line.push_str(msg);
                }
            }
            if fields.is_valid(row) {
                if let Ok(serde_json::Value::Object(map)) =
                    serde_json::from_str::<serde_json::Value>(fields.value(row))
                {
                    for (k, v) in map {
                        let v = v.as_str().map(str::to_string).unwrap_or_else(|| v.to_string());
                        fields_out.push(format!("{k}={v}"));
                    }
                }
            }
            if !fields_out.is_empty() {
                line.push_str(" - ");
                line.push_str(&fields_out.join(", "));
            }
            out.push(line);
        }
    }
    Ok(out)
}

/// Read a rolled (or live) text log back as lines.
pub(crate) fn read_text_lines(path: &Path) -> Result<Vec<String>, String> {
    let file = std::fs::File::open(path).map_err(|e| format!("opening {}: {e}", path.display()))?;
    BufReader::new(file)
        .lines()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("reading {}: {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &Path, name: &str, lines: &[&str]) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, format!("{}\n", lines.join("\n"))).expect("write");
        path
    }

    /// The round trip §6 promises: the rolled text becomes Parquet, the text file is gone, and
    /// the rows come back with a usable `ts` column.
    #[test]
    fn a_rolled_text_log_round_trips_through_parquet() {
        let dir = tempfile::tempdir().expect("tempdir");
        let text = write(
            dir.path(),
            "oxidant-2026-08-23.log",
            &[
                "2026-08-23T14:00:00.500Z [INFO] oxidant_execution - message=stage done, rows=7",
                "2026-08-23T14:00:01.000Z [WARN] oxidant_connect - message=pool exhausted",
            ],
        );
        let parquet = convert(&text).expect("convert");
        assert!(!text.exists(), "the text file goes only after the footer reads back");
        assert_eq!(parquet.file_name().unwrap(), "oxidant-2026-08-23.parquet");
        assert_eq!(footer_rows(&parquet).expect("footer"), 2);

        let lines = read_lines(&parquet).expect("read back");
        assert_eq!(
            lines,
            vec![
                "2026-08-23T14:00:00.500Z [INFO] oxidant_execution - message=stage done, rows=7",
                "2026-08-23T14:00:01.000Z [WARN] oxidant_connect - message=pool exhausted",
            ],
            "the derived form reconstructs the authoritative one"
        );
    }

    /// Compression is the whole reason to convert. A repetitive day must shrink, hard.
    #[test]
    fn conversion_compresses() {
        let dir = tempfile::tempdir().expect("tempdir");
        let line = "2026-08-23T14:00:00.500Z [INFO] oxidant_execution - message=stage done, rows=7";
        let body: Vec<&str> = std::iter::repeat_n(line, 20_000).collect();
        let text = write(dir.path(), "oxidant-2026-08-24.log", &body);
        let text_bytes = std::fs::metadata(&text).expect("meta").len();
        let parquet = convert(&text).expect("convert");
        let parquet_bytes = std::fs::metadata(&parquet).expect("meta").len();
        assert!(
            parquet_bytes * 10 < text_bytes,
            "zstd parquet must be far smaller: {parquet_bytes} vs {text_bytes}"
        );
        assert_eq!(read_lines(&parquet).expect("read back").len(), 20_000);
    }

    /// A conversion that fails leaves the text file — the only complete copy — where it was.
    #[test]
    fn a_failed_conversion_keeps_the_text_file_and_leaves_no_tmp() {
        let dir = tempfile::tempdir().expect("tempdir");
        let text = dir.path().join("oxidant-2026-08-25.log");
        // A directory where the text file should be. On Linux `File::open` refuses it outright;
        // on macOS the open *succeeds* and the first `read` returns EISDIR — which is why the
        // cleanup has to cover a failure partway through the body, not just at the open.
        std::fs::create_dir(&text).expect("mkdir");
        let err = convert(&text).expect_err("must fail");
        assert!(err.contains("oxidant-2026-08-25.log"), "{err}");
        assert!(text.is_dir(), "the source is untouched");
        assert!(
            !dir.path().join("oxidant-2026-08-25.parquet.tmp").exists(),
            "no .tmp is left behind"
        );
        assert!(
            !dir.path().join("oxidant-2026-08-25.parquet").exists(),
            "and no half-made parquet either"
        );
    }

    /// An empty rolled file still converts, so `?file=` answers the same shape either way.
    #[test]
    fn an_empty_rolled_log_still_gets_a_footer() {
        let dir = tempfile::tempdir().expect("tempdir");
        let text = dir.path().join("oxidant-2026-08-26.log");
        std::fs::write(&text, b"").expect("write");
        let parquet = convert(&text).expect("convert");
        assert_eq!(footer_rows(&parquet).expect("footer"), 0);
        assert!(read_lines(&parquet).expect("read").is_empty());
        assert!(!text.exists());
    }

    /// A line the parser could not decompose survives the conversion verbatim.
    #[test]
    fn an_undecomposable_line_survives_verbatim() {
        let dir = tempfile::tempdir().expect("tempdir");
        let text = write(
            dir.path(),
            "oxidant-2026-08-27.log",
            &["   at oxidant_execution::plan (a continuation line)"],
        );
        let parquet = convert(&text).expect("convert");
        assert_eq!(
            read_lines(&parquet).expect("read back"),
            vec!["   at oxidant_execution::plan (a continuation line)"]
        );
    }
}
