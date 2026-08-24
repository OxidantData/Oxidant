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

use super::line::{ordered_fields, parse_line, ParsedLine, TS_FORMAT};
use crate::history::fs_util;

/// Rows per Arrow `RecordBatch` handed to the writer — a memory knob, not a layout one.
///
/// `ArrowWriter::write` *appends* a batch into the row group it is currently building; it does
/// not cut one. All this bounds is how many `ParsedLine`s the converter holds at once.
const ROWS_PER_BATCH: usize = 8192;

/// Rows per Parquet **row group** — the unit §6b's predicate pushdown can skip.
///
/// This is the number that has to be chosen deliberately, and it was not being set at all.
/// parquet-rs defaults `max_row_group_row_count` to 1Mi rows; a full 256 MiB text log is roughly 2M
/// lines, so the default gave a *maxed-out* file two row groups and a typical few-MiB daily log
/// exactly one. A `ts` range or `level` predicate then prunes nothing and the reader decodes the
/// whole file anyway — which is the cost §6 says conversion buys its way out of.
///
/// 8192 rows is ~1 MiB of raw log text, so a 5 MiB day still gets ~5 groups to prune between and
/// a maxed-out one gets ~256. Groups are cut in write order, which is chronological, so the `ts`
/// min/max are tight and a time window skips everything outside it. The footer costs five column
/// chunks per group — negligible at these counts — and zstd is applied per *data page*, not per
/// row group, so the compression ratio is unaffected (`conversion_compresses` holds it to that).
const ROWS_PER_ROW_GROUP: usize = 8192;

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
    let outcome = convert_inner(text, dir, &tmp, &target);
    if outcome.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    outcome
}

fn convert_inner(text: &Path, dir: &Path, tmp: &Path, target: &Path) -> Result<PathBuf, String> {
    let source =
        std::fs::File::open(text).map_err(|e| format!("opening {}: {e}", text.display()))?;
    let out =
        fs_util::create_secure(tmp).map_err(|e| format!("creating {}: {e}", tmp.display()))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_max_row_group_row_count(Some(ROWS_PER_ROW_GROUP))
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
            .map_err(|e| format!("writing a log batch: {e}"))?;
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

/// One page of a log file: the lines asked for, and whether the file has more after them.
///
/// **The whole file is never materialised.** `OXIDANT_LOG_MAX_FILE_BYTES` defaults to 256 MiB, so
/// `?file=current` on a full live log would build a `Vec<String>` of ~2M entries — ~300 MiB with
/// the `String` headers — and `serde_json` would then serialise a second copy into the response
/// body, on a driver whose entire *result* budget is 512 MiB. The Parquet path is worse: a
/// ~25 MiB zstd file expands roughly 10× on read. Concurrent requests multiply it, and the
/// Observability page polls every 5 s.
#[derive(Debug, Default)]
pub(crate) struct Page {
    pub lines: Vec<String>,
    /// There is at least one more line after this page — either the row count says so or the byte
    /// budget cut the page short.
    pub has_more: bool,
}

/// Bytes of rendered text one page may hold before it is cut short.
///
/// `limit` bounds the line *count*; this bounds the memory, because one line has no fixed size —
/// a `tracing` field can carry a whole DataFusion plan. A page is therefore at most this plus one
/// line, whatever the caller asked for.
const MAX_PAGE_BYTES: usize = 8 * 1024 * 1024;

/// Read a rolled Parquet log back into the rendered lines `GET /api/v1/logs?file=` serves.
///
/// The rendered form is reconstructed from the columns, not stored twice: what a caller reads
/// out of a converted file is what the text file held, modulo the best-effort field parse
/// documented in [`super::line`].
///
/// `offset`/`limit` are pushed into the Parquet reader, so a page from the tail of a large file
/// decodes only the row groups it needs rather than the whole file.
pub(crate) fn read_lines(path: &Path, offset: usize, limit: usize) -> Result<Page, String> {
    let file = std::fs::File::open(path).map_err(|e| format!("opening {}: {e}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("reading {}: {e}", path.display()))?;
    let total = builder.metadata().file_metadata().num_rows().max(0) as usize;
    let reader = builder
        .with_offset(offset)
        .with_limit(limit)
        .build()
        .map_err(|e| format!("reading {}: {e}", path.display()))?;
    let mut page = Page {
        has_more: total > offset.saturating_add(limit),
        ..Default::default()
    };
    let mut bytes = 0usize;
    let out = &mut page.lines;
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
            // In order, and with `message` back where it was. `fields_json` carries a `message`
            // key only when the message did *not* lead the field list, so its presence is the
            // record of the position and its absence means "first" — which is what `tracing`
            // renders and therefore the shape that costs nothing to store.
            let pairs = if fields.is_valid(row) {
                ordered_fields(fields.value(row))
            } else {
                Vec::new()
            };
            let message_in_pairs = pairs.iter().any(|(k, _)| k == "message");
            let mut fields_out = Vec::new();
            if message.is_valid(row) {
                let msg = message.value(row);
                if !level.is_valid(row) {
                    // A line that never decomposed: it was preserved whole, so serve it whole.
                    line.push_str(msg);
                } else if !message_in_pairs {
                    fields_out.push(format!("message={msg}"));
                }
            }
            for (k, v) in pairs {
                fields_out.push(format!("{k}={v}"));
            }
            if !fields_out.is_empty() {
                line.push_str(" - ");
                line.push_str(&fields_out.join(", "));
            }
            bytes += line.len();
            out.push(line);
            if bytes >= MAX_PAGE_BYTES {
                page.has_more = true;
                return Ok(page);
            }
        }
    }
    Ok(page)
}

/// Read one page of a rolled (or live) text log.
///
/// `read_line` into a reused buffer rather than `lines().skip(offset)`: skipping through
/// `lines()` allocates a `String` per skipped line, which is the same unbounded read the page
/// exists to avoid, just discarded afterwards.
pub(crate) fn read_text_lines(path: &Path, offset: usize, limit: usize) -> Result<Page, String> {
    let file = std::fs::File::open(path).map_err(|e| format!("opening {}: {e}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut page = Page::default();
    let mut buf = String::new();
    let mut index = 0usize;
    let mut bytes = 0usize;
    loop {
        buf.clear();
        let read = reader
            .read_line(&mut buf)
            .map_err(|e| format!("reading {}: {e}", path.display()))?;
        if read == 0 {
            return Ok(page);
        }
        index += 1;
        if index <= offset {
            continue;
        }
        if page.lines.len() >= limit || bytes >= MAX_PAGE_BYTES {
            // One line past the page: proof there is more, without reading the rest of the file.
            page.has_more = true;
            return Ok(page);
        }
        let line = buf.trim_end_matches(['\n', '\r']);
        bytes += line.len();
        page.lines.push(line.to_string());
    }
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
        assert!(
            !text.exists(),
            "the text file goes only after the footer reads back"
        );
        assert_eq!(parquet.file_name().unwrap(), "oxidant-2026-08-23.parquet");
        assert_eq!(footer_rows(&parquet).expect("footer"), 2);

        let lines = read_lines(&parquet, 0, 100).expect("read back").lines;
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
        assert_eq!(
            read_lines(&parquet, 0, 100_000)
                .expect("read back")
                .lines
                .len(),
            20_000
        );
    }

    /// **M4.** The converter must cut row groups at a size it chose, not at parquet-rs' 1Mi-row
    /// default — otherwise a whole day is one or two row groups and §6b's `ts`/`level` pushdown
    /// prunes nothing, which is the entire payoff §6 claims for converting.
    ///
    /// Reads the file's own row-group metadata, and checks the `ts` statistics are per-group and
    /// ascending: groups are cut in write order, so a time window can skip the ones outside it.
    #[test]
    fn row_groups_are_cut_at_a_deliberate_size_so_a_time_window_can_skip_them() {
        use datafusion::parquet::file::statistics::Statistics;

        let dir = tempfile::tempdir().expect("tempdir");
        // Two full groups and a short third, each line a distinct millisecond.
        let count = ROWS_PER_ROW_GROUP * 2 + 500;
        let body: Vec<String> = (0..count)
            .map(|i| {
                format!(
                    "2026-08-23T00:00:{:02}.{:03}Z [INFO] oxidant_execution - message=stage done, i={i}",
                    i / 1000,
                    i % 1000
                )
            })
            .collect();
        let refs: Vec<&str> = body.iter().map(String::as_str).collect();
        let text = write(dir.path(), "oxidant-2026-08-29.log", &refs);
        let parquet = convert(&text).expect("convert");

        let file = std::fs::File::open(&parquet).expect("open");
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("builder");
        let md = builder.metadata().clone();
        assert_eq!(
            md.num_row_groups(),
            3,
            "{count} rows at {ROWS_PER_ROW_GROUP}/group is three groups; parquet-rs' default \
             would have made one"
        );
        assert_eq!(md.row_group(0).num_rows(), ROWS_PER_ROW_GROUP as i64);
        assert_eq!(md.row_group(1).num_rows(), ROWS_PER_ROW_GROUP as i64);
        assert_eq!(md.row_group(2).num_rows(), 500);

        // `ts` is column 0. Every group carries its own bounds, and they do not overlap.
        let bounds: Vec<(i64, i64)> = (0..md.num_row_groups())
            .map(|g| match md.row_group(g).column(0).statistics() {
                Some(Statistics::Int64(s)) => (
                    *s.min_opt().expect("a ts min"),
                    *s.max_opt().expect("a ts max"),
                ),
                other => panic!("row group {g} has no ts statistics to prune on: {other:?}"),
            })
            .collect();
        for pair in bounds.windows(2) {
            assert!(
                pair[0].1 < pair[1].0,
                "groups must be disjoint in time, else a window prunes nothing: {pair:?}"
            );
        }
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
        assert!(read_lines(&parquet, 0, 100).expect("read").lines.is_empty());
        assert!(!text.exists());
    }

    /// **M2.** The strings `?file=` returns must not change when the converter runs.
    ///
    /// Same file, both forms, byte-for-byte — including a line whose fields are in
    /// non-alphabetical order and a line whose message is not first.
    #[test]
    fn a_converted_file_returns_the_same_strings_as_the_text_it_replaced() {
        let dir = tempfile::tempdir().expect("tempdir");
        let lines = [
            "2026-08-23T14:00:00.500Z [INFO] oxidant_execution - message=stage done, zone=3, addr=7",
            "2026-08-23T14:00:01.000Z [INFO] oxidant_execution - zone=3, addr=7, message=stage done",
            "2026-08-23T14:00:02.000Z [WARN] oxidant_connect - role=\"driver\", dir=/srv/x, message=up",
            "2026-08-23T14:00:03.000Z [INFO] oxidant_connect - message=planned 3 stages, 2 replicated",
            "   at oxidant_execution::plan (a continuation line)",
        ];
        let text = write(dir.path(), "oxidant-2026-08-28.log", &lines);
        let before = read_text_lines(&text, 0, 100).expect("text").lines;
        assert_eq!(before, lines, "the text form is what was written");

        let parquet = convert(&text).expect("convert");
        let after = read_lines(&parquet, 0, 100).expect("parquet").lines;
        assert_eq!(
            after, before,
            "the converted form must be the same strings, or `?file=X` answers differently \
             depending on whether the background converter happened to have run"
        );
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
            read_lines(&parquet, 0, 100).expect("read back").lines,
            vec!["   at oxidant_execution::plan (a continuation line)"]
        );
    }
}
