//! The log browser's read path: filters, the backward cursor, and the Parquet pushdown (§6b).
//!
//! PR3 gave `?file=` one bounded, oldest-first page walked by `?offset=`. That is the right
//! answer for "show me this file"; it is the wrong one for "show me the errors in the last hour
//! out of a 256 MiB day", which is the question an operator actually arrives with. §6b answers
//! it with four composable filters, a cursor that pages **backward from the newest line**, and a
//! promise that a compressed day is not decompressed to answer a five-minute window.
//!
//! Three rules hold this module together, and each is here because the obvious spelling is
//! wrong:
//!
//! 1. **A filter never hides a line it cannot judge.** A line the parser could not decompose —
//!    something another producer wrote into `logs/`, or a `tracing` value that was already two
//!    physical lines before [`super::line::escape_line_breaks`] existed — has no level and no
//!    timestamp. Dropping it from a `level=error` page is precisely the silent exclusion §6
//!    calls out: the operator searching for an error loses the *tail* of the multi-line error
//!    they were searching for. So an unparseable line passes the level and time filters, and is
//!    still judged by `q` and `target`, which read the rendered string it does have. The same
//!    rule is why the row-group pruner refuses to prune a group whose `ts` statistics admit a
//!    null (`null_count > 0`): the two forms of one file must answer identically.
//!
//! 2. **The cursor is a row index from the start of the named file**, in both forms. Log files
//!    are append-only, so an earlier line's index never changes; conversion preserves row order
//!    one-for-one, so a cursor minted against `oxidant-2026-08-23.log` still means the same line
//!    after the background converter has replaced it with `oxidant-2026-08-23.parquet`. A byte
//!    offset would have been natural for the text path and meaningless for Parquet, and a
//!    format-tagged cursor would have let a caller's page change under them for a reason they
//!    could not see — the same race M2 closed for the line strings themselves.
//!
//! 3. **Memory is bounded by the page, never by the file — and never by a row group.** The
//!    text path is a forward scan holding at most `limit` matched lines (and
//!    [`super::columnar::MAX_PAGE_BYTES`] of them). The Parquet path holds the same page and,
//!    while it is filling it, one *page-sized* buffer of the group it is reading — not the
//!    group. It used to hold the group: every matching row of it, rendered, before a single
//!    byte check ran. `ROWS_PER_ROW_GROUP` is 8192 and a line has no fixed size, which is the
//!    premise the byte budget exists for, so the same file could serve three orders of
//!    magnitude more driver memory as `.parquet` than as `.log` — a difference rule 2 says a
//!    caller must not be able to see. Neither path ever materialises a file.
//!
//! **What pushdown does and does not buy, stated.** `ts` prunes whole row groups from the
//! footer statistics — groups are cut in write order, so their bounds are tight and disjoint
//! (PR3's M4). `level` and `target` are evaluated against a three-column projection
//! (`ts, level, target`) before `message`/`fields_json` — the fat columns — are touched at all,
//! and only the surviving rows of those are decoded, through a `RowSelection`. `q` is free text
//! over the *rendered* line, so it cannot be pushed down and is applied last; a `q`-only query
//! therefore still decodes every candidate group, which is the honest cost of a substring
//! search and exactly what `grep` would have paid on the text file.

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use datafusion::parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection,
    RowSelector,
};
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::file::metadata::RowGroupMetaData;
use datafusion::parquet::file::statistics::Statistics;
use oxidant_loom::arrow::array::{Array, StringArray, TimestampMillisecondArray};
use oxidant_loom::arrow::record_batch::RecordBatch;

use super::columnar::{render_row, RowColumns, MAX_PAGE_BYTES};
use super::line::{parse_line, ParsedLine, TS_FORMAT};

/// Severity rank: **smaller is louder**. `level=warn` keeps everything at rank ≤ 1.
///
/// `TRACE` folds in as its own rank rather than into `DEBUG`: the writer records what `tracing`
/// emitted, and collapsing two levels in the *filter* would make `level=debug` and `level=trace`
/// the same query on a file that distinguishes them.
fn level_rank(level: &str) -> Option<u8> {
    Some(match level.trim().to_ascii_uppercase().as_str() {
        "ERROR" => 0,
        "WARN" | "WARNING" => 1,
        "INFO" => 2,
        "DEBUG" => 3,
        "TRACE" => 4,
        _ => return None,
    })
}

/// The four composable predicates of §6b: level (≥ in severity), target prefix, free text, and
/// a half-open time range.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct LogFilter {
    /// Keep lines whose level is at least this loud (rank ≤ this).
    pub min_level: Option<u8>,
    /// Keep lines whose `target` starts with this string.
    pub target: Option<String>,
    /// Keep lines whose rendered form contains this string, case-insensitively. Stored folded.
    pub q: Option<String>,
    /// Inclusive lower bound on `ts`, epoch milliseconds.
    pub from_ms: Option<i64>,
    /// **Exclusive** upper bound on `ts`. Half-open, so `from=T&to=T+1h` and the next hour's
    /// `from=T+1h` tile a day with no line served twice.
    pub to_ms: Option<i64>,
}

impl LogFilter {
    /// Parse the query-string forms. Every rejection is a `400` with the offending value named:
    /// a filter that silently did nothing would be read as "there were no errors".
    pub(crate) fn parse(
        level: Option<&str>,
        target: Option<&str>,
        q: Option<&str>,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<Self, String> {
        let min_level = match level.map(str::trim).filter(|s| !s.is_empty()) {
            Some(raw) => Some(level_rank(raw).ok_or_else(|| {
                format!("invalid level `{raw}`: expected error, warn, info, debug or trace")
            })?),
            None => None,
        };
        let ts = |raw: Option<&str>, name: &str| -> Result<Option<i64>, String> {
            match raw.map(str::trim).filter(|s| !s.is_empty()) {
                Some(v) => chrono::DateTime::parse_from_rfc3339(v)
                    .map(|t| Some(t.timestamp_millis()))
                    .map_err(|e| {
                        format!("invalid {name} `{v}`: expected an RFC-3339 instant ({e})")
                    }),
                None => Ok(None),
            }
        };
        Ok(Self {
            min_level,
            target: target
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string),
            q: q.filter(|s| !s.is_empty()).map(str::to_lowercase),
            from_ms: ts(from, "from")?,
            to_ms: ts(to, "to")?,
        })
    }

    /// No predicate at all — the shape that keeps PR3's oldest-first `?offset=` answer.
    pub(crate) fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// Does this line survive the filter? `rendered` is the string the caller would be served.
    ///
    /// Rule 1 lives here: a `None` level or `None` ts is *unjudgeable*, and unjudgeable passes.
    pub(crate) fn keeps(&self, parsed: &ParsedLine, rendered: &str) -> bool {
        self.keeps_pushdown(
            parsed.ts_ms,
            parsed.level.as_deref(),
            parsed.target.as_deref(),
        ) && self.keeps_text(rendered)
    }

    /// Everything but `q`, against the three columns the Parquet path decodes first.
    ///
    /// This is the one function both forms evaluate, which is why they cannot disagree: the text
    /// path feeds it a `ParsedLine`'s columns and the Parquet path feeds it the same columns off
    /// the projection, and neither has a second copy of the rules to drift from.
    fn keeps_pushdown(
        &self,
        ts_ms: Option<i64>,
        level: Option<&str>,
        target: Option<&str>,
    ) -> bool {
        // Rule 1: an absent — or unrecognised — level is unjudgeable, and unjudgeable passes.
        if let (Some(min), Some(level)) = (self.min_level, level) {
            if level_rank(level).is_some_and(|rank| rank > min) {
                return false;
            }
        }
        // Rule 1 again: a line with no parseable timestamp is not outside any window.
        if let Some(ts) = ts_ms {
            if self.from_ms.is_some_and(|from| ts < from) {
                return false;
            }
            if self.to_ms.is_some_and(|to| ts >= to) {
                return false;
            }
        }
        if let Some(prefix) = &self.target {
            // The one predicate an absent value *fails*: a line with no target plainly does not
            // start with one, so the absence is itself the answer rather than a gap in it.
            match target {
                Some(t) if t.starts_with(prefix.as_str()) => {}
                _ => return false,
            }
        }
        true
    }

    /// `q` alone, applied to a rendered line the pushdown already admitted.
    fn keeps_text(&self, rendered: &str) -> bool {
        match &self.q {
            Some(needle) => rendered.to_lowercase().contains(needle.as_str()),
            None => true,
        }
    }
}

/// One backward page: newest line last, with the cursor for the page before it.
///
/// The lines are returned **oldest-first within the page** — the order they sit in the file, and
/// the order a log pane paints top to bottom — while the *pages* walk backward. Returning a page
/// reversed as well would make a UI concatenating two pages produce a line order that exists
/// nowhere in the file.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct CursorPage {
    pub lines: Vec<String>,
    /// The row index of the oldest line in this page, when older lines were left behind;
    /// `None` when the scan reached the start of the file. Pass it back as `before=`.
    pub next_before: Option<u64>,
}

/// A page under construction: the newest `limit` matches seen so far, oldest at the front.
struct Window {
    kept: VecDeque<(u64, String)>,
    bytes: usize,
    limit: usize,
    /// The byte ceiling this window evicts against. [`MAX_PAGE_BYTES`] for a page; whatever is
    /// *left* of it for the per-group buffer the Parquet walk fills first.
    max_bytes: usize,
    /// A match was dropped off the front — there is older matching content behind this page.
    dropped: bool,
}

impl Window {
    fn new(limit: usize) -> Self {
        Self::with_budget(limit, MAX_PAGE_BYTES)
    }

    fn with_budget(limit: usize, max_bytes: usize) -> Self {
        Self {
            kept: VecDeque::new(),
            bytes: 0,
            limit: limit.max(1),
            max_bytes,
            dropped: false,
        }
    }

    /// Add one match, evicting from the *old* end so the window always holds the newest.
    fn push(&mut self, index: u64, line: String) {
        self.bytes = self.bytes.saturating_add(line.len());
        self.kept.push_back((index, line));
        while self.kept.len() > self.limit || (self.kept.len() > 1 && self.bytes > self.max_bytes) {
            if let Some((_, old)) = self.kept.pop_front() {
                self.bytes = self.bytes.saturating_sub(old.len());
                self.dropped = true;
            }
        }
    }

    /// Add one match to the *old* end — the Parquet path walks row groups backward, so it fills
    /// the window from the newest end and stops when it is full.
    fn push_front(&mut self, index: u64, line: String) {
        self.bytes = self.bytes.saturating_add(line.len());
        self.kept.push_front((index, line));
    }

    fn full(&self) -> bool {
        self.kept.len() >= self.limit || self.bytes >= self.max_bytes
    }

    fn len(&self) -> usize {
        self.kept.len()
    }

    fn bytes(&self) -> usize {
        self.bytes
    }

    fn finish(self) -> CursorPage {
        let next_before = self
            .dropped
            .then(|| self.kept.front().map(|(i, _)| *i))
            .flatten();
        CursorPage {
            lines: self.kept.into_iter().map(|(_, l)| l).collect(),
            next_before,
        }
    }

    /// The Parquet path knows whether it stopped early rather than inferring it from evictions.
    fn finish_with(self, more_before: bool) -> CursorPage {
        let front = self.kept.front().map(|(i, _)| *i);
        CursorPage {
            lines: self.kept.into_iter().map(|(_, l)| l).collect(),
            next_before: if more_before { front } else { None },
        }
    }
}

/// One forward page: the matches at or after a row index, and where to resume.
///
/// **`next_after` is a scan position, not a match position.** It is one past the last row the
/// scan *examined*, whether or not that row matched, so a follow that re-asks with it reads each
/// row exactly once however selective the filter is. A cursor built from the last *match* would
/// re-read — and re-emit — every non-matching row after it on every poll.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ForwardPage {
    pub lines: Vec<String>,
    pub next_after: u64,
}

/// The most one physical line may contribute to a page, and the cap on the read that produces
/// it.
///
/// `BufReader::read_line` grows its target to **the whole file** when the file holds no `\n`,
/// and [`Window::push`] will not evict a sole entry — so one newline-free file in `logs/`
/// becomes one line, in memory, on the two paths whose entire contract is rule 3: bounded by
/// the page, never by the file. Engine-written lines cannot do this ([`super::line`]'s
/// `escape_line_breaks` guarantees one physical line per event); a file something *else* wrote
/// into `logs/` can, which is the same threat model rule 1 already contemplates.
///
/// A page is the bound because a line longer than a whole page could never be served whole
/// anyway.
const MAX_LINE_BYTES: usize = MAX_PAGE_BYTES;

/// Appended to a line the cap cut. A page that silently drops the rest of a line is the same
/// defect as a page that silently drops a line.
const LINE_TRUNCATED: &str = " … [line truncated: longer than one page]";

/// Read one physical line, never more than [`MAX_LINE_BYTES`] of it.
///
/// The rest of an over-long line is **skipped, not returned as the next row**: splitting it into
/// fragments would multiply the file's row count, and a row index is the cursor every caller
/// pages with (rule 2).
///
/// Bytes rather than [`BufRead::read_line`]'s `String`, for two reasons: the cap can land in the
/// middle of a multibyte character, which `read_line` reports as `InvalidData` and which would
/// fail the whole scan; and a foreign file that is not UTF-8 at all failed it already. Lossy
/// conversion serves the line the reader can act on instead, which is what rule 1 says about
/// every other line no parser can decompose.
fn read_capped_line<R: BufRead>(
    reader: &mut R,
    raw: &mut Vec<u8>,
    line: &mut String,
) -> std::io::Result<usize> {
    raw.clear();
    line.clear();
    let read = (&mut *reader)
        .take(MAX_LINE_BYTES as u64)
        .read_until(b'\n', raw)?;
    if read == 0 {
        return Ok(0);
    }
    let cut = read == MAX_LINE_BYTES && raw.last() != Some(&b'\n');
    if cut {
        skip_to_newline(reader)?;
    }
    line.push_str(String::from_utf8_lossy(raw).trim_end_matches(['\n', '\r']));
    if cut {
        line.push_str(LINE_TRUNCATED);
    }
    Ok(read)
}

/// Advance past the rest of a physical line without materialising any of it.
fn skip_to_newline<R: BufRead>(reader: &mut R) -> std::io::Result<()> {
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(());
        }
        match available.iter().position(|&b| b == b'\n') {
            Some(at) => {
                reader.consume(at + 1);
                return Ok(());
            }
            None => {
                let all = available.len();
                reader.consume(all);
            }
        }
    }
}

/// Scan a text log forward from a row index — the follow mode behind `/api/v1/logs/tail`.
pub(crate) fn scan_text_forward(
    path: &Path,
    filter: &LogFilter,
    after: u64,
    limit: usize,
) -> Result<ForwardPage, String> {
    let file = std::fs::File::open(path).map_err(|e| format!("opening {}: {e}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut page = ForwardPage::default();
    let mut raw = Vec::new();
    let mut buf = String::new();
    let mut index = 0u64;
    let mut bytes = 0usize;
    loop {
        let read = read_capped_line(&mut reader, &mut raw, &mut buf)
            .map_err(|e| format!("reading {}: {e}", path.display()))?;
        if read == 0 {
            break;
        }
        index += 1;
        if index <= after {
            continue;
        }
        if page.lines.len() >= limit.max(1) || bytes >= MAX_PAGE_BYTES {
            // Stop *before* consuming this row, so `next_after` names it and nothing is skipped.
            index -= 1;
            break;
        }
        let line = buf.as_str();
        if filter.is_empty() || filter.keeps(&parse_line(line), line) {
            bytes += line.len();
            page.lines.push(line.to_string());
        }
    }
    page.next_after = index;
    Ok(page)
}

/// Scan a rolled Parquet forward from a row index, with the same pruning and projection the
/// backward walk has. Same contract as [`scan_text_forward`].
///
/// **The forward walk prunes too.** It used to be a plain `with_offset(after)` that called
/// [`render_row`] — all five columns, `message` and `fields_json` included — on *every* row
/// before the filter was consulted. That is the read path the diagnostic dump uses, so a
/// one-hour bundle fully decoded up to `OXIDANT_LOG_KEEP_DAYS` of logs on every node to keep an
/// hour of them. It now walks row groups: a group the window puts wholly outside is skipped on
/// the footer statistics and the cursor moves past it, `level`/`target` are evaluated against
/// the `(ts, level, target)` projection, and only the surviving rows are rendered — the same
/// three passes [`scan_parquet`] makes, so the two directions cost the same over one file.
pub(crate) fn scan_parquet_forward(
    path: &Path,
    filter: &LogFilter,
    after: u64,
    limit: usize,
) -> Result<ForwardPage, String> {
    let file = std::fs::File::open(path).map_err(|e| format!("opening {}: {e}", path.display()))?;
    let metadata = ArrowReaderMetadata::load(&file, ArrowReaderOptions::default())
        .map_err(|e| format!("reading {}: {e}", path.display()))?;
    let md = metadata.metadata().clone();
    let total = md.file_metadata().num_rows().max(0) as u64;
    let mut page = ForwardPage {
        next_after: total.min(after),
        ..Default::default()
    };
    if after >= total {
        return Ok(page);
    }

    // Row-group boundaries, in file order. `starts[g]` is the global index of the group's row 0.
    let mut starts: Vec<u64> = Vec::with_capacity(md.num_row_groups());
    let mut at = 0u64;
    for g in 0..md.num_row_groups() {
        starts.push(at);
        at = at.saturating_add(md.row_group(g).num_rows().max(0) as u64);
    }

    let mask_pushdown = ProjectionMask::leaves(md.file_metadata().schema_descr(), [0, 1, 2]);
    let limit = limit.max(1);
    let mut index = after;
    let mut bytes = 0usize;
    for (g, &start) in starts.iter().enumerate() {
        let group_rows = md.row_group(g).num_rows().max(0) as u64;
        let end = start.saturating_add(group_rows);
        if end <= index {
            continue;
        }
        if page.lines.len() >= limit || bytes >= MAX_PAGE_BYTES {
            // Stop *before* this group, so `next_after` names its first unexamined row.
            break;
        }
        // The cursor can land inside a group; rows before it belong to a page already served.
        index = index.max(start);
        let first_in_group = (index - start) as usize;
        // A group the window puts wholly outside is *examined and skipped* — the cursor moves
        // past it without a data page being read. This is the whole payoff of converting.
        if prunable(md.row_group(g), filter) {
            index = end;
            continue;
        }

        // Pass 1 — `ts`, `level`, `target` only, exactly as the backward walk does it.
        let hits = {
            let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
                file.try_clone()
                    .map_err(|e| format!("reopening {}: {e}", path.display()))?,
                metadata.clone(),
            )
            .with_row_groups(vec![g])
            .with_projection(mask_pushdown.clone())
            .build()
            .map_err(|e| format!("reading {}: {e}", path.display()))?;
            let mut hits: Vec<usize> = Vec::new();
            let mut row = 0usize;
            for batch in reader {
                let batch = batch.map_err(|e| format!("reading {}: {e}", path.display()))?;
                let cols = Pushdown::of(&batch)?;
                for i in 0..batch.num_rows() {
                    if row >= first_in_group
                        && filter.keeps_pushdown(cols.ts_ms(i), cols.level(i), cols.target(i))
                    {
                        hits.push(row);
                    }
                    row += 1;
                }
            }
            hits
        };
        if hits.is_empty() {
            // Every row of this group was examined against the three cheap columns and rejected.
            index = end;
            continue;
        }

        // Pass 2 — the full row, for the surviving rows only.
        let selection = RowSelection::from(selectors(&hits, group_rows as usize));
        let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
            file.try_clone()
                .map_err(|e| format!("reopening {}: {e}", path.display()))?,
            metadata.clone(),
        )
        .with_row_groups(vec![g])
        .with_row_selection(selection)
        .build()
        .map_err(|e| format!("reading {}: {e}", path.display()))?;
        let mut seen = 0usize;
        let mut stopped_at: Option<u64> = None;
        'group: for batch in reader {
            let batch = batch.map_err(|e| format!("reading {}: {e}", path.display()))?;
            let cols = RowColumns::of(&batch)?;
            for i in 0..batch.num_rows() {
                // The selection was built from `hits` in order, so the nth row the reader hands
                // back is `hits[n]` — that mapping is what makes the cursor exact.
                let Some(&row) = hits.get(seen) else {
                    break 'group;
                };
                seen += 1;
                if page.lines.len() >= limit || bytes >= MAX_PAGE_BYTES {
                    // Stop *before* consuming this row, so `next_after` names it.
                    stopped_at = Some(start + row as u64);
                    break 'group;
                }
                let line = render_row(&cols, i);
                if filter.keeps_text(&line) {
                    bytes += line.len();
                    page.lines.push(line);
                }
            }
        }
        index = stopped_at.unwrap_or(end);
        if stopped_at.is_some() {
            break;
        }
    }
    page.next_after = index;
    Ok(page)
}

/// Filter the in-memory ring (`GET /api/v1/logs` with no `?file=`), newest-first with a cursor.
///
/// The ring is a `Vec<String>` the process already holds, so there is nothing to stream; the
/// cursor is the same row-index contract, over the ring's own indices. Those indices *do* shift
/// as the ring rolls — it is a ring — which is why the ring's page is the one place the cursor
/// is best-effort, and why the durable answer is `?file=current`.
pub(crate) fn filter_ring(
    lines: &[String],
    filter: &LogFilter,
    before: Option<u64>,
    limit: usize,
) -> CursorPage {
    let mut window = Window::new(limit);
    for (index, line) in lines.iter().enumerate() {
        let index = index as u64;
        if before.is_some_and(|b| index >= b) {
            break;
        }
        if filter.is_empty() || filter.keeps(&parse_line(line), line) {
            window.push(index, line.clone());
        }
    }
    window.finish()
}

/// Scan a text log backward-by-page: a forward read holding only the newest `limit` matches.
///
/// **Why a forward scan and not a reverse block reader.** The cursor is a row index (rule 2), so
/// a reverse reader would have to count the file's lines before it could name one — a full pass,
/// for the same money. What this costs is one sequential read per page, which is what `grep`
/// pays on the same file and what the text form exists to allow; what it buys is exact,
/// conversion-stable cursors and one code path for both forms. The Parquet path is the one that
/// prunes, and a rolled day is Parquet within a sweep of the roll.
pub(crate) fn scan_text(
    path: &Path,
    filter: &LogFilter,
    before: Option<u64>,
    limit: usize,
) -> Result<CursorPage, String> {
    let file = std::fs::File::open(path).map_err(|e| format!("opening {}: {e}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut window = Window::new(limit);
    let mut raw = Vec::new();
    let mut buf = String::new();
    let mut index = 0u64;
    loop {
        if before.is_some_and(|b| index >= b) {
            break;
        }
        let read = read_capped_line(&mut reader, &mut raw, &mut buf)
            .map_err(|e| format!("reading {}: {e}", path.display()))?;
        if read == 0 {
            break;
        }
        let line = buf.as_str();
        if filter.is_empty() || filter.keeps(&parse_line(line), line) {
            window.push(index, line.to_string());
        }
        index += 1;
    }
    Ok(window.finish())
}

/// Scan a rolled Parquet log backward-by-page, pruning row groups and pushing the level/target/
/// time predicates below the fat columns.
pub(crate) fn scan_parquet(
    path: &Path,
    filter: &LogFilter,
    before: Option<u64>,
    limit: usize,
) -> Result<CursorPage, String> {
    let file = std::fs::File::open(path).map_err(|e| format!("opening {}: {e}", path.display()))?;
    let metadata = ArrowReaderMetadata::load(&file, ArrowReaderOptions::default())
        .map_err(|e| format!("reading {}: {e}", path.display()))?;
    let md = metadata.metadata().clone();

    // Row-group boundaries, in file order. `starts[g]` is the global index of the group's row 0.
    let mut starts: Vec<u64> = Vec::with_capacity(md.num_row_groups());
    let mut at = 0u64;
    for g in 0..md.num_row_groups() {
        starts.push(at);
        at = at.saturating_add(md.row_group(g).num_rows().max(0) as u64);
    }

    // Candidates, newest group first: the ones the cursor has not already walked past and whose
    // `ts` statistics do not put the whole group outside the requested window.
    let candidates: Vec<usize> = (0..md.num_row_groups())
        .rev()
        .filter(|&g| before.map_or(true, |b| starts[g] < b))
        .filter(|&g| !prunable(md.row_group(g), filter))
        .collect();

    let mask_pushdown = ProjectionMask::leaves(md.file_metadata().schema_descr(), [0, 1, 2]);
    let mut window = Window::new(limit);
    let mut more_before = false;
    for g in candidates {
        if window.full() {
            // Something older than this page exists and was not read. The cursor says so; the
            // rows are not decoded, which is the whole point of stopping here.
            more_before = true;
            break;
        }
        let start = starts[g];
        let group_rows = md.row_group(g).num_rows().max(0) as u64;
        // The cursor can land inside a group; rows at or after it belong to the page already
        // served.
        let admissible = before
            .map(|b| b.saturating_sub(start).min(group_rows))
            .unwrap_or(group_rows) as usize;
        if admissible == 0 {
            continue;
        }

        // Pass 1 — `ts`, `level`, `target` only. `message` and `fields_json` are the bytes worth
        // skipping, and this is the pass that decides which of their rows are ever read.
        let mut hits = {
            let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
                file.try_clone()
                    .map_err(|e| format!("reopening {}: {e}", path.display()))?,
                metadata.clone(),
            )
            .with_row_groups(vec![g])
            .with_projection(mask_pushdown.clone())
            .build()
            .map_err(|e| format!("reading {}: {e}", path.display()))?;
            let mut hits: Vec<usize> = Vec::new();
            let mut row = 0usize;
            for batch in reader {
                let batch = batch.map_err(|e| format!("reading {}: {e}", path.display()))?;
                let cols = Pushdown::of(&batch)?;
                for i in 0..batch.num_rows() {
                    if row < admissible
                        && filter.keeps_pushdown(cols.ts_ms(i), cols.level(i), cols.target(i))
                    {
                        hits.push(row);
                    }
                    row += 1;
                }
            }
            hits
        };
        if hits.is_empty() {
            continue;
        }
        // **Only the newest matches of this group are rendered.** With no free-text predicate
        // every pushdown hit *is* a match, so the page can only ever hold the newest
        // `remaining` of them and the older ones need not have `message`/`fields_json` decoded
        // at all. `q` is the exception — it judges the rendered line, so every candidate has to
        // be built — and there the per-group window below is what bounds the memory.
        let remaining = limit.saturating_sub(window.len()).max(1);
        if filter.q.is_none() && hits.len() > remaining {
            hits.drain(..hits.len() - remaining);
            more_before = true;
        }

        // Pass 2 — the full row, for the surviving rows only.
        let selection = RowSelection::from(selectors(&hits, md.row_group(g).num_rows() as usize));
        let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
            file.try_clone()
                .map_err(|e| format!("reopening {}: {e}", path.display()))?,
            metadata.clone(),
        )
        .with_row_groups(vec![g])
        .with_row_selection(selection)
        .build()
        .map_err(|e| format!("reading {}: {e}", path.display()))?;
        // **Rule 3, on this path too.** The rendered rows go into a window of their own, sized
        // to what is *left* of the page, so it holds a page and not a row group. A plain `Vec`
        // here accumulated every matching row of the group at full rendered size before a single
        // byte check ran — 8,192 rows, and one `tracing` field can carry a whole DataFusion
        // plan — while the text path over the same file stayed bounded. Rule 2's premise is that
        // the two forms of one file are indistinguishable to a caller, and three orders of
        // magnitude of driver memory is a distinction.
        let mut newest =
            Window::with_budget(remaining, MAX_PAGE_BYTES.saturating_sub(window.bytes()));
        let mut seen = 0usize;
        for batch in reader {
            let batch = batch.map_err(|e| format!("reading {}: {e}", path.display()))?;
            let cols = RowColumns::of(&batch)?;
            for i in 0..batch.num_rows() {
                // The selection was built from `hits` in order, so the nth row the reader hands
                // back is `hits[n]` — that mapping is what makes the cursor exact.
                let Some(&row) = hits.get(seen) else { break };
                seen += 1;
                let line = render_row(&cols, i);
                if filter.keeps_text(&line) {
                    newest.push(start + row as u64, line);
                }
            }
        }
        if newest.dropped {
            // This group held matches older than the page could carry.
            more_before = true;
        }
        // Newest first into the page's front, so a full page stops before the older rows of
        // this group are even considered.
        for (index, line) in newest.kept.into_iter().rev() {
            if window.full() {
                more_before = true;
                break;
            }
            window.push_front(index, line);
        }
    }
    Ok(window.finish_with(more_before))
}

/// Contiguous `RowSelector` runs over one row group's `hits` (ascending, in-group indices).
fn selectors(hits: &[usize], group_rows: usize) -> Vec<RowSelector> {
    let mut out: Vec<RowSelector> = Vec::new();
    let mut cursor = 0usize;
    let mut i = 0usize;
    while i < hits.len() {
        let start = hits[i];
        let mut end = start + 1;
        while i + 1 < hits.len() && hits[i + 1] == end {
            i += 1;
            end += 1;
        }
        if start > cursor {
            out.push(RowSelector::skip(start - cursor));
        }
        out.push(RowSelector::select(end - start));
        cursor = end;
        i += 1;
    }
    if cursor < group_rows {
        out.push(RowSelector::skip(group_rows - cursor));
    }
    out
}

/// Can this whole row group be skipped without decoding it?
///
/// Only on `ts`, and only when the statistics are complete: a group whose `ts` column admits a
/// **null** holds a line the filter cannot judge, and rule 1 says such a line is served, so the
/// group is read. That is what keeps the Parquet answer identical to the text answer.
fn prunable(group: &RowGroupMetaData, filter: &LogFilter) -> bool {
    if filter.from_ms.is_none() && filter.to_ms.is_none() {
        return false;
    }
    let Some(stats) = group.column(0).statistics() else {
        return false;
    };
    if stats.null_count_opt().map_or(true, |n| n > 0) {
        return false;
    }
    let Statistics::Int64(s) = stats else {
        return false;
    };
    let (Some(min), Some(max)) = (s.min_opt(), s.max_opt()) else {
        return false;
    };
    // Disjoint from the half-open window `[from, to)`.
    filter.from_ms.is_some_and(|f| *max < f) || filter.to_ms.is_some_and(|t| *min >= t)
}

/// One file `GET /api/v1/logs/files` lists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileInfo {
    /// The `?file=` value that reads it: `current`, or a period stem with its optional `.N`.
    pub file: String,
    /// `false` only for `current` — the live file the writer still holds open.
    pub rolled: bool,
    /// `text` or `parquet` — §6's conversion state, read from the outside.
    pub format: &'static str,
    pub size_bytes: u64,
    /// RFC-3339 UTC bounds, when the file carries a parseable timestamp at each end. A file
    /// whose first or last line was written by something else answers `null` rather than a
    /// guess.
    pub first_ts: Option<String>,
    pub last_ts: Option<String>,
}

/// Every log file in `dir` this engine wrote, newest period first, with `current` at the head.
///
/// **Ordered by `(period end, split)`, never lexicographically** — `oxidant-2026-08-23.2.log`
/// sorts *before* the plain name (`'2' < 'l'`) while being the newer generation of the period,
/// which is the trap `disk::rolled_by_period` and `load_event_log` both had to be taught.
pub(crate) fn list_files(dir: &Path) -> Vec<FileInfo> {
    let mut out: Vec<FileInfo> = Vec::new();
    let live = dir.join(crate::history::disk::LIVE_LOG);
    if let Ok(meta) = live.symlink_metadata() {
        if meta.is_file() {
            let (first_ts, last_ts) = text_bounds(&live);
            out.push(FileInfo {
                file: "current".to_string(),
                rolled: false,
                format: "text",
                size_bytes: meta.len(),
                first_ts,
                last_ts,
            });
        }
    }
    let mut rolled: Vec<(chrono::DateTime<chrono::Utc>, u32, FileInfo)> = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some((period, split, ext)) = super::parse_rolled_name(name) else {
            continue;
        };
        let format = match ext {
            "parquet" => "parquet",
            "log" => "text",
            // `.parquet.tmp` and anything else the grammar tolerates but the browser cannot
            // serve: a listing that offers a file `?file=` would 404 on is worse than a gap.
            _ => continue,
        };
        let Ok(meta) = entry.path().symlink_metadata() else {
            continue;
        };
        if !meta.is_file() {
            continue;
        }
        let (first_ts, last_ts) = if format == "parquet" {
            parquet_bounds(&entry.path())
        } else {
            text_bounds(&entry.path())
        };
        let stem = period.stem();
        rolled.push((
            period.end().unwrap_or_else(chrono::Utc::now),
            split,
            FileInfo {
                file: if split <= 1 {
                    stem
                } else {
                    format!("{stem}.{split}")
                },
                rolled: true,
                format,
                size_bytes: meta.len(),
                first_ts,
                last_ts,
            },
        ));
    }
    rolled.sort_by(|a, b| b.0.cmp(&a.0).then(b.1.cmp(&a.1)));
    out.extend(rolled.into_iter().map(|(_, _, info)| info));
    out
}

/// RFC-3339 from epoch millis, in the writer's own spelling.
fn stamp(ms: i64) -> Option<String> {
    chrono::DateTime::from_timestamp_millis(ms).map(|t| t.format(TS_FORMAT).to_string())
}

/// The first and last parseable timestamps of a text log, read from the two ends only.
///
/// Each end is read through at most [`PROBE_BYTES`]: a listing over a 256 MiB live file must not
/// read 256 MiB, and the first and last lines of a log are within a few KiB of their end. The
/// head bound is not decoration — `read_line` with no cap reads the whole file when the file has
/// no `\n` in it, and `list_files` runs this over *every* text file in `logs/`, which is what
/// made a listing the cheapest way to make the driver read a foreign file whole.
const PROBE_BYTES: u64 = 64 * 1024;

fn text_bounds(path: &Path) -> (Option<String>, Option<String>) {
    let Ok(mut file) = std::fs::File::open(path) else {
        return (None, None);
    };
    let mut first = Vec::new();
    if BufReader::new(&file)
        .take(PROBE_BYTES)
        .read_until(b'\n', &mut first)
        .is_err()
    {
        return (None, None);
    }
    let first_ts = parse_line(String::from_utf8_lossy(&first).trim_end())
        .ts_ms
        .and_then(stamp);
    let len = file.metadata().map(|m| m.len()).unwrap_or(0);
    let back = len.min(PROBE_BYTES);
    if file.seek(SeekFrom::End(-(back as i64))).is_err() {
        return (first_ts, None);
    }
    let mut tail = String::new();
    if file.take(back).read_to_string(&mut tail).is_err() {
        // A tail that is not UTF-8 (a torn multibyte character at the probe boundary) is not an
        // error worth failing a listing over.
        return (first_ts, None);
    }
    let last_ts = tail
        .lines()
        .rev()
        .find_map(|l| parse_line(l).ts_ms)
        .and_then(stamp);
    (first_ts, last_ts)
}

/// The first and last `ts` of a rolled Parquet, from the **footer statistics** — no data pages
/// are read at all. Groups are cut in write order, so the first group's min and the last
/// group's max are the file's bounds.
fn parquet_bounds(path: &Path) -> (Option<String>, Option<String>) {
    let Ok(file) = std::fs::File::open(path) else {
        return (None, None);
    };
    let Ok(metadata) = ArrowReaderMetadata::load(&file, ArrowReaderOptions::default()) else {
        return (None, None);
    };
    let md = metadata.metadata();
    if md.num_row_groups() == 0 {
        return (None, None);
    }
    let bound = |g: usize, pick: fn(&Statistics) -> Option<i64>| -> Option<String> {
        md.row_group(g)
            .column(0)
            .statistics()
            .and_then(pick)
            .and_then(stamp)
    };
    (
        bound(0, |s| match s {
            Statistics::Int64(s) => s.min_opt().copied(),
            _ => None,
        }),
        bound(md.num_row_groups() - 1, |s| match s {
            Statistics::Int64(s) => s.max_opt().copied(),
            _ => None,
        }),
    )
}

/// The `(ts, level, target)` projection the pushdown pass decodes — deliberately **not**
/// [`RowColumns`], which carries `message`/`fields_json` because it can render. The whole point
/// of this pass is that those two columns are never touched.
struct Pushdown<'a> {
    ts: &'a TimestampMillisecondArray,
    level: &'a StringArray,
    target: &'a StringArray,
}

impl<'a> Pushdown<'a> {
    fn of(batch: &'a RecordBatch) -> Result<Self, String> {
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
        Ok(Self {
            ts,
            level: text(1)?,
            target: text(2)?,
        })
    }

    fn ts_ms(&self, row: usize) -> Option<i64> {
        self.ts.is_valid(row).then(|| self.ts.value(row))
    }

    fn level(&self, row: usize) -> Option<&str> {
        self.level.is_valid(row).then(|| self.level.value(row))
    }

    fn target(&self, row: usize) -> Option<&str> {
        self.target.is_valid(row).then(|| self.target.value(row))
    }
}

#[cfg(test)]
mod tests {
    use super::super::columnar::ROWS_PER_ROW_GROUP;
    use super::*;

    fn write(dir: &Path, name: &str, lines: &[&str]) -> std::path::PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, format!("{}\n", lines.join("\n"))).expect("write");
        path
    }

    fn filter(level: Option<&str>, target: Option<&str>, q: Option<&str>) -> LogFilter {
        LogFilter::parse(level, target, q, None, None).expect("filter")
    }

    const LINES: [&str; 6] = [
        "2026-08-23T14:00:00.000Z [INFO] oxidant_execution - message=stage 0 start",
        "2026-08-23T14:00:01.000Z [WARN] oxidant_connect - message=pool exhausted",
        "2026-08-23T14:00:02.000Z [ERROR] oxidant_execution::plan - message=stage 0 failed",
        "2026-08-23T14:00:03.000Z [INFO] oxidant_connect - message=retrying",
        "   at oxidant_execution::plan (a continuation line)",
        "2026-08-23T14:00:04.000Z [DEBUG] oxidant_execution - message=stage 0 done",
    ];

    /// The four filters compose, and every one of them means the same thing on both forms of
    /// the same file — the property the whole module exists to hold.
    #[test]
    fn the_filters_compose_identically_over_text_and_parquet() {
        let dir = tempfile::tempdir().expect("tempdir");
        let text = write(dir.path(), "oxidant-2026-08-23.log", &LINES);
        let cases: Vec<(LogFilter, Vec<&str>)> = vec![
            (
                filter(Some("warn"), None, None),
                vec![LINES[1], LINES[2], LINES[4]],
            ),
            (
                filter(None, Some("oxidant_execution"), None),
                vec![LINES[0], LINES[2], LINES[5]],
            ),
            (filter(None, None, Some("POOL")), vec![LINES[1]]),
            (
                LogFilter::parse(
                    None,
                    None,
                    None,
                    Some("2026-08-23T14:00:01Z"),
                    Some("2026-08-23T14:00:03Z"),
                )
                .expect("range"),
                vec![LINES[1], LINES[2], LINES[4]],
            ),
            (
                filter(Some("error"), Some("oxidant_execution"), Some("failed")),
                vec![LINES[2]],
            ),
        ];
        for (f, expected) in &cases {
            let from_text = scan_text(&text, f, None, 100).expect("text");
            assert_eq!(from_text.lines, *expected, "text: {f:?}");
        }
        let parquet = super::super::columnar::convert(&text).expect("convert");
        for (f, expected) in &cases {
            let from_parquet = scan_parquet(&parquet, f, None, 100).expect("parquet");
            assert_eq!(
                from_parquet.lines, *expected,
                "parquet must answer what the text did: {f:?}"
            );
        }
    }

    /// **Rule 1.** A line the parser could not decompose has no level and no timestamp, and a
    /// filter must not silently drop it: it is the continuation of the multi-line error the
    /// operator is searching for. `target` is the one predicate an absent value *fails* — the
    /// absence is the answer.
    #[test]
    fn a_filter_never_hides_a_line_it_cannot_judge() {
        let dir = tempfile::tempdir().expect("tempdir");
        let text = write(dir.path(), "oxidant-2026-08-24.log", &LINES);
        assert!(
            scan_text(&text, &filter(Some("error"), None, None), None, 100)
                .expect("scan")
                .lines
                .contains(&LINES[4].to_string()),
            "the continuation line survives a level filter"
        );
        assert!(
            !scan_text(
                &text,
                &filter(None, Some("oxidant_execution"), None),
                None,
                100
            )
            .expect("scan")
            .lines
            .contains(&LINES[4].to_string()),
            "but it plainly does not start with a target prefix"
        );
        // And the same on the converted form, where the row's ts and level are genuine nulls.
        let parquet = super::super::columnar::convert(&text).expect("convert");
        assert!(
            scan_parquet(&parquet, &filter(Some("error"), None, None), None, 100)
                .expect("scan")
                .lines
                .contains(&LINES[4].to_string()),
            "a null ts/level row is unjudgeable in the parquet too"
        );
    }

    /// The cursor walks backward, page by page, and covers the file exactly once.
    #[test]
    fn the_cursor_pages_backward_without_gaps_or_repeats() {
        let dir = tempfile::tempdir().expect("tempdir");
        let body: Vec<String> = (0..25)
            .map(|i| {
                format!("2026-08-23T14:00:{i:02}.000Z [INFO] oxidant_execution - message=line {i}")
            })
            .collect();
        let refs: Vec<&str> = body.iter().map(String::as_str).collect();
        let text = write(dir.path(), "oxidant-2026-08-25.log", &refs);
        let parquet_src = write(dir.path(), "oxidant-2026-08-26.log", &refs);
        let parquet = super::super::columnar::convert(&parquet_src).expect("convert");

        for (label, read) in [
            (
                "text",
                &scan_text
                    as &dyn Fn(&Path, &LogFilter, Option<u64>, usize) -> Result<CursorPage, String>,
            ),
            ("parquet", &scan_parquet),
        ] {
            let path = if label == "text" { &text } else { &parquet };
            let mut seen: Vec<String> = Vec::new();
            let mut before = None;
            loop {
                let page = read(path, &LogFilter::default(), before, 7).expect(label);
                assert!(page.lines.len() <= 7, "{label}: page respects the limit");
                let mut head = page.lines.clone();
                head.extend(seen);
                seen = head;
                match page.next_before {
                    Some(cursor) => {
                        assert!(
                            before.is_none_or(|b| cursor < b),
                            "{label}: the cursor must move backward"
                        );
                        before = Some(cursor);
                    }
                    None => break,
                }
            }
            assert_eq!(seen, body, "{label}: the pages reassemble the file exactly");
        }
    }

    /// A cursor minted against the text form still names the same line after the background
    /// converter has replaced the file — the race a caller cannot see (M2, one level up).
    #[test]
    fn a_cursor_survives_the_conversion() {
        let dir = tempfile::tempdir().expect("tempdir");
        let text = write(dir.path(), "oxidant-2026-08-27.log", &LINES);
        let first = scan_text(&text, &LogFilter::default(), None, 3).expect("text");
        let cursor = first.next_before.expect("a cursor");
        let parquet = super::super::columnar::convert(&text).expect("convert");
        let next = scan_parquet(&parquet, &LogFilter::default(), Some(cursor), 3).expect("parquet");
        assert_eq!(next.lines, LINES[..3].to_vec());
        assert_eq!(
            next.next_before, None,
            "and it reached the start of the file"
        );
    }

    /// **The payoff §6 claims for converting.** A narrow time window must skip whole row groups
    /// rather than decode the day — asserted on the pruner directly, over a file with the
    /// converter's real 8192-row groups.
    #[test]
    fn a_time_window_prunes_whole_row_groups() {
        let dir = tempfile::tempdir().expect("tempdir");
        let count = ROWS_PER_ROW_GROUP * 2 + 100;
        let body: Vec<String> = (0..count)
            .map(|i| {
                format!(
                    "2026-08-23T{:02}:{:02}:{:02}.000Z [INFO] oxidant_execution - message=line {i}",
                    i / 3600,
                    (i / 60) % 60,
                    i % 60
                )
            })
            .collect();
        let refs: Vec<&str> = body.iter().map(String::as_str).collect();
        let text = write(dir.path(), "oxidant-2026-08-28.log", &refs);
        let parquet = super::super::columnar::convert(&text).expect("convert");

        let file = std::fs::File::open(&parquet).expect("open");
        let metadata =
            ArrowReaderMetadata::load(&file, ArrowReaderOptions::default()).expect("metadata");
        let md = metadata.metadata();
        assert_eq!(md.num_row_groups(), 3, "three groups to prune between");

        // A window opening exactly at the third group's first line: the two groups wholly before
        // it must be skipped without a decode. (Row 16384 is 04:33:04; the row before it, the
        // last of group 1, is 04:33:03 — so the bound is tight on purpose.)
        let late =
            LogFilter::parse(None, None, None, Some("2026-08-23T04:33:04Z"), None).expect("filter");
        let pruned: Vec<bool> = (0..md.num_row_groups())
            .map(|g| prunable(md.row_group(g), &late))
            .collect();
        assert_eq!(
            pruned,
            vec![true, true, false],
            "the two groups wholly before the window are skipped without a decode"
        );
        // And the rows it returns are right.
        let page = scan_parquet(&parquet, &late, None, 10).expect("scan");
        assert_eq!(page.lines.len(), 10);
        assert_eq!(page.lines.last().unwrap(), body.last().unwrap());
    }

    /// **The forward walk prunes on the footer, and the proof is that it never opens the pages
    /// it skips.** The two groups the window puts wholly outside have their `ts` column chunk
    /// overwritten with garbage; a walk that decoded them would fail, and one that reads the
    /// statistics and steps over them cannot.
    ///
    /// This is the read path the diagnostic dump uses. Before §6b's forward scan was given the
    /// treatment its backward sibling already had, it was a plain `with_offset(after)` that
    /// rendered all five columns of every row — so a one-hour bundle decoded the whole
    /// retention on every node.
    #[test]
    fn the_forward_walk_steps_over_a_pruned_row_group_without_reading_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let count = ROWS_PER_ROW_GROUP * 2 + 100;
        let body: Vec<String> = (0..count)
            .map(|i| {
                format!(
                    "2026-08-23T{:02}:{:02}:{:02}.000Z [INFO] oxidant_execution - message=line {i}",
                    i / 3600,
                    (i / 60) % 60,
                    i % 60
                )
            })
            .collect();
        let refs: Vec<&str> = body.iter().map(String::as_str).collect();
        let text = write(dir.path(), "oxidant-2026-09-03.log", &refs);
        let parquet = super::super::columnar::convert(&text).expect("convert");

        // Scribble over the first two groups' `ts` chunks. The footer — and every statistic the
        // pruner reads — sits at the end of the file and is untouched.
        {
            let file = std::fs::File::open(&parquet).expect("open");
            let metadata =
                ArrowReaderMetadata::load(&file, ArrowReaderOptions::default()).expect("metadata");
            let md = metadata.metadata();
            assert_eq!(md.num_row_groups(), 3, "three groups to prune between");
            let mut bytes = std::fs::read(&parquet).expect("read");
            for g in 0..2 {
                let (start, len) = md.row_group(g).column(0).byte_range();
                for b in &mut bytes[start as usize..(start + len) as usize] {
                    *b = 0x5a;
                }
            }
            std::fs::write(&parquet, &bytes).expect("write");
        }

        // Unfiltered, the damage is real: the walk reaches those pages and says so.
        assert!(
            scan_parquet_forward(&parquet, &LogFilter::default(), 0, 10).is_err(),
            "the corruption must be something a decode actually trips over"
        );

        // With a window that opens at the third group, neither damaged group is opened.
        let late =
            LogFilter::parse(None, None, None, Some("2026-08-23T04:33:04Z"), None).expect("filter");
        let page = scan_parquet_forward(&parquet, &late, 0, 10).expect(
            "the pruned groups are \
            skipped on their footer statistics, not decoded",
        );
        assert_eq!(page.lines.len(), 10);
        assert_eq!(page.lines[0], body[ROWS_PER_ROW_GROUP * 2]);
        assert!(
            page.next_after > (ROWS_PER_ROW_GROUP * 2) as u64,
            "and the cursor stepped over both of them in one page: {}",
            page.next_after
        );
    }

    /// The forward cursor walks a filtered file exactly once, and both forms answer the same —
    /// the same property [`the_cursor_pages_backward_without_gaps_or_repeats`] holds for the
    /// backward one. It is the property the forward path's rewrite had to preserve.
    #[test]
    fn the_forward_cursor_walks_a_filtered_file_once_in_both_forms() {
        let dir = tempfile::tempdir().expect("tempdir");
        let body: Vec<String> = (0..60)
            .map(|i| {
                let level = if i % 7 == 0 { "ERROR" } else { "INFO" };
                format!(
                    "2026-08-23T14:{:02}:{:02}.000Z [{level}] oxidant_execution - message=line {i}",
                    i / 60,
                    i % 60
                )
            })
            .collect();
        let refs: Vec<&str> = body.iter().map(String::as_str).collect();
        let text = write(dir.path(), "oxidant-2026-09-04.log", &refs);
        let parquet_src = write(dir.path(), "oxidant-2026-09-05.log", &refs);
        let parquet = super::super::columnar::convert(&parquet_src).expect("convert");
        let errors: Vec<String> = body
            .iter()
            .filter(|l| l.contains("[ERROR]"))
            .cloned()
            .collect();

        for (label, read) in [
            (
                "text",
                &scan_text_forward
                    as &dyn Fn(&Path, &LogFilter, u64, usize) -> Result<ForwardPage, String>,
            ),
            ("parquet", &scan_parquet_forward),
        ] {
            let path = if label == "text" { &text } else { &parquet };
            let f = filter(Some("error"), None, None);
            let mut seen: Vec<String> = Vec::new();
            let mut after = 0u64;
            for _ in 0..100 {
                let page = read(path, &f, after, 3).expect(label);
                assert!(
                    page.lines.len() <= 3,
                    "{label}: the page respects the limit"
                );
                seen.extend(page.lines);
                if page.next_after <= after {
                    break;
                }
                after = page.next_after;
            }
            assert_eq!(
                seen, errors,
                "{label}: every match once, in order, and nothing else"
            );
            assert_eq!(
                after as usize, 60,
                "{label}: the scan position is the end of the file, not the last match"
            );
        }
    }

    /// A group whose `ts` admits a null is never pruned, or the two forms would disagree about
    /// a line neither can judge (rule 1).
    #[test]
    fn a_row_group_holding_an_unparseable_line_is_never_pruned() {
        let dir = tempfile::tempdir().expect("tempdir");
        let text = write(
            dir.path(),
            "oxidant-2026-08-29.log",
            &[
                "2026-08-23T14:00:00.000Z [INFO] oxidant_execution - message=a",
                "   at oxidant_execution::plan (no timestamp at all)",
            ],
        );
        let parquet = super::super::columnar::convert(&text).expect("convert");
        let file = std::fs::File::open(&parquet).expect("open");
        let metadata =
            ArrowReaderMetadata::load(&file, ArrowReaderOptions::default()).expect("metadata");
        let far =
            LogFilter::parse(None, None, None, Some("2030-01-01T00:00:00Z"), None).expect("filter");
        assert!(
            !prunable(metadata.metadata().row_group(0), &far),
            "a null ts in the group means a line the filter cannot judge, so the group is read"
        );
        assert_eq!(
            scan_parquet(&parquet, &far, None, 10).expect("scan").lines,
            vec!["   at oxidant_execution::plan (no timestamp at all)"],
            "and the unjudgeable line is served"
        );
    }

    /// The page is bounded by bytes as well as by lines: one `tracing` field can carry a whole
    /// DataFusion plan, so `limit` alone bounds nothing.
    ///
    /// **And it is bounded on both forms, identically.** Every other test in this module runs
    /// text-then-parquet; this one did not, and the Parquet path was the one that would have
    /// failed it. It buffered every matching row of a row group at full rendered size before a
    /// single byte check ran, so the same file served 8,389,088 bytes as `.parquet` and
    /// 8,388,608 as `.log`, out of a page budget of 8 MiB — on a driver whose whole result
    /// budget is 512 MiB, and from a group that can hold 8,192 rows. Rule 2's premise is that
    /// the two forms of one file are indistinguishable to a caller, so the assertion is
    /// equality rather than a bound on each.
    #[test]
    fn a_page_is_cut_short_by_its_byte_budget() {
        let dir = tempfile::tempdir().expect("tempdir");
        let fat = "x".repeat(1024 * 1024);
        let body: Vec<String> = (0..16)
            .map(|i| {
                format!("2026-08-23T14:00:{i:02}.000Z [INFO] oxidant_execution - message={fat}")
            })
            .collect();
        let refs: Vec<&str> = body.iter().map(String::as_str).collect();
        let text = write(dir.path(), "oxidant-2026-08-30.log", &refs);
        let parquet_src = write(dir.path(), "oxidant-2026-08-30.2.log", &refs);
        let parquet = super::super::columnar::convert(&parquet_src).expect("convert");

        let mut pages = Vec::new();
        for (label, path, read) in [
            (
                "text",
                &text,
                &scan_text
                    as &dyn Fn(&Path, &LogFilter, Option<u64>, usize) -> Result<CursorPage, String>,
            ),
            ("parquet", &parquet, &scan_parquet),
        ] {
            let page = read(path, &LogFilter::default(), None, 1000).expect(label);
            assert!(
                page.lines.len() < 16,
                "{label}: 16 MiB of lines must not come back as one page: {}",
                page.lines.len()
            );
            let bytes: usize = page.lines.iter().map(String::len).sum();
            assert!(
                bytes <= MAX_PAGE_BYTES,
                "{label}: a page must fit its own budget: {bytes} > {MAX_PAGE_BYTES}"
            );
            assert!(
                page.next_before.is_some(),
                "{label}: and the caller is told there is more"
            );
            pages.push(page);
        }
        assert_eq!(
            pages[0], pages[1],
            "the two forms of one file must answer the same page, byte budget included"
        );
    }

    /// Every rejection names its value: a filter that silently did nothing reads as "there were
    /// no errors".
    #[test]
    fn an_invalid_filter_is_rejected_with_its_value() {
        let err = LogFilter::parse(Some("loud"), None, None, None, None).expect_err("must fail");
        assert!(err.contains("loud"), "{err}");
        let err =
            LogFilter::parse(None, None, None, Some("yesterday"), None).expect_err("must fail");
        assert!(err.contains("yesterday"), "{err}");
        assert!(LogFilter::parse(None, None, None, None, None)
            .expect("empty")
            .is_empty());
    }

    /// Levels are a severity order, not a set: `level=warn` is "warn **and** error".
    #[test]
    fn the_level_filter_is_a_severity_floor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let text = write(dir.path(), "oxidant-2026-08-31.log", &LINES);
        let at = |level: &str| {
            scan_text(&text, &filter(Some(level), None, None), None, 100)
                .expect("scan")
                .lines
                .len()
        };
        assert_eq!(at("error"), 2, "the error, plus the unjudgeable line");
        assert_eq!(at("warn"), 3);
        assert_eq!(at("info"), 5);
        // Everything *in this fixture*, which holds no `TRACE`. On a node running with
        // `RUST_LOG=trace` it is not everything — see the test below, which is why the pane has
        // a `trace` chip.
        assert_eq!(at("debug"), 6);
    }

    /// **`debug` is a floor, not "everything".** `TRACE` has its own rank, deliberately — the
    /// writer records what `tracing` emitted and collapsing two levels in the filter would make
    /// `level=debug` and `level=trace` the same query on a file that distinguishes them. The
    /// pane's chip row stopped at `debug`, so on a node running `RUST_LOG=trace` clicking the
    /// chip labelled as the most permissive floor made lines *disappear*: no filter shows
    /// `TRACE`, `level=debug` does not. Both halves are asserted, because the `debug` count
    /// alone passes on a file with no trace line in it.
    #[test]
    fn the_debug_floor_is_not_everything_on_a_trace_enabled_node() {
        let dir = tempfile::tempdir().expect("tempdir");
        let lines = [
            "2026-08-23T14:00:00.000Z [DEBUG] oxidant_execution - message=stage 0 done",
            "2026-08-23T14:00:01.000Z [TRACE] oxidant_execution - message=row 4194304",
        ];
        let text = write(dir.path(), "oxidant-2026-08-31.log", &lines);
        let at = |level: Option<&str>| {
            scan_text(&text, &filter(level, None, None), None, 100)
                .expect("scan")
                .lines
                .len()
        };
        assert_eq!(at(None), 2, "no filter shows every level");
        assert_eq!(
            at(Some("debug")),
            1,
            "`debug` is a floor: it drops the rank below it"
        );
        assert_eq!(at(Some("trace")), 2, "which is what `trace` is for");
    }

    /// **A file with no `\n` in it is one capped line, not the file.**
    ///
    /// `BufReader::read_line` grows its target to the whole file when there is no newline, and
    /// `Window::push` will not evict a sole entry — so on the two paths whose whole contract is
    /// "bounded by the page, never by the file", a single foreign file in `logs/` became the
    /// page. Engine-written lines cannot do this (`escape_line_breaks` guarantees one physical
    /// line per event); something else writing into the directory can, which is the threat model
    /// rule 1 already contemplates and the same one the listing's ignored `syslog` stands for.
    ///
    /// The remainder is **skipped**, not served as the next rows: fragmenting one line into a
    /// hundred would multiply the file's row count, and a row index is the cursor every caller
    /// pages with.
    #[test]
    fn a_line_longer_than_a_page_is_capped_marked_and_still_one_row() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut body = "2026-08-23T14:00:00.000Z [ERROR] oxidant_execution - message=".to_string();
        body.push_str(&"x".repeat(MAX_LINE_BYTES + 4096));
        body.push('\n');
        let next = "2026-08-23T14:00:01.000Z [INFO] oxidant_connect - message=the row after it";
        body.push_str(next);
        body.push('\n');
        let path = dir.path().join("oxidant-2026-08-31.log");
        std::fs::write(&path, &body).expect("write");

        let head = scan_text_forward(&path, &filter(None, None, None), 0, 100).expect("scan");
        assert_eq!(head.lines.len(), 1);
        assert!(
            head.lines[0].ends_with(LINE_TRUNCATED),
            "a page that silently drops the rest of a line is a page that lies"
        );
        assert!(
            head.lines[0].len() <= MAX_LINE_BYTES + LINE_TRUNCATED.len(),
            "the line is capped at a page: {} bytes",
            head.lines[0].len()
        );
        assert_eq!(head.next_after, 1, "and it is one row");

        let tail = scan_text_forward(&path, &filter(None, None, None), 1, 100).expect("scan");
        assert_eq!(
            tail.lines,
            vec![next.to_string()],
            "the row after the over-long one is the next real line, not a fragment of it"
        );
        assert_eq!(tail.next_after, 2, "the file is two rows, not a hundred");

        // The backward walk over a file with no `\n` anywhere: the one shape `Window`'s
        // sole-entry guard cannot evict, so an uncapped read would return the file as the page.
        let flat = dir.path().join("oxidant-2026-08-30.log");
        std::fs::write(&flat, "y".repeat(MAX_LINE_BYTES + 4096)).expect("write");
        let page = scan_text(&flat, &filter(None, None, None), None, 100).expect("scan");
        assert_eq!(page.lines.len(), 1);
        assert!(
            page.lines[0].len() <= MAX_LINE_BYTES + LINE_TRUNCATED.len(),
            "the page held the file: {} bytes",
            page.lines[0].len()
        );

        // And the listing reads both ends of both files through a 64 KiB probe apiece — the
        // head bound matters because `list_files` runs it over *every* text file in `logs/`.
        let files = list_files(dir.path());
        assert_eq!(files.len(), 2, "both files are still listed");
        assert_eq!(
            files
                .iter()
                .find(|f| f.file == "2026-08-31")
                .and_then(|f| f.first_ts.clone()),
            Some("2026-08-23T14:00:00.000Z".to_string()),
        );
    }

    /// The listing is ordered by `(period end, split)` — the trap where `.2` sorts before the
    /// plain name lexicographically while being the newer generation.
    #[test]
    fn the_file_listing_is_newest_period_first_with_current_at_the_head() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(dir.path(), "oxidant-2026-08-23.log", &LINES);
        write(dir.path(), "oxidant-2026-08-23.2.log", &LINES);
        write(dir.path(), "oxidant-2026-09-01.log", &LINES);
        write(dir.path(), crate::history::disk::LIVE_LOG, &LINES);
        // Not ours, and never listed.
        std::fs::write(dir.path().join("syslog"), b"x").expect("write");
        let files = list_files(dir.path());
        assert_eq!(
            files.iter().map(|f| f.file.as_str()).collect::<Vec<_>>(),
            vec!["current", "2026-09-01", "2026-08-23.2", "2026-08-23"],
        );
        assert!(!files[0].rolled, "current is the live file");
        assert!(files[1].rolled);
        assert_eq!(
            files[0].first_ts.as_deref(),
            Some("2026-08-23T14:00:00.000Z")
        );
        assert_eq!(
            files[0].last_ts.as_deref(),
            Some("2026-08-23T14:00:04.000Z")
        );
    }

    /// A converted file's bounds come from the **footer**, not from a scan.
    #[test]
    fn a_converted_file_lists_its_bounds_from_the_footer() {
        let dir = tempfile::tempdir().expect("tempdir");
        let text = write(dir.path(), "oxidant-2026-09-02.log", &LINES);
        super::super::columnar::convert(&text).expect("convert");
        let files = list_files(dir.path());
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].format, "parquet");
        assert_eq!(
            files[0].first_ts.as_deref(),
            Some("2026-08-23T14:00:00.000Z")
        );
        assert_eq!(
            files[0].last_ts.as_deref(),
            Some("2026-08-23T14:00:04.000Z")
        );
    }

    /// The ring answers the same filters, so the Observability pane's chips mean one thing
    /// whether it is reading memory or a file.
    #[test]
    fn the_ring_answers_the_same_filters() {
        let lines: Vec<String> = LINES.iter().map(|l| l.to_string()).collect();
        let page = filter_ring(&lines, &filter(Some("warn"), None, None), None, 100);
        assert_eq!(page.lines, vec![LINES[1], LINES[2], LINES[4]]);
        let page = filter_ring(&lines, &LogFilter::default(), None, 2);
        assert_eq!(page.lines, vec![LINES[4], LINES[5]], "newest two");
        assert_eq!(page.next_before, Some(4));
    }
}
