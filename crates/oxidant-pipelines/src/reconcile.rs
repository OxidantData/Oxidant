//! `oxidant pipeline reconcile` — a read-only drift report between a `postgres_cdc` source's
//! upstream tables and the lakehouse tables the pipeline merges them into.
//!
//! CDC drifts. A slot is dropped, WAL is recycled past `restart_lsn`, the publisher is restored
//! from a backup, someone writes to the target by hand. None of those announce themselves: the
//! pipeline keeps running and the target keeps looking healthy while it stops being true. This
//! module answers the one question that catches all of them — *does the target still say what
//! the source says?* — and answers it without changing anything on either side.
//!
//! Three things are worth reading before the code.
//!
//! **Both sides are compared in the target's own value space.** The source is read as Postgres
//! text and converted to Arrow through `postgres_cdc::text_column_to_arrow` — the connector's own
//! mapping, the one a micro-batch would have used — and the target's columns are cast to those
//! same Arrow types before either side is rendered. So a `numeric` that prints `1.50` in `psql`
//! and a `Decimal128(38,2)` in Delta compare equal, and a difference reported here is a difference
//! the stream would have written, not an artefact of two systems spelling one value two ways.
//!
//! **The sample is a window, not a random draw.** Both sides are walked in ascending key order
//! and cut at `--sample` keys, so two runs against an unchanged pair of tables produce the same
//! report. Each side's cut then bounds what the *other* side may be accused of — see
//! [`diff_keys`] — because past that key one side simply was not looked at, and reporting its
//! rows as drift would turn every table larger than the sample into a false alarm.
//!
//! **The key walk is over the key's text form.** Ordering has to agree across two engines with
//! different collations, so both sides order by the key cast to text under byte (`C`) collation
//! and the walk compares those strings. That is exact for the key types people actually use —
//! integers, text, uuid, date, numeric — and the types where a Postgres text form and an Arrow
//! one genuinely disagree (floats, timestamps, bytea) are refused up front by name rather than
//! reported as drift that is not there.
//!
//! `--repair` (`docs/postgres-cdc.md` §4's re-snapshot) is deliberately not here: this command
//! only reads. See the "Reconciliation" section of that document.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use chrono::{DateTime, SecondsFormat, Utc};
use oxidant_common::{Error, Result};
use oxidant_config::{auto_cdc_simple_column as simple_column, AutoCdcConfig};
use oxidant_loom::arrow::array::{Array, ArrayRef, StringArray};
use oxidant_loom::arrow::compute::cast;
use oxidant_loom::arrow::datatypes::DataType;
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::arrow::util::display::{ArrayFormatter, FormatOptions};
use oxidant_loom::Engine;
use oxidant_streaming::pg_replication::{quote_identifier, ControlConnection};
use oxidant_streaming::postgres_cdc::{
    introspect_read_only, text_column_to_arrow, ColumnSchema, TableSchema, SOURCE_NAME,
};
use oxidant_streaming::{postgres_cdc_pipeline_options, ConnectorLog, PostgresCdcOptions};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::auto_cdc::quote_ident;
use crate::cron::Cron;
use crate::runner::Plan;

/// Keys compared per table unless `--sample` widens it.
pub const DEFAULT_SAMPLE: usize = 10_000;

/// Every table was compared and every one of them is in sync.
pub const EXIT_IN_SYNC: i32 = 0;
/// The comparison ran and something differed. This is the one a CI step is written against.
pub const EXIT_DRIFT: i32 = 1;
/// The comparison could not be run — an unreachable publisher, a `--table` that names nothing, a
/// key type the walk refuses. Distinct from drift so a network blip does not read as data loss.
pub const EXIT_FAILED: i32 = 2;

/// Joins the columns of a composite key into one comparable string.
///
/// `0x01` rather than a printable character: byte-wise comparison of the joined form then orders
/// identically to tuple comparison of the parts, because no Postgres text value can contain a
/// byte below `0x01` (`NUL` is the only one, and `text` cannot hold it). A `-` or a `|` would
/// reverse the order of `("a","z")` and `("ab","a")`.
const KEY_SEPARATOR: char = '\u{1}';

/// How a NULL renders inside a hashed row, distinct from an empty string.
const NULL_SENTINEL: &str = "\u{2}NULL";

/// Key types whose Postgres text form and Arrow text form are the same string.
///
/// The key walk orders both sides by this text, so a type where the two disagree would not just
/// mis-render a key — it would put the two walks in different orders and report the whole table
/// as drifted. Refusing by name is the honest answer; a `float8` or `timestamptz` primary key is
/// rare enough that no one should be surprised, and `keys:` on the source can name a different
/// column when it happens.
///
/// `Boolean` earns its place through the *cast*, not through Postgres' output function. `boolout`
/// — what pgoutput sends and what `psql` prints, and what the connector's own value parser reads
/// — spells `t`/`f`, which would not match the target's `true`/`false`; but `bool::text` is a
/// different function and spells `true`/`false`, and the key walk only ever reads the cast. See
/// [`source_key_text`], which is where that distinction is kept, and the tests that hold it.
fn key_type_is_walkable(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int32
            | DataType::Int64
            | DataType::Utf8
            | DataType::Date32
            | DataType::Decimal128(_, _)
    )
}

// ---------------------------------------------------------------------------------------------
// The diff walker
// ---------------------------------------------------------------------------------------------

/// One sampled row: its key, and a hash of everything else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRow {
    pub key: String,
    /// FNV-1a over the canonical rendering of the non-key columns. `None` when there are no
    /// non-key columns to compare, in which case a key present on both sides is simply in sync.
    pub hash: Option<u64>,
}

impl KeyRow {
    pub fn new(key: impl Into<String>, hash: Option<u64>) -> Self {
        Self {
            key: key.into(),
            hash,
        }
    }
}

/// One side's sampled window, in ascending key order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeyWindow {
    pub rows: Vec<KeyRow>,
    /// True when the walk stopped at the sample limit rather than at the end of the table.
    pub truncated: bool,
}

impl KeyWindow {
    /// The last key in the window, which is where a truncated walk stopped looking.
    fn last_key(&self) -> Option<&str> {
        self.rows.last().map(|r| r.key.as_str())
    }
}

/// What the walk found, per drift class. Keys are listed in ascending order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeyDiff {
    /// Keys present on both sides whose non-key columns were compared.
    pub compared: usize,
    /// In the source, not in the target — the classic "the stream missed a change".
    pub missing_in_target: Vec<String>,
    /// In the target, not in the source — phantoms: rows a delete never reached.
    pub missing_in_source: Vec<String>,
    /// Present on both sides with different contents.
    pub hash_mismatches: Vec<String>,
    /// The key the comparison stopped at, when a sample limit cut it short.
    pub window_end: Option<String>,
}

impl KeyDiff {
    pub fn is_clean(&self) -> bool {
        self.missing_in_target.is_empty()
            && self.missing_in_source.is_empty()
            && self.hash_mismatches.is_empty()
    }

    pub fn drift_count(&self) -> usize {
        self.missing_in_target.len() + self.missing_in_source.len() + self.hash_mismatches.len()
    }
}

/// Merge-walk two key-ordered windows and classify every difference.
///
/// Both windows must be sorted ascending by `key`; the fetch queries order them and the walk
/// relies on it, so an unsorted input is a bug in the caller rather than something to re-sort
/// here (re-sorting would hide a mis-ordered query behind a plausible-looking report).
///
/// A window cut at the sample limit bounds what the **other** side may be accused of, and only
/// that side. "This key is missing from the target" is a claim about the target's window: it
/// holds for any key below the last one the target read, whether or not the source read further.
/// The reverse claim is bounded by the source's window the same way. Collapsing both onto one
/// `min` bound — the obvious-looking simplification — would silently drop real drift in the band
/// between the two cuts, which is exactly the band a large table spends its whole sample in.
pub fn diff_keys(source: &KeyWindow, target: &KeyWindow) -> KeyDiff {
    // A side that reached the end of its table bounds nothing: everything past its last key
    // really is absent, not merely unread.
    let source_read_to = source.truncated.then(|| source.last_key()).flatten();
    let target_read_to = target.truncated.then(|| target.last_key()).flatten();
    let mut diff = KeyDiff {
        // Where full coverage stops — the earlier of the two cuts, since past it one side is
        // unknown. Reported so a truncated report never reads as a complete one.
        window_end: [source_read_to, target_read_to]
            .into_iter()
            .flatten()
            .min()
            .map(str::to_string),
        ..Default::default()
    };

    let (mut s, mut t) = (0, 0);
    let absent_from_target = |diff: &mut KeyDiff, key: &String| {
        if target_read_to.map_or(true, |read_to| key.as_str() <= read_to) {
            diff.missing_in_target.push(key.clone());
        }
    };
    let absent_from_source = |diff: &mut KeyDiff, key: &String| {
        if source_read_to.map_or(true, |read_to| key.as_str() <= read_to) {
            diff.missing_in_source.push(key.clone());
        }
    };
    while s < source.rows.len() || t < target.rows.len() {
        match (source.rows.get(s), target.rows.get(t)) {
            (None, None) => break,
            (Some(row), None) => {
                absent_from_target(&mut diff, &row.key);
                s += 1;
            }
            (None, Some(row)) => {
                absent_from_source(&mut diff, &row.key);
                t += 1;
            }
            (Some(source_row), Some(target_row)) => match source_row.key.cmp(&target_row.key) {
                std::cmp::Ordering::Less => {
                    absent_from_target(&mut diff, &source_row.key);
                    s += 1;
                }
                std::cmp::Ordering::Greater => {
                    absent_from_source(&mut diff, &target_row.key);
                    t += 1;
                }
                std::cmp::Ordering::Equal => {
                    diff.compared += 1;
                    // `None` on either side means there was nothing to compare — a table that is
                    // all key columns, or a column the target does not have. Present on both
                    // sides is the whole verdict there.
                    if let (Some(a), Some(b)) = (source_row.hash, target_row.hash) {
                        if a != b {
                            diff.hash_mismatches.push(source_row.key.clone());
                        }
                    }
                    s += 1;
                    t += 1;
                }
            },
        }
    }
    diff
}

/// FNV-1a 64 over a row's canonical rendering.
///
/// Only ever compared against another hash produced in the same process from the same renderer,
/// so a stable-across-releases digest buys nothing; 64 bits keeps a 10k-key sample at 80 KiB and
/// makes a collision (~3e-12 at that size) far less likely than the drift being looked for.
fn row_hash(values: &[String]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            hash = (hash ^ u64::from(KEY_SEPARATOR as u8)).wrapping_mul(0x100_0000_01b3);
        }
        for byte in value.as_bytes() {
            hash = (hash ^ u64::from(*byte)).wrapping_mul(0x100_0000_01b3);
        }
    }
    hash
}

// ---------------------------------------------------------------------------------------------
// The report
// ---------------------------------------------------------------------------------------------

/// One pipeline table's verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableReport {
    /// The pipeline table, as `tables:` names it.
    pub table: String,
    /// The upstream `schema.table` names its source replicates.
    pub upstream: Vec<String>,
    /// `catalog.schema.table` in the lakehouse.
    pub target: String,
    /// The row's identity, as the source resolved it (primary key, or the `keys:` override).
    pub keys: Vec<String>,
    /// `count(*)` on the upstream tables, summed. `None` when the source could not be read —
    /// unknown and zero are different answers, and the report presents this one as fact.
    pub source_rows: Option<u64>,
    /// `count(*)` on the target, or `None` when the target does not exist yet.
    pub target_rows: Option<u64>,
    /// Why the target could not be read, when it could not be.
    pub target_error: Option<String>,
    /// Why the *source* could not be read, when it could not be.
    ///
    /// Mirrors [`Self::target_error`] rather than aborting the whole run: the command's premise is
    /// "run this across N tables in CI", and an unreachable publisher for one table used to
    /// discard every other table's report along with it.
    pub source_error: Option<String>,
    pub sample: usize,
    pub source_sampled: usize,
    pub target_sampled: usize,
    /// Source columns the target does not have, excluded from the row hash and reported as drift.
    pub missing_columns: Vec<String>,
    /// Source columns `auto_cdc` does not project into the target, so their absence is not drift.
    /// Reported because a column that was never compared should not read as one that matched.
    pub excluded_columns: Vec<String>,
    pub diff: KeyDiff,
}

impl TableReport {
    /// One table the reconcile could not read at all.
    fn failed(plan: &Plan<'_>, name: &str, upstream: Vec<String>, error: String) -> Self {
        Self {
            table: name.to_string(),
            upstream,
            target: plan.target_of(name),
            keys: Vec::new(),
            source_rows: None,
            target_rows: None,
            target_error: None,
            source_error: Some(error),
            sample: 0,
            source_sampled: 0,
            target_sampled: 0,
            missing_columns: Vec::new(),
            excluded_columns: Vec::new(),
            diff: KeyDiff::default(),
        }
    }

    /// Upstream rows minus target rows: positive means the target is behind. `None` when either
    /// side's count is unknown, which is not the same as a drift of zero.
    pub fn row_count_drift(&self) -> Option<i64> {
        Some(self.source_rows? as i64 - self.target_rows? as i64)
    }

    /// Whether this table's own comparison could not be run.
    pub fn errored(&self) -> bool {
        self.source_error.is_some()
    }

    /// The drift classes this table hit, or `["in_sync"]`.
    pub fn verdicts(&self) -> Vec<&'static str> {
        if self.errored() {
            // Not a drift class: nothing was compared, so nothing can be said about the target.
            return vec!["source_error"];
        }
        let mut out = Vec::new();
        if self.target_rows.is_none() {
            out.push("target_missing");
        }
        if !self.missing_columns.is_empty() {
            out.push("schema_drift");
        }
        if self.row_count_drift().is_some_and(|drift| drift != 0) {
            out.push("row_count_drift");
        }
        if !self.diff.is_clean() {
            out.push("key_drift");
        }
        if out.is_empty() {
            out.push("in_sync");
        }
        out
    }

    /// Whether this table *was* compared and differed. A table that could not be read is
    /// [`Self::errored`], which is a different answer and a different exit code.
    pub fn drifted(&self) -> bool {
        !self.errored() && self.verdicts() != ["in_sync"]
    }
}

/// The whole run's verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileReport {
    pub pipeline: String,
    pub tables: Vec<TableReport>,
}

impl ReconcileReport {
    pub fn drifted(&self) -> usize {
        self.tables.iter().filter(|t| t.drifted()).count()
    }

    /// How many tables could not be compared at all.
    pub fn errored(&self) -> usize {
        self.tables.iter().filter(|t| t.errored()).count()
    }

    /// [`EXIT_IN_SYNC`], [`EXIT_DRIFT`] or [`EXIT_FAILED`] — the contract a cron job or a CI step
    /// reads, and the one `docs/cli.md` and `docs/pipelines.md` state.
    ///
    /// The two non-zero answers are kept apart because they call for different things. "The target
    /// no longer says what the source says" is a data problem someone has to look at; "the
    /// publisher was unreachable" is a network blip, and a CI step written as
    /// `reconcile || page_the_data_team` should not page for one. `1` is drift and only drift; any
    /// operational failure — an unreachable publisher, a `--table` typo, an unwalkable key type —
    /// is `2`, whether it comes back as a per-table `source_error` or as an error from the command
    /// itself. Failure outranks drift when a run hit both: the run was incomplete, so its
    /// "no drift here" is only a claim about the tables it managed to read.
    pub fn exit_code(&self) -> i32 {
        if self.errored() > 0 {
            EXIT_FAILED
        } else if self.drifted() > 0 {
            EXIT_DRIFT
        } else {
            EXIT_IN_SYNC
        }
    }

    /// The whole report, as the CLI prints it.
    pub fn render(&self) -> String {
        use std::fmt::Write;

        let mut out = String::new();
        let _ = writeln!(
            out,
            "reconcile pipeline `{}` — {} table(s)\n",
            self.pipeline,
            self.tables.len()
        );
        for table in &self.tables {
            let _ = writeln!(out, "table: {}", table.table);
            let _ = writeln!(out, "  source:  {}", table.upstream.join(", "));
            let _ = writeln!(out, "  target:  {}", table.target);
            if !table.keys.is_empty() {
                let _ = writeln!(out, "  keys:    {}", table.keys.join(", "));
            }
            if let Some(error) = &table.source_error {
                // Nothing below this line was measured, so none of it is printed: a zero here
                // would read as a count rather than as an absence.
                let _ = writeln!(out, "  error:   {error}");
                let _ = writeln!(out, "  verdict: {}\n", table.verdicts().join(", "));
                continue;
            }
            let source_rows = table
                .source_rows
                .map_or_else(|| "unknown".to_string(), |rows| rows.to_string());
            match (table.target_rows, &table.target_error) {
                (Some(target_rows), _) => {
                    let drift = table.row_count_drift().unwrap_or(0);
                    let _ = writeln!(
                        out,
                        "  rows:    source {}   target {}   drift {}{}",
                        source_rows,
                        target_rows,
                        if drift > 0 { "+" } else { "" },
                        drift
                    );
                }
                (None, Some(error)) => {
                    let _ = writeln!(
                        out,
                        "  rows:    source {source_rows}   target unreadable ({error})"
                    );
                }
                (None, None) => {}
            }
            let _ = writeln!(
                out,
                "  sampled: {} source / {} target key(s), limit {}{}",
                table.source_sampled,
                table.target_sampled,
                table.sample,
                match &table.diff.window_end {
                    Some(end) => format!(", compared up to key {}", show_key(end)),
                    None => String::new(),
                }
            );
            if !table.excluded_columns.is_empty() {
                let _ = writeln!(
                    out,
                    "    not compared      {:<6} {} (auto_cdc does not project them)",
                    table.excluded_columns.len(),
                    table.excluded_columns.join(", ")
                );
            }
            if !table.missing_columns.is_empty() {
                let _ = writeln!(
                    out,
                    "    missing_columns    {:<6} {}",
                    table.missing_columns.len(),
                    table.missing_columns.join(", ")
                );
            }
            for (label, keys) in [
                ("missing_in_target", &table.diff.missing_in_target),
                ("missing_in_source", &table.diff.missing_in_source),
                ("hash_mismatches", &table.diff.hash_mismatches),
            ] {
                if keys.is_empty() {
                    continue;
                }
                let _ = writeln!(out, "    {label:<18} {:<6} {}", keys.len(), sample_of(keys));
            }
            let _ = writeln!(out, "  verdict: {}\n", table.verdicts().join(", "));
        }
        let (drifted, errored, total) = (self.drifted(), self.errored(), self.tables.len());
        if errored > 0 {
            // The failure leads, because it is what makes the rest of the summary partial: the
            // tables that did read are still worth having, and are still reported above.
            let _ = writeln!(
                out,
                "summary: FAILED — {errored} of {total} table(s) could not be read, {drifted} of \
                 the rest drifted"
            );
        } else if drifted > 0 {
            let _ = writeln!(out, "summary: DRIFT — {drifted} of {total} table(s) differ");
        } else {
            let _ = writeln!(out, "summary: in sync — {total} table(s) clean");
        }
        out
    }

    /// The per-table verdict, appended to each connector's own JSONL log.
    ///
    /// `docs/postgres-cdc.md` §6 lists `reconcile` among the events an operator reads after the
    /// fact, and the platform console reads the same file — so a scheduled run leaves a record
    /// where the rest of the connector's history is, not only on whatever terminal it ran from.
    fn log_events(&self, plan: &Plan<'_>) {
        for table in &self.tables {
            connector_log(plan, &table.table).event(
                "reconcile",
                json!({
                    "verdict": table.verdicts().join(","),
                    "upstream": table.upstream,
                    "target": table.target,
                    "error": table.source_error,
                    "source_rows": table.source_rows,
                    "target_rows": table.target_rows,
                    "sampled": table.source_sampled,
                    "missing_in_target": table.diff.missing_in_target.len(),
                    "missing_in_source": table.diff.missing_in_source.len(),
                    "hash_mismatches": table.diff.hash_mismatches.len(),
                }),
            );
        }
    }
}

/// The connector log for one pipeline table, opened where the runner opens it.
///
/// Goes through [`postgres_cdc_pipeline_options`] rather than joining `logs/` here, so reconcile
/// can never write beside the file the running pipeline writes.
fn connector_log(plan: &Plan<'_>, table: &str) -> ConnectorLog {
    let mut options: BTreeMap<String, String> = plan
        .table(table)
        .and_then(|t| t.source.as_ref())
        .map(|s| s.options.clone())
        .unwrap_or_default();
    postgres_cdc_pipeline_options(
        &mut options,
        Path::new(plan.pipeline.checkpoints.trim_end_matches('/')),
        table,
    );
    ConnectorLog::new(options.get(LOG_DIR_OPTION).map(Path::new), table)
}

/// Where [`postgres_cdc_pipeline_options`] puts the log directory it injects.
const LOG_DIR_OPTION: &str = "oxidant.connector.log_dir";

/// Every pipeline table whose source is `postgres_cdc`, in declaration order.
fn cdc_table_names(plan: &Plan<'_>) -> Vec<String> {
    plan.config
        .tables
        .iter()
        .filter(|t| {
            t.source
                .as_ref()
                .is_some_and(|s| s.format.trim().eq_ignore_ascii_case(SOURCE_NAME))
        })
        .map(|t| t.name.trim().to_string())
        .collect()
}

/// The first few keys of a drift class, so a report stays readable at 10k.
fn sample_of(keys: &[String]) -> String {
    const SHOWN: usize = 5;
    let head: Vec<String> = keys.iter().take(SHOWN).map(|k| show_key(k)).collect();
    if keys.len() > SHOWN {
        format!("{}, … (+{} more)", head.join(", "), keys.len() - SHOWN)
    } else {
        head.join(", ")
    }
}

/// A composite key back in a form a human can paste into a `WHERE` clause.
fn show_key(key: &str) -> String {
    key.split(KEY_SEPARATOR).collect::<Vec<_>>().join(" | ")
}

// ---------------------------------------------------------------------------------------------
// Running one
// ---------------------------------------------------------------------------------------------

/// What `reconcile` was asked to compare.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileOptions {
    /// Restrict to these tables. Each entry names either a pipeline table or an upstream
    /// `schema.table`; empty means every `postgres_cdc` table in the pipeline.
    pub tables: Vec<String>,
    /// Keys walked per table.
    pub sample: usize,
}

impl Default for ReconcileOptions {
    fn default() -> Self {
        Self {
            tables: Vec::new(),
            sample: DEFAULT_SAMPLE,
        }
    }
}

/// Compare every `postgres_cdc` table in the pipeline against its lakehouse target.
///
/// Reads only: no slot is opened, no publication is created, nothing is written on either side.
pub async fn reconcile(
    engine: &Engine,
    plan: &Plan<'_>,
    options: &ReconcileOptions,
) -> Result<ReconcileReport> {
    let sample = options.sample.max(1);
    let cdc_tables = cdc_table_names(plan);
    if cdc_tables.is_empty() {
        return Err(Error::Io(format!(
            "pipeline `{}` declares no `format: {SOURCE_NAME}` table, so there is nothing to \
             reconcile — see docs/postgres-cdc.md §4",
            plan.pipeline.name
        )));
    }

    let mut reports = Vec::new();
    // Which `--table` entries turned out to name something. A typo among good names has to be an
    // error: silently reconciling the tables that did match would report "in sync" for a run that
    // never looked at the table the operator asked about.
    let mut matched: BTreeSet<&str> = BTreeSet::new();
    let mut known: BTreeSet<String> = BTreeSet::new();
    for name in cdc_tables {
        let source = plan
            .table(&name)
            .and_then(|t| t.source.as_ref())
            .expect("cdc_table_names only names tables that have a postgres_cdc source");
        // The source's own config block, resolved exactly as `run` resolves it — including
        // `password_env`, which reads the variable and fails by name when it is unset.
        let mut raw: BTreeMap<String, String> = source.options.clone();
        postgres_cdc_pipeline_options(
            &mut raw,
            Path::new(plan.pipeline.checkpoints.trim_end_matches('/')),
            &name,
        );
        let pg: HashMap<String, String> = raw.into_iter().collect();
        known.insert(name.clone());
        // `--table` entries naming the pipeline table are settled from the config, so they stay
        // accounted for even when this source turns out to be unreadable — "the publisher is
        // down" and "that name does not exist" are different complaints and the second one is
        // wrong here.
        let named_here: Vec<&str> = options
            .tables
            .iter()
            .map(|wanted| wanted.trim())
            .filter(|wanted| names_table(wanted, &name))
            .collect();

        let pg = match PostgresCdcOptions::from_options(&pg) {
            Ok(pg) => pg,
            Err(e) => {
                // The options are also what decide whether this table was asked for at all, so
                // this is reported only when it plainly was.
                if options.tables.is_empty() || !named_here.is_empty() {
                    matched.extend(named_here);
                    reports.push(TableReport::failed(plan, &name, Vec::new(), e.to_string()));
                }
                continue;
            }
        };
        known.extend(pg.tables.iter().cloned());

        // Decided from the config, before any connection: `--table other_source` must not fail
        // because *this* source's publisher happens to be unreachable.
        let wants_this_source = options.tables.is_empty()
            || !named_here.is_empty()
            || options.tables.iter().any(|wanted| declares(&pg, wanted));
        if !wants_this_source {
            continue;
        }

        // From here on, a failure is this table's own: it is reported as one and the remaining
        // tables are still compared. A run across N tables in CI is worth more with N-1 answers
        // and one named failure than with an error and nothing.
        let all = match introspect_read_only(&pg).await {
            Ok(all) => all,
            Err(e) => {
                matched.extend(named_here);
                matched.extend(
                    options
                        .tables
                        .iter()
                        .map(|wanted| wanted.trim())
                        .filter(|wanted| declares(&pg, wanted)),
                );
                reports.push(TableReport::failed(
                    plan,
                    &name,
                    pg.tables.clone(),
                    e.to_string(),
                ));
                continue;
            }
        };
        for table in &all {
            known.insert(table.qualified());
        }
        let (selected, hits) = select_upstream(&options.tables, &name, all);
        matched.extend(hits);
        if selected.is_empty() {
            continue;
        }
        let upstream: Vec<String> = selected.iter().map(TableSchema::qualified).collect();
        match reconcile_table(engine, plan, &name, &pg, &selected, sample).await {
            Ok(report) => reports.push(report),
            Err(e) => reports.push(TableReport::failed(plan, &name, upstream, e.to_string())),
        }
    }

    let unmatched: Vec<&str> = options
        .tables
        .iter()
        .map(|t| t.trim())
        .filter(|t| !matched.contains(t))
        .collect();
    if !unmatched.is_empty() {
        return Err(Error::Io(format!(
            "`--table {}` names no {SOURCE_NAME} table in pipeline `{}` (it has: {})",
            unmatched.join("`, `--table "),
            plan.pipeline.name,
            known.into_iter().collect::<Vec<_>>().join(", ")
        )));
    }
    let report = ReconcileReport {
        pipeline: plan.pipeline.name.clone(),
        tables: reports,
    };
    report.log_events(plan);
    Ok(report)
}

/// Which of a source's upstream tables a `--table` filter selects, and which entries it explains.
///
/// `--table` names either the pipeline table — in which case every upstream table of its source is
/// in scope — or one upstream `schema.table`, in which case only that one is. Both spellings are
/// useful and both are documented: the first is what `pipeline run --table` takes, the second is
/// what the source's `tables:` lists.
///
/// The returned set is every entry this source accounted for, and it is deliberately wider than
/// the selection: an entry naming an upstream table is explained whether or not the *pipeline*
/// table's name was also given. Marking only the entry that decided the branch is what produced
/// the report's one self-contradicting error — `--table sales_suppliers --table
/// public.sales_suppliers` took the pipeline-table branch, left the second entry unaccounted for,
/// and failed with "`--table public.sales_suppliers` names no postgres_cdc table … (it has:
/// public.sales_suppliers, sales_suppliers)", listing the very name it said did not exist.
fn select_upstream<'a>(
    wanted: &'a [String],
    pipeline_table: &str,
    all: Vec<TableSchema>,
) -> (Vec<TableSchema>, BTreeSet<&'a str>) {
    if wanted.is_empty() {
        return (all, BTreeSet::new());
    }
    let names_upstream = |entry: &str, table: &TableSchema| {
        names_table(entry, &table.qualified()) || names_table(entry, &table.table)
    };
    let mut matched: BTreeSet<&str> = BTreeSet::new();
    let mut whole_source = false;
    for entry in wanted {
        if names_table(entry, pipeline_table) {
            matched.insert(entry.trim());
            whole_source = true;
        }
        if all.iter().any(|table| names_upstream(entry, table)) {
            matched.insert(entry.trim());
        }
    }
    let selected = if whole_source {
        all
    } else {
        all.into_iter()
            .filter(|table| wanted.iter().any(|entry| names_upstream(entry, table)))
            .collect()
    };
    (selected, matched)
}

/// Whether a `--table` entry names `candidate`, ignoring case and surrounding space.
fn names_table(wanted: &str, candidate: &str) -> bool {
    wanted.trim().eq_ignore_ascii_case(candidate)
}

/// Whether a source's declared `tables:` could cover a `--table` entry, without asking the server.
///
/// Deliberately generous: `schema.*` has not been expanded yet, so any entry in that schema is a
/// maybe. Being wrong here only costs one introspection round trip, while being *too* strict
/// would skip the table the operator asked about and report a clean run over the rest.
fn declares(pg: &PostgresCdcOptions, wanted: &str) -> bool {
    let wanted = wanted.trim();
    let (wanted_schema, wanted_table) = match wanted.split_once('.') {
        Some((schema, table)) => (Some(schema), table),
        None => (None, wanted),
    };
    pg.tables.iter().any(|entry| {
        let (schema, table) = entry.split_once('.').unwrap_or(("public", entry));
        if wanted_schema.is_some_and(|w| !w.eq_ignore_ascii_case(schema)) {
            return false;
        }
        table == "*" || table.eq_ignore_ascii_case(wanted_table)
    })
}

/// One pipeline table against the union of its source's upstream tables.
async fn reconcile_table(
    engine: &Engine,
    plan: &Plan<'_>,
    name: &str,
    pg: &PostgresCdcOptions,
    upstream: &[TableSchema],
    sample: usize,
) -> Result<TableReport> {
    let first = upstream.first().expect("callers pass a non-empty slice");
    let keys = first.keys.clone();
    if keys.is_empty() {
        return Err(Error::Plan(format!(
            "postgres_cdc table `{name}`: `{}` has no primary key and the source names no \
             `keys:`, so there is no row identity to reconcile against. Add a primary key, or \
             name the identity with `keys:` in the source's `options:`.",
            first.qualified()
        )));
    }
    let (key_columns, value_columns) = split_columns(first, &keys, name)?;
    // What the *merge* was configured to write is what the target owes; a column `auto_cdc`
    // projects away is absent on purpose and is not drift.
    let (value_columns, excluded_columns) = projected_value_columns(
        plan.table(name).and_then(|t| t.auto_cdc.as_ref()),
        &value_columns,
    );

    let control = pg.connect.connect_control().await?;
    refuse_overlapping_keys(&control, upstream, &key_columns, name).await?;
    let mut source_rows: u64 = 0;
    for table in upstream {
        // Every other unknown in this module is carried as an unknown; a count that did not come
        // back must not be the one that quietly reads as zero, because zero here surfaces as a
        // large negative `row_count_drift` — a wrong number presented as a measurement.
        let counted = control
            .scalar(
                &format!(
                    "SELECT count(*)::text FROM {}.{}",
                    quote_identifier(&table.schema),
                    quote_identifier(&table.table)
                ),
                &[],
            )
            .await?;
        source_rows += counted
            .as_deref()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .ok_or_else(|| {
                Error::Execution(format!(
                    "reconcile: `count(*)` on `{}` came back as {}, which is not a row count",
                    table.qualified(),
                    counted.map_or("no row at all".to_string(), |v| format!("`{v}`"))
                ))
            })?;
    }

    let target = plan.target_of(name);
    let target_rows = count_rows(engine, &target).await.map_err(|e| e.to_string());

    // What the target actually has decides what either side hashes. A column the target is
    // missing has to come out of *both* row hashes, or every row in the table reads as a content
    // mismatch and the one real finding — the dropped column — is buried under it.
    let mut missing_columns = Vec::new();
    if matches!(target_rows, Ok(rows) if rows > 0) {
        let present = target_columns(engine, &target).await?;
        for key in &key_columns {
            if !present.iter().any(|c| c.eq_ignore_ascii_case(&key.name)) {
                return Err(Error::Execution(format!(
                    "reconcile: target `{target}` has no `{}` column, which is part of the row \
                     identity — there is no way to line its rows up against the source. \
                     Re-create the target from the current source schema.",
                    key.name
                )));
            }
        }
        missing_columns = value_columns
            .iter()
            .filter(|c| !present.iter().any(|p| p.eq_ignore_ascii_case(&c.name)))
            .map(|c| c.name.clone())
            .collect();
    }
    let compared: Vec<&ColumnSchema> = value_columns
        .iter()
        .copied()
        .filter(|c| {
            !missing_columns
                .iter()
                .any(|m| m.eq_ignore_ascii_case(&c.name))
        })
        .collect();

    let source_window = source_window(&control, upstream, &key_columns, &compared, sample).await?;
    // A target that will not even count is not one to sample: every source key is missing from
    // it, which is exactly what an empty window produces.
    let target_window = match target_rows {
        Ok(0) | Err(_) => KeyWindow::default(),
        Ok(_) => target_window(engine, &target, &key_columns, &compared, sample).await?,
    };

    let diff = diff_keys(&source_window, &target_window);
    Ok(TableReport {
        table: name.to_string(),
        upstream: upstream.iter().map(TableSchema::qualified).collect(),
        target,
        keys,
        source_rows: Some(source_rows),
        target_rows: target_rows.as_ref().ok().copied(),
        target_error: target_rows.err(),
        source_error: None,
        sample,
        source_sampled: source_window.rows.len(),
        target_sampled: target_window.rows.len(),
        missing_columns,
        excluded_columns,
        diff,
    })
}

/// The source value columns the target is *supposed* to hold, and the ones it is not.
///
/// `auto_cdc` projects the change stream on its way into the target — `column_list` names what to
/// keep, `except_column_list` what to drop (`oxidant-config`'s `AutoCdcConfig`, applied by
/// [`crate::auto_cdc::output_columns`]). Without reading that block, a source column the merge was
/// *configured* to drop comes back as `missing_columns` → `schema_drift` → exit 1, on every run,
/// forever, for a pipeline doing exactly what it was told. A CI step wired to that exit code is
/// red from the first run and gets muted, which costs more than the check was worth.
///
/// The rules mirror `output_columns`, including its case-insensitive, backtick-stripping name
/// resolution, and stop where reconcile's question stops: this only decides which *source* columns
/// the target owes. The metadata columns the merge adds on its own (`__oxidant_lsn` and friends)
/// are not source columns at all, so they never reach here — they are plumbing, never drift.
fn projected_value_columns<'a>(
    auto_cdc: Option<&AutoCdcConfig>,
    value_columns: &[&'a ColumnSchema],
) -> (Vec<&'a ColumnSchema>, Vec<String>) {
    let names = |list: &[String]| -> BTreeSet<String> {
        list.iter()
            .map(|c| {
                simple_column(c)
                    .unwrap_or_else(|| c.trim().to_string())
                    .to_ascii_lowercase()
            })
            .collect()
    };
    let keeps: Box<dyn Fn(&ColumnSchema) -> bool> = match auto_cdc {
        Some(config) => match (&config.column_list, &config.except_column_list) {
            // An explicit list is the whole target: a source column it does not name is not the
            // target's to hold. (`column_list` and `except_column_list` are mutually exclusive;
            // the config layer rejects both, and naming the list first matches `output_columns`.)
            (Some(list), _) => {
                let listed = names(list);
                Box::new(move |c| listed.contains(&c.name.to_ascii_lowercase()))
            }
            (None, Some(except)) => {
                let dropped = names(except);
                Box::new(move |c| !dropped.contains(&c.name.to_ascii_lowercase()))
            }
            (None, None) => Box::new(|_| true),
        },
        None => Box::new(|_| true),
    };
    let mut expected = Vec::with_capacity(value_columns.len());
    let mut excluded = Vec::new();
    for column in value_columns {
        if keeps(column) {
            expected.push(*column);
        } else {
            excluded.push(column.name.clone());
        }
    }
    (expected, excluded)
}

/// Split the source's columns into the key walk's columns and the ones the row hash covers.
fn split_columns<'a>(
    table: &'a TableSchema,
    keys: &[String],
    name: &str,
) -> Result<(Vec<&'a ColumnSchema>, Vec<&'a ColumnSchema>)> {
    let mut key_columns = Vec::new();
    for key in keys {
        let column = table
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(key))
            .ok_or_else(|| {
                Error::Plan(format!(
                    "postgres_cdc table `{name}`: key column `{key}` is not among the columns \
                     `{}` publishes — `exclude_columns:` cannot exclude a key.",
                    table.qualified()
                ))
            })?;
        if !key_type_is_walkable(&column.data_type) {
            return Err(Error::Unsupported(format!(
                "postgres_cdc table `{name}`: reconcile cannot walk a `{:?}` key (`{key}` on \
                 `{}`). The walk orders both sides by the key's text form, and Postgres and the \
                 lakehouse do not spell that type the same way — comparing them would report \
                 drift that is not there. Name an integer, text, uuid, date or numeric identity \
                 with `keys:` in the source's `options:`.",
                column.data_type,
                table.qualified()
            )));
        }
        key_columns.push(column);
    }
    let value_columns = table
        .columns
        .iter()
        .filter(|c| !keys.iter().any(|k| k.eq_ignore_ascii_case(&c.name)))
        .collect();
    Ok((key_columns, value_columns))
}

/// The probe that finds a key value living in more than one of a source's upstream tables.
///
/// `GROUP BY … HAVING count(*) > 1` over the same union the walk reads, projected through the same
/// [`source_key_text`], so a hit is a duplicate *as the walk would see it* rather than as Postgres
/// would compare the raw columns.
fn duplicate_key_sql(upstream: &[TableSchema], key_columns: &[&ColumnSchema]) -> String {
    let projection: Vec<String> = key_columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{} AS k{i}", source_key_text(c)))
        .collect();
    let branches: Vec<String> = upstream
        .iter()
        .map(|t| {
            format!(
                "SELECT {} FROM {}.{}",
                projection.join(", "),
                quote_identifier(&t.schema),
                quote_identifier(&t.table)
            )
        })
        .collect();
    let group: Vec<String> = (0..key_columns.len()).map(|i| format!("k{i}")).collect();
    format!(
        "SELECT {} FROM ({}) s GROUP BY {} HAVING count(*) > 1 LIMIT 1",
        group.join(", "),
        branches.join(" UNION ALL "),
        group.join(", ")
    )
}

/// Refuse a multi-table source whose upstream tables share key values.
///
/// A `postgres_cdc` source may declare several upstream tables, and the connector only requires
/// that they share a *shape* — same column names and types. Reconcile sums their `count(*)` into
/// one `source_rows` and `UNION ALL`s them into one key window, while `auto_cdc` merges the
/// combined stream into **one** target keyed on `keys:`, which therefore holds one row per key
/// value. Two upstream tables each with `id bigint primary key` starting at 1 is the ordinary
/// case, not a pathological one, and it breaks both comparisons at once: the sum over-counts
/// against the target's deduplicated rows, and the merge walk — which requires *strictly*
/// ascending keys, not merely sorted ones — reports the duplicate as missing from a target that
/// holds it, then reports whichever duplicate sorted first as the one whose contents to compare.
///
/// Refusing is the honest answer, and the same one the module gives an unwalkable key type: the
/// alternative is a report that quietly answers a different question. Namespacing the keys per
/// upstream table would make the walk self-consistent but not *true* — the target has no column
/// saying which table a row came from, so there is nothing to namespace the target side by.
/// (`docs/postgres-cdc.md` §9 already lists a multi-table single source as a v1 non-goal.)
///
/// One grouped scan of the union, and only for a source that declares more than one table.
async fn refuse_overlapping_keys(
    control: &ControlConnection,
    upstream: &[TableSchema],
    key_columns: &[&ColumnSchema],
    name: &str,
) -> Result<()> {
    if upstream.len() < 2 {
        return Ok(());
    }
    let duplicate = control
        .query(&duplicate_key_sql(upstream, key_columns), &[])
        .await?;
    let Some(row) = duplicate.first() else {
        return Ok(());
    };
    let key = row
        .iter()
        .map(|v| v.as_deref().unwrap_or(NULL_SENTINEL))
        .collect::<Vec<_>>()
        .join(" | ");
    Err(Error::Unsupported(format!(
        "postgres_cdc table `{name}`: its source replicates {} into one target keyed on `{}`, and they \
         do not have disjoint key values — `{key}` is in more than one of them. The merge keeps \
         one row per key, so the union of the sources holds more rows than the target ever \
         will: every comparison here would report drift that is not there. Declare one \
         `postgres_cdc` source per upstream table, or give the tables disjoint keys.",
        upstream
            .iter()
            .map(TableSchema::qualified)
            .collect::<Vec<_>>()
            .join(", "),
        key_columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    )))
}

/// One key column's source projection: the text the key walk compares and orders by.
///
/// One place, and a cast rather than a bare column reference, because *which* text form a value
/// takes is the whole basis of the walk. It matters most for `boolean`: Postgres has two spellings
/// of one, and only one of them matches the target.
///
/// - `boolout`, the type's output function — `t` / `f`. It is what a bare `SELECT` sends on the
///   wire, what `psql` prints, and what pgoutput carries, which is why the connector's own value
///   parser reads `t`/`f` (`postgres_cdc.rs`, `build_array`).
/// - `bool::text`, the cast — `true` / `false`. That is the target's spelling too: the target
///   query's `CAST(col AS VARCHAR)` reaches Arrow's `value_to_string`, which formats a
///   `BooleanArray` as `true`/`false`.
///
/// The walk reads the cast on both sides, so the two agree — including under the source's
/// `COLLATE "C"` ordering, where `false` sorts before `true` exactly as the target's byte order
/// puts them. Dropping the `::text` here (or reading the column through the wire's text format
/// instead) would put the two walks in different orders and report every row of a healthy
/// boolean-keyed table as both missing from the target and a phantom in it.
fn source_key_text(column: &ColumnSchema) -> String {
    format!("({})::text", quote_identifier(&column.name))
}

/// The upstream query the key walk reads: `sample` keys in ascending key order, with their
/// non-key columns as text.
fn source_window_sql(
    upstream: &[TableSchema],
    key_columns: &[&ColumnSchema],
    value_columns: &[&ColumnSchema],
    sample: usize,
) -> String {
    // Every column comes back as text: it is what `ControlConnection` returns, what pgoutput
    // sends, and therefore what the connector's own conversion table already reads.
    let projection: Vec<String> = key_columns
        .iter()
        .map(|c| source_key_text(c))
        .chain(
            value_columns
                .iter()
                .map(|c| format!("({})::text", quote_identifier(&c.name))),
        )
        .enumerate()
        .map(|(i, expr)| format!("{expr} AS c{i}"))
        .collect();
    let branches: Vec<String> = upstream
        .iter()
        .map(|t| {
            format!(
                "SELECT {} FROM {}.{}",
                projection.join(", "),
                quote_identifier(&t.schema),
                quote_identifier(&t.table)
            )
        })
        .collect();
    // `COLLATE "C"` is what makes the order byte-wise, and so the same order DataFusion puts the
    // target in. Under an `en_US` database collation `ORDER BY key` is *not* byte order, and the
    // two walks would interleave differently and report the whole table as drifted.
    let order: Vec<String> = (0..key_columns.len())
        .map(|i| format!("s.c{i} COLLATE \"C\" ASC NULLS LAST"))
        .collect();
    format!(
        "SELECT * FROM ({}) s ORDER BY {} LIMIT {}",
        branches.join(" UNION ALL "),
        order.join(", "),
        sample
    )
}

/// Read the first `sample` keys of the union of `upstream`, in key order, with their row hashes.
async fn source_window(
    control: &ControlConnection,
    upstream: &[TableSchema],
    key_columns: &[&ColumnSchema],
    value_columns: &[&ColumnSchema],
    sample: usize,
) -> Result<KeyWindow> {
    let sql = source_window_sql(upstream, key_columns, value_columns, sample);
    let rows = control.query(&sql, &[]).await?;
    let truncated = rows.len() >= sample;

    // Convert the non-key columns through the connector's own text→Arrow mapping, so the row
    // hash is over the values a micro-batch would have written rather than over Postgres' text.
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(value_columns.len());
    for (offset, column) in value_columns.iter().enumerate() {
        let index = key_columns.len() + offset;
        let values: Vec<Option<&str>> = rows
            .iter()
            .map(|row| row.get(index).and_then(Option::as_deref))
            .collect();
        arrays.push(text_column_to_arrow(
            &column.data_type,
            &column.name,
            &values,
        )?);
    }
    let hashes = hash_rows(&arrays, rows.len())?;

    let mut window = KeyWindow {
        rows: Vec::with_capacity(rows.len()),
        truncated,
    };
    for (index, row) in rows.iter().enumerate() {
        let key = row
            .iter()
            .take(key_columns.len())
            .map(|v| v.as_deref().unwrap_or(NULL_SENTINEL))
            .collect::<Vec<_>>()
            .join(&KEY_SEPARATOR.to_string());
        window
            .rows
            .push(KeyRow::new(key, hashes.get(index).copied()));
    }
    Ok(window)
}

/// `count(*)` on a lakehouse table.
async fn count_rows(engine: &Engine, target: &str) -> Result<u64> {
    let batches = engine
        .sql(&format!("SELECT count(*) AS n FROM {target}"))
        .await?;
    let opts = FormatOptions::default();
    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let formatter = ArrayFormatter::try_new(batch.column(0), &opts)
            .map_err(|e| Error::Execution(format!("reconcile: count on `{target}`: {e}")))?;
        return formatter
            .value(0)
            .to_string()
            .parse::<u64>()
            .map_err(|e| Error::Execution(format!("reconcile: count on `{target}`: {e}")));
    }
    Ok(0)
}

/// The target query the key walk reads: the same `sample` keys, in the same order.
fn target_window_sql(
    target: &str,
    key_columns: &[&ColumnSchema],
    value_columns: &[&ColumnSchema],
    sample: usize,
) -> String {
    // `CAST(... AS VARCHAR)` on the key mirrors the source's [`source_key_text`], so the two
    // walks agree on both the order and the identity of every key.
    let mut projection: Vec<String> = key_columns
        .iter()
        .map(|c| format!("CAST({} AS VARCHAR)", quote_ident(&c.name)))
        .collect();
    projection.extend(value_columns.iter().map(|c| quote_ident(&c.name)));
    let order: Vec<String> = (1..=key_columns.len())
        .map(|i| format!("{i} ASC NULLS LAST"))
        .collect();
    format!(
        "SELECT {} FROM {target} ORDER BY {} LIMIT {sample}",
        projection.join(", "),
        order.join(", ")
    )
}

/// Read the first `sample` keys of the lakehouse target, in the same order the source used.
async fn target_window(
    engine: &Engine,
    target: &str,
    key_columns: &[&ColumnSchema],
    value_columns: &[&ColumnSchema],
    sample: usize,
) -> Result<KeyWindow> {
    let sql = target_window_sql(target, key_columns, value_columns, sample);
    let batches = engine.sql(&sql).await?;

    let mut rows: Vec<KeyRow> = Vec::new();
    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        // Cast each value column to the type the source mapped it to before rendering, so the
        // two sides are compared in one type system rather than in two spellings of one.
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(value_columns.len());
        for (offset, column) in value_columns.iter().enumerate() {
            let array = batch.column(key_columns.len() + offset);
            let converted = if array.data_type() == &column.data_type {
                array.clone()
            } else {
                cast(array, &column.data_type).map_err(|e| {
                    Error::Execution(format!(
                        "reconcile: target `{target}` column `{}` is {:?}, which does not convert \
                         to the source's {:?}: {e}",
                        column.name,
                        array.data_type(),
                        column.data_type
                    ))
                })?
            };
            arrays.push(converted);
        }
        let hashes = hash_rows(&arrays, batch.num_rows())?;
        for (index, key) in batch_keys(batch, key_columns.len())?
            .into_iter()
            .enumerate()
        {
            rows.push(KeyRow::new(key, hashes.get(index).copied()));
        }
    }
    let truncated = rows.len() >= sample;
    Ok(KeyWindow { rows, truncated })
}

/// The target's column names, read from one row rather than from a catalog call.
///
/// `SELECT * … LIMIT 1` goes through the same planner the comparison queries do, so a table this
/// can read is a table those can read; a catalog lookup can succeed against metadata a scan then
/// fails on.
async fn target_columns(engine: &Engine, target: &str) -> Result<Vec<String>> {
    let batches = engine
        .sql(&format!("SELECT * FROM {target} LIMIT 1"))
        .await?;
    Ok(batches
        .first()
        .map(|batch| {
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        })
        .unwrap_or_default())
}

/// The leading `count` columns of a batch, joined into one comparable key per row.
///
/// The query casts each key to `VARCHAR`, but which Arrow string DataFusion hands back for that
/// is its business and has changed between releases (`Utf8`, `LargeUtf8`, `Utf8View`). Normalizing
/// to `Utf8` here rather than downcasting to whichever one is current keeps the walk working
/// across that; the cast is free when the array is already `Utf8`.
fn batch_keys(batch: &RecordBatch, count: usize) -> Result<Vec<String>> {
    let mut columns = Vec::with_capacity(count);
    for index in 0..count {
        let column = batch.column(index);
        let utf8 = if column.data_type() == &DataType::Utf8 {
            column.clone()
        } else {
            cast(column, &DataType::Utf8).map_err(|e| {
                Error::Execution(format!(
                    "reconcile: key column {index} came back as {:?}, which is not text: {e}",
                    column.data_type()
                ))
            })?
        };
        columns.push(
            utf8.as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    Error::Execution(
                        "reconcile: a key column did not come back as text; this is a bug in the \
                         reconcile query"
                            .into(),
                    )
                })?
                .clone(),
        );
    }
    let mut out = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        out.push(
            columns
                .iter()
                .map(|c| {
                    if c.is_null(row) {
                        NULL_SENTINEL.to_string()
                    } else {
                        c.value(row).to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join(&KEY_SEPARATOR.to_string()),
        );
    }
    Ok(out)
}

/// One hash per row over the canonical rendering of `arrays`.
///
/// `None` per row — an empty vector — when there is nothing to hash, which is what a table of
/// nothing but key columns looks like.
fn hash_rows(arrays: &[ArrayRef], rows: usize) -> Result<Vec<u64>> {
    if arrays.is_empty() {
        return Ok(Vec::new());
    }
    // A NULL and an empty string are different values and must hash differently.
    let opts = FormatOptions::default().with_null(NULL_SENTINEL);
    let formatters: Vec<ArrayFormatter> = arrays
        .iter()
        .map(|a| {
            ArrayFormatter::try_new(a, &opts)
                .map_err(|e| Error::Execution(format!("reconcile: render a value: {e}")))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut out = Vec::with_capacity(rows);
    for row in 0..rows {
        let values: Vec<String> = formatters
            .iter()
            .map(|f| f.value(row).to_string())
            .collect();
        out.push(row_hash(&values));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------------------------
// The persisted schedule
// ---------------------------------------------------------------------------------------------

/// `reconcile.json` in the pipeline's checkpoint directory.
///
/// Beside the checkpoints rather than in the config file on purpose: a schedule is operational
/// state, and `oxidant.yaml` is checked into a repository. Registering one from a laptop must not
/// mean a commit, and `pipeline run` must be able to pick it up without the file being reloaded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconcileSchedule {
    /// The config file the schedule was registered from, for an operator reading the file later.
    #[serde(default)]
    pub path: Option<String>,
    /// The cron expression, as typed.
    pub cron: String,
    /// `--table` filters carried through to each scheduled run.
    #[serde(default)]
    pub tables: Vec<String>,
    #[serde(default = "default_sample")]
    pub sample: usize,
    /// When the schedule was registered — the anchor before there has been a run.
    pub created: String,
    #[serde(default)]
    pub last_run: Option<String>,
    /// `in_sync` or `drift: N table(s)`, or the error a run failed with.
    #[serde(default)]
    pub last_result: Option<String>,
}

fn default_sample() -> usize {
    DEFAULT_SAMPLE
}

impl ReconcileSchedule {
    pub fn path_in(checkpoints: &str) -> PathBuf {
        Path::new(checkpoints.trim_end_matches('/')).join("reconcile.json")
    }

    /// The schedule, or `None` when there is none — an unreadable or corrupt file reads as none
    /// so a bad write can never stop a pipeline from running.
    pub fn load(checkpoints: &str) -> Option<Self> {
        let text = std::fs::read_to_string(Self::path_in(checkpoints)).ok()?;
        serde_json::from_str(&text).ok()
    }

    pub fn save(&self, checkpoints: &str) -> Result<()> {
        let path = Self::path_in(checkpoints);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| Error::Io(format!("create `{}`: {e}", parent.display())))?;
        }
        let text = serde_json::to_string_pretty(self)
            .map_err(|e| Error::Io(format!("encode the reconcile schedule: {e}")))?;
        std::fs::write(&path, text)
            .map_err(|e| Error::Io(format!("write `{}`: {e}", path.display())))
    }

    /// Remove the schedule, reporting whether there was one.
    pub fn remove(checkpoints: &str) -> Result<bool> {
        let path = Self::path_in(checkpoints);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(Error::Io(format!("remove `{}`: {e}", path.display()))),
        }
    }

    /// When the next firing is measured from: the last run, or registration before there is one.
    pub fn anchor(&self) -> DateTime<Utc> {
        self.last_run
            .as_deref()
            .or(Some(self.created.as_str()))
            .and_then(parse_time)
            .unwrap_or_else(Utc::now)
    }

    /// Whether a scheduled run is due at `now`.
    ///
    /// A cron expression that no longer parses reads as "not due" rather than as an error: the
    /// file is only ever written through [`Self::save`] after [`Cron::parse`] accepted it, so the
    /// only way to get here is a hand-edited file, and that must not stop the pipeline.
    pub fn is_due(&self, now: DateTime<Utc>) -> bool {
        self.is_due_since(None, now)
    }

    /// [`Self::is_due`] against a caller-held anchor as well as the file's.
    ///
    /// The scheduler holds the instant of its last run in memory and passes it here, because the
    /// file's anchor only advances if [`Self::save`] succeeded. A read-only checkpoint volume, a
    /// permissions change or an NFS blip makes that write fail — none of which should stop the
    /// pipeline, and none of which did — and with the file as the only anchor the schedule was
    /// still due on the very next pass. At the trigger intervals people actually use that is a
    /// `count(*)` plus a `--sample`-row ordered scan against the publisher several times a
    /// second, indefinitely, with a repeated `StatePersistFailed` line as the only symptom.
    ///
    /// The later of the two anchors wins, so a failed save degrades to "the schedule does not
    /// survive a restart" instead of "the schedule fires every pass".
    pub fn is_due_since(&self, in_memory: Option<DateTime<Utc>>, now: DateTime<Utc>) -> bool {
        let anchor = match in_memory {
            Some(remembered) => remembered.max(self.anchor()),
            None => self.anchor(),
        };
        Cron::parse(&self.cron).is_ok_and(|cron| cron.is_due(anchor, now))
    }

    /// Record what a run found, for `pipeline show` and for the next tick's anchor.
    ///
    /// `finished_at` is the instant the run *completed*, not the instant it began. Anchoring on
    /// the start makes a run longer than its own period due again the moment it ends — `*/5 * * *
    /// *` with a twelve-minute walk never stops running, and since the tick is awaited inline in
    /// the trigger loop, replication stops with it and the pipeline looks hung.
    pub fn record(&mut self, finished_at: DateTime<Utc>, result: String) {
        self.last_run = Some(finished_at.to_rfc3339_opts(SecondsFormat::Secs, true));
        self.last_result = Some(result);
    }

    /// The one-line result string a run records.
    pub fn result_of(report: &ReconcileReport) -> String {
        let total = report.tables.len();
        match (report.errored(), report.drifted()) {
            (0, 0) => "in_sync".to_string(),
            (0, drifted) => format!("drift: {drifted} of {total} table(s)"),
            (errored, 0) => format!("failed: {errored} of {total} table(s) could not be read"),
            (errored, drifted) => {
                format!("failed: {errored} of {total} table(s) could not be read; drift: {drifted}")
            }
        }
    }
}

fn parse_time(text: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

/// Register (or replace) the schedule, after checking the expression parses.
///
/// The schedule also goes into every `postgres_cdc` connector's log as a `reconcile` event, so
/// the file an operator already reads for this connector says when it is next going to be
/// checked — otherwise the only record of a schedule is a JSON file nobody thought to look for.
pub fn set_schedule(
    plan: &Plan<'_>,
    cron: &str,
    config_path: Option<&Path>,
    options: &ReconcileOptions,
) -> Result<ReconcileSchedule> {
    let checkpoints = plan.pipeline.checkpoints.as_str();
    let parsed = Cron::parse(cron)?;
    // Keep whatever the previous schedule learned: changing the expression does not un-run the
    // last reconcile, and losing its verdict would make `pipeline show` claim it never ran.
    let previous = ReconcileSchedule::load(checkpoints);
    let schedule = ReconcileSchedule {
        path: config_path
            .map(|p| p.display().to_string())
            .or_else(|| previous.as_ref().and_then(|p| p.path.clone())),
        cron: parsed.expr().to_string(),
        tables: options.tables.clone(),
        sample: options.sample,
        created: previous
            .as_ref()
            .map(|p| p.created.clone())
            .unwrap_or_else(|| Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)),
        last_run: previous.as_ref().and_then(|p| p.last_run.clone()),
        last_result: previous.as_ref().and_then(|p| p.last_result.clone()),
    };
    schedule.save(checkpoints)?;
    for table in cdc_table_names(plan) {
        connector_log(plan, &table).event(
            "reconcile",
            json!({
                "scheduled": schedule.cron,
                "sample": schedule.sample,
                "tables": schedule.tables,
                "next": parsed
                    .next_after(schedule.anchor())
                    .map(|t| t.to_rfc3339_opts(SecondsFormat::Secs, true)),
            }),
        );
    }
    Ok(schedule)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn window(keys: &[(&str, u64)], truncated: bool) -> KeyWindow {
        KeyWindow {
            rows: keys
                .iter()
                .map(|(k, h)| KeyRow::new(*k, Some(*h)))
                .collect(),
            truncated,
        }
    }

    #[test]
    fn two_identical_windows_are_in_sync() {
        let both = window(&[("1", 10), ("2", 20), ("3", 30)], false);
        let diff = diff_keys(&both, &both);
        assert!(diff.is_clean(), "{diff:?}");
        assert_eq!(diff.compared, 3);
        assert_eq!(diff.window_end, None);
    }

    #[test]
    fn each_drift_class_is_reported_under_its_own_name() {
        // 2 was never written to the target, 9 was never deleted from it, and 3's contents moved.
        let source = window(&[("1", 10), ("2", 20), ("3", 30)], false);
        let target = window(&[("1", 10), ("3", 99), ("9", 90)], false);
        let diff = diff_keys(&source, &target);
        assert_eq!(diff.missing_in_target, vec!["2"]);
        assert_eq!(diff.missing_in_source, vec!["9"]);
        assert_eq!(diff.hash_mismatches, vec!["3"]);
        assert_eq!(diff.compared, 2, "1 and 3 were on both sides");
        assert_eq!(diff.drift_count(), 3);
    }

    #[test]
    fn an_empty_target_makes_every_source_key_missing() {
        let source = window(&[("1", 10), ("2", 20)], false);
        let diff = diff_keys(&source, &KeyWindow::default());
        assert_eq!(diff.missing_in_target, vec!["1", "2"]);
        assert!(diff.missing_in_source.is_empty());
        assert_eq!(diff.compared, 0);
    }

    #[test]
    fn an_empty_source_makes_every_target_key_a_phantom() {
        let target = window(&[("1", 10)], false);
        let diff = diff_keys(&KeyWindow::default(), &target);
        assert_eq!(diff.missing_in_source, vec!["1"]);
        assert!(diff.missing_in_target.is_empty());
    }

    #[test]
    fn a_key_past_the_other_sides_cut_is_unread_rather_than_missing() {
        // Both sides stopped at the sample limit, at different keys. The target read up to 5, so
        // its lack of a 3 is a real finding; the source stopped at 3, so it cannot be accused of
        // lacking the target's 5 — nobody looked. Without that second bound every table larger
        // than the sample reports its whole tail as drift.
        let source = window(&[("1", 1), ("2", 2), ("3", 3)], true);
        let target = window(&[("1", 1), ("2", 2), ("5", 5)], true);
        let diff = diff_keys(&source, &target);
        assert_eq!(diff.missing_in_target, vec!["3"]);
        assert!(
            diff.missing_in_source.is_empty(),
            "5 is past the source's cut: {diff:?}"
        );
        assert_eq!(diff.compared, 2);
        assert_eq!(
            diff.window_end.as_deref(),
            Some("3"),
            "full coverage stops at the earlier of the two cuts"
        );
    }

    #[test]
    fn the_two_bounds_are_kept_apart_rather_than_collapsed_onto_the_lower_one() {
        // The mirror image, and the reason each side carries its own bound. The source read to
        // 5, so it can say the target's 2 and 3 are phantoms; the target only read to 3, so the
        // source's 4 and 5 are not its to answer for. One cut protects one direction.
        let source = window(&[("1", 1), ("4", 4), ("5", 5)], true);
        let target = window(&[("1", 1), ("2", 2), ("3", 3)], true);
        let diff = diff_keys(&source, &target);
        assert!(
            diff.missing_in_target.is_empty(),
            "4 and 5 are past the target's cut: {diff:?}"
        );
        assert_eq!(
            diff.missing_in_source,
            vec!["2", "3"],
            "both are below the source's cut"
        );
    }

    #[test]
    fn a_complete_window_bounds_nothing_even_when_the_other_side_was_cut() {
        // The source read its whole table; a key the target has past the source's last one is a
        // genuine phantom, not an artefact of the sample.
        let source = window(&[("1", 1)], false);
        let target = window(&[("1", 1), ("7", 7)], true);
        let diff = diff_keys(&source, &target);
        assert_eq!(diff.missing_in_source, vec!["7"]);
        assert_eq!(
            diff.window_end.as_deref(),
            Some("7"),
            "coverage past the target's own cut is still partial"
        );
    }

    #[test]
    fn rows_with_nothing_to_hash_are_in_sync_once_their_keys_line_up() {
        // A table that is all key columns has no non-key content to disagree about.
        let side = KeyWindow {
            rows: vec![KeyRow::new("1", None), KeyRow::new("2", None)],
            truncated: false,
        };
        let diff = diff_keys(&side, &side);
        assert!(diff.is_clean());
        assert_eq!(diff.compared, 2);
    }

    #[test]
    fn the_walk_never_loses_a_key_when_the_two_sides_interleave() {
        let source = window(&[("a", 1), ("c", 3), ("e", 5)], false);
        let target = window(&[("b", 2), ("d", 4), ("f", 6)], false);
        let diff = diff_keys(&source, &target);
        assert_eq!(diff.missing_in_target, vec!["a", "c", "e"]);
        assert_eq!(diff.missing_in_source, vec!["b", "d", "f"]);
        assert_eq!(diff.compared, 0);
    }

    #[test]
    fn the_row_hash_separates_columns_so_a_shifted_value_still_differs() {
        // Without a separator `["ab", "c"]` and `["a", "bc"]` hash the same, and a column swap
        // would read as in sync.
        assert_ne!(
            row_hash(&["ab".into(), "c".into()]),
            row_hash(&["a".into(), "bc".into()])
        );
        assert_eq!(row_hash(&["a".into()]), row_hash(&["a".into()]));
    }

    fn report(tables: Vec<TableReport>) -> ReconcileReport {
        ReconcileReport {
            pipeline: "p".into(),
            tables,
        }
    }

    fn table_report(source_rows: u64, target_rows: Option<u64>, diff: KeyDiff) -> TableReport {
        TableReport {
            table: "sales_suppliers".into(),
            upstream: vec!["public.sales_suppliers".into()],
            target: "local.live.sales_suppliers".into(),
            keys: vec!["supplierid".into()],
            source_rows: Some(source_rows),
            target_rows,
            target_error: target_rows.is_none().then(|| "table not found".to_string()),
            source_error: None,
            sample: DEFAULT_SAMPLE,
            source_sampled: source_rows as usize,
            target_sampled: target_rows.unwrap_or(0) as usize,
            missing_columns: Vec::new(),
            excluded_columns: Vec::new(),
            diff,
        }
    }

    #[test]
    fn a_clean_report_exits_zero_and_a_drifted_one_exits_one() {
        let clean = report(vec![table_report(10, Some(10), KeyDiff::default())]);
        assert_eq!(clean.exit_code(), 0);
        assert_eq!(clean.drifted(), 0);
        assert_eq!(clean.tables[0].verdicts(), vec!["in_sync"]);

        let drifted = report(vec![table_report(
            11,
            Some(10),
            KeyDiff {
                missing_in_target: vec!["11".into()],
                ..Default::default()
            },
        )]);
        assert_eq!(drifted.exit_code(), 1);
        assert_eq!(
            drifted.tables[0].verdicts(),
            vec!["row_count_drift", "key_drift"]
        );
    }

    #[test]
    fn one_drifted_table_among_clean_ones_still_exits_one() {
        let mixed = report(vec![
            table_report(10, Some(10), KeyDiff::default()),
            table_report(
                10,
                Some(10),
                KeyDiff {
                    hash_mismatches: vec!["4".into()],
                    ..Default::default()
                },
            ),
        ]);
        assert_eq!(mixed.drifted(), 1);
        assert_eq!(mixed.exit_code(), 1);
    }

    /// A table whose own comparison could not be run, as `reconcile` reports one.
    fn failed_report(error: &str) -> TableReport {
        TableReport {
            source_error: Some(error.to_string()),
            source_rows: None,
            target_rows: None,
            target_error: None,
            keys: Vec::new(),
            sample: 0,
            source_sampled: 0,
            target_sampled: 0,
            ..table_report(0, None, KeyDiff::default())
        }
    }

    #[test]
    fn a_table_that_could_not_be_read_exits_two_rather_than_one() {
        // The two non-zero answers call for different things. `1` means the target stopped saying
        // what the source says — someone has to look at the data. `2` means the comparison did not
        // happen, which for a CI step written as `reconcile || page_the_data_team` is a network
        // blip, not a page.
        let failed = report(vec![failed_report("connection refused")]);
        assert_eq!(failed.exit_code(), EXIT_FAILED);
        assert_eq!(failed.errored(), 1);
        assert_eq!(
            failed.drifted(),
            0,
            "an unread table is not a table that differed"
        );
        assert_eq!(failed.tables[0].verdicts(), vec!["source_error"]);
        assert_eq!(
            failed.tables[0].row_count_drift(),
            None,
            "unknown is not a drift of zero"
        );

        // Failure outranks drift when a run hit both: the run was incomplete, so "no drift here"
        // is only a claim about the tables it managed to read.
        let both = report(vec![
            failed_report("connection refused"),
            table_report(11, Some(10), KeyDiff::default()),
        ]);
        assert_eq!(both.exit_code(), EXIT_FAILED);
        assert_eq!((both.errored(), both.drifted()), (1, 1));
        assert_eq!(
            report(vec![table_report(11, Some(10), KeyDiff::default())]).exit_code(),
            EXIT_DRIFT
        );
        assert_eq!(
            report(vec![table_report(10, Some(10), KeyDiff::default())]).exit_code(),
            EXIT_IN_SYNC
        );
    }

    #[test]
    fn one_unreadable_table_does_not_discard_the_others_report() {
        // The whole premise is "run this across N tables in CI", so N-1 answers and one named
        // failure are worth more than an error and nothing at all.
        let mixed = report(vec![
            failed_report("connection refused"),
            table_report(
                10,
                Some(10),
                KeyDiff {
                    hash_mismatches: vec!["4".into()],
                    ..Default::default()
                },
            ),
        ]);
        let rendered = mixed.render();
        assert!(
            rendered.contains("connection refused"),
            "the failure is named:\n{rendered}"
        );
        assert!(
            rendered.contains("hash_mismatches"),
            "and the table that did read is still reported:\n{rendered}"
        );
        assert!(
            !rendered.contains("source 0"),
            "an unknown count is never printed as zero:\n{rendered}"
        );
        assert!(rendered.contains("summary: FAILED"), "{rendered}");
        assert_eq!(
            ReconcileSchedule::result_of(&mixed),
            "failed: 1 of 2 table(s) could not be read; drift: 1",
            "and `pipeline show` says both halves"
        );
        assert_eq!(
            ReconcileSchedule::result_of(&report(vec![failed_report("nope")])),
            "failed: 1 of 1 table(s) could not be read"
        );
    }

    #[test]
    fn a_missing_target_is_drift_rather_than_a_crash() {
        let missing = report(vec![table_report(10, None, KeyDiff::default())]);
        assert_eq!(missing.tables[0].verdicts(), vec!["target_missing"]);
        assert_eq!(missing.exit_code(), 1);
    }

    #[test]
    fn equal_counts_with_matching_keys_are_in_sync_even_though_rows_moved() {
        // One insert and one delete leave the count alone; the key walk is what catches it.
        let table = table_report(
            10,
            Some(10),
            KeyDiff {
                missing_in_target: vec!["11".into()],
                missing_in_source: vec!["3".into()],
                ..Default::default()
            },
        );
        assert_eq!(table.row_count_drift(), Some(0));
        assert_eq!(table.verdicts(), vec!["key_drift"]);
    }

    #[test]
    fn the_rendered_report_names_every_drift_class_and_the_summary_line() {
        let rendered = report(vec![table_report(
            12,
            Some(10),
            KeyDiff {
                compared: 9,
                missing_in_target: vec!["11".into(), "12".into()],
                missing_in_source: vec!["3".into()],
                hash_mismatches: vec!["4".into()],
                window_end: None,
            },
        )])
        .render();
        for needle in [
            "sales_suppliers",
            "public.sales_suppliers",
            "local.live.sales_suppliers",
            "missing_in_target",
            "missing_in_source",
            "hash_mismatches",
            "row_count_drift",
            "key_drift",
            "summary: DRIFT",
        ] {
            assert!(rendered.contains(needle), "missing `{needle}`:\n{rendered}");
        }
        assert!(rendered.contains("drift +2"), "{rendered}");
    }

    #[test]
    fn a_clean_report_says_in_sync_rather_than_printing_nothing() {
        let rendered = report(vec![table_report(10, Some(10), KeyDiff::default())]).render();
        assert!(rendered.contains("summary: in sync"), "{rendered}");
        assert!(!rendered.contains("missing_in_target"), "{rendered}");
    }

    #[test]
    fn a_composite_key_renders_readably_rather_than_with_a_control_byte() {
        assert_eq!(show_key(&format!("a{KEY_SEPARATOR}b")), "a | b");
    }

    /// A source column fixture — only the name and the type reach the SQL builders.
    fn column(name: &str, data_type: DataType) -> ColumnSchema {
        ColumnSchema {
            name: name.to_string(),
            type_oid: 0,
            type_modifier: -1,
            data_type,
            nullable: false,
            warning: None,
        }
    }

    /// An `auto_cdc:` block that varies only in its projection.
    fn auto_cdc(column_list: Option<Vec<String>>, except: Option<Vec<String>>) -> AutoCdcConfig {
        AutoCdcConfig {
            source: "changes".into(),
            keys: vec!["supplierid".into()],
            sequence_by: "__oxidant_lsn".into(),
            apply_as_deletes: None,
            apply_as_truncates: None,
            column_list,
            except_column_list: except,
            ignore_null_updates_columns: None,
            ignore_null_updates_except: None,
        }
    }

    /// How the target renders a value: `CAST(col AS VARCHAR)` is `arrow_cast` to `Utf8`.
    fn engine_text(array: ArrayRef) -> Vec<String> {
        let utf8 = cast(&array, &DataType::Utf8).expect("casts to text");
        let utf8 = utf8
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("a string array");
        (0..utf8.len()).map(|i| utf8.value(i).to_string()).collect()
    }

    #[test]
    fn a_boolean_key_is_read_through_the_cast_that_agrees_with_the_target_not_the_output_function()
    {
        use oxidant_loom::arrow::array::BooleanArray;

        // What the target says, through the same cast its query asks for.
        let target = engine_text(Arc::new(BooleanArray::from(vec![true, false])));
        assert_eq!(target, vec!["true".to_string(), "false".to_string()]);
        // What Postgres' *output function* says — what pgoutput carries and `psql` prints. If the
        // walk ever read a boolean key that way instead of through the cast, every key would
        // mismatch and interleave (`f` < `false` < `t` < `true`), turning a healthy table into a
        // permanent, both-directions false alarm.
        assert_ne!(target, vec!["t".to_string(), "f".to_string()]);
        // So the source projection is a cast, for booleans as for everything else. That it is
        // `true`/`false` on a live server is what the gated `a_boolean_key_is_spelled_the_same_way…`
        // test proves; this holds the shape of the query that makes it so.
        assert_eq!(
            source_key_text(&column("active", DataType::Boolean)),
            "(\"active\")::text"
        );
        assert_eq!(
            source_key_text(&column("id", DataType::Int64)),
            "(\"id\")::text"
        );
    }

    /// An upstream table fixture: only its name and key columns reach the SQL builders.
    fn upstream(schema: &str, table: &str) -> TableSchema {
        TableSchema {
            schema: schema.to_string(),
            table: table.to_string(),
            columns: vec![
                column("supplierid", DataType::Int64),
                column("name", DataType::Utf8),
            ],
            keys: vec!["supplierid".into()],
            replica_identity: 'd',
        }
    }

    #[test]
    fn a_table_filter_accounts_for_every_spelling_of_a_table_it_selected() {
        let all = vec![upstream("public", "sales_suppliers")];

        // No filter: everything, and nothing to account for.
        let (selected, matched) = select_upstream(&[], "sales_suppliers", all.clone());
        assert_eq!(selected.len(), 1);
        assert!(matched.is_empty());

        // Both documented spellings at once. The pipeline-table entry decides the selection, but
        // the upstream entry is a name this source *does* have — and calling it unmatched is what
        // produced an error that listed the very name it said did not exist.
        let wanted = vec![
            "sales_suppliers".to_string(),
            " PUBLIC.SALES_SUPPLIERS ".to_string(),
        ];
        let (selected, matched) = select_upstream(&wanted, "sales_suppliers", all.clone());
        assert_eq!(selected.len(), 1, "the whole source is in scope");
        assert_eq!(
            matched,
            BTreeSet::from(["sales_suppliers", "PUBLIC.SALES_SUPPLIERS"]),
            "case and surrounding space do not decide whether a name exists"
        );

        // The upstream spelling alone selects just that table, and a name this source does not
        // have stays unaccounted for — which is what turns a typo into an error rather than a
        // clean-looking report over the tables that did match.
        let wanted = vec!["public.sales_suppliers".into(), "public.nope".into()];
        let (selected, matched) = select_upstream(&wanted, "sales_suppliers", all);
        assert_eq!(selected.len(), 1);
        assert_eq!(matched, BTreeSet::from(["public.sales_suppliers"]));
    }

    #[test]
    fn a_source_whose_tables_share_a_key_space_is_probed_for_the_overlap() {
        let tables = [upstream("public", "a"), upstream("public", "b")];
        let columns = [column("supplierid", DataType::Int64)];
        let keys: Vec<&ColumnSchema> = columns.iter().collect();
        let sql = duplicate_key_sql(&tables, &keys);
        // The same union and the same key spelling the walk itself reads, asked the one question
        // that decides whether the walk can be trusted at all.
        assert!(sql.contains("UNION ALL"), "{sql}");
        assert!(sql.contains("\"public\".\"a\""), "{sql}");
        assert!(sql.contains("\"public\".\"b\""), "{sql}");
        assert!(sql.contains("(\"supplierid\")::text AS k0"), "{sql}");
        assert!(sql.contains("GROUP BY k0 HAVING count(*) > 1"), "{sql}");
        assert!(sql.contains("LIMIT 1"), "one example is enough: {sql}");
    }

    #[test]
    fn auto_cdc_projection_decides_which_source_columns_the_target_owes() {
        let columns = [
            column("name", DataType::Utf8),
            column("notes", DataType::Utf8),
            column("rating", DataType::Decimal128(10, 2)),
        ];
        let value_columns: Vec<&ColumnSchema> = columns.iter().collect();
        let names =
            |cs: &[&ColumnSchema]| -> Vec<String> { cs.iter().map(|c| c.name.clone()).collect() };

        // No `auto_cdc` block, and a block with neither list: every column is the target's.
        for config in [None, Some(auto_cdc(None, None))] {
            let (expected, excluded) = projected_value_columns(config.as_ref(), &value_columns);
            assert_eq!(names(&expected), ["name", "notes", "rating"]);
            assert!(excluded.is_empty());
        }

        // `except_column_list` — the case that made a healthy pipeline report `schema_drift` on
        // every run. `NOTES` in the config, `notes` on the wire: names resolve case-insensitively,
        // as `output_columns` resolves them.
        let except = auto_cdc(None, Some(vec!["NOTES".into(), "__oxidant_op".into()]));
        let (expected, excluded) = projected_value_columns(Some(&except), &value_columns);
        assert_eq!(names(&expected), ["name", "rating"]);
        assert_eq!(
            excluded,
            ["notes"],
            "not drift — the merge drops it on purpose"
        );

        // `column_list` is the whole target, so anything it does not name is not owed either.
        // Backticks are stripped the same way the merge strips them.
        let listed = auto_cdc(Some(vec!["`name`".into(), "__oxidant_lsn".into()]), None);
        let (expected, excluded) = projected_value_columns(Some(&listed), &value_columns);
        assert_eq!(names(&expected), ["name"]);
        assert_eq!(excluded, ["notes", "rating"]);
    }

    #[test]
    fn only_key_types_that_spell_the_same_on_both_sides_are_walkable() {
        for ok in [
            // `Boolean` is walkable because the cast the walk reads spells it the target's way;
            // the test above is what holds that distinction in place.
            DataType::Boolean,
            DataType::Int32,
            DataType::Int64,
            DataType::Utf8,
            DataType::Date32,
            DataType::Decimal128(38, 2),
        ] {
            assert!(key_type_is_walkable(&ok), "{ok:?} should be walkable");
        }
        for refused in [
            DataType::Float64,
            DataType::Binary,
            DataType::Timestamp(
                oxidant_loom::arrow::datatypes::TimeUnit::Microsecond,
                Some("UTC".into()),
            ),
        ] {
            assert!(
                !key_type_is_walkable(&refused),
                "{refused:?} must be refused rather than compared as text"
            );
        }
    }

    // ---------------------------------------------------------------------------------------
    // The persisted schedule
    // ---------------------------------------------------------------------------------------

    /// A minimal pipeline with one `postgres_cdc` table, checkpointed under `checkpoints`.
    ///
    /// A real config rather than a stub: registering a schedule also writes to each connector's
    /// log, and the path it writes to is derived from the pipeline — which is the part worth
    /// having a test hold to.
    fn config_with_cdc_table(checkpoints: &str) -> oxidant_config::OxidantConfig {
        oxidant_config::OxidantConfig::parse(&format!(
            "catalogs:
  local:
    type: local
    warehouse: {checkpoints}/warehouse
pipeline:
  name: sales-cdc
  catalog: local
  schema: live
  checkpoints: {checkpoints}
tables:
  - name: sales_suppliers
    source:
      format: postgres_cdc
      options:
        host: db.internal
        database: sales
        user: oxidant_cdc
        tls: disable
        publication: oxidant_sales
        slot: oxidant_sales_suppliers
        tables: public.sales_suppliers
    auto_cdc:
      source: sales_suppliers_changes
      keys: [supplierid]
      sequence_by: __oxidant_lsn
"
        ))
        .expect("the fixture config parses")
    }

    #[test]
    fn a_schedule_round_trips_through_the_checkpoint_directory() {
        let dir = tempfile::TempDir::new().unwrap();
        let checkpoints = dir.path().to_string_lossy().into_owned();
        assert!(ReconcileSchedule::load(&checkpoints).is_none());

        let options = ReconcileOptions {
            tables: vec!["public.sales_suppliers".into()],
            sample: 5000,
        };
        let config = config_with_cdc_table(&checkpoints);
        let plan = Plan::build(&config).expect("plans");
        let saved = set_schedule(
            &plan,
            "  0   6 * * *  ",
            Some(Path::new("/srv/oxidant.yaml")),
            &options,
        )
        .expect("registers");
        assert_eq!(saved.cron, "0 6 * * *", "the expression is normalized");
        assert_eq!(saved.path.as_deref(), Some("/srv/oxidant.yaml"));

        let loaded = ReconcileSchedule::load(&checkpoints).expect("reads back");
        assert_eq!(loaded, saved);
        assert_eq!(loaded.tables, vec!["public.sales_suppliers".to_string()]);
        assert_eq!(loaded.sample, 5000);
        assert!(loaded.last_run.is_none());

        assert!(ReconcileSchedule::remove(&checkpoints).expect("removes"));
        assert!(!ReconcileSchedule::remove(&checkpoints).expect("idempotent"));
        assert!(ReconcileSchedule::load(&checkpoints).is_none());
    }

    #[test]
    fn registering_a_schedule_leaves_a_line_in_the_connector_log() {
        // The JSON file is where the scheduler reads it; the connector log is where an operator
        // — and the platform console, which parses this file — will look.
        let dir = tempfile::TempDir::new().unwrap();
        let checkpoints = dir.path().to_string_lossy().into_owned();
        let config = config_with_cdc_table(&checkpoints);
        let plan = Plan::build(&config).expect("plans");
        set_schedule(&plan, "0 6 * * *", None, &ReconcileOptions::default()).expect("registers");

        let log = dir.path().join("logs").join("sales_suppliers.jsonl");
        let text =
            std::fs::read_to_string(&log).unwrap_or_else(|e| panic!("`{}`: {e}", log.display()));
        let event: serde_json::Value = serde_json::from_str(text.lines().next().expect("one line"))
            .expect("each line is one JSON object");
        assert_eq!(event["event"], "reconcile");
        assert_eq!(event["scheduled"], "0 6 * * *");
        assert_eq!(event["sample"], DEFAULT_SAMPLE);
        assert!(
            event["next"]
                .as_str()
                .is_some_and(|n| n.ends_with("T06:00:00Z")),
            "the log says when it will next fire: {event}"
        );
    }

    #[test]
    fn re_registering_keeps_what_the_last_run_found() {
        // Changing the expression does not un-run the last reconcile; dropping its verdict would
        // make `pipeline show` claim it never ran.
        let dir = tempfile::TempDir::new().unwrap();
        let checkpoints = dir.path().to_string_lossy().into_owned();
        let config = config_with_cdc_table(&checkpoints);
        let plan = Plan::build(&config).expect("plans");
        let mut first =
            set_schedule(&plan, "0 6 * * *", None, &ReconcileOptions::default()).unwrap();
        first.record(
            DateTime::parse_from_rfc3339("2026-08-23T06:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            "in_sync".into(),
        );
        first.save(&checkpoints).unwrap();

        let second =
            set_schedule(&plan, "0 */6 * * *", None, &ReconcileOptions::default()).unwrap();
        assert_eq!(second.cron, "0 */6 * * *");
        assert_eq!(second.last_result.as_deref(), Some("in_sync"));
        assert_eq!(second.last_run.as_deref(), Some("2026-08-23T06:00:00Z"));
        assert_eq!(
            second.created, first.created,
            "the anchor does not reset when the expression changes"
        );
    }

    #[test]
    fn an_unparseable_expression_is_refused_before_anything_is_written() {
        let dir = tempfile::TempDir::new().unwrap();
        let checkpoints = dir.path().to_string_lossy().into_owned();
        let config = config_with_cdc_table(&checkpoints);
        let plan = Plan::build(&config).expect("plans");
        let err = set_schedule(&plan, "every morning", None, &ReconcileOptions::default())
            .expect_err("refused");
        assert!(err.to_string().contains("5 fields"), "got: {err}");
        assert!(
            !ReconcileSchedule::path_in(&checkpoints).exists(),
            "a refused expression must not leave a file behind"
        );
    }

    #[test]
    fn a_schedule_is_due_only_once_its_expression_has_fired_since_the_last_run() {
        let mut schedule = ReconcileSchedule {
            path: None,
            cron: "0 6 * * *".into(),
            tables: vec![],
            sample: DEFAULT_SAMPLE,
            created: "2026-08-23T05:00:00Z".into(),
            last_run: None,
            last_result: None,
        };
        let at = |t: &str| DateTime::parse_from_rfc3339(t).unwrap().with_timezone(&Utc);
        assert!(!schedule.is_due(at("2026-08-23T05:30:00Z")));
        assert!(schedule.is_due(at("2026-08-23T06:00:00Z")));

        schedule.record(at("2026-08-23T06:00:00Z"), "in_sync".into());
        assert!(!schedule.is_due(at("2026-08-23T20:00:00Z")));
        assert!(schedule.is_due(at("2026-08-24T06:00:00Z")));
    }

    #[test]
    fn a_run_longer_than_its_own_period_is_not_due_again_the_moment_it_finishes() {
        // The run starts at 12:00 and walks a large table until 12:12. Anchored on the start,
        // `*/5` has fired twice by the time it lands and the next pass runs it again — forever,
        // with the replication loop blocked behind it. Anchored on the completion, the next
        // firing is the next `*/5` boundary after it finished.
        let at = |t: &str| DateTime::parse_from_rfc3339(t).unwrap().with_timezone(&Utc);
        let mut schedule = ReconcileSchedule {
            path: None,
            cron: "*/5 * * * *".into(),
            tables: vec![],
            sample: DEFAULT_SAMPLE,
            created: "2026-08-23T11:55:00Z".into(),
            last_run: None,
            last_result: None,
        };
        assert!(schedule.is_due(at("2026-08-23T12:00:00Z")), "it starts due");

        schedule.record(at("2026-08-23T12:12:00Z"), "in_sync".into());
        assert!(
            !schedule.is_due(at("2026-08-23T12:12:00Z")),
            "a twelve-minute run must not re-fire the instant it lands"
        );
        assert!(!schedule.is_due(at("2026-08-23T12:14:59Z")));
        assert!(
            schedule.is_due(at("2026-08-23T12:15:00Z")),
            "the next `*/5`"
        );
    }

    #[test]
    fn a_schedule_whose_anchor_could_not_be_written_still_waits_for_its_next_firing() {
        // `save` failed, so the file still says the schedule has never run. The scheduler's own
        // memory of the run is what keeps it from firing again on the very next pass.
        let at = |t: &str| DateTime::parse_from_rfc3339(t).unwrap().with_timezone(&Utc);
        let unsaved = ReconcileSchedule {
            path: None,
            cron: "0 6 * * *".into(),
            tables: vec![],
            sample: DEFAULT_SAMPLE,
            created: "2026-08-23T05:00:00Z".into(),
            last_run: None,
            last_result: None,
        };
        let ran_at = at("2026-08-23T06:00:00Z");
        assert!(unsaved.is_due(ran_at), "the file's anchor is still 05:00");
        assert!(
            !unsaved.is_due_since(Some(ran_at), at("2026-08-23T06:00:01Z")),
            "the run that just happened is what the next firing is measured from"
        );
        assert!(!unsaved.is_due_since(Some(ran_at), at("2026-08-23T23:59:59Z")));
        assert!(unsaved.is_due_since(Some(ran_at), at("2026-08-24T06:00:00Z")));

        // And the file wins when it is the later of the two: a schedule that was saved by an
        // earlier process is not un-run by a scheduler that has only just started.
        let mut saved = unsaved.clone();
        saved.record(at("2026-08-24T06:00:00Z"), "in_sync".into());
        assert!(!saved.is_due_since(Some(ran_at), at("2026-08-24T06:00:01Z")));
    }

    #[test]
    fn a_corrupt_or_hand_edited_schedule_never_stops_a_pipeline() {
        let dir = tempfile::TempDir::new().unwrap();
        let checkpoints = dir.path().to_string_lossy().into_owned();
        std::fs::write(ReconcileSchedule::path_in(&checkpoints), "{ not json").unwrap();
        assert!(ReconcileSchedule::load(&checkpoints).is_none());

        let hand_edited = ReconcileSchedule {
            path: None,
            cron: "every morning".into(),
            tables: vec![],
            sample: DEFAULT_SAMPLE,
            created: "2026-08-23T05:00:00Z".into(),
            last_run: None,
            last_result: None,
        };
        assert!(
            !hand_edited.is_due(Utc::now()),
            "an unparseable cron is never due"
        );
    }

    #[test]
    fn the_recorded_result_names_the_verdict_the_run_reached() {
        assert_eq!(
            ReconcileSchedule::result_of(&report(vec![table_report(
                10,
                Some(10),
                KeyDiff::default()
            )])),
            "in_sync"
        );
        assert_eq!(
            ReconcileSchedule::result_of(&report(vec![table_report(
                11,
                Some(10),
                KeyDiff::default()
            )])),
            "drift: 1 of 1 table(s)"
        );
    }
}
