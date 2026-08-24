//! The diagnostic dump — **the one time log bytes move** (§6b).
//!
//! Everywhere else in this design, logs stay where they are written and the driver federates
//! *reads* over them. A support bundle is the stated exception, and it is deliberately a
//! separate, explicitly-named, token-guarded, audited route rather than a mode of the browser:
//! an operator who copies a cluster's logs onto the driver's disk should have had to say so.
//!
//! Four properties, each of which is why a line of this file exists:
//!
//! 1. **Bounded, and refused rather than truncated.** A dump is capped by
//!    `OXIDANT_LOG_DUMP_MAX_BYTES` (1 GiB) *and* by §3's disk budget and free-space floor. A
//!    request that would breach either answers `507` — not a smaller bundle, which an operator
//!    would carry to a support case believing it held the window they asked for. A dump that
//!    breaches the cap mid-write is abandoned and its id reports the `507` on collection, for
//!    the same reason.
//! 2. **It is one Parquet with a `node` column**, not an archive of per-node files. The point of
//!    a bundle is to be queryable — `SELECT * FROM dump WHERE level='ERROR' ORDER BY ts` across
//!    the whole cluster — and an operator holding six files with six schemas has to reassemble
//!    that themselves. **Deviation from §6b**, which says already-Parquet rolled files "ship as
//!    is": shipping bytes would need a second wire shape, a second bounding path, and would
//!    hand back a heterogeneous bundle. The cost is stated: the rows are re-rendered through the
//!    same normalization `?file=` already documents for a converted file, so a dump is faithful
//!    to what the browser shows rather than byte-identical to the file.
//! 3. **It is assembled off the request.** `POST` mints an id and answers `202`; the work runs on
//!    a task. A support bundle over six nodes and a day is minutes of Flight round-trips, and an
//!    HTTP client that gave up halfway would otherwise leave a half-written file with nobody to
//!    finish or remove it.
//! 4. **A node that could not be reached is named in the bundle's own manifest**, and the dump
//!    still completes. A support bundle that silently omits the node that died is worse than no
//!    bundle: the missing node is the one the case is about.
//!
//! Expiry is 24 h, swept by `history::disk`'s dump pass — which existed and was tested before
//! anything wrote a dump, and whose `is_dump` shape (`dump-*.parquet`) is what these names take.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::{Compression, ZstdLevel};
use datafusion::parquet::file::properties::WriterProperties;
use oxidant_loom::arrow::array::{ArrayRef, StringArray, TimestampMillisecondArray};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use oxidant_loom::arrow::record_batch::RecordBatch;

use super::api::LogError;
use super::line::{parse_line, ParsedLine};
use crate::history::disk::{self, BudgetRoot};
use crate::history::{fs_util, HistoryConfig};

/// Rows buffered before a batch is handed to the writer — the memory knob, matching the
/// converter's.
const ROWS_PER_BATCH: usize = 8192;

/// `dump-<uuid>` — the id, and (with `.parquet`) the filename. Recognised by
/// [`disk::is_dump`], which shipped in PR2 with its prune step and its tests.
const DUMP_PREFIX: &str = "dump-";

/// A dump's life: 24 h (§6b), swept like a result.
pub(crate) const DUMP_TTL_SECS: i64 = 24 * 60 * 60;

/// The manifest rows' own timestamp, in the writer's spelling.
fn now_stamp() -> String {
    chrono::Utc::now()
        .format(super::line::TS_FORMAT)
        .to_string()
}

/// `(node, ts, level, target, message, fields_json)` — §6's log schema with the column that
/// makes a cluster-wide bundle one table.
pub(crate) fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("node", DataType::Utf8, false),
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

/// Where one dump is in its life.
#[derive(Clone, Debug)]
pub(crate) enum DumpState {
    /// Assembling. `GET` answers `202` — the bundle is not a lie yet, it is not there yet.
    Building,
    Ready {
        path: PathBuf,
        bytes: u64,
        rows: u64,
        /// Nodes that answered, and nodes that did not with why. A bundle that quietly omits
        /// the node the case is about is worse than no bundle.
        nodes: Vec<(String, Option<String>)>,
    },
    Failed(LogError),
}

/// The driver's dump registry: the directory, the caps, and what each minted id is doing.
pub(crate) struct DumpStore {
    dir: PathBuf,
    /// `OXIDANT_LOG_DUMP_MAX_BYTES`.
    max_bytes: u64,
    /// §3's guards, captured at boot: the same roots, budget and floor the sweeper uses, so a
    /// dump cannot be the one writer that ignores them.
    roots: Vec<BudgetRoot>,
    disk_max_bytes: u64,
    disk_min_free_bytes: u64,
    mounts: Option<Vec<(PathBuf, u64)>>,
    state: Mutex<HashMap<String, DumpState>>,
}

impl DumpStore {
    /// Build from the boot config, or `None` under `OXIDANT_HISTORY=off` — which promises that
    /// nothing is written under the data dir, and a support bundle is the largest thing that
    /// would be.
    pub(crate) fn from_config(cfg: &HistoryConfig) -> Option<Arc<Self>> {
        if !cfg.enabled {
            return None;
        }
        Some(Arc::new(Self {
            dir: cfg.dumps_dir.clone(),
            max_bytes: cfg.log_dump_max_bytes,
            roots: disk::budget_roots(cfg),
            disk_max_bytes: cfg.disk_max_bytes,
            disk_min_free_bytes: cfg.disk_min_free_bytes,
            mounts: cfg.mounts_override(),
            state: Mutex::new(HashMap::new()),
        }))
    }

    /// The cap, echoed in the `202` so an operator knows what they are being held to.
    pub(crate) fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// Would a dump of up to `max_bytes` breach §3? Checked **before** the id is minted, so a
    /// refusal is a `507` on the `POST` rather than a `507` an operator only sees when they come
    /// back for the file.
    ///
    /// The reserve is the whole cap, not an estimate of the request: the request's size is not
    /// knowable until the logs have been read, and reserving less would let a dump be admitted
    /// and then abandoned — which is the outcome this check exists to avoid.
    pub(crate) fn admit(&self) -> Result<(), LogError> {
        let usage = disk::measure_roots(&self.roots);
        if usage.billed.saturating_add(self.max_bytes) > self.disk_max_bytes {
            return Err(LogError {
                status: 507,
                message: format!(
                    "a dump needs up to {} bytes of headroom and the engine is already using {} \
                     of OXIDANT_DISK_MAX_BYTES={}; raise the budget, lower \
                     OXIDANT_LOG_DUMP_MAX_BYTES, or wait for the sweep",
                    self.max_bytes, usage.billed, self.disk_max_bytes
                ),
            });
        }
        let mounts = match &self.mounts {
            Some(entries) => disk::Mounts::from_entries(entries.clone()),
            None => disk::Mounts::probe(),
        };
        if let Some(free) = mounts.free_bytes(&self.dir) {
            if free < self.disk_min_free_bytes.saturating_add(self.max_bytes) {
                return Err(LogError {
                    status: 507,
                    message: format!(
                        "the volume holding {} has {free} bytes free and a dump needs \
                         OXIDANT_DISK_MIN_FREE_BYTES={} plus up to {} on top",
                        self.dir.display(),
                        self.disk_min_free_bytes,
                        self.max_bytes
                    ),
                });
            }
        }
        Ok(())
    }

    /// Mint an id and mark it building.
    pub(crate) fn begin(&self) -> String {
        let id = format!("{DUMP_PREFIX}{}", uuid::Uuid::new_v4());
        self.state
            .lock()
            .expect("dump registry poisoned")
            .insert(id.clone(), DumpState::Building);
        id
    }

    pub(crate) fn set(&self, id: &str, state: DumpState) {
        self.state
            .lock()
            .expect("dump registry poisoned")
            .insert(id.to_string(), state);
    }

    /// What an id is doing — and, for one this process did not mint, whether the file is
    /// nonetheless on disk. A restart loses the registry; the bundle it wrote does not stop
    /// being downloadable because of that.
    pub(crate) fn get(&self, id: &str) -> Option<DumpState> {
        if let Some(state) = self
            .state
            .lock()
            .expect("dump registry poisoned")
            .get(id)
            .cloned()
        {
            return Some(state);
        }
        let path = self.path_of(id)?;
        let meta = path.symlink_metadata().ok()?;
        if !meta.is_file() {
            return None;
        }
        Some(DumpState::Ready {
            path,
            bytes: meta.len(),
            rows: 0,
            nodes: Vec::new(),
        })
    }

    /// The file an id names — **reconstructed from a validated id**, never string-joined.
    ///
    /// Same discipline as `?file=`'s typed `LogPeriod` (§6, F12): the id must be
    /// `dump-<uuid>` exactly, so `..`, `/` and every other traversal shape fails before a path
    /// is built rather than after.
    pub(crate) fn path_of(&self, id: &str) -> Option<PathBuf> {
        let raw = id.strip_prefix(DUMP_PREFIX)?;
        let uuid = uuid::Uuid::parse_str(raw).ok()?;
        Some(self.dir.join(format!("{DUMP_PREFIX}{uuid}.parquet")))
    }

    /// Open the writer for a minted id.
    pub(crate) fn open(&self, id: &str) -> Result<DumpWriter, LogError> {
        let target = self.path_of(id).ok_or_else(|| LogError {
            status: 400,
            message: format!("invalid dump id `{id}`"),
        })?;
        fs_util::create_dir_secure(&self.dir).map_err(|e| LogError {
            status: 500,
            message: format!("creating {}: {e}", self.dir.display()),
        })?;
        let tmp = target.with_extension("parquet.tmp");
        let file = fs_util::create_secure(&tmp).map_err(|e| LogError {
            status: 500,
            message: format!("creating {}: {e}", tmp.display()),
        })?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_max_row_group_row_count(Some(ROWS_PER_BATCH))
            .build();
        let writer = ArrowWriter::try_new(file, schema(), Some(props)).map_err(|e| LogError {
            status: 500,
            message: format!("opening a parquet writer on {}: {e}", tmp.display()),
        })?;
        Ok(DumpWriter {
            writer: Some(writer),
            dir: self.dir.clone(),
            tmp,
            target,
            max_bytes: self.max_bytes,
            rows: 0,
            nodes: Vec::new(),
            buf: Vec::with_capacity(ROWS_PER_BATCH),
        })
    }
}

/// One dump under construction. Dropping it without [`DumpWriter::finish`] removes the `.tmp`,
/// so an abandoned assembly never leaves bytes the sweeper cannot recognise — the same class of
/// unprunable-but-counted file `clear_log_tmp` was added to sweep at boot.
pub(crate) struct DumpWriter {
    writer: Option<ArrowWriter<std::fs::File>>,
    dir: PathBuf,
    tmp: PathBuf,
    target: PathBuf,
    max_bytes: u64,
    rows: u64,
    nodes: Vec<(String, Option<String>)>,
    buf: Vec<(String, ParsedLine)>,
}

impl DumpWriter {
    /// Record that a node was asked, and what happened.
    ///
    /// **The record goes into the bundle, not just into a field.** A support bundle that
    /// silently omits the node that died is worse than no bundle — the missing node is the one
    /// the case is about — and an operator opens a dump by querying it, not by reading a
    /// response body they threw away when they saved the file. So each node contributes one
    /// `oxidant.dump` row saying whether it answered, and `SELECT DISTINCT node, message FROM
    /// dump WHERE target = 'oxidant.dump'` is the manifest.
    pub(crate) fn note_node(&mut self, node: &str, error: Option<String>) {
        let line = match &error {
            None => format!(
                "{} [INFO] oxidant.dump - message=node answered",
                now_stamp()
            ),
            Some(e) => format!(
                "{} [ERROR] oxidant.dump - message=node unreachable, error={}",
                now_stamp(),
                super::line::escape_line_breaks(e)
            ),
        };
        // The manifest row is part of the bundle, so it is billed against the cap like any
        // other. A cap so small that the manifest alone breaches it is a cap that would have
        // refused this dump anyway.
        let _ = self.push(node, &line);
        self.nodes.push((node.to_string(), error));
    }

    /// Add one rendered line, attributed to its node.
    pub(crate) fn push(&mut self, node: &str, line: &str) -> Result<(), LogError> {
        self.buf.push((node.to_string(), parse_line(line)));
        self.rows += 1;
        if self.buf.len() >= ROWS_PER_BATCH {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), LogError> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.buf);
        let node: ArrayRef = Arc::new(
            rows.iter()
                .map(|(n, _)| Some(n.as_str()))
                .collect::<StringArray>(),
        );
        let ts: ArrayRef = Arc::new(
            rows.iter()
                .map(|(_, r)| r.ts_ms)
                .collect::<TimestampMillisecondArray>()
                .with_timezone("UTC"),
        );
        let col = |f: fn(&ParsedLine) -> Option<&str>| -> ArrayRef {
            Arc::new(rows.iter().map(|(_, r)| f(r)).collect::<StringArray>())
        };
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                node,
                ts,
                col(|r| r.level.as_deref()),
                col(|r| r.target.as_deref()),
                col(|r| r.message.as_deref()),
                col(|r| r.fields_json.as_deref()),
            ],
        )
        .map_err(|e| LogError {
            status: 500,
            message: format!("building a dump batch: {e}"),
        })?;
        let writer = self.writer.as_mut().ok_or_else(|| LogError {
            status: 500,
            message: "the dump writer was already closed".to_string(),
        })?;
        writer.write(&batch).map_err(|e| LogError {
            status: 500,
            message: format!("writing a dump batch: {e}"),
        })?;
        // **Refused, never truncated.** The cap is checked against what is actually on disk plus
        // what the writer is still holding, after every batch. Stopping here and calling the
        // shorter file a dump would hand an operator a bundle they believe holds the window they
        // asked for.
        let written = writer.bytes_written() as u64 + writer.in_progress_size() as u64;
        if written > self.max_bytes {
            return Err(LogError {
                status: 507,
                message: format!(
                    "this dump passed OXIDANT_LOG_DUMP_MAX_BYTES={} at {written} bytes; narrow \
                     the time range or the filters, or raise the knob",
                    self.max_bytes
                ),
            });
        }
        Ok(())
    }

    /// Seal the bundle: flush, fsync, rename, fsync the directory. The same discipline as the
    /// converter's, and for the same reason — a Parquet's footer is at the end, so a `.tmp` a
    /// crash left behind is never salvageable and is removed rather than trusted.
    pub(crate) fn finish(mut self) -> Result<DumpState, LogError> {
        self.flush()?;
        let writer = self.writer.take().ok_or_else(|| LogError {
            status: 500,
            message: "the dump writer was already closed".to_string(),
        })?;
        let file = writer.into_inner().map_err(|e| LogError {
            status: 500,
            message: format!("closing the dump writer on {}: {e}", self.tmp.display()),
        })?;
        file.sync_all().map_err(|e| LogError {
            status: 500,
            message: format!("fsync {}: {e}", self.tmp.display()),
        })?;
        drop(file);
        fs_util::rename_durable(&self.tmp, &self.target, &self.dir).map_err(|e| LogError {
            status: 500,
            message: format!(
                "rename {} -> {}: {e}",
                self.tmp.display(),
                self.target.display()
            ),
        })?;
        let bytes = self
            .target
            .metadata()
            .map(|m| m.len())
            .map_err(|e| LogError {
                status: 500,
                message: format!("stat {}: {e}", self.target.display()),
            })?;
        Ok(DumpState::Ready {
            path: self.target.clone(),
            bytes,
            rows: self.rows,
            nodes: std::mem::take(&mut self.nodes),
        })
    }
}

impl Drop for DumpWriter {
    fn drop(&mut self) {
        // `finish` took the writer; anything else is an abandoned assembly.
        if self.writer.is_some() {
            let _ = std::fs::remove_file(&self.tmp);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(dir: &std::path::Path, max_bytes: u64, disk_max: u64) -> Arc<DumpStore> {
        Arc::new(DumpStore {
            dir: dir.to_path_buf(),
            max_bytes,
            roots: vec![BudgetRoot::subtree(dir.to_path_buf())],
            disk_max_bytes: disk_max,
            disk_min_free_bytes: 0,
            mounts: Some(Vec::new()),
            state: Mutex::new(HashMap::new()),
        })
    }

    /// The bundle is one table with a `node` column, and it round-trips the lines it was given.
    #[test]
    fn a_dump_is_one_queryable_table_labelled_by_node() {
        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let dir = tempfile::tempdir().expect("tempdir");
        let store = store(dir.path(), 1 << 30, u64::MAX);
        let id = store.begin();
        let mut writer = store.open(&id).expect("open");
        writer.note_node("driver", None);
        writer
            .push(
                "driver",
                "2026-08-23T14:00:00.000Z [INFO] oxidant_connect - message=listening",
            )
            .expect("push");
        writer.note_node("10.0.0.7:50051", None);
        writer
            .push(
                "10.0.0.7:50051",
                "2026-08-23T14:00:01.000Z [ERROR] oxidant_execution - message=stage 0 failed",
            )
            .expect("push");
        let state = writer.finish().expect("finish");
        let DumpState::Ready {
            path, rows, nodes, ..
        } = state
        else {
            panic!("not ready");
        };
        assert_eq!(
            rows, 4,
            "two log lines, plus the one manifest row each node contributes"
        );
        assert_eq!(nodes.len(), 2);
        assert!(
            path.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("dump-"),
            "the name is the shape `disk::is_dump` already prunes: {path:?}"
        );
        assert!(disk::is_dump(path.file_name().unwrap().to_str().unwrap()));
        assert!(
            !dir.path().join(format!("{id}.parquet.tmp")).exists(),
            "no .tmp survives a successful seal"
        );

        let file = std::fs::File::open(&path).expect("open");
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("builder")
            .build()
            .expect("reader");
        let mut rows_out: Vec<(String, String, String)> = Vec::new();
        for batch in reader {
            let batch = batch.expect("batch");
            let text = |i: usize| {
                batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("string column")
            };
            let (node, level, target) = (text(0), text(2), text(3));
            for i in 0..batch.num_rows() {
                rows_out.push((
                    node.value(i).to_string(),
                    level.value(i).to_string(),
                    target.value(i).to_string(),
                ));
            }
        }
        // Every row is labelled with the node it came from, and each node's manifest row sits
        // with the lines it introduces.
        assert_eq!(
            rows_out,
            vec![
                ("driver".into(), "INFO".into(), "oxidant.dump".into()),
                ("driver".into(), "INFO".into(), "oxidant_connect".into()),
                (
                    "10.0.0.7:50051".into(),
                    "INFO".into(),
                    "oxidant.dump".into()
                ),
                (
                    "10.0.0.7:50051".into(),
                    "ERROR".into(),
                    "oxidant_execution".into()
                ),
            ]
        );
    }

    /// **Refused, not truncated.** A dump past its cap fails; the operator does not get a
    /// shorter bundle they will carry to a support case believing it holds the window they
    /// asked for.
    #[test]
    fn a_dump_past_its_cap_is_refused_and_leaves_nothing_behind() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = store(dir.path(), 1024, u64::MAX);
        let id = store.begin();
        let mut writer = store.open(&id).expect("open");
        let mut err = None;
        for i in 0..200_000 {
            let line = format!(
                "2026-08-23T14:00:00.000Z [INFO] oxidant_execution - message=line {i}, \
                 payload={i}{i}{i}{i}{i}{i}{i}{i}"
            );
            if let Err(e) = writer.push("driver", &line) {
                err = Some(e);
                break;
            }
        }
        let err = err.expect("the cap must bite");
        assert_eq!(err.status, 507);
        assert!(
            err.message.contains("OXIDANT_LOG_DUMP_MAX_BYTES"),
            "{err:?}"
        );
        drop(writer);
        assert!(
            !store.path_of(&id).unwrap().exists(),
            "no half-bundle is published"
        );
        let names: Vec<String> = std::fs::read_dir(dir.path())
            .expect("read_dir")
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(names.is_empty(), "and no .tmp is left behind: {names:?}");
    }

    /// The §3 budget refuses a dump *before* the id is minted, so `507` lands on the request
    /// rather than on the collection.
    #[test]
    fn a_dump_that_would_breach_the_disk_budget_is_refused_up_front() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("existing"), vec![0u8; 4096]).expect("write");
        let store = store(dir.path(), 1 << 20, 8192);
        let err = store.admit().expect_err("must refuse");
        assert_eq!(err.status, 507);
        assert!(err.message.contains("OXIDANT_DISK_MAX_BYTES"), "{err:?}");
        // With room, it is admitted.
        let roomy = store2(dir.path());
        roomy.admit().expect("admitted");
    }

    fn store2(dir: &std::path::Path) -> Arc<DumpStore> {
        store(dir, 1 << 20, u64::MAX)
    }

    /// The id is validated and the filename **reconstructed** from it — the same discipline as
    /// `?file=`'s typed period, so no caller-supplied string ever reaches a path join.
    #[test]
    fn a_dump_id_is_validated_not_joined() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = store2(dir.path());
        for hostile in [
            "dump-../../etc/passwd",
            "../../etc/passwd",
            "dump-",
            "dump-not-a-uuid",
            "stmt-00000000-0000-0000-0000-000000000000",
            "",
        ] {
            assert!(store.path_of(hostile).is_none(), "{hostile:?}");
            assert!(store.get(hostile).is_none(), "{hostile:?}");
        }
        let id = store.begin();
        let path = store.path_of(&id).expect("a real id resolves");
        assert_eq!(path.parent(), Some(dir.path()));
    }

    /// A restart loses the registry; the bundle it wrote does not stop being downloadable.
    #[test]
    fn a_dump_written_by_a_previous_process_is_still_collectable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = store2(dir.path());
        let id = format!("{DUMP_PREFIX}{}", uuid::Uuid::new_v4());
        std::fs::write(store.path_of(&id).unwrap(), b"parquet-ish").expect("write");
        match store.get(&id) {
            Some(DumpState::Ready { bytes, .. }) => assert_eq!(bytes, 11),
            other => panic!("{other:?}"),
        }
    }
}
