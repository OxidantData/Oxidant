//! The per-connector operator log: one JSON object per line, under the pipeline's checkpoint root.
//!
//! A CDC connector fails in ways the query's own progress numbers cannot describe — a slot
//! growing because nothing has confirmed it, a schema change on the publisher, a snapshot that
//! took forty minutes. Those belong in a record an operator can read *after the fact*, and they
//! belong somewhere the platform console can pick up without shell access, which is why the file
//! sits next to the checkpoints rather than in the process's stderr.
//!
//! Two deliberate limits. It is an **operator** log, not an audit log: it rotates by size and the
//! oldest file is dropped, so it can never fill the volume the checkpoints live on. And a write
//! that fails is swallowed — a full disk or a read-only mount must not be the reason a pipeline
//! that is otherwise healthy stops ingesting.
//!
//! ## Two backends, because appending is a filesystem verb
//!
//! When the checkpoint root is a filesystem path the log is what it has always been: an
//! `O_APPEND` write per event. When the root is an object-store URL there is no append and no
//! rename, so the live object is held in memory and re-`PUT` whole. That makes the cost of a
//! write proportional to the log's size rather than to the line's, which is why the object
//! backend rotates at [`MAX_OBJECT_LOG_BYTES`] — a fortieth of the on-disk cap — and coalesces
//! bursts behind [`FLUSH_INTERVAL`] instead of writing per event.
//!
//! The alternative — leaving the log on the driver's disk while the checkpoints went to the
//! bucket — is what this replaces. It reads as working right up until the driver is replaced,
//! at which point the record of *why* the last pipeline stopped is gone with the instance.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use oxidant_loom::Engine;
use serde_json::json;
use tokio::sync::{mpsc, oneshot};

use crate::checkpoint::{checkpoint_store, CheckpointStore};

/// Rotate once the live file passes this, keeping [`MAX_LOG_FILES`] generations.
const MAX_LOG_BYTES: u64 = 10 * 1024 * 1024;
/// The same cap for the object backend, where a write re-`PUT`s the whole live object rather
/// than appending one line to it. Ten megabytes re-uploaded per flush would make the log the
/// most expensive thing the pipeline does; a quarter of a megabyte is still thousands of events.
const MAX_OBJECT_LOG_BYTES: u64 = 256 * 1024;
/// `<name>.jsonl` plus `.1` … `.4`.
const MAX_LOG_FILES: usize = 5;
/// Shortest gap between two `PUT`s of the live object. A CDC connector emits a burst of events
/// per batch, and each one costs a whole-object upload; this turns a burst into one write and
/// bounds the loss on an unclean kill to the events of the last interval.
const FLUSH_INTERVAL: Duration = Duration::from_millis(250);

/// An append-only JSONL log for one connector.
#[derive(Debug, Clone, Default)]
pub struct ConnectorLog {
    /// `None` when no log directory was configured — a source built from a Spark Connect
    /// `readStream` rather than from a pipeline has no checkpoint root to write under.
    sink: Option<Sink>,
}

/// Where a log's lines go. See the module docs for why there are two.
#[derive(Debug, Clone)]
enum Sink {
    /// A file on the driver's disk, appended to per event.
    Local(PathBuf),
    /// An object under the checkpoint root, maintained by a background writer.
    Object(mpsc::UnboundedSender<Message>),
}

/// What [`ConnectorLog::event`] hands the object backend's writer task.
#[derive(Debug)]
enum Message {
    Line(String),
    /// Flush everything queued and answer — for tests, and for a caller that wants the log on
    /// the store before it does something that may not come back.
    Flush(oneshot::Sender<()>),
}

impl ConnectorLog {
    /// Open (lazily — nothing is created until the first event) the log for `name` under `dir`.
    pub fn new(dir: Option<&Path>, name: &str) -> Self {
        Self {
            sink: dir.map(|dir| Sink::Local(dir.join(format!("{}.jsonl", sanitize(name))))),
        }
    }

    /// Open the log for `name` under `location`, which may be a filesystem path or an
    /// object-store URL.
    ///
    /// The URL is resolved through [`checkpoint_store`] — the same function the offsets go
    /// through — so a log and the checkpoint it sits beside can never end up in two different
    /// places. A location that will not resolve disables the log and says so once on stderr: a
    /// pipeline whose *logs* cannot be written is still a pipeline that should replicate, and
    /// the checkpoint root itself is validated separately and fatally at start.
    pub fn open(engine: Option<&Engine>, location: Option<&str>, name: &str) -> Self {
        let Some(location) = location.map(str::trim).filter(|l| !l.is_empty()) else {
            return Self::default();
        };
        if !location.contains("://") {
            return Self::new(Some(Path::new(location)), name);
        }
        let Some(engine) = engine else {
            eprintln!(
                "[oxidant] connector log `{location}` is an object-store URL, but this source                  was built without an engine to resolve it; the connector log is disabled"
            );
            return Self::default();
        };
        match checkpoint_store(engine, location) {
            Ok(store) => Self::object(store, name),
            Err(e) => {
                eprintln!("[oxidant] connector log `{location}` is unusable: {e}");
                Self::default()
            }
        }
    }

    /// The object backend: a writer task owning the live object, fed over a channel.
    ///
    /// A channel rather than a write per event because [`ConnectorLog::event`] is synchronous
    /// and is called from the middle of the decode loop — awaiting a `PUT` there would put S3's
    /// latency on the CDC critical path, and blocking on one inside the runtime would deadlock.
    fn object(store: CheckpointStore, name: &str) -> Self {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            eprintln!(
                "[oxidant] connector log for `{name}` needs a Tokio runtime to write to an                  object store; the connector log is disabled"
            );
            return Self::default();
        };
        let (tx, rx) = mpsc::unbounded_channel();
        handle.spawn(object_writer(
            store,
            format!("{}.jsonl", sanitize(name)),
            rx,
        ));
        Self {
            sink: Some(Sink::Object(tx)),
        }
    }

    /// Where events land on disk, for tests and for error messages that point an operator at the
    /// file. `None` for a log with no destination *and* for one on an object store — there is no
    /// filesystem path to name.
    pub fn path(&self) -> Option<&Path> {
        match &self.sink {
            Some(Sink::Local(path)) => Some(path.as_path()),
            _ => None,
        }
    }

    /// Wait for everything written so far to reach the store. A no-op for the local backend,
    /// which is already durable by the time [`ConnectorLog::event`] returns.
    pub async fn flush(&self) {
        let Some(Sink::Object(tx)) = &self.sink else {
            return;
        };
        let (done, wait) = oneshot::channel();
        if tx.send(Message::Flush(done)).is_ok() {
            let _ = wait.await;
        }
    }

    /// Append one event. `fields` must be a JSON object; its keys are merged into the line.
    pub fn event(&self, kind: &str, fields: serde_json::Value) {
        let Some(sink) = &self.sink else {
            return;
        };
        let mut line = json!({
            "ts": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
            "event": kind,
        });
        if let (Some(line), Some(fields)) = (line.as_object_mut(), fields.as_object()) {
            for (key, value) in fields {
                line.insert(key.clone(), value.clone());
            }
        }
        match sink {
            Sink::Local(path) => {
                let _ = append(path, &line.to_string());
            }
            // A closed channel means the writer task is gone; like every other log failure it is
            // swallowed rather than allowed to stop a healthy pipeline.
            Sink::Object(tx) => {
                let _ = tx.send(Message::Line(line.to_string()));
            }
        }
    }

    /// An error the connector hit, and whether the batch will be tried again.
    ///
    /// `will_retry` is the field an operator reads first: a retried error is noise until it
    /// repeats, and a non-retried one has already stopped the pipeline.
    pub fn error(&self, message: &str, will_retry: bool) {
        self.event(
            "error",
            json!({ "message": message, "will_retry": will_retry }),
        );
    }
}

/// The object backend's writer: owns the live object and is the only thing that writes it.
///
/// Holds the live object's bytes in memory and re-`PUT`s them, because an object store has no
/// append. It starts from whatever is already there, so a restart continues the operator's
/// record rather than truncating it — that record outliving the driver is the entire reason the
/// log moved off local disk.
async fn object_writer(
    store: CheckpointStore,
    key: String,
    mut rx: mpsc::UnboundedReceiver<Message>,
) {
    let mut live = store.read(&key).await.ok().flatten().unwrap_or_default();
    let mut waiting: Vec<oneshot::Sender<()>> = Vec::new();
    loop {
        let Some(first) = rx.recv().await else {
            // The source was dropped. Write what is left and stop.
            let _ = flush(&store, &key, &live).await;
            break;
        };
        let mut dirty = false;
        let mut message = Some(first);
        // Drain whatever else is already queued, so a batch's worth of events costs one upload.
        loop {
            match message {
                Some(Message::Line(line)) => {
                    live.extend_from_slice(line.as_bytes());
                    live.push(b'\n');
                    dirty = true;
                }
                Some(Message::Flush(done)) => waiting.push(done),
                None => break,
            }
            message = rx.try_recv().ok();
        }
        if dirty {
            // Flush *before* rotating: rotation moves the stored object, and a rotation that ran
            // first would file away a generation missing everything queued since the last write.
            let _ = flush(&store, &key, &live).await;
            if live.len() as u64 >= MAX_OBJECT_LOG_BYTES {
                rotate_objects(&store, &key).await;
                live.clear();
            }
        }
        for done in waiting.drain(..) {
            let _ = done.send(());
        }
        if dirty {
            // Rate-limit the uploads rather than the events: anything that arrives during this
            // sleep is drained by the next pass and folded into one `PUT`.
            tokio::time::sleep(FLUSH_INTERVAL).await;
        }
    }
}

/// `PUT` the live object. A failure is swallowed like every other log write failure, and the
/// buffer is kept so the next flush retries the whole thing.
async fn flush(store: &CheckpointStore, key: &str, live: &[u8]) -> std::io::Result<()> {
    store.write(key, live.to_vec()).await
}

/// `name.jsonl.3` → `name.jsonl.4`, …, `name.jsonl` → `name.jsonl.1`, oldest dropped.
///
/// Copy-then-delete rather than rename: S3 has no rename, and `ObjectStore::rename` is that same
/// pair with a name that suggests otherwise. Rotation is rare enough (one per
/// [`MAX_OBJECT_LOG_BYTES`] of events) that the extra round trips do not matter.
async fn rotate_objects(store: &CheckpointStore, key: &str) {
    let generation = |n: usize| format!("{key}.{n}");
    let _ = store.remove(&generation(MAX_LOG_FILES - 1)).await;
    for n in (1..MAX_LOG_FILES - 1).rev() {
        let _ = store.rename(&generation(n), &generation(n + 1)).await;
    }
    let _ = store.rename(key, &generation(1)).await;
}

/// Keep the file name a file name: a source named `public.orders` must not write to `public/`.
fn sanitize(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if cleaned.trim_matches('.').is_empty() {
        "connector".to_string()
    } else {
        cleaned
    }
}

fn append(path: &Path, line: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Checked before the write rather than after, so the live file never exceeds the cap by more
    // than one line.
    if std::fs::metadata(path).map(|m| m.len()).unwrap_or(0) >= MAX_LOG_BYTES {
        rotate(path);
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    writeln!(file, "{line}")
}

/// `name.jsonl.3` → `name.jsonl.4`, …, `name.jsonl` → `name.jsonl.1`, oldest dropped.
fn rotate(path: &Path) {
    let generation = |n: usize| PathBuf::from(format!("{}.{n}", path.display()));
    let _ = std::fs::remove_file(generation(MAX_LOG_FILES - 1));
    for n in (1..MAX_LOG_FILES - 1).rev() {
        let _ = std::fs::rename(generation(n), generation(n + 1));
    }
    let _ = std::fs::rename(path, generation(1));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(path: &Path) -> Vec<serde_json::Value> {
        std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).expect("each line is one JSON object"))
            .collect()
    }

    #[test]
    fn every_event_is_one_json_object_with_a_timestamp_and_a_kind() {
        let dir = tempfile::TempDir::new().unwrap();
        let log = ConnectorLog::new(Some(dir.path()), "sales_suppliers");
        log.event(
            "snapshot_start",
            json!({ "table": "public.sales_suppliers" }),
        );
        log.error("slot is gone", true);

        let events = lines(&dir.path().join("sales_suppliers.jsonl"));
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["event"], "snapshot_start");
        assert_eq!(events[0]["table"], "public.sales_suppliers");
        assert!(events[0]["ts"].as_str().unwrap().ends_with('Z'));
        assert_eq!(events[1]["event"], "error");
        assert_eq!(events[1]["will_retry"], true);
    }

    #[test]
    fn a_connector_with_no_log_directory_writes_nothing_and_does_not_fail() {
        // The Spark Connect surface has no checkpoint root to write under, and a source built
        // there must still run.
        let log = ConnectorLog::new(None, "anonymous");
        log.event("batch", json!({ "rows": 1 }));
        assert!(log.path().is_none());
    }

    #[test]
    fn a_source_name_can_never_escape_its_log_directory() {
        let dir = tempfile::TempDir::new().unwrap();
        let log = ConnectorLog::new(Some(dir.path()), "../../etc/passwd");
        log.event("batch", json!({}));
        assert_eq!(
            log.path().unwrap(),
            dir.path().join(".._.._etc_passwd.jsonl")
        );
        assert!(log.path().unwrap().exists());
    }

    #[test]
    fn rotation_keeps_five_generations_and_drops_the_oldest() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("c.jsonl");
        // Seed a full live file and four generations, then force one more rotation.
        std::fs::write(&path, vec![b'x'; MAX_LOG_BYTES as usize]).unwrap();
        for n in 1..MAX_LOG_FILES {
            std::fs::write(format!("{}.{n}", path.display()), format!("generation {n}")).unwrap();
        }
        let log = ConnectorLog::new(Some(dir.path()), "c");
        log.event("batch", json!({ "rows": 1 }));

        // The live file is the fresh one, and the count never grows past the cap.
        assert_eq!(lines(&path).len(), 1);
        assert_eq!(
            std::fs::read_to_string(format!("{}.1", path.display()))
                .unwrap()
                .len(),
            MAX_LOG_BYTES as usize,
            "the previous live file became generation 1"
        );
        assert_eq!(
            std::fs::read_to_string(format!("{}.4", path.display())).unwrap(),
            "generation 3",
            "generations shift up and the fifth is dropped"
        );
        assert!(
            !dir.path().join("c.jsonl.5").exists(),
            "rotation never creates a sixth generation"
        );
    }
}
