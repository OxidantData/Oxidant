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

use std::io::Write;
use std::path::{Path, PathBuf};

use serde_json::json;

/// Rotate once the live file passes this, keeping [`MAX_LOG_FILES`] generations.
const MAX_LOG_BYTES: u64 = 10 * 1024 * 1024;
/// `<name>.jsonl` plus `.1` … `.4`.
const MAX_LOG_FILES: usize = 5;

/// An append-only JSONL log for one connector.
#[derive(Debug, Clone, Default)]
pub struct ConnectorLog {
    /// `None` when no log directory was configured — a source built from a Spark Connect
    /// `readStream` rather than from a pipeline has no checkpoint root to write under.
    path: Option<PathBuf>,
}

impl ConnectorLog {
    /// Open (lazily — nothing is created until the first event) the log for `name` under `dir`.
    pub fn new(dir: Option<&Path>, name: &str) -> Self {
        Self {
            path: dir.map(|dir| dir.join(format!("{}.jsonl", sanitize(name)))),
        }
    }

    /// Where events land, for tests and for error messages that point an operator at the file.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Append one event. `fields` must be a JSON object; its keys are merged into the line.
    pub fn event(&self, kind: &str, fields: serde_json::Value) {
        let Some(path) = &self.path else {
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
        let _ = append(path, &line.to_string());
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
