//! Streaming data sources.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::Engine;

/// A micro-batch data source.
#[async_trait::async_trait]
pub trait Source: Send + Sync {
    /// Read the next micro-batch. Returns empty when no new data is available.
    async fn poll_batch(&mut self, engine: &Engine) -> weft_common::Result<Vec<RecordBatch>>;
}

/// File-directory source: reads new Parquet/JSON/CSV files not yet in the offset set.
pub struct FileSource {
    path: PathBuf,
    format: String,
    seen: HashSet<String>,
    #[allow(dead_code)]
    table_name: String,
}

impl FileSource {
    pub fn new(path: impl AsRef<Path>, format: &str) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            format: format.to_ascii_lowercase(),
            seen: HashSet::new(),
            table_name: "_weft_stream_src".into(),
        }
    }

    pub fn new_files(&self) -> Vec<PathBuf> {
        let Ok(entries) = std::fs::read_dir(&self.path) else {
            return vec![];
        };
        entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.is_file() && !self.seen.contains(&p.to_string_lossy().to_string()))
            .collect()
    }
}

#[async_trait::async_trait]
impl Source for FileSource {
    async fn poll_batch(&mut self, engine: &Engine) -> weft_common::Result<Vec<RecordBatch>> {
        let new_files = self.new_files();
        if new_files.is_empty() {
            return Ok(vec![]);
        }
        let mut all = Vec::new();
        for f in &new_files {
            let sql = match self.format.as_str() {
                "parquet" => format!(
                    "SELECT * FROM parquet_scan('{}')",
                    f.display().to_string().replace('\'', "''")
                ),
                "json" => format!(
                    "SELECT * FROM json_scan('{}')",
                    f.display().to_string().replace('\'', "''")
                ),
                "csv" => format!(
                    "SELECT * FROM csv_scan('{}')",
                    f.display().to_string().replace('\'', "''")
                ),
                _ => continue,
            };
            let batches = engine.sql(&sql).await?;
            all.extend(batches);
            self.seen.insert(f.to_string_lossy().to_string());
        }
        Ok(all)
    }
}

/// Rate source for tests: emits N rows per batch.
pub struct MemoryRateSource {
    rows_per_batch: u64,
    batch_count: u64,
    max_batches: u64,
}

impl MemoryRateSource {
    pub fn new(rows_per_batch: u64, max_batches: u64) -> Self {
        Self {
            rows_per_batch,
            batch_count: 0,
            max_batches,
        }
    }
}

#[async_trait::async_trait]
impl Source for MemoryRateSource {
    async fn poll_batch(&mut self, engine: &Engine) -> weft_common::Result<Vec<RecordBatch>> {
        if self.batch_count >= self.max_batches {
            return Ok(vec![]);
        }
        self.batch_count += 1;
        let sql = format!(
            "SELECT id FROM range(0, {}, 1) AS t(id)",
            self.rows_per_batch
        );
        engine.sql(&sql).await
    }
}

/// Kafka micro-batch source. Reads JSON lines from a spool directory (`WEFT_KAFKA_SPOOL`) or
/// invokes `kafka-console-consumer` when `bootstrap.servers` + `subscribe` are set.
pub struct KafkaSource {
    topic: String,
    brokers: String,
    spool_dir: std::path::PathBuf,
    offset: u64,
}

impl KafkaSource {
    pub fn from_options(options: &std::collections::HashMap<String, String>) -> Self {
        let topic = options
            .get("subscribe")
            .or_else(|| options.get("topic"))
            .cloned()
            .unwrap_or_else(|| "weft".into());
        let brokers = options
            .get("kafka.bootstrap.servers")
            .or_else(|| options.get("bootstrap.servers"))
            .cloned()
            .unwrap_or_else(|| "localhost:9092".into());
        let spool = std::env::var("WEFT_KAFKA_SPOOL")
            .unwrap_or_else(|_| format!("/tmp/weft-kafka-{topic}"));
        Self {
            topic,
            brokers,
            spool_dir: spool.into(),
            offset: 0,
        }
    }
}

#[async_trait::async_trait]
impl Source for KafkaSource {
    async fn poll_batch(&mut self, engine: &Engine) -> weft_common::Result<Vec<RecordBatch>> {
        std::fs::create_dir_all(&self.spool_dir).ok();
        let file = self.spool_dir.join(format!("batch-{}.json", self.offset));
        if file.exists() {
            self.offset += 1;
            let sql = format!(
                "SELECT * FROM json_scan('{}')",
                file.display().to_string().replace('\'', "''")
            );
            return engine.sql(&sql).await;
        }
        // Optional: pull one message via kafka-console-consumer when available.
        let out = std::process::Command::new("kafka-console-consumer")
            .args([
                "--bootstrap-server",
                &self.brokers,
                "--topic",
                &self.topic,
                "--max-messages",
                "100",
                "--timeout-ms",
                "1000",
            ])
            .output();
        if let Ok(o) = out {
            if o.status.success() && !o.stdout.is_empty() {
                std::fs::write(&file, &o.stdout).ok();
                self.offset += 1;
                let sql = format!(
                    "SELECT * FROM json_scan('{}')",
                    file.display().to_string().replace('\'', "''")
                );
                return engine.sql(&sql).await;
            }
        }
        Ok(vec![])
    }
}
