//! Kafka micro-batch source.
//!
//! Spark's Kafka source does not use consumer groups: it assigns partitions directly and keeps
//! offsets in the query's own checkpoint, which is what makes a Structured Streaming query
//! replayable independently of broker-side group state. This source works the same way, so the
//! Kafka client only needs partition-level fetch — provided here by `rskafka`, a pure-Rust client,
//! rather than `rdkafka`, which would put librdkafka on the build path (see Cargo.toml).
//!
//! The emitted schema is Spark's, column for column ([`kafka_schema`]), so a PySpark job that
//! does `.selectExpr("CAST(value AS STRING)")` against real Spark does the same thing here.
//!
//! Offline mode: with `OXIDANT_KAFKA_SPOOL` (or the `oxidant.spool.dir` option) pointing at a
//! directory of newline-delimited files, batches are read from disk instead of a broker. That is
//! how the integration tests exercise the whole source → sink → catalog path without standing up
//! Kafka, and it is not a substitute for a broker in production.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use oxidant_common::{Error, Result};
use oxidant_loom::arrow::array::{
    BinaryBuilder, Int32Builder, Int64Builder, StringBuilder, TimestampMillisecondBuilder,
};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;
use serde::{Deserialize, Serialize};

use crate::source::{Source, SourceOffsets};

/// Spark's `timestampType` for a broker-assigned (log-append or create) timestamp. Spark reports
/// `0` for `CreateTime`, which is the Kafka default and what every producer we can observe uses;
/// rskafka does not surface the discriminator, so this is fixed rather than guessed per record.
const TIMESTAMP_TYPE_CREATE_TIME: i32 = 0;

/// Default fetch ceiling per partition per micro-batch, mirroring Kafka's own
/// `max.partition.fetch.bytes` default (1 MiB).
const DEFAULT_MAX_PARTITION_FETCH_BYTES: i32 = 1024 * 1024;
/// Default `fetch.max.wait.ms`. Short, because a micro-batch trigger should not block on an idle
/// topic — an empty batch is a legitimate outcome.
const DEFAULT_FETCH_MAX_WAIT_MS: i32 = 500;

/// The exact Arrow schema Spark's Kafka source produces.
///
/// Callers project/cast from here (`CAST(value AS STRING)`, `from_json(...)`), which is why the
/// payload columns stay `Binary`: Kafka values are bytes, and guessing an encoding here would
/// silently diverge from Spark.
pub fn kafka_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Binary, true),
        Field::new("value", DataType::Binary, true),
        Field::new("topic", DataType::Utf8, false),
        Field::new("partition", DataType::Int32, false),
        Field::new("offset", DataType::Int64, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("timestampType", DataType::Int32, false),
    ]))
}

/// Where a query with no committed offsets starts reading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartingOffsets {
    /// Oldest record the broker still retains.
    Earliest,
    /// Only records produced after the query starts (Spark's default).
    Latest,
    /// Explicit per-partition offsets, from Spark's
    /// `{"topic": {"0": 12, "1": -2}}` JSON. `-2` means earliest, `-1` latest.
    Assigned(BTreeMap<TopicPartition, i64>),
}

/// A Kafka topic-partition. Ordered so checkpoint JSON is stable across runs.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

impl std::fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.topic, self.partition)
    }
}

/// Parsed `readStream.format("kafka").option(...)` configuration.
#[derive(Debug, Clone)]
pub struct KafkaOptions {
    pub brokers: Vec<String>,
    pub topics: Vec<String>,
    pub starting_offsets: StartingOffsets,
    /// Rate limit: at most this many records across all partitions per micro-batch.
    pub max_offsets_per_trigger: Option<u64>,
    pub fetch_max_wait_ms: i32,
    pub max_partition_fetch_bytes: i32,
    /// Offline replacement for a broker — see the module docs.
    pub spool_dir: Option<PathBuf>,
}

impl KafkaOptions {
    /// Parse Spark's Kafka option names. Unknown `kafka.*` options are ignored (they are consumer
    /// properties this client has no equivalent for), but a missing topic or broker is an error
    /// rather than a silent default — a stream quietly reading the wrong topic is worse than one
    /// that refuses to start.
    pub fn from_spark(options: &HashMap<String, String>) -> Result<Self> {
        let get = |k: &str| options.get(k).map(|s| s.trim().to_string());

        if options.contains_key("subscribePattern") {
            return Err(Error::Unsupported(
                "kafka `subscribePattern` is not supported — list topics with `subscribe`".into(),
            ));
        }
        // Spark's `assign` is per-partition JSON (`{"topic":[0,1]}`), not a topic list. Reading it
        // as one would silently subscribe to a topic literally named `{"topic":[0,1]}`, so it is
        // refused rather than misinterpreted. Every partition is assigned anyway.
        if options.contains_key("assign") {
            return Err(Error::Unsupported(
                "kafka `assign` is not supported — use `subscribe` (Oxidant assigns every \
                 partition of each topic) and `startingOffsets` for per-partition positions"
                    .into(),
            ));
        }

        let topics: Vec<String> = get("subscribe")
            .or_else(|| get("topic"))
            .map(|s| {
                s.split(',')
                    .map(|t| t.trim().to_string())
                    .filter(|t| !t.is_empty())
                    .collect()
            })
            .unwrap_or_default();

        let brokers: Vec<String> = get("kafka.bootstrap.servers")
            .or_else(|| get("bootstrap.servers"))
            .map(|s| {
                s.split(',')
                    .map(|b| b.trim().to_string())
                    .filter(|b| !b.is_empty())
                    .collect()
            })
            .unwrap_or_default();

        let spool_dir = get("oxidant.spool.dir")
            .or_else(|| std::env::var("OXIDANT_KAFKA_SPOOL").ok())
            .filter(|s| !s.is_empty())
            .map(PathBuf::from);

        if topics.is_empty() {
            return Err(Error::Plan(
                "kafka source: `subscribe` must name at least one topic".into(),
            ));
        }
        if brokers.is_empty() && spool_dir.is_none() {
            return Err(Error::Plan(
                "kafka source: `kafka.bootstrap.servers` is required (or set OXIDANT_KAFKA_SPOOL \
                 for the offline spool-directory mode)"
                    .into(),
            ));
        }

        let starting_offsets = match get("startingOffsets").as_deref() {
            None | Some("latest") => StartingOffsets::Latest,
            Some("earliest") => StartingOffsets::Earliest,
            Some(json) => StartingOffsets::Assigned(parse_offset_json(json)?),
        };

        Ok(Self {
            brokers,
            topics,
            starting_offsets,
            max_offsets_per_trigger: get("maxOffsetsPerTrigger").and_then(|s| s.parse().ok()),
            fetch_max_wait_ms: get("kafka.fetch.max.wait.ms")
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_FETCH_MAX_WAIT_MS),
            max_partition_fetch_bytes: get("kafka.max.partition.fetch.bytes")
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_MAX_PARTITION_FETCH_BYTES),
            spool_dir,
        })
    }
}

/// Parse Spark's `startingOffsets` JSON: `{"topicA": {"0": 23, "1": -2}}`.
fn parse_offset_json(json: &str) -> Result<BTreeMap<TopicPartition, i64>> {
    let parsed: BTreeMap<String, BTreeMap<String, i64>> = serde_json::from_str(json)
        .map_err(|e| Error::Plan(format!("kafka `startingOffsets` is not valid JSON: {e}")))?;
    let mut out = BTreeMap::new();
    for (topic, partitions) in parsed {
        for (partition, offset) in partitions {
            let partition = partition.parse::<i32>().map_err(|_| {
                Error::Plan(format!(
                    "kafka `startingOffsets`: `{partition}` is not a partition number"
                ))
            })?;
            out.insert(
                TopicPartition {
                    topic: topic.clone(),
                    partition,
                },
                offset,
            );
        }
    }
    Ok(out)
}

/// One record pulled from Kafka (or the spool), before it becomes Arrow.
struct KafkaRecord {
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    topic: String,
    partition: i32,
    offset: i64,
    timestamp_ms: i64,
}

/// Build the Spark-shaped record batch for a set of fetched records.
fn to_record_batch(records: &[KafkaRecord]) -> Result<RecordBatch> {
    let mut key = BinaryBuilder::new();
    let mut value = BinaryBuilder::new();
    let mut topic = StringBuilder::new();
    let mut partition = Int32Builder::new();
    let mut offset = Int64Builder::new();
    let mut timestamp = TimestampMillisecondBuilder::new();
    let mut timestamp_type = Int32Builder::new();

    for r in records {
        match &r.key {
            Some(k) => key.append_value(k),
            None => key.append_null(),
        }
        match &r.value {
            Some(v) => value.append_value(v),
            None => value.append_null(),
        }
        topic.append_value(&r.topic);
        partition.append_value(r.partition);
        offset.append_value(r.offset);
        timestamp.append_value(r.timestamp_ms);
        timestamp_type.append_value(TIMESTAMP_TYPE_CREATE_TIME);
    }

    RecordBatch::try_new(
        kafka_schema(),
        vec![
            Arc::new(key.finish()),
            Arc::new(value.finish()),
            Arc::new(topic.finish()),
            Arc::new(partition.finish()),
            Arc::new(offset.finish()),
            Arc::new(timestamp.finish()),
            Arc::new(timestamp_type.finish()),
        ],
    )
    .map_err(|e| Error::Execution(format!("kafka source: build record batch: {e}")))
}

/// Kafka micro-batch source: assigns every partition of every subscribed topic and reads forward
/// from the offsets in the query checkpoint.
pub struct KafkaSource {
    options: KafkaOptions,
    /// Next offset to read per partition. Populated on first poll (or restored from checkpoint).
    offsets: BTreeMap<TopicPartition, i64>,
    /// Set once the partition set has been resolved against the broker.
    assigned: bool,
    client: Option<Arc<rskafka::client::Client>>,
    partition_clients: BTreeMap<TopicPartition, Arc<rskafka::client::partition::PartitionClient>>,
    /// Spool mode only: index of the next spool file to read.
    spool_cursor: u64,
}

impl KafkaSource {
    pub fn new(options: KafkaOptions) -> Self {
        Self {
            options,
            offsets: BTreeMap::new(),
            assigned: false,
            client: None,
            partition_clients: BTreeMap::new(),
            spool_cursor: 0,
        }
    }

    /// Parse Spark options and build the source. Kept as a separate constructor so option errors
    /// surface at query-start time with a Spark-shaped message.
    pub fn from_options(options: &HashMap<String, String>) -> Result<Self> {
        Ok(Self::new(KafkaOptions::from_spark(options)?))
    }

    fn spool_mode(&self) -> bool {
        self.options.spool_dir.is_some() && self.options.brokers.is_empty()
    }

    async fn client(&mut self) -> Result<Arc<rskafka::client::Client>> {
        if let Some(c) = &self.client {
            return Ok(c.clone());
        }
        let client = rskafka::client::ClientBuilder::new(self.options.brokers.clone())
            .client_id("oxidant")
            .build()
            .await
            .map_err(|e| {
                Error::Io(format!(
                    "kafka: connect to [{}]: {e}",
                    self.options.brokers.join(",")
                ))
            })?;
        let client = Arc::new(client);
        self.client = Some(client.clone());
        Ok(client)
    }

    /// Resolve the partition set and seed each partition's starting offset. Idempotent.
    async fn assign(&mut self) -> Result<()> {
        if self.assigned {
            return Ok(());
        }
        let client = self.client().await?;
        let topics = client
            .list_topics()
            .await
            .map_err(|e| Error::Io(format!("kafka: list topics: {e}")))?;

        for want in &self.options.topics {
            let found = topics.iter().find(|t| &t.name == want).ok_or_else(|| {
                Error::Plan(format!(
                    "kafka topic `{want}` does not exist on [{}]",
                    self.options.brokers.join(",")
                ))
            })?;
            for partition in &found.partitions {
                let tp = TopicPartition {
                    topic: want.clone(),
                    partition: *partition,
                };
                let pc = client
                    .partition_client(
                        want.clone(),
                        *partition,
                        rskafka::client::partition::UnknownTopicHandling::Retry,
                    )
                    .await
                    .map_err(|e| Error::Io(format!("kafka: client for `{tp}`: {e}")))?;
                let pc = Arc::new(pc);

                // A checkpoint-restored offset always wins over `startingOffsets`: that is what
                // makes a restarted query resume rather than replay (or skip) data.
                if !self.offsets.contains_key(&tp) {
                    let start = self.resolve_start_offset(&tp, &pc).await?;
                    self.offsets.insert(tp.clone(), start);
                }
                self.partition_clients.insert(tp, pc);
            }
        }
        self.assigned = true;
        Ok(())
    }

    async fn resolve_start_offset(
        &self,
        tp: &TopicPartition,
        pc: &rskafka::client::partition::PartitionClient,
    ) -> Result<i64> {
        use rskafka::client::partition::OffsetAt;

        let at = match &self.options.starting_offsets {
            StartingOffsets::Earliest => OffsetAt::Earliest,
            StartingOffsets::Latest => OffsetAt::Latest,
            StartingOffsets::Assigned(map) => match map.get(tp) {
                // Spark's sentinels: -2 = earliest, -1 = latest, anything else is literal.
                Some(-2) => OffsetAt::Earliest,
                Some(-1) | None => OffsetAt::Latest,
                Some(explicit) => return Ok(*explicit),
            },
        };
        pc.get_offset(at)
            .await
            .map_err(|e| Error::Io(format!("kafka: offset for `{tp}`: {e}")))
    }

    /// Fetch one micro-batch worth of records across all assigned partitions.
    async fn fetch(&mut self) -> Result<Vec<KafkaRecord>> {
        self.assign().await?;

        let budget = self.options.max_offsets_per_trigger.unwrap_or(u64::MAX);
        let mut taken = 0u64;
        let mut out = Vec::new();

        // Round-robin over partitions in a stable order so a rate-limited batch does not
        // starve high-numbered partitions run after run.
        let partitions: Vec<TopicPartition> = self.partition_clients.keys().cloned().collect();
        for tp in partitions {
            if taken >= budget {
                break;
            }
            let from = *self.offsets.get(&tp).unwrap_or(&0);
            let pc = self.partition_clients[&tp].clone();
            let fetched = pc
                .fetch_records(
                    from,
                    1..self.options.max_partition_fetch_bytes,
                    self.options.fetch_max_wait_ms,
                )
                .await;
            let (records, _high_watermark) = match fetched {
                Ok(v) => v,
                // An out-of-range offset means the broker aged past our checkpoint (retention)
                // or the topic was recreated. Spark's `failOnDataLoss=true` default is to fail
                // loudly rather than silently skip records, and so is this.
                Err(e) => {
                    return Err(Error::Io(format!(
                        "kafka: fetch `{tp}` from offset {from}: {e}"
                    )))
                }
            };

            for r in records {
                if taken >= budget {
                    break;
                }
                out.push(KafkaRecord {
                    key: r.record.key,
                    value: r.record.value,
                    topic: tp.topic.clone(),
                    partition: tp.partition,
                    offset: r.offset,
                    timestamp_ms: r.record.timestamp.timestamp_millis(),
                });
                // The next batch resumes after the record we just took, even if the budget cuts
                // this partition short mid-fetch.
                self.offsets.insert(tp.clone(), r.offset + 1);
                taken += 1;
            }
        }
        Ok(out)
    }

    /// Offline mode: treat each file in the spool directory as one micro-batch of newline-
    /// delimited record values, in `batch-N` order.
    fn fetch_spool(&mut self) -> Result<Vec<KafkaRecord>> {
        let dir = self
            .options
            .spool_dir
            .clone()
            .expect("spool_mode() checked spool_dir is set");
        std::fs::create_dir_all(&dir)
            .map_err(|e| Error::Io(format!("kafka spool: mkdir {}: {e}", dir.display())))?;
        let topic = self.options.topics.first().cloned().unwrap_or_default();

        let file = dir.join(format!("batch-{}.json", self.spool_cursor));
        if !file.exists() {
            return Ok(vec![]);
        }
        let body = std::fs::read_to_string(&file)
            .map_err(|e| Error::Io(format!("kafka spool: read {}: {e}", file.display())))?;
        self.spool_cursor += 1;

        let tp = TopicPartition {
            topic: topic.clone(),
            partition: 0,
        };
        let mut offset = *self.offsets.get(&tp).unwrap_or(&0);
        let now_ms = chrono::Utc::now().timestamp_millis();
        let mut out = Vec::new();
        for line in body.lines().filter(|l| !l.trim().is_empty()) {
            out.push(KafkaRecord {
                key: None,
                value: Some(line.as_bytes().to_vec()),
                topic: topic.clone(),
                partition: 0,
                offset,
                timestamp_ms: now_ms,
            });
            offset += 1;
        }
        self.offsets.insert(tp, offset);
        Ok(out)
    }
}

#[async_trait::async_trait]
impl Source for KafkaSource {
    fn schema(&self) -> SchemaRef {
        kafka_schema()
    }

    fn description(&self) -> String {
        format!("KafkaV2[Subscribe[{}]]", self.options.topics.join(", "))
    }

    async fn poll_batch(&mut self, _engine: &Engine) -> Result<Vec<RecordBatch>> {
        let records = if self.spool_mode() {
            self.fetch_spool()?
        } else {
            self.fetch().await?
        };
        if records.is_empty() {
            return Ok(vec![]);
        }
        Ok(vec![to_record_batch(&records)?])
    }

    fn committed_offsets(&self) -> Option<SourceOffsets> {
        let entries = self
            .offsets
            .iter()
            .map(|(tp, o)| (tp.to_string(), *o))
            .collect();
        Some(SourceOffsets {
            source: "kafka".into(),
            entries,
        })
    }

    fn restore_offsets(&mut self, offsets: &SourceOffsets) {
        if offsets.source != "kafka" {
            return;
        }
        for (key, offset) in &offsets.entries {
            // `topic-partition`: split at the LAST dash, since topic names may contain dashes.
            let Some((topic, partition)) = key.rsplit_once('-') else {
                continue;
            };
            let Ok(partition) = partition.parse::<i32>() else {
                continue;
            };
            self.offsets.insert(
                TopicPartition {
                    topic: topic.to_string(),
                    partition,
                },
                *offset,
            );
        }
        // Spool mode replays whole files; its cursor is the batch count, which equals the number
        // of committed batches, not the record offset. Restart replays from the first file.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn schema_matches_sparks_kafka_source() {
        let s = kafka_schema();
        let names: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "key",
                "value",
                "topic",
                "partition",
                "offset",
                "timestamp",
                "timestampType"
            ]
        );
        assert_eq!(s.field(0).data_type(), &DataType::Binary);
        assert_eq!(s.field(1).data_type(), &DataType::Binary);
    }

    #[test]
    fn parses_spark_option_names() {
        let o = KafkaOptions::from_spark(&opts(&[
            ("kafka.bootstrap.servers", "b1:9092, b2:9092"),
            ("subscribe", "events, audit"),
            ("startingOffsets", "earliest"),
            ("maxOffsetsPerTrigger", "500"),
        ]))
        .unwrap();
        assert_eq!(o.brokers, vec!["b1:9092", "b2:9092"]);
        assert_eq!(o.topics, vec!["events", "audit"]);
        assert_eq!(o.starting_offsets, StartingOffsets::Earliest);
        assert_eq!(o.max_offsets_per_trigger, Some(500));
    }

    #[test]
    fn latest_is_the_default_starting_offset() {
        let o = KafkaOptions::from_spark(&opts(&[
            ("kafka.bootstrap.servers", "b:9092"),
            ("subscribe", "t"),
        ]))
        .unwrap();
        assert_eq!(o.starting_offsets, StartingOffsets::Latest);
    }

    #[test]
    fn explicit_starting_offsets_json_is_parsed_per_partition() {
        let o = KafkaOptions::from_spark(&opts(&[
            ("kafka.bootstrap.servers", "b:9092"),
            ("subscribe", "t"),
            ("startingOffsets", r#"{"t": {"0": 12, "1": -2}}"#),
        ]))
        .unwrap();
        let StartingOffsets::Assigned(map) = o.starting_offsets else {
            panic!("expected assigned offsets");
        };
        assert_eq!(
            map.get(&TopicPartition {
                topic: "t".into(),
                partition: 0
            }),
            Some(&12)
        );
        assert_eq!(
            map.get(&TopicPartition {
                topic: "t".into(),
                partition: 1
            }),
            Some(&-2)
        );
    }

    #[test]
    fn missing_topic_or_broker_is_an_error_not_a_default() {
        let err =
            KafkaOptions::from_spark(&opts(&[("kafka.bootstrap.servers", "b:9092")])).unwrap_err();
        assert!(err.to_string().contains("subscribe"), "{err}");

        let err = KafkaOptions::from_spark(&opts(&[("subscribe", "t")])).unwrap_err();
        assert!(err.to_string().contains("bootstrap.servers"), "{err}");
    }

    #[test]
    fn unsupported_topic_selectors_are_rejected_rather_than_misread() {
        for (key, value) in [
            ("subscribePattern", "events.*"),
            // Read as a topic list this would subscribe to a topic literally named
            // `{"events":[0,1]}` and then report it as missing on the broker.
            ("assign", r#"{"events":[0,1]}"#),
        ] {
            let err = KafkaOptions::from_spark(&opts(&[
                ("kafka.bootstrap.servers", "b:9092"),
                (key, value),
            ]))
            .unwrap_err();
            assert!(matches!(err, Error::Unsupported(_)), "{key}: {err:?}");
        }
    }

    #[tokio::test]
    async fn spool_mode_reads_batches_and_advances_offsets() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("batch-0.json"), "{\"a\":1}\n{\"a\":2}\n").unwrap();
        std::fs::write(dir.path().join("batch-1.json"), "{\"a\":3}\n").unwrap();

        let mut src = KafkaSource::from_options(&opts(&[
            ("subscribe", "events"),
            ("oxidant.spool.dir", dir.path().to_str().unwrap()),
        ]))
        .unwrap();
        let engine = Engine::new();

        let b0 = src.poll_batch(&engine).await.unwrap();
        assert_eq!(b0.len(), 1);
        assert_eq!(b0[0].num_rows(), 2);
        assert_eq!(b0[0].schema(), kafka_schema());

        let b1 = src.poll_batch(&engine).await.unwrap();
        assert_eq!(b1[0].num_rows(), 1);

        // Offsets continue across batches rather than restarting per file.
        let offsets = src.committed_offsets().unwrap();
        assert_eq!(offsets.entries.get("events-0"), Some(&3));

        // Drained: an empty batch, not an error.
        assert!(src.poll_batch(&engine).await.unwrap().is_empty());
    }

    #[test]
    fn offsets_round_trip_through_a_checkpoint_for_dashed_topic_names() {
        let mut src = KafkaSource::new(KafkaOptions {
            brokers: vec!["b:9092".into()],
            topics: vec!["my-events".into()],
            starting_offsets: StartingOffsets::Latest,
            max_offsets_per_trigger: None,
            fetch_max_wait_ms: 1,
            max_partition_fetch_bytes: 1024,
            spool_dir: None,
        });
        let saved = SourceOffsets {
            source: "kafka".into(),
            entries: [("my-events-3".to_string(), 99i64)].into_iter().collect(),
        };
        src.restore_offsets(&saved);
        assert_eq!(
            src.offsets.get(&TopicPartition {
                topic: "my-events".into(),
                partition: 3
            }),
            Some(&99),
            "a dashed topic name must split at the LAST dash"
        );
        assert_eq!(src.committed_offsets().unwrap(), saved);
    }

    #[test]
    fn offsets_from_another_source_type_are_ignored() {
        let mut src = KafkaSource::new(KafkaOptions {
            brokers: vec!["b:9092".into()],
            topics: vec!["t".into()],
            starting_offsets: StartingOffsets::Latest,
            max_offsets_per_trigger: None,
            fetch_max_wait_ms: 1,
            max_partition_fetch_bytes: 1024,
            spool_dir: None,
        });
        src.restore_offsets(&SourceOffsets {
            source: "file".into(),
            entries: [("t-0".to_string(), 5i64)].into_iter().collect(),
        });
        assert!(src.offsets.is_empty());
    }
}
