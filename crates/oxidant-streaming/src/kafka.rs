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
use std::time::{Duration, Instant};

use oxidant_common::{Error, Result};
use oxidant_loom::arrow::array::{
    BinaryBuilder, Int32Builder, Int64Builder, StringBuilder, TimestampMillisecondBuilder,
};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;
use serde::{Deserialize, Serialize};

use crate::source::{BatchRange, Source, SourceOffsets};

/// Spark's `timestampType` for a broker-assigned (log-append or create) timestamp. Spark reports
/// `0` for `CreateTime`, which is the Kafka default and what every producer we can observe uses;
/// rskafka does not surface the discriminator, so this is fixed rather than guessed per record.
const TIMESTAMP_TYPE_CREATE_TIME: i32 = 0;

/// Default fetch ceiling per partition per micro-batch, mirroring Kafka's own
/// `max.partition.fetch.bytes` default (1 MiB).
const DEFAULT_MAX_PARTITION_FETCH_BYTES: i32 = 1024 * 1024;
/// Default `fetch.max.wait.ms`. Short, because a micro-batch trigger should not block on an idle
/// topic — an empty batch is a legitimate outcome. Partitions are fetched concurrently, so this
/// is the cost of an idle *batch*, not an idle partition.
const DEFAULT_FETCH_MAX_WAIT_MS: i32 = 500;
/// How long a partition assignment is trusted before the broker is asked again.
///
/// Kafka's own `metadata.max.age.ms` default is five minutes, which is far too long for a stream:
/// a topic expanded from 12 to 24 partitions — the ordinary response to load — would go on
/// reading only the original twelve until the query restarted, silently.
const DEFAULT_METADATA_MAX_AGE_MS: u64 = 30_000;

/// Marks a batch range (and checkpoint position) as belonging to the offline spool rather than a
/// broker. The two key their positions differently — a file index versus topic-partitions — and
/// naming the variant keeps one from ever being read as the other.
const SPOOL_SOURCE: &str = "kafka-spool";
const SPOOL_FILE_KEY: &str = "file";
const SPOOL_OFFSET_KEY: &str = "offset";

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
    /// How stale a partition assignment may get before it is re-resolved against the broker.
    pub metadata_max_age: Duration,
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
            metadata_max_age: Duration::from_millis(
                get("kafka.metadata.max.age.ms")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(DEFAULT_METADATA_MAX_AGE_MS),
            ),
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
    /// Last high watermark each partition reported, which is how the next batch estimates lag
    /// without spending a round trip per partition asking for it.
    high_watermarks: BTreeMap<TopicPartition, i64>,
    /// When the partition assignment was last resolved against the broker.
    assigned_at: Option<Instant>,
    /// Rotates which partition is served first when the budget cannot cover them all, so a
    /// budget smaller than the partition count still drains every partition over time.
    round_robin: usize,
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
            high_watermarks: BTreeMap::new(),
            assigned_at: None,
            round_robin: 0,
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

    /// Resolve the partition set and seed each partition's starting offset.
    ///
    /// Re-run on an interval rather than once: partitions get *added* to busy topics, and a
    /// source that resolved its assignment once would never read them. Existing partitions keep
    /// their offsets; a partition that appears later starts at `earliest`, because every record
    /// in it postdates the query and skipping to `latest` would drop data the query should see.
    async fn assign(&mut self) -> Result<()> {
        let fresh = self
            .assigned_at
            .is_some_and(|at| at.elapsed() < self.options.metadata_max_age);
        if self.assigned && fresh {
            return Ok(());
        }
        let first_assignment = !self.assigned;
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
                if self.partition_clients.contains_key(&tp) {
                    continue;
                }
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
                    let start = self
                        .resolve_start_offset(&tp, &pc, first_assignment)
                        .await?;
                    self.offsets.insert(tp.clone(), start);
                }
                self.partition_clients.insert(tp, pc);
            }
        }
        self.assigned = true;
        self.assigned_at = Some(Instant::now());
        Ok(())
    }

    async fn resolve_start_offset(
        &self,
        tp: &TopicPartition,
        pc: &rskafka::client::partition::PartitionClient,
        first_assignment: bool,
    ) -> Result<i64> {
        use rskafka::client::partition::OffsetAt;

        // A partition that appeared *after* the query started holds only records the query has
        // not seen, so it is read from the beginning whatever `startingOffsets` said.
        if !first_assignment {
            return pc
                .get_offset(OffsetAt::Earliest)
                .await
                .map_err(|e| Error::Io(format!("kafka: offset for new partition `{tp}`: {e}")));
        }
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

    /// Ask every partition where its log currently ends, concurrently.
    ///
    /// This is the round trip that makes a batch's extent knowable *before* it is read, and it
    /// is what the previous design lacked: budgets were split using the high watermarks left
    /// over from the last fetch, which are stale by a whole trigger interval and simply absent
    /// after a restart. Two attempts at the same batch could therefore divide the same budget
    /// differently across partitions and cover different records — enough on its own to make a
    /// replay diverge, even with identical start offsets.
    async fn latest_offsets(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<BTreeMap<TopicPartition, i64>> {
        use rskafka::client::partition::OffsetAt;

        let queries = partitions.iter().map(|tp| {
            let pc = self.partition_clients[tp].clone();
            let tp = tp.clone();
            async move { (tp, pc.get_offset(OffsetAt::Latest).await) }
        });
        let mut ends = BTreeMap::new();
        for (tp, result) in futures::future::join_all(queries).await {
            let end =
                result.map_err(|e| Error::Io(format!("kafka: latest offset for `{tp}`: {e}")))?;
            ends.insert(tp, end);
        }
        Ok(ends)
    }

    /// Read exactly `[start, end)` on every partition the range names, concurrently.
    ///
    /// A single fetch is capped by `max.partition.fetch.bytes`, so covering the range can take
    /// several rounds per partition; an empty response means the rest of the range does not
    /// exist as records (compaction, or a transaction marker holding an offset) and ends that
    /// partition rather than spinning.
    async fn fetch_range(&mut self, range: &BatchRange) -> Result<Vec<KafkaRecord>> {
        /// Rounds a single partition gets to cover its slice before the batch gives up. Reached
        /// only if every round returns one record against a very large budget.
        const MAX_FETCH_ROUNDS: usize = 512;

        self.assign().await?;

        let mut plans = Vec::new();
        for (key, end) in &range.end {
            let Some(tp) = parse_topic_partition(key) else {
                continue;
            };
            let from = range.start.get(key).copied().unwrap_or(0);
            if *end <= from {
                continue;
            }
            let Some(pc) = self.partition_clients.get(&tp).cloned() else {
                // The partition was in the plan but is not assigned now. Failing is right: the
                // batch cannot be reproduced, and silently returning fewer records would be
                // committed under this batch id as though it were complete.
                return Err(Error::Io(format!(
                    "kafka: partition `{tp}` is in batch range but not assigned"
                )));
            };
            plans.push((tp, pc, from, *end));
        }

        let bytes_cap = self.options.max_partition_fetch_bytes;
        let wait = self.options.fetch_max_wait_ms;
        let fetches = plans.into_iter().map(|(tp, pc, from, end)| async move {
            let mut records = Vec::new();
            let mut next = from;
            let mut rounds = 0;
            while next < end && rounds < MAX_FETCH_ROUNDS {
                rounds += 1;
                let fetched = match pc.fetch_records(next, 1..bytes_cap, wait).await {
                    Ok((fetched, _high_watermark)) => fetched,
                    // An out-of-range offset means the broker aged past this range (retention)
                    // or the topic was recreated. Spark's `failOnDataLoss=true` default is to
                    // fail loudly rather than silently skip records, and so is this.
                    Err(e) => {
                        return Err(Error::Io(format!(
                            "kafka: fetch `{tp}` from offset {next}: {e}"
                        )))
                    }
                };
                if fetched.is_empty() {
                    break;
                }
                for r in fetched {
                    if r.offset >= end {
                        break;
                    }
                    next = r.offset + 1;
                    records.push(KafkaRecord {
                        key: r.record.key,
                        value: r.record.value,
                        topic: tp.topic.clone(),
                        partition: tp.partition,
                        offset: r.offset,
                        timestamp_ms: r.record.timestamp.timestamp_millis(),
                    });
                }
            }
            Ok(records)
        });

        let mut out = Vec::new();
        for result in futures::future::join_all(fetches).await {
            out.extend(result?);
        }
        // Ordered by (partition, offset) so a replay produces byte-identical batches whatever
        // order the concurrent fetches happened to complete in.
        out.sort_by_key(|r| (r.partition, r.offset));
        Ok(out)
    }

    /// Divide `maxOffsetsPerTrigger` across the assigned partitions.
    ///
    /// This is the whole reason the option is safe to use. Spending the budget on partitions in
    /// order — taking as much as the first will give, then the second, and stopping when the
    /// budget runs out — means a busy partition 0 consumes the entire allowance on every batch
    /// and the rest of the topic is *never read again*: no error, no warning, just unbounded and
    /// invisible lag on every partition but one.
    ///
    /// So each partition gets a floor of one record, and the remainder is split in proportion to
    /// the lag each partition reported last time. When the budget cannot even cover the floor,
    /// the starting partition rotates per batch, so every partition is still drained over time.
    fn partition_budgets(
        &self,
        partitions: &[TopicPartition],
        lag: &BTreeMap<TopicPartition, u64>,
    ) -> BTreeMap<TopicPartition, u64> {
        let Some(budget) = self.options.max_offsets_per_trigger else {
            return partitions.iter().map(|tp| (tp.clone(), u64::MAX)).collect();
        };
        let n = partitions.len() as u64;
        if n == 0 {
            return BTreeMap::new();
        }

        // Fewer records allowed than partitions: serve a rotating window of one each.
        if budget < n {
            let start = self.round_robin % partitions.len();
            return partitions
                .iter()
                .enumerate()
                .map(|(i, tp)| {
                    let offset = (i + partitions.len() - start) % partitions.len();
                    (tp.clone(), u64::from((offset as u64) < budget))
                })
                .collect();
        }

        // Lag measured against the end offsets this batch was just planned from, so both
        // attempts at a batch split the budget identically. A partition with nothing waiting
        // still counts for one, so it keeps its floor.
        let lag = |tp: &TopicPartition| -> u64 { lag.get(tp).copied().unwrap_or(1) };
        let total_lag: u128 = partitions.iter().map(|tp| lag(tp) as u128).sum();

        let mut shares: BTreeMap<TopicPartition, u64> =
            partitions.iter().map(|tp| (tp.clone(), 1)).collect();
        let mut remaining = budget - n;
        if total_lag > 0 {
            for tp in partitions {
                let share = (remaining as u128 * lag(tp) as u128 / total_lag) as u64;
                *shares.get_mut(tp).expect("seeded above") += share;
            }
            // Integer division leaves a few records unassigned; hand them to the hungriest.
            let assigned: u64 = shares.values().sum();
            remaining = budget.saturating_sub(assigned);
        }
        // Spread whatever is left evenly, computed rather than counted out one record at a time.
        // After the proportional pass `remaining` is only the integer-division dust, but when
        // there is no lag at all — every partition caught up, which is the steady state for a
        // healthy stream — the proportional pass is skipped and `remaining` is the *entire*
        // budget. Handing it out in a loop would cost one map lookup per permitted record on
        // every trigger, so a large `maxOffsetsPerTrigger` would quietly burn CPU precisely when
        // the pipeline has nothing to do.
        let each = remaining / n;
        let extra = remaining % n;
        let start = self.round_robin % partitions.len();
        for (i, tp) in partitions.iter().enumerate() {
            // Rotate which partitions get the odd record left over, so the same ones are not
            // favoured on every batch.
            let rotated = ((i + partitions.len() - start) % partitions.len()) as u64;
            *shares.get_mut(tp).expect("seeded above") += each + u64::from(rotated < extra);
        }
        shares
    }

    /// Offline mode: the batch is one spool file, named by index.
    ///
    /// The range carries the record offsets the file will produce as well as its index. Without
    /// them a replay would number the same records differently depending on what the source had
    /// already read, and `offset` is a column the query can see.
    fn plan_spool(&self) -> Result<BatchRange> {
        let Some(dir) = &self.options.spool_dir else {
            return Ok(BatchRange::default());
        };
        let file = dir.join(format!("batch-{}.json", self.spool_cursor));
        if !file.exists() {
            return Ok(BatchRange::default());
        }
        let body = std::fs::read_to_string(&file)
            .map_err(|e| Error::Io(format!("kafka spool: read {}: {e}", file.display())))?;
        let records = body.lines().filter(|l| !l.trim().is_empty()).count() as i64;
        let offset = self.spool_record_offset();
        Ok(BatchRange {
            source: SPOOL_SOURCE.into(),
            start: [
                (SPOOL_FILE_KEY.to_string(), self.spool_cursor as i64),
                (SPOOL_OFFSET_KEY.to_string(), offset),
            ]
            .into(),
            end: [
                (SPOOL_FILE_KEY.to_string(), self.spool_cursor as i64 + 1),
                (SPOOL_OFFSET_KEY.to_string(), offset + records),
            ]
            .into(),
            items: vec![],
        })
    }

    /// Read the one file `range` names, numbering its records from the offset the range fixed.
    fn read_spool(&mut self, range: &BatchRange) -> Result<Vec<KafkaRecord>> {
        let dir = self
            .options
            .spool_dir
            .clone()
            .expect("spool_mode() checked spool_dir is set");
        std::fs::create_dir_all(&dir)
            .map_err(|e| Error::Io(format!("kafka spool: mkdir {}: {e}", dir.display())))?;
        let topic = self.options.topics.first().cloned().unwrap_or_default();

        let index = range.start.get(SPOOL_FILE_KEY).copied().unwrap_or(0);
        let file = dir.join(format!("batch-{index}.json"));
        if !file.exists() {
            return Ok(vec![]);
        }
        let body = std::fs::read_to_string(&file)
            .map_err(|e| Error::Io(format!("kafka spool: read {}: {e}", file.display())))?;

        let mut offset = range.start.get(SPOOL_OFFSET_KEY).copied().unwrap_or(0);
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
        self.spool_cursor = range
            .end
            .get(SPOOL_FILE_KEY)
            .copied()
            .unwrap_or(index + 1)
            .max(0) as u64;
        self.offsets.insert(
            TopicPartition {
                topic,
                partition: 0,
            },
            offset,
        );
        Ok(out)
    }

    /// The record offset the next spool file starts at.
    fn spool_record_offset(&self) -> i64 {
        let topic = self.options.topics.first().cloned().unwrap_or_default();
        self.offsets
            .get(&TopicPartition {
                topic,
                partition: 0,
            })
            .copied()
            .unwrap_or(0)
    }
}

/// `topic-partition`, split at the LAST dash since topic names may contain dashes.
fn parse_topic_partition(key: &str) -> Option<TopicPartition> {
    let (topic, partition) = key.rsplit_once('-')?;
    Some(TopicPartition {
        topic: topic.to_string(),
        partition: partition.parse().ok()?,
    })
}

#[async_trait::async_trait]
impl Source for KafkaSource {
    fn schema(&self) -> SchemaRef {
        kafka_schema()
    }

    fn description(&self) -> String {
        format!("KafkaV2[Subscribe[{}]]", self.options.topics.join(", "))
    }

    async fn plan_batch(&mut self, _engine: &Engine) -> Result<BatchRange> {
        if self.spool_mode() {
            return self.plan_spool();
        }
        self.assign().await?;
        let partitions: Vec<TopicPartition> = self.partition_clients.keys().cloned().collect();
        if partitions.is_empty() {
            return Ok(BatchRange::default());
        }

        let ends = self.latest_offsets(&partitions).await?;
        self.high_watermarks = ends.clone();

        let lag: BTreeMap<TopicPartition, u64> = partitions
            .iter()
            .map(|tp| {
                let from = self.offsets.get(tp).copied().unwrap_or(0);
                let end = ends.get(tp).copied().unwrap_or(from);
                (tp.clone(), (end - from).max(0) as u64)
            })
            .collect();
        let budgets = self.partition_budgets(&partitions, &lag);

        let mut range = BatchRange {
            source: "kafka".into(),
            ..Default::default()
        };
        for tp in &partitions {
            let from = self.offsets.get(tp).copied().unwrap_or(0);
            let available = ends.get(tp).copied().unwrap_or(from);
            // The batch stops at whichever comes first: the end of the log as of planning, or
            // the partition's slice of `maxOffsetsPerTrigger`.
            let budget = budgets.get(tp).copied().unwrap_or(0);
            let capped = from.saturating_add(budget.min(i64::MAX as u64) as i64);
            let end = available.min(capped);
            if end <= from {
                continue;
            }
            range.start.insert(tp.to_string(), from);
            range.end.insert(tp.to_string(), end);
        }
        // Rotate only once a batch is actually planned, so an idle stream does not spin the
        // tie-breaker and hand a different partition the odd record on every empty trigger.
        if !range.is_empty() {
            self.round_robin = self.round_robin.wrapping_add(1);
        }
        Ok(range)
    }

    async fn poll_range(
        &mut self,
        _engine: &Engine,
        range: &BatchRange,
    ) -> Result<Vec<RecordBatch>> {
        if range.is_empty() {
            return Ok(vec![]);
        }
        let records = if range.source == SPOOL_SOURCE {
            self.read_spool(range)?
        } else if range.source == "kafka" {
            let records = self.fetch_range(range).await?;
            // The position moves to the end the range fixed, not to the last record actually
            // returned: a compacted offset at the tail is covered by this batch and must not be
            // re-read by the next one.
            for (key, end) in &range.end {
                if let Some(tp) = parse_topic_partition(key) {
                    self.offsets.insert(tp, *end);
                }
            }
            records
        } else {
            return Err(Error::Plan(format!(
                "batch range was planned by `{}`, not by the kafka source",
                range.source
            )));
        };
        if records.is_empty() {
            return Ok(vec![]);
        }
        Ok(vec![to_record_batch(&records)?])
    }

    fn committed_offsets(&self) -> Option<SourceOffsets> {
        if self.spool_mode() {
            return Some(SourceOffsets {
                source: SPOOL_SOURCE.into(),
                entries: [
                    (SPOOL_FILE_KEY.to_string(), self.spool_cursor as i64),
                    (SPOOL_OFFSET_KEY.to_string(), self.spool_record_offset()),
                ]
                .into(),
            });
        }
        Some(SourceOffsets {
            source: "kafka".into(),
            entries: self
                .offsets
                .iter()
                .map(|(tp, o)| (tp.to_string(), *o))
                .collect(),
        })
    }

    async fn available_end(&mut self, _engine: &Engine) -> Result<Option<BTreeMap<String, i64>>> {
        if self.spool_mode() {
            // The spool's end is the first index with no file behind it. Cheap to scan, and it
            // is scanned once per drain rather than once per batch.
            let Some(dir) = &self.options.spool_dir else {
                return Ok(None);
            };
            let mut end = self.spool_cursor as i64;
            while dir.join(format!("batch-{end}.json")).exists() {
                end += 1;
            }
            return Ok(Some([(SPOOL_FILE_KEY.to_string(), end)].into()));
        }
        self.assign().await?;
        let partitions: Vec<TopicPartition> = self.partition_clients.keys().cloned().collect();
        if partitions.is_empty() {
            return Ok(None);
        }
        Ok(Some(
            self.latest_offsets(&partitions)
                .await?
                .into_iter()
                .map(|(tp, end)| (tp.to_string(), end))
                .collect(),
        ))
    }

    fn restore_offsets(&mut self, offsets: &SourceOffsets) {
        if offsets.source == SPOOL_SOURCE {
            // Unlike the broker path, the spool's position is a file index: restoring it is what
            // stops a restarted `once` run from replaying files it already committed.
            self.spool_cursor = offsets
                .entries
                .get(SPOOL_FILE_KEY)
                .copied()
                .unwrap_or(0)
                .max(0) as u64;
            let topic = self.options.topics.first().cloned().unwrap_or_default();
            self.offsets.insert(
                TopicPartition {
                    topic,
                    partition: 0,
                },
                offsets.entries.get(SPOOL_OFFSET_KEY).copied().unwrap_or(0),
            );
            return;
        }
        if offsets.source != "kafka" {
            return;
        }
        for (key, offset) in &offsets.entries {
            if let Some(tp) = parse_topic_partition(key) {
                self.offsets.insert(tp, *offset);
            }
        }
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
    async fn a_recorded_spool_range_replays_the_same_records_and_the_same_offsets() {
        // The spool position is a file index *and* a record offset. A range carrying only the
        // first would renumber the records on replay, and `offset` is a column the query sees.
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("batch-0.json"), "a\nb\n").unwrap();
        std::fs::write(dir.path().join("batch-1.json"), "c\n").unwrap();
        let engine = Engine::new();
        let mut src = KafkaSource::from_options(&opts(&[
            ("oxidant.spool.dir", dir.path().to_str().unwrap()),
            ("subscribe", "orders"),
        ]))
        .unwrap();

        let range = src.plan_batch(&engine).await.unwrap();
        assert_eq!(range.source, SPOOL_SOURCE);
        assert_eq!(range.start.get(SPOOL_FILE_KEY), Some(&0));
        assert_eq!(range.end.get(SPOOL_FILE_KEY), Some(&1));
        assert_eq!(range.end.get(SPOOL_OFFSET_KEY), Some(&2));

        let first = src.poll_range(&engine, &range).await.unwrap();
        assert_eq!(first[0].num_rows(), 2);

        // Replaying the recorded range reproduces it exactly, even though the source has since
        // moved on to `batch-1`.
        let replay = src.poll_range(&engine, &range).await.unwrap();
        assert_eq!(replay[0].num_rows(), 2);
        assert_eq!(
            offsets_column(&replay[0]),
            offsets_column(&first[0]),
            "a replayed batch renumbers nothing"
        );

        // And the next batch still starts where the recorded one ended.
        let next = src.plan_batch(&engine).await.unwrap();
        assert_eq!(next.start.get(SPOOL_FILE_KEY), Some(&1));
        assert_eq!(next.start.get(SPOOL_OFFSET_KEY), Some(&2));
    }

    fn offsets_column(batch: &RecordBatch) -> Vec<i64> {
        use oxidant_loom::arrow::array::AsArray;
        use oxidant_loom::arrow::datatypes::Int64Type;
        batch
            .column_by_name("offset")
            .unwrap()
            .as_primitive::<Int64Type>()
            .values()
            .to_vec()
    }

    #[tokio::test]
    async fn a_planned_spool_batch_consumes_nothing_until_it_is_read() {
        // Planning must not advance the cursor: a plan that could not be recorded has to be
        // re-plannable, and a plan that consumed the file would lose it.
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("batch-0.json"), "a\n").unwrap();
        let engine = Engine::new();
        let mut src = KafkaSource::from_options(&opts(&[
            ("oxidant.spool.dir", dir.path().to_str().unwrap()),
            ("subscribe", "orders"),
        ]))
        .unwrap();

        let first = src.plan_batch(&engine).await.unwrap();
        let second = src.plan_batch(&engine).await.unwrap();
        assert_eq!(first, second, "planning twice describes the same batch");
        assert_eq!(src.spool_cursor, 0);
    }

    #[test]
    fn the_budget_split_is_fixed_by_the_planned_lag_not_by_the_previous_batch() {
        // Two attempts at one batch must divide `maxOffsetsPerTrigger` identically. The lag now
        // comes from the end offsets the batch was planned against, so it is the same number on
        // a fresh process as on the one that planned it — the stale-watermark split was a second
        // reason a replay could diverge even from identical start offsets.
        let src = KafkaSource::from_options(&opts(&[
            ("kafka.bootstrap.servers", "localhost:9092"),
            ("subscribe", "orders"),
            ("maxOffsetsPerTrigger", "100"),
        ]))
        .unwrap();
        let partitions: Vec<TopicPartition> = (0..2)
            .map(|partition| TopicPartition {
                topic: "orders".into(),
                partition,
            })
            .collect();
        let lag: BTreeMap<TopicPartition, u64> = [
            (partitions[0].clone(), 90u64),
            (partitions[1].clone(), 10u64),
        ]
        .into();

        let budgets = src.partition_budgets(&partitions, &lag);
        assert_eq!(budgets.values().sum::<u64>(), 100);
        assert!(
            budgets[&partitions[0]] > budgets[&partitions[1]],
            "the lagging partition gets the larger share: {budgets:?}"
        );
        assert_eq!(
            src.partition_budgets(&partitions, &lag),
            budgets,
            "the same lag yields the same split"
        );
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

        let plan0 = src.plan_batch(&engine).await.unwrap();
        let b0 = src.poll_range(&engine, &plan0).await.unwrap();
        assert_eq!(b0.len(), 1);
        assert_eq!(b0[0].num_rows(), 2);
        assert_eq!(b0[0].schema(), kafka_schema());

        let plan1 = src.plan_batch(&engine).await.unwrap();
        let b1 = src.poll_range(&engine, &plan1).await.unwrap();
        assert_eq!(b1[0].num_rows(), 1);

        // Record offsets continue across batches rather than restarting per file, and the file
        // index advances alongside them.
        let offsets = src.committed_offsets().unwrap();
        assert_eq!(offsets.source, SPOOL_SOURCE);
        assert_eq!(offsets.entries.get(SPOOL_OFFSET_KEY), Some(&3));
        assert_eq!(offsets.entries.get(SPOOL_FILE_KEY), Some(&2));

        // Drained: an empty range, not an error.
        assert!(src.plan_batch(&engine).await.unwrap().is_empty());
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
            metadata_max_age: Duration::from_secs(30),
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

    /// A source with `partitions` assigned, at the given offsets and high watermarks.
    fn source_with_lag(budget: Option<u64>, lags: &[(i32, i64)]) -> KafkaSource {
        let mut src = KafkaSource::new(KafkaOptions {
            brokers: vec!["b:9092".into()],
            topics: vec!["t".into()],
            starting_offsets: StartingOffsets::Latest,
            max_offsets_per_trigger: budget,
            fetch_max_wait_ms: 1,
            max_partition_fetch_bytes: 1024,
            metadata_max_age: Duration::from_secs(30),
            spool_dir: None,
        });
        for (partition, lag) in lags {
            let tp = TopicPartition {
                topic: "t".into(),
                partition: *partition,
            };
            src.offsets.insert(tp.clone(), 0);
            src.high_watermarks.insert(tp, *lag);
        }
        src
    }

    /// The lag map `plan_batch` builds, derived from the source's offsets and watermarks so the
    /// budget tests keep exercising the real inputs.
    fn lag_of(src: &KafkaSource) -> BTreeMap<TopicPartition, u64> {
        partitions_of(src)
            .into_iter()
            .map(|tp| {
                let from = src.offsets.get(&tp).copied().unwrap_or(0);
                let end = src.high_watermarks.get(&tp).copied().unwrap_or(from);
                let lag = (end - from).max(0) as u64;
                (tp, if lag == 0 { 1 } else { lag })
            })
            .collect()
    }

    fn partitions_of(src: &KafkaSource) -> Vec<TopicPartition> {
        let mut p: Vec<TopicPartition> = src.offsets.keys().cloned().collect();
        p.sort();
        p
    }

    #[test]
    fn a_rate_limited_batch_never_starves_a_partition() {
        // The regression this guards is severe and silent: spending `maxOffsetsPerTrigger` on
        // partitions in order lets a busy partition 0 take the entire allowance every batch, so
        // partitions 1..N are never read again and their lag grows without bound or complaint.
        let src = source_with_lag(Some(1_000), &[(0, 10_000_000), (1, 5), (2, 5), (3, 5)]);
        let budgets = src.partition_budgets(&partitions_of(&src), &lag_of(&src));

        assert_eq!(
            budgets.values().sum::<u64>(),
            1_000,
            "the whole budget is used"
        );
        for (tp, share) in &budgets {
            assert!(*share > 0, "`{tp}` was starved with {share}");
        }
        // The backlogged partition still gets the lion's share — this is a fair split, not an
        // equal one.
        let hot = budgets[&TopicPartition {
            topic: "t".into(),
            partition: 0,
        }];
        assert!(
            hot > 900,
            "the lagging partition should dominate, got {hot}"
        );
    }

    #[test]
    fn with_no_lag_information_the_budget_splits_evenly() {
        // The first batch of a query has no high watermarks yet.
        let mut src = source_with_lag(Some(400), &[]);
        for partition in 0..4 {
            src.offsets.insert(
                TopicPartition {
                    topic: "t".into(),
                    partition,
                },
                0,
            );
        }
        let budgets = src.partition_budgets(&partitions_of(&src), &lag_of(&src));
        assert_eq!(budgets.values().sum::<u64>(), 400);
        assert!(budgets.values().all(|s| *s == 100), "{budgets:?}");
    }

    /// A caught-up topic is the steady state, and it is the case with *no* lag to apportion by —
    /// so the whole budget falls through to the even-split path. Computing that split must not
    /// cost one operation per permitted record: with a large `maxOffsetsPerTrigger` this test
    /// would take minutes handing out records one at a time, on every single trigger, precisely
    /// when the pipeline has nothing to do.
    #[test]
    fn a_caught_up_topic_splits_a_huge_budget_without_counting_it_out() {
        let mut src = source_with_lag(Some(200_000_000), &[]);
        for partition in 0..4 {
            let tp = TopicPartition {
                topic: "t".into(),
                partition,
            };
            // Consumed right up to the high watermark: zero lag everywhere.
            src.offsets.insert(tp.clone(), 500);
            src.high_watermarks.insert(tp, 500);
        }
        let started = std::time::Instant::now();
        let budgets = src.partition_budgets(&partitions_of(&src), &lag_of(&src));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "budget split took {:?} — it is counting out records one at a time",
            started.elapsed()
        );
        assert_eq!(budgets.values().sum::<u64>(), 200_000_000);
        assert!(
            budgets.values().all(|s| *s == 50_000_000),
            "a caught-up topic must split evenly: {budgets:?}"
        );
    }

    /// The dust left by integer division still has to be handed out, and to a different partition
    /// each batch — otherwise partition 0 is permanently one record richer than partition 3.
    #[test]
    fn the_remainder_rotates_across_batches() {
        let mut src = source_with_lag(Some(10), &[]);
        for partition in 0..4 {
            let tp = TopicPartition {
                topic: "t".into(),
                partition,
            };
            src.offsets.insert(tp.clone(), 0);
            src.high_watermarks.insert(tp, 0);
        }
        let partitions = partitions_of(&src);

        let mut totals: BTreeMap<TopicPartition, u64> = BTreeMap::new();
        for _ in 0..4 {
            let budgets = src.partition_budgets(&partitions, &lag_of(&src));
            assert_eq!(
                budgets.values().sum::<u64>(),
                10,
                "the budget is never exceeded"
            );
            for (tp, share) in budgets {
                *totals.entry(tp).or_default() += share;
            }
            src.round_robin = src.round_robin.wrapping_add(1);
        }
        // 10 across 4 partitions is 2 each with 2 left over; over four batches every partition
        // must have collected the same total.
        assert!(
            totals.values().all(|t| *t == 10),
            "the remainder favoured some partitions: {totals:?}"
        );
    }

    #[test]
    fn a_budget_smaller_than_the_partition_count_rotates_instead_of_starving() {
        let mut src = source_with_lag(Some(2), &[(0, 100), (1, 100), (2, 100), (3, 100)]);
        let partitions = partitions_of(&src);

        // Over four batches every partition must be served, even though only two fit per batch.
        let mut served: BTreeMap<TopicPartition, u64> = BTreeMap::new();
        for _ in 0..4 {
            for (tp, share) in src.partition_budgets(&partitions, &lag_of(&src)) {
                *served.entry(tp).or_default() += share;
            }
            src.round_robin += 1;
        }
        assert_eq!(served.len(), 4);
        for (tp, total) in &served {
            assert!(*total >= 1, "`{tp}` never got a turn");
        }
    }

    #[test]
    fn an_unlimited_budget_puts_no_ceiling_on_any_partition() {
        let src = source_with_lag(None, &[(0, 10), (1, 10)]);
        let budgets = src.partition_budgets(&partitions_of(&src), &lag_of(&src));
        assert!(budgets.values().all(|s| *s == u64::MAX));
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
            metadata_max_age: Duration::from_secs(30),
            spool_dir: None,
        });
        src.restore_offsets(&SourceOffsets {
            source: "file".into(),
            entries: [("t-0".to_string(), 5i64)].into_iter().collect(),
        });
        assert!(src.offsets.is_empty());
    }
}
