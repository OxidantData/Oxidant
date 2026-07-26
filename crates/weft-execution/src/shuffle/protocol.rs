//! The control envelopes carried in Flight `do_get` tickets for distributed execution.
//!
//! These are prost messages (not raw SQL) so a ticket can describe a whole stage: which
//! plan to run, how its output is partitioned, and where the upstream partitions live. The
//! serialized DataFusion physical-plan fragment (when present) rides as opaque bytes in
//! [`StageTicket::plan_fragment`]; only that field depends on `datafusion-proto`, so a proto
//! version skew there never touches this envelope.

use prost::Message;

/// Number of bytes in the fixed-width [`ShuffleExchangeHeader`] encoding.
pub const SHUFFLE_EXCHANGE_HEADER_LEN: usize = 8;

/// The first `do_exchange` frame carries this header in `FlightData.app_metadata`, followed by
/// normal Arrow IPC `FlightData` schema/batch frames for that stage partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShuffleExchangeHeader {
    /// The stage whose output partition is being pushed to this worker.
    pub stage_id: u32,
    /// The target shuffle partition on this worker.
    pub partition_id: u32,
}

impl ShuffleExchangeHeader {
    /// Encode as `stage_id` little-endian followed by `partition_id` little-endian.
    pub fn encode(self) -> [u8; SHUFFLE_EXCHANGE_HEADER_LEN] {
        let mut out = [0u8; SHUFFLE_EXCHANGE_HEADER_LEN];
        out[..4].copy_from_slice(&self.stage_id.to_le_bytes());
        out[4..].copy_from_slice(&self.partition_id.to_le_bytes());
        out
    }

    /// Decode a fixed-width exchange header.
    pub fn decode(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.len() != SHUFFLE_EXCHANGE_HEADER_LEN {
            return Err("bad shuffle exchange header length");
        }
        let stage_id = u32::from_le_bytes(bytes[..4].try_into().expect("slice length checked"));
        let partition_id = u32::from_le_bytes(bytes[4..].try_into().expect("slice length checked"));
        Ok(Self {
            stage_id,
            partition_id,
        })
    }
}

/// A unit of distributed work: run a stage on one worker and stream its result back.
///
/// For the MVP the stage is expressed as SQL ([`stage_sql`]); once a stage has upstreams it
/// first pulls each upstream's bucket for [`partition_id`] (see [`ShuffleReadTicket`]) and
/// registers them as the `shuffle_input` table before running.
#[derive(Clone, PartialEq, Message)]
pub struct StageTicket {
    /// Identifies this stage's output across the cluster (upstreams cache under this id).
    #[prost(uint32, tag = "1")]
    pub stage_id: u32,
    /// Which output partition (== which downstream worker) this invocation produces.
    #[prost(uint32, tag = "2")]
    pub partition_id: u32,
    /// Number of output partitions == number of workers (the shuffle fan-out).
    #[prost(uint32, tag = "3")]
    pub num_partitions: u32,
    /// Flight endpoints to pull this stage's input partition from (empty for a leaf stage).
    #[prost(string, repeated, tag = "4")]
    pub upstream_endpoints: Vec<String>,
    /// SQL to run for this stage (the MVP execution path; the `shuffle_input` table is in
    /// scope when `upstream_endpoints` is non-empty).
    #[prost(string, tag = "5")]
    pub stage_sql: String,
    /// Serialized `datafusion-proto` `PhysicalPlanNode`; preferred over `stage_sql` when set.
    #[prost(bytes = "vec", tag = "6")]
    pub plan_fragment: Vec<u8>,
    /// Output column indices to hash-partition this stage's result on (the shuffle key).
    #[prost(uint32, repeated, tag = "7")]
    pub hash_key_cols: Vec<u32>,
    /// Upstream stage ids this stage consumes (empty == a leaf producer). One entry per shuffle
    /// input: the consumer pulls bucket `partition_id` of each upstream stage from every worker in
    /// `upstream_endpoints` and registers it as `shuffle_input` (single upstream) or
    /// `shuffle_input_{i}` (the i-th of several — e.g. the two sides of a shuffle join). Replaces
    /// the former implicit `stage_id - 1` chaining so an arbitrary stage DAG can be expressed.
    #[prost(uint32, repeated, tag = "8")]
    pub upstream_stage_ids: Vec<u32>,
    /// Whether this stage *produces* for downstreams: hash-partition its output by `hash_key_cols`
    /// and cache the buckets (returning empty), vs. an *output* stage that returns its result. A
    /// stage may both consume upstreams and produce (an intermediate stage of a multi-shuffle DAG,
    /// e.g. a join whose result is re-shuffled before a final aggregate).
    #[prost(bool, tag = "9")]
    pub produce: bool,
}

/// A pull request for one hash bucket of an already-produced stage output.
#[derive(Clone, PartialEq, Message)]
pub struct ShuffleReadTicket {
    /// The upstream stage whose cached output to read.
    #[prost(uint32, tag = "1")]
    pub stage_id: u32,
    /// The bucket (downstream partition) the caller wants.
    #[prost(uint32, tag = "2")]
    pub target_partition: u32,
}

/// Tag byte prefixed to a ticket so the worker can route without ambiguity against the legacy
/// raw-UTF-8-SQL ticket path. (Plain SQL tickets carry no such prefix and fall through.)
pub mod tag {
    /// A [`StageTicket`] follows.
    pub const STAGE: u8 = 0x01;
    /// A [`ShuffleReadTicket`] follows.
    pub const SHUFFLE_READ: u8 = 0x02;
}

impl StageTicket {
    /// Encode as ticket bytes: a [`tag::STAGE`] prefix then the prost message.
    pub fn to_ticket_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + self.encoded_len());
        buf.push(tag::STAGE);
        self.encode(&mut buf)
            .expect("prost encode into Vec is infallible");
        buf
    }
}

impl ShuffleReadTicket {
    /// Encode as ticket bytes: a [`tag::SHUFFLE_READ`] prefix then the prost message.
    pub fn to_ticket_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + self.encoded_len());
        buf.push(tag::SHUFFLE_READ);
        self.encode(&mut buf)
            .expect("prost encode into Vec is infallible");
        buf
    }
}

/// What a worker decoded a ticket as.
pub enum Ticket {
    /// Run a stage.
    Stage(StageTicket),
    /// Serve a cached shuffle bucket.
    ShuffleRead(ShuffleReadTicket),
    /// Legacy path: a raw SQL string (no tag prefix).
    Sql(String),
}

/// Decode raw ticket bytes. A leading [`tag::STAGE`]/[`tag::SHUFFLE_READ`] selects a prost
/// message; anything else is treated as a legacy UTF-8 SQL ticket (keeps the single-stage
/// roundtrip working).
pub fn decode_ticket(bytes: &[u8]) -> Result<Ticket, prost::DecodeError> {
    match bytes.first().copied() {
        Some(tag::STAGE) => Ok(Ticket::Stage(StageTicket::decode(&bytes[1..])?)),
        Some(tag::SHUFFLE_READ) => Ok(Ticket::ShuffleRead(ShuffleReadTicket::decode(&bytes[1..])?)),
        _ => Ok(Ticket::Sql(String::from_utf8_lossy(bytes).into_owned())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage_ticket_round_trips() {
        let t = StageTicket {
            stage_id: 1,
            partition_id: 0,
            num_partitions: 2,
            upstream_endpoints: vec!["http://a:1".into(), "http://b:2".into()],
            stage_sql: "SELECT k, SUM(c) FROM shuffle_input GROUP BY k".into(),
            plan_fragment: vec![],
            hash_key_cols: vec![0],
            upstream_stage_ids: vec![0],
            produce: false,
        };
        let bytes = t.to_ticket_bytes();
        match decode_ticket(&bytes).unwrap() {
            Ticket::Stage(got) => assert_eq!(got, t),
            _ => panic!("expected Stage"),
        }
    }

    #[test]
    fn shuffle_read_ticket_round_trips() {
        let t = ShuffleReadTicket {
            stage_id: 0,
            target_partition: 3,
        };
        let bytes = t.to_ticket_bytes();
        match decode_ticket(&bytes).unwrap() {
            Ticket::ShuffleRead(got) => assert_eq!(got, t),
            _ => panic!("expected ShuffleRead"),
        }
    }

    #[test]
    fn legacy_sql_ticket_still_decodes() {
        let bytes = b"SELECT 21 + 21 AS answer".to_vec();
        match decode_ticket(&bytes).unwrap() {
            Ticket::Sql(sql) => assert_eq!(sql, "SELECT 21 + 21 AS answer"),
            _ => panic!("expected Sql"),
        }
    }

    #[test]
    fn shuffle_exchange_header_round_trips() {
        let h = ShuffleExchangeHeader {
            stage_id: 42,
            partition_id: 7,
        };
        assert_eq!(ShuffleExchangeHeader::decode(&h.encode()).unwrap(), h);
        assert!(ShuffleExchangeHeader::decode(&h.encode()[..7]).is_err());
    }
}
