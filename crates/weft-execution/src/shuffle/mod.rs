//! Distributed shuffle: the control envelope carried in Flight tickets, hash partitioning of
//! stage output into per-downstream buckets, disk spill for large stage caches, and (gated)
//! serialized physical-plan fragments.
//!
//! The MVP shape is `partial-agg per worker → hash shuffle by key → re-aggregate per worker`,
//! which is the smallest real shuffle that proves the mechanism while only needing
//! SQL-expressible re-combinable aggregates (COUNT→SUM, SUM→SUM, MIN→MIN, MAX→MAX).

pub mod codec;
pub mod partition;
pub mod protocol;
pub mod spill;

pub use partition::hash_partition;
pub use protocol::{decode_ticket, ShuffleReadTicket, StageTicket, Ticket};
pub use spill::SpillStore;

/// The table name a stage's shuffle input is registered under before its SQL runs.
pub const SHUFFLE_INPUT_TABLE: &str = "shuffle_input";
