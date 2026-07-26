//! Spark Structured Streaming micro-batch engine for Weft.
//!
//! Implements a subset of Spark's Structured Streaming: file-based sources/sinks, processing-time
//! triggers, checkpoint metadata, and query lifecycle management.

mod checkpoint;
mod config;
mod query;
mod scheduler;
mod sink;
mod source;
mod state;
mod watermark;

pub use checkpoint::CheckpointStore;
pub use config::StreamQueryConfig;
pub use query::{QueryProgress, QueryStatus, StreamingQuery, StreamingQueryId};
pub use scheduler::{StreamingQueryManager, Trigger};
pub use sink::{FileSink, MemorySink, Sink};
pub use source::{FileSource, KafkaSource, MemoryRateSource, Source};
pub use state::DedupState;
pub use watermark::WatermarkConfig;
