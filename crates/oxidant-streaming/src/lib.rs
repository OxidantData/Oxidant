//! Spark Structured Streaming micro-batch engine for Oxidant.
//!
//! Implements the shape of Spark's Structured Streaming that a live-table pipeline needs:
//! a Kafka source with checkpointed per-partition offsets, the user's DataFrame transformation
//! re-executed per micro-batch, and a datalake sink that commits each batch into a catalog table
//! (Delta on Glue) so a dashboard can read fresh data between batches.
//!
//! - [`kafka`] — the Kafka source and its Spark-parity schema.
//! - [`postgres_cdc`] — the Postgres change-data-capture source, over [`pg_replication`].
//! - [`input`] — the swappable table the streaming DataFrame is planned against.
//! - [`lake_sink`] — Delta/Parquet writes plus database + table creation in the catalog.
//! - [`scheduler`] — triggers, the micro-batch loop, and checkpoint commits.

mod checkpoint;
mod config;
mod connector_log;
pub mod input;
pub mod kafka;
pub mod lake_sink;
pub mod pg_replication;
pub mod postgres_cdc;
mod query;
mod scheduler;
mod sink;
mod source;
mod state;
mod watermark;

pub use checkpoint::{CheckpointState, CheckpointStore};
pub use config::{
    ExpectationAction, SinkDestination, StreamExpectation, StreamQueryConfig,
    DEFAULT_ICEBERG_SUFFIX,
};
pub use connector_log::ConnectorLog;
pub use input::{
    capture as capture_stream_inputs, stream_input, stream_input_name, MicroBatchInput,
};
pub use kafka::{kafka_schema, KafkaOptions, KafkaSource, StartingOffsets};
pub use lake_sink::{writable_format, LakeSink, LakeSinkOptions, LakeTarget};
pub use postgres_cdc::{
    postgres_cdc_pipeline_options, PostgresCdcOptions, PostgresCdcSource, KNOWN_OPTIONS,
    LSN_COLUMN, OP_COLUMN, TS_COLUMN,
};
pub use query::{QueryProgress, QueryStatus, SourceProgress, StreamingQuery, StreamingQueryId};
pub use scheduler::{
    build_source, source_schema, MicroBatchPipeline, StartOptions, StreamingQueryManager, Trigger,
};
pub use sink::{FileSink, MemorySink, Sink};
pub use source::{BatchRange, FileSource, MemoryRateSource, Source, SourceOffsets};
pub use state::DedupState;
// Re-exported so a caller configuring a sink does not need its own dependency on the
// datasource crate just to name the default.
pub use oxidant_datasource::delta_write::DEFAULT_CHECKPOINT_INTERVAL;
pub use watermark::WatermarkConfig;
