//! Declarative pipeline runner: build a table DAG from config and execute it headlessly.
//!
//! The CLI (`oxidant pipeline`) and the Connect server share this crate. Config types live in
//! [`oxidant_config`]; this crate owns graph resolution, expectation composition, and the run loop.

pub mod auto_cdc;
mod cdc_sink;
pub mod cron;
pub mod expectations;
pub mod graph;
mod output_write;
pub mod reconcile;
mod runner;
pub mod sql_graph;

pub use auto_cdc::{build_merge_sql, output_columns, validate_auto_cdc, CdcMerge};
pub use cdc_sink::CdcMergeSink;
pub use cron::Cron;
pub use graph::{table_references, Graph, Node};
pub use output_write::{
    flow_queries, parse_output_schema, split_table_properties, union_flow_sql,
    validate_external_sink_format, validate_output_format, FlowQuery,
};
pub use reconcile::{
    diff_keys, reconcile, set_schedule, KeyDiff, KeyRow, KeyWindow, ReconcileOptions,
    ReconcileReport, ReconcileSchedule, TableReport, DEFAULT_SAMPLE, EXIT_DRIFT, EXIT_FAILED,
    EXIT_IN_SYNC, MAX_SAMPLE,
};
pub use runner::{
    clear_pipeline_state, run_pipeline, Plan, RunEvent, RunEventKind, TableOutcome, TableStatus,
};
pub use sql_graph::{
    parse, parse_with_context, split_statements, OutputKind, ParsedAutoCdcFlow, ParsedFlow,
    ParsedOutput, SqlGraphElements,
};

/// The alias a streaming table's `sql:` uses to read its own source.
///
/// Fixed rather than configurable: a streaming table has exactly one source, so a name to choose
/// would be a name to get wrong.
pub const STREAM_ALIAS: &str = "stream";
