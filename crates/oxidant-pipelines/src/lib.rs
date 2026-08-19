//! Declarative pipeline runner: build a table DAG from config and execute it headlessly.
//!
//! The CLI (`oxidant pipeline`) and the Connect server share this crate. Config types live in
//! [`oxidant_config`]; this crate owns graph resolution, expectation composition, and the run loop.

pub mod expectations;
pub mod graph;
mod runner;

pub use graph::{Graph, Node};
pub use runner::{run_pipeline, Plan, RunEvent, TableOutcome, TableStatus};

/// The alias a streaming table's `sql:` uses to read its own source.
///
/// Fixed rather than configurable: a streaming table has exactly one source, so a name to choose
/// would be a name to get wrong.
pub const STREAM_ALIAS: &str = "stream";
