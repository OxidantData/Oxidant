//! Runtime observability for Oxidant: events, Spark-compatible REST models, and in-memory store.

mod events;
mod model;
mod status;
mod store;
mod tracker;

pub use events::*;
pub use model::*;
pub use status::{query_state, QueryStatus, StatusSnapshot};
pub use store::{AppStateStore, OperationState, SharedStore};
pub use tracker::{emit_worker_task, set_worker_store, worker_store, QueryTracker};
