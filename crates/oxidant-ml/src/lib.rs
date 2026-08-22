//! SPIKE (issue #118): ONNX batch inference behind a SQL `ml_predict` UDF.
//!
//! Throwaway spike code — the verdict, benchmark numbers, and the tract gaps this crate works
//! around are written up in `docs/spikes/ml-predict.md`.

pub mod cache;
pub mod compat;
pub mod model;
pub mod probe;
pub mod store;

pub use model::{OnnxModel, Predictions};
pub use store::{install_blob_source, BlobSource, BlobVersion};
