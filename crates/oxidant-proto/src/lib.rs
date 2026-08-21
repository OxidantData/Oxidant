//! `oxidant-proto` — generated Spark Connect protobuf + gRPC types.
//!
//! The `spark/connect/*.proto` files are vendored under `proto/` from a pinned
//! `apache/spark` checkout (target **Spark 4.x**) and compiled at build time by
//! `build.rs` using `protox` (pure-Rust) + `tonic-build`. The generated items live under
//! [`spark::connect`] — e.g. `spark::connect::Plan`, `spark::connect::ExecutePlanRequest`,
//! and `spark::connect::spark_connect_service_server::SparkConnectService`.

#[allow(clippy::all, missing_docs, rustdoc::all)]
pub mod spark {
    #[allow(clippy::all, missing_docs, rustdoc::all)]
    pub mod connect {
        tonic::include_proto!("spark.connect");
    }
}

/// The Spark protocol version these vendored protos track. The wire protocol is
/// version-coupled; we test against an exact `pyspark-client`.
pub const TARGET_SPARK_VERSION: &str = "4.x";
