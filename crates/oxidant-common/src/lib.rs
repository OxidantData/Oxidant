//! `oxidant-common` — types shared across the Oxidant workspace: the error model, config,
//! session identity, and (eventually) the Arrow⇄Spark type mapping.
//!
//! Kept dependency-free today so the workspace builds offline; `arrow`/`thiserror`
//! land here when the runtime crates do.

use std::fmt;

/// Crate-wide result type.
pub type Result<T> = std::result::Result<T, Error>;

/// The Oxidant error model.
///
/// Mirrors the Spark Connect error contract: each variant maps to a stable Spark
/// `errorClass` + SQLSTATE so [`oxidant-connect`](../oxidant_connect/index.html) can populate
/// `google.rpc.ErrorInfo` metadata and an unmodified PySpark client raises the matching
/// exception type (`AnalysisException`, `ParseException`, …).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Parse / analysis failures → Spark `AnalysisException` / `ParseException`.
    Plan(String),
    /// A backend failed while executing a physical plan.
    Execution(String),
    /// A Spark relation/expression/config we do not implement yet.
    Unsupported(String),
    /// I/O, catalog, or storage failure.
    Io(String),
}

impl Error {
    /// Stable Spark `errorClass` string for this error (placeholder mapping; the real
    /// table is filled in alongside the Spark Connect error work).
    pub fn spark_error_class(&self) -> &'static str {
        match self {
            Error::Plan(_) => "ANALYSIS",
            Error::Execution(_) => "EXECUTION",
            Error::Unsupported(_) => "UNSUPPORTED_FEATURE",
            Error::Io(_) => "IO",
        }
    }

    /// SQLSTATE returned to the client.
    pub fn sql_state(&self) -> &'static str {
        match self {
            Error::Plan(_) => "42000",
            Error::Unsupported(_) => "0A000",
            Error::Execution(_) | Error::Io(_) => "58030",
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Plan(m) => write!(f, "plan error: {m}"),
            Error::Execution(m) => write!(f, "execution error: {m}"),
            Error::Unsupported(m) => write!(f, "unsupported: {m}"),
            Error::Io(m) => write!(f, "io error: {m}"),
        }
    }
}

impl std::error::Error for Error {}

/// A Spark Connect session identity. Sessions are keyed by the client-supplied
/// `session_id` (a UUID); the server mints a `server_side_session_id` that the client
/// echoes back so a server restart is detectable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionId {
    /// Client-supplied UUID.
    pub client: String,
    /// Server-minted UUID for this server lifetime.
    pub server: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_classes_are_stable() {
        assert_eq!(
            Error::Unsupported("RDD".into()).spark_error_class(),
            "UNSUPPORTED_FEATURE"
        );
        assert_eq!(Error::Plan("bad".into()).sql_state(), "42000");
    }
}
