//! Kubernetes worker discovery via DNS (headless Service A-record lookup).
//!
//! Behind the `k8s` feature flag. Resolves `WEFT_WORKER_SERVICE` (e.g.
//! `weft-worker.weft-cl-abc.svc.cluster.local`) to live pod IPs at query scheduling time.

use super::{ClusterMembership, WorkerEndpoint};

/// Resolve worker endpoints from a Kubernetes headless Service DNS name.
#[derive(Debug, Clone)]
pub struct K8sMembership {
    /// DNS hostname of the worker headless Service (no scheme).
    service_host: String,
    /// Flight port workers listen on.
    port: u16,
}

impl K8sMembership {
    /// Build from environment: `WEFT_WORKER_SERVICE` and optional `WEFT_WORKER_PORT` (default 50561).
    pub fn from_env() -> Option<Self> {
        let host = std::env::var("WEFT_WORKER_SERVICE").ok()?;
        if host.is_empty() {
            return None;
        }
        let port = std::env::var("WEFT_WORKER_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(50561);
        Some(Self {
            service_host: host,
            port,
        })
    }

    /// Explicit host/port constructor (tests and gateway provisioning).
    pub fn new(service_host: impl Into<String>, port: u16) -> Self {
        Self {
            service_host: service_host.into(),
            port,
        }
    }

    fn resolve_sync(&self) -> Vec<WorkerEndpoint> {
        let addr = format!("{}:{}", self.service_host, self.port);
        match addr.to_socket_addrs() {
            Ok(addrs) => addrs
                .map(|a| format!("http://{}:{}", a.ip(), a.port()))
                .collect(),
            Err(e) => {
                eprintln!("[K8sMembership] DNS resolve {addr} failed: {e}");
                Vec::new()
            }
        }
    }
}

use std::net::ToSocketAddrs;

impl ClusterMembership for K8sMembership {
    fn endpoints(&self) -> Vec<WorkerEndpoint> {
        self.resolve_sync()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn localhost_dns_resolves() {
        let m = K8sMembership::new("localhost", 50561);
        let eps = m.endpoints();
        assert!(!eps.is_empty());
        assert!(eps[0].starts_with("http://"));
    }
}
