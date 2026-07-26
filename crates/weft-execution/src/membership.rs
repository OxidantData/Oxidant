//! Cluster membership: where the driver finds its workers.
//!
//! Today the driver takes a **static** `Vec<String>` of worker endpoints ([`Role::Driver`]).
//! On EKS, worker pods come and go (autoscaling, restarts), so the static list must become a
//! live view. This module introduces the [`ClusterMembership`] seam the distributed driver reads
//! at stage-scheduling time:
//!
//! - [`StaticMembership`] — the current behavior, kept for tests and local runs.
//! - [`DnsMembership`] — resolves a Kubernetes headless Service (`WEFT_WORKER_SERVICE`) via DNS
//!   A records to the pods that are currently Ready.
//!
//! Crucially, membership also defines a **stable partition→worker assignment** (consistent
//! hashing) so a worker restart doesn't reshuffle ownership mid-query.
//!
//! [`Role::Driver`]: crate::Role

use std::sync::RwLock;

use weft_common::{Error, Result};

/// A worker endpoint (`host:port` or `http://host:port`) the driver can dial over Arrow Flight.
pub type WorkerEndpoint = String;

/// A live view of the worker set backing a distributed cluster.
pub trait ClusterMembership: Send + Sync {
    /// Snapshot the current worker endpoints. Called at the start of stage scheduling so the
    /// partition count tracks live workers.
    fn endpoints(&self) -> Vec<WorkerEndpoint>;

    /// The endpoint that owns `partition` out of `num_partitions`, using a **stable** assignment
    /// so a membership change doesn't reshuffle existing ownership. Default: rendezvous
    /// (highest-random-weight) hashing over the current endpoints.
    fn owner_of(&self, partition: u32, num_partitions: u32) -> Option<WorkerEndpoint> {
        let eps = self.endpoints();
        if eps.is_empty() || num_partitions == 0 {
            return None;
        }
        // Rendezvous hashing: assign the partition to the endpoint with the highest combined hash.
        eps.into_iter()
            .max_by_key(|ep| rendezvous_weight(ep, partition))
    }
}

/// FNV-1a over `(endpoint, partition)` — cheap, dependency-free weight for rendezvous hashing.
fn rendezvous_weight(endpoint: &str, partition: u32) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h = OFFSET;
    for b in endpoint.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(PRIME);
    }
    for b in partition.to_le_bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(PRIME);
    }
    h
}

/// A fixed worker list — the pre-EKS behavior, kept for tests and single-host runs.
pub struct StaticMembership {
    endpoints: Vec<WorkerEndpoint>,
}

impl StaticMembership {
    /// Wrap a fixed list of worker endpoints.
    pub fn new(endpoints: Vec<WorkerEndpoint>) -> Self {
        Self { endpoints }
    }
}

impl ClusterMembership for StaticMembership {
    fn endpoints(&self) -> Vec<WorkerEndpoint> {
        self.endpoints.clone()
    }
}

/// Kubernetes headless-Service membership resolved via DNS A records.
///
/// Set `WEFT_WORKER_SERVICE` to the DNS name of the workers Service (no port), e.g.
/// `weft-c1-workers.weft-cl-c1.svc.cluster.local`. Optional `WEFT_WORKER_PORT` (default 50561).
pub struct DnsMembership {
    service_dns: String,
    port: u16,
    cached: RwLock<Vec<WorkerEndpoint>>,
}

impl DnsMembership {
    /// Build from an explicit service DNS name and Flight port.
    pub fn new(service_dns: impl Into<String>, port: u16) -> Self {
        Self {
            service_dns: service_dns.into(),
            port,
            cached: RwLock::new(Vec::new()),
        }
    }

    /// Read `WEFT_WORKER_SERVICE` (+ optional `WEFT_WORKER_PORT`).
    pub fn from_env() -> Option<Self> {
        let service_dns = std::env::var("WEFT_WORKER_SERVICE").ok()?;
        let service_dns = service_dns.trim();
        if service_dns.is_empty() {
            return None;
        }
        let port = std::env::var("WEFT_WORKER_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(50561);
        Some(Self::new(service_dns, port))
    }

    /// Resolve the headless Service to pod IPs and refresh the cached endpoint list.
    pub async fn resolve(&self) -> Result<Vec<WorkerEndpoint>> {
        let host = format!("{}:{}", self.service_dns, self.port);
        let addrs = tokio::net::lookup_host(&host)
            .await
            .map_err(|e| Error::Io(format!("dns lookup `{host}`: {e}")))?;
        let mut endpoints: Vec<WorkerEndpoint> = addrs
            .map(|a| format!("http://{}:{}", a.ip(), self.port))
            .collect();
        endpoints.sort();
        endpoints.dedup();
        *self
            .cached
            .write()
            .map_err(|_| Error::Execution("membership cache poisoned".into()))? = endpoints.clone();
        Ok(endpoints)
    }
}

impl ClusterMembership for DnsMembership {
    fn endpoints(&self) -> Vec<WorkerEndpoint> {
        self.cached
            .read()
            .map(|g| g.clone())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_membership_returns_endpoints() {
        let m = StaticMembership::new(vec!["a:1".into(), "b:1".into()]);
        assert_eq!(m.endpoints(), vec!["a:1".to_string(), "b:1".to_string()]);
    }

    #[test]
    fn assignment_is_stable_under_membership_change() {
        let full = StaticMembership::new(vec!["a:1".into(), "b:1".into(), "c:1".into()]);
        let n = 12u32;
        let before: Vec<_> = (0..n).map(|p| full.owner_of(p, n)).collect();

        // Remove one worker; every partition that did NOT belong to the removed worker keeps its
        // owner (rendezvous hashing's stability property — no global reshuffle).
        let reduced = StaticMembership::new(vec!["a:1".into(), "c:1".into()]);
        for p in 0..n {
            let owner_before = before[p as usize].clone().unwrap();
            let owner_after = reduced.owner_of(p, n).unwrap();
            if owner_before != "b:1" {
                assert_eq!(
                    owner_before, owner_after,
                    "partition {p} reshuffled unexpectedly"
                );
            } else {
                assert_ne!(owner_after, "b:1"); // reassigned away from the removed node
            }
        }
    }

    #[test]
    fn empty_membership_has_no_owner() {
        let m = StaticMembership::new(vec![]);
        assert_eq!(m.owner_of(0, 4), None);
    }

    #[test]
    fn dns_membership_from_env_reads_service() {
        std::env::set_var("WEFT_WORKER_SERVICE", "workers.ns.svc.cluster.local");
        std::env::set_var("WEFT_WORKER_PORT", "50561");
        let m = DnsMembership::from_env().expect("from_env");
        assert_eq!(m.service_dns, "workers.ns.svc.cluster.local");
        assert_eq!(m.port, 50561);
        std::env::remove_var("WEFT_WORKER_SERVICE");
        std::env::remove_var("WEFT_WORKER_PORT");
    }
}
