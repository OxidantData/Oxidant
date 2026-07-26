//! Cluster membership: where the driver finds its workers.
//!
//! Today the driver takes a **static** `Vec<String>` of worker endpoints ([`Role::Driver`]).
//! On EKS, worker pods come and go (autoscaling, restarts), so the static list must become a
//! live view. This module introduces the [`ClusterMembership`] seam the distributed driver reads
//! at stage-scheduling time:
//!
//! - [`StaticMembership`] — the current behavior, kept for tests and local runs.
//! - `K8sMembership` (Wave 1, behind a `kube` dependency) — resolves the worker headless Service
//!   via DNS-SRV or an EndpointSlice watch.
//!
//! Crucially, membership also defines a **stable partition→worker assignment** (consistent
//! hashing) so a worker restart doesn't reshuffle ownership mid-query. The driver's `run_stages`
//! is refactored to consume this in the engine workstream; the trait is frozen here first.
//!
//! [`Role::Driver`]: crate::Role

use std::sync::Arc;

#[cfg(feature = "k8s")]
pub mod k8s;

#[cfg(feature = "k8s")]
pub use k8s::K8sMembership;

/// A worker endpoint (`host:port`) the driver can dial over Arrow Flight.
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

/// Resolve cluster membership for distributed execution.
pub fn resolve_membership(static_workers: &[WorkerEndpoint]) -> Arc<dyn ClusterMembership> {
    #[cfg(feature = "k8s")]
    if let Some(k8s) = k8s::K8sMembership::from_env() {
        return Arc::new(RefreshingMembership::new(k8s));
    }
    Arc::new(RefreshingMembership::new(StaticMembership::new(
        static_workers.to_vec(),
    )))
}

/// TTL-cached membership that re-resolves endpoints on each `endpoints()` call after expiry.
pub struct RefreshingMembership {
    inner: Arc<dyn ClusterMembership>,
    ttl: std::time::Duration,
    cache: std::sync::Mutex<(std::time::Instant, Vec<WorkerEndpoint>)>,
}

impl RefreshingMembership {
    pub fn new(inner: impl ClusterMembership + 'static) -> Self {
        let ttl_ms = std::env::var("WEFT_MEMBERSHIP_TTL_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2000);
        Self {
            inner: Arc::new(inner),
            ttl: std::time::Duration::from_millis(ttl_ms),
            cache: std::sync::Mutex::new((std::time::Instant::now(), Vec::new())),
        }
    }

    fn refresh_if_needed(&self) -> Vec<WorkerEndpoint> {
        let mut guard = self.cache.lock().expect("membership cache poisoned");
        if guard.1.is_empty() || guard.0.elapsed() >= self.ttl {
            guard.1 = self.inner.endpoints();
            guard.0 = std::time::Instant::now();
        }
        guard.1.clone()
    }
}

impl ClusterMembership for RefreshingMembership {
    fn endpoints(&self) -> Vec<WorkerEndpoint> {
        self.refresh_if_needed()
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
}
