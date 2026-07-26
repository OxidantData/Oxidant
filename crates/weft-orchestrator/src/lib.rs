//! Cluster provisioning backends for elastic worker pools.

pub mod autoscale;
pub mod backend;
pub mod spec;

pub use autoscale::{recommend_for_cluster, scale_if_needed, AutoscaleBounds};
pub use backend::{ClusterBackend, ClusterInfo, K8sBackend, StaticBackend};
pub use spec::ClusterSpec;
