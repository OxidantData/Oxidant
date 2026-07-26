//! Declarative cluster specification (driver + worker images, sizing).

use serde::{Deserialize, Serialize};

/// Desired cluster shape for provisioning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSpec {
    pub cluster_id: String,
    pub namespace: String,
    pub worker_count: u32,
    pub worker_port: u16,
    pub min_workers: u32,
    pub max_workers: u32,
    pub worker_image: String,
    pub connect_image: String,
}

impl ClusterSpec {
    pub fn local_demo(id: &str, workers: u32) -> Self {
        Self {
            cluster_id: id.to_string(),
            namespace: format!("weft-cl-{id}"),
            worker_count: workers,
            worker_port: 50561,
            min_workers: workers,
            max_workers: workers.saturating_mul(4).max(workers),
            worker_image: "weft/worker:latest".into(),
            connect_image: "weft/connect-server:latest".into(),
        }
    }

    /// Headless Service DNS name workers register under.
    pub fn worker_service_host(&self) -> String {
        format!("weft-worker.{}.svc.cluster.local", self.namespace)
    }
}
