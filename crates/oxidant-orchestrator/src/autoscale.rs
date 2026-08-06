//! Apply parallelism-driven scale recommendations to cluster specs and backends.

use oxidant_execution::autoscale::{ParallelismDemand, ScaleRecommendation};

use crate::backend::{worker_bounds, ClusterBackend, ClusterInfo};
use crate::spec::ClusterSpec;

/// Bounds for autoscaling a live cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutoscaleBounds {
    pub min_workers: u32,
    pub max_workers: u32,
}

impl AutoscaleBounds {
    pub fn from_spec(spec: &ClusterSpec) -> Self {
        let b = worker_bounds(spec);
        Self {
            min_workers: b.min,
            max_workers: b.max,
        }
    }
}

/// Merge execution demand with orchestrator bounds into a scale recommendation.
pub fn recommend_for_cluster(
    spec: &ClusterSpec,
    demand: &ParallelismDemand,
    task_slots: u32,
) -> ScaleRecommendation {
    let bounds = AutoscaleBounds::from_spec(spec);
    let current = worker_bounds(spec).desired;
    oxidant_execution::autoscale::recommend_worker_count(
        current,
        bounds.min_workers,
        bounds.max_workers,
        demand,
        task_slots,
    )
}

/// Scale the backend when the recommendation calls for more workers.
pub fn scale_if_needed(
    backend: &dyn ClusterBackend,
    spec: &ClusterSpec,
    rec: &ScaleRecommendation,
) -> oxidant_common::Result<Option<ClusterInfo>> {
    if !rec.should_scale {
        return Ok(None);
    }
    let info = backend.scale_up(spec, rec.recommended_workers)?;
    Ok(Some(info))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::StaticBackend;
    use oxidant_execution::autoscale::parallelism_demand;
    use oxidant_execution::driver::{Cluster, StageDef};

    #[test]
    fn recommend_for_cluster_clamps_to_spec_max() {
        let spec = ClusterSpec::local_demo("t", 2);
        let stages = vec![StageDef::new(0, "SELECT 1", vec![], vec![0])];
        let mut cluster = Cluster::new(vec!["http://a:1".into(), "http://b:1".into()]);
        cluster.num_partitions = 64;
        let demand = parallelism_demand(&cluster, &stages);
        let rec = recommend_for_cluster(&spec, &demand, 4);
        assert!(rec.should_scale);
        assert_eq!(rec.recommended_workers, spec.max_workers);
    }

    #[test]
    fn static_backend_scale_if_needed_increases_endpoints() {
        let spec = ClusterSpec::local_demo("t", 2);
        let backend = StaticBackend::new(vec![]);
        let rec = ScaleRecommendation {
            current_workers: 2,
            recommended_workers: 4,
            peak_task_demand: 32,
            task_slots_per_worker: 4,
            should_scale: true,
            reason: "test".into(),
        };
        let info = scale_if_needed(&backend, &spec, &rec)
            .expect("scale")
            .expect("scaled");
        assert_eq!(info.worker_endpoints.len(), 4);
    }
}
