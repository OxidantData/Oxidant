//! Parallelism-driven worker scaling recommendations.
//!
//! When `WEFT_AUTOSCALE=1` and `WEFT_GATEWAY_URL` + `WEFT_CLUSTER_ID` are set, the driver
//! posts a scale request before running distributed stages so the cluster can grow to match
//! shuffle partition count and peak stage task demand (replacing idle / CPU-only scaling).

use crate::driver::{Cluster, ExchangeMode, StageDef};

/// Per-stage task count for one distributed query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageTaskDemand {
    pub stage_id: u32,
    pub num_tasks: u32,
}

/// Query-level parallelism signal used to recommend worker replica count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParallelismDemand {
    pub shuffle_partitions: u32,
    pub peak_stage_tasks: u32,
    pub stages: Vec<StageTaskDemand>,
}

/// Scale-up recommendation derived from [`ParallelismDemand`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScaleRecommendation {
    pub current_workers: u32,
    pub recommended_workers: u32,
    pub peak_task_demand: u32,
    pub task_slots_per_worker: u32,
    pub should_scale: bool,
    pub reason: String,
}

/// Whether the driver should request a scale-up before scheduling distributed tasks.
pub fn autoscale_enabled() -> bool {
    std::env::var("WEFT_AUTOSCALE")
        .ok()
        .is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
}

/// Task slots per worker (`WEFT_WORKER_TASK_SLOTS`, default = available parallelism).
pub fn task_slots_per_worker() -> u32 {
    crate::flight::worker_task_slots().max(1) as u32
}

/// Count runnable tasks for one stage (mirrors connect observability).
pub fn stage_num_tasks(stage: &StageDef, stages: &[StageDef], cluster: &Cluster) -> u32 {
    if stage.exchange == ExchangeMode::Forward {
        return 1;
    }
    let is_output = !stages
        .iter()
        .any(|s| s.upstream_stage_ids.contains(&stage.stage_id));
    if is_output && !stage.upstream_stage_ids.is_empty() {
        cluster.num_partitions
    } else {
        cluster.worker_count().max(1) as u32
    }
}

/// Build a [`ParallelismDemand`] snapshot for the upcoming distributed run.
pub fn parallelism_demand(cluster: &Cluster, stages: &[StageDef]) -> ParallelismDemand {
    let stage_demands: Vec<StageTaskDemand> = stages
        .iter()
        .map(|s| StageTaskDemand {
            stage_id: s.stage_id,
            num_tasks: stage_num_tasks(s, stages, cluster),
        })
        .collect();
    let peak_stage_tasks = stage_demands
        .iter()
        .map(|d| d.num_tasks)
        .max()
        .unwrap_or(1)
        .max(1);
    ParallelismDemand {
        shuffle_partitions: cluster.num_partitions.max(1),
        peak_stage_tasks,
        stages: stage_demands,
    }
}

/// Recommend worker replica count given cluster bounds and task-slot capacity.
pub fn recommend_worker_count(
    current_workers: u32,
    min_workers: u32,
    max_workers: u32,
    demand: &ParallelismDemand,
    task_slots: u32,
) -> ScaleRecommendation {
    let slots = task_slots.max(1);
    let min = min_workers.max(1);
    let max = max_workers.max(min);
    let current = current_workers.max(min).min(max);

    // Avoid `u32::div_ceil` (stable 1.73) while crate MSRV is 1.72.
    let by_partitions = demand.shuffle_partitions.saturating_add(slots - 1) / slots;
    let by_stage_tasks = demand.peak_stage_tasks.saturating_add(slots - 1) / slots;
    let needed = by_partitions.max(by_stage_tasks).max(min);
    let recommended = needed.min(max).max(current);
    let peak = demand.peak_stage_tasks.max(demand.shuffle_partitions);
    let should_scale = recommended > current;
    let reason = if should_scale {
        format!(
            "parallelism demand peak={peak} partitions={} slots/worker={slots} needs {needed} workers (have {current})",
            demand.shuffle_partitions
        )
    } else {
        format!(
            "current {current} workers satisfy peak={peak} partitions={} with {slots} slots/worker",
            demand.shuffle_partitions
        )
    };

    ScaleRecommendation {
        current_workers: current,
        recommended_workers: recommended,
        peak_task_demand: peak,
        task_slots_per_worker: slots,
        should_scale,
        reason,
    }
}

/// Gateway URL and cluster id for proactive scale-up (`WEFT_GATEWAY_URL`, `WEFT_CLUSTER_ID`).
pub fn autoscale_target() -> Option<(String, String)> {
    let url = std::env::var("WEFT_GATEWAY_URL")
        .ok()
        .filter(|s| !s.is_empty())?;
    let id = std::env::var("WEFT_CLUSTER_ID")
        .ok()
        .filter(|s| !s.is_empty())?;
    Some((url.trim_end_matches('/').to_string(), id))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cluster(workers: usize, partitions: u32) -> Cluster {
        let mut c = Cluster::new(
            (0..workers)
                .map(|i| format!("http://127.0.0.1:5056{i}"))
                .collect(),
        );
        c.num_partitions = partitions;
        c
    }

    #[test]
    fn recommend_scales_up_when_partitions_exceed_capacity() {
        let stages = vec![
            StageDef::new(0, "SELECT 1", vec![], vec![0]),
            StageDef::new(1, "SELECT 2", vec![0], vec![]),
        ];
        let demand = parallelism_demand(&cluster(2, 32), &stages);
        assert_eq!(demand.peak_stage_tasks, 32);
        let rec = recommend_worker_count(2, 2, 16, &demand, 4);
        assert!(rec.should_scale);
        assert_eq!(rec.recommended_workers, 8);
        assert!(rec.reason.contains("needs 8 workers"));
    }

    #[test]
    fn recommend_respects_max_and_does_not_scale_down() {
        let demand = ParallelismDemand {
            shuffle_partitions: 4,
            peak_stage_tasks: 4,
            stages: vec![],
        };
        let rec = recommend_worker_count(8, 2, 8, &demand, 4);
        assert!(!rec.should_scale);
        assert_eq!(rec.recommended_workers, 8);
    }

    #[test]
    fn forward_stage_peak_is_one() {
        let stage = StageDef {
            exchange: ExchangeMode::Forward,
            ..StageDef::default()
        };
        assert_eq!(
            stage_num_tasks(&stage, std::slice::from_ref(&stage), &cluster(4, 4)),
            1
        );
    }
}
