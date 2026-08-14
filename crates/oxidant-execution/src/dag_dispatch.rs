//! Dependency-layer bookkeeping for dependency-aware concurrent stage dispatch
//! (`OXIDANT_CONCURRENT_STAGES`, default on; see [`crate::driver`]).
//!
//! The legacy driver dispatched stages strictly sequentially: one stage's tasks plus its
//! stage barrier had to finish before the next stage in the topological list was built.
//! That wastes whole-cluster time on branch DAGs — TPC-DS Q4/Q78's three fact arms and
//! Q61's two store_sales scans are independent stages that never overlap, losing ~1–2s per
//! multi-arm query. [`StageDag`] computes each stage's dependency set so the driver can
//! dispatch a stage as soon as ALL of its upstreams have completed: independent stages run
//! concurrently, while a consumer still waits for every upstream — the barrier semantics
//! are preserved per consumer, just no longer globally ordered.
//!
//! Two dependency sources make a stage ready:
//!
//! - [`StageDef::upstream_stage_ids`] — the shuffle edges the planner already emits.
//! - The KAN-27 scalar-token edge: a stage whose SQL carries the scalar placeholder
//!   ([`crate::driver::SCALAR_TOKEN`]) reads the literal pulled from the scalar-combine
//!   stage's output, but never lists that stage as an upstream (the sequential loop got
//!   the ordering from the topological list position alone). The token carrier therefore
//!   gains an implicit edge on the scalar stage.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::driver::{StageDef, SCALAR_TOKEN};

/// Which non-output stages may dispatch now, and what unblocks when one finishes.
///
/// Pure bookkeeping — no I/O — so the dependency computation is unit-testable without a
/// cluster. The driver feeds it stage completions (`complete`) and failures (`fail`); the
/// ready queue is FIFO in stage-list (topological) order so dispatch preference stays
/// close to the sequential order the list encodes.
pub struct StageDag {
    /// Dispatch-set stage id -> upstream stages (within the dispatch set) not yet completed.
    deps_remaining: HashMap<u32, usize>,
    /// Dispatch-set stage id -> stages that list it as a dependency.
    dependents: HashMap<u32, Vec<u32>>,
    /// Stages whose dependencies all completed, in stage-list order, not yet dispatched.
    ready: VecDeque<u32>,
    /// Stages that completed successfully.
    completed: HashSet<u32>,
    /// Stages that can never run because an upstream (transitively) failed.
    skipped: HashSet<u32>,
    total: usize,
}

impl StageDag {
    /// Build the dependency layer over every stage except the (single) output stage.
    /// `scalar_stage_id` is the KAN-27 scalar-combine stage when the plan carries scalar
    /// tokens. Upstream ids outside the dispatch set (a dangling reference the sequential
    /// loop also ignored — the worker would read empty buckets) are not waited on.
    pub fn new(stages: &[StageDef], output_stage_id: u32, scalar_stage_id: Option<u32>) -> Self {
        let dispatch: HashSet<u32> = stages
            .iter()
            .map(|s| s.stage_id)
            .filter(|id| *id != output_stage_id)
            .collect();
        let mut deps_remaining: HashMap<u32, usize> = HashMap::new();
        let mut dependents: HashMap<u32, Vec<u32>> = HashMap::new();
        let mut ready = VecDeque::new();
        for s in stages {
            if !dispatch.contains(&s.stage_id) {
                continue;
            }
            let mut deps: HashSet<u32> = s
                .upstream_stage_ids
                .iter()
                .copied()
                .filter(|u| dispatch.contains(u))
                .collect();
            // KAN-27: the token carrier reads the scalar stage's output via literal
            // injection, an edge the plan expresses only positionally. Indexed tokens
            // (`__OXIDANT_SCALAR_STAGE_{i}__`, KAN-144) share the same edge.
            if (s.sql.contains(SCALAR_TOKEN) || s.sql.contains("__OXIDANT_SCALAR_STAGE_"))
                && scalar_stage_id != Some(s.stage_id)
            {
                if let Some(scalar) = scalar_stage_id.filter(|id| dispatch.contains(id)) {
                    deps.insert(scalar);
                }
            }
            for &d in &deps {
                dependents.entry(d).or_default().push(s.stage_id);
            }
            if deps.is_empty() {
                ready.push_back(s.stage_id);
            }
            deps_remaining.insert(s.stage_id, deps.len());
        }
        Self {
            deps_remaining,
            dependents,
            ready,
            completed: HashSet::new(),
            skipped: HashSet::new(),
            total: dispatch.len(),
        }
    }

    /// The next dispatchable stage (dependencies complete, not yet dispatched).
    pub fn take_ready(&mut self) -> Option<u32> {
        self.ready.pop_front()
    }

    /// Mark `id` successfully completed; dependents whose last outstanding dependency this
    /// was become ready. Completing an unknown or already-counted stage is a no-op.
    pub fn complete(&mut self, id: u32) {
        if !self.deps_remaining.contains_key(&id) || !self.completed.insert(id) {
            return;
        }
        let Some(deps) = self.dependents.get(&id) else {
            return;
        };
        // Clone the list: releasing a dependent mutates `deps_remaining`.
        let deps = deps.clone();
        for d in deps {
            if let Some(remaining) = self.deps_remaining.get_mut(&d) {
                *remaining = remaining.saturating_sub(1);
                if *remaining == 0 {
                    self.ready.push_back(d);
                }
            }
        }
    }

    /// Mark `id` failed: every transitive dependent is skipped (it can never become ready —
    /// a stage is only released once ALL of its dependencies completed). Returns the skipped
    /// stage ids in deterministic (sorted) order. In-flight independents are unaffected;
    /// the driver drops them when it surfaces `id`'s error.
    pub fn fail(&mut self, id: u32) -> Vec<u32> {
        let mut stack = self.dependents.get(&id).cloned().unwrap_or_default();
        while let Some(d) = stack.pop() {
            if self.completed.contains(&d) || !self.skipped.insert(d) {
                continue;
            }
            if let Some(next) = self.dependents.get(&d) {
                stack.extend(next.iter().copied());
            }
        }
        let mut out: Vec<u32> = self.skipped.iter().copied().collect();
        out.sort_unstable();
        out
    }

    /// Stages neither completed nor skipped. Non-zero after the dispatch loop drains means
    /// the DAG had a dependency cycle (the planner emits topological orders; this is a
    /// defensive guard, not an expected path).
    pub fn unfinished(&self) -> usize {
        self.total - self.completed.len() - self.skipped.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stage(id: u32, upstreams: &[u32]) -> StageDef {
        StageDef::new(id, format!("SELECT {id}"), upstreams.to_vec(), vec![])
    }

    /// Linear chain 0 → 1 → 2 with output 3: exactly one stage is ever ready.
    #[test]
    fn chain_releases_one_stage_at_a_time() {
        let stages = vec![
            stage(0, &[]),
            stage(1, &[0]),
            stage(2, &[1]),
            stage(3, &[2]),
        ];
        let mut dag = StageDag::new(&stages, 3, None);
        assert_eq!(dag.take_ready(), Some(0));
        assert_eq!(dag.take_ready(), None, "1 waits on 0");
        dag.complete(0);
        assert_eq!(dag.take_ready(), Some(1));
        dag.complete(1);
        assert_eq!(dag.take_ready(), Some(2));
        dag.complete(2);
        assert_eq!(dag.take_ready(), None);
        assert_eq!(dag.unfinished(), 0);
    }

    /// Diamond: leaves 0 and 1 are both immediately ready (concurrent arms); the join 2
    /// waits for BOTH; the output 3 is excluded from the dispatch set.
    #[test]
    fn diamond_holds_consumer_until_all_upstreams_complete() {
        let stages = vec![
            stage(0, &[]),
            stage(1, &[]),
            stage(2, &[0, 1]),
            stage(3, &[2]),
        ];
        let mut dag = StageDag::new(&stages, 3, None);
        let mut first_wave = vec![dag.take_ready(), dag.take_ready()];
        assert_eq!(dag.take_ready(), None, "only the two leaves are ready");
        first_wave.sort();
        assert_eq!(first_wave, vec![Some(0), Some(1)]);
        dag.complete(0);
        assert_eq!(dag.take_ready(), None, "2 still waits on 1");
        dag.complete(1);
        assert_eq!(dag.take_ready(), Some(2));
    }

    /// Disjoint arms 0→1 and 2→3 under output 4: each arm progresses independently of the
    /// other's completion order.
    #[test]
    fn disjoint_arms_progress_independently() {
        let stages = vec![
            stage(0, &[]),
            stage(1, &[0]),
            stage(2, &[]),
            stage(3, &[2]),
            stage(4, &[1, 3]),
        ];
        let mut dag = StageDag::new(&stages, 4, None);
        assert_eq!(dag.take_ready(), Some(0));
        assert_eq!(dag.take_ready(), Some(2));
        assert_eq!(dag.take_ready(), None);
        dag.complete(2);
        assert_eq!(dag.take_ready(), Some(3), "arm 2→3 unblocks on 2 alone");
        dag.complete(0);
        assert_eq!(dag.take_ready(), Some(1));
    }

    /// Failure propagation: failing leaf 0 of the diamond skips the join 2 (transitively
    /// everything downstream of it) but leaves the independent arm 1 dispatchable.
    #[test]
    fn failure_marks_transitive_dependents_skipped() {
        let stages = vec![
            stage(0, &[]),
            stage(1, &[]),
            stage(2, &[0, 1]),
            stage(3, &[2]),
        ];
        let mut dag = StageDag::new(&stages, 3, None);
        assert_eq!(dag.take_ready(), Some(0));
        assert_eq!(dag.take_ready(), Some(1));
        assert_eq!(dag.fail(0), vec![2], "the join is skipped; 1 is not");
        // The surviving arm still completes; the skipped stage never becomes ready.
        dag.complete(1);
        assert_eq!(dag.take_ready(), None);
        assert_eq!(dag.unfinished(), 1, "only the failed stage 0 remains");
    }

    /// A mid-chain failure skips only that failure's downstream, not earlier or sibling
    /// stages.
    #[test]
    fn mid_chain_failure_skips_downstream_only() {
        // 0,1 leaves; 2 joins them; 3 consumes 2; 4 is an independent leaf. Output 5
        // consumes 3 and 4 — transitively skipped through 3, but 4 itself is unaffected.
        let stages = vec![
            stage(0, &[]),
            stage(1, &[]),
            stage(2, &[0, 1]),
            stage(3, &[2]),
            stage(4, &[]),
            stage(5, &[3, 4]),
        ];
        let mut dag = StageDag::new(&stages, 5, None);
        assert_eq!(dag.take_ready(), Some(0));
        assert_eq!(dag.take_ready(), Some(1));
        assert_eq!(dag.take_ready(), Some(4));
        dag.complete(0);
        dag.complete(1);
        assert_eq!(dag.take_ready(), Some(2));
        assert_eq!(dag.fail(2), vec![3], "only 3 is downstream of 2");
        dag.complete(4);
        assert_eq!(
            dag.take_ready(),
            None,
            "the output was never dispatchable anyway"
        );
        assert_eq!(dag.unfinished(), 1, "only the failed stage 2 remains");
    }

    /// KAN-27 scalar-token edge: the token carrier never lists the scalar-combine stage as
    /// an upstream, yet must not dispatch before it completes.
    #[test]
    fn scalar_token_stage_waits_on_scalar_stage() {
        let mut carrier = stage(2, &[]);
        carrier.sql = format!("SELECT * FROM t WHERE x > '{SCALAR_TOKEN}'");
        // Scalar partial 0 → scalar combine 1 (unconsumed by any stage); carrier 2;
        // output 3.
        let stages = vec![stage(0, &[]), stage(1, &[0]), carrier, stage(3, &[2])];
        let mut dag = StageDag::new(&stages, 3, Some(1));
        assert_eq!(
            dag.take_ready(),
            Some(0),
            "only the scalar partial is ready"
        );
        assert_eq!(
            dag.take_ready(),
            None,
            "the carrier waits on the scalar combine"
        );
        dag.complete(0);
        assert_eq!(dag.take_ready(), Some(1));
        dag.complete(1);
        assert_eq!(dag.take_ready(), Some(2));
    }

    /// Defensive: an upstream id no stage defines is not waited on (the sequential loop
    /// likewise dispatched such a stage in list order; the worker reads empty buckets).
    #[test]
    fn dangling_upstream_is_not_waited_on() {
        let stages = vec![stage(0, &[9]), stage(1, &[0])];
        let mut dag = StageDag::new(&stages, 1, None);
        assert_eq!(dag.take_ready(), Some(0));
    }

    /// Defensive: a cycle drains the ready queue with stages left over — the driver turns
    /// `unfinished() != 0` into a plan error instead of hanging.
    #[test]
    fn cycle_is_detected_via_unfinished() {
        let stages = vec![stage(0, &[1]), stage(1, &[0]), stage(2, &[0, 1])];
        let mut dag = StageDag::new(&stages, 2, None);
        assert_eq!(dag.take_ready(), None);
        assert_eq!(dag.unfinished(), 2);
    }
}
