//! GPU offload spike for the Oxidant engine (KAN-70).
//!
//! NVIDIA libcudf benchmarks 3.8x–9.0x faster than Oxidant's CPU path per TPC-H SF10
//! query, so the engine grows GPU offload behind a conservative DataFusion physical
//! optimizer rule: [`rule::GpuOffloadRule`] matches ONLY a final-stage aggregation
//! over conjunctive column-vs-literal filters over local parquet part files
//! (TPC-H Q1/Q6 shapes, KAN-75 multi-file; aggregate inputs may be arithmetic
//! expressions over columns/literals, KAN-76 derived columns) and replaces that
//! subtree with [`exec::GpuScanAggExec`],
//! which ships a JSON [`spec::GpuOpSpec`] through a plain-C FFI shim
//! ([`ffi::exec_spec`]) and streams the Arrow C Data Interface result back.
//!
//! Link modes: the default build compiles the CPU **mock shim** (`csrc/mock_shim.c`,
//! returns one fixed `mock_sum = 42` batch) so the whole path is testable in CI;
//! `--features gpu` links the real `libcudf_shim` (being built separately).
//!
//! The rule is registered only when the [`ENV_GPU_OFFLOAD`] env var is set
//! ([`register_if_enabled`]); default engine behavior is byte-identical to before.

pub mod exec;
pub mod ffi;
pub mod rule;
pub mod spec;

use std::sync::Arc;

use datafusion::physical_optimizer::PhysicalOptimizerRule;

/// Env var that turns GPU offload on (`1` or `true`). Anything else (including
/// unset) leaves the engine's physical optimizer pipeline untouched.
pub const ENV_GPU_OFFLOAD: &str = "OXIDANT_GPU_OFFLOAD";

/// Whether GPU offload is enabled for this process.
pub fn offload_enabled() -> bool {
    std::env::var(ENV_GPU_OFFLOAD)
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Insert [`rule::GpuOffloadRule`] into a physical optimizer pipeline — but ONLY
/// when [`offload_enabled`], and only immediately BEFORE `EnforceDistribution`:
/// the replaced subtree's output partitioning differs from the original
/// final-aggregate's, so distribution enforcement must still run above the
/// replacement. Without the `EnforceDistribution` anchor the rule is NOT
/// registered (a post-distribution rewrite could silently violate a parent's
/// partitioning requirements; no rule is strictly better than a misplaced one).
pub fn register_if_enabled(rules: &mut Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>) {
    if !offload_enabled() {
        return;
    }
    let Some(position) = rules.iter().position(|r| r.name() == "EnforceDistribution") else {
        return;
    };
    rules.insert(position, Arc::new(rule::GpuOffloadRule));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The registration helper is the guard against accidental default enablement:
    /// no env var → the pipeline is left untouched; env set → exactly one
    /// `gpu_offload` rule immediately before `EnforceDistribution`.
    #[test]
    fn registration_requires_env_and_anchor() {
        std::env::remove_var(ENV_GPU_OFFLOAD);
        let mut rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> =
            vec![Arc::new(EnforceDistributionStub)];
        register_if_enabled(&mut rules);
        assert!(
            rules.iter().all(|r| r.name() != "gpu_offload"),
            "rule must NOT register with {ENV_GPU_OFFLOAD} unset"
        );

        std::env::set_var(ENV_GPU_OFFLOAD, "1");
        register_if_enabled(&mut rules);
        assert_eq!(
            rules.iter().map(|r| r.name()).collect::<Vec<_>>(),
            vec!["gpu_offload", "EnforceDistribution"],
            "enabled: rule lands immediately before EnforceDistribution"
        );
        std::env::remove_var(ENV_GPU_OFFLOAD);

        // Without the EnforceDistribution anchor the rule must refuse to register.
        std::env::set_var(ENV_GPU_OFFLOAD, "1");
        let mut no_anchor: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> =
            vec![Arc::new(OtherRuleStub)];
        register_if_enabled(&mut no_anchor);
        assert!(
            no_anchor.iter().all(|r| r.name() != "gpu_offload"),
            "rule must NOT register without an EnforceDistribution anchor"
        );
        std::env::remove_var(ENV_GPU_OFFLOAD);
    }

    #[derive(Debug)]
    struct EnforceDistributionStub;
    impl PhysicalOptimizerRule for EnforceDistributionStub {
        fn optimize(
            &self,
            plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
            _config: &datafusion::common::config::ConfigOptions,
        ) -> datafusion::common::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
            Ok(plan)
        }
        fn name(&self) -> &str {
            "EnforceDistribution"
        }
        fn schema_check(&self) -> bool {
            true
        }
    }

    #[derive(Debug)]
    struct OtherRuleStub;
    impl PhysicalOptimizerRule for OtherRuleStub {
        fn optimize(
            &self,
            plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
            _config: &datafusion::common::config::ConfigOptions,
        ) -> datafusion::common::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
            Ok(plan)
        }
        fn name(&self) -> &str {
            "other_rule"
        }
        fn schema_check(&self) -> bool {
            true
        }
    }
}
