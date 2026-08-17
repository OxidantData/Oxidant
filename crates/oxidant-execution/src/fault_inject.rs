//! Test-only worker fault injection (env: `OXIDANT_FAULT_EXIT_ON_TASK`, `OXIDANT_FAULT_EXIT_STAGE`).
//!
//! When set, the worker process exits abruptly (exit code 137) on the Nth matching stage task
//! so driver retry / alternate-worker / lineage-recompute paths can be exercised in integration
//! tests without external signal timing.

use std::sync::atomic::{AtomicU32, Ordering};

use crate::shuffle::protocol::StageTicket;

static MATCHING_TASKS: AtomicU32 = AtomicU32::new(0);

fn stage_matches_filter(ticket: &StageTicket) -> bool {
    match std::env::var("OXIDANT_FAULT_EXIT_STAGE") {
        Ok(f) => match f.to_ascii_lowercase().as_str() {
            "producer" => ticket.produce,
            "consumer" => !ticket.produce,
            _ => true,
        },
        Err(_) => true,
    }
}

/// Exit the worker process on the configured task if fault injection is enabled.
pub fn maybe_fault_exit(ticket: &StageTicket) {
    let Ok(raw) = std::env::var("OXIDANT_FAULT_EXIT_ON_TASK") else {
        return;
    };
    let exit_on: u32 = raw.parse().unwrap_or(0);
    if exit_on == 0 || !stage_matches_filter(ticket) {
        return;
    }
    let n = MATCHING_TASKS.fetch_add(1, Ordering::SeqCst) + 1;
    if n == exit_on {
        eprintln!(
            "oxidant worker fault injection: exit on matching task {n} \
             (stage_id={}, produce={})",
            ticket.stage_id, ticket.produce
        );
        // 128 + 9 (SIGKILL) — mimics abrupt worker loss.
        std::process::exit(137);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    /// Serialize env-mutating tests — `OXIDANT_FAULT_EXIT_STAGE` is process-global.
    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    fn ticket(produce: bool) -> StageTicket {
        StageTicket {
            stage_id: if produce { 0 } else { 1 },
            partition_id: 0,
            num_partitions: 2,
            upstream_endpoints: vec![],
            stage_sql: String::new(),
            plan_fragment: vec![],
            hash_key_cols: vec![0],
            upstream_stage_ids: if produce { vec![] } else { vec![0] },
            produce,
            lakehouse_snapshot_pins: String::new(),
            replicated_tables: String::new(),
            coalesce_read_modulus: 0,
            forward_upstream_stage_ids: vec![],
            upstream_bucket_rows: vec![],
            lakeformation_required: false,
            lakeformation_principal: String::new(),
        }
    }

    #[test]
    fn stage_filter_matches_producer_and_consumer() {
        let _guard = env_lock();
        std::env::set_var("OXIDANT_FAULT_EXIT_STAGE", "producer");
        assert!(stage_matches_filter(&ticket(true)));
        assert!(!stage_matches_filter(&ticket(false)));
        std::env::set_var("OXIDANT_FAULT_EXIT_STAGE", "consumer");
        assert!(!stage_matches_filter(&ticket(true)));
        assert!(stage_matches_filter(&ticket(false)));
        std::env::remove_var("OXIDANT_FAULT_EXIT_STAGE");
    }

    #[test]
    fn unknown_or_missing_stage_filter_matches_all_tickets() {
        let _guard = env_lock();
        std::env::remove_var("OXIDANT_FAULT_EXIT_STAGE");
        assert!(stage_matches_filter(&ticket(true)));
        assert!(stage_matches_filter(&ticket(false)));

        // Unrecognized filter values intentionally match everything (fail-open for tests).
        std::env::set_var("OXIDANT_FAULT_EXIT_STAGE", "any");
        assert!(stage_matches_filter(&ticket(true)));
        assert!(stage_matches_filter(&ticket(false)));
        std::env::remove_var("OXIDANT_FAULT_EXIT_STAGE");
    }
}
