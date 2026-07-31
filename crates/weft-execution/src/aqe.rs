//! Adaptive query execution: runtime partition coalescing suggestions when shuffle output is
//! small/skewed.
//!
//! [`coalesced_partitions`] is consulted after producer stages for observability
//! (`AqeCoalesced` / tracing). The driver must **not** shrink the consumer partition range
//! mid-query until the shuffle read path can merge `p, p+w, …` buckets — otherwise rows in
//! orphaned buckets are silently dropped.

use weft_common::Result;

/// After a producer stage, suggest a reduced shuffle partition count when every bucket is
/// below `WEFT_AQE_COALESCE_MAX_ROWS` (default 4096) per sampled worker.
///
/// Callers must treat the return value as a **suggestion** for metrics / future planning —
/// applying it as the consumer's `0..new_p` range orphans producer buckets `new_p..current`.
pub fn coalesced_partitions(
    num_workers: usize,
    current_partitions: u32,
    bucket_row_counts: &[usize],
) -> Result<u32> {
    if !aqe_enabled() {
        return Ok(current_partitions);
    }
    let max_rows = aqe_coalesce_max_rows();
    if bucket_row_counts.is_empty() {
        return Ok(current_partitions);
    }
    let total: usize = bucket_row_counts.iter().sum();
    if total == 0 {
        return Ok(1);
    }
    let max_bucket = *bucket_row_counts.iter().max().unwrap_or(&0);
    // Skew: one bucket dominates — keep partitions (skew join handling is future work).
    if max_bucket * 3 > total && bucket_row_counts.len() > 2 {
        return Ok(current_partitions);
    }
    if max_bucket <= max_rows && bucket_row_counts.len() > num_workers.max(1) {
        Ok(num_workers.max(1) as u32)
    } else {
        Ok(current_partitions)
    }
}

/// AQE sampling is **off by default**: the suggestion it computes is deliberately never
/// applied (see [`finish_stage_barrier`](crate::driver) — shrinking the consumer range would
/// orphan producer buckets), so the per-stage `bucket_row_counts` round trips were pure
/// overhead on every query. Set `WEFT_AQE=1` to re-enable the observability sample.
pub fn aqe_enabled() -> bool {
    std::env::var("WEFT_AQE")
        .ok()
        .as_deref()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn aqe_coalesce_max_rows() -> usize {
    std::env::var("WEFT_AQE_COALESCE_MAX_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4096)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// `WEFT_AQE` is process-global; serialize tests that mutate it.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn coalesces_small_uniform_buckets() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("WEFT_AQE", "1");
        let p = coalesced_partitions(4, 8, &[100, 120, 90, 110, 80, 95, 105, 100]).unwrap();
        std::env::remove_var("WEFT_AQE");
        assert_eq!(p, 4);
    }

    #[test]
    fn keeps_partitions_on_skew() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("WEFT_AQE", "1");
        let p = coalesced_partitions(2, 4, &[10, 10, 10, 9000]).unwrap();
        std::env::remove_var("WEFT_AQE");
        assert_eq!(p, 4);
    }

    #[test]
    fn aqe_defaults_off_and_env_opts_in() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("WEFT_AQE");
        assert!(
            !aqe_enabled(),
            "AQE sampling must default off — its suggestion is never applied, so the \
             per-stage bucket-row-count round trips are pure overhead"
        );
        // With the default off, the suggestion path is a no-op pass-through.
        let p = coalesced_partitions(4, 8, &[100, 120, 90, 110, 80, 95, 105, 100]).unwrap();
        assert_eq!(p, 8, "disabled AQE must not suggest coalescing");

        std::env::set_var("WEFT_AQE", "1");
        assert!(aqe_enabled());
        std::env::remove_var("WEFT_AQE");
    }
}
