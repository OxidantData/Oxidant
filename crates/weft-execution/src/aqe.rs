//! Adaptive query execution: runtime partition coalescing when shuffle output is small/skewed.
//!
//! [`coalesced_partitions`] is consulted after producer stages. The driver never shrinks the
//! planned partition count mid-query — producers already wrote `np` buckets — so a coalesced
//! stage is instead read through the modulus mapping in [`coalesced_read_buckets`]: the
//! consumer dispatches `new_p` reader partitions, each pulling every bucket `b ≡ p mod new_p`,
//! which reads every written bucket exactly once (a plain `0..new_p` range would orphan
//! buckets `new_p..np-1` and silently drop their rows).

use weft_common::Result;

/// After a producer stage, suggest a reduced shuffle partition count when every bucket is
/// below `WEFT_AQE_COALESCE_MAX_ROWS` (default 4096) per sampled worker.
///
/// The return value is a **read modulus**, not a partition range: consumers must read the
/// coalesced stage through [`coalesced_read_buckets`], never as a plain `0..new_p` range
/// (that would orphan producer buckets `new_p..current`).
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

/// The producer bucket ids a coalesced consumer partition reads: `partition, partition + m,
/// partition + 2m, …` below `np` — every bucket `b ≡ partition (mod m)`. The driver dispatches
/// exactly `m` reader partitions for a coalesced consumer, so across those readers each of the
/// `np` written buckets is read exactly once. A `partition >= m` (which the driver never
/// dispatches) reads nothing; `m >= np` is the identity read — every partition pulls exactly
/// its own bucket — which is the no-AQE path.
pub fn coalesced_read_buckets(np: u32, m: u32, partition: u32) -> Vec<u32> {
    let m = m.max(1);
    if partition >= m {
        return Vec::new();
    }
    (partition..np).step_by(m as usize).collect()
}

/// AQE sampling is **off by default**: it costs one `bucket_row_counts` action round trip per
/// worker after every producer stage. Set `WEFT_AQE=1` to enable the sample; a coalesce
/// decision is then recorded at the stage barrier and applied as the read modulus of the
/// stage's downstream consumers (see [`coalesced_read_buckets`]).
pub fn aqe_enabled() -> bool {
    std::env::var("WEFT_AQE")
        .ok()
        .as_deref()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Whether the driver measures producer-stage row counts at the stage barrier for consumer
/// join-strategy statistics (env `WEFT_STAGE_INPUT_STATS`, default **on**). The measured
/// per-bucket totals ride the consumer's [`crate::shuffle::protocol::StageTicket`] so the
/// worker can attach exact row counts to its `shuffle_input*` scans — the runtime
/// SMJ→hash conversion input (Spark AQE's runtime join-strategy conversion). The sample is
/// one `bucket_row_counts` action round trip per worker per producer stage — a local
/// in-memory row count, no data movement (KAN-32) — and shares the AQE sample's round trip
/// when [`aqe_enabled`] is also set. `0`/`false`/`off`/`no` restores the pre-KAN-2 behavior
/// (no sampling, no ticket counts; workers fall back to DataFusion's own MemTable
/// statistics).
pub fn stage_input_stats_enabled() -> bool {
    std::env::var("WEFT_STAGE_INPUT_STATS")
        .ok()
        .as_deref()
        .map(|v| {
            !(v == "0"
                || v.eq_ignore_ascii_case("false")
                || v.eq_ignore_ascii_case("off")
                || v.eq_ignore_ascii_case("no"))
        })
        .unwrap_or(true)
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
            "AQE must default off — the per-stage bucket-row-count sample costs one action \
             round trip per worker on every query"
        );
        // With the default off, the suggestion path is a no-op pass-through.
        let p = coalesced_partitions(4, 8, &[100, 120, 90, 110, 80, 95, 105, 100]).unwrap();
        assert_eq!(p, 8, "disabled AQE must not suggest coalescing");

        std::env::set_var("WEFT_AQE", "1");
        assert!(aqe_enabled());
        std::env::remove_var("WEFT_AQE");
    }

    #[test]
    fn stage_input_stats_defaults_on_with_kill_switch() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("WEFT_STAGE_INPUT_STATS");
        assert!(
            stage_input_stats_enabled(),
            "stage-input stats sampling must default on — the barrier row-count sample is a \
             cheap local action (KAN-32)"
        );
        for v in ["0", "false", "off", "no", "FALSE"] {
            std::env::set_var("WEFT_STAGE_INPUT_STATS", v);
            assert!(
                !stage_input_stats_enabled(),
                "WEFT_STAGE_INPUT_STATS={v} must disable the sample"
            );
        }
        std::env::set_var("WEFT_STAGE_INPUT_STATS", "1");
        assert!(stage_input_stats_enabled());
        std::env::remove_var("WEFT_STAGE_INPUT_STATS");
    }

    #[test]
    fn coalesced_read_buckets_mapping() {
        assert_eq!(
            coalesced_read_buckets(8, 8, 3),
            vec![3],
            "m == np: identity"
        );
        assert_eq!(
            coalesced_read_buckets(8, 1, 0),
            (0..8).collect::<Vec<_>>(),
            "m == 1: one reader pulls every bucket"
        );
        // m does not divide np: readers get uneven but disjoint bucket classes.
        assert_eq!(coalesced_read_buckets(8, 3, 0), vec![0, 3, 6]);
        assert_eq!(coalesced_read_buckets(8, 3, 1), vec![1, 4, 7]);
        assert_eq!(coalesced_read_buckets(8, 3, 2), vec![2, 5]);
        assert!(
            coalesced_read_buckets(8, 3, 5).is_empty(),
            "a partition beyond the reader range is never dispatched and reads nothing"
        );
        assert_eq!(coalesced_read_buckets(5, 2, 1), vec![1, 3]);
    }

    #[test]
    fn coalesced_read_buckets_cover_every_bucket_exactly_once() {
        for (np, m) in [
            (1, 1),
            (4, 4),
            (8, 8),
            (8, 1),
            (8, 2),
            (8, 3),
            (8, 5),
            (5, 2),
            (9, 4),
            (16, 4),
        ] {
            let mut reads = vec![0usize; np as usize];
            // The driver dispatches partitions `0..m`; iterating `0..np` additionally proves
            // the never-dispatched `p >= m` partitions read nothing.
            for p in 0..np {
                for b in coalesced_read_buckets(np, m, p) {
                    reads[b as usize] += 1;
                }
            }
            assert!(
                reads.iter().all(|&n| n == 1),
                "np={np} m={m}: every bucket must be read exactly once, got {reads:?}"
            );
        }
    }
}
