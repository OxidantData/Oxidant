//! Adaptive query execution: runtime partition coalescing when shuffle output is small/skewed.
//!
//! [`coalesced_partitions`] is consulted after producer stages. The driver never shrinks the
//! planned partition count mid-query — producers already wrote `np` buckets — so a coalesced
//! stage is instead read through the modulus mapping in [`coalesced_read_buckets`]: the
//! consumer dispatches `new_p` reader partitions, each pulling every bucket `b ≡ p mod new_p`,
//! which reads every written bucket exactly once (a plain `0..new_p` range would orphan
//! buckets `new_p..np-1` and silently drop their rows).

use oxidant_common::Result;

/// Spark EMR advisory shuffle partition size (~64 MiB). When AQE is on, coalesce toward
/// `ceil(total_rows / rows_per_advisory_partition)` rather than always collapsing to
/// `num_workers` (which re-creates giant per-task working sets on SF100).
const DEFAULT_AQE_ADVISORY_PARTITION_BYTES: usize = 64 * 1024 * 1024;

/// Flat row-width assumption when converting the advisory byte target into a row count
/// (matches the coarse estimator used by the hash-join build guard).
const AQE_ADVISORY_ROW_WIDTH_BYTES: usize = 64;

/// After a producer stage, suggest a reduced shuffle partition count when every bucket is
/// below `OXIDANT_AQE_COALESCE_MAX_ROWS` (default 4096) per sampled worker, targeting Spark's
/// advisory partition size (`OXIDANT_AQE_ADVISORY_PARTITION_BYTES`, default 64 MiB).
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

    // Concentrated output: coalescing provably cannot make any reader hold more than it already
    // does. Reader `p` pulls exactly the buckets `b ≡ p (mod m)`, so when the worst such sum is
    // no larger than the largest single bucket, no reader grows and the only thing that changes
    // is that `current_partitions - m` tasks that would have read nothing are never dispatched.
    //
    // The motivating case is the empty-hash-key shape (`keys[]` — a gathered stage puts every row
    // in bucket 0). The row-ratio skew test below reads that as maximal skew, and the
    // `max_bucket > max_rows` test rejects it once the single bucket exceeds 4096 rows, so both
    // guards veto precisely the case that most needs coalescing. TPC-DS Q14's three per-arm
    // recombine stages each dispatched 200 tasks of which exactly **one** produced rows; the other
    // 199 cost ~220 ms of setup apiece, ~132 s of the query's ~200 s of CPU.
    //
    // Search upward for the *smallest* workable `m` rather than only trying the worker floor: a
    // stage whose rows sit in a handful of buckets cannot merge down to `floor` without stacking
    // two populated buckets onto one reader, but it can usually merge to some modest `m` that
    // still leaves every reader holding at most one. TPC-DS Q67's three 200-task stages spread 11
    // populated buckets across 200, so 189 of every 200 tasks read nothing.
    let floor = num_workers.max(1) as u32;

    // Skew: one bucket dominates. Or: a bucket is too big to merge, or there are too few buckets
    // to bother. These are the cases the advisory sizing below must not touch.
    let declined = (max_bucket * 3 > total && bucket_row_counts.len() > 2)
        || max_bucket > max_rows
        || bucket_row_counts.len() <= floor as usize;

    if !declined {
        // Spark-like target: enough reader partitions that each holds ~advisory bytes of rows.
        let rows_per_part = aqe_advisory_partition_rows().max(1);
        let target = ((total + rows_per_part - 1) / rows_per_part) as u32;
        return Ok(target.clamp(floor, current_partitions));
    }

    // Only now — where the guards above refuse to size the stage at all — fall back to the
    // provable check. This ordering matters: run first, it *overrides* the advisory sizing and
    // makes things worse, because a moderately uneven stage can satisfy `worst <= max_bucket` at
    // some mid-range `m` (say 50) that the advisory path would have sized at `floor`. Measured on
    // the sf10 sweep: probing first took the run from 5,157 tasks / 385 s of stage CPU to 9,726
    // tasks / 416 s.
    if !concentrated_coalesce_enabled() {
        return Ok(current_partitions);
    }
    // An extra "never stack two populated buckets on one reader" condition was tried here and
    // MEASURED WORSE: it cost +8.95 s over the SF10 99 (q67 alone +2.70 s, which is exactly the
    // stage shape this path exists to help) and did not rescue the queries it was written for —
    // q37/q50 still exhausted the pool. The reasoning is sound but the memory it saves is not
    // where the problem is, so it is not worth its price. `stacks_populated_buckets` stays as a
    // tested helper documenting the attempt; do not re-add it to this predicate without a
    // measurement that beats 62 s. (The helper itself is deleted rather than left dead — the
    // predicate was `counts.iter().skip(p).step_by(m).filter(|&&c| c > max_rows).count() > 1`
    // for every reader p.)
    (floor..current_partitions)
        .find(|&m| worst_coalesced_load(bucket_row_counts, m) <= max_bucket)
        .map_or(Ok(current_partitions), Ok)
}

/// The largest number of rows any single reader would hold if the stage were read with modulus
/// `m` — reader `p` pulls every bucket `b ≡ p (mod m)`, so this is the max over `p` of those sums.
/// Mirrors [`coalesced_read_buckets`]; keep the two in lockstep.
fn worst_coalesced_load(bucket_row_counts: &[usize], m: u32) -> usize {
    let m = m.max(1) as usize;
    (0..m)
        .map(|p| bucket_row_counts.iter().skip(p).step_by(m).sum::<usize>())
        .max()
        .unwrap_or(0)
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

/// AQE sampling defaults **on** (Spark 3+ / EMR parity). Set `OXIDANT_AQE=0`/`false`/`off` to
/// disable the per-producer `bucket_row_counts` round trip. When enabled, a coalesce decision
/// is recorded at the stage barrier and applied as the read modulus of the stage's downstream
/// consumers (see [`coalesced_read_buckets`]).
pub fn aqe_enabled() -> bool {
    std::env::var("OXIDANT_AQE")
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

/// Whether the driver measures producer-stage row counts at the stage barrier for consumer
/// join-strategy statistics (env `OXIDANT_STAGE_INPUT_STATS`, default **on**). The measured
/// per-bucket totals ride the consumer's [`crate::shuffle::protocol::StageTicket`] so the
/// worker can attach exact row counts to its `shuffle_input*` scans — the runtime
/// SMJ→hash conversion input (Spark AQE's runtime join-strategy conversion). The sample is
/// one `bucket_row_counts` action round trip per worker per producer stage — a local
/// in-memory row count, no data movement (KAN-32) — and shares the AQE sample's round trip
/// when [`aqe_enabled`] is also set. `0`/`false`/`off`/`no` restores the pre-KAN-2 behavior
/// (no sampling, no ticket counts; workers fall back to DataFusion's own MemTable
/// statistics).
pub fn stage_input_stats_enabled() -> bool {
    std::env::var("OXIDANT_STAGE_INPUT_STATS")
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

/// Kill switch for the concentrated-output coalesce path (`OXIDANT_AQE_CONCENTRATED=0`).
///
/// Defaults ON. Exists so a cluster can A/B the change from one image: without it the only way to
/// turn the path off is `OXIDANT_AQE=0`, which disables *all* coalescing and so measures something
/// else entirely. Any optimization that changes how many tasks get dispatched deserves an off
/// switch an operator can reach without a redeploy.
fn concentrated_coalesce_enabled() -> bool {
    std::env::var("OXIDANT_AQE_CONCENTRATED")
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
    std::env::var("OXIDANT_AQE_COALESCE_MAX_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4096)
}

fn aqe_advisory_partition_bytes() -> usize {
    std::env::var("OXIDANT_AQE_ADVISORY_PARTITION_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n: &usize| n > 0)
        .unwrap_or(DEFAULT_AQE_ADVISORY_PARTITION_BYTES)
}

fn aqe_advisory_partition_rows() -> usize {
    aqe_advisory_partition_bytes() / AQE_ADVISORY_ROW_WIDTH_BYTES
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// `OXIDANT_AQE` is process-global; serialize tests that mutate it.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn coalesces_small_uniform_buckets_toward_advisory_target() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("OXIDANT_AQE", "1");
        // 8 × ~100 rows: tiny vs 64 MiB advisory → collapse toward worker floor.
        let p = coalesced_partitions(4, 8, &[100, 120, 90, 110, 80, 95, 105, 100]).unwrap();
        std::env::remove_var("OXIDANT_AQE");
        assert_eq!(p, 4);
    }

    #[test]
    fn keeps_more_partitions_when_advisory_size_requires() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("OXIDANT_AQE", "1");
        // Force a tiny advisory so target stays above the worker floor.
        std::env::set_var("OXIDANT_AQE_ADVISORY_PARTITION_BYTES", "1024"); // 16 rows/part
        std::env::set_var("OXIDANT_AQE_COALESCE_MAX_ROWS", "100000");
        // 8 buckets × 1000 rows = 8000 rows → ceil(8000/16) = 500, clamped to current=8.
        let counts = vec![1000usize; 8];
        let p = coalesced_partitions(2, 8, &counts).unwrap();
        std::env::remove_var("OXIDANT_AQE");
        std::env::remove_var("OXIDANT_AQE_ADVISORY_PARTITION_BYTES");
        std::env::remove_var("OXIDANT_AQE_COALESCE_MAX_ROWS");
        assert_eq!(
            p, 8,
            "advisory sizing must not over-collapse large shuffles"
        );
    }

    #[test]
    fn keeps_partitions_on_skew() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("OXIDANT_AQE", "1");
        let p = coalesced_partitions(2, 4, &[10, 10, 10, 9000]).unwrap();
        std::env::remove_var("OXIDANT_AQE");
        assert_eq!(p, 4);
    }

    /// A gathered stage (`keys[]` — empty hash key) puts every row in bucket 0. That reads as
    /// maximal skew to [`keeps_partitions_on_skew`]'s test and blows past `max_rows` once the
    /// bucket exceeds 4096 rows, yet it is exactly the shape that must coalesce: without it the
    /// consumer dispatches `current_partitions` tasks and all but one read nothing. TPC-DS Q14's
    /// per-arm recombine stages hit this with 7,254 rows in 1 of 200 buckets.
    #[test]
    fn coalesces_single_bucket_stage_despite_skew_and_size() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("OXIDANT_AQE", "1");
        let mut counts = vec![0usize; 200];
        counts[0] = 7254; // above OXIDANT_AQE_COALESCE_MAX_ROWS (4096), and maximally skewed
        let p = coalesced_partitions(2, 200, &counts).unwrap();
        std::env::remove_var("OXIDANT_AQE");
        assert_eq!(
            p, 2,
            "a stage whose rows sit in one bucket must coalesce: no reader can grow, and the \
             other 198 tasks would read nothing"
        );
    }

    /// The concentrated-output path must never accept a coalesce that builds a reader larger
    /// than the biggest bucket that already existed — that is the straggler the skew guard is
    /// there to prevent.
    #[test]
    fn concentrated_path_rejects_when_a_reader_would_grow() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("OXIDANT_AQE", "1");
        // Two equal large buckets in the same residue class mod 2 would sum to 2x the largest
        // bucket, so the concentrated path must decline and leave the decision to the guards.
        let p = coalesced_partitions(2, 4, &[9000, 10, 9000, 10]).unwrap();
        std::env::remove_var("OXIDANT_AQE");
        assert_eq!(p, 4, "coalescing must not double a reader's working set");
    }

    /// The concentrated path must be switchable off from the environment so a cluster can A/B it
    /// without a second image — and without `OXIDANT_AQE=0`, which would disable all coalescing.
    // The row-sum test alone would accept a modulus that stacks two big buckets on one reader:
    // a consumer holds its whole modulus class in memory for the stage and nothing can spill it,
    // so that is a peak-RSS multiplier, not a wash. Two sharded facts (TPC-DS q37, q50) exhausted
    // the worker pool mid-pull because of exactly this.
    #[test]
    fn concentrated_path_has_a_kill_switch() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("OXIDANT_AQE", "1");
        let mut counts = vec![0usize; 200];
        counts[0] = 7254;

        std::env::remove_var("OXIDANT_AQE_CONCENTRATED");
        assert_eq!(
            coalesced_partitions(2, 200, &counts).unwrap(),
            2,
            "default ON"
        );

        std::env::set_var("OXIDANT_AQE_CONCENTRATED", "0");
        assert_eq!(
            coalesced_partitions(2, 200, &counts).unwrap(),
            200,
            "OXIDANT_AQE_CONCENTRATED=0 restores the pre-change fan-out"
        );
        std::env::remove_var("OXIDANT_AQE_CONCENTRATED");
        std::env::remove_var("OXIDANT_AQE");
    }

    /// Ordering regression guard. The provable concentrated check must run only where the skew /
    /// size guards decline — never ahead of the advisory sizing. A uniformly-filled stage is
    /// sized to the worker floor by the advisory path; the concentrated check would refuse to go
    /// below `current` for the same input, so probing first silently keeps every task. Measured
    /// on the sf10 sweep, probing first cost 5,157 -> 9,726 tasks and 385 s -> 416 s of CPU.
    #[test]
    fn advisory_sizing_wins_over_the_concentrated_probe() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("OXIDANT_AQE", "1");
        let counts = vec![100usize; 200]; // uniform: no skew, every bucket small
        let p = coalesced_partitions(2, 200, &counts).unwrap();
        std::env::remove_var("OXIDANT_AQE");
        assert_eq!(
            p, 2,
            "uniform small buckets must size to the worker floor, not stay at 200"
        );
        // The concentrated check alone would never accept anything below `current` here.
        assert!(worst_coalesced_load(&counts, 2) > 100);
    }

    /// A stage with a handful of populated buckets (not just one) must still shed its empty
    /// tasks: pick the smallest modulus that keeps every reader at or below the largest existing
    /// bucket. TPC-DS Q67 spreads 11 populated buckets over 200, so 189/200 tasks read nothing.
    #[test]
    fn coalesces_few_populated_buckets_to_the_smallest_safe_modulus() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("OXIDANT_AQE", "1");
        let mut counts = vec![0usize; 200];
        for i in 0..11 {
            counts[i * 7] = 9000; // 11 populated buckets, above max_rows, spread out
        }
        let p = coalesced_partitions(2, 200, &counts).unwrap();
        std::env::remove_var("OXIDANT_AQE");
        assert!(p < 200, "must coalesce away the 189 empty readers, got {p}");
        assert!(
            worst_coalesced_load(&counts, p) <= 9000,
            "chosen modulus {p} must not stack two populated buckets onto one reader"
        );
    }

    #[test]
    fn worst_coalesced_load_matches_the_read_mapping() {
        let counts = [5usize, 1, 7, 2, 3, 4];
        // Reader p pulls buckets b ≡ p (mod 2): {0,2,4} = 15 and {1,3,5} = 7.
        assert_eq!(worst_coalesced_load(&counts, 2), 15);
        // Cross-check against the mapping the consumers actually use, so the two cannot drift.
        for m in 1..=6u32 {
            let want = (0..m)
                .map(|p| {
                    coalesced_read_buckets(counts.len() as u32, m, p)
                        .iter()
                        .map(|&b| counts[b as usize])
                        .sum::<usize>()
                })
                .max()
                .unwrap();
            assert_eq!(worst_coalesced_load(&counts, m), want, "m={m}");
        }
    }

    #[test]
    fn aqe_defaults_on_and_env_opts_out() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("OXIDANT_AQE");
        assert!(
            aqe_enabled(),
            "AQE must default on — Spark 3+ / EMR parity for shuffle sizing"
        );
        // With the default on, small uniform buckets coalesce toward the worker floor.
        let p = coalesced_partitions(4, 8, &[100, 120, 90, 110, 80, 95, 105, 100]).unwrap();
        assert_eq!(p, 4, "enabled AQE must coalesce tiny uniform buckets");

        std::env::set_var("OXIDANT_AQE", "0");
        assert!(!aqe_enabled());
        let p = coalesced_partitions(4, 8, &[100, 120, 90, 110, 80, 95, 105, 100]).unwrap();
        assert_eq!(p, 8, "disabled AQE must not suggest coalescing");
        std::env::remove_var("OXIDANT_AQE");
    }

    #[test]
    fn stage_input_stats_defaults_on_with_kill_switch() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("OXIDANT_STAGE_INPUT_STATS");
        assert!(
            stage_input_stats_enabled(),
            "stage-input stats sampling must default on — the barrier row-count sample is a \
             cheap local action (KAN-32)"
        );
        for v in ["0", "false", "off", "no", "FALSE"] {
            std::env::set_var("OXIDANT_STAGE_INPUT_STATS", v);
            assert!(
                !stage_input_stats_enabled(),
                "OXIDANT_STAGE_INPUT_STATS={v} must disable the sample"
            );
        }
        std::env::set_var("OXIDANT_STAGE_INPUT_STATS", "1");
        assert!(stage_input_stats_enabled());
        std::env::remove_var("OXIDANT_STAGE_INPUT_STATS");
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
