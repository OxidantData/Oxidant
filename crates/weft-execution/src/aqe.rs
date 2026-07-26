//! Adaptive query execution: runtime partition coalescing when shuffle output is small/skewed.

use weft_common::Result;

/// After a producer stage, optionally reduce shuffle partitions for downstream stages when
/// every bucket is below `WEFT_AQE_COALESCE_MAX_ROWS` (default 4096) per worker.
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

pub fn aqe_enabled() -> bool {
    std::env::var("WEFT_AQE")
        .ok()
        .as_deref()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
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

    #[test]
    fn coalesces_small_uniform_buckets() {
        let p = coalesced_partitions(4, 8, &[100, 120, 90, 110, 80, 95, 105, 100]).unwrap();
        assert_eq!(p, 4);
    }

    #[test]
    fn keeps_partitions_on_skew() {
        let p = coalesced_partitions(2, 4, &[10, 10, 10, 9000]).unwrap();
        assert_eq!(p, 4);
    }
}
