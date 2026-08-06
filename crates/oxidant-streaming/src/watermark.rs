//! Event-time watermarks for late-data handling in micro-batch streaming.

use std::time::Duration;

/// Watermark configuration (Spark `withWatermark` equivalent).
#[derive(Debug, Clone)]
pub struct WatermarkConfig {
    /// Column holding event time (must be timestamp or date).
    pub event_time_column: String,
    /// Allowed lateness before rows are dropped.
    pub delay: Duration,
}

impl WatermarkConfig {
    pub fn from_options(options: &std::collections::HashMap<String, String>) -> Option<Self> {
        let col = options
            .get("eventTimeColumn")
            .or_else(|| options.get("watermarkColumn"))?;
        let delay_ms = options
            .get("delayMs")
            .or_else(|| options.get("watermarkDelayMs"))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        Some(Self {
            event_time_column: col.clone(),
            delay: Duration::from_millis(delay_ms),
        })
    }

    /// Current watermark in microseconds since epoch (processing time minus delay).
    pub fn watermark_micros(&self, now_micros: i64) -> i64 {
        now_micros.saturating_sub(self.delay.as_micros() as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermark_subtracts_delay() {
        let wm = WatermarkConfig {
            event_time_column: "ts".into(),
            delay: Duration::from_secs(10),
        };
        let now = 1_000_000_000_000i64; // micros
        assert_eq!(wm.watermark_micros(now), now - 10_000_000);
    }
}
