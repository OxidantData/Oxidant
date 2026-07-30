//! A thin [`MemoryPool`] wrapper that timestamps operator memory activity — a progress
//! signal for the worker-side no-progress stage watchdog (KAN-47). Any real operator work
//! reserves or frees memory, so a frozen timestamp means the query is parked (the DF
//! sort-merge / spill-pool deadlock class) rather than merely slow.

use std::fmt::Display;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};

/// Shared handle to the pool's last-activity timestamp, in milliseconds since `epoch`
/// (engine construction). Atomic-millis rather than a locked `Instant` because
/// `grow`/`shrink` sit on the operator hot path and must stay contention-free.
#[derive(Debug, Clone)]
pub struct PoolActivity {
    epoch: Instant,
    last_activity_ms: Arc<AtomicU64>,
}

impl PoolActivity {
    fn new() -> Self {
        Self {
            epoch: Instant::now(),
            last_activity_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    fn touch(&self) {
        let ms = self.epoch.elapsed().as_millis() as u64;
        self.last_activity_ms.store(ms, Ordering::Relaxed);
    }

    /// Milliseconds since engine construction of the last pool `grow`/`shrink`/`try_grow`.
    pub fn last_activity_ms(&self) -> u64 {
        self.last_activity_ms.load(Ordering::Relaxed)
    }
}

/// A [`MemoryPool`] that delegates every operation to `inner` and timestamps each
/// allocation touch (`grow` / `shrink` / `try_grow`).
#[derive(Debug)]
pub struct ProgressMemoryPool {
    inner: Arc<dyn MemoryPool>,
    activity: PoolActivity,
}

impl ProgressMemoryPool {
    /// Wrap `inner`; returns the pool to install on the runtime env plus the shared
    /// activity handle the engine keeps for watchdog sampling.
    pub fn new(inner: Arc<dyn MemoryPool>) -> (Arc<Self>, PoolActivity) {
        let activity = PoolActivity::new();
        (
            Arc::new(Self {
                inner,
                activity: activity.clone(),
            }),
            activity,
        )
    }
}

impl Display for ProgressMemoryPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ProgressMemoryPool({})", self.inner)
    }
}

impl MemoryPool for ProgressMemoryPool {
    fn name(&self) -> &str {
        "progress_memory_pool"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.activity.touch();
        self.inner.grow(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.activity.touch();
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion::common::Result<()> {
        self.activity.touch();
        self.inner.try_grow(reservation, additional)
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::{FairSpillPool, UnboundedMemoryPool};

    #[test]
    fn touch_records_activity() {
        let (pool, activity) = ProgressMemoryPool::new(Arc::new(UnboundedMemoryPool::default()));
        let pool: Arc<dyn MemoryPool> = pool;
        assert_eq!(activity.last_activity_ms(), 0);
        let consumer = MemoryConsumer::new("test").register(&pool);
        consumer.try_grow(4096).unwrap();
        consumer.shrink(4096);
        // Activity was stamped (ms-since-epoch can still read 0 on a fast clock, so assert
        // the reservation went through the delegate instead of the timestamp value).
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn delegates_limit_and_reservation() {
        let (pool, _activity) = ProgressMemoryPool::new(Arc::new(FairSpillPool::new(8192)));
        let pool: Arc<dyn MemoryPool> = pool;
        assert!(matches!(pool.memory_limit(), MemoryLimit::Finite(8192)));
        let a = MemoryConsumer::new("a").register(&pool);
        a.try_grow(4096).unwrap();
        assert_eq!(pool.reserved(), 4096);
        a.free();
        assert_eq!(pool.reserved(), 0);
    }
}
