//! Regression coverage for the pinned DataFusion FairSpillPool, not sorter liveness.
use datafusion::execution::memory_pool::{FairSpillPool, MemoryConsumer, MemoryPool};
use std::sync::Arc;

#[test]
fn late_spiller_cannot_overcommit_existing_reservations() {
    let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(100));
    let early = MemoryConsumer::new("early")
        .with_can_spill(true)
        .register(&pool);
    early.try_grow(80).unwrap();
    let late = MemoryConsumer::new("late")
        .with_can_spill(true)
        .register(&pool);
    // The new share is 50, but only 20 total bytes remain. Existing allocations
    // do not shrink merely because a new consumer registers.
    let error = late.try_grow(30).expect_err("must enforce total capacity");
    assert!(error.to_string().contains("Resources exhausted"));
    assert_eq!((pool.reserved(), early.size(), late.size()), (80, 80, 0));
    late.try_grow(20).unwrap();
    assert_eq!((pool.reserved(), early.size(), late.size()), (100, 80, 20));
    early.shrink(10);
    late.try_grow(10).unwrap();
    assert_eq!((pool.reserved(), early.size(), late.size()), (100, 70, 30));
    drop(early);
    late.try_grow(70).unwrap(); // Last early registration was unregistered.
    assert_eq!((pool.reserved(), late.size()), (100, 100));
    drop(late);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn split_reservations_share_one_total_budget() {
    let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(100));
    let mut original = MemoryConsumer::new("split")
        .with_can_spill(true)
        .register(&pool);
    original.try_grow(80).unwrap();
    let child = original.split(60);
    let empty = original.new_empty();
    let taken = original.take();
    assert_eq!((original.size(), taken.size(), child.size()), (0, 20, 60));
    assert_eq!(pool.reserved(), 80);
    child
        .try_grow(30)
        .expect_err("split must not evade total bound");
    empty
        .try_grow(21)
        .expect_err("new_empty must not evade total bound");
    assert_eq!(
        (pool.reserved(), taken.size(), child.size(), empty.size()),
        (80, 20, 60, 0)
    );
    empty.try_grow(20).unwrap();
    assert_eq!(pool.reserved(), 100);
    drop(original);
    drop(taken);
    assert_eq!(pool.reserved(), 80);
    drop(child);
    empty.try_grow(80).unwrap();
    assert_eq!((pool.reserved(), empty.size()), (100, 100));
    drop(empty);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn nonspillable_pressure_does_not_create_spill_headroom() {
    let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(100));
    let early = MemoryConsumer::new("early")
        .with_can_spill(true)
        .register(&pool);
    early.try_grow(80).unwrap();
    let late = MemoryConsumer::new("late")
        .with_can_spill(true)
        .register(&pool);
    let fixed = MemoryConsumer::new("fixed").register(&pool);
    fixed.try_grow(20).unwrap();
    late.try_grow(1)
        .expect_err("the pool is full despite a 40-byte share");
    assert_eq!(
        (pool.reserved(), early.size(), late.size(), fixed.size()),
        (100, 80, 0, 20)
    );
    fixed.shrink(10);
    late.try_grow(10).unwrap();
    fixed
        .try_grow(1)
        .expect_err("nonspillable growth is also bounded");
    assert_eq!(
        (pool.reserved(), early.size(), late.size(), fixed.size()),
        (100, 80, 10, 10)
    );
    drop((early, late, fixed));
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn fair_share_and_last_registration_drop_still_apply() {
    let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(100));
    let a = MemoryConsumer::new("a")
        .with_can_spill(true)
        .register(&pool);
    let b = MemoryConsumer::new("b")
        .with_can_spill(true)
        .register(&pool);
    a.try_grow(60)
        .expect_err("share still applies with 100 bytes free");
    assert_eq!((pool.reserved(), a.size(), b.size()), (0, 0, 0));
    a.try_grow(50).unwrap();
    b.try_grow(50).unwrap();
    assert_eq!(pool.reserved(), 100);
    assert_eq!(b.free(), 50);
    let keeper = b.new_empty();
    drop(b);
    a.try_grow(1)
        .expect_err("empty child keeps the second consumer registered");
    assert_eq!((pool.reserved(), a.size(), keeper.size()), (50, 50, 0));
    drop(keeper);
    a.try_grow(50).unwrap();
    assert_eq!((pool.reserved(), a.size()), (100, 100));
    drop(a);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn huge_growth_failure_is_atomic_for_both_classes() {
    // Accounting only: no usize::MAX-sized allocation is made.
    for can_spill in [false, true] {
        let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(usize::MAX));
        let r = MemoryConsumer::new("boundary")
            .with_can_spill(can_spill)
            .register(&pool);
        r.try_grow(1).unwrap();
        r.try_grow(usize::MAX)
            .expect_err("request plus current bytes would overflow");
        assert_eq!((pool.reserved(), r.size()), (1, 1));
        r.try_grow(usize::MAX - 1).unwrap();
        assert_eq!((pool.reserved(), r.size()), (usize::MAX, usize::MAX));
        r.try_grow(1)
            .expect_err("exactly full is not more headroom");
        assert_eq!((pool.reserved(), r.size()), (usize::MAX, usize::MAX));
        r.shrink(1);
        r.try_grow(1).unwrap();
        assert_eq!(r.free(), usize::MAX);
        assert_eq!((pool.reserved(), r.size()), (0, 0));
    }
}

#[test]
fn mixed_classes_can_fill_the_usize_boundary_exactly() {
    let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(usize::MAX));
    let fixed = MemoryConsumer::new("fixed").register(&pool);
    let spill = MemoryConsumer::new("spill")
        .with_can_spill(true)
        .register(&pool);
    fixed.try_grow(1).unwrap();
    spill.try_grow(usize::MAX - 1).unwrap();
    for r in [&fixed, &spill] {
        r.try_grow(1).expect_err("mixed total has no capacity");
    }
    assert_eq!(
        (pool.reserved(), fixed.size(), spill.size()),
        (usize::MAX, 1, usize::MAX - 1)
    );
    spill.shrink(1);
    fixed.try_grow(1).unwrap();
    assert_eq!(
        (pool.reserved(), fixed.size(), spill.size()),
        (usize::MAX, 2, usize::MAX - 2)
    );
    drop((fixed, spill));
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn infallible_overcommit_remains_accounted_and_blocks_fallible_growth() {
    let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(100));
    let spill = MemoryConsumer::new("spill")
        .with_can_spill(true)
        .register(&pool);
    let fixed = MemoryConsumer::new("fixed").register(&pool);
    // MemoryPool::grow must always succeed; it is deliberately not a hard cap.
    spill.grow(120);
    fixed.grow(30);
    let empty = spill.new_empty();
    for r in [&spill, &fixed, &empty] {
        for additional in [0, 1] {
            r.try_grow(additional)
                .expect_err("an overcommitted pool admits no fallible request");
            assert_eq!(
                (pool.reserved(), spill.size(), fixed.size(), empty.size()),
                (150, 120, 30, 0)
            );
        }
    }
    assert_eq!(spill.free(), 120);
    fixed.try_grow(70).unwrap();
    assert_eq!((pool.reserved(), fixed.size()), (100, 100));
    drop((spill, fixed, empty));
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn unrepresentable_infallible_total_rejects_without_more_growth() {
    let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(usize::MAX));
    let spill = MemoryConsumer::new("spill")
        .with_can_spill(true)
        .register(&pool);
    let fixed = MemoryConsumer::new("fixed").register(&pool);
    // Each counter is representable, but their sum is not. Do not call reserved()
    // until the pre-existing infallible overcommit is released; its API is unchanged.
    spill.grow(usize::MAX);
    fixed.grow(1);
    let empty = spill.new_empty();
    for r in [&fixed, &empty] {
        for additional in [0, 1] {
            r.try_grow(additional)
                .expect_err("invalid headroom must not overflow or admit");
            assert_eq!(
                (spill.size(), fixed.size(), empty.size()),
                (usize::MAX, 1, 0)
            );
        }
    }
    assert_eq!(fixed.free(), 1);
    assert_eq!(pool.reserved(), usize::MAX);
    assert_eq!(spill.free(), usize::MAX);
    assert_eq!(pool.reserved(), 0);
    fixed.try_grow(1).unwrap();
    drop((fixed, spill, empty));
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn zero_capacity_allows_empty_noops_but_no_growth() {
    let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(0));
    for can_spill in [false, true] {
        let r = MemoryConsumer::new("zero")
            .with_can_spill(can_spill)
            .register(&pool);
        r.try_grow(0).unwrap();
        r.try_grow(1).expect_err("zero capacity");
        assert_eq!((pool.reserved(), r.size()), (0, 0));
        assert_eq!(r.free(), 0);
    }
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn concurrent_children_cannot_both_take_the_last_headroom() {
    use std::sync::Barrier;
    let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(100));
    let root = MemoryConsumer::new("concurrent")
        .with_can_spill(true)
        .register(&pool);
    root.try_grow(80).unwrap();
    let barrier = Barrier::new(2);
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let child = root.new_empty();
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    let result = child.try_grow(15);
                    (child, result)
                })
            })
            .collect();
        // Retain both children until all admissions have completed.
        let outcomes: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(outcomes.iter().filter(|(_, r)| r.is_ok()).count(), 1);
        assert_eq!(pool.reserved(), 95);
        for (child, result) in &outcomes {
            assert_eq!(child.size(), if result.is_ok() { 15 } else { 0 });
        }
    });
    assert_eq!((pool.reserved(), root.size()), (80, 80));
    drop(root);
    assert_eq!(pool.reserved(), 0);
}
