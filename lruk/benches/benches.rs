use std::sync::atomic::{self, AtomicU64};
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use lruk::{Clock, LruK};

fn insertions() {
    const CAPACITY: usize = 12 * 1024 * 1024 / 4096;
    let mut cache = LruK::<usize, Arc<()>, CounterClock>::new(CAPACITY, 100, 20);

    const MULTIPLIER: usize = 5;

    for i in 0..CAPACITY * MULTIPLIER {
        cache.insert(i, Arc::new(()));
    }
}

fn bench_insertions(c: &mut Criterion) {
    c.bench_function("insertions", |b| b.iter(insertions));
}

criterion_group!(benches, bench_insertions);
criterion_main!(benches);

#[derive(Default)]
struct CounterClock {
    counter: AtomicU64,
}

impl Clock for CounterClock {
    type Time = u64;
    type Duration = u64;

    fn now(&self) -> Self::Time {
        self.counter.fetch_add(1, atomic::Ordering::Relaxed)
    }
}
