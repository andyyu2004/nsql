#![feature(test)]

use std::hint::black_box;
use std::sync::atomic::{self, AtomicU64};
use std::sync::Arc;

use lruk::{Clock, LruK};

extern crate test;

fn insertions() {
    const CAPACITY: usize = 12 * 1024 * 1024 / 4096;
    let mut cache = LruK::<usize, Arc<()>, CounterClock>::new(CAPACITY, 100, 20);

    const MULTIPLIER: usize = 5;

    for i in 0..CAPACITY * MULTIPLIER {
        cache.insert(i, Arc::new(()));
        // cache.get(key);
    }
}

#[bench]
fn bench(b: &mut test::Bencher) {
    b.iter(black_box(insertions))
}

#[derive(Default)]
struct CounterClock {
    counter: AtomicU64,
}

impl Clock for CounterClock {
    type Time = u64;
    type Duration = u64;

    fn now(&self) -> Self::Time {
        self.counter.fetch_add(1, atomic::Ordering::SeqCst)
    }
}
