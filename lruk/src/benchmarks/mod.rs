use std::hint::black_box;
use std::sync::atomic::{self, AtomicU64};
use std::sync::Arc;

use crate::{Clock, LruK};

extern crate test;

// #[test]
fn f() {
    // const CAPACITY: usize = 1024 * 1024 / 4096;
    const CAPACITY: usize = 3;
    let mut cache = LruK::<usize, Arc<usize>, CounterClock>::new(CAPACITY, 100, 0);

    for i in 0..CAPACITY + 1 {
        drop(cache.insert(i % 1024, Arc::new(i)));
    }
}

// #[bench]
fn bench(b: &mut test::Bencher) {
    b.iter(black_box(f))
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
