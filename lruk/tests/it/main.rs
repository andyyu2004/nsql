use std::sync::atomic::{self, AtomicU64};
use std::sync::Arc;
use std::time::{Duration, Instant};

use lruk::{Clock, LruK};
use proptest::prelude::*;

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

#[derive(Default)]
struct RealClock;

impl Clock for RealClock {
    type Time = Instant;
    type Duration = Duration;

    fn now(&self) -> Self::Time {
        Instant::now()
    }
}

#[test]
fn test_cache_as_lru() {
    let cache = LruK::<i32, Arc<char>, CounterClock, 1>::new(2, 0, 0);

    macro_rules! get {
        ($key:expr) => {
            cache.get($key).as_deref().copied()
        };
    }

    macro_rules! insert {
        ($key:expr => $value:expr) => {
            cache.insert($key, Arc::new($value));
        };
    }

    assert!(cache.is_empty());
    assert!(get!(1).is_none());

    insert!(1 => 'a');

    assert!(!cache.is_empty());
    assert_eq!(cache.len(), 1);
    assert_eq!(get!(1), Some('a'));

    insert!(2 => 'b');
    assert_eq!(cache.len(), 2);
    assert_eq!(get!(2), Some('b'));

    insert!(3 => 'c');
    assert_eq!(cache.len(), 2);
    assert_eq!(get!(1), None);
    assert_eq!(get!(2), Some('b'));
    assert_eq!(get!(3), Some('c'));

    insert!(4 => 'd');
    assert_eq!(cache.len(), 2);
    assert_eq!(get!(1), None);
    assert_eq!(get!(2), None);
    assert_eq!(get!(3), Some('c'));
    assert_eq!(get!(4), Some('d'));
}

// The correctness of the eviction policy is currently a best-effort sort of thing, they are not essential for correctness.
// The property essential for correctness is that values that are referenced do NOT get evicted.
proptest! {
    #[test]
    fn test_cache_does_not_evict_referenced_values(
        capacity in 15..50,
        retained_information_period in 0..20,
        correlated_reference_period in 0..20,
        elements in prop::collection::vec(0..20, 0..100),
    ) {
        let cache = LruK::<i32, Arc<char>, CounterClock, 3>::new(
            capacity as usize,
            retained_information_period as u64,
            correlated_reference_period as u64,
        );

        let mut references = vec![];
        for (i, &element) in elements.iter().enumerate() {
            if cache.try_insert(element, Arc::new(i as u8 as char)) {
                let value = cache.get(element).expect("key shouldn't be immediately evicted");
                references.push(value);
            }
        }
    }
}

proptest! {
    #[test]
    fn test_cache_internal_assertions(
        capacity in 1..20,
        retained_information_period in 0..20,
        correlated_reference_period in 0..20,
        elements in prop::collection::vec(0..20, 0..100),
    ) {
        let cache = LruK::<i32, Arc<char>, CounterClock, 3>::new(
            capacity as usize,
            retained_information_period as u64,
            correlated_reference_period as u64,
        );

        for (i, &element) in elements.iter().enumerate() {
            if cache.try_insert(element, Arc::new(i as u8 as char)) {
                assert!(cache.get(element).is_some(), "key shouldn't be immediately evicted");
            }
        }
    }
}

#[test]
#[should_panic]
fn test_disallow_zero_capacity() {
    LruK::<i32, Arc<char>, CounterClock, 2>::new(0, 0, 0);
}
