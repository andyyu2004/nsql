use std::sync::atomic::{self, AtomicU64};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use lruk::{CacheFull, Clock, LruK};
use proptest::prelude::*;

#[derive(Default)]
pub struct Callbacks<K, V> {
    evicted: RwLock<Vec<(K, V)>>,
}

impl<K: Clone, V: Clone> lruk::Callbacks for Callbacks<K, V> {
    type Key = K;
    type Value = V;

    fn on_evict(&self, key: &Self::Key, value: &Self::Value) {
        self.evicted.write().unwrap().push((key.clone(), value.clone()));
    }
}

#[test]
fn test_cache_as_lru() {
    let cbs = Callbacks::default();
    let mut cache =
        LruK::<i32, Arc<char>, CounterClock, Callbacks<i32, Arc<char>>, 1>::new_with_callbacks(
            2, 0, 0, cbs,
        );

    macro_rules! get {
        ($key:expr) => {
            cache.get($key).as_deref().copied()
        };
    }

    macro_rules! insert {
        ($key:expr => $value:expr) => {
            *cache.insert($key, Arc::new($value)).as_ref()
        };
    }

    assert!(cache.is_empty());
    assert!(get!(1).is_none());

    assert_eq!(insert!(1 => 'a'), 'a');

    assert!(!cache.is_empty());
    assert_eq!(cache.len(), 1);
    assert_eq!(get!(1), Some('a'));

    assert_eq!(insert!(2 => 'b'), 'b');
    assert_eq!(cache.len(), 2);
    assert_eq!(get!(2), Some('b'));

    assert_eq!(insert!(3 => 'c'), 'c');
    assert_eq!(cache.len(), 2);
    assert_eq!(get!(1), None);
    assert_eq!(get!(2), Some('b'));
    assert_eq!(get!(3), Some('c'));

    assert_eq!(insert!(4 => 'd'), 'd');
    assert_eq!(cache.len(), 2);
    assert_eq!(get!(1), None);
    assert_eq!(get!(2), None);
    assert_eq!(get!(3), Some('c'));
    assert_eq!(get!(4), Some('d'));

    // keeps former value
    assert_eq!(insert!(4 => 'e'), 'd');
    assert_eq!(cache.len(), 2);
    assert_eq!(get!(1), None);
    assert_eq!(get!(2), None);
    assert_eq!(get!(3), Some('c'));
    assert_eq!(get!(4), Some('d'));

    let evicted = cache.callbacks().evicted.read().unwrap();
    assert_eq!(evicted.len(), 2);
    assert_eq!(evicted[0], (1, Arc::new('a')));
    assert_eq!(evicted[1], (2, Arc::new('b')));
}

// The correctness of the eviction policy is currently a best-effort sort of thing, they are not essential for correctness.
// The property essential for correctness is that values that are referenced do NOT get evicted.
proptest! {
    #[test]
    fn test_cache_does_not_evict_referenced_values(
        capacity in 15..50,
        retained_information_period in 0..20,
        correlated_reference_period in 0..20,
        elements in prop::collection::vec(0..20, 0..50),
    ) {
        let mut cache = LruK::<i32, Arc<char>, CounterClock>::new(
            capacity as usize,
            retained_information_period as u64,
            correlated_reference_period as u64,
        );

        let mut references = vec![];
        for (i, &element) in elements.iter().enumerate() {
            if cache.try_insert(element, Arc::new(i as u8 as char)).is_ok() {
                let value = cache.get(element).expect("key shouldn't be immediately evicted");
                references.push(value);
            }
        }
    }
}

proptest! {
    #[test]
    fn test_cache_is_never_full_when_correlated_reference_period_is_zero(
        capacity in 1..20,
        retained_information_period in 0..20,
        elements in prop::collection::vec(0..20, 0..50),
    ) {
        let mut cache = LruK::<i32, Arc<char>, CounterClock>::new(
            capacity as usize,
            retained_information_period as u64,
            0,
        );

        for (i, &element) in elements.iter().enumerate() {
            cache.insert(element, Arc::new(i as u8 as char));
        }
    }
}

proptest! {
    #[test]
    fn test_cache_internal_assertions(
        capacity in 1..20,
        retained_information_period in 0..20,
        correlated_reference_period in 0..20,
        elements in prop::collection::vec(0..20, 0..50),
    ) {
        let mut cache = LruK::<i32, Arc<char>, CounterClock>::new(
            capacity as usize,
            retained_information_period as u64,
            correlated_reference_period as u64,
        );

        for (i, &element) in elements.iter().enumerate() {
            if cache.try_insert(element, Arc::new(i as u8 as char)).is_ok() {
                assert!(cache.get(element).is_some(), "key should never be immediately evicted");
            }
        }
    }
}

#[test]
#[should_panic]
fn test_disallow_zero_capacity() {
    LruK::<i32, Arc<char>, CounterClock>::new(0, 0, 0);
}

#[test]
fn test_reference_is_not_evicted() {
    let mut cache = LruK::<usize, Arc<u64>, CounterClock>::new(1, 0, 0);
    let arc = Arc::new(1);
    cache.insert(1, Arc::clone(&arc));
    assert_eq!(cache.try_insert(2, arc), Err(CacheFull));
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

#[derive(Default)]
struct RealClock;

impl Clock for RealClock {
    type Time = Instant;
    type Duration = Duration;

    fn now(&self) -> Self::Time {
        Instant::now()
    }
}
