use std::sync::atomic::{self, AtomicU64};
use std::sync::Arc;
use std::time::{Duration, Instant};

use lruk::{CacheFull, Clock, InsertionResult, LruK};
use proptest::collection::vec;
use test_strategy::proptest;

macro_rules! assert_insert {
    ($cache:ident: $key:expr => $value:expr) => {
        let value = Arc::new($value);
        assert_eq!($cache.insert($key, value.clone()), InsertionResult::Inserted(value));
    };
}

macro_rules! assert_evict {
    ($cache:ident: $key:expr => $value:expr, $evicted:expr) => {
        let value = Arc::new($value);
        assert_eq!(
            $cache.insert($key, value.clone()),
            InsertionResult::InsertedWithEviction { value, evicted: Arc::new($evicted) }
        );
    };
}

#[test]
fn test_lru_cache_eviction() {
    let mut cache = LruK::<i32, Arc<char>, CounterClock, 1>::new(2, 0, 0);
    assert_insert!(cache: 1 => 'a');
    assert_insert!(cache: 2 => 'b');

    assert_evict!(cache: 3 => 'c', 'a');
    assert_evict!(cache: 4 => 'd', 'b');
}

#[test]
fn test_cache_as_lru() {
    let mut cache = LruK::<i32, Arc<char>, CounterClock, 1>::new(2, 0, 0);

    macro_rules! get {
        ($key:expr) => {
            cache.get($key).as_deref().copied()
        };
    }

    macro_rules! insert {
        ($key:expr => $value:expr) => {
            **cache.insert($key, Arc::new($value)).as_ref()
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
}

// The correctness of the eviction policy is currently a best-effort sort of thing, they are not essential for correctness.
// The property essential for correctness is that values that are referenced do NOT get evicted.
#[proptest]
fn test_cache_does_not_evict_referenced_values(
    #[strategy(1..20)] capacity: i32,
    #[strategy(0..600)] retained_information_period: i32,
    #[strategy(0..200)] correlated_reference_period: i32,
    #[strategy(vec(0..20, 0..500))] elements: Vec<i32>,
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

#[proptest]
fn test_cache_is_never_full_when_correlated_reference_period_is_zero(
    #[strategy(1..20)] capacity: i32,
    #[strategy(0..50)] retained_information_period: i32,
    #[strategy(vec(0..20, 0..500))] elements: Vec<i32>,
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

#[proptest]
fn test_cache_internal_assertions(
    #[strategy(1..21)] capacity: i32,
    #[strategy(0..200)] retained_information_period: i32,
    #[strategy(0..100)] correlated_reference_period: i32,
    #[strategy(vec(0..20, 0..500))] elements: Vec<i32>,
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
