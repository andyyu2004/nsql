#![cfg_attr(test, feature(test))]
#![deny(rust_2018_idioms)]

use std::cell::RefCell;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Sub;
use std::sync::Arc;

mod history;
mod value_ordered_map;

use rustc_hash::FxHashMap;

use self::history::History;
use self::value_ordered_map::ValueOrderedMap;

pub trait RefCounted: Clone {
    fn ref_count(&self) -> usize;
}

pub trait Clock: Default {
    type Time: Hash + Copy + Ord + Debug + Sub<Self::Time, Output = Self::Duration>;
    type Duration: Hash + Copy + Ord + Debug;

    fn now(&self) -> Self::Time;
}

impl<T: ?Sized> RefCounted for Arc<T> {
    #[inline]
    fn ref_count(&self) -> usize {
        Arc::strong_count(self)
    }
}

// based off https://www.cs.cmu.edu/~natassa/courses/15-721/papers/p297-o_neil.pdf
pub struct LruK<K, V, C: Clock, const N: usize = 2> {
    map: FxHashMap<K, V>,
    kth_reference_times: RefCell<ValueOrderedMap<K, Option<C::Time>>>,
    histories: RefCell<FxHashMap<K, History<C, N>>>,
    last_accessed: RefCell<FxHashMap<K, C::Time>>,
    capacity: usize,
    clock: C,
    retained_information_period: C::Duration,
    /// The span of time where another reference to a key will be considered to be correlated with the prior reference.
    /// These references will not be considered a second reference.
    /// It is also the minimum span of time where a key must be retained in the cache.
    correlated_reference_period: C::Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheFull;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertionResult<V> {
    /// The provided value was inserted.
    /// `V` is a clone of the value
    Inserted(V),
    /// The provided value was not inserted as it already exists in the cache.
    /// `V` is a clone of the existing value
    AlreadyExists(V),
    InsertedWithEviction {
        value: V,
        evicted: V,
    },
}

impl<V> AsRef<V> for InsertionResult<V> {
    fn as_ref(&self) -> &V {
        match self {
            InsertionResult::InsertedWithEviction { value, .. } => value,
            InsertionResult::AlreadyExists(value) | InsertionResult::Inserted(value) => value,
        }
    }
}

impl<K, V, C, const N: usize> LruK<K, V, C, N>
where
    // K doesn't really need to be copy, can relax to clone if needed
    K: Debug + Eq + Hash + Copy + 'static,
    V: Send + Sync + RefCounted + 'static,
    C: Clock,
{
    #[inline]
    pub fn new(
        capacity: usize,
        retained_information_period: C::Duration,
        correlated_reference_period: C::Duration,
    ) -> Self {
        assert!(capacity > 0, "capacity must be greater than 0");
        Self {
            capacity,
            retained_information_period,
            correlated_reference_period,
            map: FxHashMap::with_capacity_and_hasher(capacity + 1, Default::default()),
            clock: C::default(),
            kth_reference_times: Default::default(),
            last_accessed: Default::default(),
            histories: Default::default(),
        }
    }

    #[inline]
    pub fn get(&self, key: K) -> Option<V> {
        assert!(!self.is_overfull());
        let now = self.clock.now();
        let value = self.map.get(&key)?;

        self.register_access_at(key, now);
        Some(value.clone())
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.map.len() >= self.capacity
    }

    // attempts to insert `(K, V)` into the cache failing if the cache is full and there are no eviction candidates
    // If the key already exists, it is NOT replaced.
    #[inline]
    pub fn try_insert(&mut self, key: K, value: V) -> Result<InsertionResult<V>, CacheFull> {
        assert!(!self.is_overfull());
        let now = self.clock.now();

        if let Some(old_value) = self.map.get(&key) {
            // don't replace the old value with the given `value`, just return it
            self.register_access_at(key, now);
            return Ok(InsertionResult::AlreadyExists(old_value.clone()));
        }

        // `k` is a new value, so we need to evict something if the cache is already full
        let evicted = if self.is_full() {
            match self.evict(now) {
                Some(evicted) => Some(evicted),
                None => return Err(CacheFull),
            }
        } else {
            None
        };

        assert!(self.map.insert(key, value.clone()).is_none());
        self.register_access_at(key, now);

        assert!(!self.is_overfull());

        match evicted {
            Some(evicted) => Ok(InsertionResult::InsertedWithEviction { value, evicted }),
            None => Ok(InsertionResult::Inserted(value)),
        }
    }

    #[inline]
    // panicking variant of `try_insert`
    pub fn insert(&mut self, key: K, value: V) -> InsertionResult<V> {
        match self.try_insert(key, value) {
            Ok(value) => value,
            Err(CacheFull) => panic!("failed to insert: cache is full"),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    // update required metadata
    fn register_access_at(&self, k: K, at: C::Time) {
        self.last_accessed.borrow_mut().insert(k, at);

        let mut histories = self.histories.borrow_mut();
        let hist =
            histories.entry(k).or_insert_with(|| History::new(self.correlated_reference_period));
        hist.register_access_at(at);

        self.kth_reference_times.borrow_mut().insert(k, hist.kth());
    }

    #[track_caller]
    fn is_overfull(&self) -> bool {
        assert!(self.map.len() <= self.capacity + 1, "map is overfull by more than one element");
        assert!(
            self.last_accessed.borrow().len() <= self.capacity + 2,
            "last_accessed has not been cleaned properly"
        );
        self.map.len() > self.capacity
    }

    fn find_eviction_candidate(&mut self, at: C::Time) -> Option<K> {
        let last_accessed = self.last_accessed.borrow();
        // find key with the maximum kth reference time that matches the critieria
        self.kth_reference_times
            .borrow()
            .keys()
            .find(|k| self.is_key_safe_for_eviction(&last_accessed, k, at))
    }

    fn is_key_safe_for_eviction(
        &self,
        last_accessed: &FxHashMap<K, C::Time>,
        k: &K,
        at: C::Time,
    ) -> bool {
        self.map[k].ref_count() == 1
            // don't evict keys that have been referenced within the last `correlated_reference_period`
            && match last_accessed.get(k).copied() {
                Some(last) => at - last > self.correlated_reference_period,
                None => true,
            }
    }

    // evict a key returning true if an eviction occurred and false due to no eviction candidates
    fn evict(&mut self, at: C::Time) -> Option<V> {
        let victim = self.find_eviction_candidate(at);

        self.prune_history(at);

        let key = victim?;

        let mut last_accessed = self.last_accessed.borrow_mut();
        let value = self.map.remove(&key).unwrap();
        assert_eq!(value.ref_count(), 1, "eviction victim has outstanding references");
        last_accessed.remove(&key);
        drop(last_accessed);
        assert!(self.kth_reference_times.borrow_mut().remove(&key).is_some());
        // not removing from history as we want to retain the history using a separate parameter
        assert!(!self.is_overfull());

        Some(value)
    }

    // drop any entries for keys that have not been referenced in the last `retained_information_period`
    fn prune_history(&mut self, at: C::Time) {
        self.histories.borrow_mut().retain(|_, hist| match hist.latest_uncorrelated_access() {
            Some(last) => at - last < self.retained_information_period,
            None => false,
        });
    }
}

#[cfg(test)]
mod tests;
