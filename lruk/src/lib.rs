#![cfg_attr(test, feature(test))]
#![deny(rust_2018_idioms)]

use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::Sub;
use std::sync::Arc;

use arrayvec::ArrayVec;
use rustc_hash::{FxHashMap, FxHashSet};

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

struct History<T, const K: usize>(ArrayVec<T, K>);

impl<T, const K: usize> Default for History<T, K> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<T: Copy, const K: usize> History<T, K> {
    pub fn kth(&self) -> Option<T> {
        // if we don't even have `K` measurements, return None
        if self.0.len() < K { None } else { self.0.first().copied() }
    }

    pub fn last(&self) -> Option<T> {
        self.0.last().copied()
    }

    pub fn push(&mut self, value: T) {
        if self.0.is_full() {
            self.0.remove(0);
        }
        self.0.push(value);
    }

    pub fn update_last(&mut self, value: T) {
        self.0.pop().unwrap();
        self.0.push(value);
    }
}

pub trait Callbacks {
    type Key;
    type Value;

    fn on_evict(&self, key: &Self::Key, value: &Self::Value);
}

pub struct NullCallbacks<K, V> {
    marker: PhantomData<(K, V)>,
}

impl<K, V> NullCallbacks<K, V> {
    fn new() -> Self {
        Self { marker: PhantomData }
    }
}

impl<K, V> Callbacks for NullCallbacks<K, V> {
    type Key = K;
    type Value = V;

    fn on_evict(&self, _: &K, _: &V) {}
}

/// A map that maintains ordering based on `V` but only keeps the value for the latest key.
#[derive(Debug)]
struct WeirdMap<K, V> {
    map: FxHashMap<K, V>,
    ordering: BTreeMap<V, FxHashSet<K>>,
}

impl<K, V> Default for WeirdMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> WeirdMap<K, V> {
    pub fn new() -> Self {
        Self { ordering: Default::default(), map: Default::default() }
    }
}

impl<K, V> WeirdMap<K, V>
where
    K: Debug + Hash + Eq + Copy,
    V: Debug + Copy + Hash + Ord,
{
    pub fn insert(&mut self, k: K, v: V) {
        if let Some(old_value) = self.map.insert(k, v) {
            self.ordering.remove(&old_value);
        }

        assert!(self.ordering.entry(v).or_default().insert(k));
    }

    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: ?Sized + Eq + Hash,
    {
        let value = self.map.remove(key)?;
        let vs = self.ordering.get_mut(&value).unwrap();
        assert!(vs.remove(key));
        vs.shrink_to_fit();
        Some(value)
    }

    /// Iterator over keys in order of their values.
    /// If there are multiple values with the same key, the keys are returned in insertion order.
    pub fn keys(&self) -> impl Iterator<Item = K> + '_ {
        self.ordering.values().flatten().copied()
    }
}

// based off https://www.cs.cmu.edu/~natassa/courses/15-721/papers/p297-o_neil.pdf
pub struct LruK<K, V, C: Clock, F: Callbacks = NullCallbacks<K, V>, const N: usize = 2> {
    map: FxHashMap<K, V>,
    kth_reference_times: RefCell<WeirdMap<K, Option<C::Time>>>,
    histories: RefCell<FxHashMap<K, History<C::Time, N>>>,
    last_accessed: RefCell<FxHashMap<K, C::Time>>,
    capacity: usize,
    clock: C,
    retained_information_period: C::Duration,
    /// The span of time where another reference to a key will be considered to be correlated with the prior reference.
    /// These references will not be considered a second reference.
    /// It is also the minimum span of time where a key must be retained in the cache.
    correlated_reference_period: C::Duration,
    callbacks: F,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheFull;

impl<K, V, C, const N: usize> LruK<K, V, C, NullCallbacks<K, V>, N>
where
    // K doesn't really need to be copy, can relax to clone if needed
    K: Debug + Eq + Hash + Copy + 'static,
    V: Send + Sync + RefCounted + 'static,
    C: Clock,
{
    pub fn new(
        capacity: usize,
        retained_information_period: C::Duration,
        correlated_reference_period: C::Duration,
    ) -> Self {
        assert!(capacity > 0, "capacity must be greater than 0");
        Self::new_with_callbacks(
            capacity,
            retained_information_period,
            correlated_reference_period,
            NullCallbacks::new(),
        )
    }
}

impl<K, V, C, F, const N: usize> LruK<K, V, C, F, N>
where
    // K doesn't really need to be copy, can relax to clone if needed
    K: Debug + Eq + Hash + Copy + 'static,
    F: Callbacks<Key = K, Value = V>,
    V: Send + Sync + RefCounted + 'static,
    C: Clock,
{
    pub fn new_with_callbacks(
        capacity: usize,
        retained_information_period: C::Duration,
        correlated_reference_period: C::Duration,
        callbacks: F,
    ) -> Self {
        // the capacity isn't exactly what we pass to it
        Self {
            capacity,
            callbacks,
            retained_information_period,
            correlated_reference_period,
            map: HashMap::with_capacity_and_hasher(capacity + 1, Default::default()),
            clock: C::default(),
            kth_reference_times: Default::default(),
            last_accessed: Default::default(),
            histories: Default::default(),
        }
    }

    pub fn get(&self, key: K) -> Option<V> {
        assert!(self.map.len() <= self.capacity);
        let now = self.clock.now();
        let value = self.map.get(&key)?;

        self.mark_access(key, now);
        Some(value.clone())
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.map.len() >= self.capacity
    }

    // attempts to insert `(K, V)` into the cache failing if the cache is full and there are no eviction candidates
    // If the key already exists, it is NOT replaced.
    pub fn try_insert(&mut self, k: K, v: V) -> Result<V, CacheFull> {
        assert!(!self.is_overfull());
        let now = self.clock.now();

        if let Some(old_value) = self.map.get(&k) {
            // don't replace the old value with the given `value`, just return it
            self.mark_access(k, now);
            return Ok(old_value.clone());
        }

        // `k` is a new value, so we need to evict something if the cache is already full
        if self.is_full() && !self.evict(now) {
            return Err(CacheFull);
        }

        assert!(self.map.insert(k, v.clone()).is_none());
        self.mark_access(k, now);

        assert!(!self.is_overfull());
        Ok(v)
    }

    #[inline]
    // panicking variant of `try_insert`
    pub fn insert(&mut self, key: K, value: V) -> V {
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

    #[inline]
    pub fn callbacks(&self) -> &F {
        &self.callbacks
    }

    // update required metadata
    fn mark_access(&self, k: K, at: C::Time) {
        self.last_accessed.borrow_mut().insert(k, at);

        let mut histories = self.histories.borrow_mut();
        let hist = histories.entry(k).or_default();
        match hist.last() {
            // if it was a correlated reference, then we just update the last reference time
            Some(last) if at - last < self.correlated_reference_period => hist.update_last(at),
            // otherwise, we push a new reference time
            _ => hist.push(at),
        }

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

    fn find_eviction_candidate(&mut self, now: C::Time) -> Option<K> {
        let last_accessed = self.last_accessed.borrow();
        // find key with the maximum kth reference time that matches the critieria
        self.kth_reference_times.borrow().keys().filter(|k| self.map[k].ref_count() == 1).find(
            // don't evict keys that have been referenced within the last `correlated_reference_period`
            |k| match last_accessed.get(k).copied() {
                Some(last) => now - last > self.correlated_reference_period,
                None => true,
            },
        )
    }

    // evict a key returning true if an eviction occurred and false due to no eviction candidates
    fn evict(&mut self, now: C::Time) -> bool {
        let victim = self.find_eviction_candidate(now);
        let succeeded = victim.is_some();

        if let Some(key) = victim {
            // one reference is `victim` and one in the map
            let value = self.map.remove(&key).unwrap();
            assert_eq!(value.ref_count(), 1, "eviction victim has outstanding references");
            self.callbacks.on_evict(&key, &value);
            assert!(self.last_accessed.borrow_mut().remove(&key).is_some());
            assert!(self.kth_reference_times.borrow_mut().remove(&key).is_some());
            // not removing from history as we want to retain the history using a separate parameter
            assert!(!self.is_overfull());
        }

        // drop any entries for keys that have not been referenced in the last `retained_information_period`
        self.histories.borrow_mut().retain(|_, hist| match hist.last() {
            Some(last) => now - last < self.retained_information_period,
            None => false,
        });

        succeeded
    }
}

#[cfg(test)]
mod tests;
