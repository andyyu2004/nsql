#![deny(rust_2018_idioms)]

use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::{Add, Sub};
use std::sync::Arc;

use arrayvec::ArrayVec;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;

pub trait RefCounted: Clone {
    fn ref_count(&self) -> usize;
}

pub trait Clock: Default {
    type Time: Copy
        + Ord
        + Add<Self::Duration, Output = Self::Time>
        + Sub<Self::Time, Output = Self::Duration>;
    type Duration: Copy + Ord;

    fn now(&self) -> Self::Time;
}

impl<T: ?Sized> RefCounted for Arc<T> {
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

impl<T, const K: usize> History<T, K> {
    pub fn kth(&self) -> Option<&T> {
        self.0.first()
    }

    pub fn last(&self) -> Option<&T> {
        self.0.last()
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

// based off https://www.cs.cmu.edu/~natassa/courses/15-721/papers/p297-o_neil.pdf
pub struct LruK<K, V, C: Clock, F: Callbacks = NullCallbacks<K, V>, const N: usize = 2> {
    map: DashMap<K, V>,
    histories: DashMap<K, History<C::Time, N>>,
    last_accessed: DashMap<K, C::Time>,
    capacity: usize,
    clock: C,
    retained_information_period: C::Duration,
    /// The span of time where another reference to a key will be considered to be correlated with the prior reference.
    /// These references will not be considered a second reference.
    /// It is also the minimum span of time where a key must be retained in the cache.
    correlated_reference_period: C::Duration,
    callbacks: F,
}

pub struct CacheFull;

impl<K, V, C, const N: usize> LruK<K, V, C, NullCallbacks<K, V>, N>
where
    // K doesn't really need to be copy, can relax to clone if needed
    K: Eq + Hash + Copy + 'static,
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
    K: Eq + Hash + Copy + 'static,
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
            map: DashMap::with_capacity(capacity + 1),
            clock: C::default(),
            last_accessed: Default::default(),
            histories: Default::default(),
        }
    }

    pub fn get(&self, key: K) -> Option<V> {
        assert!(self.map.len() <= self.capacity);
        let now = self.clock.now();
        let value = self.map.get(&key);

        self.last_accessed.insert(key, now);

        let mut hist = self.histories.entry(key).or_default();
        match hist.last().copied() {
            // if it was a correlated reference, then we just update the last reference time
            Some(last) if now - last < self.correlated_reference_period => {
                hist.update_last(now);
            }
            // otherwise, we push a new reference time
            _ => hist.push(now),
        }
        value.map(|r| r.value().clone())
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.map.len() >= self.capacity
    }

    fn is_overfull(&self) -> bool {
        assert!(self.map.len() <= self.capacity + 1, "map is overfull by more than one element");
        self.map.len() > self.capacity
    }

    // attempts to insert `(K, V)` into the cache failing if the cache is full and there are no eviction candidates
    // If the key already exists, it is NOT replaced.
    pub fn try_insert(&self, key: K, value: V) -> Result<V, CacheFull> {
        let now = self.clock.now();
        // checking for `overfullness` because the new key may already exist
        if self.is_overfull() && !self.evict(now) {
            return Err(CacheFull);
        }

        match self.map.entry(key) {
            // don't replace the old value
            Entry::Occupied(entry) => return Ok(entry.get().clone()),
            Entry::Vacant(entry) => entry,
        }
        .insert(value.clone());

        // insert into history so it is not possible for they key to be immediately evicted
        let prev = self.last_accessed.insert(key, now);

        // if we did actually insert something (i.e. it was a new key) then we try another eviction
        // and if this fails we undo the insertion
        if self.is_overfull() && !self.evict(now) {
            self.map.remove(&key);
            match prev {
                Some(prev) => assert!(self.last_accessed.insert(key, prev).is_some()),
                None => assert!(self.last_accessed.remove(&key).is_some()),
            }
            assert!(!self.is_overfull());
            return Err(CacheFull);
        }

        assert!(!self.is_overfull());
        Ok(value)
    }

    // panicking variant of `try_insert`
    pub fn insert(&self, key: K, value: V) -> V {
        match self.try_insert(key, value) {
            Ok(value) => value,
            Err(CacheFull) => panic!("failed to insert: cache is full"),
        }
    }

    // evict a key returning true if an eviction occurred and false due to no eviction candidates
    fn evict(&self, now: C::Time) -> bool {
        assert!(
            self.map.len() <= self.capacity + 1,
            "map is larger than capacity by more than one at the start of eviction"
        );
        let victim = self
            .map
            .iter()
            .filter(|r| r.value().ref_count() == 1)
            .filter(|r| {
                // don't evict keys that have been referenced in the last `retained_information_period` or are still referenced
                let has_references = r.value().ref_count() > 1;
                let referenced_too_recently = match self.last_accessed.get(r.key()) {
                    Some(last) => now - *last.value() < self.retained_information_period,
                    None => false,
                };

                !has_references && !referenced_too_recently
            })
            // find the key with the minimum kth reference time (i.e. the `kth` reference is the least recent reference)
            .min_by_key(|k| match self.histories.get(k.key()) {
                Some(hist) => hist.value().kth().copied().unwrap(),
                None => now,
            });

        let succeeded = victim.is_some();
        if let Some(victim) = victim {
            assert!(victim.value().ref_count() == 1);
            self.callbacks.on_evict(victim.key(), victim.value());
            let key = *victim.key();
            drop(victim);
            debug_assert!(self.map.remove(&key).is_some());
            debug_assert!(self.last_accessed.remove(&key).is_some());
            // not removing from history as we want to retain the history using a separate parameter
        }

        // drop any entries for keys that have not been referenced in the last `retained_information_period`
        self.histories.retain(|_, hist| match hist.last().copied() {
            Some(last) => now - last < self.retained_information_period,
            None => false,
        });

        if succeeded {
            assert!(self.map.len() <= self.capacity);
        }
        succeeded
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn callbacks(&self) -> &F {
        &self.callbacks
    }
}
