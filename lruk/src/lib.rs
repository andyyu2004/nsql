#![cfg_attr(test, feature(test))]
#![deny(rust_2018_idioms)]

use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::Sub;
use std::sync::Arc;

use arrayvec::ArrayVec;

pub trait RefCounted: Clone {
    fn ref_count(&self) -> usize;
}

pub trait Clock: Default {
    type Time: Copy + Ord + Debug + Sub<Self::Time, Output = Self::Duration>;
    type Duration: Copy + Ord + Debug;

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
    map: HashMap<K, V>,
    histories: RefCell<HashMap<K, History<C::Time, N>>>,
    last_accessed: RefCell<HashMap<K, C::Time>>,
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
            map: HashMap::with_capacity(capacity + 1),
            clock: C::default(),
            last_accessed: Default::default(),
            histories: Default::default(),
        }
    }

    pub fn get(&self, key: K) -> Option<V> {
        assert!(self.map.len() <= self.capacity);
        let now = self.clock.now();
        let value = self.map.get(&key)?;

        // if key was found, we update the `history` and `last_accessed` maps
        self.last_accessed.borrow_mut().insert(key, now);
        let mut histories = self.histories.borrow_mut();
        let hist = histories.entry(key).or_default();
        match hist.last().copied() {
            // if it was a correlated reference, then we just update the last reference time
            Some(last) if now - last < self.correlated_reference_period => hist.update_last(now),
            // otherwise, we push a new reference time
            _ => hist.push(now),
        }
        Some(value.clone())
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.map.len() >= self.capacity
    }

    fn is_overfull(&self) -> bool {
        assert!(self.map.len() <= self.capacity + 1, "map is overfull by more than one element");
        assert!(
            self.last_accessed.borrow().len() <= self.capacity + 1,
            "last_accessed has not been cleaned properly"
        );
        self.map.len() > self.capacity
    }

    // attempts to insert `(K, V)` into the cache failing if the cache is full and there are no eviction candidates
    // If the key already exists, it is NOT replaced.
    pub fn try_insert(&mut self, key: K, value: V) -> Result<V, CacheFull> {
        assert!(!self.is_overfull());
        let now = self.clock.now();

        match self.map.entry(key) {
            // don't replace the old value
            Entry::Occupied(entry) => return Ok(entry.get().clone()),
            Entry::Vacant(entry) => entry,
        }
        .insert(value.clone());

        // insert into history so it is not possible for they key to be immediately evicted
        let prev = self.last_accessed.borrow_mut().insert(key, now);

        // if we did actually insert something (i.e. it was a new key) then we try another eviction
        // and if this fails we undo the insertion
        if self.is_overfull() && !self.evict(now) {
            assert!(self.map.remove(&key).is_some());
            match prev {
                Some(prev) => assert!(self.last_accessed.borrow_mut().insert(key, prev).is_some()),
                None => assert!(self.last_accessed.borrow_mut().remove(&key).is_some()),
            }
            assert!(!self.is_overfull());
            return Err(CacheFull);
        }

        assert!(!self.is_overfull());
        Ok(value)
    }

    // panicking variant of `try_insert`
    pub fn insert(&mut self, key: K, value: V) -> V {
        match self.try_insert(key, value) {
            Ok(value) => value,
            Err(CacheFull) => panic!("failed to insert: cache is full"),
        }
    }

    fn find_eviction_candidate(&mut self, now: C::Time) -> Option<(K, V)> {
        let (key, value) = self
            .map
            .iter()
            // MUST NOT evict keys that still have references
            .filter(|(_, v)| v.ref_count() == 1)
            .filter(|(k, _)| {
                // don't evict keys that have been referenced within the last `correlated_reference_period`
                let referenced_too_recently = match self.last_accessed.borrow().get(k).copied() {
                    Some(last) => now - last < self.correlated_reference_period,
                    None => false,
                };

                !referenced_too_recently
            })
            // find the key with the minimum kth reference time (i.e. the `kth` reference is the least recent reference)
            .min_by_key(|(k, _)| match self.histories.borrow().get(k) {
                Some(hist) => hist.kth().copied().unwrap(),
                None => now,
            })?;

        Some((*key, value.clone()))
    }

    // evict a key returning true if an eviction occurred and false due to no eviction candidates
    fn evict(&mut self, now: C::Time) -> bool {
        let victim = self.find_eviction_candidate(now);
        let succeeded = victim.is_some();

        if let Some((key, value)) = victim {
            // one reference is `victim` and one in the map
            assert!(value.ref_count() == 2, "eviction candidate has outstanding references");
            self.callbacks.on_evict(&key, &value);
            assert!(self.map.remove(&key).is_some());
            assert!(self.last_accessed.borrow_mut().remove(&key).is_some());
            // not removing from history as we want to retain the history using a separate parameter
            assert!(!self.is_overfull());
        }

        // drop any entries for keys that have not been referenced in the last `retained_information_period`
        self.histories.borrow_mut().retain(|_, hist| match hist.last().copied() {
            Some(last) => now - last < self.retained_information_period,
            None => false,
        });

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
