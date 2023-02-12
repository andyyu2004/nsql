#![deny(rust_2018_idioms)]

use std::hash::Hash;
use std::ops::{Add, Sub};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use arrayvec::ArrayVec;
use dashmap::mapref::entry::Entry;
use dashmap::mapref::one::Ref;
use dashmap::DashMap;

pub trait RefCounted {
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

struct Hist<T, const K: usize>(ArrayVec<T, K>);

impl<T, const K: usize> Default for Hist<T, K> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<T, const K: usize> Hist<T, K> {
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

// based off https://www.cs.cmu.edu/~natassa/courses/15-721/papers/p297-o_neil.pdf
pub struct LruK<K, V, C: Clock, const N: usize> {
    map: DashMap<K, V>,
    history: DashMap<K, Hist<C::Time, N>>,
    last: DashMap<K, C::Time>,
    capacity: usize,
    timer: C,
    retained_information_period: C::Duration,
    /// The span of time where another reference to a key will be considered to be correlated with the prior reference.
    /// These references will not be considered a second reference.
    /// It is also the minimum span of time where a key must be retained in the cache.
    correlated_reference_period: C::Duration,
}

impl<K, V, C, const N: usize> LruK<K, V, C, N>
where
    K: Eq + Hash + Clone + 'static,
    V: Send + Sync + RefCounted + 'static,
    C: Clock,
{
    pub fn new(
        capacity: usize,
        retained_information_period: C::Duration,
        correlated_reference_period: C::Duration,
    ) -> Self {
        Self {
            capacity,
            retained_information_period,
            correlated_reference_period,
            map: DashMap::with_capacity(capacity),
            timer: C::default(),
            last: Default::default(),
            history: Default::default(),
        }
    }

    pub fn get(&self, key: K) -> Option<Ref<'_, K, V>> {
        let now = self.timer.now();
        let value = self.map.get(&key);

        self.last.insert(key.clone(), self.timer.now());

        let mut hist = self.history.entry(key).or_default();
        match hist.last().copied() {
            // if it was a correlated reference, then we just update the last reference time
            Some(last) if now - last < self.correlated_reference_period => {
                hist.update_last(now);
            }
            // otherwise, we push a new reference time
            _ => hist.push(now),
        }
        value
    }

    pub fn insert(&self, key: K, value: V) {
        let entry = match self.map.entry(key) {
            Entry::Occupied(mut occupied) => {
                occupied.insert(value);
                return;
            }
            Entry::Vacant(entry) => entry,
        };

        entry.insert(value);
        if self.map.len() > self.capacity {
            self.evict();
        }
    }

    fn evict(&self) {
        let now = self.timer.now();

        let victim = self
            .map
            .iter()
            .filter(|r| r.value().ref_count() == 1)
            .filter(|r| {
                // don't evict keys that have been referenced in the last `retained_information_period` or are still referenced
                let has_references = r.value().ref_count() > 1;
                let referenced_too_recently = match self.last.get(r.key()) {
                    Some(last) => now - *last.value() < self.retained_information_period,
                    None => false,
                };

                !has_references && !referenced_too_recently
            })
            // find the key with the minimum kth reference time (i.e. the `kth` reference is the least recent reference)
            .min_by_key(|k| match self.history.get(k.key()) {
                Some(hist) => hist.value().kth().copied().unwrap(),
                None => now,
            });

        match victim {
            Some(victim) => {
                assert!(victim.value().ref_count() == 1);
                let key = victim.key().clone();
                drop(victim);
                debug_assert!(self.map.remove(&key).is_some());
                debug_assert!(self.last.remove(&key).is_some());
                // not removing from history as we want to keep the history for a bit longer
            }
            None => panic!("out of capacity, could not evict from cache"),
        }

        // drop any entries for keys that have not been referenced in the last `retained_information_period`
        self.history.retain(|_, hist| match hist.last().copied() {
            Some(last) => now - last < self.retained_information_period,
            None => false,
        });

        assert!(self.map.len() <= self.capacity);
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}
