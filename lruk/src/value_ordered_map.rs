use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::hash::Hash;

use rustc_hash::{FxHashMap, FxHashSet};

/// A map that maintains ordering based on `V` but only keeps the value for the latest key.
#[derive(Debug)]
pub struct ValueOrderedMap<K, V> {
    map: FxHashMap<K, V>,
    ordering: BTreeMap<V, FxHashSet<K>>,
}

impl<K, V> Default for ValueOrderedMap<K, V> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> ValueOrderedMap<K, V> {
    #[inline]
    pub fn new() -> Self {
        Self { ordering: Default::default(), map: Default::default() }
    }
}

impl<K, V> ValueOrderedMap<K, V>
where
    K: Debug + Hash + Eq + Copy,
    V: Debug + Copy + Hash + Ord,
{
    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.map.get(key)
    }

    #[inline]
    pub fn insert(&mut self, k: K, v: V) {
        if let Some(old_v) = self.map.insert(k, v) {
            self.remove_inner(&k, &old_v);
        }

        assert!(self.ordering.entry(v).or_default().insert(k));
        // FIXME debug_assert that the len's of the maps match
        // debug_assert!(self.ordering.values().flatten().sum());
    }

    #[inline]
    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: ?Sized + Eq + Hash,
    {
        let value = self.map.remove(key)?;
        self.remove_inner(key, &value);
        Some(value)
    }

    /// Iterator over keys in order of their values.
    /// If there are multiple values with the same key, the keys are returned in insertion order.
    #[inline]
    pub fn keys(&self) -> impl Iterator<Item = K> + '_ {
        self.ordering.values().flatten().copied()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    fn remove_inner<Q>(&mut self, k: &Q, v: &V)
    where
        K: Borrow<Q>,
        Q: ?Sized + Eq + Hash,
    {
        let vs = self.ordering.get_mut(v).unwrap();
        assert!(vs.remove(k));
        vs.shrink_to_fit();
    }
}
