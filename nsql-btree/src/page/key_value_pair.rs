use std::cmp::Ordering;
use std::fmt;

use rkyv::Archive;

/// Essentially a `(K, V)` tuple but implements traits like `Ord` and `PartialOrd` based on the key only
// FIXME needs some work to make this work with unsized types
#[derive(Debug, Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct KeyValuePair<K, V> {
    pub key: K,
    pub value: V,
}

impl<K, V> KeyValuePair<K, V> {
    pub fn new(key: K, value: V) -> Self {
        Self { key, value }
    }
}

impl<K: Archive, V: Archive> ArchivedKeyValuePair<K, V> {
    pub fn new(key: K::Archived, value: V::Archived) -> Self {
        Self { key, value }
    }
}

pub trait KeyOrd {
    type Key: Ord;
    fn key_cmp(&self, key: &Self::Key) -> Ordering;
}

impl<K, V> KeyOrd for ArchivedKeyValuePair<K, V>
where
    K: Archive,
    K::Archived: Ord,
    V: Archive,
{
    type Key = K::Archived;

    fn key_cmp(&self, key: &Self::Key) -> Ordering {
        self.key.cmp(key)
    }
}

impl<K, V> PartialEq for KeyValuePair<K, V>
where
    K: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl<K, V> Eq for KeyValuePair<K, V> where K: Eq {}

impl<K, V> PartialOrd for KeyValuePair<K, V>
where
    K: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.key.cmp(&other.key))
    }
}

impl<K, V> Ord for KeyValuePair<K, V>
where
    K: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl<K, V> Eq for ArchivedKeyValuePair<K, V>
where
    K: Archive,
    K::Archived: Eq,
    V: Archive,
{
}

impl<K, V> PartialEq for ArchivedKeyValuePair<K, V>
where
    K: Archive,
    K::Archived: PartialEq,
    V: Archive,
{
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl<K, V> PartialOrd for ArchivedKeyValuePair<K, V>
where
    K: Archive,
    K::Archived: Ord,
    V: Archive,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.key.cmp(&other.key))
    }
}

impl<K, V> Ord for ArchivedKeyValuePair<K, V>
where
    K: Archive,
    K::Archived: Ord,
    V: Archive,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl<K, V> fmt::Debug for ArchivedKeyValuePair<K, V>
where
    K: Archive,
    K::Archived: fmt::Debug,
    V: Archive,
    V::Archived: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyValuePair").field("key", &self.key).field("value", &self.value).finish()
    }
}
