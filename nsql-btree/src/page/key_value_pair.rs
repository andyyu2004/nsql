use std::cmp::Ordering;
use std::convert::Infallible;

use rkyv::{Archive, Deserialize, Serialize};

/// Essentially a `(K, V)` tuple but implements traits like `Ord` and `PartialOrd` based on the key only
// FIXME needs some work to make this work with unsized types
#[derive(Debug)]
#[repr(C)]
pub struct KeyValuePair<K, V> {
    pub key: K,
    pub value: V,
}

impl<'a, K: Archive, V: Archive> Archive for KeyValuePair<&'a K, &'a V> {
    type Archived = KeyValuePair<K::Archived, V::Archived>;
    type Resolver = (K::Resolver, V::Resolver);

    #[inline]
    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        let (fp, fo) = rkyv::out_field!(out.key);
        self.key.resolve(pos + fp, resolver.0, fo);
        let (fp, fo) = rkyv::out_field!(out.value);
        self.value.resolve(pos + fp, resolver.1, fo);
    }
}

impl<'a, S: rkyv::Fallible, K: Serialize<S>, V: Serialize<S>> Serialize<S>
    for KeyValuePair<&'a K, &'a V>
{
    fn serialize(
        &self,
        serializer: &mut S,
    ) -> Result<Self::Resolver, <S as rkyv::Fallible>::Error> {
        Ok((self.key.serialize(serializer)?, self.value.serialize(serializer)?))
    }
}

impl<K, V> Deserialize<KeyValuePair<K, V>, rkyv::Infallible>
    for KeyValuePair<K::Archived, V::Archived>
where
    K: Archive,
    V: Archive,
    K::Archived: Deserialize<K, rkyv::Infallible>,
    V::Archived: Deserialize<V, rkyv::Infallible>,
{
    fn deserialize(
        &self,
        deserializer: &mut rkyv::Infallible,
    ) -> Result<KeyValuePair<K, V>, Infallible> {
        Ok(KeyValuePair {
            key: self.key.deserialize(deserializer)?,
            value: self.value.deserialize(deserializer)?,
        })
    }
}

impl<K, V> KeyValuePair<K, V> {
    pub fn new(key: K, value: V) -> Self {
        Self { key, value }
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
