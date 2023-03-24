use std::cmp::Ordering;

use rkyv::{Archive, Serialize};

/// Essentially a `(K, V)` tuple but implements traits like `Ord` and `PartialOrd` based on the key only
// FIXME needs some work to make this work with unsized types
#[derive(Debug)]
#[repr(C)]
pub(crate) struct Entry<K, V> {
    pub key: K,
    pub value: V,
}

impl<'a, K: Archive, V: Archive> Archive for Entry<&'a K, &'a V> {
    type Archived = Entry<K::Archived, V::Archived>;
    type Resolver = (K::Resolver, V::Resolver);

    #[inline]
    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        let (fp, fo) = rkyv::out_field!(out.key);
        self.key.resolve(pos + fp, resolver.0, fo);
        let (fp, fo) = rkyv::out_field!(out.value);
        self.value.resolve(pos + fp, resolver.1, fo);
    }
}

impl<'a, S: rkyv::Fallible, K: Serialize<S>, V: Serialize<S>> Serialize<S> for Entry<&'a K, &'a V> {
    fn serialize(
        &self,
        serializer: &mut S,
    ) -> Result<Self::Resolver, <S as rkyv::Fallible>::Error> {
        Ok((self.key.serialize(serializer)?, self.value.serialize(serializer)?))
    }
}

impl<K, V> PartialEq for Entry<K, V>
where
    K: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl<K, V> Eq for Entry<K, V> where K: Eq {}

impl<K, V> PartialOrd for Entry<K, V>
where
    K: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.key.cmp(&other.key))
    }
}

impl<K, V> Ord for Entry<K, V>
where
    K: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}
