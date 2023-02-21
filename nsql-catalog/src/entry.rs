use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use smol_str::SmolStr;

#[derive(Debug)]
pub struct Oid<T: ?Sized> {
    oid: u64,
    marker: PhantomData<fn() -> T>,
}

impl<T: ?Sized> Copy for Oid<T> {}

impl<T: ?Sized> Clone for Oid<T> {
    fn clone(&self) -> Self {
        Self { oid: self.oid, marker: self.marker }
    }
}

impl<T: ?Sized> PartialEq for Oid<T> {
    fn eq(&self, other: &Self) -> bool {
        self.oid == other.oid
    }
}

impl<T: ?Sized> Eq for Oid<T> {}

impl<T: ?Sized> Hash for Oid<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.oid.hash(state);
    }
}

impl<T> Oid<T> {
    pub(crate) fn new(oid: u64) -> Self {
        Self { oid, marker: PhantomData }
    }
}

/// Lowercase name of a catalog entry (for case insensitive lookup)
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EntryName(SmolStr);

impl EntryName {
    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl<S> From<S> for EntryName
where
    S: AsRef<str>,
{
    fn from(s: S) -> Self {
        Self(SmolStr::new(s.as_ref().to_lowercase()))
    }
}

impl Borrow<str> for EntryName {
    fn borrow(&self) -> &str {
        self.0.as_ref()
    }
}
