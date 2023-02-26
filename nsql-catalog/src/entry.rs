use std::borrow::Borrow;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use nsql_serde::{Deserialize, Serialize};
use smol_str::SmolStr;

pub struct Oid<T: ?Sized> {
    oid: u64,
    marker: PhantomData<fn() -> T>,
}

impl<T: ?Sized> fmt::Debug for Oid<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Oid<{}>({})", std::any::type_name::<T>(), self.oid)
    }
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
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Name {
    name: SmolStr,
}

impl Name {
    #[inline]
    pub fn as_str(&self) -> &str {
        self.name.as_str()
    }
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.name.fmt(f)
    }
}

impl<S> From<S> for Name
where
    S: AsRef<str>,
{
    fn from(s: S) -> Self {
        Self { name: SmolStr::new(s.as_ref().to_lowercase()) }
    }
}

impl Borrow<str> for Name {
    fn borrow(&self) -> &str {
        self.name.as_ref()
    }
}
