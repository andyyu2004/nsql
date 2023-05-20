use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

/// An opaque identifier for an entity in a catalog set.
/// This is only unique per catalog set. If you have multiple catalog sets containing the same
/// type, it is currently possible to misuse this type and read from the wrong set.
// This must only be constructed internally by the catalog.
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

impl<T: ?Sized> Oid<T> {
    pub(crate) fn new(oid: u64) -> Self {
        Self { oid, marker: PhantomData }
    }
}
