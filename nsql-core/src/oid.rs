use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use rkyv::{Archive, Deserialize, Serialize};

/// An opaque identifier for an entity in a catalog set.
/// This is only unique per catalog set. If you have multiple catalog sets containing the same
/// type, it is currently possible to misuse this type and read from the wrong set.
// This must only be constructed internally by the catalog.
// FIXME move the typedoid to catalog crate but keep the untyped one here
#[derive(Archive, Serialize, Deserialize)]
pub struct Oid<T: ?Sized> {
    oid: u64,
    marker: PhantomData<fn() -> T>,
}

pub type UntypedOid = Oid<()>;

impl<T> PartialEq for Oid<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.oid == other.oid
    }
}

impl<T> Eq for Oid<T> {}

impl<T> PartialOrd for Oid<T> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.oid.partial_cmp(&other.oid)
    }
}

impl<T> Ord for Oid<T> {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.oid.cmp(&other.oid)
    }
}

impl<T: ?Sized> fmt::Debug for Oid<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Oid<{}>({})", std::any::type_name::<T>(), self.oid)
    }
}

impl<T: ?Sized> fmt::Debug for ArchivedOid<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Oid<{}>({})", std::any::type_name::<T>(), self.oid)
    }
}

impl<T: ?Sized> fmt::Display for Oid<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.oid)
    }
}

impl<T: ?Sized> Copy for Oid<T> {}

impl<T: ?Sized> Clone for Oid<T> {
    fn clone(&self) -> Self {
        Self { oid: self.oid, marker: self.marker }
    }
}

impl<T: ?Sized> Hash for Oid<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.oid.hash(state);
    }
}

impl<T: ?Sized> Oid<T> {
    pub const fn new(oid: u64) -> Self {
        Self { oid, marker: PhantomData }
    }

    pub const fn untyped(self) -> UntypedOid {
        Oid::new(self.oid)
    }
}

impl UntypedOid {
    pub fn cast<T: ?Sized>(self) -> Oid<T> {
        Oid::new(self.oid)
    }
}
