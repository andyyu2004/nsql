use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::str::FromStr;
use std::{cmp, fmt};

use rkyv::{Archive, Deserialize, Serialize};

/// An opaque identifier for an entity in a catalog set.
/// This is only unique per catalog set. If you have multiple catalog sets containing the same
/// type, it is currently possible to misuse this type and read from the wrong set.
// This must only be constructed internally by the catalog.
// FIXME move the typedoid to catalog crate but keep the untyped one here
#[derive(Archive, Serialize, Deserialize)]
// #[structural_match]
pub struct Oid<T: ?Sized> {
    oid: u64,
    marker: PhantomData<fn() -> T>,
}

impl<T: ?Sized> PartialEq for Oid<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.oid == other.oid
    }
}

impl<T: ?Sized> Eq for Oid<T> {}

impl<T: ?Sized> PartialOrd for Oid<T> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: ?Sized> Ord for Oid<T> {
    #[inline]
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.oid.cmp(&other.oid)
    }
}

impl<T: ?Sized> std::marker::StructuralEq for Oid<T> {}

impl<T> Oid<T> {
    pub const NULL: Self = Self::new(0);
}

pub type UntypedOid = Oid<()>;

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
        write!(f, "#{}", self.oid)
    }
}

impl<T: ?Sized> FromStr for Oid<T> {
    type Err = anyhow::Error;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            anyhow::bail!("invalid empty tuple index")
        }

        match s.split_at(1) {
            ("#", index) => Ok(Self::new(index.parse()?)),
            _ => anyhow::bail!("tuple index must be prefixed with `#`"),
        }
    }
}

impl<T: ?Sized> Copy for Oid<T> {}

impl<T: ?Sized> Clone for Oid<T> {
    fn clone(&self) -> Self {
        *self
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
