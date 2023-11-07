#![deny(rust_2018_idioms)]
#![feature(lazy_cell)]

mod oid;
pub mod ty;

use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;

pub use anyhow;
pub use oid::{Oid, UntypedOid};
pub use smol_str::SmolStr;
pub use ty::{LogicalType, Schema};

pub trait Profiler {
    type EventId;

    /// A convenient way to get a profiler guard that will profile some experimental bit of code.
    fn debug_event_id(&self) -> Self::EventId;

    fn profile<R>(&self, event_id: Self::EventId, f: impl FnOnce() -> R) -> R;
}

/// Lowercase name of a catalog entry (for case insensitive lookup)
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Name {
    name: SmolStr,
}

impl FromStr for Name {
    type Err = std::convert::Infallible;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(s.into())
    }
}

impl PartialEq<&str> for Name {
    #[inline]
    fn eq(&self, other: &&str) -> bool {
        self.name.eq_ignore_ascii_case(other)
    }
}

impl rkyv::Archive for Name {
    type Archived = rkyv::string::ArchivedString;
    type Resolver = rkyv::string::StringResolver;

    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        self.name.to_string().resolve(pos, resolver, out);
    }
}

impl Name {
    /// The string is expected to be lowercase
    #[inline]
    pub const fn new_inline(s: &str) -> Self {
        Self { name: SmolStr::new_inline(s) }
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        self.name.as_str()
    }

    #[inline]
    pub fn into_inner(self) -> SmolStr {
        self.name
    }
}

impl From<Name> for String {
    #[inline]
    fn from(value: Name) -> Self {
        value.name.into()
    }
}

impl fmt::Debug for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl fmt::Display for Name {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.name.fmt(f)
    }
}

impl Deref for Name {
    type Target = str;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.name.deref()
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

#[macro_export]
macro_rules! not_implemented {
    ($msg:literal) => {
        $crate::anyhow::bail!("not implemented: {}", $msg)
    };
}

#[macro_export]
macro_rules! not_implemented_if {
    ($cond:expr) => {
        if $cond {
            $crate::anyhow::bail!("not implemented: {}", stringify!($cond))
        }
    };
}
