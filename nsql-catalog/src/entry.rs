use std::borrow::Borrow;

use smol_str::SmolStr;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Oid(u64);

impl Oid {
    pub(crate) fn new(id: u64) -> Self {
        Self(id)
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
