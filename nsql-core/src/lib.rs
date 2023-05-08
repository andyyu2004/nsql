#![deny(rust_2018_idioms)]
#![feature(async_fn_in_trait)]
#![allow(incomplete_features)]

pub mod schema;
pub mod value;

use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;

use nsql_serde::{StreamDeserialize, StreamSerialize};
use smol_str::SmolStr;

/// Lowercase name of a catalog entry (for case insensitive lookup)
#[derive(Clone, Debug, PartialEq, Eq, Hash, StreamSerialize, StreamDeserialize)]
pub struct Name {
    name: SmolStr,
}

impl Name {
    #[inline]
    pub fn as_str(&self) -> &str {
        self.name.as_str()
    }
}

impl From<Name> for String {
    fn from(value: Name) -> Self {
        value.name.into()
    }
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.name.fmt(f)
    }
}

impl Deref for Name {
    type Target = str;

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
