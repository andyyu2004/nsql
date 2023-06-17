#![deny(rust_2018_idioms)]

mod oid;
pub mod schema;

use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;

pub use oid::{Oid, UntypedOid};
use smol_str::SmolStr;

/// Lowercase name of a catalog entry (for case insensitive lookup)
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Name {
    name: SmolStr,
}

impl rkyv::Archive for Name {
    type Archived = rkyv::string::ArchivedString;
    type Resolver = rkyv::string::StringResolver;

    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        self.name.to_string().resolve(pos, resolver, out);
    }
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

#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub enum LogicalType {
    Null,
    Bool,
    Int,
    Decimal,
    Text,
    Oid,
    Bytea,
}

impl LogicalType {
    pub fn physical_type(&self) -> PhysicalType {
        match self {
            LogicalType::Bool => PhysicalType::Bool,
            LogicalType::Int => PhysicalType::Int32,
            LogicalType::Decimal => PhysicalType::Decimal,
            LogicalType::Text => todo!(),
            LogicalType::Null => todo!(),
            LogicalType::Oid => todo!(),
            LogicalType::Bytea => todo!(),
        }
    }
}

impl fmt::Display for LogicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalType::Bool => write!(f, "boolean"),
            LogicalType::Int => write!(f, "int"),
            LogicalType::Decimal => write!(f, "decimal"),
            LogicalType::Text => write!(f, "text"),
            LogicalType::Null => write!(f, "null"),
            LogicalType::Oid => write!(f, "oid"),
            LogicalType::Bytea => write!(f, "bytea"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalType {
    /// 8-bit boolean
    Bool,
    /// 32-bit signed integer
    Int32,
    /// 128-bit fixed-size decimal
    Decimal,
}
