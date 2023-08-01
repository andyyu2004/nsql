#![deny(rust_2018_idioms)]

mod oid;

use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;

pub use oid::{Oid, UntypedOid};
use rkyv::{Archive, Deserialize, Serialize};
use smol_str::SmolStr;

/// Lowercase name of a catalog entry (for case insensitive lookup)
#[derive(Clone, PartialEq, Eq, Hash)]
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
    /// The string is expected to be lowercase
    #[inline]
    pub const fn new_inline(s: &str) -> Self {
        Self { name: SmolStr::new_inline(s) }
    }

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

impl fmt::Debug for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
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

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash, Archive, Serialize, Deserialize)]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
pub enum LogicalType {
    Null,
    Byte,
    Bool,
    Int32,
    Decimal,
    Text,
    Oid,
    Bytea,
    Type,
    TupleExpr,
    Array(#[omit_bounds] Box<LogicalType>),
}

impl LogicalType {
    #[inline]
    pub fn array(inner: LogicalType) -> Self {
        LogicalType::Array(Box::new(inner))
    }

    // HACK to workaround null type equality for now
    #[inline]
    pub fn is_subtype_of(&self, supertype: &Self) -> bool {
        if matches!(self, LogicalType::Null) { true } else { self == supertype }
    }
}

impl fmt::Display for LogicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalType::Bool => write!(f, "boolean"),
            LogicalType::Byte => write!(f, "byte"),
            LogicalType::Int32 => write!(f, "int"),
            LogicalType::Decimal => write!(f, "decimal"),
            LogicalType::Text => write!(f, "text"),
            LogicalType::Null => write!(f, "null"),
            LogicalType::Oid => write!(f, "oid"),
            LogicalType::Bytea => write!(f, "bytea"),
            LogicalType::Type => write!(f, "type"),
            LogicalType::TupleExpr => write!(f, "tuple"),
            LogicalType::Array(element) => write!(f, "[{element}]"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    types: Box<[LogicalType]>,
}

impl Schema {
    #[inline]
    pub fn new(types: impl Into<Box<[LogicalType]>>) -> Self {
        Self { types: types.into() }
    }

    #[inline]
    pub fn empty() -> Self {
        Self::new([])
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.types.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.types.len()
    }

    #[inline]
    pub fn types(&self) -> &[LogicalType] {
        &self.types
    }

    #[inline]
    pub fn is_subtype_of(&self, supertype: &[LogicalType]) -> bool {
        self.types.len() == supertype.len()
            && self.types.iter().zip(supertype.iter()).all(|(a, b)| a.is_subtype_of(b))
    }
}

impl Deref for Schema {
    type Target = [LogicalType];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.types
    }
}

impl FromIterator<LogicalType> for Schema {
    #[inline]
    fn from_iter<T: IntoIterator<Item = LogicalType>>(iter: T) -> Self {
        Self { types: iter.into_iter().collect::<Vec<_>>().into_boxed_slice() }
    }
}
