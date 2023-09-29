use std::fmt;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::LazyLock;

use anyhow::bail;
use itertools::Itertools;
use rkyv::{Archive, Deserialize, Serialize};

mod fold;
mod zip;

pub use self::fold::{TypeFold, TypeFolder};
pub use self::zip::{Zip, ZipError, ZipResult, Zipper};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash, Archive, Serialize, Deserialize)]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
pub enum LogicalType {
    Null,
    Byte,
    Bool,
    Int64,
    Float64,
    Decimal,
    Text,
    Oid,
    Bytea,
    Type,
    Expr,
    TupleExpr,
    Array(#[omit_bounds] Box<LogicalType>),
    /// pseudotype for function types
    Any,
}

impl FromStr for LogicalType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "byte" => Ok(Self::Byte),
            "bool" => Ok(Self::Bool),
            "int" => Ok(Self::Int64),
            "float" => Ok(Self::Float64),
            "text" => Ok(Self::Text),
            "oid" => Ok(Self::Oid),
            _ => bail!("unhandled value `{s}` in LogicalType::from_str"),
        }
    }
}

impl LogicalType {
    #[inline]
    pub fn array(inner: LogicalType) -> Self {
        LogicalType::Array(Box::new(inner))
    }

    // HACK to workaround null type equality for now
    #[inline]
    pub fn is_subtype_of(&self, supertype: &Self) -> bool {
        matches!(self, LogicalType::Null) || self == supertype
    }

    #[inline]
    pub fn common_supertype(&self, other: &Self) -> Option<Self> {
        match (self, other) {
            (LogicalType::Null, b) => Some(b.clone()),
            (a, LogicalType::Null) => Some(a.clone()),
            (a, b) if a == b => Some(a.clone()),
            _ => None,
        }
    }

    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

impl fmt::Display for LogicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalType::Bool => write!(f, "boolean"),
            LogicalType::Byte => write!(f, "byte"),
            LogicalType::Int64 => write!(f, "int"),
            LogicalType::Float64 => write!(f, "float"),
            LogicalType::Decimal => write!(f, "decimal"),
            LogicalType::Text => write!(f, "text"),
            LogicalType::Null => write!(f, "null"),
            LogicalType::Oid => write!(f, "oid"),
            LogicalType::Bytea => write!(f, "bytea"),
            LogicalType::Type => write!(f, "type"),
            LogicalType::Expr => write!(f, "expr"),
            LogicalType::TupleExpr => write!(f, "tuple"),
            LogicalType::Array(element) => write!(f, "[{element}]"),
            LogicalType::Any => write!(f, "any"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Schema {
    types: Box<[LogicalType]>,
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;
        self.types.iter().format(", ").fmt(f)?;
        write!(f, ")")
    }
}

impl IntoIterator for Schema {
    type Item = LogicalType;
    type IntoIter = std::vec::IntoIter<LogicalType>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.types.into_vec().into_iter()
    }
}

impl<'a> IntoIterator for &'a Schema {
    type Item = &'a LogicalType;
    type IntoIter = std::slice::Iter<'a, LogicalType>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.types.iter()
    }
}

impl Schema {
    #[inline]
    pub fn new(types: impl Into<Box<[LogicalType]>>) -> Self {
        Self { types: types.into() }
    }

    #[inline]
    pub fn empty_ref<'a>() -> &'a Self {
        static EMPTY: LazyLock<Schema> = LazyLock::new(|| Schema { types: Box::new([]) });
        &EMPTY
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
    pub fn is_subschema_of(&self, supertype: &[LogicalType]) -> bool {
        self.types.len() == supertype.len()
            && self.types.iter().zip(supertype.iter()).all(|(a, b)| a.is_subtype_of(b))
    }

    #[inline]
    pub fn into_inner(self) -> Box<[LogicalType]> {
        self.types
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
