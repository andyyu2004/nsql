use std::error::Error;
use std::fmt;
use std::marker::PhantomData;

use itertools::Itertools;
use nsql_core::{LogicalType, Name, Oid, UntypedOid};
use rust_decimal::prelude::ToPrimitive;
pub use rust_decimal::Decimal;

pub struct CastError<T> {
    value: Value,
    phantom: PhantomData<fn() -> T>,
}

impl<T> CastError<T> {
    pub fn new(value: Value) -> Self {
        Self { value, phantom: PhantomData }
    }

    pub fn cast<U>(self) -> CastError<U> {
        CastError::new(self.value)
    }
}

impl<T> fmt::Debug for CastError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl<T> fmt::Display for CastError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cannot cast value {:?} of type {} to {}",
            self.value,
            self.value.ty(),
            std::any::type_name::<T>()
        )
    }
}

impl<T> Error for CastError<T> {}

/// An nsql value
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    // deriving this as implementing manually is a pain,
    // but different variants should not be compared with each other (other than `Null` which is < all other values)
    PartialOrd,
    Ord,
    Hash,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[omit_bounds]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
pub enum Value {
    Null,
    Int32(i32),
    Oid(UntypedOid),
    Bool(bool),
    Decimal(Decimal),
    Text(String),
    Bytea(Box<[u8]>),

    Array(#[omit_bounds] Vec<Value>),
}

impl From<Box<[u8]>> for Value {
    #[inline]
    fn from(v: Box<[u8]>) -> Self {
        Self::Bytea(v)
    }
}

impl From<String> for Value {
    #[inline]
    fn from(v: String) -> Self {
        Self::Text(v)
    }
}

impl From<Name> for Value {
    #[inline]
    fn from(v: Name) -> Self {
        Self::Text(v.into())
    }
}

impl From<bool> for Value {
    #[inline]
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

impl From<i32> for Value {
    #[inline]
    fn from(v: i32) -> Self {
        Self::Int32(v)
    }
}

impl<T> From<Oid<T>> for Value {
    #[inline]
    fn from(v: Oid<T>) -> Self {
        Self::Oid(v.untyped())
    }
}

impl<'a> rkyv::Archive for &'a Value {
    type Archived = <Value as rkyv::Archive>::Archived;

    type Resolver = <Value as rkyv::Archive>::Resolver;

    #[inline]
    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        (**self).resolve(pos, resolver, out)
    }
}

impl<'a, S: rkyv::ser::Serializer + rkyv::ser::ScratchSpace> rkyv::Serialize<S> for &'a Value {
    #[inline]
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        (**self).serialize(serializer)
    }
}

impl Value {
    #[inline]
    pub fn take(&mut self) -> Self {
        std::mem::replace(self, Value::Null)
    }

    #[inline]
    pub fn cast<T: FromValue>(self) -> Result<Option<T>, CastError<T>> {
        if self.is_null() {
            return Ok(None);
        }

        self.cast_non_null().map(Some)
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    #[inline]
    pub fn cast_non_null<T: FromValue>(self) -> Result<T, CastError<T>> {
        if self.is_null() {
            return Err(CastError::new(self));
        }

        T::from_value(self)
    }

    #[inline]
    pub fn ty(&self) -> LogicalType {
        match self {
            Value::Null => LogicalType::Null,
            Value::Int32(_) => LogicalType::Int,
            Value::Bool(_) => LogicalType::Bool,
            Value::Decimal(_) => LogicalType::Decimal,
            Value::Text(_) => LogicalType::Text,
            Value::Oid(_) => LogicalType::Oid,
            Value::Bytea(_) => LogicalType::Bytea,
            // Keep this in sync with binder logic
            Value::Array(values) => match &values[..] {
                [] => LogicalType::Int,
                [first, ..] => first.ty(),
            },
        }
    }
}

impl fmt::Display for Value {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Decimal(d) => write!(f, "{d}"),
            Value::Text(s) => write!(f, "{s}"),
            Value::Int32(i) => write!(f, "{i}"),
            Value::Oid(oid) => write!(f, "{oid}"),
            Value::Bytea(bytes) => write!(f, "{bytes:x?}"),
            Value::Array(values) => write!(f, "[{}]", values.iter().format(", ")),
        }
    }
}

// FIXME missing lots of implementations
pub trait FromValue: Sized {
    /// Cast a nsql `value` to a rust value.
    fn from_value(value: Value) -> Result<Self, CastError<Self>>;
}

impl FromValue for u64 {
    fn from_value(value: Value) -> Result<Self, CastError<Self>> {
        match value {
            Value::Bool(b) => Ok(b as u64),
            Value::Int32(i) => Ok(i as u64),
            Value::Decimal(d) => d.to_u64().ok_or_else(|| CastError::new(value)),
            _ => Err(CastError { value, phantom: PhantomData }),
        }
    }
}

impl<T> FromValue for Oid<T> {
    fn from_value(value: Value) -> Result<Self, CastError<Self>> {
        match value {
            Value::Oid(oid) => Ok(oid.cast()),
            _ => Err(CastError { value, phantom: PhantomData }),
        }
    }
}

impl FromValue for bool {
    fn from_value(value: Value) -> Result<Self, CastError<Self>> {
        match value {
            Value::Bool(b) => Ok(b),
            _ => Err(CastError { value, phantom: PhantomData }),
        }
    }
}

impl FromValue for i8 {
    fn from_value(value: Value) -> Result<Self, CastError<Self>> {
        match value {
            Value::Int32(i) => Ok(i as i8),
            _ => Err(CastError { value, phantom: PhantomData }),
        }
    }
}

impl FromValue for u8 {
    fn from_value(value: Value) -> Result<Self, CastError<Self>> {
        match value {
            Value::Int32(i) => Ok(i as u8),
            _ => Err(CastError { value, phantom: PhantomData }),
        }
    }
}

impl FromValue for String {
    fn from_value(value: Value) -> Result<Self, CastError<Self>> {
        match value {
            Value::Text(s) => Ok(s),
            _ => Err(CastError { value, phantom: PhantomData }),
        }
    }
}

impl FromValue for Box<[u8]> {
    fn from_value(value: Value) -> Result<Self, CastError<Self>> {
        match value {
            Value::Bytea(b) => Ok(b),
            _ => Err(CastError { value, phantom: PhantomData }),
        }
    }
}

impl FromValue for Name {
    fn from_value(value: Value) -> Result<Self, CastError<Self>> {
        match value {
            Value::Text(s) => Ok(s.into()),
            _ => Err(CastError { value, phantom: PhantomData }),
        }
    }
}
