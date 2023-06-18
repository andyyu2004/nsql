use std::error::Error;
use std::marker::PhantomData;
use std::{cmp, fmt};

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
// FIXME write a custom archive impl that has a schema in scope to avoid needing to archive a disriminant
#[derive(Debug, Clone, PartialEq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[archive_attr(derive(Debug))]
pub enum Value {
    Null,
    Int32(i32),
    Oid(UntypedOid),
    Bool(bool),
    Decimal(Decimal),
    Text(String),
    Bytea(Box<[u8]>),
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match (self, other) {
            (Value::Null, Value::Null) => todo!(),
            (Value::Null, _) => todo!(),
            (_, Value::Null) => todo!(),
            (Value::Int32(a), Value::Int32(b)) => a.partial_cmp(b),
            (Value::Oid(a), Value::Oid(b)) => a.partial_cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
            (Value::Decimal(a), Value::Decimal(b)) => a.partial_cmp(b),
            (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
            (Value::Bytea(a), Value::Bytea(b)) => a.partial_cmp(b),
            (a, b) => panic!("cannot compare values {:?} and {:?}", a, b),
        }
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

pub trait IntoValue {
    fn into_value(self) -> Value;
}
