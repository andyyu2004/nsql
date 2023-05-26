use std::error::Error;
use std::fmt;
use std::marker::PhantomData;

use rust_decimal::prelude::ToPrimitive;
pub use rust_decimal::Decimal;

use crate::schema::LogicalType;

pub struct CastError<T> {
    value: Value,
    phantom: PhantomData<T>,
}

impl<T> CastError<T> {
    pub fn new(value: Value) -> Self {
        Self { value, phantom: PhantomData }
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
    Int(i32),
    Bool(bool),
    Decimal(Decimal),
    Text(String),
}

impl<'a> rkyv::Archive for &'a Value {
    type Archived = <Value as rkyv::Archive>::Archived;

    type Resolver = <Value as rkyv::Archive>::Resolver;

    #[inline]
    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        (**self).resolve(pos, resolver, out)
    }
}

impl<'a, S: rkyv::ser::Serializer> rkyv::Serialize<S> for &'a Value {
    #[inline]
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        (**self).serialize(serializer)
    }
}

impl Value {
    #[inline]
    pub fn cast<T: Cast>(self, default: T) -> Result<T, CastError<T>> {
        if self.is_null() {
            return Ok(default);
        }

        self.cast_non_null()
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    #[inline]
    pub fn cast_non_null<T: Cast>(self) -> Result<T, CastError<T>> {
        assert!(!self.is_null());
        T::cast(self)
    }

    #[inline]
    pub fn ty(&self) -> LogicalType {
        match self {
            Value::Null => LogicalType::Null,
            Value::Int(_) => LogicalType::Int,
            Value::Bool(_) => LogicalType::Bool,
            Value::Decimal(_) => LogicalType::Decimal,
            Value::Text(_) => LogicalType::Text,
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
            Value::Int(i) => write!(f, "{i}"),
        }
    }
}

// FIXME missing lots of implementations
pub trait Cast: private::Sealed + Sized {
    /// Cast a nsql `value` to a rust value.
    #[doc(hidden)]
    fn cast(value: Value) -> Result<Self, CastError<Self>>;
}

impl private::Sealed for u64 {}

impl Cast for u64 {
    fn cast(value: Value) -> Result<Self, CastError<Self>> {
        match value {
            Value::Bool(b) => Ok(b as u64),
            Value::Int(i) => Ok(i as u64),
            Value::Decimal(d) => d.to_u64().ok_or_else(|| CastError::new(value)),
            _ => Err(CastError { value, phantom: PhantomData }),
        }
    }
}

impl private::Sealed for bool {}

impl Cast for bool {
    fn cast(value: Value) -> Result<Self, CastError<Self>> {
        match value {
            Value::Bool(b) => Ok(b),
            _ => Err(CastError { value, phantom: PhantomData }),
        }
    }
}

mod private {
    pub trait Sealed {}
}
