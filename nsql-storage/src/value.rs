use std::error::Error;
use std::fmt;
use std::marker::PhantomData;

use nsql_core::schema::LogicalType;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

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
            self.value.logical_type(),
            std::any::type_name::<T>()
        )
    }
}

impl<T> Error for CastError<T> {}

/// An nsql value
// Keep this in sync with `NonNullValue`
#[derive(Debug, Clone, PartialEq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Decimal(Decimal),
    Text(String),
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

    pub fn logical_type(&self) -> LogicalType {
        match self {
            // FIXME default null to have type int for now
            Value::Null => LogicalType::Int,
            Value::Bool(_) => LogicalType::Bool,
            Value::Decimal(_) => LogicalType::Decimal,
            Value::Text(_) => LogicalType::Text,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Decimal(d) => write!(f, "{d}"),
            Value::Text(s) => write!(f, "{s}"),
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
