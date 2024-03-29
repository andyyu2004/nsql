use std::error::Error;
use std::ops::Deref;
use std::str::FromStr;
use std::{fmt, mem};

use itertools::Itertools;
use nsql_core::{LogicalType, Name, Oid, SmolStr, UntypedOid};
use nsql_util::static_assert_eq;
use rust_decimal::prelude::ToPrimitive;
pub use rust_decimal::Decimal;

use crate::expr::{Expr, TupleExpr};

pub struct CastError {
    value: Value,
    to: LogicalType,
}

impl CastError {
    pub fn new(value: Value, to: LogicalType) -> Self {
        Self { value, to }
    }
}

impl fmt::Debug for CastError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl fmt::Display for CastError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cannot cast value `{}` to type `{}`", self.value, self.to)
    }
}

impl Error for CastError {}

// Avoid accidental size growth
static_assert_eq!(mem::size_of::<Value>(), 24);

/// An nsql value
#[derive(
    Debug,
    Default,
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
    Byte(u8),
    Int64(i64),
    Float64(u64),
    Oid(UntypedOid),
    Bool(bool),
    Decimal(Decimal),
    Text(#[omit_bounds] Text),
    Bytea(Bytea),
    Array(#[omit_bounds] Box<[Value]>),
    // experiment adding this as a value for serialiazation purposes
    Type(LogicalType),
    Expr(#[omit_bounds] Expr),
    TupleExpr(TupleExpr),
    #[default]
    Null, // put this variant here so NULL sorts last
}

// manual implementation of clone as we get more control:
// - can use * instead of .clone when we know better
// - can control inlining
impl Clone for Value {
    #[inline]
    fn clone(&self) -> Self {
        match self {
            Value::Byte(b) => Value::Byte(*b),
            Value::Int64(i) => Value::Int64(*i),
            Value::Float64(f) => Value::Float64(*f),
            Value::Oid(o) => Value::Oid(*o),
            Value::Bool(b) => Value::Bool(*b),
            Value::Decimal(d) => Value::Decimal(*d),
            Value::Text(t) => Value::Text(t.clone()),
            Value::Bytea(b) => Value::Bytea(b.clone()),
            Value::Array(a) => Value::Array(a.clone()),
            Value::Type(ty) => Value::Type(ty.clone()),
            Value::Expr(expr) => Value::Expr(expr.clone()),
            Value::TupleExpr(expr) => Value::TupleExpr(expr.clone()),
            Value::Null => Value::Null,
        }
    }
}

/// very limited implementation of `FromStr` for literal values (used for egg)
impl FromStr for Value {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::Text(s.into()))
    }
}

#[derive(
    Debug,
    Default,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    // rkyv::Archive,
    // rkyv::Serialize,
    // rkyv::Deserialize,
)]
pub struct Text(SmolStr);

impl Deref for Text {
    type Target = str;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.as_str()
    }
}

impl Text {
    #[inline]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    #[inline]
    pub fn into_inner(self) -> SmolStr {
        self.0
    }
}

impl From<SmolStr> for Text {
    #[inline]
    fn from(s: SmolStr) -> Self {
        Self(s)
    }
}

impl From<&str> for Text {
    #[inline]
    fn from(s: &str) -> Self {
        Self(s.into())
    }
}

impl From<Name> for Text {
    #[inline]
    fn from(name: Name) -> Self {
        Self(name.into_inner())
    }
}

impl fmt::Display for Text {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.as_str())
    }
}

impl rkyv::Archive for Text {
    type Archived = <String as rkyv::Archive>::Archived;

    type Resolver = <String as rkyv::Archive>::Resolver;

    #[inline]
    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        rkyv::string::ArchivedString::resolve_from_str(self.as_str(), pos, resolver, out);
    }
}

impl<S: rkyv::Fallible + ?Sized> rkyv::Serialize<S> for Text
where
    str: rkyv::SerializeUnsized<S>,
{
    #[inline]
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        rkyv::string::ArchivedString::serialize_from_str(self.as_str(), serializer)
    }
}

impl<D: rkyv::Fallible + ?Sized> rkyv::Deserialize<Text, D> for rkyv::string::ArchivedString
where
    str: rkyv::DeserializeUnsized<str, D>,
{
    #[inline]
    fn deserialize(&self, _deserializer: &mut D) -> Result<Text, D::Error> {
        Ok(self.as_str().into())
    }
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct Bytea(Box<[u8]>);

impl<B> From<B> for Bytea
where
    B: AsRef<[u8]>,
{
    #[inline]
    fn from(value: B) -> Self {
        Self(value.as_ref().into())
    }
}

impl Deref for Bytea {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<V> From<Vec<V>> for Value
where
    V: Into<Value>,
{
    #[inline]
    fn from(vs: Vec<V>) -> Self {
        Self::Array(vs.into_iter().map(Into::into).collect())
    }
}

impl<V> From<Box<[V]>> for Value
where
    Value: From<V>,
{
    #[inline]
    fn from(vs: Box<[V]>) -> Self {
        vs.into_vec().into()
    }
}

impl<V> From<Option<V>> for Value
where
    Value: From<V>,
{
    #[inline]
    fn from(v: Option<V>) -> Self {
        match v {
            Some(v) => v.into(),
            None => Value::Null,
        }
    }
}

impl From<f64> for Value {
    #[inline]
    fn from(f: f64) -> Self {
        Self::Float64(f.to_bits())
    }
}

impl From<Decimal> for Value {
    #[inline]
    fn from(dec: Decimal) -> Self {
        Self::Decimal(dec)
    }
}

impl From<LogicalType> for Value {
    #[inline]
    fn from(v: LogicalType) -> Self {
        Self::Type(v)
    }
}

impl From<Box<[u8]>> for Value {
    #[inline]
    fn from(v: Box<[u8]>) -> Self {
        Self::Bytea(Bytea(v))
    }
}

impl From<Name> for Value {
    #[inline]
    fn from(v: Name) -> Self {
        Self::Text(v.into_inner().into())
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
        Self::Int64(v as i64)
    }
}

impl From<i64> for Value {
    #[inline]
    fn from(v: i64) -> Self {
        Self::Int64(v)
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
    pub fn cast<T: FromValue>(self) -> Result<T, CastError> {
        T::from_value(self)
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    #[inline]
    pub fn is_not_null(&self) -> bool {
        !self.is_null()
    }

    /// Sanity check routine to ensure we haven't messed up the types to badly.
    /// We should never be comparing `INT` with `TEXT` directly for example.
    #[inline]
    pub fn is_compat_with(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => true,
            (Value::Array(xs), Value::Array(ys)) if xs.is_empty() && ys.is_empty() => true,
            (Value::Array(xs), Value::Array(ys)) if !xs.is_empty() && !ys.is_empty() => {
                xs[0].is_compat_with(&ys[0])
            }
            _ => mem::discriminant(self) == mem::discriminant(other),
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
            // Value::Text(s) => write!(f, "'{s}'"),
            // should probably be quoted, need to fix tests
            Value::Text(s) => write!(f, "{s}"),
            Value::Int64(i) => write!(f, "{i}"),
            Value::Oid(oid) => write!(f, "{oid}"),
            Value::Byte(byte) => write!(f, "{byte:x}"),
            Value::Bytea(bytes) => write!(f, "{bytes:x?}"),
            Value::Array(values) => write!(f, "[{}]", values.iter().format(", ")),
            Value::Type(ty) => write!(f, "{ty}"),
            Value::Expr(_expr) => write!(f, "<expr>"),
            Value::TupleExpr(_expr) => write!(f, "<tuple-expr>"),
            Value::Float64(d) => write!(f, "{}", f64::from_bits(*d)),
        }
    }
}

// FIXME missing lots of implementations
pub trait FromValue: Sized {
    /// Cast a nsql `value` to a rust value.
    fn from_value(value: Value) -> Result<Self, CastError>;
}

impl FromValue for Value {
    #[inline]
    fn from_value(value: Self) -> Result<Self, CastError> {
        Ok(value)
    }
}

impl<V: FromValue> FromValue for Option<V> {
    #[inline]
    fn from_value(value: Value) -> Result<Self, CastError> {
        match value {
            Value::Null => Ok(None),
            value => V::from_value(value).map(Some),
        }
    }
}

impl<V: FromValue> FromValue for Box<[V]> {
    #[inline]
    fn from_value(value: Value) -> Result<Self, CastError> {
        Vec::<V>::from_value(value).map(|v| v.into_boxed_slice())
    }
}

impl<V: FromValue> FromValue for Vec<V> {
    #[inline]
    fn from_value(value: Value) -> Result<Self, CastError> {
        match value {
            Value::Array(values) => {
                values.into_vec().into_iter().map(|value| V::from_value(value)).collect()
            }
            _ => Err(CastError::new(value, LogicalType::array(LogicalType::Any))),
        }
    }
}

impl FromValue for u64 {
    #[inline]
    fn from_value(value: Value) -> Result<Self, CastError> {
        match value {
            Value::Bool(b) => Ok(u64::from(b)),
            Value::Int64(i) => {
                if i.is_negative() {
                    Err(CastError::new(value, LogicalType::Int64))
                } else {
                    Ok(i as u64)
                }
            }
            Value::Decimal(d) => {
                d.to_u64().ok_or_else(|| CastError::new(value, LogicalType::Int64))
            }
            _ => Err(CastError::new(value, LogicalType::Int64)),
        }
    }
}

impl<T> FromValue for Oid<T> {
    #[inline]
    fn from_value(value: Value) -> Result<Self, CastError> {
        match value {
            Value::Oid(oid) => Ok(oid.cast()),
            Value::Int64(i) => {
                if i < 0 {
                    Err(CastError::new(value, LogicalType::Oid))
                } else {
                    Ok(Oid::new(i as u64))
                }
            }
            _ => Err(CastError::new(value, LogicalType::Oid)),
        }
    }
}

impl FromValue for bool {
    #[inline]
    fn from_value(value: Value) -> Result<Self, CastError> {
        match value {
            Value::Bool(b) => Ok(b),
            _ => Err(CastError::new(value, LogicalType::Bool)),
        }
    }
}

impl FromValue for f64 {
    #[inline]
    fn from_value(value: Value) -> Result<Self, CastError> {
        match value {
            Value::Float64(u) => Ok(f64::from_bits(u)),
            Value::Int64(i) => Ok(i as f64),
            _ => Err(CastError::new(value, LogicalType::Float64)),
        }
    }
}

impl FromValue for Decimal {
    #[inline]
    fn from_value(value: Value) -> Result<Self, CastError> {
        match value {
            Value::Decimal(d) => Ok(d),
            Value::Int64(i) => Ok(Decimal::from(i)),
            _ => Err(CastError::new(value, LogicalType::Decimal)),
        }
    }
}

impl FromValue for i64 {
    #[inline]
    fn from_value(value: Value) -> Result<Self, CastError> {
        match value {
            Value::Int64(i) => Ok(i),
            _ => Err(CastError::new(value, LogicalType::Int64)),
        }
    }
}

impl FromValue for i32 {
    #[inline]
    fn from_value(value: Value) -> Result<Self, CastError> {
        match value {
            Value::Int64(i) => Ok(i as i32),
            _ => Err(CastError::new(value, LogicalType::Int64)),
        }
    }
}

impl FromValue for i8 {
    #[inline]
    fn from_value(value: Value) -> Result<Self, CastError> {
        match value {
            Value::Byte(u) => Ok(u as i8),
            Value::Int64(i) => Ok(i as i8),
            _ => Err(CastError::new(value, LogicalType::Byte)),
        }
    }
}

impl FromValue for u8 {
    #[inline]
    fn from_value(value: Value) -> Result<Self, CastError> {
        match value {
            Value::Byte(u) => Ok(u),
            Value::Int64(i) => Ok(i as u8),
            _ => Err(CastError::new(value, LogicalType::Byte)),
        }
    }
}

impl FromValue for Text {
    #[inline]
    fn from_value(value: Value) -> Result<Self, CastError> {
        match value {
            Value::Text(s) => Ok(s),
            _ => Err(CastError::new(value, LogicalType::Text)),
        }
    }
}

impl FromValue for Bytea {
    #[inline]
    fn from_value(value: Value) -> Result<Self, CastError> {
        match value {
            Value::Bytea(b) => Ok(b),
            _ => Err(CastError::new(value, LogicalType::Bytea)),
        }
    }
}

impl FromValue for Name {
    #[inline]
    fn from_value(value: Value) -> Result<Self, CastError> {
        value.cast::<Text>().map(Text::into_inner).map(Into::into)
    }
}
