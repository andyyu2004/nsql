use std::error::Error;
use std::fmt;
use std::hash::Hash;
use std::ops::{Add, Index, IndexMut};
use std::str::FromStr;

use anyhow::bail;
use itertools::Itertools;
use rkyv::with::RefAsBox;
use rkyv::{Archive, Archived, Deserialize, Serialize};

use crate::value::{CastError, FromValue, Value};

// FIXME make this cheap to clone
#[derive(
    Clone, PartialOrd, Ord, PartialEq, Eq, Hash, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct Tuple {
    values: Box<[Value]>,
}

impl fmt::Debug for Tuple {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({})", self.values.iter().format(", "))
    }
}

impl IntoIterator for Tuple {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.values.into_vec().into_iter()
    }
}

#[derive(Debug, Clone, PartialEq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[repr(transparent)]
pub struct TupleRef<'a>(#[with(RefAsBox)] &'a [Value]);

impl Tuple {
    #[inline]
    pub fn new(values: impl Into<Box<[Value]>>) -> Self {
        Self { values: values.into() }
    }

    #[inline]
    pub fn width(&self) -> usize {
        self.values.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.width() == 0
    }

    #[inline]
    pub fn empty() -> Self {
        Self::new([])
    }

    #[inline]
    pub fn values(&self) -> impl Iterator<Item = &Value> {
        self.values.iter()
    }

    #[inline]
    pub fn into_values(self) -> Box<[Value]> {
        self.values
    }

    #[inline]
    pub fn join(self, other: &Self) -> Self {
        let mut values = self.values.into_vec();
        values.reserve_exact(other.values.len());
        values.extend_from_slice(&other.values);
        Self::new(values.into_boxed_slice())
    }

    #[inline]
    pub fn project_archived(values: &[&Archived<Value>], projection: &[TupleIndex]) -> Tuple {
        projection.iter().map(|&idx| nsql_rkyv::deserialize(values[idx.as_usize()])).collect()
    }

    #[inline]
    pub fn pad_right(self, n: usize) -> Tuple {
        self.pad_right_with(n, || Value::Null)
    }

    pub fn pad_right_with<V: Into<Value>>(self, n: usize, f: impl Fn() -> V) -> Tuple {
        let mut values = self.values.into_vec();
        let new_len = values.len() + n;
        values.reserve_exact(n);
        values.resize_with(new_len, || f().into());
        debug_assert_eq!(values.len(), new_len);
        debug_assert_eq!(values.capacity(), new_len);
        Self::new(values.into_boxed_slice())
    }
}

impl ArchivedTuple {
    #[inline]
    pub fn project(&self, projection: &[TupleIndex]) -> Tuple {
        projection.iter().map(|&idx| nsql_rkyv::deserialize(&self.values[idx.as_usize()])).collect()
    }
}

impl fmt::Display for Tuple {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;
        let mut iter = self.values();
        if let Some(value) = iter.next() {
            write!(f, "{value}")?;
            for value in iter {
                write!(f, ", {}", value)?;
            }
        }
        write!(f, ")")
    }
}

impl From<Vec<Value>> for Tuple {
    #[inline]
    fn from(values: Vec<Value>) -> Self {
        Self::new(values.into_boxed_slice())
    }
}

impl<const N: usize> From<[Value; N]> for Tuple {
    #[inline]
    fn from(values: [Value; N]) -> Self {
        Self::new(values)
    }
}

impl From<Box<[Value]>> for Tuple {
    #[inline]
    fn from(values: Box<[Value]>) -> Self {
        Self::new(values)
    }
}

impl FromIterator<Value> for Tuple {
    #[inline]
    fn from_iter<I: IntoIterator<Item = Value>>(iter: I) -> Self {
        Self::new(iter.into_iter().collect::<Box<[_]>>())
    }
}

impl Index<TupleIndex> for Tuple {
    type Output = Value;

    #[inline]
    fn index(&self, index: TupleIndex) -> &Self::Output {
        &self.values[index.as_usize()]
    }
}

impl Index<usize> for Tuple {
    type Output = Value;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.values[index]
    }
}

impl IndexMut<usize> for Tuple {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.values[index]
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Archive, Serialize, Deserialize,
)]
pub struct TupleIndex(u16);

impl TupleIndex {
    #[inline]
    pub fn new(idx: usize) -> Self {
        Self(idx.try_into().unwrap())
    }

    // FIXME ideally we don't want to expose this as we're unwrapping the abstraction
    #[inline]
    pub fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

impl Add<usize> for TupleIndex {
    type Output = Self;

    #[inline]
    fn add(self, rhs: usize) -> Self::Output {
        Self::new(self.as_usize() + rhs)
    }
}

impl FromStr for TupleIndex {
    type Err = anyhow::Error;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            bail!("invalid empty tuple index")
        }

        match s.split_at(1) {
            ("$", index) => Ok(TupleIndex::new(index.parse()?)),
            _ => bail!("tuple index must be prefixed with `$`"),
        }
    }
}

impl fmt::Display for TupleIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "@{}", self.0)
    }
}

#[derive(Debug)]
pub enum FromTupleError {
    TooManyValues,
    NotEnoughValues,
    InvalidCast(Box<dyn Error + Send + Sync>),
}

impl fmt::Display for FromTupleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyValues => write!(f, "too many values"),
            Self::NotEnoughValues => write!(f, "not enough values"),
            Self::InvalidCast(err) => write!(f, "invalid cast: {}", err),
        }
    }
}

impl Error for FromTupleError {}

impl From<CastError> for FromTupleError {
    fn from(err: CastError) -> Self {
        Self::InvalidCast(Box::new(err))
    }
}

pub trait FromTuple: Sized {
    fn from_values(values: impl Iterator<Item = Value>) -> Result<Self, FromTupleError>;

    #[inline]
    fn from_tuple(tuple: Tuple) -> Result<Self, FromTupleError> {
        Self::from_values(tuple.into_values().into_vec().into_iter())
    }
}

impl FromTuple for () {
    #[inline]
    fn from_values(_: impl Iterator<Item = Value>) -> Result<Self, FromTupleError> {
        Ok(())
    }
}

impl<T, U> FromTuple for (T, U)
where
    T: FromValue + 'static,
    U: FromValue + 'static,
{
    #[inline]
    fn from_values(mut values: impl Iterator<Item = Value>) -> Result<Self, FromTupleError> {
        let fst = values.next().ok_or(FromTupleError::NotEnoughValues)?.cast()?;
        let snd = values.next().ok_or(FromTupleError::NotEnoughValues)?.cast()?;
        Ok((fst, snd))
    }
}

impl<T: FromValue + 'static> FromTuple for T {
    #[inline]
    fn from_values(mut values: impl Iterator<Item = Value>) -> Result<Self, FromTupleError> {
        Ok(T::from_value(values.next().ok_or(FromTupleError::NotEnoughValues)?)?)
    }
}

pub trait IntoTuple {
    fn into_tuple(self) -> Tuple;
}

impl<V: Into<Value>> IntoTuple for V {
    #[inline]
    fn into_tuple(self) -> Tuple {
        Tuple::new([self.into()])
    }
}

impl IntoTuple for Tuple {
    #[inline]
    fn into_tuple(self) -> Tuple {
        self
    }
}

impl IntoTuple for () {
    #[inline]
    fn into_tuple(self) -> Tuple {
        Tuple::empty()
    }
}

impl<T, U> IntoTuple for (T, U)
where
    T: Into<Value>,
    U: Into<Value>,
{
    #[inline]
    fn into_tuple(self) -> Tuple {
        Tuple::new([self.0.into(), self.1.into()])
    }
}
