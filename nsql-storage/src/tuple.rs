use std::error::Error;
use std::fmt;
use std::ops::{Index, IndexMut};

use rkyv::with::RefAsBox;
use rkyv::{Archive, Archived, Deserialize, Serialize};

use crate::value::{CastError, FromValue, Value};

#[derive(Debug, Clone, PartialEq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[repr(transparent)]
pub struct Tuple(Box<[Value]>);

#[derive(Debug, Clone, PartialEq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[repr(transparent)]
pub struct TupleRef<'a>(#[with(RefAsBox)] &'a [Value]);

impl Tuple {
    #[inline]
    pub fn new(values: Box<[Value]>) -> Self {
        Self(values)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn empty() -> Self {
        Self::new(Box::new([]))
    }

    #[inline]
    pub fn values(&self) -> impl Iterator<Item = &Value> {
        self.0.iter()
    }

    #[inline]
    pub fn into_values(self) -> Box<[Value]> {
        self.0
    }

    #[inline]
    pub fn join(self, other: &Self) -> Self {
        let mut values = self.0.into_vec();
        values.extend_from_slice(&other.0);
        Self::new(values.into_boxed_slice())
    }

    #[inline]
    pub fn project_archived(values: &[&Archived<Value>], projection: &[TupleIndex]) -> Tuple {
        projection.iter().map(|&idx| nsql_rkyv::deserialize(values[idx.0])).collect()
    }
}

impl ArchivedTuple {
    #[inline]
    pub fn project(&self, projection: &[TupleIndex]) -> Tuple {
        projection.iter().map(|&idx| nsql_rkyv::deserialize(&self.0[idx.0])).collect()
    }
}

impl fmt::Display for Tuple {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;
        let mut iter = self.0.iter();
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
        Self::new(values.into())
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
        Self::new(iter.into_iter().collect())
    }
}

impl Index<TupleIndex> for Tuple {
    type Output = Value;

    #[inline]
    fn index(&self, index: TupleIndex) -> &Self::Output {
        &self.0[index.0]
    }
}

impl Index<usize> for Tuple {
    type Output = Value;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl IndexMut<usize> for Tuple {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.0[index]
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Archive, Serialize, Deserialize,
)]
pub struct TupleIndex(usize);

impl TupleIndex {
    #[inline]
    pub fn new(idx: usize) -> Self {
        Self(idx)
    }

    // FIXME ideally we don't want to expose this as we're unwrapping the abstraction
    #[inline]
    pub fn as_usize(&self) -> usize {
        self.0
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

impl<T: 'static> From<CastError<T>> for FromTupleError {
    fn from(err: CastError<T>) -> Self {
        Self::InvalidCast(Box::new(err))
    }
}

pub trait FromTuple: Sized {
    fn from_values(values: impl Iterator<Item = Value>) -> Result<Self, FromTupleError>;

    #[inline]
    fn from_tuple(tuple: Tuple) -> Result<Self, FromTupleError> {
        Self::from_values(Vec::from(tuple.into_values()).into_iter())
    }
}

impl FromTuple for () {
    fn from_values(_: impl Iterator<Item = Value>) -> Result<Self, FromTupleError> {
        Ok(())
    }
}

impl<T, U> FromTuple for (T, U)
where
    T: FromValue + 'static,
    U: FromValue + 'static,
{
    fn from_values(mut values: impl Iterator<Item = Value>) -> Result<Self, FromTupleError> {
        let fst = values.next().ok_or(FromTupleError::NotEnoughValues)?.cast_non_null()?;
        let snd = values.next().ok_or(FromTupleError::NotEnoughValues)?.cast_non_null()?;
        Ok((fst, snd))
    }
}

impl<T: FromValue + 'static> FromTuple for T {
    fn from_values(mut values: impl Iterator<Item = Value>) -> Result<Self, FromTupleError> {
        Ok(T::from_value(values.next().ok_or(FromTupleError::NotEnoughValues)?)?)
    }
}

pub trait IntoTuple {
    fn into_tuple(self) -> Tuple;
}

impl IntoTuple for Value {
    #[inline]
    fn into_tuple(self) -> Tuple {
        Tuple::new(Box::new([self]))
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
