use std::error::Error;
use std::fmt;
use std::ops::{Index, IndexMut};

use rkyv::with::RefAsBox;
use rkyv::Archived;

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

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
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
    ColumnCountMismatch { expected: usize, actual: usize },
    InvalidCast(Box<dyn Error + Send + Sync>),
}

impl fmt::Display for FromTupleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ColumnCountMismatch { expected, actual } => {
                write!(f, "expected {} columns, got {}", expected, actual)
            }
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
    fn from_tuple(tuple: Tuple) -> Result<Self, FromTupleError>;
}

impl FromTuple for ! {
    fn from_tuple(_tuple: Tuple) -> Result<Self, FromTupleError> {
        panic!()
    }
}

pub trait IntoTuple {
    fn into_tuple(self) -> Tuple;
}

impl IntoTuple for Tuple {
    #[inline]
    fn into_tuple(self) -> Tuple {
        self
    }
}

impl IntoTuple for ! {
    fn into_tuple(self) -> Tuple {
        self
    }
}
