use std::error::Error;
use std::fmt;
use std::hash::Hash;
use std::ops::{Add, Index, IndexMut, Sub};
use std::str::FromStr;

use anyhow::bail;
use itertools::Itertools;
use rkyv::with::RefAsBox;
use rkyv::{Archive, Archived, Deserialize, Serialize};

use crate::value::{CastError, FromValue, Value};

pub trait TupleLike: Index<TupleIndex, Output = Value> {
    fn width(&self) -> usize;

    fn to_raw_parts(&self) -> (*const (), &'static RawTupleLikeVTable);
}

#[repr(C)]
pub struct RawTupleLikeVTable {
    pub width: unsafe extern "C" fn(*const ()) -> usize,
    pub index: unsafe extern "C" fn(*const (), TupleIndex) -> *const Value,
}

impl RawTupleLikeVTable {
    /// Convert a raw vtable and data pointer into an implementation `TupleLike`
    /// # Safety
    /// todo
    pub unsafe fn make_tuple_like(&'static self, data: *const ()) -> impl TupleLike {
        struct Impl {
            data: *const (),
            vtable: &'static RawTupleLikeVTable,
        }

        impl Index<TupleIndex> for Impl {
            type Output = Value;

            #[inline]
            fn index(&self, index: TupleIndex) -> &Self::Output {
                unsafe { &*(self.vtable.index)(self.data, index) }
            }
        }

        impl TupleLike for Impl {
            #[inline]
            fn width(&self) -> usize {
                unsafe { (self.vtable.width)(self.data) }
            }

            #[inline]
            fn to_raw_parts(&self) -> (*const (), &'static RawTupleLikeVTable) {
                (self.data, self.vtable)
            }
        }

        Impl { data, vtable: self }
    }
}

pub trait Tuple:
    TupleLike
    + Hash
    + Eq
    + Ord
    + Clone
    + FromIterator<Value>
    + IntoIterator<Item = Value>
    + From<FlatTuple>
    + Into<FlatTuple>
    + Default
    + fmt::Debug
    + fmt::Display
    + 'static
{
    fn values(&self) -> impl Iterator<Item = &Value>;

    fn join(self, other: &Self) -> Self;

    fn pad_right(self, n: usize) -> Self;

    fn pad_right_with<V: Into<Value>>(self, n: usize, f: impl Fn() -> V) -> Self;

    #[inline]
    fn take(&mut self) -> Self {
        std::mem::take(self)
    }

    #[inline]
    fn empty() -> Self {
        Self::from_iter([])
    }
}

#[derive(
    Clone,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
    Hash,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    Default,
)]
pub struct FlatTuple {
    values: Box<[Value]>,
}

impl AsRef<Self> for FlatTuple {
    #[inline]
    fn as_ref(&self) -> &Self {
        self
    }
}

static FLAT_TUPLE_VTABLE: &RawTupleLikeVTable =
    &RawTupleLikeVTable { width: FlatTuple::raw_width, index: FlatTuple::raw_index };

impl FlatTuple {
    extern "C" fn raw_width(this: *const ()) -> usize {
        unsafe { (*(this as *const FlatTuple)).width() }
    }

    extern "C" fn raw_index(this: *const (), index: TupleIndex) -> *const Value {
        unsafe { &(*(this as *const FlatTuple))[index] }
    }
}

impl TupleLike for FlatTuple {
    #[inline]
    fn width(&self) -> usize {
        self.values.len()
    }

    #[inline]
    fn to_raw_parts(&self) -> (*const (), &'static RawTupleLikeVTable) {
        (self as *const _ as *const (), FLAT_TUPLE_VTABLE)
    }
}

impl Tuple for FlatTuple {
    #[inline]
    fn join(self, other: &Self) -> Self {
        let mut values = self.values.into_vec();
        values.reserve_exact(other.values.len());
        values.extend_from_slice(&other.values);
        Self::new(values.into_boxed_slice())
    }

    #[inline]
    fn pad_right(self, n: usize) -> FlatTuple {
        self.pad_right_with(n, || Value::Null)
    }

    #[inline]
    fn pad_right_with<V: Into<Value>>(self, n: usize, f: impl Fn() -> V) -> FlatTuple {
        let mut values = self.values.into_vec();
        let new_len = values.len() + n;
        values.reserve_exact(n);
        values.resize_with(new_len, || f().into());
        debug_assert_eq!(values.len(), new_len);
        debug_assert_eq!(values.capacity(), new_len);
        Self::new(values.into_boxed_slice())
    }

    #[inline]
    fn values(&self) -> impl Iterator<Item = &Value> {
        self.values.iter()
    }
}

impl fmt::Debug for FlatTuple {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({})", self.values.iter().format(", "))
    }
}

impl IntoIterator for FlatTuple {
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

impl FlatTuple {
    #[inline]
    pub fn new(values: impl Into<Box<[Value]>>) -> Self {
        Self { values: values.into() }
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
    pub fn into_values(self) -> Box<[Value]> {
        self.values
    }

    #[inline]
    pub fn project_archived(values: &[&Archived<Value>], projection: &[TupleIndex]) -> FlatTuple {
        projection.iter().map(|&idx| nsql_rkyv::deserialize(values[idx.as_usize()])).collect()
    }
}

impl ArchivedFlatTuple {
    #[inline]
    pub fn project(&self, projection: &[TupleIndex]) -> FlatTuple {
        projection.iter().map(|&idx| nsql_rkyv::deserialize(&self.values[idx.as_usize()])).collect()
    }
}

impl fmt::Display for FlatTuple {
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

impl From<Vec<Value>> for FlatTuple {
    #[inline]
    fn from(values: Vec<Value>) -> Self {
        Self::new(values.into_boxed_slice())
    }
}

impl<const N: usize> From<[Value; N]> for FlatTuple {
    #[inline]
    fn from(values: [Value; N]) -> Self {
        Self::new(values)
    }
}

impl From<Box<[Value]>> for FlatTuple {
    #[inline]
    fn from(values: Box<[Value]>) -> Self {
        Self::new(values)
    }
}

impl FromIterator<Value> for FlatTuple {
    #[inline]
    fn from_iter<I: IntoIterator<Item = Value>>(iter: I) -> Self {
        Self::new(iter.into_iter().collect::<Box<[_]>>())
    }
}

impl Index<TupleIndex> for FlatTuple {
    type Output = Value;

    #[inline]
    fn index(&self, index: TupleIndex) -> &Self::Output {
        &self.values[index.as_usize()]
    }
}

impl Index<usize> for FlatTuple {
    type Output = Value;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.values[index]
    }
}

impl IndexMut<usize> for FlatTuple {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.values[index]
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Archive, Serialize, Deserialize,
)]
#[repr(C)]
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

impl Sub<usize> for TupleIndex {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: usize) -> Self::Output {
        Self::new(self.as_usize() - rhs)
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
pub enum FromFlatTupleError {
    TooManyValues,
    NotEnoughValues,
    InvalidCast(Box<dyn Error + Send + Sync>),
}

impl fmt::Display for FromFlatTupleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyValues => write!(f, "too many values"),
            Self::NotEnoughValues => write!(f, "not enough values"),
            Self::InvalidCast(err) => write!(f, "invalid cast: {}", err),
        }
    }
}

impl Error for FromFlatTupleError {}

impl From<CastError> for FromFlatTupleError {
    fn from(err: CastError) -> Self {
        Self::InvalidCast(Box::new(err))
    }
}

pub trait FromTuple: Sized {
    fn from_values(values: impl Iterator<Item = Value>) -> Result<Self, FromFlatTupleError>;

    #[inline]
    fn from_tuple(tuple: impl Tuple) -> Result<Self, FromFlatTupleError> {
        Self::from_values(tuple.into_iter())
    }
}

impl FromTuple for () {
    #[inline]
    fn from_values(_: impl Iterator<Item = Value>) -> Result<Self, FromFlatTupleError> {
        Ok(())
    }
}

impl<T, U> FromTuple for (T, U)
where
    T: FromValue + 'static,
    U: FromValue + 'static,
{
    #[inline]
    fn from_values(mut values: impl Iterator<Item = Value>) -> Result<Self, FromFlatTupleError> {
        let fst = values.next().ok_or(FromFlatTupleError::NotEnoughValues)?.cast()?;
        let snd = values.next().ok_or(FromFlatTupleError::NotEnoughValues)?.cast()?;
        Ok((fst, snd))
    }
}

impl<T: FromValue + 'static> FromTuple for T {
    #[inline]
    fn from_values(mut values: impl Iterator<Item = Value>) -> Result<Self, FromFlatTupleError> {
        Ok(T::from_value(values.next().ok_or(FromFlatTupleError::NotEnoughValues)?)?)
    }
}

pub trait IntoFlatTuple {
    fn into_tuple(self) -> FlatTuple;
}

impl<V: Into<Value>> IntoFlatTuple for V {
    #[inline]
    fn into_tuple(self) -> FlatTuple {
        FlatTuple::new([self.into()])
    }
}

impl IntoFlatTuple for FlatTuple {
    #[inline]
    fn into_tuple(self) -> FlatTuple {
        self
    }
}

impl IntoFlatTuple for () {
    #[inline]
    fn into_tuple(self) -> FlatTuple {
        FlatTuple::empty()
    }
}

impl<T, U> IntoFlatTuple for (T, U)
where
    T: Into<Value>,
    U: Into<Value>,
{
    #[inline]
    fn into_tuple(self) -> FlatTuple {
        FlatTuple::new([self.0.into(), self.1.into()])
    }
}

#[derive(Clone)]
pub struct JointTuple<'a>(pub &'a dyn TupleLike, pub &'a dyn TupleLike);

static JOINT_TUPLE_VTABLE: &RawTupleLikeVTable =
    &RawTupleLikeVTable { width: JointTuple::raw_width, index: JointTuple::raw_index };

impl<'a> JointTuple<'a> {
    extern "C" fn raw_width(this: *const ()) -> usize {
        unsafe { (*(this as *const JointTuple<'a>)).width() }
    }

    extern "C" fn raw_index(this: *const (), index: TupleIndex) -> *const Value {
        unsafe { &(*(this as *const JointTuple<'a>))[index] }
    }
}

impl<'a> TupleLike for JointTuple<'a> {
    #[inline]
    fn width(&self) -> usize {
        let Self(left, right) = self;
        left.width() + right.width()
    }

    fn to_raw_parts(&self) -> (*const (), &'static RawTupleLikeVTable) {
        (self as *const _ as *const (), JOINT_TUPLE_VTABLE)
    }
}

impl<'a> Index<TupleIndex> for JointTuple<'a> {
    type Output = Value;

    #[inline]
    fn index(&self, index: TupleIndex) -> &Self::Output {
        let Self(left, right) = self;
        if index.as_usize() < left.width() { &left[index] } else { &right[index - left.width()] }
    }
}
