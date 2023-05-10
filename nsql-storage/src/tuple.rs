use std::fmt;
use std::ops::Index;
use std::sync::Arc;

use nsql_serde::{StreamDeserialize, StreamDeserializeWith, StreamDeserializer};

use crate::schema::{PhysicalType, Schema};
use crate::table_storage::TupleId;
use crate::value::{Decimal, Value};

pub struct TupleDeserializationContext {
    pub schema: Arc<Schema>,
}

#[derive(Debug, Clone, PartialEq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[repr(transparent)]
pub struct Tuple(Box<[Value]>);

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
    // FIXME find more efficient representation to avoid all this copying
    pub fn split_last(&self) -> Option<(Tuple, Value)> {
        let (last, rest) = self.0.split_last()?;
        Some((Self::from(rest.to_vec()), last.clone()))
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
    pub fn project(&self, projection: &[TupleIndex]) -> Self {
        projection.iter().map(|&idx| self[idx].clone()).collect()
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

impl ArchivedTuple {
    #[inline]
    pub fn project(&self, tid: TupleId, projection: &[TupleIndex]) -> Tuple {
        let n = self.0.len();
        projection
            .iter()
            .map(|col| match col {
                i if i.0 == n => Value::Tid(tid),
                i => nsql_rkyv::deserialize(&self.0[i.0]),
            })
            .collect()
    }
}

impl StreamDeserializeWith for Tuple {
    type Context<'a> = TupleDeserializationContext;

    async fn deserialize_with<D: StreamDeserializer>(
        ctx: &Self::Context<'_>,
        de: &mut D,
    ) -> nsql_serde::Result<Self> {
        let attributes = ctx.schema.attributes();
        let mut values = Vec::with_capacity(attributes.len());

        for attribute in attributes {
            let value = match attribute.physical_type() {
                PhysicalType::Bool => {
                    let b = bool::deserialize(de).await?;
                    Value::Bool(b)
                }
                PhysicalType::Decimal => {
                    let d = <Decimal as StreamDeserialize>::deserialize(de).await?;
                    Value::Decimal(d)
                }
                PhysicalType::Int32 => todo!(),
            };
            values.push(value);
        }

        Ok(Self::from(values))
    }
}

impl From<Vec<Value>> for Tuple {
    fn from(values: Vec<Value>) -> Self {
        Self::new(values.into_boxed_slice())
    }
}

impl From<Box<[Value]>> for Tuple {
    fn from(values: Box<[Value]>) -> Self {
        Self::new(values)
    }
}

impl FromIterator<Value> for Tuple {
    fn from_iter<I: IntoIterator<Item = Value>>(iter: I) -> Self {
        Self::new(iter.into_iter().collect())
    }
}

impl Index<TupleIndex> for Tuple {
    type Output = Value;

    fn index(&self, index: TupleIndex) -> &Self::Output {
        &self.0[index.0]
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
