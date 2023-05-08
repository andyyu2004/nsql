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
    pub fn project(&self, projections: &[TupleIndex]) -> Self {
        projections.iter().map(|&idx| self[idx].clone()).collect()
    }
}

impl ArchivedTuple {
    #[inline]
    pub fn project(&self, tid: TupleId, projections: &[TupleIndex]) -> Tuple {
        let n = self.0.len();
        projections
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
}
