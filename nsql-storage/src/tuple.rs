use std::ops::Index;
use std::sync::Arc;

use nsql_serde::{StreamDeserialize, StreamDeserializeWith, StreamDeserializer};

use crate::schema::{PhysicalType, Schema};
use crate::value::{Decimal, Value};

pub struct TupleDeserializationContext {
    pub schema: Arc<Schema>,
}

#[derive(Debug, Clone, PartialEq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[repr(transparent)]
pub struct Tuple(Vec<Value>);

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

impl Tuple {
    #[inline]
    pub fn new(values: Vec<Value>) -> Self {
        assert!(!values.is_empty(), "tuple must have at least one value");
        Self(values)
    }

    #[inline]
    pub fn empty() -> Self {
        Self(Vec::new())
    }

    #[inline]
    pub fn values(&self) -> impl Iterator<Item = &Value> {
        self.0.iter()
    }

    #[inline]
    pub(crate) fn append(self, value: Value) -> Tuple {
        let mut values = self.0;
        values.push(value);
        Self::new(values)
    }
}

impl From<Vec<Value>> for Tuple {
    fn from(values: Vec<Value>) -> Self {
        Self::new(values)
    }
}

impl FromIterator<Value> for Tuple {
    fn from_iter<I: IntoIterator<Item = Value>>(iter: I) -> Self {
        Self::new(iter.into_iter().collect::<Vec<_>>())
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
