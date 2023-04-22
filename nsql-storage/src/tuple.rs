use std::fmt;
use std::sync::Arc;

use nsql_core::schema::{PhysicalType, Schema};
use nsql_serde::{StreamDeserialize, StreamDeserializeWith, StreamDeserializer};
use rust_decimal::Decimal;

pub struct TupleDeserializationContext {
    pub schema: Arc<Schema>,
}

#[derive(Debug, Clone, PartialEq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[repr(transparent)]
pub struct Tuple(Box<[Value]>);

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
                    Value::Literal(Literal::Bool(b))
                }
                PhysicalType::Decimal => {
                    let d = <Decimal as StreamDeserialize>::deserialize(de).await?;
                    Value::Literal(Literal::Decimal(d))
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
    pub fn new(values: Box<[Value]>) -> Self {
        assert!(!values.is_empty(), "tuple must have at least one value");
        Self(values)
    }

    #[inline]
    pub fn empty() -> Self {
        // FIXME consider making this a static to avoid allocations
        Self(Box::new([]))
    }

    #[inline]
    pub fn values(&self) -> &[Value] {
        self.0.as_ref()
    }
}

impl From<Vec<Value>> for Tuple {
    fn from(values: Vec<Value>) -> Self {
        Self::new(values.into_boxed_slice())
    }
}

impl FromIterator<Value> for Tuple {
    fn from_iter<I: IntoIterator<Item = Value>>(iter: I) -> Self {
        Self::new(iter.into_iter().collect::<Vec<_>>().into_boxed_slice())
    }
}

#[derive(Debug, Clone, PartialEq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum Value {
    Literal(Literal),
}

#[derive(Debug, Clone, PartialEq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum Literal {
    Null,
    Bool(bool),
    Decimal(Decimal),
    String(String),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Literal(literal) => write!(f, "{literal}"),
        }
    }
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::Null => write!(f, "NULL"),
            Literal::Bool(b) => write!(f, "{b}"),
            Literal::Decimal(d) => write!(f, "{d}"),
            Literal::String(s) => write!(f, "{s}"),
        }
    }
}
