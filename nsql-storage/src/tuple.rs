use std::fmt;
use std::sync::Arc;

use nsql_core::schema::{PhysicalType, Schema};
use nsql_serde::{Deserialize, DeserializeWith, Deserializer, Serialize};
use rust_decimal::Decimal;

pub struct TupleDeserializationContext {
    pub schema: Arc<Schema>,
}

#[derive(Debug, PartialEq, Serialize)]
pub struct Tuple {
    values: Box<[Value]>,
}

impl DeserializeWith for Tuple {
    type Context<'a> = TupleDeserializationContext;

    async fn deserialize_with(
        ctx: &Self::Context<'_>,
        de: &mut dyn Deserializer<'_>,
    ) -> Result<Self, Self::Error> {
        let attributes = ctx.schema.attributes();
        let mut values = Vec::with_capacity(attributes.len());

        for attribute in attributes {
            let value = match attribute.physical_type() {
                PhysicalType::Bool => {
                    let b = bool::deserialize(de).await?;
                    Value::Literal(Literal::Bool(b))
                }
                PhysicalType::Decimal => {
                    let d = <Decimal as Deserialize>::deserialize(de).await?;
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
    pub fn new(values: Box<[Value]>) -> Self {
        assert!(!values.is_empty(), "tuple must have at least one value");
        Self { values }
    }

    #[inline]
    pub fn values(&self) -> &[Value] {
        self.values.as_ref()
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

#[derive(Debug, PartialEq, Serialize)]
pub enum Value {
    Literal(Literal),
}

#[derive(Debug, PartialEq, Serialize)]
pub enum Literal {
    #[serde(skip)]
    Null,
    Bool(bool),
    Decimal(Decimal),
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
        }
    }
}
