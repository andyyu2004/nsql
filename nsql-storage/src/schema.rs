use std::fmt;
use std::sync::OnceLock;

use nsql_core::Name;
use nsql_serde::{StreamDeserialize, StreamDeserializer, StreamSerialize, StreamSerializer};
use rust_decimal::Decimal;

use crate::table_storage::TupleId;
use crate::value::{Cast, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    attributes: Vec<Attribute>,
}

impl Schema {
    pub fn new(attributes: Vec<Attribute>) -> Self {
        Self { attributes }
    }

    #[inline]
    pub fn attributes(&self) -> &[Attribute] {
        &self.attributes
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attribute {
    name: Name,
    logical_type: LogicalType,
    cached_physical_type: OnceLock<PhysicalType>,
}

impl Attribute {
    pub fn new(name: impl Into<Name>, logical_type: LogicalType) -> Self {
        Self { name: name.into(), logical_type, cached_physical_type: Default::default() }
    }

    #[inline]
    pub fn physical_type(&self) -> &PhysicalType {
        self.cached_physical_type.get_or_init(|| (&self.logical_type).into())
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum LogicalType {
    Null,
    Bool,
    Int,
    Decimal,
    Text,
    Tid,
}

impl fmt::Display for LogicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalType::Bool => write!(f, "boolean"),
            LogicalType::Int => write!(f, "int"),
            LogicalType::Decimal => write!(f, "decimal"),
            LogicalType::Text => write!(f, "text"),
            LogicalType::Null => write!(f, "null"),
            LogicalType::Tid => write!(f, "tid"),
        }
    }
}

impl StreamSerialize for LogicalType {
    async fn serialize<S: StreamSerializer>(&self, _ser: &mut S) -> nsql_serde::Result<()> {
        todo!()
    }
}

impl StreamDeserialize for LogicalType {
    async fn deserialize<D: StreamDeserializer>(_de: &mut D) -> nsql_serde::Result<Self> {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalType {
    /// 8-bit boolean
    Bool,
    /// 32-bit signed integer
    Int32,
    /// 128-bit fixed-size decimal
    Decimal,
}

impl<'a> From<&'a LogicalType> for PhysicalType {
    fn from(val: &'a LogicalType) -> Self {
        match val {
            LogicalType::Bool => PhysicalType::Bool,
            LogicalType::Int => PhysicalType::Int32,
            LogicalType::Decimal => PhysicalType::Decimal,
            LogicalType::Text => todo!(),
            LogicalType::Null => todo!(),
            LogicalType::Tid => todo!(),
        }
    }
}