use std::sync::OnceLock;

use nsql_core::{LogicalType, Name, PhysicalType};

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
        self.cached_physical_type.get_or_init(|| self.logical_type.physical_type())
    }
}
