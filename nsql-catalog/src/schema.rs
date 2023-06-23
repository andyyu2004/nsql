use nsql_core::{LogicalType, Name};

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
}

impl Attribute {
    pub fn new(name: impl Into<Name>, logical_type: LogicalType) -> Self {
        Self { name: name.into(), logical_type }
    }
}
