use nsql_serde::{Deserialize, Deserializer};
use nsql_storage::tuple::PhysicalType;

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum LogicalType {
    Int,
}

impl<'a> From<&'a LogicalType> for PhysicalType {
    fn from(val: &'a LogicalType) -> Self {
        match val {
            LogicalType::Int => PhysicalType::Int32,
        }
    }
}

impl Deserialize for LogicalType {
    type Error = std::io::Error;

    async fn deserialize(_de: &mut dyn Deserializer<'_>) -> Result<Self, Self::Error> {
        todo!()
    }
}
