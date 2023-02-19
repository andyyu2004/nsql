use nsql_serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{CatalogEntity, EntryName};

#[derive(Clone)]
pub struct Schema {
    name: EntryName,
}

impl Schema {
    pub(crate) fn new(name: EntryName) -> Self {
        Self { name }
    }

    pub fn name(&self) -> &EntryName {
        &self.name
    }
}

impl Serialize for Schema {
    type Error = std::io::Error;

    async fn serialize(&self, writer: &mut dyn Serializer<'_>) -> Result<(), Self::Error> {
        todo!()
    }
}

impl Deserialize for Schema {
    type Error = std::io::Error;

    async fn deserialize(reader: &mut dyn Deserializer<'_>) -> Result<Self, Self::Error> {
        todo!()
    }
}

impl CatalogEntity for Schema {
    fn name(&self) -> &EntryName {
        &self.name
    }
}
