use std::sync::Arc;

use nsql_serde::{Deserialize, Deserializer, Serialize, Serializer};
use nsql_transaction::Transaction;
use parking_lot::RwLock;

use crate::entry::Oid;
use crate::private::Sealed;
use crate::set::CatalogSet;
use crate::{Catalog, CatalogEntity, EntryName};

#[derive(Clone)]
pub struct Schema {
    name: EntryName,
}

#[derive(Debug)]
pub struct CreateSchemaInfo {
    name: EntryName,
}

impl Schema {
    #[inline]
    pub(crate) fn new(info: CreateSchemaInfo) -> Self {
        Self { name: info.name }
    }

    #[inline]
    pub fn name(&self) -> &EntryName {
        &self.name
    }
}

impl Serialize for Schema {
    type Error = std::io::Error;

    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), Self::Error> {
        ser.write_str(self.name.as_str()).await
    }
}

impl Deserialize for CreateSchemaInfo {
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, Self::Error> {
        let s = de.read_str().await?;
        Ok(Self { name: EntryName::from(s.as_str()) })
    }
}

impl Sealed for Schema {
    fn catalog_set(catalog: &Catalog) -> &RwLock<CatalogSet<Self>> {
        &catalog.schemas
    }
}

impl CatalogEntity for Schema {
    type CreateInfo = CreateSchemaInfo;

    #[inline]
    fn new(info: Self::CreateInfo) -> Self {
        Self { name: info.name }
    }

    #[inline]
    fn name(&self) -> &EntryName {
        &self.name
    }
}
