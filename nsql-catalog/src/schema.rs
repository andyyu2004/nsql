use nsql_serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::private::CatalogEntity;
use crate::set::CatalogSet;
use crate::{Catalog, Container, Entity, Name, Table};

#[derive(Debug)]
pub struct Schema {
    name: Name,
    pub(crate) tables: CatalogSet<Table>,
}

pub trait SchemaEntity: CatalogEntity<Container = Schema> {}

impl<T: CatalogEntity<Container = Schema>> SchemaEntity for T {}

impl Container for Schema {}

pub(crate) mod private {
    use super::*;

    pub trait SchemaEntity: Entity + Sized {}
}

#[derive(Debug)]
pub struct CreateSchemaInfo {
    pub name: Name,
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
        Ok(Self { name: Name::from(s.as_str()) })
    }
}

impl CatalogEntity for Schema {
    type Container = Catalog;

    type CreateInfo = CreateSchemaInfo;

    #[inline]
    fn catalog_set(catalog: &Catalog) -> &CatalogSet<Self> {
        &catalog.schemas
    }

    #[inline]
    fn new(info: Self::CreateInfo) -> Self {
        Self { name: info.name, tables: Default::default() }
    }
}

impl Entity for Schema {
    #[inline]
    fn desc() -> &'static str {
        "schema"
    }

    #[inline]
    fn name(&self) -> &Name {
        &self.name
    }
}
