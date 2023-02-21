use nsql_serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::private::CatalogEntity;
use crate::set::CatalogSet;
use crate::{Entity, Name, Schema};

#[derive(Debug, Clone)]
pub struct Table {
    name: Name,
}

#[derive(Debug)]
pub struct CreateTableInfo {
    name: Name,
}

impl Serialize for Table {
    type Error = std::io::Error;

    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), Self::Error> {
        ser.write_str(self.name.as_str()).await
    }
}

impl Deserialize for CreateTableInfo {
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, Self::Error> {
        let s = de.read_str().await?;
        Ok(Self { name: Name::from(s.as_str()) })
    }
}

impl Entity for Table {
    fn name(&self) -> &Name {
        &self.name
    }

    fn desc() -> &'static str {
        "table"
    }
}

impl CatalogEntity for Table {
    type Container = Schema;

    type CreateInfo = CreateTableInfo;

    fn catalog_set(container: &Self::Container) -> &CatalogSet<Self> {
        &container.tables
    }

    fn new(info: Self::CreateInfo) -> Self {
        Self { name: info.name }
    }
}
