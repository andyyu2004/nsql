use nsql_serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::private::CatalogEntity;
use crate::set::CatalogSet;
use crate::{Entity, Name, Schema, Ty};

#[derive(Debug, Clone)]
pub struct Table {
    name: Name,
}

#[derive(Debug, Clone)]
pub struct CreateTableInfo {
    pub name: Name,
    pub columns: Vec<CreateColumnInfo>,
}

#[derive(Debug, Clone)]
pub struct CreateColumnInfo {
    pub name: Name,
    pub ty: Ty,
}

impl Deserialize for CreateColumnInfo {
    type Error = std::io::Error;

    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, Self::Error> {
        let name = Name::deserialize(de).await?;
        let ty = Ty::deserialize(de).await?;
        Ok(Self { name, ty })
    }
}

impl Serialize for Table {
    type Error = std::io::Error;

    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), Self::Error> {
        ser.write_str(self.name.as_str()).await
    }
}

impl Deserialize for CreateTableInfo {
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, Self::Error> {
        let name = Name::deserialize(de).await?;
        let columns = Vec::<CreateColumnInfo>::deserialize(de).await?;
        Ok(Self { name, columns })
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
