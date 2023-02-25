use nsql_serde::{Deserialize, Serialize};

use crate::private::CatalogEntity;
use crate::set::CatalogSet;
use crate::{Entity, Name, Schema, Ty};

#[derive(Debug, Clone, Serialize)]
pub struct Table {
    name: Name,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateTableInfo {
    pub name: Name,
    pub columns: Vec<CreateColumnInfo>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateColumnInfo {
    pub name: Name,
    pub ty: Ty,
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
