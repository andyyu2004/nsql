use std::fmt;
use std::sync::Arc;

use nsql_serde::{Deserialize, Serialize};

use crate::private::CatalogEntity;
use crate::set::CatalogSet;
use crate::{Entity, Name, Schema, Ty};

#[derive(Clone, Serialize)]
pub struct Table {
    name: Name,
    #[serde(skip)]
    storage: Arc<dyn TableStorage>,
}

impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Table").field("name", &self.name).finish_non_exhaustive()
    }
}

// this trait exists partially to break the cyclic dependency between `catalog` and `storage`
// (if we were to depend on `nsql-storage` here)
pub trait TableStorage: Send + Sync {}

#[derive(Clone)]
pub struct CreateTableInfo {
    pub name: Name,
    pub columns: Vec<CreateColumnInfo>,
    pub storage: Arc<dyn TableStorage>,
}

impl fmt::Debug for CreateTableInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateTableInfo")
            .field("name", &self.name)
            .field("columns", &self.columns)
            .finish_non_exhaustive()
    }
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
        Self { name: info.name, storage: info.storage }
    }
}
