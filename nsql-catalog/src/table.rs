use std::fmt;
use std::sync::Arc;

use nsql_core::schema::LogicalType;
use nsql_serde::{StreamDeserialize, StreamSerialize};
use nsql_storage::TableStorage;

use crate::private::CatalogEntity;
use crate::set::CatalogSet;
use crate::{Entity, Name, Namespace};

#[derive(Clone, StreamSerialize)]
pub struct Table {
    name: Name,
    #[serde(skip)]
    storage: Arc<TableStorage>,
}

impl Table {
    #[inline]
    pub fn storage(&self) -> &TableStorage {
        self.storage.as_ref()
    }
}

impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Table").field("name", &self.name).finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct CreateTableInfo {
    pub name: Name,
    pub columns: Vec<CreateColumnInfo>,
    pub storage: Arc<TableStorage>,
}

impl fmt::Debug for CreateTableInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateTableInfo")
            .field("name", &self.name)
            .field("columns", &self.columns)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, StreamDeserialize)]
pub struct CreateColumnInfo {
    pub name: Name,
    pub ty: LogicalType,
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
    type Container = Namespace;

    type CreateInfo = CreateTableInfo;

    fn catalog_set(container: &Self::Container) -> &CatalogSet<Self> {
        &container.tables
    }

    fn new(info: Self::CreateInfo) -> Self {
        Self { name: info.name, storage: info.storage }
    }
}
