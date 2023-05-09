mod column;

use std::fmt;
use std::sync::Arc;

use nsql_serde::StreamSerialize;
use nsql_storage::TableStorage;
use nsql_transaction::Transaction;

pub use self::column::{Column, CreateColumnInfo};
use crate::private::CatalogEntity;
use crate::set::CatalogSet;
use crate::{Container, Entity, Name, Namespace};

#[derive(StreamSerialize)]
pub struct Table {
    name: Name,
    columns: CatalogSet<Column>,
    #[serde(skip)]
    storage: Arc<TableStorage>,
}

impl Table {
    #[inline]
    pub fn name(&self) -> &Name {
        &self.name
    }

    #[inline]
    pub fn storage(&self) -> &Arc<TableStorage> {
        &self.storage
    }
}

impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Table").field("name", &self.name).finish_non_exhaustive()
    }
}

pub struct CreateTableInfo {
    pub name: Name,
    pub storage: Arc<TableStorage>,
}

impl fmt::Debug for CreateTableInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateTableInfo").field("name", &self.name).finish_non_exhaustive()
    }
}

impl Entity for Table {
    #[inline]
    fn name(&self) -> Name {
        Name::clone(&self.name)
    }

    #[inline]
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

    fn new(_tx: &Transaction, info: Self::CreateInfo) -> Self {
        Self { name: info.name, storage: info.storage, columns: Default::default() }
    }
}

impl Container for Table {}
