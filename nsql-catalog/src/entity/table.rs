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

    fn new(tx: &Transaction, info: Self::CreateInfo) -> Self {
        let columns = CatalogSet::default();
        for column in info.columns {
            columns.insert(tx, Column::new(tx, column));
        }
        Self { name: info.name, storage: info.storage, columns }
    }
}

impl Container for Table {}
