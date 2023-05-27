mod column;

use std::fmt;
use std::sync::Arc;

use nsql_storage::TableStorage;
use nsql_storage_engine::StorageEngine;

pub use self::column::{Column, ColumnIndex, CreateColumnInfo};
use crate::private::CatalogEntity;
use crate::set::CatalogSet;
use crate::{Container, Entity, Name, Namespace};

pub struct Table<S> {
    name: Name,
    columns: CatalogSet<S, Column>,
    // storage: Arc<TableStorage<S>>,
}

impl<S: StorageEngine> Table<S> {
    #[inline]
    pub fn name(&self) -> &Name {
        &self.name
    }

    // #[inline]
    // pub fn storage(&self) -> &Arc<TableStorage<S>> {
    //     &self.storage
    // }

    #[inline]
    /// Returns the index of the special `tid` column
    pub fn tid_column_index(&self) -> ColumnIndex {
        ColumnIndex::new(self.columns.len().try_into().unwrap())
    }
}

impl<S> fmt::Debug for Table<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Table").field("name", &self.name).finish_non_exhaustive()
    }
}

pub struct CreateTableInfo {
    pub name: Name,
}

impl fmt::Debug for CreateTableInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateTableInfo").field("name", &self.name).finish_non_exhaustive()
    }
}

impl<S: StorageEngine> Entity for Table<S> {
    #[inline]
    fn name(&self) -> Name {
        Name::clone(&self.name)
    }

    #[inline]
    fn desc() -> &'static str {
        "table"
    }
}

impl<S: StorageEngine> CatalogEntity<S> for Table<S> {
    type Container = Namespace<S>;

    type CreateInfo = CreateTableInfo;

    fn catalog_set(container: &Self::Container) -> &CatalogSet<S, Self> {
        &container.tables
    }

    fn create(_tx: &mut S::WriteTransaction<'_>, info: Self::CreateInfo) -> Self {
        Self { name: info.name, columns: Default::default() }
    }
}

impl<S: StorageEngine> Container<S> for Table<S> {}
