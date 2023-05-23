use std::fmt;

use nsql_storage::schema::LogicalType;
use nsql_storage_engine::StorageEngine;

use crate::private::CatalogEntity;
use crate::set::CatalogSet;
use crate::{Entity, Name, Table};

#[derive(Clone)]
pub struct Column {
    name: Name,
    index: ColumnIndex,
    ty: LogicalType,
    is_primary_key: bool,
}

impl Column {
    #[inline]
    pub fn index(&self) -> ColumnIndex {
        self.index
    }

    #[inline]
    pub fn logical_type(&self) -> LogicalType {
        self.ty.clone()
    }

    #[inline]
    pub fn is_primary_key(&self) -> bool {
        self.is_primary_key
    }
}

impl fmt::Debug for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Column").field("name", &self.name).finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColumnIndex {
    index: u8,
}

impl ColumnIndex {
    // FIXME ideally this would be private
    #[inline]
    pub fn new(index: u8) -> Self {
        Self { index }
    }

    #[inline]
    pub fn as_usize(self) -> usize {
        self.index as usize
    }
}

#[derive(Debug, Clone)]
pub struct CreateColumnInfo {
    pub name: Name,
    /// The index of the column in the table.
    pub index: u8,
    pub ty: LogicalType,
    pub is_primary_key: bool,
}

impl Entity for Column {
    #[inline]
    fn name(&self) -> Name {
        Name::clone(&self.name)
    }

    #[inline]
    fn desc() -> &'static str {
        "column"
    }
}

impl<S: StorageEngine> CatalogEntity<S> for Column {
    type Container = Table<S>;

    type CreateInfo = CreateColumnInfo;

    fn catalog_set(table: &Self::Container) -> &CatalogSet<S, Self> {
        &table.columns
    }

    fn create(_tx: &mut S::WriteTransaction<'_>, info: Self::CreateInfo) -> Self {
        Self {
            name: info.name,
            index: ColumnIndex::new(info.index),
            ty: info.ty,
            is_primary_key: info.is_primary_key,
        }
    }
}
