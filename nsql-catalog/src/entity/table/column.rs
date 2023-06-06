use std::fmt;
use std::sync::Arc;

use nsql_core::LogicalType;
use nsql_storage::ColumnStorageInfo;
use nsql_storage_engine::{StorageEngine, Transaction};

use super::TableRef;
use crate::private::CatalogEntity;
use crate::set::CatalogSet;
use crate::{Catalog, Entity, EntityRef, Name, Oid, Table};

#[derive(Clone)]
pub struct Column<S> {
    oid: Oid<Self>,
    table_oid: Oid<Table<S>>,
    name: Name,
    index: ColumnIndex,
    ty: LogicalType,
    is_primary_key: bool,
}

impl<S> From<&Column<S>> for ColumnStorageInfo {
    fn from(val: &Column<S>) -> Self {
        ColumnStorageInfo::new(val.ty.clone(), val.is_primary_key)
    }
}

impl<S> Column<S> {
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

impl<S> fmt::Debug for Column<S> {
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

impl<S> Entity for Column<S> {
    #[inline]
    fn oid(&self) -> Oid<Self> {
        self.oid
    }

    #[inline]
    fn name(&self) -> Name {
        Name::clone(&self.name)
    }

    #[inline]
    fn desc() -> &'static str {
        "column"
    }
}

impl<S: StorageEngine> CatalogEntity<S> for Column<S> {
    type Container = Table<S>;

    type CreateInfo = CreateColumnInfo;

    fn catalog_set(table: &Self::Container) -> &CatalogSet<S, Self> {
        &table.columns
    }

    fn create(
        _tx: &S::WriteTransaction<'_>,
        container: &Self::Container,
        oid: Oid<Self>,
        info: Self::CreateInfo,
    ) -> Self {
        Self {
            oid,
            table_oid: container.oid(),
            name: info.name,
            index: ColumnIndex::new(info.index),
            ty: info.ty,
            is_primary_key: info.is_primary_key,
        }
    }
}

#[derive(Debug)]
pub struct ColumnRef<S> {
    pub table_ref: TableRef<S>,
    pub column: Oid<Column<S>>,
}

impl<S> Clone for ColumnRef<S> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Copy for ColumnRef<S> {}

impl<S: StorageEngine> EntityRef<S> for ColumnRef<S> {
    type Entity = Column<S>;

    type Container = Table<S>;

    #[inline]
    fn container(self, catalog: &Catalog<S>, tx: &dyn Transaction<'_, S>) -> Arc<Self::Container> {
        self.table_ref.get(catalog, tx)
    }

    #[inline]
    fn entity_oid(self) -> Oid<Self::Entity> {
        self.column
    }
}
