mod column;

use std::fmt;
use std::sync::Arc;

// use nsql_storage::TableStorage;
use nsql_storage_engine::{StorageEngine, Transaction};

pub use self::column::{Column, ColumnIndex, ColumnRef, CreateColumnInfo};
use crate::private::CatalogEntity;
use crate::set::CatalogSet;
use crate::{Catalog, Container, Entity, EntityRef, Name, Namespace, Oid};

pub struct Table<S> {
    oid: Oid<Self>,
    name: Name,
    columns: CatalogSet<S, Column>,
    // storage: Arc<TableStorage<S>>,
}

impl<S: StorageEngine> Table<S> {
    #[inline]
    pub fn name(&self) -> &Name {
        &self.name
    }

    #[inline]
    pub fn columns(&self, tx: &impl Transaction<'_, S>) -> Vec<Arc<Column>> {
        self.all::<Column>(tx)
    }

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
    fn oid(&self) -> Oid<Self> {
        self.oid
    }

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

    fn create(_tx: &S::WriteTransaction<'_>, oid: Oid<Self>, info: Self::CreateInfo) -> Self {
        Self { oid, name: info.name, columns: Default::default() }
    }
}

impl<S: StorageEngine> Container<S> for Table<S> {}

pub struct TableRef<S> {
    pub namespace: Oid<Namespace<S>>,
    pub table: Oid<Table<S>>,
}

impl<S> fmt::Debug for TableRef<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableRef")
            .field("namespace", &self.namespace)
            .field("table", &self.table)
            .finish()
    }
}

impl<S> fmt::Display for TableRef<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.namespace, self.table)
    }
}

impl<S> Clone for TableRef<S> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Copy for TableRef<S> {}

impl<S: StorageEngine> EntityRef<S> for TableRef<S> {
    type Entity = Table<S>;

    type Container = Namespace<S>;

    #[inline]
    fn container(self, catalog: &Catalog<S>, tx: &impl Transaction<'_, S>) -> Arc<Self::Container> {
        catalog.get(tx, self.namespace).expect("namespace should exist for `tx`")
    }

    #[inline]
    fn entity_oid(self) -> Oid<Self::Entity> {
        self.table
    }
}
