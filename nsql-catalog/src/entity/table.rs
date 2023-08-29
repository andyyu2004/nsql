use std::fmt;

use anyhow::Result;
use nsql_storage::{IndexStorageInfo, TableStorage, TableStorageInfo};
use nsql_storage_engine::{
    ExecutionMode, FallibleIterator, ReadWriteExecutionMode, StorageEngine, Transaction,
};

use super::*;
use crate::{
    Catalog, Column, ColumnIdentity, ColumnIndex, Index, Name, Namespace, Oid, SystemEntity,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, FromTuple, IntoTuple)]
pub struct Table {
    pub(crate) oid: Oid<Self>,
    pub(crate) namespace: Oid<Namespace>,
    pub(crate) name: Name,
}

impl Table {
    pub fn new(namespace: Oid<Namespace>, name: Name) -> Self {
        Self { oid: crate::hack_new_oid_tmp(), namespace, name }
    }

    #[inline]
    pub fn name(&self) -> Name {
        Name::clone(&self.name)
    }

    pub fn storage<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
        catalog: Catalog<'env, S>,
        tx: M::TransactionRef<'txn>,
    ) -> Result<TableStorage<'env, 'txn, S, M>> {
        Ok(TableStorage::open(
            catalog.storage,
            tx,
            self.table_storage_info(catalog, &tx)?,
            self.index_storage_infos(catalog, &tx)?,
        )?)
    }

    pub fn get_or_create_storage<'env, 'txn, S: StorageEngine>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &'txn S::WriteTransaction<'env>,
    ) -> Result<TableStorage<'env, 'txn, S, ReadWriteExecutionMode>> {
        let storage = catalog.storage();
        Ok(TableStorage::create(
            storage,
            tx,
            self.table_storage_info(catalog, tx)?,
            self.index_storage_infos(catalog, tx)?,
        )?)
    }

    pub(crate) fn table_storage_info<'env, S: StorageEngine>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<TableStorageInfo> {
        Ok(TableStorageInfo::new(
            self.oid.untyped(),
            self.columns(catalog, tx)?.iter().map(|c| c.into()).collect(),
        ))
    }

    pub fn columns<'env, S: StorageEngine>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<Vec<Column>> {
        let mut columns = catalog
            .columns(tx)?
            .scan()?
            .filter(|col| Ok(col.table == self.oid))
            .collect::<Vec<_>>()?;
        assert!(!columns.is_empty(), "no columns found for table `{}` {}`", self.oid, self.name);

        columns.sort_by_key(|col| col.index());

        Ok(columns)
    }

    #[inline]
    pub fn namespace(&self) -> Oid<Namespace> {
        self.namespace
    }

    /// Returns all indexes on this table.
    fn indexes<'env, S: StorageEngine>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<Vec<Index>> {
        catalog
            .system_table::<Index>(tx)?
            .scan()?
            .filter(|index| Ok(index.target == self.oid))
            .collect()
    }

    fn index_storage_infos<'env, S: StorageEngine>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<Vec<IndexStorageInfo>> {
        self.indexes(catalog, tx)?
            .into_iter()
            .map(|index| index.storage_info(catalog, tx))
            .collect()
    }

    pub(crate) fn create_storage_for_bootstrap<'env, S: StorageEngine>(
        &self,
        storage: &'env S,
        tx: &S::WriteTransaction<'env>,
        oid: Oid<Table>,
    ) -> Result<(), S::Error> {
        storage.open_write_tree(tx, &TableStorageInfo::derive_name(oid.untyped()))?;
        Ok(())
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

impl SystemEntity for Table {
    type Parent = Namespace;

    type Key = Oid<Self>;

    type SearchKey = Name;

    #[inline]
    fn key(&self) -> Oid<Self> {
        self.oid
    }

    #[inline]
    fn search_key(&self) -> Self::SearchKey {
        self.name()
    }

    #[inline]
    fn name<'env, S: StorageEngine>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn Transaction<'env, S>,
    ) -> Result<Name> {
        Ok(Name::clone(&self.name))
    }

    #[inline]
    fn desc() -> &'static str {
        "table"
    }

    #[inline]
    fn parent_oid<'env, S: StorageEngine>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn Transaction<'env, S>,
    ) -> Result<Option<Oid<Self::Parent>>> {
        Ok(Some(self.namespace))
    }

    #[inline]
    fn bootstrap_column_info() -> Vec<Column> {
        let table = Self::table();

        vec![
            Column {
                table,
                index: ColumnIndex::new(0),
                ty: LogicalType::Oid,
                name: "oid".into(),
                is_primary_key: true,
                identity: ColumnIdentity::None,
            },
            Column {
                table,
                index: ColumnIndex::new(1),
                ty: LogicalType::Oid,
                name: "namespace".into(),
                is_primary_key: false,
                identity: ColumnIdentity::None,
            },
            Column {
                table,
                index: ColumnIndex::new(2),
                ty: LogicalType::Text,
                name: "name".into(),
                is_primary_key: false,
                identity: ColumnIdentity::None,
            },
        ]
    }

    #[inline]
    fn table() -> Oid<Table> {
        Table::TABLE
    }
}
