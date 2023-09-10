use anyhow::Result;
use nsql_storage::eval::Expr;
use nsql_storage::{IndexStorageInfo, TableStorage, TableStorageInfo};
use nsql_storage_engine::{
    ExecutionMode, FallibleIterator, ReadWriteExecutionMode, StorageEngine, Transaction,
};

use super::*;
use crate::{
    Catalog, Column, ColumnIdentity, Function, Index, Name, Namespace, Oid, SystemEntity,
    SystemEntityPrivate,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, FromTuple, IntoTuple)]
pub struct Table {
    pub(crate) oid: Oid<Self>,
    pub(crate) namespace: Oid<Namespace>,
    pub(crate) name: Name,
}

impl Table {
    #[inline]
    pub fn name(&self) -> Name {
        Name::clone(&self.name)
    }

    #[track_caller]
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
        assert!(!columns.is_empty(), "no columns found for table `{}` `{}`", self.oid, self.name);

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
    ) -> Result<(), S::Error> {
        storage.open_write_tree(tx, &TableStorageInfo::derive_name(self.oid.untyped()))?;
        Ok(())
    }

    pub fn create_storage<'env, S: StorageEngine>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &S::WriteTransaction<'env>,
    ) -> Result<()> {
        Ok(self.create_storage_for_bootstrap(catalog.storage, tx)?)
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
}

impl SystemEntityPrivate for Table {
    #[inline]
    fn bootstrap_column_info() -> Vec<BootstrapColumn> {
        vec![
            BootstrapColumn {
                ty: LogicalType::Oid,
                name: "oid",
                is_primary_key: true,
                identity: ColumnIdentity::Always,
                default_expr: Expr::call(
                    Function::NEXTVAL_OID.untyped(),
                    [Value::Oid(Table::TABLE_OID_SEQ.untyped())],
                ),
                seq: Some(BootstrapSequence {
                    table: Table::TABLE_OID_SEQ,
                    name: "nsql_table_oid_seq",
                }),
            },
            BootstrapColumn {
                ty: LogicalType::Oid,
                name: "namespace",
                is_primary_key: false,
                identity: ColumnIdentity::None,
                default_expr: Expr::null(),
                seq: None,
            },
            BootstrapColumn {
                ty: LogicalType::Text,
                name: "name",
                is_primary_key: false,
                identity: ColumnIdentity::None,
                default_expr: Expr::null(),
                seq: None,
            },
        ]
    }

    #[inline]
    fn table() -> Oid<Table> {
        Table::TABLE
    }
}
