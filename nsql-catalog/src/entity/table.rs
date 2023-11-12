mod storage;

use anyhow::Result;
use nsql_core::LogicalType;
use nsql_derive::{FromTuple, IntoTuple};
use nsql_storage::expr::Expr;
use nsql_storage::value::Value;
use nsql_storage_engine::{ExecutionMode, FallibleIterator, ReadWriteExecutionMode, StorageEngine};

pub use self::storage::{
    ColumnStorageInfo, IndexStorageInfo, PrimaryKeyConflict, TableStorage, TableStorageInfo,
};
use super::*;
use crate::bootstrap::{BootstrapColumn, BootstrapSequence};
use crate::{
    Catalog, Column, ColumnIdentity, Function, Index, Name, Namespace, Oid, SystemEntity,
    SystemEntityPrivate, TransactionContext,
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
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<TableStorage<'env, 'txn, S, M>> {
        Ok(TableStorage::open(
            catalog.storage,
            tx,
            self.table_storage_info(catalog, tx)?,
            self.index_storage_infos(catalog, tx)?,
        )?)
    }

    pub fn get_or_create_storage<'env, 'txn, S: StorageEngine>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, ReadWriteExecutionMode>,
    ) -> Result<TableStorage<'env, 'txn, S, ReadWriteExecutionMode>> {
        let storage = catalog.storage();
        Ok(TableStorage::create(
            storage,
            tx,
            self.table_storage_info(catalog, tx)?,
            self.index_storage_infos(catalog, tx)?,
        )?)
    }

    pub(crate) fn table_storage_info<
        'env: 'txn,
        'txn,
        S: StorageEngine,
        M: ExecutionMode<'env, S>,
    >(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<TableStorageInfo> {
        Ok(TableStorageInfo::new(
            self.oid,
            self.columns(catalog, tx)?.iter().map(|c| c.into()).collect(),
        ))
    }

    pub fn columns<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Box<[Column]>> {
        let mk_columns = || {
            let mut columns = catalog
                .columns(tx)?
                .as_ref()
                .scan(..)?
                .filter(|col| Ok(col.table == self.oid))
                .collect::<Vec<_>>()?;
            assert!(
                !columns.is_empty(),
                "no columns found for table `{}` `{}`",
                self.oid,
                self.name
            );

            columns.sort_by_key(|col| col.index());

            Ok(columns.into_boxed_slice())
        };

        if M::READONLY {
            tx.catalog_caches()
                .table_columns
                .entry(self.oid)
                .or_try_insert_with(mk_columns)
                .map(|r| r.value().clone())
        } else {
            // we could cache this too, but we would need to update the cache on any column updates
            mk_columns()
        }
    }

    #[inline]
    pub fn namespace(&self) -> Oid<Namespace> {
        self.namespace
    }

    /// Returns all indexes on this table.
    fn indexes<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Vec<Index>> {
        catalog
            .indexes(tx)?
            .as_ref()
            .scan(..)?
            .filter(|index| Ok(index.target == self.oid))
            .collect()
    }

    fn index_storage_infos<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Vec<IndexStorageInfo>> {
        self.indexes(catalog, tx)?
            .into_iter()
            .map(|index| index.storage_info(catalog, tx))
            .collect()
    }

    pub fn create_storage<'env, S: StorageEngine>(
        &self,
        storage: &'env S,
        tx: &S::WriteTransaction<'env>,
    ) -> Result<()> {
        storage.open_write_tree(tx, &self.oid.to_string())?;
        Ok(())
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
    fn name<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
        _catalog: Catalog<'env, S>,
        _tcx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Name> {
        Ok(Name::clone(&self.name))
    }

    #[inline]
    fn desc() -> &'static str {
        "table"
    }

    #[inline]
    fn parent_oid<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
        _catalog: Catalog<'env, S>,
        _tcx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Option<Oid<Self::Parent>>> {
        Ok(Some(self.namespace))
    }

    fn extract_cache<'a, 'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        caches: &'a TransactionLocalCatalogCaches<'env, 'txn, S, M>,
    ) -> &'a OnceLock<SystemTableView<'env, 'txn, S, M, Self>> {
        &caches.tables
    }
}

impl SystemEntityPrivate for Table {
    const TABLE: Oid<Table> = Table::TABLE;

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
            BootstrapColumn { ty: LogicalType::Oid, name: "namespace", ..Default::default() },
            BootstrapColumn { ty: LogicalType::Text, name: "name", ..Default::default() },
        ]
    }
}
