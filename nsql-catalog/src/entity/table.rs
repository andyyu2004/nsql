use std::fmt;

use anyhow::Result;
use nsql_storage::TableStorage;
use nsql_storage_engine::{
    ExecutionMode, FallibleIterator, ReadWriteExecutionMode, StorageEngine, Transaction,
};

use super::*;
use crate::{Catalog, Column, Name, Namespace, Oid, SystemEntity};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    // duplicating the trait method here as it's more convenient to call for external crates
    pub fn name<'env, S: StorageEngine>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn Transaction<'env, S>,
    ) -> Result<Name> {
        Ok(Name::clone(&self.name))
    }

    pub fn storage<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
        catalog: Catalog<'env, S>,
        tx: M::TransactionRef<'txn>,
    ) -> Result<TableStorage<'env, 'txn, S, M>> {
        Ok(TableStorage::open(catalog.storage, tx, self.table_storage_info(catalog, &tx)?)?)
    }

    pub fn get_or_create_storage<'env, 'txn, S: StorageEngine>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &'txn S::WriteTransaction<'env>,
    ) -> Result<TableStorage<'env, 'txn, S, ReadWriteExecutionMode>> {
        let storage = catalog.storage();
        Ok(TableStorage::create(storage, tx, self.table_storage_info(catalog, tx)?)?)
    }

    fn table_storage_info<'env, S: StorageEngine>(
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

    type Id = Oid<Self>;

    #[inline]
    fn id(&self) -> Oid<Self> {
        self.oid
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

    fn storage_info() -> TableStorageInfo {
        TableStorageInfo::new(
            Table::TABLE.untyped(),
            vec![
                ColumnStorageInfo::new(LogicalType::Oid, true),
                ColumnStorageInfo::new(LogicalType::Oid, false),
                ColumnStorageInfo::new(LogicalType::Text, false),
            ],
        )
    }
}

impl IntoTuple for Table {
    fn into_tuple(self) -> Tuple {
        Tuple::from([
            Value::Oid(self.oid.untyped()),
            Value::Oid(self.namespace.untyped()),
            Value::Text(self.name.to_string()),
        ])
    }
}

impl FromTuple for Table {
    fn from_values(mut values: impl Iterator<Item = Value>) -> Result<Self, FromTupleError> {
        Ok(Self {
            oid: values.next().ok_or(FromTupleError::NotEnoughValues)?.cast_non_null()?,
            namespace: values.next().ok_or(FromTupleError::NotEnoughValues)?.cast_non_null()?,
            name: values.next().ok_or(FromTupleError::NotEnoughValues)?.cast_non_null()?,
        })
    }
}
