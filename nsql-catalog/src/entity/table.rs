mod column;

use std::fmt;

use anyhow::Result;
use nsql_core::LogicalType;
use nsql_storage::tuple::{FromTuple, FromTupleError, IntoTuple, Tuple};
use nsql_storage::value::Value;
use nsql_storage::{ColumnStorageInfo, TableStorage, TableStorageInfo};
use nsql_storage_engine::{
    ExecutionMode, FallibleIterator, ReadWriteExecutionMode, StorageEngine, Transaction,
};

pub use self::column::{Column, ColumnIndex, CreateColumnInfo};
use crate::{Catalog, Entity, Name, Namespace, Oid, SystemEntity};

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
    pub fn name(&self) -> &Name {
        &self.name
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
            Name::from(format!("{}", TableRef { namespace: self.namespace, table: self.oid })),
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

impl Entity for Table {
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

// impl CatalogEntity for Table {
//     type Container = Namespace;
//
//     type CreateInfo = CreateTableInfo;
//
//     fn catalog_set(container: &Self::Container) -> Catalog<'_, S>,Set<S, Self> {
//         &container.tables
//     }
//
//     fn create(
//         _tx: &S::WriteTransaction<'_>,
//         container: &Self::Container,
//         oid: Oid<Self>,
//         info: Self::CreateInfo,
//     ) -> Self {
//         Self { oid, namespace: container.oid(), name: info.name, columns: Default::default() }
//     }
// }
//
// impl Container for Table {}

pub struct TableRef {
    pub namespace: Oid<Namespace>,
    pub table: Oid<Table>,
}

impl fmt::Debug for TableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableRef")
            .field("namespace", &self.namespace)
            .field("table", &self.table)
            .finish()
    }
}

impl fmt::Display for TableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.namespace, self.table)
    }
}

impl Clone for TableRef {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl Copy for TableRef {}

// impl EntityRef for TableRef {
//     type Entity = Table;
//
//     type Container = Namespace;
//
//     #[inline]
//     fn container(self, catalog: Catalog<'_, S>, tx: &dyn Transaction<'_, S>) -> Arc<Self::Container> {
//         catalog.get(tx, self.namespace).expect("namespace should exist for `tx`")
//     }
//
//     #[inline]
//     fn entity_oid(self) -> Oid<Self::Entity> {
//         self.table
//     }
// }

impl SystemEntity for Table {
    type Parent = Namespace;

    #[inline]
    fn oid(&self) -> Oid<Self> {
        self.oid
    }

    #[inline]
    fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    fn parent_oid(&self) -> Option<Oid<Self::Parent>> {
        Some(self.namespace)
    }

    fn storage_info() -> TableStorageInfo {
        TableStorageInfo::new(
            "nsql_catalog.nsql_table",
            vec![
                ColumnStorageInfo::new(LogicalType::Oid, true),
                ColumnStorageInfo::new(LogicalType::Oid, false),
                ColumnStorageInfo::new(LogicalType::Text, false),
            ],
        )
    }

    fn desc() -> &'static str {
        "table"
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
    fn from_tuple(mut tuple: Tuple) -> Result<Self, FromTupleError> {
        if tuple.len() != 3 {
            return Err(FromTupleError::ColumnCountMismatch { expected: 3, actual: tuple.len() });
        }

        Ok(Self {
            oid: tuple[0].take().cast_non_null()?,
            namespace: tuple[1].take().cast_non_null()?,
            name: tuple[2].take().cast_non_null()?,
        })
    }
}
