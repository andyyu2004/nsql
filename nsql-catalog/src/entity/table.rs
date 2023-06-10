mod column;

use std::fmt;
use std::sync::Arc;

use nsql_core::LogicalType;
use nsql_storage::tuple::{FromTuple, FromTupleError, IntoTuple, Tuple};
use nsql_storage::value::Value;
use nsql_storage::{ColumnStorageInfo, TableStorage, TableStorageInfo};
use nsql_storage_engine::{ExecutionMode, ReadWriteExecutionMode, StorageEngine, Transaction};

pub use self::column::{Column, ColumnIndex, CreateColumnInfo};
use crate::{BootstrapNamespace, BootstrapTable, Entity, Name, Oid, SystemEntity};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Table {
    pub(crate) oid: Oid<Self>,
    pub(crate) namespace: Oid<BootstrapNamespace>,
    pub(crate) name: Name,
}

impl Table {
    #[inline]
    pub fn name(&self) -> &Name {
        &self.name
    }

    #[inline]
    pub fn columns<S: StorageEngine>(&self, tx: &dyn Transaction<'_, S>) -> Vec<Arc<Column>> {
        todo!()
        // self.all::<Column<S>>(tx)
    }

    pub fn storage<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
        storage: &S,
        tx: M::TransactionRef<'txn>,
    ) -> Result<TableStorage<'env, 'txn, S, M>, S::Error> {
        TableStorage::open(
            storage,
            tx,
            TableStorageInfo::new(
                Name::from(format!("{}", TableRef { namespace: self.namespace, table: self.oid })),
                self.columns(tx.as_dyn()).iter().map(|c| c.as_ref().into()).collect(),
            ),
        )
    }

    pub fn get_or_create_storage<'env, 'txn, S: StorageEngine>(
        &self,
        storage: &S,
        tx: &'txn S::WriteTransaction<'env>,
    ) -> Result<TableStorage<'env, 'txn, S, ReadWriteExecutionMode>, S::Error> {
        TableStorage::create(
            storage,
            tx,
            TableStorageInfo::new(
                Name::from(format!("{}", TableRef { namespace: self.namespace, table: self.oid })),
                self.columns(tx).iter().map(|c| c.as_ref().into()).collect(),
            ),
        )
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
//     fn catalog_set(container: &Self::Container) -> &CatalogSet<S, Self> {
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
    pub namespace: Oid<BootstrapNamespace>,
    pub table: Oid<BootstrapTable>,
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
//     type Entity = BootstrapTable;
//
//     type Container = Namespace;
//
//     #[inline]
//     fn container(self, catalog: &Catalog, tx: &dyn Transaction<'_, S>) -> Arc<Self::Container> {
//         catalog.get(tx, self.namespace).expect("namespace should exist for `tx`")
//     }
//
//     #[inline]
//     fn entity_oid(self) -> Oid<Self::Entity> {
//         self.table
//     }
// }

impl SystemEntity for BootstrapTable {
    type Parent = BootstrapNamespace;

    #[inline]
    fn name(&self) -> &str {
        &self.name
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

    #[inline]
    fn oid(&self) -> Oid<Self> {
        self.oid
    }

    #[inline]
    fn parent_oid(&self) -> Option<Oid<Self::Parent>> {
        Some(self.namespace)
    }

    fn desc() -> &'static str {
        "table"
    }
}

impl IntoTuple for BootstrapTable {
    fn into_tuple(self) -> Tuple {
        Tuple::from([
            Value::Oid(self.oid.untyped()),
            Value::Oid(self.namespace.untyped()),
            Value::Text(self.name.to_string()),
        ])
    }
}

impl FromTuple for BootstrapTable {
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
