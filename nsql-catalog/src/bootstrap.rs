use nsql_core::LogicalType;
use nsql_storage::tuple::{FromTuple, FromTupleError, IntoTuple, Tuple};
use nsql_storage::value::Value;
use nsql_storage::{ColumnStorageInfo, Result, TableStorageInfo};
use nsql_storage_engine::{ReadWriteExecutionMode, StorageEngine};

use crate::system_table::{SystemEntity, SystemTableView};
use crate::{Column, ColumnIndex, Namespace, Oid, Table};

pub(crate) fn bootstrap<'env, S: StorageEngine>(
    storage: &'env S,
    txn: &S::WriteTransaction<'env>,
) -> Result<()> {
    tracing::debug!("bootstrapping namespaces");
    let mut namespace_table =
        SystemTableView::<S, ReadWriteExecutionMode, BootstrapNamespace>::new(storage, txn)?;
    for namespace in bootstrap_nsql_namespaces() {
        namespace_table.insert(namespace)?;
    }

    tracing::debug!("bootstrapping tables");
    let mut table_table =
        SystemTableView::<S, ReadWriteExecutionMode, BootstrapTable>::new(storage, txn)?;
    for table in bootstrap_nsql_tables() {
        table_table.insert(table)?;
    }

    tracing::debug!("bootstrapping columns");
    let mut column_table =
        SystemTableView::<S, ReadWriteExecutionMode, BootstrapColumn>::new(storage, txn)?;
    for column in bootstrap_nsql_column() {
        column_table.insert(column)?;
    }

    tracing::debug!("bootstrapping types");
    let mut ty_table = SystemTableView::<S, ReadWriteExecutionMode, Type>::new(storage, txn)?;
    for ty in bootstrap_nsql_types() {
        ty_table.insert(ty)?;
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CatalogPath<T: SystemEntity> {
    oid: Oid<T>,
    parent_oid: Option<Oid<T::Parent>>,
}

impl<T: SystemEntity> CatalogPath<T> {
    #[inline]
    pub fn new(oid: Oid<T>, parent_oid: Option<Oid<T::Parent>>) -> Self {
        Self { oid, parent_oid }
    }

    #[inline]
    pub fn oid(&self) -> Oid<T> {
        self.oid
    }

    #[inline]
    pub fn parent_oid(&self) -> Option<Oid<<T as SystemEntity>::Parent>> {
        self.parent_oid
    }
}

pub type BootstrapNamespace = Namespace;

pub type BootstrapTable = Table;

type BootstrapColumn = Column;

mod oid {
    use super::*;

    pub(super) const TABLE_NAMESPACE: Oid<BootstrapTable> = Oid::new(100);
    pub(super) const TABLE_TABLE: Oid<BootstrapTable> = Oid::new(101);
    pub(super) const TABLE_ATTRIBUTE: Oid<BootstrapTable> = Oid::new(102);
    pub(super) const TABLE_TYPE: Oid<BootstrapTable> = Oid::new(103);

    pub(super) const TY_OID: Oid<Type> = Oid::new(100);
    pub(super) const TY_BOOL: Oid<Type> = Oid::new(101);
    pub(super) const TY_INT: Oid<Type> = Oid::new(102);
    pub(super) const TY_TEXT: Oid<Type> = Oid::new(103);
}

fn bootstrap_nsql_namespaces() -> Vec<Namespace> {
    vec![
        Namespace { oid: Namespace::MAIN, name: "main".into() },
        Namespace { oid: Namespace::CATALOG, name: "nsql_catalog".into() },
    ]
}

fn bootstrap_nsql_tables() -> Vec<BootstrapTable> {
    vec![
        BootstrapTable {
            oid: oid::TABLE_NAMESPACE,
            name: "nsql_namespace".into(),
            namespace: Namespace::CATALOG,
        },
        BootstrapTable {
            oid: oid::TABLE_TABLE,
            name: "nsql_table".into(),
            namespace: Namespace::CATALOG,
        },
        BootstrapTable {
            oid: oid::TABLE_ATTRIBUTE,
            name: "nsql_attribute".into(),
            namespace: Namespace::CATALOG,
        },
        BootstrapTable {
            oid: oid::TABLE_TYPE,
            name: "nsql_type".into(),
            namespace: Namespace::CATALOG,
        },
    ]
}

fn bootstrap_nsql_column() -> Vec<BootstrapColumn> {
    vec![
        BootstrapColumn {
            oid: Oid::new(0),
            name: "oid".into(),
            table: oid::TABLE_NAMESPACE,
            index: ColumnIndex::new(0),
            ty: oid::TY_OID,
            is_primary_key: true,
        },
        BootstrapColumn {
            oid: Oid::new(1),
            name: "name".into(),
            table: oid::TABLE_NAMESPACE,
            index: ColumnIndex::new(1),
            ty: oid::TY_TEXT,
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(2),
            name: "oid".into(),
            table: oid::TABLE_TABLE,
            index: ColumnIndex::new(0),
            ty: oid::TY_OID,
            is_primary_key: true,
        },
        BootstrapColumn {
            oid: Oid::new(3),
            name: "name".into(),
            table: oid::TABLE_TABLE,
            index: ColumnIndex::new(1),
            ty: oid::TY_TEXT,
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(4),
            name: "oid".into(),
            table: oid::TABLE_ATTRIBUTE,
            index: ColumnIndex::new(0),
            ty: oid::TY_OID,
            is_primary_key: true,
        },
        BootstrapColumn {
            oid: Oid::new(5),
            name: "table".into(),
            table: oid::TABLE_ATTRIBUTE,
            index: ColumnIndex::new(1),
            ty: oid::TY_OID,
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(6),
            name: "name".into(),
            table: oid::TABLE_ATTRIBUTE,
            index: ColumnIndex::new(2),
            ty: oid::TY_TEXT,
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(7),
            name: "index".into(),
            table: oid::TABLE_ATTRIBUTE,
            index: ColumnIndex::new(3),
            ty: oid::TY_INT,
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(8),
            name: "ty".into(),
            table: oid::TABLE_ATTRIBUTE,
            index: ColumnIndex::new(4),
            ty: oid::TY_OID,
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(9),
            name: "is_primary_key".into(),
            table: oid::TABLE_ATTRIBUTE,
            index: ColumnIndex::new(5),
            ty: oid::TY_BOOL,
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(10),
            name: "oid".into(),
            table: oid::TABLE_TYPE,
            index: ColumnIndex::new(0),
            ty: oid::TY_OID,
            is_primary_key: true,
        },
    ]
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Type {
    oid: Oid<Type>,
    name: String,
}

impl SystemEntity for Type {
    // should types be namespaced?
    type Parent = !;

    #[inline]
    fn oid(&self) -> Oid<Self> {
        self.oid
    }

    #[inline]
    fn name(&self) -> &str {
        &self.name
    }

    fn parent_oid(&self) -> Option<Oid<Self::Parent>> {
        None
    }

    fn storage_info() -> TableStorageInfo {
        TableStorageInfo::new(
            "nsql_catalog.nsql_type",
            vec![
                ColumnStorageInfo::new(LogicalType::Oid, true),
                ColumnStorageInfo::new(LogicalType::Text, false),
            ],
        )
    }

    fn desc() -> &'static str {
        "type"
    }
}

impl FromTuple for Type {
    fn from_tuple(mut tuple: Tuple) -> Result<Self, FromTupleError> {
        if tuple.len() != 2 {
            return Err(FromTupleError::ColumnCountMismatch { expected: 2, actual: tuple.len() });
        }

        Ok(Type { oid: tuple[0].take().cast_non_null()?, name: tuple[1].take().cast_non_null()? })
    }
}

impl IntoTuple for Type {
    fn into_tuple(self) -> Tuple {
        Tuple::from([Value::Oid(self.oid.untyped()), Value::Text(self.name)])
    }
}

fn bootstrap_nsql_types() -> Vec<Type> {
    vec![
        Type { oid: oid::TY_OID, name: "oid".into() },
        Type { oid: oid::TY_BOOL, name: "bool".into() },
        Type { oid: oid::TY_INT, name: "int".into() },
        Type { oid: oid::TY_TEXT, name: "text".into() },
    ]
}

impl Type {
    pub fn oid_to_logical_type(oid: Oid<Self>) -> LogicalType {
        match oid {
            oid::TY_OID => LogicalType::Oid,
            oid::TY_BOOL => LogicalType::Bool,
            oid::TY_INT => LogicalType::Int,
            oid::TY_TEXT => LogicalType::Text,
            _ => panic!(),
        }
    }

    pub fn logical_type_to_oid(logical_type: &LogicalType) -> Oid<Self> {
        match logical_type {
            LogicalType::Oid => oid::TY_OID,
            LogicalType::Bool => oid::TY_BOOL,
            LogicalType::Int => oid::TY_INT,
            LogicalType::Text => oid::TY_TEXT,
            _ => todo!(),
        }
    }
}

impl SystemEntity for ! {
    type Parent = !;

    fn storage_info() -> TableStorageInfo {
        todo!()
    }

    fn name(&self) -> &str {
        unreachable!()
    }

    fn oid(&self) -> Oid<Self> {
        unreachable!()
    }

    fn parent_oid(&self) -> Option<Oid<Self::Parent>> {
        unreachable!()
    }

    fn desc() -> &'static str {
        "catalog"
    }
}
