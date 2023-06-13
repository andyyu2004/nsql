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
        SystemTableView::<S, ReadWriteExecutionMode, Namespace>::new(storage, txn)?;
    for namespace in bootstrap_nsql_namespaces() {
        namespace_table.insert(namespace)?;
    }

    tracing::debug!("bootstrapping tables");
    let mut table_table = SystemTableView::<S, ReadWriteExecutionMode, Table>::new(storage, txn)?;
    for table in bootstrap_nsql_tables() {
        table_table.insert(table)?;
    }

    tracing::debug!("bootstrapping columns");
    let mut column_table = SystemTableView::<S, ReadWriteExecutionMode, Column>::new(storage, txn)?;
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

pub(super) mod oid {
    use super::*;

    pub(crate) const TABLE_NAMESPACE: Oid<Table> = Oid::new(100);
    pub(crate) const TABLE_TABLE: Oid<Table> = Oid::new(101);
    pub(crate) const TABLE_ATTRIBUTE: Oid<Table> = Oid::new(102);
    pub(crate) const TABLE_TYPE: Oid<Table> = Oid::new(103);

    pub(crate) const TY_OID: Oid<Type> = Oid::new(100);
    pub(crate) const TY_BOOL: Oid<Type> = Oid::new(101);
    pub(crate) const TY_INT: Oid<Type> = Oid::new(102);
    pub(crate) const TY_TEXT: Oid<Type> = Oid::new(103);
}

fn bootstrap_nsql_namespaces() -> Vec<Namespace> {
    vec![
        Namespace { oid: Namespace::MAIN, name: "main".into() },
        Namespace { oid: Namespace::CATALOG, name: "nsql_catalog".into() },
    ]
}

fn bootstrap_nsql_tables() -> Vec<Table> {
    vec![
        Table {
            oid: oid::TABLE_NAMESPACE,
            name: "nsql_namespace".into(),
            namespace: Namespace::CATALOG,
        },
        Table { oid: oid::TABLE_TABLE, name: "nsql_table".into(), namespace: Namespace::CATALOG },
        Table {
            oid: oid::TABLE_ATTRIBUTE,
            name: "nsql_attribute".into(),
            namespace: Namespace::CATALOG,
        },
        Table { oid: oid::TABLE_TYPE, name: "nsql_type".into(), namespace: Namespace::CATALOG },
    ]
}

fn bootstrap_nsql_column() -> Vec<Column> {
    vec![
        Column {
            oid: Oid::new(0),
            name: "oid".into(),
            table: oid::TABLE_NAMESPACE,
            index: ColumnIndex::new(0),
            ty: oid::TY_OID,
            is_primary_key: true,
        },
        Column {
            oid: Oid::new(1),
            name: "name".into(),
            table: oid::TABLE_NAMESPACE,
            index: ColumnIndex::new(1),
            ty: oid::TY_TEXT,
            is_primary_key: false,
        },
        Column {
            oid: Oid::new(2),
            name: "oid".into(),
            table: oid::TABLE_TABLE,
            index: ColumnIndex::new(0),
            ty: oid::TY_OID,
            is_primary_key: true,
        },
        Column {
            oid: Oid::new(3),
            name: "namespace".into(),
            table: oid::TABLE_TABLE,
            index: ColumnIndex::new(1),
            ty: oid::TY_OID,
            is_primary_key: false,
        },
        Column {
            oid: Oid::new(4),
            name: "name".into(),
            table: oid::TABLE_TABLE,
            index: ColumnIndex::new(2),
            ty: oid::TY_TEXT,
            is_primary_key: false,
        },
        Column {
            oid: Oid::new(5),
            name: "oid".into(),
            table: oid::TABLE_ATTRIBUTE,
            index: ColumnIndex::new(0),
            ty: oid::TY_OID,
            is_primary_key: true,
        },
        Column {
            oid: Oid::new(6),
            name: "table".into(),
            table: oid::TABLE_ATTRIBUTE,
            index: ColumnIndex::new(1),
            ty: oid::TY_OID,
            is_primary_key: false,
        },
        Column {
            oid: Oid::new(7),
            name: "name".into(),
            table: oid::TABLE_ATTRIBUTE,
            index: ColumnIndex::new(2),
            ty: oid::TY_TEXT,
            is_primary_key: false,
        },
        Column {
            oid: Oid::new(8),
            name: "index".into(),
            table: oid::TABLE_ATTRIBUTE,
            index: ColumnIndex::new(3),
            ty: oid::TY_INT,
            is_primary_key: false,
        },
        Column {
            oid: Oid::new(9),
            name: "ty".into(),
            table: oid::TABLE_ATTRIBUTE,
            index: ColumnIndex::new(4),
            ty: oid::TY_OID,
            is_primary_key: false,
        },
        Column {
            oid: Oid::new(10),
            name: "is_primary_key".into(),
            table: oid::TABLE_ATTRIBUTE,
            index: ColumnIndex::new(5),
            ty: oid::TY_BOOL,
            is_primary_key: false,
        },
        Column {
            oid: Oid::new(11),
            name: "oid".into(),
            table: oid::TABLE_TYPE,
            index: ColumnIndex::new(0),
            ty: oid::TY_OID,
            is_primary_key: true,
        },
        Column {
            oid: Oid::new(12),
            name: "name".into(),
            table: oid::TABLE_TYPE,
            index: ColumnIndex::new(1),
            ty: oid::TY_TEXT,
            is_primary_key: false,
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
    type Parent = ();

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
            oid::TABLE_TYPE.untyped(),
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

impl SystemEntity for () {
    type Parent = ();

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
