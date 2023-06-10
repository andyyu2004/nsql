use nsql_core::LogicalType;
use nsql_storage::tuple::{FromTuple, FromTupleError, IntoTuple, Tuple};
use nsql_storage::value::Value;
use nsql_storage::{ColumnStorageInfo, Result, TableStorageInfo};
use nsql_storage_engine::{ReadWriteExecutionMode, StorageEngine};

use crate::system_table::{SystemEntity, SystemTableView};
use crate::{Column, ColumnIndex, Oid, Table};

pub(crate) fn bootstrap<'env, S: StorageEngine>(
    storage: &'env S,
    txn: &S::WriteTransaction<'env>,
) -> Result<()> {
    let mut namespace_table =
        SystemTableView::<S, ReadWriteExecutionMode, BootstrapNamespace>::new(storage, txn)?;

    for namespace in bootstrap_nsql_namespaces() {
        namespace_table.insert(namespace)?;
    }

    let mut table_table =
        SystemTableView::<S, ReadWriteExecutionMode, BootstrapTable>::new(storage, txn)?;

    let tables = bootstrap_nsql_tables();
    for table in tables {
        table_table.insert(table)?;
    }

    let mut column_table =
        SystemTableView::<S, ReadWriteExecutionMode, BootstrapColumn>::new(storage, txn)?;

    let columns = bootstrap_nsql_column();
    for column in columns {
        column_table.insert(column)?;
    }

    let mut ty_table =
        SystemTableView::<S, ReadWriteExecutionMode, BootstrapType>::new(storage, txn)?;
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

pub type Namespace = BootstrapNamespace;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BootstrapNamespace {
    oid: Oid<BootstrapNamespace>,
    name: String,
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

impl SystemEntity for BootstrapNamespace {
    type Parent = !;

    fn storage_info() -> TableStorageInfo {
        TableStorageInfo::new(
            "nsql_catalog.nsql_namespace",
            vec![
                ColumnStorageInfo::new(LogicalType::Oid, true),
                ColumnStorageInfo::new(LogicalType::Text, false),
            ],
        )
    }

    #[inline]
    fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    fn oid(&self) -> Oid<Self> {
        self.oid
    }

    #[inline]
    fn parent_oid(&self) -> Option<Oid<Self::Parent>> {
        None
    }

    fn desc() -> &'static str {
        "namespace"
    }
}

impl FromTuple for BootstrapNamespace {
    fn from_tuple(mut tuple: Tuple) -> Result<Self, FromTupleError> {
        if tuple.len() != 2 {
            return Err(FromTupleError::ColumnCountMismatch { expected: 2, actual: tuple.len() });
        }

        Ok(Self { oid: tuple[0].take().cast_non_null()?, name: tuple[1].take().cast_non_null()? })
    }
}

impl IntoTuple for BootstrapNamespace {
    fn into_tuple(self) -> Tuple {
        Tuple::from([Value::Oid(self.oid.untyped()), Value::Text(self.name)])
    }
}

pub type BootstrapTable = Table;

type BootstrapColumn = Column;

const fn catalog_namespace_oid() -> Oid<BootstrapNamespace> {
    Oid::new(100)
}

const fn main_namespace_oid() -> Oid<BootstrapNamespace> {
    Oid::new(101)
}

const fn nsql_namespace_table_oid() -> Oid<BootstrapTable> {
    Oid::new(100)
}

const fn nsql_table_table_oid() -> Oid<BootstrapTable> {
    Oid::new(101)
}

const fn nsql_attribute_table_oid() -> Oid<BootstrapTable> {
    Oid::new(102)
}

const fn nsql_ty_table_oid() -> Oid<BootstrapTable> {
    Oid::new(103)
}

const fn nsql_ty_oid_oid() -> Oid<BootstrapType> {
    Oid::new(100)
}

const fn nsql_ty_bool_oid() -> Oid<BootstrapType> {
    Oid::new(101)
}

const fn nsql_ty_int_oid() -> Oid<BootstrapType> {
    Oid::new(102)
}

const fn nsql_ty_text_oid() -> Oid<BootstrapType> {
    Oid::new(103)
}

fn bootstrap_nsql_namespaces() -> Vec<BootstrapNamespace> {
    vec![
        BootstrapNamespace { oid: main_namespace_oid(), name: "main".into() },
        BootstrapNamespace { oid: catalog_namespace_oid(), name: "nsql_catalog".into() },
    ]
}

fn bootstrap_nsql_tables() -> Vec<BootstrapTable> {
    vec![
        BootstrapTable {
            oid: nsql_namespace_table_oid(),
            name: "nsql_namespace".into(),
            namespace: catalog_namespace_oid(),
        },
        BootstrapTable {
            oid: nsql_table_table_oid(),
            name: "nsql_table.into()".into(),
            namespace: catalog_namespace_oid(),
        },
        BootstrapTable {
            oid: nsql_attribute_table_oid(),
            name: "nsql_attribute.into()".into(),
            namespace: catalog_namespace_oid(),
        },
        BootstrapTable {
            oid: nsql_ty_table_oid(),
            name: "nsql_ty.into()".into(),
            namespace: catalog_namespace_oid(),
        },
    ]
}

fn bootstrap_nsql_column() -> Vec<BootstrapColumn> {
    vec![
        BootstrapColumn {
            oid: Oid::new(0),
            name: "oid".into(),
            table: nsql_namespace_table_oid(),
            index: ColumnIndex::new(0),
            ty: nsql_ty_oid_oid(),
            is_primary_key: true,
        },
        BootstrapColumn {
            oid: Oid::new(1),
            name: "name".into(),
            table: nsql_namespace_table_oid(),
            index: ColumnIndex::new(1),
            ty: nsql_ty_text_oid(),
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(2),
            name: "oid".into(),
            table: nsql_table_table_oid(),
            index: ColumnIndex::new(0),
            ty: nsql_ty_oid_oid(),
            is_primary_key: true,
        },
        BootstrapColumn {
            oid: Oid::new(3),
            name: "name".into(),
            table: nsql_table_table_oid(),
            index: ColumnIndex::new(1),
            ty: nsql_ty_text_oid(),
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(4),
            name: "oid".into(),
            table: nsql_attribute_table_oid(),
            index: ColumnIndex::new(0),
            ty: nsql_ty_oid_oid(),
            is_primary_key: true,
        },
        BootstrapColumn {
            oid: Oid::new(5),
            name: "table".into(),
            table: nsql_attribute_table_oid(),
            index: ColumnIndex::new(1),
            ty: nsql_ty_oid_oid(),
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(6),
            name: "name".into(),
            table: nsql_attribute_table_oid(),
            index: ColumnIndex::new(2),
            ty: nsql_ty_text_oid(),
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(7),
            name: "index".into(),
            table: nsql_attribute_table_oid(),
            index: ColumnIndex::new(3),
            ty: nsql_ty_int_oid(),
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(8),
            name: "ty".into(),
            table: nsql_attribute_table_oid(),
            index: ColumnIndex::new(4),
            // text for now, but should probably reference `nsql_type` table
            ty: nsql_ty_text_oid(),
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(9),
            name: "is_primary_key".into(),
            table: nsql_attribute_table_oid(),
            index: ColumnIndex::new(5),
            ty: nsql_ty_bool_oid(),
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(10),
            name: "oid".into(),
            table: nsql_ty_table_oid(),
            index: ColumnIndex::new(0),
            ty: nsql_ty_oid_oid(),
            is_primary_key: true,
        },
    ]
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BootstrapType {
    oid: Oid<BootstrapType>,
    name: String,
}

impl SystemEntity for BootstrapType {
    // should types be namespaced?
    type Parent = !;

    #[inline]
    fn name(&self) -> &str {
        &self.name
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

    #[inline]
    fn oid(&self) -> Oid<Self> {
        self.oid
    }

    fn parent_oid(&self) -> Option<Oid<Self::Parent>> {
        None
    }

    fn desc() -> &'static str {
        "type"
    }
}

impl FromTuple for BootstrapType {
    fn from_tuple(mut tuple: Tuple) -> Result<Self, FromTupleError> {
        if tuple.len() != 2 {
            return Err(FromTupleError::ColumnCountMismatch { expected: 2, actual: tuple.len() });
        }

        Ok(BootstrapType {
            oid: tuple[0].take().cast_non_null()?,
            name: tuple[1].take().cast_non_null()?,
        })
    }
}

impl IntoTuple for BootstrapType {
    fn into_tuple(self) -> Tuple {
        Tuple::from([Value::Oid(self.oid.untyped()), Value::Text(self.name)])
    }
}

fn bootstrap_nsql_types() -> Vec<BootstrapType> {
    vec![
        BootstrapType { oid: nsql_ty_oid_oid(), name: "oid".into() },
        BootstrapType { oid: nsql_ty_bool_oid(), name: "bool".into() },
        BootstrapType { oid: nsql_ty_int_oid(), name: "int".into() },
        BootstrapType { oid: nsql_ty_text_oid(), name: "text".into() },
    ]
}
