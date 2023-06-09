use nsql_core::LogicalType;
use nsql_storage::tuple::{FromTuple, FromTupleError, IntoTuple, Tuple};
use nsql_storage::value::Value;
use nsql_storage::{ColumnStorageInfo, Result, TableStorage, TableStorageInfo};
use nsql_storage_engine::{ReadWriteExecutionMode, StorageEngine};

use crate::system_table::SystemTableView;
use crate::{Column, Namespace, Oid, Table, TableRef};

pub(crate) fn bootstrap<'env, S: StorageEngine>(
    storage: &'env S,
    txn: &S::WriteTransaction<'env>,
) -> Result<()> {
    let mut namespace_table =
        SystemTableView::<S, ReadWriteExecutionMode, BootstrapNamespace<S>>::new(
            storage,
            txn,
            TableStorageInfo::new(
                TableRef {
                    namespace: catalog_namespace_oid::<S>(),
                    table: nsql_namespace_table_oid::<S>(),
                }
                .to_string()
                .into(),
                vec![
                    ColumnStorageInfo::new(LogicalType::Oid, true),
                    ColumnStorageInfo::new(LogicalType::Text, false),
                ],
            ),
        )?;

    for namespace in bootstrap_nsql_namespaces::<S>() {
        namespace_table.insert(namespace)?;
    }

    let mut table_table = SystemTableView::<S, ReadWriteExecutionMode, BootstrapTable<S>>::new(
        storage,
        txn,
        TableStorageInfo::new(
            TableRef {
                namespace: catalog_namespace_oid::<S>(),
                table: nsql_table_table_oid::<S>(),
            }
            .to_string()
            .into(),
            vec![
                ColumnStorageInfo::new(LogicalType::Oid, true),
                ColumnStorageInfo::new(LogicalType::Oid, false),
                ColumnStorageInfo::new(LogicalType::Text, false),
            ],
        ),
    )?;

    let tables = bootstrap_nsql_tables::<S>();
    for table in tables {
        table_table.insert(table)?;
    }

    let mut column_table = SystemTableView::<S, ReadWriteExecutionMode, BootstrapColumn<S>>::new(
        storage,
        txn,
        TableStorageInfo::new(
            TableRef {
                namespace: catalog_namespace_oid::<S>(),
                table: nsql_attribute_table_oid::<S>(),
            }
            .to_string()
            .into(),
            vec![
                ColumnStorageInfo::new(LogicalType::Oid, true),
                ColumnStorageInfo::new(LogicalType::Oid, false),
                ColumnStorageInfo::new(LogicalType::Text, false),
                ColumnStorageInfo::new(LogicalType::Int, false),
                ColumnStorageInfo::new(LogicalType::Text, false),
                ColumnStorageInfo::new(LogicalType::Bool, false),
            ],
        ),
    )?;

    let columns = bootstrap_nsql_column::<S>();
    for column in columns {
        column_table.insert(column)?;
    }

    Ok(())
}

struct BootstrapNamespace<S> {
    oid: Oid<Namespace<S>>,
    name: String,
}

impl<S: StorageEngine> FromTuple for BootstrapNamespace<S> {
    fn from_tuple(mut tuple: Tuple) -> std::result::Result<Self, FromTupleError> {
        if tuple.len() != 2 {
            return Err(FromTupleError::ColumnCountMismatch { expected: 2, actual: tuple.len() });
        }

        Ok(Self { oid: tuple[0].take().cast_non_null()?, name: tuple[1].take().cast_non_null()? })
    }
}

impl<S> IntoTuple for BootstrapNamespace<S> {
    fn into_tuple(self) -> Tuple {
        Tuple::from([Value::Oid(self.oid.untyped()), Value::Text(self.name)])
    }
}

struct BootstrapTable<S> {
    oid: Oid<Table<S>>,
    namespace: Oid<Namespace<S>>,
    name: String,
}

impl<S> IntoTuple for BootstrapTable<S> {
    fn into_tuple(self) -> Tuple {
        Tuple::from([
            Value::Oid(self.oid.untyped()),
            Value::Oid(self.namespace.untyped()),
            Value::Text(self.name),
        ])
    }
}

struct BootstrapColumn<S> {
    oid: Oid<Column<S>>,
    table: Oid<Table<S>>,
    name: String,
    index: u8,
    ty: Oid<LogicalType>,
    is_primary_key: bool,
}

impl<S: StorageEngine> FromTuple for BootstrapColumn<S> {
    fn from_tuple(mut tuple: Tuple) -> Result<Self, FromTupleError> {
        if tuple.len() != 6 {
            return Err(FromTupleError::ColumnCountMismatch { expected: 6, actual: tuple.len() });
        }

        Ok(Self {
            oid: tuple[0].take().cast_non_null()?,
            table: tuple[1].take().cast_non_null()?,
            name: tuple[2].take().cast_non_null()?,
            index: tuple[3].take().cast_non_null()?,
            ty: tuple[4].take().cast_non_null()?,
            is_primary_key: tuple[5].take().cast_non_null()?,
        })
    }
}

impl<S> IntoTuple for BootstrapColumn<S> {
    fn into_tuple(self) -> Tuple {
        Tuple::from([
            Value::Oid(self.oid.untyped()),
            Value::Oid(self.table.untyped()),
            Value::Text(self.name),
            Value::Int(self.index as i32),
            Value::Text(self.ty.to_string()),
            Value::Bool(self.is_primary_key),
        ])
    }
}

const fn catalog_namespace_oid<S>() -> Oid<Namespace<S>> {
    Oid::new(100)
}

const fn main_namespace_oid<S>() -> Oid<Namespace<S>> {
    Oid::new(101)
}

const fn nsql_namespace_table_oid<S>() -> Oid<Table<S>> {
    Oid::new(100)
}

const fn nsql_table_table_oid<S>() -> Oid<Table<S>> {
    Oid::new(101)
}

const fn nsql_attribute_table_oid<S>() -> Oid<Table<S>> {
    Oid::new(102)
}

const fn nsql_ty_table_oid<S>() -> Oid<Table<S>> {
    Oid::new(103)
}

const fn nsql_ty_oid_oid() -> Oid<LogicalType> {
    Oid::new(100)
}

const fn nsql_ty_bool_oid() -> Oid<LogicalType> {
    Oid::new(101)
}

const fn nsql_ty_int_oid() -> Oid<LogicalType> {
    Oid::new(102)
}

const fn nsql_ty_text_oid() -> Oid<LogicalType> {
    Oid::new(103)
}

fn bootstrap_nsql_namespaces<S: StorageEngine>() -> Vec<BootstrapNamespace<S>> {
    vec![
        BootstrapNamespace { oid: main_namespace_oid(), name: "main".into() },
        BootstrapNamespace { oid: catalog_namespace_oid(), name: "nsql_catalog".into() },
    ]
}

fn bootstrap_nsql_tables<S: StorageEngine>() -> Vec<BootstrapTable<S>> {
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

fn bootstrap_nsql_column<S: StorageEngine>() -> Vec<BootstrapColumn<S>> {
    vec![
        BootstrapColumn {
            oid: Oid::new(0),
            name: "oid".into(),
            table: nsql_namespace_table_oid(),
            index: 0,
            ty: nsql_ty_oid_oid(),
            is_primary_key: true,
        },
        BootstrapColumn {
            oid: Oid::new(1),
            name: "name".into(),
            table: nsql_namespace_table_oid(),
            index: 1,
            ty: nsql_ty_text_oid(),
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(2),
            name: "oid".into(),
            table: nsql_table_table_oid(),
            index: 0,
            ty: nsql_ty_oid_oid(),
            is_primary_key: true,
        },
        BootstrapColumn {
            oid: Oid::new(3),
            name: "name".into(),
            table: nsql_table_table_oid(),
            index: 1,
            ty: nsql_ty_text_oid(),
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(4),
            name: "oid".into(),
            table: nsql_attribute_table_oid(),
            index: 0,
            ty: nsql_ty_oid_oid(),
            is_primary_key: true,
        },
        BootstrapColumn {
            oid: Oid::new(5),
            name: "table".into(),
            table: nsql_attribute_table_oid(),
            index: 1,
            ty: nsql_ty_oid_oid(),
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(6),
            name: "name".into(),
            table: nsql_attribute_table_oid(),
            index: 2,
            ty: nsql_ty_text_oid(),
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(7),
            name: "index".into(),
            table: nsql_attribute_table_oid(),
            index: 3,
            ty: nsql_ty_int_oid(),
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(8),
            name: "ty".into(),
            table: nsql_attribute_table_oid(),
            index: 4,
            // text for now, but should probably reference `nsql_type` table
            ty: nsql_ty_text_oid(),
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(9),
            name: "is_primary_key".into(),
            table: nsql_attribute_table_oid(),
            index: 5,
            ty: nsql_ty_bool_oid(),
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(10),
            name: "oid".into(),
            table: nsql_ty_table_oid(),
            index: 0,
            ty: nsql_ty_oid_oid(),
            is_primary_key: true,
        },
    ]
}
