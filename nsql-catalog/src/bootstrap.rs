use nsql_core::LogicalType;
use nsql_storage::tuple::{FromTuple, Tuple};
use nsql_storage::value::Value;
use nsql_storage::{ColumnStorageInfo, Result, TableStorage, TableStorageInfo};
use nsql_storage_engine::StorageEngine;

use crate::{Column, Namespace, Oid, Table, TableRef};

pub(crate) fn namespace_storage_info<S: StorageEngine>() -> TableStorageInfo {
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
    )
}

pub(crate) fn bootstrap<S: StorageEngine>(
    storage: &S,
    txn: &S::WriteTransaction<'_>,
) -> Result<()> {
    // FIXME can derive/cleanup a lot of this
    let mut namespace_storage = TableStorage::create(storage, txn, namespace_storage_info::<S>())?;

    for namespace in bootstrap_nsql_namespaces::<S>() {
        namespace_storage.insert(&Tuple::from([
            Value::Oid(namespace.oid.untyped()),
            Value::Text(namespace.name.into()),
        ]))?;
    }

    let mut table_storage = TableStorage::create(
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
        table_storage.insert(&Tuple::from([
            Value::Oid(table.oid.untyped()),
            Value::Oid(table.namespace.untyped()),
            Value::Text(table.name.into()),
        ]))?;
    }

    let mut column_storage = TableStorage::create(
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
        column_storage.insert(&Tuple::from([
            Value::Oid(column.oid.untyped()),
            Value::Oid(column.table.untyped()),
            Value::Text(column.name.into()),
            Value::Int(column.index as i32),
            Value::Text(column.ty.to_string()),
            Value::Bool(column.is_primary_key),
        ]))?;
    }

    Ok(())
}

struct BoostrapNamespace<S> {
    oid: Oid<Namespace<S>>,
    name: &'static str,
}

struct BoostrapTable<S> {
    oid: Oid<Table<S>>,
    namespace: Oid<Namespace<S>>,
    name: &'static str,
}

struct BootstrapColumn<S> {
    oid: Oid<Column<S>>,
    table: Oid<Table<S>>,
    name: &'static str,
    index: usize,
    ty: LogicalType,
    is_primary_key: bool,
}

impl<S> FromTuple for BootstrapColumn<S> {
    fn from_tuple(tuple: &Tuple) -> std::result::Result<Self, nsql_storage::tuple::FromTupleError> {
        todo!()
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

fn bootstrap_nsql_namespaces<S: StorageEngine>() -> Vec<BoostrapNamespace<S>> {
    vec![
        BoostrapNamespace { oid: main_namespace_oid(), name: "main" },
        BoostrapNamespace { oid: catalog_namespace_oid(), name: "nsql_catalog" },
    ]
}

fn bootstrap_nsql_tables<S: StorageEngine>() -> Vec<BoostrapTable<S>> {
    vec![
        BoostrapTable {
            oid: nsql_namespace_table_oid(),
            name: "nsql_namespace",
            namespace: catalog_namespace_oid(),
        },
        BoostrapTable {
            oid: nsql_table_table_oid(),
            name: "nsql_table",
            namespace: catalog_namespace_oid(),
        },
        BoostrapTable {
            oid: nsql_attribute_table_oid(),
            name: "nsql_attribute",
            namespace: catalog_namespace_oid(),
        },
    ]
}

fn bootstrap_nsql_column<S: StorageEngine>() -> Vec<BootstrapColumn<S>> {
    vec![
        BootstrapColumn {
            oid: Oid::new(0),
            name: "oid",
            table: nsql_namespace_table_oid(),
            index: 0,
            ty: LogicalType::Oid,
            is_primary_key: true,
        },
        BootstrapColumn {
            oid: Oid::new(1),
            name: "name",
            table: nsql_namespace_table_oid(),
            index: 1,
            ty: LogicalType::Text,
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(2),
            name: "oid",
            table: nsql_table_table_oid(),
            index: 0,
            ty: LogicalType::Oid,
            is_primary_key: true,
        },
        BootstrapColumn {
            oid: Oid::new(3),
            name: "name",
            table: nsql_table_table_oid(),
            index: 1,
            ty: LogicalType::Text,
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(4),
            name: "oid",
            table: nsql_attribute_table_oid(),
            index: 0,
            ty: LogicalType::Oid,
            is_primary_key: true,
        },
        BootstrapColumn {
            oid: Oid::new(5),
            name: "table",
            table: nsql_attribute_table_oid(),
            index: 1,
            ty: LogicalType::Oid,
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(6),
            name: "name",
            table: nsql_attribute_table_oid(),
            index: 2,
            ty: LogicalType::Text,
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(7),
            name: "index",
            table: nsql_attribute_table_oid(),
            index: 3,
            ty: LogicalType::Int,
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(8),
            name: "ty",
            table: nsql_attribute_table_oid(),
            index: 4,
            // text for now, but should probably reference `nsql_type` table
            ty: LogicalType::Text,
            is_primary_key: false,
        },
        BootstrapColumn {
            oid: Oid::new(9),
            name: "is_primary_key",
            table: nsql_attribute_table_oid(),
            index: 5,
            ty: LogicalType::Bool,
            is_primary_key: false,
        },
    ]
}
