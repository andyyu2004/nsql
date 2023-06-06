use nsql_core::LogicalType;
use nsql_storage::tuple::Tuple;
use nsql_storage::value::Value;
use nsql_storage::{Result, TableStorage, TableStorageInfo};
use nsql_storage_engine::StorageEngine;

use crate::{Namespace, Oid, Table, TableRef};

pub fn bootstrap<S: StorageEngine>(storage: &S, txn: &S::WriteTransaction<'_>) -> Result<()> {
    let namespace_table_ref = TableRef {
        namespace: catalog_namespace_oid::<S>(),
        table: nsql_namespace_table_oid::<S>(),
    };

    let mut s = TableStorage::create(
        storage,
        txn,
        TableStorageInfo::new(namespace_table_ref.to_string().into(), vec![]),
    )?;

    for namespace in bootstrap_nsql_namespace::<S>() {
        s.insert(&Tuple::from([
            Value::Oid(namespace.oid.untyped()),
            Value::Text(namespace.name.into()),
        ]))?;
    }

    let tables = bootstrap_nsql_table::<S>();
    let columns = bootstrap_nsql_column::<S>();

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

struct BoostrapColumn<S> {
    name: &'static str,
    table: Oid<Table<S>>,
    index: usize,
    ty: LogicalType,
    is_primary_key: bool,
}

const fn catalog_namespace_oid<S>() -> Oid<Namespace<S>> {
    Oid::new(0)
}

const fn main_namespace_oid<S>() -> Oid<Namespace<S>> {
    Oid::new(1)
}

const fn nsql_namespace_table_oid<S>() -> Oid<Table<S>> {
    Oid::new(0)
}

const fn nsql_table_table_oid<S>() -> Oid<Table<S>> {
    Oid::new(1)
}

fn bootstrap_nsql_namespace<S: StorageEngine>() -> Vec<BoostrapNamespace<S>> {
    vec![
        BoostrapNamespace { oid: main_namespace_oid(), name: "main" },
        BoostrapNamespace { oid: catalog_namespace_oid(), name: "nsql_catalog" },
    ]
}

fn bootstrap_nsql_table<S: StorageEngine>() -> Vec<BoostrapTable<S>> {
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
    ]
}

fn bootstrap_nsql_column<S: StorageEngine>() -> Vec<BoostrapColumn<S>> {
    vec![
        BoostrapColumn {
            name: "oid",
            table: nsql_namespace_table_oid(),
            index: 0,
            ty: LogicalType::Int,
            is_primary_key: true,
        },
        BoostrapColumn {
            name: "name",
            table: nsql_namespace_table_oid(),
            index: 1,
            ty: LogicalType::Text,
            is_primary_key: false,
        },
        BoostrapColumn {
            name: "oid",
            table: nsql_table_table_oid(),
            index: 0,
            ty: LogicalType::Int,
            is_primary_key: true,
        },
        BoostrapColumn {
            name: "name",
            table: nsql_table_table_oid(),
            index: 1,
            ty: LogicalType::Text,
            is_primary_key: false,
        },
    ]
}
