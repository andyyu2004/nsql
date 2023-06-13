use nsql_core::Name;
use nsql_storage::{Result, TableStorageInfo};
use nsql_storage_engine::{ReadWriteExecutionMode, StorageEngine};

use crate::system_table::{SystemEntity, SystemTableView};
use crate::{Column, ColumnIndex, Entity, Namespace, Oid, Table, Type};

pub(crate) fn bootstrap<'env, S: StorageEngine>(
    storage: &'env S,
    txn: &S::WriteTransaction<'env>,
) -> Result<()> {
    tracing::debug!("bootstrapping namespaces");
    let mut namespace_table =
        SystemTableView::<S, ReadWriteExecutionMode, Namespace>::new(storage, txn)?;
    bootstrap_nsql_namespaces().try_for_each(|namespace| namespace_table.insert(namespace))?;

    tracing::debug!("bootstrapping tables");
    let mut table_table = SystemTableView::<S, ReadWriteExecutionMode, Table>::new(storage, txn)?;
    bootstrap_nsql_tables().try_for_each(|table| table_table.insert(table))?;

    tracing::debug!("bootstrapping columns");
    let mut column_table = SystemTableView::<S, ReadWriteExecutionMode, Column>::new(storage, txn)?;
    bootstrap_nsql_column().try_for_each(|column| column_table.insert(column))?;

    tracing::debug!("bootstrapping types");
    let mut ty_table = SystemTableView::<S, ReadWriteExecutionMode, Type>::new(storage, txn)?;
    bootstrap_nsql_types().try_for_each(|ty| ty_table.insert(ty))?;

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

fn bootstrap_nsql_namespaces() -> impl Iterator<Item = Namespace> {
    vec![
        Namespace { oid: Namespace::MAIN, name: "main".into() },
        Namespace { oid: Namespace::CATALOG, name: "nsql_catalog".into() },
    ]
    .into_iter()
}

fn bootstrap_nsql_tables() -> impl Iterator<Item = Table> {
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
    .into_iter()
}

fn bootstrap_nsql_column() -> impl Iterator<Item = Column> {
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
    .into_iter()
}

fn bootstrap_nsql_types() -> impl Iterator<Item = Type> {
    vec![
        Type { oid: oid::TY_OID, name: "oid".into() },
        Type { oid: oid::TY_BOOL, name: "bool".into() },
        Type { oid: oid::TY_INT, name: "int".into() },
        Type { oid: oid::TY_TEXT, name: "text".into() },
    ]
    .into_iter()
}

impl Entity for () {
    fn name(&self) -> Name {
        unreachable!()
    }

    fn oid(&self) -> Oid<Self> {
        unreachable!()
    }

    fn desc() -> &'static str {
        "catalog"
    }
}

impl SystemEntity for () {
    type Parent = ();

    fn storage_info() -> TableStorageInfo {
        todo!()
    }

    fn parent_oid(&self) -> Option<Oid<Self::Parent>> {
        unreachable!()
    }
}
