use nsql_core::Name;
use nsql_storage::{expr_project, Result, TableStorageInfo};
use nsql_storage_engine::{ReadWriteExecutionMode, StorageEngine, Transaction};

use crate::{
    Catalog, Column, ColumnIndex, Index, IndexKind, Namespace, Oid, SystemEntity, SystemTableView,
    Table, Type, MAIN_SCHEMA,
};

pub(crate) fn bootstrap<'env, S: StorageEngine>(
    storage: &'env S,
    txn: &S::WriteTransaction<'env>,
) -> Result<()> {
    tracing::debug!("bootstrapping namespaces");
    let mut namespace_table =
        SystemTableView::<S, ReadWriteExecutionMode, Namespace>::new_bootstrap(storage, txn)?;
    bootstrap_nsql_namespaces().try_for_each(|namespace| namespace_table.insert(namespace))?;

    tracing::debug!("bootstrapping tables");
    let mut table_table =
        SystemTableView::<S, ReadWriteExecutionMode, Table>::new_bootstrap(storage, txn)?;
    bootstrap_nsql_tables().try_for_each(|table| table_table.insert(table))?;

    tracing::debug!("bootstrapping columns");
    let mut column_table =
        SystemTableView::<S, ReadWriteExecutionMode, Column>::new_bootstrap(storage, txn)?;
    bootstrap_nsql_column().try_for_each(|column| column_table.insert(column))?;

    tracing::debug!("bootstrapping types");
    let mut ty_table =
        SystemTableView::<S, ReadWriteExecutionMode, Type>::new_bootstrap(storage, txn)?;
    bootstrap_nsql_types().try_for_each(|ty| ty_table.insert(ty))?;

    tracing::debug!("bootstrapping indexes");
    let mut index_table =
        SystemTableView::<S, ReadWriteExecutionMode, Index>::new_bootstrap(storage, txn)?;
    bootstrap_nsql_indexes().try_for_each(|index| index_table.insert(index))?;

    Ok(())
}

impl Table {
    pub(crate) const NAMESPACE: Oid<Self> = Oid::new(100);
    pub(crate) const TABLE: Oid<Self> = Oid::new(101);
    pub(crate) const ATTRIBUTE: Oid<Self> = Oid::new(102);
    pub(crate) const TYPE: Oid<Self> = Oid::new(103);
    pub(crate) const INDEX: Oid<Self> = Oid::new(104);

    pub(crate) const NAMESPACE_NAME_UNIQUE_INDEX: Oid<Self> = Oid::new(105);
    pub(crate) const TABLE_NAME_UNIQUE_INDEX: Oid<Self> = Oid::new(106);
}

impl Namespace {
    pub const MAIN: Oid<Self> = Oid::new(101);

    pub(crate) const CATALOG: Oid<Self> = Oid::new(100);
}

impl Type {
    pub(crate) const OID: Oid<Self> = Oid::new(100);
    pub(crate) const BOOL: Oid<Self> = Oid::new(101);
    pub(crate) const INT: Oid<Self> = Oid::new(102);
    pub(crate) const TEXT: Oid<Self> = Oid::new(103);
}

fn bootstrap_nsql_namespaces() -> impl Iterator<Item = Namespace> {
    [
        Namespace { oid: Namespace::MAIN, name: MAIN_SCHEMA.into() },
        Namespace { oid: Namespace::CATALOG, name: "nsql_catalog".into() },
    ]
    .into_iter()
}

fn bootstrap_nsql_tables() -> impl Iterator<Item = Table> {
    [
        Table {
            oid: Table::NAMESPACE,
            name: "nsql_namespace".into(),
            namespace: Namespace::CATALOG,
        },
        Table { oid: Table::TABLE, name: "nsql_table".into(), namespace: Namespace::CATALOG },
        Table {
            oid: Table::ATTRIBUTE,
            name: "nsql_attribute".into(),
            namespace: Namespace::CATALOG,
        },
        Table { oid: Table::TYPE, name: "nsql_type".into(), namespace: Namespace::CATALOG },
        Table {
            oid: Table::NAMESPACE_NAME_UNIQUE_INDEX,
            name: "nsql_namespace_name_index".into(),
            namespace: Namespace::CATALOG,
        },
        Table {
            oid: Table::TABLE_NAME_UNIQUE_INDEX,
            name: "nsql_table_namespace_name_index".into(),
            namespace: Namespace::CATALOG,
        },
    ]
    .into_iter()
}

fn bootstrap_nsql_indexes() -> impl Iterator<Item = Index> {
    [
        Index {
            table: Table::NAMESPACE_NAME_UNIQUE_INDEX,
            kind: IndexKind::Unique,
            target: Table::NAMESPACE,
            index_expr: expr_project![1],
        },
        Index {
            table: Table::TABLE_NAME_UNIQUE_INDEX,
            kind: IndexKind::Unique,
            target: Table::TABLE,
            index_expr: expr_project![1, 2],
        },
    ]
    .into_iter()
}

fn bootstrap_nsql_column() -> impl Iterator<Item = Column> {
    [
        // nsql_namespace
        Column {
            name: "oid".into(),
            table: Table::NAMESPACE,
            index: ColumnIndex::new(0),
            ty: Type::OID,
            is_primary_key: true,
        },
        Column {
            name: "name".into(),
            table: Table::NAMESPACE,
            index: ColumnIndex::new(1),
            ty: Type::TEXT,
            is_primary_key: false,
        },
        // nsql_table
        Column {
            name: "oid".into(),
            table: Table::TABLE,
            index: ColumnIndex::new(0),
            ty: Type::OID,
            is_primary_key: true,
        },
        Column {
            name: "namespace".into(),
            table: Table::TABLE,
            index: ColumnIndex::new(1),
            ty: Type::OID,
            is_primary_key: false,
        },
        Column {
            name: "name".into(),
            table: Table::TABLE,
            index: ColumnIndex::new(2),
            ty: Type::TEXT,
            is_primary_key: false,
        },
        // nsql_column
        Column {
            name: "table".into(),
            table: Table::ATTRIBUTE,
            index: ColumnIndex::new(0),
            ty: Type::OID,
            is_primary_key: true,
        },
        Column {
            name: "index".into(),
            table: Table::ATTRIBUTE,
            index: ColumnIndex::new(1),
            ty: Type::INT,
            is_primary_key: true,
        },
        Column {
            name: "ty".into(),
            table: Table::ATTRIBUTE,
            index: ColumnIndex::new(2),
            ty: Type::OID,
            is_primary_key: false,
        },
        Column {
            name: "name".into(),
            table: Table::ATTRIBUTE,
            index: ColumnIndex::new(3),
            ty: Type::TEXT,
            is_primary_key: false,
        },
        Column {
            name: "is_primary_key".into(),
            table: Table::ATTRIBUTE,
            index: ColumnIndex::new(4),
            ty: Type::BOOL,
            is_primary_key: false,
        },
        // nsql_type
        Column {
            name: "oid".into(),
            table: Table::TYPE,
            index: ColumnIndex::new(0),
            ty: Type::OID,
            is_primary_key: true,
        },
        Column {
            name: "name".into(),
            table: Table::TYPE,
            index: ColumnIndex::new(1),
            ty: Type::TEXT,
            is_primary_key: false,
        },
        // nsql_namespace_name_index
        Column {
            table: Table::NAMESPACE_NAME_UNIQUE_INDEX,
            index: ColumnIndex::new(0),
            ty: Type::TEXT,
            name: "name".into(),
            is_primary_key: true,
        },
        // nsql_table_namespace_name_index
        Column {
            table: Table::TABLE_NAME_UNIQUE_INDEX,
            index: ColumnIndex::new(0),
            ty: Type::OID,
            name: "namespace".into(),
            is_primary_key: true,
        },
        Column {
            table: Table::TABLE_NAME_UNIQUE_INDEX,
            index: ColumnIndex::new(1),
            ty: Type::TEXT,
            name: "name".into(),
            is_primary_key: true,
        },
    ]
    .into_iter()
}

fn bootstrap_nsql_types() -> impl Iterator<Item = Type> {
    [
        Type { oid: Type::OID, name: "oid".into() },
        Type { oid: Type::BOOL, name: "bool".into() },
        Type { oid: Type::INT, name: "int".into() },
        Type { oid: Type::TEXT, name: "text".into() },
    ]
    .into_iter()
}

impl SystemEntity for () {
    type Parent = ();

    type Key = ();

    fn name<'env, S: StorageEngine>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn Transaction<'env, S>,
    ) -> Result<Name> {
        unreachable!()
    }

    fn key(&self) -> Self::Key {}

    fn desc() -> &'static str {
        "catalog"
    }

    fn bootstrap_table_storage_info() -> TableStorageInfo {
        todo!()
    }

    fn parent_oid<'env, S: StorageEngine>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn Transaction<'env, S>,
    ) -> Result<Option<Oid<Self::Parent>>> {
        unreachable!()
    }

    fn table() -> Oid<Table> {
        todo!()
    }
}
