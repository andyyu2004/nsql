use nsql_storage::eval::{Expr, ExprOp, TupleExpr};
use nsql_storage::tuple::TupleIndex;
use nsql_storage::Result;
use nsql_storage_engine::{ReadWriteExecutionMode, StorageEngine};

use crate::{
    Column, ColumnIndex, Function, Index, IndexKind, Namespace, Oid, SystemEntity, SystemTableView,
    Table, Type, MAIN_SCHEMA,
};

struct BootstrapType {
    oid: Oid<Type>,
    name: &'static str,
}

struct BootstrapFunction {
    oid: Oid<Function>,
    name: &'static str,
    args: Vec<Oid<Type>>,
    ret: Oid<Type>,
}

struct BootstrapNamespace {
    oid: Oid<Namespace>,
    name: &'static str,
}

struct BootstrapTable {
    oid: Oid<Table>,
    name: &'static str,
    columns: Vec<BootstrapColumn>,
    indexes: Vec<BootstrapIndex>,
}

struct BootstrapIndex {
    table: Oid<Table>,
    name: &'static str,
    kind: IndexKind,
    column_names: Vec<&'static str>,
}

struct BootstrapColumn {
    name: &'static str,
    ty: Oid<Type>,
    pk: bool,
}

pub(crate) fn bootstrap<'env, S: StorageEngine>(
    storage: &'env S,
    txn: &S::WriteTransaction<'env>,
) -> Result<()> {
    tracing::debug!("bootstrapping namespaces");

    let (mut namespaces, mut tables, mut columns, mut types, mut indexes, mut functions) =
        bootstrap_info().desugar();

    let mut namespace_table =
        SystemTableView::<S, ReadWriteExecutionMode, Namespace>::new_bootstrap(storage, txn)?;
    namespaces.try_for_each(|namespace| namespace_table.insert(namespace))?;
    drop(namespace_table);

    tracing::debug!("bootstrapping tables");
    let mut table_table =
        SystemTableView::<S, ReadWriteExecutionMode, Table>::new_bootstrap(storage, txn)?;
    tables.try_for_each(|table| {
        if table.oid != Table::TABLE {
            // can't do this as this table is currently open
            table.create_storage_for_bootstrap(storage, txn, table.key())?;
        }
        table_table.insert(table)
    })?;

    tracing::debug!("bootstrapping columns");
    let mut column_table =
        SystemTableView::<S, ReadWriteExecutionMode, Column>::new_bootstrap(storage, txn)?;
    columns.try_for_each(|column| column_table.insert(column))?;

    tracing::debug!("bootstrapping types");
    let mut ty_table =
        SystemTableView::<S, ReadWriteExecutionMode, Type>::new_bootstrap(storage, txn)?;
    types.try_for_each(|ty| ty_table.insert(ty))?;

    tracing::debug!("bootstrapping indexes");
    let mut index_table =
        SystemTableView::<S, ReadWriteExecutionMode, Index>::new_bootstrap(storage, txn)?;
    indexes.try_for_each(|index| index_table.insert(index))?;

    tracing::debug!("bootstrapping functions");
    let mut functions_table =
        SystemTableView::<S, ReadWriteExecutionMode, Function>::new_bootstrap(storage, txn)?;
    functions.try_for_each(|function| functions_table.insert(function))?;

    Ok(())
}

impl Table {
    pub(crate) const NAMESPACE: Oid<Self> = Oid::new(100);
    pub(crate) const TABLE: Oid<Self> = Oid::new(101);
    pub(crate) const ATTRIBUTE: Oid<Self> = Oid::new(102);
    pub(crate) const TYPE: Oid<Self> = Oid::new(103);
    pub(crate) const INDEX: Oid<Self> = Oid::new(104);
    pub(crate) const FUNCTION: Oid<Self> = Oid::new(105);

    pub(crate) const NAMESPACE_NAME_UNIQUE_INDEX: Oid<Self> = Oid::new(205);
    pub(crate) const TABLE_NAME_UNIQUE_INDEX: Oid<Self> = Oid::new(206);
    pub(crate) const ATTRIBUTE_NAME_UNIQUE_INDEX: Oid<Self> = Oid::new(207);
    pub(crate) const TYPE_NAME_UNIQUE_INDEX: Oid<Self> = Oid::new(208);
    pub(crate) const FUNCTION_NAME_ARGS_UNIQUE_INDEX: Oid<Self> = Oid::new(209);
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
    pub(crate) const BYTEA: Oid<Self> = Oid::new(104);
}

impl Function {
    pub(crate) const RANGE2: Oid<Self> = Oid::new(100);
}

// FIXME there is still quite a bit of duplicated work between here and `bootstrap_table_storage_info`
fn bootstrap_info() -> BootstrapInfo {
    BootstrapInfo {
        namespaces: vec![
            BootstrapNamespace { oid: Namespace::CATALOG, name: "nsql_catalog" },
            BootstrapNamespace { oid: Namespace::MAIN, name: MAIN_SCHEMA },
        ],
        tables: vec![
            BootstrapTable {
                oid: Table::NAMESPACE,
                name: "nsql_namespace",
                columns: vec![
                    BootstrapColumn { name: "oid", ty: Type::OID, pk: true },
                    BootstrapColumn { name: "name", ty: Type::TEXT, pk: false },
                ],
                indexes: vec![BootstrapIndex {
                    table: Table::NAMESPACE_NAME_UNIQUE_INDEX,
                    name: "nsql_namespace_name_index",
                    kind: IndexKind::Unique,
                    column_names: vec!["name"],
                }],
            },
            BootstrapTable {
                oid: Table::TABLE,
                name: "nsql_table",
                columns: vec![
                    BootstrapColumn { name: "oid", ty: Type::OID, pk: true },
                    BootstrapColumn { name: "namespace", ty: Type::OID, pk: false },
                    BootstrapColumn { name: "name", ty: Type::TEXT, pk: false },
                ],
                indexes: vec![BootstrapIndex {
                    table: Table::TABLE_NAME_UNIQUE_INDEX,
                    name: "nsql_table_namespace_name_index",
                    kind: IndexKind::Unique,
                    column_names: vec!["namespace", "name"],
                }],
            },
            BootstrapTable {
                oid: Table::ATTRIBUTE,
                name: "nsql_attribute",
                columns: vec![
                    BootstrapColumn { name: "table", ty: Type::OID, pk: true },
                    BootstrapColumn { name: "index", ty: Type::INT, pk: true },
                    BootstrapColumn { name: "ty", ty: Type::OID, pk: false },
                    BootstrapColumn { name: "name", ty: Type::TEXT, pk: false },
                    BootstrapColumn { name: "is_primary_key", ty: Type::BOOL, pk: false },
                ],
                indexes: vec![BootstrapIndex {
                    table: Table::ATTRIBUTE_NAME_UNIQUE_INDEX,
                    name: "nsql_attribute_table_name_index",
                    kind: IndexKind::Unique,
                    column_names: vec!["table", "name"],
                }],
            },
            BootstrapTable {
                oid: Table::TYPE,
                name: "nsql_type",
                columns: vec![
                    BootstrapColumn { name: "oid", ty: Type::OID, pk: true },
                    BootstrapColumn { name: "name", ty: Type::TEXT, pk: false },
                ],
                indexes: vec![BootstrapIndex {
                    table: Table::TYPE_NAME_UNIQUE_INDEX,
                    name: "nsql_type_name_index",
                    kind: IndexKind::Unique,
                    column_names: vec!["name"],
                }],
            },
            BootstrapTable {
                oid: Table::INDEX,
                name: "nsql_index",
                columns: vec![
                    BootstrapColumn { name: "table", ty: Type::OID, pk: true },
                    BootstrapColumn { name: "target", ty: Type::OID, pk: false },
                    BootstrapColumn { name: "kind", ty: Type::INT, pk: false },
                    BootstrapColumn { name: "index_expr", ty: Type::BYTEA, pk: false },
                ],
                indexes: vec![],
            },
            BootstrapTable {
                oid: Table::FUNCTION,
                name: "nsql_function",
                columns: vec![
                    BootstrapColumn { name: "oid", ty: Type::OID, pk: true },
                    BootstrapColumn { name: "namespace", ty: Type::OID, pk: false },
                    BootstrapColumn { name: "name", ty: Type::INT, pk: false },
                    BootstrapColumn { name: "args", ty: Type::BYTEA, pk: false },
                ],
                indexes: vec![BootstrapIndex {
                    table: Table::FUNCTION_NAME_ARGS_UNIQUE_INDEX,
                    name: "nsql_function_namespace_name_args_index",
                    kind: IndexKind::Unique,
                    column_names: vec!["namespace", "name", "args"],
                }],
            },
        ],
        types: vec![
            BootstrapType { oid: Type::OID, name: "oid" },
            BootstrapType { oid: Type::BOOL, name: "bool" },
            BootstrapType { oid: Type::INT, name: "int" },
            BootstrapType { oid: Type::TEXT, name: "text" },
            BootstrapType { oid: Type::BYTEA, name: "bytea" },
        ],
        functions: vec![BootstrapFunction {
            oid: Function::RANGE2,
            name: "range",
            args: vec![Type::INT, Type::INT],
            ret: Type::OID,
        }],
    }
}

struct BootstrapInfo {
    namespaces: Vec<BootstrapNamespace>,
    tables: Vec<BootstrapTable>,
    types: Vec<BootstrapType>,
    functions: Vec<BootstrapFunction>,
}

impl BootstrapInfo {
    fn desugar(
        self,
    ) -> (
        impl Iterator<Item = Namespace>,
        impl Iterator<Item = Table>,
        impl Iterator<Item = Column>,
        impl Iterator<Item = Type>,
        impl Iterator<Item = Index>,
        impl Iterator<Item = Function>,
    ) {
        let namespaces =
            self.namespaces.into_iter().map(|ns| Namespace { oid: ns.oid, name: ns.name.into() });

        let functions = self.functions.into_iter().map(|f| Function {
            oid: f.oid,
            namespace: Namespace::MAIN,
            name: f.name.into(),
            args: f.args.into_boxed_slice(),
            ret: f.ret,
        });

        let types = self.types.into_iter().map(|ty| Type { oid: ty.oid, name: ty.name.into() });

        let mut tables = vec![];
        let mut columns = vec![];
        let mut indexes = vec![];

        for table in self.tables {
            tables.push(Table {
                oid: table.oid,
                namespace: Namespace::CATALOG,
                name: table.name.into(),
            });

            for index in table.indexes {
                // create the table entry for the index
                tables.push(Table {
                    oid: index.table,
                    namespace: Namespace::CATALOG,
                    name: index.name.into(),
                });

                // build a projection expression for the index by projecting the required columns
                // (from the target table)
                let index_expr = TupleExpr::new(
                    index
                        .column_names
                        .iter()
                        .enumerate()
                        .map(|(col_index, &name)| {
                            let target_idx = table
                                .columns
                                .iter()
                                .position(|column| column.name == name)
                                .expect("column not found in target table");

                            let target_column = &table.columns[target_idx];

                            // create a column entry for each column in the index
                            columns.push(Column {
                                table: index.table,
                                index: ColumnIndex::new(col_index.try_into().unwrap()),
                                ty: target_column.ty,
                                name: target_column.name.into(),
                                // all columns in an index are part of the primary key (for now) as
                                // we only have unique indexes
                                is_primary_key: true,
                            });

                            Expr::new([ExprOp::Project(TupleIndex::new(target_idx))])
                        })
                        .collect::<Vec<_>>(),
                );

                indexes.push(Index {
                    table: index.table,
                    target: table.oid,
                    kind: index.kind,
                    index_expr,
                });
            }

            for (idx, column) in table.columns.into_iter().enumerate() {
                columns.push(Column {
                    table: table.oid,
                    index: ColumnIndex::new(idx.try_into().unwrap()),
                    ty: column.ty,
                    name: column.name.into(),
                    is_primary_key: column.pk,
                });
            }
        }

        (
            namespaces.into_iter(),
            tables.into_iter(),
            columns.into_iter(),
            types.into_iter(),
            indexes.into_iter(),
            functions.into_iter(),
        )
    }
}
