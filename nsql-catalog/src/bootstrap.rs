mod function;
mod namespace;
mod table;

use anyhow::bail;
use nsql_core::{LogicalType, UntypedOid};
use nsql_storage::eval::{Expr, ExprOp, FunctionCatalog, TupleExpr};
use nsql_storage::tuple::TupleIndex;
use nsql_storage::{ColumnStorageInfo, Result};
use nsql_storage_engine::{ReadWriteExecutionMode, StorageEngine, Transaction};

use self::function::BootstrapFunction;
use self::namespace::BootstrapNamespace;
use self::table::BootstrapTable;
use crate::entity::function::FunctionKind;
use crate::{
    Column, ColumnIndex, Function, Index, IndexKind, Namespace, Oid, SystemEntity, SystemTableView,
    Table, MAIN_SCHEMA,
};

macro_rules! mk_consts {
    ([$count:expr] $name:ident) => {
        pub(crate) const $name: Oid<Self> = Oid::new($count);
    };
    ([$count:expr] $name:ident, $($rest:ident),*) => {
        pub(crate) const $name: Oid<Self> = Oid::new($count);
        mk_consts!([$count + 1u64] $($rest),*);
    };
    ($($name:ident),*) => {
        mk_consts!([0u64] $($name),*);
    };
}

use mk_consts;

struct BootstrapFunctionCatalog;

impl<'env, S> FunctionCatalog<'env, S> for BootstrapFunctionCatalog {
    fn get_function(
        &self,
        _tx: &dyn Transaction<'env, S>,
        _oid: UntypedOid,
    ) -> Result<Box<dyn nsql_storage::eval::ScalarFunction>> {
        bail!("cannot get function during bootstrap")
    }
}

pub(crate) fn bootstrap<'env, S: StorageEngine>(
    storage: &'env S,
    tx: &S::WriteTransaction<'env>,
) -> Result<()> {
    tracing::debug!("bootstrapping namespaces");

    let catalog = &BootstrapFunctionCatalog;

    let (mut namespaces, mut tables, mut columns, mut indexes, mut functions) =
        bootstrap_info().desugar();

    let mut namespace_table =
        SystemTableView::<S, ReadWriteExecutionMode, Namespace>::new_bootstrap(storage, tx)?;
    namespaces.try_for_each(|namespace| namespace_table.insert(catalog, tx, namespace))?;
    drop(namespace_table);

    tracing::debug!("bootstrapping tables");
    let mut table_table =
        SystemTableView::<S, ReadWriteExecutionMode, Table>::new_bootstrap(storage, tx)?;
    tables.try_for_each(|table| {
        if table.oid != Table::TABLE {
            // can't do this as this table is currently open
            table.create_storage_for_bootstrap(storage, tx, table.key())?;
        }
        table_table.insert(catalog, tx, table)
    })?;

    tracing::debug!("bootstrapping columns");
    let mut column_table =
        SystemTableView::<S, ReadWriteExecutionMode, Column>::new_bootstrap(storage, tx)?;
    columns.try_for_each(|column| column_table.insert(catalog, tx, column))?;

    tracing::debug!("bootstrapping indexes");
    let mut index_table =
        SystemTableView::<S, ReadWriteExecutionMode, Index>::new_bootstrap(storage, tx)?;
    indexes.try_for_each(|index| index_table.insert(catalog, tx, index))?;

    tracing::debug!("bootstrapping functions");
    let mut functions_table =
        SystemTableView::<S, ReadWriteExecutionMode, Function>::new_bootstrap(storage, tx)?;
    functions.try_for_each(|function| functions_table.insert(catalog, tx, function))?;

    Ok(())
}

fn bootstrap_info() -> BootstrapData {
    BootstrapData {
        namespaces: namespace::bootstrap_data(),
        tables: table::bootstrap_data(),
        functions: function::bootstrap_data(),
    }
}

struct BootstrapData {
    namespaces: Box<[BootstrapNamespace]>,
    tables: Box<[BootstrapTable]>,
    functions: Box<[BootstrapFunction]>,
}

impl BootstrapData {
    fn desugar(
        self,
    ) -> (
        impl Iterator<Item = Namespace>,
        impl Iterator<Item = Table>,
        impl Iterator<Item = Column>,
        impl Iterator<Item = Index>,
        impl Iterator<Item = Function>,
    ) {
        let namespaces = self
            .namespaces
            .into_vec()
            .into_iter()
            .map(|ns| Namespace { oid: ns.oid, name: ns.name.into() });

        let functions = self.functions.into_vec().into_iter().map(|f| Function {
            oid: f.oid,
            namespace: Namespace::MAIN,
            name: f.name.into(),
            args: f.args.into_boxed_slice(),
            ret: f.ret,
            kind: f.kind,
        });

        let mut tables = vec![];
        let mut columns = vec![];
        let mut indexes = vec![];

        for table in self.tables.into_vec() {
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
                                ty: target_column.logical_type.clone(),
                                name: target_column.name.clone(),
                                // all columns in an index are part of the primary key (for now) as
                                // we only have unique indexes
                                is_primary_key: true,
                            });

                            Expr::new(
                                name,
                                [
                                    ExprOp::Project { index: TupleIndex::new(target_idx) },
                                    ExprOp::Return,
                                ],
                            )
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
                    ty: column.logical_type,
                    name: column.name,
                    is_primary_key: column.is_primary_key,
                });
            }
        }

        (
            namespaces.into_iter(),
            tables.into_iter(),
            columns.into_iter(),
            indexes.into_iter(),
            functions.into_iter(),
        )
    }
}
