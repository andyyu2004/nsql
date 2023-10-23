mod function;
mod namespace;
mod operator;
mod table;

use std::collections::HashMap;

use anyhow::bail;
use nsql_core::{LogicalType, UntypedOid};
use nsql_storage::expr::{Expr, ExprOp, FunctionCatalog, TupleExpr};
use nsql_storage::tuple::TupleIndex;
use nsql_storage::Result;
use nsql_storage_engine::{ReadWriteExecutionMode, StorageEngine, Transaction};

use self::namespace::BootstrapNamespace;
use self::table::BootstrapTable;
pub(crate) use self::table::{BootstrapColumn, BootstrapSequence};
use crate::{
    Column, ColumnIdentity, ColumnIndex, Function, Index, IndexKind, Namespace, Oid, Operator,
    OperatorKind, Sequence, SystemTableView, Table, MAIN_SCHEMA_PATH,
};

// The order matters as it will determine which id is assigned to each element
macro_rules! mk_consts {
    ([$count:expr] $name:ident) => {
        pub const $name: Oid<Self> = Oid::new($count);
    };
    ([$count:expr] $name:ident, $($rest:ident),*) => {
        pub const $name: Oid<Self> = Oid::new($count);
        mk_consts!([$count + 1u64] $($rest),*);
    };
    ($($name:ident),*) => {
        mk_consts!([1u64] $($name),*);
    };
}

use mk_consts;

struct BootstrapFunctionCatalog;

impl<'env, S, M, F> FunctionCatalog<'env, S, M, F> for BootstrapFunctionCatalog {
    fn storage(&self) -> &'env S {
        panic!("cannot get storage during bootstrap")
    }

    fn get_function(&self, _tx: &dyn Transaction<'env, S>, _oid: UntypedOid) -> Result<F> {
        bail!("cannot get function during bootstrap")
    }
}

pub(crate) fn bootstrap<'env, S: StorageEngine>(
    storage: &'env S,
    tx: &S::WriteTransaction<'env>,
) -> Result<()> {
    tracing::debug!("bootstrapping namespaces");

    let catalog = &BootstrapFunctionCatalog;

    let (
        mut namespaces,
        mut tables,
        mut columns,
        mut indexes,
        mut functions,
        mut operators,
        mut sequences,
    ) = bootstrap_info().desugar();

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
            table.create_storage_for_bootstrap(storage, tx)?;
        }
        table_table.insert(catalog, tx, table)
    })?;
    drop(table_table);

    tracing::debug!("bootstrapping sequences");
    let mut sequence_table =
        SystemTableView::<S, ReadWriteExecutionMode, Sequence>::new_bootstrap(storage, tx)?;
    sequences.try_for_each(|sequence| sequence_table.insert(catalog, tx, sequence))?;
    drop(sequence_table);

    tracing::debug!("bootstrapping columns");
    let mut column_table =
        SystemTableView::<S, ReadWriteExecutionMode, Column>::new_bootstrap(storage, tx)?;
    columns.try_for_each(|column| column_table.insert(catalog, tx, column))?;
    drop(column_table);

    tracing::debug!("bootstrapping indexes");
    let mut index_table =
        SystemTableView::<S, ReadWriteExecutionMode, Index>::new_bootstrap(storage, tx)?;
    indexes.try_for_each(|index| index_table.insert(catalog, tx, index))?;
    drop(index_table);

    tracing::debug!("bootstrapping functions");
    let mut functions_table =
        SystemTableView::<S, ReadWriteExecutionMode, Function>::new_bootstrap(storage, tx)?;
    functions.try_for_each(|function| functions_table.insert(catalog, tx, function))?;
    drop(functions_table);

    tracing::debug!("bootstrapping operators");
    let mut operators_table =
        SystemTableView::<S, ReadWriteExecutionMode, Operator>::new_bootstrap(storage, tx)?;
    operators.try_for_each(|operator| operators_table.insert(catalog, tx, operator))?;
    drop(operators_table);

    Ok(())
}

fn bootstrap_info() -> BootstrapData {
    BootstrapData {
        namespaces: namespace::bootstrap_data(),
        tables: table::bootstrap_data(),
        functions: Function::bootstrap_data(),
        operators: Operator::bootstrap_data(),
    }
}

struct BootstrapData {
    namespaces: Box<[BootstrapNamespace]>,
    tables: Box<[BootstrapTable]>,
    functions: Box<[Function]>,
    operators: Box<[Operator]>,
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
        impl Iterator<Item = Operator>,
        impl Iterator<Item = Sequence>,
    ) {
        let namespaces = self
            .namespaces
            .into_vec()
            .into_iter()
            .map(|ns| Namespace { oid: ns.oid, name: ns.name.into() });

        let mut functions_map = HashMap::with_capacity(self.functions.len());

        let functions = self
            .functions
            .into_vec()
            .into_iter()
            .map(|f| {
                assert!(functions_map.insert(f.oid, f.clone()).is_none());
                f
            })
            .collect::<Vec<_>>();

        let operators = self
            .operators
            .into_vec()
            .into_iter()
            .map(|op| {
                let f = &functions_map.get(&op.function).expect("function not found for operator");
                match op.kind {
                    OperatorKind::Unary => assert_eq!(
                        f.args().len(),
                        1,
                        "function of prefix operator should take 1 argument"
                    ),
                    OperatorKind::Binary => assert_eq!(
                        f.args().len(),
                        2,
                        "function of binary operator should take 2 arguments"
                    ),
                };

                op
            })
            .collect::<Vec<_>>();

        let mut tables = vec![];
        let mut columns = vec![];
        let mut indexes = vec![];
        let mut sequences = vec![];

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
                                ty: target_column.ty.clone(),
                                name: target_column.name.into(),
                                // all columns in an index are part of the primary key (for now) as
                                // we only have unique indexes
                                is_primary_key: true,
                                identity: target_column.identity,
                                default_expr: Expr::null(),
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

            for (idx, col) in table.columns.into_iter().enumerate() {
                match col.seq {
                    Some(seq) => {
                        assert!(!matches!(col.identity, ColumnIdentity::None));
                        tables.push(Table {
                            oid: seq.table,
                            namespace: Namespace::CATALOG,
                            name: seq.name.into(),
                        });
                        sequences.push(Sequence {
                            oid: seq.table,
                            // provide some space for bootstrap oids
                            start: 1000,
                            step: 1,
                        });

                        // The backing table of a sequence has two columns, the value of the sequence and an id.
                        // The `id` is just some arbitary value as will only be one row.
                        // This needs to match the `SequenceData`
                        columns.push(Column {
                            table: seq.table,
                            index: ColumnIndex::new(0),
                            ty: LogicalType::Oid,
                            name: "key".into(),
                            is_primary_key: true,
                            identity: ColumnIdentity::None,
                            default_expr: Expr::null(),
                        });
                        columns.push(Column {
                            table: seq.table,
                            index: ColumnIndex::new(1),
                            ty: LogicalType::Int64,
                            name: "value".into(),
                            is_primary_key: false,
                            identity: ColumnIdentity::None,
                            default_expr: Expr::null(),
                        });
                    }
                    None => assert!(matches!(col.identity, ColumnIdentity::None)),
                }
                columns.push(Column {
                    table: table.oid,
                    index: ColumnIndex::new(idx.try_into().unwrap()),
                    ty: col.ty,
                    name: col.name.into(),
                    is_primary_key: col.is_primary_key,
                    identity: col.identity,
                    default_expr: col.default_expr,
                });
            }
        }

        (
            namespaces.into_iter(),
            tables.into_iter(),
            columns.into_iter(),
            indexes.into_iter(),
            functions.into_iter(),
            operators.into_iter(),
            sequences.into_iter(),
        )
    }
}
