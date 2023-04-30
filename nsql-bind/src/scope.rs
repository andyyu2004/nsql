use nsql_catalog::{Column, Container, Entity, EntityRef, Table};
use nsql_core::schema::LogicalType;
use nsql_core::Name;
use nsql_parse::ast;

use crate::{Binder, Error, Path, Result};

#[derive(Debug, Clone, Default)]
pub(crate) struct Scope {
    tables: rpds::HashTrieMap<Path, ir::TableRef>,
    columns: rpds::HashTrieMap<Name, (ir::ColumnRef, ir::TupleIndex)>,
}

impl Scope {
    /// Insert a table and its columns to the scope
    /// * `name` - Ordered columns of the table being bound.
    #[tracing::instrument(skip(self, binder, table_ref))]
    pub fn bind_table(
        &self,
        binder: &Binder,
        name: Path,
        table_ref: ir::TableRef,
    ) -> Result<Scope> {
        tracing::debug!("binding table");
        let mut columns = self.columns.clone();

        let table = table_ref.get(&binder.catalog, &binder.tx)?;
        let table_columns = table.all::<Column>(&binder.tx)?;

        for (oid, column) in table_columns {
            if columns.contains_key(&column.name()) {
                todo!("handle duplicate names")
            }
            columns = columns.insert(
                column.name().clone(),
                // FIXME the column_index != tuple_index, hack impl for now
                (ir::ColumnRef { table_ref, column: oid }, ir::TupleIndex::new(column.index())),
            );
        }

        Ok(Self { tables: self.tables.insert(name, table_ref), columns })
    }

    pub fn lookup_column(
        &self,
        binder: &Binder,
        path: &Path,
    ) -> Result<(LogicalType, ir::TupleIndex)> {
        match path {
            Path::Qualified { prefix, name } => {
                let table_ref = self
                    .tables
                    .get(prefix)
                    .ok_or_else(|| Error::Unbound { kind: Table::desc(), path: *prefix.clone() })?;

                let (column_ref, idx) = self
                    .columns
                    .get(name)
                    .cloned()
                    .ok_or_else(|| Error::Unbound { kind: Column::desc(), path: path.clone() })?;

                assert!(
                    column_ref.table_ref == *table_ref,
                    "this must be the case currently as we don't support ambiguous column names"
                );

                let column = column_ref.get(&binder.catalog, &binder.tx)?;

                Ok((column.logical_type(), idx))
            }
            Path::Unqualified(name) => {
                let (column_ref, idx) = self
                    .columns
                    .get(name)
                    .cloned()
                    .ok_or_else(|| Error::Unbound { kind: Column::desc(), path: path.clone() })?;
                let column = column_ref.get(&binder.catalog, &binder.tx)?;
                Ok((column.logical_type(), idx))
            }
        }
    }

    pub(crate) fn alias(&self, alias: &ast::TableAlias) -> Scope {
        todo!()
    }
}
