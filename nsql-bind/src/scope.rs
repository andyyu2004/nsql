use anyhow::bail;
use nsql_catalog::{Column, Container, Entity, EntityRef, Table};
use nsql_core::schema::LogicalType;
use nsql_core::Name;

use super::unbound;
use crate::{Binder, Path, Result, TableAlias};

#[derive(Debug, Clone, Default)]
pub(crate) struct Scope {
    tables: rpds::HashTrieMap<Path, ir::TableRef>,
    columns: rpds::HashTrieMap<Name, (ir::ColumnRef, ir::TupleIndex)>,
}

impl Scope {
    /// Insert a table and its columns to the scope
    #[tracing::instrument(skip(self, binder, table_ref))]
    pub fn bind_table(
        &self,
        binder: &Binder,
        table_path: Path,
        table_ref: ir::TableRef,
        alias: Option<&TableAlias>,
    ) -> Result<Scope> {
        tracing::debug!("binding table");
        let mut columns = self.columns.clone();

        let table = table_ref.get(&binder.catalog, &binder.tx)?;
        let table_columns = table.all::<Column>(&binder.tx)?;

        if let Some(alias) = alias {
            // if no columns are specified, we only rename the table
            if !alias.columns.is_empty() && table_columns.len() != alias.columns.len() {
                bail!(
                    "table `{}` has {} columns, but {} columns were specified in alias",
                    table_path,
                    table_columns.len(),
                    alias.columns.len()
                );
            }
        }

        for (i, (oid, column)) in table_columns.into_iter().enumerate() {
            let name = match alias {
                Some(alias) if !alias.columns.is_empty() => alias.columns[i].clone(),
                _ => column.name(),
            };

            if columns.contains_key(&name) {
                todo!("handle duplicate names")
            }

            columns = columns.insert(
                name,
                // FIXME the column_index != tuple_index, hack impl for now
                (ir::ColumnRef { table_ref, column: oid }, ir::TupleIndex::new(column.index())),
            );
        }

        let table_path = match alias {
            Some(alias) => Path::Unqualified(alias.table_name.clone()),
            None => table_path,
        };
        Ok(Self { tables: self.tables.insert(table_path, table_ref), columns })
    }

    pub fn lookup_column(
        &self,
        binder: &Binder,
        path: &Path,
    ) -> Result<(LogicalType, ir::TupleIndex)> {
        match path {
            Path::Qualified { prefix, name } => {
                let table_ref = self.tables.get(prefix).ok_or_else(|| unbound!(Table, prefix))?;

                let (column_ref, idx) =
                    self.columns.get(name).cloned().ok_or_else(|| unbound!(Column, path))?;

                assert!(
                    column_ref.table_ref == *table_ref,
                    "this must be the case currently as we don't support ambiguous column names"
                );

                let column = column_ref.get(&binder.catalog, &binder.tx)?;

                Ok((column.logical_type(), idx))
            }
            Path::Unqualified(name) => {
                let (column_ref, idx) =
                    self.columns.get(name).cloned().ok_or_else(|| unbound!(Column, path))?;
                let column = column_ref.get(&binder.catalog, &binder.tx)?;
                Ok((column.logical_type(), idx))
            }
        }
    }
}
