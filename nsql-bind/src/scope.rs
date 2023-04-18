use std::sync::Arc;

use nsql_catalog::{Column, Entity, Oid};
use nsql_core::Name;

use crate::{Error, Ident, Result};

#[derive(Debug, Clone, Default)]
pub(crate) struct Scope {
    tables: rpds::HashTrieMap<Ident, ir::TableRef>,
    columns: rpds::HashTrieMap<Name, (ir::ColumnRef, usize)>,
}

impl Scope {
    /// Insert a table and its columns to the scope
    /// * `name` - Ordered columns of the table being bound.
    #[tracing::instrument(skip(self, table_ref))]
    pub fn bind_table(
        &self,
        name: Ident,
        table_ref: ir::TableRef,
        table_columns: Vec<(Oid<Column>, Arc<Column>)>,
    ) -> Scope {
        tracing::debug!("binding table");
        let mut columns = self.columns.clone();
        for (oid, column) in table_columns {
            if columns.contains_key(&column.name()) {
                todo!("handle duplicate names")
            }
            columns = columns.insert(
                column.name().clone(),
                (ir::ColumnRef { table_ref, column: oid }, column.index()),
            );
        }

        Self { tables: self.tables.insert(name, table_ref), columns }
    }

    pub fn lookup_column(&self, ident: &Ident) -> Result<(ir::ColumnRef, usize)> {
        match ident {
            Ident::Qualified { .. } => todo!(),
            Ident::Unqualified { name } => self
                .columns
                .get(name)
                .cloned()
                .ok_or_else(|| Error::Unbound { kind: Column::desc(), ident: ident.clone() }),
        }
    }
}
