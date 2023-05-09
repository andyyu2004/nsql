// FIXME this file is a logical mess

use anyhow::bail;
use nsql_catalog::{Column, Container, Entity, EntityRef};
use nsql_core::Name;
use nsql_storage::schema::LogicalType;

use super::unbound;
use crate::{Binder, Path, Result, TableAlias};

#[derive(Debug, Clone, Default)]
pub(crate) struct Scope {
    // FIXME tables need to store more info than this
    tables: rpds::HashTrieMap<Path, ()>,
    columns: rpds::Vector<(Path, LogicalType)>,
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

        let table = table_ref.get(&binder.catalog, &binder.tx);
        let mut table_columns = table.all::<Column>(&binder.tx)?;
        table_columns.sort_by_key(|(_, col)| col.index());

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

        let table_path = match alias {
            Some(alias) => Path::Unqualified(alias.table_name.clone()),
            None => table_path,
        };

        for (i, (_oid, column)) in table_columns.into_iter().enumerate() {
            let name = match alias {
                Some(alias) if !alias.columns.is_empty() => alias.columns[i].clone(),
                _ => column.name(),
            };

            if columns.iter().any(|(p, _)| p.name() == name) {
                todo!("handle duplicate names (this will not work with current impl correctly)")
            }

            columns = columns
                .push_back((Path::qualified(table_path.clone(), name), column.logical_type()));
        }

        Ok(Self { tables: self.tables.insert(table_path, ()), columns })
    }

    pub fn lookup_column(&self, path: &Path) -> Result<(LogicalType, ir::TupleIndex)> {
        match path {
            Path::Qualified { .. } => {
                let idx = self
                    .columns
                    .iter()
                    .position(|(p, _)| p == path)
                    .ok_or_else(|| unbound!(Column, path))?;

                let (_, ty) = &self.columns[idx];
                Ok((ty.clone(), ir::TupleIndex::new(idx)))
            }
            Path::Unqualified(column_name) => {
                match &self
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, (p, _))| &p.name() == column_name)
                    .collect::<Vec<_>>()[..]
                {
                    [] => Err(unbound!(Column, path)),
                    [(idx, (_, ty))] => Ok((ty.clone(), ir::TupleIndex::new(*idx))),
                    matches => {
                        bail!(
                            "column `{}` is ambiguous, it could refer to any one of {}",
                            column_name,
                            matches
                                .iter()
                                .map(|(_, (p, _))| format!("`{}`", p))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    }
                }
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub fn bind_values(&self, values: &ir::Values) -> Result<Scope> {
        let mut columns = self.columns.clone();

        for (i, expr) in values[0].iter().enumerate() {
            // default column names are col1, col2, etc.
            let name = Name::from(format!("col{}", i + 1));
            columns = columns.push_back((Path::Unqualified(name), expr.ty.clone()));
        }

        Ok(Self { tables: self.tables.clone(), columns })
    }

    /// Returns an iterator over the columns in the scope exposed as `Expr`s
    pub fn column_refs(&self) -> impl Iterator<Item = ir::Expr> + '_ {
        self.columns.iter().enumerate().map(|(i, (_p, ty))| ir::Expr {
            ty: ty.clone(),
            kind: ir::ExprKind::ColumnRef(ir::TupleIndex::new(i)),
        })
    }

    pub fn alias(&self, alias: TableAlias) -> Result<Self> {
        assert!(self.tables.is_empty(), "not sure when this occurs");

        // if no columns are specified, we only rename the table
        if !alias.columns.is_empty() && self.columns.len() != alias.columns.len() {
            bail!(
                "table expression has {} columns, but {} columns were specified in alias",
                self.columns.len(),
                alias.columns.len()
            );
        }

        let mut columns = rpds::Vector::new();
        for (i, (path, ty)) in self.columns.iter().enumerate() {
            let path = match alias.columns.get(i) {
                Some(name) => {
                    Path::qualified(Path::Unqualified(alias.table_name.clone()), name.clone())
                }
                None => path.clone(),
            };

            if self.columns.iter().any(|(p, _)| p == &path) {
                todo!("handle duplicate names (this will not work with current impl correctly)")
            }

            columns = columns.push_back((path, ty.clone()));
        }

        Ok(Scope { tables: self.tables.insert(Path::Unqualified(alias.table_name), ()), columns })
    }
}
