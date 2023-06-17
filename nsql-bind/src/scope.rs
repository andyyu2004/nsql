use std::marker::PhantomData;

use anyhow::bail;
use nsql_catalog::{Column, SystemEntity, Table};
use nsql_core::{LogicalType, Name, Oid};
use nsql_storage_engine::{StorageEngine, Transaction};

use super::unbound;
use crate::{Binder, Path, Result, TableAlias};

#[derive(Debug)]
pub(crate) struct Scope<S> {
    // FIXME tables need to store more info than this
    tables: rpds::HashTrieMap<Path, ()>,
    bound_columns: rpds::Vector<(Path, LogicalType)>,
    marker: std::marker::PhantomData<S>,
}

impl<S> Clone for Scope<S> {
    fn clone(&self) -> Self {
        Self {
            tables: self.tables.clone(),
            bound_columns: self.bound_columns.clone(),
            marker: PhantomData,
        }
    }
}

impl<S> Default for Scope<S> {
    fn default() -> Self {
        Self { tables: Default::default(), bound_columns: Default::default(), marker: PhantomData }
    }
}

impl<S: StorageEngine> Scope<S> {
    /// Insert a table and its columns to the scope
    #[tracing::instrument(skip(self, tx, binder))]
    pub fn bind_table<'env>(
        &self,
        binder: &Binder<'env, S>,
        tx: &dyn Transaction<'env, S>,
        table_path: Path,
        table: Oid<Table>,
        alias: Option<&TableAlias>,
    ) -> Result<Scope<S>> {
        tracing::debug!("binding table");
        let mut bound_columns = self.bound_columns.clone();

        let table = binder.catalog.table(tx, table)?;
        let table_columns = table.columns(binder.catalog, tx)?;

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

        for (i, column) in table_columns.into_iter().enumerate() {
            let name = match alias {
                Some(alias) if !alias.columns.is_empty() => alias.columns[i].clone(),
                _ => column.name(binder.catalog, tx)?,
            };

            if bound_columns.iter().any(|(p, _)| p.name() == name) {
                todo!("handle duplicate names (this will not work with current impl correctly)")
            }

            bound_columns = bound_columns
                .push_back((Path::qualified(table_path.clone(), name), column.logical_type()));
        }

        Ok(Self { tables: self.tables.insert(table_path, ()), bound_columns, marker: PhantomData })
    }

    pub fn lookup_column(&self, path: &Path) -> Result<(LogicalType, ir::TupleIndex)> {
        match path {
            Path::Qualified { .. } => {
                let idx = self
                    .bound_columns
                    .iter()
                    .position(|(p, _)| p == path)
                    .ok_or_else(|| unbound!(Column, path))?;

                let (_, ty) = &self.bound_columns[idx];
                Ok((ty.clone(), ir::TupleIndex::new(idx)))
            }
            Path::Unqualified(column_name) => {
                match &self
                    .bound_columns
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
    pub fn bind_values(&self, values: &ir::Values) -> Result<Scope<S>> {
        let mut bound_columns = self.bound_columns.clone();

        for (i, expr) in values[0].iter().enumerate() {
            // default column names are col1, col2, etc.
            let name = Name::from(format!("col{}", i + 1));
            bound_columns = bound_columns.push_back((Path::Unqualified(name), expr.ty.clone()));
        }

        Ok(Self { tables: self.tables.clone(), bound_columns, marker: PhantomData })
    }

    /// Returns an iterator over the columns in the scope exposed as `Expr`s
    pub fn column_refs<'a>(
        &'a self,
        exclude: &'a [ir::TupleIndex],
    ) -> impl Iterator<Item = ir::Expr> + 'a {
        self.bound_columns
            .iter()
            .enumerate()
            .map(|(i, x)| (ir::TupleIndex::new(i), x))
            .filter(|(idx, _)| !exclude.contains(idx))
            .map(move |(index, (path, ty))| ir::Expr {
                ty: ty.clone(),
                kind: ir::ExprKind::ColumnRef { path: path.clone(), index },
            })
    }

    pub fn alias(&self, alias: TableAlias) -> Result<Self> {
        assert!(self.tables.is_empty(), "not sure when this occurs");

        // if no columns are specified, we only rename the table
        if !alias.columns.is_empty() && self.bound_columns.len() != alias.columns.len() {
            bail!(
                "table expression has {} columns, but {} columns were specified in alias",
                self.bound_columns.len(),
                alias.columns.len()
            );
        }

        let mut columns = rpds::Vector::new();
        for (i, (path, ty)) in self.bound_columns.iter().enumerate() {
            let path = match alias.columns.get(i) {
                Some(name) => {
                    Path::qualified(Path::Unqualified(alias.table_name.clone()), name.clone())
                }
                None => path.clone(),
            };

            if self.bound_columns.iter().any(|(p, _)| p == &path) {
                todo!("handle duplicate names (this will not work with current impl correctly)")
            }

            columns = columns.push_back((path, ty.clone()));
        }

        Ok(Scope {
            tables: self.tables.insert(Path::Unqualified(alias.table_name), ()),
            bound_columns: columns,
            marker: PhantomData,
        })
    }
}
