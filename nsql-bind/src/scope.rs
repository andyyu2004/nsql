use std::fmt;

use anyhow::{bail, ensure};
use ir::QPath;
use nsql_catalog::{Column, SystemEntity, Table};
use nsql_core::{LogicalType, Name, Oid};
use nsql_storage_engine::{StorageEngine, Transaction};

use super::unbound;
use crate::{Binder, Path, Result, TableAlias};

#[derive(Default)]
pub(crate) struct Scope {
    columns: rpds::Vector<(QPath, LogicalType)>,
}

impl fmt::Debug for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (i, (path, ty)) in self.columns.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}: {}", path, ty)?;
        }
        write!(f, "]")
    }
}

impl Clone for Scope {
    fn clone(&self) -> Self {
        Self { columns: self.columns.clone() }
    }
}

impl Scope {
    pub fn project(&self, projection: &[ir::Expr]) -> Self {
        let columns = projection
            .iter()
            .map(|expr| match &expr.kind {
                // FIXME this is missing the alias case right? need a test case
                // ir::ExprKind::Alias {..}
                ir::ExprKind::ColumnRef { qpath, .. } => (qpath.clone(), expr.ty.clone()),
                // the generated column name is a string representation of the expression
                _ => (QPath::new("unknowntabletodo", expr.to_string()), expr.ty.clone()),
            })
            .collect();

        Self { columns }
    }

    pub fn merge(&self, other: &Self) -> Result<Self> {
        let mut columns = self.columns.clone();
        for (path, ty) in other.columns.iter() {
            columns.push_back_mut((path.clone(), ty.clone()));
        }

        Ok(Self { columns })
    }

    /// Insert a table and its columns to the scope
    #[tracing::instrument(skip(self, tx, binder))]
    pub fn bind_table<'env, S: StorageEngine>(
        &self,
        binder: &Binder<'env, S>,
        tx: &dyn Transaction<'env, S>,
        table_path: Path,
        table: Oid<Table>,
        alias: Option<&TableAlias>,
    ) -> Result<Scope> {
        tracing::debug!("binding table");
        let mut columns = self.columns.clone();

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
                _ => column.name(),
            };

            columns.push_back_mut((QPath::new(table_path.clone(), name), column.logical_type()));
        }

        Ok(Self { columns })
    }

    pub fn lookup_column(&self, path: &Path) -> Result<(QPath, LogicalType, ir::TupleIndex)> {
        match path {
            Path::Qualified(qpath) => {
                let idx = self
                    .columns
                    .iter()
                    .position(|(p, _)| p == qpath)
                    .ok_or_else(|| unbound!(Column, path))?;

                let (qpath, ty) = &self.columns[idx];
                Ok((qpath.clone(), ty.clone(), ir::TupleIndex::new(idx)))
            }
            Path::Unqualified(column_name) => {
                match &self
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, (p, _))| &p.name == column_name)
                    .collect::<Vec<_>>()[..]
                {
                    [] => Err(unbound!(Column, path)),
                    [(idx, (qpath, ty))] => {
                        Ok((qpath.clone(), ty.clone(), ir::TupleIndex::new(*idx)))
                    }
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
    pub fn bind_values(&self, values: &ir::Values) -> Scope {
        let mut columns = self.columns.clone();

        for (i, expr) in values[0].iter().enumerate() {
            // default column names are col1, col2, etc.
            let name = Name::from(format!("col{}", i + 1));
            columns.push_back_mut((QPath::new("values", name), expr.ty.clone()));
        }

        Self { columns }
    }

    pub fn bind_unnest(&self, expr: &ir::Expr) -> Result<Scope> {
        ensure!(
            matches!(expr.ty, LogicalType::Array(_)),
            "UNNEST expression must be an array, got {}",
            expr.ty,
        );

        let ty = match &expr.ty {
            LogicalType::Array(element_type) => *element_type.clone(),
            _ => unreachable!(),
        };

        Ok(Self { columns: self.columns.push_back((QPath::new("", "unnest"), ty)) })
    }

    /// Returns an iterator over the columns in the scope exposed as `ir::Expr`s
    pub fn column_refs<'a>(
        &'a self,
        exclude: &'a [ir::TupleIndex],
    ) -> impl Iterator<Item = ir::Expr> + 'a {
        self.columns
            .iter()
            .enumerate()
            .map(|(i, x)| (ir::TupleIndex::new(i), x))
            .filter(|(idx, _)| !exclude.contains(idx))
            .map(move |(index, (path, ty))| ir::Expr {
                ty: ty.clone(),
                kind: ir::ExprKind::ColumnRef { qpath: path.clone(), index },
            })
    }

    pub fn alias(&self, alias: TableAlias) -> Result<Self> {
        // if no columns are specified, we only rename the table
        if !alias.columns.is_empty() && self.columns.len() != alias.columns.len() {
            bail!(
                "table expression has {} columns, but {} columns were specified in alias",
                self.columns.len(),
                alias.columns.len()
            );
        }

        let mut columns = rpds::Vector::new();
        for (i, (qpath, ty)) in self.columns.iter().enumerate() {
            let path = match alias.columns.get(i) {
                Some(name) => QPath::new(alias.table_name.clone(), name.clone()),
                None => qpath.clone(),
            };

            if self.columns.iter().any(|(p, _)| p == &path) {
                todo!("handle duplicate names (this will not work with current impl correctly)")
            }

            columns = columns.push_back((path, ty.clone()));
        }

        Ok(Scope { columns })
    }
}
