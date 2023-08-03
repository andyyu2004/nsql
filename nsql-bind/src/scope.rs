use anyhow::{bail, ensure};
use ir::QPath;
use nsql_catalog::{Column, SystemEntity, Table};
use nsql_core::{LogicalType, Name, Oid};
use nsql_storage_engine::{StorageEngine, Transaction};

use super::unbound;
use crate::{Binder, Path, Result, TableAlias};

#[derive(Debug, Default)]
pub(crate) struct Scope {
    // FIXME tables need to store more info than this
    tables: rpds::HashTrieMap<Path, ()>,
    bound_columns: rpds::Vector<(QPath, LogicalType)>,
}

impl Clone for Scope {
    fn clone(&self) -> Self {
        Self { tables: self.tables.clone(), bound_columns: self.bound_columns.clone() }
    }
}

impl Scope {
    pub fn project(&self, projection: &[ir::Expr]) -> Self {
        let bound_columns = projection
            .iter()
            .map(|expr| match &expr.kind {
                // FIXME this is missing the alias case right? need a test case
                // ir::ExprKind::Alias {..}
                ir::ExprKind::ColumnRef { qpath, .. } => (qpath.clone(), expr.ty.clone()),
                // the generated column name is a string representation of the expression
                _ => (QPath::new("unknowntabletodo", expr.to_string()), expr.ty.clone()),
            })
            .collect();

        Self { bound_columns, tables: Default::default() }
    }

    pub fn merge(&self, other: &Self) -> Result<Self> {
        let mut tables = self.tables.clone();
        for (path, ()) in other.tables.iter() {
            tables.insert_mut(path.clone(), ());
        }

        let mut bound_columns = self.bound_columns.clone();
        for (path, ty) in other.bound_columns.iter() {
            bound_columns.push_back_mut((path.clone(), ty.clone()));
        }

        Ok(Self { tables, bound_columns })
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
                _ => column.name(),
            };

            bound_columns
                .push_back_mut((QPath::new(table_path.clone(), name), column.logical_type()));
        }

        Ok(Self { tables: self.tables.insert(table_path, ()), bound_columns })
    }

    pub fn lookup_column(&self, path: &Path) -> Result<(QPath, LogicalType, ir::TupleIndex)> {
        match path {
            Path::Qualified(qpath) => {
                let idx = self
                    .bound_columns
                    .iter()
                    .position(|(p, _)| p == qpath)
                    .ok_or_else(|| unbound!(Column, path))?;

                let (qpath, ty) = &self.bound_columns[idx];
                Ok((qpath.clone(), ty.clone(), ir::TupleIndex::new(idx)))
            }
            Path::Unqualified(column_name) => {
                match &self
                    .bound_columns
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
        let mut bound_columns = self.bound_columns.clone();

        for (i, expr) in values[0].iter().enumerate() {
            // default column names are col1, col2, etc.
            let name = Name::from(format!("col{}", i + 1));
            bound_columns.push_back_mut((QPath::new("values", name), expr.ty.clone()));
        }

        Self { tables: self.tables.clone(), bound_columns }
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

        Ok(Self {
            tables: self.tables.clone(),
            bound_columns: self.bound_columns.push_back((QPath::new("", "unnest"), ty)),
        })
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
                kind: ir::ExprKind::ColumnRef { qpath: path.clone(), index },
            })
    }

    pub fn alias(&self, alias: TableAlias) -> Result<Self> {
        // if no columns are specified, we only rename the table
        if !alias.columns.is_empty() && self.bound_columns.len() != alias.columns.len() {
            bail!(
                "table expression has {} columns, but {} columns were specified in alias",
                self.bound_columns.len(),
                alias.columns.len()
            );
        }

        let mut columns = rpds::Vector::new();
        for (i, (qpath, ty)) in self.bound_columns.iter().enumerate() {
            let path = match alias.columns.get(i) {
                Some(name) => QPath::new(alias.table_name.clone(), name.clone()),
                None => qpath.clone(),
            };

            if self.bound_columns.iter().any(|(p, _)| p == &path) {
                todo!("handle duplicate names (this will not work with current impl correctly)")
            }

            columns = columns.push_back((path, ty.clone()));
        }

        Ok(Scope {
            tables: self.tables.insert(Path::Unqualified(alias.table_name), ()),
            bound_columns: columns,
        })
    }
}
