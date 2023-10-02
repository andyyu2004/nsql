use std::fmt;

use anyhow::{bail, ensure};
use ir::QPath;
use nsql_catalog::{Column, SystemEntity, Table};
use nsql_core::{LogicalType, Name, Oid, Schema};
use nsql_storage_engine::{StorageEngine, Transaction};

use super::unbound;
use crate::{Binder, Path, Result, TableAlias};

#[derive(Default, Clone, PartialEq, Eq)]
pub(crate) struct Scope {
    parent: Option<Box<Scope>>,
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

impl Scope {
    pub fn subscope(&self) -> Self {
        Self { parent: Some(Box::new(self.clone())), columns: rpds::Vector::new() }
    }

    fn with_columns(&self, columns: rpds::Vector<(QPath, LogicalType)>) -> Self {
        Self { columns, parent: self.parent.clone() }
    }

    pub fn project(&self, projection: &[ir::Expr]) -> Self {
        let columns = projection
            .iter()
            .map(|expr| match &expr.kind {
                ir::ExprKind::Alias { alias, .. } => {
                    (QPath::new("", Name::clone(alias)), expr.ty.clone())
                }
                ir::ExprKind::ColumnRef(ir::ColumnRef { qpath, .. }) => {
                    (qpath.clone(), expr.ty.clone())
                }
                // the generated column name is a string representation of the expression
                _ => (QPath::new("unknowntabletodo", expr.name()), expr.ty.clone()),
            })
            .collect();

        self.with_columns(columns)
    }

    /// Find a reference to a table:
    /// If it is a cte, return a reference to the cte and it's scope.
    /// Otherwise, add a new table and its columns to the scope.
    pub fn bind_table<'env, S: StorageEngine>(
        &self,
        binder: &Binder<'env, S>,
        tx: &dyn Transaction<'env, S>,
        path: &Path,
        alias: Option<&TableAlias>,
    ) -> Result<(Scope, TableBinding)> {
        match path {
            Path::Unqualified(name) if let Some((kind, scope, plan)) = binder.ctes.borrow().get(name) => match kind {
                CteKind::Inline => Ok((scope.clone(), TableBinding::InlineCte(plan.clone()))),
                CteKind::Materialized => Ok((scope.clone(), TableBinding::MaterializedCte(name.clone(), plan.schema().clone()))),
            }
            _ => {
                let (scope, table) = self.bind_base_table(binder, tx, path, alias)?;
                Ok((scope, TableBinding::Table(table)))
            }
        }
    }

    /// Find a reference to a base table
    #[tracing::instrument(skip(self, tx, binder))]
    pub fn bind_base_table<'env, S: StorageEngine>(
        &self,
        binder: &Binder<'env, S>,
        tx: &dyn Transaction<'env, S>,
        path: &Path,
        alias: Option<&TableAlias>,
    ) -> Result<(Scope, Oid<Table>)> {
        tracing::debug!("binding table");
        let mut columns = self.columns.clone();

        let table_oid = binder.bind_namespaced_entity::<Table>(tx, &path)?;
        let table = binder.catalog.table(tx, table_oid)?;
        let table_columns = table.columns(binder.catalog, tx)?;

        if let Some(alias) = alias {
            // if no columns are specified, we only rename the table
            if !alias.columns.is_empty() && table_columns.len() != alias.columns.len() {
                bail!(
                    "table `{}` has {} columns, but {} columns were specified in alias",
                    path,
                    table_columns.len(),
                    alias.columns.len()
                );
            }
        }

        let path = match alias {
            Some(alias) => Path::Unqualified(alias.table_name.clone()),
            None => path.clone(),
        };

        for (i, column) in table_columns.into_iter().enumerate() {
            let name = match alias {
                Some(alias) if !alias.columns.is_empty() => alias.columns[i].clone(),
                _ => column.name(),
            };

            columns.push_back_mut((QPath::new(path.clone(), name), column.logical_type()));
        }

        Ok((self.with_columns(columns), table_oid))
    }

    pub fn lookup_by_index(&self, index: usize) -> (QPath, LogicalType) {
        self.columns[index].clone()
    }

    pub fn lookup_column(&self, path: &Path) -> Result<(LogicalType, ir::ColumnRef)> {
        self.lookup_column_rec(path, 0)
    }

    fn lookup_column_rec(&self, path: &Path, level: u8) -> Result<(LogicalType, ir::ColumnRef)> {
        match self.lookup_column_inner(path, level)? {
            Some((ty, column_ref)) => Ok((ty, column_ref)),
            None => match &self.parent {
                Some(parent) => parent.lookup_column_rec(path, level + 1),
                None => Err(unbound!(Column, path)),
            },
        }
    }

    fn lookup_column_inner(
        &self,
        path: &Path,
        level: u8,
    ) -> Result<Option<(LogicalType, ir::ColumnRef)>> {
        match path {
            Path::Qualified(qpath) => {
                let idx = match self.columns.iter().position(|(p, _)| p == qpath) {
                    Some(idx) => idx,
                    None => return Ok(None),
                };

                let (qpath, ty) = &self.columns[idx];
                Ok(Some((
                    ty.clone(),
                    ir::ColumnRef { qpath: qpath.clone(), index: ir::TupleIndex::new(idx), level },
                )))
            }
            Path::Unqualified(column_name) => {
                match &self
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, (p, _))| &p.name == column_name)
                    .collect::<Vec<_>>()[..]
                {
                    [] => Ok(None),
                    [(idx, (qpath, ty))] => Ok(Some((
                        ty.clone(),
                        ir::ColumnRef {
                            qpath: qpath.clone(),
                            index: ir::TupleIndex::new(*idx),
                            level,
                        },
                    ))),
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

    #[tracing::instrument(skip(self, values))]
    pub fn bind_values(&self, values: &ir::Values) -> Scope {
        let mut columns = self.columns.clone();

        for (i, expr) in values[0].iter().enumerate() {
            // default column names are col1, col2, etc.
            let name = Name::from(format!("col{}", i + 1));
            columns.push_back_mut((QPath::new("values", name), expr.ty.clone()));
        }

        self.with_columns(columns)
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

        Ok(self.with_columns(self.columns.push_back((QPath::new("", "unnest"), ty))))
    }

    pub fn len(&self) -> usize {
        self.columns.len()
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
                kind: ir::ExprKind::ColumnRef(ir::ColumnRef::new(index, path.clone())),
            })
    }

    pub fn join(&self, other: &Self) -> Self {
        let mut columns = self.columns.clone();
        for (path, ty) in other.columns.iter() {
            columns.push_back_mut((path.clone(), ty.clone()));
        }

        self.with_columns(columns)
    }

    pub fn alias(&self, alias: TableAlias) -> Result<Self> {
        // if no columns are specified, we only rename the table
        if !alias.columns.is_empty() && self.columns.len() < alias.columns.len() {
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

            columns = columns.push_back((path, ty.clone()));
        }

        Ok(self.with_columns(columns))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum CteKind {
    // FIXME there is no syntax supported by the parser to specify materialized/not materialized yet.
    // We should be smart enough and use an inline cte if there is only one reference to it.
    #[allow(dead_code)]
    Inline,
    Materialized,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TableBinding {
    /// A materialized cte materializes into a pseudo-table and is read by a `CTE scan` node.
    MaterializedCte(Name, Schema),
    /// An inline (NOT MATERIALIZED) CTE is where the plan gets "inlined" into its use-sites.
    InlineCte(Box<ir::QueryPlan>),
    /// A standard base table
    Table(Oid<Table>),
}
