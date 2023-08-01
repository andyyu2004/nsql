use std::fmt;

use indexmap::IndexSet;
use ir::fold::{ExprFold, ExprFolder};

use crate::*;

pub(crate) struct SelectBinder<'a, 'env, S> {
    binder: &'a Binder<'env, S>,
    group_by: Box<[ir::Expr]>,
    aggregates: IndexSet<(Function, Box<[ir::Expr]>)>,
}

pub struct SelectBindOutput {
    pub aggregates: Box<[(Function, Box<[ir::Expr]>)]>,
    pub projection: Box<[ir::Expr]>,
    pub group_by: Box<[ir::Expr]>,
    pub order_by: Box<[ir::OrderExpr]>,
}

impl<'a, 'env, S: StorageEngine> SelectBinder<'a, 'env, S> {
    pub fn new(binder: &'a Binder<'env, S>, group_by: Box<[ir::Expr]>) -> Self {
        Self { binder, group_by, aggregates: Default::default() }
    }

    pub fn bind(
        mut self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope<S>,
        items: &[ast::SelectItem],
        order_by: &[ast::OrderByExpr],
    ) -> Result<SelectBindOutput> {
        let projection = items
            .iter()
            .map(|item| self.bind_select_item(tx, scope, item))
            .flatten_ok()
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|expr| self.check_unaggregated(expr))
            .collect::<Result<Box<_>>>()?;

        let order_by = order_by
            .iter()
            .map(|expr| self.bind_order_by_expr(tx, scope, expr))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|expr| self.check_unaggregated(expr))
            .collect::<Result<Box<_>>>()?;

        let aggregates = self.aggregates.into_iter().collect::<Box<_>>();

        Ok(SelectBindOutput { aggregates, projection, order_by, group_by: self.group_by })
    }

    // effectively the aggregate plan only exposes values of the group by clauses and the aggregate functions.
    // we must therefore ensure that all expressions in the select clause are projections of these values (i.e. one of the expressions above is a subexpression of the select/order expression)
    fn check_unaggregated<F: ExprFold + fmt::Display>(&self, expr: F) -> Result<F> {
        if self.aggregates.is_empty() && self.group_by.is_empty() {
            return Ok(expr);
        }

        struct ExprReplacer<'a> {
            group_by: &'a [ir::Expr],
            found_match: bool,
            contains_column_ref: bool,
        }

        impl ExprFolder for ExprReplacer<'_> {
            fn fold_expr(&mut self, expr: &ir::Expr) -> ir::Expr {
                const AGGREGATE_TABLE_NAME: Path = Path::Unqualified(Name::new_inline("agg"));
                self.contains_column_ref |= matches!(expr.kind, ir::ExprKind::ColumnRef { .. });

                match &expr.kind {
                    ir::ExprKind::ColumnRef { qpath, .. }
                        if qpath.prefix.as_ref() == &AGGREGATE_TABLE_NAME =>
                    {
                        // expression is a column reference to an aggregate, we're good
                        self.found_match = true;
                        expr.clone()
                    }
                    _ => {
                        if let Some(i) = self.group_by.iter().position(|g| g == expr) {
                            // if the expression is in the group by clause, then replace it with a column reference to it
                            // (reminder: first G values are group by columns, followed by N aggregate columns)
                            self.found_match = true;
                            let g = &self.group_by[i];
                            return ir::Expr {
                                ty: g.ty.clone(),
                                kind: ir::ExprKind::ColumnRef {
                                    qpath: QPath::new(AGGREGATE_TABLE_NAME, g.to_string()),
                                    index: TupleIndex::new(i),
                                },
                            };
                        };

                        // otherwise, recurse and process subexpressions
                        expr.fold_with(self)
                    }
                }
            }
        }

        let mut folder = ExprReplacer {
            group_by: &self.group_by,
            found_match: false,
            contains_column_ref: false,
        };

        let expr = expr.super_fold_with(&mut folder);

        // if the select expression `expr` contains a column reference and `expr` is not
        // a super-expression of any aggregate, then we have a problem
        if folder.found_match || !folder.contains_column_ref {
            Ok(expr)
        } else {
            bail!(
                "expression `{}` must appear in the GROUP BY clause or be used in an aggregate function",
                expr
            )
        }
    }

    fn bind_order_by_expr(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope<S>,
        order_expr: &ast::OrderByExpr,
    ) -> Result<ir::OrderExpr> {
        not_implemented!(!order_expr.nulls_first.unwrap_or(true));
        Ok(ir::OrderExpr {
            expr: self.bind_maybe_aggregate_expr(tx, scope, &order_expr.expr)?,
            asc: order_expr.asc.unwrap_or(true),
        })
    }

    fn bind_select_item(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope<S>,
        item: &ast::SelectItem,
    ) -> Result<Vec<ir::Expr>> {
        let expr = match item {
            ast::SelectItem::UnnamedExpr(expr) => {
                self.bind_maybe_aggregate_expr(tx, scope, expr)?
            }
            ast::SelectItem::ExprWithAlias { expr, alias } => {
                self.bind_maybe_aggregate_expr(tx, scope, expr)?.alias(&alias.value)
            }
            _ => return self.binder.bind_select_item(tx, scope, item),
        };

        Ok(vec![expr])
    }

    fn bind_expr(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope<S>,
        expr: &ast::Expr,
    ) -> Result<ir::Expr> {
        self.binder
            .walk_expr(tx, scope, expr, |expr| self.bind_maybe_aggregate_expr(tx, scope, expr))
    }

    fn bind_maybe_aggregate_expr(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope<S>,
        expr: &ast::Expr,
    ) -> Result<ir::Expr> {
        match expr {
            ast::Expr::Function(f) => {
                let (function, args) = self.binder.bind_function(tx, scope, f)?;
                let ty = function.return_type();
                let kind = match function.kind() {
                    FunctionKind::Function => ir::ExprKind::FunctionCall { function, args },
                    FunctionKind::Aggregate => {
                        let (idx, _exists) = self.aggregates.insert_full((function, args));
                        ir::ExprKind::ColumnRef {
                            qpath: QPath::new("agg", expr.to_string()),
                            // the first N columns are the group by columns followed by the aggregate columns
                            index: TupleIndex::new(self.group_by.len() + idx),
                        }
                    }
                };

                Ok(ir::Expr { ty, kind })
            }
            _ => self.bind_expr(tx, scope, expr),
        }
    }
}
