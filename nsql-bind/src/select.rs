use std::fmt;
use std::ops::ControlFlow;

use indexmap::IndexSet;
use ir::fold::{ExprFold, Folder};

use crate::*;

pub(crate) struct SelectBinder<'b, 'a, 'env, 'txn, S, M> {
    binder: &'b mut Binder<'a, 'env, 'txn, S, M>,
    group_by: Box<[ir::Expr]>,
    aggregates: IndexSet<(ir::MonoFunction, Box<[ir::Expr]>)>,
}

impl<'b, 'a, 'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    SelectBinder<'b, 'a, 'env, 'txn, S, M>
{
    pub fn new(binder: &'b mut Binder<'a, 'env, 'txn, S, M>, group_by: Box<[ir::Expr]>) -> Self {
        Self { binder, group_by, aggregates: Default::default() }
    }

    pub fn bind(
        mut self,
        scope: &Scope,
        source: Box<ir::QueryPlan>,
        items: &[ast::SelectItem],
        order_by: &[ast::OrderByExpr],
        having: Option<&ast::Expr>,
    ) -> Result<(Scope, Box<ir::QueryPlan>)> {
        // contains any additional expressions that are required to evaluate the order_by, or having clauses
        let mut bound_extra_exprs = vec![];
        let mut extra_exprs = IndexSet::new();

        // if the order by expression contains a column reference,
        // then we need to add the expression to the projection to make any potential aliases available
        for expr in order_by.iter().map(|order_expr| &order_expr.expr).chain(having) {
            if let ControlFlow::Break(()) = ast::visit_expressions(expr, |expr| match expr {
                ast::Expr::Identifier { .. } | ast::Expr::CompoundIdentifier { .. } => {
                    ControlFlow::Break(())
                }
                _ => ControlFlow::Continue(()),
            }) {
                // We add any expressions that we can successfully bind to the pre-projection.
                // The bind may fail either because it tries to reference an alias or is otherwise bad.
                if let Ok(bound_expr) = self.bind_expr(scope, expr) {
                    // alias to make it unnameable to avoid name clashes and thus artificial `ambiguous reference to column` errors
                    bound_extra_exprs.push(bound_expr.alias(""));
                    extra_exprs.insert(expr.clone());
                }
            }
        }

        let pre_projection = items
            .iter()
            .map(|item| self.bind_select_item(scope, item))
            .flatten_ok()
            .collect::<Result<Vec<_>>>()?;

        let original_projection_len = pre_projection.len();

        let pre_projection = pre_projection
            .into_iter()
            .chain(bound_extra_exprs)
            .map(|expr| self.check_unaggregated(expr))
            .collect::<Result<Box<_>>>()?;

        let scope = scope.project(&pre_projection);

        let order_by = order_by
            .iter()
            .map(|order_expr| match extra_exprs.get_index_of(&order_expr.expr) {
                // if the order_by was not added to the pre-projection, we bind it normally
                None => self.bind_order_by_expr(&scope, order_expr),
                // otherwise we bind it to the corresponding column in the pre-projection
                Some(k) => {
                    let target_index = original_projection_len + k;
                    let target_expr = &pre_projection[target_index];
                    Ok(ir::OrderExpr {
                        asc: order_expr.asc.unwrap_or(true),
                        expr: ir::Expr {
                            ty: target_expr.ty.clone(),
                            kind: ir::ExprKind::ColumnRef(ir::ColumnRef::new(
                                TupleIndex::new(target_index),
                                QPath::new("", target_expr.to_string()),
                            )),
                        },
                    })
                }
            })
            .collect::<Result<Box<_>>>()?;

        // we add another projection that removes extra columns that we added for the order_by
        let post_projection = (0..original_projection_len)
            .map(|i| {
                let expr = &pre_projection[i];
                ir::Expr {
                    ty: expr.ty.clone(),
                    kind: ir::ExprKind::ColumnRef(ir::ColumnRef::new(
                        TupleIndex::new(i),
                        QPath::new("", expr.name()),
                    )),
                }
            })
            .collect::<Vec<_>>();

        let having = having
            .map(|expr| {
                let expr = match extra_exprs.get_index_of(expr) {
                    Some(k) => {
                        let target_index = original_projection_len + k;
                        let target_expr = &pre_projection[target_index];
                        ir::Expr {
                            ty: target_expr.ty.clone(),
                            kind: ir::ExprKind::ColumnRef(ir::ColumnRef::new(
                                TupleIndex::new(target_index),
                                QPath::new("", target_expr.to_string()),
                            )),
                        }
                    }
                    None => self.bind_expr(&scope, expr)?,
                };
                ensure!(
                    matches!(expr.ty, LogicalType::Bool | LogicalType::Null),
                    "HAVING clause must be of type bool, found {}",
                    expr.ty
                );
                Ok(expr)
            })
            .transpose()?;

        let scope = scope.project(&post_projection);

        let aggregates = self.aggregates.into_iter().collect::<Box<_>>();
        let mut plan =
            source.aggregate(self.group_by, aggregates).project(pre_projection).order_by(order_by);

        if let Some(having) = having {
            plan = plan.filter(having);
        }

        plan = plan.project(post_projection);

        Ok((scope, plan))
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
            unaggregated_column_ref_expr: Option<ir::Expr>,
        }

        impl Folder for ExprReplacer<'_> {
            #[inline]
            fn as_dyn(&mut self) -> &mut dyn Folder {
                self
            }

            fn fold_expr(&mut self, plan: &mut ir::QueryPlan, expr: ir::Expr) -> ir::Expr {
                const AGGREGATE_TABLE_NAME: Path = Path::Unqualified(Name::new_inline("agg"));

                match &expr.kind {
                    ir::ExprKind::ColumnRef(ir::ColumnRef { qpath, .. })
                        if qpath.prefix.as_ref() == &AGGREGATE_TABLE_NAME =>
                    {
                        // expression is a column reference to an aggregate, we're good
                        self.found_match = true;
                        expr.clone()
                    }
                    _ => {
                        if let Some(i) = self.group_by.iter().position(|g| g == &expr) {
                            // if the expression is in the group by clause, then replace it with a column reference to it
                            // (reminder: first G values are group by columns, followed by N aggregate columns)
                            self.found_match = true;
                            let g = &self.group_by[i];
                            return ir::Expr {
                                ty: g.ty.clone(),
                                kind: ir::ExprKind::ColumnRef(ir::ColumnRef::new(
                                    TupleIndex::new(i),
                                    QPath::new("", g.to_string()),
                                )),
                            };
                        } else if let ir::ExprKind::ColumnRef { .. } = expr.kind {
                            self.unaggregated_column_ref_expr = Some(expr.clone());
                        }

                        // otherwise, recurse and process subexpressions
                        expr.fold_with(self, plan)
                    }
                }
            }
        }

        let mut folder = ExprReplacer {
            group_by: &self.group_by,
            found_match: false,
            unaggregated_column_ref_expr: None,
        };

        // this folder doesn't require the plan, so we just pass in an empty plan
        let expr = expr.super_fold_with(&mut folder, &mut ir::QueryPlan::DummyScan);

        // if the select expression `expr` contains a column reference and `expr` is not
        // a super-expression of any aggregate, then we have a problem
        match folder.unaggregated_column_ref_expr {
            // if there is an unaggregated column reference, then report an error
            Some(col_expr) if !folder.found_match => {
                bail!(
                    "expression `{col_expr}` must appear in the GROUP BY clause or be used in an aggregate function",
                )
            }
            _ => Ok(expr),
        }
    }

    fn bind_order_by_expr(
        &mut self,
        scope: &Scope,
        order_expr: &ast::OrderByExpr,
    ) -> Result<ir::OrderExpr> {
        not_implemented_if!(!order_expr.nulls_first.unwrap_or(true));
        let expr = self.bind_expr(scope, &order_expr.expr)?;
        let expr = match expr.kind {
            ir::ExprKind::Literal(ir::Value::Int64(i)) => {
                ensure!(
                    i >= 1,
                    "order index out of range, should be between 1 and {}",
                    scope.len()
                );
                let i = i as usize - 1;
                ensure!(
                    i < scope.len(),
                    "order index out of range, should be between 1 and {}",
                    scope.len(),
                );

                let (qpath, ty) = scope.lookup_by_index(i);
                ir::Expr {
                    ty,
                    kind: ir::ExprKind::ColumnRef(ir::ColumnRef::new(
                        TupleIndex::new(i),
                        qpath.clone(),
                    )),
                }
            }
            _ => expr,
        };
        Ok(ir::OrderExpr { expr, asc: order_expr.asc.unwrap_or(true) })
    }
}

impl<'a, 'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    ExprBinder<'a, 'env, 'txn, S, M> for SelectBinder<'_, 'a, 'env, 'txn, S, M>
{
    fn binder(&mut self) -> &mut Binder<'a, 'env, 'txn, S, M> {
        self.binder
    }

    fn bind_function(&mut self, scope: &Scope, f: &ast::Function) -> Result<ir::Expr> {
        let function_expr = self.binder.bind_function(scope, f)?;
        let display = function_expr.to_string();
        let kind = match function_expr.kind {
            ir::ExprKind::FunctionCall { function, args }
                if matches!(function.kind(), FunctionKind::Aggregate) =>
            {
                let (idx, _exists) = self.aggregates.insert_full((function, args));
                ir::ExprKind::ColumnRef(ir::ColumnRef::new(
                    // the first N columns are the group by columns followed by the aggregate columns
                    TupleIndex::new(self.group_by.len() + idx),
                    QPath::new("agg", display),
                ))
            }
            kind => kind,
        };

        Ok(ir::Expr { ty: function_expr.ty, kind })
    }
}
