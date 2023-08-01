use indexmap::IndexSet;
use ir::visit::Visitor;

use crate::*;

pub(crate) struct SelectBinder<'a, 'env, S> {
    binder: &'a Binder<'env, S>,
    group_by: Box<[ir::Expr]>,
    aggregates: IndexSet<(Function, Box<[ir::Expr]>)>,
    unaggregated_exprs: Vec<ir::Expr>,
}

pub struct SelectBindOutput {
    pub aggregates: Box<[(Function, Box<[ir::Expr]>)]>,
    pub projection: Box<[ir::Expr]>,
    pub group_by: Box<[ir::Expr]>,
    pub order_by: Box<[ir::OrderExpr]>,
}

impl<'a, 'env, S: StorageEngine> SelectBinder<'a, 'env, S> {
    pub fn new(binder: &'a Binder<'env, S>, group_by: Box<[ir::Expr]>) -> Self {
        Self {
            binder,
            group_by,
            aggregates: Default::default(),
            unaggregated_exprs: Default::default(),
        }
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
            .collect::<Result<Box<_>>>()?;

        let order_by = order_by
            .iter()
            .map(|expr| self.bind_order_by_expr(tx, scope, expr))
            .collect::<Result<Box<_>>>()?;

        let aggregates = self.aggregates.into_iter().collect::<Box<_>>();

        // We pick the last unaggregated expression to report an error on as it will be the largest
        // expression and hopefully the most useful to the user.
        if !aggregates.is_empty() || !self.group_by.is_empty() {
            if let Some(unaggregated) = self.unaggregated_exprs.last() {
                bail!(
                    "expression `{}` must appear in the GROUP BY clause or be used in an aggregate function",
                    unaggregated,
                )
            }
        }

        Ok(SelectBindOutput { aggregates, projection, order_by, group_by: self.group_by })
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

    fn bind_maybe_aggregate_expr(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope<S>,
        expr: &ast::Expr,
    ) -> Result<ir::Expr> {
        match expr {
            // BIG FIXME this needs to be a general recursive thing for all expression not just
            // unary operators. Hacking to pass the tests for now
            ast::Expr::UnaryOp { op, expr } => {
                let expr = self.bind_maybe_aggregate_expr(tx, scope, expr)?;
                let (ty, op) = match op {
                    ast::UnaryOperator::Plus => todo!(),
                    ast::UnaryOperator::Minus => {
                        ensure!(
                            matches!(expr.ty, LogicalType::Int32),
                            "cannot negate value of type {}",
                            expr.ty
                        );
                        (LogicalType::Int32, ir::UnaryOp::Neg)
                    }
                    ast::UnaryOperator::Not => todo!(),
                    ast::UnaryOperator::PGBitwiseNot => todo!(),
                    ast::UnaryOperator::PGSquareRoot => todo!(),
                    ast::UnaryOperator::PGCubeRoot => todo!(),
                    ast::UnaryOperator::PGPostfixFactorial => todo!(),
                    ast::UnaryOperator::PGPrefixFactorial => todo!(),
                    ast::UnaryOperator::PGAbs => todo!(),
                };

                Ok(ir::Expr { ty, kind: ir::ExprKind::UnaryOp { op, expr: Box::new(expr) } })
            }
            ast::Expr::Function(f) => {
                let (function, args) = self.binder.bind_function(tx, scope, f)?;
                let ty = function.return_type();
                let kind = match function.kind() {
                    FunctionKind::Function => ir::ExprKind::FunctionCall { function, args },
                    FunctionKind::Aggregate => {
                        let (idx, _exists) = self.aggregates.insert_full((function, args));
                        ir::ExprKind::ColumnRef {
                            qpath: QPath::new("", expr.to_string()),
                            // the first N columns are the group by columns followed by the aggregate columns
                            index: TupleIndex::new(self.group_by.len() + idx),
                        }
                    }
                };

                Ok(ir::Expr { ty, kind })
            }
            _ => {
                /// Visitor to find all column references in an expression that are not in the group by
                struct ColumnRefVisitor;

                impl Visitor for ColumnRefVisitor {
                    fn visit_expr(&mut self, expr: &ir::Expr) -> bool {
                        matches!(expr.kind, ir::ExprKind::ColumnRef { .. }) || self.walk_expr(expr)
                    }
                }

                let expr = self.binder.bind_expr(tx, scope, expr)?;

                // if the select expression `expr` contains a column reference and `expr` is not
                // contained in the group by clause, then add it to the list of unaggregated expressionss
                let expr_contains_column_ref = ColumnRefVisitor.visit_expr(&expr);
                if expr_contains_column_ref {
                    if let Some(i) = self.group_by.iter().position(|g| g == &expr) {
                        // if the expression is in the group by clause, then replace it with a column reference to it
                        // (reminder: first G values are group by columns, followed by N aggregate columns)
                        let g = &self.group_by[i];
                        return Ok(ir::Expr {
                            ty: g.ty.clone(),
                            kind: ir::ExprKind::ColumnRef {
                                qpath: QPath::new("", g.to_string()),
                                index: TupleIndex::new(i),
                            },
                        });
                    } else {
                        self.unaggregated_exprs.push(expr.clone());
                    }
                }

                Ok(expr)
            }
        }
    }
}
