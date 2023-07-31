use ir::visit::Visitor;

use crate::*;

pub(crate) struct SelectBinder<'a, 'env, S> {
    binder: &'a Binder<'env, S>,
    group_by: Box<[ir::Expr]>,
    aggregates: Vec<(Function, Box<[ir::Expr]>)>,
    unaggregated_exprs: Vec<ir::Expr>,
}

pub struct SelectBindOutput {
    pub aggregates: Box<[(Function, Box<[ir::Expr]>)]>,
    pub projection: Box<[ir::Expr]>,
    pub group_by: Box<[ir::Expr]>,
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
    ) -> Result<SelectBindOutput> {
        let projection = items
            .iter()
            .map(|item| self.bind_select_item(tx, scope, item))
            .flatten_ok()
            .collect::<Result<Box<_>>>()?;

        let aggregates = self.aggregates.into_boxed_slice();

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

        Ok(SelectBindOutput { aggregates, projection, group_by: self.group_by })
    }

    fn bind_select_item(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope<S>,
        item: &ast::SelectItem,
    ) -> Result<Vec<ir::Expr>> {
        let expr = match item {
            ast::SelectItem::UnnamedExpr(expr) => self.bind_select_expr(tx, scope, expr)?,
            ast::SelectItem::ExprWithAlias { expr, alias } => {
                self.bind_select_expr(tx, scope, expr)?.alias(&alias.value)
            }
            _ => return self.binder.bind_select_item(tx, scope, item),
        };

        Ok(vec![expr])
    }

    fn bind_select_expr(
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
                        // the first N columns are the group by columns followed by the aggregate columns
                        let idx = self.group_by.len() + self.aggregates.len();
                        self.aggregates.push((function, args));
                        ir::ExprKind::ColumnRef {
                            qpath: QPath::new("", expr.to_string()),
                            index: TupleIndex::new(idx),
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
