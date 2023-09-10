use crate::*;

pub trait Folder {
    fn as_dyn(&mut self) -> &mut dyn Folder;

    #[inline]
    fn fold_plan(&mut self, plan: QueryPlan) -> QueryPlan {
        plan.fold_with(self.as_dyn())
    }

    /// Replace an expression, this must be type preserving.
    /// Implement this method if you want to replace an expression with a different expression.
    #[inline]
    fn fold_expr(&mut self, plan: &mut QueryPlan, expr: Expr) -> Expr {
        expr.fold_with(self.as_dyn(), plan)
    }

    #[inline]
    fn fold_boxed_plan(&mut self, mut boxed_plan: Box<QueryPlan>) -> Box<QueryPlan> {
        self.fold_plan_in_place(&mut boxed_plan);
        boxed_plan
    }

    #[inline]
    fn fold_plan_in_place(&mut self, plan: &mut QueryPlan) {
        *plan = self.fold_plan(mem::take(&mut *plan));
    }

    /// Convenience method for folding a boxed expression, implement `fold_expr` not this
    #[inline]
    fn fold_boxed_expr(&mut self, plan: &mut QueryPlan, mut boxed_expr: Box<Expr>) -> Box<Expr> {
        self.fold_expr_in_place(plan, &mut boxed_expr);
        boxed_expr
    }

    #[inline]
    fn fold_expr_in_place(&mut self, plan: &mut QueryPlan, expr: &mut Expr) {
        *expr = self.fold_expr(plan, mem::take(&mut *expr));
    }

    #[inline]
    fn fold_exprs(&mut self, plan: &mut QueryPlan, mut exprs: Box<[Expr]>) -> Box<[Expr]> {
        for expr in &mut exprs[..] {
            self.fold_expr_in_place(plan, expr);
        }
        exprs
    }
}

pub trait PlanFold: Sized {
    fn fold_with(self, folder: &mut dyn Folder) -> Self;

    #[inline]
    fn super_fold_with(self, folder: &mut dyn Folder) -> Self {
        self.fold_with(folder)
    }
}

impl PlanFold for Plan {
    fn fold_with(self, folder: &mut dyn Folder) -> Self {
        match self {
            Plan::Show(_)
            | Plan::Drop(_)
            | Plan::Transaction(_)
            | Plan::SetVariable { name: _, value: _, scope: _ } => self,
            Plan::Explain(query) => Plan::Explain(Box::new(query.super_fold_with(folder))),
            Plan::Query(query) => Plan::Query(Box::new(query.super_fold_with(folder))),
        }
    }
}

impl PlanFold for QueryPlan {
    #[inline]
    fn super_fold_with(self, folder: &mut dyn Folder) -> Self {
        folder.fold_plan(self)
    }

    fn fold_with(self, folder: &mut dyn Folder) -> Self {
        match self {
            // maybe can incorporate an expression folder in here too?
            QueryPlan::TableScan { table, projection, projected_schema } => {
                QueryPlan::TableScan { table, projection, projected_schema }
            }
            QueryPlan::Projection { source, projection, projected_schema } => {
                let mut source = folder.fold_boxed_plan(source);
                QueryPlan::Projection {
                    projection: projection
                        .into_vec()
                        .into_iter()
                        .map(|e| folder.fold_expr(&mut source, e))
                        .collect(),
                    projected_schema,
                    source,
                }
            }
            QueryPlan::Filter { source, predicate } => {
                let mut source = folder.fold_boxed_plan(source);
                QueryPlan::Filter { predicate: folder.fold_expr(&mut source, predicate), source }
            }

            QueryPlan::Unnest { expr, schema } => QueryPlan::Unnest {
                expr: folder.fold_expr(&mut QueryPlan::DummyScan, expr),
                schema,
            },
            QueryPlan::Values { values, schema } => QueryPlan::Values { values, schema },
            QueryPlan::Join { schema, join, lhs, rhs } => QueryPlan::Join {
                schema,
                join,
                lhs: folder.fold_boxed_plan(lhs),
                rhs: folder.fold_boxed_plan(rhs),
            },
            QueryPlan::Limit { source, limit, exceeded_message } => {
                QueryPlan::Limit { source: folder.fold_boxed_plan(source), limit, exceeded_message }
            }
            QueryPlan::Order { source, order } => {
                let mut source = folder.fold_boxed_plan(source);
                QueryPlan::Order {
                    order: order
                        .into_vec()
                        .into_iter()
                        .map(|order_expr| order_expr.fold_with(folder, &mut source))
                        .collect(),
                    source,
                }
            }
            QueryPlan::DummyScan => QueryPlan::DummyScan,
            QueryPlan::Insert { table, source, returning, schema } => {
                let mut source = folder.fold_boxed_plan(source);
                QueryPlan::Insert {
                    table,
                    returning: folder.fold_exprs(&mut source, returning),
                    schema,
                    source,
                }
            }
            QueryPlan::Update { table, source, returning, schema } => {
                let mut source = folder.fold_boxed_plan(source);
                QueryPlan::Update {
                    table,
                    returning: returning.map(|exprs| folder.fold_exprs(&mut source, exprs)),
                    source,
                    schema,
                }
            }
            QueryPlan::Aggregate { source, aggregates, group_by, schema } => {
                let mut source = folder.fold_boxed_plan(source);
                QueryPlan::Aggregate {
                    aggregates: aggregates
                        .into_vec()
                        .into_iter()
                        .map(|(f, args)| (f, folder.fold_exprs(&mut source, args)))
                        .collect(),
                    group_by: folder.fold_exprs(&mut source, group_by),
                    source: folder.fold_boxed_plan(source),
                    schema,
                }
            }
            QueryPlan::Union { schema, lhs, rhs } => QueryPlan::Union {
                schema,
                lhs: folder.fold_boxed_plan(lhs),
                rhs: folder.fold_boxed_plan(rhs),
            },
            QueryPlan::CteScan { name, schema } => QueryPlan::CteScan { name, schema },
            QueryPlan::Cte { cte, child } => QueryPlan::Cte {
                cte: Cte { name: cte.name, plan: folder.fold_boxed_plan(cte.plan) },
                child: folder.fold_boxed_plan(child),
            },
        }
    }
}

pub trait ExprFold: Sized {
    fn fold_with(self, folder: &mut dyn Folder, plan: &mut QueryPlan) -> Self;

    fn super_fold_with(self, folder: &mut dyn Folder, plan: &mut QueryPlan) -> Self {
        self.fold_with(folder, plan)
    }
}

impl ExprFold for OrderExpr {
    #[inline]
    fn fold_with(self, folder: &mut dyn Folder, plan: &mut QueryPlan) -> Self {
        Self { expr: folder.fold_expr(plan, self.expr), asc: self.asc }
    }
}

impl ExprFold for Expr {
    fn super_fold_with(self, folder: &mut dyn Folder, plan: &mut QueryPlan) -> Self {
        folder.fold_expr(plan, self)
    }

    fn fold_with(self, folder: &mut dyn Folder, plan: &mut QueryPlan) -> Self {
        let kind = match self.kind {
            ExprKind::Literal(lit) => ExprKind::Literal(lit),
            ExprKind::ColumnRef(col) => ExprKind::ColumnRef(col),
            ExprKind::Array(exprs) => ExprKind::Array(folder.fold_exprs(plan, exprs)),
            ExprKind::Alias { alias, expr } => {
                ExprKind::Alias { alias: alias.clone(), expr: folder.fold_boxed_expr(plan, expr) }
            }
            ExprKind::UnaryOperator { operator, expr } => {
                ExprKind::UnaryOperator { operator, expr: folder.fold_boxed_expr(plan, expr) }
            }
            ExprKind::FunctionCall { function, args } => ExprKind::FunctionCall {
                function: function.clone(),
                args: folder.fold_exprs(plan, args),
            },
            ExprKind::Case { scrutinee, cases, else_result } => ExprKind::Case {
                scrutinee: folder.fold_boxed_expr(plan, scrutinee),
                cases: cases
                    .into_vec()
                    .into_iter()
                    .map(|case| Case {
                        when: folder.fold_expr(plan, case.when),
                        then: folder.fold_expr(plan, case.then),
                    })
                    .collect(),
                else_result: else_result.map(|expr| folder.fold_boxed_expr(plan, expr)),
            },
            ExprKind::Subquery(kind, plan) => {
                ExprKind::Subquery(kind, folder.fold_boxed_plan(plan))
            }
            ExprKind::BinaryOperator { operator, lhs, rhs } => ExprKind::BinaryOperator {
                operator,
                lhs: folder.fold_boxed_expr(plan, lhs),
                rhs: folder.fold_boxed_expr(plan, rhs),
            },
            ExprKind::Compiled(expr) => ExprKind::Compiled(expr),
        };

        Expr { ty: self.ty.clone(), kind }
    }
}
