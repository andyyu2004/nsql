use crate::*;

pub trait Folder {
    fn as_dyn(&mut self) -> &mut dyn Folder;

    #[inline]
    fn fold_plan(&mut self, plan: Plan) -> Plan {
        plan.fold_with(self.as_dyn())
    }

    /// Replace an expression, this must be type preserving.
    /// Implement this method if you want to replace an expression with a different expression.
    #[inline]
    fn fold_expr(&mut self, plan: &mut Plan, expr: Expr) -> Expr {
        expr.fold_with(self.as_dyn(), plan)
    }

    #[inline]
    fn fold_boxed_plan(&mut self, mut boxed_plan: Box<Plan>) -> Box<Plan> {
        self.fold_plan_in_place(&mut boxed_plan);
        boxed_plan
    }

    #[inline]
    fn fold_plan_in_place(&mut self, plan: &mut Plan) {
        *plan = self.fold_plan(mem::take(&mut *plan));
    }

    /// Convenience method for folding a boxed expression, implement `fold_expr` not this
    #[inline]
    fn fold_boxed_expr(&mut self, plan: &mut Plan, mut boxed_expr: Box<Expr>) -> Box<Expr> {
        self.fold_expr_in_place(plan, &mut boxed_expr);
        boxed_expr
    }

    #[inline]
    fn fold_expr_in_place(&mut self, plan: &mut Plan, expr: &mut Expr) {
        *expr = self.fold_expr(plan, mem::take(&mut *expr));
    }

    #[inline]
    fn fold_exprs(&mut self, plan: &mut Plan, mut exprs: Box<[Expr]>) -> Box<[Expr]> {
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
    #[inline]
    fn super_fold_with(self, folder: &mut dyn Folder) -> Self {
        folder.fold_plan(self)
    }

    fn fold_with(self, folder: &mut dyn Folder) -> Self {
        match self {
            Plan::Show(_)
            | Plan::Drop(_)
            | Plan::Transaction(_)
            | Plan::CreateNamespace(_)
            | Plan::SetVariable { name: _, value: _, scope: _ }
            | Plan::CreateTable(_) => self,

            // maybe can incorporate an expression folder in here too?
            Plan::TableScan { table, projection, projected_schema } => {
                Plan::TableScan { table, projection, projected_schema }
            }
            Plan::Projection { source, projection, projected_schema } => {
                let mut source = folder.fold_boxed_plan(source);
                Plan::Projection {
                    projection: projection
                        .into_vec()
                        .into_iter()
                        .map(|e| folder.fold_expr(&mut source, e))
                        .collect(),
                    projected_schema,
                    source,
                }
            }
            Plan::Filter { source, predicate } => {
                let mut source = folder.fold_boxed_plan(source);
                Plan::Filter { predicate: folder.fold_expr(&mut source, predicate), source }
            }

            Plan::Unnest { expr, schema } => {
                Plan::Unnest { expr: folder.fold_expr(&mut Plan::Empty, expr), schema }
            }
            Plan::Values { values, schema } => Plan::Values { values, schema },
            Plan::Join { schema, join, lhs, rhs } => {
                let mut fold_join_constraint = |constraint| match constraint {
                    JoinConstraint::On(expr) => {
                        // FIXME: not sure how to pass the plan here
                        JoinConstraint::On(folder.fold_expr(&mut Plan::Empty, expr))
                    }
                    JoinConstraint::None => JoinConstraint::None,
                };
                let join = match join {
                    Join::Inner(constraint) => Join::Inner(fold_join_constraint(constraint)),
                    Join::Left(constraint) => Join::Left(fold_join_constraint(constraint)),
                    Join::Right(constraint) => Join::Right(fold_join_constraint(constraint)),
                    Join::Full(constraint) => Join::Full(fold_join_constraint(constraint)),
                    Join::Cross => Join::Cross,
                };

                Plan::Join {
                    schema,
                    join,
                    lhs: folder.fold_boxed_plan(lhs),
                    rhs: folder.fold_boxed_plan(rhs),
                }
            }
            Plan::Limit { source, limit, exceeded_message } => {
                Plan::Limit { source: folder.fold_boxed_plan(source), limit, exceeded_message }
            }
            Plan::Order { source, order } => {
                let mut source = folder.fold_boxed_plan(source);
                Plan::Order {
                    order: order
                        .into_vec()
                        .into_iter()
                        .map(|order_expr| order_expr.fold_with(folder, &mut source))
                        .collect(),
                    source,
                }
            }
            Plan::Empty => Plan::Empty,
            Plan::Explain(plan) => Plan::Explain(folder.fold_boxed_plan(plan)),
            Plan::Insert { table, source, returning, schema } => {
                let mut source = folder.fold_boxed_plan(source);
                Plan::Insert {
                    table,
                    returning: returning.map(|exprs| folder.fold_exprs(&mut source, exprs)),
                    schema,
                    source,
                }
            }
            Plan::Update { table, source, returning, schema } => {
                let mut source = folder.fold_boxed_plan(source);
                Plan::Update {
                    table,
                    returning: returning.map(|exprs| folder.fold_exprs(&mut source, exprs)),
                    source,
                    schema,
                }
            }
            Plan::Aggregate { source, functions, group_by, schema } => {
                let mut source = folder.fold_boxed_plan(source);
                Plan::Aggregate {
                    functions: functions
                        .into_vec()
                        .into_iter()
                        .map(|(f, args)| (f, folder.fold_exprs(&mut source, args)))
                        .collect(),
                    group_by: folder.fold_exprs(&mut source, group_by),
                    source: folder.fold_boxed_plan(source),
                    schema,
                }
            }
        }
    }
}

pub trait ExprFold: Sized {
    fn fold_with(self, folder: &mut dyn Folder, plan: &mut Plan) -> Self;

    fn super_fold_with(self, folder: &mut dyn Folder, plan: &mut Plan) -> Self {
        self.fold_with(folder, plan)
    }
}

impl ExprFold for OrderExpr {
    #[inline]
    fn fold_with(self, folder: &mut dyn Folder, plan: &mut Plan) -> Self {
        Self { expr: folder.fold_expr(plan, self.expr), asc: self.asc }
    }
}

impl ExprFold for Expr {
    fn super_fold_with(self, folder: &mut dyn Folder, plan: &mut Plan) -> Self {
        folder.fold_expr(plan, self)
    }

    fn fold_with(self, folder: &mut dyn Folder, plan: &mut Plan) -> Self {
        let kind = match self.kind {
            ExprKind::Literal(lit) => ExprKind::Literal(lit),
            ExprKind::ColumnRef { qpath, index } => ExprKind::ColumnRef { qpath, index },
            ExprKind::Array(exprs) => ExprKind::Array(folder.fold_exprs(plan, exprs)),
            ExprKind::Alias { alias, expr } => {
                ExprKind::Alias { alias: alias.clone(), expr: folder.fold_boxed_expr(plan, expr) }
            }
            ExprKind::UnaryOp { op, expr } => {
                ExprKind::UnaryOp { op, expr: folder.fold_boxed_expr(plan, expr) }
            }
            ExprKind::BinOp { op, lhs, rhs } => ExprKind::BinOp {
                op,
                lhs: folder.fold_boxed_expr(plan, lhs),
                rhs: folder.fold_boxed_expr(plan, rhs),
            },
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
            ExprKind::Subquery(plan) => ExprKind::Subquery(folder.fold_boxed_plan(plan)),
        };

        Expr { ty: self.ty.clone(), kind }
    }
}
