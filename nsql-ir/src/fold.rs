use crate::*;

pub trait Folder {
    fn fold_plan(&mut self, plan: Plan) -> Plan {
        plan
    }

    #[inline]
    fn fold_boxed_plan(&mut self, mut boxed_plan: Box<Plan>) -> Box<Plan> {
        *boxed_plan = self.fold_plan(mem::replace(&mut *boxed_plan, Plan::Empty));
        boxed_plan
    }

    /// Replace an expression, this must be type preserving.
    /// Implement this method if you want to replace an expression with a different expression.
    fn fold_expr(&mut self, expr: Expr) -> Expr {
        expr
    }

    /// Convenience method for folding a boxed expression, implement `fold_expr` not this
    #[inline]
    fn fold_expr_boxed(&mut self, mut boxed_expr: Box<Expr>) -> Box<Expr> {
        *boxed_expr = self.fold_expr(mem::replace(&mut *boxed_expr, Expr::NULL));
        boxed_expr
    }
}

pub trait PlanFold: Sized {
    fn fold_with(self, folder: &mut impl Folder) -> Self;

    #[inline]
    fn super_fold_with(self, folder: &mut impl Folder) -> Self {
        self.fold_with(folder)
    }
}

impl PlanFold for Plan {
    #[inline]
    fn super_fold_with(self, folder: &mut impl Folder) -> Self {
        folder.fold_plan(self)
    }

    fn fold_with(self, folder: &mut impl Folder) -> Self {
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
            Plan::Projection { source, projection, projected_schema } => Plan::Projection {
                source: folder.fold_boxed_plan(source),
                projection: projection
                    .into_vec()
                    .into_iter()
                    .map(|e| folder.fold_expr(e))
                    .collect(),
                projected_schema,
            },
            Plan::Filter { source, predicate } => {
                Plan::Filter { source: folder.fold_boxed_plan(source), predicate }
            }

            Plan::Unnest { expr, schema } => Plan::Unnest { expr: folder.fold_expr(expr), schema },
            Plan::Values { values, schema } => Plan::Values { values, schema },
            Plan::Join { schema, join, lhs, rhs } => {
                let mut fold_join_constraint = |constraint| match constraint {
                    JoinConstraint::On(expr) => JoinConstraint::On(folder.fold_expr(expr)),
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
            Plan::Limit { source, limit } => {
                Plan::Limit { source: folder.fold_boxed_plan(source), limit }
            }
            Plan::Order { source, order } => Plan::Order {
                source: folder.fold_boxed_plan(source),
                order: order.into_vec().into_iter().map(|e| e.fold_with(folder)).collect(),
            },
            Plan::Empty => Plan::Empty,
            Plan::Explain(plan) => Plan::Explain(folder.fold_boxed_plan(plan)),
            Plan::Insert { table, source, returning, schema } => Plan::Insert {
                table,
                source: folder.fold_boxed_plan(source),
                returning: returning.map(|exprs| {
                    exprs.into_vec().into_iter().map(|e| folder.fold_expr(e)).collect()
                }),
                schema,
            },
            Plan::Update { table, source, returning, schema } => Plan::Update {
                table,
                source: folder.fold_boxed_plan(source),
                returning: returning.map(|exprs| {
                    exprs.into_vec().into_iter().map(|e| folder.fold_expr(e)).collect()
                }),
                schema,
            },
            Plan::Aggregate { source, functions, group_by, schema } => Plan::Aggregate {
                source: folder.fold_boxed_plan(source),
                functions: functions
                    .into_vec()
                    .into_iter()
                    .map(|(f, args)| {
                        (f, args.into_vec().into_iter().map(|e| folder.fold_expr(e)).collect())
                    })
                    .collect(),
                group_by: group_by.into_vec().into_iter().map(|e| folder.fold_expr(e)).collect(),
                schema,
            },
        }
    }
}

pub trait ExprFold: Sized {
    fn fold_with(self, folder: &mut impl Folder) -> Self;

    fn super_fold_with(self, folder: &mut impl Folder) -> Self {
        self.fold_with(folder)
    }
}

impl ExprFold for OrderExpr {
    #[inline]
    fn fold_with(self, folder: &mut impl Folder) -> Self {
        Self { expr: folder.fold_expr(self.expr), asc: self.asc }
    }
}

impl ExprFold for Expr {
    fn super_fold_with(self, folder: &mut impl Folder) -> Self {
        folder.fold_expr(self)
    }

    fn fold_with(self, folder: &mut impl Folder) -> Self {
        let kind = match self.kind {
            ExprKind::Literal(lit) => ExprKind::Literal(lit),
            ExprKind::ColumnRef { qpath, index } => ExprKind::ColumnRef { qpath, index },
            ExprKind::Array(exprs) => ExprKind::Array(
                exprs.into_vec().into_iter().map(|expr| folder.fold_expr(expr)).collect(),
            ),
            ExprKind::Alias { alias, expr } => {
                ExprKind::Alias { alias: alias.clone(), expr: folder.fold_expr_boxed(expr) }
            }
            ExprKind::UnaryOp { op, expr } => {
                ExprKind::UnaryOp { op, expr: folder.fold_expr_boxed(expr) }
            }
            ExprKind::BinOp { op, lhs, rhs } => ExprKind::BinOp {
                op,
                lhs: folder.fold_expr_boxed(lhs),
                rhs: folder.fold_expr_boxed(rhs),
            },
            ExprKind::FunctionCall { function, args } => ExprKind::FunctionCall {
                function: function.clone(),
                args: args.into_vec().into_iter().map(|expr| folder.fold_expr(expr)).collect(),
            },
            ExprKind::Case { scrutinee, cases, else_result } => ExprKind::Case {
                scrutinee: folder.fold_expr_boxed(scrutinee),
                cases: cases
                    .into_vec()
                    .into_iter()
                    .map(|case| Case {
                        when: folder.fold_expr(case.when),
                        then: folder.fold_expr(case.then),
                    })
                    .collect(),
                else_result: else_result.map(|expr| folder.fold_expr_boxed(expr)),
            },
        };

        Expr { ty: self.ty.clone(), kind }
    }
}
