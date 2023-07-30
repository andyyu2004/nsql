use crate::{Expr, ExprKind, Join, JoinConstraint, Plan};

/// A trait for walking a plan and its expressions.
/// The `visit_*` methods are called when the walker encounters the corresponding node and are
/// intended to be overridden by implementations.
/// The `walk_*` methods are called by the default implementations of the `visit_*` methods and
/// generally should not be overriden.
pub trait Visitor {
    /// Any transformation must be type-preserving
    #[inline]
    fn visit_plan(&mut self, plan: &Plan) -> bool {
        self.walk_plan(plan)
    }

    fn walk_plan(&mut self, plan: &Plan) -> bool {
        match plan {
            Plan::Show(_typ) => false,
            Plan::Drop(_refs) => false,
            Plan::Transaction(_tx) => false,
            Plan::CreateNamespace(_info) => false,
            Plan::CreateTable(_info) => false,
            Plan::TableScan { table: _, projection: _, projected_schema: _ } => false,
            Plan::Projection { source, projection, projected_schema: _ } => {
                self.visit_plan(source) || self.visit_exprs(projection)
            }
            Plan::Filter { source, predicate } => {
                self.visit_plan(source) || self.visit_expr(predicate)
            }
            Plan::Unnest { expr, schema: _ } => self.visit_expr(expr),
            Plan::Values { values: _, schema: _ } => false,
            Plan::Join { schema: _, join, lhs, rhs } => {
                self.visit_plan(lhs)
                    || self.visit_plan(rhs)
                    || match join {
                        Join::Inner(constraint)
                        | Join::Left(constraint)
                        | Join::Right(constraint)
                        | Join::Full(constraint) => self.visit_join_constraint(constraint),
                        Join::Cross => false,
                    }
            }
            Plan::Limit { source, limit: _ } => self.visit_plan(source),
            Plan::Order { source, order } => {
                self.visit_plan(source)
                    || order.iter().any(|order_expr| self.visit_expr(&order_expr.expr))
            }
            Plan::Empty => false,
            Plan::Explain(_, plan) => self.visit_plan(plan),
            Plan::Insert { table: _, source, returning, schema: _ } => {
                self.visit_plan(source)
                    || returning.as_ref().map_or(false, |returning| self.visit_exprs(returning))
            }
            Plan::Update { table: _, source, returning, schema: _ } => {
                self.visit_plan(source)
                    || returning.as_ref().map_or(false, |returning| self.visit_exprs(returning))
            }
            Plan::Aggregate { source, functions: _, group_by, schema: _ } => {
                self.visit_plan(source) || self.visit_exprs(group_by)
            }
        }
    }

    fn visit_join_constraint(&mut self, constraint: &JoinConstraint) -> bool {
        match constraint {
            JoinConstraint::On(expr) => self.visit_expr(expr),
            JoinConstraint::None => false,
        }
    }

    #[inline]
    fn visit_exprs(&mut self, exprs: &[Expr]) -> bool {
        exprs.iter().any(|expr| self.visit_expr(expr))
    }

    #[inline]
    fn visit_expr(&mut self, expr: &Expr) -> bool {
        self.walk_expr(expr)
    }

    fn walk_expr(&mut self, expr: &Expr) -> bool {
        match &expr.kind {
            ExprKind::Literal(_) => false,
            ExprKind::Array(exprs) => self.visit_exprs(exprs),
            ExprKind::BinOp { op: _, lhs, rhs } => self.visit_expr(lhs) || self.visit_expr(rhs),
            ExprKind::ColumnRef { .. } => false,
            ExprKind::FunctionCall { function: _, args } => self.visit_exprs(args),
            ExprKind::Alias { alias: _, expr } => self.visit_expr(expr),
            ExprKind::Case { scrutinee, cases, else_result } => {
                self.visit_expr(scrutinee)
                    || cases
                        .iter()
                        .any(|case| self.visit_expr(&case.when) || self.visit_expr(&case.then))
                    || else_result
                        .as_ref()
                        .as_ref()
                        .map_or(false, |else_result| self.visit_expr(else_result))
            }
        }
    }
}
pub trait VisitorMut {
    /// Any transformation must be type-preserving
    #[inline]
    fn visit_plan_mut(&mut self, plan: &mut Plan) {
        self.walk_plan_mut(plan);
    }

    fn walk_plan_mut(&mut self, plan: &mut Plan) {
        match plan {
            Plan::Show(_typ) => {}
            Plan::Drop(_refs) => {}
            Plan::Transaction(_tx) => {}
            Plan::CreateNamespace(_info) => {}
            Plan::CreateTable(_info) => {}
            Plan::TableScan { table: _, projection: _, projected_schema: _ } => {}
            Plan::Projection { source, projection, projected_schema: _ } => {
                self.visit_plan_mut(source);
                self.visit_exprs_mut(projection);
            }
            Plan::Filter { source, predicate } => {
                self.visit_plan_mut(source);
                self.visit_expr_mut(predicate);
            }
            Plan::Unnest { expr, schema: _ } => self.visit_expr_mut(expr),
            Plan::Values { values: _, schema: _ } => {}
            Plan::Join { schema: _, join, lhs, rhs } => {
                self.visit_plan_mut(lhs);
                self.visit_plan_mut(rhs);
                match join {
                    Join::Inner(constraint)
                    | Join::Left(constraint)
                    | Join::Right(constraint)
                    | Join::Full(constraint) => self.visit_join_constraint_mut(constraint),
                    Join::Cross => {}
                }
            }
            Plan::Limit { source, limit: _ } => self.visit_plan_mut(source),
            Plan::Order { source, order } => {
                self.visit_plan_mut(source);
                for order_expr in order.iter_mut() {
                    self.visit_expr_mut(&mut order_expr.expr);
                }
            }
            Plan::Empty => {}
            Plan::Explain(_, plan) => self.visit_plan_mut(plan),
            Plan::Insert { table: _, source, returning, schema: _ } => {
                self.visit_plan_mut(source);
                if let Some(returning) = returning {
                    self.visit_exprs_mut(returning);
                }
            }
            Plan::Update { table: _, source, returning, schema: _ } => {
                self.visit_plan_mut(source);
                if let Some(returning) = returning {
                    self.visit_exprs_mut(returning);
                }
            }
            Plan::Aggregate { source, functions: _, group_by, schema: _ } => {
                self.visit_plan_mut(source);
                self.visit_exprs_mut(group_by);
            }
        }
    }

    fn visit_join_constraint_mut(&mut self, constraint: &mut JoinConstraint) {
        match constraint {
            JoinConstraint::On(expr) => self.visit_expr_mut(expr),
            JoinConstraint::None => {}
        }
    }

    #[inline]
    fn visit_exprs_mut(&mut self, expr: &mut [Expr]) {
        for expr in expr {
            self.visit_expr_mut(expr);
        }
    }

    fn visit_expr_mut(&mut self, expr: &mut Expr) {
        match &mut expr.kind {
            ExprKind::Literal(_) => {}
            ExprKind::Array(exprs) => self.visit_exprs_mut(exprs),
            ExprKind::BinOp { op: _, lhs, rhs } => {
                self.visit_expr_mut(lhs);
                self.visit_expr_mut(rhs);
            }
            ExprKind::ColumnRef { .. } => {}
            ExprKind::FunctionCall { function: _, args } => self.visit_exprs_mut(args),
            ExprKind::Alias { alias: _, expr } => {
                self.visit_expr_mut(expr);
            }
            ExprKind::Case { scrutinee, cases, else_result } => {
                self.visit_expr_mut(scrutinee);
                for case in &mut cases[..] {
                    self.visit_expr_mut(&mut case.when);
                    self.visit_expr_mut(&mut case.then);
                }
                if let Some(else_result) = else_result.as_mut() {
                    self.visit_expr_mut(else_result);
                }
            }
        }
    }
}
