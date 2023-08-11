use crate::{Expr, ExprKind, Join, JoinConstraint, Plan};
// FIXME returning ControlFlow is probably better than using bool these days, also lets the visitor
// carry a value on break which is convenient (can also use `?` for ergonomics)

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
            Plan::SetVariable { .. } => false,
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
            Plan::Explain(plan) => self.visit_plan(plan),
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
            ExprKind::UnaryOp { op: _, expr } => self.visit_expr(expr),
            ExprKind::Subquery(plan) => self.visit_plan(plan),
        }
    }
}
