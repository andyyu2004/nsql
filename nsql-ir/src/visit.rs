use std::ops::ControlFlow;

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
    fn visit_plan(&mut self, plan: &Plan) -> ControlFlow<()> {
        self.walk_plan(plan)
    }

    fn walk_plan(&mut self, plan: &Plan) -> ControlFlow<()> {
        // we must visit the expression before recursing on the plan (i.e. somewhat breadth-first)
        match plan {
            Plan::Show(_typ) => ControlFlow::Continue(()),
            Plan::Drop(_refs) => ControlFlow::Continue(()),
            Plan::Transaction(_tx) => ControlFlow::Continue(()),
            Plan::CreateNamespace(_info) => ControlFlow::Continue(()),
            Plan::CreateTable(_info) => ControlFlow::Continue(()),
            Plan::SetVariable { .. } => ControlFlow::Continue(()),
            Plan::TableScan { table: _, projection: _, projected_schema: _ } => {
                ControlFlow::Continue(())
            }
            Plan::Projection { source, projection, projected_schema: _ } => {
                self.visit_exprs(projection)?;
                self.visit_plan(source)
            }
            Plan::Filter { source, predicate } => {
                self.visit_expr(predicate)?;
                self.visit_plan(source)
            }
            Plan::Unnest { expr, schema: _ } => self.visit_expr(expr),
            Plan::Values { values: _, schema: _ } => ControlFlow::Continue(()),
            Plan::Join { schema: _, join, lhs, rhs } => {
                match join {
                    Join::Inner(constraint)
                    | Join::Left(constraint)
                    | Join::Right(constraint)
                    | Join::Full(constraint) => self.visit_join_constraint(constraint),
                    Join::Cross => ControlFlow::Continue(()),
                }?;
                self.visit_plan(lhs)?;
                self.visit_plan(rhs)
            }
            Plan::Limit { source, limit: _ } => self.visit_plan(source),
            Plan::Order { source, order } => {
                for order_expr in &order[..] {
                    self.visit_expr(&order_expr.expr)?;
                }
                self.visit_plan(source)
            }
            Plan::Empty => ControlFlow::Continue(()),
            Plan::Explain(plan) => self.visit_plan(plan),
            Plan::Insert { table: _, source, returning, schema: _ } => {
                if let Some(returning) = returning {
                    self.visit_exprs(returning)?;
                }
                self.visit_plan(source)
            }
            Plan::Update { table: _, source, returning, schema: _ } => {
                if let Some(returning) = returning {
                    self.visit_exprs(returning)?;
                }
                self.visit_plan(source)
            }
            Plan::Aggregate { source, functions, group_by, schema: _ } => {
                for (_f, args) in &functions[..] {
                    for arg in &args[..] {
                        self.visit_expr(arg)?;
                    }
                }
                self.visit_exprs(group_by)?;
                self.visit_plan(source)
            }
        }
    }

    fn visit_join_constraint(&mut self, constraint: &JoinConstraint) -> ControlFlow<()> {
        match constraint {
            JoinConstraint::On(expr) => self.visit_expr(expr),
            JoinConstraint::None => ControlFlow::Continue(()),
        }
    }

    #[inline]
    fn visit_exprs(&mut self, exprs: &[Expr]) -> ControlFlow<()> {
        for expr in exprs {
            self.visit_expr(expr)?;
        }
        ControlFlow::Continue(())
    }

    #[inline]
    fn visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        self.walk_expr(expr)
    }

    fn walk_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        match &expr.kind {
            ExprKind::Literal(_) => ControlFlow::Continue(()),
            ExprKind::Array(exprs) => self.visit_exprs(exprs),
            ExprKind::BinOp { op: _, lhs, rhs } => {
                self.visit_expr(lhs)?;
                self.visit_expr(rhs)
            }
            ExprKind::ColumnRef { .. } => ControlFlow::Continue(()),
            ExprKind::FunctionCall { function: _, args } => self.visit_exprs(args),
            ExprKind::Alias { alias: _, expr } => self.visit_expr(expr),
            ExprKind::Case { scrutinee, cases, else_result } => {
                self.visit_expr(scrutinee)?;
                for case in &cases[..] {
                    self.visit_expr(&case.when)?;
                    self.visit_expr(&case.then)?;
                }

                if let Some(else_result) = else_result {
                    self.visit_expr(else_result)?;
                }

                ControlFlow::Continue(())
            }
            ExprKind::UnaryOp { op: _, expr } => self.visit_expr(expr),
            ExprKind::Subquery(plan) => self.visit_plan(plan),
        }
    }
}
