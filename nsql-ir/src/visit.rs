use crate::{Expr, ExprKind, JoinConstraint, Plan};

/// A trait for walking a plan and its expressions.
/// The `visit_*` methods are called when the walker encounters the corresponding node and are
/// intended to be overridden by implementations.
/// The `walk_*` methods are called by the default implementations of the `visit_*` methods and
/// generally should not be overriden.
pub trait VisitorMut {
    /// Any transformation must be type-preserving
    fn visit_plan_mut(&self, plan: &mut Plan) {
        self.walk_plan_mut(plan);
    }

    fn walk_plan_mut(&self, plan: &mut Plan) {
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
            Plan::Values { values, schema: _ } => {}
            Plan::Join { schema, join, lhs, rhs } => {
                self.visit_plan_mut(lhs);
                self.visit_plan_mut(rhs);
                match join {
                    crate::Join::Inner(constraint)
                    | crate::Join::Left(constraint)
                    | crate::Join::Full(constraint) => self.visit_join_constraint_mut(constraint),
                    crate::Join::Cross => {}
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
        }
    }

    fn visit_join_constraint_mut(&self, constraint: &mut JoinConstraint) {
        match constraint {
            JoinConstraint::On(expr) => self.visit_expr_mut(expr),
            JoinConstraint::None => {}
        }
    }

    fn visit_exprs_mut(&self, expr: &mut [Expr]) {
        for expr in expr {
            self.visit_expr_mut(expr);
        }
    }

    fn visit_expr_mut(&self, expr: &mut Expr) {
        match &mut expr.kind {
            ExprKind::Value(_) => {}
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
        }
    }
}
