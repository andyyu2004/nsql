use std::ops::ControlFlow;

use crate::{Copy, CopyTo, Expr, ExprKind, Plan, QueryPlan};

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
            Plan::Transaction(_tcx) => ControlFlow::Continue(()),
            Plan::SetVariable { .. } => ControlFlow::Continue(()),
            Plan::Explain(_opts, plan) => self.walk_plan(plan),
            Plan::Query(plan) => self.visit_query_plan(plan),
            Plan::Copy(copy) => match copy {
                Copy::To(CopyTo { src, dst: _ }) => self.visit_query_plan(src),
            },
        }
    }

    #[inline]
    fn visit_query_plan(&mut self, plan: &QueryPlan) -> ControlFlow<()> {
        self.walk_query_plan(plan)
    }

    #[inline]
    fn walk_query_plan(&mut self, plan: &QueryPlan) -> ControlFlow<()> {
        // we must visit the expression before recursing on the plan
        match plan {
            QueryPlan::DummyScan | QueryPlan::Empty { schema: _ } => ControlFlow::Continue(()),
            QueryPlan::TableScan { table: _, projection: _, projected_schema: _ } => {
                ControlFlow::Continue(())
            }
            QueryPlan::Projection { source, projection, projected_schema: _ } => {
                self.visit_exprs(source, projection)?;
                self.visit_query_plan(source)
            }
            QueryPlan::Filter { source, predicate } => {
                self.visit_expr(source, predicate)?;
                self.visit_query_plan(source)
            }
            // FIXME where is the child of unnest try unnesting a column in a test before continuing with this change
            QueryPlan::Unnest { expr, schema: _ } => self.visit_expr(&QueryPlan::DummyScan, expr),
            QueryPlan::Values { values: _, schema: _ } => ControlFlow::Continue(()),
            QueryPlan::Join { schema: _, kind: _, lhs, rhs, conditions } => {
                for condition in &conditions[..] {
                    self.visit_expr(plan, &condition.lhs)?;
                    self.visit_expr(plan, &condition.rhs)?;
                }
                self.visit_query_plan(lhs)?;
                self.visit_query_plan(rhs)
            }
            QueryPlan::Limit { source, limit: _, exceeded_message: _ } => {
                self.visit_query_plan(source)
            }
            QueryPlan::Order { source, order } => {
                for order_expr in &order[..] {
                    self.visit_expr(plan, &order_expr.expr)?;
                }
                self.visit_query_plan(source)
            }
            QueryPlan::Insert { table: _, source, returning, schema: _ } => {
                self.visit_exprs(source, returning)?;
                self.visit_query_plan(source)
            }
            QueryPlan::Update { table: _, source, returning, schema: _ } => {
                self.visit_exprs(source, returning)?;
                self.visit_query_plan(source)
            }
            QueryPlan::Aggregate { source, aggregates, group_by, schema: _ } => {
                self.visit_exprs(source, group_by)?;

                for (_f, args) in &aggregates[..] {
                    for arg in &args[..] {
                        self.visit_expr(source, arg)?;
                    }
                }

                self.visit_query_plan(source)
            }
            QueryPlan::Union { schema: _, lhs, rhs } => {
                self.visit_query_plan(lhs)?;
                self.visit_query_plan(rhs)
            }
            QueryPlan::CteScan { name: _, schema: _ } => ControlFlow::Continue(()),
            QueryPlan::Cte { cte, child } => {
                self.visit_query_plan(&cte.plan)?;
                self.visit_query_plan(child)
            }
            QueryPlan::Distinct { source } => self.visit_query_plan(source),
        }
    }

    #[inline]
    fn visit_exprs(&mut self, plan: &QueryPlan, exprs: &[Expr]) -> ControlFlow<()> {
        for expr in exprs {
            self.visit_expr(plan, expr)?;
        }
        ControlFlow::Continue(())
    }

    #[inline]
    fn visit_expr(&mut self, plan: &QueryPlan, expr: &Expr) -> ControlFlow<()> {
        self.walk_expr(plan, expr)
    }

    fn walk_expr(&mut self, plan: &QueryPlan, expr: &Expr) -> ControlFlow<()> {
        match &expr.kind {
            ExprKind::Literal(_) => ControlFlow::Continue(()),
            ExprKind::Array(exprs) => self.visit_exprs(plan, exprs),
            ExprKind::ColumnRef { .. } => ControlFlow::Continue(()),
            ExprKind::FunctionCall { function: _, args } => self.visit_exprs(plan, args),
            ExprKind::Alias { alias: _, expr } => self.visit_expr(plan, expr),
            ExprKind::Case { scrutinee, cases, else_result } => {
                self.visit_expr(plan, scrutinee)?;
                for case in &cases[..] {
                    self.visit_expr(plan, &case.when)?;
                    self.visit_expr(plan, &case.then)?;
                }

                if let Some(else_result) = else_result {
                    self.visit_expr(plan, else_result)?;
                }

                ControlFlow::Continue(())
            }
            ExprKind::Subquery(_kind, plan) => self.visit_query_plan(plan),
            ExprKind::UnaryOperator { operator: _, expr } => self.visit_expr(plan, expr),
            ExprKind::BinaryOperator { operator: _, lhs, rhs } => {
                self.visit_expr(plan, lhs)?;
                self.visit_expr(plan, rhs)
            }
            ExprKind::Compiled(_expr) => ControlFlow::Continue(()),
            ExprKind::Quote(expr) => self.visit_expr(plan, expr),
            ExprKind::Coalesce(exprs) => self.visit_exprs(plan, exprs),
        }
    }
}
