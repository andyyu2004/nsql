mod aggregate;
pub(crate) mod explain;
mod join;
mod physical_create_namespace;
mod physical_create_table;
mod physical_drop;
mod physical_dummy_scan;
mod physical_explain;
mod physical_filter;
mod physical_insert;
mod physical_limit;
mod physical_order;
mod physical_projection;
mod physical_show;
mod physical_table_scan;
mod physical_transaction;
mod physical_unnest;
mod physical_update;
mod physical_values;

use std::fmt;
use std::sync::Arc;

use anyhow::Result;
use ir::Plan;
use nsql_catalog::Catalog;
use nsql_core::{LogicalType, Schema};
use nsql_storage::eval::{ExecutableExpr, ExecutableTupleExpr};
use nsql_storage_engine::{StorageEngine, Transaction};

use self::aggregate::{PhysicalHashAggregate, PhysicalUngroupedAggregate};
pub use self::explain::Explain;
use self::join::PhysicalNestedLoopJoin;
use self::physical_create_namespace::PhysicalCreateNamespace;
use self::physical_create_table::PhysicalCreateTable;
use self::physical_drop::PhysicalDrop;
use self::physical_dummy_scan::PhysicalDummyScan;
use self::physical_explain::PhysicalExplain;
use self::physical_filter::PhysicalFilter;
use self::physical_insert::PhysicalInsert;
use self::physical_limit::PhysicalLimit;
use self::physical_order::PhysicalOrder;
use self::physical_projection::PhysicalProjection;
use self::physical_show::PhysicalShow;
use self::physical_table_scan::PhysicalTableScan;
use self::physical_transaction::PhysicalTransaction;
use self::physical_unnest::PhysicalUnnest;
use self::physical_update::PhysicalUpdate;
use self::physical_values::PhysicalValues;
use crate::compile::Compiler;
use crate::executor::OutputSink;
use crate::{
    ExecutionContext, ExecutionMode, ExecutionResult, OperatorState, PhysicalNode,
    PhysicalOperator, PhysicalSink, PhysicalSource, ReadWriteExecutionMode, ReadonlyExecutionMode,
    Tuple, TupleStream,
};

pub struct PhysicalPlanner<'env, S> {
    catalog: Catalog<'env, S>,
    compiler: Compiler,
}

/// Opaque physical plan that is ready to be executed
pub struct PhysicalPlan<'env, 'txn, S, M>(Arc<dyn PhysicalNode<'env, 'txn, S, M>>);

impl<'env, 'txn, S, M> PhysicalPlan<'env, 'txn, S, M> {
    pub(crate) fn root(&self) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::clone(&self.0)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalPlanner<'env, S> {
    pub fn new(catalog: Catalog<'env, S>) -> Self {
        Self { catalog, compiler: Default::default() }
    }

    #[inline]
    pub fn plan(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        plan: Box<Plan>,
    ) -> Result<PhysicalPlan<'env, 'txn, S, ReadonlyExecutionMode>> {
        self.plan_node(tx, plan).map(PhysicalPlan)
    }

    #[inline]
    pub fn plan_write(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        plan: Box<Plan>,
    ) -> Result<PhysicalPlan<'env, 'txn, S, ReadWriteExecutionMode>> {
        self.plan_write_node(tx, plan).map(PhysicalPlan)
    }

    fn plan_write_node(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        plan: Box<Plan>,
    ) -> Result<Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>> {
        let plan = match *plan {
            Plan::Update { table, source, returning, schema } => {
                let source = self.plan_write_node(tx, source)?;
                PhysicalUpdate::plan(
                    schema,
                    table,
                    source,
                    returning.map(|e| self.compile_exprs(e)),
                )
            }
            Plan::Insert { table, source, returning, schema } => {
                let source = self.plan_write_node(tx, source)?;
                PhysicalInsert::plan(
                    schema,
                    table,
                    source,
                    returning.map(|exprs| self.compile_exprs(exprs)),
                )
            }
            Plan::CreateTable(info) => PhysicalCreateTable::plan(info),
            Plan::CreateNamespace(info) => PhysicalCreateNamespace::plan(info),
            Plan::Drop(refs) => PhysicalDrop::plan(refs),
            _ => return self.fold_plan(tx, plan, Self::plan_write_node),
        };

        Ok(plan)
    }

    fn explain_plan<M: ExecutionMode<'env, S>>(
        &self,
        tx: &dyn Transaction<'env, S>,
        kind: ir::ExplainMode,
        plan: Arc<dyn PhysicalNode<'env, 'txn, S, M>>,
    ) -> Result<Arc<dyn PhysicalNode<'env, 'txn, S, M>>> {
        let stringified = match kind {
            ir::ExplainMode::Physical => {
                explain::display(self.catalog, tx, &explain::explain(Arc::clone(&plan))).to_string()
            }
            ir::ExplainMode::Pipeline => {
                let sink = Arc::new(OutputSink::default());
                let pipeline = crate::build_pipelines(sink, PhysicalPlan(Arc::clone(&plan)));
                explain::display(self.catalog, tx, &explain::explain_pipeline(&pipeline))
                    .to_string()
            }
        };

        Ok(PhysicalExplain::plan(stringified, plan))
    }

    #[allow(clippy::boxed_local)]
    fn plan_node<M: ExecutionMode<'env, S>>(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        plan: Box<Plan>,
    ) -> Result<Arc<dyn PhysicalNode<'env, 'txn, S, M>>> {
        self.fold_plan(tx, plan, Self::plan_node)
    }

    #[allow(clippy::boxed_local)]
    fn fold_plan<M: ExecutionMode<'env, S>>(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        plan: Box<Plan>,
        mut f: impl FnMut(
            &mut Self,
            &dyn Transaction<'env, S>,
            Box<Plan>,
        ) -> Result<Arc<dyn PhysicalNode<'env, 'txn, S, M>>>,
    ) -> Result<Arc<dyn PhysicalNode<'env, 'txn, S, M>>> {
        let plan = match *plan {
            Plan::Transaction(kind) => PhysicalTransaction::plan(kind),
            Plan::TableScan { table, projection, projected_schema } => {
                PhysicalTableScan::plan(projected_schema, table, projection)
            }
            Plan::Show(object_type) => PhysicalShow::plan(object_type),
            Plan::Explain(kind, plan) => {
                let plan = f(self, tx, plan)?;
                return self.explain_plan(tx, kind, plan);
            }
            Plan::Values { schema, values } => {
                PhysicalValues::plan(schema, self.compile_values(values))
            }
            Plan::Projection { projected_schema, source, projection } => PhysicalProjection::plan(
                projected_schema,
                f(self, tx, source)?,
                self.compile_exprs(projection),
            ),
            Plan::Limit { source, limit } => PhysicalLimit::plan(f(self, tx, source)?, limit),
            Plan::Order { source, order } => {
                PhysicalOrder::plan(f(self, tx, source)?, self.compile_order_exprs(order))
            }
            Plan::Filter { source, predicate } => {
                PhysicalFilter::plan(f(self, tx, source)?, self.compile_expr(predicate))
            }
            Plan::Join { schema, join, lhs, rhs } => PhysicalNestedLoopJoin::plan(
                schema,
                join.map(|expr| self.compile_expr(expr)),
                f(self, tx, lhs)?,
                f(self, tx, rhs)?,
            ),
            Plan::Aggregate { functions, source, schema, group_by } if group_by.is_empty() => {
                let functions = functions
                    .into_vec()
                    .into_iter()
                    .map(|(f, args)| {
                        assert_eq!(
                            args.len(),
                            1,
                            "only one argument allowed for aggregate functions for now"
                        );
                        let arg = self.compile_expr(args[0].clone());
                        Ok((f, arg))
                    })
                    .collect::<Result<Box<_>>>()?;
                PhysicalUngroupedAggregate::plan(schema, functions, f(self, tx, source)?)
            }
            Plan::Aggregate { functions, source, schema, group_by } => PhysicalHashAggregate::plan(
                schema,
                f(self, tx, source)?,
                self.compile_exprs(group_by),
            ),
            Plan::Unnest { schema, expr } => PhysicalUnnest::plan(schema, self.compile_expr(expr)),
            Plan::Empty => PhysicalDummyScan::plan(),
            Plan::CreateTable(_)
            | Plan::CreateNamespace(_)
            | Plan::Drop(_)
            | Plan::Update { .. }
            | Plan::Insert { .. } => {
                unreachable!("write plans should go through plan_node_write, got plan {:?}", plan)
            }
        };

        Ok(plan)
    }

    fn compile_values(&mut self, values: ir::Values) -> Box<[ExecutableTupleExpr]> {
        values.into_inner().into_vec().into_iter().map(|e| self.compile_exprs(e)).collect()
    }

    fn compile_exprs(&mut self, exprs: Box<[ir::Expr]>) -> ExecutableTupleExpr {
        self.compiler.compile_many(exprs.into_vec())
    }

    fn compile_expr(&mut self, expr: ir::Expr) -> ExecutableExpr {
        self.compiler.compile(expr)
    }

    fn compile_order_exprs(
        &mut self,
        expr: Box<[ir::OrderExpr]>,
    ) -> Box<[ir::OrderExpr<ExecutableExpr>]> {
        expr.into_vec().into_iter().map(|e| self.compile_order_expr(e)).collect()
    }

    fn compile_order_expr(&mut self, expr: ir::OrderExpr) -> ir::OrderExpr<ExecutableExpr> {
        ir::OrderExpr { expr: self.compiler.compile(expr.expr), asc: expr.asc }
    }
}
