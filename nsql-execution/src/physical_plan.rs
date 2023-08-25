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
mod physical_set;
mod physical_show;
mod physical_table_scan;
mod physical_transaction;
mod physical_unnest;
mod physical_update;
mod physical_values;

use std::fmt;
use std::sync::Arc;

use anyhow::Result;
use nsql_catalog::Catalog;
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
use self::physical_set::PhysicalSet;
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

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalPlanner<'env, S> {
    pub fn planv2_write(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        plan: Box<ir::Plan<opt::Query>>,
    ) -> Result<PhysicalPlan<'env, 'txn, S, ReadWriteExecutionMode>> {
        let node = match *plan {
            ir::Plan::Transaction(kind) => PhysicalTransaction::plan(kind),
            ir::Plan::SetVariable { name, value, scope } => PhysicalSet::plan(name, value, scope),
            ir::Plan::Show(object_type) => PhysicalShow::plan(object_type),
            ir::Plan::CreateTable(info) => PhysicalCreateTable::plan(info),
            ir::Plan::CreateNamespace(info) => PhysicalCreateNamespace::plan(info),
            ir::Plan::Drop(refs) => PhysicalDrop::plan(refs),
            ir::Plan::Query(q) => self.plan_write_query_nodev2(tx, &q, q.root())?,
            ir::Plan::Explain(logical_plan) => {
                let physical_plan = self.planv2_write(tx, logical_plan.clone())?;
                self.explain_plan(logical_plan, physical_plan.0)?
            }
        };

        Ok(PhysicalPlan(node))
    }

    fn plan_write_query_nodev2(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        q: &opt::Query,
        plan: opt::Plan<'_>,
    ) -> Result<Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>> {
        let plan = match plan {
            opt::Plan::Update(insert) => PhysicalUpdate::plan(
                insert.table(q),
                self.plan_write_query_nodev2(tx, q, insert.source(q))?,
                self.compile_exprsv2(tx, q, insert.returning(q))?,
            ),
            opt::Plan::Insert(insert) => PhysicalInsert::plan(
                insert.table(q),
                self.plan_write_query_nodev2(tx, q, insert.source(q))?,
                self.compile_exprsv2(tx, q, insert.returning(q))?,
            ),
            _ => {
                return self.fold_plan_nodev2(tx, q, plan, |planner, node| {
                    planner.plan_write_query_nodev2(tx, q, node)
                });
            }
        };

        Ok(plan)
    }

    pub fn planv2<M: ExecutionMode<'env, S>>(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        plan: Box<ir::Plan<opt::Query>>,
    ) -> Result<PhysicalPlan<'env, 'txn, S, M>> {
        let node = match *plan {
            ir::Plan::Transaction(kind) => PhysicalTransaction::plan(kind),
            ir::Plan::SetVariable { name, value, scope } => PhysicalSet::plan(name, value, scope),
            ir::Plan::Show(object_type) => PhysicalShow::plan(object_type),
            ir::Plan::Explain(logical_plan) => {
                let physical_plan = self.planv2(tx, logical_plan.clone())?;
                self.explain_plan(logical_plan, physical_plan.0)?
            }
            ir::Plan::Drop(_) | ir::Plan::CreateNamespace(_) | ir::Plan::CreateTable(_) => {
                todo!();
                // unreachable!("write plans should go through plan_write_node, got plan {:?}", plan)
            }
            ir::Plan::Query(q) => self.plan_nodev2(tx, &q, q.root())?,
        };

        Ok(PhysicalPlan(node))
    }

    fn plan_nodev2<M: ExecutionMode<'env, S>>(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        q: &opt::Query,
        plan: opt::Plan<'_>,
    ) -> Result<Arc<dyn PhysicalNode<'env, 'txn, S, M>>> {
        self.fold_plan_nodev2(tx, q, plan, |planner, node| planner.plan_nodev2(tx, q, node))
    }

    fn fold_plan_nodev2<M: ExecutionMode<'env, S>>(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        q: &opt::Query,
        plan: opt::Plan<'_>,
        mut f: impl FnMut(&mut Self, opt::Plan<'_>) -> Result<Arc<dyn PhysicalNode<'env, 'txn, S, M>>>,
    ) -> Result<Arc<dyn PhysicalNode<'env, 'txn, S, M>>> {
        let plan = match plan {
            opt::Plan::Projection(projection) => {
                let source = projection.source(q);
                let exprs = projection.exprs(q);
                PhysicalProjection::plan(f(self, source)?, self.compile_exprsv2(tx, q, exprs)?)
            }
            opt::Plan::CrossJoin(join) => PhysicalNestedLoopJoin::plan(
                ir::Join::Cross,
                f(self, join.lhs(q))?,
                f(self, join.rhs(q))?,
            ),
            opt::Plan::Join(join) => PhysicalNestedLoopJoin::plan(
                join.join_expr(q).try_map(|expr| self.compile_exprv2(tx, q, expr))?,
                f(self, join.lhs(q))?,
                f(self, join.rhs(q))?,
            ),
            opt::Plan::Limit(limit) => {
                let source = f(self, limit.source(q))?;
                PhysicalLimit::plan(source, limit.limit(q), limit.limit_exceeded_message(q))
            }
            opt::Plan::Filter(filter) => PhysicalFilter::plan(
                f(self, filter.source(q))?,
                self.compile_exprv2(tx, q, filter.predicate(q))?,
            ),
            opt::Plan::Aggregate(agg) => {
                let functions = self.compile_aggregate_functionsv2(tx, q, agg.aggregates(q))?;
                let group_by = agg.group_by(q);
                let source = f(self, agg.source(q))?;
                if group_by.is_empty() {
                    PhysicalUngroupedAggregate::plan(functions, source)
                } else {
                    PhysicalHashAggregate::plan(
                        functions,
                        source,
                        self.compile_exprsv2(tx, q, group_by)?,
                    )
                }
            }
            opt::Plan::Values(values) => PhysicalValues::plan(
                values
                    .rows(q)
                    .map(|exprs| self.compile_exprsv2(tx, q, exprs))
                    .collect::<Result<_>>()?,
            ),
            opt::Plan::Order(order) => PhysicalOrder::plan(
                f(self, order.source(q))?,
                self.compile_order_exprsv2(tx, q, order.order_exprs(q))?,
            ),
            opt::Plan::TableScan(scan) => PhysicalTableScan::plan(scan.table(q), None),
            opt::Plan::DummyScan => PhysicalDummyScan::plan(),
            opt::Plan::Unnest(unnest) => {
                PhysicalUnnest::plan(self.compile_exprv2(tx, q, unnest.expr(q))?)
            }
            opt::Plan::Update(..) | opt::Plan::Insert(..) => unreachable!(
                "write query plans should go through plan_write_query_node, got plan {:?}",
                plan,
            ),
        };

        Ok(plan)
    }

    fn compile_order_exprsv2(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        q: &opt::Query,
        exprs: impl Iterator<Item = ir::OrderExpr<opt::Expr<'_>>>,
    ) -> Result<Box<[ir::OrderExpr<ExecutableExpr>]>> {
        exprs.map(|expr| self.compile_order_exprv2(tx, q, expr)).collect()
    }

    fn compile_order_exprv2(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        q: &opt::Query,
        expr: ir::OrderExpr<opt::Expr<'_>>,
    ) -> Result<ir::OrderExpr<ExecutableExpr>> {
        Ok(ir::OrderExpr { expr: self.compile_exprv2(tx, q, expr.expr)?, asc: expr.asc })
    }

    fn compile_exprsv2(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        q: &opt::Query,
        exprs: impl Iterator<Item = opt::Expr<'_>>,
    ) -> Result<ExecutableTupleExpr> {
        self.compiler.compile_many2(&self.catalog, tx, q, exprs)
    }

    fn compile_exprv2(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        q: &opt::Query,
        expr: opt::Expr<'_>,
    ) -> Result<ExecutableExpr> {
        self.compiler.compile2(&self.catalog, tx, q, expr)
    }

    #[allow(clippy::type_complexity)]
    fn compile_aggregate_functionsv2(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        q: &opt::Query,
        functions: impl IntoIterator<Item = opt::CallExpr<'_>>,
    ) -> Result<Box<[(ir::Function, Option<ExecutableExpr>)]>> {
        functions
            .into_iter()
            .map(|call| {
                let f = self.catalog.get(tx, call.function())?;
                let mut args = call.args(q);
                assert!(
                    args.len() <= 1,
                    "no more than one argument allowed for aggregate functions for now"
                );

                match args.next() {
                    Some(arg) => {
                        let arg = self.compile_exprv2(tx, q, arg)?;
                        Ok((f, Some(arg)))
                    }
                    None => Ok((f, None)),
                }
            })
            .collect()
    }
}

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
        plan: Box<ir::Plan>,
    ) -> Result<PhysicalPlan<'env, 'txn, S, ReadonlyExecutionMode>> {
        self.plan_node(tx, plan).map(PhysicalPlan)
    }

    #[allow(clippy::boxed_local)]
    fn plan_node<M: ExecutionMode<'env, S>>(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        plan: Box<ir::Plan>,
    ) -> Result<Arc<dyn PhysicalNode<'env, 'txn, S, M>>> {
        let plan = match *plan {
            ir::Plan::Transaction(kind) => PhysicalTransaction::plan(kind),
            ir::Plan::SetVariable { name, value, scope } => PhysicalSet::plan(name, value, scope),
            ir::Plan::Show(object_type) => PhysicalShow::plan(object_type),
            ir::Plan::Explain(_logical_plan) => {
                todo!()
                // let physical_plan = self.plan_node(tx, logical_plan.clone())?;
                // return self.explain_plan(logical_plan, physical_plan);
            }
            ir::Plan::Drop(_) | ir::Plan::CreateNamespace(_) | ir::Plan::CreateTable(_) => {
                unreachable!("write plans should go through plan_write_node, got plan {:?}", plan)
            }
            ir::Plan::Query(query) => self.plan_query_node(tx, query)?,
        };

        Ok(plan)
    }

    #[inline]
    pub fn plan_write(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        plan: Box<ir::Plan>,
    ) -> Result<PhysicalPlan<'env, 'txn, S, ReadWriteExecutionMode>> {
        self.plan_write_node(tx, plan).map(PhysicalPlan)
    }

    #[allow(clippy::boxed_local)]
    fn plan_write_query_node(
        &mut self,
        _tx: &dyn Transaction<'env, S>,
        _plan: Box<ir::QueryPlan>,
    ) -> Result<Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>> {
        panic!();
        // let plan = match *plan {
        //     ir::QueryPlan::Update { table, source, returning, schema: _ } => {
        //         panic!()
        //     }
        //     ir::QueryPlan::Insert { table, source, returning, schema: _ } => {
        //         panic!()
        //         // let source = self.plan_write_query_node(tx, source)?;
        //         // PhysicalInsert::plan(
        //         //     table,
        //         //     source,
        //         //     returning.map(|exprs| self.compile_exprs(tx, exprs)).transpose()?,
        //         // )
        //     }
        //     _ => return self.fold_query_plan(tx, plan, Self::plan_write_query_node),
        // };
        //
        // Ok(plan)
    }

    fn plan_write_node(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        plan: Box<ir::Plan>,
    ) -> Result<Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>> {
        let plan = match *plan {
            ir::Plan::CreateTable(info) => PhysicalCreateTable::plan(info),
            ir::Plan::CreateNamespace(info) => PhysicalCreateNamespace::plan(info),
            ir::Plan::Drop(refs) => PhysicalDrop::plan(refs),
            ir::Plan::Query(query) => self.plan_write_query_node(tx, query)?,
            ir::Plan::Explain(_logical_plan) => {
                todo!()
                // let physical_plan = self.plan_write_node(tx, logical_plan.clone())?;
                // return self.explain_plan(logical_plan, physical_plan);
            }
            _ => self.plan_node(tx, plan)?,
        };

        Ok(plan)
    }

    fn explain_plan<M: ExecutionMode<'env, S>>(
        &self,
        logical_plan: Box<ir::Plan<opt::Query>>,
        physical_plan: Arc<dyn PhysicalNode<'env, 'txn, S, M>>,
    ) -> Result<Arc<dyn PhysicalNode<'env, 'txn, S, M>>> {
        Ok(PhysicalExplain::plan(logical_plan, physical_plan))
    }

    #[allow(clippy::boxed_local)]
    fn plan_query_node<M: ExecutionMode<'env, S>>(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        plan: Box<ir::QueryPlan>,
    ) -> Result<Arc<dyn PhysicalNode<'env, 'txn, S, M>>> {
        self.fold_query_plan(tx, plan, Self::plan_query_node)
    }

    #[allow(clippy::boxed_local)]
    fn fold_query_plan<M: ExecutionMode<'env, S>>(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        plan: Box<ir::QueryPlan>,
        mut f: impl FnMut(
            &mut Self,
            &dyn Transaction<'env, S>,
            Box<ir::QueryPlan>,
        ) -> Result<Arc<dyn PhysicalNode<'env, 'txn, S, M>>>,
    ) -> Result<Arc<dyn PhysicalNode<'env, 'txn, S, M>>> {
        let plan = match *plan {
            ir::QueryPlan::TableScan { table, projection, projected_schema: _ } => {
                PhysicalTableScan::plan(table, projection)
            }
            ir::QueryPlan::Values { schema: _, values } => {
                PhysicalValues::plan(self.compile_values(tx, values)?)
            }
            ir::QueryPlan::Projection { source, projection, projected_schema: _ } => {
                PhysicalProjection::plan(f(self, tx, source)?, self.compile_exprs(tx, projection)?)
            }
            ir::QueryPlan::Limit { source, limit, exceeded_message } => {
                PhysicalLimit::plan(f(self, tx, source)?, limit, exceeded_message.map(Into::into))
            }
            ir::QueryPlan::Order { source, order } => {
                PhysicalOrder::plan(f(self, tx, source)?, self.compile_order_exprs(tx, order)?)
            }
            ir::QueryPlan::Filter { source, predicate } => {
                PhysicalFilter::plan(f(self, tx, source)?, self.compile_expr(tx, predicate)?)
            }
            ir::QueryPlan::Join { schema: _, join, lhs, rhs } => PhysicalNestedLoopJoin::plan(
                join.try_map(|expr| self.compile_expr(tx, expr))?,
                f(self, tx, lhs)?,
                f(self, tx, rhs)?,
            ),
            ir::QueryPlan::Aggregate { aggregates: _, source: _, group_by, schema: _ }
                if group_by.is_empty() =>
            {
                panic!();
                // let functions = self.compile_aggregate_functions(tx, aggregates.to_vec())?;
                // PhysicalUngroupedAggregate::plan(functions, f(self, tx, source)?)
            }
            ir::QueryPlan::Aggregate { aggregates: _, source: _, group_by: _, schema: _ } => {
                panic!();
                // PhysicalHashAggregate::plan(
                //     self.compile_aggregate_functions(tx, aggregates.to_vec())?,
                //     f(self, tx, source)?,
                //     self.compile_exprs(tx, group_by)?,
                // )
            }
            ir::QueryPlan::Unnest { expr, schema: _ } => {
                PhysicalUnnest::plan(self.compile_expr(tx, expr)?)
            }
            ir::QueryPlan::DummyScan => PhysicalDummyScan::plan(),
            ir::QueryPlan::Update { .. } | ir::QueryPlan::Insert { .. } => {
                unreachable!(
                    "write query plans should go through plan_write_query_node, got plan {:?}",
                    plan
                )
            }
        };

        Ok(plan)
    }

    fn compile_values(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        values: ir::Values,
    ) -> Result<Box<[ExecutableTupleExpr]>> {
        values
            .into_inner()
            .into_vec()
            .into_iter()
            .map(|expr| self.compile_exprs(tx, expr))
            .collect()
    }

    fn compile_exprs(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        exprs: Box<[ir::Expr]>,
    ) -> Result<ExecutableTupleExpr> {
        self.compiler.compile_many(&self.catalog, tx, exprs.into_vec())
    }

    fn compile_expr(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        expr: ir::Expr,
    ) -> Result<ExecutableExpr> {
        self.compiler.compile(&self.catalog, tx, expr)
    }

    fn compile_order_exprs(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        expr: Box<[ir::OrderExpr]>,
    ) -> Result<Box<[ir::OrderExpr<ExecutableExpr>]>> {
        expr.into_vec().into_iter().map(|expr| self.compile_order_expr(tx, expr)).collect()
    }

    fn compile_order_expr(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        expr: ir::OrderExpr,
    ) -> Result<ir::OrderExpr<ExecutableExpr>> {
        Ok(ir::OrderExpr { expr: self.compile_expr(tx, expr.expr)?, asc: expr.asc })
    }
}
