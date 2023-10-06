mod aggregate;
pub(crate) mod explain;
mod join;
mod physical_copy_to;
mod physical_cte;
mod physical_cte_scan;
mod physical_drop;
mod physical_dummy_scan;
mod physical_explain;
mod physical_filter;
mod physical_hash_distinct;
mod physical_insert;
mod physical_limit;
mod physical_order;
mod physical_projection;
mod physical_set;
mod physical_show;
mod physical_table_scan;
mod physical_transaction;
mod physical_union;
mod physical_unnest;
mod physical_update;
mod physical_values;

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use anyhow::Result;
use nsql_catalog::Catalog;
use nsql_core::Name;
use nsql_storage::eval::{self, ExecutableExpr, ExecutableFunction, ExecutableTupleExpr};
use nsql_storage_engine::{StorageEngine, Transaction};

use self::aggregate::{PhysicalHashAggregate, PhysicalUngroupedAggregate};
pub use self::explain::Explain;
use self::join::PhysicalNestedLoopJoin;
use self::physical_copy_to::PhysicalCopyTo;
use self::physical_cte::PhysicalCte;
use self::physical_cte_scan::PhysicalCteScan;
use self::physical_drop::PhysicalDrop;
use self::physical_dummy_scan::PhysicalDummyScan;
use self::physical_explain::PhysicalExplain;
use self::physical_filter::PhysicalFilter;
use self::physical_hash_distinct::PhysicalHashDistinct;
use self::physical_insert::PhysicalInsert;
use self::physical_limit::PhysicalLimit;
use self::physical_order::PhysicalOrder;
use self::physical_projection::PhysicalProjection;
use self::physical_set::PhysicalSet;
use self::physical_show::PhysicalShow;
use self::physical_table_scan::PhysicalTableScan;
use self::physical_transaction::PhysicalTransaction;
use self::physical_union::PhysicalUnion;
use self::physical_unnest::PhysicalUnnest;
use self::physical_update::PhysicalUpdate;
use self::physical_values::PhysicalValues;
use crate::compile::Compiler;
use crate::executor::OutputSink;
use crate::pipeline::*;
use crate::{
    ExecutionContext, ExecutionMode, ExecutionResult, OperatorState, PhysicalNode,
    PhysicalOperator, PhysicalSink, PhysicalSource, ReadWriteExecutionMode, Tuple, TupleStream,
};

pub struct PhysicalPlanner<'env, 'txn, S, M> {
    catalog: Catalog<'env, S>,
    compiler: Compiler<ExecutableFunction<S>>,
    ctes: HashMap<Name, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>,
}

/// Opaque physical plan that is ready to be executed
pub struct PhysicalPlan<'env, 'txn, S, M>(Arc<dyn PhysicalNode<'env, 'txn, S, M>>);

impl<'env, 'txn, S, M> PhysicalPlan<'env, 'txn, S, M> {
    pub(crate) fn root(&self) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::clone(&self.0)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalPlanner<'env, 'txn, S, M>
{
    pub fn new(catalog: Catalog<'env, S>) -> Self {
        Self { catalog, compiler: Default::default(), ctes: Default::default() }
    }

    #[inline]
    pub fn plan(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        plan: Box<ir::Plan<opt::Query>>,
    ) -> Result<PhysicalPlan<'env, 'txn, S, M>> {
        let node = match *plan {
            ir::Plan::Transaction(kind) => PhysicalTransaction::plan(kind),
            ir::Plan::SetVariable { name, value, scope } => PhysicalSet::plan(name, value, scope),
            ir::Plan::Show(object_type) => PhysicalShow::plan(object_type),
            ir::Plan::Query(q) => self.plan_root_node(tx, &q)?,
            ir::Plan::Explain(logical_plan) => {
                let logical_explain = logical_plan.to_string();
                let physical_plan = self.plan(tx, logical_plan)?;
                PhysicalExplain::plan(logical_explain, physical_plan.0)
            }
            ir::Plan::Drop(..) => unreachable!("write plans should go through plan_write_node"),
            ir::Plan::Copy(cp) => match cp {
                ir::Copy::To(ir::CopyTo { src, dst }) => {
                    PhysicalCopyTo::plan(self.plan_root_node(tx, &src)?, dst)
                }
            },
        };

        Ok(PhysicalPlan(node))
    }

    fn plan_root_node(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        q: &opt::Query,
    ) -> Result<Arc<dyn PhysicalNode<'env, 'txn, S, M>>> {
        self.plan_node(tx, q, q.root())
    }

    fn plan_node(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        q: &opt::Query,
        plan: opt::Plan<'_>,
    ) -> Result<Arc<dyn PhysicalNode<'env, 'txn, S, M>>> {
        self.fold_plan_node(tx, q, plan, |planner, node| planner.plan_node(tx, q, node))
    }

    fn fold_plan_node(
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
                PhysicalProjection::plan(f(self, source)?, self.compile_exprs(tx, q, exprs)?)
            }
            opt::Plan::Filter(filter) => match filter.source(q) {
                // we expect a join node to be followed by a filter
                opt::Plan::Join(join) => PhysicalNestedLoopJoin::plan(
                    join.kind(),
                    self.compile_expr(tx, q, filter.predicate(q))?,
                    f(self, join.lhs(q))?,
                    f(self, join.rhs(q))?,
                ),
                _ => {
                    let source = f(self, filter.source(q))?;
                    let predicate = self.compile_expr(tx, q, filter.predicate(q))?;
                    PhysicalFilter::plan(source, predicate)
                }
            },
            opt::Plan::Join(join) => match join.kind() {
                // cross-join
                ir::JoinKind::Inner => PhysicalNestedLoopJoin::plan(
                    ir::JoinKind::Inner,
                    eval::Expr::literal(true),
                    f(self, join.lhs(q))?,
                    f(self, join.rhs(q))?,
                ),
                _ => unreachable!("expected non-dependent join to be the child of a filter"),
            },
            opt::Plan::Limit(limit) => {
                let source = f(self, limit.source(q))?;
                PhysicalLimit::plan(source, limit.limit(q), limit.limit_exceeded_message(q))
            }
            opt::Plan::Distinct(distinct) => {
                let source = f(self, distinct.source(q))?;
                PhysicalHashDistinct::plan(source)
            }
            opt::Plan::Aggregate(agg) => {
                let aggregates = self.compile_aggregate_functions(tx, q, agg.aggregates(q))?;
                let group_by = agg.group_by(q);
                let source = f(self, agg.source(q))?;
                if group_by.is_empty() {
                    PhysicalUngroupedAggregate::plan(aggregates, source)
                } else {
                    PhysicalHashAggregate::plan(
                        aggregates,
                        source,
                        self.compile_exprs(tx, q, group_by)?,
                    )
                }
            }
            opt::Plan::Values(values) => PhysicalValues::plan(
                values
                    .rows(q)
                    .map(|exprs| self.compile_exprs(tx, q, exprs))
                    .collect::<Result<_>>()?,
            ),
            opt::Plan::Order(order) => PhysicalOrder::plan(
                f(self, order.source(q))?,
                self.compile_order_exprs(tx, q, order.order_exprs(q))?,
            ),
            opt::Plan::TableScan(scan) => {
                let table = self.catalog.table(tx, scan.table(q))?;
                let columns = table.columns(self.catalog, tx)?;
                PhysicalTableScan::plan(table, columns, None)
            }
            opt::Plan::DummyScan => PhysicalDummyScan::plan(None),
            opt::Plan::Empty(empty) => PhysicalDummyScan::plan(Some(empty.width())),
            opt::Plan::Union(union) => {
                PhysicalUnion::plan(f(self, union.lhs(q))?, f(self, union.rhs(q))?)
            }
            opt::Plan::Unnest(unnest) => {
                PhysicalUnnest::plan(self.compile_expr(tx, q, unnest.expr(q))?)
            }
            opt::Plan::Update(..) | opt::Plan::Insert(..) => unreachable!(
                "write query plans should go through plan_write_query_node, got plan {:?}",
                plan,
            ),
            opt::Plan::CteScan(scan) => {
                let name = scan.name();
                let cte =
                    self.ctes.get(&name).cloned().expect("cte should be planned before the scan");
                PhysicalCteScan::plan(name, cte)
            }
            opt::Plan::Cte(cte) => {
                let cte_plan = f(self, cte.cte_plan(q))?;
                self.ctes.insert(cte.name(), Arc::clone(&cte_plan));
                PhysicalCte::plan(cte.name(), cte_plan, f(self, cte.child(q))?)
            }
        };

        Ok(plan)
    }

    fn compile_order_exprs(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        q: &opt::Query,
        exprs: impl Iterator<Item = ir::OrderExpr<opt::Expr<'_>>>,
    ) -> Result<Box<[ir::OrderExpr<ExecutableExpr<S>>]>> {
        exprs.map(|expr| self.compile_order_expr(tx, q, expr)).collect()
    }

    fn compile_order_expr(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        q: &opt::Query,
        expr: ir::OrderExpr<opt::Expr<'_>>,
    ) -> Result<ir::OrderExpr<ExecutableExpr<S>>> {
        Ok(ir::OrderExpr { expr: self.compile_expr(tx, q, expr.expr)?, asc: expr.asc })
    }

    fn compile_exprs(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        q: &opt::Query,
        exprs: impl Iterator<Item = opt::Expr<'_>>,
    ) -> Result<ExecutableTupleExpr<S>> {
        self.compiler.compile_many(&self.catalog, tx, q, exprs)
    }

    fn compile_expr(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        q: &opt::Query,
        expr: opt::Expr<'_>,
    ) -> Result<ExecutableExpr<S>> {
        self.compiler.compile(&self.catalog, tx, q, expr)
    }

    #[allow(clippy::type_complexity)]
    fn compile_aggregate_functions(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        q: &opt::Query,
        functions: impl IntoIterator<Item = opt::CallExpr<'_>>,
    ) -> Result<Box<[(ir::Function, Option<ExecutableExpr<S>>)]>> {
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
                        let arg = self.compile_expr(tx, q, arg)?;
                        Ok((f, Some(arg)))
                    }
                    None => Ok((f, None)),
                }
            })
            .collect()
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalPlanner<'env, 'txn, S, ReadWriteExecutionMode> {
    pub fn plan_write(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        plan: Box<ir::Plan<opt::Query>>,
    ) -> Result<PhysicalPlan<'env, 'txn, S, ReadWriteExecutionMode>> {
        let node = match *plan {
            ir::Plan::Drop(refs) => PhysicalDrop::plan(refs),
            ir::Plan::Query(q) => self.plan_write_query(tx, &q, q.root())?,
            ir::Plan::Explain(logical_plan) => {
                let logical_explain = logical_plan.to_string();
                let physical_plan = self.plan_write(tx, logical_plan)?;
                PhysicalExplain::plan(logical_explain, physical_plan.0)
            }
            ir::Plan::Copy(..)
            | ir::Plan::Transaction(..)
            | ir::Plan::SetVariable { .. }
            | ir::Plan::Show(..) => return self.plan(tx, plan),
        };

        Ok(PhysicalPlan(node))
    }

    fn plan_write_query(
        &mut self,
        tx: &dyn Transaction<'env, S>,
        q: &opt::Query,
        plan: opt::Plan<'_>,
    ) -> Result<Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>> {
        let plan = match plan {
            opt::Plan::Update(insert) => PhysicalUpdate::plan(
                insert.table(q),
                self.plan_write_query(tx, q, insert.source(q))?,
                self.compile_exprs(tx, q, insert.returning(q))?,
            ),
            opt::Plan::Insert(insert) => PhysicalInsert::plan(
                insert.table(q),
                self.plan_write_query(tx, q, insert.source(q))?,
                self.compile_exprs(tx, q, insert.returning(q))?,
            ),
            _ => {
                return self.fold_plan_node(tx, q, plan, |planner, node| {
                    planner.plan_write_query(tx, q, node)
                });
            }
        };

        Ok(plan)
    }
}
