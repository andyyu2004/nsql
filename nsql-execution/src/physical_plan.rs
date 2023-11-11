mod aggregate;
pub(crate) mod explain;
mod join;
mod physical_copy_to;
mod physical_cross_product;
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
use std::sync::Arc;
use std::{fmt, mem};

use anyhow::Result;
use nsql_catalog::expr::{
    Evaluator, ExecutableExpr, ExecutableFunction, ExecutableTupleExpr, ExprEvalExt,
};
use nsql_catalog::{Catalog, TransactionContext};
use nsql_core::Name;
use nsql_storage_engine::StorageEngine;

use self::aggregate::{PhysicalHashAggregate, PhysicalUngroupedAggregate};
pub use self::explain::Explain;
use self::join::{PhysicalHashJoin, PhysicalNestedLoopJoin};
use self::physical_copy_to::PhysicalCopyTo;
use self::physical_cross_product::PhysicalCrossProduct;
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
    impl_physical_node_conversions, ExecutionContext, ExecutionMode, ExecutionResult,
    OperatorState, PhysicalNode, PhysicalNodeArena, PhysicalNodeId, PhysicalOperator, PhysicalSink,
    PhysicalSource, ReadWriteExecutionMode, Tuple, TupleStream, TupleTrait,
};

pub trait PlannerProfiler: nsql_core::Profiler {
    fn catalog_function_lookup_event_id(&self) -> Self::EventId;

    fn compile_event_id(&self) -> Self::EventId;

    fn explain_event_id(&self) -> Self::EventId;
}

pub struct PhysicalPlanner<'env, 'txn, S, M, T> {
    arena: PhysicalNodeArena<'env, 'txn, S, M, T>,
    catalog: Catalog<'env, S>,
    compiler: Compiler<ExecutableFunction<'env, 'txn, S, M>>,
    ctes: HashMap<Name, PhysicalNodeId>,
}

/// Opaque physical plan that is ready to be executed
pub struct PhysicalPlan<'env, 'txn, S, M, T> {
    nodes: PhysicalNodeArena<'env, 'txn, S, M, T>,
    root: PhysicalNodeId,
}

impl<'env, 'txn, S, M, T> PhysicalPlan<'env, 'txn, S, M, T> {
    pub(crate) fn root(&self) -> PhysicalNodeId {
        self.root
    }

    pub(crate) fn arena(&self) -> &PhysicalNodeArena<'env, 'txn, S, M, T> {
        &self.nodes
    }

    pub(crate) fn arena_mut(&mut self) -> &mut PhysicalNodeArena<'env, 'txn, S, M, T> {
        &mut self.nodes
    }

    pub(crate) fn into_arena(self) -> PhysicalNodeArena<'env, 'txn, S, M, T> {
        self.nodes
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait>
    PhysicalPlanner<'env, 'txn, S, M, T>
{
    pub fn new(catalog: Catalog<'env, S>) -> Self {
        Self {
            catalog,
            arena: PhysicalNodeArena { nodes: Arena::new() },
            compiler: Default::default(),
            ctes: Default::default(),
        }
    }

    #[inline]
    pub fn plan(
        mut self,
        profiler: &impl PlannerProfiler,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        plan: Box<ir::Plan<opt::Query>>,
    ) -> Result<PhysicalPlan<'env, 'txn, S, M, T>> {
        self.do_plan(profiler, tx, plan).map(|root| PhysicalPlan { nodes: self.arena, root })
    }

    #[inline]
    fn do_plan(
        &mut self,
        profiler: &impl PlannerProfiler,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        plan: Box<ir::Plan<opt::Query>>,
    ) -> Result<PhysicalNodeId> {
        self.fold_plan(
            profiler,
            tx,
            plan,
            |planner, plan| planner.do_plan(profiler, tx, plan),
            |planner, q| planner.plan_root_query(profiler, tx, q),
        )
    }

    fn fold_plan(
        &mut self,
        profiler: &impl PlannerProfiler,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        plan: Box<ir::Plan<opt::Query>>,
        mut f: impl FnMut(&mut Self, Box<ir::Plan<opt::Query>>) -> Result<PhysicalNodeId>,
        plan_query: impl FnOnce(&mut Self, &opt::Query) -> Result<PhysicalNodeId>,
    ) -> Result<PhysicalNodeId> {
        let node = match *plan {
            ir::Plan::Transaction(kind) => PhysicalTransaction::plan(kind, &mut self.arena),
            ir::Plan::SetVariable { name, value, scope } => {
                PhysicalSet::plan(name, value, scope, &mut self.arena)
            }
            ir::Plan::Show(object_type) => PhysicalShow::plan(object_type, &mut self.arena),
            ir::Plan::Query(q) => plan_query(self, &q)?,
            ir::Plan::Explain(opts, logical_plan) => {
                profiler.profile::<Result<_>>(profiler.explain_event_id(), || {
                    let logical_explain = if opts.verbose {
                        format!("{logical_plan:#}")
                    } else {
                        format!("{logical_plan}")
                    };

                    let root = f(self, logical_plan)?;
                    let mut physical_plan =
                        PhysicalPlan { nodes: mem::take(&mut self.arena), root };
                    let physical_explain = physical_plan.explain_tree(self.catalog, tx);
                    let sink = OutputSink::plan(physical_plan.arena_mut());
                    let pipeline = crate::build_pipelines(sink, physical_plan);
                    let pipeline_explain = pipeline.display(self.catalog, tx).to_string();
                    let (_pipelines, arena) = pipeline.into_parts();
                    self.arena = arena;
                    Ok(PhysicalExplain::plan(
                        opts,
                        root,
                        logical_explain,
                        physical_explain,
                        pipeline_explain,
                        &mut self.arena,
                    ))
                })?
            }
            ir::Plan::Drop(..) => {
                unreachable!("write plans should go through plan_write_node, got drop node")
            }
            ir::Plan::Copy(cp) => match cp {
                ir::Copy::To(ir::CopyTo { src, dst }) => {
                    PhysicalCopyTo::plan(plan_query(self, &src)?, dst, &mut self.arena)
                }
            },
        };

        Ok(node)
    }

    fn plan_root_query(
        &mut self,
        profiler: &impl PlannerProfiler,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        q: &opt::Query,
    ) -> Result<PhysicalNodeId> {
        self.plan_node(profiler, tx, q, q.root())
    }

    fn plan_node(
        &mut self,
        profiler: &impl PlannerProfiler,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        q: &opt::Query,
        plan: opt::Plan<'_>,
    ) -> Result<PhysicalNodeId> {
        self.fold_query_plan(profiler, tx, q, plan, |planner, node| {
            planner.plan_node(profiler, tx, q, node)
        })
    }

    fn fold_query_plan(
        &mut self,
        profiler: &impl PlannerProfiler,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        q: &opt::Query,
        plan: opt::Plan<'_>,
        mut f: impl FnMut(&mut Self, opt::Plan<'_>) -> Result<PhysicalNodeId>,
    ) -> Result<PhysicalNodeId> {
        let plan = match plan {
            opt::Plan::Projection(projection) => {
                let source = projection.source(q);
                let exprs = projection.exprs(q);
                PhysicalProjection::plan(
                    f(self, source)?,
                    self.compile_exprs(profiler, tx, q, exprs)?,
                    &mut self.arena,
                )
            }
            opt::Plan::Filter(filter) => match filter.source(q) {
                // push the filter into NLP joins if possible
                opt::Plan::Join(join) if join.conditions(q).is_empty() => {
                    PhysicalNestedLoopJoin::plan(
                        join.kind(),
                        self.compile_expr(profiler, tx, q, filter.predicate(q))?,
                        f(self, join.lhs(q))?,
                        f(self, join.rhs(q))?,
                        &mut self.arena,
                    )
                }
                _ => {
                    let source = f(self, filter.source(q))?;
                    let predicate = self.compile_expr(profiler, tx, q, filter.predicate(q))?;
                    PhysicalFilter::plan(source, predicate, &mut self.arena)
                }
            },
            opt::Plan::Join(join) => {
                let lhs = f(self, join.lhs(q))?;
                let rhs = f(self, join.rhs(q))?;

                match join.kind() {
                    // cross-join
                    ir::JoinKind::Inner if join.conditions(q).is_empty() => {
                        PhysicalCrossProduct::plan(lhs, rhs, &mut self.arena)
                    }
                    // currently if we arrive here then it's computable via a hash-join
                    kind => PhysicalHashJoin::plan(
                        kind,
                        self.compile_join_conditions(profiler, tx, q, join.conditions(q))?,
                        lhs,
                        rhs,
                        &mut self.arena,
                    ),
                }
            }
            opt::Plan::Limit(limit) => {
                let source = f(self, limit.source(q))?;
                PhysicalLimit::plan(
                    source,
                    limit.limit(q),
                    limit.limit_exceeded_message(q).cloned(),
                    &mut self.arena,
                )
            }
            opt::Plan::Distinct(distinct) => {
                let source = f(self, distinct.source(q))?;
                PhysicalHashDistinct::plan(source, &mut self.arena)
            }
            opt::Plan::Aggregate(agg) => {
                let aggregates =
                    self.compile_aggregate_functions(profiler, tx, q, agg.aggregates(q))?;
                let group_by = agg.group_by(q);
                let source = f(self, agg.source(q))?;
                if group_by.is_empty() {
                    PhysicalUngroupedAggregate::plan(aggregates, source, &mut self.arena)
                } else {
                    PhysicalHashAggregate::plan(
                        aggregates,
                        source,
                        self.compile_exprs(profiler, tx, q, group_by)?,
                        &mut self.arena,
                    )
                }
            }
            opt::Plan::Values(values) => PhysicalValues::plan(
                values
                    .rows(q)
                    .map(|exprs| self.compile_exprs(profiler, tx, q, exprs))
                    .collect::<Result<_>>()?,
                &mut self.arena,
            ),
            opt::Plan::Order(order) => PhysicalOrder::plan(
                f(self, order.source(q))?,
                self.compile_order_exprs(profiler, tx, q, order.order_exprs(q))?,
                &mut self.arena,
            ),
            opt::Plan::TableScan(scan) => {
                let table = self.catalog.table(tx, scan.table(q))?;
                let columns = table.columns(self.catalog, tx)?;
                PhysicalTableScan::plan(table, columns, None, &mut self.arena)
            }
            opt::Plan::DummyScan => PhysicalDummyScan::plan(None, &mut self.arena),
            opt::Plan::Empty(empty) => {
                PhysicalDummyScan::plan(Some(empty.width()), &mut self.arena)
            }
            opt::Plan::Union(union) => {
                PhysicalUnion::plan(f(self, union.lhs(q))?, f(self, union.rhs(q))?, &mut self.arena)
            }
            opt::Plan::Unnest(unnest) => PhysicalUnnest::plan(
                self.compile_expr(profiler, tx, q, unnest.expr(q))?,
                &mut self.arena,
            ),
            opt::Plan::Update(..) | opt::Plan::Insert(..) => unreachable!(
                "write query plans should go through plan_write_query_node, got plan {:?}",
                plan,
            ),
            opt::Plan::CteScan(scan) => {
                let name = scan.name();
                let cte =
                    self.ctes.get(&name).cloned().expect("cte should be planned before the scan");
                PhysicalCteScan::plan(name, cte, &mut self.arena)
            }
            opt::Plan::Cte(cte) => {
                let cte_plan = f(self, cte.cte_plan(q))?;
                self.ctes.insert(cte.name(), cte_plan);
                PhysicalCte::plan(cte.name(), cte_plan, f(self, cte.child(q))?, &mut self.arena)
            }
        };

        Ok(plan)
    }

    #[allow(clippy::type_complexity)]
    fn compile_join_conditions(
        &mut self,
        profiler: &impl PlannerProfiler,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        q: &opt::Query,
        conditions: impl Iterator<Item = ir::JoinCondition<opt::Expr<'_>>>,
    ) -> Result<Box<[ir::JoinCondition<ExecutableExpr<'env, 'txn, S, M>>]>> {
        conditions
            .map(|condition| self.compile_join_condition(profiler, tx, q, condition))
            .collect()
    }

    fn compile_join_condition(
        &mut self,
        profiler: &impl PlannerProfiler,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        q: &opt::Query,
        condition: ir::JoinCondition<opt::Expr<'_>>,
    ) -> Result<ir::JoinCondition<ExecutableExpr<'env, 'txn, S, M>>> {
        Ok(ir::JoinCondition {
            op: condition.op,
            lhs: self.compile_expr(profiler, tx, q, condition.lhs)?,
            rhs: self.compile_expr(profiler, tx, q, condition.rhs)?,
        })
    }

    #[allow(clippy::type_complexity)]
    fn compile_order_exprs(
        &mut self,
        profiler: &impl PlannerProfiler,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        q: &opt::Query,
        exprs: impl Iterator<Item = ir::OrderExpr<opt::Expr<'_>>>,
    ) -> Result<Box<[ir::OrderExpr<ExecutableExpr<'env, 'txn, S, M>>]>> {
        exprs.map(|expr| self.compile_order_expr(profiler, tx, q, expr)).collect()
    }

    fn compile_order_expr(
        &mut self,
        profiler: &impl PlannerProfiler,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        q: &opt::Query,
        expr: ir::OrderExpr<opt::Expr<'_>>,
    ) -> Result<ir::OrderExpr<ExecutableExpr<'env, 'txn, S, M>>> {
        Ok(ir::OrderExpr { expr: self.compile_expr(profiler, tx, q, expr.expr)?, asc: expr.asc })
    }

    fn compile_exprs(
        &mut self,
        profiler: &impl PlannerProfiler,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        q: &opt::Query,
        exprs: impl Iterator<Item = opt::Expr<'_>>,
    ) -> Result<ExecutableTupleExpr<'env, 'txn, S, M>> {
        self.compiler.compile_many(profiler, &self.catalog, tx, q, exprs)
    }

    fn compile_expr(
        &mut self,
        profiler: &impl PlannerProfiler,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        q: &opt::Query,
        expr: opt::Expr<'_>,
    ) -> Result<ExecutableExpr<'env, 'txn, S, M>> {
        self.compiler.compile(profiler, &self.catalog, tx, q, expr)
    }

    #[allow(clippy::type_complexity)]
    fn compile_aggregate_functions(
        &mut self,
        profiler: &impl PlannerProfiler,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        q: &opt::Query,
        functions: impl IntoIterator<Item = opt::CallExpr<'_>>,
    ) -> Result<Box<[(ir::Function, Option<ExecutableExpr<'env, 'txn, S, M>>)]>> {
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
                        let arg = self.compile_expr(profiler, tx, q, arg)?;
                        Ok((f, Some(arg)))
                    }
                    None => Ok((f, None)),
                }
            })
            .collect()
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, T: TupleTrait>
    PhysicalPlanner<'env, 'txn, S, ReadWriteExecutionMode, T>
{
    pub fn plan_write(
        mut self,
        profiler: &impl PlannerProfiler,
        tx: &dyn TransactionContext<'env, 'txn, S, ReadWriteExecutionMode>,
        plan: Box<ir::Plan<opt::Query>>,
    ) -> Result<PhysicalPlan<'env, 'txn, S, ReadWriteExecutionMode, T>> {
        self.do_plan_write(profiler, tx, plan).map(|root| PhysicalPlan { nodes: self.arena, root })
    }

    fn do_plan_write(
        &mut self,
        profiler: &impl PlannerProfiler,
        tx: &dyn TransactionContext<'env, 'txn, S, ReadWriteExecutionMode>,
        plan: Box<ir::Plan<opt::Query>>,
    ) -> Result<PhysicalNodeId> {
        match *plan {
            ir::Plan::Drop(refs) => Ok(PhysicalDrop::plan(refs, &mut self.arena)),
            _ => self.fold_plan(
                profiler,
                tx,
                plan,
                |planner, plan| planner.do_plan_write(profiler, tx, plan),
                |planner, q| planner.plan_root_write_query(profiler, tx, q),
            ),
        }
    }

    fn plan_root_write_query(
        &mut self,
        profiler: &impl PlannerProfiler,
        tx: &dyn TransactionContext<'env, 'txn, S, ReadWriteExecutionMode>,
        q: &opt::Query,
    ) -> Result<PhysicalNodeId> {
        self.plan_write_query(profiler, tx, q, q.root())
    }

    fn plan_write_query(
        &mut self,
        profiler: &impl PlannerProfiler,
        tx: &dyn TransactionContext<'env, 'txn, S, ReadWriteExecutionMode>,
        q: &opt::Query,
        plan: opt::Plan<'_>,
    ) -> Result<PhysicalNodeId> {
        let plan = match plan {
            opt::Plan::Update(insert) => PhysicalUpdate::plan(
                insert.table(q),
                self.plan_write_query(profiler, tx, q, insert.source(q))?,
                self.compile_exprs(profiler, tx, q, insert.returning(q))?,
                &mut self.arena,
            ),
            opt::Plan::Insert(insert) => PhysicalInsert::plan(
                insert.table(q),
                self.plan_write_query(profiler, tx, q, insert.source(q))?,
                self.compile_exprs(profiler, tx, q, insert.returning(q))?,
                &mut self.arena,
            ),
            _ => {
                return self.fold_query_plan(profiler, tx, q, plan, |planner, node| {
                    planner.plan_write_query(profiler, tx, q, node)
                });
            }
        };

        Ok(plan)
    }
}
