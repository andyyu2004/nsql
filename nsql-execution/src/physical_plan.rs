pub(crate) mod explain;
mod physical_create_namespace;
mod physical_create_table;
mod physical_drop;
mod physical_dummy_scan;
mod physical_explain;
mod physical_filter;
mod physical_insert;
mod physical_limit;
mod physical_projection;
mod physical_show;
mod physical_table_scan;
mod physical_transaction;
mod physical_update;
mod physical_values;

use std::fmt;
use std::sync::Arc;

use anyhow::Result;
use nsql_catalog::Catalog;
use nsql_plan::Plan;
use nsql_storage_engine::{StorageEngine, Transaction};

pub use self::explain::Explain;
use self::physical_create_namespace::PhysicalCreateNamespace;
use self::physical_create_table::PhysicalCreateTable;
use self::physical_drop::PhysicalDrop;
use self::physical_dummy_scan::PhysicalDummyScan;
use self::physical_explain::PhysicalExplain;
use self::physical_filter::PhysicalFilter;
use self::physical_insert::PhysicalInsert;
use self::physical_limit::PhysicalLimit;
use self::physical_projection::PhysicalProjection;
use self::physical_show::PhysicalShow;
use self::physical_table_scan::PhysicalTableScan;
use self::physical_transaction::PhysicalTransaction;
use self::physical_update::PhysicalUpdate;
use self::physical_values::PhysicalValues;
use crate::executor::OutputSink;
use crate::{
    Evaluator, ExecutionContext, ExecutionMode, ExecutionResult, OperatorState, PhysicalNode,
    PhysicalOperator, PhysicalSink, PhysicalSource, ReadWriteExecutionMode, ReadonlyExecutionMode,
    Tuple, TupleStream,
};

pub struct PhysicalPlanner<'env, S> {
    catalog: Catalog<'env, S>,
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
        Self { catalog }
    }

    #[inline]
    pub fn plan(
        &self,
        tx: &dyn Transaction<'env, S>,
        plan: Box<Plan>,
    ) -> Result<PhysicalPlan<'env, 'txn, S, ReadonlyExecutionMode>> {
        self.plan_node(tx, plan).map(PhysicalPlan)
    }

    #[inline]
    pub fn plan_write(
        &self,
        tx: &dyn Transaction<'env, S>,
        plan: Box<Plan>,
    ) -> Result<PhysicalPlan<'env, 'txn, S, ReadWriteExecutionMode>> {
        self.plan_write_node(tx, plan).map(PhysicalPlan)
    }

    fn plan_write_node(
        &self,
        tx: &dyn Transaction<'env, S>,
        plan: Box<Plan>,
    ) -> Result<Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>> {
        let plan = match *plan {
            Plan::Update { table_ref, source, returning } => {
                let source = self.plan_write_node(tx, source)?;
                PhysicalUpdate::plan(table_ref, source, returning)
            }
            Plan::Insert { table_ref, projection, source, returning } => {
                let mut source = self.plan_write_node(tx, source)?;
                if !projection.is_empty() {
                    source = PhysicalProjection::plan(source, projection);
                };
                PhysicalInsert::plan(table_ref, source, returning)
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
        &self,
        tx: &dyn Transaction<'env, S>,
        plan: Box<Plan>,
    ) -> Result<Arc<dyn PhysicalNode<'env, 'txn, S, M>>> {
        self.fold_plan(tx, plan, Self::plan_node)
    }

    #[allow(clippy::boxed_local)]
    fn fold_plan<M: ExecutionMode<'env, S>>(
        &self,
        tx: &dyn Transaction<'env, S>,
        plan: Box<Plan>,
        f: impl FnOnce(
            &Self,
            &dyn Transaction<'env, S>,
            Box<Plan>,
        ) -> Result<Arc<dyn PhysicalNode<'env, 'txn, S, M>>>,
    ) -> Result<Arc<dyn PhysicalNode<'env, 'txn, S, M>>> {
        let plan = match *plan {
            Plan::Transaction(kind) => PhysicalTransaction::plan(kind),
            Plan::Scan { table_ref, projection } => PhysicalTableScan::plan(table_ref, projection),
            Plan::Show(object_type) => PhysicalShow::plan(object_type),
            Plan::Explain(kind, plan) => {
                return self.explain_plan(tx, kind, f(self, tx, plan)?);
            }
            Plan::Values { values } => PhysicalValues::plan(values),
            Plan::Projection { source, projection } => {
                PhysicalProjection::plan(f(self, tx, source)?, projection)
            }
            Plan::Limit { source, limit } => PhysicalLimit::plan(f(self, tx, source)?, limit),
            Plan::Filter { source, predicate } => {
                PhysicalFilter::plan(f(self, tx, source)?, predicate)
            }
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
}
