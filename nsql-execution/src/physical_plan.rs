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
use nsql_catalog::{Catalog, EntityRef};
use nsql_plan::Plan;
use nsql_storage_engine::{StorageEngine, Transaction};

pub use self::explain::Explain;
use self::physical_create_namespace::PhysicalCreateNamespace;
use self::physical_create_table::PhysicalCreateTable;
use self::physical_drop::PhysicalDrop;
use self::physical_dummy_scan::PhysicalDummyScan;
use self::physical_filter::PhysicalFilter;
use self::physical_insert::PhysicalInsert;
use self::physical_limit::PhysicalLimit;
use self::physical_projection::PhysicalProjection;
use self::physical_show::PhysicalShow;
use self::physical_table_scan::PhysicalTableScan;
use self::physical_transaction::PhysicalTransaction;
use self::physical_update::PhysicalUpdate;
use self::physical_values::PhysicalValues;
use crate::{
    Chunk, Evaluator, ExecutionContext, ExecutionMode, ExecutionResult, OperatorState,
    PhysicalNode, PhysicalOperator, PhysicalSink, PhysicalSource, ReadWriteExecutionMode,
    ReadonlyExecutionMode, SourceState, Tuple,
};

pub struct PhysicalPlanner<S> {
    catalog: Arc<Catalog<S>>,
}

/// Opaque physical plan that is ready to be executed
pub struct PhysicalPlan<'env, S, M>(Arc<dyn PhysicalNode<'env, S, M>>);

impl<'env, S, M> PhysicalPlan<'env, S, M> {
    pub(crate) fn root(&self) -> Arc<dyn PhysicalNode<'env, S, M>> {
        Arc::clone(&self.0)
    }
}

impl<'env, S: StorageEngine> PhysicalPlanner<S> {
    pub fn new(catalog: Arc<Catalog<S>>) -> Self {
        Self { catalog }
    }

    #[inline]
    pub fn plan(
        &self,
        tx: &impl Transaction<'_, S>,
        plan: Box<Plan<S>>,
    ) -> Result<PhysicalPlan<'env, S, ReadonlyExecutionMode<S>>> {
        self.plan_node(tx, plan).map(PhysicalPlan)
    }

    #[inline]
    pub fn plan_write(
        &self,
        tx: &impl Transaction<'_, S>,
        plan: Box<Plan<S>>,
    ) -> Result<PhysicalPlan<'env, S, ReadWriteExecutionMode<S>>> {
        self.plan_write_node(tx, plan).map(PhysicalPlan)
    }

    fn plan_write_node(
        &self,
        tx: &impl Transaction<'_, S>,
        plan: Box<Plan<S>>,
    ) -> Result<Arc<dyn PhysicalNode<'env, S, ReadWriteExecutionMode<S>> + 'env>> {
        match *plan {
            Plan::Update { table_ref, source, returning } => {
                let source = self.plan_node(tx, source)?;
                Ok(PhysicalUpdate::plan(table_ref, source, returning))
            }
            Plan::Insert { table_ref, projection, source, returning } => {
                let mut source = self.plan_node(tx, source)?;
                if !projection.is_empty() {
                    source = PhysicalProjection::plan(source, projection);
                };
                Ok(PhysicalInsert::plan(table_ref, source, returning))
            }
            Plan::CreateTable(info) => Ok(PhysicalCreateTable::plan(info)),
            Plan::CreateNamespace(info) => Ok(PhysicalCreateNamespace::plan(info)),
            Plan::Drop(refs) => Ok(PhysicalDrop::plan(refs)),
            _ => self.plan_node(tx, plan),
        }
    }

    #[allow(clippy::boxed_local)]
    fn plan_node<M: ExecutionMode<'env, S>>(
        &self,
        tx: &impl Transaction<'_, S>,
        plan: Box<Plan<S>>,
    ) -> Result<Arc<dyn PhysicalNode<'env, S, M>>> {
        let plan = match *plan {
            Plan::Transaction(kind) => PhysicalTransaction::plan(kind),
            Plan::Scan { table_ref, projection } => PhysicalTableScan::plan(table_ref, projection),
            Plan::Show(show) => PhysicalShow::plan(show),
            Plan::Explain(_kind, _plan) => {
                todo!()
                // let plan = self.plan_node(tx, plan)?;
                // let stringified = match kind {
                //     ir::ExplainMode::Physical => {
                //         todo!()
                //         // explain::explain(&self.catalog, tx, plan.as_ref()).to_string()
                //     }
                //     ir::ExplainMode::Pipeline => {
                //         todo!()
                //         let sink = Arc::new(OutputSink::default());
                //         let pipeline =
                //             crate::build_pipelines(sink, PhysicalPlan(Arc::clone(&plan)));
                //         let disp = explain::explain_pipeline(&self.catalog, tx, &pipeline);
                //         disp.to_string()
                //     }
                // };
                //
                // PhysicalExplain::plan(stringified, plan)
            }
            Plan::Values { values } => PhysicalValues::plan(values),
            Plan::Projection { source, projection } => {
                let source = self.plan_node(tx, source)?;
                PhysicalProjection::plan(source, projection)
            }
            Plan::Limit { source, limit } => {
                PhysicalLimit::plan(self.plan_node(tx, source)?, limit)
            }
            Plan::Filter { source, predicate } => {
                PhysicalFilter::plan(self.plan_node(tx, source)?, predicate)
            }
            Plan::Empty => PhysicalDummyScan::plan(),
            Plan::CreateTable(_)
            | Plan::CreateNamespace(_)
            | Plan::Drop(_)
            | Plan::Update { .. }
            | Plan::Insert { .. } => unreachable!("write plans should go through plan_node_write"),
        };

        Ok(plan)
    }
}
