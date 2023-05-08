mod physical_create_namespace;
mod physical_create_table;
mod physical_drop;
mod physical_filter;
mod physical_insert;
mod physical_limit;
mod physical_projection;
mod physical_show;
mod physical_table_scan;
mod physical_transaction;
mod physical_update;
mod physical_values;

use std::sync::Arc;

use anyhow::Result;
use nsql_catalog::{Catalog, Column, Container, EntityRef, Transaction};
use nsql_plan::Plan;
use nsql_storage::tuple::TupleIndex;

use self::physical_create_namespace::PhysicalCreateNamespace;
use self::physical_create_table::PhysicalCreateTable;
use self::physical_drop::PhysicalDrop;
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
    Chunk, Evaluator, ExecutionContext, ExecutionResult, OperatorState, PhysicalNode,
    PhysicalOperator, PhysicalSink, PhysicalSource, Tuple,
};

pub struct PhysicalPlanner {
    tx: Arc<Transaction>,
    catalog: Arc<Catalog>,
}

/// Opaque physical plan that is ready to be executed
#[derive(Debug)]
pub struct PhysicalPlan(Arc<dyn PhysicalNode>);

impl PhysicalPlan {
    pub(crate) fn root(self) -> Arc<dyn PhysicalNode> {
        self.0
    }
}

impl PhysicalPlanner {
    pub fn new(catalog: Arc<Catalog>, tx: Arc<Transaction>) -> Self {
        Self { tx, catalog }
    }

    pub fn plan(&self, plan: Box<Plan>) -> Result<PhysicalPlan> {
        self.plan_node(plan).map(PhysicalPlan)
    }

    fn plan_node(&self, plan: Box<Plan>) -> Result<Arc<dyn PhysicalNode>> {
        let plan = match *plan {
            Plan::Transaction(kind) => PhysicalTransaction::plan(kind),
            Plan::CreateTable(info) => PhysicalCreateTable::plan(info),
            Plan::CreateNamespace(info) => PhysicalCreateNamespace::plan(info),
            Plan::Drop(refs) => PhysicalDrop::plan(refs),
            Plan::Scan { table_ref, projection } => PhysicalTableScan::plan(table_ref, projection),
            Plan::Show(show) => PhysicalShow::plan(show),
            Plan::Insert { table_ref, projection, source, returning } => {
                let mut source = self.plan_node(source)?;
                if !projection.is_empty() {
                    source = PhysicalProjection::plan(source, projection);
                };
                PhysicalInsert::plan(table_ref, source, returning)
            }
            Plan::Values { values } => PhysicalValues::plan(values),
            Plan::Projection { source, projection } => {
                let source = self.plan_node(source)?;
                PhysicalProjection::plan(source, projection)
            }
            Plan::Limit { source, limit } => PhysicalLimit::plan(self.plan_node(source)?, limit),
            Plan::Filter { source, predicate } => {
                PhysicalFilter::plan(self.plan_node(source)?, predicate)
            }
            Plan::Update { table_ref, source, returning } => {
                let source = self.plan_node(source)?;
                PhysicalUpdate::plan(table_ref, source, returning)
            }
        };

        Ok(plan)
    }
}
