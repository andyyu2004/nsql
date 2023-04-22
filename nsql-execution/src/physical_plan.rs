mod physical_create_namespace;
mod physical_create_table;
mod physical_insert;
mod physical_projection;
mod physical_table_scan;
mod physical_transaction;
mod physical_values;

use std::sync::Arc;

use nsql_plan::Plan;

use self::physical_create_namespace::PhysicalCreateNamespace;
use self::physical_create_table::PhysicalCreateTable;
use self::physical_insert::PhysicalInsert;
use self::physical_projection::PhysicalProjection;
use self::physical_table_scan::PhysicalTableScan;
use self::physical_transaction::PhysicalTransaction;
use self::physical_values::PhysicalValues;
use crate::{
    Chunk, Evaluator, ExecutionContext, ExecutionResult, PhysicalNode, PhysicalOperator,
    PhysicalSink, PhysicalSource, Tuple,
};

pub struct PhysicalPlanner {}

/// Opaque physical plan that is ready to be executed
#[derive(Debug)]
pub struct PhysicalPlan(Arc<dyn PhysicalNode>);

impl PhysicalPlan {
    pub(crate) fn root(self) -> Arc<dyn PhysicalNode> {
        self.0
    }
}

impl PhysicalPlanner {
    pub fn new() -> Self {
        Self {}
    }

    pub fn plan(&self, plan: Box<Plan>) -> PhysicalPlan {
        PhysicalPlan(self.plan_node(plan))
    }

    fn plan_node(&self, plan: Box<Plan>) -> Arc<dyn PhysicalNode> {
        match *plan {
            Plan::Transaction(kind) => PhysicalTransaction::plan(kind),
            Plan::CreateTable(info) => PhysicalCreateTable::plan(info),
            Plan::CreateNamespace(info) => PhysicalCreateNamespace::plan(info),
            Plan::Insert { table_ref, projection, source, returning } => {
                let mut source = self.plan_node(source);
                if !projection.is_empty() {
                    source = PhysicalProjection::plan(source, projection)
                };
                PhysicalInsert::plan(table_ref, source, returning)
            }
            Plan::Values { values } => PhysicalValues::plan(values),
            Plan::Projection { source, projection } => {
                let source = self.plan_node(source);
                PhysicalProjection::plan(source, projection)
            }
            Plan::Scan { table_ref } => PhysicalTableScan::plan(table_ref),
            Plan::Dummy => todo!(),
        }
    }
}
