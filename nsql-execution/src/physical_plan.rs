mod physical_create_table;
mod physical_insert;
mod physical_values;

use std::sync::Arc;

use nsql_pager::Pager;
use nsql_plan::Plan;

use self::physical_create_table::PhysicalCreateTable;
use self::physical_insert::PhysicalInsert;
use self::physical_values::PhysicalValues;
use crate::{
    Evaluator, ExecutionContext, ExecutionResult, PhysicalNode, PhysicalOperator, PhysicalSink,
    PhysicalSource, Tuple,
};

#[derive(Default)]
pub struct PhysicalPlanner {}

/// Opaque physical plan that is ready to be executed
pub struct PhysicalPlan(Arc<dyn PhysicalNode>);

impl PhysicalPlan {
    pub(crate) fn root(self) -> Arc<dyn PhysicalNode> {
        self.0
    }
}

impl PhysicalPlanner {
    pub fn plan(&self, plan: &Plan) -> PhysicalPlan {
        PhysicalPlan(self.plan_inner(plan))
    }

    fn plan_inner(&self, plan: &Plan) -> Arc<dyn PhysicalNode> {
        match plan {
            Plan::CreateTable { schema, info } => PhysicalCreateTable::make(*schema, info.clone()),
            Plan::Insert { schema, table, source, returning } => {
                let source = self.plan_inner(source);
                PhysicalInsert::make(*schema, *table, source, returning.clone())
            }
            Plan::Values { values } => PhysicalValues::make(values.clone()),
        }
    }
}
