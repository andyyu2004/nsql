mod physical_create_table;
mod physical_insert;
mod physical_values;

use std::sync::Arc;

use nsql_buffer::Pool;
use nsql_plan::Plan;

use self::physical_create_table::PhysicalCreateTable;
use self::physical_insert::PhysicalInsert;
use self::physical_values::PhysicalValues;
use crate::{
    Evaluator, ExecutionContext, ExecutionResult, PhysicalNode, PhysicalOperator, PhysicalSink,
    PhysicalSource, Tuple,
};

pub struct PhysicalPlanner {
    pool: Arc<dyn Pool>,
}

/// Opaque physical plan that is ready to be executed
pub struct PhysicalPlan(Arc<dyn PhysicalNode>);

impl PhysicalPlan {
    pub(crate) fn root(self) -> Arc<dyn PhysicalNode> {
        self.0
    }
}

impl PhysicalPlanner {
    pub fn new(pool: Arc<dyn Pool>) -> Self {
        Self { pool }
    }

    pub fn plan(&self, plan: &Plan) -> PhysicalPlan {
        PhysicalPlan(self.plan_inner(plan))
    }

    fn plan_inner(&self, plan: &Plan) -> Arc<dyn PhysicalNode> {
        match plan {
            Plan::CreateTable { namespace, info } => {
                PhysicalCreateTable::make(Arc::clone(&self.pool), *namespace, info.clone())
            }
            Plan::Insert { namespace, table, source, returning } => {
                let source = self.plan_inner(source);
                PhysicalInsert::make(*namespace, *table, source, returning.clone())
            }
            Plan::Values { values } => PhysicalValues::make(values.clone()),
        }
    }
}
