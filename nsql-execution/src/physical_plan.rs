mod physical_create_table;

use nsql_plan::Plan;

use self::physical_create_table::PhysicalCreateTable;
use crate::PhysicalNode;

#[derive(Default)]
pub struct PhysicalPlanner {}

impl PhysicalPlanner {
    pub fn plan(&self, plan: Plan) -> PhysicalNode {
        match plan {
            Plan::CreateTable { schema, info } => PhysicalCreateTable::make(schema, info),
        }
    }
}
