use nsql_catalog::{CreateTableInfo, Oid, Schema};
use nsql_ir as ir;

pub enum Plan {
    CreateTable { schema: Oid<Schema>, info: CreateTableInfo },
}

#[derive(Default)]
pub struct Planner {}

impl Planner {
    pub fn plan(&self, stmt: ir::Stmt) -> Plan {
        match stmt {
            ir::Stmt::CreateTable { schema, info } => Plan::CreateTable { schema, info },
        }
    }
}
