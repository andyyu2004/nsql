use nsql_catalog::{Oid, Schema, Table};
use nsql_ir as ir;

pub enum Plan {
    CreateTable {
        schema: Oid<Schema>,
        info: ir::CreateTableInfo,
    },
    Insert {
        schema: Oid<Schema>,
        table: Oid<Table>,
        source: Box<Plan>,
        returning: Option<Vec<ir::Expr>>,
    },
    Values {
        values: nsql_ir::Values,
    },
}

#[derive(Default)]
pub struct Planner {}

impl Planner {
    pub fn plan(&self, stmt: ir::Stmt) -> Box<Plan> {
        let plan = match stmt {
            ir::Stmt::CreateTable { schema, info } => Plan::CreateTable { schema, info },
            ir::Stmt::Insert { schema, table, source, returning } => {
                let source = self.plan_table_expr(source);
                Plan::Insert { schema, table, source, returning }
            }
        };

        Box::new(plan)
    }

    fn plan_table_expr(&self, table_expr: ir::TableExpr) -> Box<Plan> {
        let plan = match table_expr {
            ir::TableExpr::Values(values) => Plan::Values { values },
        };

        Box::new(plan)
    }
}
