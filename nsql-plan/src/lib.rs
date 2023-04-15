use nsql_catalog::{Namespace, Oid, Table};
use nsql_ir as ir;

pub enum Plan {
    CreateTable {
        namespace: Oid<Namespace>,
        info: ir::CreateTableInfo,
    },
    Insert {
        namespace: Oid<Namespace>,
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
            ir::Stmt::CreateTable { namespace, info } => Plan::CreateTable { namespace, info },
            ir::Stmt::Insert { namespace, table, source, returning } => {
                let source = self.plan_table_expr(source);
                Plan::Insert { namespace, table, source, returning }
            }
            ir::Stmt::Query(_) => todo!(),
        };

        Box::new(plan)
    }

    fn plan_table_expr(&self, table_expr: ir::TableExpr) -> Box<Plan> {
        let plan = match table_expr {
            ir::TableExpr::Values(values) => Plan::Values { values },
            ir::TableExpr::Selection(sel) => todo!(),
        };

        Box::new(plan)
    }
}
