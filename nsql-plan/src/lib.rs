use nsql_catalog::{Namespace, Oid, Table};

pub enum Plan {
    CreateTable(ir::CreateTableInfo),
    CreateNamespace(ir::CreateNamespaceInfo),
    Project {
        source: Box<Plan>,
        projection: Vec<ir::Expr>,
    },
    Insert {
        namespace: Oid<Namespace>,
        table: Oid<Table>,
        source: Box<Plan>,
        returning: Option<Vec<ir::Expr>>,
    },
    Values {
        values: ir::Values,
    },
    Scan {
        table_ref: ir::TableRef,
    },
    Dummy,
}

#[derive(Default)]
pub struct Planner {}

impl Planner {
    pub fn plan(&self, stmt: ir::Stmt) -> Box<Plan> {
        let plan = match stmt {
            ir::Stmt::CreateTable(info) => Plan::CreateTable(info),
            ir::Stmt::CreateNamespace(info) => Plan::CreateNamespace(info),
            ir::Stmt::Insert { namespace, table, source, returning } => {
                let source = self.plan_table_expr(source);
                Plan::Insert { namespace, table, source, returning }
            }
            ir::Stmt::Query(query) => return self.plan_table_expr(query),
        };

        Box::new(plan)
    }

    fn plan_table_expr(&self, table_expr: ir::TableExpr) -> Box<Plan> {
        let plan = match table_expr {
            ir::TableExpr::Values(values) => Plan::Values { values },
            ir::TableExpr::Selection(sel) => self.plan_select(sel),
            ir::TableExpr::TableRef(table_ref) => Plan::Scan { table_ref },
            ir::TableExpr::Empty => todo!(),
        };

        Box::new(plan)
    }

    fn plan_select(&self, sel: ir::Selection) -> Plan {
        let source = self.plan_table_expr(*sel.source);
        Plan::Project { source, projection: sel.projection }
    }
}
