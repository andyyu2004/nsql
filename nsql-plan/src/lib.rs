#[derive(Debug)]
pub enum Plan {
    Transaction(ir::TransactionKind),
    CreateTable(ir::CreateTableInfo),
    CreateNamespace(ir::CreateNamespaceInfo),
    Projection {
        source: Box<Plan>,
        projection: Vec<ir::Expr>,
    },
    Insert {
        table_ref: ir::TableRef,
        projection: Vec<ir::Expr>,
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
            ir::Stmt::Transaction(kind) => Plan::Transaction(kind),
            ir::Stmt::CreateTable(info) => Plan::CreateTable(info),
            ir::Stmt::CreateNamespace(info) => Plan::CreateNamespace(info),
            ir::Stmt::Insert { table_ref, projection, source, returning } => {
                let source = self.plan_table_expr(source);
                Plan::Insert { table_ref, projection, source, returning }
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
        Plan::Projection { source, projection: sel.projection }
    }
}
