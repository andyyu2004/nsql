use nsql_catalog::{ColumnIndex, CreateNamespaceInfo, Table};
use nsql_core::Oid;

// FIXME not sure how useful this layer is...
#[derive(Debug)]
pub enum Plan {
    Empty,
    Transaction(ir::TransactionStmtKind),
    CreateTable(ir::CreateTableInfo),
    CreateNamespace(CreateNamespaceInfo),
    Drop(Vec<ir::EntityRef>),
    Show(ir::ObjectType),
    Explain(ir::ExplainMode, Box<Plan>),
    Unnest { expr: ir::Expr },
    Update { table: Oid<Table>, source: Box<Plan>, returning: Option<Box<[ir::Expr]>> },
    Filter { source: Box<Plan>, predicate: ir::Expr },
    Projection { source: Box<Plan>, projection: Box<[ir::Expr]> },
    Insert { table: Oid<Table>, source: Box<Plan>, returning: Option<Box<[ir::Expr]>> },
    Values { values: ir::Values },
    Scan { table: Oid<Table>, projection: Option<Box<[ColumnIndex]>> },
    Limit { source: Box<Plan>, limit: u64 },
    Order { source: Box<Plan>, order: Box<[ir::OrderExpr]> },
    Join { lhs: Box<Plan>, rhs: Box<Plan> },
}

#[derive(Debug, Default)]
pub struct Planner {}

impl Planner {
    pub fn plan(&self, stmt: ir::Stmt) -> Box<Plan> {
        let plan = match stmt {
            ir::Stmt::Transaction(kind) => Plan::Transaction(kind),
            ir::Stmt::CreateTable(info) => Plan::CreateTable(info),
            ir::Stmt::CreateNamespace(info) => Plan::CreateNamespace(info),
            ir::Stmt::Insert { table, source, returning } => {
                let source = self.plan_query(source);
                Plan::Insert { table, source, returning }
            }
            ir::Stmt::Query(query) => return self.plan_query(query),
            ir::Stmt::Show(show) => Plan::Show(show),
            ir::Stmt::Drop(refs) => Plan::Drop(refs),
            ir::Stmt::Update { table, source, returning } => {
                Plan::Update { table, source: self.plan_query(source), returning }
            }
            ir::Stmt::Explain(kind, stmt) => Plan::Explain(kind, self.plan(*stmt)),
        };

        Box::new(plan)
    }

    #[allow(clippy::boxed_local)]
    fn plan_query(&self, plan: Box<ir::QueryPlan>) -> Box<Plan> {
        let plan = match *plan {
            ir::QueryPlan::Values { values, .. } => Plan::Values { values },
            ir::QueryPlan::Filter { source, predicate } => {
                let source = self.plan_query(source);
                Plan::Filter { source, predicate }
            }
            ir::QueryPlan::Projection { source, projection, .. } => {
                let source = self.plan_query(source);
                Plan::Projection { source, projection }
            }
            ir::QueryPlan::TableScan { table, projection, .. } => Plan::Scan { table, projection },
            ir::QueryPlan::Empty => Plan::Empty,
            ir::QueryPlan::Limit { source, limit } => {
                let source = self.plan_query(source);
                Plan::Limit { source, limit }
            }
            ir::QueryPlan::Order { source, order } => {
                let source = self.plan_query(source);
                Plan::Order { source, order }
            }
            ir::QueryPlan::Unnest { expr, .. } => Plan::Unnest { expr },
            ir::QueryPlan::Join { lhs, rhs, .. } => {
                Plan::Join { lhs: self.plan_query(lhs), rhs: self.plan_query(rhs) }
            }
        };

        Box::new(plan)
    }
}
