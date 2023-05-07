//! fixme this crate seems a bit useless

#[derive(Debug)]
pub enum Plan {
    Transaction(ir::TransactionKind),
    CreateTable(ir::CreateTableInfo),
    CreateNamespace(ir::CreateNamespaceInfo),
    Drop(Vec<ir::EntityRef>),
    Show(ir::ObjectType),
    Update {
        table_ref: ir::TableRef,
        assignments: Box<[ir::Assignment]>,
        filter: Option<ir::Expr>,
        returning: Option<Box<[ir::Expr]>>,
    },
    Filter {
        source: Box<Plan>,
        predicate: ir::Expr,
    },
    Projection {
        source: Box<Plan>,
        projection: Box<[ir::Expr]>,
    },
    Insert {
        table_ref: ir::TableRef,
        projection: Box<[ir::Expr]>,
        source: Box<Plan>,
        returning: Option<Box<[ir::Expr]>>,
    },
    Values {
        values: ir::Values,
    },
    Scan {
        table_ref: ir::TableRef,
    },
    Limit {
        source: Box<Plan>,
        limit: u64,
    },
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
                let source = self.plan_query(source);
                Plan::Insert { table_ref, projection, source, returning }
            }
            ir::Stmt::Query(query) => return self.plan_query(query),
            ir::Stmt::Show(show) => Plan::Show(show),
            ir::Stmt::Drop(refs) => Plan::Drop(refs),
            ir::Stmt::Update { table_ref, assignments, filter, returning } => {
                Plan::Update { table_ref, assignments, filter, returning }
            }
        };

        Box::new(plan)
    }

    fn plan_query(&self, plan: Box<ir::QueryPlan>) -> Box<Plan> {
        let plan = match *plan {
            ir::QueryPlan::Values(values) => Plan::Values { values },
            ir::QueryPlan::Filter { source, predicate } => {
                let source = self.plan_query(source);
                Plan::Filter { source, predicate }
            }
            ir::QueryPlan::Projection { source, projection } => {
                let source = self.plan_query(source);
                Plan::Projection { source, projection }
            }
            ir::QueryPlan::TableRef { table_ref } => Plan::Scan { table_ref },
            ir::QueryPlan::Empty => todo!(),
            ir::QueryPlan::Limit(source, limit) => {
                let source = self.plan_query(source);
                Plan::Limit { source, limit }
            }
        };

        Box::new(plan)
    }
}
