//! fixme this crate seems a bit useless

use nsql_catalog::ColumnIndex;
use nsql_storage_engine::StorageEngine;

#[derive(Debug)]
pub enum Plan<S> {
    Empty,
    Transaction(ir::TransactionStmtKind),
    CreateTable(ir::CreateTableInfo<S>),
    CreateNamespace(ir::CreateNamespaceInfo),
    Drop(Vec<ir::EntityRef<S>>),
    Show(ir::ObjectType),
    Explain(ir::ExplainMode, Box<Plan<S>>),
    Update {
        table_ref: ir::TableRef<S>,
        source: Box<Plan<S>>,
        returning: Option<Box<[ir::Expr]>>,
    },
    Filter {
        source: Box<Plan<S>>,
        predicate: ir::Expr,
    },
    Projection {
        source: Box<Plan<S>>,
        projection: Box<[ir::Expr]>,
    },
    Insert {
        table_ref: ir::TableRef<S>,
        projection: Box<[ir::Expr]>,
        source: Box<Plan<S>>,
        returning: Option<Box<[ir::Expr]>>,
    },
    Values {
        values: ir::Values,
    },
    Scan {
        table_ref: ir::TableRef<S>,
        projection: Option<Box<[ColumnIndex]>>,
    },
    Limit {
        source: Box<Plan<S>>,
        limit: u64,
    },
}

impl<S> Plan<S> {
    /// Returns true if this plan requires a write transaction to execute
    pub fn requires_write_transaction(&self) -> bool {
        match self {
            Plan::Empty
            | Plan::Transaction(_)
            | Plan::Show(_)
            | Plan::Explain(..)
            | Plan::Scan { .. }
            | Plan::Values { .. } => false,
            Plan::Projection { source, .. }
            | Plan::Limit { source, .. }
            | Plan::Filter { source, .. } => source.requires_write_transaction(),
            Plan::Insert { .. }
            | Plan::Drop(_)
            | Plan::CreateTable(_)
            | Plan::CreateNamespace(_)
            | Plan::Update { .. } => true,
        }
    }
}

pub struct Planner<S> {
    _marker: std::marker::PhantomData<S>,
}

impl<S> Default for Planner<S> {
    fn default() -> Self {
        Self { _marker: std::marker::PhantomData }
    }
}

impl<S: StorageEngine> Planner<S> {
    pub fn plan(&self, stmt: ir::Stmt<S>) -> Box<Plan<S>> {
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
            ir::Stmt::Update { table_ref, source, returning } => {
                Plan::Update { table_ref, source: self.plan_query(source), returning }
            }
            ir::Stmt::Explain(kind, stmt) => Plan::Explain(kind, self.plan(*stmt)),
        };

        Box::new(plan)
    }

    fn plan_query(&self, plan: Box<ir::QueryPlan<S>>) -> Box<Plan<S>> {
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
            ir::QueryPlan::TableRef { table_ref, projection } => {
                Plan::Scan { table_ref, projection }
            }
            ir::QueryPlan::Empty => Plan::Empty,
            ir::QueryPlan::Limit(source, limit) => {
                let source = self.plan_query(source);
                Plan::Limit { source, limit }
            }
        };

        Box::new(plan)
    }
}
