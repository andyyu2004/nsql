#![deny(rust_2018_idioms)]
#![feature(iter_array_chunks, if_let_guard, lazy_cell)]

mod decorrelate;
mod node;
mod rules;
pub mod view;

use std::mem;

pub use egg::Id as NodeId;
use ir::fold::{ExprFold, Folder, PlanFold};
use nsql_core::LogicalType;

use self::decorrelate::Decorrelator;
pub use self::view::{CallExpr, Expr, Plan, Query};

trait Pass: Folder {
    fn name(&self) -> &'static str;
}

#[allow(clippy::boxed_local)]
pub fn optimize(plan: Box<ir::Plan>) -> Box<ir::Plan<Query>> {
    let optimized = match *plan {
        ir::Plan::Show(show) => ir::Plan::Show(show),
        ir::Plan::Drop(refs) => ir::Plan::Drop(refs),
        ir::Plan::Transaction(txn) => ir::Plan::Transaction(txn),
        ir::Plan::SetVariable { name, value, scope } => {
            ir::Plan::SetVariable { name, value, scope }
        }
        ir::Plan::Explain(query) => ir::Plan::Explain(optimize(query)),
        ir::Plan::Query(query) => ir::Plan::Query(optimize_query(query)),
    };

    Box::new(optimized)
}

fn optimize_query(mut plan: Box<ir::QueryPlan>) -> Query {
    plan.validate().unwrap_or_else(|err| panic!("invalid plan passed to optimizer: {err}"));

    loop {
        let passes = [&mut IdentityProjectionRemover as &mut dyn Pass, &mut Decorrelator];
        let pre_opt_plan = plan.clone();
        for pass in passes {
            plan = pass.fold_boxed_plan(plan);
            plan.validate().unwrap_or_else(|err| {
                panic!("invalid plan after pass `{}`: {err}\n{plan:#}", pass.name())
            });
        }

        if plan == pre_opt_plan {
            break;
        }
    }

    let mut builder = node::Builder::default();
    let root = builder.build(&plan);
    builder.finalize(root)
}

struct IdentityProjectionRemover;

impl Pass for IdentityProjectionRemover {
    fn name(&self) -> &'static str {
        "identity projection removal"
    }
}

impl Folder for IdentityProjectionRemover {
    #[inline]
    fn as_dyn(&mut self) -> &mut dyn Folder {
        self
    }

    fn fold_plan(&mut self, plan: ir::QueryPlan) -> ir::QueryPlan {
        fn is_identity_projection(source_schema: &[LogicalType], projection: &[ir::Expr]) -> bool {
            source_schema.len() == projection.len()
                && projection.iter().enumerate().all(|(i, expr)| match &expr.kind {
                    ir::ExprKind::ColumnRef(ir::ColumnRef { index, .. }) => index.as_usize() == i,
                    _ => false,
                })
        }

        if let ir::QueryPlan::Projection { source, projection, projected_schema } = plan {
            if is_identity_projection(source.schema(), &projection) {
                source.super_fold_with(self)
            } else {
                ir::QueryPlan::Projection { source, projection, projected_schema }.fold_with(self)
            }
        } else {
            plan.fold_with(self)
        }
    }
}
