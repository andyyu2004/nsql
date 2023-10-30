#![deny(rust_2018_idioms)]
#![feature(iter_array_chunks, if_let_guard, lazy_cell)]

mod decorrelate;
mod node;
mod rules;
pub mod view;

use std::mem;

pub use egg::Id as NodeId;
use ir::fold::{ExprFold, Folder, PlanFold};
use nsql_core::{LogicalType, Name};
use rustc_hash::FxHashSet;

use self::decorrelate::Decorrelate;
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
        ir::Plan::Explain(opts, query) => ir::Plan::Explain(opts, optimize(query)),
        ir::Plan::Query(query) => ir::Plan::Query(optimize_query(query)),
        ir::Plan::Copy(cp) => ir::Plan::Copy(match cp {
            ir::Copy::To(ir::CopyTo { src, dst }) => {
                ir::Copy::To(ir::CopyTo { src: optimize_query(src), dst })
            }
        }),
    };

    Box::new(optimized)
}

fn optimize_query(mut plan: Box<ir::QueryPlan>) -> Query {
    plan.validate().unwrap_or_else(|err| panic!("invalid plan passed to optimizer: {err}"));

    // loop {
    //     let passes = [
    //         &mut IdentityProjectionElimination as &mut dyn Pass,
    //         &mut EmptyPlanElimination,
    //         &mut Decorrelate,
    //         &mut DeduplicateCtes::default(),
    //     ];
    //     let pre_opt_plan = plan.clone();
    //     for pass in passes {
    //         plan = pass.fold_boxed_query_plan(plan);
    //         plan.validate().unwrap_or_else(|err| {
    //             panic!("invalid plan after pass `{}`: {err}\n{plan:#}", pass.name())
    //         });
    //         tracing::debug!("plan after pass `{}`:\n{:#}", pass.name(), plan);
    //     }

    //     if plan == pre_opt_plan {
    //         break;
    //     }
    // }

    let mut builder = node::Builder::default();
    let root = builder.build(&plan);
    builder.finalize(root)
}

struct IdentityProjectionElimination;

impl Pass for IdentityProjectionElimination {
    fn name(&self) -> &'static str {
        "identity projection removal"
    }
}

impl Folder for IdentityProjectionElimination {
    #[inline]
    fn as_dyn(&mut self) -> &mut dyn Folder {
        self
    }

    fn fold_query_plan(&mut self, plan: ir::QueryPlan) -> ir::QueryPlan {
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

struct EmptyPlanElimination;

impl Pass for EmptyPlanElimination {
    fn name(&self) -> &'static str {
        "empty plan elimination"
    }
}

impl Folder for EmptyPlanElimination {
    #[inline]
    fn as_dyn(&mut self) -> &mut dyn Folder {
        self
    }

    fn fold_query_plan(&mut self, plan: ir::QueryPlan) -> ir::QueryPlan {
        match plan {
            ir::QueryPlan::DummyScan => ir::QueryPlan::DummyScan,
            ir::QueryPlan::Limit { source, limit: 0, exceeded_message: None } => {
                ir::QueryPlan::Empty { schema: source.schema().clone() }
            }
            ir::QueryPlan::Filter { source, predicate }
                if predicate.kind == ir::ExprKind::Literal(false.into()) =>
            {
                ir::QueryPlan::Empty { schema: source.schema().clone() }
            }
            ir::QueryPlan::Projection { source, projection, projected_schema } => {
                let source = self.fold_boxed_query_plan(source);
                if source.is_empty() {
                    ir::QueryPlan::Empty { schema: projected_schema }
                } else {
                    ir::QueryPlan::Projection { source, projection, projected_schema }
                }
            }
            ir::QueryPlan::Join { join, lhs, rhs, schema } => {
                let lhs = self.fold_boxed_query_plan(lhs);
                let rhs = self.fold_boxed_query_plan(rhs);

                // FIXME there's a lot more cases where we can eliminate the join
                if (lhs.is_empty() || rhs.is_empty()) && matches!(join, ir::JoinKind::Inner) {
                    ir::QueryPlan::Empty { schema }
                } else {
                    ir::QueryPlan::Join { join, lhs, rhs, schema }
                }
            }
            _ => plan.fold_with(self),
        }
    }
}

/// With all the rewriting we do, sometimes we end up with the cte node being copied around.
/// This results in the same cte being evaluated multiple times, which is wasteful (and also causes errors as we check for this)
#[derive(Default)]
struct DeduplicateCtes {
    ctes: FxHashSet<Name>,
}

impl Pass for DeduplicateCtes {
    fn name(&self) -> &'static str {
        "cte deduplication"
    }
}

impl Folder for DeduplicateCtes {
    fn as_dyn(&mut self) -> &mut dyn Folder {
        self
    }

    fn fold_query_plan(&mut self, plan: ir::QueryPlan) -> ir::QueryPlan {
        match plan {
            ir::QueryPlan::Cte { cte, child } => {
                if self.ctes.insert(Name::clone(&cte.name)) {
                    ir::QueryPlan::Cte {
                        cte: cte.fold_with(self),
                        child: self.fold_boxed_query_plan(child),
                    }
                } else {
                    *self.fold_boxed_query_plan(child)
                }
            }
            plan => plan.fold_with(self),
        }
    }
}
