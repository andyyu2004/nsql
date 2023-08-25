#![deny(rust_2018_idioms)]
#![feature(iter_array_chunks, if_let_guard)]

mod node;
pub mod view;

use std::mem;

pub use egg::Id as NodeId;
use ir::fold::{ExprFold, Folder, PlanFold};
use nsql_core::LogicalType;

pub use self::view::{CallExpr, Expr, ExprKind, Plan, Query};

trait Pass: Folder {
    fn name(&self) -> &'static str;
}

#[allow(clippy::boxed_local)]
pub fn optimize(plan: Box<ir::Plan>) -> Box<ir::Plan<Query>> {
    let optimized = match *plan {
        // ir::Plan::Show(_)
        // | ir::Plan::Drop(_)
        // | ir::Plan::Transaction(_)
        // | ir::Plan::CreateNamespace(_)
        // | ir::Plan::CreateTable(_)
        // | ir::Plan::SetVariable { .. } => plan,
        ir::Plan::Show(show) => ir::Plan::Show(show),
        ir::Plan::Drop(refs) => ir::Plan::Drop(refs),
        ir::Plan::Transaction(txn) => ir::Plan::Transaction(txn),
        ir::Plan::CreateNamespace(info) => ir::Plan::CreateNamespace(info),
        ir::Plan::CreateTable(info) => ir::Plan::CreateTable(info),
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
        let passes = [&mut IdentityProjectionRemover as &mut dyn Pass, &mut SubqueryFlattener];
        let pre_opt_plan = plan.clone();
        for pass in passes {
            plan = pass.fold_boxed_plan(plan);
            plan.validate()
                .unwrap_or_else(|err| panic!("invalid plan after pass `{}`: {err}", pass.name()));
        }

        if plan == pre_opt_plan {
            break;
        }
    }

    node::Builder::default().build(&plan)
}

struct SubqueryFlattener;

impl Pass for SubqueryFlattener {
    fn name(&self) -> &'static str {
        "subquery flattening"
    }
}

impl Folder for SubqueryFlattener {
    #[inline]
    fn as_dyn(&mut self) -> &mut dyn Folder {
        self
    }

    fn fold_plan(&mut self, plan: ir::QueryPlan) -> ir::QueryPlan {
        #[derive(Debug)]
        struct Flattener {
            found_subquery: bool,
        }

        impl Folder for Flattener {
            fn as_dyn(&mut self) -> &mut dyn Folder {
                self
            }

            fn fold_plan(&mut self, plan: ir::QueryPlan) -> ir::QueryPlan {
                // we only flatten one layer of the plan at a time, we don't recurse here
                plan
            }

            fn fold_expr(&mut self, plan: &mut ir::QueryPlan, expr: ir::Expr) -> ir::Expr {
                match expr.kind {
                    ir::ExprKind::Subquery(kind, subquery_plan) => match kind {
                        ir::SubqueryKind::Scalar => {
                            self.found_subquery |= true;
                            assert_eq!(subquery_plan.schema().len(), 1);
                            // FIXME we should error if the number of rows exceeds one
                            let subquery_plan = subquery_plan.strict_limit(
                                1,
                                "subquery used as an expression must return at most one row",
                            );
                            let ty = subquery_plan.schema()[0].clone();

                            let i = ir::TupleIndex::new(plan.schema().len());
                            // Replace the parent plan with a join of the former parent plan and the subquery plan.
                            // This will add a new column to the parent plan's schema, which we will then reference
                            *plan = *Box::new(mem::take(plan)).join(ir::Join::Cross, subquery_plan);

                            ir::Expr::new_column_ref(
                                ty,
                                ir::QPath::new("", "__scalar_subquery__"),
                                i,
                            )
                        }
                        ir::SubqueryKind::Exists => {
                            self.found_subquery |= true;
                            let subquery_plan = subquery_plan
                                // add a `limit 1` clause as we only care about existence
                                .limit(1)
                                // add a `COUNT(*)` aggregate over the limit
                                // this will cause the output to be exactly one row of either `0` or `1`
                                .ungrouped_aggregate([(
                                    ir::MonoFunction::new(
                                        ir::Function::count_star(),
                                        LogicalType::Int64,
                                    ),
                                    [].into(),
                                )])
                                // We need to convert the `0 | 1` output to be `false | true` respectively.
                                // We do this by comparing for equality with the constant `1`
                                .project([ir::Expr {
                                    ty: LogicalType::Bool,
                                    kind: ir::ExprKind::BinaryOperator {
                                        operator: ir::MonoOperator::new(
                                            ir::Operator::equal(),
                                            ir::MonoFunction::new(
                                                ir::Function::equal(),
                                                LogicalType::Bool,
                                            ),
                                        ),
                                        lhs: Box::new(ir::Expr {
                                            ty: LogicalType::Int64,
                                            kind: ir::ExprKind::ColumnRef {
                                                index: ir::TupleIndex::new(0),
                                                qpath: ir::QPath::new("", "count()"),
                                            },
                                        }),
                                        rhs: Box::new(ir::Expr {
                                            ty: LogicalType::Int64,
                                            kind: ir::ExprKind::Literal(ir::Value::Int64(1)),
                                        }),
                                    },
                                }]);

                            // again, we replace the parent with a cross join
                            let i = ir::TupleIndex::new(plan.schema().len());
                            *plan = *Box::new(mem::take(plan)).join(ir::Join::Cross, subquery_plan);

                            ir::Expr::new_column_ref(
                                LogicalType::Int64,
                                ir::QPath::new("", "__exists_subquery__"),
                                i,
                            )
                        }
                    },
                    _ => expr.fold_with(self, plan),
                }
            }
        }

        let original_plan_columns = plan.schema().len();
        let mut flattener = Flattener { found_subquery: false };
        // apply the flattener to one layer of the plan
        let plan = plan.fold_with(&mut flattener);

        // then recurse
        let plan = plan.fold_with(self.as_dyn());

        if flattener.found_subquery && plan.schema().len() > original_plan_columns {
            // if the flattener added a column, we need to project it away
            *Box::new(plan).project_leftmost_k(original_plan_columns)
        } else {
            plan
        }
    }
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
                    ir::ExprKind::ColumnRef { index, .. } => index.as_usize() == i,
                    _ => false,
                })
        }

        if let ir::QueryPlan::Projection { source, projection, projected_schema } = plan {
            if is_identity_projection(source.schema(), &projection) {
                source.super_fold_with(self)
            } else {
                ir::QueryPlan::Projection {
                    source: self.fold_boxed_plan(source),
                    projection,
                    projected_schema,
                }
            }
        } else {
            plan.fold_with(self)
        }
    }
}
