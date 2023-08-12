use std::mem;

use ir::fold::{ExprFold, Folder, PlanFold};
use nsql_core::LogicalType;

trait Pass: Folder {
    fn name(&self) -> &'static str;
}

pub fn optimize(mut plan: Box<ir::Plan>) -> Box<ir::Plan> {
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

    plan
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

    fn fold_plan(&mut self, plan: ir::Plan) -> ir::Plan {
        #[derive(Debug)]
        struct Flattener {
            found_subquery: bool,
        }

        impl Folder for Flattener {
            fn as_dyn(&mut self) -> &mut dyn Folder {
                self
            }

            fn fold_plan(&mut self, plan: ir::Plan) -> ir::Plan {
                // we only flatten one layer of the plan at a time, we don't recurse here
                plan
            }

            fn fold_expr(&mut self, plan: &mut ir::Plan, expr: ir::Expr) -> ir::Expr {
                match expr.kind {
                    ir::ExprKind::Subquery(subquery_plan) => {
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

                        ir::Expr::new_column_ref(ty, ir::QPath::new("", "__subquery__"), i)
                    }
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

    fn fold_plan(&mut self, plan: ir::Plan) -> ir::Plan {
        fn is_identity_projection(source_schema: &[LogicalType], projection: &[ir::Expr]) -> bool {
            source_schema.len() == projection.len()
                && projection.iter().enumerate().all(|(i, expr)| match &expr.kind {
                    ir::ExprKind::ColumnRef { index, .. } => index.as_usize() == i,
                    _ => false,
                })
        }

        if let ir::Plan::Projection { source, projection, projected_schema } = plan {
            if is_identity_projection(source.schema(), &projection) {
                source.super_fold_with(self)
            } else {
                ir::Plan::Projection {
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
