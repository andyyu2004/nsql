use ir::fold::{ExprFold, Folder, PlanFold};
use nsql_core::LogicalType;

trait Pass: Folder {
    fn name(&self) -> &'static str;
}

pub fn optimize(mut plan: Box<ir::Plan>) -> Box<ir::Plan> {
    plan.validate().unwrap_or_else(|err| panic!("invalid plan passed to optimizer: {err}"));

    // loop {
    let passes = [&mut IdentityProjectionRemover as &mut dyn Pass, &mut SubqueryFlattener];
    // let pre_opt_plan = plan.clone();
    for pass in passes {
        plan = pass.fold_boxed_plan(plan);
        println!("plan after pass `{}`:\n{plan}", pass.name());
        plan.validate()
            .unwrap_or_else(|err| panic!("invalid plan after pass `{}`: {err}", pass.name()));
    }

    // if plan == pre_opt_plan {
    //     break;
    // }
    // }

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
            column_count: usize,
            #[allow(clippy::vec_box)]
            subquery_plans: Vec<Box<ir::Plan>>,
        }

        impl Folder for Flattener {
            fn as_dyn(&mut self) -> &mut dyn Folder {
                self
            }

            fn fold_plan(&mut self, plan: ir::Plan) -> ir::Plan {
                // we only flatten one layer of the plan at a time, we don't recurse here
                plan
            }

            fn fold_expr(&mut self, expr: ir::Expr) -> ir::Expr {
                match expr.kind {
                    ir::ExprKind::Subquery(subquery_plan) => {
                        assert_eq!(subquery_plan.schema().len(), 1);
                        let subquery_plan = subquery_plan.limit(1);
                        let ty = subquery_plan.schema()[0].clone();
                        // we will add a new column to the parent plan (via a join) and then reference it
                        let i = ir::TupleIndex::new(self.column_count);
                        self.subquery_plans.push(subquery_plan);
                        ir::Expr::new_column_ref(ty, ir::QPath::new("", "__subquery__"), i)
                    }
                    _ => expr.fold_with(self),
                }
            }
        }

        let mut flattener = Flattener { column_count: plan.schema().len(), subquery_plans: vec![] };
        let mut plan = Box::new(plan.fold_with(&mut flattener));
        assert!(flattener.subquery_plans.len() <= 1, "multiple subqueries not implemented");

        if let Some(subquery_plan) = flattener.subquery_plans.pop() {
            plan = plan.join(ir::Join::Cross, subquery_plan)
        }

        plan.fold_with(self.as_dyn())
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
