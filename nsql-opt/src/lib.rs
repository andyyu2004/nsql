use ir::fold::{ExprFold, Folder, PlanFold};
use nsql_core::LogicalType;

pub fn optimize(mut plan: Box<ir::Plan>) -> Box<ir::Plan> {
    println!("{}", plan);
    loop {
        let passes = [&mut IdentityProjectionRemover as &mut dyn Folder, &mut Flattener];
        let pre_opt_plan = plan.clone();
        for pass in passes {
            plan = pass.fold_boxed_plan(plan);
        }

        if plan == pre_opt_plan {
            break;
        }
    }

    plan
}

struct Flattener;

impl Folder for Flattener {
    #[inline]
    fn as_dyn(&mut self) -> &mut dyn Folder {
        self
    }

    fn fold_expr(&mut self, expr: ir::Expr) -> ir::Expr {
        match expr.kind {
            ir::ExprKind::Subquery(plan) => {
                todo!()
            }
            _ => expr.fold_with(self),
        }
    }
}

struct IdentityProjectionRemover;

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
