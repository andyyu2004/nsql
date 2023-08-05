use ir::fold::{Folder, PlanFold};
use nsql_core::LogicalType;

pub fn optimize(plan: Box<ir::Plan>) -> Box<ir::Plan> {
    RemoveIdentityProjections.fold_boxed_plan(plan)
}

struct RemoveIdentityProjections;

fn is_identity_projection(source_schema: &[LogicalType], projection: &[ir::Expr]) -> bool {
    source_schema.len() == projection.len()
        && projection.iter().enumerate().all(|(i, expr)| match &expr.kind {
            ir::ExprKind::ColumnRef { index, .. } => index.as_usize() == i,
            _ => false,
        })
}

impl Folder for RemoveIdentityProjections {
    fn fold_plan(&mut self, plan: ir::Plan) -> ir::Plan {
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
