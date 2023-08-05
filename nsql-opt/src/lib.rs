use ir::fold::{Folder, PlanFold};

pub fn optimize(plan: Box<ir::Plan>) -> Box<ir::Plan> {
    RemoveIdentityProjections.fold_boxed_plan(plan)
}

struct RemoveIdentityProjections;

impl Folder for RemoveIdentityProjections {
    fn fold_plan(&mut self, plan: ir::Plan) -> ir::Plan {
        eprintln!("{plan}");
        if let ir::Plan::Projection { source, projection, projected_schema } = plan {
            let is_identity_projection = source.schema().len() == projection.len()
                && projection.iter().enumerate().all(|(i, expr)| match &expr.kind {
                    ir::ExprKind::ColumnRef { index, .. } => index.as_usize() == i,
                    _ => false,
                });

            if is_identity_projection {
                source.fold_with(self)
            } else {
                ir::Plan::Projection {
                    source: Box::new(source.fold_with(self)),
                    projection,
                    projected_schema,
                }
            }
        } else {
            plan.fold_with(self)
        }
    }
}
