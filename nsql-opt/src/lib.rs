use ir::visit::VisitorMut;

pub fn optimize_in_place(plan: &mut ir::Plan) {
    // RemoveIdentityProjections.visit_plan_mut(plan);
}

struct RemoveIdentityProjections;

impl VisitorMut for RemoveIdentityProjections {
    fn visit_plan_mut(&self, plan: &mut ir::Plan) {
        if let ir::Plan::Projection { source, projection, .. } = plan {
            let is_identity_projection = source.schema().len() == projection.len()
                && projection.iter().enumerate().all(|(i, expr)| match &expr.kind {
                    ir::ExprKind::ColumnRef { index, .. } => index.as_usize() == i,
                    _ => false,
                });

            if is_identity_projection {
                *plan = source.take();
                self.visit_plan_mut(plan);
            }
        }

        self.walk_plan_mut(plan);
    }
}
