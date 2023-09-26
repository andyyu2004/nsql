use ir::fold::{ExprFold, Folder, PlanFold};
use rustc_hash::FxHashMap;

pub(super) struct PushdownDependentJoin {
    /// The parent plan the subquery is correlated with
    correlated_plan: Box<ir::QueryPlan>,
    correlated_map: FxHashMap<ir::TupleIndex, ir::TupleIndex>,
}

impl PushdownDependentJoin {
    pub(super) fn new(
        correlated_plan: Box<ir::QueryPlan>,
        correlated_map: FxHashMap<ir::TupleIndex, ir::TupleIndex>,
    ) -> Self {
        debug_assert!(!correlated_plan.is_correlated());
        Self { correlated_plan, correlated_map }
    }
}

impl Folder for PushdownDependentJoin {
    fn as_dyn(&mut self) -> &mut dyn Folder {
        self
    }

    fn fold_plan(&mut self, plan: ir::QueryPlan) -> ir::QueryPlan {
        let correlated_columns = plan.correlated_columns();
        if correlated_columns.is_empty() {
            // no more dependent columns on the rhs, can replace the (implicit) dependent join with a cross product
            return *self.correlated_plan.clone().cross_join(Box::new(plan));
        }

        for correlated_column in &correlated_columns {
            assert_eq!(
                correlated_column.col.level, 1,
                "unhandled (level > 1) nested correlated expression"
            );
        }

        match plan {
            ir::QueryPlan::DummyScan
            | ir::QueryPlan::Empty { .. }
            | ir::QueryPlan::CteScan { .. }
            | ir::QueryPlan::Unnest { .. }
            | ir::QueryPlan::TableScan { .. }
            | ir::QueryPlan::Values { .. } => {
                unreachable!("these plans can't contain correlated references")
            }
            ir::QueryPlan::Cte { cte: _, child: _ } => todo!(),
            ir::QueryPlan::Aggregate { aggregates, source, group_by, schema: _ } => {
                let mut source = self.fold_boxed_plan(source);
                let aggregates = aggregates
                    .into_vec()
                    .into_iter()
                    .map(|(f, args)| (f, self.fold_exprs(&mut source, args)))
                    .collect::<Box<_>>();

                let original_group_by = self.fold_exprs(&mut source, group_by).into_vec();

                // create a projection for the columns of the lhs plan so they don't get lost
                let mut group_by = self.correlated_plan.build_identity_projection().into_vec();
                group_by.extend(original_group_by);

                *ir::QueryPlan::aggregate(source, group_by, aggregates)
            }
            ir::QueryPlan::Projection { source, projection, projected_schema: _ } => {
                let mut source = self.fold_boxed_plan(source);

                let original_projection = self.fold_exprs(&mut source, projection).into_vec();

                // create a projection for the columns of the lhs plan so they don't get lost
                // and append on the rewritten projections
                let mut projection = self.correlated_plan.build_identity_projection().into_vec();
                projection.extend(original_projection);

                *ir::QueryPlan::project(source, projection)
            }
            ir::QueryPlan::Filter { .. } => plan.fold_with(self),
            ir::QueryPlan::Limit { .. } => todo!(), // naively recursing through limit is not correct for this
            ir::QueryPlan::Union { schema: _, lhs: _, rhs: _ } => todo!(),
            ir::QueryPlan::Join { schema: _, join: _, lhs: _, rhs: _ } => todo!(),
            ir::QueryPlan::Order { source: _, order: _ } => todo!(),
            ir::QueryPlan::Insert { table: _, source: _, returning: _, schema: _ } => todo!(),
            ir::QueryPlan::Update { table: _, source: _, returning: _, schema: _ } => todo!(),
            ir::QueryPlan::Distinct { source: _ } => todo!(),
        }
    }

    fn fold_expr(&mut self, plan: &mut ir::QueryPlan, expr: ir::Expr) -> ir::Expr {
        match expr.kind {
            ir::ExprKind::ColumnRef(col) => {
                let new_index = if col.is_correlated() {
                    // the correlated indices have shifted due to the projection
                    self.correlated_map[&col.index]
                } else {
                    // The original projections are shifted to the right hence the new index is `old + shift`
                    col.index + self.correlated_plan.schema().len()
                };

                ir::Expr::column_ref(expr.ty, col.qpath, new_index)
            }
            _ => expr.fold_with(self, plan),
        }
    }
}
