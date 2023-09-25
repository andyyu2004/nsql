use std::marker::PhantomData;

use ir::fold::{ExprFold, Folder};

pub(super) struct PushdownDependentJoin {
    /// The parent plan the subquery is correlated with
    correlated_plan: ir::QueryPlan,
}

impl PushdownDependentJoin {
    pub(super) fn new(correlated_plan: ir::QueryPlan) -> Self {
        debug_assert!(!correlated_plan.is_correlated());
        Self { correlated_plan }
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
            return *Box::new(self.correlated_plan.clone()).cross_join(Box::new(plan));
        }

        for correlated_column in &correlated_columns {
            assert_eq!(
                correlated_column.col.level, 1,
                "unhandled (level > 1) nested correlated expression"
            );
        }

        struct CorrelatedColumnRewriter<'a> {
            shift: usize,
            maybe_need_lifetime_again: PhantomData<&'a ()>,
        }

        impl Folder for CorrelatedColumnRewriter<'_> {
            fn as_dyn(&mut self) -> &mut dyn Folder {
                self
            }

            fn fold_expr(&mut self, plan: &mut ir::QueryPlan, expr: ir::Expr) -> ir::Expr {
                match expr.kind {
                    ir::ExprKind::ColumnRef(col) => {
                        let new_index = if col.is_correlated() {
                            // `shift` is the number of columns of the correlated plan.
                            // We keep those columns on the left so the index is still correct.
                            col.index
                        } else {
                            // The original projections are shifted to the right hence the new index is `old + shift`
                            col.index + self.shift
                        };

                        ir::Expr::column_ref(expr.ty, col.qpath, new_index)
                    }
                    _ => expr.fold_with(self, plan),
                }
            }
        }

        let mut rewriter = CorrelatedColumnRewriter {
            shift: self.correlated_plan.schema().len(),
            maybe_need_lifetime_again: PhantomData,
        };

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
                    .map(|(f, args)| (f, rewriter.fold_exprs(&mut source, args)))
                    .collect::<Box<_>>();

                let original_group_by = rewriter.fold_exprs(&mut source, group_by).into_vec();

                // create a projection for the columns of the lhs plan so they don't get lost
                let mut group_by = self.correlated_plan.build_identity_projection().into_vec();
                group_by.extend(original_group_by);

                let plan = *ir::QueryPlan::aggregate(source, group_by, aggregates);
                plan
            }
            ir::QueryPlan::Projection { source, projection, projected_schema: _ } => {
                let mut source = self.fold_boxed_plan(source);
                let original_projection = rewriter.fold_exprs(&mut source, projection).into_vec();

                // create a projection for the columns of the lhs plan so they don't get lost
                // and append on the rewritten projections
                let mut projection = self.correlated_plan.build_identity_projection().into_vec();
                projection.extend(original_projection);

                *ir::QueryPlan::project(source, projection)
            }
            ir::QueryPlan::Filter { source, predicate } => {
                let mut source = self.fold_boxed_plan(source);
                let predicate = rewriter.fold_expr(&mut source, predicate);
                ir::QueryPlan::Filter { source, predicate }
            }
            ir::QueryPlan::Limit { .. } => todo!(), // naively recursing through limit is not correct for this
            ir::QueryPlan::Union { schema: _, lhs: _, rhs: _ } => todo!(),
            ir::QueryPlan::Join { schema: _, join: _, lhs: _, rhs: _ } => todo!(),
            ir::QueryPlan::Order { source: _, order: _ } => todo!(),
            ir::QueryPlan::Insert { table: _, source: _, returning: _, schema: _ } => todo!(),
            ir::QueryPlan::Update { table: _, source: _, returning: _, schema: _ } => todo!(),
        }
    }
}
