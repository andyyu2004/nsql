use std::mem;

use ir::fold::{ExprFold, Folder, PlanFold};
use nsql_core::LogicalType;
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

                // Create a projection for the columns of the lhs plan so they don't get lost.
                let mut group_by = self.correlated_plan.build_identity_projection().into_vec();
                group_by.extend(original_group_by);

                // We have to special case `COUNT` and `COUNT(*)` (and any other aggregate that has a non-null initial value?).
                // TODO check other cases such as `SUM` and `PRODUCT` and ensure they work properly too.
                // Refer to the `ekshaully` section of the duckdb (unnesting arbitrary subquery) slides.
                let special_aggregate_indices = aggregates
                    .iter()
                    .enumerate()
                    .filter_map(|(i, (f, _))| {
                        [ir::Function::COUNT, ir::Function::COUNT_STAR]
                            .contains(&f.function().oid())
                            .then_some(i)
                    })
                    .collect::<Vec<_>>();

                let requires_left_join = !special_aggregate_indices.is_empty();

                let mut plan = ir::QueryPlan::aggregate(source, group_by, aggregates);

                debug_assert_eq!(correlated_columns.len(), self.correlated_plan.schema().len());
                if requires_left_join {
                    // Left join the delim lhs with the aggregate to get the lost groups back.
                    // Join on the correlated columns as usual
                    let join_predicate = correlated_columns
                        .iter()
                        .enumerate()
                        .map(|(idx, cor)| {
                            ir::Expr::call(
                                ir::MonoFunction::new(
                                    ir::Function::is_not_distinct_from(),
                                    nsql_core::LogicalType::Bool,
                                ),
                                [
                                    // the lhs of the join
                                    ir::Expr::column_ref(
                                        cor.ty.clone(),
                                        cor.col.qpath.clone(),
                                        // the first n columns post-join are the correlated columns, so `idx` is the one we want
                                        ir::TupleIndex::new(idx),
                                    ),
                                    // the rhs of the join
                                    ir::Expr::column_ref(
                                        cor.ty.clone(),
                                        cor.col.qpath.clone(),
                                        // The first n columns of the rhs pre-join are the correlated columns of the aggregate plan.
                                        // Post join we need to just shift it.
                                        ir::TupleIndex::new(idx + correlated_columns.len()),
                                    ),
                                ],
                            )
                        })
                        .reduce(|a, b| {
                            ir::Expr::call(
                                ir::MonoFunction::new(
                                    ir::Function::and(),
                                    nsql_core::LogicalType::Bool,
                                ),
                                [a, b],
                            )
                        })
                        .expect("there is at least one correlated column");

                    let k = plan.schema().len();
                    plan = self
                        // should probably use a CTE instead of recomputing the correlated plan
                        .correlated_plan
                        .clone()
                        .join(ir::JoinKind::Left, plan, join_predicate);

                    // project away all the extra lhs join columns, we just needed it to pad nulls for the missing groups.
                    let mut projection = plan.build_rightmost_k_projection(k);
                    // First we replace the correlated column references with those from the lhs
                    // Those on the RHS could be null due to the left join.
                    for (i, cor) in correlated_columns.iter().enumerate() {
                        projection[i] = ir::Expr::column_ref(
                            cor.ty.clone(),
                            cor.col.qpath.clone(),
                            ir::TupleIndex::new(i),
                        );
                    }

                    // meanwhile we take the opportunity rewrite `COUNT(*)` as `CASE COUNT(*) WHEN NULL THEN 0 ELSE COUNT(*) END`
                    for idx in special_aggregate_indices {
                        // the first `n` columns are the group_by columns, but we want the aggregates.
                        let idx = idx + correlated_columns.len();
                        let expr = mem::take(&mut projection[idx]);
                        projection[idx] = ir::Expr {
                            ty: expr.ty(),
                            kind: ir::ExprKind::Case {
                                scrutinee: Box::new(expr.clone()),
                                cases: [ir::Case {
                                    when: ir::Expr::NULL,
                                    then: ir::Expr::lit(LogicalType::Int64, 0),
                                }]
                                .into(),
                                else_result: Some(Box::new(expr)),
                            },
                        }
                    }
                    plan = plan.project(projection)
                }

                *plan
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
            ir::QueryPlan::Limit { .. } => unimplemented!("limit within correlated subquery"),
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
