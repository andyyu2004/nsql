mod rewrite_correlated_exprs;

use super::*;
use crate::decorrelate::rewrite_correlated_exprs::*;

pub(crate) struct Decorrelator;

impl Pass for Decorrelator {
    fn name(&self) -> &'static str {
        "subquery decorrelation"
    }
}

impl Folder for Decorrelator {
    #[inline]
    fn as_dyn(&mut self) -> &mut dyn Folder {
        self
    }

    fn fold_plan(&mut self, plan: ir::QueryPlan) -> ir::QueryPlan {
        let original_plan_columns = plan.schema().len();
        let mut flattener = Flattener;
        // apply the flattener to one layer of the plan
        let plan = plan.fold_with(&mut flattener);

        // then recurse
        let plan = plan.fold_with(self.as_dyn());

        debug_assert!(plan.schema().len() >= original_plan_columns, "plan lost columns after pass");
        if plan.schema().len() > original_plan_columns {
            // if the flattener added a column, we need to project it away
            *Box::new(plan).project_leftmost_k(original_plan_columns)
        } else {
            plan
        }
    }
}

#[derive(Debug)]
struct Flattener;

impl Flattener {
    fn flatten_correlated_subquery(
        &mut self,
        plan: &mut ir::QueryPlan,
        kind: ir::SubqueryKind,
        subquery_plan: Box<ir::QueryPlan>,
    ) -> ir::Expr {
        // We use the approach from Neumann's `Unnesting Arbitrary Subqueries`.
        // https://btw-2015.informatik.uni-hamburg.de/res/proceedings/Hauptband/Wiss/Neumann-Unnesting_Arbitrary_Querie.pdf
        // Complementary slides from duckdb: https://drive.google.com/file/d/17_sVIwwxFM5RZB5McQZ8dzT8JvOHZuAq/view?pli=1
        // The gist is that a correlated subquery can be initially represented as a dependent join `<plan> dependent-join <subquery-plan>`.
        // Then we can push this dependent join down until there are no more correlated/dependent columns and then we can turn it into an equivalent cross product.
        // This is what the `PushdownDependentJoin` transform implements. We never explicitly create the dependent join node, but only create the cross product.
        match kind {
            ir::SubqueryKind::Scalar => {
                let n = plan.schema().len();
                *plan =
                    *PushdownDependentJoin { lhs: mem::take(plan) }.fold_boxed_plan(subquery_plan);

                assert_eq!(
                    n + 1,
                    plan.schema().len(),
                    "there should be exactly one extra column glued on for a scalar subquery"
                );

                ir::Expr::column_ref(
                    plan.schema()[n].clone(),
                    ir::QPath::new("", "__correlated_scalar__"),
                    ir::TupleIndex::new(n),
                )
            }
            ir::SubqueryKind::Exists => todo!("exists correlated subquery"),
        }
    }

    fn flatten_uncorrelated_subquery(
        &mut self,
        plan: &mut ir::QueryPlan,
        kind: ir::SubqueryKind,
        subquery_plan: Box<ir::QueryPlan>,
    ) -> ir::Expr {
        match kind {
            ir::SubqueryKind::Scalar => {
                assert_eq!(subquery_plan.schema().len(), 1);
                let ty = subquery_plan.schema()[0].clone();
                let subquery_plan = subquery_plan
                    // add a `strict limit 1` as the subquery should not return more than one row
                    .strict_limit(1, "subquery used as an expression must return at most one row")
                    // add a `FIRST(#0)` aggregate over the limit for when it returns no rows, we want to return `NULL` in that case
                    .ungrouped_aggregate([(
                        ir::MonoFunction::new(ir::Function::first(), ty.clone()),
                        [ir::Expr::column_ref(
                            ty.clone(),
                            ir::QPath::new("", "__scalar_subquery__"),
                            ir::TupleIndex::new(0), // we know the subquery only has one column
                        )]
                        .into(),
                    )]);

                let i = ir::TupleIndex::new(plan.schema().len());
                // Replace the parent plan with a join of the former parent plan and the subquery plan.
                // This will add a new column to the parent plan's schema, which we will then reference
                *plan = *Box::new(mem::take(plan)).cross_join(subquery_plan);

                ir::Expr::column_ref(ty, ir::QPath::new("", "__scalar_subquery__"), i)
            }
            ir::SubqueryKind::Exists => {
                let subquery_plan = subquery_plan
                    // add a `limit 1` clause as we only care about existence
                    .limit(1)
                    // add a `COUNT(*)` aggregate over the limit
                    // this will cause the output to be exactly one row of either `0` or `1`
                    .ungrouped_aggregate([(
                        ir::MonoFunction::new(ir::Function::count_star(), LogicalType::Int64),
                        [].into(),
                    )])
                    // We need to convert the `0 | 1` output to be `false | true` respectively.
                    // We do this by comparing for equality with the constant `1`
                    .project([ir::Expr {
                        ty: LogicalType::Bool,
                        kind: ir::ExprKind::BinaryOperator {
                            operator: ir::MonoOperator::new(
                                ir::Operator::equal(),
                                ir::MonoFunction::new(ir::Function::equal(), LogicalType::Bool),
                            ),
                            lhs: Box::new(ir::Expr::column_ref(
                                LogicalType::Int64,
                                ir::QPath::new("", "count()"),
                                ir::TupleIndex::new(0),
                            )),
                            rhs: Box::new(ir::Expr {
                                ty: LogicalType::Int64,
                                kind: ir::ExprKind::Literal(ir::Value::Int64(1)),
                            }),
                        },
                    }]);

                // again, we replace the parent with a cross join
                let i = ir::TupleIndex::new(plan.schema().len());
                *plan = *Box::new(mem::take(plan)).cross_join(subquery_plan);

                ir::Expr::column_ref(
                    LogicalType::Int64,
                    ir::QPath::new("", "__exists_subquery__"),
                    i,
                )
            }
        }
    }
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
            ir::ExprKind::Subquery(kind, subquery_plan) => {
                if subquery_plan.is_correlated() {
                    self.flatten_correlated_subquery(plan, kind, subquery_plan)
                } else {
                    self.flatten_uncorrelated_subquery(plan, kind, subquery_plan)
                }
            }
            _ => expr.fold_with(self, plan),
        }
    }
}
