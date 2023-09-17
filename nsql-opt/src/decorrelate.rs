mod rewrite_correlated_exprs;

use super::*;
use crate::decorrelate::rewrite_correlated_exprs::*;

pub(crate) struct SubqueryFlattener;

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

    fn fold_plan(&mut self, plan: ir::QueryPlan) -> ir::QueryPlan {
        #[derive(Debug)]
        struct Flattener {
            found_subquery: bool,
        }

        impl Flattener {
            fn flatten_correlated_subquery(
                &mut self,
                plan: &mut ir::QueryPlan,
                kind: ir::SubqueryKind,
                subquery_plan: Box<ir::QueryPlan>,
            ) -> ir::Expr {
                match kind {
                    ir::SubqueryKind::Scalar => {
                        let n = plan.schema().len();
                        *plan = *PushdownDependentJoin { lhs: mem::take(plan) }
                            .fold_boxed_plan(subquery_plan);

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
                        self.found_subquery |= true;
                        assert_eq!(subquery_plan.schema().len(), 1);
                        let ty = subquery_plan.schema()[0].clone();
                        let subquery_plan = subquery_plan
                            // add a `strict limit 1` as the subquery should not return more than one row
                            .strict_limit(
                                1,
                                "subquery used as an expression must return at most one row",
                            )
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
                        self.found_subquery |= true;
                        let subquery_plan = subquery_plan
                            // add a `limit 1` clause as we only care about existence
                            .limit(1)
                            // add a `COUNT(*)` aggregate over the limit
                            // this will cause the output to be exactly one row of either `0` or `1`
                            .ungrouped_aggregate([(
                                ir::MonoFunction::new(
                                    ir::Function::count_star(),
                                    LogicalType::Int64,
                                ),
                                [].into(),
                            )])
                            // We need to convert the `0 | 1` output to be `false | true` respectively.
                            // We do this by comparing for equality with the constant `1`
                            .project([ir::Expr {
                                ty: LogicalType::Bool,
                                kind: ir::ExprKind::BinaryOperator {
                                    operator: ir::MonoOperator::new(
                                        ir::Operator::equal(),
                                        ir::MonoFunction::new(
                                            ir::Function::equal(),
                                            LogicalType::Bool,
                                        ),
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
                        let correlated_columns = subquery_plan.correlated_columns();
                        if correlated_columns.is_empty() {
                            self.flatten_uncorrelated_subquery(plan, kind, subquery_plan)
                        } else {
                            self.flatten_correlated_subquery(plan, kind, subquery_plan)
                        }
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
