mod rewrite_correlated_exprs;

use std::hash::{Hash, Hasher};

use nsql_core::Name;
use rustc_hash::{FxHashMap, FxHasher};

use super::*;
use crate::decorrelate::rewrite_correlated_exprs::*;

pub(crate) struct Decorrelate;

impl Pass for Decorrelate {
    fn name(&self) -> &'static str {
        "subquery decorrelation"
    }
}

impl Folder for Decorrelate {
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
    // We use the approach from Neumann's `Unnesting Arbitrary Subqueries`.
    // https://btw-2015.informatik.uni-hamburg.de/res/proceedings/Hauptband/Wiss/Neumann-Unnesting_Arbitrary_Querie.pdf
    // Complementary slides from duckdb: https://drive.google.com/file/d/17_sVIwwxFM5RZB5McQZ8dzT8JvOHZuAq/view?pli=1
    // The gist is that a correlated subquery can be initially represented as a dependent join `<plan> dependent-join <subquery-plan>`.
    // Then we can push this dependent join down until there are no more correlated/dependent columns and then we can turn it into an equivalent cross product.
    // This is what the `PushdownDependentJoin` transform implements. We never explicitly create the dependent join node, but only create the cross product.
    fn flatten_correlated_subquery(
        &mut self,
        plan: &mut ir::QueryPlan,
        kind: ir::SubqueryKind,
        subquery_plan: Box<ir::QueryPlan>,
    ) -> ir::Expr {
        debug_assert!(subquery_plan.is_correlated());
        let correlated_plan = Box::new(mem::take(plan));
        let correlated_columns = subquery_plan.correlated_columns();
        // mapping from the old correlated column index to the new correlated column index (post-projection)
        let mut correlated_map = FxHashMap::default();
        let correlated_projection = correlated_columns
            .iter()
            .inspect(|cor| assert!(cor.col.level == 1, "unhandled nested correlated expression"))
            .enumerate()
            .map(|(i, cor)| {
                correlated_map.insert(cor.col.index, ir::TupleIndex::new(i));
                ir::Expr::column_ref(cor.ty.clone(), cor.col.qpath.clone(), cor.col.index)
            })
            .collect::<Vec<_>>();

        // We only need to compute the subquery once per unique combination of correlated columns.
        let delim_correlated_plan =
            correlated_plan.clone().project(correlated_projection).distinct();

        fn hash<T: Hash>(t: &T) -> u64 {
            let mut s = FxHasher::default();
            t.hash(&mut s);
            s.finish()
        }

        // generating a unique name by hashing the correlated plan
        // FIXME each plan should probably get assigned an id so we don't need this hack
        let delim_scan_name =
            Name::from(format!("$__delim_scan_{:x}", hash(&delim_correlated_plan)));

        let delim_scan = Box::new(ir::QueryPlan::CteScan {
            name: Name::clone(&delim_scan_name),
            schema: delim_correlated_plan.schema().clone(),
        });

        let magic = PushdownDependentJoin::new(delim_scan, correlated_map.clone())
            .fold_boxed_plan(subquery_plan);

        let shift = correlated_plan.schema().len();
        // join the delim rhs back with the original plan on the correlated columns (see the slides for details)
        let join_predicate = correlated_columns
            .iter()
            .map(|cor| {
                ir::Expr::call(
                    // we need an `is not distinct from` join, (then we can add the distinct up top back)
                    ir::MonoFunction::new(ir::Function::is_not_distinct_from(), LogicalType::Bool),
                    [
                        // the column in the lhs of the join
                        ir::Expr::column_ref(cor.ty.clone(), cor.col.qpath.clone(), cor.col.index),
                        // the column belonging on the rhs of the join
                        ir::Expr::column_ref(
                            cor.ty.clone(),
                            cor.col.qpath.clone(),
                            correlated_map[&cor.col.index] + shift,
                        ),
                    ],
                )
            })
            .reduce(|a, b| {
                ir::Expr::call(
                    ir::MonoFunction::new(ir::Function::and(), LogicalType::Bool),
                    [a, b],
                )
            })
            .expect("there is at least one correlated column");

        let join_kind = match kind {
            ir::SubqueryKind::Exists => ir::JoinKind::Mark,
            ir::SubqueryKind::Scalar => ir::JoinKind::Single,
        };

        *plan = *correlated_plan
            .join(join_kind, magic, join_predicate)
            .with_cte(ir::Cte { name: delim_scan_name, plan: delim_correlated_plan });

        // TODO is the last column always the correct one?
        let idx = plan.schema().len() - 1;
        ir::Expr::column_ref(
            plan.schema()[idx].clone(),
            ir::QPath::new("", "__correlated_scalar__"),
            ir::TupleIndex::new(idx),
        )
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
