use ir::fold::{ExprFold, Folder};

pub(super) struct PushdownDependentJoin {
    pub(super) lhs: ir::QueryPlan,
}

impl Folder for PushdownDependentJoin {
    fn as_dyn(&mut self) -> &mut dyn Folder {
        self
    }

    fn fold_plan(&mut self, plan: ir::QueryPlan) -> ir::QueryPlan {
        let correlated_columns = plan.correlated_columns();
        if correlated_columns.is_empty() {
            // no more dependent columns on the rhs, can replace dependent join with cross product
            return *Box::new(self.lhs.clone()).cross_join(Box::new(plan));
        }

        for correlated_column in &correlated_columns {
            assert_eq!(
                correlated_column.col.level, 1,
                "unhandled (level > 1) nested correlated expression"
            );
        }

        match plan {
            ir::QueryPlan::DummyScan | ir::QueryPlan::CteScan { .. } => {
                unreachable!("these plans can't have correlated references")
            }
            ir::QueryPlan::Cte { cte: _, child: _ } => todo!(),
            ir::QueryPlan::Aggregate { aggregates: _, source: _, group_by: _, schema: _ } => {
                todo!()
            }
            ir::QueryPlan::TableScan { table: _, projection: _, projected_schema: _ } => todo!(),
            ir::QueryPlan::Projection { source, projection, projected_schema: _ } => {
                let mut source = self.fold_boxed_plan(source);

                struct CorrelatedColumnRewriter<'a> {
                    correlated_columns: &'a [ir::CorrelatedColumn],
                }

                impl Folder for CorrelatedColumnRewriter<'_> {
                    fn as_dyn(&mut self) -> &mut dyn Folder {
                        self
                    }

                    fn fold_expr(&mut self, plan: &mut ir::QueryPlan, expr: ir::Expr) -> ir::Expr {
                        match expr.kind {
                            ir::ExprKind::ColumnRef(col) if col.is_correlated() => {
                                // FIXME don't think this is quite the right computation to get the index
                                let index = self
                                    .correlated_columns
                                    .iter()
                                    .position(|cor| cor.col == col)
                                    .expect("there should be a match");

                                ir::Expr::column_ref(expr.ty, col.qpath, ir::TupleIndex::new(index))
                            }
                            _ => expr.fold_with(self, plan),
                        }
                    }
                }

                let mut rewriter =
                    CorrelatedColumnRewriter { correlated_columns: &correlated_columns };
                let original_projection = rewriter.fold_exprs(&mut source, projection).into_vec();

                let lhs_schema = self.lhs.schema();
                let mut projection =
                    self.lhs.build_leftmost_k_projection(lhs_schema.len()).into_vec();
                projection.extend(original_projection);

                let projected_schema = projection.iter().map(|expr| expr.ty()).collect();
                ir::QueryPlan::Projection {
                    source,
                    projection: projection.into_boxed_slice(),
                    projected_schema,
                }
            }
            ir::QueryPlan::Filter { source: _, predicate: _ } => todo!(),
            ir::QueryPlan::Unnest { schema: _, expr: _ } => todo!(),
            ir::QueryPlan::Values { schema: _, values: _ } => todo!(),
            ir::QueryPlan::Union { schema: _, lhs: _, rhs: _ } => todo!(),
            ir::QueryPlan::Join { schema: _, join: _, lhs: _, rhs: _ } => todo!(),
            ir::QueryPlan::Limit { source: _, limit: _, exceeded_message: _ } => todo!(),
            ir::QueryPlan::Order { source: _, order: _ } => todo!(),
            ir::QueryPlan::Insert { table: _, source: _, returning: _, schema: _ } => todo!(),
            ir::QueryPlan::Update { table: _, source: _, returning: _, schema: _ } => todo!(),
        }
    }
}
