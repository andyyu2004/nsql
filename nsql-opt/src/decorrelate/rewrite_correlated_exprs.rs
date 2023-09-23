use ir::fold::{ExprFold, Folder, PlanFold};

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
            // no more dependent columns on the rhs, can replace the (implicit) dependent join with a cross product
            return *Box::new(self.lhs.clone()).cross_join(Box::new(plan));
        }

        for correlated_column in &correlated_columns {
            assert_eq!(
                correlated_column.col.level, 1,
                "unhandled (level > 1) nested correlated expression"
            );
        }

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

        let mut rewriter = CorrelatedColumnRewriter { correlated_columns: &correlated_columns };

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

                let lhs_schema = self.lhs.schema();
                // create a projection for the columns of the lhs plan so they don't get lost
                let mut group_by =
                    self.lhs.build_leftmost_k_projection(lhs_schema.len()).into_vec();
                // append on the rewritten projections
                group_by.extend(original_group_by);

                *ir::QueryPlan::aggregate(source, group_by, aggregates)
            }
            ir::QueryPlan::Projection { source, projection, projected_schema: _ } => {
                let mut source = self.fold_boxed_plan(source);

                let original_projection = rewriter.fold_exprs(&mut source, projection).into_vec();

                let lhs_schema = self.lhs.schema();
                // create a projection for the columns of the lhs plan so they don't get lost
                let mut projection =
                    self.lhs.build_leftmost_k_projection(lhs_schema.len()).into_vec();
                // append on the rewritten projections
                projection.extend(original_projection);

                *ir::QueryPlan::project(source, projection)
            }
            ir::QueryPlan::Filter { source, predicate } => {
                let mut source = self.fold_boxed_plan(source);
                let predicate = rewriter.fold_expr(&mut source, predicate);
                ir::QueryPlan::Filter { source, predicate }
            }
            ir::QueryPlan::Limit { .. } => plan.fold_with(self),
            ir::QueryPlan::Union { schema: _, lhs: _, rhs: _ } => todo!(),
            ir::QueryPlan::Join { schema: _, join: _, lhs: _, rhs: _ } => todo!(),
            ir::QueryPlan::Order { source: _, order: _ } => todo!(),
            ir::QueryPlan::Insert { table: _, source: _, returning: _, schema: _ } => todo!(),
            ir::QueryPlan::Update { table: _, source: _, returning: _, schema: _ } => todo!(),
        }
    }
}
