use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum JoinSide {
    None,
    Left,
    Right,
    Both,
}

impl<'a, 'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    Binder<'a, 'env, 'txn, S, M>
{
    pub(crate) fn bind_join(
        &mut self,
        lhs_scope: &Scope,
        lhs: Box<ir::QueryPlan>,
        join: &ast::Join,
    ) -> Result<(Scope, Box<ir::QueryPlan>)> {
        let (rhs_scope, rhs) = self.bind_table_factor(&Scope::default(), &join.relation)?;

        let scope = lhs_scope.join(&rhs_scope);
        let kind = match join.join_operator {
            ast::JoinOperator::Inner(_) | ast::JoinOperator::CrossJoin => ir::JoinKind::Inner,
            ast::JoinOperator::LeftOuter(_) => ir::JoinKind::Left,
            ast::JoinOperator::RightOuter(_) => ir::JoinKind::Right,
            _ => not_implemented!("{kind}"),
        };

        let plan = match &join.join_operator {
            ast::JoinOperator::CrossJoin | ast::JoinOperator::Inner(ast::JoinConstraint::None) => {
                lhs.cross_join(rhs)
            }
            ast::JoinOperator::Inner(constraint)
            | ast::JoinOperator::LeftOuter(constraint)
            | ast::JoinOperator::RightOuter(constraint)
            | ast::JoinOperator::FullOuter(constraint) => match constraint {
                ast::JoinConstraint::On(predicate) => {
                    let predicate =
                        self.bind_join_predicate(lhs_scope, &rhs_scope, &scope, predicate)?;
                    lhs.join(kind, rhs, predicate)
                }
                ast::JoinConstraint::Using(_) => not_implemented!("using join"),
                ast::JoinConstraint::Natural => not_implemented!("natural join"),
                ast::JoinConstraint::None => {
                    bail!("must provide a join constraint for non-inner joins")
                }
            },
            ast::JoinOperator::LeftSemi(_)
            | ast::JoinOperator::RightSemi(_)
            | ast::JoinOperator::LeftAnti(_)
            | ast::JoinOperator::RightAnti(_)
            | ast::JoinOperator::CrossApply
            | ast::JoinOperator::OuterApply => not_implemented!("unsupported join type"),
        };

        Ok((scope, plan))
    }

    fn expr_join_side(
        &mut self,
        lhs_scope: &Scope,
        rhs_scope: &Scope,
        joint_scope: &Scope, // this must be `lhs_scope.join(rhs_scope)`, computing it once for efficiency
        expr: &ast::Expr,
    ) -> Result<(JoinSide, ir::Expr)> {
        if let Ok(expr) = self.bind_expr(&Scope::default(), expr) {
            return Ok((JoinSide::None, expr));
        } else if let Ok(expr) = self.bind_expr(lhs_scope, expr) {
            return Ok((JoinSide::Left, expr));
        } else if let Ok(expr) = self.bind_expr(rhs_scope, expr) {
            return Ok((JoinSide::Right, expr));
        }

        Ok((JoinSide::Both, self.bind_expr(joint_scope, expr)?))
    }

    fn bind_join_predicate(
        &mut self,
        lhs_scope: &Scope,
        rhs_scope: &Scope,
        joint_scope: &Scope, // this must be `lhs_scope.join(rhs_scope)`, computing it once for efficiency
        expr: &ast::Expr,
    ) -> Result<ir::JoinPredicate> {
        let mut arbitrary_expr = None;
        let mut conditions = vec![];

        match expr {
            ast::Expr::BinaryOp { left, op, right } if *op == ast::BinaryOperator::Eq => {
                let (sa, a) = self.expr_join_side(lhs_scope, rhs_scope, joint_scope, left)?;
                let (sb, b) = self.expr_join_side(lhs_scope, rhs_scope, joint_scope, right)?;
                ensure!(
                    a.ty.is_compat_with(&b.ty),
                    "compare compare types `{}` and `{}` for equality in join",
                    a.ty,
                    b.ty
                );
                let op = Operator::EQ_ANY;
                match (sa, sb) {
                    (JoinSide::None | JoinSide::Left, JoinSide::None | JoinSide::Right) => {
                        conditions.push(ir::JoinCondition { op, lhs: a, rhs: b })
                    }
                    (JoinSide::None | JoinSide::Right, JoinSide::None | JoinSide::Left) => {
                        conditions.push(ir::JoinCondition { op, lhs: b, rhs: a })
                    }
                    _ => {
                        arbitrary_expr = Some(ir::Expr::binop(
                            ir::MonoOperator::new(
                                Operator::equal(),
                                ir::MonoFunction::new(Function::equal(), LogicalType::Bool),
                            ),
                            Box::new(a),
                            Box::new(b),
                        ))
                    }
                };
            }
            expr => arbitrary_expr = Some(self.bind_expr(joint_scope, expr)?),
        };

        Ok(ir::JoinPredicate { conditions: conditions.into_boxed_slice(), arbitrary_expr })
    }
}
