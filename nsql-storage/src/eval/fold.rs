use super::*;

impl<F> TupleExpr<F> {
    pub fn fold<G>(self, f: impl Fn(F) -> Result<G> + Copy) -> Result<TupleExpr<G>> {
        self.exprs
            .into_vec()
            .into_iter()
            .map(|expr| expr.fold(f))
            .collect::<Result<Box<_>, _>>()
            .map(TupleExpr::new)
    }
}

impl<F> Expr<F> {
    pub fn fold<G>(self, f: impl Fn(F) -> Result<G> + Copy) -> Result<Expr<G>> {
        Ok(Expr::new(
            self.pretty,
            self.ops.into_vec().into_iter().map(|op| op.fold(f)).collect::<Result<Box<_>, _>>()?,
        ))
    }
}

impl<F> ExprOp<F> {
    pub fn fold<G>(self, f: impl Fn(F) -> Result<G>) -> Result<ExprOp<G>> {
        match self {
            ExprOp::Push(value) => Ok(ExprOp::Push(value)),
            ExprOp::Project { index } => Ok(ExprOp::Project { index }),
            ExprOp::MkArray { len } => Ok(ExprOp::MkArray { len }),
            ExprOp::Call { function } => Ok(ExprOp::Call { function: f(function)? }),
            ExprOp::BinOp { op } => Ok(ExprOp::BinOp { op }),
        }
    }
}
