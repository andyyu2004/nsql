use nsql_storage::value::Value;

use crate::{Expr, ExprKind};

pub struct EvalNotConst;

impl Expr {
    #[inline]
    pub fn const_eval(&self) -> Result<Value, EvalNotConst> {
        match &self.kind {
            ExprKind::Literal(val) => Ok(val.clone()),
            ExprKind::UnaryOperator { .. }
            | ExprKind::BinaryOperator { .. }
            | ExprKind::Subquery(_)
            | ExprKind::ColumnRef { .. } => Err(EvalNotConst),
            // we can actually recurse for this case but not necessary for now
            ExprKind::Array(exprs) => exprs
                .iter()
                .map(|expr| expr.const_eval())
                .collect::<Result<_, _>>()
                .map(Value::Array),
            ExprKind::Alias { expr, .. } => expr.const_eval(),
            ExprKind::FunctionCall { .. } | ExprKind::Case { .. } => Err(EvalNotConst),
        }
    }
}
