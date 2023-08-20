use nsql_storage::value::Value;

use crate::{Expr, ExprKind};

pub struct EvalNotConst;

impl Expr {
    #[inline]
    pub fn const_eval(&self) -> Result<Value, EvalNotConst> {
        match &self.kind {
            ExprKind::Literal(val) => Ok(val.clone()),
            ExprKind::InfixOperator { .. } | ExprKind::Subquery(_) | ExprKind::ColumnRef { .. } => {
                Err(EvalNotConst)
            }
            // we can actually recurse for this case but not necessary for now
            ExprKind::Array(exprs) => exprs
                .iter()
                .map(|expr| expr.const_eval())
                .collect::<Result<_, _>>()
                .map(Value::Array),
            ExprKind::Alias { expr, .. } => expr.const_eval(),
            ExprKind::FunctionCall { .. } | ExprKind::Case { .. } => Err(EvalNotConst),
            ExprKind::UnaryOp { op, expr } => {
                let val = expr.const_eval()?;
                match op {
                    crate::UnaryOp::Neg => match val {
                        Value::Int64(i) => Ok(Value::Int64(-i)),
                        _ => todo!(),
                    },
                }
            }
        }
    }
}
