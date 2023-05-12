use nsql_storage::value::Value;

use crate::{BinOp, Expr, ExprKind};

pub struct EvalNotConst;

impl Expr {
    #[inline]
    pub fn const_eval(&self) -> Result<Value, EvalNotConst> {
        match &self.kind {
            ExprKind::Value(val) => Ok(val.clone()),
            ExprKind::BinOp { op, lhs, rhs } => {
                let lhs = lhs.const_eval()?;
                let rhs = rhs.const_eval()?;
                match op {
                    BinOp::Add => todo!(),
                    BinOp::Sub => todo!(),
                    BinOp::Mul => todo!(),
                    BinOp::Div => todo!(),
                    BinOp::Mod => todo!(),
                    BinOp::Eq => Ok(Value::Bool(lhs == rhs)),
                    BinOp::Ne => todo!(),
                    BinOp::Lt => todo!(),
                    BinOp::Le => todo!(),
                    BinOp::Gt => todo!(),
                    BinOp::Ge => todo!(),
                    BinOp::And => todo!(),
                    BinOp::Or => todo!(),
                }
            }
            ExprKind::ColumnRef { .. } => Err(EvalNotConst),
        }
    }
}
