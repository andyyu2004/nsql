use nsql_storage::tuple::Tuple;
use nsql_storage::value::Value;

#[derive(Debug)]
pub(crate) struct Evaluator {}

impl Evaluator {
    pub fn new() -> Self {
        Self {}
    }

    #[inline]
    pub fn evaluate(&self, input: &Tuple, exprs: &[ir::Expr]) -> Tuple {
        let values = exprs.iter().map(|expr| self.evaluate_expr(input, expr));
        Tuple::from_iter(values)
    }

    #[inline]
    pub fn evaluate_expr(&self, input: &Tuple, expr: &ir::Expr) -> Value {
        match &expr.kind {
            ir::ExprKind::Value(value) => value.clone(),
            ir::ExprKind::ColumnRef { index, .. } => input[*index].clone(),
            ir::ExprKind::BinOp { op, lhs, rhs } => {
                let lhs = self.evaluate_expr(input, lhs);
                let rhs = self.evaluate_expr(input, rhs);
                match op {
                    ir::BinOp::Add => todo!(),
                    ir::BinOp::Sub => todo!(),
                    ir::BinOp::Mul => todo!(),
                    ir::BinOp::Div => todo!(),
                    ir::BinOp::Mod => todo!(),
                    ir::BinOp::Eq => Value::Bool(lhs == rhs),
                    ir::BinOp::Ne => todo!(),
                    ir::BinOp::Lt => todo!(),
                    ir::BinOp::Le => todo!(),
                    ir::BinOp::Gt => todo!(),
                    ir::BinOp::Ge => todo!(),
                    ir::BinOp::And => todo!(),
                    ir::BinOp::Or => todo!(),
                }
            }
            ir::ExprKind::Array(exprs) => {
                let values = exprs.iter().map(|expr| self.evaluate_expr(input, expr));
                Value::Array(values.collect())
            }
            ir::ExprKind::FunctionCall { function, args } => {
                let args =
                    args.iter().map(|expr| self.evaluate_expr(input, expr)).collect::<Box<_>>();
                function.call(args)
            }
            ir::ExprKind::Alias { expr, .. } => self.evaluate_expr(input, expr),
        }
    }
}

impl Evaluator {}
