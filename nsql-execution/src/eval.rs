use nsql_ir::Expr;
use nsql_storage::tuple::{Literal, Tuple, Value};

pub(crate) struct Evaluator {}

impl Evaluator {
    pub fn new() -> Self {
        Self {}
    }

    pub fn evaluate(&self, exprs: &[Expr]) -> Tuple {
        let values = exprs.iter().map(|expr| self.evaluate_expr(expr));
        Tuple::from_iter(values)
    }

    pub fn evaluate_expr(&self, expr: &Expr) -> Value {
        match expr {
            Expr::Literal(lit) => match lit {
                nsql_ir::Literal::Null => Value::Literal(Literal::Null),
                nsql_ir::Literal::Bool(b) => Value::Literal(Literal::Bool(*b)),
                nsql_ir::Literal::Decimal(d) => Value::Literal(Literal::Decimal(*d)),
            },
            Expr::ColumnRef { .. } => todo!(),
        }
    }
}

impl Evaluator {}
