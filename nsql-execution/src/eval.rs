use ir::Expr;
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
                ir::Literal::Null => Value::Literal(Literal::Null),
                ir::Literal::Bool(b) => Value::Literal(Literal::Bool(*b)),
                ir::Literal::Decimal(d) => Value::Literal(Literal::Decimal(*d)),
            },
            Expr::ColumnRef { .. } => todo!(),
        }
    }
}

impl Evaluator {}
