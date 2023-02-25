use nsql_ir::Expr;

use crate::{Tuple, Value};

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
            Expr::Literal(lit) => Value::Literal(lit.clone()),
        }
    }
}

impl Evaluator {}
