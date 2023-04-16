use nsql_storage::tuple::{Literal, Tuple, Value};

#[derive(Debug)]
pub(crate) struct Evaluator {}

impl Evaluator {
    pub fn new() -> Self {
        Self {}
    }

    pub fn evaluate(&self, input: &Tuple, exprs: &[ir::Expr]) -> Tuple {
        let values = exprs.iter().map(|expr| self.evaluate_expr(input, expr));
        Tuple::from_iter(values)
    }

    pub fn evaluate_expr(&self, input: &Tuple, expr: &ir::Expr) -> Value {
        match expr {
            ir::Expr::Literal(lit) => match lit {
                ir::Literal::Null => Value::Literal(Literal::Null),
                ir::Literal::Bool(b) => Value::Literal(Literal::Bool(*b)),
                ir::Literal::Decimal(d) => Value::Literal(Literal::Decimal(*d)),
            },
            ir::Expr::ColumnRef(column_ref) => {
                // FIXME obviously wrong impl
                input.values()[0].clone()
            }
        }
    }
}

impl Evaluator {}
