use nsql_storage::tuple::Tuple;
use nsql_storage::value::Value;

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
        match &expr.kind {
            ir::ExprKind::Value(lit) => match lit {
                ir::Value::Null => Value::Null,
                ir::Value::Bool(b) => Value::Bool(*b),
                ir::Value::Decimal(d) => Value::Decimal(*d),
                ir::Value::Text(s) => Value::Text(s.clone()),
            },
            ir::ExprKind::ColumnRef(idx) => input[*idx].clone(),
        }
    }
}

impl Evaluator {}
