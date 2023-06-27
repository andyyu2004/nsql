use std::mem;

use nsql_catalog::SystemEntity;
use nsql_storage::eval::{BinOp, Expr, ExprOp, TupleExpr};

#[derive(Default, Debug)]
pub(crate) struct Compiler {
    ops: Vec<ExprOp>,
}

impl Compiler {
    pub fn compile_many(&mut self, exprs: impl IntoIterator<Item = ir::Expr>) -> TupleExpr {
        TupleExpr::new(exprs.into_iter().map(|expr| self.compile(expr)).collect::<Box<_>>())
    }

    pub fn compile(&mut self, expr: ir::Expr) -> Expr {
        let pretty = expr.to_string();
        assert!(self.ops.is_empty());
        self.build(expr);
        Expr::new(pretty, mem::take(&mut self.ops))
    }

    fn build(&mut self, expr: ir::Expr) {
        match expr.kind {
            ir::ExprKind::Value(value) => self.ops.push(ExprOp::Push(value)),
            ir::ExprKind::Array(exprs) => {
                let len = exprs.len();
                for expr in exprs.into_vec() {
                    self.build(expr);
                }
                self.ops.push(ExprOp::MkArray { len });
            }
            ir::ExprKind::Alias { expr, .. } => self.build(*expr),
            ir::ExprKind::BinOp { op, lhs, rhs } => {
                self.build(*lhs);
                self.build(*rhs);
                let op = match op {
                    ir::BinOp::Eq => BinOp::Eq,
                    _ => todo!(),
                };
                self.ops.push(ExprOp::BinOp { op });
            }
            ir::ExprKind::ColumnRef { index, .. } => self.ops.push(ExprOp::Project { index }),
            ir::ExprKind::FunctionCall { function, args } => {
                for arg in args.into_vec() {
                    self.build(arg);
                }
                self.ops.push(ExprOp::Call { function_oid: function.key().untyped() });
            }
        }
    }
}
