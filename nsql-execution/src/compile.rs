use std::mem;

use nsql_storage::eval::{
    BinOp, ExecutableExpr, ExecutableExprOp, ExecutableTupleExpr, Expr, ExprOp, TupleExpr,
};

#[derive(Default, Debug)]
pub(crate) struct Compiler {
    ops: Vec<ExecutableExprOp>,
}

impl Compiler {
    pub fn compile_many(
        &mut self,
        exprs: impl IntoIterator<Item = ir::Expr>,
    ) -> ExecutableTupleExpr {
        TupleExpr::new(exprs.into_iter().map(|expr| self.compile(expr)).collect::<Box<_>>())
    }

    pub fn compile(&mut self, expr: ir::Expr) -> ExecutableExpr {
        let pretty = expr.to_string();
        assert!(self.ops.is_empty());
        self.build(expr);
        self.emit(ExprOp::Return);
        Expr::new(pretty, mem::take(&mut self.ops))
    }

    fn emit(&mut self, op: ExecutableExprOp) {
        self.ops.push(op);
    }

    fn build(&mut self, expr: ir::Expr) {
        match expr.kind {
            ir::ExprKind::Literal(value) => self.ops.push(ExprOp::Push(value)),
            ir::ExprKind::Array(exprs) => {
                let len = exprs.len();
                for expr in exprs.into_vec() {
                    self.build(expr);
                }
                self.emit(ExprOp::MkArray { len });
            }
            ir::ExprKind::Alias { expr, .. } => self.build(*expr),
            ir::ExprKind::BinOp { op, lhs, rhs } => {
                self.build(*lhs);
                self.build(*rhs);
                let op = match op {
                    ir::BinOp::Eq => BinOp::Eq,
                    ir::BinOp::Plus => BinOp::Plus,
                    _ => todo!(),
                };
                self.emit(ExprOp::BinOp { op });
            }
            ir::ExprKind::ColumnRef { index, .. } => self.ops.push(ExprOp::Project { index }),
            ir::ExprKind::FunctionCall { function, args } => {
                for arg in args.into_vec() {
                    self.build(arg);
                }
                self.emit(ExprOp::Call { function: Box::new(function) });
            }
            ir::ExprKind::Case { scrutinee, cases, else_result } => {
                debug_assert!(!cases.is_empty());
                let scrutinee = *scrutinee;
                let mut next_branch_marker: Option<JumpMarker> = None;
                let mut end_markers = Vec::with_capacity(cases.len());

                for case in cases.into_vec() {
                    // if there is a marker for the previous branch to backpatch, backpatch it
                    if let Some(marker) = next_branch_marker.take() {
                        marker.backpatch(self);
                    }

                    // FIXME should this be evaluated once or once per branch?
                    // probably once in total (so might need a dup instruction rather than rebuilding it every time)
                    // push the scrutinee onto the stack
                    self.build(scrutinee.clone());
                    // push the comparison expression onto the stack
                    self.build(case.when);
                    // if the comparison is false, jump to the the branch
                    next_branch_marker = Some(self.emit_jmp(|offset| ExprOp::IfNeJmp { offset }));
                    // otherwise, build the then expression
                    self.build(case.then);
                    // and jump to the end of the case
                    end_markers.push(self.emit_jmp(|offset| ExprOp::Jmp { offset }));
                }

                // if all branches fail, jump to the else branch
                next_branch_marker
                    .take()
                    .expect("this should exist as cases are non-empty")
                    .backpatch(self);

                // the else branch is optional, and defaults to null
                match *else_result {
                    Some(else_result) => self.build(else_result),
                    None => self.emit(ExprOp::Push(ir::Value::Null)),
                }

                // backpatch all the end markers
                for marker in end_markers {
                    marker.backpatch(self);
                }
            }
        }
    }

    fn emit_jmp(&mut self, mk_jmp: fn(u32) -> ExecutableExprOp) -> JumpMarker {
        JumpMarker::new(self, mk_jmp)
    }
}

struct JumpMarker {
    /// The offset of the jump instruction to backpatch
    offset: usize,
    mk_jmp: fn(u32) -> ExecutableExprOp,
}

impl JumpMarker {
    fn new(compiler: &mut Compiler, mk_jmp: fn(u32) -> ExecutableExprOp) -> Self {
        let offset = compiler.ops.len();
        compiler.emit(mk_jmp(u32::MAX));
        JumpMarker { offset, mk_jmp }
    }

    fn backpatch(self, compiler: &mut Compiler) {
        let offset = compiler.ops.len() - self.offset;
        compiler.ops[self.offset] = (self.mk_jmp)(offset as u32);
    }
}
