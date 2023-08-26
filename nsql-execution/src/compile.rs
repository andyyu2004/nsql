use std::mem;

use anyhow::Result;
use nsql_storage::eval::{
    ExecutableExpr, ExecutableExprOp, ExecutableTupleExpr, Expr, ExprOp, FunctionCatalog, TupleExpr,
};
use nsql_storage_engine::{StorageEngine, Transaction};

#[derive(Default, Debug)]
pub(crate) struct Compiler {
    ops: Vec<ExecutableExprOp>,
}

impl Compiler {
    pub fn compile_many<'env, S: StorageEngine>(
        &mut self,
        catalog: &dyn FunctionCatalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
        q: &opt::Query,
        exprs: impl IntoIterator<Item = opt::Expr<'_>>,
    ) -> Result<ExecutableTupleExpr> {
        exprs
            .into_iter()
            .map(|expr| self.compile(catalog, tx, q, expr))
            .collect::<Result<Box<_>>>()
            .map(TupleExpr::new)
    }

    pub fn compile<'env, S: StorageEngine>(
        &mut self,
        catalog: &dyn FunctionCatalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
        q: &opt::Query,
        expr: opt::Expr<'_>,
    ) -> Result<ExecutableExpr> {
        self.build(catalog, tx, q, &expr)?;
        self.emit(ExprOp::Return);
        Ok(Expr::new(expr.display(q), mem::take(&mut self.ops)))
    }

    fn build<'env, S: StorageEngine>(
        &mut self,
        catalog: &dyn FunctionCatalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
        q: &opt::Query,
        expr: &opt::Expr<'_>,
    ) -> Result<()> {
        match &expr {
            opt::Expr::ColumnRef(col) => self.emit(ExprOp::Project { index: col.index }),
            opt::Expr::Literal(lit) => self.emit(ExprOp::Push(lit.value(q).clone())),
            opt::Expr::Array(array) => {
                let exprs = array.exprs(q);
                let len = exprs.len();
                for expr in exprs {
                    self.build(catalog, tx, q, &expr)?;
                }
                self.emit(ExprOp::MkArray { len });
            }
            opt::Expr::Call(call) => {
                let function = catalog.get_function(tx, call.function().untyped())?;
                let args = call.args(q);
                assert_eq!(function.arity(), args.len());
                for arg in args {
                    self.build(catalog, tx, q, &arg)?;
                }
                self.emit(ExprOp::Call { function });
            }
            opt::Expr::Case(case) => {
                let scrutinee = case.scrutinee(q);
                let cases = case.cases(q);
                let else_expr = case.else_expr(q);

                let mut next_branch_marker: Option<JumpMarker> = None;
                let mut end_markers = Vec::with_capacity(cases.len());

                for (when, then) in cases {
                    // if there is a marker for the previous branch to backpatch, backpatch it
                    if let Some(marker) = next_branch_marker.take() {
                        marker.backpatch(self);
                    }

                    // FIXME should this be evaluated once or once per branch?
                    // probably once in total (so might need a dup instruction rather than rebuilding it every time)
                    // push the scrutinee onto the stack
                    self.build(catalog, tx, q, &scrutinee)?;
                    // push the comparison expression onto the stack
                    self.build(catalog, tx, q, &when)?;
                    // if the comparison is false, jump to the the branch
                    next_branch_marker = Some(self.emit_jmp(|offset| ExprOp::IfNeJmp { offset }));
                    // otherwise, build the then expression
                    self.build(catalog, tx, q, &then)?;
                    // and jump to the end of the case
                    end_markers.push(self.emit_jmp(|offset| ExprOp::Jmp { offset }));
                }

                // if all branches fail, jump to the else branch
                next_branch_marker
                    .take()
                    .expect("this should exist as cases are non-empty")
                    .backpatch(self);

                self.build(catalog, tx, q, &else_expr)?;

                // backpatch all the end markers
                for marker in end_markers {
                    marker.backpatch(self);
                }
            }
        }

        Ok(())
    }

    fn emit(&mut self, op: ExecutableExprOp) {
        self.ops.push(op);
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
