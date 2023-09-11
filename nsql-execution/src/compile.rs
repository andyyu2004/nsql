use std::mem;

use anyhow::Result;
use nsql_core::UntypedOid;
use nsql_storage::eval::{Expr, ExprOp, FunctionCatalog, TupleExpr};
use nsql_storage_engine::{StorageEngine, Transaction};

#[derive(Debug)]
pub(crate) struct Compiler<F> {
    ops: Vec<ExprOp<F>>,
}

impl<F> Default for Compiler<F> {
    fn default() -> Self {
        Self { ops: Vec::new() }
    }
}

impl<F> Compiler<F> {
    pub fn compile_many<'env, S: StorageEngine>(
        &mut self,
        catalog: &dyn FunctionCatalog<'env, S, F>,
        tx: &dyn Transaction<'env, S>,
        q: &opt::Query,
        exprs: impl IntoIterator<Item = opt::Expr<'_>>,
    ) -> Result<TupleExpr<F>> {
        exprs
            .into_iter()
            .map(|expr| self.compile(catalog, tx, q, expr))
            .collect::<Result<Box<_>>>()
            .map(TupleExpr::new)
    }

    pub fn compile<'env, S: StorageEngine>(
        &mut self,
        catalog: &dyn FunctionCatalog<'env, S, F>,
        tx: &dyn Transaction<'env, S>,
        q: &opt::Query,
        expr: opt::Expr<'_>,
    ) -> Result<Expr<F>> {
        self.build(catalog, tx, q, &expr)?;
        self.emit(ExprOp::Return);
        Ok(Expr::new(expr.display(q), mem::take(&mut self.ops)))
    }

    fn build<'env, S: StorageEngine>(
        &mut self,
        catalog: &dyn FunctionCatalog<'env, S, F>,
        tx: &dyn Transaction<'env, S>,
        q: &opt::Query,
        expr: &opt::Expr<'_>,
    ) -> Result<()> {
        match &expr {
            opt::Expr::Compiled(expr) => {
                assert!(
                    self.ops.is_empty(),
                    "compiled expressions are expected to be at the top level"
                );
                let expr = (*expr).clone();
                self.ops = expr.resolve(catalog, tx)?.into_ops();
            }
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
                for arg in args {
                    self.build(catalog, tx, q, &arg)?;
                }
                self.emit(ExprOp::Call { function });
            }
            opt::Expr::Case(case) => {
                let scrutinee = case.scrutinee(q);
                let cases = case.cases(q);
                let else_expr = case.else_expr(q);

                let mut next_branch_marker: Option<JumpMarker<F>> = None;
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
            opt::Expr::Quote(expr) => {
                // When we compile a quoted expression we want it to be in the `storable` state not the `executable` state.
                struct NoopCatalog<'env, S>(&'env S);

                impl<'env, S> FunctionCatalog<'env, S, UntypedOid> for NoopCatalog<'env, S> {
                    fn storage(&self) -> &'env S {
                        self.0
                    }

                    fn get_function(
                        &self,
                        _tx: &dyn Transaction<'_, S>,
                        oid: UntypedOid,
                    ) -> Result<UntypedOid> {
                        Ok(oid)
                    }
                }

                let mut compiler = Compiler::<UntypedOid>::default();
                let expr =
                    compiler.compile(&NoopCatalog(catalog.storage()), tx, q, expr.expr(q))?;
                // compile the `opt::Expr` into `eval::Expr` and use the expression as a value
                self.emit(ExprOp::Push(ir::Value::Expr(expr)));
            }
        }

        Ok(())
    }

    fn emit(&mut self, op: ExprOp<F>) {
        self.ops.push(op);
    }

    fn emit_jmp(&mut self, mk_jmp: fn(u32) -> ExprOp<F>) -> JumpMarker<F> {
        JumpMarker::new(self, mk_jmp)
    }
}

struct JumpMarker<F> {
    /// The offset of the jump instruction to backpatch
    offset: usize,
    mk_jmp: fn(u32) -> ExprOp<F>,
}

impl<F> JumpMarker<F> {
    fn new(compiler: &mut Compiler<F>, mk_jmp: fn(u32) -> ExprOp<F>) -> Self {
        let offset = compiler.ops.len();
        compiler.emit(mk_jmp(u32::MAX));
        JumpMarker { offset, mk_jmp }
    }

    fn backpatch(self, compiler: &mut Compiler<F>) {
        let offset = compiler.ops.len() - self.offset;
        compiler.ops[self.offset] = (self.mk_jmp)(offset as u32);
    }
}
