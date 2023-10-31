use std::mem;

use anyhow::Result;
use nsql_catalog::expr::ExprResolveExt;
use nsql_catalog::{Function, FunctionCatalog, TransactionContext};
use nsql_core::{Oid, UntypedOid};
use nsql_storage::expr::{Expr, ExprOp, TupleExpr};
use nsql_storage_engine::{ExecutionMode, StorageEngine};

use crate::physical_plan::PlannerProfiler;

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
    pub fn compile_many<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &mut self,
        profiler: &impl PlannerProfiler,
        catalog: &dyn FunctionCatalog<'env, S, M, F>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        q: &opt::Query,
        exprs: impl IntoIterator<Item = opt::Expr<'_>>,
    ) -> Result<TupleExpr<F>> {
        exprs
            .into_iter()
            .map(|expr| self.compile(profiler, catalog, tx, q, expr))
            .collect::<Result<Box<_>>>()
            .map(TupleExpr::new)
    }

    pub fn compile<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &mut self,
        profiler: &impl PlannerProfiler,
        catalog: &dyn FunctionCatalog<'env, S, M, F>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        q: &opt::Query,
        expr: opt::Expr<'_>,
    ) -> Result<Expr<F>> {
        profiler.profile(profiler.compile_event_id(), || {
            self.build(profiler, catalog, tx, q, &expr)?;
            self.emit(ExprOp::Return);
            Ok(Expr::new(expr.display(q), mem::take(&mut self.ops)))
        })
    }

    fn build<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &mut self,
        profiler: &impl PlannerProfiler,
        catalog: &dyn FunctionCatalog<'env, S, M, F>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
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
            opt::Expr::ColumnRef(col) => {
                debug_assert!(
                    col.level == 0,
                    "correlated column references should be resolved before compilation, got column reference: {col:?}",
                );
                self.emit(ExprOp::Project { index: col.index })
            }
            opt::Expr::Literal(lit) => self.emit(ExprOp::Push(lit.value(q).clone())),
            opt::Expr::Array(array) => {
                let exprs = array.exprs(q);
                let len = exprs.len();
                for expr in exprs {
                    self.build(profiler, catalog, tx, q, &expr)?;
                }
                self.emit(ExprOp::MkArray { len });
            }
            opt::Expr::Call(call) => {
                let function = catalog.get_function(tx, call.function())?;
                let args = call.args(q);
                for arg in args {
                    self.build(profiler, catalog, tx, q, &arg)?;
                }
                self.emit(ExprOp::Call { function });
            }
            opt::Expr::Coalesce(coalesce) => {
                let exprs = coalesce.exprs(q);
                let mut next_branch_marker: Option<JumpMarker<F>> = None;
                let mut end_markers = Vec::with_capacity(exprs.len());
                for expr in exprs {
                    if let Some(marker) = next_branch_marker.take() {
                        marker.backpatch(self);
                        // pop the duplicated of the prior branch
                        self.emit(ExprOp::Pop);
                    }

                    self.build(profiler, catalog, tx, q, &expr)?;
                    // we need to duplicate the value because we need to keep it on the stack if it is non-null
                    self.emit(ExprOp::Dup);
                    next_branch_marker = Some(self.emit_jmp(ExprOp::IfNullJmp));
                    end_markers.push(self.emit_jmp(ExprOp::Jmp));
                }

                // if all cases fail, jump to the "default" case
                next_branch_marker
                    .take()
                    .expect("this should exist as cases are non-empty")
                    .backpatch(self);

                for marker in end_markers {
                    marker.backpatch(self);
                }

                // Each case should have left it's value on top of the stack.
                // (If the last case was null, then there's a null on top of the stack which is what we need).
                // So we're done.
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
                    self.build(profiler, catalog, tx, q, &scrutinee)?;
                    // push the comparison expression onto the stack
                    self.build(profiler, catalog, tx, q, &when)?;
                    // if the comparison is false, jump to the next `when` branch
                    next_branch_marker = Some(self.emit_jmp(ExprOp::IfNeJmp));
                    // build the `then` expression
                    self.build(profiler, catalog, tx, q, &then)?;
                    // and jump to the end of the case
                    end_markers.push(self.emit_jmp(ExprOp::Jmp));
                }

                // if all branches fail, jump to the else branch
                next_branch_marker
                    .take()
                    .expect("this should exist as cases are non-empty")
                    .backpatch(self);

                self.build(profiler, catalog, tx, q, &else_expr)?;

                // backpatch all the end markers
                for marker in end_markers {
                    marker.backpatch(self);
                }
            }
            opt::Expr::Quote(expr) => {
                // When we compile a quoted expression we want it to be in the `storable` state not the `executable` state.
                struct NoopCatalog<'env, S>(&'env S);

                impl<'env, S, M> FunctionCatalog<'env, S, M, UntypedOid> for NoopCatalog<'env, S> {
                    fn storage(&self) -> &'env S {
                        self.0
                    }

                    fn get_function<'txn>(
                        &self,
                        _tx: &dyn TransactionContext<'env, 'txn, S, M>,
                        oid: Oid<Function>,
                    ) -> Result<UntypedOid>
                    where
                        'env: 'txn,
                    {
                        Ok(oid.untyped())
                    }
                }

                let mut compiler = Compiler::<UntypedOid>::default();
                let expr = compiler.compile::<S, M>(
                    profiler,
                    &NoopCatalog(catalog.storage()),
                    tx,
                    q,
                    expr.expr(q),
                )?;
                // compile the `opt::Expr` into `expr::Expr` and use the expression as a value
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
