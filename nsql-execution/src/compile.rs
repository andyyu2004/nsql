use std::mem;

use anyhow::Result;
use nsql_storage::eval::{
    ExecutableExpr, ExecutableExprOp, ExecutableTupleExpr, Expr, ExprOp, FunctionCatalog,
    TupleExpr, UnaryOp,
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
        exprs: impl IntoIterator<Item = ir::Expr>,
    ) -> Result<ExecutableTupleExpr> {
        exprs
            .into_iter()
            .map(|expr| self.compile(catalog, tx, expr))
            .collect::<Result<Box<_>>>()
            .map(TupleExpr::new)
    }

    pub fn compile<'env, S: StorageEngine>(
        &mut self,
        catalog: &dyn FunctionCatalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
        expr: ir::Expr,
    ) -> Result<ExecutableExpr> {
        let pretty = expr.to_string();
        assert!(self.ops.is_empty());
        self.build(catalog, tx, expr)?;
        self.emit(ExprOp::Return);
        Ok(Expr::new(pretty, mem::take(&mut self.ops)))
    }

    fn emit(&mut self, op: ExecutableExprOp) {
        self.ops.push(op);
    }

    fn build<'env, S: StorageEngine>(
        &mut self,
        catalog: &dyn FunctionCatalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
        expr: ir::Expr,
    ) -> Result<()> {
        match expr.kind {
            ir::ExprKind::Literal(value) => self.ops.push(ExprOp::Push(value)),
            ir::ExprKind::Array(exprs) => {
                let len = exprs.len();
                for expr in exprs.into_vec() {
                    self.build(catalog, tx, expr)?;
                }
                self.emit(ExprOp::MkArray { len });
            }
            ir::ExprKind::Alias { expr, .. } => self.build(catalog, tx, *expr)?,
            ir::ExprKind::UnaryOp { op, expr } => {
                self.build(catalog, tx, *expr)?;
                let op = match op {
                    ir::UnaryOp::Neg => UnaryOp::Neg,
                };
                self.emit(ExprOp::UnaryOp(op));
            }
            ir::ExprKind::ColumnRef { index, .. } => self.ops.push(ExprOp::Project { index }),
            ir::ExprKind::FunctionCall { function, args } => {
                for arg in args.into_vec() {
                    self.build(catalog, tx, arg)?;
                }
                self.emit(ExprOp::Call { function: Box::new(function.function()) });
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
                    self.build(catalog, tx, scrutinee.clone())?;
                    // push the comparison expression onto the stack
                    self.build(catalog, tx, case.when)?;
                    // if the comparison is false, jump to the the branch
                    next_branch_marker = Some(self.emit_jmp(|offset| ExprOp::IfNeJmp { offset }));
                    // otherwise, build the then expression
                    self.build(catalog, tx, case.then)?;
                    // and jump to the end of the case
                    end_markers.push(self.emit_jmp(|offset| ExprOp::Jmp { offset }));
                }

                // if all branches fail, jump to the else branch
                next_branch_marker
                    .take()
                    .expect("this should exist as cases are non-empty")
                    .backpatch(self);

                // the else branch is optional, and defaults to null
                match else_result {
                    Some(else_result) => self.build(catalog, tx, *else_result)?,
                    None => self.emit(ExprOp::Push(ir::Value::Null)),
                }

                // backpatch all the end markers
                for marker in end_markers {
                    marker.backpatch(self);
                }
            }
            ir::ExprKind::InfixOperator { operator, lhs, rhs } => {
                let function = catalog.get_function(tx, operator.function().untyped())?;
                self.build(catalog, tx, *lhs)?;
                self.build(catalog, tx, *rhs)?;
                self.emit(ExprOp::Call { function });
            }
            ir::ExprKind::Subquery(_) => {
                unimplemented!("cannot compile subqueries (needs to be flattened during planning)")
            }
        }

        Ok(())
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
