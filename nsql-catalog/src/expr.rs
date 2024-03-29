use anyhow::Result;
use nsql_profile::Profiler;
use nsql_storage::expr::{Expr, ExprOp, TupleExpr};
use nsql_storage::tuple::{Tuple, TupleLike};
use nsql_storage::value::Value;
use nsql_storage_engine::{ExecutionMode, StorageEngine};

use crate::{Catalog, FunctionCatalog, ScalarFunctionPtr, TransactionContext};

pub type FunctionArgs<'a> = &'a mut Vec<Value>;

pub type ExecutableTupleExpr<'env, 'txn, S, M> = TupleExpr<ExecutableFunction<'env, 'txn, S, M>>;

pub type ExecutableExpr<'env, 'txn, S, M> = Expr<ExecutableFunction<'env, 'txn, S, M>>;

pub type ExecutableFunction<'env, 'txn, S, M> = ScalarFunctionPtr<'env, 'txn, S, M>;

pub type ExecutableExprOp<'env, 'txn, S, M> = ExprOp<ExecutableFunction<'env, 'txn, S, M>>;

pub trait TupleExprResolveExt {
    /// Prepare this tuple expression for evaluation.
    // This resolves any function oids and replaces them with the actual function.
    fn resolve<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, F>(
        self,
        catalog: &dyn FunctionCatalog<'env, 'txn, S, M, F>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<TupleExpr<F>>;
}

impl TupleExprResolveExt for TupleExpr {
    #[inline]
    fn resolve<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, F>(
        self,
        catalog: &dyn FunctionCatalog<'env, 'txn, S, M, F>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<TupleExpr<F>> {
        self.map(|oid| catalog.get_function(tx, oid.cast()))
    }
}

pub trait ExprResolveExt {
    /// Prepare this expression for evaluation.
    // This resolves any function oids and replaces them with what the catalog returns
    fn resolve<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, F>(
        self,
        catalog: &dyn FunctionCatalog<'env, 'txn, S, M, F>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Expr<F>>
    where
        'env: 'txn;
}

impl ExprResolveExt for Expr {
    #[inline]
    fn resolve<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, F>(
        self,
        catalog: &dyn FunctionCatalog<'env, 'txn, S, M, F>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Expr<F>>
    where
        'env: 'txn,
    {
        self.map(|oid| catalog.get_function(tx, oid.cast()))
    }
}

pub trait ExprEvalExt<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T> {
    type Output;

    fn eval(
        &self,
        evaluator: &mut Evaluator,
        storage: &'env S,
        prof: &Profiler,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        tuple: &T,
    ) -> Result<Self::Output>;
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    ExprEvalExt<'env, 'txn, S, M, T> for ExecutableTupleExpr<'env, 'txn, S, M>
{
    type Output = T;

    #[inline]
    fn eval(
        &self,
        evaluator: &mut Evaluator,
        storage: &'env S,
        prof: &Profiler,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        tuple: &T,
    ) -> Result<Self::Output> {
        self.exprs().iter().map(|expr| expr.eval(evaluator, storage, prof, tx, tuple)).collect()
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleLike>
    ExprEvalExt<'env, 'txn, S, M, T> for ExecutableExpr<'env, 'txn, S, M>
{
    type Output = Value;

    #[inline]
    fn eval(
        &self,
        evaluator: &mut Evaluator,
        storage: &'env S,
        prof: &Profiler,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        tuple: &T,
    ) -> Result<Value> {
        evaluator.eval_expr(storage, prof, tx, tuple, self)
    }
}

#[derive(Debug, Default)]
pub struct Evaluator {
    stack: Vec<Value>,
    ip: usize,
}

impl Evaluator {
    pub fn eval_expr<
        'env: 'txn,
        'txn,
        S: StorageEngine,
        M: ExecutionMode<'env, S>,
        T: TupleLike,
    >(
        &mut self,
        storage: &'env S,
        prof: &Profiler,
        tcx: &dyn TransactionContext<'env, 'txn, S, M>,
        tuple: &T,
        expr: &ExecutableExpr<'env, 'txn, S, M>,
    ) -> Result<Value> {
        let _guard = prof.start(prof.execute_expr_id);

        self.ip = 0;
        self.stack.clear();
        loop {
            let op = &expr.ops()[self.ip];
            if matches!(op, ExprOp::Ret) {
                break;
            }
            self.execute_op(storage, prof, tcx, tuple, op)?;
        }

        debug_assert_eq!(
            self.stack.len(),
            1,
            "stack should have exactly one value after execution, had {}",
            self.stack.len()
        );

        Ok(self.pop())
    }

    fn execute_op<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleLike>(
        &mut self,
        storage: &'env S,
        prof: &Profiler,
        tcx: &dyn TransactionContext<'env, 'txn, S, M>,
        tuple: &T,
        op: &ExecutableExprOp<'env, 'txn, S, M>,
    ) -> Result<()> {
        let value = match op {
            ExprOp::Project { index } => tuple[*index].clone(),
            ExprOp::Push(value) => value.clone(),
            ExprOp::MkArray { len } => {
                let array = self.stack.drain(self.stack.len() - *len..).collect::<Box<[Value]>>();
                Value::Array(array)
            }
            ExprOp::Call { function } => {
                function(Catalog::new(storage), prof, tcx, &mut self.stack)?
            }
            ExprOp::IfNeJmp(offset) => {
                let rhs = self.pop();
                let lhs = self.pop();
                // maybe we should just call the `NOT_EQUAL` function but that would be slower
                self.ip +=
                    if lhs.is_null() || rhs.is_null() || lhs != rhs { *offset as usize } else { 1 };
                return Ok(());
            }
            ExprOp::IfNullJmp(offset) => {
                let value = self.pop();
                self.ip += if value.is_null() { *offset as usize } else { 1 };
                return Ok(());
            }
            ExprOp::Jmp(offset) => {
                self.ip += *offset as usize;
                return Ok(());
            }
            ExprOp::Dup => self.stack.last().unwrap().clone(),
            ExprOp::Pop => {
                self.pop();
                self.ip += 1;
                return Ok(());
            }
            ExprOp::Ret => return Ok(()),
        };

        self.stack.push(value);
        self.ip += 1;
        Ok(())
    }

    fn pop(&mut self) -> Value {
        self.stack.pop().unwrap()
    }
}
