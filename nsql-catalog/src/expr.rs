use std::fmt;

use anyhow::Result;
use nsql_storage::expr::{Expr, ExprOp, TupleExpr};
use nsql_storage::tuple::Tuple;
use nsql_storage::value::Value;
use nsql_storage_engine::{ExecutionMode, StorageEngine};

use crate::{FunctionCatalog, TransactionContext};

// Using the exact type to avoid an allocation
pub type FunctionArgs<'a> = std::vec::Drain<'a, Value>; // also `impl ExactSizeIterator + 'a` doesn't work

pub type ExecutableTupleExpr<'env, S, M> = TupleExpr<ExecutableFunction<'env, S, M>>;

pub type ExecutableExpr<'env, S, M> = Expr<ExecutableFunction<'env, S, M>>;

pub type ExecutableFunction<'env, S, M> = Box<dyn ScalarFunction<'env, S, M>>;

pub type ExecutableExprOp<'env, S, M> = ExprOp<ExecutableFunction<'env, S, M>>;

pub trait ScalarFunction<'env, S: StorageEngine, M: ExecutionMode<'env, S>>:
    fmt::Debug + Send + Sync
{
    fn invoke<'txn>(
        &self,
        storage: &'env S,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        args: FunctionArgs<'_>,
    ) -> Result<Value>
    where
        'env: 'txn;

    /// The number f arguments this function takes.
    fn arity(&self) -> usize;
}

pub trait TupleExprResolveExt {
    /// Prepare this tuple expression for evaluation.
    // This resolves any function oids and replaces them with the actual function.
    fn resolve<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, F>(
        self,
        catalog: &dyn FunctionCatalog<'env, S, M, F>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<TupleExpr<F>>;
}

impl TupleExprResolveExt for TupleExpr {
    #[inline]
    fn resolve<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, F>(
        self,
        catalog: &dyn FunctionCatalog<'env, S, M, F>,
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
        catalog: &dyn FunctionCatalog<'env, S, M, F>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Expr<F>>
    where
        'env: 'txn;
}

impl ExprResolveExt for Expr {
    #[inline]
    fn resolve<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, F>(
        self,
        catalog: &dyn FunctionCatalog<'env, S, M, F>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Expr<F>>
    where
        'env: 'txn,
    {
        self.map(|oid| catalog.get_function(tx, oid.cast()))
    }
}

pub trait ExprEvalExt<'env, S: StorageEngine, M: ExecutionMode<'env, S>> {
    type Output;

    fn eval<'txn>(
        &self,
        evaluator: &mut Evaluator,
        storage: &'env S,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        tuple: &Tuple,
    ) -> Result<Self::Output>
    where
        'env: 'txn;
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> ExprEvalExt<'env, S, M>
    for ExecutableTupleExpr<'env, S, M>
{
    type Output = Tuple;

    #[inline]
    fn eval<'txn>(
        &self,
        evaluator: &mut Evaluator,
        storage: &'env S,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        tuple: &Tuple,
    ) -> Result<Tuple>
    where
        'env: 'txn,
    {
        self.exprs().iter().map(|expr| expr.eval(evaluator, storage, tx, tuple)).collect()
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> ExprEvalExt<'env, S, M>
    for ExecutableExpr<'env, S, M>
{
    type Output = Value;

    #[inline]
    fn eval<'txn>(
        &self,
        evaluator: &mut Evaluator,
        storage: &'env S,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        tuple: &Tuple,
    ) -> Result<Value>
    where
        'env: 'txn,
    {
        evaluator.eval_expr(storage, tx, tuple, self)
    }
}

#[derive(Debug, Default)]
pub struct Evaluator {
    stack: Vec<Value>,
    ip: usize,
}

impl Evaluator {
    pub fn eval_expr<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &mut self,
        storage: &'env S,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        tuple: &Tuple,
        expr: &ExecutableExpr<'env, S, M>,
    ) -> Result<Value> {
        self.ip = 0;
        self.stack.clear();
        loop {
            let op = &expr.ops()[self.ip];
            if matches!(op, ExprOp::Return) {
                break;
            }
            self.execute_op(storage, tx, tuple, op)?;
        }

        assert_eq!(
            self.stack.len(),
            1,
            "stack should have exactly one value after execution, had {}",
            self.stack.len()
        );

        Ok(self.stack.pop().unwrap())
    }

    fn execute_op<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &mut self,
        storage: &'env S,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        tuple: &Tuple,
        op: &ExecutableExprOp<'env, S, M>,
    ) -> Result<()> {
        let value = match op {
            ExprOp::Project { index } => tuple[*index].clone(),
            ExprOp::Push(value) => value.clone(),
            ExprOp::MkArray { len } => {
                let array = self.stack.drain(self.stack.len() - *len..).collect::<Box<[Value]>>();
                Value::Array(array)
            }
            ExprOp::Call { function } => {
                let args = self.stack.drain(self.stack.len() - function.arity()..);
                function.invoke(storage, tx, args)?
            }
            ExprOp::IfNeJmp(offset) => {
                let rhs = self.stack.pop().unwrap();
                let lhs = self.stack.pop().unwrap();
                // maybe we should just call the `NOT_EQUAL` function but that would be slower
                self.ip +=
                    if lhs.is_null() || rhs.is_null() || lhs != rhs { *offset as usize } else { 1 };
                return Ok(());
            }
            ExprOp::IfNullJmp(offset) => {
                let value = self.stack.pop().unwrap();
                self.ip += if value.is_null() { *offset as usize } else { 1 };
                return Ok(());
            }
            ExprOp::Jmp(offset) => {
                self.ip += *offset as usize;
                return Ok(());
            }
            ExprOp::Dup => self.stack.last().unwrap().clone(),
            ExprOp::Pop => {
                self.stack.pop().unwrap();
                self.ip += 1;
                return Ok(());
            }
            ExprOp::Return => return Ok(()),
        };

        self.stack.push(value);
        self.ip += 1;
        Ok(())
    }
}
