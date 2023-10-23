use anyhow::Result;
use nsql_storage::expr::{
    ExecutableExpr, ExecutableExprOp, ExecutableTupleExpr, Expr, ExprOp, FunctionArgs, TupleExpr,
};
use nsql_storage::tuple::Tuple;
use nsql_storage::value::Value;
use nsql_storage_engine::{ExecutionMode, StorageEngine, Transaction};

use crate::FunctionCatalog;

pub trait TupleExprResolveExt {
    /// Prepare this tuple expression for evaluation.
    // This resolves any function oids and replaces them with the actual function.
    fn resolve<'env, S: StorageEngine, M: ExecutionMode<'env, S>, F>(
        self,
        catalog: &dyn FunctionCatalog<'env, S, M, F>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<TupleExpr<F>>;
}

impl TupleExprResolveExt for TupleExpr {
    #[inline]
    fn resolve<'env, S: StorageEngine, M: ExecutionMode<'env, S>, F>(
        self,
        catalog: &dyn FunctionCatalog<'env, S, M, F>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<TupleExpr<F>> {
        self.map(|oid| catalog.get_function(tx, oid.cast()))
    }
}

pub trait ExprResolveExt {
    /// Prepare this expression for evaluation.
    // This resolves any function oids and replaces them with what the catalog returns
    fn resolve<'env, S: StorageEngine, M: ExecutionMode<'env, S>, F>(
        self,
        catalog: &dyn FunctionCatalog<'env, S, M, F>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<Expr<F>>;
}

impl ExprResolveExt for Expr {
    #[inline]
    fn resolve<'env, S: StorageEngine, M: ExecutionMode<'env, S>, F>(
        self,
        catalog: &dyn FunctionCatalog<'env, S, M, F>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<Expr<F>> {
        self.map(|oid| catalog.get_function(tx, oid.cast()))
    }
}

pub trait ExprEvalExt<'env, S: StorageEngine, M: ExecutionMode<'env, S>> {
    type Output;

    fn eval(
        &self,
        storage: &'env S,
        tx: M::TransactionRef<'_>,
        tuple: &Tuple,
    ) -> Result<Self::Output>;

    fn eval_with(
        &self,
        evaluator: &mut Evaluator,
        storage: &'env S,
        tx: M::TransactionRef<'_>,
        tuple: &Tuple,
    ) -> Result<Self::Output>;
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> ExprEvalExt<'env, S, M>
    for ExecutableTupleExpr<'env, S, M>
{
    type Output = Tuple;

    #[inline]
    fn eval(&self, storage: &'env S, tx: M::TransactionRef<'_>, tuple: &Tuple) -> Result<Tuple> {
        let mut evaluator = Evaluator::default();
        self.eval_with(&mut evaluator, storage, tx, tuple)
    }

    #[inline]
    fn eval_with(
        &self,
        evaluator: &mut Evaluator,
        storage: &'env S,
        tx: M::TransactionRef<'_>,
        tuple: &Tuple,
    ) -> Result<Tuple> {
        self.exprs().iter().map(|expr| expr.eval_with(evaluator, storage, tx, tuple)).collect()
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> ExprEvalExt<'env, S, M>
    for ExecutableExpr<'env, S, M>
{
    type Output = Value;

    #[inline]
    fn eval(&self, storage: &'env S, tx: M::TransactionRef<'_>, tuple: &Tuple) -> Result<Value> {
        let mut evaluator = Evaluator::default();
        self.eval_with(&mut evaluator, storage, tx, tuple)
    }

    #[inline]
    fn eval_with(
        &self,
        evaluator: &mut Evaluator,
        storage: &'env S,
        tx: M::TransactionRef<'_>,
        tuple: &Tuple,
    ) -> Result<Value> {
        evaluator.eval_expr(storage, tx, tuple, self)
    }
}

#[derive(Default)]
pub struct Evaluator {
    stack: Vec<Value>,
    ip: usize,
}

impl Evaluator {
    pub fn eval_expr<'env, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &mut self,
        storage: &'env S,
        tx: M::TransactionRef<'_>,
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

    fn execute_op<'env, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &mut self,
        storage: &'env S,
        tx: M::TransactionRef<'_>,
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
                let args = self
                    .stack
                    .drain(self.stack.len() - function.arity()..)
                    .collect::<FunctionArgs>();
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
