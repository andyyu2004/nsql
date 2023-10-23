//! A serializable stack-based bytecode for evaluating expressions.

use std::str::FromStr;
use std::{fmt, mem};

use anyhow::Result;
use itertools::Itertools;
use nsql_core::{LogicalType, UntypedOid};
use nsql_storage_engine::{ExecutionMode, StorageEngine, Transaction};
use nsql_util::static_assert_eq;
use rkyv::{Archive, Deserialize, Serialize};

use crate::tuple::{Tuple, TupleIndex};
use crate::value::{CastError, FromValue, Value};

pub trait FunctionCatalog<'env, S, M, F = Box<dyn ScalarFunction<'env, S, M>>> {
    fn storage(&self) -> &'env S;

    fn get_function(&self, tx: &dyn Transaction<'env, S>, oid: UntypedOid) -> Result<F>;
}

// Note: smallvec seems to always be slower than this for sqlite/select3.sql
// This alias is useful for testing different types
pub type FunctionArgs = Box<[Value]>;

pub trait ScalarFunction<'env, S: StorageEngine, M: ExecutionMode<'env, S>>:
    fmt::Debug + Send + Sync
{
    fn invoke(
        &self,
        storage: &'env S,
        tx: M::TransactionRef<'_>,
        args: FunctionArgs,
    ) -> Result<Value>;

    /// The number f arguments this function takes.
    fn arity(&self) -> usize;
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
#[omit_bounds]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
pub struct TupleExpr<F = UntypedOid> {
    exprs: Box<[Expr<F>]>,
}

pub type ExecutableTupleExpr<'env, S, M> = TupleExpr<Box<dyn ScalarFunction<'env, S, M>>>;

impl<F> fmt::Display for TupleExpr<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.exprs.iter().join(", "))
    }
}

impl<F> TupleExpr<F> {
    #[inline]
    pub fn new(exprs: impl Into<Box<[Expr<F>]>>) -> Self {
        Self { exprs: exprs.into() }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.exprs.is_empty()
    }

    /// The width of the tuples produced by this expression.
    #[inline]
    pub fn width(&self) -> usize {
        self.exprs.len()
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> ExecutableTupleExpr<'env, S, M> {
    #[inline]
    pub fn execute(
        &self,
        storage: &'env S,
        tx: M::TransactionRef<'_>,
        tuple: &Tuple,
    ) -> Result<Tuple> {
        self.exprs.iter().map(|expr| expr.execute(storage, tx, tuple)).collect()
    }
}

impl FromValue for TupleExpr {
    #[inline]
    fn from_value(value: Value) -> Result<Self, CastError> {
        match value {
            Value::TupleExpr(expr) => Ok(expr),
            _ => Err(CastError::new(value, LogicalType::TupleExpr)),
        }
    }
}

impl FromValue for LogicalType {
    #[inline]
    fn from_value(value: Value) -> Result<Self, CastError> {
        match value {
            Value::Type(ty) => Ok(ty),
            _ => Err(CastError::new(value, LogicalType::Type)),
        }
    }
}

impl From<TupleExpr> for Value {
    #[inline]
    fn from(expr: TupleExpr) -> Self {
        Value::TupleExpr(expr)
    }
}

pub type ExecutableExpr<'env, S, M> = Expr<ExecutableFunction<'env, S, M>>;

pub type ExecutableFunction<'env, S, M> = Box<dyn ScalarFunction<'env, S, M>>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
pub struct Expr<F = UntypedOid> {
    pretty: Box<str>,
    ops: Box<[ExprOp<F>]>,
}

// this is just to allow egg stuff to compile for now
impl<F> FromStr for Expr<F> {
    type Err = ();

    fn from_str(_s: &str) -> std::result::Result<Self, Self::Err> {
        Err(())
    }
}

static_assert_eq!(mem::size_of::<Expr>(), 32);

impl FromValue for Expr {
    #[inline]
    fn from_value(value: Value) -> Result<Self, CastError> {
        match value {
            Value::Expr(expr) => Ok(expr),
            _ => Err(CastError::new(value, LogicalType::Expr)),
        }
    }
}

impl From<Expr> for Value {
    #[inline]
    fn from(val: Expr) -> Self {
        Value::Expr(val)
    }
}

impl<F> fmt::Display for Expr<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.pretty)
    }
}

impl<F> Expr<F> {
    #[inline]
    pub fn null() -> Self {
        Self { pretty: "NULL".into(), ops: Box::new([ExprOp::Push(Value::Null), ExprOp::Return]) }
    }

    #[inline]
    pub fn is_literal(&self, value: impl Into<Value>) -> bool {
        matches!(self.ops.as_ref(), [ExprOp::Push(v), ExprOp::Return] if v == &value.into())
    }

    #[inline]
    pub fn literal(value: impl Into<Value>) -> Self {
        let value = value.into();
        Self {
            pretty: format!("{value}").into(),
            ops: Box::new([ExprOp::Push(value), ExprOp::Return]),
        }
    }

    pub fn call(function: F, args: impl IntoIterator<Item = Value>) -> Self {
        let mut ops = vec![];
        ops.extend(args.into_iter().map(ExprOp::Push));
        ops.push(ExprOp::Call { function });
        Self { pretty: "call".into(), ops: ops.into_boxed_slice() }
    }

    pub fn new(pretty: impl fmt::Display, ops: impl Into<Box<[ExprOp<F>]>>) -> Self {
        let ops = ops.into();
        assert!(ops.len() > 1, "should have at least one value and one return");
        Self { pretty: format!("{pretty}").into(), ops }
    }

    #[inline]
    pub fn ops(&self) -> &[ExprOp<F>] {
        self.ops.as_ref()
    }

    #[inline]
    pub fn into_ops(self) -> Vec<ExprOp<F>> {
        self.ops.into_vec()
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> ExecutableExpr<'env, S, M> {
    pub fn execute(
        &self,
        storage: &'env S,
        tx: M::TransactionRef<'_>,
        tuple: &Tuple,
    ) -> Result<Value> {
        let mut ip = 0;
        let mut stack = vec![];
        loop {
            let op = &self.ops[ip];
            if matches!(op, ExprOp::Return) {
                break;
            }
            op.execute(storage, tx, &mut stack, &mut ip, tuple)?;
        }

        assert_eq!(
            stack.len(),
            1,
            "stack should have exactly one value after execution, had {}",
            stack.len()
        );

        Ok(stack.pop().unwrap())
    }
}

pub type ExecutableExprOp<'env, S, M> = ExprOp<Box<dyn ScalarFunction<'env, S, M>>>;

/// `Expr` is generic over the representation of functions.
/// For storage in the catalog, we need to be able to serialize the function and `F = UntypedOid` (morally `Oid<Function>`).
/// For execution, we want to be able to invoke the function (without looking into the catalog as that's slow) and so `F = Box<dyn ScalarFunction<S>>`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
#[omit_bounds]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
pub enum ExprOp<F = UntypedOid> {
    Push(#[omit_bounds] Value),
    Project { index: TupleIndex },
    MkArray { len: usize },
    Call { function: F },
    IfNeJmp(u32),
    IfNullJmp(u32),
    Jmp(u32),
    Dup,
    Pop,
    Return,
}

static_assert_eq!(mem::size_of::<ExprOp>(), 40);

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>>
    ExprOp<Box<dyn ScalarFunction<'env, S, M>>>
{
    fn execute(
        &self,
        storage: &'env S,
        tx: M::TransactionRef<'_>,
        stack: &mut Vec<Value>,
        ip: &mut usize,
        tuple: &Tuple,
    ) -> Result<()> {
        let value = match self {
            ExprOp::Project { index } => tuple[*index].clone(),
            ExprOp::Push(value) => value.clone(),
            ExprOp::MkArray { len } => {
                let array = stack.drain(stack.len() - *len..).collect::<Box<[Value]>>();
                Value::Array(array)
            }
            ExprOp::Call { function } => {
                let args = stack.drain(stack.len() - function.arity()..).collect::<FunctionArgs>();
                function.invoke(storage, tx, args)?
            }
            ExprOp::IfNeJmp(offset) => {
                let rhs = stack.pop().unwrap();
                let lhs = stack.pop().unwrap();
                // maybe we should just call the `NOT_EQUAL` function but that would be slower
                *ip +=
                    if lhs.is_null() || rhs.is_null() || lhs != rhs { *offset as usize } else { 1 };
                return Ok(());
            }
            ExprOp::IfNullJmp(offset) => {
                let value = stack.pop().unwrap();
                *ip += if value.is_null() { *offset as usize } else { 1 };
                return Ok(());
            }
            ExprOp::Jmp(offset) => {
                *ip += *offset as usize;
                return Ok(());
            }
            ExprOp::Dup => stack.last().unwrap().clone(),
            ExprOp::Pop => {
                stack.pop().unwrap();
                *ip += 1;
                return Ok(());
            }
            ExprOp::Return => return Ok(()),
        };

        stack.push(value);
        *ip += 1;
        Ok(())
    }
}

mod fold;
#[cfg(test)]
mod tests;
