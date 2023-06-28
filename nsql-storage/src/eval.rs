//! A serializable stack-based bytecode for evaluating expressions.

use std::{fmt, mem};

use anyhow::Result;
use itertools::Itertools;
use nsql_core::{LogicalType, UntypedOid};
use nsql_storage_engine::Transaction;
use nsql_util::static_assert_eq;
use rkyv::{Archive, Deserialize, Serialize};

use crate::tuple::{Tuple, TupleIndex};
use crate::value::{CastError, FromValue, Value};

pub trait FunctionCatalog<'env, S> {
    fn get_function(
        &self,
        tx: &dyn Transaction<'env, S>,
        oid: UntypedOid,
    ) -> Result<Box<dyn Function>>;
}

pub trait Function: fmt::Debug + Send + Sync + 'static {
    /// The number f arguments this function takes.
    fn arity(&self) -> usize;

    fn call(&self, args: Box<[Value]>) -> Value;
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
#[omit_bounds]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
pub struct TupleExpr<F = UntypedOid> {
    exprs: Box<[Expr<F>]>,
}

pub type ExecutableTupleExpr = TupleExpr<Box<dyn Function>>;

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
}

impl TupleExpr {
    /// Prepare this expression for evaluation.
    // This resolves any function oids and replaces them with the actual function.
    pub fn prepare<'env, S>(
        self,
        catalog: &dyn FunctionCatalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<ExecutableTupleExpr> {
        self.map(|oid| catalog.get_function(tx, oid))
    }
}

impl ExecutableTupleExpr {
    #[inline]
    pub fn execute(&self, tuple: &Tuple) -> Tuple {
        self.exprs.iter().map(|expr| expr.execute(tuple)).collect()
    }
}

impl FromValue for TupleExpr {
    #[inline]
    fn from_value(value: Value) -> Result<Self, CastError<Self>> {
        match value {
            Value::TupleExpr(expr) => Ok(expr),
            _ => Err(CastError::<Self>::new(value)),
        }
    }
}

impl FromValue for LogicalType {
    #[inline]
    fn from_value(value: Value) -> Result<Self, CastError<Self>> {
        match value {
            Value::Type(ty) => Ok(ty),
            _ => Err(CastError::<Self>::new(value)),
        }
    }
}

impl From<TupleExpr> for Value {
    #[inline]
    fn from(expr: TupleExpr) -> Self {
        Value::TupleExpr(expr)
    }
}

pub type ExecutableExpr = Expr<Box<dyn Function>>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
pub struct Expr<F = UntypedOid> {
    pretty: String,
    ops: Box<[ExprOp<F>]>,
}

impl<F> fmt::Display for Expr<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.pretty)
    }
}

impl<F> Expr<F> {
    pub fn new(pretty: impl fmt::Display, ops: impl Into<Box<[ExprOp<F>]>>) -> Self {
        let ops = ops.into();
        assert!(ops.len() > 1, "should have at least one value and one return");
        Self { pretty: pretty.to_string(), ops }
    }
}

impl ExecutableExpr {
    pub fn execute(&self, tuple: &Tuple) -> Value {
        let mut ip = 0;
        let mut stack = vec![];
        loop {
            let op = &self.ops[ip];
            if matches!(op, ExprOp::Return) {
                break;
            }
            op.execute(&mut stack, &mut ip, tuple);
        }

        assert_eq!(
            stack.len(),
            1,
            "stack should have exactly one value after execution, had {}",
            stack.len()
        );

        stack.pop().unwrap()
    }
}

pub type ExecutableExprOp = ExprOp<Box<dyn Function>>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
#[omit_bounds]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
pub enum ExprOp<F = UntypedOid> {
    Push(#[omit_bounds] Value),
    Project { index: TupleIndex },
    MkArray { len: usize },
    Call { function: F },
    BinOp { op: BinOp },
    IfNeJmp { offset: u32 },
    Jmp { offset: u32 },
    Return,
}

static_assert_eq!(mem::size_of::<ExprOp>(), 32);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
pub enum BinOp {
    Eq,
}

impl ExprOp<Box<dyn Function>> {
    fn execute(&self, stack: &mut Vec<Value>, ip: &mut usize, tuple: &Tuple) {
        let value = match self {
            ExprOp::Project { index } => tuple[*index].clone(),
            ExprOp::Push(value) => value.clone(),
            ExprOp::MkArray { len } => {
                let array = stack.drain(stack.len() - *len..).collect::<Box<[Value]>>();
                Value::Array(array)
            }
            ExprOp::Call { function } => {
                let args = stack.drain(stack.len() - function.arity()..).collect::<Box<[Value]>>();
                function.call(args)
            }
            ExprOp::BinOp { op } => {
                let rhs = stack.pop().unwrap();
                let lhs = stack.pop().unwrap();
                match op {
                    BinOp::Eq => Value::Bool(lhs == rhs),
                }
            }
            ExprOp::IfNeJmp { offset } => {
                let rhs = stack.pop().unwrap();
                let lhs = stack.pop().unwrap();
                return *ip += if lhs != rhs { *offset as usize } else { 1 };
            }
            ExprOp::Jmp { offset } => return *ip += *offset as usize,
            ExprOp::Return => return,
        };

        stack.push(value);
        *ip += 1;
    }
}

mod fold;
#[cfg(test)]
mod tests;
