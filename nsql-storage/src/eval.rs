//! A serializable stack-based bytecode for evaluating expressions.

use std::str::FromStr;
use std::sync::Arc;
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
    ) -> Result<Arc<dyn ScalarFunction>>;
}

pub trait ScalarFunction: fmt::Debug + Send + Sync + 'static {
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

pub type ExecutableTupleExpr = TupleExpr<Arc<dyn ScalarFunction>>;

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
}

impl TupleExpr {
    /// Prepare this tuple expression for evaluation.
    // This resolves any function oids and replaces them with the actual function.
    pub fn prepare<'env, S>(
        self,
        catalog: &dyn FunctionCatalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<ExecutableTupleExpr> {
        self.map(|oid| catalog.get_function(tx, oid))
    }
}

impl Expr {
    /// Prepare this expression for evaluation.
    // This resolves any function oids and replaces them with the actual function.
    pub fn prepare<'env, S>(
        self,
        catalog: &dyn FunctionCatalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<ExecutableExpr> {
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

pub type ExecutableExpr = Expr<Arc<dyn ScalarFunction>>;

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
    pub fn null() -> Self {
        Self { pretty: "NULL".into(), ops: Box::new([ExprOp::Push(Value::Null), ExprOp::Return]) }
    }

    pub fn new(pretty: impl fmt::Display, ops: impl Into<Box<[ExprOp<F>]>>) -> Self {
        let ops = ops.into();
        assert!(ops.len() > 1, "should have at least one value and one return");
        Self { pretty: pretty.to_string().into(), ops }
    }

    #[inline]
    pub fn ops(&self) -> &[ExprOp<F>] {
        self.ops.as_ref()
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

pub type ExecutableExprOp = ExprOp<Arc<dyn ScalarFunction>>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
#[omit_bounds]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
pub enum ExprOp<F = UntypedOid> {
    Push(#[omit_bounds] Value),
    Project { index: TupleIndex },
    MkArray { len: usize },
    Call { function: F },
    IfNeJmp { offset: u32 },
    Jmp { offset: u32 },
    Return,
}

static_assert_eq!(mem::size_of::<ExprOp>(), 40);

impl ExprOp<Arc<dyn ScalarFunction>> {
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
