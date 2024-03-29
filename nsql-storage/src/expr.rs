//! A serializable stack-based bytecode for evaluating expressions.
mod fold;

use std::str::FromStr;
use std::{fmt, mem};

use anyhow::Result;
use itertools::Itertools;
use nsql_core::{LogicalType, UntypedOid};
use nsql_util::static_assert_eq;
use rkyv::{Archive, Deserialize, Serialize};

use crate::tuple::TupleIndex;
use crate::value::{CastError, FromValue, Value};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
#[omit_bounds]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
pub struct TupleExpr<F = UntypedOid> {
    exprs: Box<[Expr<F>]>,
}

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

    #[inline]
    pub fn exprs(&self) -> &[Expr<F>] {
        self.exprs.as_ref()
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
pub struct Expr<F = UntypedOid> {
    ops: Box<[ExprOp<F>]>,
}

// this is just to allow egg stuff to compile for now
impl<F> FromStr for Expr<F> {
    type Err = ();

    fn from_str(_s: &str) -> std::result::Result<Self, Self::Err> {
        Err(())
    }
}

// avoid accidental size growth
static_assert_eq!(mem::size_of::<Expr>(), 16);

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
        write!(f, "{}", self.ops.iter().join(" "))
    }
}

impl<F> Expr<F> {
    #[inline]
    pub fn null() -> Self {
        Self { ops: Box::new([ExprOp::Push(Value::Null), ExprOp::Ret]) }
    }

    #[inline]
    pub fn is_literal(&self, value: impl Into<Value>) -> bool {
        matches!(self.ops.as_ref(), [ExprOp::Push(v), ExprOp::Ret] if v == &value.into())
    }

    #[inline]
    pub fn literal(value: impl Into<Value>) -> Self {
        let value = value.into();
        Self { ops: Box::new([ExprOp::Push(value), ExprOp::Ret]) }
    }

    pub fn call(function: F, args: impl IntoIterator<Item = Value>) -> Self {
        let mut ops = vec![];
        ops.extend(args.into_iter().map(ExprOp::Push));
        ops.push(ExprOp::Call { function });
        Self { ops: ops.into_boxed_slice() }
    }

    pub fn new(ops: impl Into<Box<[ExprOp<F>]>>) -> Self {
        let ops = ops.into();
        assert!(ops.len() > 1, "should have at least one value and one return");
        Self { ops }
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
    Ret,
}

static_assert_eq!(mem::size_of::<ExprOp>(), 24);

impl<F> fmt::Display for ExprOp<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExprOp::Push(value) => write!(f, ":push {value}",),
            ExprOp::Project { index } => write!(f, ":project {index}"),
            ExprOp::MkArray { len } => write!(f, ":mkarray {len}"),
            ExprOp::Call { function: _ } => write!(f, ":call"),
            ExprOp::IfNeJmp(_) => write!(f, ":jmpifne"),
            ExprOp::IfNullJmp(_) => write!(f, ":jmpifnull"),
            ExprOp::Jmp(_) => write!(f, ":jmp"),
            ExprOp::Dup => write!(f, ":dup"),
            ExprOp::Pop => write!(f, ":pop"),
            ExprOp::Ret => write!(f, ":ret"),
        }
    }
}

#[cfg(test)]
mod tests;
