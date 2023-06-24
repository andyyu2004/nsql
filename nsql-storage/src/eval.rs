//! A serializable stack-based bytecode for evaluating expressions.
use core::fmt;
use std::error::Error;

use rkyv::{Archive, Deserialize, Serialize};

use crate::tuple::{Tuple, TupleIndex};
use crate::value::{CastError, FromValue, Value};

#[macro_export]
macro_rules! expr_project {
    ($($indices:expr),*) => {
        $crate::eval::TupleExpr::new([
            $($crate::eval::Expr::new([
                $crate::eval::ExprOp::Project($crate::tuple::TupleIndex::new($indices))
            ])),*
        ])
    };
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Archive, Serialize, Deserialize)]
pub struct TupleExpr {
    exprs: Box<[Expr]>,
}

impl TupleExpr {
    #[inline]
    pub fn new(exprs: impl Into<Box<[Expr]>>) -> Self {
        Self { exprs: exprs.into() }
    }

    #[inline]
    pub fn eval(&self, tuple: &Tuple) -> Result<Tuple, ExecutionError> {
        self.exprs.iter().map(|expr| expr.execute(tuple)).collect()
    }
}

impl FromValue for TupleExpr {
    #[inline]
    fn from_value(value: Value) -> Result<Self, CastError<Self>> {
        let bytes = value.cast_non_null::<Box<[u8]>>().map_err(CastError::cast)?;
        // FIXME we need to validate the bytes
        Ok(unsafe { nsql_rkyv::deserialize_raw(&bytes) })
    }
}

impl Into<Value> for TupleExpr {
    #[inline]
    fn into(self) -> Value {
        let bytes = nsql_rkyv::to_bytes(&self);
        Value::Bytea(bytes.as_ref().into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Archive, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Expr {
    ops: Box<[ExprOp]>,
}

impl Expr {
    pub fn new(ops: impl Into<Box<[ExprOp]>>) -> Self {
        Self { ops: ops.into() }
    }

    pub fn execute(&self, tuple: &Tuple) -> Result<Value, ExecutionError> {
        let mut stack = vec![];
        for op in &self.ops[..] {
            op.execute(&mut stack, tuple)?;
        }

        assert_eq!(stack.len(), 1, "stack should have exactly one value after execution");
        Ok(stack.pop().unwrap())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Archive, Serialize, Deserialize)]
pub enum ExprOp {
    Project(TupleIndex),
}

impl ExprOp {
    fn execute(&self, stack: &mut Vec<Value>, tuple: &Tuple) -> Result<(), ExecutionError> {
        match self {
            ExprOp::Project(idx) => {
                stack.push(tuple[*idx].clone());
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub enum ExecutionError {}

impl fmt::Display for ExecutionError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for ExecutionError {}

#[cfg(test)]
mod tests;
