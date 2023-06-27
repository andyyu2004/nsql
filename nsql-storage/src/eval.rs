//! A serializable stack-based bytecode for evaluating expressions.

use std::fmt;

use anyhow::Result;
use itertools::Itertools;
use nsql_core::{LogicalType, UntypedOid};
use nsql_storage_engine::Transaction;
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

pub trait Function {
    /// The number of arguments this function takes.
    fn arity(&self) -> usize;

    fn call(&self, args: Box<[Value]>) -> Value;
}

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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
#[omit_bounds]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
pub struct TupleExpr {
    exprs: Box<[Expr]>,
}

impl fmt::Display for TupleExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.exprs.iter().join(", "))
    }
}

impl TupleExpr {
    #[inline]
    pub fn new(exprs: impl Into<Box<[Expr]>>) -> Self {
        Self { exprs: exprs.into() }
    }

    #[inline]
    pub fn eval<'env, S>(
        &self,
        catalog: &dyn FunctionCatalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
        tuple: &Tuple,
    ) -> Result<Tuple> {
        self.exprs.iter().map(|expr| expr.execute(catalog, tx, tuple)).collect()
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
pub struct Expr {
    pretty: String,
    ops: Box<[ExprOp]>,
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.pretty)
    }
}

impl Expr {
    pub fn new(pretty: impl fmt::Display, ops: impl Into<Box<[ExprOp]>>) -> Self {
        Self { pretty: pretty.to_string(), ops: ops.into() }
    }

    pub fn execute<'env, S>(
        &self,
        catalog: &dyn FunctionCatalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
        tuple: &Tuple,
    ) -> Result<Value> {
        let mut stack = vec![];
        for op in &self.ops[..] {
            op.execute(&mut stack, catalog, tx, tuple)?;
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
#[omit_bounds]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
pub enum ExprOp {
    Push(#[omit_bounds] Value),
    Project { index: TupleIndex },
    MkArray { len: usize },
    Call { function_oid: UntypedOid },
    BinOp { op: BinOp },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
pub enum BinOp {
    Eq,
}

impl ExprOp {
    fn execute<'env, S>(
        &self,
        stack: &mut Vec<Value>,
        catalog: &dyn FunctionCatalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
        tuple: &Tuple,
    ) -> Result<()> {
        let value = match self {
            ExprOp::Project { index } => tuple[*index].clone(),
            ExprOp::Push(value) => value.clone(),
            ExprOp::MkArray { len } => {
                let array = stack.drain(stack.len() - *len..).collect::<Box<[Value]>>();
                Value::Array(array)
            }
            ExprOp::Call { function_oid } => {
                let f = catalog.get_function(tx, *function_oid)?;
                let args = stack.drain(stack.len() - f.arity()..).collect::<Box<[Value]>>();
                f.call(args)
            }
            ExprOp::BinOp { op } => {
                let rhs = stack.pop().unwrap();
                let lhs = stack.pop().unwrap();
                match op {
                    BinOp::Eq => Value::Bool(lhs == rhs),
                }
            }
        };

        stack.push(value);

        Ok(())
    }
}

#[cfg(test)]
mod tests;
