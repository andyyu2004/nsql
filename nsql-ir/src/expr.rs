mod eval;

use std::fmt;
use std::ops::Deref;

pub use eval::EvalNotConst;
use itertools::Itertools;
use nsql_catalog::{ColumnIndex, Table};
use nsql_core::{LogicalType, Oid};
use nsql_storage::tuple::TupleIndex;
use nsql_storage::value::Value;

use crate::Path;

#[derive(Debug, Clone)]
pub enum QueryPlan {
    TableScan { table: Oid<Table>, projection: Option<Box<[ColumnIndex]>> },
    Projection { source: Box<QueryPlan>, projection: Box<[Expr]> },
    Filter { source: Box<QueryPlan>, predicate: Expr },
    Values(Values),
    Limit(Box<QueryPlan>, u64),
    Order(Box<QueryPlan>, Box<[OrderExpr]>),
    Empty,
}

#[derive(Debug, Clone)]
pub struct OrderExpr {
    pub expr: Expr,
    pub asc: bool,
    pub nulls_first: bool,
}

impl QueryPlan {
    #[inline]
    pub fn limit(self: Box<Self>, limit: u64) -> Box<QueryPlan> {
        Box::new(QueryPlan::Limit(self, limit))
    }

    pub fn order_by(self: Box<Self>, order: impl Into<Box<[OrderExpr]>>) -> Box<QueryPlan> {
        Box::new(QueryPlan::Order(self, order.into()))
    }

    #[inline]
    pub fn filter(self: Box<Self>, predicate: Expr) -> Box<QueryPlan> {
        assert!(matches!(predicate.ty, LogicalType::Bool | LogicalType::Null));
        Box::new(QueryPlan::Filter { source: self, predicate })
    }

    #[inline]
    pub fn project(self: Box<Self>, projection: impl Into<Box<[Expr]>>) -> Box<QueryPlan> {
        Box::new(QueryPlan::Projection { source: self, projection: projection.into() })
    }
}

#[derive(Debug, Clone)]
pub struct Expr {
    pub ty: LogicalType,
    pub kind: ExprKind,
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind)
    }
}

#[derive(Debug, Clone)]
pub enum ExprKind {
    Value(Value),
    Array(Box<[Expr]>),
    BinOp {
        op: BinOp,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
    },
    ColumnRef {
        /// A display path for the column (for pretty printing etc)
        path: Path,
        /// An index into the tuple the expression is evaluated against
        index: TupleIndex,
    },
}

impl fmt::Display for ExprKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            ExprKind::Value(value) => write!(f, "{value}"),
            ExprKind::ColumnRef { path, .. } => write!(f, "{path}"),
            ExprKind::BinOp { op, lhs, rhs } => write!(f, "({lhs} {op} {rhs})"),
            ExprKind::Array(exprs) => write!(f, "[{}]", exprs.iter().format(", ")),
        }
    }
}

#[derive(Debug, Clone)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}

impl fmt::Display for BinOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            BinOp::Add => write!(f, "+"),
            BinOp::Sub => write!(f, "-"),
            BinOp::Mul => write!(f, "*"),
            BinOp::Div => write!(f, "/"),
            BinOp::Mod => write!(f, "%"),
            BinOp::Eq => write!(f, "="),
            BinOp::Ne => write!(f, "!="),
            BinOp::Lt => write!(f, "<"),
            BinOp::Le => write!(f, "<="),
            BinOp::Gt => write!(f, ">"),
            BinOp::Ge => write!(f, ">="),
            BinOp::And => write!(f, "AND"),
            BinOp::Or => write!(f, "OR"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Values(Vec<Vec<Expr>>);

impl Values {
    /// Create a new `Values` expression
    /// The `values` must be a non-empty vector of non-empty vectors of expressions
    /// The inner vectors must each be the same length
    ///
    /// Panics if these conditions are not met
    #[inline]
    pub fn new(values: Vec<Vec<Expr>>) -> Self {
        assert!(!values.is_empty(), "values must be non-empty");
        let len = values[0].len();
        assert!(values.iter().all(|v| v.len() == len), "all inner vectors must be the same length");
        Self(values)
    }
}

impl Deref for Values {
    type Target = Vec<Vec<Expr>>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
