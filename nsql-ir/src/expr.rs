mod eval;

use std::fmt;
use std::ops::Deref;

pub use eval::EvalNotConst;
use nsql_catalog::schema::LogicalType;
use nsql_catalog::{ColumnIndex, TableRef};
use nsql_storage::tuple::TupleIndex;
use nsql_storage::value::Value;

use crate::Path;

#[derive(Debug, Clone)]
pub enum QueryPlan<S> {
    TableRef { table_ref: TableRef<S>, projection: Option<Box<[ColumnIndex]>> },
    Projection { source: Box<QueryPlan<S>>, projection: Box<[Expr]> },
    Filter { source: Box<QueryPlan<S>>, predicate: Expr },
    Values(Values),
    Limit(Box<QueryPlan<S>>, u64),
    Empty,
}

impl<S> QueryPlan<S> {
    #[inline]
    pub fn limit(self: Box<Self>, limit: u64) -> Box<QueryPlan<S>> {
        Box::new(QueryPlan::Limit(self, limit))
    }

    #[inline]
    pub fn filter(self: Box<Self>, predicate: Expr) -> Box<QueryPlan<S>> {
        assert!(matches!(predicate.ty, LogicalType::Bool | LogicalType::Null));
        Box::new(QueryPlan::Filter { source: self, predicate })
    }

    #[inline]
    pub fn project(self: Box<Self>, projection: impl Into<Box<[Expr]>>) -> Box<QueryPlan<S>> {
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
