mod eval;

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

pub use eval::EvalNotConst;
use nsql_catalog::{
    Catalog, Column, ColumnIndex, Container, EntityRef, Namespace, Oid, Table, Transaction,
};
use nsql_storage::schema::LogicalType;
use nsql_storage::tuple::TupleIndex;
use nsql_storage::value::Value;
use nsql_storage_engine::StorageEngine;

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

pub struct TableRef<S> {
    pub namespace: Oid<Namespace<S>>,
    pub table: Oid<Table<S>>,
}

impl<S> fmt::Debug for TableRef<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableRef")
            .field("namespace", &self.namespace)
            .field("table", &self.table)
            .finish()
    }
}

impl<S> Clone for TableRef<S> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Copy for TableRef<S> {}

impl<S: StorageEngine> EntityRef<S> for TableRef<S> {
    type Entity = Table<S>;

    type Container = Namespace<S>;

    #[inline]
    fn container(self, catalog: &Catalog<S>, tx: &Transaction) -> Arc<Self::Container> {
        catalog.get(tx, self.namespace).expect("namespace should exist for `tx`")
    }

    #[inline]
    fn entity_oid(self) -> Oid<Self::Entity> {
        self.table
    }
}

#[derive(Debug)]
pub struct ColumnRef<S> {
    pub table_ref: TableRef<S>,
    pub column: Oid<Column>,
}

impl<S> Clone for ColumnRef<S> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Copy for ColumnRef<S> {}

impl<S: StorageEngine> EntityRef<S> for ColumnRef<S> {
    type Entity = Column;

    type Container = Table<S>;

    #[inline]
    fn container(self, catalog: &Catalog<S>, tx: &Transaction) -> Arc<Self::Container> {
        self.table_ref.get(catalog, tx)
    }

    #[inline]
    fn entity_oid(self) -> Oid<Self::Entity> {
        self.column
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
