use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use nsql_catalog::{
    Catalog, Column, ColumnIndex, Container, EntityRef, Namespace, Oid, Table, Transaction,
};
use nsql_storage::schema::LogicalType;
use nsql_storage::tuple::TupleIndex;
use nsql_storage::value::Value;

use crate::Path;

#[derive(Debug, Clone)]
pub enum QueryPlan {
    TableRef { table_ref: TableRef, projection: Option<Box<[ColumnIndex]>> },
    Projection { source: Box<QueryPlan>, projection: Box<[Expr]> },
    Filter { source: Box<QueryPlan>, predicate: Expr },
    Values(Values),
    Limit(Box<QueryPlan>, u64),
    Empty,
}

impl QueryPlan {
    #[inline]
    pub fn limit(self: Box<Self>, limit: u64) -> Box<QueryPlan> {
        Box::new(QueryPlan::Limit(self, limit))
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
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct TableRef {
    pub namespace: Oid<Namespace>,
    pub table: Oid<Table>,
}

impl EntityRef for TableRef {
    type Entity = Table;

    type Container = Namespace;

    #[inline]
    fn container(self, catalog: &Catalog, tx: &Transaction) -> Arc<Self::Container> {
        catalog.get(tx, self.namespace).expect("namespace should exist for `tx`")
    }

    #[inline]
    fn entity_oid(self) -> Oid<Self::Entity> {
        self.table
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ColumnRef {
    pub table_ref: TableRef,
    pub column: Oid<Column>,
}

impl EntityRef for ColumnRef {
    type Entity = Column;

    type Container = Table;

    #[inline]
    fn container(self, catalog: &Catalog, tx: &Transaction) -> Arc<Self::Container> {
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
