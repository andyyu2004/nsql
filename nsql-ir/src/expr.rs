use std::fmt;
use std::sync::Arc;

use nsql_catalog::{Catalog, Column, Container, EntityRef, Namespace, Oid, Table, Transaction};
use nsql_core::schema::LogicalType;
use nsql_storage::tuple::TupleIndex;
use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub enum TableExpr {
    TableRef(TableRef),
    Selection(Selection),
    Values(Values),
    Limit(Box<TableExpr>, u64),
    Empty,
}

impl TableExpr {
    #[inline]
    pub fn limit(self, limit: u64) -> TableExpr {
        TableExpr::Limit(Box::new(self), limit)
    }
}

#[derive(Debug, Clone)]
pub struct Selection {
    pub source: Box<TableExpr>,
    pub projection: Vec<Expr>,
}

#[derive(Debug, Clone)]
pub struct Expr {
    pub ty: LogicalType,
    pub kind: ExprKind,
}

#[derive(Debug, Clone)]
pub enum ExprKind {
    Literal(Literal),
    ColumnRef(TupleIndex),
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
    fn container(
        self,
        catalog: &Catalog,
        tx: &Transaction,
    ) -> nsql_catalog::Result<Arc<Self::Container>> {
        Ok(catalog.get(tx, self.namespace)?.expect("namespace should exist for `tx`"))
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
    fn container(
        self,
        catalog: &Catalog,
        tx: &Transaction,
    ) -> nsql_catalog::Result<Arc<Self::Container>> {
        self.table_ref.get(catalog, tx)
    }

    #[inline]
    fn entity_oid(self) -> Oid<Self::Entity> {
        self.column
    }
}

#[derive(Debug, Clone)]
pub enum Literal {
    Null,
    Bool(bool),
    Decimal(Decimal),
    String(String),
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::Null => write!(f, "NULL"),
            Literal::Bool(b) => write!(f, "{b}"),
            Literal::Decimal(n) => write!(f, "{n}"),
            Literal::String(s) => write!(f, "{s}"),
        }
    }
}

pub type Values = Vec<Vec<Expr>>;
