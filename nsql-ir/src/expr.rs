use std::fmt;

use nsql_catalog::{Column, Namespace, Oid, Table};
use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub enum TableExpr {
    TableRef(TableRef),
    Selection(Selection),
    Values(Values),
    Empty,
}

#[derive(Debug, Clone)]
pub struct Selection {
    pub source: Box<TableExpr>,
    pub projection: Vec<Expr>,
}

#[derive(Debug, Clone)]
pub enum Expr {
    Literal(Literal),
    ColumnRef(ColumnRef, usize),
}

#[derive(Debug, Copy, Clone)]
pub struct TableRef {
    pub namespace: Oid<Namespace>,
    pub table: Oid<Table>,
}

#[derive(Debug, Copy, Clone)]
pub struct ColumnRef {
    pub table_ref: TableRef,
    pub column: Oid<Column>,
}

#[derive(Debug, Clone)]
pub enum Literal {
    Null,
    Bool(bool),
    Decimal(Decimal),
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::Null => write!(f, "NULL"),
            Literal::Bool(b) => write!(f, "{b}"),
            Literal::Decimal(n) => write!(f, "{n}"),
        }
    }
}

pub type Values = Vec<Vec<Expr>>;
