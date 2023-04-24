use std::fmt;
use std::sync::Arc;

use nsql_catalog::{Catalog, Column, Container, Namespace, Oid, Table, Transaction};
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

impl TableRef {
    pub fn get(self, catalog: &Catalog, tx: &Transaction) -> nsql_catalog::Result<Arc<Table>> {
        let namespace = catalog.get::<Namespace>(tx, self.namespace)?.unwrap();
        Ok(namespace.get::<Table>(tx, self.table)?.unwrap())
    }

    pub fn delete(self, catalog: &Catalog, tx: &Transaction) -> nsql_catalog::Result<()> {
        let namespace = catalog.get::<Namespace>(tx, self.namespace)?.unwrap();
        namespace.delete::<Table>(tx, self.table);
        Ok(())
    }
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
