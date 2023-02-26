#![deny(rust_2018_idioms)]

mod expr;
use nsql_catalog::{CreateColumnInfo, Name, Namespace, Oid, Table};
pub use rust_decimal::Decimal;

pub use self::expr::{Expr, Literal, TableExpr, Values};

#[derive(Debug, Clone)]
pub struct CreateTableInfo {
    pub name: Name,
    pub columns: Vec<CreateColumnInfo>,
}

#[derive(Debug, Clone)]
pub enum Stmt {
    CreateTable {
        schema: Oid<Namespace>,
        info: CreateTableInfo,
    },
    Insert {
        schema: Oid<Namespace>,
        table: Oid<Table>,
        source: TableExpr,
        returning: Option<Vec<Expr>>,
    },
}
