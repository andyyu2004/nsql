#![deny(rust_2018_idioms)]

mod expr;
use nsql_catalog::{CreateColumnInfo, Name, Oid, Schema, Table};
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
        schema: Oid<Schema>,
        info: CreateTableInfo,
    },
    Insert {
        schema: Oid<Schema>,
        table: Oid<Table>,
        source: TableExpr,
        returning: Option<Vec<Expr>>,
    },
}
