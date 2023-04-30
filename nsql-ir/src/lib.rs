#![deny(rust_2018_idioms)]

mod expr;
use nsql_catalog::{CreateColumnInfo, Namespace, Oid};
use nsql_core::Name;
pub use nsql_storage::tuple::TupleIndex;
pub use rust_decimal::Decimal;

pub use self::expr::*;

#[derive(Debug, Clone)]
pub struct CreateTableInfo {
    pub name: Name,
    pub namespace: Oid<Namespace>,
    pub columns: Vec<CreateColumnInfo>,
}

#[derive(Debug, Clone)]
pub struct CreateNamespaceInfo {
    pub name: Name,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub enum TransactionKind {
    Begin,
    Commit,
    Rollback,
}

#[derive(Debug, Clone)]
pub enum ObjectType {
    Table,
}

#[derive(Debug, Clone)]
pub enum EntityRef {
    Table(TableRef),
}

#[derive(Debug, Clone)]
pub enum Stmt {
    Show(ObjectType),
    Drop(Vec<EntityRef>),
    Transaction(TransactionKind),
    CreateNamespace(CreateNamespaceInfo),
    CreateTable(CreateTableInfo),
    Insert {
        table_ref: TableRef,
        projection: Vec<Expr>,
        source: TableExpr,
        returning: Option<Vec<Expr>>,
    },
    Query(TableExpr),
}

#[derive(Debug, Clone)]
pub enum Query {}
