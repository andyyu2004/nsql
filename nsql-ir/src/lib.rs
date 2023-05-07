#![deny(rust_2018_idioms)]

mod expr;
use nsql_catalog::{CreateColumnInfo, Namespace, Oid};
use nsql_core::Name;
pub use nsql_storage::tuple::TupleIndex;
pub use nsql_storage::value::{Decimal, Value};

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
        projection: Box<[Expr]>,
        source: Box<QueryPlan>,
        returning: Option<Box<[Expr]>>,
    },
    Update {
        table_ref: TableRef,
        assignments: Box<[Assignment]>,
        filter: Option<Expr>,
        returning: Option<Box<[Expr]>>,
    },
    Query(Box<QueryPlan>),
}

#[derive(Debug, Clone)]
pub struct Assignment {
    pub column_ref: ColumnRef,
    pub expr: Expr,
}

#[derive(Debug, Clone)]
pub enum Query {}
