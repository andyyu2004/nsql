#![deny(rust_2018_idioms)]

mod expr;
use std::fmt;

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

impl fmt::Display for ObjectType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Table => write!(f, "table"),
        }
    }
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
    Query(Box<QueryPlan>),
    Explain(ExplainMode, Box<Stmt>),
    Insert {
        table_ref: TableRef,
        projection: Box<[Expr]>,
        source: Box<QueryPlan>,
        returning: Option<Box<[Expr]>>,
    },
    Update {
        table_ref: TableRef,
        source: Box<QueryPlan>,
        returning: Option<Box<[Expr]>>,
    },
}

#[derive(Debug, Clone)]
pub enum ExplainMode {
    /// Show the physical query plan
    Physical,
    /// Show the pipelines
    Pipeline,
}

#[derive(Debug, Clone)]
pub enum Query {}
