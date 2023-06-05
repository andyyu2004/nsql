#![feature(anonymous_lifetime_in_impl_trait)]
#![deny(rust_2018_idioms)]

pub mod expr;
use std::fmt;

use nsql_catalog::{CreateColumnInfo, Namespace, Oid, TableRef};
use nsql_core::Name;
pub use nsql_storage::tuple::TupleIndex;
pub use nsql_storage::value::{Decimal, Value};

pub use self::expr::*;

#[derive(Clone)]
pub struct CreateTableInfo<S> {
    pub name: Name,
    pub namespace: Oid<Namespace<S>>,
    pub columns: Vec<CreateColumnInfo>,
}

impl<S> fmt::Debug for CreateTableInfo<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateTableInfo")
            .field("name", &self.name)
            .field("namespace", &self.namespace)
            .field("columns", &self.columns)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct CreateNamespaceInfo {
    pub name: Name,
    pub if_not_exists: bool,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum TransactionMode {
    ReadOnly,
    ReadWrite,
}

impl fmt::Display for TransactionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadWrite => write!(f, "read write"),
            Self::ReadOnly => write!(f, "read only"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum TransactionStmtKind {
    Begin(TransactionMode),
    Commit,
    Abort,
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

#[derive(Clone)]
pub enum EntityRef<S> {
    Table(TableRef<S>),
}

impl<S> fmt::Debug for EntityRef<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Table(table) => write!(f, "{table:?}"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Stmt<S> {
    Show(ObjectType),
    Drop(Vec<EntityRef<S>>),
    Transaction(TransactionStmtKind),
    CreateNamespace(CreateNamespaceInfo),
    CreateTable(CreateTableInfo<S>),
    Query(Box<QueryPlan<S>>),
    Explain(ExplainMode, Box<Stmt<S>>),
    Insert {
        table_ref: TableRef<S>,
        projection: Box<[Expr]>,
        source: Box<QueryPlan<S>>,
        returning: Option<Box<[Expr]>>,
    },
    Update {
        table_ref: TableRef<S>,
        source: Box<QueryPlan<S>>,
        returning: Option<Box<[Expr]>>,
    },
}

impl<S> Stmt<S> {
    pub fn required_transaction_mode(&self) -> TransactionMode {
        match self {
            Stmt::Drop(_)
            | Stmt::CreateNamespace(_)
            | Stmt::CreateTable(_)
            | Stmt::Update { .. }
            | Stmt::Insert { .. } => TransactionMode::ReadWrite,
            Stmt::Transaction(kind) => match kind {
                TransactionStmtKind::Begin(mode) => *mode,
                TransactionStmtKind::Commit | TransactionStmtKind::Abort => {
                    todo!("when would this occur?")
                }
            },
            Stmt::Show(..) | Stmt::Query(..) | Stmt::Explain(..) => TransactionMode::ReadOnly,
        }
    }
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

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Path {
    Qualified { prefix: Box<Path>, name: Name },
    Unqualified(Name),
}

impl Path {
    pub fn qualified(prefix: Path, name: Name) -> Path {
        Path::Qualified { prefix: Box::new(prefix), name }
    }

    pub fn unqualified(name: impl Into<Name>) -> Path {
        Path::Unqualified(name.into())
    }

    pub fn prefix(&self) -> Option<&Path> {
        match self {
            Path::Qualified { prefix, .. } => Some(prefix),
            Path::Unqualified { .. } => None,
        }
    }

    pub fn name(&self) -> Name {
        match self {
            Path::Qualified { name, .. } => name.as_str().into(),
            Path::Unqualified(name) => name.as_str().into(),
        }
    }
}

impl fmt::Debug for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Path::Qualified { prefix, name: object } => write!(f, "{prefix}.{object}"),
            Path::Unqualified(name) => write!(f, "{name}"),
        }
    }
}
