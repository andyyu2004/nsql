#![feature(anonymous_lifetime_in_impl_trait)]
#![deny(rust_2018_idioms)]

pub mod expr;
pub mod visit;
use std::{fmt, mem};

use nsql_catalog::{
    ColumnIndex, CreateColumnInfo, CreateNamespaceInfo, Function, Namespace, Table,
};
use nsql_core::{LogicalType, Name, Oid, Schema};
pub use nsql_storage::tuple::TupleIndex;
pub use nsql_storage::value::{Decimal, Value};

pub use self::expr::*;

#[derive(Clone)]
pub struct CreateTableInfo {
    pub name: Name,
    pub namespace: Oid<Namespace>,
    pub columns: Vec<CreateColumnInfo>,
}

impl fmt::Debug for CreateTableInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateTableInfo")
            .field("name", &self.name)
            .field("namespace", &self.namespace)
            .field("columns", &self.columns)
            .finish()
    }
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

#[derive(Clone, Copy)]
pub enum EntityRef {
    Table(Oid<Table>),
}

impl fmt::Debug for EntityRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Table(table) => write!(f, "{table:?}"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Plan {
    Show(ObjectType),
    Drop(Vec<EntityRef>),
    Transaction(TransactionStmtKind),
    CreateNamespace(CreateNamespaceInfo),
    CreateTable(CreateTableInfo),
    Aggregate {
        /// aggregate functions (and it's args) to apply
        functions: Box<[(Function, Box<[Expr]>)]>,
        source: Box<Plan>,
        group_by: Box<[Expr]>,
        schema: Schema,
    },
    TableScan {
        table: Oid<Table>,
        projection: Option<Box<[ColumnIndex]>>,
        projected_schema: Schema,
    },
    Projection {
        source: Box<Plan>,
        projection: Box<[Expr]>,
        projected_schema: Schema,
    },
    Filter {
        source: Box<Plan>,
        predicate: Expr,
    },
    // maybe this can be implemented as a standard table function in the future (change the sqlparser dialect to duckdb if so)
    Unnest {
        schema: Schema,
        expr: Expr,
    },
    Values {
        values: Values,
        schema: Schema,
    },
    Join {
        schema: Schema,
        join: Join,
        lhs: Box<Plan>,
        rhs: Box<Plan>,
    },
    Limit {
        source: Box<Plan>,
        limit: u64,
    },
    Order {
        source: Box<Plan>,
        order: Box<[OrderExpr]>,
    },
    Empty,
    Explain(ExplainMode, Box<Plan>),
    Insert {
        table: Oid<Table>,
        source: Box<Plan>,
        returning: Option<Box<[Expr]>>,
        schema: Schema,
    },
    Update {
        table: Oid<Table>,
        source: Box<Plan>,
        returning: Option<Box<[Expr]>>,
        schema: Schema,
    },
}

impl Plan {
    #[inline]
    pub fn take(&mut self) -> Self {
        mem::replace(self, Plan::Empty)
    }

    pub fn required_transaction_mode(&self) -> TransactionMode {
        match self {
            Plan::Drop(_)
            | Plan::CreateNamespace(_)
            | Plan::CreateTable(_)
            | Plan::Update { .. }
            | Plan::Insert { .. } => TransactionMode::ReadWrite,
            Plan::Transaction(kind) => match kind {
                TransactionStmtKind::Begin(mode) => *mode,
                TransactionStmtKind::Commit | TransactionStmtKind::Abort => {
                    TransactionMode::ReadOnly
                }
            },
            // even though `explain` doesn't execute the plan, the planning stage currently
            // still checks we have a write transaction available
            Plan::Explain(_, inner) => inner.required_transaction_mode(),
            Plan::Show(..) | Plan::TableScan { .. } => TransactionMode::ReadOnly,
            Plan::Limit { source, .. }
            | Plan::Aggregate { source, .. }
            | Plan::Order { source, .. }
            | Plan::Projection { source, .. }
            | Plan::Filter { source, .. } => source.required_transaction_mode(),
            Plan::Empty | Plan::Unnest { .. } | Plan::Values { .. } => TransactionMode::ReadOnly,
            Plan::Join { lhs, rhs, .. } => {
                lhs.required_transaction_mode().max(rhs.required_transaction_mode())
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum Join<E = Expr> {
    Inner(JoinConstraint<E>),
    Left(JoinConstraint<E>),
    Right(JoinConstraint<E>),
    Full(JoinConstraint<E>),
    Cross,
}

impl<E> Join<E> {
    #[inline]
    pub fn is_left(&self) -> bool {
        matches!(self, Join::Left(_) | Join::Full(_))
    }

    #[inline]
    pub fn is_right(&self) -> bool {
        matches!(self, Join::Right(_) | Join::Full(_))
    }

    pub fn map<X>(self, f: impl FnOnce(E) -> X) -> Join<X> {
        match self {
            Join::Inner(constraint) => Join::Inner(constraint.map(f)),
            Join::Left(constraint) => Join::Left(constraint.map(f)),
            Join::Right(constraint) => Join::Right(constraint.map(f)),
            Join::Full(constraint) => Join::Full(constraint.map(f)),
            Join::Cross => Join::Cross,
        }
    }
}

impl<E: fmt::Display> fmt::Display for Join<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Join::Inner(constraint) => write!(f, "INNER JOIN{}", constraint),
            Join::Left(constraint) => write!(f, "LEFT JOIN{}", constraint),
            Join::Full(constraint) => write!(f, "FULL JOIN{}", constraint),
            Join::Right(constraint) => write!(f, "RIGHT JOIN{}", constraint),
            Join::Cross => write!(f, "CROSS JOIN"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum JoinConstraint<E = Expr> {
    On(E),
    None,
}

impl<E> JoinConstraint<E> {
    #[inline]
    pub fn map<X>(self, f: impl FnOnce(E) -> X) -> JoinConstraint<X> {
        match self {
            JoinConstraint::On(expr) => JoinConstraint::On(f(expr)),
            JoinConstraint::None => JoinConstraint::None,
        }
    }
}

impl<E: fmt::Display> fmt::Display for JoinConstraint<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinConstraint::On(expr) => write!(f, " ON {}", expr),
            JoinConstraint::None => Ok(()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OrderExpr<E = Expr> {
    pub expr: E,
    pub asc: bool,
}

impl Plan {
    pub fn schema(&self) -> &[LogicalType] {
        match self {
            Plan::TableScan { projected_schema, .. }
            | Plan::Projection { projected_schema, .. } => projected_schema,
            Plan::Aggregate { schema, .. }
            | Plan::Join { schema, .. }
            | Plan::Unnest { schema, .. }
            | Plan::Insert { schema, .. }
            | Plan::Update { schema, .. }
            | Plan::Values { schema, .. } => schema,
            Plan::Filter { source, .. }
            | Plan::Limit { source, .. }
            | Plan::Order { source, .. } => source.schema(),
            Plan::Empty => &[],
            Plan::Show(..)
            | Plan::Drop(..)
            | Plan::Transaction(..)
            | Plan::CreateNamespace(..)
            | Plan::CreateTable(..)
            | Plan::Explain(..) => todo!("not sure if these cases will ever be hit"),
        }
    }

    pub fn values(values: Values) -> Self {
        let schema = values
            .iter()
            .map(|row| row.iter().map(|expr| expr.ty.clone()).collect())
            .next()
            .expect("values should be non-empty");
        Plan::Values { values, schema }
    }

    /// Construct an `unnest` plan. `expr` must have an array type
    pub fn unnest(expr: Expr) -> Self {
        match &expr.ty {
            LogicalType::Array(element_type) => {
                Plan::Unnest { schema: Schema::new([*element_type.clone()]), expr }
            }
            _ => panic!("unnest expression must be an array"),
        }
    }

    pub fn aggregate(
        self: Box<Self>,
        functions: Box<[(Function, Box<[Expr]>)]>,
        group_by: Box<[Expr]>,
    ) -> Box<Self> {
        assert!(group_by.is_empty());
        let schema = functions.iter().map(|(f, _args)| f.return_type()).collect();
        Box::new(Plan::Aggregate { schema, functions, group_by, source: self })
    }

    #[inline]
    pub fn limit(self: Box<Self>, limit: u64) -> Box<Plan> {
        Box::new(Plan::Limit { source: self, limit })
    }

    #[inline]
    pub fn order_by(self: Box<Self>, order: impl Into<Box<[OrderExpr]>>) -> Box<Plan> {
        Box::new(Plan::Order { source: self, order: order.into() })
    }

    pub fn join(self: Box<Self>, join: Join, rhs: Box<Plan>) -> Box<Plan> {
        let schema = self.schema().iter().chain(rhs.schema()).cloned().collect();
        Box::new(Plan::Join { schema, join, lhs: self, rhs })
    }

    #[inline]
    pub fn filter(self: Box<Self>, predicate: Expr) -> Box<Plan> {
        assert!(matches!(predicate.ty, LogicalType::Bool | LogicalType::Null));
        Box::new(Plan::Filter { source: self, predicate })
    }

    #[inline]
    pub fn project(self: Box<Self>, projection: impl Into<Box<[Expr]>>) -> Box<Plan> {
        let projection = projection.into();
        let projected_schema = projection.iter().map(|expr| expr.ty.clone()).collect();
        Box::new(Plan::Projection { source: self, projection, projected_schema })
    }
}

#[derive(Debug, Clone)]
pub enum ExplainMode {
    /// Show the physical query plan
    Physical,
    /// Show the pipelines
    Pipeline,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct QPath {
    pub prefix: Box<Path>,
    pub name: Name,
}

impl QPath {
    #[inline]
    pub fn new(prefix: impl Into<Path>, name: impl Into<Name>) -> Self {
        Self { prefix: Box::new(prefix.into()), name: name.into() }
    }
}

impl fmt::Debug for QPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl fmt::Display for QPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.prefix, self.name)
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Path {
    Qualified(QPath),
    Unqualified(Name),
}

impl From<&str> for Path {
    #[inline]
    fn from(s: &str) -> Self {
        Path::Unqualified(s.into())
    }
}

impl From<Name> for Path {
    #[inline]
    fn from(s: Name) -> Self {
        Path::Unqualified(s)
    }
}

impl Path {
    pub fn qualified(prefix: Path, name: Name) -> Path {
        Path::Qualified(QPath { prefix: Box::new(prefix), name })
    }

    pub fn unqualified(name: impl Into<Name>) -> Path {
        Path::Unqualified(name.into())
    }

    pub fn prefix(&self) -> Option<&Path> {
        match self {
            Path::Qualified(QPath { prefix, .. }) => Some(prefix),
            Path::Unqualified { .. } => None,
        }
    }

    pub fn name(&self) -> Name {
        match self {
            Path::Qualified(QPath { name, .. }) => name.as_str().into(),
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
            Path::Qualified(qpath) => write!(f, "{qpath}"),
            Path::Unqualified(name) => write!(f, "{name}"),
        }
    }
}
