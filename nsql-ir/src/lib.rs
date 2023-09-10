#![deny(rust_2018_idioms)]

pub mod expr;
pub mod fold;
mod validate;
pub mod visit;
use std::ops::ControlFlow;
use std::str::FromStr;
use std::{fmt, mem};

use anyhow::bail;
use itertools::Itertools;
use nsql_catalog::{ColumnIndex, CreateColumnInfo, Namespace};
pub use nsql_catalog::{Function, Operator, Table};
use nsql_core::{LogicalType, Name, Oid, Schema};
pub use nsql_storage::tuple::TupleIndex;
pub use nsql_storage::value::{Decimal, Value};

pub use self::expr::*;
use crate::visit::Visitor;

#[derive(Clone, PartialEq, Eq, Hash)]
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

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransactionStmt {
    Begin(TransactionMode),
    Commit,
    Abort,
}

impl fmt::Display for TransactionStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionStmt::Begin(mode) => write!(f, "BEGIN {mode}"),
            TransactionStmt::Commit => write!(f, "COMMIT"),
            TransactionStmt::Abort => write!(f, "ABORT"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
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

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VariableScope {
    Local,
    Global,
}

impl fmt::Display for VariableScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local => write!(f, "LOCAL"),
            Self::Global => write!(f, "GLOBAL"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Cte {
    /// The name of the cte
    pub name: Name,
    /// The plan to populate the materialized cte
    pub plan: Box<QueryPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub enum QueryPlan {
    #[default]
    DummyScan,
    // introduce a cte
    Cte {
        cte: Cte,
        child: Box<QueryPlan>,
    },
    // consume a cte
    CteScan {
        name: Name,
        schema: Schema,
    },
    Aggregate {
        aggregates: Box<[FunctionCall]>,
        source: Box<QueryPlan>,
        group_by: Box<[Expr]>,
        schema: Schema,
    },
    TableScan {
        table: Oid<Table>,
        projection: Option<Box<[ColumnIndex]>>,
        projected_schema: Schema,
    },
    Projection {
        source: Box<QueryPlan>,
        projection: Box<[Expr]>,
        projected_schema: Schema,
    },
    Filter {
        source: Box<QueryPlan>,
        predicate: Expr,
    },
    // maybe this can be implemented as a standard table function in the future (change the sqlparser dialect to duckdb if so)
    Unnest {
        schema: Schema,
        expr: Expr,
    },
    Values {
        schema: Schema,
        values: Values,
    },
    Union {
        schema: Schema,
        lhs: Box<QueryPlan>,
        rhs: Box<QueryPlan>,
    },
    Join {
        schema: Schema,
        join: JoinKind,
        lhs: Box<QueryPlan>,
        rhs: Box<QueryPlan>,
    },
    Limit {
        source: Box<QueryPlan>,
        limit: u64,
        exceeded_message: Option<&'static str>,
    },
    Order {
        source: Box<QueryPlan>,
        order: Box<[OrderExpr]>,
    },
    Insert {
        table: Oid<Table>,
        source: Box<QueryPlan>,
        returning: Box<[Expr]>,
        schema: Schema,
    },
    Update {
        table: Oid<Table>,
        source: Box<QueryPlan>,
        returning: Option<Box<[Expr]>>,
        schema: Schema,
    },
}

impl QueryPlan {
    pub fn required_transaction_mode(&self) -> TransactionMode {
        struct V {
            requires_write: bool,
        }

        impl Visitor for V {
            fn visit_query_plan(&mut self, plan: &QueryPlan) -> ControlFlow<()> {
                match plan {
                    QueryPlan::Update { .. } | QueryPlan::Insert { .. } => {
                        self.requires_write = true;
                        ControlFlow::Break(())
                    }
                    _ => self.walk_query_plan(plan),
                }
            }
        }

        let mut v = V { requires_write: false };
        v.visit_query_plan(self);
        if v.requires_write { TransactionMode::ReadWrite } else { TransactionMode::ReadOnly }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Plan<Q = Box<QueryPlan>> {
    Show(ObjectType),
    Drop(Vec<EntityRef>),
    Transaction(TransactionStmt),
    SetVariable { name: Name, value: Value, scope: VariableScope },
    Explain(Box<Plan<Q>>),
    Query(Q),
}

impl Default for Plan {
    #[inline]
    fn default() -> Self {
        Self::Query(Default::default())
    }
}

impl Plan {
    #[inline]
    pub fn take(&mut self) -> Self {
        mem::take(self)
    }

    pub fn required_transaction_mode(&self) -> TransactionMode {
        match self {
            Plan::Drop(_) => TransactionMode::ReadWrite,
            Plan::Transaction(kind) => match kind {
                TransactionStmt::Begin(mode) => *mode,
                TransactionStmt::Commit | TransactionStmt::Abort => TransactionMode::ReadOnly,
            },
            Plan::Show(..) => TransactionMode::ReadOnly,
            Plan::SetVariable { .. } => TransactionMode::ReadOnly,
            // even though `explain` doesn't execute the plan, the planning stage currently
            // still checks we have a write transaction available
            Plan::Explain(plan) => plan.required_transaction_mode(),
            Plan::Query(query) => query.required_transaction_mode(),
        }
    }
}

struct PlanFormatter<'p> {
    plan: &'p QueryPlan,
    indent: usize,
}

impl<'p> PlanFormatter<'p> {
    fn child(&self, plan: &'p QueryPlan) -> Self {
        Self { plan, indent: self.indent + 2 }
    }
}

impl fmt::Display for PlanFormatter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:indent$}", "", indent = self.indent)?;
        match self.plan {
            QueryPlan::Aggregate { aggregates: functions, source, group_by, schema: _ } => {
                writeln!(
                    f,
                    "aggregate ({})",
                    functions
                        .iter()
                        .map(|(f, args)| format!("{}({})", f.name(), args.iter().format(",")))
                        .collect::<Vec<_>>()
                        .join(","),
                )?;

                if !group_by.is_empty() {
                    write!(f, " by {}", group_by.iter().format(","))?;
                }

                self.child(source).fmt(f)
            }
            QueryPlan::TableScan { table, projection, projected_schema: _ } => {
                write!(f, "table scan {}", table)?;
                if let Some(projection) = projection {
                    write!(f, "({})", projection.iter().format(","))?;
                }
                writeln!(f)
            }
            QueryPlan::Projection { source, projection, projected_schema: _ } => {
                writeln!(f, "projection ({})", projection.iter().format(","),)?;
                self.child(source).fmt(f)
            }
            QueryPlan::Filter { source, predicate } => {
                writeln!(f, "filter ({})", predicate)?;
                self.child(source).fmt(f)
            }
            QueryPlan::Unnest { schema: _, expr } => write!(f, "UNNEST ({})", expr),
            QueryPlan::Values { values: _, schema: _ } => writeln!(f, "VALUES"),
            QueryPlan::Join { schema: _, join, lhs, rhs } => {
                writeln!(f, "join ({})", join)?;
                self.child(lhs).fmt(f)?;
                self.child(rhs).fmt(f)
            }
            QueryPlan::Limit { source, limit, exceeded_message: _ } => {
                writeln!(f, "limit ({})", limit)?;
                self.child(source).fmt(f)
            }
            QueryPlan::Order { source, order } => {
                write!(f, "order ({})", order.iter().format(","),)?;
                self.child(source).fmt(f)
            }
            QueryPlan::DummyScan => write!(f, "dummy scan"),
            QueryPlan::Insert { table, source, returning, schema: _ } => {
                write!(f, "INSERT INTO {table}")?;
                if !returning.is_empty() {
                    write!(f, " RETURNING ({})", returning.iter().format(","))?;
                }
                self.child(source).fmt(f)
            }
            QueryPlan::Update { table, source, returning, schema: _ } => {
                write!(f, "UPDATE {table} SET", table = table)?;
                if let Some(returning) = returning {
                    write!(f, " RETURNING ({})", returning.iter().format(","))?;
                }
                self.child(source).fmt(f)
            }
            QueryPlan::Union { schema: _, lhs, rhs } => {
                writeln!(f, "UNION")?;
                self.child(lhs).fmt(f)?;
                self.child(rhs).fmt(f)
            }
            QueryPlan::CteScan { name, schema: _ } => writeln!(f, "CTE scan on `{name}`"),
            QueryPlan::Cte { cte, child } => {
                writeln!(f, "CTE {}", cte.name)?;
                self.child(child).fmt(f)
            }
        }
    }
}

impl fmt::Display for QueryPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        PlanFormatter { plan: self, indent: 0 }.fmt(f)
    }
}

impl<Q: fmt::Display> fmt::Display for Plan<Q> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Plan::Show(kind) => write!(f, "SHOW {kind}"),
            Plan::Drop(_refs) => write!(f, "DROP"),
            Plan::Transaction(tx) => write!(f, "{tx}"),
            Plan::SetVariable { name, value, scope } => write!(f, "SET {scope} {name} = {value}"),
            Plan::Explain(query) => write!(f, "EXPLAIN {query}"),
            Plan::Query(query) => write!(f, "{query}"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
}

impl FromStr for JoinKind {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "INNER" | "inner" => Ok(Self::Inner),
            "LEFT" | "left" => Ok(Self::Left),
            "RIGHT" | "right" => Ok(Self::Right),
            "FULL" | "full" => Ok(Self::Full),
            _ => bail!("invalid join type `{s}`"),
        }
    }
}

impl JoinKind {
    #[inline]
    pub fn is_left(&self) -> bool {
        matches!(self, JoinKind::Left | JoinKind::Full)
    }

    #[inline]
    pub fn is_right(&self) -> bool {
        matches!(self, JoinKind::Right | JoinKind::Full)
    }
}

impl fmt::Display for JoinKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinKind::Inner => write!(f, "INNER"),
            JoinKind::Left => write!(f, "LEFT"),
            JoinKind::Right => write!(f, "RIGHT"),
            JoinKind::Full => write!(f, "FULL"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderExpr<E = Expr> {
    pub expr: E,
    pub asc: bool,
}

impl<E: fmt::Display> fmt::Display for OrderExpr<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        if !self.asc {
            write!(f, " DESC")?;
        }
        Ok(())
    }
}

impl QueryPlan {
    pub fn schema(&self) -> &Schema {
        match self {
            QueryPlan::TableScan { projected_schema, .. }
            | QueryPlan::Projection { projected_schema, .. } => projected_schema,
            QueryPlan::Union { schema, .. }
            | QueryPlan::Aggregate { schema, .. }
            | QueryPlan::Join { schema, .. }
            | QueryPlan::Unnest { schema, .. }
            | QueryPlan::Insert { schema, .. }
            | QueryPlan::Update { schema, .. }
            | QueryPlan::CteScan { schema, .. }
            | QueryPlan::Values { schema, .. } => schema,
            QueryPlan::Filter { source, .. }
            | QueryPlan::Limit { source, .. }
            | QueryPlan::Order { source, .. } => source.schema(),
            QueryPlan::Cte { cte: _, child } => child.schema(),
            QueryPlan::DummyScan => Schema::empty_ref(),
        }
    }

    pub fn ungrouped_aggregate(
        self: Box<Self>,
        aggregates: impl Into<Box<[FunctionCall]>>,
    ) -> Box<Self> {
        self.aggregate([], aggregates)
    }

    pub fn aggregate(
        self: Box<Self>,
        group_by: impl Into<Box<[Expr]>>,
        aggregates: impl Into<Box<[FunctionCall]>>,
    ) -> Box<Self> {
        let group_by = group_by.into();
        let aggregates = aggregates.into();

        if aggregates.is_empty() && group_by.is_empty() {
            return self;
        }

        // The aggregate plan returns all the columns used in the group by clause followed by the aggregate values
        let schema = group_by
            .iter()
            .map(|e| e.ty.clone())
            .chain(aggregates.iter().map(|(f, _args)| f.return_type()))
            .collect();
        Box::new(Self::Aggregate { schema, aggregates, group_by, source: self })
    }

    #[inline]
    pub fn limit(self: Box<Self>, limit: u64) -> Box<Self> {
        Box::new(Self::Limit { source: self, limit, exceeded_message: None })
    }

    #[inline]
    pub fn with_cte(self: Box<Self>, cte: Cte) -> Box<Self> {
        Box::new(Self::Cte { cte, child: self })
    }

    #[inline]
    pub fn strict_limit(self: Box<Self>, limit: u64, exceeded_message: &'static str) -> Box<Self> {
        Box::new(Self::Limit { source: self, limit, exceeded_message: Some(exceeded_message) })
    }

    #[inline]
    pub fn order_by(self: Box<Self>, order: impl Into<Box<[OrderExpr]>>) -> Box<Self> {
        let order = order.into();
        if order.is_empty() {
            return self;
        }

        Box::new(Self::Order { source: self, order })
    }

    #[inline]
    pub fn cross_join(self: Box<Self>, rhs: Box<Self>) -> Box<Self> {
        self.join(
            JoinKind::Inner,
            rhs,
            Expr { ty: LogicalType::Bool, kind: ExprKind::Literal(Value::Bool(true)) },
        )
    }

    #[inline]
    pub fn join(self: Box<Self>, join: JoinKind, rhs: Box<Self>, predicate: Expr) -> Box<Self> {
        let schema = self.schema().iter().chain(rhs.schema().iter()).cloned().collect();
        Box::new(Self::Join { schema, join, lhs: self, rhs }).filter(predicate)
    }

    #[inline]
    pub fn filter(self: Box<Self>, predicate: Expr) -> Box<Self> {
        assert!(matches!(predicate.ty, LogicalType::Bool | LogicalType::Null));
        Box::new(Self::Filter { source: self, predicate })
    }

    #[inline]
    pub fn cte_scan(name: Name, schema: Schema) -> Box<Self> {
        Box::new(Self::CteScan { name, schema })
    }

    #[inline]
    pub fn project(self: Box<Self>, projection: impl Into<Box<[Expr]>>) -> Box<Self> {
        let projection = projection.into();
        let projected_schema = projection.iter().map(|expr| expr.ty.clone()).collect();
        Box::new(Self::Projection { source: self, projection, projected_schema })
    }

    /// Create a projection that projects the first `k` columns of the plan
    #[inline]
    pub fn project_leftmost_k(self: Box<Self>, k: usize) -> Box<Self> {
        let schema = self.schema();
        assert!(k <= schema.len(), "k must be less than or equal to the number of columns");
        let projection = (0..k)
            .map(|i| Expr {
                ty: schema[i].clone(),
                kind: ExprKind::ColumnRef(ColumnRef {
                    qpath: QPath::new("", format!("{i}")),
                    index: TupleIndex::new(i),
                }),
            })
            .collect::<Box<_>>();
        self.project(projection)
    }

    pub fn values(values: Values) -> Box<Self> {
        let schema = values
            .iter()
            .map(|row| row.iter().map(|expr| expr.ty.clone()).collect())
            .next()
            .expect("values should be non-empty");
        Box::new(QueryPlan::Values { values, schema })
    }

    /// Construct an `unnest` plan. `expr` must have an array type
    pub fn unnest(expr: Expr) -> Box<Self> {
        match &expr.ty {
            LogicalType::Array(element_type) => {
                Box::new(QueryPlan::Unnest { schema: Schema::new([*element_type.clone()]), expr })
            }
            _ => panic!("unnest expression must be an array"),
        }
    }

    pub fn union(self: Box<Self>, schema: Schema, rhs: Box<Self>) -> Box<Self> {
        assert_eq!(self.schema(), rhs.schema());
        Box::new(Self::Union { schema, lhs: self, rhs })
    }
}

impl Plan {
    pub fn schema(&self) -> &[LogicalType] {
        match self {
            Plan::Show(..) | Plan::Explain(..) => &[LogicalType::Text],
            Plan::Drop(..) | Plan::Transaction(..) | Plan::SetVariable { .. } => &[],
            Plan::Query(query) => query.schema(),
        }
    }

    pub fn query(query: QueryPlan) -> Self {
        Plan::Query(Box::new(query))
    }
}

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct QPath {
    pub prefix: Box<Path>,
    pub name: Name,
}

impl QPath {
    #[inline]
    pub fn new(prefix: impl Into<Path>, name: impl Into<Name>) -> Self {
        Self { prefix: Box::new(prefix.into()), name: name.into() }
    }

    pub fn components(&self) -> impl Iterator<Item = Name> + '_ {
        self.prefix.components().chain(std::iter::once(Name::clone(&self.name)))
    }
}

impl FromStr for QPath {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.rsplit_once(s) {
            Some((prefix, name)) => {
                Ok(Self { prefix: Box::new(prefix.parse()?), name: name.into() })
            }
            None => bail!("invalid qualified path `{}`", s),
        }
    }
}

impl FromStr for Path {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.contains('.') {
            true => Ok(Self::Qualified(s.parse()?)),
            false => Ok(Self::Unqualified(s.into())),
        }
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

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
    pub fn qualified(prefix: impl Into<Path>, name: impl Into<Name>) -> Path {
        Path::Qualified(QPath { prefix: Box::new(prefix.into()), name: name.into() })
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

    pub fn components(&self) -> Box<dyn Iterator<Item = Name> + '_> {
        match self {
            Path::Qualified(qpath) => Box::new(qpath.components()),
            Path::Unqualified(name) => Box::new(std::iter::once(Name::clone(name))),
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
