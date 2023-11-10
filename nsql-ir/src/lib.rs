#![deny(rust_2018_idioms)]

pub mod expr;
pub mod fold;
mod validate;
pub mod visit;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::str::FromStr;
use std::{fmt, mem};

use anyhow::bail;
use itertools::Itertools;
use nsql_catalog::ColumnIndex;
pub use nsql_catalog::{Function, Operator, Table};
use nsql_core::{LogicalType, Name, Oid, Schema};
pub use nsql_storage::tuple::TupleIndex;
pub use nsql_storage::value::{Decimal, Value};

pub use self::expr::*;
use crate::visit::Visitor;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum TransactionMode {
    #[default]
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
    /// Single empty tuple as output
    #[default]
    DummyScan,
    /// No rows output. This can have any schema as it produces no output.
    Empty {
        schema: Schema,
    },
    Distinct {
        source: Box<QueryPlan>,
    },
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
        kind: JoinKind,
        lhs: Box<QueryPlan>,
        rhs: Box<QueryPlan>,
        conditions: Box<[JoinCondition]>,
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
        returning: Box<[Expr]>,
        schema: Schema,
    },
}

impl QueryPlan {
    #[inline]
    pub fn take(&mut self) -> Self {
        mem::take(self)
    }

    pub fn required_transaction_mode(&self) -> TransactionMode {
        let mut v = RequiredTransactionModeVisitor::default();
        v.visit_query_plan(self);
        v.required_mode
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Plan<Q = Box<QueryPlan>> {
    Show(ObjectType),
    Drop(Vec<EntityRef>),
    Transaction(TransactionStmt),
    SetVariable { name: Name, value: Value, scope: VariableScope },
    Explain(ExplainOptions, Box<Plan<Q>>),
    Copy(Copy<Q>),
    Query(Q),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExplainOptions {
    pub analyze: bool,
    pub verbose: bool,
    pub timing: bool,
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
        let mut v = RequiredTransactionModeVisitor::default();
        v.visit_plan(self);
        v.required_mode
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
    // for recursive format calls (to either plan or exprs), make sure to pass the same `f` to preserve formatting flags.
    // i.e. don't do `write!(f, "{expr}")`, instead do `expr.fmt(f)`
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:indent$}", "", indent = self.indent)?;
        match self.plan {
            QueryPlan::Empty { .. } => writeln!(f, "empty"),
            QueryPlan::DummyScan => writeln!(f, "dummy scan"),
            QueryPlan::Aggregate { aggregates, source, group_by, schema } => {
                write!(f, "aggregate ")?;
                aggregates
                    .iter()
                    .map(|(f, args)| format!("{}({})", f.name(), args.iter().format(",")))
                    .collect::<Vec<_>>()
                    .join(",")
                    .fmt(f)?;

                if !group_by.is_empty() {
                    write!(f, " by ")?;
                    group_by.iter().format(",").fmt(f)?;
                }

                if f.alternate() {
                    write!(f, " :: {schema}")?;
                }

                writeln!(f)?;

                self.child(source).fmt(f)
            }
            QueryPlan::TableScan { table, projection, projected_schema } => {
                write!(f, "table scan {table}")?;
                if let Some(projection) = projection {
                    write!(f, "({})", projection.iter().format(", "))?;
                }

                if f.alternate() {
                    write!(f, " :: {projected_schema}")?;
                }

                writeln!(f)
            }
            QueryPlan::Projection { source, projection, projected_schema } => {
                write!(f, "projection (")?;
                projection.iter().format(",").fmt(f)?;
                write!(f, ")")?;
                if f.alternate() {
                    write!(f, " :: {projected_schema}")?;
                }

                writeln!(f)?;
                self.child(source).fmt(f)
            }
            QueryPlan::Filter { source, predicate } => {
                write!(f, "filter (")?;
                predicate.fmt(f)?;
                writeln!(f, ")")?;
                self.child(source).fmt(f)
            }
            QueryPlan::Unnest { schema: _, expr } => write!(f, "UNNEST ({})", expr),
            QueryPlan::Values { values: _, schema: _ } => writeln!(f, "VALUES"),
            QueryPlan::Join { schema, kind, lhs, rhs, conditions } => {
                write!(f, "join ({kind})")?;
                if !conditions.is_empty() {
                    write!(f, " on (")?;
                    for (i, condition) in conditions.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }

                        condition.lhs.fmt(f)?;
                        write!(f, " {} ", condition.op)?;
                        condition.rhs.fmt(f)?;
                    }
                    write!(f, ")")?;
                }
                if f.alternate() {
                    write!(f, " :: {schema}")?;
                }
                writeln!(f)?;
                self.child(lhs).fmt(f)?;
                self.child(rhs).fmt(f)
            }
            QueryPlan::Limit { source, limit, exceeded_message: _ } => {
                writeln!(f, "limit ({})", limit)?;
                self.child(source).fmt(f)
            }
            QueryPlan::Order { source, order } => {
                write!(f, "order (")?;
                order.iter().format(", ").fmt(f)?;
                writeln!(f, ")")?;
                self.child(source).fmt(f)
            }
            QueryPlan::Insert { table, source, returning, schema: _ } => {
                write!(f, "insert {table}")?;
                if !returning.is_empty() {
                    write!(f, " RETURNING (")?;
                    returning.iter().format(",").fmt(f)?;
                    write!(f, ")")?;
                }
                writeln!(f)?;
                self.child(source).fmt(f)
            }
            QueryPlan::Update { table, source, returning, schema: _ } => {
                write!(f, "UPDATE {table} SET", table = table)?;
                if !returning.is_empty() {
                    write!(f, " RETURNING (")?;
                    returning.iter().format(",").fmt(f)?;
                    write!(f, ")")?;
                }
                writeln!(f)?;
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
                self.child(&cte.plan).fmt(f)?;
                self.child(child).fmt(f)
            }
            QueryPlan::Distinct { source } => {
                writeln!(f, "distinct")?;
                self.child(source).fmt(f)
            }
        }?;

        Ok(())
    }
}

impl fmt::Display for QueryPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        PlanFormatter { plan: self, indent: 0 }.fmt(f)
    }
}

impl<Q: fmt::Display> fmt::Display for Plan<Q> {
    // write the recursive calls in a way such that the same `f` is used
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Plan::Show(kind) => write!(f, "SHOW {kind}"),
            Plan::Drop(_refs) => write!(f, "DROP"),
            Plan::Transaction(tx) => write!(f, "{tx}"),
            Plan::SetVariable { name, value, scope } => write!(f, "SET {scope} {name} = {value}"),
            Plan::Explain(opts, query) => {
                write!(f, "EXPLAIN ")?;
                if opts.analyze {
                    write!(f, "ANALYZE ")?;
                }
                if opts.verbose {
                    write!(f, "VERBOSE ")?;
                }
                query.fmt(f)
            }
            Plan::Query(query) => query.fmt(f),
            Plan::Copy(copy) => copy.fmt(f),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum JoinKind {
    /// Similar to a `LEFT JOIN` but returns only a boolean indicating whether a match was found
    Mark,
    /// Similar to a `LEFT JOIN` but returns at most one matching row from the right side (NULL if no match)
    Single,
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct JoinPredicate {
    pub conditions: Box<[JoinCondition]>,
    /// expressions that reference both sides of the join
    pub arbitrary_expr: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JoinCondition<E = Expr> {
    pub op: Oid<Operator>,
    pub lhs: E,
    pub rhs: E,
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
        matches!(self, JoinKind::Left | JoinKind::Full | JoinKind::Single | JoinKind::Mark)
    }

    #[inline]
    pub fn is_right(&self) -> bool {
        matches!(self, JoinKind::Right | JoinKind::Full)
    }
}

impl fmt::Display for JoinKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinKind::Mark => write!(f, "MARK"),
            JoinKind::Inner => write!(f, "INNER"),
            JoinKind::Left => write!(f, "LEFT"),
            JoinKind::Right => write!(f, "RIGHT"),
            JoinKind::Full => write!(f, "FULL"),
            JoinKind::Single => write!(f, "SINGLE"),
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
    #[inline]
    pub fn is_correlated(&self) -> bool {
        #[derive(Default)]
        struct CorrelatedColumnsVisitor;

        impl Visitor for CorrelatedColumnsVisitor {
            fn visit_expr(&mut self, plan: &QueryPlan, expr: &Expr) -> ControlFlow<()> {
                match &expr.kind {
                    ExprKind::ColumnRef(col) if col.is_correlated() => ControlFlow::Break(()),
                    _ => self.walk_expr(plan, expr),
                }
            }
        }

        CorrelatedColumnsVisitor.visit_query_plan(self).is_break()
    }

    /// Prefer `is_correlated` over `correlated_columns().is_empty()`.
    #[inline]
    pub fn correlated_columns(&self) -> Vec<CorrelatedColumn> {
        #[derive(Default)]
        struct CorrelatedColumnVisitor {
            correlated_columns: Vec<CorrelatedColumn>,
        }

        impl Visitor for CorrelatedColumnVisitor {
            fn visit_expr(&mut self, plan: &QueryPlan, expr: &Expr) -> ControlFlow<()> {
                match &expr.kind {
                    ExprKind::ColumnRef(col) if col.is_correlated() => self
                        .correlated_columns
                        .push(CorrelatedColumn { ty: expr.ty.clone(), col: col.clone() }),
                    _ => (),
                }

                self.walk_expr(plan, expr)
            }
        }

        let mut visitor = CorrelatedColumnVisitor::default();
        visitor.visit_query_plan(self);
        visitor.correlated_columns
    }

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
            | QueryPlan::Empty { schema }
            | QueryPlan::Values { schema, .. } => schema,
            QueryPlan::Distinct { source }
            | QueryPlan::Filter { source, .. }
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
    pub fn distinct(self: Box<Self>) -> Box<Self> {
        Box::new(Self::Distinct { source: self })
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
        self.join(JoinKind::Inner, rhs, JoinPredicate::default())
    }

    #[inline]
    pub fn join(
        self: Box<Self>,
        kind: JoinKind,
        rhs: Box<Self>,
        predicate: JoinPredicate,
    ) -> Box<Self> {
        let schema = match kind {
            JoinKind::Mark => {
                self.schema().iter().cloned().chain(std::iter::once(LogicalType::Bool)).collect()
            }
            _ => self.schema().iter().chain(rhs.schema().iter()).cloned().collect(),
        };
        let mut join =
            Box::new(Self::Join { schema, kind, lhs: self, rhs, conditions: predicate.conditions });
        if let Some(expr) = predicate.arbitrary_expr {
            join = join.filter(expr);
        }
        join
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

    #[inline]
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Empty { .. })
    }

    pub fn build_leftmost_k_projection(&self, k: usize) -> Box<[Expr]> {
        let schema = self.schema();
        assert!(k <= schema.width(), "k must be less than or equal to the number of columns");
        (0..k)
            .map(|i| Expr {
                ty: schema[i].clone(),
                kind: ExprKind::ColumnRef(ColumnRef::new(
                    TupleIndex::new(i),
                    QPath::new("", format!("{i}")),
                )),
            })
            .collect::<Box<_>>()
    }

    pub fn build_rightmost_k_projection(&self, k: usize) -> Box<[Expr]> {
        let schema = self.schema();
        assert!(k <= schema.width(), "k must be less than or equal to the number of columns");
        (schema.width() - k..schema.width())
            .map(|i| Expr {
                ty: schema[i].clone(),
                kind: ExprKind::ColumnRef(ColumnRef::new(
                    TupleIndex::new(i),
                    QPath::new("", format!("{i}")),
                )),
            })
            .collect::<Box<_>>()
    }

    #[inline]
    pub fn build_identity_projection(&self) -> Box<[Expr]> {
        self.build_leftmost_k_projection(self.schema().width())
    }

    /// Create a projection that projects the first `k` columns of the plan
    #[inline]
    pub fn project_leftmost_k(self: Box<Self>, k: usize) -> Box<Self> {
        let projection = self.build_leftmost_k_projection(k);
        self.project(projection)
    }

    pub fn project_rightmost_k(self: Box<Self>, k: usize) -> Box<Self> {
        let projection = self.build_rightmost_k_projection(k);
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
        debug_assert_eq!(self.schema(), rhs.schema());
        Box::new(Self::Union { schema, lhs: self, rhs })
    }
}

impl Plan {
    pub fn schema(&self) -> &[LogicalType] {
        match self {
            Plan::Show(..) | Plan::Explain(..) => &[LogicalType::Text],
            Plan::Copy(_) | Plan::Drop(..) | Plan::Transaction(..) | Plan::SetVariable { .. } => {
                &[]
            }
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Copy<Q = Box<QueryPlan>> {
    To(CopyTo<Q>),
}

impl<Q: fmt::Display> fmt::Display for Copy<Q> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::To(copy_to) => copy_to.fmt(f),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CopyTo<Q = Box<QueryPlan>> {
    pub src: Q,
    pub dst: CopyDestination,
}

impl<Q: fmt::Display> fmt::Display for CopyTo<Q> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.src.fmt(f)?;
        write!(f, " TO ")?;
        self.dst.fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CopyDestination {
    File(PathBuf),
}

impl fmt::Display for CopyDestination {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::File(path) => write!(f, "{}", path.display()),
        }
    }
}

#[derive(Default)]
struct RequiredTransactionModeVisitor {
    required_mode: TransactionMode,
}

impl RequiredTransactionModeVisitor {
    fn requires_write(&mut self) -> ControlFlow<()> {
        self.required_mode = TransactionMode::ReadWrite;
        ControlFlow::Break(())
    }
}

impl Visitor for RequiredTransactionModeVisitor {
    fn visit_plan(&mut self, plan: &Plan) -> ControlFlow<()> {
        match plan {
            Plan::Drop(_) => self.requires_write(),
            Plan::Transaction(kind) => match kind {
                TransactionStmt::Begin(TransactionMode::ReadWrite) => self.requires_write(),
                _ => ControlFlow::Continue(()),
            },
            _ => self.walk_plan(plan),
        }
    }

    fn visit_query_plan(&mut self, plan: &QueryPlan) -> ControlFlow<()> {
        match plan {
            QueryPlan::Update { .. } | QueryPlan::Insert { .. } => {
                self.required_mode = TransactionMode::ReadWrite;
                ControlFlow::Break(())
            }
            _ => self.walk_query_plan(plan),
        }
    }

    fn visit_expr(&mut self, plan: &QueryPlan, expr: &Expr) -> ControlFlow<()> {
        match &expr.kind {
            ExprKind::FunctionCall { function, args: _ } => {
                // FIXME a function should probably be able to specify its required transaction mode or something like that.
                // Or maybe something more general like it's purity.
                // Special casing them for now
                if function.oid() == Function::NEXTVAL {
                    self.required_mode = TransactionMode::ReadWrite;
                    ControlFlow::Break(())
                } else {
                    self.walk_expr(plan, expr)
                }
            }
            _ => self.walk_expr(plan, expr),
        }
    }
}
