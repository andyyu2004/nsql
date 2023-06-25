mod eval;

use std::fmt;
use std::ops::Deref;

pub use eval::EvalNotConst;
use itertools::Itertools;
use nsql_catalog::{ColumnIndex, Function, Table};
use nsql_core::{LogicalType, Oid};
use nsql_storage::tuple::TupleIndex;
use nsql_storage::value::Value;

use crate::Path;

pub type QueryPlanSchema = Vec<LogicalType>;

#[derive(Debug, Clone)]
pub enum QueryPlan {
    TableScan {
        table: Oid<Table>,
        projection: Option<Box<[ColumnIndex]>>,
        projected_schema: QueryPlanSchema,
    },
    Projection {
        source: Box<QueryPlan>,
        projection: Box<[Expr]>,
        projected_schema: QueryPlanSchema,
    },
    Filter {
        source: Box<QueryPlan>,
        predicate: Expr,
    },
    // maybe this can be implemented as a standard table function in the future (change the sqlparser dialect to duckdb if so)
    Unnest {
        schema: QueryPlanSchema,
        expr: Expr,
    },
    Values {
        values: Values,
        schema: QueryPlanSchema,
    },
    Join {
        schema: QueryPlanSchema,
        join: Join,
        lhs: Box<QueryPlan>,
        rhs: Box<QueryPlan>,
        // cross join only for now
    },
    Limit {
        source: Box<QueryPlan>,
        limit: u64,
    },
    Order {
        source: Box<QueryPlan>,
        order: Box<[OrderExpr]>,
    },
    Empty,
}

#[derive(Debug, Clone)]
pub enum Join {
    Inner(JoinConstraint),
    Left(JoinConstraint),
    Full(JoinConstraint),
    Cross,
}

impl Join {
    #[inline]
    pub fn is_left(&self) -> bool {
        matches!(self, Join::Left(_) | Join::Full(_))
    }
}

impl fmt::Display for Join {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Join::Inner(constraint) => write!(f, "INNER JOIN {}", constraint),
            Join::Left(constraint) => write!(f, "LEFT JOIN {}", constraint),
            Join::Full(constraint) => write!(f, "FULL JOIN {}", constraint),
            Join::Cross => write!(f, "CROSS JOIN"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum JoinConstraint {
    On(Expr),
}

impl fmt::Display for JoinConstraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinConstraint::On(expr) => write!(f, "ON {}", expr),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OrderExpr {
    pub expr: Expr,
    pub asc: bool,
    pub nulls_first: bool,
}

impl QueryPlan {
    pub fn schema(&self) -> &[LogicalType] {
        match self {
            QueryPlan::TableScan { projected_schema, .. }
            | QueryPlan::Projection { projected_schema, .. } => projected_schema,
            QueryPlan::Join { schema, .. }
            | QueryPlan::Unnest { schema, .. }
            | QueryPlan::Values { schema, .. } => schema,
            QueryPlan::Filter { source, .. }
            | QueryPlan::Limit { source, .. }
            | QueryPlan::Order { source, .. } => source.schema(),
            QueryPlan::Empty => &[],
        }
    }

    pub fn values(values: Values) -> Self {
        let schema = values
            .iter()
            .map(|row| row.iter().map(|expr| expr.ty.clone()).collect::<Vec<_>>())
            .next()
            .expect("values should be non-empty");
        QueryPlan::Values { values, schema }
    }

    /// Construct an `unnest` plan. `expr` must have an array type
    pub fn unnest(expr: Expr) -> Self {
        match &expr.ty {
            LogicalType::Array(element_type) => {
                QueryPlan::Unnest { schema: vec![*element_type.clone()], expr }
            }
            _ => panic!("unnest expression must be an array"),
        }
    }

    #[inline]
    pub fn limit(self: Box<Self>, limit: u64) -> Box<QueryPlan> {
        Box::new(QueryPlan::Limit { source: self, limit })
    }

    #[inline]
    pub fn order_by(self: Box<Self>, order: impl Into<Box<[OrderExpr]>>) -> Box<QueryPlan> {
        Box::new(QueryPlan::Order { source: self, order: order.into() })
    }

    pub fn join(self: Box<Self>, join: Join, rhs: Box<QueryPlan>) -> Box<QueryPlan> {
        let schema = self.schema().iter().chain(rhs.schema()).cloned().collect();
        Box::new(QueryPlan::Join { schema, join, lhs: self, rhs })
    }

    #[inline]
    pub fn filter(self: Box<Self>, predicate: Expr) -> Box<QueryPlan> {
        assert!(matches!(predicate.ty, LogicalType::Bool | LogicalType::Null));
        Box::new(QueryPlan::Filter { source: self, predicate })
    }

    #[inline]
    pub fn project(self: Box<Self>, projection: impl Into<Box<[Expr]>>) -> Box<QueryPlan> {
        let projection = projection.into();
        let projected_schema = projection.iter().map(|expr| expr.ty.clone()).collect();
        Box::new(QueryPlan::Projection { source: self, projection, projected_schema })
    }
}

#[derive(Debug, Clone)]
pub struct Expr {
    pub ty: LogicalType,
    pub kind: ExprKind,
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind)
    }
}

#[derive(Debug, Clone)]
pub enum ExprKind {
    Value(Value),
    Array(Box<[Expr]>),
    BinOp {
        op: BinOp,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
    },
    ColumnRef {
        /// A display path for the column (for pretty printing etc)
        display_path: Path,
        /// An index into the tuple the expression is evaluated against
        index: TupleIndex,
    },
    FunctionCall {
        function: Function,
        args: Box<[Expr]>,
    },
}

impl fmt::Display for ExprKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            ExprKind::Value(value) => write!(f, "{value}"),
            ExprKind::ColumnRef { display_path, .. } => write!(f, "{display_path}"),
            ExprKind::BinOp { op, lhs, rhs } => write!(f, "({lhs} {op} {rhs})"),
            ExprKind::Array(exprs) => write!(f, "[{}]", exprs.iter().format(", ")),
            ExprKind::FunctionCall { function, args } => {
                write!(f, "{}({})", function.name(), args.iter().format(", "))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}

impl fmt::Display for BinOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            BinOp::Add => write!(f, "+"),
            BinOp::Sub => write!(f, "-"),
            BinOp::Mul => write!(f, "*"),
            BinOp::Div => write!(f, "/"),
            BinOp::Mod => write!(f, "%"),
            BinOp::Eq => write!(f, "="),
            BinOp::Ne => write!(f, "!="),
            BinOp::Lt => write!(f, "<"),
            BinOp::Le => write!(f, "<="),
            BinOp::Gt => write!(f, ">"),
            BinOp::Ge => write!(f, ">="),
            BinOp::And => write!(f, "AND"),
            BinOp::Or => write!(f, "OR"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Values(Vec<Vec<Expr>>);

impl Values {
    /// Create a new `Values` expression
    /// The `values` must be a non-empty vector of non-empty vectors of expressions
    /// The inner vectors must each be the same length
    ///
    /// Panics if these conditions are not met
    #[inline]
    pub fn new(values: Vec<Vec<Expr>>) -> Self {
        assert!(!values.is_empty(), "values must be non-empty");
        let len = values[0].len();
        assert!(values.iter().all(|v| v.len() == len), "all inner vectors must be the same length");
        Self(values)
    }
}

impl Deref for Values {
    type Target = Vec<Vec<Expr>>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
