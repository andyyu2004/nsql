mod const_eval;

use std::ops::Deref;
use std::str::FromStr;
use std::{fmt, mem};

use anyhow::ensure;
pub use const_eval::EvalNotConst;
use itertools::Itertools;
use nsql_catalog::{Function, Operator};
use nsql_core::{LogicalType, Name};
use nsql_storage::eval;
use nsql_storage::tuple::TupleIndex;
use nsql_storage::value::Value;
use nsql_util::static_assert_eq;

use crate::{QPath, QueryPlan};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Expr {
    pub ty: LogicalType,
    pub kind: ExprKind,
}

// convenient for `mem::take`
impl Default for Expr {
    #[inline]
    fn default() -> Self {
        Expr::NULL
    }
}

impl Expr {
    pub const NULL: Expr = Expr { ty: LogicalType::Null, kind: ExprKind::Literal(Value::Null) };

    #[inline]
    pub fn column_ref(ty: LogicalType, qpath: QPath, index: TupleIndex) -> Self {
        Self { ty, kind: ExprKind::ColumnRef(ColumnRef::new(index, qpath)) }
    }

    #[inline]
    pub fn quote(expr: Self) -> Self {
        Self { ty: LogicalType::Expr, kind: ExprKind::Quote(Box::new(expr)) }
    }

    pub fn lit(ty: LogicalType, value: impl Into<Value>) -> Self {
        Self { ty, kind: ExprKind::Literal(value.into()) }
    }

    #[inline]
    pub fn call(function: MonoFunction, args: impl Into<Box<[Expr]>>) -> Expr {
        let args = args.into();
        let ty = function.return_type();
        Expr { ty, kind: ExprKind::FunctionCall { function, args } }
    }

    pub fn scalar_subquery(plan: Box<QueryPlan>) -> anyhow::Result<Expr> {
        let schema = plan.schema();
        ensure!(
            schema.len() == 1,
            "subquery expression must return exactly one column, got {}",
            schema.len()
        );

        let ty = schema[0].clone();
        Ok(Expr { ty, kind: ExprKind::Subquery(SubqueryKind::Scalar, plan) })
    }

    #[inline]
    pub fn ty(&self) -> LogicalType {
        self.ty.clone()
    }

    #[inline]
    pub fn alias(self, alias: impl AsRef<str>) -> Expr {
        Expr {
            ty: self.ty.clone(),
            kind: ExprKind::Alias { expr: Box::new(self), alias: alias.as_ref().into() },
        }
    }

    /// Generates a column name for the expression.
    /// This is generally just the pretty-printed expression, but may be overridden by an alias
    pub fn name(&self) -> String {
        match &self.kind {
            ExprKind::Alias { alias, .. } => alias.to_string(),
            _ => self.to_string(),
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() { write!(f, "{:#}", self.kind) } else { write!(f, "{}", self.kind) }
    }
}

// This is just for awareness of the size of the enum
static_assert_eq!(mem::size_of::<ExprKind>(), 48);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExprKind {
    Literal(Value),
    Array(Box<[Expr]>),
    Alias {
        alias: Name,
        expr: Box<Expr>,
    },
    ColumnRef(ColumnRef),
    FunctionCall {
        function: MonoFunction,
        args: Box<[Expr]>,
    },
    UnaryOperator {
        operator: MonoOperator,
        expr: Box<Expr>,
    },
    BinaryOperator {
        operator: MonoOperator,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
    },
    Case {
        scrutinee: Box<Expr>,
        cases: Box<[Case]>,
        else_result: Option<Box<Expr>>,
    },
    Subquery(SubqueryKind, Box<QueryPlan>),
    Compiled(eval::Expr),
    /// An expression that evaluates to an expression.
    /// Similar to `Compiled` above but is yet to be compiled.
    Quote(Box<Expr>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SubqueryKind {
    Scalar,
    Exists,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CorrelatedColumn {
    pub ty: LogicalType,
    pub col: ColumnRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnRef {
    /// An index into the tuple the expression is evaluated against
    pub index: TupleIndex,
    /// A qualified display path for the column (for pretty printing etc)
    pub qpath: QPath,
    /// The scope level, where 0 is the current scope.
    /// level > 0 means the column is a correlated reference to an outer scope
    pub level: u8,
}

impl ColumnRef {
    #[inline]
    pub fn new(index: TupleIndex, qpath: QPath) -> Self {
        Self { index, qpath, level: 0 }
    }

    #[inline]
    pub fn is_correlated(&self) -> bool {
        self.level > 0
    }
}

impl fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() { write!(f, "{}", self.index) } else { write!(f, "{}", self.qpath) }
    }
}

impl FromStr for ColumnRef {
    type Err = anyhow::Error;

    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        // this is just to satisfy the trait for egraphs
        todo!()
    }
}

pub type FunctionCall<E = Expr> = (MonoFunction, Box<[E]>);

/// A function that has been "monomorphized", i.e. all ANY types have been replaced with a concrete type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MonoOperator(Box<(Operator, MonoFunction)>);

impl MonoOperator {
    pub fn new(operator: Operator, function: MonoFunction) -> Self {
        Self(Box::new((operator, function)))
    }

    #[inline]
    pub fn operator(&self) -> &Operator {
        &self.0.0
    }

    #[inline]
    pub fn mono_function(&self) -> &MonoFunction {
        &self.0.1
    }

    #[inline]
    pub fn return_type(&self) -> LogicalType {
        self.0.1.return_type()
    }
}

impl fmt::Display for MonoOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.operator())
    }
}

/// A function that has been "monomorphized", i.e. all ANY types have been replaced with a concrete type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MonoFunction(Box<(Function, LogicalType)>);

impl MonoFunction {
    // FIXME should probably default the return_type to function.return_type()
    // Then have another function that allows you to replace the `ANY` return if necessary
    #[track_caller]
    pub fn new(function: Function, return_type: LogicalType) -> Self {
        assert!(
            matches!(function.return_type(), LogicalType::Any)
                || function.return_type() == return_type,
            "specified return type must match function return type, expected {}, got {}",
            function.return_type(),
            return_type
        );
        Self(Box::new((function, return_type)))
    }

    #[inline]
    pub fn function(&self) -> Function {
        self.0.0.clone()
    }

    #[inline]
    pub fn return_type(&self) -> LogicalType {
        self.0.1.clone()
    }
}

impl Deref for MonoFunction {
    type Target = Function;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0.0
    }
}

impl fmt::Display for ExprKind {
    // note this is written in this way (i.e. avoiding the `write!` macro for recursive calls)
    // because we want to preserve the formatter flags for child nodes
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            ExprKind::Literal(value) => value.fmt(f),
            ExprKind::ColumnRef(col) => col.fmt(f),
            ExprKind::Array(exprs) => {
                write!(f, "[")?;
                exprs.iter().format(", ").fmt(f)?;
                write!(f, "]")
            }
            ExprKind::FunctionCall { function, args } => {
                function.name().fmt(f)?;
                write!(f, "(")?;
                args.iter().format(", ").fmt(f)?;
                write!(f, ")")
            }
            ExprKind::Alias { alias, expr } => {
                expr.fmt(f)?;
                if !alias.is_empty() {
                    write!(f, " AS {alias}")?;
                }

                Ok(())
            }
            ExprKind::Case { scrutinee, cases, else_result } => {
                write!(f, "CASE ")?;
                scrutinee.fmt(f)?;
                for case in cases.iter() {
                    write!(f, " WHEN")?;
                    case.when.fmt(f)?;
                    write!(f, " THEN ")?;
                    case.then.fmt(f)?;
                }

                if let Some(else_result) = else_result.as_ref() {
                    write!(f, " ELSE ")?;
                    else_result.fmt(f)?;
                }

                write!(f, "END")
            }
            ExprKind::Subquery(kind, plan) => match kind {
                SubqueryKind::Scalar => {
                    write!(f, "(")?;
                    plan.fmt(f)?;
                    write!(f, ")")
                }
                SubqueryKind::Exists => {
                    write!(f, "EXISTS (")?;
                    plan.fmt(f)?;
                    write!(f, ")")
                }
            },
            ExprKind::UnaryOperator { operator, expr } => {
                operator.fmt(f)?;
                expr.fmt(f)
            }
            ExprKind::BinaryOperator { operator, lhs, rhs } => {
                lhs.fmt(f)?;
                write!(f, " {operator} ")?;
                rhs.fmt(f)
            }
            ExprKind::Compiled(expr) => write!(f, "{expr}"),
            ExprKind::Quote(expr) => write!(f, "'({expr})"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Case {
    pub when: Expr,
    pub then: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Values(Box<[Box<[Expr]>]>);

impl Values {
    /// Create a new `Values` expression
    /// The `values` must be a non-empty vector of non-empty vectors of expressions
    /// The inner vectors must each be the same length
    ///
    /// Panics if these conditions are not met
    #[inline]
    pub fn new(values: Box<[Box<[Expr]>]>) -> Self {
        assert!(!values.is_empty(), "values must be non-empty");
        let len = values[0].len();
        assert!(values.iter().all(|v| v.len() == len), "all inner vectors must be the same length");
        Self(values)
    }

    #[inline]
    pub fn into_inner(self) -> Box<[Box<[Expr]>]> {
        self.0
    }
}

impl Deref for Values {
    type Target = Box<[Box<[Expr]>]>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
