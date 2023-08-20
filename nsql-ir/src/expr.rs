mod const_eval;

use std::ops::Deref;
use std::{fmt, mem};

pub use const_eval::EvalNotConst;
use itertools::Itertools;
use nsql_catalog::{Function, Operator};
use nsql_core::{LogicalType, Name};
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
    pub(crate) const NULL: Expr =
        Expr { ty: LogicalType::Null, kind: ExprKind::Literal(Value::Null) };

    #[inline]
    pub fn new_column_ref(ty: LogicalType, qpath: QPath, index: TupleIndex) -> Self {
        Self { ty, kind: ExprKind::ColumnRef { qpath, index } }
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
        write!(f, "{}", self.kind)
    }
}

// This is just for awareness of the size of the enum
static_assert_eq!(mem::size_of::<ExprKind>(), 40);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExprKind {
    Literal(Value),
    Array(Box<[Expr]>),
    Alias {
        alias: Name,
        expr: Box<Expr>,
    },
    ColumnRef {
        /// A qualified display path for the column (for pretty printing etc)
        qpath: QPath,
        /// An index into the tuple the expression is evaluated against
        index: TupleIndex,
    },
    FunctionCall {
        function: MonoFunction,
        args: Box<[Expr]>,
    },
    UnaryOperator {
        operator: Box<MonoOperator>,
        expr: Box<Expr>,
    },
    BinaryOperator {
        operator: Box<MonoOperator>,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
    },
    Case {
        scrutinee: Box<Expr>,
        cases: Box<[Case]>,
        else_result: Option<Box<Expr>>,
    },
    Subquery(Box<QueryPlan>),
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
pub struct MonoFunction(Box<(Function, Box<[LogicalType]>, LogicalType)>);

impl MonoFunction {
    pub fn new(function: Function, args: Box<[LogicalType]>, return_type: LogicalType) -> Self {
        Self(Box::new((function, args, return_type)))
    }

    #[inline]
    pub fn function(&self) -> Function {
        self.0.0.clone()
    }

    #[inline]
    pub fn return_type(&self) -> LogicalType {
        self.0.2.clone()
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            ExprKind::Literal(value) => write!(f, "{value}"),
            ExprKind::ColumnRef { qpath, index: _ } => write!(f, "{qpath}"),
            ExprKind::Array(exprs) => write!(f, "[{}]", exprs.iter().format(", ")),
            ExprKind::FunctionCall { function, args } => {
                write!(f, "{}({})", function.name(), args.iter().format(", "))
            }
            ExprKind::Alias { alias, expr } if alias.is_empty() => write!(f, "{expr}"),
            ExprKind::Alias { alias, expr } => write!(f, r#"({expr} AS "{alias}")"#),
            ExprKind::Case { scrutinee, cases, else_result } => {
                write!(f, "CASE {scrutinee} ")?;
                for case in cases.iter() {
                    writeln!(f, "\tWHEN {} THEN {} ", case.when, case.then)?;
                }

                if let Some(else_result) = else_result.as_ref() {
                    write!(f, "\tELSE {else_result} ")?;
                }
                write!(f, "END")
            }
            ExprKind::Subquery(_plan) => write!(f, "<subquery>"),
            ExprKind::UnaryOperator { operator, expr } => write!(f, "{operator}{expr}"),
            ExprKind::BinaryOperator { operator, lhs, rhs } => {
                write!(f, "({lhs} {operator} {rhs})")
            }
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
