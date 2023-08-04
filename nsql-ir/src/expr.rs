mod const_eval;

use std::fmt;
use std::ops::Deref;

pub use const_eval::EvalNotConst;
use itertools::Itertools;
use nsql_catalog::Function;
use nsql_core::{LogicalType, Name};
use nsql_storage::tuple::TupleIndex;
use nsql_storage::value::Value;

use crate::QPath;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Expr {
    pub ty: LogicalType,
    pub kind: ExprKind,
}

impl Expr {
    pub(crate) const NULL: Expr =
        Expr { ty: LogicalType::Null, kind: ExprKind::Literal(Value::Null) };

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExprKind {
    Literal(Value),
    Array(Box<[Expr]>),
    Alias {
        alias: Name,
        expr: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    BinOp {
        op: BinOp,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
    },
    ColumnRef {
        /// A qualified display path for the column (for pretty printing etc)
        qpath: QPath,
        /// An index into the tuple the expression is evaluated against
        index: TupleIndex,
    },
    FunctionCall {
        function: Function,
        args: Box<[Expr]>,
    },
    Case {
        scrutinee: Box<Expr>,
        cases: Box<[Case]>,
        else_result: Option<Box<Expr>>,
    },
}

impl fmt::Display for ExprKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            ExprKind::Literal(value) => write!(f, "{value}"),
            ExprKind::ColumnRef { qpath, index: _ } => write!(f, "{qpath}"),
            ExprKind::BinOp { op, lhs, rhs } => write!(f, "{lhs} {op} {rhs}"),
            ExprKind::Array(exprs) => write!(f, "[{}]", exprs.iter().format(", ")),
            ExprKind::FunctionCall { function, args } => {
                write!(f, "{}({})", function.name(), args.iter().format(", "))
            }
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
            ExprKind::UnaryOp { op, expr } => write!(f, "{op}{expr}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Case {
    pub when: Expr,
    pub then: Expr,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum UnaryOp {
    Neg,
}

impl fmt::Display for UnaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            UnaryOp::Neg => write!(f, "-"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum BinOp {
    Plus,
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
            BinOp::Plus => write!(f, "+"),
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
