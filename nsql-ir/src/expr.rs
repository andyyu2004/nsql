use std::fmt;

use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub enum TableExpr {
    Values(Values),
}

#[derive(Debug, Clone)]
pub enum Expr {
    Literal(Literal),
}

#[derive(Debug, Clone)]
pub enum Literal {
    Null,
    Bool(bool),
    Decimal(Decimal),
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::Null => write!(f, "NULL"),
            Literal::Bool(b) => write!(f, "{b}"),
            Literal::Decimal(n) => write!(f, "{n}"),
        }
    }
}

pub type Values = Vec<Vec<Expr>>;