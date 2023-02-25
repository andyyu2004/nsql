use bigdecimal::BigDecimal;

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
    Number(BigDecimal),
}

pub type Values = Vec<Vec<Expr>>;
