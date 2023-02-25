use std::fmt;

use bigdecimal::BigDecimal;

#[derive(Debug)]
pub struct Tuple {
    values: Box<[Value]>,
}

impl Tuple {
    #[inline]
    pub fn values(&self) -> &[Value] {
        self.values.as_ref()
    }
}

impl From<Vec<Value>> for Tuple {
    fn from(values: Vec<Value>) -> Self {
        Self { values: values.into_boxed_slice() }
    }
}

impl FromIterator<Value> for Tuple {
    fn from_iter<I: IntoIterator<Item = Value>>(iter: I) -> Self {
        Self { values: iter.into_iter().collect::<Vec<_>>().into_boxed_slice() }
    }
}

#[derive(Debug)]
pub enum Value {
    Literal(Literal),
}

#[derive(Debug)]
pub enum Literal {
    Null,
    Bool(bool),
    Decimal(BigDecimal),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Literal(literal) => write!(f, "{literal}"),
        }
    }
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::Null => write!(f, "NULL"),
            Literal::Bool(b) => write!(f, "{b}"),
            Literal::Decimal(d) => write!(f, "{d}"),
        }
    }
}
