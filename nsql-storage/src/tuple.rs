use std::fmt;

use nsql_ir::Literal;

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

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Literal(literal) => write!(f, "{literal}"),
        }
    }
}
