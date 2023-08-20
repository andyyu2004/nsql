use super::*;
use crate::OperatorKind;

pub(super) struct BootstrapOperator {
    pub oid: Oid<Operator>,
    pub name: &'static str,
    pub kind: OperatorKind,
    pub function: Oid<Function>,
}

impl Operator {
    mk_consts![EQ, LT, LTE, GTE, GT, ADD_INT, NEG_INT, NOT_BOOL];

    pub const LESS: &'static str = "<";
    pub const LESS_EQUAL: &'static str = "<=";
    pub const EQUAL: &'static str = "=";
    pub const NOT_EQUAL: &'static str = "=";
    pub const GREATER_EQUAL: &'static str = ">=";
    pub const GREATER: &'static str = ">";
    pub const PLUS: &'static str = "+";
    pub const MINUS: &'static str = "-";
    pub const NOT: &'static str = "!";
    pub const CAST: &'static str = "::";
}

macro_rules! operator {
    ($name:ident, $oid:ident, $kind:ident) => {
        BootstrapOperator {
            oid: Operator::$oid,
            name: Operator::$name,
            kind: OperatorKind::$kind,
            function: Function::$oid,
        }
    };
}

macro_rules! binary {
    ($name:ident, $oid:ident) => {
        operator!($name, $oid, Binary)
    };
}

macro_rules! unary {
    ($name:ident, $oid:ident) => {
        operator!($name, $oid, Unary)
    };
}

pub(super) fn bootstrap_data() -> Box<[BootstrapOperator]> {
    // the macro assumes that the corresponding `Function::$oid` is the same name as the `Operator::$oid`
    vec![
        unary!(MINUS, NEG_INT),
        unary!(NOT, NOT_BOOL),
        binary!(PLUS, ADD_INT),
        binary!(EQUAL, EQ),
        binary!(LESS, LT),
        binary!(LESS_EQUAL, LTE),
        binary!(GREATER_EQUAL, GTE),
        binary!(GREATER, GT),
    ]
    .into_boxed_slice()
}
