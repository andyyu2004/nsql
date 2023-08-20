use super::*;
use crate::OperatorKind;

pub(super) struct BootstrapOperator {
    pub oid: Oid<Operator>,
    pub name: &'static str,
    pub kind: OperatorKind,
    pub function: Oid<Function>,
}

impl Operator {
    mk_consts![EQ, LT, LTE, GTE, GT, ADD_INT, NEG_INT];

    pub const LESS: &'static str = "<";
    pub const LESS_EQUAL: &'static str = "<=";
    pub const EQUAL: &'static str = "=";
    pub const NOT_EQUAL: &'static str = "=";
    pub const GREATER_EQUAL: &'static str = ">=";
    pub const GREATER: &'static str = ">";
    pub const PLUS: &'static str = "+";
    pub const MINUS: &'static str = "-";
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

macro_rules! infix {
    ($name:ident, $oid:ident) => {
        operator!($name, $oid, Infix)
    };
}

macro_rules! prefix {
    ($name:ident, $oid:ident) => {
        operator!($name, $oid, Prefix)
    };
}

pub(super) fn bootstrap_data() -> Box<[BootstrapOperator]> {
    // the macro assumes that the corresponding `Function::$oid` is the same name as the `Operator::$oid`
    vec![
        prefix!(MINUS, NEG_INT),
        infix!(PLUS, ADD_INT),
        infix!(EQUAL, EQ),
        infix!(LESS, LT),
        infix!(LESS_EQUAL, LTE),
        infix!(GREATER_EQUAL, GTE),
        infix!(GREATER, GT),
    ]
    .into_boxed_slice()
}
