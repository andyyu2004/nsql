use nsql_core::Name;

use super::*;
use crate::OperatorKind;

impl Operator {
    mk_consts![
        EQ, NEQ, LT, LTE, GTE, GT, NEG_INT, NOT_BOOL, ADD_INT, ADD_FLOAT, ADD_DEC, SUB_INT,
        SUB_FLOAT, SUB_DEC, MUL_INT, MUL_FLOAT, MUL_DEC, DIV_INT, DIV_FLOAT, DIV_DEC, AND_BOOL,
        OR_BOOL
    ];

    pub const LESS: &'static str = "<";
    pub const LESS_EQUAL: &'static str = "<=";
    pub const EQUAL: &'static str = "=";
    pub const NOT_EQUAL: &'static str = "!=";
    pub const GREATER_EQUAL: &'static str = ">=";
    pub const GREATER: &'static str = ">";
    pub const STAR: &'static str = "*";
    pub const SLASH: &'static str = "/";
    pub const PLUS: &'static str = "+";
    pub const MINUS: &'static str = "-";
    pub const NOT: &'static str = "!";
    pub const CAST: &'static str = "::";
    pub const AND: &'static str = "AND";
    pub const OR: &'static str = "OR";
}

macro_rules! operator {
    ($name:ident, $oid:ident, $kind:ident) => {
        Operator {
            oid: Operator::$oid,
            namespace: Namespace::CATALOG,
            name: Name::new_inline(Operator::$name),
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

impl Operator {
    pub fn equal() -> Self {
        binary!(EQUAL, EQ)
    }

    pub(super) fn bootstrap_data() -> Box<[Operator]> {
        // the macro assumes that the corresponding `Function::$oid` is the same name as the `Operator::$oid`
        vec![
            unary!(MINUS, NEG_INT),
            unary!(NOT, NOT_BOOL),
            binary!(PLUS, ADD_INT),
            binary!(PLUS, ADD_FLOAT),
            binary!(PLUS, ADD_DEC),
            binary!(MINUS, SUB_INT),
            binary!(MINUS, SUB_FLOAT),
            binary!(MINUS, SUB_DEC),
            binary!(STAR, MUL_INT),
            binary!(STAR, MUL_FLOAT),
            binary!(STAR, MUL_DEC),
            binary!(SLASH, DIV_INT),
            binary!(SLASH, DIV_FLOAT),
            binary!(SLASH, DIV_DEC),
            binary!(EQUAL, EQ),
            binary!(NOT_EQUAL, NEQ),
            binary!(LESS, LT),
            binary!(LESS_EQUAL, LTE),
            binary!(GREATER_EQUAL, GTE),
            binary!(GREATER, GT),
            binary!(AND, AND_BOOL),
            binary!(OR, OR_BOOL),
        ]
        .into_boxed_slice()
    }
}
