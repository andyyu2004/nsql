use nsql_core::Name;

use super::*;
use crate::OperatorKind;

impl Operator {
    mk_consts![
        EQ_ANY,
        NEQ_ANY,
        LT_ANY,
        LTE_ANY,
        GTE_ANY,
        GT_ANY,
        IS_DISTINCT_FROM_ANY,
        IS_NOT_DISTINCT_FROM_ANY,
        NEG_INT,
        NEG_FLOAT,
        NEG_DEC,
        NOT_BOOL,
        ADD_INT,
        ADD_FLOAT,
        ADD_DEC,
        SUB_INT,
        SUB_FLOAT,
        SUB_DEC,
        MUL_INT,
        MUL_FLOAT,
        MUL_DEC,
        DIV_INT,
        DIV_FLOAT,
        DIV_DEC,
        AND_BOOL,
        OR_BOOL
    ];

    pub const LT: &'static str = "<";
    pub const LTE: &'static str = "<=";
    pub const EQ: &'static str = "=";
    pub const NEQ: &'static str = "!=";
    pub const GTE: &'static str = ">=";
    pub const GT: &'static str = ">";
    pub const STAR: &'static str = "*";
    pub const SLASH: &'static str = "/";
    pub const PLUS: &'static str = "+";
    pub const MINUS: &'static str = "-";
    pub const NOT: &'static str = "!";
    pub const CAST: &'static str = "::";
    pub const AND: &'static str = "AND";
    pub const OR: &'static str = "OR";
    pub const IS_DISTINCT_FROM: &'static str = "IS DISTINCT FROM";
    pub const IS_NOT_DISTINCT_FROM: &'static str = "IS NOT DISTINCT FROM";
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
        binary!(EQ, EQ_ANY)
    }

    pub(super) fn bootstrap_data() -> Box<[Operator]> {
        // the macro assumes that the corresponding `Function::$oid` is the same name as the `Operator::$oid`
        vec![
            unary!(MINUS, NEG_INT),
            unary!(MINUS, NEG_FLOAT),
            unary!(MINUS, NEG_DEC),
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
            binary!(EQ, EQ_ANY),
            binary!(NEQ, NEQ_ANY),
            binary!(LT, LT_ANY),
            binary!(LTE, LTE_ANY),
            binary!(GTE, GTE_ANY),
            binary!(GT, GT_ANY),
            binary!(AND, AND_BOOL),
            binary!(OR, OR_BOOL),
            binary!(IS_DISTINCT_FROM, IS_DISTINCT_FROM_ANY),
            binary!(IS_NOT_DISTINCT_FROM, IS_NOT_DISTINCT_FROM_ANY),
        ]
        .into_boxed_slice()
    }
}
