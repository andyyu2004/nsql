use super::*;
use crate::OperatorKind;

pub(super) struct BootstrapOperator {
    pub oid: Oid<Operator>,
    pub name: &'static str,
    pub kind: OperatorKind,
    pub function: Oid<Function>,
}

impl Operator {
    mk_consts![
        LT_INT, LT_FLOAT, LT_DEC, LTE_INT, LTE_FLOAT, LTE_DEC, GTE_INT, GTE_FLOAT, GTE_DEC, GT_INT,
        GT_FLOAT, GT_DEC, EQ_INT, EQ_FLOAT, EQ_DEC, ADD_INT, NEG_INT
    ];

    pub const LT: &'static str = "<";
    pub const LTE: &'static str = "<=";
    pub const EQ: &'static str = "=";
    pub const GTE: &'static str = ">=";
    pub const GT: &'static str = ">";
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
        infix!(LT, LT_INT),
        infix!(LT, LT_FLOAT),
        infix!(LT, LT_DEC),
        infix!(LTE, LTE_INT),
        infix!(LTE, LTE_FLOAT),
        infix!(LTE, LTE_DEC),
        infix!(EQ, EQ_INT),
        infix!(EQ, EQ_FLOAT),
        infix!(EQ, EQ_DEC),
        infix!(GTE, GTE_INT),
        infix!(GTE, GTE_FLOAT),
        infix!(GTE, GTE_DEC),
        infix!(GT, GT_INT),
        infix!(GT, GT_FLOAT),
        infix!(GT, GT_DEC),
    ]
    .into_boxed_slice()
}
