use super::*;
use crate::OperatorKind;

pub(super) struct BootstrapOperator {
    pub oid: Oid<Operator>,
    pub name: &'static str,
    pub kind: OperatorKind,
    pub function: Oid<Function>,
}

impl Operator {
    mk_consts![GT_INT, GT_DEC, EQ_INT, ADD_INT, MINUS_INT];

    pub const GT: &'static str = ">";
    pub const EQ: &'static str = "=";
    pub const PLUS: &'static str = "+";
    pub const MINUS: &'static str = "-";
}

pub(super) fn bootstrap_data() -> Box<[BootstrapOperator]> {
    vec![
        BootstrapOperator {
            oid: Operator::ADD_INT,
            name: Operator::PLUS,
            kind: OperatorKind::Infix,
            function: Function::ADD_INT,
        },
        BootstrapOperator {
            oid: Operator::MINUS_INT,
            name: Operator::PLUS,
            kind: OperatorKind::Prefix,
            function: Function::NEG_INT,
        },
        BootstrapOperator {
            oid: Operator::GT_INT,
            name: Operator::GT,
            kind: OperatorKind::Infix,
            function: Function::GT_INT,
        },
        BootstrapOperator {
            oid: Operator::GT_DEC,
            name: Operator::GT,
            kind: OperatorKind::Infix,
            function: Function::GT_DEC,
        },
        BootstrapOperator {
            oid: Operator::EQ_INT,
            name: Operator::EQ,
            kind: OperatorKind::Infix,
            function: Function::EQ_INT,
        },
    ]
    .into_boxed_slice()
}
