use super::*;
use crate::OperatorKind;

pub(super) struct BootstrapOperator {
    pub oid: Oid<Operator>,
    pub name: &'static str,
    pub kind: OperatorKind,
    pub function: Oid<Function>,
}

impl Operator {
    mk_consts![GT_INT];
}

pub(super) fn bootstrap_data() -> Box<[BootstrapOperator]> {
    vec![BootstrapOperator {
        oid: Operator::GT_INT,
        name: ">",
        kind: OperatorKind::Infix,
        function: Function::GT_INT,
    }]
    .into_boxed_slice()
}
