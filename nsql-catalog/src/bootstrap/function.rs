use super::*;
use crate::FunctionKind;

pub(super) struct BootstrapFunction {
    pub oid: Oid<Function>,
    pub name: &'static str,
    pub kind: FunctionKind,
    pub args: Vec<LogicalType>,
    pub ret: LogicalType,
}

impl Function {
    mk_consts![
        RANGE2,
        SUM_INT,
        PRODUCT_INT,
        NEG_INT,
        AVG_INT,
        EQ_INT,
        GT_INT,
        GT_FLOAT,
        GT_DEC,
        ADD_INT,
        ARRAY_ELEMENT,
        ARRAY_POSITION
    ];
}

pub(super) fn bootstrap_data() -> Box<[BootstrapFunction]> {
    vec![
        BootstrapFunction {
            oid: Function::RANGE2,
            name: "range",
            kind: FunctionKind::Scalar,
            args: vec![LogicalType::Int64, LogicalType::Int64],
            ret: LogicalType::array(LogicalType::Int64),
        },
        BootstrapFunction {
            oid: Function::SUM_INT,
            name: "sum",
            kind: FunctionKind::Aggregate,
            args: vec![LogicalType::Int64],
            ret: LogicalType::Int64,
        },
        BootstrapFunction {
            oid: Function::PRODUCT_INT,
            name: "product",
            kind: FunctionKind::Aggregate,
            args: vec![LogicalType::Int64],
            ret: LogicalType::Int64,
        },
        BootstrapFunction {
            oid: Function::AVG_INT,
            name: "avg",
            kind: FunctionKind::Aggregate,
            args: vec![LogicalType::Int64],
            ret: LogicalType::Float64,
        },
        BootstrapFunction {
            oid: Function::EQ_INT,
            name: "=",
            kind: FunctionKind::Scalar,
            args: vec![LogicalType::Int64, LogicalType::Int64],
            ret: LogicalType::Bool,
        },
        BootstrapFunction {
            oid: Function::GT_INT,
            name: ">",
            kind: FunctionKind::Scalar,
            args: vec![LogicalType::Int64, LogicalType::Int64],
            ret: LogicalType::Bool,
        },
        BootstrapFunction {
            oid: Function::GT_FLOAT,
            name: ">",
            kind: FunctionKind::Scalar,
            args: vec![LogicalType::Float64, LogicalType::Int64],
            ret: LogicalType::Bool,
        },
        BootstrapFunction {
            oid: Function::GT_DEC,
            name: ">",
            kind: FunctionKind::Scalar,
            args: vec![LogicalType::Decimal, LogicalType::Decimal],
            ret: LogicalType::Bool,
        },
        BootstrapFunction {
            oid: Function::ADD_INT,
            name: "+",
            kind: FunctionKind::Scalar,
            args: vec![LogicalType::Int64, LogicalType::Int64],
            ret: LogicalType::Int64,
        },
        BootstrapFunction {
            oid: Function::NEG_INT,
            name: "-",
            kind: FunctionKind::Scalar,
            args: vec![LogicalType::Int64],
            ret: LogicalType::Int64,
        },
        BootstrapFunction {
            oid: Function::ARRAY_ELEMENT,
            name: "array_element",
            kind: FunctionKind::Scalar,
            args: vec![LogicalType::array(LogicalType::Any), LogicalType::Int64],
            ret: LogicalType::Any,
        },
        BootstrapFunction {
            oid: Function::ARRAY_POSITION,
            name: "array_position",
            kind: FunctionKind::Scalar,
            args: vec![LogicalType::array(LogicalType::Any), LogicalType::Any],
            ret: LogicalType::Int64,
        },
    ]
    .into_boxed_slice()
}
