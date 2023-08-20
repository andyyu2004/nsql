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

fn array(ty: LogicalType) -> LogicalType {
    LogicalType::array(ty)
}

macro_rules! function {
    ($oid:ident @ $name:tt $kind:ident : ( $($args:expr),* ) -> $ret:expr) => {{
        use LogicalType::*;
        BootstrapFunction {
            oid: Function::$oid,
            name: stringify!($name),
            kind: FunctionKind::$kind,
            args: vec![ $($args),* ],
            ret: $ret
        }
    }};
}

macro_rules! aggregate {
    ($oid:ident @ $name:tt : ( $($args:expr),* ) -> $ret:expr) => {
        function!($oid@$name Aggregate : ( $($args),* ) -> $ret)
    };
}

macro_rules! scalar {
    ($oid:ident @ $name:tt : ( $($args:expr),* ) -> $ret:expr) => {{
        function!($oid @ $name Scalar : ( $($args),* ) -> $ret)
    }};
}

macro_rules! comparison {
    ($oid:ident @ $name:tt : $ty:expr) => {{
        scalar!($oid @ $name : ( $ty, $ty ) -> Bool)
    }};
}

macro_rules! binary {
    ($oid:ident @ $name:tt : $ty:expr) => {{
        scalar!($oid @ $name : ( $ty, $ty ) -> $ty)
    }};
}

pub(super) fn bootstrap_data() -> Box<[BootstrapFunction]> {
    vec![
        // `(a, a) -> bool` operations
        comparison!(EQ_INT   @ = : Int64),
        comparison!(GT_INT   @ > : Int64),
        comparison!(GT_FLOAT @ > : Float64),
        comparison!(GT_DEC   @ > : Decimal),
        // `(a, a) -> a` operations
        binary!(ADD_INT @ + : Int64),
        scalar!(NEG_INT @ - : (Int64) -> Int64),
        scalar!(RANGE2 @ range : (Int64, Int64) -> array(Int64)),
        scalar!(ARRAY_ELEMENT @ array_element : (array(Any), Int64) -> Any),
        scalar!(ARRAY_POSITION @ array_position : (array(Any), Any) -> Int64),
        aggregate!(SUM_INT @ sum : (Int64) -> Int64),
        aggregate!(AVG_INT @ avg : (Int64) -> Float64),
        aggregate!(PRODUCT_INT @ product : (Int64) -> Int64),
    ]
    .into_boxed_slice()
}
