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
        NEG_INT,
        EQ_INT,
        EQ_DEC,
        EQ_FLOAT,
        LT_INT,
        LT_FLOAT,
        LT_DEC,
        LTE_INT,
        LTE_FLOAT,
        LTE_DEC,
        GTE_INT,
        GTE_FLOAT,
        GTE_DEC,
        GT_INT,
        GT_FLOAT,
        GT_DEC,
        ADD_INT,
        ARRAY_ELEMENT,
        ARRAY_POSITION,
        RANGE2,
        AVG_INT,
        SUM_INT,
        PRODUCT_INT
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
        comparison!(EQ_INT    @ = : Int64),
        comparison!(EQ_DEC    @ = : Decimal),
        comparison!(EQ_FLOAT  @ = : Float64),
        comparison!(LT_INT    @ > : Int64),
        comparison!(LT_FLOAT  @ > : Float64),
        comparison!(LT_DEC    @ > : Decimal),
        comparison!(LTE_INT   @ >= : Int64),
        comparison!(LTE_FLOAT @ >= : Float64),
        comparison!(LTE_DEC   @ >= : Decimal),
        comparison!(GTE_INT   @ >= : Int64),
        comparison!(GTE_FLOAT @ >= : Float64),
        comparison!(GTE_DEC   @ >= : Decimal),
        comparison!(GT_INT    @ > : Int64),
        comparison!(GT_FLOAT  @ > : Float64),
        comparison!(GT_DEC    @ > : Decimal),
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
