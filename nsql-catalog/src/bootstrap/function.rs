use super::*;
use crate::FunctionKind;

pub(super) struct BootstrapFunction {
    pub oid: Oid<Function>,
    pub name: &'static str,
    pub kind: FunctionKind,
    pub args: Vec<LogicalType>,
    pub ret: LogicalType,
}

fn array(ty: LogicalType) -> LogicalType {
    LogicalType::array(ty)
}

macro_rules! function {
    ($oid:ident $name:tt $kind:ident : ( $($args:expr),* ) -> $ret:expr) => {{
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
    ($oid:ident $name:tt : ( $($args:expr),* ) -> $ret:expr) => {
        function!($oid $name Aggregate : ( $($args),* ) -> $ret)
    };
}

macro_rules! scalar {
    ($oid:ident $name:tt : ( $($args:expr),* ) -> $ret:expr) => {{
        function!($oid $name Scalar : ( $($args),* ) -> $ret)
    }};
}

macro_rules! cast {
    ($oid:ident $name:tt : $from:expr => $to:expr) => {{
        function!($oid $name Scalar : ( $from, $to ) -> $to)
    }};
}

macro_rules! comparison {
    ($oid:ident $name:tt : $ty:expr) => {{
        scalar!($oid $name : ( $ty, $ty ) -> Bool)
    }};
}

macro_rules! binary {
    ($oid:ident $name:tt : $ty:expr) => {{
        scalar!($oid $name : ( $ty, $ty ) -> $ty)
    }};
}

impl Function {
    mk_consts![
        NEG_INT,
        NOT_BOOL,
        EQ,
        LT,
        LTE,
        GTE,
        GT,
        ADD_INT,
        ARRAY_ELEMENT,
        ARRAY_POSITION,
        RANGE2,
        AVG_INT,
        SUM_INT,
        PRODUCT_INT,
        CAST_INT_TO_DEC
    ];
}

pub(super) fn bootstrap_data() -> Box<[BootstrapFunction]> {
    vec![
        // `(a, a) -> bool` operations
        comparison!(EQ        =  : Any),
        comparison!(LT        >  : Any),
        comparison!(LTE       >= : Any),
        comparison!(GTE       >= : Any),
        comparison!(GT        >  : Any),
        // `(a, a) -> a` operations
        binary!(ADD_INT  + : Int64),
        scalar!(NEG_INT  - : (Int64) -> Int64),
        scalar!(NOT_BOOL - : (Bool) -> Bool),
        scalar!(RANGE2 range : (Int64, Int64) -> array(Int64)),
        scalar!(ARRAY_ELEMENT array_element : (array(Any), Int64) -> Any),
        scalar!(ARRAY_POSITION array_position : (array(Any), Any) -> Int64),
        aggregate!(SUM_INT sum : (Int64) -> Int64),
        aggregate!(AVG_INT avg : (Int64) -> Float64),
        aggregate!(PRODUCT_INT product : (Int64) -> Int64),
        cast!(CAST_INT_TO_DEC cast : Int64 => Decimal),
    ]
    .into_boxed_slice()
}
