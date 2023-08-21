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
    ($oid:ident : $from:expr => $to:expr) => {{
        // casts from a => b are implemented as functions `(a, b) -> b`
        // where the second argument is a dummy just for function overloading resolution
        scalar!($oid cast : ( $from, $to ) -> $to)
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
        ARRAY_ELEMENT,
        ARRAY_POSITION,
        RANGE2,
        AVG_INT,
        SUM_INT,
        PRODUCT_INT,
        CAST_SELF,
        CAST_INT_TO_DEC,
        CAST_INT_TO_FLOAT
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
        binary!(ADD_INT    + : Int64),
        binary!(ADD_FLOAT  + : Float64),
        binary!(ADD_DEC    + : Decimal),
        binary!(SUB_INT    - : Int64),
        binary!(SUB_FLOAT  - : Float64),
        binary!(SUB_DEC    - : Decimal),
        binary!(MUL_INT    * : Int64),
        binary!(MUL_FLOAT  * : Float64),
        binary!(MUL_DEC    * : Decimal),
        binary!(DIV_INT    / : Int64),
        binary!(DIV_FLOAT  / : Float64),
        binary!(DIV_DEC    / : Decimal),
        // general scalar functions
        scalar!(NEG_INT  - : (Int64) -> Int64),
        scalar!(NOT_BOOL - : (Bool) -> Bool),
        scalar!(RANGE2 range : (Int64, Int64) -> array(Int64)),
        scalar!(ARRAY_ELEMENT array_element : (array(Any), Int64) -> Any),
        scalar!(ARRAY_POSITION array_position : (array(Any), Any) -> Int64),
        // aggregates
        aggregate!(SUM_INT sum : (Int64) -> Int64),
        aggregate!(AVG_INT avg : (Int64) -> Float64),
        aggregate!(PRODUCT_INT product : (Int64) -> Int64),
        // casts
        cast!(CAST_SELF          : Any => Any),
        cast!(CAST_INT_TO_DEC    : Int64 => Decimal),
        cast!(CAST_INT_TO_FLOAT  : Int64 => Float64),
    ]
    .into_boxed_slice()
}
