use nsql_core::Name;

use super::*;
use crate::FunctionKind;

fn array(ty: LogicalType) -> LogicalType {
    LogicalType::array(ty)
}

macro_rules! function {
    ($oid:ident $name:tt $kind:ident : ( $($args:expr),* ) -> $ret:expr) => {{
        use LogicalType::*;
        Function {
            namespace: Namespace::MAIN,
            oid: Function::$oid,
            name: Name::new_inline(stringify!($name)),
            kind: FunctionKind::$kind,
            args: vec![ $($args),* ].into_boxed_slice(),
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
        NEG_FLOAT,
        NEG_DEC,
        NOT_BOOL,
        EQ_ANY,
        NEQ_ANY,
        LT_ANY,
        LTE_ANY,
        GTE_ANY,
        GT_ANY,
        MAX_ANY,
        MIN_ANY,
        IS_DISTINCT_FROM_ANY,
        IS_NOT_DISTINCT_FROM_ANY,
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
        ABS_INT,
        ABS_FLOAT,
        ABS_DEC,
        BETWEEN_ANY,
        OR_BOOL,
        AND_BOOL,
        ARRAY_ELEMENT,
        ARRAY_POSITION,
        RANGE2,
        FIRST,
        AVG_INT,
        SUM_INT,
        PRODUCT_INT,
        COUNT,
        COUNT_STAR,
        CAST_SELF,
        CAST_INT_TO_DEC,
        CAST_INT_TO_FLOAT,
        CAST_INT_TO_OID,
        NEXTVAL,
        NEXTVAL_OID,
        MK_NEXTVAL_EXPR
    ];

    pub fn count_star() -> Function {
        aggregate!(COUNT_STAR count: () -> Int64)
    }

    pub fn first() -> Function {
        aggregate!(FIRST first: (Any) -> Any)
    }

    pub fn equal() -> Function {
        comparison!(EQ_ANY = : Any)
    }

    pub fn is_not_distinct_from() -> Function {
        comparison!(IS_NOT_DISTINCT_FROM_ANY is_not_distinct_from : Any)
    }

    pub fn and() -> Function {
        binary!(AND_BOOL AND : Bool)
    }

    pub(super) fn bootstrap_data() -> Box<[Function]> {
        vec![
            // `(a, a) -> bool` operations
            comparison!(EQ_ANY                   =                    : Any),
            comparison!(NEQ_ANY                  !=                   : Any),
            comparison!(LT_ANY                   <                    : Any),
            comparison!(LTE_ANY                  <=                   : Any),
            comparison!(GTE_ANY                  >=                   : Any),
            comparison!(GT_ANY                   >                    : Any),
            comparison!(IS_DISTINCT_FROM_ANY     is_distinct_from     : Any),
            comparison!(IS_NOT_DISTINCT_FROM_ANY is_not_distinct_from : Any),
            // `(a, a) -> a` operations
            binary!(ADD_INT    +   : Int64),
            binary!(ADD_FLOAT  +   : Float64),
            binary!(ADD_DEC    +   : Decimal),
            binary!(SUB_INT    -   : Int64),
            binary!(SUB_FLOAT  -   : Float64),
            binary!(SUB_DEC    -   : Decimal),
            binary!(MUL_INT    *   : Int64),
            binary!(MUL_FLOAT  *   : Float64),
            binary!(MUL_DEC    *   : Decimal),
            binary!(DIV_INT    /   : Int64),
            binary!(DIV_FLOAT  /   : Float64),
            binary!(DIV_DEC    /   : Decimal),
            binary!(AND_BOOL   and : Bool),
            binary!(OR_BOOL    or  : Bool),
            // general scalar functions
            scalar!(NEG_INT   -     : (Int64) -> Int64),
            scalar!(NEG_FLOAT -     : (Float64) -> Float64),
            scalar!(NEG_DEC   -     : (Decimal) -> Decimal),
            scalar!(NOT_BOOL  -     : (Bool) -> Bool),
            scalar!(ABS_INT   abs   : (Int64) -> Int64),
            scalar!(ABS_FLOAT abs   : (Float64) -> Float64),
            scalar!(ABS_DEC   abs   : (Decimal) -> Decimal),
            scalar!(RANGE2    range : (Int64, Int64) -> array(Int64)),
            scalar!(ARRAY_ELEMENT array_element : (array(Any), Int64) -> Any),
            scalar!(ARRAY_POSITION array_position : (array(Any), Any) -> Int64),
            scalar!(BETWEEN_ANY between : (Any, Any, Any) -> Bool),
            // aggregates
            aggregate!(SUM_INT     sum     : (Int64) -> Int64),
            aggregate!(AVG_INT     avg     : (Int64) -> Float64),
            aggregate!(PRODUCT_INT product : (Int64) -> Int64),
            aggregate!(FIRST       first   : (Any)   -> Any),
            aggregate!(COUNT       count   : (Any)   -> Int64),
            aggregate!(COUNT_STAR  count   : ()      -> Int64),
            aggregate!(MIN_ANY         min     : (Any)   -> Any),
            aggregate!(MAX_ANY         max     : (Any)   -> Any),
            // casts
            cast!(CAST_SELF          : Any   => Any),
            cast!(CAST_INT_TO_DEC    : Int64 => Decimal),
            cast!(CAST_INT_TO_FLOAT  : Int64 => Float64),
            cast!(CAST_INT_TO_OID    : Int64 => Oid),
            // sequences
            scalar!(NEXTVAL             nextval         : (Oid) -> Int64),
            scalar!(NEXTVAL_OID         nextval_oid     : (Oid) -> Oid),
            scalar!(MK_NEXTVAL_EXPR     mk_nextval_expr : (Oid) -> Expr),
        ]
        .into_boxed_slice()
    }
}
