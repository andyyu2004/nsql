#[macro_export]
macro_rules! static_assert {
    ($cond:expr) => {
        const _: [(); 1] = [(); $cond as usize];
    };
}


#[macro_export]
macro_rules! static_assert_eq {
    ($lhs:expr, $rhs:expr) => {
        $crate::static_assert!($lhs == $rhs);
    };
}

