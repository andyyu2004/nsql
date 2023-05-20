pub use tempfile;

#[macro_export]
macro_rules! tmp {
    () => {
        $crate::tempfile::tempdir()?.path().join("test.db")
    };
}

#[macro_export]
macro_rules! test_each_impl {
    (
        async fn $test_name:ident($var:ident) $body:block
        for [ $($impl_name:ident: $imp:expr),* ]
    ) => {
        mod $test_name {
            use super::*;
            $(
                    use super::*;

                    #[test]
                    fn $impl_name() -> Result<()> {
                        $crate::start(async {
                            let $var = $imp;
                            $body
                        })
                    }
            )*
        }
    };
}

// #[macro_export]
// macro_rules! test_each_impl {
//     ($test_name:ident, [$($impl_name:ident: $impl:expr),*], |$x:ident| $block:block) => {
//         $(
//             mod $impl_name {
//                 use super::*;
//                 #[test]
//                 fn $test_name() -> Result<()> {
//                     $crate::start(async {
//                         let $x = $impl;
//                         $block
//                     })
//                 }
//             }
//         )*
//     };
// }
