use std::future::Future;

pub use {tempfile, tokio_uring};

#[macro_export]
macro_rules! tmp {
    () => {
        $crate::tempfile::tempdir()?.path().join("test.db")
    };
}

#[macro_export]
macro_rules! mk_storage {
    () => {
        nsql_fs::File::create($crate::tmp!()).await?
    };
}

#[macro_export]
macro_rules! mk_mem_buffer_pool {
    () => {
        nsql_buffer::BufferPool::new(::std::sync::Arc::new(InMemoryPager::new()))
    };
}

#[macro_export]
macro_rules! mk_file_pager {
    () => {
        SingleFilePager::create($crate::tmp!()).await?
    };
}

#[inline]
pub fn start<F: Future>(fut: F) -> F::Output {
    tokio_uring::start(fut)
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
