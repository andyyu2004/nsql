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
        nsql_storage::Storage::create($crate::tmp!()).await?
    };
}

#[macro_export]
macro_rules! mk_file_pager {
    () => {
        nsql_pager::SingleFilePager::create($crate::tmp!()).await?
    };
}

#[inline]
pub fn start<F: Future>(fut: F) -> F::Output {
    tokio_uring::start(fut)
}

#[macro_export]
macro_rules! test_each_impl {
    ($test_name:ident, [$($impl_name:ident: $impl:expr),*], |$x:ident| $block:block) => {
        $(
            mod $impl_name {
                use super::*;

                #[test]
                fn $test_name() -> Result<()> {
                    $crate::start(async {
                        let $x = $impl;
                        $block
                    })
                }
            }
        )*
    };
}
