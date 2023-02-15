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
        nsql_pager::SingleFilePager::open($crate::tmp!()).await?
    };
}

#[macro_export]
macro_rules! mk_file_pager_sync {
    () => {
        $crate::run(nsql_pager::SingleFilePager::open($crate::tmp!())).unwrap()
    };
}

pub fn run<F: Future>(fut: F) -> F::Output {
    tokio_uring::start(fut)
}

#[macro_export]
macro_rules! test_each_impl {
    ($test_name:ident, [$($impl_name:ident: $impl:expr),*], |$x:ident| $block:block) => {
        $(
            mod $impl_name {
                use super::*;

                fn $test_name() -> Result<()> {
                    nsql_test::run(async {
                        let $x = $impl;
                        $block
                        Ok(())
                    })
                }
            }
        )*
    };
}
