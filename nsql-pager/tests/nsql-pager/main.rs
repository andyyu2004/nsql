#![deny(rust_2018_idioms)]
#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]

use nsql_pager::{InMemoryPager, Pager, Result, PAGE_SIZE};
use nsql_test::{mk_file_pager, test_each_impl};

test_each_impl!(
    test_pager_alloc_zeroes_page,
    [single_file: mk_file_pager!(), in_memory: InMemoryPager::default()],
    |pager| {
        let idx = pager.alloc_page().await?;
        let page = pager.read_page(idx).await?;
        assert_eq!(page.data(), &[0; PAGE_SIZE]);
        Ok(())
    }
);
