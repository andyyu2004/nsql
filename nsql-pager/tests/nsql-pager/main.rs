#![deny(rust_2018_idioms)]
#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]

use nsql_pager::{InMemoryPager, Result};
use nsql_test::{mk_file_pager_sync, test_each_impl};

test_each_impl!(
    test_pager,
    [single_file: mk_file_pager_sync!(), in_memory: InMemoryPager::default()],
    |pager| {}
);
