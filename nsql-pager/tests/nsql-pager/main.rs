#![deny(rust_2018_idioms)]
#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]

use nsql_pager::{InMemoryPager, Pager, Result, PAGE_SIZE};
use nsql_test::{mk_file_pager, test_each_impl};

macro_rules! test_each_pager {
    (fn $test_name:ident($var:ident) $body:block) => {
        test_each_impl! {
            fn $test_name($var) $body
            for [
                file_pager: mk_file_pager!(),
                mem_pager: InMemoryPager::default()
            ]
        }
    };
}

test_each_pager! {
    fn test_pager_alloc_zeroes_page(pager) {
        for _ in 0..100 {
            let idx = pager.alloc_page().await?;
            let page = pager.read_page(idx).await?;
            assert_eq!(page.data(), &[0; PAGE_SIZE]);
        }

        Ok(())
    }
}

test_each_pager! {
    fn test_pager_read_after_write(pager) {
        for i in 0..PAGE_SIZE {
            let idx = pager.alloc_page().await?;
            let mut page = pager.read_page(idx).await?;

            page.data_mut()[i] = (i % u8::MAX as usize) as u8;
            let expected = *page.data();

            pager.write_page(idx, page).await?;
            let page = pager.read_page(idx).await?;
            assert_eq!(page.data(), &expected);
        }

        Ok(())
    }
}
