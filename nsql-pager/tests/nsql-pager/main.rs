#![deny(rust_2018_idioms)]
#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]

use nsql_pager::{InMemoryPager, Pager, Result, SingleFilePager, PAGE_SIZE};

macro_rules! test_each_pager {
    (async fn $test_name:ident($var:ident) $body:block) => {
        nsql_test::test_each_impl! {
            async fn $test_name($var) $body
            for [
                file_pager: nsql_test::mk_file_pager!(),
                mem_pager: InMemoryPager::default()
            ]
        }
    };
}

test_each_pager! {
    async fn test_pager_alloc_zeroes_page(pager) {
        for _ in 0..100 {
            let idx = pager.alloc_page().await?;
            let page = pager.read_page(idx).await?;
            assert_eq!(page.data().as_ref(), [0u8; PAGE_SIZE]);
        }

        Ok(())
    }
}

test_each_pager! {
    async fn test_pager_read_after_write(pager) {
        for i in 0..PAGE_SIZE {
            let idx = pager.alloc_page().await?;
            let page = pager.read_page(idx).await?;

            page.data_mut()[i] = (i % u8::MAX as usize) as u8;
            let expected = *page.data();

            pager.write_page(page).await?;
            let page = pager.read_page(idx).await?;
            assert_eq!(page.data().as_ref(), expected.as_ref());
        }

        Ok(())
    }
}
