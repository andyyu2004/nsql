use std::io;

use nsql_test::mk_file_pager;
use proptest::sample::size_range;
use test_strategy::{proptest, Arbitrary};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{MetaPageReader, MetaPageWriter};
use crate::{InMemoryPager, Pager, Result, SingleFilePager, PAGE_SIZE};

#[derive(Debug, Clone, Copy, Arbitrary)]
enum Action {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    // adding this variant to speed up proptest generation as we can then use a lower number of actions
    // whilst still exercise multiple pages
    Large([u8; 128]),
}

/// run a sequence of write actions and then assert that reading returns the values that were written in order
async fn run_read_write(pager: impl Pager, actions: &[Action]) -> Result<()> {
    let initial_page = pager.alloc_page().await?;

    let mut writer = MetaPageWriter::new(&pager, initial_page);
    for &action in actions {
        match action {
            Action::U8(u) => writer.write_u8(u).await?,
            Action::U16(u) => writer.write_u16(u).await?,
            Action::U32(u) => writer.write_u32(u).await?,
            Action::U64(u) => writer.write_u64(u).await?,
            Action::Large(large) => writer.write_all(&large).await?,
        }
    }

    let mut reader = MetaPageReader::new(&pager, initial_page);
    for &action in actions {
        match action {
            Action::U8(u) => assert_eq!(reader.read_u8().await?, u),
            Action::U16(u) => assert_eq!(reader.read_u16().await?, u),
            Action::U32(u) => assert_eq!(reader.read_u32().await?, u),
            Action::U64(u) => assert_eq!(reader.read_u64().await?, u),
            Action::Large(u) => {
                let mut buf = [0u8; 128];
                reader.read_exact(&mut buf).await?;
                assert_eq!(buf, u);
            }
        }
    }

    pager.read_page(initial_page).await?;
    panic!("should have failed to read page");

    // for _ in 0..PAGE_SIZE {
    //     // if we keep reading we should hit EOF by the end of the last page
    //     // this is testing that the next pointer is correctly set to INVALID
    //     match reader.read_u8().await {
    //         Ok(u) => assert_eq!(u, 0, "rest of page should be zeroed"),
    //         Err(err) => match err.kind() {
    //             std::io::ErrorKind::UnexpectedEof => return Ok(()),
    //             _ => Err(err)?,
    //         },
    //     }
    // }

    Err(io::Error::new(io::ErrorKind::Other, "expected to hit EOF by now"))?
}

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
    async fn test_meta_page_read_write_simple(pager) {
        let actions = (0..10000u16).map(Action::U16).collect::<Vec<_>>();
        run_read_write(pager, &actions).await
    }
}

test_each_pager! {
    async fn test_meta_page_read_of_unwritten_page(pager) {
        // if we read from a page that has not been written to then we should get an error
        let err = run_read_write(pager, &[]).await.unwrap_err();
        assert_eq!(err.current_context().kind(), io::ErrorKind::Other);
        Ok(())
    }
}

#[proptest]
fn test_meta_page_read_write(#[any(size_range(1..100).lift())] actions: Vec<Action>) {
    nsql_test::start(async {
        run_read_write(InMemoryPager::default(), &actions).await?;
        run_read_write(mk_file_pager!(), &actions).await
    })
    .unwrap()
}
