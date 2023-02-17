use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::{cmp, io};

use bytes::BufMut;
use pin_utils::pin_mut;
use tokio::io::AsyncWrite;

use super::PAGE_IDX_SIZE;
use crate::{Page, PageIndex, Pager, PAGE_SIZE};
pub(crate) struct MetaPageWriter<'a, P> {
    pager: &'a P,
    page_idx: PageIndex,
    page: Option<Page>,
    byte_index: Cell<usize>,
}

impl<'a, P> MetaPageWriter<'a, P> {
    // note: we have to careful if we're writing out the free list, because we can't reuse the free list maybe?
    pub fn new(pager: &'a P, initial_page_idx: PageIndex) -> Self {
        assert!(initial_page_idx.is_valid(), "passed in invalid initial page index");
        Self { pager, page_idx: initial_page_idx, page: None, byte_index: Cell::new(0) }
    }
}

impl<'a, P: Pager> AsyncWrite for MetaPageWriter<'a, P> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let page = match &self.page {
            Some(page) => page,
            None => {
                let fut = self.pager.read_page(self.page_idx);
                pin_mut!(fut);
                let page = ready!(fut.poll(cx))?;
                // initialize the next page index to invalid so the reader knows when it's finished
                (&mut page.data_mut()[..PAGE_IDX_SIZE]).put_u32(PageIndex::INVALID.as_u32());
                self.page = Some(page);
                unsafe { self.page.as_ref().unwrap_unchecked() }
            }
        };

        let byte_index = self.byte_index.get();
        let amt = cmp::min(buf.len(), PAGE_SIZE - PAGE_IDX_SIZE - byte_index);
        page.data_mut()[PAGE_IDX_SIZE + byte_index..PAGE_IDX_SIZE + byte_index + amt]
            .copy_from_slice(&buf[..amt]);
        self.byte_index.set(byte_index + amt);

        if byte_index + amt == PAGE_SIZE - PAGE_IDX_SIZE {
            let next_page_idx_fut = self.pager.alloc_page();
            pin_mut!(next_page_idx_fut);
            let next_page_idx = ready!(next_page_idx_fut.poll(cx))?;
            (&mut page.data_mut()[..PAGE_IDX_SIZE]).put_u32(next_page_idx.as_u32());
            debug_assert_eq!(page.data()[..PAGE_IDX_SIZE], next_page_idx.as_u32().to_be_bytes());

            let fut = self.pager.write_page(self.page_idx, self.page.take().unwrap());
            pin_mut!(fut);
            ready!(fut.poll(cx))?;

            self.page_idx = next_page_idx;
            self.byte_index.set(0);
        }

        Poll::Ready(Ok(amt))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }
}
