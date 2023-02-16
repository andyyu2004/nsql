use std::cell::Cell;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use bytes::Buf;
use pin_utils::pin_mut;
use tokio::io::{AsyncRead, ReadBuf};

use crate::{Page, PageIndex, Pager, Result, PAGE_SIZE};

/// A meta page contains metadata and has the following format (excluding the usual checksum):
/// [next_page_idx: 4 bytes][arbitrary data]
pub(crate) struct MetaPageReader<'a, P> {
    pager: &'a P,
    next_page_idx: PageIndex,
    page: Option<Page>,
    byte_index: Cell<usize>,
}

impl<'a, P> MetaPageReader<'a, P> {
    pub(crate) fn new(pager: &'a P, page_idx: PageIndex) -> Self {
        assert!(page_idx.is_valid());
        Self { pager, next_page_idx: page_idx, page: None, byte_index: Cell::new(0) }
    }
}

const PAGE_IDX_SIZE: usize = std::mem::size_of::<PageIndex>();

impl<'a, P: Pager> AsyncRead for MetaPageReader<'a, P> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.page.is_none() || self.byte_index.get() >= PAGE_SIZE - PAGE_IDX_SIZE {
            if self.next_page_idx == PageIndex::INVALID {
                return Poll::Ready(Ok(()));
            }

            let fut = self.pager.read_page(self.next_page_idx);
            pin_mut!(fut);
            let page = ready!(fut.poll(cx))?;
            self.next_page_idx = PageIndex::new(page.data().as_ref().get_u32());
            self.page = Some(page);
        }

        debug_assert!(self.page.is_some());
        let page = unsafe { self.page.as_ref().unwrap_unchecked() };

        let index = self.byte_index.get();
        let data = &page.data()[PAGE_IDX_SIZE + index..];
        let amt = std::cmp::min(data.len(), buf.remaining());
        buf.put_slice(&data[..amt]);
        self.byte_index.set(index + amt);

        Poll::Ready(Ok(()))
    }
}
