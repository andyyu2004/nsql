use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::{cmp, io};

use bytes::Buf;
use pin_utils::pin_mut;
use tokio::io::{AsyncRead, ReadBuf};

use super::PAGE_IDX_SIZE;
use crate::{Page, PageIndex, Pager, PAGE_SIZE};

/// A meta page contains metadata and has the following format (excluding the usual checksum):
/// [next_page_idx: 4 bytes][arbitrary data]
pub(crate) struct MetaPageReader<'a, P> {
    pager: &'a P,
    next_page_idx: PageIndex,
    page: Option<Page>,
    byte_index: Cell<usize>,
}

impl<'a, P> MetaPageReader<'a, P> {
    pub fn new(pager: &'a P, page_idx: PageIndex) -> Self {
        assert!(page_idx.is_valid());
        Self { pager, next_page_idx: page_idx, page: None, byte_index: Cell::new(0) }
    }
}

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
            self.next_page_idx = PageIndex::new_maybe_invalid(page.data().as_ref().get_u32());
            self.page = Some(page);
            self.byte_index.set(0);

            if self.next_page_idx.is_zero() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "`{}` is not a valid meta page (the `next_page_idx` has not been written)",
                        self.next_page_idx
                    ),
                )));
            }
        }

        debug_assert!(self.page.is_some());
        let page = unsafe { self.page.as_ref().unwrap_unchecked() };

        let index = self.byte_index.get();
        let data = &page.data()[PAGE_IDX_SIZE + index..];
        let amt = cmp::min(data.len(), buf.remaining());
        buf.put_slice(&data[..amt]);
        self.byte_index.set(index + amt);

        Poll::Ready(Ok(()))
    }
}
