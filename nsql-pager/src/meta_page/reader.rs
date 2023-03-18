use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::{cmp, io, mem};

use bytes::Buf;
use futures_executor::block_on;
use tokio::io::{AsyncRead, ReadBuf};

use super::try_io;
use crate::{BoxFuture, Page, PageIndex, Pager, Result, PAGE_DATA_SIZE};

/// A meta page contains metadata and has the following format (excluding the usual checksum):
/// [next_page_idx: 4 bytes][arbitrary data]
pub struct MetaPageReader<'a, P> {
    pager: &'a P,
    state: State<'a>,
}

impl<'a, P> MetaPageReader<'a, P> {
    pub fn new(pager: &'a P, page_idx: PageIndex) -> Self {
        Self { pager, state: State::NeedNext { next_page_idx: Some(page_idx) } }
    }
}

enum State<'a> {
    NeedNext { next_page_idx: Option<PageIndex> },
    PollNext { fut: BoxFuture<'a, Result<Page>> },
    Read { page: Page, byte_index: usize },
}

impl<P: Pager> AsyncRead for MetaPageReader<'_, P> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            let pager = self.pager;
            match &mut self.state {
                State::NeedNext { next_page_idx } => {
                    let next_page_idx = match next_page_idx {
                        Some(idx) => idx,
                        None => return Poll::Ready(Ok(())),
                    };

                    if next_page_idx.is_zero() {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "page `{next_page_idx}` is not a valid meta page (the `next_page_idx` has not been written)",
                            ),
                        )));
                    }

                    let fut = Box::pin(pager.read_page(*next_page_idx));
                    self.state = State::PollNext { fut };
                }
                State::PollNext { fut } => {
                    let page = try_io!(ready!(fut.as_mut().poll(cx)));
                    self.state = State::Read { page, byte_index: 0 };
                }
                State::Read { page, byte_index } => {
                    let view = block_on(page.data());
                    let data = &view[mem::size_of::<PageIndex>() + *byte_index..];
                    let amt = cmp::min(data.len(), buf.remaining());
                    buf.put_slice(&data[..amt]);
                    *byte_index += amt;

                    debug_assert!(*byte_index <= PAGE_DATA_SIZE - mem::size_of::<PageIndex>());

                    if *byte_index == PAGE_DATA_SIZE - mem::size_of::<PageIndex>() {
                        let next_page_idx = PageIndex::new_maybe_invalid(view.as_ref().get_u32());
                        drop(view);
                        self.state = State::NeedNext { next_page_idx };
                    }

                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}
