use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::{cmp, io};

use bytes::BufMut;
use tokio::io::AsyncWrite;

use super::PAGE_IDX_SIZE;
use crate::{BoxFuture, Page, PageIndex, Pager, Result, PAGE_DATA_SIZE};

/// You must write something with [`crate::meta_page::MetaPageWriter`] before it is valid to be read from.
/// Just constructing this struct does not do anything.
pub struct MetaPageWriter<'a, P> {
    pager: &'a P,
    flushed: bool,
    state: State<'a>,
}

impl<'a, P: Pager> MetaPageWriter<'a, P> {
    /// Create a new [`crate::meta_page::MetaPageWriter`] that will write to the given page index.
    /// You must not use this page index again as the pager may free it in the case where nothing is written.
    // note: we have to careful if we're writing out the free list, because we can't reuse the free list maybe?
    pub fn new(pager: &'a P, initial_page_idx: PageIndex) -> Self {
        assert!(initial_page_idx.is_valid(), "passed in invalid initial page index");
        Self {
            pager,
            state: State::PollNext { read_page_fut: Box::pin(pager.read_page(initial_page_idx)) },
            flushed: false,
        }
    }
}

enum State<'a> {
    PollNext {
        read_page_fut: BoxFuture<'a, Result<Page>>,
    },
    /// We're writing to the page at `page_idx` at offset `byte_index`.
    Write {
        page: Option<Page>,
        byte_index: usize,
    },
    /// we filled up a page, so we're allocating the next one so that
    /// we can write the next index to the current page
    PollAlloc {
        page: Option<Page>,
        alloc_fut: BoxFuture<'a, Result<PageIndex>>,
    },
    /// write to the current page, keeping track of the next allocated page
    PollWrite {
        next_page_idx: PageIndex,
        write_page_fut: BoxFuture<'a, Result<()>>,
    },
}

impl<'a, P: Pager> AsyncWrite for MetaPageWriter<'a, P> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, io::Error>> {
        loop {
            let pager = self.pager;
            match &mut self.state {
                State::PollNext { read_page_fut } => {
                    let page = ready!(read_page_fut.as_mut().poll(cx))?;
                    // initialize the next page index to invalid so the reader knows when it's finished
                    (&mut page.data_mut()[..PAGE_IDX_SIZE]).put_u32(PageIndex::INVALID.as_u32());
                    self.state = State::Write { page: Some(page), byte_index: PAGE_IDX_SIZE };
                }
                State::Write { page, byte_index } => {
                    let amt = cmp::min(buf.len(), PAGE_DATA_SIZE - *byte_index);
                    page.as_ref().unwrap().data_mut()[*byte_index..*byte_index + amt]
                        .copy_from_slice(&buf[..amt]);
                    debug_assert_eq!(
                        page.as_ref().unwrap().data()[*byte_index..*byte_index + amt],
                        buf[..amt]
                    );
                    *byte_index += amt;

                    if *byte_index >= PAGE_DATA_SIZE {
                        assert_eq!(*byte_index, PAGE_DATA_SIZE);
                        self.state = State::PollAlloc {
                            page: Some(page.take().unwrap()),
                            alloc_fut: Box::pin(pager.alloc_page()),
                        };
                    }
                    return Poll::Ready(Ok(amt));
                }
                State::PollAlloc { page, alloc_fut } => {
                    let next_page_idx = ready!(alloc_fut.as_mut().poll(cx))?;

                    let page = page.take().unwrap();

                    (&mut page.data_mut()[..PAGE_IDX_SIZE]).put_u32(next_page_idx.as_u32());
                    debug_assert_eq!(
                        page.data()[..PAGE_IDX_SIZE],
                        next_page_idx.as_u32().to_be_bytes()
                    );

                    self.state = State::PollWrite {
                        next_page_idx,
                        write_page_fut: Box::pin(pager.write_page(page)),
                    };
                }
                State::PollWrite { next_page_idx, write_page_fut } => {
                    assert!(next_page_idx.is_valid());
                    ready!(write_page_fut.as_mut().poll(cx))?;
                    self.state = State::PollNext {
                        read_page_fut: Box::pin(pager.read_page(*next_page_idx)),
                    };
                }
            }
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), io::Error>> {
        assert!(!self.flushed, "can only flush this writer once once all data has been written");
        // FIXME edge case where new page was allocated but the NEXT_PAGE_IDX has not been written to it
        let pager = self.pager;
        loop {
            match &mut self.state {
                State::PollNext { read_page_fut } => {
                    let page = ready!(read_page_fut.as_mut().poll(cx))?;
                    (&mut page.data_mut()[..PAGE_IDX_SIZE]).put_u32(PageIndex::INVALID.as_u32());
                    self.state = State::PollWrite {
                        next_page_idx: PageIndex::INVALID,
                        write_page_fut: Box::pin(pager.write_page(page)),
                    };
                }
                State::Write { page, .. } => {
                    self.state = State::PollWrite {
                        next_page_idx: PageIndex::INVALID,
                        write_page_fut: Box::pin(pager.write_page(page.take().unwrap())),
                    }
                }
                // if we're in the middle of allocating a page, we don't need it anymore so we do nothing
                State::PollAlloc { .. } => break,
                // finish writing out the page we're currently writing to
                State::PollWrite { next_page_idx: _, write_page_fut } => {
                    break ready!(write_page_fut.as_mut().poll(cx))?;
                }
            }
        }

        self.flushed = true;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl<'a, P> Drop for MetaPageWriter<'a, P> {
    fn drop(&mut self) {
        assert!(self.flushed, "must flush this writer before dropping it");
    }
}
