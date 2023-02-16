use std::future::{ready, Future};
use std::io;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use pin_utils::pin_mut;
use tokio::io::{AsyncRead, ReadBuf};

use crate::{PageIndex, Pager};

pub(crate) struct MetaPageReader<'a, P> {
    pager: &'a P,
    page_idx: PageIndex,
}

impl<'a, P> MetaPageReader<'a, P> {
    pub(crate) fn new(pager: &'a P, page_idx: PageIndex) -> Self {
        assert!(page_idx.is_valid());
        Self { pager, page_idx }
    }
}

impl<'a, P: Pager> AsyncRead for MetaPageReader<'a, P> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let fut = self.pager.read_page(self.page_idx);
        pin_mut!(fut);
        ready!(fut.poll(cx))?;
        todo!()
    }
}
