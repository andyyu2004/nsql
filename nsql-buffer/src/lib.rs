#![deny(rust_2018_idioms)]
#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]

use std::io;

use nsql_pager::{Page, PageIndex, Pager};

trait BufferPoolInterface {
    async fn load(&self, index: PageIndex) -> io::Result<BufferHandle>;
    fn max_memory_bytes(&self) -> usize;
}

pub struct BufferHandle {
    page: Page,
}

pub struct BufferPool<P> {
    pager: P,
    max_memory_bytes: usize,
}

impl<P> BufferPool<P> {
    pub fn new(pager: P) -> Self {
        Self { pager, max_memory_bytes: 128 * 1024 * 1024 }
    }
}

impl<P: Pager> BufferPoolInterface for BufferPool<P> {
    fn max_memory_bytes(&self) -> usize {
        self.max_memory_bytes
    }

    async fn load(&self, index: PageIndex) -> io::Result<BufferHandle> {
        let page = self.pager.read_page(index).await?;
        Ok(BufferHandle { page })
    }
}

