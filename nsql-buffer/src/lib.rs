#![deny(rust_2018_idioms)]
#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]
#![feature(once_cell)]

mod cache;

use std::io;

use nsql_pager::{Page, PageIndex, Pager, PAGE_SIZE};

use self::cache::Cache;

trait BufferPoolInterface {
    async fn load(&self, index: PageIndex) -> io::Result<BufferHandle>;
    fn max_memory_bytes(&self) -> usize;
}

#[derive(Clone)]
pub struct BufferHandle {
    page: Page,
}

pub struct BufferPool<P> {
    pager: P,
    cache: Cache<PageIndex, BufferHandle>,
    max_memory_bytes: usize,
    max_pages: usize,
}

impl<P> BufferPool<P> {
    pub fn new(pager: P) -> Self {
        let max_memory_bytes = 128 * 1024 * 1024;
        let max_pages = max_memory_bytes / PAGE_SIZE;
        Self { pager, cache: Cache::new(max_pages), max_memory_bytes, max_pages }
    }
}

impl<P: Pager> BufferPoolInterface for BufferPool<P> {
    fn max_memory_bytes(&self) -> usize {
        self.max_memory_bytes
    }

    async fn load(&self, index: PageIndex) -> io::Result<BufferHandle> {
        // TODO check cache first
        let page = self.pager.read_page(index).await?;
        Ok(BufferHandle { page })
    }
}
