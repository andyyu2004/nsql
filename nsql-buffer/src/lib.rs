#![deny(rust_2018_idioms)]
#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]
#![feature(once_cell)]

use std::io;
use std::sync::Arc;

use lruk::{LruK, RefCounted};
use nsql_pager::{Page, PageIndex, Pager, PAGE_SIZE};

trait BufferPoolInterface {
    async fn load(&self, index: PageIndex) -> io::Result<BufferHandle>;
}

#[derive(Clone)]
pub struct BufferHandle {
    page: Arc<Page>,
}

impl RefCounted for BufferHandle {
    fn ref_count(&self) -> usize {
        self.page.ref_count()
    }
}

#[derive(Default)]
pub struct Timer;

impl lruk::Clock for Timer {
    type Time = coarsetime::Instant;
    type Duration = coarsetime::Duration;

    fn now(&self) -> Self::Time {
        coarsetime::Instant::now()
    }
}

pub struct BufferPool<P> {
    pager: P,
    cache: LruK<PageIndex, BufferHandle, Timer, 2>,
}

impl<P> BufferPool<P> {
    pub fn new(pager: P) -> Self {
        let max_memory_bytes = 128 * 1024 * 1024;
        let max_pages = max_memory_bytes / PAGE_SIZE;
        Self {
            pager,
            cache: LruK::new(
                max_pages,
                if cfg!(test) {
                    coarsetime::Duration::from_millis(100)
                } else {
                    coarsetime::Duration::from_secs(200)
                },
                if cfg!(test) {
                    coarsetime::Duration::from_millis(10)
                } else {
                    coarsetime::Duration::from_millis(50)
                },
            ),
        }
    }
}

impl<P: Pager> BufferPoolInterface for BufferPool<P> {
    async fn load(&self, index: PageIndex) -> io::Result<BufferHandle> {
        // TODO check cache first
        // self.cache.get();
        let page = Arc::new(self.pager.read_page(index).await?);
        Ok(BufferHandle { page })
    }
}
