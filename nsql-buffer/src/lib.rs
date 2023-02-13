#![deny(rust_2018_idioms)]
#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]
#![feature(once_cell)]

use std::sync::Arc;

use lruk::{LruK, RefCounted};
use nsql_pager::{Page, PageIndex, Pager, Result, PAGE_SIZE};
use parking_lot::RwLock;

trait BufferPoolInterface {
    async fn load(&self, index: PageIndex) -> Result<BufferHandle>;
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

pub struct BufferPool<P> {
    pager: P,
    cache: RwLock<LruK<PageIndex, BufferHandle, Clock>>,
}

impl<P> BufferPool<P> {
    pub fn new(pager: P) -> Self {
        let max_memory_bytes = 128 * 1024 * 1024;
        let max_pages = max_memory_bytes / PAGE_SIZE;
        Self {
            pager,
            cache: RwLock::new(LruK::new(
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
            )),
        }
    }
}

impl<P: Pager> BufferPoolInterface for BufferPool<P> {
    async fn load(&self, index: PageIndex) -> Result<BufferHandle> {
        if let Some(handle) = self.cache.read().get(index) {
            return Ok(handle);
        }

        let page = Arc::new(self.pager.read_page(index).await?);
        let handle = BufferHandle { page };
        Ok(self.cache.write().insert(index, handle))
    }
}

#[derive(Default)]
pub struct Clock;

impl lruk::Clock for Clock {
    type Time = coarsetime::Instant;
    type Duration = coarsetime::Duration;

    fn now(&self) -> Self::Time {
        coarsetime::Instant::now()
    }
}
