#![deny(rust_2018_idioms)]
#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]
#![feature(once_cell)]

use std::sync::Arc;

use coarsetime::Duration;
use lruk::{LruK, RefCounted};
pub use nsql_pager::Result;
use nsql_pager::{Page, PageIndex, Pager, PAGE_DATA_SIZE};
use parking_lot::RwLock;

// this trait is here just to have clear view of the interface of the buffer pool
trait BufferPoolInterface {
    // Create a new buffer pool with the given pager implementation.
    // Returns the buffer pool and a future that must be polled to completion.
    async fn alloc(&self) -> Result<BufferHandle>;
    async fn load(&self, index: PageIndex) -> Result<BufferHandle>;
}

#[derive(Clone)]
pub struct BufferHandle {
    page: Arc<Page>,
}

impl BufferHandle {
    pub fn new(page: Page) -> Self {
        Self { page: Arc::new(page) }
    }
}

impl RefCounted for BufferHandle {
    fn ref_count(&self) -> usize {
        self.page.ref_count()
    }
}

#[derive(Clone)]
pub struct BufferPool {
    inner: Arc<Inner>,
}

impl BufferPool {
    #[inline]
    pub fn pager(&self) -> Arc<dyn Pager> {
        Arc::clone(&self.inner.pager)
    }
}

struct Inner {
    pager: Arc<dyn Pager>,
    cache: RwLock<LruK<PageIndex, BufferHandle, Clock>>,
}

impl BufferPool {
    pub fn new(pager: Arc<dyn Pager>) -> Self {
        let max_memory_bytes = if cfg!(test) { 1024 * 1024 } else { 128 * 1024 * 1024 };
        let max_pages = max_memory_bytes / PAGE_DATA_SIZE;

        let inner = Arc::new(Inner {
            pager,
            cache: RwLock::new(LruK::new(
                max_pages,
                if cfg!(test) { Duration::from_millis(100) } else { Duration::from_secs(200) },
                if cfg!(test) { Duration::from_millis(10) } else { Duration::from_millis(50) },
            )),
        });

        Self { inner }
    }
}

impl BufferPoolInterface for BufferPool {
    #[inline]
    async fn alloc(&self) -> Result<BufferHandle> {
        let idx = self.inner.pager.alloc_page().await?;
        self.load(idx).await
    }

    async fn load(&self, index: PageIndex) -> Result<BufferHandle> {
        let inner = &self.inner;
        if let Some(handle) = inner.cache.read().get(index) {
            return Ok(handle);
        }

        let page = inner.pager.read_page(index).await?;
        let result = inner.cache.write().insert(index, BufferHandle::new(page));
        let handle = match result {
            lruk::InsertionResult::InsertedWithEviction { value, evicted } => {
                let page = Arc::try_unwrap(evicted.page)
                    .expect("evicted page was not the final reference");
                // FIXME check whether page is dirty
                inner.pager.write_page(page).await?;
                value
            }
            lruk::InsertionResult::Inserted(value)
            | lruk::InsertionResult::AlreadyExists(value) => value,
        };

        Ok(handle)
    }
}

#[derive(Default)]
struct Clock;

impl lruk::Clock for Clock {
    type Time = coarsetime::Instant;
    type Duration = coarsetime::Duration;

    fn now(&self) -> Self::Time {
        coarsetime::Instant::now()
    }
}
