#![deny(rust_2018_idioms)]
#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]
#![feature(once_cell)]

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;
use coarsetime::Duration;
use lruk::{LruK, RefCounted};
pub use nsql_pager::Result;
use nsql_pager::{Page, PageIndex, Pager, PAGE_DATA_SIZE};
use parking_lot::RwLock;

#[async_trait]
pub trait Pool: Send + Sync + 'static {
    fn pager(&self) -> &Arc<dyn Pager>;
    async fn alloc(&self) -> Result<BufferHandle>;
    async fn load(&self, idx: PageIndex) -> Result<BufferHandle>;
}

#[derive(Clone)]
pub struct BufferHandle {
    page: Page,
}

impl Deref for BufferHandle {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

impl BufferHandle {
    #[inline]
    pub fn page(&self) -> &Page {
        &self.page
    }

    #[inline]
    pub fn page_idx(&self) -> PageIndex {
        self.page.page_idx()
    }

    fn new(page: Page) -> Self {
        Self { page }
    }
}

impl RefCounted for BufferHandle {
    fn ref_count(&self) -> usize {
        self.page.arced_data().ref_count()
    }
}

pub struct BufferPool {
    pager: Arc<dyn Pager>,
    cache: RwLock<LruK<PageIndex, BufferHandle, Clock>>,
}

impl BufferPool {
    pub fn new(pager: Arc<dyn Pager>) -> Self {
        let max_memory_bytes = if cfg!(test) { 1024 * 1024 } else { 128 * 1024 * 1024 };
        let max_pages = max_memory_bytes / PAGE_DATA_SIZE;

        Self {
            pager,
            cache: RwLock::new(LruK::new(
                max_pages,
                if cfg!(test) { Duration::from_millis(100) } else { Duration::from_secs(200) },
                if cfg!(test) { Duration::from_millis(10) } else { Duration::from_millis(50) },
            )),
        }
    }
}

#[async_trait]
impl Pool for BufferPool {
    #[inline]
    fn pager(&self) -> &Arc<dyn Pager> {
        &self.pager
    }

    #[inline]
    async fn alloc(&self) -> Result<BufferHandle> {
        let idx = self.pager.alloc_page().await?;
        self.load(idx).await
    }

    async fn load(&self, index: PageIndex) -> Result<BufferHandle> {
        if let Some(handle) = self.cache.read().get(index) {
            return Ok(handle);
        }

        let page = self.pager.read_page(index).await?;
        let result = self.cache.write().insert(index, BufferHandle::new(page));
        let handle = match result {
            lruk::InsertionResult::InsertedWithEviction { value, evicted } => {
                assert_eq!(
                    evicted.page.arced_data().ref_count(),
                    1,
                    "evicted page was not the final reference"
                );
                // FIXME check whether page is dirty
                self.pager.write_page(evicted.page).await?;
                value
            }
            lruk::InsertionResult::Inserted(value)
            | lruk::InsertionResult::AlreadyExists(value) => value,
        };

        Ok(handle)
    }
}

/// A fast implementation of a buffer pool that essentially just wraps the underlying pager.
/// Does not deal with eviction.
/// Useful for tests only!
pub struct FastUnboundedBufferPool {
    pager: Arc<dyn Pager>,
    cache: RwLock<HashMap<PageIndex, BufferHandle>>,
}

impl FastUnboundedBufferPool {
    pub fn new(pager: Arc<dyn Pager>) -> Self {
        Self { pager, cache: Default::default() }
    }
}

#[async_trait]
impl Pool for FastUnboundedBufferPool {
    fn pager(&self) -> &Arc<dyn Pager> {
        &self.pager
    }

    async fn alloc(&self) -> Result<BufferHandle> {
        let idx = self.pager.alloc_page().await?;
        self.load(idx).await
    }

    async fn load(&self, idx: PageIndex) -> Result<BufferHandle> {
        if let Some(handle) = self.cache.read().get(&idx) {
            return Ok(handle.clone());
        }

        let page = self.pager.read_page(idx).await?;
        let handle = BufferHandle::new(page);
        assert!(self.cache.write().insert(idx, handle.clone()).is_none());
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
