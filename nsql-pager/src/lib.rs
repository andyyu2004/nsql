#![deny(rust_2018_idioms)]
#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]
#![feature(const_option)]

mod file;
mod mem;
mod meta_page;
mod page;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub use nsql_fs::{Error, Result};

pub use self::file::SingleFilePager;
pub use self::mem::InMemoryPager;
pub use self::meta_page::{MetaPageReader, MetaPageWriter};
pub use self::page::{
    ArchivedPageIndex, Page, PageIndex, PageOffset, PageReadGuard, PageWriteGuard,
};

/// The size of a page in bytes minus the size of the page header.
pub const PAGE_DATA_SIZE: usize = PAGE_SIZE - PAGE_META_LENGTH;

// #[cfg(not(test))]
pub const PAGE_SIZE: usize = 4096;

// #[cfg(test)]
// pub const PAGE_SIZE: usize = 256;

// currently just the checksum
const PAGE_META_LENGTH: usize = std::mem::size_of::<u64>();

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[async_trait::async_trait]
pub trait Pager: Send + Sync + 'static {
    /// Allocate a new unused [`crate::PageIndex`]
    async fn alloc_page(&self) -> Result<PageIndex>;
    /// Mark the given [`crate::PageIndex`] as unused
    async fn free_page(&self, idx: PageIndex) -> Result<()>;

    async fn read_page(&self, idx: PageIndex) -> Result<Page>;
    /// Write the given page to the given index and fsync
    async fn write_page(&self, page: Page) -> Result<()>;
}

#[async_trait::async_trait]
impl<P: Pager> Pager for Arc<P> {
    async fn alloc_page(&self) -> Result<PageIndex> {
        (**self).alloc_page().await
    }

    async fn free_page(&self, idx: PageIndex) -> Result<()> {
        (**self).free_page(idx).await
    }

    async fn read_page(&self, idx: PageIndex) -> Result<Page> {
        (**self).read_page(idx).await
    }

    async fn write_page(&self, page: Page) -> Result<()> {
        (**self).write_page(page).await
    }
}
