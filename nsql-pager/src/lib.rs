#![deny(rust_2018_idioms)]
#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]

mod file;
mod mem;
mod meta_page;
mod page;

pub use nsql_storage::Result;

pub use self::file::SingleFilePager;
pub use self::mem::InMemoryPager;
pub use self::page::{Page, PageIndex};

/// The size of a page in bytes minus the size of the page header.
pub const PAGE_SIZE: usize = RAW_PAGE_SIZE - CHECKSUM_LENGTH;
const RAW_PAGE_SIZE: usize = 4096;
const CHECKSUM_LENGTH: usize = std::mem::size_of::<u64>();

pub trait Pager: 'static {
    /// Allocate a new unused [`crate::PageIndex`]
    async fn alloc_page(&self) -> Result<PageIndex>;
    /// Mark the given [`crate::PageIndex`] as unused
    async fn free_page(&self, idx: PageIndex) -> Result<()>;

    async fn read_page(&self, idx: PageIndex) -> Result<Page>;
    /// Write the given page to the given index and fsync
    async fn write_page(&self, idx: PageIndex, page: Page) -> Result<()>;
}
