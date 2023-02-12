#![deny(rust_2018_idioms)]
#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]

use std::path::Path;
use std::sync::RwLock;
use std::{fmt, io};

use nsql_storage::Storage;
pub use nsql_storage::{HEADER_SIZE, PAGE_SIZE};

pub trait Pager {
    // fn alloc_page(&self) -> io::Result<PageIndex>;
    async fn read_page(&self, idx: PageIndex) -> io::Result<Page>;
    async fn write_page(&self, idx: PageIndex, page: Page) -> io::Result<()>;
}

pub struct InMemoryPager {
    pages: RwLock<Vec<Page>>,
}

impl Pager for InMemoryPager {
    async fn read_page(&self, idx: PageIndex) -> io::Result<Page> {
        Ok(self.pages.read().unwrap()[idx.0].clone())
    }

    async fn write_page(&self, idx: PageIndex, page: Page) -> io::Result<()> {
        self.pages.write().unwrap()[idx.0] = page;
        Ok(())
    }
}

pub struct SingleFilePager {
    storage: Storage,
}

const CHECKSUM_LENGTH: usize = std::mem::size_of::<u64>();

impl Pager for SingleFilePager {
    async fn read_page(&self, idx: PageIndex) -> io::Result<Page> {
        let offset = self.offset_for_page(idx);
        let data = self.storage.read_at(offset as u64, PAGE_SIZE).await?;
        let expected_checksum = u64::from_be_bytes(data[..CHECKSUM_LENGTH].try_into().unwrap());
        let computed_checksum = checksum(&data[CHECKSUM_LENGTH..]);
        if expected_checksum != computed_checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "checksum mismatch on page {idx}: expected {expected_checksum}, got {computed_checksum}",
                ),
            ));
        }

        Ok(Page::new(
            data.to_vec().into_boxed_slice().try_into().expect("data was incorrect length"),
        ))
    }

    async fn write_page(&self, idx: PageIndex, mut page: Page) -> io::Result<()> {
        let offset = self.offset_for_page(idx);
        let checksum = checksum(&page.bytes[CHECKSUM_LENGTH..]);
        let checksum_slice = &mut page.bytes[..CHECKSUM_LENGTH];
        assert_eq!(
            u64::from_be_bytes(checksum_slice.try_into().unwrap()),
            0,
            "checksum slice is non-zero"
        );
        checksum_slice.copy_from_slice(&checksum.to_be_bytes());

        self.storage.write_at(offset as u64, &page.bytes).await?;
        Ok(())
    }
}

impl SingleFilePager {
    #[inline]
    pub async fn open(path: impl AsRef<Path>) -> io::Result<SingleFilePager> {
        Ok(SingleFilePager { storage: Storage::open(path).await? })
    }
}

// private helpers
impl SingleFilePager {
    fn offset_for_page(&self, idx: PageIndex) -> usize {
        (idx.0 * PAGE_SIZE) + HEADER_SIZE
    }
}

#[derive(Clone)]
pub struct Page {
    bytes: Box<[u8; PAGE_SIZE]>,
}

impl Page {
    pub fn new(bytes: Box<[u8; PAGE_SIZE]>) -> Self {
        Self { bytes }
    }
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct PageIndex(usize);

impl fmt::Display for PageIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

fn checksum(data: &[u8]) -> u64 {
    crc::Crc::<u64>::new(&crc::CRC_64_WE).checksum(data)
}
