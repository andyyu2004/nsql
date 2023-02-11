#![deny(rust_2018_idioms)]
#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]

use std::path::Path;
use std::sync::Arc;
use std::{fmt, io};

use nsql_storage::{Buffer, DmaFileStorage, Storage, HEADER_SIZE, PAGE_SIZE};

pub trait Pager {
    // fn alloc_page(&self) -> io::Result<PageIndex>;
    async fn read_page(&self, idx: PageIndex) -> io::Result<Page>;
}

pub struct InMemoryPager {
    pages: Vec<Page>,
}

impl Pager for InMemoryPager {
    async fn read_page(&self, idx: PageIndex) -> io::Result<Page> {
        Ok(self.pages[idx.0].clone())
    }
}

pub struct SingleFilePager<S> {
    storage: S,
}

impl<S> Pager for SingleFilePager<S>
where
    S: Storage,
{
    async fn read_page(&self, idx: PageIndex) -> io::Result<Page> {
        let offset = self.offset_for_page(idx);
        let data = self.storage.read_at(offset as u64, PAGE_SIZE).await?;
        let expected_checksum = u64::from_be_bytes(data[0..4].try_into().unwrap());
        let computed_checksum = checksum(&data[4..]);
        if expected_checksum != computed_checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "checksum mismatch on page {idx}: expected {expected_checksum}, got {computed_checksum}",
                ),
            ));
        }

        Ok(Page::new(Arc::new(data)))
    }
}

impl SingleFilePager<DmaFileStorage> {
    #[inline]
    pub async fn open(path: impl AsRef<Path>) -> io::Result<SingleFilePager<DmaFileStorage>> {
        Ok(SingleFilePager { storage: DmaFileStorage::open(path).await? })
    }
}

// private helpers
impl<S> SingleFilePager<S> {
    fn offset_for_page(&self, idx: PageIndex) -> usize {
        (idx.0 * PAGE_SIZE) + HEADER_SIZE
    }
}

#[derive(Clone)]
pub struct Page {
    data: Arc<dyn Buffer>,
}

impl Page {
    pub fn new(data: Arc<dyn Buffer>) -> Self {
        Self { data }
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
