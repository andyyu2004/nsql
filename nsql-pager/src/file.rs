use std::path::Path;
use std::sync::atomic::{self, AtomicU32, AtomicU64};
use std::{io, mem};

use bytes::{Buf, BufMut};
use nsql_storage::Storage;

use crate::{Page, PageIndex, Pager, Result, CHECKSUM_LENGTH, RAW_PAGE_SIZE};

const _: () = [(); 1][(mem::size_of::<DbHeader>() < PAGE_SIZE) as usize ^ 1];
const _: () = [(); 1][(mem::size_of::<FileHeader>() < PAGE_SIZE) as usize ^ 1];

pub const FILE_HEADER_START: u64 = 0;
pub const DB_HEADER_START: u64 = RAW_PAGE_SIZE as u64;
pub const PAGE_SIZE: usize = 4096;
pub const MAGIC: [u8; 4] = *b"NSQL";

pub const CURRENT_VERSION: u32 = 1;

trait Serialize {
    fn serialize(&self, buf: &mut [u8]);
}

trait Deserialize {
    fn deserialize(buf: &[u8]) -> Self;
}

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub struct FileHeader {
    magic: [u8; 4],
    version: u32,
}

impl Serialize for FileHeader {
    fn serialize(&self, mut buf: &mut [u8]) {
        buf.put_slice(&self.magic);
        buf.put_u32(self.version);
    }
}

impl Deserialize for FileHeader {
    fn deserialize(mut buf: &[u8]) -> Self {
        let mut magic = [0; 4];
        buf.copy_to_slice(&mut magic);
        let version = buf.get_u32();
        Self { magic, version }
    }
}

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
struct DbHeader {
    free_list_head: PageIndex,
    page_count: PageIndex,
}

impl Serialize for DbHeader {
    fn serialize(&self, mut buf: &mut [u8]) {
        buf.put_u32(self.free_list_head.as_u32());
        buf.put_u32(self.page_count.as_u32());
    }
}

impl Deserialize for DbHeader {
    fn deserialize(mut buf: &[u8]) -> Self {
        let free_list_head = PageIndex::new(buf.get_u32());
        let page_count = PageIndex::new(buf.get_u32());
        Self { free_list_head, page_count }
    }
}

pub struct SingleFilePager {
    storage: Storage<RAW_PAGE_SIZE>,
    max_page_index: AtomicU32,
    // free_list: AtomicU64,
}

impl Pager for SingleFilePager {
    async fn alloc_page(&self) -> Result<PageIndex> {
        let next_index = PageIndex::new(self.max_page_index.fetch_add(1, atomic::Ordering::SeqCst));
        self.write_page(next_index, Page::zeroed()).await?;
        Ok(next_index)
    }

    async fn free_page(&self, idx: PageIndex) -> Result<()> {
        self.assert_page_in_bounds(idx);
        Ok(())
    }

    async fn read_page(&self, idx: PageIndex) -> Result<Page> {
        self.assert_page_in_bounds(idx);
        let offset = self.offset_for_page(idx);
        let page = Page::new(self.storage.read_at(offset).await?);

        let expected_checksum = page.expected_checksum();
        let computed_checksum = page.compute_checksum();

        if expected_checksum != computed_checksum {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "checksum mismatch on page {idx}: expected {expected_checksum}, got {computed_checksum}",
                ),
            ))?;
        }

        Ok(page)
    }

    async fn write_page(&self, idx: PageIndex, mut page: Page) -> Result<()> {
        self.assert_page_in_bounds(idx);
        let offset = self.offset_for_page(idx);
        page.update_checksum();

        self.storage.write_at(offset, page.bytes()).await?;
        self.storage.sync().await?;
        Ok(())
    }
}

impl SingleFilePager {
    #[inline]
    pub async fn open(path: impl AsRef<Path>) -> Result<SingleFilePager> {
        let storage = Storage::open(path).await?;
        Self::check_file_header(&storage).await?;

        let db_header = Self::read_database_header(&storage).await?;

        Ok(Self::new(storage, db_header))
    }

    // Create a new database file at the given path.
    #[inline]
    pub async fn create(path: impl AsRef<Path>) -> Result<SingleFilePager> {
        let storage = Storage::create(path).await?;

        let mut buf = [0; PAGE_SIZE];
        let file_header = FileHeader { magic: MAGIC, version: CURRENT_VERSION };
        file_header.serialize(buf.as_mut());
        storage.write_at(FILE_HEADER_START, &buf).await?;

        let db_header =
            DbHeader { free_list_head: PageIndex::INVALID, page_count: PageIndex::new(0) };
        storage.write_at(DB_HEADER_START, &buf).await?;
        storage.sync().await?;

        Ok(Self::new(storage, db_header))
    }

    #[inline]
    fn new(storage: Storage<PAGE_SIZE>, db_header: DbHeader) -> Self {
        Self {
            storage,
            max_page_index: AtomicU32::new(db_header.page_count.as_u32()),
            // free_list: db_header.free_list_head,
        }
    }

    async fn load_free_list(&self) -> Result<Vec<PageIndex>> {
        todo!()
    }

    async fn read_database_header(storage: &Storage<PAGE_SIZE>) -> Result<DbHeader> {
        let buf = storage.read_at(DB_HEADER_START).await?;
        let db_header = DbHeader::deserialize(&buf);
        Ok(db_header)
    }

    async fn check_file_header(storage: &Storage<PAGE_SIZE>) -> Result<()> {
        let buf = storage.read_at(FILE_HEADER_START).await?;
        let file_header = FileHeader::deserialize(&buf);

        if file_header.magic != MAGIC {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "this is not a valid nsql file (magic number mismatch)",
            ))?;
        }

        if file_header.version != CURRENT_VERSION {
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "attempting to read nsql database file with version `{}`, but can only read version `{CURRENT_VERSION}",
                    file_header.version
                ),
            ))?;
        }

        Ok(())
    }

    fn assert_page_in_bounds(&self, idx: PageIndex) {
        assert!(
            idx.as_u32() < self.max_page_index.load(atomic::Ordering::SeqCst),
            "page index out of bounds"
        );
    }
}

// private helpers
impl SingleFilePager {
    fn offset_for_page(&self, idx: PageIndex) -> u64 {
        // reserving 3 pages for the file header, and two database headers
        let offset = (idx.as_u32() as u64 + 3) * PAGE_SIZE as u64;
        assert_eq!(offset % PAGE_SIZE as u64, 0);
        offset
    }
}

#[cfg(test)]
mod tests;
