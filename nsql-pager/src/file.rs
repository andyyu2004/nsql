use std::path::Path;
use std::sync::atomic::{self, AtomicU32};
use std::{io, mem};

use bytes::{Buf, BufMut};
use nsql_fs::File;
use nsql_serde::{Deserialize, DeserializeSync, Serialize, SerializeSync};
use nsql_util::static_assert;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{OnceCell, RwLock};

use crate::meta_page::{MetaPageReader, MetaPageWriter};
use crate::{Page, PageIndex, Pager, Result, RAW_PAGE_SIZE};

static_assert!(mem::size_of::<FileHeader>() < PAGE_SIZE);
static_assert!(mem::size_of::<PagerHeader>() < PAGE_SIZE);

pub const FILE_HEADER_START: u64 = 0;
pub const DB_HEADER_START: u64 = RAW_PAGE_SIZE as u64;
pub const PAGE_SIZE: usize = 4096;
pub const MAGIC: [u8; 4] = *b"NSQL";
const N_RESERVED_PAGES: u32 = 3;

pub const CURRENT_VERSION: u32 = 1;

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub struct FileHeader {
    magic: [u8; 4],
    version: u32,
}

impl SerializeSync for FileHeader {
    fn serialize_sync(&self, mut buf: &mut dyn BufMut) {
        buf.put_slice(&self.magic);
        buf.put_u32(self.version);
    }
}

impl DeserializeSync for FileHeader {
    fn deserialize_sync(buf: &mut dyn Buf) -> Self {
        let mut magic = [0; 4];
        buf.copy_to_slice(&mut magic);
        let version = buf.get_u32();
        Self { magic, version }
    }
}

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
struct PagerHeader {
    free_list_head: PageIndex,
    page_count: PageIndex,
}

impl SerializeSync for PagerHeader {
    fn serialize_sync(&self, buf: &mut dyn BufMut) {
        self.free_list_head.serialize_sync(buf);
        self.page_count.serialize_sync(buf);
    }
}

impl DeserializeSync for PagerHeader {
    fn deserialize_sync(mut buf: &mut dyn Buf) -> Self {
        let free_list_head = PageIndex::new(buf.get_u32());
        let page_count = PageIndex::new(buf.get_u32());
        Self { free_list_head, page_count }
    }
}

pub struct SingleFilePager {
    storage: File<RAW_PAGE_SIZE>,
    page_count: AtomicU32,
    free_list: OnceCell<RwLock<Vec<PageIndex>>>,
    free_list_head: PageIndex,
}

impl Pager for SingleFilePager {
    async fn alloc_page(&self) -> Result<PageIndex> {
        if let Some(idx) = self.free_list().await?.write().await.pop() {
            return Ok(idx);
        }

        let next_index = PageIndex::new(self.page_count.fetch_add(1, atomic::Ordering::SeqCst));
        self.write_page(Page::zeroed(next_index)).await?;
        self.assert_page_in_bounds(next_index);
        Ok(next_index)
    }

    async fn free_page(&self, idx: PageIndex) -> Result<()> {
        self.free_list().await?.write().await.push(idx);
        Ok(())
    }

    async fn read_page(&self, idx: PageIndex) -> Result<Page> {
        self.assert_page_in_bounds(idx);
        let offset = self.offset_for_page(idx);
        let bytes = self.storage.read_at(offset).await?;
        let page = Page::new(idx, bytes);

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

    async fn write_page(&self, mut page: Page) -> Result<()> {
        let idx = page.idx();
        self.assert_page_in_bounds(idx);
        let offset = self.offset_for_page(idx);
        page.update_checksum();

        let bytes = *page.bytes();
        // drop is important to avoid holding lock across await
        drop(page);
        self.storage.write_at(offset, bytes).await?;
        self.storage.sync().await?;
        Ok(())
    }
}

impl SingleFilePager {
    #[inline]
    pub async fn open(path: impl AsRef<Path>) -> Result<SingleFilePager> {
        let storage = File::open(path).await?;
        Self::check_file_header(&storage).await?;

        let db_header = Self::read_database_header(&storage).await?;

        Ok(Self::new(storage, db_header))
    }

    // Create a new database file at the given path.
    #[inline]
    pub async fn create(path: impl AsRef<Path>) -> Result<SingleFilePager> {
        let storage = File::create(path).await?;

        let mut buf = [0; PAGE_SIZE];
        let file_header = FileHeader { magic: MAGIC, version: CURRENT_VERSION };
        file_header.serialize_sync(&mut buf.as_mut());
        storage.write_at(FILE_HEADER_START, buf).await?;

        let db_header = PagerHeader {
            free_list_head: PageIndex::INVALID,
            page_count: PageIndex::new(N_RESERVED_PAGES),
        };
        storage.write_at(DB_HEADER_START, buf).await?;
        storage.sync().await?;

        Ok(Self::new(storage, db_header))
    }

    #[inline]
    fn new(storage: File<PAGE_SIZE>, db_header: PagerHeader) -> Self {
        Self {
            storage,
            page_count: AtomicU32::new(db_header.page_count.as_u32()),
            free_list_head: db_header.free_list_head,
            free_list: OnceCell::new(),
        }
    }

    async fn write_free_list(&self) -> Result<()> {
        let free_list = self.free_list().await?.read().await;
        if free_list.is_empty() {
            return Ok(());
        }

        let initial_block = self.alloc_page().await?;

        let mut writer = MetaPageWriter::new(self, initial_block);
        writer.write_u32(free_list.len() as u32).await?;
        for idx in free_list.iter() {
            idx.serialize(&mut writer).await?;
        }

        writer.flush().await?;

        Ok(())
    }

    async fn free_list(&self) -> Result<&RwLock<Vec<PageIndex>>> {
        self.free_list
            .get_or_try_init(|| async {
                if !self.free_list_head.is_valid() {
                    return Ok(RwLock::new(vec![]));
                }

                let mut reader = BufReader::new(MetaPageReader::new(self, self.free_list_head));
                let count = reader.read_u32().await?;
                let mut free_list = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    let idx = PageIndex::deserialize(&mut reader).await?;
                    free_list.push(idx);
                }

                Ok(RwLock::new(free_list))
            })
            .await
    }

    async fn read_database_header(storage: &File<PAGE_SIZE>) -> Result<PagerHeader> {
        let buf = storage.read_at(DB_HEADER_START).await?;
        let db_header = PagerHeader::deserialize_sync(&mut &buf[..]);
        Ok(db_header)
    }

    async fn check_file_header(storage: &File<PAGE_SIZE>) -> Result<()> {
        let buf = storage.read_at(FILE_HEADER_START).await?;
        let file_header = FileHeader::deserialize_sync(&mut &buf[..]);

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

    /// checks that the given page is within the bounds
    fn assert_page_in_bounds(&self, idx: PageIndex) {
        assert!(idx.is_valid(), "page index is invalid");
        assert!(
            idx.as_u32() >= N_RESERVED_PAGES,
            "page index `{idx} < {N_RESERVED_PAGES}` is reserved"
        );
        assert!(
            idx.as_u32() < self.page_count.load(atomic::Ordering::SeqCst),
            "page index out of bounds"
        );
    }
}

// private helpers
impl SingleFilePager {
    fn offset_for_page(&self, idx: PageIndex) -> u64 {
        // reserving 3 pages for the file header, and two database headers
        let offset = (idx.as_u32() as u64 + N_RESERVED_PAGES as u64) * PAGE_SIZE as u64;
        assert_eq!(offset % PAGE_SIZE as u64, 0);
        offset
    }
}

#[cfg(test)]
mod tests;
