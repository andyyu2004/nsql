use std::io;
use std::path::Path;
use std::sync::atomic::{self, AtomicUsize};

use bytes::{Buf, BufMut};
use nsql_storage::Storage;

use crate::{Page, PageIndex, Pager, Result};

const CHECKSUM_LENGTH: usize = std::mem::size_of::<u64>();

pub const HEADER_SIZE: usize = std::mem::size_of::<Header>();
pub const FILE_HEADER_START: u64 = 0;
pub const PAGE_SIZE: usize = 4096;
pub const MAGIC: [u8; 4] = *b"NSQL";

pub const CURRENT_VERSION: u32 = 1;

#[derive(Debug)]
pub struct FileHeader {
    magic: [u8; 4],
    version: u32,
}

impl FileHeader {
    fn serialize(&self, mut buf: &mut [u8]) {
        buf.put_slice(&self.magic);
        buf.put_u32(self.version);
    }

    fn deserialize(mut buf: &[u8]) -> Self {
        let mut magic = [0; 4];
        buf.copy_to_slice(&mut magic);
        let version = buf.get_u32();
        Self { magic, version }
    }
}

#[derive(Debug)]
struct Header {
    free_list_head: PageIndex,
    page_count: PageIndex,
}

pub struct SingleFilePager {
    storage: Storage<PAGE_SIZE>,
    max_page_index: AtomicUsize,
}

impl Pager for SingleFilePager {
    async fn alloc_page(&self) -> Result<PageIndex> {
        Ok(PageIndex::new(self.max_page_index.fetch_add(1, atomic::Ordering::SeqCst)))
    }

    async fn read_page(&self, idx: PageIndex) -> Result<Page> {
        let offset = self.offset_for_page(idx);
        let data = self.storage.read_at(offset as u64).await?;
        let expected_checksum = u64::from_be_bytes(data[..CHECKSUM_LENGTH].try_into().unwrap());
        let computed_checksum = checksum(&data[CHECKSUM_LENGTH..]);
        if expected_checksum != computed_checksum {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "checksum mismatch on page {idx}: expected {expected_checksum}, got {computed_checksum}",
                ),
            ))?;
        }

        Ok(Page::new(
            data.to_vec().into_boxed_slice().try_into().expect("data was incorrect length"),
        ))
    }

    async fn write_page(&self, idx: PageIndex, mut page: Page) -> Result<()> {
        let offset = self.offset_for_page(idx);
        let checksum = checksum(&page.bytes()[CHECKSUM_LENGTH..]);
        let checksum_slice = &mut page.bytes_mut()[..CHECKSUM_LENGTH];
        assert_eq!(
            u64::from_be_bytes(checksum_slice.try_into().unwrap()),
            0,
            "checksum slice is non-zero"
        );
        checksum_slice.copy_from_slice(&checksum.to_be_bytes());

        self.storage.write_at(offset as u64, page.bytes()).await?;
        self.storage.sync().await?;
        Ok(())
    }
}

impl SingleFilePager {
    #[inline]
    pub fn new(storage: Storage<PAGE_SIZE>) -> Self {
        Self { storage, max_page_index: todo!() }
    }

    #[inline]
    pub async fn open(path: impl AsRef<Path>) -> Result<SingleFilePager> {
        let storage = Storage::open(path).await?;

        let buf = storage.read_at(FILE_HEADER_START).await?;
        let header = FileHeader::deserialize(&buf);

        if header.magic != MAGIC {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "this is not a valid nsql file (magic number mismatch)",
            ))?;
        }

        if header.version != CURRENT_VERSION {
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "attempting to read nsql database file with version `{}`, but can only read version `{CURRENT_VERSION}",
                    header.version
                ),
            ))?;
        }

        Ok(Self::new(storage))
    }

    #[inline]
    pub async fn create(path: impl AsRef<Path>) -> Result<SingleFilePager> {
        let mut buf = [0; PAGE_SIZE];
        let header = FileHeader { magic: MAGIC, version: CURRENT_VERSION };
        header.serialize(buf.as_mut());

        let storage = Storage::open(path).await?;
        storage.write_at(FILE_HEADER_START, &buf).await?;
        storage.sync().await?;

        Ok(Self::new(storage))
    }
}

// private helpers
impl SingleFilePager {
    fn offset_for_page(&self, idx: PageIndex) -> usize {
        (idx.as_usize() * PAGE_SIZE) + HEADER_SIZE
    }
}

fn checksum(data: &[u8]) -> u64 {
    crc::Crc::<u64>::new(&crc::CRC_64_WE).checksum(data)
}
