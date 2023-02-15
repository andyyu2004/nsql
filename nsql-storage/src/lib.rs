#![deny(rust_2018_idioms)]
#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]

use std::io;
use std::path::Path;
use std::sync::Arc;

use bytes::{Buf, BufMut};
use tokio_uring::fs::{File, OpenOptions};

pub const HEADER_SIZE: usize = PAGE_SIZE;
pub const HEADER_START: u64 = 0;
pub const PAGE_SIZE: usize = 4096;
pub const MAGIC: [u8; 4] = *b"NSQL";

pub const CURRENT_VERSION: u32 = 1;

#[derive(Debug)]
pub struct Header {
    magic: [u8; 4],
    version: u32,
}

impl Header {
    fn serialize(&self, mut buf: &mut [u8]) {
        buf.put_slice(&self.magic);
        buf.put_u32(self.version);
    }

    fn deserialize(mut buf: &[u8]) -> io::Result<Self> {
        let mut magic = [0; 4];
        buf.copy_to_slice(&mut magic);
        if magic != MAGIC {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "this is not a valid nsql file (magic number mismatch)",
            ))?;
        }

        let version = buf.get_u32();
        if version != CURRENT_VERSION {
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "attempting to read nsql database file with version `{version}`, but can only read version `{CURRENT_VERSION}"
                ),
            ))?;
        }

        Ok(Self { magic: MAGIC, version })
    }
}

pub struct Storage {
    file: File,
}

pub type Result<T, E = std::io::Error> = std::result::Result<T, error_stack::Report<E>>;

// FIXME we could probably use fixed iouring buffers
impl Storage {
    fn new(file: File) -> Self {
        Self { file }
    }

    #[inline]
    pub async fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            // .custom_flags(libc::O_DIRECT) // FIXME
            .open(path)
            .await?;

        let mut buffer = vec![0; HEADER_SIZE];
        let header = Header { magic: MAGIC, version: CURRENT_VERSION };
        header.serialize(buffer.as_mut());
        let (res, _) = file.write_all_at(buffer, HEADER_START).await;
        res?;
        file.sync_all().await?;

        Ok(Self::new(file))
    }

    #[inline]
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            // .custom_flags(libc::O_DIRECT)
            .open(path)
            .await?;

        let (res, buf) = file.read_exact_at(vec![0; HEADER_SIZE], HEADER_START).await;
        res?;
        Header::deserialize(&buf)?;
        Ok(Self::new(file))
    }

    #[inline]
    pub async fn read_at<const N: usize>(&self, pos: u64) -> Result<Vec<u8>> {
        Self::assert_aligned::<N>(pos);
        let (res, buf) = self.file.read_exact_at(vec![0; N], pos).await;
        res?;
        Ok(buf)
    }

    #[inline]
    pub async fn write_at<const N: usize>(&self, pos: u64, data: &[u8; N]) -> Result<()> {
        Self::assert_aligned::<N>(pos);
        self.file.write_all_at(data.to_vec(), pos).await.0?;
        Ok(())
    }

    #[inline]
    pub async fn sync(&self) -> Result<()> {
        self.file.sync_all().await?;
        Ok(())
    }

    const fn assert_aligned<const N: usize>(pos: u64) {
        assert!(Self::is_aligned::<N>(pos), "position is not aligned to size");
    }

    const fn is_aligned<const N: usize>(pos: u64) -> bool {
        pos % N as u64 == 0
    }
}
