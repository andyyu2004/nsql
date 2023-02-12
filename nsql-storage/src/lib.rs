#![deny(rust_2018_idioms)]
#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]

use std::io;
use std::path::Path;
use std::sync::Arc;

use bytes::{Buf, BufMut};
use tokio_uring::buf::BoundedBuf;
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
        buf.put_slice(&MAGIC);
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

#[derive(Clone)]
pub struct Storage {
    file: Arc<File>,
}

// FIXME we could probably use fixed iouring buffers
impl Storage {
    #[inline]
    pub async fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = Arc::new(OpenOptions::new().create_new(true).open(path).await?);

        let mut buffer = vec![0; HEADER_SIZE];
        let header = Header { magic: MAGIC, version: CURRENT_VERSION };
        header.serialize(buffer.as_mut());
        let (res, _) = file.write_all_at(buffer, HEADER_START).await;
        res?;
        file.sync_data().await?;

        Ok(Self { file })
    }

    #[inline]
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = Arc::new(File::open(path).await?);
        let (res, buf) = file.read_exact_at(vec![0; HEADER_SIZE], HEADER_START).await;
        res?;
        Header::deserialize(&buf)?;
        Ok(Self { file })
    }

    #[inline]
    pub async fn read_at(&self, pos: u64, size: usize) -> io::Result<Vec<u8>> {
        let (res, buf) = self.file.read_exact_at(vec![0; size], pos).await;
        res?;
        Ok(buf)
    }

    #[inline]
    pub async fn write_at(&self, pos: u64, data: &[u8; PAGE_SIZE]) -> io::Result<()> {
        self.file.write_all_at(data.to_vec(), pos).await.0
    }
}
