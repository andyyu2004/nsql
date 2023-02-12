#![deny(rust_2018_idioms)]
#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]

use std::io;
use std::path::Path;
use std::sync::Arc;

use bytes::{Buf, BufMut};
use glommio::io::{DmaFile, OpenOptions, ReadResult};
use glommio::GlommioError;

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
    file: Arc<DmaFile>,
}

impl Storage {
    #[inline]
    pub async fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = Arc::new(
            OpenOptions::new().create_new(true).dma_open(path).await.map_err(convert_err)?,
        );

        let mut buffer = file.alloc_dma_buffer(HEADER_SIZE);
        buffer.as_mut().copy_from_slice(&[0; HEADER_SIZE]);
        let header = Header { magic: MAGIC, version: CURRENT_VERSION };
        header.serialize(buffer.as_mut());
        file.write_at(buffer, HEADER_START).await.map_err(convert_err)?;
        file.fdatasync().await.map_err(convert_err)?;

        Ok(Self { file })
    }

    #[inline]
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = Arc::new(DmaFile::open(path).await.map_err(convert_err)?);
        let buf = file.read_at(HEADER_START, HEADER_SIZE).await.map_err(convert_err)?;
        Header::deserialize(&buf)?;
        Ok(Self { file })
    }

    #[inline]
    pub async fn read_at(&self, pos: u64, size: usize) -> io::Result<ReadResult> {
        self.file.read_at(pos, size).await.map_err(convert_err)
    }

    #[inline]
    pub async fn write_at(&self, pos: u64, data: &[u8]) -> io::Result<()> {
        let size = data.len();
        let mut buf = self.file.alloc_dma_buffer(size);
        buf.as_mut().copy_from_slice(data);
        let n = self.file.write_at(buf, pos).await.map_err(convert_err)?;
        if n < size {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "failed to write all data to dmafile",
            ));
        }
        Ok(())
    }
}

fn convert_err<T>(err: GlommioError<T>) -> std::io::Error {
    match err {
        GlommioError::IoError(err) => err,
        GlommioError::EnhancedIoError { source, .. } => source,
        _ => todo!("unhandled glommio::io error"),
    }
}
