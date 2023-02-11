#![deny(rust_2018_idioms)]
#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]

use std::io;
use std::ops::Deref;
use std::path::Path;

use glommio::io::{DmaFile, ReadResult};
use glommio::GlommioError;

pub const HEADER_SIZE: usize = 4096;
pub const PAGE_SIZE: usize = 4096;

pub struct DmaFileStorage {
    file: DmaFile,
}

impl DmaFileStorage {
    #[inline]
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Ok(Self { file: DmaFile::open(path).await.map_err(convert_err)? })
    }
}

pub trait Buffer: Deref<Target = [u8]> + 'static {}

impl Buffer for ReadResult {}

pub trait Storage {
    type Bytes: Buffer;
    async fn read_at(&self, pos: u64, size: usize) -> Result<Self::Bytes, std::io::Error>;
}

impl Storage for DmaFileStorage {
    type Bytes = ReadResult;

    async fn read_at(&self, pos: u64, size: usize) -> Result<Self::Bytes, std::io::Error> {
        self.file.read_at(pos, size).await.map_err(convert_err)
    }
}

fn convert_err<T>(err: GlommioError<T>) -> std::io::Error {
    match err {
        GlommioError::IoError(err) => err,
        GlommioError::EnhancedIoError { source, .. } => source,
        _ => todo!("unhandled glommio::io error"),
    }
}
