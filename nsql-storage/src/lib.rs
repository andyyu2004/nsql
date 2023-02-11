#![deny(rust_2018_idioms)]
#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]

use std::io;
use std::path::Path;
use std::sync::Arc;

use glommio::io::{DmaFile, ReadResult};
use glommio::GlommioError;

pub const HEADER_SIZE: usize = 4096;
pub const PAGE_SIZE: usize = 4096;

#[derive(Clone)]
pub struct Storage {
    file: Arc<DmaFile>,
}

impl Storage {
    #[inline]
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Ok(Self { file: Arc::new(DmaFile::open(path).await.map_err(convert_err)?) })
    }

    #[inline]
    pub async fn read_at(&self, pos: u64, size: usize) -> io::Result<ReadResult> {
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
