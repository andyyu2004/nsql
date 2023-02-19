#![deny(rust_2018_idioms)]
#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]

use std::path::Path;

use tokio_uring::fs::{self, OpenOptions};

/// `File` provides the low-level interface to the underlying file
/// `N` represents block size
pub struct File<const N: usize> {
    file: fs::File,
}

pub type Result<T, E = std::io::Error> = nsql_error::Result<T, E>;

// FIXME we could probably use fixed iouring buffers
impl<const N: usize> File<N> {
    #[inline]
    pub async fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            // .custom_flags(libc::O_DIRECT) // FIXME
            .open(path)
            .await?;

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

        Ok(Self::new(file))
    }

    #[inline]
    pub async fn read_at(&self, pos: u64) -> Result<[u8; N]> {
        Self::assert_aligned(pos);
        let (res, buf) = self.file.read_exact_at(vec![0; N], pos).await;
        res?;
        Ok(buf.try_into().expect("we specified the correct length"))
    }

    #[inline]
    pub async fn write_at(&self, pos: u64, data: [u8; N]) -> Result<()> {
        Self::assert_aligned(pos);
        self.file.write_all_at(data.to_vec(), pos).await.0?;
        Ok(())
    }

    #[inline]
    pub async fn sync(&self) -> Result<()> {
        self.file.sync_all().await?;
        Ok(())
    }

    fn new(file: fs::File) -> Self {
        assert!(N.is_power_of_two(), "N must be a power of two");
        Self { file }
    }

    fn assert_aligned(pos: u64) {
        assert!(Self::is_aligned(pos), "position `{pos}` is not aligned to size `{N}`");
    }

    const fn is_aligned(pos: u64) -> bool {
        pos % N as u64 == 0
    }
}
