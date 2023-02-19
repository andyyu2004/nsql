use std::path::Path;

use nsql_buffer::{BufferPool, Result};
use nsql_pager::{InMemoryPager, Pager, SingleFilePager};

pub struct Nsql<P> {
    buffer_pool: BufferPool<P>,
}

impl Nsql<InMemoryPager> {
    pub fn mem() -> Result<Self> {
        Ok(Self { buffer_pool: BufferPool::new(InMemoryPager::default()) })
    }
}

impl Nsql<SingleFilePager> {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self { buffer_pool: BufferPool::new(SingleFilePager::open(path).await?) })
    }
}

impl<P: Pager> Nsql<P> {
    pub fn query(&self, query: &str) -> Result<()> {
        todo!()
    }
}
