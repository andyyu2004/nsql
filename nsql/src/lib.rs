#![deny(rust_2018_idioms)]

use std::path::Path;
use std::sync::Arc;

use nsql_buffer::{BufferPool, Result};
use nsql_pager::{InMemoryPager, Pager, SingleFilePager};

#[derive(Clone)]
pub struct Nsql<P> {
    inner: Arc<Shared<P>>,
}

impl<P: Pager> Nsql<P> {
    pub fn query(&self, query: &str) -> Result<()> {
        todo!()
    }
}

impl<P> Nsql<P> {
    fn new(inner: Shared<P>) -> Self {
        Self { inner: Arc::new(inner) }
    }
}

struct Shared<P> {
    buffer_pool: BufferPool<P>,
}

impl<P> Shared<P> {
    fn new(buffer_pool: BufferPool<P>) -> Self {
        Self { buffer_pool }
    }
}

impl Nsql<InMemoryPager> {
    pub fn mem() -> Self {
        Self::new(Shared::new(BufferPool::new(InMemoryPager::default())))
    }
}

impl Nsql<SingleFilePager> {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self::new(Shared::new(BufferPool::new(SingleFilePager::open(path).await?))))
    }
}
