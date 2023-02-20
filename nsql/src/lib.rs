#![deny(rust_2018_idioms)]

use std::path::Path;
use std::sync::Arc;

use nsql_buffer::{BufferPool, Result};
use nsql_catalog::Catalog;
use nsql_pager::{InMemoryPager, Pager, SingleFilePager};
use nsql_storage::Storage;
use nsql_transaction::TransactionManager;

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
    storage: Storage<P>,
    buffer_pool: BufferPool<P>,
    txm: TransactionManager,
    catalog: Catalog,
}

impl Nsql<InMemoryPager> {
    pub fn mem() -> Self {
        let txm = TransactionManager::default();
        let pager = Arc::new(InMemoryPager::default());
        let storage = Storage::new(Arc::clone(&pager));
        let buffer_pool = BufferPool::new(pager);
        Self::new(Shared { storage, buffer_pool, txm, catalog: Catalog::default() })
    }
}

impl Nsql<SingleFilePager> {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let txm = TransactionManager::default();
        let pager = Arc::new(SingleFilePager::open(path).await?);
        let storage = Storage::new(Arc::clone(&pager));
        let buffer_pool = BufferPool::new(pager);

        let tx = txm.begin();
        let checkpoint = storage.load(&tx).await?;
        tx.commit().await;
        let catalog = checkpoint.catalog;

        Ok(Self::new(Shared { storage, buffer_pool, txm, catalog }))
    }
}
