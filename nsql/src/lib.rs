#![deny(rust_2018_idioms)]

use std::path::Path;
use std::sync::Arc;

use nsql_bind::Binder;
use nsql_buffer::BufferPool;
use nsql_catalog::Catalog;
use nsql_pager::{InMemoryPager, Pager, SingleFilePager};
use nsql_storage::Storage;
use nsql_transaction::TransactionManager;
use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Parse(#[from] nsql_parse::Error),
    #[error(transparent)]
    Storage(#[from] nsql_storage::Error),
    #[error(transparent)]
    Bind(#[from] nsql_bind::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[derive(Clone)]
pub struct Nsql<P> {
    inner: Arc<Shared<P>>,
}

pub struct MaterializedQueryOutput {}

impl<P: Pager> Nsql<P> {
    pub async fn query(&self, query: &str) -> Result<MaterializedQueryOutput> {
        let tx = self.inner.txm.begin().await;
        let statements = nsql_parse::parse_statements(query)?;
        if statements.is_empty() {
            return Ok(MaterializedQueryOutput {});
        }

        if statements.len() > 1 {
            todo!();
        }

        let statement = &statements[0];
        let binder = Binder::new(&self.inner.catalog, &tx);
        binder.bind(statement)?;
        tx.commit().await;

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

        let tx = txm.begin().await;
        let checkpoint = storage.load(&tx).await?;
        tx.commit().await;
        let catalog = checkpoint.catalog;

        Ok(Self::new(Shared { storage, buffer_pool, txm, catalog }))
    }
}
