#![deny(rust_2018_idioms)]

use std::path::Path;
use std::sync::Arc;

use nsql_bind::Binder;
use nsql_buffer::BufferPool;
use nsql_catalog::Catalog;
use nsql_core::schema::LogicalType;
use nsql_execution::PhysicalPlanner;
use nsql_opt::optimize;
use nsql_pager::{InMemoryPager, Pager, SingleFilePager};
use nsql_plan::Planner;
use nsql_storage::tuple::Tuple;
use nsql_storage::Storage;
use nsql_transaction::TransactionManager;
use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct Nsql {
    inner: Arc<Shared>,
}

pub struct MaterializedQueryOutput {
    pub types: Vec<LogicalType>,
    pub tuples: Vec<Tuple>,
}

impl Nsql {
    pub async fn query(&self, query: &str) -> Result<MaterializedQueryOutput> {
        let tx = self.inner.txm.begin().await;
        let statements = nsql_parse::parse_statements(query)?;
        if statements.is_empty() {
            return Ok(MaterializedQueryOutput { types: vec![], tuples: vec![] });
        }

        if statements.len() > 1 {
            todo!("multiple statements");
        }

        let catalog = &self.inner.catalog;
        let stmt = &statements[0];
        let stmt = Binder::new(&tx, &self.inner.catalog).bind(stmt)?;

        let plan = Planner::default().plan(stmt);
        let plan = optimize(plan);

        let physical_plan = PhysicalPlanner::new(self.inner.storage.pager()).plan(&plan);
        let tuples = nsql_execution::execute(&tx, catalog, physical_plan).await?;

        tx.commit().await;

        Ok(MaterializedQueryOutput { types: vec![], tuples })
    }
}

impl Nsql {
    fn new(inner: Shared) -> Self {
        Self { inner: Arc::new(inner) }
    }
}

struct Shared {
    storage: Storage,
    buffer_pool: BufferPool,
    txm: TransactionManager,
    catalog: Catalog,
}

impl Nsql {
    pub async fn mem() -> Result<Self> {
        let txm = TransactionManager::default();
        let pager = Arc::new(InMemoryPager::default()) as Arc<dyn Pager>;
        let storage = Storage::new(Arc::clone(&pager));
        let buffer_pool = BufferPool::new(pager);

        let tx = txm.begin().await;
        let catalog = Catalog::create(&tx)?;
        tx.commit().await;

        Ok(Self::new(Shared { storage, buffer_pool, txm, catalog }))
    }
}

impl Nsql {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let txm = TransactionManager::default();
        let pager = Arc::new(SingleFilePager::open(path).await?) as Arc<dyn Pager>;
        let storage = Storage::new(Arc::clone(&pager));
        let _buffer_pool = BufferPool::new(pager);

        let tx = txm.begin().await;
        storage.load(&tx).await?;
        tx.commit().await;
        // let catalog = checkpoint.catalog;
        todo!()

        // Ok(Self::new(Shared { storage, buffer_pool, txm, catalog }))
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Parse(#[from] nsql_parse::Error),
    #[error(transparent)]
    Storage(#[from] nsql_storage::Error),
    #[error(transparent)]
    Bind(#[from] nsql_bind::Error),
    #[error(transparent)]
    Catalog(#[from] nsql_catalog::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Execution(#[from] nsql_execution::Error),
}
