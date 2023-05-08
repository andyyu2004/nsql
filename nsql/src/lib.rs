#![deny(rust_2018_idioms)]

use std::io;
use std::path::Path;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use error_stack::Report;
use nsql_bind::Binder;
use nsql_buffer::{BufferPool, Pool};
use nsql_catalog::{Catalog, Transaction};
use nsql_execution::{ExecutionContext, PhysicalPlanner};
use nsql_opt::optimize;
use nsql_pager::{InMemoryPager, Pager, SingleFilePager};
use nsql_plan::Planner;
use nsql_storage::schema::LogicalType;
use nsql_storage::tuple::Tuple;
use nsql_storage::Storage;
use nsql_transaction::{TransactionManager, TransactionState};
use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct Nsql {
    shared: Arc<Shared>,
}

pub struct MaterializedQueryOutput {
    pub types: Vec<LogicalType>,
    pub tuples: Vec<Tuple>,
}

impl Nsql {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let txm = TransactionManager::default();
        let pager = Arc::new(SingleFilePager::open(path).await?) as Arc<dyn Pager>;
        let storage = Storage::new(Arc::clone(&pager));
        let _buffer_pool = BufferPool::new(pager);

        let tx = txm.begin();
        storage.load(&tx).await?;
        tx.commit();
        todo!()

        // Ok(Self::new(Shared { storage, buffer_pool, txm, catalog }))
    }

    pub async fn mem() -> Result<Self> {
        let txm = TransactionManager::default();
        let pager = Arc::new(InMemoryPager::default()) as Arc<dyn Pager>;
        let storage = Storage::new(Arc::clone(&pager));
        let buffer_pool = Arc::new(BufferPool::new(pager));

        let tx = txm.begin();
        let catalog = Arc::new(Catalog::create(&tx)?);
        tx.commit();

        Ok(Self::new(Shared { storage, buffer_pool, txm, catalog }))
    }

    pub fn connect(&self) -> Connection {
        Connection { db: self.clone(), current_tx: Default::default() }
    }

    fn new(inner: Shared) -> Self {
        Self { shared: Arc::new(inner) }
    }
}

struct Shared {
    storage: Storage,
    buffer_pool: Arc<dyn Pool>,
    txm: TransactionManager,
    catalog: Arc<Catalog>,
}

pub struct Connection {
    db: Nsql,
    current_tx: ArcSwapOption<Transaction>,
}

impl Connection {
    pub async fn query(&self, query: &str) -> Result<MaterializedQueryOutput> {
        let tx = match self.current_tx.load_full() {
            Some(tx) => {
                tracing::debug!(txid = %tx.id(), "continuing existing tx");
                tx
            }
            None => {
                let tx = self.db.shared.txm.begin();
                tracing::debug!(txid = ?tx, "beginning new tx");
                self.current_tx.store(Some(Arc::clone(&tx)));
                tx
            }
        };

        let output = self.db.shared.query(Arc::clone(&tx), query).await?;

        if tx.auto_commit() {
            self.current_tx.store(None);
            tx.commit();
        } else if matches!(tx.state(), TransactionState::Committed | TransactionState::RolledBack) {
            self.current_tx.store(None);
        }

        Ok(output)
    }
}

impl Shared {
    async fn query(&self, tx: Arc<Transaction>, query: &str) -> Result<MaterializedQueryOutput> {
        let statements = nsql_parse::parse_statements(query)?;
        if statements.is_empty() {
            return Ok(MaterializedQueryOutput { types: vec![], tuples: vec![] });
        }

        if statements.len() > 1 {
            todo!("multiple statements");
        }

        let catalog = Arc::clone(&self.catalog);
        let stmt = &statements[0];
        let stmt = Binder::new(Arc::clone(&catalog), Arc::clone(&tx)).bind(stmt)?;

        let plan = Planner::default().plan(stmt);

        let plan = optimize(plan);

        let physical_plan =
            PhysicalPlanner::new(Arc::clone(&catalog), Arc::clone(&tx)).plan(plan)?;
        let ctx = ExecutionContext::new(Arc::clone(&self.buffer_pool), catalog, Arc::clone(&tx));
        let tuples = nsql_execution::execute(ctx, physical_plan).await?;

        Ok(MaterializedQueryOutput { types: vec![], tuples })
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Parse(#[from] nsql_parse::Error),
    #[error(transparent)]
    Storage(#[from] nsql_storage::Error),
    #[error("binder error: {0}")]
    Bind(nsql_bind::Error),
    #[error("catalog error: {0}")]
    Catalog(nsql_catalog::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Execution(#[from] nsql_execution::Error),
}

impl From<Report<io::Error>> for Error {
    fn from(report: Report<io::Error>) -> Self {
        Error::Io(io::Error::new(report.current_context().kind(), report))
    }
}
