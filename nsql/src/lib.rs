#![deny(rust_2018_idioms)]

use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

pub use anyhow::Error;
use arc_swap::ArcSwapOption;
use nsql_bind::Binder;
use nsql_catalog::Catalog;
use nsql_execution::{
    ExecutionContext, PhysicalPlanner, ReadWriteExecutionMode, ReadonlyExecutionMode,
};
use nsql_lmdb::LmdbStorageEngine;
use nsql_opt::optimize;
use nsql_plan::Planner;
pub use nsql_storage::schema::LogicalType;
pub use nsql_storage::tuple::Tuple;
use nsql_storage::Storage;
use nsql_storage_engine::{ReadOrWriteTransaction, StorageEngine, WriteTransaction};

pub type Result<T, E = anyhow::Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct Nsql<S> {
    shared: Arc<Shared<S>>,
}

pub struct MaterializedQueryOutput {
    pub types: Vec<LogicalType>,
    pub tuples: Vec<Tuple>,
}

impl<S: StorageEngine> Nsql<S> {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let storage = S::open(path)?;
        let mut tx = storage.begin()?;
        let catalog = Arc::new(Catalog::<S>::create(&mut tx)?);
        tx.commit()?;

        todo!()

        // Ok(Self::new(Shared { storage, buffer_pool, txm, catalog }))
    }

    pub fn connect(&self) -> Connection<'_, S> {
        Connection { db: self.clone(), current_tx: Default::default() }
    }

    fn new(inner: Shared<S>) -> Self {
        Self { shared: Arc::new(inner) }
    }
}

// FIXME don't have an im memory impl currently
impl Nsql<LmdbStorageEngine> {
    #[cfg(feature = "in-memory")]
    pub fn in_memory() -> Result<Self> {
        todo!()
    }
}

struct Shared<S> {
    storage: Storage<S>,
    catalog: Arc<Catalog<S>>,
}

pub struct Connection<'env, S: StorageEngine> {
    db: Nsql<S>,
    current_tx: ArcSwapOption<S::Transaction<'env>>,
}

impl<S: StorageEngine> Connection<'_, S> {
    pub fn query(&self, query: &str) -> Result<MaterializedQueryOutput> {
        // let tx = match self.current_tx.load_full() {
        //     Some(tx) => {
        //         tracing::debug!(xid = %tx.xid(), "continuing existing tx");
        //         tx
        //     }
        //     None => {
        //         let tx = self.db.shared.txm.begin();
        //         tracing::debug!(xid = ?tx, "beginning new tx");
        //         self.current_tx.store(Some(Arc::clone(&tx)));
        //         tx
        //     }
        // };

        todo!();
        // let output = self.db.shared.query(Arc::clone(&tx), query).await?;

        // FIXME
        // if tx.auto_commit() {
        //     self.current_tx.store(None);
        //     tx.commit().await?;
        // } else if matches!(tx.state(), TransactionState::Committed | TransactionState::RolledBack) {
        //     self.current_tx.store(None);
        // }

        // Ok(output)
    }
}

impl<S: StorageEngine> Shared<S> {
    fn query(
        &self,
        tx: &mut Option<ReadOrWriteTransaction<'_, S>>,
        query: &str,
    ) -> Result<MaterializedQueryOutput> {
        let tx = tx.take().unwrap();
        let statements = nsql_parse::parse_statements(query)?;
        if statements.is_empty() {
            return Ok(MaterializedQueryOutput { types: vec![], tuples: vec![] });
        }

        if statements.len() > 1 {
            todo!("multiple statements");
        }

        let catalog = Arc::clone(&self.catalog);
        let stmt = &statements[0];
        let stmt = Binder::new(Arc::clone(&catalog)).bind(&tx, stmt)?;

        let plan = Planner::default().plan(stmt);

        let plan = optimize(plan);

        let planner = PhysicalPlanner::new(Arc::clone(&catalog));
        let tuples = match tx {
            ReadOrWriteTransaction::Read(tx) => {
                let physical_plan = planner.plan::<ReadonlyExecutionMode<S>>(&tx, plan)?;
                let ctx = ExecutionContext::new(self.storage.storage(), catalog, tx.clone());
                nsql_execution::execute(ctx, physical_plan)?
            }
            ReadOrWriteTransaction::Write(tx) => {
                let physical_plan = planner.plan::<ReadWriteExecutionMode<S>>(&tx, plan)?;
                let ctx = ExecutionContext::new(self.storage.storage(), catalog, tx);
                nsql_execution::execute(ctx, physical_plan)?
            }
        };

        // FIXME need to get the types
        Ok(MaterializedQueryOutput { types: vec![], tuples })
    }
}
