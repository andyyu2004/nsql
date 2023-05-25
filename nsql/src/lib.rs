#![deny(rust_2018_idioms)]

use std::path::Path;
use std::sync::Arc;

pub use anyhow::Error;
use arc_swap::ArcSwapOption;
use nsql_bind::Binder;
use nsql_catalog::Catalog;
use nsql_execution::{ExecutionContext, PhysicalPlanner};
use nsql_lmdb::LmdbStorageEngine;
use nsql_opt::optimize;
use nsql_plan::Planner;
pub use nsql_storage::schema::LogicalType;
pub use nsql_storage::tuple::Tuple;
use nsql_storage::Storage;
use nsql_storage_engine::{
    ReadOrWriteTransaction, ReadOrWriteTransactionRef, StorageEngine, WriteTransaction,
};

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
        let mut tx = storage.begin_write()?;
        let catalog = Arc::new(Catalog::<S>::create(&mut tx)?);
        tx.commit()?;

        Ok(Self::new(Shared { storage: Storage::new(storage), catalog }))
    }

    #[inline]
    pub fn connect<'any>(&self) -> (Connection<S>, ConnectionState<'any, S>) {
        (Connection { db: self.clone() }, ConnectionState::default())
    }

    #[inline]
    fn new(inner: Shared<S>) -> Self {
        Self { shared: Arc::new(inner) }
    }
}

// FIXME don't have an im memory impl currently
impl Nsql<LmdbStorageEngine> {
    #[cfg(feature = "in-memory")]
    pub fn in_memory() -> Result<Self> {
        Self::open(":memory:")
    }
}

struct Shared<S> {
    storage: Storage<S>,
    catalog: Arc<Catalog<S>>,
}

pub struct Connection<S: StorageEngine> {
    db: Nsql<S>,
}

pub struct ConnectionState<'env, S: StorageEngine> {
    current_tx: ArcSwapOption<ReadOrWriteTransaction<'env, S>>,
}

impl<S: StorageEngine> Default for ConnectionState<'_, S> {
    #[inline]
    fn default() -> Self {
        Self { current_tx: ArcSwapOption::default() }
    }
}

impl<S: StorageEngine> Connection<S> {
    pub fn query<'env>(
        &'env self,
        state: &ConnectionState<'env, S>,
        query: &str,
    ) -> Result<MaterializedQueryOutput> {
        let tx = match state.current_tx.swap(None) {
            // Some(tx) => tx,
            Some(tx) => todo!(),
            None => Arc::new(ReadOrWriteTransaction::Write(self.db.shared.storage.begin_write()?)),
        };

        // FIXME need to get the transaction back somehow if it wasn't used to either commit or abort
        let tx = Arc::into_inner(tx).expect("unexpected outstanding references to transaction");
        let output = self.db.shared.query(tx, query)?;

        // FIXME
        // if tx.auto_commit() {
        //     self.current_tx.store(None);
        //     tx.commit().await?;
        // } else if matches!(tx.state(), TransactionState::Committed | TransactionState::RolledBack) {
        //     self.current_tx.store(None);
        // }

        Ok(output)
    }
}

impl<S: StorageEngine> Shared<S> {
    fn query(
        &self,
        tx: ReadOrWriteTransaction<'_, S>,
        query: &str,
    ) -> Result<MaterializedQueryOutput> {
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
                let physical_plan = planner.plan(&tx, plan)?;
                let ctx = ExecutionContext::new(self.storage.storage(), catalog, tx);
                nsql_execution::execute(ctx, physical_plan)?
            }
            ReadOrWriteTransaction::Write(tx) => {
                let physical_plan = planner.plan_write(&tx, plan)?;
                let ctx = ExecutionContext::new(self.storage.storage(), catalog, tx);
                nsql_execution::execute(ctx, physical_plan)?
            }
        };

        // FIXME need to get the types
        Ok(MaterializedQueryOutput { types: vec![], tuples })
    }
}
