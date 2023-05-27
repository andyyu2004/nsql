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
        let (tx, output) = self.db.shared.query(tx, query)?;

        // `tx` was not committed or aborted (including autocommits), so we need to put it back
        if let Some(tx) = tx {
            state.current_tx.store(Some(Arc::new(tx)));
        }

        Ok(output)
    }
}

impl<S: StorageEngine> Shared<S> {
    fn query<'env>(
        &self,
        tx: ReadOrWriteTransaction<'env, S>,
        query: &str,
    ) -> Result<(Option<ReadOrWriteTransaction<'env, S>>, MaterializedQueryOutput)> {
        let statements = nsql_parse::parse_statements(query)?;
        if statements.is_empty() {
            return Ok((Some(tx), MaterializedQueryOutput { types: vec![], tuples: vec![] }));
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
        let (tx, tuples) = match tx {
            ReadOrWriteTransaction::Read(tx) => {
                let physical_plan = planner.plan(&tx, plan)?;
                let ctx = ExecutionContext::new(self.storage.storage(), catalog, tx);
                let tuples = nsql_execution::execute(&ctx, physical_plan)?;
                let (_auto_commit, txn) = ctx.take_txn();
                (txn.map(ReadOrWriteTransaction::<S>::Read), tuples)
            }
            ReadOrWriteTransaction::Write(tx) => {
                let physical_plan = planner.plan_write(&tx, plan)?;
                let ctx = ExecutionContext::new(self.storage.storage(), catalog, tx);
                let tuples = nsql_execution::execute(&ctx, physical_plan)?;
                let (auto_commit, mut txn) = ctx.take_txn();
                // FIXME need to remember `auto_commit` the next call
                if let Some(txn) = txn.take() {
                    if auto_commit {
                        txn.commit()?;
                    }
                }
                (txn.map(ReadOrWriteTransaction::<S>::Write), tuples)
            }
        };

        // FIXME need to get the types
        Ok((tx, MaterializedQueryOutput { types: vec![], tuples }))
    }
}
