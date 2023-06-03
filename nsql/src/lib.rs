#![deny(rust_2018_idioms)]

use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::{atomic, Arc};

pub use anyhow::Error;
use arc_swap::ArcSwapOption;
use nsql_bind::Binder;
pub use nsql_catalog::schema::LogicalType;
use nsql_catalog::Catalog;
use nsql_execution::{ExecutionContext, PhysicalPlanner, TransactionContext, TransactionState};
use nsql_opt::optimize;
use nsql_plan::Planner;
use nsql_redb::RedbStorageEngine;
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
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let storage = S::create(path)?;
        let tx = storage.begin_write()?;
        let catalog = Arc::new(Catalog::<S>::create(&tx)?);
        tx.commit()?;

        Ok(Self::new(Shared { storage: Storage::new(storage), catalog }))
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let storage = S::open(path)?;
        let tx = storage.begin_write()?;
        let catalog = Arc::new(Catalog::<S>::create(&tx)?);
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
impl Nsql<RedbStorageEngine> {
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
        let (auto_commit, tx) = match state.current_tx.swap(None) {
            // if there's an existing transaction, we reuse it.
            // It is not an auto_commit if it's still active
            Some(tx) => (false, tx),
            // start a fresh transaction (write by default as we don't know what queries will be executed so we can't guarantee that they're all readonly)
            None => (
                true,
                Arc::new(ReadOrWriteTransaction::Write(self.db.shared.storage.begin_write()?)),
            ),
        };

        let tx = Arc::into_inner(tx).expect("unexpected outstanding references to transaction");
        let (tx, output) = self.db.shared.query(tx, auto_commit, query)?;

        // `tx` was not committed or aborted (including autocommits), so we need to put it back
        if let Some(tx) = tx {
            // if the transaction is still active, it must be the case that it's not an auto-commit
            state.current_tx.store(Some(Arc::new(tx)));
        }

        Ok(output)
    }
}

impl<S: StorageEngine> Shared<S> {
    fn query<'env>(
        &self,
        tx: ReadOrWriteTransaction<'env, S>,
        auto_commit: bool,
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
                tracing::info!("executing readonly query");
                let physical_plan = planner.plan(&tx, plan)?;
                let ctx = ExecutionContext::new(
                    self.storage.storage(),
                    catalog,
                    TransactionContext::new(tx, auto_commit),
                );
                let tuples = nsql_execution::execute(&ctx, physical_plan)?;
                let (auto_commit, state, txn) = ctx.take_txn();
                if auto_commit || !matches!(state, TransactionState::Active) {
                    tracing::info!("ending readonly transaction");
                    (None, tuples)
                } else {
                    (Some(ReadOrWriteTransaction::<S>::Read(txn)), tuples)
                }
            }
            ReadOrWriteTransaction::Write(tx) => {
                tracing::info!("executing write query");
                let physical_plan = planner.plan_write(&tx, plan)?;
                let ctx = ExecutionContext::new(
                    self.storage.storage(),
                    catalog,
                    TransactionContext::new(tx, auto_commit),
                );
                let tuples = nsql_execution::execute_write(&ctx, physical_plan)?;
                let (auto_commit, state, txn) = ctx.take_txn();
                // FIXME need to remember `auto_commit` value for the next call, otherwise
                // auto_commit will be true again
                match state {
                    TransactionState::Active if auto_commit => {
                        tracing::info!("auto-committing write transaction");
                        txn.commit()?;
                    }
                    TransactionState::Active => {
                        return Ok((
                            Some(ReadOrWriteTransaction::Write(txn)),
                            MaterializedQueryOutput { types: vec![], tuples },
                        ));
                    }
                    TransactionState::Committed => {
                        tracing::info!("committing write transaction");
                        txn.commit()?
                    }
                    TransactionState::Aborted => {
                        tracing::info!("aborting write transaction");
                        txn.abort()?
                    }
                }

                (None, tuples)
            }
        };

        // FIXME need to get the types
        Ok((tx, MaterializedQueryOutput { types: vec![], tuples }))
    }
}
