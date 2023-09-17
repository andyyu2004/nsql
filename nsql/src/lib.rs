#![deny(rust_2018_idioms)]

use std::path::Path;
use std::sync::Arc;

pub use anyhow::Error;
use arc_swap::ArcSwapOption;
use mimalloc::MiMalloc;
use nsql_bind::Binder;
use nsql_catalog::Catalog;
pub use nsql_core::LogicalType;
use nsql_execution::config::SessionConfig;
use nsql_execution::{ExecutionContext, PhysicalPlanner, TransactionContext, TransactionState};
pub use nsql_lmdb::LmdbStorageEngine;
use nsql_opt::optimize;
pub use nsql_parse::parse;
pub use nsql_redb::RedbStorageEngine;
pub use nsql_storage::tuple::Tuple;
use nsql_storage::Storage;
pub use nsql_storage_engine::StorageEngine;
use nsql_storage_engine::{ReadOrWriteTransaction, WriteTransaction};

pub type Result<T, E = anyhow::Error> = std::result::Result<T, E>;

pub struct Nsql<S> {
    shared: Arc<Shared<S>>,
}

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

impl<S> Clone for Nsql<S> {
    fn clone(&self) -> Self {
        Self { shared: self.shared.clone() }
    }
}

pub struct MaterializedQueryOutput {
    pub types: Vec<LogicalType>,
    pub tuples: Vec<Tuple>,
}

impl<S: StorageEngine> Nsql<S> {
    /// Create a new database at the given path, overwriting any existing database.
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let storage = S::create(path)?;
        let tx = storage.begin_write()?;
        Catalog::create(&storage, &tx)?;
        tx.commit()?;

        Ok(Self::new(Shared { storage: Storage::new(storage) }))
    }

    /// Open an existing database at the given path, creating one if it does not exist.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Self::create(path);
        }

        Ok(Self::new(Shared { storage: Storage::new(S::open(path)?) }))
    }

    // FIXME can't find a way to get lifetimes to checkout without separating the connection and state.
    // Ideally we'd have the connection hold the state.
    #[inline]
    pub fn connect<'any>(&self) -> (Connection<S>, SessionContext<'any, S>) {
        (Connection { db: self.clone() }, Default::default())
    }

    #[inline]
    fn new(inner: Shared<S>) -> Self {
        Self { shared: Arc::new(inner) }
    }
}

struct Shared<S> {
    storage: Storage<S>,
}

pub struct Connection<S: StorageEngine> {
    db: Nsql<S>,
}

pub struct SessionContext<'env, S: StorageEngine> {
    current_tx: ArcSwapOption<ReadOrWriteTransaction<'env, S>>,
    config: SessionConfig,
}

impl<S: StorageEngine> Default for SessionContext<'_, S> {
    #[inline]
    fn default() -> Self {
        Self { current_tx: ArcSwapOption::default(), config: SessionConfig::default() }
    }
}

impl<'env, S: StorageEngine> nsql_execution::SessionContext for SessionContext<'env, S> {
    #[inline]
    fn config(&self) -> &SessionConfig {
        &self.config
    }
}

impl<S: StorageEngine> Connection<S> {
    pub fn query<'env>(
        &'env self,
        state: &SessionContext<'env, S>,
        query: &str,
    ) -> Result<MaterializedQueryOutput> {
        let output = self.db.shared.query(state, query)?;

        Ok(output)
    }
}

impl<S: StorageEngine> Shared<S> {
    fn query<'env>(
        &'env self,
        state: &SessionContext<'env, S>,
        query: &str,
    ) -> Result<MaterializedQueryOutput> {
        let statements = nsql_parse::parse_statements(query)?;
        if statements.is_empty() {
            return Ok(MaterializedQueryOutput { types: vec![], tuples: vec![] });
        }

        if statements.len() > 1 {
            todo!("multiple statements");
        }

        let tx = state.current_tx.swap(None).map(|tx| {
            Arc::into_inner(tx).expect("unexpected outstanding references to transaction")
        });

        let storage = self.storage.storage();
        let catalog = Catalog::new(storage);
        let stmt = &statements[0];

        let binder = Binder::new(catalog);
        let (auto_commit, tx, plan) = match tx {
            Some(tx) => {
                tracing::info!("reusing existing transaction");
                let plan = binder.bind_with(&tx, stmt)?;
                (false, tx, plan)
            }
            None => {
                let plan = binder.bind(stmt)?;
                let tx = match plan.required_transaction_mode() {
                    ir::TransactionMode::ReadOnly => {
                        tracing::info!("beginning fresh read transaction");
                        ReadOrWriteTransaction::Read(storage.begin()?)
                    }
                    ir::TransactionMode::ReadWrite => {
                        tracing::info!("beginning fresh write transaction");
                        ReadOrWriteTransaction::Write(storage.begin_write()?)
                    }
                };
                (true, tx, plan)
            }
        };

        let plan = optimize(plan);

        let mut planner = PhysicalPlanner::new(catalog);

        let (tx, tuples) = match tx {
            ReadOrWriteTransaction::Read(tx) => {
                tracing::info!(query, "executing readonly query");
                let physical_plan = planner.plan(&tx, plan)?;
                let ecx =
                    ExecutionContext::new(catalog, TransactionContext::new(tx, auto_commit), state);
                let tuples = nsql_execution::execute(&ecx, physical_plan)?;
                let (auto_commit, state, tx) = ecx.take_txn();
                if auto_commit || !matches!(state, TransactionState::Active) {
                    tracing::info!("ending readonly transaction");
                    (None, tuples)
                } else {
                    (Some(ReadOrWriteTransaction::<S>::Read(tx)), tuples)
                }
            }
            ReadOrWriteTransaction::Write(tx) => {
                tracing::info!(query, "executing write query");
                let physical_plan = planner.plan_write(&tx, plan)?;
                let ecx =
                    ExecutionContext::new(catalog, TransactionContext::new(tx, auto_commit), state);
                let tuples = match nsql_execution::execute_write(&ecx, physical_plan) {
                    Ok(tuples) => tuples,
                    Err(err) => {
                        tracing::info!(error = %err, "aborting write transaction due to error during execution");
                        let (_, _, tx) = ecx.take_txn();
                        tx.abort()?;
                        return Err(err);
                    }
                };

                let (auto_commit, tx_state, tx) = ecx.take_txn();
                match tx_state {
                    TransactionState::Active if auto_commit => {
                        tracing::info!("auto-committing write transaction");
                        tx.commit()?;
                    }
                    TransactionState::Active => {
                        state.current_tx.store(Some(Arc::new(ReadOrWriteTransaction::Write(tx))));
                        return Ok(MaterializedQueryOutput { types: vec![], tuples });
                    }
                    TransactionState::Committed => {
                        tracing::info!("committing write transaction");
                        tx.commit()?
                    }
                    TransactionState::Aborted => {
                        tracing::info!("aborting write transaction");
                        tx.abort()?
                    }
                }

                (None, tuples)
            }
        };

        // `tx` was not committed or aborted (including autocommits), so we need to put it back
        if let Some(tx) = tx {
            // if the transaction is still active, it must be the case that it's not an auto-commit
            state.current_tx.store(Some(Arc::new(tx)));
        }

        // FIXME need to get the types
        Ok(MaterializedQueryOutput { types: vec![], tuples })
    }
}
