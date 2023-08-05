#![deny(rust_2018_idioms)]

use std::path::Path;
use std::sync::Arc;

pub use anyhow::Error;
use arc_swap::ArcSwapOption;
use mimalloc::MiMalloc;
use nsql_bind::Binder;
use nsql_catalog::Catalog;
pub use nsql_core::LogicalType;
use nsql_execution::{ExecutionContext, PhysicalPlanner, TransactionContext, TransactionState};
pub use nsql_lmdb::LmdbStorageEngine;
use nsql_opt::optimize;
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

    #[inline]
    pub fn connect<'any>(&self) -> (Connection<S>, ConnectionState<'any, S>) {
        (Connection { db: self.clone() }, ConnectionState::default())
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
        let tx = state.current_tx.swap(None).map(|tx| {
            Arc::into_inner(tx).expect("unexpected outstanding references to transaction")
        });

        let (tx, output) = self.db.shared.query(tx, query)?;

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
        &'env self,
        tx: Option<ReadOrWriteTransaction<'env, S>>,
        query: &str,
    ) -> Result<(Option<ReadOrWriteTransaction<'env, S>>, MaterializedQueryOutput)> {
        let statements = nsql_parse::parse_statements(query)?;
        if statements.is_empty() {
            return Ok((tx, MaterializedQueryOutput { types: vec![], tuples: vec![] }));
        }

        if statements.len() > 1 {
            todo!("multiple statements");
        }

        let storage = self.storage.storage();
        let catalog = Catalog::open(storage);
        let stmt = &statements[0];

        let binder = Binder::new(catalog);
        let (auto_commit, tx) = match tx {
            Some(tx) => (false, tx),
            None => (
                true,
                match binder.requires_write_transaction(storage, stmt)? {
                    true => {
                        tracing::info!("beginning fresh write transaction");
                        ReadOrWriteTransaction::Write(storage.begin_write()?)
                    }
                    false => {
                        tracing::info!("beginning fresh read transaction");
                        ReadOrWriteTransaction::Read(storage.begin()?)
                    }
                },
            ),
        };

        let plan = optimize(binder.bind(&tx, stmt)?);

        let mut planner = PhysicalPlanner::new(catalog);

        let (tx, tuples) = match tx {
            ReadOrWriteTransaction::Read(tx) => {
                tracing::info!("executing readonly query");
                let physical_plan = planner.plan(&tx, plan)?;
                let ecx = ExecutionContext::new(catalog, TransactionContext::new(tx, auto_commit));
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
                tracing::info!("executing write query");
                let physical_plan = planner.plan_write(&tx, plan)?;
                let ecx = ExecutionContext::new(catalog, TransactionContext::new(tx, auto_commit));
                let tuples = match nsql_execution::execute_write(&ecx, physical_plan) {
                    Ok(tuples) => tuples,
                    Err(err) => {
                        tracing::info!(error = %err, "aborting write transaction due to error during execution");
                        let (_, _, tx) = ecx.take_txn();
                        tx.abort()?;
                        return Err(err);
                    }
                };

                let (auto_commit, state, tx) = ecx.take_txn();
                match state {
                    TransactionState::Active if auto_commit => {
                        tracing::info!("auto-committing write transaction");
                        tx.commit()?;
                    }
                    TransactionState::Active => {
                        return Ok((
                            Some(ReadOrWriteTransaction::Write(tx)),
                            MaterializedQueryOutput { types: vec![], tuples },
                        ));
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

        // FIXME need to get the types
        Ok((tx, MaterializedQueryOutput { types: vec![], tuples }))
    }
}
