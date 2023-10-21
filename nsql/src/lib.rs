#![deny(rust_2018_idioms)]
#![feature(thread_id_value)]

use core::fmt;
use std::panic::AssertUnwindSafe;
use std::path::Path;
use std::sync::Arc;
use std::{env, process};

pub use anyhow::{anyhow, Error};
use arc_swap::ArcSwapOption;
use measureme::{EventId, Profiler, StringId};
use mimalloc::MiMalloc;
use nsql_bind::Binder;
use nsql_catalog::Catalog;
pub use nsql_core::LogicalType;
use nsql_core::Schema;
use nsql_execution::config::SessionConfig;
use nsql_execution::{ExecutionContext, PhysicalPlanner, TransactionContext, TransactionState};
pub use nsql_lmdb::LmdbStorageEngine;
use nsql_opt::optimize;
use nsql_parse::ast;
pub use nsql_parse::parse;
pub use nsql_redb::RedbStorageEngine;
pub use nsql_storage::tuple::Tuple;
use nsql_storage::Storage;
pub use nsql_storage_engine::StorageEngine;
use nsql_storage_engine::{ReadOrWriteTransaction, ReadonlyExecutionMode, WriteTransaction};

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

#[derive(Debug)]
pub struct MaterializedQueryOutput {
    pub schema: Schema,
    pub tuples: Vec<Tuple>,
}

impl MaterializedQueryOutput {
    fn new(schema: Schema, mut tuples: Vec<Tuple>) -> Self {
        if !tuples.is_empty() {
            assert_eq!(tuples[0].width(), schema.width());
        }

        tuples.shrink_to_fit();
        Self { schema, tuples }
    }
}

impl fmt::Display for MaterializedQueryOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use tabled::builder::Builder;
        use tabled::settings::Style;
        let mut builder = Builder::default();
        for tuple in &self.tuples {
            builder.push_record(tuple.values().map(|v| v.to_string()));
        }

        let mut table = builder.build();
        table.with(Style::empty());
        table.fmt(f)
    }
}

impl<S: StorageEngine> Nsql<S> {
    /// Create a new database at the given path, overwriting any existing database.
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let storage = S::create(path)?;
        let tx = storage.begin_write()?;
        Catalog::create(&storage, &tx)?;
        tx.commit()?;

        Self::try_new(storage)
    }

    /// Open an existing database at the given path, creating one if it does not exist.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Self::create(path);
        }

        Self::try_new(S::open(path)?)
    }

    // FIXME can't find a way to get lifetimes to checkout without separating the connection and state.
    // Ideally we'd have the connection hold the state.
    #[inline]
    pub fn connect<'any>(&self) -> (Connection<S>, SessionContext<'any, S>) {
        (Connection { db: self.clone() }, Default::default())
    }

    #[inline]
    fn try_new(storage: S) -> Result<Self> {
        let pid: u32 = process::id();
        let filename = format!("nsql-{pid:07}.profile");
        let path = env::temp_dir().join(filename);
        let profiler = Profiler::new(path).map_err(|err| anyhow!(err))?;
        Ok(Self { shared: Arc::new(Shared::new(Storage::new(storage), profiler)) })
    }
}

struct Shared<S> {
    storage: Storage<S>,
    profiler: NsqlProfiler,
}

struct NsqlProfiler {
    profiler: Profiler,
    generic_event_kind: StringId,
    bind_event_id: EventId,
    optimize_event_id: EventId,
    physical_plan_event_id: EventId,
    execute_event_id: EventId,
    thread_id: u32,
}

impl NsqlProfiler {
    fn new(profiler: Profiler) -> Self {
        Self {
            bind_event_id: EventId::from_label(profiler.alloc_string("bind")),
            optimize_event_id: EventId::from_label(profiler.alloc_string("optimize")),
            physical_plan_event_id: EventId::from_label(profiler.alloc_string("physical_plan")),
            execute_event_id: EventId::from_label(profiler.alloc_string("execute")),
            generic_event_kind: profiler.alloc_string("generic"),
            // everything is currently single-threaded and always will be except for execution stuff
            thread_id: std::thread::current().id().as_u64().get() as u32,
            profiler,
        }
    }

    fn profile<R>(&self, event_id: EventId, f: impl FnOnce() -> Result<R>) -> Result<R> {
        let _guard = self.profiler.start_recording_interval_event(
            self.generic_event_kind,
            event_id,
            self.thread_id,
        );
        f()
    }
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
        ctx: &SessionContext<'env, S>,
        query: &str,
    ) -> Result<MaterializedQueryOutput> {
        std::panic::catch_unwind(AssertUnwindSafe(|| {
            let output = self.db.shared.query(ctx, query)?;

            Ok(output)
        }))
        .map_err(|data| match data.downcast::<String>() {
            Ok(msg) => anyhow!("{msg}"),
            Err(_) => anyhow!("caught panic"),
        })?
    }
}

impl<S: StorageEngine> Shared<S> {
    fn new(storage: Storage<S>, profiler: Profiler) -> Self {
        Self { storage, profiler: NsqlProfiler::new(profiler) }
    }

    fn query<'env>(
        &'env self,
        ctx: &SessionContext<'env, S>,
        query: &str,
    ) -> Result<MaterializedQueryOutput> {
        let stmts = nsql_parse::parse_statements(query)?;

        match &stmts[..] {
            [] => Ok(MaterializedQueryOutput { schema: Schema::empty(), tuples: vec![] }),
            [stmts @ .., last] => {
                for stmt in stmts {
                    let _output = self.execute(ctx, stmt);
                }
                self.execute(ctx, last)
            }
        }
    }

    #[tracing::instrument(skip(self, ctx, stmt), fields(%stmt))]
    fn execute<'env>(
        &'env self,
        ctx: &SessionContext<'env, S>,
        stmt: &ast::Statement,
    ) -> Result<MaterializedQueryOutput> {
        let tx = ctx.current_tx.swap(None).map(|tx| {
            Arc::into_inner(tx).expect("unexpected outstanding references to transaction")
        });

        let storage = self.storage.storage();
        let catalog = Catalog::new(storage);

        let binder = Binder::new(catalog);
        let (auto_commit, tx, plan) =
            self.profiler.profile(self.profiler.bind_event_id, || match tx {
                Some(tx) => {
                    tracing::debug!("reusing existing transaction");
                    let plan = binder.bind_with(&tx, stmt)?;
                    Ok((false, tx, plan))
                }
                None => {
                    let plan = binder.bind(stmt)?;
                    let tx = match plan.required_transaction_mode() {
                        ir::TransactionMode::ReadOnly => {
                            tracing::debug!("beginning fresh read transaction");
                            ReadOrWriteTransaction::Read(storage.begin()?)
                        }
                        ir::TransactionMode::ReadWrite => {
                            tracing::debug!("beginning fresh write transaction");
                            ReadOrWriteTransaction::Write(storage.begin_write()?)
                        }
                    };
                    Ok((true, tx, plan))
                }
            })?;
        let schema = Schema::new(plan.schema());
        let plan = self.profiler.profile(self.profiler.optimize_event_id, || Ok(optimize(plan)))?;

        let (tx, tuples) = match tx {
            ReadOrWriteTransaction::Read(tx) => {
                tracing::debug!("executing readonly query");
                let planner = PhysicalPlanner::new(catalog);
                let physical_plan = self
                    .profiler
                    .profile(self.profiler.physical_plan_event_id, || planner.plan(&tx, plan))?;
                let ecx =
                    ExecutionContext::new(catalog, TransactionContext::new(tx, auto_commit), ctx);
                let tuples = self.profiler.profile(self.profiler.execute_event_id, || {
                    nsql_execution::execute::<S, ReadonlyExecutionMode>(&ecx, physical_plan)
                })?;
                let (auto_commit, state, tx) = ecx.take_txn();
                if auto_commit || !matches!(state, TransactionState::Active) {
                    tracing::debug!("ending readonly transaction");
                    (None, tuples)
                } else {
                    (Some(ReadOrWriteTransaction::<S>::Read(tx)), tuples)
                }
            }
            ReadOrWriteTransaction::Write(tx) => {
                tracing::debug!("executing write query");
                let planner = PhysicalPlanner::new(catalog);
                let physical_plan =
                    self.profiler.profile(self.profiler.physical_plan_event_id, || {
                        planner.plan_write(&tx, plan)
                    })?;
                let ecx =
                    ExecutionContext::new(catalog, TransactionContext::new(tx, auto_commit), ctx);
                let tuples = self.profiler.profile(self.profiler.execute_event_id, || {
                    nsql_execution::execute(&ecx, physical_plan)
                });

                let tuples = match tuples {
                    Ok(tuples) => tuples,
                    Err(err) => {
                        tracing::debug!(error = %err, "aborting write transaction due to error during execution");
                        let (_, _, tx) = ecx.take_txn();
                        tx.abort()?;
                        return Err(err);
                    }
                };

                let (auto_commit, tx_state, tx) = ecx.take_txn();
                match tx_state {
                    TransactionState::Active if auto_commit => {
                        tracing::debug!("auto-committing write transaction");
                        tx.commit()?;
                    }
                    TransactionState::Active => {
                        ctx.current_tx.store(Some(Arc::new(ReadOrWriteTransaction::Write(tx))));
                        return Ok(MaterializedQueryOutput { schema, tuples });
                    }
                    TransactionState::Committed => {
                        tracing::debug!("committing write transaction");
                        tx.commit()?
                    }
                    TransactionState::Aborted => {
                        tracing::debug!("aborting write transaction");
                        tx.abort()?
                    }
                }

                (None, tuples)
            }
        };

        // `tx` was not committed or aborted (including autocommits), so we need to put it back
        if let Some(tx) = tx {
            // if the transaction is still active, it must be the case that it's not an auto-commit
            ctx.current_tx.store(Some(Arc::new(tx)));
        }

        Ok(MaterializedQueryOutput::new(schema, tuples))
    }
}
