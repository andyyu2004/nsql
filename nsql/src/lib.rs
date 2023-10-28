#![deny(rust_2018_idioms)]
#![feature(thread_id_value, trait_upcasting)]

use core::fmt;
use std::ops::ControlFlow;
use std::panic::AssertUnwindSafe;
use std::path::Path;
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;
use std::{env, process};

use anyhow::ensure;
pub use anyhow::{anyhow, Error};
use arc_swap::ArcSwapOption;
use measureme::{EventId, Profiler, StringId};
use mimalloc::MiMalloc;
use nsql_bind::Binder;
use nsql_catalog::{Catalog, TransactionLocalCatalogCaches};
pub use nsql_core::LogicalType;
use nsql_core::Schema;
use nsql_execution::config::SessionConfig;
use nsql_execution::{ExecutionContext, PhysicalPlan, PhysicalPlanner, TransactionState};
pub use nsql_lmdb::LmdbStorageEngine;
use nsql_opt::optimize;
use nsql_parse::ast::{self, Visit};
pub use nsql_parse::parse;
pub use nsql_redb::RedbStorageEngine;
pub use nsql_storage::tuple::Tuple;
use nsql_storage::Storage;
pub use nsql_storage_engine::StorageEngine;
use nsql_storage_engine::{
    ExecutionMode, ReadWriteExecutionMode, ReadonlyExecutionMode, Transaction,
};
use nsql_util::atomic::AtomicEnum;

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
        let tcx = TransactionContext::make(tx);
        tcx.with(|tcx| Catalog::create(&storage, &tcx))?;
        tcx.commit()?;

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

    #[inline]
    pub fn connect(&self) -> Connection<'_, S> {
        Connection { db: self, ctx: Default::default() }
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

pub struct Connection<'env, S: StorageEngine> {
    db: &'env Nsql<S>,
    ctx: SessionContext<'env, S>,
}

struct SessionContext<'env, S: StorageEngine> {
    current_tcx: ArcSwapOption<ReadOrWriteTransactionContext<'env, S>>,
    config: SessionConfig,
}

enum ReadOrWriteTransactionContext<'env, S: StorageEngine> {
    Read(TransactionContext<'env, S, ReadonlyExecutionMode>),
    Write(TransactionContext<'env, S, ReadWriteExecutionMode>),
}

impl<'env, S: StorageEngine> ReadOrWriteTransactionContext<'env, S> {
    #[must_use]
    fn is_write(&self) -> bool {
        matches!(self, Self::Write(..))
    }
}

#[ouroboros::self_referencing]
struct TransactionContext<'env, S: StorageEngine, M: ExecutionMode<'env, S>> {
    tx: Box<M::Transaction>,
    #[borrows(tx)]
    #[not_covariant]
    cache: TransactionLocalCatalogCaches<'env, 'this, S, M>,
    auto_commit: AtomicBool,
    state: AtomicEnum<TransactionState>,
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> TransactionContext<'env, S, M> {
    #[inline]
    pub fn make(tx: M::Transaction) -> TransactionContext<'env, S, M> {
        TransactionContextBuilder {
            tx: Box::new(tx),
            cache_builder: |_| Default::default(),
            auto_commit: AtomicBool::new(true),
            state: AtomicEnum::new(TransactionState::Active),
        }
        .build()
    }

    #[inline]
    pub fn commit(self) -> Result<(), S::Error> {
        self.into_heads().tx.commit_boxed()
    }

    #[inline]
    pub fn abort(self) -> Result<(), S::Error> {
        self.into_heads().tx.abort_boxed()
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    nsql_catalog::TransactionContext<'env, 'txn, S, M>
    for ouroboros_impl_transaction_context::BorrowedFields<'_, 'txn, 'env, S, M>
{
    #[inline]
    fn transaction(&self) -> &'txn M::Transaction {
        self.tx
    }

    #[inline]
    fn catalog_caches(&self) -> &TransactionLocalCatalogCaches<'env, 'txn, S, M> {
        self.cache
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    nsql_execution::TransactionContext<'env, 'txn, S, M>
    for ouroboros_impl_transaction_context::BorrowedFields<'_, 'txn, 'env, S, M>
{
    #[inline]
    fn get_auto_commit(&self) -> &AtomicBool {
        self.auto_commit
    }

    #[inline]
    fn state(&self) -> &AtomicEnum<TransactionState> {
        self.state
    }
}

impl<S: StorageEngine> Default for SessionContext<'_, S> {
    #[inline]
    fn default() -> Self {
        Self { current_tcx: ArcSwapOption::default(), config: SessionConfig::default() }
    }
}

impl<'env, S: StorageEngine> nsql_execution::SessionContext for SessionContext<'env, S> {
    #[inline]
    fn config(&self) -> &SessionConfig {
        &self.config
    }
}

impl<'env, S: StorageEngine> Connection<'env, S> {
    pub fn query(&self, query: &str) -> Result<MaterializedQueryOutput> {
        std::panic::catch_unwind(AssertUnwindSafe(|| self.db.shared.query(&self.ctx, query)))
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
        let tcx = ctx.current_tcx.swap(None).map(|tx| {
            Arc::into_inner(tx).expect("unexpected outstanding references to transaction context")
        });

        let catalog = Catalog::new(self.storage.storage());

        // FIXME this is only a best-effort strategy at best.
        // There must be a better way to figure out which transaction we should be using?
        let requires_write_transaction =
            matches!(stmt.visit(&mut RequiredTransactionModeVisitor), ControlFlow::Break(()));

        let tcx = match tcx {
            Some(tcx) => {
                tracing::debug!("reusing existing transaction");
                if requires_write_transaction {
                    ensure!(tcx.is_write(), "cannot execute write operation in read transaction");
                }

                tcx
            }
            None => {
                tracing::debug!("beginning fresh read transaction");
                if requires_write_transaction {
                    ReadOrWriteTransactionContext::Write(TransactionContext::make(
                        catalog.storage().begin_write()?,
                    ))
                } else {
                    ReadOrWriteTransactionContext::Read(TransactionContext::make(
                        catalog.storage().begin()?,
                    ))
                }
            }
        };

        let (tcx, output) = match tcx {
            ReadOrWriteTransactionContext::Read(tcx) => {
                let (tcx, output) =
                    self.execute_in(ctx, tcx, stmt, |planner, tcx, plan| planner.plan(tcx, plan))?;
                (tcx.map(ReadOrWriteTransactionContext::Read), output)
            }
            ReadOrWriteTransactionContext::Write(tcx) => {
                let (tcx, output) = self.execute_in(ctx, tcx, stmt, |planner, tcx, plan| {
                    planner.plan_write(tcx, plan)
                })?;
                (tcx.map(ReadOrWriteTransactionContext::Write), output)
            }
        };

        // `tx` was not committed or aborted (including autocommits), so we need to put it back
        if let Some(tcx) = tcx {
            ctx.current_tcx.store(Some(Arc::new(tcx)));
        }

        Ok(output)
    }

    fn execute_in<'env, M: ExecutionMode<'env, S>>(
        &'env self,
        ctx: &SessionContext<'env, S>,
        tcx: TransactionContext<'env, S, M>,
        stmt: &ast::Statement,
        do_plan: impl for<'txn> FnOnce(
            PhysicalPlanner<'env, 'txn, S, M>,
            &dyn nsql_execution::TransactionContext<'env, 'txn, S, M>,
            Box<ir::Plan<nsql_opt::Query>>,
        ) -> Result<PhysicalPlan<'env, 'txn, S, M>>,
    ) -> Result<(Option<TransactionContext<'env, S, M>>, MaterializedQueryOutput)> {
        let (auto_commit, state, output) = tcx.with::<Result<_>>(|tcx| {
            let catalog = Catalog::new(self.storage.storage());

            let plan = self
                .profiler
                .profile(self.profiler.bind_event_id, || Binder::new(catalog, &tcx).bind(stmt))?;

            let schema = Schema::new(plan.schema());
            let plan =
                self.profiler.profile(self.profiler.optimize_event_id, || Ok(optimize(plan)))?;

            let physical_planner = PhysicalPlanner::new(catalog);
            let physical_plan =
                self.profiler.profile(self.profiler.physical_plan_event_id, || {
                    do_plan(physical_planner, &tcx, plan)
                    // physical_planner.plan(&tcx, plan)
                })?;

            let ecx = ExecutionContext::<S, M>::new(catalog, &tcx, ctx);
            let tuples = self.profiler.profile(self.profiler.execute_event_id, || {
                nsql_execution::execute(&ecx, physical_plan)
            })?;

            let (auto_commit, state) = (
                tcx.auto_commit.load(atomic::Ordering::Acquire),
                tcx.state.load(atomic::Ordering::Acquire),
            );

            Ok((auto_commit, state, MaterializedQueryOutput::new(schema, tuples)))
        })?;

        match state {
            TransactionState::Active if auto_commit => {
                tracing::debug!("auto-committing write transaction");
                tcx.commit()?;
                Ok((None, output))
            }
            TransactionState::Committed => {
                tracing::debug!("committing write transaction");
                tcx.commit()?;
                Ok((None, output))
            }
            TransactionState::Aborted => {
                tracing::debug!("aborting write transaction");
                tcx.abort()?;
                Ok((None, output))
            }
            _ => Ok((Some(tcx), output)),
        }
    }
}

struct RequiredTransactionModeVisitor;

impl nsql_parse::ast::Visitor for RequiredTransactionModeVisitor {
    type Break = ();

    fn pre_visit_statement(&mut self, stmt: &ast::Statement) -> ControlFlow<Self::Break> {
        match stmt {
            ast::Statement::Update { .. }
            | ast::Statement::Insert { .. }
            | ast::Statement::Delete { .. }
            | ast::Statement::Truncate { .. }
            | ast::Statement::CreateView { .. }
            | ast::Statement::CreateTable { .. }
            | ast::Statement::CreateSequence { .. }
            | ast::Statement::CreateIndex { .. }
            | ast::Statement::CreateSchema { .. }
            | ast::Statement::Drop { .. }
            | ast::Statement::DropFunction { .. } => ControlFlow::Break(()),
            // RW unless explicitly read-only
            ast::Statement::StartTransaction { modes, begin: _ }
                if modes.iter().all(|&mode| {
                    mode != ast::TransactionMode::AccessMode(ast::TransactionAccessMode::ReadOnly)
                }) =>
            {
                ControlFlow::Break(())
            }
            _ => ControlFlow::Continue(()),
        }
    }

    fn pre_visit_expr(&mut self, expr: &ast::Expr) -> ControlFlow<Self::Break> {
        // This is a bit of a hack as it's far from complete
        match expr {
            ast::Expr::Function(f) => {
                match f.name.0.last().unwrap().value.to_lowercase().as_str() {
                    "nextval" => ControlFlow::Break(()),
                    _ => ControlFlow::Continue(()),
                }
            }
            _ => ControlFlow::Continue(()),
        }
    }
}
