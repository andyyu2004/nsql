#![feature(anonymous_lifetime_in_impl_trait)]
#![deny(rust_2018_idioms)]
#![feature(trait_upcasting)]
#![feature(once_cell_try)]

mod eval;
mod executor;
mod physical_plan;
mod pipeline;
mod vis;

use std::fmt;
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;

pub use anyhow::Error;
use nsql_arena::Idx;
use nsql_catalog::Catalog;
use nsql_storage::tuple::Tuple;
use nsql_storage_engine::{
    ExecutionMode, FallibleIterator, ReadWriteExecutionMode, ReadonlyExecutionMode, StorageEngine,
    Transaction,
};
use nsql_util::atomic::AtomicEnum;
pub use physical_plan::PhysicalPlanner;

use self::eval::Evaluator;
pub use self::executor::{execute, execute_write};
use self::physical_plan::{explain, Explain, PhysicalPlan};
use self::pipeline::{
    MetaPipeline, MetaPipelineBuilder, Pipeline, PipelineArena, PipelineBuilder,
    PipelineBuilderArena,
};

pub type ExecutionResult<T, E = Error> = std::result::Result<T, E>;

fn build_pipelines<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
    sink: Arc<dyn PhysicalSink<'env, 'txn, S, M>>,
    plan: PhysicalPlan<'env, 'txn, S, M>,
) -> RootPipeline<'env, 'txn, S, M> {
    let (mut builder, root_meta_pipeline) = PipelineBuilderArena::new(sink);
    builder.build(root_meta_pipeline, plan.root());
    let arena = builder.finish();
    RootPipeline { arena }
}

#[allow(clippy::type_complexity)]
trait PhysicalNode<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>:
    Send + Sync + Explain<S> + 'txn
{
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, 'txn, S, M>>];

    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>;

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>;

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>;

    fn build_pipelines(
        self: Arc<Self>,
        arena: &mut PipelineBuilderArena<'env, 'txn, S, M>,
        meta_builder: Idx<MetaPipelineBuilder<'env, 'txn, S, M>>,
        current: Idx<PipelineBuilder<'env, 'txn, S, M>>,
    ) {
        match self.as_sink() {
            Ok(sink) => {
                assert_eq!(
                    sink.children().len(),
                    1,
                    "default `build_pipelines` implementation only supports unary nodes for sinks"
                );
                let child = Arc::clone(&sink.children()[0]);
                // If we have a sink node (which is also a source),
                // - set it to be the source of the current pipeline,
                // - recursively build the pipeline for its child with `sink` as the sink of the new metapipeline
                arena[current]
                    .set_source(Arc::clone(&sink) as Arc<dyn PhysicalSource<'env, 'txn, S, M>>);
                let child_meta_builder = arena.new_child_meta_pipeline(meta_builder, sink);
                arena.build(child_meta_builder, child);
            }
            Err(node) => match node.as_source() {
                Ok(source) => arena[current].set_source(source),
                Err(operator) => {
                    // Safety: we checked the other two cases above, must be an operator
                    let operator = unsafe { operator.as_operator().unwrap_unchecked() };
                    let children = operator.children();
                    assert_eq!(
                        children.len(),
                        1,
                        "default `build_pipelines` implementation only supports unary operators"
                    );
                    let child = Arc::clone(&children[0]);
                    arena[current].add_operator(operator);
                    arena.build(meta_builder, child);
                }
            },
        }
    }
}

#[derive(Debug)]
enum OperatorState<T> {
    /// The operator has an output is ready to process the next input
    Yield(T),
    /// The operator produced no output for the given input and is ready to process the next input
    Continue,
    /// The operator is done processing input tuples and will never produce more output
    Done,
}

trait PhysicalOperator<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T = Tuple>:
    PhysicalNode<'env, 'txn, S, M>
{
    fn execute(
        &self,
        ctx: &'txn ExecutionContext<'env, S, M>,
        input: T,
    ) -> ExecutionResult<OperatorState<T>>;
}

type TupleStream<'txn, S> =
    Box<dyn FallibleIterator<Item = Tuple, Error = <S as StorageEngine>::Error> + 'txn>;

trait PhysicalSource<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T = Tuple>:
    PhysicalNode<'env, 'txn, S, M>
{
    /// Return the next chunk from the source. An empty chunk indicates that the source is exhausted.
    fn source(
        self: Arc<Self>,
        ctx: &'txn ExecutionContext<'env, S, M>,
    ) -> ExecutionResult<TupleStream<'txn, S>>;
}

trait PhysicalSink<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>:
    PhysicalSource<'env, 'txn, S, M>
{
    fn sink(&self, ctx: &'txn ExecutionContext<'env, S, M>, tuple: Tuple) -> ExecutionResult<()>;

    fn finalize(&self, _ctx: &'txn ExecutionContext<'env, S, M>) -> ExecutionResult<()> {
        Ok(())
    }
}

pub struct TransactionContext<'env, S: StorageEngine, M: ExecutionMode<'env, S>> {
    tx: M::Transaction,
    auto_commit: AtomicBool,
    state: AtomicEnum<TransactionState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TransactionState {
    Active,
    Committed,
    Aborted,
}

impl From<TransactionState> for u8 {
    #[inline]
    fn from(state: TransactionState) -> Self {
        state as u8
    }
}

impl From<u8> for TransactionState {
    #[inline]
    fn from(state: u8) -> Self {
        assert!(state <= TransactionState::Aborted as u8);
        unsafe { std::mem::transmute(state) }
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> TransactionContext<'env, S, M> {
    #[inline]
    pub fn new(tx: M::Transaction, auto_commit: bool) -> Self {
        let auto_commit = AtomicBool::new(auto_commit);
        Self { tx, auto_commit, state: AtomicEnum::new(TransactionState::Active) }
    }

    #[inline]
    pub fn auto_commit(&self) -> bool {
        self.auto_commit.load(atomic::Ordering::Acquire)
    }

    #[inline]
    pub fn commit(&self) {
        self.state.store(TransactionState::Committed, atomic::Ordering::Release);
    }

    #[inline]
    pub fn abort(&self) {
        self.state.store(TransactionState::Aborted, atomic::Ordering::Release);
    }

    #[inline]
    pub fn unset_auto_commit(&self) {
        self.auto_commit.store(false, atomic::Ordering::Release)
    }
}

pub struct ExecutionContext<'env, S: StorageEngine, M: ExecutionMode<'env, S>> {
    storage: &'env S,
    catalog: Arc<Catalog<S>>,
    tcx: TransactionContext<'env, S, M>,
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> ExecutionContext<'env, S, M> {
    #[inline]
    pub fn take_txn(self) -> (bool, TransactionState, M::Transaction) {
        let tx = self.tcx;
        (tx.auto_commit.into_inner(), tx.state.into_inner(), tx.tx)
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> ExecutionContext<'env, S, M> {
    #[inline]
    pub fn new(
        storage: &'env S,
        catalog: Arc<Catalog<S>>,
        tcx: TransactionContext<'env, S, M>,
    ) -> Self {
        Self { storage, catalog, tcx }
    }

    #[inline]
    pub fn storage(&self) -> &S {
        self.storage
    }

    #[inline]
    pub fn tcx(&self) -> &TransactionContext<'env, S, M> {
        &self.tcx
    }

    #[inline]
    pub fn tx(&self) -> Result<&M::Transaction, S::Error> {
        Ok(&self.tcx.tx)
    }

    #[inline]
    pub fn tx_mut(&mut self) -> &mut TransactionContext<'env, S, M> {
        &mut self.tcx
    }

    #[inline]
    pub fn catalog(&self) -> Arc<Catalog<S>> {
        Arc::clone(&self.catalog)
    }
}

struct RootPipeline<'env, 'txn, S, M> {
    arena: PipelineArena<'env, 'txn, S, M>,
}
