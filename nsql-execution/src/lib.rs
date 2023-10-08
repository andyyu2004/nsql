#![deny(rust_2018_idioms)]
#![feature(trait_upcasting, once_cell_try, anonymous_lifetime_in_impl_trait, exact_size_is_empty)]

mod compile;
pub mod config;
mod executor;
mod physical_plan;
mod pipeline;

use std::fmt;
use std::ops::Index;
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;

pub use anyhow::Error;
use dashmap::DashMap;
use nsql_arena::{Arena, Idx};
use nsql_catalog::Catalog;
use nsql_core::Name;
use nsql_storage::tuple::Tuple;
use nsql_storage_engine::{
    ExecutionMode, FallibleIterator, ReadWriteExecutionMode, ReadonlyExecutionMode, StorageEngine,
    Transaction, TransactionConversionHack,
};
use nsql_util::atomic::AtomicEnum;
pub use physical_plan::PhysicalPlanner;

use self::config::SessionConfig;
pub use self::executor::{execute, execute_write};
use self::physical_plan::{explain, Explain, PhysicalPlan};
use self::pipeline::{
    MetaPipeline, MetaPipelineBuilder, Pipeline, PipelineArena, PipelineBuilder,
    PipelineBuilderArena,
};

pub type ExecutionResult<T, E = Error> = std::result::Result<T, E>;

fn build_pipelines<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
    sink: Arc<dyn PhysicalSink<'env, 'txn, S, M>>,
    plan: &PhysicalPlan<'env, 'txn, S, M>,
) -> RootPipeline<'env, 'txn, S, M> {
    let (mut builder, root_meta_pipeline) = PipelineBuilderArena::new(sink);
    builder.build(plan.arena(), root_meta_pipeline, plan.root());
    let arena = builder.finish();
    RootPipeline { arena }
}

struct PhysicalNodeArena<'env, 'txn, S, M> {
    // FIXME change to box dyn once first pass is done
    nodes: Arena<Arc<dyn PhysicalNode<'env, 'txn, S, M>>>,
}

impl<'env, 'txn, S, M> Clone for PhysicalNodeArena<'env, 'txn, S, M> {
    fn clone(&self) -> Self {
        Self { nodes: self.nodes.clone() }
    }
}

impl<'env, 'txn, S, M> Index<PhysicalNodeId<'env, 'txn, S, M>>
    for PhysicalNodeArena<'env, 'txn, S, M>
{
    type Output = Arc<dyn PhysicalNode<'env, 'txn, S, M>>;

    #[inline]
    fn index(&self, index: PhysicalNodeId<'env, 'txn, S, M>) -> &Self::Output {
        &self.nodes[index]
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNodeArena<'env, 'txn, S, M> {
    pub fn alloc(
        &mut self,
        node: Arc<dyn PhysicalNode<'env, 'txn, S, M>>,
    ) -> PhysicalNodeId<'env, 'txn, S, M> {
        self.nodes.alloc(node)
    }
}

type PhysicalNodeId<'env, 'txn, S, M> = Idx<Arc<dyn PhysicalNode<'env, 'txn, S, M>>>;

// keep this trait crate-private
#[allow(clippy::type_complexity)]
trait PhysicalNode<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>:
    Explain<'env, S> + fmt::Debug + 'txn
{
    /// The width of the tuples produced by this node
    fn width(&self, nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize;

    fn children(&self) -> &[PhysicalNodeId<'env, 'txn, S, M>];

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
        nodes: &PhysicalNodeArena<'env, 'txn, S, M>,
        arena: &mut PipelineBuilderArena<'env, 'txn, S, M>,
        meta_builder: Idx<MetaPipelineBuilder<'env, 'txn, S, M>>,
        current: Idx<PipelineBuilder<'env, 'txn, S, M>>,
    ) {
        match self.as_sink() {
            Ok(sink) => {
                // If we have a sink node (which is also a source),
                // - set it to be the source of the current pipeline,
                // - recursively build the pipeline for its child with `sink` as the sink of the new metapipeline
                arena[current]
                    .set_source(Arc::clone(&sink) as Arc<dyn PhysicalSource<'env, 'txn, S, M>>);
                assert!(
                    sink.children().len() <= 1,
                    "default `build_pipelines` implementation only supports unary or nullary nodes for sinks"
                );

                if !sink.children().is_empty() {
                    let child = sink.children()[0];
                    let child_meta_builder = arena.new_child_meta_pipeline(meta_builder, sink);
                    arena.build(nodes, child_meta_builder, child);
                }
            }
            Err(node) => match node.as_source() {
                Ok(source) => {
                    assert!(
                        source.children().is_empty(),
                        "default `build_pipelines` implementation only supports sources at the leaf"
                    );
                    arena[current].set_source(source)
                }
                Err(operator) => {
                    let operator =
                        operator.as_operator().expect("node is not a source, sink, or operator");
                    let children = operator.children();
                    assert_eq!(
                        children.len(),
                        1,
                        "default `build_pipelines` implementation only supports unary operators"
                    );
                    let child = operator.children()[0];
                    arena[current].add_operator(operator);
                    nodes[child].clone().build_pipelines(nodes, arena, meta_builder, current)
                }
            },
        }
    }
}

#[derive(Debug)]
enum OperatorState<T> {
    /// The operator potentially has more output for the same input
    Again(Option<T>),
    /// The operator has an output and is ready to process the next input
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
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        input: T,
    ) -> ExecutionResult<OperatorState<T>>;
}

type TupleStream<'txn> = Box<dyn FallibleIterator<Item = Tuple, Error = anyhow::Error> + 'txn>;

trait PhysicalSource<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T = Tuple>:
    PhysicalNode<'env, 'txn, S, M>
{
    /// Return the next chunk from the source. An empty chunk indicates that the source is exhausted.
    fn source(
        self: Arc<Self>,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'txn>>;
}

trait PhysicalSink<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>:
    PhysicalSource<'env, 'txn, S, M>
{
    fn sink(
        &self,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        tuple: Tuple,
    ) -> ExecutionResult<()>;

    fn finalize(&self, _ecx: &'txn ExecutionContext<'_, 'env, S, M>) -> ExecutionResult<()> {
        Ok(())
    }
}

pub trait SessionContext {
    fn config(&self) -> &SessionConfig;
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

pub struct ExecutionContext<'a, 'env, S: StorageEngine, M: ExecutionMode<'env, S>> {
    catalog: Catalog<'env, S>,
    tcx: TransactionContext<'env, S, M>,
    scx: &'a (dyn SessionContext + 'a),
    materialized_ctes: DashMap<Name, Arc<[Tuple]>>,
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> ExecutionContext<'_, 'env, S, M> {
    #[inline]
    pub fn take_txn(self) -> (bool, TransactionState, M::Transaction) {
        let tx = self.tcx;
        (tx.auto_commit.into_inner(), tx.state.into_inner(), tx.tx)
    }
}

impl<'a, 'env, S: StorageEngine, M: ExecutionMode<'env, S>> ExecutionContext<'a, 'env, S, M> {
    #[inline]
    pub fn new(
        catalog: Catalog<'env, S>,
        tcx: TransactionContext<'env, S, M>,
        scx: &'a (dyn SessionContext + 'a),
    ) -> Self {
        Self { catalog, tcx, scx, materialized_ctes: Default::default() }
    }

    #[inline]
    pub fn scx(&self) -> &(dyn SessionContext + '_) {
        self.scx
    }

    #[inline]
    pub fn tcx(&self) -> &TransactionContext<'env, S, M> {
        &self.tcx
    }

    #[inline]
    pub fn tx(&self) -> M::TransactionRef<'_> {
        TransactionConversionHack::as_tx_ref(&self.tcx.tx)
    }

    #[inline]
    pub fn catalog(&self) -> Catalog<'env, S> {
        self.catalog
    }

    pub fn get_materialized_cte_data(&self, name: &Name) -> Arc<[Tuple]> {
        self.materialized_ctes
            .get(name)
            .map(|tuples| Arc::clone(tuples.value()))
            .expect("attempting to get materialized cte data before it is materialized")
    }

    pub fn instantiate_materialized_cte(&self, name: Name, tuples: impl Into<Arc<[Tuple]>>) {
        assert!(
            self.materialized_ctes.insert(name, tuples.into()).is_none(),
            "cte was already materialized"
        );
    }

    #[inline]
    pub fn storage(&self) -> &'env S {
        self.catalog.storage()
    }
}

struct RootPipeline<'env, 'txn, S, M> {
    arena: PipelineArena<'env, 'txn, S, M>,
}
