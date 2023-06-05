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
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;

pub use anyhow::Error;
use nsql_arena::Idx;
use nsql_catalog::Catalog;
use nsql_storage::tuple::Tuple;
use nsql_storage_engine::{
    ExecutionMode, FallibleIterator, ReadWriteExecutionMode, ReadonlyExecutionMode, StorageEngine,
};
use nsql_util::atomic::AtomicEnum;
pub use physical_plan::PhysicalPlanner;
use smallvec::SmallVec;

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
struct Chunk<T = Tuple>(SmallVec<[T; 1]>);

impl<T> From<Vec<T>> for Chunk<T> {
    fn from(vec: Vec<T>) -> Self {
        Self(SmallVec::from_vec(vec))
    }
}

impl<T> Chunk<T> {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn empty() -> Self {
        Self(SmallVec::new())
    }

    pub fn singleton(tuple: T) -> Self {
        Self(SmallVec::from_buf([tuple]))
    }
}

impl<T> IntoIterator for Chunk<T> {
    type Item = T;

    type IntoIter = smallvec::IntoIter<[Self::Item; 1]>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
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
        ctx: &ExecutionContext<'env, 'txn, S, M>,
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
        ctx: &ExecutionContext<'env, 'txn, S, M>,
    ) -> ExecutionResult<TupleStream<'txn, S>>;
}

trait PhysicalSink<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>:
    PhysicalSource<'env, 'txn, S, M>
{
    fn sink(&self, ctx: &ExecutionContext<'env, 'txn, S, M>, tuple: Tuple) -> ExecutionResult<()>;
}

trait Reborrow<'short, _Outlives = &'short Self> {
    type Target;

    fn rb(&'short self) -> &'short Self::Target;
}

impl<'short, 'a, T> Reborrow<'short> for &'a T {
    type Target = T;

    #[inline]
    fn rb(&'short self) -> &'short Self::Target {
        self
    }
}

impl<'short, 'a, T> Reborrow<'short> for &'a mut T {
    type Target = T;

    #[inline]
    fn rb(&'short self) -> &'short Self::Target {
        self
    }
}

pub struct TransactionContext<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> {
    // probably can just take a reference to the transaction
    pd: PhantomData<&'txn ()>,
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

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    TransactionContext<'env, 'txn, S, M>
{
    #[inline]
    pub fn new(tx: M::Transaction, auto_commit: bool) -> Self {
        Self {
            pd: PhantomData,
            tx,
            auto_commit: AtomicBool::new(auto_commit),
            state: AtomicEnum::new(TransactionState::Active),
        }
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

impl<'env, 'txn, S, M> Deref for TransactionContext<'env, 'txn, S, M>
where
    S: StorageEngine,
    M: ExecutionMode<'env, S>,
{
    type Target = M::Transaction;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl<'env, 'txn, S, M> DerefMut for TransactionContext<'env, 'txn, S, M>
where
    S: StorageEngine,
    M: ExecutionMode<'env, S>,
{
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

pub struct ExecutionContext<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> {
    storage: S,
    catalog: Arc<Catalog<S>>,
    tx: TransactionContext<'env, 'txn, S, M>,
    pd: PhantomData<&'txn ()>,
}

impl<'env: 'txn, 'txn, S: StorageEngine> ExecutionContext<'env, 'txn, S, ReadonlyExecutionMode<S>> {
    #[inline]
    pub fn take_txn(self) -> (bool, TransactionState, S::Transaction<'env>) {
        let tx = self.tx;
        (tx.auto_commit.into_inner(), tx.state.into_inner(), tx.tx)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine>
    ExecutionContext<'env, 'txn, S, ReadWriteExecutionMode<S>>
{
    #[inline]
    pub fn take_txn(self) -> (bool, TransactionState, S::WriteTransaction<'env>) {
        let tx = self.tx;
        (tx.auto_commit.into_inner(), tx.state.into_inner(), tx.tx)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    ExecutionContext<'env, 'txn, S, M>
{
    #[inline]
    pub fn new(
        storage: S,
        catalog: Arc<Catalog<S>>,
        tx: TransactionContext<'env, 'txn, S, M>,
    ) -> Self {
        Self { storage, catalog, tx, pd: PhantomData }
    }

    #[inline]
    pub fn storage(&self) -> S {
        self.storage.clone()
    }

    #[inline]
    pub fn tx(&self) -> &TransactionContext<'env, 'txn, S, M> {
        &self.tx
    }

    #[inline]
    pub fn tx_mut(&mut self) -> &mut TransactionContext<'env, 'txn, S, M> {
        &mut self.tx
    }

    #[inline]
    pub fn catalog(&self) -> Arc<Catalog<S>> {
        Arc::clone(&self.catalog)
    }
}

struct RootPipeline<'env, 'txn, S, M> {
    arena: PipelineArena<'env, 'txn, S, M>,
}
