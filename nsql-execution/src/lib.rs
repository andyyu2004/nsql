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
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;

pub use anyhow::Error;
use nsql_arena::Idx;
use nsql_catalog::Catalog;
use nsql_storage::tuple::Tuple;
use nsql_storage_engine::{FallibleIterator, StorageEngine, WriteTransaction};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
pub use physical_plan::PhysicalPlanner;
use smallvec::SmallVec;

use self::eval::Evaluator;
pub use self::executor::execute;
use self::physical_plan::{explain, Explain, PhysicalPlan};
use self::pipeline::{
    MetaPipeline, MetaPipelineBuilder, Pipeline, PipelineArena, PipelineBuilder,
    PipelineBuilderArena,
};

pub type ExecutionResult<T, E = Error> = std::result::Result<T, E>;

fn build_pipelines<'env, S: StorageEngine, M: ExecutionMode<'env, S>>(
    sink: Arc<dyn PhysicalSink<'env, S, M>>,
    plan: PhysicalPlan<'env, S, M>,
) -> RootPipeline<'env, S, M> {
    let (mut builder, root_meta_pipeline) = PipelineBuilderArena::new(sink);
    builder.build(root_meta_pipeline, plan.root());
    let arena = builder.finish();
    RootPipeline { arena }
}

trait PhysicalNode<'env, S: StorageEngine, M: ExecutionMode<'env, S>>:
    Send + Sync + Explain<S> + 'env
{
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, S, M>>];

    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>>;

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>>;

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>>;

    fn build_pipelines(
        self: Arc<Self>,
        arena: &mut PipelineBuilderArena<'env, S, M>,
        meta_builder: Idx<MetaPipelineBuilder<'env, S, M>>,
        current: Idx<PipelineBuilder<'env, S, M>>,
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
                arena[current].set_source(Arc::clone(&sink) as Arc<dyn PhysicalSource<'env, S, M>>);
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

trait PhysicalOperator<'env, S: StorageEngine, M: ExecutionMode<'env, S>, T = Tuple>:
    PhysicalNode<'env, S, M>
{
    fn execute(
        &self,
        ctx: &ExecutionContext<'env, S, M>,
        input: T,
    ) -> ExecutionResult<OperatorState<T>>;
}

type TupleStream<S> =
    Box<dyn FallibleIterator<Item = Tuple, Error = <S as StorageEngine>::Error>>;

trait PhysicalSource<'env, S: StorageEngine, M: ExecutionMode<'env, S>, T = Tuple>:
    PhysicalNode<'env, S, M>
{
    /// Return the next chunk from the source. An empty chunk indicates that the source is exhausted.
    fn source(
        self: Arc<Self>,
        ctx: &ExecutionContext<'env, S, M>,
    ) -> ExecutionResult<TupleStream<S>>;
}

trait PhysicalSink<'env, S: StorageEngine, M: ExecutionMode<'env, S>>:
    PhysicalSource<'env, S, M>
{
    fn sink(&self, ctx: &ExecutionContext<'env, S, M>, tuple: Tuple) -> ExecutionResult<()>;
}

pub trait ExecutionMode<'env, S: StorageEngine>: private::Sealed + Clone + Copy + 'env {
    type Transaction: nsql_storage_engine::Transaction<'env, S>;

    fn commit(transaction: Self::Transaction) -> ExecutionResult<()>;

    fn abort(transaction: Self::Transaction) -> ExecutionResult<()>;
}

mod private {
    pub trait Sealed {}
}

pub struct ReadonlyExecutionMode<S>(std::marker::PhantomData<S>);

impl<S> Clone for ReadonlyExecutionMode<S> {
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0)
    }
}

impl<S> private::Sealed for ReadonlyExecutionMode<S> {}

impl<S> Copy for ReadonlyExecutionMode<S> {}

impl<'env, S: StorageEngine> ExecutionMode<'env, S> for ReadonlyExecutionMode<S> {
    type Transaction = S::Transaction<'env>;

    #[inline]
    fn commit(transaction: Self::Transaction) -> ExecutionResult<()> {
        Ok(())
    }

    #[inline]
    fn abort(transaction: Self::Transaction) -> ExecutionResult<()> {
        Ok(())
    }
}

pub struct ReadWriteExecutionMode<S>(std::marker::PhantomData<S>);

impl<S> Clone for ReadWriteExecutionMode<S> {
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0)
    }
}

impl<S> Copy for ReadWriteExecutionMode<S> {}

impl<S> private::Sealed for ReadWriteExecutionMode<S> {}

impl<'env, S: StorageEngine> ExecutionMode<'env, S> for ReadWriteExecutionMode<S> {
    type Transaction = S::WriteTransaction<'env>;

    #[inline]
    fn commit(transaction: Self::Transaction) -> ExecutionResult<()> {
        Ok(transaction.commit()?)
    }

    #[inline]
    fn abort(transaction: Self::Transaction) -> ExecutionResult<()> {
        Ok(transaction.abort()?)
    }
}

pub struct TransactionContext<'env, S: StorageEngine, M: ExecutionMode<'env, S>> {
    // This option is consumed on commit/abort
    tx: Option<M::Transaction>,
    auto_commit: AtomicBool,
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> TransactionContext<'env, S, M> {
    #[inline]
    pub fn auto_commit(&self) -> bool {
        self.auto_commit.load(atomic::Ordering::Acquire)
    }

    #[inline]
    pub fn commit(&mut self) -> ExecutionResult<()> {
        M::commit(self.tx.take().unwrap())
    }

    #[inline]
    pub fn abort(&mut self) -> ExecutionResult<()> {
        M::abort(self.tx.take().unwrap())
    }

    #[inline]
    pub fn unset_auto_commit(&self) {
        self.auto_commit.store(false, atomic::Ordering::Release)
    }
}

impl<'env, S, M> Deref for TransactionContext<'env, S, M>
where
    S: StorageEngine,
    M: ExecutionMode<'env, S>,
{
    type Target = M::Transaction;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.tx.as_ref().unwrap()
    }
}

impl<'env, S, M> DerefMut for TransactionContext<'env, S, M>
where
    S: StorageEngine,
    M: ExecutionMode<'env, S>,
{
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.tx.as_mut().unwrap()
    }
}

pub struct ExecutionContext<'env, S: StorageEngine, M: ExecutionMode<'env, S>> {
    storage: S,
    catalog: Arc<Catalog<S>>,
    tx: RwLock<TransactionContext<'env, S, M>>,
}

impl<'env, S: StorageEngine> ExecutionContext<'env, S, ReadonlyExecutionMode<S>> {
    #[inline]
    pub fn take_txn(self) -> (bool, Option<S::Transaction<'env>>) {
        let tx = self.tx.into_inner();
        (tx.auto_commit.into_inner(), tx.tx)
    }
}

impl<'env, S: StorageEngine> ExecutionContext<'env, S, ReadWriteExecutionMode<S>> {
    #[inline]
    pub fn take_txn(self) -> (bool, Option<S::WriteTransaction<'env>>) {
        let tx = self.tx.into_inner();
        (tx.auto_commit.into_inner(), tx.tx)
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> ExecutionContext<'env, S, M> {
    #[inline]
    pub fn new(storage: S, catalog: Arc<Catalog<S>>, tx: M::Transaction) -> Self {
        Self {
            storage,
            catalog,
            tx: RwLock::new(TransactionContext {
                tx: Some(tx),
                auto_commit: AtomicBool::new(true),
            }),
        }
    }

    #[inline]
    pub fn storage(&self) -> S {
        self.storage.clone()
    }

    #[inline]
    pub fn tx(&self) -> RwLockReadGuard<'_, TransactionContext<'env, S, M>> {
        self.tx.read()
    }

    #[inline]
    pub fn tx_mut(&self) -> RwLockWriteGuard<'_, TransactionContext<'env, S, M>> {
        self.tx.write()
    }

    #[inline]
    pub fn catalog(&self) -> Arc<Catalog<S>> {
        Arc::clone(&self.catalog)
    }
}

struct RootPipeline<'env, S, M> {
    arena: PipelineArena<'env, S, M>,
}
