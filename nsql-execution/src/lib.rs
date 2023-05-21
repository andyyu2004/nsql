#![feature(anonymous_lifetime_in_impl_trait)]
#![deny(rust_2018_idioms)]
#![feature(trait_upcasting)]
#![feature(once_cell_try)]

mod eval;
mod executor;
mod physical_plan;
mod pipeline;
mod vis;

use std::any::Any;
use std::fmt;
use std::sync::Arc;

pub use anyhow::Error;
use nsql_arena::Idx;
use nsql_catalog::Catalog;
use nsql_storage::tuple::Tuple;
use nsql_storage_engine::{StorageEngine, Transaction};
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

fn build_pipelines<S: StorageEngine, M: ExecutionMode<S>>(
    sink: Arc<dyn PhysicalSink<S, M>>,
    plan: PhysicalPlan<S, M>,
) -> RootPipeline<S, M> {
    let (mut builder, root_meta_pipeline) = PipelineBuilderArena::new(sink);
    builder.build(root_meta_pipeline, plan.root());
    let arena = builder.finish();
    RootPipeline { arena }
}

trait PhysicalNode<S: StorageEngine, M: ExecutionMode<S>>:
    Send + Sync + Explain<S> + Any + 'static
{
    fn children(&self) -> &[Arc<dyn PhysicalNode<S, M>>];

    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<S, M>>, Arc<dyn PhysicalNode<S, M>>>;

    fn as_sink(self: Arc<Self>)
    -> Result<Arc<dyn PhysicalSink<S, M>>, Arc<dyn PhysicalNode<S, M>>>;

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<S, M>>, Arc<dyn PhysicalNode<S, M>>>;

    fn build_pipelines(
        self: Arc<Self>,
        arena: &mut PipelineBuilderArena<S, M>,
        meta_builder: Idx<MetaPipelineBuilder<S, M>>,
        current: Idx<PipelineBuilder<S, M>>,
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
                arena[current].set_source(Arc::clone(&sink) as Arc<dyn PhysicalSource<S, M>>);
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

#[derive(Debug)]
enum SourceState<T> {
    Yield(T),
    Final(T),
    Done,
}

#[async_trait::async_trait]
trait PhysicalOperator<S: StorageEngine, M: ExecutionMode<S>, T = Tuple>: PhysicalNode<S, M> {
    fn execute(
        &self,
        ctx: &ExecutionContext<'_, '_, S, M>,
        input: T,
    ) -> ExecutionResult<OperatorState<T>>;
}

#[async_trait::async_trait]
trait PhysicalSource<S: StorageEngine, M: ExecutionMode<S>, T = Tuple>: PhysicalNode<S, M> {
    /// Return the next chunk from the source. An empty chunk indicates that the source is exhausted.
    fn source(
        &self,
        ctx: &ExecutionContext<'_, '_, S, M>,
    ) -> ExecutionResult<SourceState<Chunk<T>>>;
}

#[async_trait::async_trait]
trait PhysicalSink<'env, S: StorageEngine, M: ExecutionMode<'env, S>>: PhysicalSource<S, M> {
    fn sink(&self, ctx: &ExecutionContext<'_, '_, S, M>, tuple: Tuple) -> ExecutionResult<()>;
}

pub trait ExecutionMode<'env, S: StorageEngine>: private::Sealed + Clone + Copy + 'static {
    type Transaction: Transaction<'env, S>;

    type Ref<'a, T>
    where
        T: 'a,
        'env: 'a;
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

impl<S: StorageEngine> ExecutionMode<S> for ReadonlyExecutionMode<S> {
    type Transaction<'env> = S::Transaction<'env>;

    type Ref<'a, T> = &'a T where T: 'a;
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

impl<S: StorageEngine> ExecutionMode<S> for ReadWriteExecutionMode<S> {
    type Transaction<'env> = S::WriteTransaction<'env>;

    type Ref<'a, T> = &'a mut T where T: 'a;
}

pub struct ExecutionContext<'a, 'env, S: StorageEngine, M: ExecutionMode<S>> {
    storage: S,
    catalog: Arc<Catalog<S>>,
    tx: M::Ref<'a>,
}

impl<'a, 'env, S: StorageEngine, M: ExecutionMode<S>> ExecutionContext<'a, 'env, S, M> {
    #[inline]
    pub fn new(storage: S, catalog: Arc<Catalog<S>>, tx: M::Ref<'a>) -> Self {
        Self { storage, catalog, tx }
    }

    #[inline]
    pub fn storage(&self) -> S {
        self.storage.clone()
    }

    #[inline]
    pub fn tx(&self) -> &M::Ref<'a> {
        &self.tx
    }

    #[inline]
    pub fn tx_mut(&mut self) -> &mut M::Ref<'a> {
        &mut self.tx
    }

    #[inline]
    pub fn catalog(&self) -> Arc<Catalog<S>> {
        Arc::clone(&self.catalog)
    }
}

struct RootPipeline<S, M> {
    arena: PipelineArena<S, M>,
}
