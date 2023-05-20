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
use nsql_buffer::Pool;
use nsql_catalog::Catalog;
use nsql_storage::tuple::Tuple;
use nsql_storage::Transaction;
use nsql_storage_engine::StorageEngine;
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

fn build_pipelines<S>(sink: Arc<dyn PhysicalSink<S>>, plan: PhysicalPlan<S>) -> RootPipeline<S> {
    let (mut builder, root_meta_pipeline) = PipelineBuilderArena::new(sink);
    builder.build(root_meta_pipeline, plan.root());
    let arena = builder.finish();
    RootPipeline { arena }
}

trait PhysicalNode<S: StorageEngine>: Send + Sync + Explain<S> + Any + 'static {
    fn children(&self) -> &[Arc<dyn PhysicalNode<S>>];

    fn as_source(self: Arc<Self>) -> Result<Arc<dyn PhysicalSource<S>>, Arc<dyn PhysicalNode<S>>>;

    fn as_sink(self: Arc<Self>) -> Result<Arc<dyn PhysicalSink<S>>, Arc<dyn PhysicalNode<S>>>;

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<S>>, Arc<dyn PhysicalNode<S>>>;

    fn build_pipelines(
        self: Arc<Self>,
        arena: &mut PipelineBuilderArena<S>,
        meta_builder: Idx<MetaPipelineBuilder<S>>,
        current: Idx<PipelineBuilder<S>>,
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
                arena[current].set_source(Arc::clone(&sink) as Arc<dyn PhysicalSource<S>>);
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
trait PhysicalOperator<S: StorageEngine, T = Tuple>: PhysicalNode<S> {
    async fn execute(
        &self,
        ctx: &ExecutionContext<'_, S>,
        input: T,
    ) -> ExecutionResult<OperatorState<T>>;
}

#[async_trait::async_trait]
trait PhysicalSource<S: StorageEngine, T = Tuple>: PhysicalNode<S> {
    /// Return the next chunk from the source. An empty chunk indicates that the source is exhausted.
    async fn source(&self, ctx: &ExecutionContext<'_, S>)
    -> ExecutionResult<SourceState<Chunk<T>>>;
}

#[async_trait::async_trait]
trait PhysicalSink<S: StorageEngine>: PhysicalSource<S> {
    async fn sink(&self, ctx: &ExecutionContext<'_, S>, tuple: Tuple) -> ExecutionResult<()>;
}

#[derive(Clone)]
pub struct ExecutionContext<'env, S: StorageEngine> {
    pool: Arc<dyn Pool>,
    storage: S,
    catalog: Arc<Catalog<S>>,
    tx: S::Transaction<'env>,
}

impl<'env, S: StorageEngine> ExecutionContext<'env, S> {
    #[inline]
    pub fn new(
        storage: S,
        pool: Arc<dyn Pool>,
        catalog: Arc<Catalog<S>>,
        tx: S::Transaction<'env>,
    ) -> Self {
        Self { storage, pool, catalog, tx }
    }

    #[inline]
    pub fn storage(&self) -> S {
        self.storage.clone()
    }

    #[inline]
    pub fn tx(&self) -> &S::Transaction<'_> {
        &self.tx
    }

    #[inline]
    pub fn catalog(&self) -> Arc<Catalog<S>> {
        Arc::clone(&self.catalog)
    }
}

struct RootPipeline<S> {
    arena: PipelineArena<S>,
}
