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
use nsql_transaction::Transaction;
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

fn build_pipelines(sink: Arc<dyn PhysicalSink>, plan: PhysicalPlan) -> RootPipeline {
    let (mut builder, root_meta_pipeline) = PipelineBuilderArena::new(sink);
    builder.build(root_meta_pipeline, plan.root());
    let arena = builder.finish();
    RootPipeline { arena }
}

trait PhysicalNode: Send + Sync + fmt::Debug + Explain + Any + 'static {
    fn children(&self) -> &[Arc<dyn PhysicalNode>];

    // override the default implementation if the node is a source with `Ok(self)`, otherwise `Err(self)`
    fn as_source(self: Arc<Self>) -> Result<Arc<dyn PhysicalSource>, Arc<dyn PhysicalNode>>;

    fn as_sink(self: Arc<Self>) -> Result<Arc<dyn PhysicalSink>, Arc<dyn PhysicalNode>>;

    fn as_operator(self: Arc<Self>) -> Result<Arc<dyn PhysicalOperator>, Arc<dyn PhysicalNode>>;

    fn build_pipelines(
        self: Arc<Self>,
        arena: &mut PipelineBuilderArena,
        meta_builder: Idx<MetaPipelineBuilder>,
        current: Idx<PipelineBuilder>,
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
                arena[current].set_source(Arc::clone(&sink) as Arc<dyn PhysicalSource>);
                let child_meta_builder = arena.new_child_meta_pipeline(meta_builder, sink);
                arena.build(child_meta_builder, child);
            }
            Err(node) => match node.as_source() {
                Ok(source) => arena[current].set_source(source),
                Err(operator) => {
                    let operator = operator.as_operator().unwrap();
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

#[async_trait::async_trait]
trait PhysicalOperator<T = Tuple>: PhysicalNode {
    async fn execute(&self, ctx: &ExecutionContext, input: T) -> ExecutionResult<OperatorState<T>>;
}

#[async_trait::async_trait]
trait PhysicalSource<T = Tuple>: PhysicalNode {
    /// Return the next chunk from the source. An empty chunk indicates that the source is exhausted.
    async fn source(&self, ctx: &ExecutionContext) -> ExecutionResult<Chunk<T>>;
}

#[async_trait::async_trait]
trait PhysicalSink: PhysicalSource {
    async fn sink(&self, ctx: &ExecutionContext, tuple: Tuple) -> ExecutionResult<()>;
}

#[derive(Clone)]
pub struct ExecutionContext {
    pool: Arc<dyn Pool>,
    catalog: Arc<Catalog>,
    tx: Arc<Transaction>,
}

impl ExecutionContext {
    #[inline]
    pub fn new(pool: Arc<dyn Pool>, catalog: Arc<Catalog>, tx: Arc<Transaction>) -> Self {
        Self { pool, catalog, tx }
    }

    #[inline]
    pub fn pool(&self) -> Arc<dyn Pool> {
        Arc::clone(&self.pool)
    }

    #[inline]
    pub fn tx(&self) -> Arc<Transaction> {
        Arc::clone(&self.tx)
    }

    #[inline]
    pub fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.catalog)
    }
}

struct RootPipeline {
    arena: PipelineArena,
}
