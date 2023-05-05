#![deny(rust_2018_idioms)]
#![feature(trait_upcasting)]
#![feature(once_cell_try)]

mod eval;
mod executor;
mod physical_plan;
mod pipeline;
mod vis;

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
use self::physical_plan::PhysicalPlan;
use self::pipeline::{
    MetaPipeline, MetaPipelineBuilder, PipelineArena, PipelineBuilder, PipelineBuilderArena,
};

pub type ExecutionResult<T, E = Error> = std::result::Result<T, E>;

fn build_pipelines(sink: Arc<dyn PhysicalSink>, plan: PhysicalPlan) -> RootPipeline {
    let mut arena = PipelineBuilderArena::default();
    let root = MetaPipelineBuilder::new(&mut arena, sink);
    arena.build(root, plan.root());
    let arena = arena.finish();
    RootPipeline { arena, root: root.cast() }
}

trait PhysicalNode: Send + Sync + fmt::Debug + 'static {
    fn desc(&self) -> &'static str;

    fn children(&self) -> &[Arc<dyn PhysicalNode>];

    // override the default implementation if the node is a source with `Ok(self)`, otherwise `Err(self)`
    fn as_source(self: Arc<Self>) -> Result<Arc<dyn PhysicalSource>, Arc<dyn PhysicalNode>>;

    fn as_sink(self: Arc<Self>) -> Result<Arc<dyn PhysicalSink>, Arc<dyn PhysicalNode>>;

    fn as_operator(self: Arc<Self>) -> Result<Arc<dyn PhysicalOperator>, Arc<dyn PhysicalNode>>;

    fn build_pipelines(
        self: Arc<Self>,
        arena: &mut PipelineBuilderArena,
        current: Idx<PipelineBuilder>,
        meta_builder: Idx<MetaPipelineBuilder>,
    ) {
        match self.as_sink() {
            Ok(sink) => {
                assert_eq!(
                    sink.children().len(),
                    1,
                    "default `build_pipelines` implementation only supports unary nodes for sinks"
                );
                let child = Arc::clone(&sink.children()[0]);
                // If we have a sink `op` (which is also a source), we set the source of current to `sink`
                // and then build the pipeline for `sink`'s child with `sink` as the sink of the new pipeline
                arena[current].set_source(Arc::clone(&sink) as Arc<dyn PhysicalSource>);
                let child_meta_builder = arena.new_child_meta_pipeline(meta_builder, current, sink);
                arena.build(child_meta_builder, child);
            }
            Err(node) => match node.as_source() {
                Ok(source) => arena[current].set_source(source),
                Err(node) => {
                    let operator = node.as_operator().unwrap();
                    let children = operator.children();
                    assert_eq!(
                        children.len(),
                        1,
                        "default `build_pipelines` implementation only supports unary operators"
                    );
                    let child = Arc::clone(&children[0]);
                    arena[current].add_operator(operator);
                    child.build_pipelines(arena, current, meta_builder);
                }
            },
        }
    }
}

#[derive(Debug)]
struct Chunk(SmallVec<[Tuple; 1]>);

impl From<Vec<Tuple>> for Chunk {
    fn from(vec: Vec<Tuple>) -> Self {
        Self(SmallVec::from_vec(vec))
    }
}

impl Chunk {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn empty() -> Self {
        Self(SmallVec::new())
    }

    pub fn singleton(tuple: Tuple) -> Self {
        Self(SmallVec::from_buf([tuple]))
    }
}

impl IntoIterator for Chunk {
    type Item = Tuple;

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
trait PhysicalOperator: PhysicalNode {
    async fn execute(
        &self,
        ctx: &ExecutionContext,
        input: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>>;
}

#[async_trait::async_trait]
trait PhysicalSource: PhysicalNode {
    /// Return the next chunk from the source. An empty chunk indicates that the source is exhausted.
    async fn source(&self, ctx: &ExecutionContext) -> ExecutionResult<Chunk>;

    fn estimated_cardinality(&self) -> usize;
}

#[async_trait::async_trait]
trait PhysicalSink: PhysicalSource {
    async fn sink(&self, ctx: &ExecutionContext, tuple: Tuple) -> ExecutionResult<()>;
}

pub struct ExecutionContext {
    pool: Arc<dyn Pool>,
    tx: Arc<Transaction>,
    catalog: Arc<Catalog>,
}

impl ExecutionContext {
    #[inline]
    pub fn new(pool: Arc<dyn Pool>, tx: Arc<Transaction>, catalog: Arc<Catalog>) -> Self {
        Self { pool, tx, catalog }
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
    root: Idx<MetaPipeline>,
}
