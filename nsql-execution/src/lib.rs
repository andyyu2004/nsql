#![deny(rust_2018_idioms)]
#![feature(trait_upcasting)]

mod eval;
mod executor;
mod physical_plan;
mod pipeline;

use std::sync::Arc;
use std::{fmt, io};

use error_stack::Report;
use nsql_arena::Idx;
use nsql_catalog::Catalog;
use nsql_storage::tuple::Tuple;
use nsql_transaction::Transaction;
pub use physical_plan::PhysicalPlanner;
use thiserror::Error;

use self::eval::Evaluator;
pub use self::executor::execute;
use self::physical_plan::PhysicalPlan;
use self::pipeline::{
    MetaPipeline, MetaPipelineBuilder, PipelineArena, PipelineBuilder, PipelineBuilderArena,
};

pub type ExecutionResult<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Catalog(#[from] nsql_catalog::Error),
    #[error(transparent)]
    Storage(#[from] nsql_storage::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl From<Report<io::Error>> for Error {
    fn from(report: Report<io::Error>) -> Self {
        Self::Io(io::Error::new(report.current_context().kind(), report))
    }
}

fn build_pipelines(
    sink: Arc<dyn PhysicalSink>,
    plan: PhysicalPlan,
) -> (PipelineArena, Idx<MetaPipeline>) {
    let mut arena = PipelineBuilderArena::default();
    let root = MetaPipelineBuilder::new(&mut arena, sink);
    MetaPipelineBuilder::build(&mut arena, root, plan.root());
    let arena = arena.finish();
    (arena, root.cast())
}

trait PhysicalNode: Send + Sync + fmt::Debug + 'static {
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
            Ok(op) => {
                assert_eq!(
                    op.children().len(),
                    1,
                    "default `build_pipelines` implementation only supports unary operators for sinks"
                );
                let child = Arc::clone(&op.children()[0]);
                // If we have a sink `op` (which is also a source), we set the source of current to `op`
                // and then build the pipeline for `op`'s child with `t` as the sink of the new pipeline
                arena[current].set_source(Arc::clone(&op) as Arc<dyn PhysicalSource>);
                let child_meta_builder =
                    MetaPipelineBuilder::new_child_meta_pipeline(arena, meta_builder, current, op);
                MetaPipelineBuilder::build(arena, child_meta_builder, child);
            }
            Err(node) => match node.as_source() {
                Ok(source) => arena[current].set_source(source),
                Err(node) => {
                    let operator = node.as_operator().unwrap();
                    assert_eq!(
                        operator.children().len(),
                        1,
                        "default `build_pipelines` implementation only supports unary operators for operators"
                    );
                }
            },
        }
    }
}

#[async_trait::async_trait]
trait PhysicalOperator: PhysicalNode {
    async fn execute(&self, ctx: &ExecutionContext<'_>, input: Tuple) -> ExecutionResult<Tuple>;
}

#[async_trait::async_trait]
trait PhysicalSource: PhysicalNode {
    async fn source(&self, ctx: &ExecutionContext<'_>) -> ExecutionResult<Option<Tuple>>;

    fn estimated_cardinality(&self) -> usize;
}

#[async_trait::async_trait]
trait PhysicalSink: PhysicalSource {
    async fn sink(&self, ctx: &ExecutionContext<'_>, tuple: Tuple) -> ExecutionResult<()>;
}

struct ExecutionContext<'a> {
    tx: &'a Transaction,
    catalog: &'a Catalog,
}

impl<'a> ExecutionContext<'a> {
    pub fn new(tx: &'a Transaction, catalog: &'a Catalog) -> Self {
        Self { tx, catalog }
    }

    #[inline]
    pub fn tx(&self) -> &Transaction {
        self.tx
    }

    #[inline]
    pub fn catalog(&self) -> &Catalog {
        self.catalog
    }
}
