mod physical_plan;
mod pipeline;

use std::fmt;
use std::sync::Arc;

use nsql_catalog::Catalog;
use nsql_transaction::Transaction;
pub use physical_plan::PhysicalPlanner;
use thiserror::Error;

use self::pipeline::{Pipeline, PipelineBuilder};

pub type ExecutionResult<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Catalog(#[from] nsql_catalog::Error),
}

pub fn execute(
    tx: &Transaction,
    catalog: &Catalog,
    node: PhysicalNode,
) -> ExecutionResult<Vec<Box<dyn Tuple>>> {
    let pipeline = build_pipeline(node);
    execute_pipeline(tx, catalog, pipeline)
}

fn execute_pipeline(
    tx: &Transaction,
    catalog: &Catalog,
    pipeline: Pipeline,
) -> ExecutionResult<Vec<Box<dyn Tuple>>> {
    let source = pipeline.source;
    let ctx = ExecutionContext::new(tx, catalog);
    let tup = source.source(&ctx)?;

    if let Some(tup) = tup { Ok(vec![tup]) } else { Ok(vec![]) }
}

fn build_pipeline(node: PhysicalNode) -> Pipeline {
    let mut builder = PipelineBuilder::default();
    node.build_pipelines(&mut builder, ());
    builder.build()
}

#[derive(Clone)]
pub enum PhysicalNode {
    Source(Arc<dyn PhysicalSource>),
    Operator(Arc<dyn PhysicalOperator>),
    Sink(Arc<dyn PhysicalSink>),
}

impl PhysicalNode {
    fn build_pipelines(&self, current: &mut PipelineBuilder, meta: ()) {
        match self {
            PhysicalNode::Source(source) => current.set_source(Arc::clone(source)),
            PhysicalNode::Operator(_) => todo!(),
            PhysicalNode::Sink(_) => todo!(),
        }
    }

    pub(crate) fn source(source: impl PhysicalSource) -> PhysicalNode {
        PhysicalNode::Source(Arc::new(source))
    }
}

pub trait PhysicalNodeBase: Send + Sync + fmt::Debug + 'static {
    fn as_any(&self) -> &dyn std::any::Any;

    fn estimated_cardinality(&self) -> usize;

    fn children(&self) -> &[PhysicalNode];
}

pub trait PhysicalOperator: PhysicalNodeBase {
    fn execute(
        &self,
        ctx: &ExecutionContext<'_>,
        input: Box<dyn Tuple>,
        output: &mut dyn Tuple,
    ) -> ExecutionResult<()>;
}

pub trait PhysicalSource: PhysicalNodeBase {
    fn source(&self, ctx: &ExecutionContext<'_>) -> ExecutionResult<Option<Box<dyn Tuple>>>;
}

pub trait PhysicalSink: PhysicalNodeBase {
    fn source(&self, ctx: &ExecutionContext<'_>, tuple: Box<dyn Tuple>) -> ExecutionResult<()>;
}

pub struct ExecutionContext<'a> {
    tx: &'a Transaction,
    catalog: &'a Catalog,
}

impl<'a> ExecutionContext<'a> {
    pub fn new(tx: &'a Transaction, catalog: &'a Catalog) -> Self {
        Self { tx, catalog }
    }

    pub fn tx(&self) -> &Transaction {
        self.tx
    }

    pub fn catalog(&self) -> &Catalog {
        self.catalog
    }
}

pub trait Tuple: fmt::Debug + Send {}
