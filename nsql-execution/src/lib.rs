mod physical_plan;
mod pipeline;

use std::fmt;

use nsql_catalog::Catalog;
use nsql_transaction::Transaction;
pub use physical_plan::PhysicalPlanner;
use thiserror::Error;

use self::pipeline::Pipeline;

pub type ExecutionResult<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Catalog(#[from] nsql_catalog::Error),
}

pub fn execute(node: Box<dyn PhysicalNode>) -> Vec<Box<dyn Tuple>> {
    vec![]
}

fn build_pipeline(node: Box<dyn PhysicalNode>) -> Pipeline {}

pub trait PhysicalNode: Send + fmt::Debug {
    fn estimated_cardinality(&self) -> usize;
}

pub trait PhysicalOperator {
    fn execute(
        &self,
        ctx: &dyn ExecutionContext,
        input: Box<dyn Tuple>,
        output: &mut dyn Tuple,
    ) -> ExecutionResult<()>;
}

pub trait PhysicalSource: PhysicalNode {
    fn source(
        &self,
        ctx: &dyn ExecutionContext,
        out: Option<&mut dyn Tuple>,
    ) -> ExecutionResult<()>;
}

pub trait ExecutionContext {
    fn tx(&self) -> &Transaction;

    fn catalog(&self) -> &Catalog;
}

pub trait Tuple {}
