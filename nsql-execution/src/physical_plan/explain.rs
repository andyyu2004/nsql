use std::fmt;

use crate::{ExecutionContext, PhysicalNode};

pub type Result<T = ()> = std::result::Result<T, fmt::Error>;

pub trait Explain {
    fn explain(&self, ctx: &ExecutionContext, f: &mut fmt::Formatter<'_>) -> Result;
}

pub(crate) fn explain<'a>(ctx: &'a ExecutionContext, node: &'a dyn PhysicalNode) -> Explained<'a> {
    Explained { ctx, node }
}

pub struct Explained<'a> {
    ctx: &'a ExecutionContext,
    node: &'a dyn PhysicalNode,
}

impl fmt::Display for Explained<'_> {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}
