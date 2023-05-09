use std::fmt;

use crate::{ExecutionContext, PhysicalNode};

pub type Result<T = ()> = std::result::Result<T, fmt::Error>;

pub trait Explain {
    fn explain(&self, ctx: &ExecutionContext, f: &mut fmt::Formatter<'_>) -> Result;
}

pub(crate) fn explain(ctx: &ExecutionContext, node: &dyn PhysicalNode) -> Result<Explained> {
    todo!()
}

pub struct Explained {}
