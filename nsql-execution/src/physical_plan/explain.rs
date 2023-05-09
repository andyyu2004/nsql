use std::fmt;

use crate::ExecutionContext;

pub type Result = std::result::Result<(), anyhow::Error>;

pub trait Explain {
    fn explain(&self, ctx: &ExecutionContext, f: &mut fmt::Formatter<'_>) -> Result;
}
