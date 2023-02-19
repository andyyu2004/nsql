#![deny(rust_2018_idioms)]

mod checkpoint;
mod wal;

use std::sync::Arc;

use nsql_pager::{Pager, Result};

pub struct Storage<P> {
    pager: Arc<P>,
}

impl<P> Storage<P> {
    pub fn new(pager: Arc<P>) -> Self {
        Self { pager }
    }
}

impl<P: Pager> Storage<P> {
    pub fn checkpoint(&self) -> Result<()> {
        Ok(())
    }
}
