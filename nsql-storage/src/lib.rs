#![deny(rust_2018_idioms)]

mod checkpoint;
mod wal;

use std::sync::Arc;

use nsql_pager::{Pager, Result, SingleFilePager};
use nsql_transaction::Transaction;

use self::checkpoint::{Checkpoint, Checkpointer};

pub struct Storage<P> {
    pager: Arc<P>,
}

impl Storage<SingleFilePager> {
    pub async fn load(&self, tx: &Transaction) -> Result<Checkpoint> {
        let reader = self.pager.meta_page_reader();
        let checkpointer = Checkpointer::new(self.pager.as_ref());
        let checkpoint = checkpointer.load_checkpoint(tx, reader).await?;
        Ok(checkpoint)
    }

    pub async fn checkpoint(&self) -> Result<()> {
        Ok(())
    }
}

impl<P: Pager> Storage<P> {
    pub fn new(pager: Arc<P>) -> Self {
        Self { pager }
    }
}
