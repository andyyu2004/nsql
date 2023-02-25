#![deny(rust_2018_idioms)]

mod checkpoint;
mod table_storage;
pub mod tuple;
mod wal;

use std::io;
use std::sync::Arc;

use nsql_pager::{Pager, SingleFilePager};
use nsql_transaction::Transaction;
pub use table_storage::TableStorage;
use thiserror::Error;

use self::checkpoint::{Checkpoint, Checkpointer};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Catalog(#[from] nsql_catalog::Error),
    #[error(transparent)]
    Fs(#[from] io::Error),
}

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
