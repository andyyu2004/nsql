#![deny(rust_2018_idioms)]
#![feature(async_fn_in_trait)]
#![allow(incomplete_features)]

mod table_storage;
pub mod tuple;
mod wal;

use std::io;
use std::sync::Arc;

use nsql_pager::Pager;
use nsql_transaction::Transaction;
pub use table_storage::TableStorage;
use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Fs(#[from] io::Error),
}

pub struct Storage {
    pager: Arc<dyn Pager>,
}

impl Storage {
    pub async fn load(&self, _tx: &Transaction) -> Result<()> {
        todo!()
        // let reader = self.pager.meta_page_reader();
        // let checkpointer = Checkpointer::new(self.pager.as_ref());
        // let checkpoint = checkpointer.load_checkpoint(tx, reader).await?;
        // Ok(checkpoint)
    }

    pub async fn checkpoint(&self) -> Result<()> {
        Ok(())
    }

    #[inline]
    pub fn new(pager: Arc<dyn Pager>) -> Self {
        Self { pager }
    }

    #[inline]
    pub fn pager(&self) -> Arc<dyn Pager> {
        Arc::clone(&self.pager)
    }
}
