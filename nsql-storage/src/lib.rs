#![deny(rust_2018_idioms)]
#![feature(async_fn_in_trait)]
#![feature(split_array)]
#![allow(incomplete_features)]

pub mod schema;
mod table_storage;
mod transaction;
pub mod tuple;
pub mod value;
mod wal;

use std::io;
use std::sync::Arc;

use nsql_pager::Pager;
pub use table_storage::{TableStorage, TableStorageInfo};
use thiserror::Error;

pub use self::transaction::{
    Transaction, TransactionError, TransactionManager, TransactionState, Transactional, Version,
    Xid,
};

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
