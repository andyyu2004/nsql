#![deny(rust_2018_idioms)]

mod table_storage;
pub mod tuple;
pub mod value;

use anyhow::Error;
use nsql_storage_engine::{StorageEngine, Transaction};
pub use table_storage::{ColumnStorageInfo, TableStorage, TableStorageInfo};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct Storage<S> {
    storage: S,
}

impl<S: StorageEngine> Storage<S> {
    #[inline]
    pub fn begin(&self) -> Result<S::Transaction<'_>, S::Error> {
        self.storage.begin()
    }

    #[inline]
    pub fn begin_write(&self) -> Result<S::WriteTransaction<'_>, S::Error> {
        self.storage.begin_write()
    }

    pub fn load(&self, _tx: &dyn Transaction<'_, S>) -> Result<()> {
        todo!()
        // let reader = self.pager.meta_page_reader();
        // let checkpointer = Checkpointer::new(self.pager.as_ref());
        // let checkpoint = checkpointer.load_checkpoint(tx, reader).await?;
        // Ok(checkpoint)
    }

    pub fn checkpoint(&self) -> Result<()> {
        Ok(())
    }

    #[inline]
    pub fn new(storage: S) -> Self {
        Self { storage }
    }

    #[inline]
    pub fn storage(&self) -> &S {
        &self.storage
    }
}
