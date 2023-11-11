#![feature(trivial_bounds)]
#![feature(once_cell_try)]

pub mod expr;
pub mod tuple;
pub mod value;

use anyhow::Error;
use nsql_storage_engine::StorageEngine;

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
