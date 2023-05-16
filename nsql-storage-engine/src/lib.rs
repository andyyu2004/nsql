//! This crate defines the storage engine interfaces

use std::path::Path;

pub trait StorageEngine {
    type Error;

    type ReadTransaction<'a>: ReadTransaction
    where
        Self: 'a;

    type Transaction<'a>: Transaction
    where
        Self: 'a;

    fn open(path: impl AsRef<Path>) -> Result<Self, Self::Error>
    where
        Self: Sized;

    fn begin_readonly(&self) -> Result<Self::ReadTransaction<'_>, Self::Error>;

    fn begin(&self) -> Result<Self::Transaction<'_>, Self::Error>;
}

pub trait ReadTransaction {
    type Error;

    fn get(&self, key: &[u8]) -> Result<Option<&[u8]>, Self::Error>;
}

pub trait Transaction: ReadTransaction {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error>;

    fn delete(&mut self, key: &[u8]) -> Result<(), Self::Error>;
}
