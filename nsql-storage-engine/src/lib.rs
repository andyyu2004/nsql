//! This crate defines the storage engine interfaces

use std::path::Path;

pub trait StorageEngine {
    type Error;

    type ReadonlyTransaction<'a>: ReadonlyTransaction
    where
        Self: 'a;

    type ReadWriteTransaction<'a>: ReadWriteTransaction
    where
        Self: 'a;

    fn open(path: impl AsRef<Path>) -> Result<Self, Self::Error>
    where
        Self: Sized;

    fn begin_readonly(&self) -> Result<Self::ReadonlyTransaction<'_>, Self::Error>;

    fn begin(&self) -> Result<Self::ReadWriteTransaction<'_>, Self::Error>;
}

pub trait ReadonlyTransaction {
    type Error;

    fn get(&self, key: &[u8]) -> Result<Option<&[u8]>, Self::Error>;
}

pub trait ReadWriteTransaction: ReadonlyTransaction {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error>;

    fn delete(&mut self, key: &[u8]) -> Result<(), Self::Error>;
}
