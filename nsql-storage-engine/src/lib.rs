//! This crate defines the storage engine interfaces

use std::path::Path;

pub trait StorageEngine: Sized {
    type Error;

    type ReadTransaction<'env>: ReadTransaction
    where
        Self: 'env;

    type Transaction<'txn>: Transaction
    where
        Self: 'txn;

    type ReadTree<'env, 'txn>: ReadTree<'env, 'txn, Self>
    where
        Self: 'env + 'txn,
        'env: 'txn;

    type Tree<'env, 'txn>: Tree<'env, 'txn, Self>
    where
        Self: 'env + 'txn,
        'env: 'txn;

    fn open(path: impl AsRef<Path>) -> Result<Self, Self::Error>
    where
        Self: Sized;

    fn begin_readonly(&self) -> Result<Self::ReadTransaction<'_>, Self::Error>;

    fn begin(&self) -> Result<Self::Transaction<'_>, Self::Error>;

    fn open_tree_readonly<'env, 'txn>(
        &self,
        txn: &'env Self::ReadTransaction<'txn>,
        name: &str,
    ) -> Result<Option<Self::ReadTree<'env, 'txn>>, Self::Error>
    where
        'env: 'txn;

    fn open_tree<'env, 'txn>(
        &self,
        txn: &'txn Self::Transaction<'env>,
        name: &str,
    ) -> Result<Option<Self::Tree<'env, 'txn>>, Self::Error>;
}

pub trait ReadTree<'env, 'txn, S: StorageEngine> {
    fn get(
        &self,
        txn: &'txn S::ReadTransaction<'_>,
        key: &[u8],
    ) -> Result<Option<&'txn [u8]>, S::Error>;
}

pub trait Tree<'env, 'txn, S: StorageEngine>: ReadTree<'env, 'txn, S> {
    fn put(&self, txn: &mut S::Transaction<'_>, key: &[u8], value: &[u8]) -> Result<(), S::Error>;

    fn delete(&self, txn: &mut S::Transaction<'_>, key: &[u8]) -> Result<(), S::Error>;
}

pub trait ReadTransaction {
    type Error;
}

pub trait Transaction: ReadTransaction {}
