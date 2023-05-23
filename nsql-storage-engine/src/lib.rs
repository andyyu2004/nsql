#![feature(return_position_impl_trait_in_trait)]
#![feature(impl_trait_projections)]
//! This crate defines the storage engine interfaces

use std::error::Error;
use std::ops::{Deref, RangeBounds};
use std::path::Path;

pub trait StorageEngine: Clone + Send + Sync + Sized + 'static {
    type Error: Send + Sync + Error + 'static;

    type Bytes<'txn>: Deref<Target = [u8]>;

    type Transaction<'env>: Transaction<'env, Self> + Clone + Send + Sync
    where
        Self: 'env;

    type WriteTransaction<'env>: WriteTransaction<'env, Self>
    where
        Self: 'env;

    type ReadTree<'env, 'txn>: ReadTree<'env, 'txn, Self>
    where
        Self: 'env + 'txn,
        'env: 'txn;

    type WriteTree<'env, 'txn>: WriteTree<'env, 'txn, Self>
    where
        Self: 'env + 'txn,
        'env: 'txn;

    fn open(path: impl AsRef<Path>) -> Result<Self, Self::Error>
    where
        Self: Sized;

    fn begin(&self) -> Result<Self::Transaction<'_>, Self::Error>;

    fn begin_write(&self) -> Result<Self::WriteTransaction<'_>, Self::Error>;

    fn open_tree<'env, 'txn>(
        &self,
        txn: &'txn Self::Transaction<'env>,
        name: &str,
    ) -> Result<Option<Self::ReadTree<'env, 'txn>>, Self::Error>
    where
        'env: 'txn;

    /// Open a tree for read/write access, creating it if it doesn't exist.
    fn open_write_tree<'env, 'txn>(
        &self,
        txn: &'txn mut Self::WriteTransaction<'env>,
        name: &str,
    ) -> Result<Self::WriteTree<'env, 'txn>, Self::Error>
    where
        'env: 'txn;
}

pub trait ReadTree<'env, 'txn, S: StorageEngine> {
    fn get(&'txn self, key: &[u8]) -> Result<Option<S::Bytes<'txn>>, S::Error>;

    fn range(
        &'txn self,
        range: impl RangeBounds<[u8]> + 'txn,
    ) -> Result<impl Iterator<Item = Result<(S::Bytes<'txn>, S::Bytes<'txn>), S::Error>>, S::Error>;

    fn rev_range(
        &'txn self,
        range: impl RangeBounds<[u8]> + 'txn,
    ) -> Result<impl Iterator<Item = Result<(S::Bytes<'txn>, S::Bytes<'txn>), S::Error>>, S::Error>;
}

pub trait WriteTree<'env, 'txn, S: StorageEngine>: ReadTree<'env, 'txn, S> {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), S::Error>;

    fn delete(&mut self, key: &[u8]) -> std::result::Result<bool, S::Error>;
}

pub trait Transaction<'env, S: StorageEngine> {}

pub trait WriteTransaction<'env, S: StorageEngine>: Transaction<'env, S> {
    fn commit(self) -> Result<(), S::Error>;

    fn rollback(self) -> Result<(), S::Error>;
}

pub enum ReadOrWriteTransaction<'env, S: StorageEngine> {
    Read(S::Transaction<'env>),
    Write(S::WriteTransaction<'env>),
}

impl<'env, S: StorageEngine> Transaction<'env, S> for ReadOrWriteTransaction<'env, S> {}
