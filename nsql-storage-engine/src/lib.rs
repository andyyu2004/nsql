#![feature(return_position_impl_trait_in_trait)]
#![feature(impl_trait_projections)]
//! This crate defines the storage engine interfaces

use std::error::Error;
use std::ops::{Deref, RangeBounds};
use std::path::Path;

pub trait StorageEngine: Clone + Send + Sync + Sized + 'static {
    type Error: Send + Sync + Error + 'static;

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

    type Tree<'env, 'txn>: WriteTree<'env, 'txn, Self>
    where
        Self: 'env + 'txn,
        'env: 'txn;

    fn open(path: impl AsRef<Path>) -> Result<Self, Self::Error>
    where
        Self: Sized;

    fn begin_readonly(&self) -> Result<Self::Transaction<'_>, Self::Error>;

    fn begin(&self) -> Result<Self::WriteTransaction<'_>, Self::Error>;

    fn open_tree_readonly<'env, 'txn>(
        &self,
        txn: &'txn Self::Transaction<'env>,
        name: &str,
    ) -> Result<Option<Self::ReadTree<'env, 'txn>>, Self::Error>
    where
        'env: 'txn;

    /// Open a tree for read/write access, creating it if it doesn't exist.
    fn open_tree<'env, 'txn>(
        &self,
        txn: &'txn mut Self::WriteTransaction<'env>,
        name: &str,
    ) -> Result<Self::Tree<'env, 'txn>, Self::Error>
    where
        'env: 'txn;
}

pub trait ReadTree<'env, 'txn, S: StorageEngine> {
    type Bytes: Deref<Target = [u8]>;
    // type Range: Iterator<Item = Result<(Self::Bytes, Self::Bytes), S::Error>>;

    fn get(
        &'txn self,
        txn: &'txn S::Transaction<'_>,
        key: &[u8],
    ) -> Result<Option<Self::Bytes>, S::Error>;

    fn range(
        &'txn self,
        txn: &'txn S::Transaction<'_>,
        range: impl RangeBounds<[u8]>,
    ) -> Result<impl Iterator<Item = Result<(Self::Bytes, Self::Bytes), S::Error>>, S::Error>;

    fn rev_range(
        &'txn self,
        txn: &'txn S::Transaction<'_>,
        range: impl RangeBounds<[u8]>,
    ) -> Result<impl Iterator<Item = Result<(Self::Bytes, Self::Bytes), S::Error>>, S::Error>;
}

pub trait WriteTree<'env, 'txn, S: StorageEngine>: ReadTree<'env, 'txn, S> {
    fn put(
        &mut self,
        txn: &mut S::WriteTransaction<'_>,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), S::Error>;

    fn delete(&mut self, txn: &mut S::WriteTransaction<'_>, key: &[u8]) -> Result<bool, S::Error>;
}

pub trait Transaction<'env, S: StorageEngine> {
    type Error;

    fn upgrade(&mut self) -> Result<Option<&mut S::WriteTransaction<'env>>, Self::Error>;
}

pub trait WriteTransaction<'env, S: StorageEngine>: Transaction<'env, S> {
    fn commit(self) -> Result<(), Self::Error>;

    fn rollback(self) -> Result<(), Self::Error>;
}
