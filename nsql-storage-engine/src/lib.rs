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

    /// Open a tree for read/write access
    fn open_write_tree<'env, 'txn>(
        &self,
        txn: &'txn mut Self::WriteTransaction<'env>,
        name: &str,
    ) -> Result<Self::WriteTree<'env, 'txn>, Self::Error>
    where
        'env: 'txn;

    /// Open a tree for read-only access given either a read or write transaction, creating it if it doesn't exist.
    /// Prefer the other open methods if you have a read or write transaction as they are more efficient.
    fn open_read_or_write_tree<'env, 'txn>(
        &self,
        txn: &'txn ReadOrWriteTransaction<'env, Self>,
        name: &str,
    ) -> Result<Option<ReadOrWriteTree<'env, 'txn, Self>>, Self::Error>
    where
        'env: 'txn,
    {
        match txn {
            ReadOrWriteTransaction::Read(tx) => {
                Ok(self.open_tree(tx, name)?.map(ReadOrWriteTree::Read))
            }
            ReadOrWriteTransaction::Write(tx) => {
                // FIXME need a method to open a readtree given a write tx
                Ok(Some(ReadOrWriteTree::Write(self.open_tree(tx, name)?)))
            }
        }
    }
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

pub enum ReadOrWriteTree<'env: 'txn, 'txn, S: StorageEngine> {
    Read(S::ReadTree<'env, 'txn>),
    Write(S::WriteTree<'env, 'txn>),
}

impl<'env, 'txn, S: StorageEngine> ReadTree<'env, 'txn, S> for ReadOrWriteTree<'env, 'txn, S> {
    fn get(
        &'txn self,
        key: &[u8],
    ) -> Result<Option<<S as StorageEngine>::Bytes<'txn>>, <S as StorageEngine>::Error> {
        match self {
            ReadOrWriteTree::Read(tree) => tree.get(key),
            ReadOrWriteTree::Write(tree) => tree.get(key),
        }
    }

    fn range(
        &'txn self,
        range: impl RangeBounds<[u8]> + 'txn,
    ) -> Result<
        Box<
            dyn Iterator<
                    Item = Result<
                        (<S as StorageEngine>::Bytes<'txn>, <S as StorageEngine>::Bytes<'txn>),
                        <S as StorageEngine>::Error,
                    >,
                > + 'txn,
        >,
        <S as StorageEngine>::Error,
    > {
        match self {
            ReadOrWriteTree::Read(tree) => Ok(Box::new(tree.range(range)?)),
            ReadOrWriteTree::Write(tree) => Ok(Box::new(tree.range(range)?)),
        }
    }

    fn rev_range(
        &'txn self,
        range: impl RangeBounds<[u8]> + 'txn,
    ) -> Result<
        Box<
            dyn Iterator<
                    Item = Result<
                        (<S as StorageEngine>::Bytes<'txn>, <S as StorageEngine>::Bytes<'txn>),
                        <S as StorageEngine>::Error,
                    >,
                > + 'txn,
        >,
        <S as StorageEngine>::Error,
    > {
        match self {
            ReadOrWriteTree::Read(tree) => Ok(Box::new(tree.rev_range(range)?)),
            ReadOrWriteTree::Write(tree) => Ok(Box::new(tree.rev_range(range)?)),
        }
    }
}
