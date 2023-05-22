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
    ) -> Result<Self::WriteTree<'env, 'txn>, Self::Error>
    where
        'env: 'txn;
}

pub trait ReadTree<'env, 'txn, S: StorageEngine> {}

pub trait WriteTree<'env, 'txn, S: StorageEngine>: ReadTree<'env, 'txn, S> {}

pub trait Transaction<'env, S: StorageEngine> {
    fn get<'txn>(
        &'txn self,
        tree: &'txn S::ReadTree<'env, 'txn>,
        key: &[u8],
    ) -> Result<Option<S::Bytes<'txn>>, S::Error>;

    fn range<'txn>(
        &'txn self,
        tree: &'txn S::ReadTree<'env, 'txn>,
        range: impl RangeBounds<[u8]> + 'txn,
    ) -> Result<impl Iterator<Item = Result<(S::Bytes<'txn>, S::Bytes<'txn>), S::Error>>, S::Error>;

    fn rev_range<'txn>(
        &'txn self,
        tree: &'txn S::ReadTree<'env, 'txn>,
        range: impl RangeBounds<[u8]> + 'txn,
    ) -> Result<impl Iterator<Item = Result<(S::Bytes<'txn>, S::Bytes<'txn>), S::Error>>, S::Error>;
}

pub trait WriteTransaction<'env, S: StorageEngine>: Transaction<'env, S> {
    fn put<'txn>(
        &'txn mut self,
        tree: &mut S::WriteTree<'env, 'txn>,
        key: &[u8],
        value: &[u8],
    ) -> Result<bool, S::Error>;

    fn delete<'txn>(
        &mut self,
        tree: &mut S::WriteTree<'env, 'txn>,
        key: &[u8],
    ) -> Result<bool, S::Error>;

    fn commit(self) -> Result<(), S::Error>;

    fn rollback(self) -> Result<(), S::Error>;
}

pub enum ReadOrWriteTransaction<'env, S: StorageEngine> {
    Read(S::Transaction<'env>),
    Write(S::WriteTransaction<'env>),
}

impl<'env, S: StorageEngine> Transaction<'env, S> for ReadOrWriteTransaction<'env, S> {
    #[inline]
    fn get<'txn>(
        &'txn self,
        tree: &'txn <S as StorageEngine>::ReadTree<'env, 'txn>,
        key: &[u8],
    ) -> Result<Option<<S as StorageEngine>::Bytes<'txn>>, <S as StorageEngine>::Error> {
        match self {
            ReadOrWriteTransaction::Read(txn) => txn.get(tree, key),
            ReadOrWriteTransaction::Write(txn) => txn.get(tree, key),
        }
    }

    #[inline]
    fn range<'txn>(
        &'txn self,
        tree: &'txn <S as StorageEngine>::ReadTree<'env, 'txn>,
        range: impl RangeBounds<[u8]> + 'txn,
    ) -> Result<
        impl Iterator<
            Item = Result<
                (<S as StorageEngine>::Bytes<'txn>, <S as StorageEngine>::Bytes<'txn>),
                <S as StorageEngine>::Error,
            >,
        >,
        <S as StorageEngine>::Error,
    > {
        match self {
            ReadOrWriteTransaction::Read(txn) => {
                Ok(Box::new(txn.range(tree, range)?) as Box<dyn Iterator<Item = _>>)
            }
            ReadOrWriteTransaction::Write(txn) => Ok(Box::new(txn.range(tree, range)?) as _),
        }
    }

    #[inline]
    fn rev_range<'txn>(
        &'txn self,
        tree: &'txn <S as StorageEngine>::ReadTree<'env, 'txn>,
        range: impl RangeBounds<[u8]> + 'txn,
    ) -> Result<
        impl Iterator<
            Item = Result<
                (<S as StorageEngine>::Bytes<'txn>, <S as StorageEngine>::Bytes<'txn>),
                <S as StorageEngine>::Error,
            >,
        >,
        <S as StorageEngine>::Error,
    > {
        match self {
            ReadOrWriteTransaction::Read(txn) => {
                Ok(Box::new(txn.rev_range(tree, range)?) as Box<dyn Iterator<Item = _>>)
            }
            ReadOrWriteTransaction::Write(txn) => Ok(Box::new(txn.rev_range(tree, range)?) as _),
        }
    }
}
