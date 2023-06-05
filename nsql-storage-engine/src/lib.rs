// #![feature(return_position_impl_trait_in_trait)]
// #![feature(impl_trait_projections)]
//! This crate defines the storage engine interfaces

#[cfg(test)]
mod example;

use std::error::Error;
use std::ops::{Deref, RangeBounds};
use std::path::Path;

pub use fallible_iterator;
pub use fallible_iterator::FallibleIterator;

pub trait StorageEngine: Clone + Send + Sync + Sized + 'static {
    type Error: Send + Sync + Error + 'static;

    type Bytes<'txn>: Deref<Target = [u8]>;

    type Transaction<'env>: Transaction<'env, Self> + Clone + Send + Sync
    where
        Self: 'env;

    type WriteTransaction<'env>: WriteTransaction<'env, Self>
    where
        Self: 'env;

    type ReadTree<'env, 'txn>: ReadTree<'env, 'txn, Self> + Send + Sync
    where
        Self: 'env + 'txn,
        'env: 'txn;

    type WriteTree<'env, 'txn>: WriteTree<'env, 'txn, Self>
    where
        Self: 'env + 'txn,
        'env: 'txn;

    fn create(path: impl AsRef<Path>) -> Result<Self, Self::Error>
    where
        Self: Sized;

    fn open(path: impl AsRef<Path>) -> Result<Self, Self::Error>
    where
        Self: Sized;

    fn begin(&self) -> Result<Self::Transaction<'_>, Self::Error>;

    fn begin_write(&self) -> Result<Self::WriteTransaction<'_>, Self::Error>;

    fn open_tree<'env, 'txn>(
        &self,
        txn: &'txn impl Transaction<'env, Self>,
        name: &str,
    ) -> Result<Option<Self::ReadTree<'env, 'txn>>, Self::Error>
    where
        'env: 'txn;

    /// Open a tree for read/write access
    fn open_write_tree<'env, 'txn>(
        &self,
        txn: &'txn Self::WriteTransaction<'env>,
        name: &str,
    ) -> Result<Self::WriteTree<'env, 'txn>, Self::Error>
    where
        'env: 'txn;
}

pub type Range<'a, S> = Box<
    dyn FallibleIterator<
            Item = (<S as StorageEngine>::Bytes<'a>, <S as StorageEngine>::Bytes<'a>),
            Error = <S as StorageEngine>::Error,
        > + Unpin
        + 'a,
>;

pub trait ReadTree<'env, 'txn, S: StorageEngine> {
    fn get<'a>(&'a self, key: &[u8]) -> Result<Option<S::Bytes<'a>>, S::Error>;

    fn range<'a>(&'a self, range: impl RangeBounds<[u8]> + 'a) -> Result<Range<'a, S>, S::Error>;

    fn rev_range<'a>(
        &'a self,
        range: impl RangeBounds<[u8]> + 'a,
    ) -> Result<Range<'a, S>, S::Error>;
}

pub trait WriteTree<'env, 'txn, S: StorageEngine>: ReadTree<'env, 'txn, S> {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), S::Error>;

    fn delete(&mut self, key: &[u8]) -> std::result::Result<bool, S::Error>;
}

pub trait Transaction<'env, S: StorageEngine> {
    fn as_read_or_write(&self) -> ReadOrWriteTransactionRef<'env, '_, S>;
}

pub trait WriteTransaction<'env, S: StorageEngine>: Transaction<'env, S> {
    fn commit(self) -> Result<(), S::Error>;

    fn abort(self) -> Result<(), S::Error>;
}

pub enum ReadOrWriteTransaction<'env, S: StorageEngine> {
    Read(S::Transaction<'env>),
    Write(S::WriteTransaction<'env>),
}

impl<'env, S: StorageEngine> Transaction<'env, S> for ReadOrWriteTransaction<'env, S> {
    #[inline]
    fn as_read_or_write(&self) -> ReadOrWriteTransactionRef<'env, '_, S> {
        match self {
            ReadOrWriteTransaction::Read(tx) => ReadOrWriteTransactionRef::Read(tx),
            ReadOrWriteTransaction::Write(tx) => ReadOrWriteTransactionRef::Write(tx),
        }
    }
}

pub enum ReadOrWriteTransactionRef<'env, 'txn, S: StorageEngine> {
    Read(&'txn S::Transaction<'env>),
    Write(&'txn S::WriteTransaction<'env>),
}

impl<'env: 'txn, 'txn, S: StorageEngine> Clone for ReadOrWriteTransactionRef<'env, 'txn, S> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> Copy for ReadOrWriteTransactionRef<'env, 'txn, S> {}

impl<'env: 'txn, 'txn, S: StorageEngine> Transaction<'env, S>
    for ReadOrWriteTransactionRef<'env, 'txn, S>
{
    fn as_read_or_write(&self) -> ReadOrWriteTransactionRef<'env, '_, S> {
        *self
    }
}

pub enum ReadOrWriteTree<'env: 'txn, 'txn, S: StorageEngine> {
    Read(S::ReadTree<'env, 'txn>),
    Write(S::WriteTree<'env, 'txn>),
}

impl<'env: 'txn, 'txn, S: StorageEngine> ReadTree<'env, 'txn, S>
    for ReadOrWriteTree<'env, 'txn, S>
{
    fn get<'a>(&'a self, key: &[u8]) -> Result<Option<S::Bytes<'a>>, S::Error> {
        match self {
            ReadOrWriteTree::Read(tree) => tree.get(key),
            ReadOrWriteTree::Write(tree) => tree.get(key),
        }
    }

    fn range<'a>(&'a self, range: impl RangeBounds<[u8]> + 'a) -> Result<Range<'a, S>, S::Error> {
        match self {
            ReadOrWriteTree::Read(tree) => Ok(Box::new(tree.range(range)?)),
            ReadOrWriteTree::Write(tree) => Ok(Box::new(tree.range(range)?)),
        }
    }

    fn rev_range<'a>(
        &'a self,
        range: impl RangeBounds<[u8]> + 'a,
    ) -> Result<Range<'a, S>, S::Error> {
        match self {
            ReadOrWriteTree::Read(tree) => Ok(Box::new(tree.rev_range(range)?)),
            ReadOrWriteTree::Write(tree) => Ok(Box::new(tree.rev_range(range)?)),
        }
    }
}

pub trait ExecutionMode<'env, S: StorageEngine>: private::Sealed + Clone + Copy + 'static {
    type Transaction: Transaction<'env, S>;

    type Tree<'txn>: ReadTree<'env, 'txn, S>
    where
        'env: 'txn;

    fn open_tree<'txn>(
        storage: &S,
        txn: &'txn Self::Transaction,
        name: &str,
    ) -> Result<Self::Tree<'txn>, S::Error>;
}

mod private {
    pub trait Sealed {}
}

pub struct ReadonlyExecutionMode<S>(std::marker::PhantomData<S>);

impl<S> Clone for ReadonlyExecutionMode<S> {
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0)
    }
}

impl<S> private::Sealed for ReadonlyExecutionMode<S> {}

impl<S> Copy for ReadonlyExecutionMode<S> {}

impl<'env, S: StorageEngine> ExecutionMode<'env, S> for ReadonlyExecutionMode<S> {
    type Transaction = S::Transaction<'env>;

    type Tree<'txn> = S::ReadTree<'env, 'txn> where 'env: 'txn;

    fn open_tree<'txn>(
        storage: &S,
        txn: &'txn Self::Transaction,
        name: &str,
    ) -> Result<Self::Tree<'txn>, S::Error> {
        // FIXME don't unwrap
        storage.open_tree(txn, name).map(Option::unwrap)
    }
}

pub struct ReadWriteExecutionMode<S>(std::marker::PhantomData<S>);

impl<S> Clone for ReadWriteExecutionMode<S> {
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0)
    }
}

impl<S> Copy for ReadWriteExecutionMode<S> {}

impl<S> private::Sealed for ReadWriteExecutionMode<S> {}

impl<'env, S: StorageEngine> ExecutionMode<'env, S> for ReadWriteExecutionMode<S> {
    type Transaction = S::WriteTransaction<'env>;

    type Tree<'txn> = S::WriteTree<'env, 'txn>
    where
        'env: 'txn;

    fn open_tree<'txn>(
        storage: &S,
        txn: &'txn Self::Transaction,
        name: &str,
    ) -> Result<Self::Tree<'txn>, <S as StorageEngine>::Error> {
        // FIXME don't unwrap
        storage.open_write_tree(txn, name)
    }
}
