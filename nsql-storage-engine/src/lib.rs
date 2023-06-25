// #![feature(return_position_impl_trait_in_trait)]
// #![feature(impl_trait_projections)]
//! This crate defines the storage engine interfaces

#[cfg(test)]
mod example;

use std::error::Error;
use std::fmt;
use std::ops::{Deref, RangeBounds};
use std::path::Path;

pub use fallible_iterator;
pub use fallible_iterator::FallibleIterator;

pub trait StorageEngine: Send + Sync + Sized + fmt::Debug + 'static {
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
        txn: &'txn dyn Transaction<'env, Self>,
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

    fn drop_tree(&self, txn: &Self::WriteTransaction<'_>, name: &str) -> Result<(), Self::Error>;
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

    #[inline]
    fn exists(&self, key: &[u8]) -> Result<bool, S::Error> {
        self.get(key).map(|v| v.is_some())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KeyExists;

pub trait WriteTree<'env, 'txn, S: StorageEngine>: ReadTree<'env, 'txn, S> {
    /// Insert a key/value pair into the tree.
    /// This must return an `KeyExists` error if the key already exists.
    /// It may still do the insert if this is the case (the caller can rollback the transaction).
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<Result<(), KeyExists>, S::Error>;

    /// Update a key/value pair in the tree.
    /// The key may or may not exist before this call.
    fn update(&mut self, key: &[u8], value: &[u8]) -> Result<(), S::Error>;

    fn delete(&mut self, key: &[u8]) -> std::result::Result<bool, S::Error>;
}

pub trait Transaction<'env, S: StorageEngine> {
    fn as_read_or_write_ref(&self) -> ReadOrWriteTransactionRef<'env, '_, S>;

    fn as_dyn(&self) -> &dyn Transaction<'env, S>;
}

impl<'a, 'env, S: StorageEngine> Transaction<'env, S> for &'a dyn Transaction<'env, S> {
    #[inline]
    fn as_read_or_write_ref(&self) -> ReadOrWriteTransactionRef<'env, '_, S> {
        (*self).as_read_or_write_ref()
    }

    #[inline]
    fn as_dyn(&self) -> &dyn Transaction<'env, S> {
        *self
    }
}

impl<'a, 'env, Tx, S: StorageEngine> Transaction<'env, S> for &'a Tx
where
    Tx: Transaction<'env, S>,
{
    #[inline]
    fn as_read_or_write_ref(&self) -> ReadOrWriteTransactionRef<'env, '_, S> {
        (*self).as_read_or_write_ref()
    }

    #[inline]
    fn as_dyn(&self) -> &dyn Transaction<'env, S> {
        *self
    }
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
    fn as_read_or_write_ref(&self) -> ReadOrWriteTransactionRef<'env, '_, S> {
        match self {
            ReadOrWriteTransaction::Read(tx) => ReadOrWriteTransactionRef::Read(tx),
            ReadOrWriteTransaction::Write(tx) => ReadOrWriteTransactionRef::Write(tx),
        }
    }

    #[inline]
    fn as_dyn(&self) -> &dyn Transaction<'env, S> {
        self
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
    #[inline]
    fn as_read_or_write_ref(&self) -> ReadOrWriteTransactionRef<'env, '_, S> {
        *self
    }

    #[inline]
    fn as_dyn(&self) -> &dyn Transaction<'env, S> {
        self
    }
}

// TODO not sure if this type will be necessary
pub enum ReadOrWriteTree<'env: 'txn, 'txn, S: StorageEngine> {
    Read(S::ReadTree<'env, 'txn>),
    Write(S::WriteTree<'env, 'txn>),
}

impl<'env: 'txn, 'txn, S: StorageEngine> ReadTree<'env, 'txn, S>
    for ReadOrWriteTree<'env, 'txn, S>
{
    #[inline]
    fn get<'a>(&'a self, key: &[u8]) -> Result<Option<S::Bytes<'a>>, S::Error> {
        match self {
            ReadOrWriteTree::Read(tree) => tree.get(key),
            ReadOrWriteTree::Write(tree) => tree.get(key),
        }
    }

    #[inline]
    fn exists(&self, key: &[u8]) -> Result<bool, <S as StorageEngine>::Error> {
        match self {
            ReadOrWriteTree::Read(tree) => tree.exists(key),
            ReadOrWriteTree::Write(tree) => tree.exists(key),
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

// helper trait to help with conversion
// there is probably a way to do this without this trait
#[doc(hidden)]
pub trait TransactionConversionHack<'env, 'txn, Tx, S> {
    fn as_tx_ref(tx: &'txn Tx) -> Self;

    fn dyn_ref(self) -> &'txn dyn Transaction<'env, S>;
}

impl<'env, 'txn, S: StorageEngine> TransactionConversionHack<'env, 'txn, S::Transaction<'env>, S>
    for &'txn dyn Transaction<'env, S>
{
    #[inline]
    fn as_tx_ref(tx: &'txn S::Transaction<'env>) -> Self {
        tx
    }

    #[inline]
    fn dyn_ref(self) -> &'txn dyn Transaction<'env, S> {
        self
    }
}

impl<'env, 'txn, T: Transaction<'env, S>, S: StorageEngine>
    TransactionConversionHack<'env, 'txn, T, S> for &'txn T
{
    #[inline]
    fn as_tx_ref(tx: &'txn T) -> Self {
        tx
    }

    #[inline]
    fn dyn_ref(self) -> &'txn dyn Transaction<'env, S> {
        self
    }
}

pub trait ExecutionMode<'env, S: StorageEngine>:
    private::Sealed + Clone + Copy + fmt::Debug + 'static
{
    type Transaction: Transaction<'env, S>;

    type TransactionRef<'txn>: Transaction<'env, S>
        + 'txn
        + Copy
        + TransactionConversionHack<'env, 'txn, Self::Transaction, S>
    where
        'env: 'txn;

    type Tree<'txn>: ReadTree<'env, 'txn, S>
    where
        'env: 'txn;

    fn begin(storage: &'env S) -> Result<Self::Transaction, S::Error>;

    fn open_tree<'txn>(
        storage: &S,
        txn: Self::TransactionRef<'txn>,
        name: &str,
    ) -> Result<Self::Tree<'txn>, S::Error>;
}

mod private {
    pub trait Sealed {}
}

#[derive(Clone, Copy, Debug)]
pub enum ReadonlyExecutionMode {}

impl private::Sealed for ReadonlyExecutionMode {}

impl<'env, S: StorageEngine> ExecutionMode<'env, S> for ReadonlyExecutionMode {
    type Transaction = S::Transaction<'env>;

    type TransactionRef<'txn> = &'txn dyn Transaction<'env, S> where 'env: 'txn;

    type Tree<'txn> = S::ReadTree<'env, 'txn> where 'env: 'txn;

    fn begin(storage: &'env S) -> Result<Self::Transaction, <S as StorageEngine>::Error> {
        storage.begin()
    }

    fn open_tree<'txn>(
        storage: &S,
        txn: Self::TransactionRef<'txn>,
        name: &str,
    ) -> Result<Self::Tree<'txn>, S::Error> {
        Ok(storage
            .open_tree(txn, name)?
            .unwrap_or_else(|| panic!("tree {:?} does not exist ", name)))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ReadWriteExecutionMode;

impl private::Sealed for ReadWriteExecutionMode {}

impl<'env, S: StorageEngine> ExecutionMode<'env, S> for ReadWriteExecutionMode {
    type Transaction = S::WriteTransaction<'env>;

    type TransactionRef<'txn> = &'txn S::WriteTransaction<'env> where 'env: 'txn;

    type Tree<'txn> = S::WriteTree<'env, 'txn>
    where
        'env: 'txn;

    fn begin(storage: &'env S) -> Result<Self::Transaction, S::Error> {
        storage.begin_write()
    }

    fn open_tree<'txn>(
        storage: &S,
        txn: Self::TransactionRef<'txn>,
        name: &str,
    ) -> Result<Self::Tree<'txn>, <S as StorageEngine>::Error> {
        storage.open_write_tree(txn, name)
    }
}
