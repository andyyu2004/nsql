#![deny(rust_2018_idioms)]
#![feature(return_position_impl_trait_in_trait)]
#![feature(impl_trait_projections)]

use std::ops::{Deref, RangeBounds};
use std::path::Path;
use std::sync::Arc;

use heed::types::ByteSlice;
use heed::Flag;
use nsql_storage_engine::{
    fallible_iterator, Range, ReadOrWriteTransactionRef, ReadTree, StorageEngine, Transaction,
    WriteTransaction, WriteTree,
};

type Result<T, E = heed::Error> = std::result::Result<T, E>;

type UntypedDatabase = heed::Database<ByteSlice, ByteSlice>;

#[derive(Clone, Debug)]
pub struct LmdbStorageEngine {
    env: heed::Env,
}

#[derive(Clone)]
pub struct ReadonlyTx<'env>(Arc<SendRoTxnWrapper<'env>>);

struct SendRoTxnWrapper<'env>(heed::RoTxn<'env>);

impl<'env> Deref for SendRoTxnWrapper<'env> {
    type Target = heed::RoTxn<'env>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// This type is apparently safe to send across threads but heed doesn't have the implementation
// The `sync-read-txn` enables `Sync` but not `Send` currently.
// https://github.com/meilisearch/heed/issues/149
// FIXME judging by `lmdb.h` comments I don't think it is `Sync` (but it is `Send`)
unsafe impl Send for SendRoTxnWrapper<'_> {}
unsafe impl Sync for SendRoTxnWrapper<'_> {}

pub struct ReadWriteTx<'env>(heed::RwTxn<'env>);

impl StorageEngine for LmdbStorageEngine {
    type Error = heed::Error;

    type Bytes<'txn> = &'txn [u8];

    type Transaction<'env> = ReadonlyTx<'env>;

    type WriteTransaction<'env> = ReadWriteTx<'env>;

    type ReadTree<'env, 'txn> = LmdbReadTree<'env, 'txn> where 'env: 'txn;

    type WriteTree<'env, 'txn> = LmdbWriteTree<'env, 'txn> where 'env: 'txn;

    fn create(path: impl AsRef<Path>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        std::fs::OpenOptions::new().create(true).write(true).truncate(false).open(&path)?;
        Self::open(path)
    }

    #[inline]
    fn open(path: impl AsRef<Path>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        // large value `max_readers` has a performance issues so I don't think having a lmdb database per table is practical.
        // Perhaps we can do a lmdb database per schema and have a reasonable limit on it (say ~100)
        let env = unsafe { heed::EnvOpenOptions::new().flag(Flag::NoSubDir).flag(Flag::NoTls) }
            .max_dbs(100)
            .open(path)?;
        Ok(Self { env })
    }

    #[inline]
    fn begin(&self) -> Result<Self::Transaction<'_>, Self::Error> {
        let tx = self.env.read_txn()?;
        Ok(ReadonlyTx(Arc::new(SendRoTxnWrapper(tx))))
    }

    #[inline]
    fn begin_write(&self) -> Result<Self::WriteTransaction<'_>, Self::Error> {
        let inner = self.env.write_txn()?;
        Ok(ReadWriteTx(inner))
    }

    #[inline]
    fn open_tree<'env, 'txn>(
        &self,
        txn: &'txn dyn Transaction<'env, Self>,
        name: &str,
    ) -> Result<Option<Self::ReadTree<'env, 'txn>>, Self::Error>
    where
        'env: 'txn,
    {
        let txn = match txn.as_read_or_write_ref() {
            ReadOrWriteTransactionRef::Read(txn) => &*txn.0,
            ReadOrWriteTransactionRef::Write(txn) => &*txn.0,
        };
        Ok(self.env.open_database(txn, Some(name))?.map(|db| LmdbReadTree { db, txn }))
    }

    #[inline]
    fn open_write_tree<'env, 'txn>(
        &self,
        txn: &'txn Self::WriteTransaction<'env>,
        name: &str,
    ) -> Result<Self::WriteTree<'env, 'txn>, Self::Error>
    where
        'env: 'txn,
    {
        let db = self.env.create_database(&txn.0, Some(name))?;
        Ok(LmdbWriteTree { db, txn })
    }

    #[inline]
    fn drop_tree(&self, txn: &Self::WriteTransaction<'_>, name: &str) -> Result<(), Self::Error> {
        self.open_write_tree(txn, name)?.db.drop(&txn.0)?;
        Ok(())
    }
}

pub struct LmdbReadTree<'env, 'txn> {
    db: UntypedDatabase,
    txn: &'txn heed::RoTxn<'env>,
}

unsafe impl Send for LmdbReadTree<'_, '_> {}

unsafe impl Sync for LmdbReadTree<'_, '_> {}

pub struct LmdbWriteTree<'env, 'txn> {
    db: UntypedDatabase,
    txn: &'txn ReadWriteTx<'env>,
}

unsafe impl Send for LmdbWriteTree<'_, '_> {}

impl<'env, 'txn> ReadTree<'env, 'txn, LmdbStorageEngine> for LmdbReadTree<'env, 'txn> {
    #[inline]
    fn get<'a>(
        &'a self,
        key: &[u8],
    ) -> Result<Option<<LmdbStorageEngine as StorageEngine>::Bytes<'a>>, heed::Error> {
        self.db.get(self.txn, key)
    }

    #[inline]
    fn range<'a>(
        &'a self,
        range: impl RangeBounds<[u8]> + 'a,
    ) -> Result<Range<'a, LmdbStorageEngine>, heed::Error> {
        self.db
            .range(self.txn, &range)
            .map(fallible_iterator::convert)
            .map(|iter| Box::new(iter) as _)
    }

    #[inline]
    fn rev_range<'a>(
        &'a self,
        range: impl RangeBounds<[u8]> + 'a,
    ) -> Result<Range<'a, LmdbStorageEngine>, heed::Error> {
        self.db
            .rev_range(self.txn, &range)
            .map(fallible_iterator::convert)
            .map(|iter| Box::new(iter) as _)
    }
}

impl<'env, 'txn> ReadTree<'env, 'txn, LmdbStorageEngine> for LmdbWriteTree<'env, 'txn> {
    #[inline]
    fn get<'a>(
        &'a self,
        key: &[u8],
    ) -> Result<
        Option<<LmdbStorageEngine as StorageEngine>::Bytes<'a>>,
        <LmdbStorageEngine as StorageEngine>::Error,
    > {
        self.db.get(&self.txn.0, key)
    }

    #[inline]
    fn range<'a>(
        &'a self,
        range: impl RangeBounds<[u8]> + 'a,
    ) -> Result<Range<'a, LmdbStorageEngine>, heed::Error> {
        self.db
            .range(&self.txn.0, &range)
            .map(fallible_iterator::convert)
            .map(|iter| Box::new(iter) as _)
    }

    #[inline]
    fn rev_range<'a>(
        &'a self,
        range: impl RangeBounds<[u8]> + 'a,
    ) -> Result<Range<'a, LmdbStorageEngine>, heed::Error> {
        self.db
            .rev_range(&self.txn.0, &range)
            .map(fallible_iterator::convert)
            .map(|iter| Box::new(iter) as _)
    }
}

impl<'env, 'txn> WriteTree<'env, 'txn, LmdbStorageEngine> for LmdbWriteTree<'env, 'txn> {
    #[inline]
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), heed::Error> {
        self.db.put(&self.txn.0, key, value)
    }

    #[inline]
    fn delete(&mut self, key: &[u8]) -> Result<bool, heed::Error> {
        self.db.delete(&self.txn.0, key)
    }
}

impl<'env> Transaction<'env, LmdbStorageEngine> for ReadonlyTx<'env> {
    #[inline]
    fn as_read_or_write_ref(&self) -> ReadOrWriteTransactionRef<'env, '_, LmdbStorageEngine> {
        ReadOrWriteTransactionRef::Read(self)
    }

    #[inline]
    fn as_dyn(&self) -> &dyn Transaction<'env, LmdbStorageEngine> {
        self
    }
}

impl<'env> Transaction<'env, LmdbStorageEngine> for ReadWriteTx<'env> {
    #[inline]
    fn as_read_or_write_ref(&self) -> ReadOrWriteTransactionRef<'env, '_, LmdbStorageEngine> {
        ReadOrWriteTransactionRef::Write(self)
    }

    #[inline]
    fn as_dyn(&self) -> &dyn Transaction<'env, LmdbStorageEngine> {
        self
    }
}

impl<'env> WriteTransaction<'env, LmdbStorageEngine> for ReadWriteTx<'env> {
    #[inline]
    fn commit(self) -> Result<(), heed::Error> {
        self.0.commit()
    }

    #[inline]
    fn abort(self) -> Result<(), heed::Error> {
        self.0.abort();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
