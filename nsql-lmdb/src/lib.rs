#![deny(rust_2018_idioms)]
#![feature(return_position_impl_trait_in_trait)]
#![feature(impl_trait_projections)]

use std::ops::{Deref, RangeBounds};
use std::path::Path;
use std::sync::Arc;

use heed::types::ByteSlice;
use heed::Flag;
use nsql_storage_engine::{ReadTree, StorageEngine, Transaction, WriteTransaction, WriteTree};

type Result<T, E = heed::Error> = std::result::Result<T, E>;

type UntypedDatabase = heed::Database<ByteSlice, ByteSlice>;

#[derive(Clone)]
pub struct LmdbStorageEngine {
    env: heed::Env,
    main_db: UntypedDatabase,
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

    type Transaction<'env> = ReadonlyTx<'env>;

    type WriteTransaction<'env> = ReadWriteTx<'env>;

    type ReadTree<'env, 'txn> = UntypedDatabase where 'env: 'txn;

    type Tree<'env, 'txn> = UntypedDatabase where 'env: 'txn;

    #[inline]
    fn open(path: impl AsRef<Path>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        // large value `max_readers` has a performance issues so I don't think having a lmdb database per table is practical.
        // Perhaps we can do a lmdb database per schema and have a reasonable limit on it (say ~100)
        std::fs::OpenOptions::new().create(true).write(true).truncate(false).open(&path)?;
        let env = unsafe { heed::EnvOpenOptions::new().flag(Flag::NoSubDir).flag(Flag::NoTls) }
            .open(path)?;
        let main_db =
            env.open_database(&env.read_txn()?, None)?.expect("main database should exist");
        Ok(Self { main_db, env })
    }

    #[inline]
    fn begin_readonly(&self) -> Result<Self::Transaction<'_>, Self::Error> {
        let tx = self.env.read_txn()?;
        Ok(ReadonlyTx(Arc::new(SendRoTxnWrapper(tx))))
    }

    #[inline]
    fn begin(&self) -> std::result::Result<Self::WriteTransaction<'_>, Self::Error> {
        let inner = self.env.write_txn()?;
        Ok(ReadWriteTx(inner))
    }

    #[inline]
    fn open_tree_readonly<'env, 'txn>(
        &self,
        txn: &'txn Self::Transaction<'env>,
        name: &str,
    ) -> Result<Option<Self::ReadTree<'env, 'txn>>, Self::Error>
    where
        'env: 'txn,
    {
        self.env.open_database(&txn.0, Some(name))
    }

    #[inline]
    fn open_tree<'env, 'txn>(
        &self,
        txn: &'txn mut Self::WriteTransaction<'env>,
        name: &str,
    ) -> Result<Self::Tree<'env, 'txn>, Self::Error>
    where
        'env: 'txn,
    {
        self.env.create_database(&mut txn.0, Some(name))
    }
}

impl<'txn> ReadTree<'_, 'txn, LmdbStorageEngine> for UntypedDatabase {
    type Bytes = &'txn [u8];

    #[inline]
    fn get(
        &self,
        txn: &'txn ReadonlyTx<'_>,
        key: &[u8],
    ) -> Result<Option<Self::Bytes>, heed::Error> {
        self.get(&txn.0, key)
    }

    #[inline]
    fn range(
        &'txn self,
        txn: &'txn ReadonlyTx<'_>,
        range: impl RangeBounds<[u8]>,
    ) -> Result<impl Iterator<Item = Result<(Self::Bytes, Self::Bytes), heed::Error>>> {
        self.range(&txn.0, &(range.start_bound(), range.end_bound()))
    }

    #[inline]
    fn rev_range(
        &'txn self,
        txn: &'txn ReadonlyTx<'_>,
        range: impl RangeBounds<[u8]>,
    ) -> Result<impl Iterator<Item = Result<(Self::Bytes, Self::Bytes), heed::Error>>, heed::Error>
    {
        self.rev_range(&txn.0, &range)
    }
}

impl WriteTree<'_, '_, LmdbStorageEngine> for UntypedDatabase {
    #[inline]
    fn put(
        &mut self,
        txn: &mut ReadWriteTx<'_>,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), heed::Error> {
        (*self).put(&mut txn.0, key, value)
    }

    #[inline]
    fn delete(&mut self, txn: &mut ReadWriteTx<'_>, key: &[u8]) -> Result<bool, heed::Error> {
        (*self).delete(&mut txn.0, key)
    }
}

impl<'env> Transaction<'env, LmdbStorageEngine> for ReadonlyTx<'env> {}

impl<'env> Transaction<'env, LmdbStorageEngine> for ReadWriteTx<'env> {}

impl<'env> WriteTransaction<'env, LmdbStorageEngine> for ReadWriteTx<'env> {
    fn commit(self) -> Result<(), heed::Error> {
        self.0.commit()
    }

    #[inline]
    fn rollback(self) -> Result<(), heed::Error> {
        self.0.abort();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
