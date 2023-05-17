#![deny(rust_2018_idioms)]
use std::path::Path;

use heed::UntypedDatabase;
use nsql_storage_engine::{ReadTransaction, ReadTree, StorageEngine, Transaction, Tree};

type Result<T, E = heed::Error> = std::result::Result<T, E>;

pub struct LmdbStorageEngine {
    env: heed::Env,
    main_db: UntypedDatabase,
}

pub struct ReadonlyTx<'env> {
    tx: heed::RoTxn<'env>,
}

pub struct ReadWriteTx<'env> {
    tx: heed::RwTxn<'env, 'env>,
}

impl StorageEngine for LmdbStorageEngine {
    type Error = heed::Error;

    type ReadTransaction<'env> = ReadonlyTx<'env>;

    type Transaction<'env> = ReadWriteTx<'env>;

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
        let env = unsafe { heed::EnvOpenOptions::new().flag(heed::flags::Flags::MdbNoSubDir) }
            .open(path)?;
        let main_db = env.open_database(None)?.expect("main database should exist");
        Ok(Self { main_db, env })
    }

    #[inline]
    fn begin_readonly(&self) -> Result<Self::ReadTransaction<'_>, Self::Error> {
        let tx = self.env.read_txn()?;
        Ok(ReadonlyTx { tx })
    }

    #[inline]
    fn begin(&self) -> std::result::Result<Self::Transaction<'_>, Self::Error> {
        let inner = self.env.write_txn()?;
        Ok(ReadWriteTx { tx: inner })
    }

    fn open_tree_readonly<'env, 'txn>(
        &self,
        _txn: &'env Self::ReadTransaction<'txn>,
        name: &str,
    ) -> Result<Option<Self::ReadTree<'env, 'txn>>, Self::Error>
    where
        'env: 'txn,
    {
        self.env.open_database(Some(name))
    }

    fn open_tree<'env, 'txn>(
        &self,
        _txn: &'txn Self::Transaction<'env>,
        name: &str,
    ) -> Result<Option<Self::Tree<'env, 'txn>>, Self::Error> {
        self.env.open_database(Some(name))
    }
}

impl<'txn> ReadTree<'_, 'txn, LmdbStorageEngine> for heed::UntypedDatabase {
    type Bytes = &'txn [u8];

    #[inline]
    fn get(
        &self,
        txn: &'txn ReadonlyTx<'_>,
        key: &[u8],
    ) -> Result<Option<Self::Bytes>, heed::Error> {
        self.get(&txn.tx, key)
    }
}

impl Tree<'_, '_, LmdbStorageEngine> for heed::UntypedDatabase {
    #[inline]
    fn put(
        &mut self,
        txn: &mut ReadWriteTx<'_>,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), heed::Error> {
        (*self).put(&mut txn.tx, key, value)
    }

    #[inline]
    fn delete(&mut self, txn: &mut ReadWriteTx<'_>, key: &[u8]) -> Result<bool, heed::Error> {
        (*self).delete(&mut txn.tx, key)
    }
}

impl<'env> ReadTransaction for ReadonlyTx<'env> {
    type Error = heed::Error;
}

impl<'env> ReadTransaction for ReadWriteTx<'env> {
    type Error = heed::Error;
}

impl<'env> Transaction for ReadWriteTx<'env> {}

#[cfg(test)]
mod tests;
